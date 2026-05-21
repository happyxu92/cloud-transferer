"""核心迁移逻辑。

整体流程：
1. scan_job: 递归列出 src，与 dst 做 diff，生成/更新 FileTask
2. run_job:  并发拉取 pending/failed 任务，单文件级 copy + 轮询 + 重试
"""
from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime
from pathlib import PurePosixPath

from loguru import logger
from sqlalchemy import func
from sqlmodel import select

from .alist_client import (
    AListClient,
    AListError,
    CopyTask,
    FINISHED_STATES,
    SUCCESS_STATES,
)
from .config import settings
from .db import session_scope
from .disk_guard import should_pause
from .models import FileTask, JobStatus, MigrationJob, TaskStatus


def _join(base: str, rel: str) -> str:
    base = base.rstrip("/")
    rel = rel.lstrip("/")
    return f"{base}/{rel}" if rel else base


def _relpath(full: str, root: str) -> str:
    full_p = PurePosixPath(full)
    root_p = PurePosixPath(root)
    return str(full_p.relative_to(root_p))


# =================================================================
# Scan
# =================================================================
async def scan_job(client: AListClient, job_id: int) -> int:
    """扫描 src，生成 FileTask。返回新增任务数。"""
    with session_scope() as s:
        job = s.get(MigrationJob, job_id)
        if not job:
            raise ValueError(f"job {job_id} 不存在")
        job.status = JobStatus.SCANNING
        job.last_message = "正在扫描源目录..."
        job.updated_at = datetime.utcnow()
        src_path = job.src_path
        dst_path = job.dst_path
        s.add(job)

    logger.info(f"[job {job_id}] 扫描源目录 {src_path}")
    try:
        files = await client.list_dir_recursive(src_path, refresh=False)
    except AListError as e:
        with session_scope() as s:
            job = s.get(MigrationJob, job_id)
            assert job
            job.status = JobStatus.ERROR
            job.last_message = f"扫描失败: {e}"
            s.add(job)
        raise

    logger.info(f"[job {job_id}] 源文件 {len(files)} 个，开始对比目的目录")

    # 列出目的根（用于增量判断）。失败也无所谓，按未存在处理。
    dst_index: dict[str, int] = {}  # full_path -> size
    try:
        # 目标端容易命中 AList 缓存，增量判断需要强制刷新避免漏判已存在文件。
        dst_files = await client.list_dir_recursive(dst_path, refresh=True)
        dst_index = {f.path: f.size for f in dst_files}
    except AListError as e:
        logger.warning(f"[job {job_id}] 目的目录扫描失败（视为全部未存在）: {e}")

    new_count = 0
    with session_scope() as s:
        # 已有 FileTask 的 src 路径集合，避免重复 insert
        existing_rows = s.exec(
            select(FileTask.src_full_path).where(FileTask.job_id == job_id)
        ).all()
        existing: set[str] = set(existing_rows)

        for f in files:
            rel = _relpath(f.path, src_path)
            dst_full = _join(dst_path, rel)

            # 已存在策略
            if dst_full in dst_index and dst_index[dst_full] == f.size:
                if f.path in existing:
                    continue
                ft = FileTask(
                    job_id=job_id,
                    src_full_path=f.path,
                    dst_full_path=dst_full,
                    size_bytes=f.size,
                    status=TaskStatus.SKIPPED,
                    last_error="目的已存在同名同大小文件",
                    finished_at=datetime.utcnow(),
                )
                s.add(ft)
                continue

            # 超大文件
            if f.size > settings.max_file_bytes:
                if f.path in existing:
                    continue
                ft = FileTask(
                    job_id=job_id,
                    src_full_path=f.path,
                    dst_full_path=dst_full,
                    size_bytes=f.size,
                    status=TaskStatus.OVERSIZE,
                    last_error=f"超过 {settings.max_file_gb}GB 限制",
                )
                s.add(ft)
                continue

            if f.path in existing:
                continue

            ft = FileTask(
                job_id=job_id,
                src_full_path=f.path,
                dst_full_path=dst_full,
                size_bytes=f.size,
                status=TaskStatus.PENDING,
            )
            s.add(ft)
            new_count += 1

        job = s.get(MigrationJob, job_id)
        assert job
        job.last_message = f"扫描完成，新增 {new_count} 个待迁移文件"
        job.updated_at = datetime.utcnow()
        s.add(job)

    logger.info(f"[job {job_id}] 扫描完成，新增 {new_count} 个待迁移")
    return new_count


# =================================================================
# Copy single file
# =================================================================
def _record_failure(ft_id: int, err: str) -> None:
    with session_scope() as s:
        ft = s.get(FileTask, ft_id)
        if not ft:
            return
        ft.retry_count += 1
        ft.last_error = err[:500]
        if ft.retry_count >= settings.max_retry:
            ft.status = TaskStatus.FAILED
            ft.finished_at = datetime.utcnow()
            # 失败任务结构化日志
            _append_failed_log(ft)
            logger.error(
                f"[task {ft_id}] 失败，停止重试 "
                f"({ft.retry_count}/{settings.max_retry}) {ft.dst_full_path}: {ft.last_error}"
            )
        else:
            ft.status = TaskStatus.PENDING
            logger.warning(
                f"[task {ft_id}] 失败，重新加入 pending 等待重试 "
                f"({ft.retry_count}/{settings.max_retry}) {ft.dst_full_path}: {ft.last_error}"
            )
        s.add(ft)


def _claim_next_pending(job_id: int) -> int | None:
    with session_scope() as s:
        ft = s.exec(
            select(FileTask)
            .where(
                FileTask.job_id == job_id,
                FileTask.status == TaskStatus.PENDING,
            )
            .order_by(FileTask.id)
        ).first()
        if not ft or ft.id is None:
            return None
        ft.status = TaskStatus.COPYING
        ft.started_at = datetime.utcnow()
        s.add(ft)
        return ft.id


def _mark_success(ft_id: int) -> None:
    with session_scope() as s:
        ft = s.get(FileTask, ft_id)
        if not ft:
            return
        ft.status = TaskStatus.SUCCESS
        ft.finished_at = datetime.utcnow()
        ft.last_error = ""
        s.add(ft)


def _mark_skipped(ft_id: int, reason: str) -> None:
    with session_scope() as s:
        ft = s.get(FileTask, ft_id)
        if not ft:
            return
        ft.status = TaskStatus.SKIPPED
        ft.finished_at = datetime.utcnow()
        ft.last_error = reason[:500]
        s.add(ft)


def _reset_pending(ft_id: int, err: str = "") -> None:
    with session_scope() as s:
        ft = s.get(FileTask, ft_id)
        if not ft:
            return
        ft.status = TaskStatus.PENDING
        ft.alist_task_id = ""
        ft.started_at = None
        ft.finished_at = None
        ft.last_error = err[:500]
        s.add(ft)


def _append_failed_log(ft: FileTask) -> None:
    rec = {
        "ts": datetime.utcnow().isoformat(),
        "job_id": ft.job_id,
        "src": ft.src_full_path,
        "dst": ft.dst_full_path,
        "size": ft.size_bytes,
        "retry": ft.retry_count,
        "error": ft.last_error,
    }
    try:
        with open(settings.failed_log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception as e:
        logger.warning(f"写失败日志失败: {e}")


def _get_job_task_counts(job_id: int) -> dict[TaskStatus, int]:
    with session_scope() as s:
        rows = s.exec(
            select(FileTask.status, func.count())
            .where(FileTask.job_id == job_id)
            .group_by(FileTask.status)
        ).all()
    return {status: int(cnt) for status, cnt in rows}


def _update_job_summary(job_id: int, *, active_copying: int = 0) -> None:
    counts = _get_job_task_counts(job_id)
    pending = counts.get(TaskStatus.PENDING, 0)
    copying = counts.get(TaskStatus.COPYING, 0)
    success = counts.get(TaskStatus.SUCCESS, 0)
    failed = counts.get(TaskStatus.FAILED, 0)
    skipped = counts.get(TaskStatus.SKIPPED, 0)
    oversize = counts.get(TaskStatus.OVERSIZE, 0)
    total = sum(counts.values())

    with session_scope() as s:
        job = s.get(MigrationJob, job_id)
        if not job:
            return

        if total == 0:
            if job.status in {JobStatus.RUNNING, JobStatus.SCANNING}:
                job.status = JobStatus.IDLE
                job.last_message = "执行中断，待重新运行"
                job.updated_at = datetime.utcnow()
                s.add(job)
            return

        if active_copying > 0:
            job.status = JobStatus.RUNNING
            job.last_message = f"正在迁移... 活跃 {active_copying}，待处理 {pending}"
        elif pending > 0 or copying > 0:
            job.status = JobStatus.IDLE
            job.last_message = f"执行中断，待重新运行: 待处理 {pending + copying}"
        elif failed == 0:
            job.status = JobStatus.DONE
            job.last_message = (
                f"完成: 共{total} 成功{success} 跳过{skipped} "
                f"超大{oversize} 失败{failed}"
            )
        else:
            job.status = JobStatus.ERROR
            job.last_message = (
                f"完成: 共{total} 成功{success} 跳过{skipped} "
                f"超大{oversize} 失败{failed}"
            )

        job.updated_at = datetime.utcnow()
        s.add(job)


def mark_job_interrupted(job_id: int) -> None:
    with session_scope() as s:
        rows = s.exec(
            select(FileTask).where(
                FileTask.job_id == job_id,
                FileTask.status == TaskStatus.COPYING,
                FileTask.alist_task_id == "",
            )
        ).all()
        for ft in rows:
            ft.status = TaskStatus.PENDING
            ft.started_at = None
            ft.finished_at = None
            ft.last_error = "执行中断，等待恢复"
            s.add(ft)

    counts = _get_job_task_counts(job_id)
    active_copying = counts.get(TaskStatus.COPYING, 0)

    with session_scope() as s:
        job = s.get(MigrationJob, job_id)
        if not job:
            return
        if active_copying > 0:
            job.status = JobStatus.RUNNING
        elif counts.get(TaskStatus.PENDING, 0) > 0:
            job.status = JobStatus.IDLE
        job.last_message = "执行已中断，可重新运行以恢复"
        job.updated_at = datetime.utcnow()
        s.add(job)


async def _copy_one(client: AListClient, ft_id: int, *, already_claimed: bool = False) -> None:
    with session_scope() as s:
        ft = s.get(FileTask, ft_id)
        if not ft:
            return
        if not already_claimed:
            ft.status = TaskStatus.COPYING
            ft.started_at = datetime.utcnow()
            s.add(ft)
        src_full = ft.src_full_path
        dst_full = ft.dst_full_path
        size = ft.size_bytes

    src_dir = str(PurePosixPath(src_full).parent)
    dst_dir = str(PurePosixPath(dst_full).parent)
    name = PurePosixPath(src_full).name

    try:
        await client.ensure_dir(dst_dir)
    except AListError as e:
        _record_failure(ft_id, f"mkdir 失败: {e}")
        return

    logger.info(
        f"[task {ft_id}] copy {src_full} -> {dst_full} ({size/1024/1024:.1f} MB)"
    )

    try:
        task_ids = await client.copy(src_dir, dst_dir, [name])
    except AListError as e:
        err_text = str(e).lower()
        if settings.exists_policy == "skip" and "exists" in err_text:
            if await _verify_dst(client, dst_full, size):
                _mark_skipped(ft_id, "目的已存在同名同大小文件")
                logger.info(f"[task {ft_id}] 跳过已存在文件 {dst_full}")
                return
            _record_failure(ft_id, f"目标已存在但大小不匹配: {e}")
            return
        _record_failure(ft_id, f"copy 触发失败: {e}")
        return

    # AList 老版本未返回 id：通过名字匹配找回
    task_id = task_ids[0] if task_ids else await _find_task_id_by_name(client, name)
    if not task_id:
        _record_failure(ft_id, "未能取得 AList task id")
        return

    with session_scope() as s:
        ft = s.get(FileTask, ft_id)
        if ft:
            ft.alist_task_id = task_id
            s.add(ft)

    await _finish_copy_task(client, ft_id, task_id, dst_full, size)


async def _finish_copy_task(
    client: AListClient,
    ft_id: int,
    task_id: str,
    dst_full: str,
    size: int,
) -> None:

    # 轮询
    try:
        deadline = time.time() + settings.task_timeout_sec
        task: CopyTask | None = None
        missing_checks = 0
        while time.time() < deadline:
            task = await client.get_copy_task(task_id)
            if task is None:
                missing_checks += 1
                if missing_checks >= 2 and await _verify_dst(client, dst_full, size):
                    _mark_success(ft_id)
                    logger.success(f"[task {ft_id}] 成功 {dst_full}")
                    return
            else:
                missing_checks = 0
                if task.state in FINISHED_STATES:
                    break
            await asyncio.sleep(5.0)

        if task is None:
            if await _verify_dst(client, dst_full, size):
                _mark_success(ft_id)
                logger.success(f"[task {ft_id}] 成功 {dst_full}")
                return
            raise TimeoutError(f"任务 {task_id} 在 AList 中未找到")
        if task.state not in FINISHED_STATES:
            raise TimeoutError(
                f"任务 {task_id} 超时未完成 (state={task.state}, progress={task.progress:.1f})"
            )
    except TimeoutError as e:
        _record_failure(ft_id, str(e))
        return
    except Exception as e:
        _record_failure(ft_id, f"轮询异常: {e}")
        return

    if task.state in SUCCESS_STATES:
        # 校验目的文件大小
        ok = await _verify_dst(client, dst_full, size)
        if not ok:
            _record_failure(ft_id, "目的文件大小校验失败")
            await _safe_delete_task(client, task_id)
            return
        _mark_success(ft_id)
        logger.success(f"[task {ft_id}] 成功 {dst_full}")
    else:
        _record_failure(
            ft_id, f"AList 任务失败 state={task.state} err={task.error}"
        )

    await _safe_delete_task(client, task_id)


async def _resume_copy_one(client: AListClient, ft_id: int) -> None:
    with session_scope() as s:
        ft = s.get(FileTask, ft_id)
        if not ft:
            return
        task_id = ft.alist_task_id
        dst_full = ft.dst_full_path
        size = ft.size_bytes

    if not task_id:
        _reset_pending(ft_id, "恢复中断任务: 缺少 AList task id")
        return

    logger.info(f"[task {ft_id}] 继续等待 AList 任务 {task_id}")
    await _finish_copy_task(client, ft_id, task_id, dst_full, size)


async def _recover_copying_tasks(client: AListClient, job_id: int) -> list[int]:
    with session_scope() as s:
        rows = s.exec(
            select(FileTask).where(
                FileTask.job_id == job_id,
                FileTask.status == TaskStatus.COPYING,
            )
        ).all()
        tasks = [
            (t.id, t.alist_task_id, t.dst_full_path, t.size_bytes)
            for t in rows
            if t.id is not None
        ]

    if not tasks:
        return []

    task_map: dict[str, CopyTask] = {}
    for t in await client.list_undone_copy():
        task_map[t.id] = t
    for t in await client.list_done_copy():
        task_map[t.id] = t

    active_ids: list[int] = []
    recovered = 0
    for ft_id, task_id, dst_full, size in tasks:
        if not task_id:
            _reset_pending(ft_id, "恢复中断任务")
            recovered += 1
            continue

        backend = task_map.get(task_id)
        if backend is None:
            if await _verify_dst(client, dst_full, size):
                _mark_success(ft_id)
            else:
                _reset_pending(ft_id, "恢复中断任务")
            recovered += 1
            continue

        if backend.state in SUCCESS_STATES:
            if await _verify_dst(client, dst_full, size):
                _mark_success(ft_id)
            else:
                _record_failure(ft_id, "目的文件大小校验失败")
            await _safe_delete_task(client, task_id)
            recovered += 1
            continue

        if backend.state in FINISHED_STATES:
            _record_failure(
                ft_id, f"AList 任务失败 state={backend.state} err={backend.error}"
            )
            await _safe_delete_task(client, task_id)
            recovered += 1
            continue

        active_ids.append(ft_id)

    if recovered or active_ids:
        logger.info(
            f"[job {job_id}] 恢复 {recovered} 个中断任务，继续等待 {len(active_ids)} 个在途任务"
        )
    return active_ids


async def _find_task_id_by_name(client: AListClient, name: str) -> str | None:
    """老版本 AList 兼容：通过任务名查找最近一个匹配。"""
    tasks: list[CopyTask] = []
    tasks += await client.list_undone_copy()
    tasks += await client.list_done_copy()
    # 倒序，取最新
    for t in reversed(tasks):
        if name in t.name:
            return t.id
    return None


async def _verify_dst(
    client: AListClient,
    path: str,
    expect_size: int,
    *,
    timeout_sec: int = 10,
    poll_interval: float = 3.0,
) -> bool:
    # AList 后台任务完成后，夸克侧目录刷新经常会滞后几秒到几十秒。
    # 仅调用 fs/get 仍可能命中旧缓存，这里补一次 refresh=True 的目录级校验，
    # 避免把已成功复制误判为失败并触发重试。
    parent = str(PurePosixPath(path).parent)
    name = PurePosixPath(path).name

    async def _exists_with_expected_size() -> bool:
        info = await client.get(path)
        if info and int(info.get("size") or 0) == expect_size:
            return True

        try:
            entries = await client.list_dir(parent, refresh=True)
        except AListError:
            return False

        for entry in entries:
            if entry.name == name and int(entry.size or 0) == expect_size:
                return True
        return False

    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        if await _exists_with_expected_size():
            return True
        await asyncio.sleep(poll_interval)

    return await _exists_with_expected_size()


async def _safe_delete_task(client: AListClient, task_id: str) -> None:
    try:
        await client.delete_copy_task(task_id)
    except Exception:
        pass


# =================================================================
# Run job
# =================================================================
async def run_job(client: AListClient, job_id: int) -> None:
    """执行 job：扫描 + 并发 copy。"""
    active_copying = await _recover_copying_tasks(client, job_id)
    await scan_job(client, job_id)

    with session_scope() as s:
        job = s.get(MigrationJob, job_id)
        assert job
        job.status = JobStatus.RUNNING
        job.last_message = "正在迁移..."
        s.add(job)

    with session_scope() as s:
        pending = s.exec(
            select(func.count()).select_from(FileTask).where(
                FileTask.job_id == job_id,
                FileTask.status == TaskStatus.PENDING,
            )
        ).one()
    if pending:
        logger.info(f"[job {job_id}] 本轮待处理 {pending} 个文件")

    resume_queue: asyncio.Queue[int] = asyncio.Queue()
    for ft_id in active_copying:
        resume_queue.put_nowait(ft_id)

    async def wait_if_paused() -> None:
        if should_pause():
            # 等待直到磁盘恢复或循环上限
            for _ in range(60):
                await asyncio.sleep(30)
                if not should_pause():
                    break

    async def worker(slot: int) -> None:
        while True:
            next_id: int | None = None
            try:
                ft_id = resume_queue.get_nowait()
            except asyncio.QueueEmpty:
                ft_id = None

            try:
                if ft_id is not None:
                    await _resume_copy_one(client, ft_id)
                    continue

                await wait_if_paused()
                next_id = _claim_next_pending(job_id)
                if next_id is None:
                    return

                await _copy_one(client, next_id, already_claimed=True)
            except Exception as e:
                failed_id = ft_id if ft_id is not None else next_id
                if failed_id is not None:
                    _record_failure(failed_id, f"未捕获异常: {e}")
                logger.exception(f"[job {job_id}] worker {slot} 执行异常: {e}")

    await asyncio.gather(
        *(worker(slot) for slot in range(max(1, settings.max_concurrency)))
    )
    # 让 DB 状态稳定
    await asyncio.sleep(1)

    # 汇总
    with session_scope() as s:
        total = s.exec(
            select(func.count()).select_from(FileTask).where(
                FileTask.job_id == job_id
            )
        ).one()
        ok = s.exec(
            select(func.count()).select_from(FileTask).where(
                FileTask.job_id == job_id,
                FileTask.status == TaskStatus.SUCCESS,
            )
        ).one()
        failed = s.exec(
            select(func.count()).select_from(FileTask).where(
                FileTask.job_id == job_id,
                FileTask.status == TaskStatus.FAILED,
            )
        ).one()
        skipped = s.exec(
            select(func.count()).select_from(FileTask).where(
                FileTask.job_id == job_id,
                FileTask.status == TaskStatus.SKIPPED,
            )
        ).one()
        oversize = s.exec(
            select(func.count()).select_from(FileTask).where(
                FileTask.job_id == job_id,
                FileTask.status == TaskStatus.OVERSIZE,
            )
        ).one()

        job = s.get(MigrationJob, job_id)
        assert job
        if failed == 0:
            job.status = JobStatus.DONE
        else:
            job.status = JobStatus.ERROR
        job.last_message = (
            f"完成: 共{total} 成功{ok} 跳过{skipped} "
            f"超大{oversize} 失败{failed}"
        )
        job.updated_at = datetime.utcnow()
        s.add(job)
        logger.info(f"[job {job_id}] {job.last_message}")
