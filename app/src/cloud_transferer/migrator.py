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
        else:
            ft.status = TaskStatus.PENDING
        s.add(ft)


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


async def _copy_one(client: AListClient, ft_id: int) -> None:
    with session_scope() as s:
        ft = s.get(FileTask, ft_id)
        if not ft:
            return
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
        task = await client.wait_task(
            task_id,
            timeout_sec=settings.task_timeout_sec,
            poll_interval=5.0,
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
    # 这里按总超时轮询，避免把已成功复制误判为校验失败并触发重试。
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        info = await client.get(path)
        if info and int(info.get("size") or 0) == expect_size:
            return True
        await asyncio.sleep(poll_interval)

    info = await client.get(path)
    return bool(info and int(info.get("size") or 0) == expect_size)


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

    sem = asyncio.Semaphore(settings.max_concurrency)

    async def worker(ft_id: int) -> None:
        async with sem:
            if should_pause():
                # 等待直到磁盘恢复或循环上限
                for _ in range(60):
                    await asyncio.sleep(30)
                    if not should_pause():
                        break
            await _copy_one(client, ft_id)

    async def resume_worker(ft_id: int) -> None:
        async with sem:
            await _resume_copy_one(client, ft_id)

    if active_copying:
        await asyncio.gather(*(resume_worker(rid) for rid in active_copying))

    # 循环直到没有 pending 任务（处理重试导致的回流）
    while True:
        with session_scope() as s:
            rows = s.exec(
                select(FileTask.id).where(
                    FileTask.job_id == job_id,
                    FileTask.status == TaskStatus.PENDING,
                )
            ).all()
        if not rows:
            break
        logger.info(f"[job {job_id}] 本轮待处理 {len(rows)} 个文件")
        await asyncio.gather(*(worker(rid) for rid in rows))
        # 让 DB 状态稳定
        await asyncio.sleep(1)

    # 汇总
    with session_scope() as s:
        from sqlalchemy import func

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
