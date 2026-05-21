"""命令行入口。

用法:
  ct run                            # 长驻：启动调度器+默认 job
  ct job add NAME SRC DST [--cron "0 3 * * *"] [--once]
  ct job list
  ct job rm ID
  ct job enable ID / disable ID
  ct job run ID                     # 立即执行一次
  ct task list [--job ID] [--status failed]
  ct task progress [--job ID]       # 持续刷新查看 copying 任务实时进度
  ct task retry [--job ID]          # 把 failed 重置为 pending
  ct doctor                         # 连通性检查
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime
from typing import Optional

import typer
from loguru import logger
from rich.console import Console, Group
from rich.live import Live
from rich.table import Table
from rich.text import Text
from sqlmodel import select

from .alist_client import AListClient, AListError, CopyTask
from .config import settings
from .db import init_db, session_scope
from .logging_setup import setup_logging
from .migrator import mark_job_interrupted, run_job
from .models import FileTask, JobMode, JobStatus, MigrationJob, TaskStatus
from .scheduler import serve_forever

app = typer.Typer(add_completion=False, help="百度->夸克 自动迁移工具")
job_app = typer.Typer(help="迁移任务管理")
task_app = typer.Typer(help="文件级任务管理")
app.add_typer(job_app, name="job")
app.add_typer(task_app, name="task")

console = Console()

ALIST_STATE_LABELS = {
    0: "pending",
    1: "running",
    2: "success",
    3: "canceling",
    4: "canceled",
    5: "errored",
    6: "failing",
    7: "failed",
    8: "waiting",
    9: "before",
}


def _new_client() -> AListClient:
    return AListClient(
        base_url=settings.alist_base,
        username=settings.alist_username,
        password=settings.alist_password,
    )


def _alist_state_label(state: int | None) -> str:
    if state is None:
        return "-"
    return ALIST_STATE_LABELS.get(state, str(state))


def _format_speed(bytes_per_sec: float | None) -> str:
    if bytes_per_sec is None or bytes_per_sec <= 0:
        return "-"
    units = ["B/s", "KB/s", "MB/s", "GB/s", "TB/s"]
    value = bytes_per_sec
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f} {unit}"
        value /= 1024
    return "-"


def _estimate_done_bytes(task: CopyTask) -> float | None:
    if task.total_bytes <= 0:
        return None
    progress = min(max(task.progress, 0.0), 100.0)
    return task.total_bytes * progress / 100.0


def _build_progress_renderable(
    rows: list[dict[str, object]],
    task_map: dict[str, CopyTask],
    samples: dict[str, tuple[float, float]],
    job: Optional[int],
    interval: float,
) -> Group:
    now = time.monotonic()
    title = f"Task Progress (job={job or 'ALL'}, refresh={interval:.1f}s)"
    table = Table(title=title)
    for col in ["ID", "Job", "Progress", "Speed", "AList", "Size(MB)", "Src", "Dst", "Error"]:
        table.add_column(col)

    active_ids: set[str] = set()
    for row in rows:
        backend = task_map.get(str(row["alist_task_id"]))
        speed = "-"
        if not row["alist_task_id"]:
            progress = "starting"
            alist_state = "starting"
        elif backend is None:
            progress = "-"
            alist_state = "missing"
        else:
            active_ids.add(backend.id)
            progress = f"{backend.progress:.1f}%"
            alist_state = _alist_state_label(backend.state)
            if backend.error and backend.state == 1:
                alist_state = "running(err)"

            done_bytes = _estimate_done_bytes(backend)
            prev = samples.get(backend.id)
            if done_bytes is not None and prev is not None and backend.state == 1:
                prev_ts, prev_done_bytes = prev
                delta_time = now - prev_ts
                delta_bytes = done_bytes - prev_done_bytes
                if delta_time > 0 and delta_bytes >= 0:
                    speed = _format_speed(delta_bytes / delta_time)
                elif delta_bytes < 0:
                    speed = "-"
            elif done_bytes is not None and backend.state == 1:
                speed = "warming"

            if done_bytes is not None:
                samples[backend.id] = (now, done_bytes)

        err = ""
        if backend and backend.error:
            err = backend.error
        elif row["last_error"]:
            err = str(row["last_error"])

        table.add_row(
            str(row["id"]),
            str(row["job_id"]),
            progress,
            speed,
            alist_state,
            f"{int(row['size_bytes'])/1024/1024:.1f}",
            str(row["src_full_path"])[-50:],
            str(row["dst_full_path"])[-50:],
            err[:80],
        )

    stale_ids = set(samples) - active_ids
    for task_id in stale_ids:
        samples.pop(task_id, None)

    if rows:
        status = Text(
            f"Updated {datetime.now().strftime('%H:%M:%S')}  Ctrl+C to exit",
            style="cyan",
        )
    else:
        status = Text(
            f"job={job or 'ALL'} 当前没有处于 copying 的任务，等待刷新...  Ctrl+C to exit",
            style="yellow",
        )
    return Group(status, table)


@app.callback()
def _bootstrap() -> None:
    setup_logging()
    init_db()


# ----------------------------- run -----------------------------
@app.command("run")
def cmd_run() -> None:
    """长驻进程：启动调度器。"""
    async def _main() -> None:
        client = _new_client()
        try:
            await serve_forever(client)
        finally:
            await client.aclose()

    asyncio.run(_main())


# ----------------------------- doctor -----------------------------
@app.command("doctor")
def cmd_doctor() -> None:
    """检查 AList 连通性与百度/夸克 driver 是否就绪。"""
    async def _main() -> None:
        client = _new_client()
        try:
            try:
                await client._ensure_token()  # type: ignore
                console.print("[green]✓[/] AList 登录成功")
            except Exception as e:
                console.print(f"[red]✗[/] AList 登录失败: {e}")
                return

            for label, path in (("百度", settings.baidu_root), ("夸克", settings.quark_root)):
                try:
                    items = await client.list_dir(path)
                    console.print(
                        f"[green]✓[/] {label} 根目录 {path} 可访问，共 {len(items)} 项"
                    )
                except AListError as e:
                    console.print(f"[red]✗[/] {label} 根目录 {path} 失败: {e}")
        finally:
            await client.aclose()

    asyncio.run(_main())


# ----------------------------- job sub -----------------------------
@job_app.command("add")
def cmd_job_add(
    name: str,
    src: str = typer.Argument(..., help="源路径，如 /baidu/电影"),
    dst: str = typer.Argument(..., help="目的路径，如 /quark/归档/电影"),
    cron: Optional[str] = typer.Option(None, help="cron 表达式，省略则为一次性 job"),
    once: bool = typer.Option(False, help="强制一次性"),
    exists_policy: str = typer.Option(settings.exists_policy, help="skip|overwrite|rename"),
) -> None:
    mode = JobMode.CRON if cron and not once else JobMode.ONCE
    with session_scope() as s:
        job = MigrationJob(
            name=name,
            src_path=src,
            dst_path=dst,
            mode=mode,
            cron_expr=cron or "",
            exists_policy=exists_policy,
            enabled=True,
        )
        s.add(job)
        s.flush()
        console.print(f"[green]已创建 job {job.id}[/] mode={mode.value}")


@job_app.command("list")
def cmd_job_list() -> None:
    table = Table(title="Migration Jobs")
    for col in ["ID", "Name", "Mode", "Cron", "Src", "Dst", "Enabled", "Status", "Message", "Updated"]:
        table.add_column(col)
    with session_scope() as s:
        jobs = s.exec(select(MigrationJob).order_by(MigrationJob.id)).all()
        for j in jobs:
            table.add_row(
                str(j.id),
                j.name,
                j.mode.value,
                j.cron_expr or "-",
                j.src_path,
                j.dst_path,
                "Y" if j.enabled else "N",
                j.status.value,
                (j.last_message or "")[:60],
                j.updated_at.strftime("%m-%d %H:%M"),
            )
    console.print(table)


@job_app.command("rm")
def cmd_job_rm(job_id: int) -> None:
    with session_scope() as s:
        j = s.get(MigrationJob, job_id)
        if not j:
            console.print(f"[red]job {job_id} 不存在[/]")
            raise typer.Exit(1)
        # 同时删除其 file tasks
        tasks = s.exec(select(FileTask).where(FileTask.job_id == job_id)).all()
        for t in tasks:
            s.delete(t)
        s.delete(j)
    console.print(f"[green]已删除 job {job_id}[/]")


@job_app.command("enable")
def cmd_job_enable(job_id: int) -> None:
    _toggle_job(job_id, True)


@job_app.command("disable")
def cmd_job_disable(job_id: int) -> None:
    _toggle_job(job_id, False)


def _toggle_job(job_id: int, enabled: bool) -> None:
    with session_scope() as s:
        j = s.get(MigrationJob, job_id)
        if not j:
            console.print(f"[red]job {job_id} 不存在[/]")
            raise typer.Exit(1)
        j.enabled = enabled
        j.updated_at = datetime.utcnow()
        s.add(j)
    console.print(f"job {job_id} enabled={enabled}")


@job_app.command("run")
def cmd_job_run(job_id: int) -> None:
    """立即执行一次该 job（前台运行，Ctrl+C 中断）。"""
    async def _main() -> None:
        client = _new_client()
        try:
            await run_job(client, job_id)
        except (KeyboardInterrupt, asyncio.CancelledError):
            mark_job_interrupted(job_id)
            console.print(f"[yellow]job {job_id} 已中断，状态已更新为可恢复[/]")
            raise
        finally:
            await client.aclose()

    asyncio.run(_main())


# ----------------------------- task sub -----------------------------
@task_app.command("list")
def cmd_task_list(
    job: Optional[int] = typer.Option(None),
    status: Optional[str] = typer.Option(None, help="pending|copying|success|failed|skipped|oversize"),
    limit: int = typer.Option(50),
) -> None:
    table = Table(title="File Tasks")
    for col in ["ID", "Job", "Src", "Dst", "Size(MB)", "Status", "Retry", "Error"]:
        table.add_column(col)
    with session_scope() as s:
        q = select(FileTask).order_by(FileTask.id.desc())
        if job:
            q = q.where(FileTask.job_id == job)
        if status:
            q = q.where(FileTask.status == TaskStatus(status))
        q = q.limit(limit)
        for t in s.exec(q).all():
            table.add_row(
                str(t.id),
                str(t.job_id),
                t.src_full_path[-50:],
                t.dst_full_path[-50:],
                f"{t.size_bytes/1024/1024:.1f}",
                t.status.value,
                str(t.retry_count),
                (t.last_error or "")[:50],
            )
    console.print(table)


@task_app.command("retry")
def cmd_task_retry(
    job: Optional[int] = typer.Option(None),
    include_oversize: bool = typer.Option(False),
) -> None:
    """把 failed (和可选 oversize) 任务重置为 pending。"""
    with session_scope() as s:
        statuses = [TaskStatus.FAILED]
        if include_oversize:
            statuses.append(TaskStatus.OVERSIZE)
        q = select(FileTask).where(FileTask.status.in_(statuses))
        if job:
            q = q.where(FileTask.job_id == job)
        rows = s.exec(q).all()
        for t in rows:
            t.status = TaskStatus.PENDING
            t.retry_count = 0
            t.last_error = ""
            s.add(t)
        console.print(f"[green]已重置 {len(rows)} 个任务为 pending[/]")


@task_app.command("stats")
def cmd_task_stats(job: Optional[int] = typer.Option(None)) -> None:
    from sqlalchemy import func

    with session_scope() as s:
        q = select(FileTask.status, func.count(), func.coalesce(func.sum(FileTask.size_bytes), 0)).group_by(FileTask.status)
        if job:
            q = q.where(FileTask.job_id == job)
        table = Table(title=f"Task Stats (job={job or 'ALL'})")
        for c in ["Status", "Count", "Size(GB)"]:
            table.add_column(c)
        for status, cnt, sz in s.exec(q).all():
            table.add_row(status.value, str(cnt), f"{(sz or 0)/1024/1024/1024:.2f}")
        console.print(table)


@task_app.command("progress")
def cmd_task_progress(
    job: Optional[int] = typer.Option(None),
    limit: int = typer.Option(20, help="最多显示多少个 copying 任务"),
    interval: float = typer.Option(2.0, min=0.5, help="刷新间隔（秒）"),
) -> None:
    async def _main() -> None:
        client = _new_client()
        try:
            samples: dict[str, tuple[float, float]] = {}
            with Live(console=console, refresh_per_second=max(1, int(1 / interval) + 1)) as live:
                while True:
                    with session_scope() as s:
                        q = select(FileTask).where(FileTask.status == TaskStatus.COPYING).order_by(FileTask.id)
                        if job:
                            q = q.where(FileTask.job_id == job)
                        q = q.limit(limit)
                        rows = [
                            {
                                "id": t.id,
                                "job_id": t.job_id,
                                "alist_task_id": t.alist_task_id,
                                "last_error": t.last_error,
                                "size_bytes": t.size_bytes,
                                "src_full_path": t.src_full_path,
                                "dst_full_path": t.dst_full_path,
                            }
                            for t in s.exec(q).all()
                        ]

                    try:
                        task_map: dict[str, CopyTask] = {}
                        for t in await client.list_undone_copy():
                            task_map[t.id] = t
                        for t in await client.list_done_copy():
                            task_map[t.id] = t
                    except AListError as e:
                        live.update(Text(f"读取 AList 后台任务失败: {e}", style="red"))
                        await asyncio.sleep(interval)
                        continue

                    live.update(
                        _build_progress_renderable(rows, task_map, samples, job, interval)
                    )
                    await asyncio.sleep(interval)
        finally:
            await client.aclose()

    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    app()
