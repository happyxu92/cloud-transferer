"""APScheduler 调度器：周期性运行启用了 cron 的 job。"""
from __future__ import annotations

import asyncio

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from loguru import logger
from sqlmodel import select

from .alist_client import AListClient
from .config import settings
from .db import session_scope
from .migrator import run_job
from .models import JobMode, MigrationJob


class CronManager:
    def __init__(self, client: AListClient) -> None:
        self.client = client
        self.scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")
        self._running: set[int] = set()

    def start(self) -> None:
        self.reload()
        self.scheduler.start()
        logger.info("调度器已启动")

    def reload(self) -> None:
        """根据 DB 中的 cron job 重建任务表。"""
        # 清空现有
        for j in list(self.scheduler.get_jobs()):
            j.remove()
        with session_scope() as s:
            jobs = s.exec(
                select(MigrationJob).where(
                    MigrationJob.mode == JobMode.CRON,
                    MigrationJob.enabled == True,  # noqa: E712
                )
            ).all()
        for job in jobs:
            if not job.cron_expr:
                continue
            try:
                trigger = CronTrigger.from_crontab(
                    job.cron_expr, timezone="Asia/Shanghai"
                )
            except Exception as e:
                logger.error(f"job {job.id} cron 表达式无效: {e}")
                continue
            self.scheduler.add_job(
                self._wrapped_run,
                trigger=trigger,
                id=f"job-{job.id}",
                args=[job.id],
                replace_existing=True,
                misfire_grace_time=3600,
                coalesce=True,
                max_instances=1,
            )
            logger.info(f"已注册 cron job {job.id} ({job.name}) -> {job.cron_expr}")

    async def _wrapped_run(self, job_id: int) -> None:
        if job_id in self._running:
            logger.warning(f"job {job_id} 上次执行未结束，跳过本次")
            return
        self._running.add(job_id)
        try:
            await run_job(self.client, job_id)
        except Exception as e:
            logger.exception(f"job {job_id} 执行异常: {e}")
        finally:
            self._running.discard(job_id)

    async def shutdown(self) -> None:
        self.scheduler.shutdown(wait=False)


async def serve_forever(client: AListClient) -> None:
    mgr = CronManager(client)
    mgr.start()
    # 默认 job（来自 .env）：若 DB 中尚未有同名 job 则插入
    _ensure_default_job()
    mgr.reload()
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        await mgr.shutdown()


def _ensure_default_job() -> None:
    if not settings.default_job_cron or not settings.default_job_src or not settings.default_job_dst:
        return
    name = "default"
    with session_scope() as s:
        existing = s.exec(
            select(MigrationJob).where(MigrationJob.name == name)
        ).first()
        if existing:
            return
        s.add(
            MigrationJob(
                name=name,
                src_path=settings.default_job_src,
                dst_path=settings.default_job_dst,
                mode=JobMode.CRON,
                cron_expr=settings.default_job_cron,
                exists_policy=settings.exists_policy,
                enabled=True,
            )
        )
        logger.info(f"已写入默认 job: {settings.default_job_src} -> {settings.default_job_dst}")
