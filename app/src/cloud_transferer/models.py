from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Optional

from sqlmodel import Field, SQLModel


class JobMode(str, Enum):
    ONCE = "once"
    CRON = "cron"


class JobStatus(str, Enum):
    IDLE = "idle"
    SCANNING = "scanning"
    RUNNING = "running"
    PAUSED = "paused"
    DONE = "done"
    ERROR = "error"


class TaskStatus(str, Enum):
    PENDING = "pending"
    COPYING = "copying"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    OVERSIZE = "oversize"


class MigrationJob(SQLModel, table=True):
    __tablename__ = "migration_job"

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    src_path: str                       # e.g. /baidu/电影
    dst_path: str                       # e.g. /quark/归档/电影
    mode: JobMode = JobMode.ONCE
    cron_expr: str = ""
    exists_policy: str = "skip"         # skip | overwrite | rename
    enabled: bool = True
    status: JobStatus = JobStatus.IDLE
    last_message: str = ""
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class FileTask(SQLModel, table=True):
    __tablename__ = "file_task"

    id: Optional[int] = Field(default=None, primary_key=True)
    job_id: int = Field(index=True, foreign_key="migration_job.id")
    src_full_path: str = Field(index=True)
    dst_full_path: str
    size_bytes: int = 0
    alist_task_id: str = ""
    status: TaskStatus = Field(default=TaskStatus.PENDING, index=True)
    retry_count: int = 0
    last_error: str = ""
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
