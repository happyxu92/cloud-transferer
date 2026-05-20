from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # AList
    alist_base: str = "http://alist:5244"
    alist_username: str = "admin"
    alist_password: str = "changeme"

    # Paths
    baidu_root: str = "/baidu"
    quark_root: str = "/quark"

    # Strategy
    max_concurrency: int = 2
    max_file_gb: int = 20
    exists_policy: Literal["skip", "overwrite", "rename"] = "skip"
    max_retry: int = 3
    task_timeout_sec: int = 14400

    # Disk guard
    alist_temp_dir: str = "/alist-data/temp"
    disk_pause_percent: int = 80

    # Scheduler
    default_job_cron: str = ""
    default_job_src: str = ""
    default_job_dst: str = ""

    # Storage
    data_dir: str = "/data"
    log_level: str = "INFO"

    @property
    def db_path(self) -> Path:
        p = Path(self.data_dir)
        p.mkdir(parents=True, exist_ok=True)
        return p / "ct.db"

    @property
    def log_path(self) -> Path:
        p = Path(self.data_dir) / "logs"
        p.mkdir(parents=True, exist_ok=True)
        return p / "app.log"

    @property
    def failed_log_path(self) -> Path:
        return Path(self.data_dir) / "logs" / "failed_tasks.jsonl"

    @property
    def max_file_bytes(self) -> int:
        return self.max_file_gb * 1024 * 1024 * 1024


settings = Settings()
