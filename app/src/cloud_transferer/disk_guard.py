"""磁盘占用哨兵：当 AList 临时目录占用过高时暂停下发新任务。"""
from __future__ import annotations

import shutil
from pathlib import Path

from loguru import logger

from .config import settings


def get_usage_percent(path: str | None = None) -> float:
    p = Path(path or settings.alist_temp_dir)
    # 若 temp 目录还没创建，退化为检查上级目录
    target = p if p.exists() else p.parent
    if not target.exists():
        return 0.0
    total, used, free = shutil.disk_usage(target)
    return used / total * 100.0


def should_pause() -> bool:
    pct = get_usage_percent()
    if pct >= settings.disk_pause_percent:
        logger.warning(
            f"磁盘占用 {pct:.1f}% >= 阈值 {settings.disk_pause_percent}%，暂停新任务"
        )
        return True
    return False
