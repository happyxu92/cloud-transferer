from __future__ import annotations

import sys

from loguru import logger

from .config import settings


def setup_logging() -> None:
    logger.remove()
    logger.add(
        sys.stderr,
        level=settings.log_level,
        format=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> "
            "<level>{level: <7}</level> "
            "<cyan>{name}:{line}</cyan> | <level>{message}</level>"
        ),
    )
    logger.add(
        settings.log_path,
        level=settings.log_level,
        rotation="10 MB",
        retention=5,
        enqueue=True,
        encoding="utf-8",
        format="{time:YYYY-MM-DD HH:mm:ss} {level: <7} {name}:{line} | {message}",
    )
