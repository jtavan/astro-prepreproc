"""Action callbacks that can be referenced from configuration - these are related to file cleanup functions."""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, Dict

from .actions import ActionContext

logger = logging.getLogger(__name__)


def list_old_files(context: ActionContext, options: Dict[str, Any]) -> None:
    """Report how many files exist in each directory that are older than the configured threshold_days days old."""

    level_name = str(options.get("level", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)

    threshold_option = options.get("threshold_days")
    if threshold_option is None:
        logger.error("list_old_files requires a 'threshold_days' option")
        return

    try:
        threshold_days = float(threshold_option)
    except (TypeError, ValueError):
        logger.error("list_old_files received non-numeric 'threshold_days': %r", threshold_option)
        return

    if threshold_days < 0:
        logger.error("list_old_files received negative 'threshold_days': %s", threshold_days)
        return

    threshold_display = int(threshold_days) if float(threshold_days).is_integer() else threshold_days
    cutoff_epoch = time.time() - (threshold_days * 86400)

    counts: Dict[str, int] = {}
    total_old = 0
    for path, (mtime, _size) in context.snapshot.items():
        if mtime > cutoff_epoch:
            continue
        directory_key = _relative_directory(path.parent, context.root_path)
        counts[directory_key] = counts.get(directory_key, 0) + 1
        total_old += 1

    if not counts:
        logger.log(
            level,
            "No files older than %s days for %s",
            threshold_display,
            context.root_path,
        )
        return

    summary_items = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    summary = ", ".join(f"{directory}: {count}" for directory, count in summary_items)

    logger.log(
        level,
        "Files older than %s days (%s total) for %s -> %s",
        threshold_display,
        total_old,
        context.root_path,
        summary,
    )


def _relative_directory(directory: Path, root_path: Path) -> str:
    try:
        relative = directory.relative_to(root_path)
    except ValueError:
        return str(directory)

    relative_str = str(relative)
    return relative_str if relative_str else "."
