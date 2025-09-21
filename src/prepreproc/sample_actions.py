"""Example action callbacks that can be referenced from configuration."""
from __future__ import annotations

import logging
import subprocess
from typing import Any, Dict

from .actions import ActionContext
from .events import EventType

logger = logging.getLogger(__name__)


def log_event(context: ActionContext, options: Dict[str, Any]) -> None:
    """Log file-level events or scheduled scans depending on trigger type."""

    level_name = str(options.get("level", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)

    if context.event is not None:
        message = options.get("message", "Filesystem event detected")
        logger.log(level, "%s: %s", message, _describe_event(context.event))
        return

    message = options.get("schedule_message", "Scheduled directory scan")
    total_files = len(context.snapshot)
    total_bytes = sum(size for _, size in context.snapshot.values())
    changed_count = len(context.modified_events)
    logger.log(
        level,
        "%s: %s files tracked, %.2f MB total, %s changes since last run",
        message,
        total_files,
        total_bytes / (1024 * 1024) if total_bytes else 0.0,
        changed_count,
    )


def summarize_directory(context: ActionContext, options: Dict[str, Any]) -> None:
    """Summarize extensions within the tracked directory tree."""

    level_name = str(options.get("level", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)
    min_count = int(options.get("min_count", 1))

    counts: Dict[str, int] = {}
    for path in context.snapshot.keys():
        suffix = path.suffix.lower() or "<no-ext>"
        counts[suffix] = counts.get(suffix, 0) + 1

    filtered = {ext: count for ext, count in counts.items() if count >= min_count}
    if not filtered:
        filtered = counts

    sorted_items = sorted(filtered.items(), key=lambda item: item[0])
    summary = ", ".join(f"{ext}: {count}" for ext, count in sorted_items) or "<empty>"
    logger.log(level, "Directory summary for %s -> %s", context.root_path, summary)


def summarize_added_files(context: ActionContext, options: Dict[str, Any]) -> None:
    """Report how many new files were created in each directory since the last run."""

    level_name = str(options.get("level", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)

    events = context.modified_events
    if context.event is not None:
        events = [context.event]

    created = [event for event in events if event.event_type is EventType.CREATED]
    if not created:
        logger.log(level, "No new files detected for %s", context.root_path)
        return

    counts: Dict[str, int] = {}
    for event in created:
        directory = _relative_directory(event.path.parent, context.root_path)
        counts[directory] = counts.get(directory, 0) + 1

    total_new = len(created)
    directory_summary = ", ".join(
        f"{directory}: {counts[directory]}" for directory in sorted(counts)
    )
    logger.log(
        level,
        "New files (%s total) for %s -> %s",
        total_new,
        context.root_path,
        directory_summary,
    )


def run_shell_command(context: ActionContext, options: Dict[str, Any]) -> None:
    """Execute a templated shell command when an event-driven action fires."""

    if context.event is None:
        logger.debug("run_shell_command skipped: no event in context")
        return

    template = options.get("command")
    if not template:
        logger.error("run_shell_command requires a 'command' option")
        return

    event_path = context.event.path
    values = {
        "path": str(event_path),
        "directory": str(event_path.parent),
        "filename": event_path.name,
        "root": str(context.root_path),
    }
    if context.event.previous_path is not None:
        prev_path = context.event.previous_path
        values.update(
            previous_path=str(prev_path),
            previous_directory=str(prev_path.parent),
            previous_filename=prev_path.name,
        )

    try:
        command = str(template).format(**values)
    except KeyError as exc:
        logger.error("run_shell_command missing placeholder value for '%s'", exc)
        return

    logger.info("Executing shell command for %s: %s", event_path, command)
    try:
        subprocess.run(command, shell=True, check=True)
    except subprocess.CalledProcessError as exc:
        logger.error("Shell command failed (exit %s): %s", exc.returncode, command)



def _describe_event(event) -> str:
    details = [f"type={event.event_type}", f"path={event.path}"]
    if event.size is not None:
        details.append(f"size={event.size}")
    if event.mtime is not None:
        details.append(f"mtime={event.mtime}")
    if event.previous_path:
        details.append(f"previous={event.previous_path}")
    return ", ".join(details)


def _relative_directory(directory, root_path):
    try:
        relative = directory.relative_to(root_path)
        if str(relative) == ".":
            return "."
        return str(relative)
    except ValueError:
        return str(directory)
