"""Filesystem monitoring loop with a simple polling backend."""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Set, Tuple

from .actions import ActionRegistry
from .config import MonitorConfig
from .events import EventType, FileEvent

logger = logging.getLogger(__name__)

Snapshot = Dict[Path, Tuple[float, int]]


@dataclass
class MonitorStats:
    """Counters emitted by the monitor for observability."""

    cycles: int = 0
    events_emitted: int = 0


class DirectoryMonitor:
    """Polls a directory tree and emits events for file changes."""

    def __init__(self, config: MonitorConfig, actions: ActionRegistry):
        self._config = config
        self._actions = actions
        self._stop_event = threading.Event()
        self._snapshot: Snapshot = {}
        self._stats = MonitorStats()

    def run(self) -> None:
        """Run the monitoring loop until stopped."""

        logger.info("Starting monitor for %s", self._config.root_path)
        try:
            self._snapshot = self._scan()
            self._actions.prime_scheduled_snapshots(self._snapshot)
            while not self._stop_event.is_set():
                start_time = time.time()
                events, new_snapshot = self._detect_changes()
                for event in events:
                    self._actions.dispatch_event(
                        event,
                        root_path=self._config.root_path,
                        snapshot=new_snapshot,
                    )

                self._actions.dispatch_scheduled(
                    now=datetime.now(),
                    root_path=self._config.root_path,
                    snapshot=new_snapshot,
                )
                self._snapshot = new_snapshot
                self._stats.cycles += 1
                self._stats.events_emitted += len(events)
                self._sleep_until_next_cycle(start_time)
        except KeyboardInterrupt:
            logger.info("Monitor interrupted by user")
        finally:
            logger.info(
                "Monitor stopped after %s cycles, %s events",
                self._stats.cycles,
                self._stats.events_emitted,
            )

    def stop(self) -> None:
        """Signal the monitor to stop at the next opportunity."""

        self._stop_event.set()

    def _sleep_until_next_cycle(self, started_at: float) -> None:
        elapsed = time.time() - started_at
        remaining = max(self._config.poll_interval - elapsed, 0.0)
        if remaining > 0:
            time.sleep(remaining)

    def _detect_changes(self) -> Tuple[List[FileEvent], Snapshot]:
        new_snapshot = self._scan()
        events = list(_diff_snapshots(self._snapshot, new_snapshot))
        return events, new_snapshot

    def _scan(self) -> Snapshot:
        root = self._config.root_path
        if not root.exists():
            logger.warning("Root path %s does not exist yet; skipping scan", root)
            return {}
        results: Snapshot = {}
        paths = _iter_paths(root, recursive=self._config.recursive)
        for path in paths:
            if not path.is_file():
                continue
            if not _matches_patterns(path, self._config.include_patterns, self._config.exclude_patterns):
                continue
            try:
                stat = path.stat()
            except FileNotFoundError:
                continue
            results[path] = (stat.st_mtime, stat.st_size)
        return results


def _iter_paths(root: Path, *, recursive: bool) -> Iterable[Path]:
    if recursive:
        yield from root.rglob("*")
    else:
        yield from root.glob("*")


def _matches_patterns(path: Path, include_patterns: List[str], exclude_patterns: List[str]) -> bool:
    from fnmatch import fnmatch

    relative = path.name
    rel_from_root = str(path)

    if exclude_patterns and any(fnmatch(relative, pat) or fnmatch(rel_from_root, pat) for pat in exclude_patterns):
        return False

    if not include_patterns:
        return True

    return any(fnmatch(relative, pat) or fnmatch(rel_from_root, pat) for pat in include_patterns)


def _diff_snapshots(old: Snapshot, new: Snapshot) -> Iterable[FileEvent]:
    seen: Set[Path] = set()

    for path, (mtime, size) in new.items():
        if path not in old:
            yield FileEvent(event_type=EventType.CREATED, path=path, size=size, mtime=mtime)
        else:
            old_mtime, old_size = old[path]
            if old_mtime != mtime or old_size != size:
                yield FileEvent(event_type=EventType.MODIFIED, path=path, size=size, mtime=mtime)
        seen.add(path)

    for path, (mtime, size) in old.items():
        if path not in seen:
            yield FileEvent(event_type=EventType.DELETED, path=path, size=size, mtime=mtime)
