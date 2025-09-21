"""Event models shared across monitor components."""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Optional


class EventType(str, Enum):
    """Types of filesystem changes emitted by the monitor."""

    CREATED = "created"
    MODIFIED = "modified"
    DELETED = "deleted"
    MOVED = "moved"


@dataclass(frozen=True)
class FileEvent:
    """A single change observed in the watched directory tree."""

    event_type: EventType
    path: Path
    previous_path: Optional[Path] = None
    size: Optional[int] = None
    mtime: Optional[float] = None
