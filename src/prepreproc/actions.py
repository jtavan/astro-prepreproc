"""Dynamic action loading and dispatch helpers."""
from __future__ import annotations

import calendar
import importlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, cast

from .config import ActionConfig, ActionTriggerConfig, TriggerType
from .events import EventType, FileEvent

logger = logging.getLogger(__name__)

Snapshot = Dict[Path, Tuple[float, int]]


ActionCallback = Callable[["ActionContext", Dict[str, Any]], None]

@dataclass(frozen=True)
class ActionContext:
    """Context passed to action callbacks."""

    root_path: Path
    snapshot: Snapshot
    event: Optional[FileEvent] = None
    modified_events: List[FileEvent] = field(default_factory=list)


@dataclass
class Action:
    """Callable wrapper associated with configuration metadata."""

    name: str
    callback: ActionCallback
    options: Dict[str, Any]
    trigger: ActionTriggerConfig
    next_run_at: Optional[datetime] = None
    previous_snapshot: Optional[Snapshot] = None

    def is_event_trigger(self) -> bool:
        return self.trigger.type is TriggerType.EVENT

    def schedule_next_run(self, reference: datetime) -> None:
        self.next_run_at = _compute_next_run(self.trigger, reference)

    def invoke(self, context: ActionContext) -> None:
        logger.debug("Dispatching action %s (trigger=%s)", self.name, self.trigger.type.value)
        self.callback(context, self.options)


class ActionRegistry:
    """Loads and stores configured actions."""

    def __init__(self, actions: Iterable[ActionConfig], *, reference_time: Optional[datetime] = None):
        self._actions: List[Action] = [self._load_action(cfg) for cfg in actions]
        self._event_actions: List[Action] = [action for action in self._actions if action.is_event_trigger()]
        self._scheduled_actions: List[Action] = [action for action in self._actions if not action.is_event_trigger()]

        now = reference_time or datetime.now()
        for action in self._scheduled_actions:
            action.schedule_next_run(now)

    def __iter__(self):
        return iter(self._actions)

    def dispatch_event(self, event: FileEvent, *, root_path: Path, snapshot: Snapshot) -> None:
        if not self._event_actions:
            return
        context = ActionContext(root_path=root_path, snapshot=snapshot, event=event)
        for action in self._event_actions:
            self._safe_invoke(action, context, event_hint=event)

    def dispatch_scheduled(self, *, now: datetime, root_path: Path, snapshot: Snapshot) -> None:
        if not self._scheduled_actions:
            return
        for action in self._scheduled_actions:
            if action.next_run_at is None:
                action.schedule_next_run(now)
            if action.next_run_at is None or now < action.next_run_at:
                continue
            modified_events: List[FileEvent] = []
            if action.previous_snapshot is not None:
                modified_events = _diff_snapshots(action.previous_snapshot, snapshot)
            context = ActionContext(
                root_path=root_path,
                snapshot=snapshot,
                modified_events=modified_events,
            )
            self._safe_invoke(action, context, event_hint=None)
            action.previous_snapshot = dict(snapshot)
            # Schedule the next run one second after the current reference to avoid repeated triggering
            action.schedule_next_run(now + timedelta(seconds=1))

    def prime_scheduled_snapshots(self, snapshot: Snapshot) -> None:
        """Seed scheduled actions with an initial snapshot baseline."""

        for action in self._scheduled_actions:
            if action.previous_snapshot is None:
                action.previous_snapshot = dict(snapshot)

    def _load_action(self, config: ActionConfig) -> Action:
        module = _import_module(config.module)
        try:
            callback = getattr(module, config.function)
        except AttributeError as exc:
            raise RuntimeError(
                f"Action '{config.name}' could not find function '{config.function}' in {config.module}"
            ) from exc

        if not callable(callback):
            raise RuntimeError(
                f"Action '{config.name}' attribute '{config.function}' in {config.module} is not callable"
            )

        options = dict(config.options or {})
        callback_fn = cast(ActionCallback, callback)
        return Action(name=config.name, callback=callback_fn, options=options, trigger=config.trigger)

    def _safe_invoke(self, action: Action, context: ActionContext, *, event_hint: Optional[FileEvent]) -> None:
        try:
            action.invoke(context)
        except Exception:  # pragma: no cover - protective logging
            if event_hint is None:
                logger.exception("Scheduled action %s failed", action.name)
            else:
                logger.exception("Action %s failed for event %s", action.name, event_hint)


def _diff_snapshots(old: Snapshot, new: Snapshot) -> List[FileEvent]:
    events: List[FileEvent] = []
    seen: Set[Path] = set()

    for path, (mtime, size) in new.items():
        old_values = old.get(path)
        if old_values is None:
            events.append(FileEvent(event_type=EventType.CREATED, path=path, size=size, mtime=mtime))
        else:
            old_mtime, old_size = old_values
            if old_mtime != mtime or old_size != size:
                events.append(FileEvent(event_type=EventType.MODIFIED, path=path, size=size, mtime=mtime))
        seen.add(path)

    for path, (mtime, size) in old.items():
        if path not in seen:
            events.append(FileEvent(event_type=EventType.DELETED, path=path, size=size, mtime=mtime))

    return events


def _import_module(module_path: str) -> ModuleType:
    try:
        return importlib.import_module(module_path)
    except ImportError as exc:  # pragma: no cover - defensive logging
        raise RuntimeError(f"Unable to import action module '{module_path}'") from exc


def _compute_next_run(trigger: ActionTriggerConfig, reference: datetime) -> Optional[datetime]:
    if trigger.type is TriggerType.EVENT:
        return None

    if trigger.type is TriggerType.MINUTELY:
        second = trigger.time.second if trigger.time is not None else 0
        base = reference.replace(second=second, microsecond=0)
        if base <= reference:
            base += timedelta(minutes=1)
        return base
    
    if trigger.type is TriggerType.HOURLY:
        minute = trigger.minute if trigger.minute is not None else 0
        second = trigger.time.second if trigger.time is not None else 0
        base = reference.replace(minute=minute, second=second, microsecond=0)
        if base <= reference:
            base += timedelta(hours=1)
        return base

    time_of_day = trigger.time
    if time_of_day is None:
        time_of_day = _DEFAULT_TIME

    base = reference.replace(
        hour=time_of_day.hour,
        minute=time_of_day.minute,
        second=time_of_day.second,
        microsecond=0,
    )

    if trigger.type is TriggerType.DAILY:
        if base <= reference:
            base += timedelta(days=1)
        return base

    if trigger.type is TriggerType.WEEKLY:
        weekday = trigger.weekday if trigger.weekday is not None else reference.weekday()
        days_ahead = (weekday - base.weekday()) % 7
        candidate = base + timedelta(days=days_ahead)
        if candidate <= reference:
            candidate += timedelta(days=7)
        return candidate

    if trigger.type is TriggerType.MONTHLY:
        day = trigger.day if trigger.day is not None else reference.day
        year = base.year
        month = base.month
        candidate = _clamped_month_datetime(year, month, day, time_of_day)
        if candidate <= reference:
            month += 1
            if month > 12:
                month = 1
                year += 1
            candidate = _clamped_month_datetime(year, month, day, time_of_day)
        return candidate

    logger.warning("Unknown trigger type %s; skipping schedule", trigger.type)
    return None


_DEFAULT_TIME = datetime.min.time().replace(hour=0, minute=0, second=0, microsecond=0)


def _clamped_month_datetime(year: int, month: int, day: int, time_of_day) -> datetime:
    last_day = calendar.monthrange(year, month)[1]
    target_day = min(day, last_day)
    return datetime(
        year,
        month,
        target_day,
        time_of_day.hour,
        time_of_day.minute,
        time_of_day.second,
        0,
    )
