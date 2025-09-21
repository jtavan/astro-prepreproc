"""Configuration loading utilities for the pre-processing monitor."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import time as dt_time
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml # type: ignore


logger = logging.getLogger(__name__)


class ConfigError(Exception):
    """Raised when the configuration file is missing or invalid."""


class TriggerType(str, Enum):
    """Available trigger frequencies for configured actions."""

    EVENT = "event"
    MINUTELY = "minutely"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


@dataclass
class MonitorConfig:
    """Options describing how the filesystem monitor should behave."""

    root_path: Path
    poll_interval: float = 2.0
    recursive: bool = True
    include_patterns: List[str] = field(default_factory=list)
    exclude_patterns: List[str] = field(default_factory=list)


@dataclass
class ActionTriggerConfig:
    """Configuration describing how often an action should be invoked."""

    type: TriggerType = TriggerType.EVENT
    time: Optional[dt_time] = None
    weekday: Optional[int] = None  # Monday=0, Sunday=6
    day: Optional[int] = None  # 1-31
    minute: Optional[int] = None  # 0-59 for hourly triggers


@dataclass
class ActionConfig:
    """Dynamic hook definition loaded from the configuration file."""

    name: str
    module: str
    function: str
    trigger: ActionTriggerConfig
    options: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AppConfig:
    """Top-level configuration structure."""

    monitor: MonitorConfig
    actions: List[ActionConfig] = field(default_factory=list)


def load_config(path: Path) -> AppConfig:
    """Load and validate the YAML configuration file."""

    if not path.exists():
        raise ConfigError(f"Configuration file not found: {path}")

    try:
        data = yaml.safe_load(path.read_text())
    except yaml.YAMLError as exc:  # pragma: no cover - logging helper
        raise ConfigError(f"Failed to parse YAML configuration: {exc}") from exc

    if not isinstance(data, dict):
        raise ConfigError("Configuration root must be a mapping")

    monitor_cfg = _parse_monitor_config(data.get("monitor"), config_path=path)
    actions_cfg = _parse_actions_config(data.get("actions", []), config_path=path)

    return AppConfig(monitor=monitor_cfg, actions=actions_cfg)


def _parse_monitor_config(raw: Any, *, config_path: Path) -> MonitorConfig:
    if not isinstance(raw, dict):
        raise ConfigError("'monitor' section must be a mapping")

    root_path_raw = raw.get("root_path")
    if not isinstance(root_path_raw, str):
        raise ConfigError("monitor.root_path must be a string")

    root_path = Path(root_path_raw)
    if not root_path.is_absolute():
        root_path = (config_path.parent / root_path).resolve()

    poll_interval = raw.get("poll_interval", 2.0)
    try:
        poll_interval_val = float(poll_interval)
    except (TypeError, ValueError) as exc:
        raise ConfigError("monitor.poll_interval must be numeric") from exc
    if poll_interval_val <= 0:
        raise ConfigError("monitor.poll_interval must be positive")

    recursive_flag = raw.get("recursive", True)
    if not isinstance(recursive_flag, bool):
        raise ConfigError("monitor.recursive must be a boolean")

    include_patterns = _ensure_str_list(raw.get("include_patterns", []), "monitor.include_patterns")
    exclude_patterns = _ensure_str_list(raw.get("exclude_patterns", []), "monitor.exclude_patterns")

    return MonitorConfig(
        root_path=root_path,
        poll_interval=poll_interval_val,
        recursive=recursive_flag,
        include_patterns=include_patterns,
        exclude_patterns=exclude_patterns,
    )


def _parse_actions_config(raw: Any, *, config_path: Path) -> List[ActionConfig]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ConfigError("'actions' section must be a list")

    actions: List[ActionConfig] = []
    for index, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ConfigError(f"actions[{index}] must be a mapping")

        name = item.get("name") or f"action_{index}"
        module = item.get("module")
        function = item.get("function")
        options = item.get("options", {})
        trigger = _parse_action_trigger(item.get("trigger"), action_index=index)

        if not isinstance(module, str) or not isinstance(function, str):
            raise ConfigError(f"actions[{index}] must include 'module' and 'function' strings")
        if options is None:
            options = {}
        if not isinstance(options, dict):
            raise ConfigError(f"actions[{index}].options must be a mapping if provided")

        action_cfg = ActionConfig(
            name=str(name),
            module=module,
            function=function,
            trigger=trigger,
            options=options,
        )
        logger.info(
            "Loaded action '%s' (%s.%s) trigger=%s",
            action_cfg.name,
            action_cfg.module,
            action_cfg.function,
            action_cfg.trigger.type.value,
        )
        actions.append(action_cfg)

    return actions


def _parse_action_trigger(raw: Any, *, action_index: int) -> ActionTriggerConfig:
    if raw is None:
        return ActionTriggerConfig(type=TriggerType.EVENT)
    if not isinstance(raw, dict):
        raise ConfigError(f"actions[{action_index}].trigger must be a mapping")

    trigger_type_raw = raw.get("type", TriggerType.EVENT.value)
    try:
        trigger_type = TriggerType(trigger_type_raw)
    except ValueError as exc:
        allowed = ", ".join(option.value for option in TriggerType)
        raise ConfigError(
            f"actions[{action_index}].trigger.type must be one of: {allowed}"
        ) from exc

    if trigger_type is TriggerType.EVENT:
        return ActionTriggerConfig(type=trigger_type)

    time_value = raw.get("time")
    time_obj: Optional[dt_time] = None
    if time_value is not None:
        time_obj = _parse_time_field(
            time_value,
            field_name=f"actions[{action_index}].trigger.time",
        )

    weekday_value: Optional[int] = None
    day_value: Optional[int] = None
    minute_value: Optional[int] = None

    if trigger_type in (TriggerType.DAILY, TriggerType.WEEKLY, TriggerType.MONTHLY):
        if time_obj is None:
            raise ConfigError(
                f"actions[{action_index}].trigger.time is required for {trigger_type.value} triggers"
            )

    if trigger_type is TriggerType.HOURLY:
        minute_raw = raw.get("minute")
        if minute_raw is not None:
            minute_value = _parse_minute_field(
                minute_raw,
                field_name=f"actions[{action_index}].trigger.minute",
            )
        elif time_obj is not None:
            minute_value = time_obj.minute
        else:
            minute_value = 0

        if time_obj is not None and minute_value is not None and time_obj.minute != minute_value:
            raise ConfigError(
                f"actions[{action_index}].trigger.minute must match the minute in trigger.time"
            )

    if trigger_type is TriggerType.WEEKLY:
        weekday_value = _parse_weekday_field(
            raw.get("weekday"),
            field_name=f"actions[{action_index}].trigger.weekday",
        )
    elif trigger_type is TriggerType.MONTHLY:
        day_value = _parse_month_day_field(
            raw.get("day"),
            field_name=f"actions[{action_index}].trigger.day",
        )

    return ActionTriggerConfig(
        type=trigger_type,
        time=time_obj,
        weekday=weekday_value,
        day=day_value,
        minute=minute_value,
    )


def _parse_time_field(value: Any, *, field_name: str) -> dt_time:
    if not isinstance(value, str):
        raise ConfigError(f"{field_name} must be a string in HH:MM or HH:MM:SS format")
    parts = value.split(":")
    if len(parts) not in (2, 3):
        raise ConfigError(f"{field_name} must be in HH:MM or HH:MM:SS format")
    try:
        hour = int(parts[0])
        minute = int(parts[1])
        second = int(parts[2]) if len(parts) == 3 else 0
    except ValueError as exc:
        raise ConfigError(f"{field_name} must contain numeric time components") from exc
    if not (0 <= hour <= 23 and 0 <= minute <= 59 and 0 <= second <= 59):
        raise ConfigError(f"{field_name} must specify a valid time of day")
    return dt_time(hour=hour, minute=minute, second=second)


def _parse_weekday_field(value: Any, *, field_name: str) -> int:
    if value is None:
        raise ConfigError(f"{field_name} is required for weekly triggers")
    weekday_lookup = {
        "monday": 0,
        "tuesday": 1,
        "wednesday": 2,
        "thursday": 3,
        "friday": 4,
        "saturday": 5,
        "sunday": 6,
    }
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized not in weekday_lookup:
            raise ConfigError(f"{field_name} must be a weekday name or integer 0-6")
        return weekday_lookup[normalized]
    if isinstance(value, int):
        if 0 <= value <= 6:
            return value
        raise ConfigError(f"{field_name} integer must be between 0 (Monday) and 6 (Sunday)")
    raise ConfigError(f"{field_name} must be a weekday name or integer 0-6")


def _parse_month_day_field(value: Any, *, field_name: str) -> int:
    if value is None:
        raise ConfigError(f"{field_name} is required for monthly triggers")
    if isinstance(value, int):
        if 1 <= value <= 31:
            return value
        raise ConfigError(f"{field_name} must be between 1 and 31")
    raise ConfigError(f"{field_name} must be an integer between 1 and 31")


def _parse_minute_field(value: Any, *, field_name: str) -> int:
    if isinstance(value, int):
        if 0 <= value <= 59:
            return value
        raise ConfigError(f"{field_name} must be between 0 and 59")
    raise ConfigError(f"{field_name} must be an integer between 0 and 59")


def _ensure_str_list(value: Any, field_name: str) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if not isinstance(value, list):
        raise ConfigError(f"{field_name} must be a list of strings")
    items: List[str] = []
    for elem in value:
        if not isinstance(elem, str):
            raise ConfigError(f"{field_name} must contain only strings")
        items.append(elem)
    return items
