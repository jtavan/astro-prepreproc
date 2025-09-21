"""Command-line entry point for the pre-processing monitor."""
from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .actions import ActionRegistry
from .config import ConfigError, load_config
from .monitor import DirectoryMonitor


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor astrophotography raw data folders")
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to the YAML configuration file (default: %(default)s)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    config_path = Path(args.config)
    try:
        app_config = load_config(config_path)
    except ConfigError as exc:
        logging.error("%s", exc)
        raise SystemExit(2) from exc

    registry = ActionRegistry(app_config.actions)
    monitor = DirectoryMonitor(app_config.monitor, registry)
    monitor.run()


if __name__ == "__main__":
    main()
