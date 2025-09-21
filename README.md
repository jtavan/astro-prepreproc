# Pre-Pre-Processing Monitor

A lightweight polling monitor for astrophotography raw data folders. The service reads a YAML configuration file, watches a directory tree, and triggers Python callbacks when new data arrives or on scheduled cadences.

## Getting Started

1. Create and activate a Python 3.7+ virtual environment.
2. Install dependencies from the requirements file:
   ```bash
   pip install -r requirements.txt
   ```
3. Copy `config.example.yaml` to `config.yaml` and adjust paths, triggers, and action modules.
4. Run the monitor:
   ```bash
   python -m prepreproc --config config.yaml --log-level INFO
   ```

## Configuration Overview

```yaml
monitor:
  root_path: ./data/incoming  # Directory to watch; relative paths resolved from the config file
  poll_interval: 2.0          # Seconds between scans
  recursive: true             # Recurse into subdirectories
  include_patterns: ["*.fits", "*.cr3"]
  exclude_patterns: ["*.tmp"]
actions:
  - name: log-new-data
    module: prepreproc.sample_actions
    function: log_event
    trigger:
      type: event              # Run on every file-level change event
    options:
      level: INFO
      message: "New astrophotography data"
  - name: nightly-summary
    module: prepreproc.sample_actions
    function: summarize_directory
    trigger:
      type: daily              # Run once per day at the specified HH:MM[:SS]
      time: "02:30"
    options:
      level: INFO
      min_count: 1
```

Available trigger frequencies:
- `event`: Execute on every detected file event.
- `daily`: Execute once per day at `time`.
- `weekly`: Execute once per week at `time` on the provided `weekday` (0-6 or weekday name).
- `monthly`: Execute once per month at `time` on the provided calendar `day` (clamped to the last day when shorter months occur).

Each configured action points to a Python callable with the signature `callback(context: ActionContext, options: dict)`. The `ActionContext` exposes:
- `context.root_path`: the root directory being monitored.
- `context.snapshot`: the latest file snapshot `{Path: (mtime, size)}`.
- `context.event`: the triggering `FileEvent`, or `None` for scheduled runs.

See `src/prepreproc/sample_actions.py` for examples of per-event logging and scheduled directory summaries. Extend the pipeline by adding new modules and referencing them from the configuration.

## Next Steps

- Add persistence or queueing to hand off events to downstream processing pipelines.
- Implement additional scheduled actions (e.g., nightly calibration jobs, weekly data roll-ups).
- Replace the polling backend with `watchdog` or another filesystem notification library if lower latency is required.
