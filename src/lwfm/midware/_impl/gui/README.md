# lwfm GUI (midware/_impl/gui)

A lightweight Tkinter GUI to monitor and control running lwfm jobs.

Features:
- Scrollable, sortable, filterable grid of running (non-terminal) jobs
- Auto-refresh (default 5s), configurable
- Per-row actions: Cancel job, Show associated files (put/get)

Run locally:

```bash
python src/lwfm/midware/_impl/gui/run_gui.py
```

Environment:
- LWFM_GUI_REFRESH: refresh interval in seconds (default 5)

The GUI relies on the lwfm middleware being up (started via `lwfm.sh`).
