"""
Tkinter-based GUI for lwfm job monitoring and control.
This lean app module focuses on main window wiring and delegates dialogs
(metasheets, workflow, etc.) to submodules in this package.
"""
from __future__ import annotations

import json
import os
import threading
import traceback
from datetime import datetime, timedelta
import signal
from typing import Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import messagebox, ttk
from urllib import request, error

from lwfm.base.JobStatus import JobStatus
from lwfm.base.Metasheet import Metasheet  # type: ignore
from lwfm.base.WorkflowEvent import WorkflowEvent, MetadataEvent, JobEvent, NotificationEvent
from lwfm.midware.LwfManager import lwfManager
from lwfm.midware._impl.SiteConfig import SiteConfig
from lwfm.midware._impl.gui import open_metasheets_dialog, open_workflow_dialog

RefreshIntervalSecDefault = 5


class LwfmGui(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("lwfm GUI")
        self.geometry("1240x720")

        # Top toolbar
        toolbar = ttk.Frame(self)
        toolbar.pack(side=tk.TOP, fill=tk.X)
        ttk.Label(toolbar, text="Interval (s):").pack(side=tk.LEFT, padx=(4, 4))
        self.interval_entry = ttk.Entry(toolbar, width=6)
        self.interval_entry.insert(0, str(RefreshIntervalSecDefault))
        self.interval_entry.pack(side=tk.LEFT, padx=(0, 16))
        ttk.Label(toolbar, text="Filter:").pack(side=tk.LEFT, padx=(8, 4))
        self.filter_entry = ttk.Entry(toolbar, width=30)
        self.filter_entry.pack(side=tk.LEFT, padx=(0, 8))
        self.filter_entry.bind("<KeyRelease>", self._on_filter_change)
        # Time-range pick list (UI only for now). Default to 'today'.
        self.time_range_var = tk.StringVar(value='today')
        self.time_combo = ttk.Combobox(
            toolbar,
            width=10,
            state='readonly',
            values=['today', 'week', 'month', 'all'],
            textvariable=self.time_range_var,
        )
        self.time_combo.pack(side=tk.LEFT, padx=(0, 8))
        # Wire selection to refresh filter
        self.time_combo.bind('<<ComboboxSelected>>', lambda _e: self._apply_filter_and_sort())
        ttk.Button(toolbar, text="Metasheets", command=self.view_metasheets).pack(side=tk.LEFT)
        ttk.Button(toolbar, text="Events", command=self.view_events).pack(side=tk.LEFT)

        # Jobs table with scrollbar in a frame
        table_frame = ttk.Frame(self)
        table_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=8)
        
        cols = ("job_id", "name", "workflow", "status", "native", "site", "last_update", "files")
        self.tree = ttk.Treeview(table_frame, columns=cols, show="headings", selectmode="browse")
        headings = {
            "job_id": "Job ID",
            "name": "Name",
            "workflow": "Workflow",
            "status": "Status",
            "native": "Native",
            "site": "Site",
            "last_update": "Last Update (UTC)",
            "files": "Files"
        }
        widths = {
            "job_id": 180,
            "name": 160,
            "workflow": 140,
            "status": 100,
            "native": 120,
            "site": 160,
            "last_update": 180,
            "files": 80,
        }
        for c in cols:
            self.tree.heading(c, text=headings[c], command=lambda col=c: self._sort_by_column(col))
            anchor = tk.CENTER if c == "files" else tk.W
            self.tree.column(c, width=widths[c], anchor=anchor)
        
        ysb = ttk.Scrollbar(table_frame, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=ysb.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ysb.pack(side=tk.RIGHT, fill=tk.Y)

        # Color tags
        try:
            self.tree.tag_configure("status-bad", foreground="#d32f2f")
            self.tree.tag_configure("status-good", foreground="#2e7d32")
        except Exception:
            pass

        # Bind clicks (workflow opens dialog; files opens files window)
        self.tree.bind("<Button-1>", self.on_tree_click)
        self.tree.bind("<Motion>", self.on_tree_motion)

        # Status bar with connection indicator
        status_frame = ttk.Frame(self)
        status_frame.pack(side=tk.BOTTOM, fill=tk.X, padx=8, pady=(0, 8))
        self.status_var = tk.StringVar(value="Ready")
        self.connection_var = tk.StringVar(value="●")
        ttk.Label(status_frame, textvariable=self.status_var, anchor=tk.W).pack(side=tk.LEFT, fill=tk.X, expand=True)
        # Use tk.Label so foreground color changes reliably reflect (ttk.Label can ignore foreground on some themes)
        # Start as red (assume disconnected) until a successful refresh proves connectivity
        self.connection_label = tk.Label(status_frame, textvariable=self.connection_var, fg="red")
        self.connection_label.pack(side=tk.RIGHT)

        self.refresh_interval = RefreshIntervalSecDefault
        self._timer_id: Optional[str] = None
        self._loading = False
        # Assume disconnected until we confirm connection
        self._connection_ok = False
        self._sort_column = "last_update"
        self._sort_reverse = True  # Newest first by default
        self._filter_text = ""
        self._all_rows = []  # Store unfiltered data
        self._last_status_counts: Optional[Tuple[int, int, bool]] = None  # (filtered, total, has_filter)
        
        # Track open detail windows for replace-in-place behavior
        self._current_status_window: Optional[tk.Toplevel] = None
        self._current_files_window: Optional[tk.Toplevel] = None
        
        # Keyboard shortcuts
        self.bind_all("<F5>", lambda e: self.refresh())
        self.bind_all("<Control-r>", lambda e: self.refresh())
        self.bind_all("<Control-m>", lambda e: self.view_metasheets())
        self.bind_all("<Control-e>", lambda e: self.view_events())
        self.bind_all("<Control-l>", lambda e: self.view_server_log())
        self.bind_all("<Control-f>", lambda e: self.filter_entry.focus_set())
        self.bind_all("<Control-q>", lambda e: self.quit())
        
        self.refresh()
        self._tick()
        self._start_health_checks()

    def _create_progress_dialog(self, title: str, message: str) -> tuple:
        """Create a centered progress dialog with indeterminate progress bar."""
        progress_win = tk.Toplevel(self)
        progress_win.title(title)
        progress_win.geometry("350x120")
        progress_win.transient(self)
        progress_win.grab_set()
        progress_win.resizable(False, False)
        
        # Center the progress window
        progress_win.update_idletasks()
        x = (progress_win.winfo_screenwidth() // 2) - (350 // 2)
        y = (progress_win.winfo_screenheight() // 2) - (120 // 2)
        progress_win.geometry(f"350x120+{x}+{y}")
        
        ttk.Label(progress_win, text=message).pack(pady=(20, 10))
        progress_bar = ttk.Progressbar(progress_win, mode='indeterminate')
        progress_bar.pack(pady=10, padx=20, fill=tk.X)
        progress_bar.start()
        
        return progress_win, progress_bar

    def _update_connection_status(self, connected: bool):
        """Update the connection status indicator."""
        self._connection_ok = connected
        if connected:
            self.connection_var.set("●")
            self.connection_label.configure(fg="green")
        else:
            self.connection_var.set("●")
            self.connection_label.configure(fg="red")

    # --- Health check ---
    def _health_check_once(self) -> bool:
        """Ping the server /isRunning; return True if HTTP 200."""
        try:
            props = SiteConfig.getSiteProperties("lwfm") or {}
            host = props.get("host", "127.0.0.1")
            port = str(props.get("port", "3000"))
            url = f"http://{host}:{port}/isRunning"
            req = request.Request(url, method="GET")
            with request.urlopen(req, timeout=2.5) as resp:
                return 200 <= resp.getcode() < 300
        except Exception:
            return False

    def _start_health_checks(self):
        """Begin periodic health checks in the UI loop."""
        self._health_timer_id: Optional[str] = None

        def tick():
            ok = self._health_check_once()
            self._update_connection_status(ok)
            try:
                n = int(self.interval_entry.get())
                if n <= 0:
                    n = RefreshIntervalSecDefault
            except Exception:
                n = self.refresh_interval or RefreshIntervalSecDefault
            self._health_timer_id = self.after(n * 1000, tick)

        # initial schedule
        self._health_timer_id = self.after(500, tick)

    def _sort_by_column(self, column: str):
        """Sort the table by the specified column."""
        if self._sort_column == column:
            # Toggle sort direction if clicking same column
            self._sort_reverse = not self._sort_reverse
        else:
            # New column, default to ascending (except for last_update which defaults to descending)
            self._sort_column = column
            self._sort_reverse = (column == "last_update")
        
        # Update column headers to show sort direction
        cols = ("job_id", "name", "workflow", "status", "native", "site", "last_update", "files")
        headings = {
            "job_id": "Job ID",
            "name": "Name",
            "workflow": "Workflow", 
            "status": "Status",
            "native": "Native",
            "site": "Site",
            "last_update": "Last Update (UTC)",
            "files": "Files",
        }
        
        for c in cols:
            if c == column:
                arrow = " ▼" if self._sort_reverse else " ▲"
                text = headings[c] + arrow
            else:
                text = headings[c]
            self.tree.heading(c, text=text)
        
        self._apply_filter_and_sort()


    def _on_filter_change(self, event):
        """Handle filter text changes."""
        self._filter_text = self.filter_entry.get().strip().lower()
        self._apply_filter_and_sort()

    def _clear_filter(self):
        """Clear the filter text and refresh display."""
        self.filter_entry.delete(0, tk.END)
        self._filter_text = ""
        self._apply_filter_and_sort()

    def _apply_filter_and_sort(self):
        """Apply current filter and sort to the stored data."""
        if not self._all_rows:
            return
        
        # Apply filter
        filtered_rows = []
        for row in self._all_rows:
            if self._matches_filter(row):
                filtered_rows.append(row)
        
        # Apply sorting
        if filtered_rows:
            col_index = {"job_id": 0, "name": 1, "workflow": 2, "status": 3, "native": 4, "site": 5, "last_update": 6}[self._sort_column]
            
            def sort_key(row):
                val = row[col_index] if col_index < len(row) else ""
                if self._sort_column == "last_update" and val:
                    return val  # Already in YYYY-MM-DD HH:MM:SS format
                return val.lower() if isinstance(val, str) else str(val)
            
            filtered_rows.sort(key=sort_key, reverse=self._sort_reverse)
        
        # Update display
        self.tree.delete(*self.tree.get_children())
        for row in filtered_rows:
            # Handle variable length rows (some may have missing fields)
            if len(row) >= 7:
                jid, name, wid, stat, nat, site, ts = row[:7]
            elif len(row) == 6:
                jid, name, wid, stat, nat, site = row
                ts = ""
            else:
                # Skip malformed rows
                continue
            tag = ()
            up = (stat or "").upper()
            if up in (JobStatus.FAILED, JobStatus.CANCELLED):
                tag = ("status-bad",)
            elif up == JobStatus.COMPLETE:
                tag = ("status-good",)
            self.tree.insert("", tk.END, values=(jid, name, wid, stat, nat, site, ts, "[ Files ]"), tags=tag)
        
        # Update status text only when counts/filter state change to avoid flicker
        total_count = len(self._all_rows)
        filtered_count = len(filtered_rows)
        has_filter = bool(self._filter_text)
        sig = (filtered_count, total_count, has_filter)
        if sig != (self._last_status_counts or (-1, -1, None)):
            if has_filter:
                self.status_var.set(f"Jobs: {filtered_count}/{total_count} (filtered)")
            else:
                self.status_var.set(f"Jobs: {filtered_count}")
            self._last_status_counts = sig

    def _matches_filter(self, row) -> bool:
        """Check if a row matches the current filter text."""
        if not self._filter_text:
            # still apply time-range check even if no text filter
            return self._in_time_range(row[5])
        
        # Search across key columns
        search_text = " ".join([
            row[0] or "",  # job_id
            row[1] or "",  # name
            row[2] or "",  # workflow
            row[3] or "",  # status
            row[4] or "",  # native
            row[5] or "",  # site
            row[6] or "",  # last_update
        ]).lower()
        
        if self._filter_text in search_text:
            return self._in_time_range(row[5])
        return False

    def _in_time_range(self, ts: str) -> bool:
        """Check if timestamp string (YYYY-MM-DD HH:MM:SS) falls within selected time range.

        'today' = last 24 hours, 'week' = last 7 days, 'month' = last 30 days, 'all' = no filter.
        Timestamps are assumed to be UTC strings.
        """
        choice = (self.time_range_var.get() or 'all').lower()
        if choice == 'all' or not ts:
            return True
        try:
            dt = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
            now = datetime.utcnow()
            if choice == 'today':
                return dt >= now - timedelta(hours=24)
            if choice == 'week':
                return dt >= now - timedelta(days=7)
            if choice == 'month':
                return dt >= now - timedelta(days=30)
        except Exception:
            return True
        return True

    # --- Main table data ---
    def _tick(self):
        """Periodic refresh based on the Interval entry; reschedules itself."""
        try:
            try:
                n = int(self.interval_entry.get())
                if n <= 0:
                    n = RefreshIntervalSecDefault
            except Exception:
                n = self.refresh_interval or RefreshIntervalSecDefault
            self.refresh()
            self._timer_id = self.after(n * 1000, self._tick)
        except Exception:
            # Best effort; try again later with default
            self._timer_id = self.after(RefreshIntervalSecDefault * 1000, self._tick)

    def _get_best_status_for_display(self, current: JobStatus, new: JobStatus) -> JobStatus:
        """
        Determine which JobStatus to display in the main jobs table.
        Prioritizes terminal states over non-terminal ones, even if non-terminal is newer.
        Treats INFO-only jobs as successful terminal state.
        """
        cur_status = (current.getStatus() or "").upper()
        new_status = (new.getStatus() or "").upper()
        
        # Define terminal states
        terminal_states = {JobStatus.COMPLETE, JobStatus.FAILED, JobStatus.CANCELLED}
        
        # If current is terminal and new is not, keep current
        if cur_status in terminal_states and new_status not in terminal_states:
            return current
        
        # If new is terminal and current is not, use new
        if new_status in terminal_states and cur_status not in terminal_states:
            return new
        
        # If both are terminal or both are non-terminal, use the newer one
        if new.getEmitTime() > current.getEmitTime():
            return new
        else:
            return current

    def fetch_job_rows(self) -> Tuple[List[Tuple[str, str, str, str, str, str, str]], Optional[str]]:
        """Return (rows, error_msg): rows are (job_id, name, workflow_id, status, native, site, last_update).
        Aggregates latest statuses per job across all workflows.
        Returns error message if connection fails.
        """
        latest_by_job: Dict[str, JobStatus] = {}
        all_statuses_by_job: Dict[str, List[JobStatus]] = {}
        
        try:
            workflows = lwfManager.getAllWorkflows() or []
            self._update_connection_status(True)
        except Exception as e:
            self._update_connection_status(False)
            return [], f"Failed to connect to lwfm service: {str(e)}"
        
        for wf in workflows:
            try:
                wid = wf.getWorkflowId()
                # get latest per job for this workflow
                w_statuses = lwfManager.getJobStatusesForWorkflow(wid) or []
                for s in w_statuses:
                    jid = s.getJobContext().getJobId()
                    
                    # Track all statuses for INFO-only detection
                    if jid not in all_statuses_by_job:
                        all_statuses_by_job[jid] = []
                    all_statuses_by_job[jid].append(s)
                    
                    # Track best status for display
                    cur = latest_by_job.get(jid)
                    if cur is None:
                        latest_by_job[jid] = s
                    else:
                        # Determine the best status to display
                        latest_by_job[jid] = self._get_best_status_for_display(cur, s)
            except Exception as e:
                # Quiet by default; enable with LWFM_GUI_DEBUG=1
                if os.environ.get("LWFM_GUI_DEBUG"):
                    print(f"Warning: Failed to get statuses for workflow {wid}: {e}")
                continue
        
        rows: List[Tuple[str, str, str, str, str, str, str]] = []
        for s in latest_by_job.values():
            try:
                ctx = s.getJobContext()
                jid = ctx.getJobId()
                wid = ctx.getWorkflowId() or ""
                name = ctx.getName() or ""
                
                # Check if this job has only INFO statuses
                job_statuses = all_statuses_by_job.get(jid, [])
                all_status_types = {(status.getStatus() or "").upper() for status in job_statuses}
                is_info_only = len(all_status_types) > 0 and all_status_types == {JobStatus.INFO}
                
                # Display status - treat INFO-only jobs as COMPLETE
                if is_info_only:
                    stat = JobStatus.COMPLETE
                else:
                    stat = (s.getStatus() or "")
                
                nat = s.getNativeStatusStr() or ""
                ts = s.getEmitTime().strftime('%Y-%m-%d %H:%M:%S') if s.getEmitTime() else ""
                try:
                    site = ctx.getSiteName() or ""
                except Exception:
                    site = ""
                if not site:
                    try:
                        st = lwfManager.getStatus(jid)
                        site = st.getJobContext().getSiteName() if st else ""
                    except Exception:
                        site = ""
                name = ctx.getName() or jid  # Use job ID as fallback if no name
                rows.append((jid, name, wid, stat, nat, site, ts))
            except Exception as e:
                if os.environ.get("LWFM_GUI_DEBUG"):
                    print(f"Warning: Failed to process job status: {e}")
                continue
        
        return rows, None

    def rebuild_table(self):
        """Rebuild the table with current data (synchronous version for internal use)."""
        self.tree.delete(*self.tree.get_children())
        try:
            rows, error_msg = self.fetch_job_rows()
            if error_msg:
                self.status_var.set(f"Error: {error_msg}")
                return
            
            # Store all rows and apply filter/sort
            self._all_rows = rows
            self._apply_filter_and_sort()
        except Exception as e:
            self.status_var.set(f"Error rebuilding table: {str(e)}")
            messagebox.showerror("Error", f"Failed to rebuild job table: {str(e)}")

    def refresh(self):
        if self._loading:
            return  # Prevent multiple simultaneous refreshes
        
        self._loading = True
        
        def worker():
            try:
                rows, error_msg = self.fetch_job_rows()
            except Exception as e:
                rows, error_msg = [], f"Unexpected error: {str(e)}"
            
            def done():
                self._loading = False
                if error_msg:
                    # Connection status shown only via colored dot indicator
                    pass
                else:
                    # Store all rows and apply filter/sort
                    self._all_rows = rows
                    self._apply_filter_and_sort()
            
            self.after(0, done)
        
        threading.Thread(target=worker, daemon=True).start()

    # --- Click handling ---
    def on_tree_click(self, event):
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            return
        row_id = self.tree.identify_row(event.y)
        col_id = self.tree.identify_column(event.x)
        if not row_id or not col_id:
            return
        col_index = int(col_id.replace('#', '')) - 1
        vals = self.tree.item(row_id, "values") or []
        if not vals:
            return
        job_id = vals[0]
        workflow_id = vals[2]  # Workflow is now at index 2 (after job_id, name)
        if col_index == 2 and workflow_id:
            # Workflow column
            self.view_workflow(workflow_id)
            return
        if col_index == 7:
            self.show_files(job_id)
            return
        # Otherwise, open workflow dialog with this job highlighted
        if workflow_id:
            self.view_workflow(workflow_id, highlight_job_id=job_id)
        else:
            # Fallback to status popup if no workflow ID
            self.show_job_status(job_id, workflow_id)

    def on_tree_motion(self, event):
        # Change cursor to hand over clickable cells
        row_id = self.tree.identify_row(event.y)
        col_id = self.tree.identify_column(event.x)
        if row_id and col_id in ('#2', '#7'):
            self.tree.configure(cursor="hand2")
        else:
            self.tree.configure(cursor="")

    # --- Dialog delegations ---
    def view_metasheets(self):
        progress_win, progress_bar = self._create_progress_dialog("Loading...", "Loading metasheets...")
        
        def worker():
            try:
                # This will be handled by the dialog itself, but we show progress first
                def show_dialog():
                    progress_win.destroy()
                    open_metasheets_dialog(self)
                self.after(0, show_dialog)
            except Exception as e:
                def show_error():
                    progress_win.destroy()
                    messagebox.showerror("Error", f"Failed to open metasheets dialog: {str(e)}")
                self.after(0, show_error)
        
        # Small delay to show progress indicator
        self.after(100, lambda: threading.Thread(target=worker, daemon=True).start())

    def view_workflow(self, workflow_id: str, highlight_job_id: str = ""):
        open_workflow_dialog(self, workflow_id, highlight_job_id)

    # --- Status history ---
    def show_job_status(self, job_id: str, workflow_id: str = ""):
        progress_win, progress_bar = self._create_progress_dialog("Loading...", f"Loading status for job {job_id}...")
        
        def worker():
            try:
                if workflow_id:
                    statuses = lwfManager.getAllJobStatusesForWorkflow(workflow_id) or []
                    statuses = [s for s in statuses if s.getJobContext().getJobId() == job_id]
                else:
                    statuses = lwfManager.getAllStatus(job_id) or []
                statuses.sort(key=lambda x: x.getEmitTime())
            except Exception as e:
                statuses = []
                # Show error in main thread
                def show_error():
                    progress_win.destroy()
                    messagebox.showerror("Error", 
                        f"Failed to load job status for {job_id}: {str(e)}")
                self.after(0, show_error)
                return
            
            def show_results():
                progress_win.destroy()
                self._show_status_window(job_id, statuses)
            
            self.after(0, show_results)

        threading.Thread(target=worker, daemon=True).start()

    def _show_status_window(self, job_id: str, statuses: List[JobStatus]):
        # Close existing status window if open
        if self._current_status_window:
            try:
                if self._current_status_window.winfo_exists():
                    self._current_status_window.destroy()
            except tk.TclError:
                # Window was already destroyed
                pass
            self._current_status_window = None
        
        # Window with a list of all status messages; details pane wraps long info
        win = tk.Toplevel(self)
        win.title(f"Status history for {job_id}")
        win.geometry("900x500")
        
        # Track this window and clear reference when closed
        self._current_status_window = win
        
        def on_window_close():
            self._current_status_window = None
            win.destroy()
        
        win.protocol("WM_DELETE_WINDOW", on_window_close)

        # Header with job info
        header = ttk.Frame(win)
        header.pack(side=tk.TOP, fill=tk.X, padx=8, pady=8)
        
        # Get job context info from first status
        parent_job_id = ""
        workflow_id = ""
        if statuses:
            try:
                context = statuses[0].getJobContext()
                parent_job_id = context.getParentJobId() or ""
                workflow_id = context.getWorkflowId() or ""
            except Exception:
                pass
        
        # Job ID (non-clickable)
        ttk.Label(header, text=f"Job ID: {job_id}").pack(side=tk.LEFT, padx=(0, 16))
        
        # Parent Job ID (clickable if exists)
        if parent_job_id:
            parent_label = ttk.Label(header, text=f"Parent Job: {parent_job_id}", foreground="#FF6B35", cursor="hand2")
            parent_label.pack(side=tk.LEFT, padx=(0, 16))
            parent_label.bind("<Button-1>", lambda e: self.show_job_status(parent_job_id))
        
        # Workflow ID (clickable if exists)
        if workflow_id:
            workflow_label = ttk.Label(header, text=f"Workflow: {workflow_id}", foreground="#FF6B35", cursor="hand2")
            workflow_label.pack(side=tk.LEFT)
            workflow_label.bind("<Button-1>", lambda e: self.view_workflow(workflow_id))

        top = ttk.Frame(win)
        top.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        cols = ("time", "status", "native")
        tv = ttk.Treeview(top, columns=cols, show="headings")
        # Use a lighter selection to keep colored text readable
        try:
            style = ttk.Style(tv)
            style_name = "LwfmStatus.Treeview"
            style.map(style_name,
                      background=[('selected', '#eaf2ff')],
                      foreground=[('selected', '#000000')])
            tv.configure(style=style_name)
        except Exception:
            pass
        # Color tags for statuses in the popup
        try:
            tv.tag_configure("status-bad", foreground="#d32f2f")    # red
            tv.tag_configure("status-good", foreground="#2e7d32")   # green
            tv.tag_configure("status-info", foreground="#1565c0")   # blue
        except Exception:
            pass
        tv.heading("time", text="Time")
        tv.heading("status", text="Status")
        tv.heading("native", text="Native")
        tv.column("time", width=200)
        tv.column("status", width=140)
        tv.column("native", width=200)
        tv.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        ysb = ttk.Scrollbar(top, orient=tk.VERTICAL, command=tv.yview)
        tv.configure(yscrollcommand=ysb.set)
        ysb.pack(side=tk.RIGHT, fill=tk.Y)

        # Horizontal scrollbar for long native strings
        xsb = ttk.Scrollbar(win, orient=tk.HORIZONTAL, command=tv.xview)
        tv.configure(xscrollcommand=xsb.set)
        xsb.pack(side=tk.TOP, fill=tk.X)

        # Map items to info text
        info_by_iid = {}
        for s in statuses:
            t = s.getEmitTime()
            time_str = datetime.fromtimestamp(t.timestamp()).strftime('%Y-%m-%d %H:%M:%S')
            s_val = (s.getStatus() or "").upper()
            if s_val in (JobStatus.FAILED, JobStatus.CANCELLED):
                tag = "status-bad"
            elif s_val == JobStatus.COMPLETE:
                tag = "status-good"
            elif s_val == JobStatus.INFO:
                tag = "status-info"
            else:
                tag = ""
            iid = tv.insert("", tk.END, values=(time_str, s.getStatus(), s.getNativeStatusStr() or ""),
                            tags=((tag,) if tag else ()))
            info_by_iid[iid] = s.getNativeInfo() or ""

        # Details area with wrapping text
        details = ttk.Frame(win)
        details.pack(side=tk.BOTTOM, fill=tk.BOTH, expand=False)
        ttk.Label(details, text="Details:").pack(side=tk.TOP, anchor=tk.W)
        text = tk.Text(details, height=8, wrap=tk.WORD)
        text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        dx = ttk.Scrollbar(details, orient=tk.VERTICAL, command=text.yview)
        text.configure(yscrollcommand=dx.set)
        dx.pack(side=tk.RIGHT, fill=tk.Y)

        def on_select(_event):
            sel = tv.selection()
            if not sel:
                return
            iid = sel[0]
            text.delete(1.0, tk.END)
            text.insert(tk.END, info_by_iid.get(iid, ""))

        tv.bind("<<TreeviewSelect>>", on_select)
        # Auto-select the latest message to populate details immediately
        last = tv.get_children()
        if last:
            last_iid = last[-1]
            tv.selection_set(last_iid)
            tv.see(last_iid)
            text.delete(1.0, tk.END)
            text.insert(tk.END, info_by_iid.get(last_iid, ""))
        # Determine cancelability and site
        try:
            latest = statuses[-1] if statuses else None
            latest_status = ((latest.getStatus() if latest else "") or "").upper()
            only_info = bool(statuses) and all(((s.getStatus() or "").upper()) == JobStatus.INFO for s in statuses)
            # Show cancel ONLY when pre-running or running
            cancelable = (latest_status in (JobStatus.READY, JobStatus.PENDING, JobStatus.RUNNING)) and (not only_info)
            site_name = (latest.getJobContext().getSiteName() if latest else None) or ""
            if not site_name:
                st = lwfManager.getStatus(job_id)
                site_name = st.getJobContext().getSiteName() if st else ""
        except Exception:
            cancelable = False
            site_name = ""

        # Bottom buttons
        btns = ttk.Frame(win)
        btns.pack(side=tk.BOTTOM, fill=tk.X)

        if cancelable and site_name:
            def do_cancel():
                self.cancel_job(job_id, site_name)
            ttk.Button(btns, text="Cancel", command=do_cancel).pack(side=tk.LEFT, padx=6, pady=6)

        # Show Open Log button if ~/.lwfm/logs/<jobId>.log exists
        try:
            log_dir = os.path.expanduser(SiteConfig.getLogFilename())
            log_path = os.path.join(log_dir, f"{job_id}.log")
        except Exception:
            log_path = ""

        def open_log():
            if not log_path or not os.path.exists(log_path):
                messagebox.showinfo("Log", "Log file not found.")
                return
            lw = tk.Toplevel(win)
            lw.title(f"Log {job_id}")
            lw.geometry("900x500")
            txt = tk.Text(lw, wrap=tk.NONE)
            xsb = ttk.Scrollbar(lw, orient=tk.HORIZONTAL, command=txt.xview)
            ysb = ttk.Scrollbar(lw, orient=tk.VERTICAL, command=txt.yview)
            txt.configure(xscrollcommand=xsb.set, yscrollcommand=ysb.set)
            txt.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
            ysb.pack(side=tk.RIGHT, fill=tk.Y)
            xsb.pack(side=tk.BOTTOM, fill=tk.X)
            try:
                with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
                    txt.insert(tk.END, f.read())
            except Exception as ex:
                txt.insert(tk.END, f"Error reading log: {ex}")

        if log_path and os.path.exists(log_path):
            ttk.Button(btns, text="Open Log", command=open_log).pack(side=tk.LEFT, padx=6, pady=6)

    def cancel_job(self, job_id: str, site: str):
        def worker():
            try:
                site_obj = lwfManager.getSite(site)
                ok = site_obj.getRunDriver().cancel(job_id)
                self.after(0, lambda: messagebox.showinfo("Cancel", f"Cancel {'succeeded' if ok else 'failed'} for {job_id}"))
            except Exception as ex:
                self.after(0, lambda: messagebox.showerror("Cancel", f"Error cancelling {job_id}: {ex}"))
        threading.Thread(target=worker, daemon=True).start()

    def show_files(self, job_id: str):
        progress_win, progress_bar = self._create_progress_dialog("Loading...", f"Loading files for job {job_id}...")
        
        def worker():
            try:
                metasheets = lwfManager.find({"_jobId": job_id}) or []
            except Exception as e:
                # Show error in main thread
                def show_error():
                    progress_win.destroy()
                    messagebox.showerror("Error", 
                        f"Failed to load files for {job_id}: {str(e)}")
                self.after(0, show_error)
                return
            
            def show_results():
                progress_win.destroy()
                self._show_files_window(job_id, metasheets)
            
            self.after(0, show_results)

        threading.Thread(target=worker, daemon=True).start()

    def _show_files_window(self, job_id: str, metas: List[Metasheet]):
        # Close existing files window if open
        if self._current_files_window:
            try:
                if self._current_files_window.winfo_exists():
                    self._current_files_window.destroy()
            except tk.TclError:
                # Window was already destroyed
                pass
            self._current_files_window = None
        
        win = tk.Toplevel(self)
        win.title(f"Files for {job_id}")
        win.geometry("900x500")
        
        # Track this window and clear reference when closed
        self._current_files_window = win
        
        def on_window_close():
            self._current_files_window = None
            win.destroy()
        
        win.protocol("WM_DELETE_WINDOW", on_window_close)

        top = ttk.Frame(win)
        top.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        cols = ("direction", "local", "siteobj")
        tv = ttk.Treeview(top, columns=cols, show="headings")
        tv.heading("direction", text="Dir")
        tv.heading("local", text="Local Path")
        tv.heading("siteobj", text="Site Object")
        tv.column("direction", width=80)
        tv.column("local", width=360)
        tv.column("siteobj", width=360)
        tv.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        ysb = ttk.Scrollbar(top, orient=tk.VERTICAL, command=tv.yview)
        tv.configure(yscrollcommand=ysb.set)
        ysb.pack(side=tk.RIGHT, fill=tk.Y)

        # Details area with full metasheet props
        details = ttk.Frame(win)
        details.pack(side=tk.BOTTOM, fill=tk.BOTH, expand=False)
        ttk.Label(details, text="Metasheet:").pack(side=tk.TOP, anchor=tk.W)
        text = tk.Text(details, height=10, wrap=tk.NONE)
        xsb = ttk.Scrollbar(details, orient=tk.HORIZONTAL, command=text.xview)
        ysb2 = ttk.Scrollbar(details, orient=tk.VERTICAL, command=text.yview)
        text.configure(xscrollcommand=xsb.set, yscrollcommand=ysb2.set)
        text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ysb2.pack(side=tk.RIGHT, fill=tk.Y)
        xsb.pack(side=tk.BOTTOM, fill=tk.X)

        info_by_iid = {}
        for ms in metas:
            p = ms.getProps() or {}
            iid = tv.insert("", tk.END, values=(p.get("_direction", ""), p.get("_localPath", ""), p.get("_siteObjPath", "")))
            try:
                info_by_iid[iid] = json.dumps(p, indent=2, sort_keys=True)
            except Exception:
                info_by_iid[iid] = str(p)

        def on_select(_event):
            sel = tv.selection()
            if not sel:
                return
            iid = sel[0]
            text.delete(1.0, tk.END)
            text.insert(tk.END, info_by_iid.get(iid, ""))

        tv.bind("<<TreeviewSelect>>", on_select)
        ttk.Button(win, text="Close", command=win.destroy).pack(side=tk.BOTTOM, pady=6)

    def _show_metasheet_window(self, metasheet: Metasheet):
        """Display a single metasheet's properties in a simple viewer."""
        win = tk.Toplevel(self)
        ms_id = ""
        try:
            ms_id = metasheet.getSheetId() or ""
        except Exception:
            pass
        title = f"Metasheet {ms_id}" if ms_id else "Metasheet"
        win.title(title)
        win.geometry("900x480")

        text = tk.Text(win, wrap=tk.NONE)
        xsb = ttk.Scrollbar(win, orient=tk.HORIZONTAL, command=text.xview)
        ysb = ttk.Scrollbar(win, orient=tk.VERTICAL, command=text.yview)
        text.configure(xscrollcommand=xsb.set, yscrollcommand=ysb.set)
        text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ysb.pack(side=tk.RIGHT, fill=tk.Y)
        xsb.pack(side=tk.BOTTOM, fill=tk.X)

        try:
            props = metasheet.getProps() or {}
            text.insert(tk.END, json.dumps(props, indent=2, sort_keys=True))
        except Exception as ex:
            text.insert(tk.END, f"Error reading metasheet: {ex}")
        ttk.Button(win, text="Close", command=win.destroy).pack(side=tk.BOTTOM, padx=6, pady=6)

    def view_server_log(self):
        """Open a simple read-only log viewer that tails the server log."""
        log_dir = os.path.expanduser(SiteConfig.getLogFilename())
        log_path = os.path.join(log_dir, "midware.log")
        win = tk.Toplevel(self)
        win.title("lwfm Server Log")
        win.geometry("900x500")

        frm = ttk.Frame(win)
        frm.pack(fill=tk.BOTH, expand=True)

        text = tk.Text(frm, wrap=tk.WORD)
        ysb = ttk.Scrollbar(frm, orient=tk.VERTICAL, command=text.yview)
        text.configure(yscrollcommand=ysb.set, state=tk.NORMAL, font=("Menlo", 10))
        text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ysb.pack(side=tk.RIGHT, fill=tk.Y)

        btns = ttk.Frame(win)
        btns.pack(fill=tk.X)
        ttk.Button(btns, text="Close", command=win.destroy).pack(side=tk.RIGHT, padx=6, pady=6)

        last_size = 0
        closed = False

        def poll():
            nonlocal last_size, closed
            if closed:
                return
            try:
                if os.path.exists(log_path):
                    size = os.path.getsize(log_path)
                    if size < last_size:
                        with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
                            text.delete(1.0, tk.END)
                            text.insert(tk.END, f.read())
                        last_size = size
                    elif size > last_size:
                        with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
                            f.seek(last_size)
                            text.insert(tk.END, f.read())
                        last_size = size
                    text.see(tk.END)
            except Exception:
                pass
            win.after(1000, poll)

        def on_close():
            nonlocal closed
            closed = True
            win.destroy()

        win.protocol("WM_DELETE_WINDOW", on_close)
        try:
            if os.path.exists(log_path):
                with open(log_path, 'r', encoding='utf-8', errors='replace') as f:
                    text.insert(tk.END, f.read())
        except Exception:
            pass
        poll()

    def shutdown_server(self):
        """Send a graceful shutdown signal to the lwfm server using its PID file."""
        # Confirm intent
        if not messagebox.askyesno(
            "Shutdown Server",
            "Are you sure you want to shut down the lwfm server?\n\nActive operations will be stopped.",
        ):
            return
        try:
            log_dir = os.path.expanduser(SiteConfig.getLogFilename())
            pid_path = os.path.join(log_dir, 'midware.pid')
        except Exception as ex:
            messagebox.showerror("Shutdown Server", f"Could not resolve PID file path: {ex}")
            return

        if not os.path.exists(pid_path):
            messagebox.showinfo("Shutdown Server", "Server PID file not found. The server may not be running.")
            return

        try:
            with open(pid_path, 'r', encoding='utf-8') as pf:
                pid_str = pf.read().strip()
            pid = int(pid_str)
        except Exception as ex:
            messagebox.showerror("Shutdown Server", f"Failed to read PID file: {ex}")
            return

        # Check if process appears alive
        try:
            os.kill(pid, 0)
        except Exception:
            messagebox.showinfo("Shutdown Server", "The server process is not running (stale PID file).")
            return

        # Try to send SIGTERM to the process group first (SvcLauncher uses start_new_session)
        try:
            os.killpg(os.getpgid(pid), signal.SIGTERM)
            message = "Shutdown signal sent to server (process group)."
        except Exception:
            try:
                os.kill(pid, signal.SIGTERM)
                message = "Shutdown signal sent to server (PID)."
            except Exception as ex:
                messagebox.showerror("Shutdown Server", f"Failed to send shutdown signal: {ex}")
                return

        # Nudge UI to reflect disconnect after a short delay
        self.after(1500, self.refresh)
        messagebox.showinfo("Shutdown Server", message)

    def view_events(self):
        """Open a panel listing active workflow events with filtering, sorting, and unset."""
        progress_win, progress_bar = self._create_progress_dialog("Loading...", "Loading workflow events...")
        
        def load_events():
            try:
                events = lwfManager.getActiveWfEvents() or []
                def show_events_window():
                    progress_win.destroy()
                    self._show_events_window(events)
                self.after(0, show_events_window)
            except Exception as e:
                def show_error():
                    progress_win.destroy()
                    messagebox.showerror("Error", f"Failed to load events: {str(e)}")
                self.after(0, show_error)
        
        threading.Thread(target=load_events, daemon=True).start()
    
    def _show_events_window(self, initial_events):
        """Display the events window with the loaded events."""
        win = tk.Toplevel(self)
        win.title("Pending Events")
        win.geometry("1000x560")

        # State
        events: List[WorkflowEvent] = initial_events
        rows: List[Dict[str, Any]] = []
        sort_by = "event_id"  # default sort by id for stability
        sort_asc = True
        filter_text = ""

        # Top controls
        top = ttk.Frame(win)
        top.pack(side=tk.TOP, fill=tk.X, padx=8, pady=6)
        ttk.Label(top, text="Filter:").pack(side=tk.LEFT)
        filt_entry = ttk.Entry(top, width=30)
        filt_entry.pack(side=tk.LEFT, padx=(4, 10))

        # Table
        table = ttk.Frame(win)
        table.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=6)
        cols = ("event_id", "type", "site", "workflow_id", "rule_or_query", "fire_job_id", "parent", "action")
        tv = ttk.Treeview(table, columns=cols, show="headings", selectmode="browse")
        headings = {
            "event_id": "Event ID",
            "type": "Type",
            "site": "Site",
            "workflow_id": "Workflow",
            "rule_or_query": "Rule / Query",
            "fire_job_id": "Fire Job",
            "parent": "Parent Job",
            "action": "",
        }
        widths = {
            "event_id": 180,
            "type": 110,
            "site": 100,
            "workflow_id": 160,
            "rule_or_query": 320,
            "fire_job_id": 160,
            "parent": 160,
            "action": 90,
        }

        def on_sort(column: str):
            nonlocal sort_by, sort_asc
            if sort_by == column:
                sort_asc = not sort_asc
            else:
                sort_by = column
                sort_asc = True
            rebuild()

        for cid in cols:
            tv.heading(cid, text=headings[cid], command=(lambda c=cid: on_sort(c)))
            anchor = tk.CENTER if cid in ("action",) else tk.W
            tv.column(cid, width=widths[cid], anchor=anchor)
        tv.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ysb = ttk.Scrollbar(table, orient=tk.VERTICAL, command=tv.yview)
        tv.configure(yscrollcommand=ysb.set)
        ysb.pack(side=tk.RIGHT, fill=tk.Y)

        # Details area
        bottom = ttk.Frame(win)
        bottom.pack(side=tk.BOTTOM, fill=tk.BOTH, expand=False)
        ttk.Label(bottom, text="Details:").pack(side=tk.TOP, anchor=tk.W)
        details = tk.Text(bottom, height=8, wrap=tk.WORD)
        dscroll = ttk.Scrollbar(bottom, orient=tk.VERTICAL, command=details.yview)
        details.configure(yscrollcommand=dscroll.set)
        details.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        dscroll.pack(side=tk.RIGHT, fill=tk.Y)

        # Mapping from iid to event
        by_iid: Dict[str, WorkflowEvent] = {}

        def describe_event(ev: WorkflowEvent) -> Dict[str, Any]:
            try:
                if isinstance(ev, NotificationEvent):
                    etype = "NotificationEvent"
                elif isinstance(ev, MetadataEvent):
                    etype = "MetadataEvent"
                elif isinstance(ev, JobEvent):
                    etype = "JobEvent"
                else:
                    etype = "WorkflowEvent"
                site = ev.getFireSite() if hasattr(ev, "getFireSite") else ""
                wid = ev.getWorkflowId() if hasattr(ev, "getWorkflowId") else ""
                fire_job = ev.getFireJobId() if hasattr(ev, "getFireJobId") else ""
                parent = ev.getParentId() if hasattr(ev, "getParentId") else ""
                rule_or_query = ""
                if isinstance(ev, JobEvent):
                    try:
                        rule_or_query = f"job={ev.getRuleJobId()} status={ev.getRuleStatus()}"
                    except Exception:
                        rule_or_query = "job/status"
                elif isinstance(ev, MetadataEvent):
                    try:
                        rule_or_query = json.dumps(ev.getQueryRegExs(), separators=(",", ":"))
                    except Exception:
                        rule_or_query = "query"
                return {
                    "event_id": ev.getEventId(),
                    "type": etype,
                    "site": site,
                    "workflow_id": wid,
                    "rule_or_query": rule_or_query,
                    "fire_job_id": fire_job,
                    "parent": parent,
                }
            except Exception:
                return {"event_id": "", "type": "", "site": "", "workflow_id": "",
                        "rule_or_query": "", "fire_job_id": "", "parent": ""}

        def fetch() -> List[WorkflowEvent]:
            try:
                return lwfManager.getActiveWfEvents() or []
            except Exception:
                return []

        def apply_filter_sort(in_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            f = (filter_text or "").lower()
            filt_rows: List[Dict[str, Any]] = []
            for r in in_rows:
                hay = " ".join([
                    str(r.get(k, "")) for k in (
                        "event_id", "type", "site", "workflow_id", "rule_or_query", "fire_job_id", "parent"
                    )
                ]).lower()
                if f and f not in hay:
                    continue
                filt_rows.append(r)
            key = sort_by
            rev = not sort_asc
            return sorted(filt_rows, key=lambda rr: str(rr.get(key, "")), reverse=rev)

        def rebuild():
            tv.delete(*tv.get_children())
            show_rows = apply_filter_sort(rows)
            by_iid.clear()
            by_id = {e.getEventId(): e for e in events}
            for r in show_rows:
                vals = (
                    r.get("event_id", ""), r.get("type", ""), r.get("site", ""), r.get("workflow_id", ""),
                    r.get("rule_or_query", ""), r.get("fire_job_id", ""), r.get("parent", ""), "[ Unset ]"
                )
                iid = tv.insert("", tk.END, values=vals)
                ev_obj = by_id.get(r.get("event_id", ""))
                if ev_obj:
                    by_iid[iid] = ev_obj
            try:
                win.title(f"Pending Events ({len(show_rows)})")
            except Exception:
                pass

        def refresh(load: bool = False):
            nonlocal events, rows
            if load:
                events = fetch()
                rows = [describe_event(ev) for ev in events]
            rebuild()

        def unset_selected():
            sel = tv.selection()
            if not sel:
                messagebox.showinfo("Unset", "Select an event to unset.")
                return
            ev = by_iid.get(sel[0])
            if not ev:
                return
            try:
                lwfManager.unsetEvent(ev)
                refresh(load=True)
            except Exception as ex:
                messagebox.showerror("Unset", f"Error unsetting event: {ex}")

        def on_row_click(event):
            region = tv.identify("region", event.x, event.y)
            if region != "cell":
                return
            row_id = tv.identify_row(event.y)
            col_id = tv.identify_column(event.x)
            if not row_id or not col_id:
                return
            col_index = int(col_id.replace('#', '')) - 1
            if cols[col_index] == "action":
                ev = by_iid.get(row_id)
                if ev:
                    try:
                        lwfManager.unsetEvent(ev)
                        refresh(load=True)
                    except Exception as ex:
                        messagebox.showerror("Unset", f"Error unsetting event: {ex}")
                return
            # update details
            ev = by_iid.get(row_id)
            details.delete(1.0, tk.END)
            try:
                details.insert(tk.END, str(ev) if ev else "")
            except Exception as ex:
                details.insert(tk.END, f"Error: {ex}")

        # Bindings and buttons now that handlers exist
        def apply_from_entry():
            nonlocal filter_text
            filter_text = (filt_entry.get() or "").strip()
            rebuild()

        filt_entry.bind("<Return>", lambda e: apply_from_entry())
        ttk.Button(top, text="Apply", command=apply_from_entry).pack(side=tk.LEFT)
        ttk.Button(top, text="Refresh", command=lambda: refresh(load=True)).pack(side=tk.LEFT, padx=(10, 0))
        unset_btn = ttk.Button(top, text="Unset Selected", command=unset_selected)
        unset_btn.pack(side=tk.LEFT, padx=(10, 0))
        tv.bind("<Button-1>", on_row_click)

        # Initial load
        refresh(load=True)

        # Auto-refresh loop tied to the main interval; cancels on window close
        timer_id: Optional[str] = None
        closed = False

        def get_interval_ms() -> int:
            try:
                txt = self.interval_entry.get()
                n = int(txt)
                if n <= 0:
                    n = RefreshIntervalSecDefault
            except Exception:
                n = self.refresh_interval or RefreshIntervalSecDefault
            return n * 1000

        def tick():
            nonlocal timer_id
            if closed:
                return
            refresh(load=True)
            try:
                ms = get_interval_ms()
            except Exception:
                ms = RefreshIntervalSecDefault * 1000
            timer_id = win.after(ms, tick)

        def on_close():
            nonlocal closed, timer_id
            closed = True
            try:
                if timer_id:
                    win.after_cancel(timer_id)
            except Exception:
                pass
            win.destroy()

        # Start auto-refresh and bind close
        win.protocol("WM_DELETE_WINDOW", on_close)
        timer_id = win.after(get_interval_ms(), tick)



def main():
    app = LwfmGui()
    app.mainloop()
