"""
Tkinter-based GUI for lwfm job monitoring and control.
This lean app module focuses on main window wiring and delegates dialogs
(metasheets, workflow, etc.) to submodules in this package.
"""
from __future__ import annotations

import json
import os
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import tkinter as tk
from tkinter import messagebox, ttk

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
        self.geometry("1120x720")

        # Top toolbar
        toolbar = ttk.Frame(self)
        toolbar.pack(side=tk.TOP, fill=tk.X)
        ttk.Button(toolbar, text="Refresh", command=self.refresh).pack(side=tk.LEFT, padx=6, pady=6)
        ttk.Button(toolbar, text="Metasheets", command=self.view_metasheets).pack(side=tk.LEFT)
        ttk.Button(toolbar, text="Events", command=self.view_events).pack(side=tk.LEFT)
        ttk.Button(toolbar, text="Server Log", command=self.view_server_log).pack(side=tk.LEFT)
        ttk.Label(toolbar, text="Interval (s):").pack(side=tk.LEFT, padx=(16, 4))
        self.interval_entry = ttk.Entry(toolbar, width=6)
        self.interval_entry.insert(0, str(RefreshIntervalSecDefault))
        self.interval_entry.pack(side=tk.LEFT)

        # Jobs table
        cols = ("job_id", "workflow", "status", "native", "last_update", "files")
        self.tree = ttk.Treeview(self, columns=cols, show="headings", selectmode="browse")
        headings = {
            "job_id": "Job ID",
            "workflow": "Workflow",
            "status": "Status",
            "native": "Native",
            "last_update": "Last Update",
            "files": "Files",
        }
        widths = {
            "job_id": 260,
            "workflow": 200,
            "status": 100,
            "native": 200,
            "last_update": 180,
            "files": 80,
        }
        for c in cols:
            self.tree.heading(c, text=headings[c])
            anchor = tk.CENTER if c == "files" else tk.W
            self.tree.column(c, width=widths[c], anchor=anchor)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(8, 0), pady=8)
        ysb = ttk.Scrollbar(self, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=ysb.set)
        ysb.pack(side=tk.RIGHT, fill=tk.Y, padx=(0, 8), pady=8)

        # Color tags
        try:
            self.tree.tag_configure("status-bad", foreground="#d32f2f")
            self.tree.tag_configure("status-good", foreground="#2e7d32")
        except Exception:
            pass

        # Bind clicks (workflow opens dialog; files opens files window)
        self.tree.bind("<Button-1>", self.on_tree_click)
        self.tree.bind("<Motion>", self.on_tree_motion)

        # Status bar
        self.status_var = tk.StringVar(value="")
        ttk.Label(self, textvariable=self.status_var, anchor=tk.W).pack(side=tk.BOTTOM, fill=tk.X)

        self.refresh_interval = RefreshIntervalSecDefault
        self._timer_id: Optional[str] = None
        self.refresh()
        self._tick()

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

    def fetch_job_rows(self) -> List[Tuple[str, str, str, str, str]]:
        """Return rows: (job_id, workflow_id, status, native, last_update).
        Aggregates latest statuses per job across all workflows.
        """
        latest_by_job: Dict[str, JobStatus] = {}
        try:
            workflows = lwfManager.getAllWorkflows() or []
        except Exception:
            workflows = []
        for wf in workflows:
            try:
                wid = wf.getWorkflowId()
                # get latest per job for this workflow
                w_statuses = lwfManager.getJobStatusesForWorkflow(wid) or []
                for s in w_statuses:
                    jid = s.getJobContext().getJobId()
                    cur = latest_by_job.get(jid)
                    if (cur is None) or (s.getEmitTime() > cur.getEmitTime()):
                        latest_by_job[jid] = s
            except Exception:
                continue
        rows: List[Tuple[str, str, str, str, str]] = []
        for s in latest_by_job.values():
            ctx = s.getJobContext()
            jid = ctx.getJobId()
            wid = ctx.getWorkflowId() or ""
            stat = (s.getStatus() or "")
            nat = s.getNativeStatusStr() or ""
            ts = s.getEmitTime().strftime('%Y-%m-%d %H:%M:%S') if s.getEmitTime() else ""
            rows.append((jid, wid, stat, nat, ts))
        # Sort newest first by time string (same format; safe lexicographically)
        rows.sort(key=lambda r: r[4], reverse=True)
        return rows

    def rebuild_table(self):
        self.tree.delete(*self.tree.get_children())
        rows = self.fetch_job_rows()
        for jid, wid, stat, nat, ts in rows:
            tag = ()
            up = (stat or "").upper()
            if up in (JobStatus.FAILED, JobStatus.CANCELLED):
                tag = ("status-bad",)
            elif up == JobStatus.COMPLETE:
                tag = ("status-good",)
            self.tree.insert("", tk.END, values=(jid, wid, stat, nat, ts, "[ Files ]"), tags=tag)
        self.status_var.set(f"Jobs: {len(rows)}")

    def refresh(self):
        def worker():
            try:
                rows = self.fetch_job_rows()
            except Exception:
                rows = []
            def done():
                self.tree.delete(*self.tree.get_children())
                for jid, wid, stat, nat, ts in rows:
                    tag = ()
                    up = (stat or "").upper()
                    if up in (JobStatus.FAILED, JobStatus.CANCELLED):
                        tag = ("status-bad",)
                    elif up == JobStatus.COMPLETE:
                        tag = ("status-good",)
                    self.tree.insert("", tk.END, values=(jid, wid, stat, nat, ts, "[ Files ]"), tags=tag)
                self.status_var.set(f"Jobs: {len(rows)}")
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
        workflow_id = vals[1]
        if col_index == 1 and workflow_id:
            # Workflow column
            self.view_workflow(workflow_id)
            return
        if col_index == 5:
            self.show_files(job_id)
            return
        # Otherwise, show status details
        self.show_job_status(job_id, workflow_id)

    def on_tree_motion(self, event):
        # Change cursor to hand over clickable cells
        row_id = self.tree.identify_row(event.y)
        col_id = self.tree.identify_column(event.x)
        if row_id and col_id in ('#2', '#6'):
            self.tree.configure(cursor="hand2")
        else:
            self.tree.configure(cursor="")

    # --- Dialog delegations ---
    def view_metasheets(self):
        open_metasheets_dialog(self)

    def view_workflow(self, workflow_id: str):
        open_workflow_dialog(self, workflow_id)

    # --- Status history ---
    def show_job_status(self, job_id: str, workflow_id: str = ""):
        def worker():
            try:
                if workflow_id:
                    statuses = lwfManager.getAllJobStatusesForWorkflow(workflow_id) or []
                    statuses = [s for s in statuses if s.getJobContext().getJobId() == job_id]
                else:
                    s = lwfManager.getStatus(job_id)
                    statuses = [s] if s else []
                statuses.sort(key=lambda x: x.getEmitTime())
            except Exception:
                statuses = []
            self.after(0, lambda: self._show_status_window(job_id, statuses))
        threading.Thread(target=worker, daemon=True).start()

    def _show_status_window(self, job_id: str, statuses: List[JobStatus]):
        # Window with a list of all status messages; details pane wraps long info
        win = tk.Toplevel(self)
        win.title(f"Status history for {job_id}")
        win.geometry("900x500")

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
        ttk.Button(btns, text="Close", command=win.destroy).pack(side=tk.RIGHT, padx=6, pady=6)

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
        def worker():
            metas: List[Metasheet] = lwfManager.find({"_jobId": job_id}) or []
            self.after(0, lambda: self._show_files_window(job_id, metas))
        threading.Thread(target=worker, daemon=True).start()

    def _show_files_window(self, job_id: str, metas: List[Metasheet]):
        win = tk.Toplevel(self)
        win.title(f"Files for {job_id}")
        win.geometry("900x500")

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
                last_size = os.path.getsize(log_path)
                text.see(tk.END)
        except Exception:
            pass
        win.after(1000, poll)

    def view_events(self):
        """Open a panel listing active workflow events with filtering, sorting, and unset."""
        win = tk.Toplevel(self)
        win.title("Pending Events")
        win.geometry("1000x560")

        # State
        events: List[WorkflowEvent] = []
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
