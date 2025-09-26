from __future__ import annotations

import json
import os
import threading
from datetime import datetime
from typing import Any, Dict, List
import tkinter as tk
from tkinter import messagebox, ttk

from lwfm.base.JobStatus import JobStatus
from lwfm.base.Metasheet import Metasheet  # type: ignore
from lwfm.midware.LwfManager import lwfManager
from lwfm.midware._impl.SiteConfig import SiteConfig


def open_workflow_dialog(gui: tk.Misc, workflow_id: str, highlight_job_id: str = ""):
    """Open the workflow details dialog with Overview, Jobs, Data, Graph tabs."""
    win = tk.Toplevel(gui)
    win.title(f"Workflow {workflow_id}")
    win.geometry("1100x680")

    # Load workflow data immediately
    try:
        wf = lwfManager.getWorkflow(workflow_id)
        wf_name = wf.getName() if wf else "(not found)"
        wf_desc = wf.getDescription() if wf else "(not found)"
        wf_props = wf.getProps() if wf else {}
    except Exception:
        wf = None
        wf_name = "(error loading)"
        wf_desc = "(error loading)"
        wf_props = {}

    nb = ttk.Notebook(win)
    nb.pack(fill=tk.BOTH, expand=True)

    # --- Overview tab ---
    tab_overview = ttk.Frame(nb)
    nb.add(tab_overview, text="Overview")
    ov_top = ttk.Frame(tab_overview)
    ov_top.pack(side=tk.TOP, fill=tk.X, padx=8, pady=8)
    
    ttk.Label(ov_top, text="Workflow ID:").grid(row=0, column=0, sticky=tk.W)
    wf_id_entry = ttk.Entry(ov_top, width=60, state="readonly")
    wf_id_entry.grid(row=0, column=1, sticky=tk.W, padx=(4, 16))
    wf_id_entry.configure(state="normal")
    wf_id_entry.insert(0, workflow_id)
    wf_id_entry.configure(state="readonly")
    ttk.Label(ov_top, text="Name:").grid(row=1, column=0, sticky=tk.W)
    ttk.Label(ov_top, text=wf_name, wraplength=400).grid(row=1, column=1, sticky=tk.W, padx=(4, 16))
    ttk.Label(ov_top, text="Description:").grid(row=2, column=0, sticky=tk.W)
    ttk.Label(ov_top, text=wf_desc, wraplength=400).grid(row=2, column=1, sticky=tk.W, padx=(4, 16))
    
    # Properties viewer
    ttk.Label(tab_overview, text="Properties:").pack(side=tk.TOP, anchor=tk.W, padx=8)
    props_text = tk.Text(tab_overview, height=10, wrap=tk.NONE)
    px = ttk.Scrollbar(tab_overview, orient=tk.HORIZONTAL, command=props_text.xview)
    py = ttk.Scrollbar(tab_overview, orient=tk.VERTICAL, command=props_text.yview)
    props_text.configure(xscrollcommand=px.set, yscrollcommand=py.set)
    props_frame = ttk.Frame(tab_overview)
    props_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))
    props_text.pack(in_=props_frame, side=tk.LEFT, fill=tk.BOTH, expand=True)
    py.pack(in_=props_frame, side=tk.RIGHT, fill=tk.Y)
    px.pack(in_=props_frame, side=tk.BOTTOM, fill=tk.X)
    
    # Insert properties
    props_text.insert(tk.END, json.dumps(wf_props, indent=2, sort_keys=True))
    props_text.configure(state=tk.DISABLED)

    # --- Jobs tab ---
    tab_jobs = ttk.Frame(nb)
    nb.add(tab_jobs, text="Jobs")
    
    # Create split pane with jobs list on top and details on bottom
    jobs_paned = ttk.PanedWindow(tab_jobs, orient=tk.VERTICAL)
    jobs_paned.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
    
    # Top pane: Jobs list
    jobs_top_frame = ttk.Frame(jobs_paned)
    jobs_paned.add(jobs_top_frame, weight=1)
    
    cols = ("job_id", "status", "native", "last_update", "files")
    tv_jobs = ttk.Treeview(jobs_top_frame, columns=cols, show="headings", selectmode="browse")
    for cid, w in zip(cols, (260, 120, 180, 180, 80)):
        tv_jobs.heading(cid, text=cid.replace("_", " ").title())
        tv_jobs.column(cid, width=w, anchor=(tk.CENTER if cid == "files" else tk.W))
    tv_jobs.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    ysb_jobs = ttk.Scrollbar(jobs_top_frame, orient=tk.VERTICAL, command=tv_jobs.yview)
    tv_jobs.configure(yscrollcommand=ysb_jobs.set)
    ysb_jobs.pack(side=tk.RIGHT, fill=tk.Y)
    try:
        tv_jobs.tag_configure("status-bad", foreground="#d32f2f")
        tv_jobs.tag_configure("status-good", foreground="#2e7d32")
    except Exception:
        pass

    # Bottom pane: Job status details
    jobs_bottom_frame = ttk.Frame(jobs_paned)
    jobs_paned.add(jobs_bottom_frame, weight=1)
    
    # Job details header
    details_header = ttk.Frame(jobs_bottom_frame)
    details_header.pack(side=tk.TOP, fill=tk.X, padx=8, pady=8)
    
    # Job info labels (will be populated when job is selected)
    job_info_frame = ttk.Frame(details_header)
    job_info_frame.pack(side=tk.LEFT, fill=tk.X, expand=True)
    
    # Create horizontal split pane between status history and details
    details_paned = ttk.PanedWindow(jobs_bottom_frame, orient=tk.HORIZONTAL)
    details_paned.pack(fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))
    
    # Left pane: Status history table
    status_frame = ttk.Frame(details_paned)
    details_paned.add(status_frame, weight=2)
    
    status_cols = ("time", "status", "native")
    tv_status = ttk.Treeview(status_frame, columns=status_cols, show="headings")
    try:
        style = ttk.Style(tv_status)
        style_name = "WorkflowStatus.Treeview"
        style.map(style_name,
                  background=[('selected', '#eaf2ff')],
                  foreground=[('selected', '#000000')])
        tv_status.configure(style=style_name)
    except Exception:
        pass
    try:
        tv_status.tag_configure("status-bad", foreground="#d32f2f")
        tv_status.tag_configure("status-good", foreground="#2e7d32")
        tv_status.tag_configure("status-info", foreground="#1565c0")
    except Exception:
        pass
    tv_status.heading("time", text="Time")
    tv_status.heading("status", text="Status")
    tv_status.heading("native", text="Native")
    tv_status.column("time", width=200)
    tv_status.column("status", width=140)
    tv_status.column("native", width=200)
    tv_status.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    
    ysb_status = ttk.Scrollbar(status_frame, orient=tk.VERTICAL, command=tv_status.yview)
    tv_status.configure(yscrollcommand=ysb_status.set)
    ysb_status.pack(side=tk.RIGHT, fill=tk.Y)
    
    # Right pane: Details text area
    details_frame = ttk.Frame(details_paned)
    details_paned.add(details_frame, weight=1)
    
    ttk.Label(details_frame, text="Details:").pack(side=tk.TOP, anchor=tk.W)
    details_text = tk.Text(details_frame, wrap=tk.WORD)
    details_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    details_scroll = ttk.Scrollbar(details_frame, orient=tk.VERTICAL, command=details_text.yview)
    details_text.configure(yscrollcommand=details_scroll.set)
    details_scroll.pack(side=tk.RIGHT, fill=tk.Y)

    # --- Data tab ---
    tab_data = ttk.Frame(nb)
    nb.add(tab_data, text="Data")
    data_cols = ("direction", "local", "siteobj")
    tv_data = ttk.Treeview(tab_data, columns=data_cols, show="headings")
    tv_data.heading("direction", text="Dir")
    tv_data.heading("local", text="Local Path")
    tv_data.heading("siteobj", text="Site Object")
    tv_data.column("direction", width=80)
    tv_data.column("local", width=460)
    tv_data.column("siteobj", width=460)
    tv_data.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(8, 0), pady=8)
    ysb_data = ttk.Scrollbar(tab_data, orient=tk.VERTICAL, command=tv_data.yview)
    tv_data.configure(yscrollcommand=ysb_data.set)
    ysb_data.pack(side=tk.RIGHT, fill=tk.Y, padx=(0, 8), pady=8)

    # --- Graph tab ---
    tab_graph = ttk.Frame(nb)
    nb.add(tab_graph, text="Graph")
    # Controls row
    ctrl = ttk.Frame(tab_graph)
    ctrl.pack(side=tk.TOP, fill=tk.X)
    ttk.Label(ctrl, text="Layout:").pack(side=tk.LEFT, padx=(8, 4), pady=4)
    layout_var = tk.StringVar(value="Hierarchy")
    layout_box = ttk.Combobox(ctrl, textvariable=layout_var, values=[
        "Hierarchy", "Left-right", "Circle"
    ], state="readonly", width=14)
    layout_box.pack(side=tk.LEFT, padx=(0, 8), pady=4)
    # Dark mode canvas
    canvas = tk.Canvas(tab_graph, background="#121212", highlightthickness=0)
    canvas.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

    jobs_map: Dict[str, List[JobStatus]] = {}
    metas: List[Metasheet] = []

    # Pan/zoom state and helpers
    state = {"scale": 1.0, "ox": 0.0, "oy": 0.0, "drag": False, "sx": 0, "sy": 0, "moved": 0.0}

    def to_view(x: float, y: float) -> tuple[float, float]:
        s = state["scale"]
        return x * s + state["ox"], y * s + state["oy"]

    # Keep last graph for hit-testing and actions
    last_node_pos: Dict[str, tuple] = {}
    last_edges: List[tuple] = []
    last_data_nodes: Dict[str, Dict[str, Any]] = {}

    def jobs_rebuild():
        tv_jobs.delete(*tv_jobs.get_children())
        # Precompute which jobs have files based on metas (_jobId present)
        jobs_with_files = set()
        try:
            for ms in metas:
                p = ms.getProps() or {}
                jid = str(p.get("_jobId") or "")
                if jid:
                    jobs_with_files.add(jid)
        except Exception:
            pass

        for job_id, statuses in jobs_map.items():
            latest = None
            if statuses:
                latest = max(statuses, key=lambda s: s.getEmitTime())
            stat = (latest.getStatus() if latest else "") or ""
            up = stat.upper()
            tag = ()
            if up in (JobStatus.FAILED, JobStatus.CANCELLED):
                tag = ("status-bad",)
            elif up == JobStatus.COMPLETE:
                tag = ("status-good",)
            last_time = latest.getEmitTime().strftime('%Y-%m-%d %H:%M:%S') if latest else ""
            nat = latest.getNativeStatusStr() if latest else ""
            files_cell = "[ Files ]" if job_id in jobs_with_files else ""
            tv_jobs.insert("", tk.END, values=(job_id, stat, nat, last_time, files_cell), tags=tag)

    def populate_job_details(job_id: str):
        """Populate the job details pane with status history for the selected job."""
        # Clear existing details
        tv_status.delete(*tv_status.get_children())
        details_text.delete(1.0, tk.END)
        
        # Clear and rebuild job info header
        for widget in job_info_frame.winfo_children():
            widget.destroy()
        
        # Get job statuses
        statuses = jobs_map.get(job_id, [])
        if not statuses:
            ttk.Label(job_info_frame, text=f"Job ID: {job_id} (no status data)").pack(side=tk.LEFT)
            return
        
        # Get job context info from first status
        parent_job_id = ""
        try:
            context = statuses[0].getJobContext()
            parent_job_id = context.getParentJobId() or ""
        except Exception:
            pass
        
        # Job ID (non-clickable)
        ttk.Label(job_info_frame, text=f"Job ID: {job_id}").pack(side=tk.LEFT, padx=(0, 16))
        
        # Parent Job ID (clickable if exists and different from current job)
        if parent_job_id and parent_job_id != job_id and parent_job_id in jobs_map:
            parent_label = ttk.Label(job_info_frame, text=f"Parent Job: {parent_job_id}", 
                                   foreground="#FF6B35", cursor="hand2")
            parent_label.pack(side=tk.LEFT, padx=(0, 16))
            parent_label.bind("<Button-1>", lambda e: populate_job_details(parent_job_id))
        
        # Workflow ID (clickable)
        workflow_label = ttk.Label(job_info_frame, text=f"Workflow: {workflow_id}", 
                                 foreground="#FF6B35", cursor="hand2")
        workflow_label.pack(side=tk.LEFT, padx=(0, 16))
        # Note: workflow click could open a new workflow dialog, but that might be confusing
        
        # Add Open Log button if log file exists
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
            ttk.Button(job_info_frame, text="Job Log", command=open_log).pack(side=tk.LEFT, padx=(0, 8))
        
        # Populate status history
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
            iid = tv_status.insert("", tk.END, values=(time_str, s.getStatus(), s.getNativeStatusStr() or ""),
                                 tags=((tag,) if tag else ()))
            info_by_iid[iid] = s.getNativeInfo() or ""
        
        # Auto-select the latest message to populate details immediately
        last = tv_status.get_children()
        if last:
            last_iid = last[-1]
            tv_status.selection_set(last_iid)
            tv_status.see(last_iid)
            details_text.insert(tk.END, info_by_iid.get(last_iid, ""))
        
        # Set up selection handler for status details
        def on_status_select(_event):
            sel = tv_status.selection()
            if not sel:
                return
            iid = sel[0]
            details_text.delete(1.0, tk.END)
            details_text.insert(tk.END, info_by_iid.get(iid, ""))
        
        # Remove any existing bindings and add new one
        tv_status.unbind("<<TreeviewSelect>>")
        tv_status.bind("<<TreeviewSelect>>", on_status_select)

    def data_rebuild():
        tv_data.delete(*tv_data.get_children())
        for ms in metas:
            p = ms.getProps() or {}
            tv_data.insert("", tk.END, values=(p.get("_direction", ""), p.get("_localPath", ""), p.get("_siteObjPath", "")))

    def on_jobs_click(event):
        region = tv_jobs.identify("region", event.x, event.y)
        if region != "cell":
            return
        row_id = tv_jobs.identify_row(event.y)
        col_id = tv_jobs.identify_column(event.x)
        if not row_id or not col_id:
            return
        idx = int(col_id.replace('#', '')) - 1
        vals = tv_jobs.item(row_id, "values") or []
        if not vals:
            return
        job_id = vals[0]
        if cols[idx] == "files":
            try:
                # Only open files if this row actually has a Files link
                if (vals[4] or "").strip():
                    gui.show_files(job_id)  # type: ignore[attr-defined]
            except Exception:
                pass
        else:
            # Any other column click populates the job details pane
            populate_job_details(job_id)
    
    tv_jobs.bind("<Button-1>", on_jobs_click)
    
    # Also handle selection changes to populate details
    def on_jobs_select(_event):
        sel = tv_jobs.selection()
        if sel:
            vals = tv_jobs.item(sel[0], "values") or []
            if vals:
                populate_job_details(vals[0])
    
    tv_jobs.bind("<<TreeviewSelect>>", on_jobs_select)

    def on_data_dbl(_e):
        sel = tv_data.selection()
        if not sel:
            return
        idx = tv_data.index(sel[0])
        if 0 <= idx < len(metas):
            try:
                gui._show_metasheet_window(metas[idx])  # type: ignore[attr-defined]
            except Exception:
                pass
    tv_data.bind("<Double-1>", on_data_dbl)

    def draw_graph():
        try:
            canvas.delete("all")
        except tk.TclError:
            # Canvas has been destroyed, nothing to draw
            return
        # Colors
        grid = "#1e1e1e"
        text = "#e0e0e0"
        edge_default = "#9e9e9e"
        edge_put = "#64b5f6"   # light blue
        edge_get = "#ffb74d"   # light orange
        edge_flow = "#bdbdbd"
        edge_root = "#81c784"   # greenish
        # Job node colors based on terminal status
        job_fill_default = "#1e1e1e"      # dark - for in-flight jobs
        job_outline_default = "#90caf9"   # light blue - for in-flight jobs
        job_fill_success = "#1b5e20"      # dark green - for successful terminal jobs
        job_outline_success = "#4caf50"   # bright green - for successful terminal jobs  
        job_fill_failed = "#b71c1c"       # dark red - for failed jobs
        job_outline_failed = "#f44336"    # bright red - for failed jobs
        data_fill = "#2a1f14"
        data_outline = "#ef6c00"
        wf_fill = "#283593"      # indigo 800
        wf_outline = "#c5cae9"   # indigo 100

        # Grid
        try:
            w = max(canvas.winfo_width(), 1)
            h = max(canvas.winfo_height(), 1)
            step = 80  # model units; scales with zoom
            s = state["scale"]
            ox = state["ox"]; oy = state["oy"]
            # Visible model-space bounds
            x0 = (0 - ox) / max(s, 1e-6)
            y0 = (0 - oy) / max(s, 1e-6)
            x1 = (w - ox) / max(s, 1e-6)
            y1 = (h - oy) / max(s, 1e-6)
            import math as _math
            gx0 = _math.floor(x0 / step) * step
            gy0 = _math.floor(y0 / step) * step
            # Vertical lines
            x = gx0
            while x <= x1:
                vx0, vy0 = to_view(x, y0)
                vx1, vy1 = to_view(x, y1)
                canvas.create_line(vx0, 0, vx1, h, fill=grid)
                x += step
            # Horizontal lines
            y = gy0
            while y <= y1:
                vx0, vy0 = to_view(x0, y)
                vx1, vy1 = to_view(x1, y)
                canvas.create_line(0, vy0, w, vy1, fill=grid)
                y += step
        except Exception:
            pass
        # Build a bipartite graph of Job nodes and Data nodes and draw directional edges
        # Coalesce data nodes by site path if present else by local path
        # Nodes
        job_nodes = list(jobs_map.keys())
        # Build data nodes from metasheets
        data_nodes: Dict[str, Dict[str, Any]] = {}  # key -> props summary
        edges: List[tuple] = []  # (src_id, dst_id, kind) where ids include prefix 'J:' or 'D:'
        for ms in metas:
            p = ms.getProps() or {}
            key = p.get("_siteObjPath") or p.get("_localPath") or ms.getSheetId()
            key = str(key)
            if key not in data_nodes:
                data_nodes[key] = {
                    "site": p.get("_siteName", ""),
                    "path": key,
                }
            direction = (p.get("_direction") or "").lower()
            job_id = str(p.get("_jobId") or "")
            if not job_id:
                continue
            if direction == "put":
                edges.append((f"J:{job_id}", f"D:{key}", "put"))
            elif direction == "get":
                edges.append((f"D:{key}", f"J:{job_id}", "get"))
        # Track parent relations and find root jobs (no parent in this workflow)
        parents: Dict[str, str] = {}
        for job_id, sts in jobs_map.items():
            if not sts:
                continue
            latest = max(sts, key=lambda s: s.getEmitTime())
            try:
                ctx = latest.getJobContext()
                parent = ctx.getParentJobId()
                if parent and parent in job_nodes:
                    edges.append((f"J:{parent}", f"J:{job_id}", "flow"))
                    parents[job_id] = parent
            except Exception:
                pass

        # Add a single workflow node and connect to root jobs
        wf_id = workflow_id
        wf_node_id = f"W:{wf_id}"
        for jid in job_nodes:
            if jid not in parents:
                edges.append((wf_node_id, f"J:{jid}", "root"))
        # Layout levels alternating for bipartite portions
        from collections import defaultdict, deque
        children = defaultdict(list)
        indeg = defaultdict(int)
        nodes_all = set([f"W:{wf_id}"]) | set([f"J:{j}" for j in job_nodes]) | set([f"D:{d}" for d in data_nodes.keys()])
        for a, b, _k in edges:
            children[a].append(b)
            indeg[b] += 1
            nodes_all.add(a); nodes_all.add(b)
        roots = [n for n in nodes_all if indeg[n] == 0]
        level = {n: 0 for n in roots}
        dq = deque(roots)
        while dq:
            u = dq.popleft()
            for v in children.get(u, []):
                if v not in level:
                    level[v] = level[u] + 1
                    dq.append(v)
        for n in list(nodes_all):
            level.setdefault(n, 0)
        # Group
        by_lvl: Dict[int, List[str]] = {}
        for n, l in level.items():
            by_lvl.setdefault(l, []).append(n)
        max_lvl = max(by_lvl.keys()) if by_lvl else 0
        W = max(canvas.winfo_width(), 1000)
        H = max(canvas.winfo_height(), 560)
        node_pos: Dict[str, tuple] = {}
        mode = (layout_var.get() or "Hierarchy").lower()
        if mode == "hierarchy":
            vgap = max(100, H // (max_lvl + 2))
            for l in range(0, max_lvl + 1):
                row = by_lvl.get(l, [])
                count = max(1, len(row))
                hgap = max(120, W // (count + 1))
                y = (l + 1) * vgap
                for i, nid in enumerate(sorted(row)):
                    x = (i + 1) * hgap
                    node_pos[nid] = (x, y)
        elif mode == "left-right":
            hgap_lvl = max(200, W // (max_lvl + 2))
            # Levels along X, nodes stacked along Y
            for l in range(0, max_lvl + 1):
                row = by_lvl.get(l, [])
                count = max(1, len(row))
                vgap_row = max(80, H // (count + 1))
                x = (l + 1) * hgap_lvl
                for i, nid in enumerate(sorted(row)):
                    y = (i + 1) * vgap_row
                    node_pos[nid] = (x, y)
        else:  # circle
            import math as _math
            nodes_all_list = sorted(nodes_all)
            R = max(180, min(W, H) // 2 - 40)
            cx, cy = W // 2, H // 2
            n = max(1, len(nodes_all_list))
            for i, nid in enumerate(nodes_all_list):
                ang = 2 * _math.pi * (i / n)
                x = cx + R * _math.cos(ang)
                y = cy + R * _math.sin(ang)
                node_pos[nid] = (x, y)
        # Draw edges
        def _trim_line(x1: float, y1: float, x2: float, y2: float, m1: float = 26.0, m2: float = 32.0):
            # Move endpoints inward by m1 (source) and m2 (dest) so arrowheads aren't hidden under nodes
            import math as _math
            dx = x2 - x1
            dy = y2 - y1
            d = _math.hypot(dx, dy) or 1.0
            ux, uy = dx / d, dy / d
            return x1 + ux * m1, y1 + uy * m1, x2 - ux * m2, y2 - uy * m2
        arrowshape = (14, 18, 6)
        for a, b, kind in edges:
            mx1, my1 = node_pos.get(a, (50, 50))
            mx2, my2 = node_pos.get(b, (50, 50))
            x1, y1 = to_view(mx1, my1)
            x2, y2 = to_view(mx2, my2)
            x1, y1, x2, y2 = _trim_line(x1, y1, x2, y2)
            color = edge_default
            dash = None
            if kind == "put":
                color = edge_put
            elif kind == "get":
                color = edge_get
            elif kind == "flow":
                color = edge_flow; dash = (3, 3)
            elif kind == "root":
                color = edge_root
            canvas.create_line(x1, y1, x2, y2, arrow=tk.LAST, arrowshape=arrowshape, fill=color, dash=dash, width=2)
        # Draw nodes
        for nid, (x, y) in node_pos.items():
            vx, vy = to_view(x, y)
            if nid.startswith("W:"):
                # Workflow node as hexagon (no label text)
                r = 34
                pts = [
                    (vx, vy - r), (vx + 0.866*r, vy - 0.5*r), (vx + 0.866*r, vy + 0.5*r),
                    (vx, vy + r), (vx - 0.866*r, vy + 0.5*r), (vx - 0.866*r, vy - 0.5*r)
                ]
                canvas.create_polygon(*[c for p in pts for c in p], fill=wf_fill, outline=wf_outline, width=2)
                # intentionally no text label for workflow node
            elif nid.startswith("J:"):
                jid = nid[2:]
                # Determine job coloring based on terminal status
                sts = jobs_map.get(jid, [])
                up_statuses = [((s.getStatus() or "").upper()) for s in sts]
                
                # Check for terminal states
                terminal_states = {JobStatus.COMPLETE, JobStatus.FAILED, JobStatus.CANCELLED}
                failed_states = {JobStatus.FAILED, JobStatus.CANCELLED}
                success_states = {JobStatus.COMPLETE}
                
                has_terminal = any(status in terminal_states for status in up_statuses)
                has_failed = any(status in failed_states for status in up_statuses)
                has_success = any(status in success_states for status in up_statuses)
                has_only_info = (len(up_statuses) > 0 and all(s == JobStatus.INFO for s in up_statuses))
                
                # Color logic: 1) Failed = red, 2) Success/INFO-only = green, 3) In-flight = default
                if has_failed:
                    fill, outline = job_fill_failed, job_outline_failed
                elif has_success or has_only_info:
                    fill, outline = job_fill_success, job_outline_success
                else:
                    # In-flight or no meaningful status
                    fill, outline = job_fill_default, job_outline_default
                
                label = jid[:10] + ("…" if len(jid) > 10 else "")
                canvas.create_rectangle(vx-60, vy-18, vx+60, vy+18, fill=fill, outline=outline, width=2)
                canvas.create_text(vx, vy, text=label, fill=text)
            else:
                key = nid[2:]
                info = data_nodes.get(key, {})
                label = (info.get("path") or key)
                short = str(label)
                if len(short) > 22:
                    short = "…" + short[-21:]
                canvas.create_oval(vx-14, vy-14, vx+14, vy+14, fill=data_fill, outline=data_outline, width=2)
                canvas.create_text(vx, vy-24, text=short, anchor=tk.S, fill=text)
        # Optional: legend
        canvas.create_rectangle(10, 10, 320, 120, fill="#1c1c1c", outline="#2a2a2a")
        # Edge legend
        canvas.create_line(20, 34, 40, 34, arrow=tk.LAST, fill=edge_put, width=2)
        canvas.create_text(52, 34, text="put (Job → File)", fill=text, anchor=tk.W)
        canvas.create_line(20, 56, 40, 56, arrow=tk.LAST, fill=edge_get, width=2)
        canvas.create_text(52, 56, text="get (File → Job)", fill=text, anchor=tk.W)
        canvas.create_line(20, 78, 40, 78, arrow=tk.LAST, fill=edge_flow, dash=(3,3), width=2)
        canvas.create_text(52, 78, text="parent → child (Job)", fill=text, anchor=tk.W)
        canvas.create_line(20, 100, 40, 100, arrow=tk.LAST, fill=edge_root, width=2)
        canvas.create_text(52, 100, text="workflow → job", fill=text, anchor=tk.W)

        # Save for hit-testing
        nonlocal last_node_pos, last_edges, last_data_nodes
        last_node_pos = dict(node_pos)
        last_edges = list(edges)
        last_data_nodes = dict(data_nodes)

    # Pan/zoom handlers
    def _on_wheel(event):
        try:
            # Zoom toward cursor
            factor = 1.1 if (event.delta > 0 or getattr(event, 'num', 0) == 4) else 0.9
            old = state["scale"]
            new = max(0.3, min(3.5, old * factor))
            if new == old:
                return
            cx = canvas.canvasx(event.x)
            cy = canvas.canvasy(event.y)
            state["ox"] = cx - (cx - state["ox"]) * (new / old)
            state["oy"] = cy - (cy - state["oy"]) * (new / old)
            state["scale"] = new
            draw_graph()
        except tk.TclError:
            # Canvas has been destroyed, ignore the event
            pass

    def _on_press(e):
        state["drag"] = True
        state["sx"], state["sy"] = e.x, e.y
        state["moved"] = 0.0

    def _on_release(e):
        try:
            was_drag = state["drag"]
            state["drag"] = False
            # If this was a click (not a drag), perform hit-test
            if state.get("moved", 0.0) < 5.0 and was_drag:
                _handle_click(e.x, e.y)
        except tk.TclError:
            # Canvas has been destroyed, ignore the event
            pass

    def _on_motion(e):
        try:
            if not state["drag"]:
                return
            dx = e.x - state["sx"]; dy = e.y - state["sy"]
            state["sx"], state["sy"] = e.x, e.y
            state["ox"] += dx; state["oy"] += dy
            try:
                import math as _math
                state["moved"] += _math.hypot(dx, dy)
            except Exception:
                pass
            draw_graph()
        except tk.TclError:
            # Canvas has been destroyed, ignore the event
            pass

    def _handle_click(x: int, y: int):
        # Convert screen x,y; our node positions are in model and converted via to_view for centers
        # Find closest node whose shape contains the point
        # Check in order: workflow, jobs, data
        # Build small helper to test
        px, py = x, y
        # First try workflow node
        wf_center = last_node_pos.get(f"W:{workflow_id}")
        if wf_center:
            vx, vy = to_view(*wf_center)
            r = 36
            if abs(px - vx) <= r and abs(py - vy) <= r:
                # Open workflow dialog
                try:
                    open_workflow_dialog(gui, workflow_id)  # type: ignore[attr-defined]
                except Exception:
                    pass
                return
        # Jobs
        for nid, (mx, my) in last_node_pos.items():
            if not nid.startswith("J:"):
                continue
            vx, vy = to_view(mx, my)
            if (vx - 60) <= px <= (vx + 60) and (vy - 18) <= py <= (vy + 18):
                jid = nid[2:]
                # Switch to Jobs tab and populate details for this job
                try:
                    nb.select(tab_jobs)  # Switch to Jobs tab
                    populate_job_details(jid)
                    # Also select the job in the list if it exists
                    for item in tv_jobs.get_children():
                        vals = tv_jobs.item(item, "values")
                        if vals and vals[0] == jid:
                            tv_jobs.selection_set(item)
                            tv_jobs.see(item)
                            break
                except Exception:
                    pass
                return
        # Data nodes (circle radius ~14)
        for nid, (mx, my) in last_node_pos.items():
            if not nid.startswith("D:"):
                continue
            vx, vy = to_view(mx, my)
            dx = px - vx; dy = py - vy
            if (dx*dx + dy*dy) <= (16*16):
                # Find a connected job and open its Files panel
                job_id = None
                for a, b, k in last_edges:
                    if a == nid and b.startswith("J:"):
                        job_id = b[2:]; break
                    if b == nid and a.startswith("J:"):
                        job_id = a[2:]; break
                if job_id:
                    try:
                        gui.show_files(job_id)  # type: ignore[attr-defined]
                    except Exception:
                        pass
                else:
                    # If no job, do nothing for now
                    pass
                return

    # Redraw graph on resize
    def _on_cfg(_e):
        try:
            draw_graph()
        except Exception:
            pass
    canvas.bind("<Configure>", _on_cfg)
    canvas.bind("<ButtonPress-1>", _on_press)
    canvas.bind("<B1-Motion>", _on_motion)
    canvas.bind("<ButtonRelease-1>", _on_release)
    canvas.bind_all("<MouseWheel>", _on_wheel)
    canvas.bind_all("<Button-4>", _on_wheel)
    canvas.bind_all("<Button-5>", _on_wheel)
    layout_box.bind("<<ComboboxSelected>>", lambda _e: draw_graph())

    # Load job and metadata for other tabs
    jobs_map: Dict[str, List[JobStatus]] = {}
    metas: List[Metasheet] = []
    
    try:
        all_stats = lwfManager.getAllJobStatusesForWorkflow(workflow_id) or []
        for s in all_stats:
            jid = s.getJobContext().getJobId()
            jobs_map.setdefault(jid, []).append(s)
        for lst in jobs_map.values():
            lst.sort(key=lambda x: x.getEmitTime())
    except Exception:
        pass
        
    try:
        metas = lwfManager.find({"_workflowId": workflow_id}) or []
    except Exception:
        pass
    
    jobs_rebuild()
    data_rebuild()
    draw_graph()
    
    # If a specific job was requested, switch to Jobs tab and highlight it
    if highlight_job_id:
        try:
            nb.select(tab_jobs)  # Switch to Jobs tab
            # Find and select the job in the list
            for item in tv_jobs.get_children():
                vals = tv_jobs.item(item, "values")
                if vals and vals[0] == highlight_job_id:
                    tv_jobs.selection_set(item)
                    tv_jobs.see(item)
                    # Populate the details pane for this job
                    populate_job_details(highlight_job_id)
                    break
        except Exception:
            pass
