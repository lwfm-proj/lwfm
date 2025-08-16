from __future__ import annotations

import json
import threading
from typing import Any, Dict, List
import tkinter as tk
from tkinter import ttk

from lwfm.base.JobStatus import JobStatus
from lwfm.base.Metasheet import Metasheet  # type: ignore
from lwfm.midware.LwfManager import lwfManager


def open_workflow_dialog(gui: tk.Misc, workflow_id: str):
    """Open the workflow details dialog with Overview, Jobs, Data, Graph tabs."""
    win = tk.Toplevel(gui)
    win.title(f"Workflow {workflow_id}")
    win.geometry("1100x680")

    nb = ttk.Notebook(win)
    nb.pack(fill=tk.BOTH, expand=True)

    # --- Overview tab ---
    tab_overview = ttk.Frame(nb)
    nb.add(tab_overview, text="Overview")
    ov_top = ttk.Frame(tab_overview)
    ov_top.pack(side=tk.TOP, fill=tk.X, padx=8, pady=8)
    name_var = tk.StringVar(value="")
    desc_var = tk.StringVar(value="")
    jobs_count = tk.StringVar(value="0")
    data_count = tk.StringVar(value="0")
    wf_id_var = tk.StringVar(value=workflow_id)
    ttk.Label(ov_top, text="Name:").grid(row=0, column=0, sticky=tk.W)
    ttk.Label(ov_top, textvariable=name_var).grid(row=0, column=1, sticky=tk.W, padx=(4, 16))
    ttk.Label(ov_top, text="Description:").grid(row=1, column=0, sticky=tk.W)
    ttk.Label(ov_top, textvariable=desc_var).grid(row=1, column=1, sticky=tk.W, padx=(4, 16))
    ttk.Label(ov_top, text="Workflow ID:").grid(row=2, column=0, sticky=tk.W)
    ttk.Entry(ov_top, textvariable=wf_id_var, width=60, state="readonly").grid(row=2, column=1, sticky=tk.W, padx=(4, 16))
    ttk.Label(ov_top, text="Jobs:").grid(row=0, column=2, sticky=tk.W)
    ttk.Label(ov_top, textvariable=jobs_count).grid(row=0, column=3, sticky=tk.W, padx=(4, 16))
    ttk.Label(ov_top, text="Data items:").grid(row=1, column=2, sticky=tk.W)
    ttk.Label(ov_top, textvariable=data_count).grid(row=1, column=3, sticky=tk.W, padx=(4, 16))
    # Properties viewer
    ttk.Label(tab_overview, text="Properties:").pack(side=tk.TOP, anchor=tk.W, padx=8)
    props_text = tk.Text(tab_overview, height=10, wrap=tk.NONE)
    px = ttk.Scrollbar(tab_overview, orient=tk.HORIZONTAL, command=props_text.xview)
    py = ttk.Scrollbar(tab_overview, orient=tk.VERTICAL, command=props_text.yview)
    props_text.configure(xscrollcommand=px.set, yscrollcommand=py.set, state=tk.DISABLED)
    props_frame = ttk.Frame(tab_overview)
    props_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=(0, 8))
    props_text.pack(in_=props_frame, side=tk.LEFT, fill=tk.BOTH, expand=True)
    py.pack(in_=props_frame, side=tk.RIGHT, fill=tk.Y)
    px.pack(in_=props_frame, side=tk.BOTTOM, fill=tk.X)

    # --- Jobs tab ---
    tab_jobs = ttk.Frame(nb)
    nb.add(tab_jobs, text="Jobs")
    cols = ("job_id", "status", "native", "last_update", "files", "actions")
    tv_jobs = ttk.Treeview(tab_jobs, columns=cols, show="headings", selectmode="browse")
    for cid, w in zip(cols, (260, 120, 180, 180, 80, 120)):
        tv_jobs.heading(cid, text=cid.replace("_", " ").title())
        tv_jobs.column(cid, width=w, anchor=(tk.CENTER if cid in ("files", "actions") else tk.W))
    tv_jobs.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(8, 0), pady=8)
    ysb_jobs = ttk.Scrollbar(tab_jobs, orient=tk.VERTICAL, command=tv_jobs.yview)
    tv_jobs.configure(yscrollcommand=ysb_jobs.set)
    ysb_jobs.pack(side=tk.RIGHT, fill=tk.Y, padx=(0, 8), pady=8)
    try:
        tv_jobs.tag_configure("status-bad", foreground="#d32f2f")
        tv_jobs.tag_configure("status-good", foreground="#2e7d32")
    except Exception:
        pass

    details_jobs = tk.Text(tab_jobs, height=10, wrap=tk.WORD)
    details_jobs.pack(side=tk.BOTTOM, fill=tk.X, padx=8, pady=(0, 8))

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
    canvas = tk.Canvas(tab_graph, background="#ffffff")
    canvas.pack(fill=tk.BOTH, expand=True)

    jobs_map: Dict[str, List[JobStatus]] = {}
    metas: List[Metasheet] = []

    def jobs_rebuild():
        tv_jobs.delete(*tv_jobs.get_children())
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
            tv_jobs.insert("", tk.END, values=(job_id, stat, nat, last_time, "[ Files ]", "[ Status ]"), tags=tag)

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
                gui.show_files(job_id)  # type: ignore[attr-defined]
            except Exception:
                pass
        elif cols[idx] == "actions":
            try:
                gui.show_job_status(job_id, workflow_id)  # type: ignore[attr-defined]
            except Exception:
                pass
        else:
            ss = jobs_map.get(job_id, [])
            try:
                details_jobs.delete(1.0, tk.END)
                for s in ss:
                    t = s.getEmitTime().strftime('%Y-%m-%d %H:%M:%S')
                    details_jobs.insert(tk.END, f"[{t}] {s.getStatus()} {s.getNativeStatusStr() or ''}\n")
                details_jobs.see(tk.END)
            except Exception:
                pass
    tv_jobs.bind("<Button-1>", on_jobs_click)

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
        canvas.delete("all")
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
        # Add parent relations between jobs (dashed)
        for job_id, sts in jobs_map.items():
            if not sts:
                continue
            latest = max(sts, key=lambda s: s.getEmitTime())
            try:
                ctx = latest.getJobContext()
                parent = ctx.getParentJobId()
                if parent and parent in job_nodes:
                    edges.append((f"J:{parent}", f"J:{job_id}", "flow"))
            except Exception:
                pass
        # Layout levels alternating for bipartite portions
        from collections import defaultdict, deque
        children = defaultdict(list)
        indeg = defaultdict(int)
        nodes_all = set([f"J:{j}" for j in job_nodes]) | set([f"D:{d}" for d in data_nodes.keys()])
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
        vgap = max(100, H // (max_lvl + 2))
        node_pos: Dict[str, tuple] = {}
        for l in range(0, max_lvl + 1):
            row = by_lvl.get(l, [])
            count = max(1, len(row))
            hgap = max(120, W // (count + 1))
            y = (l + 1) * vgap
            for i, nid in enumerate(sorted(row)):
                x = (i + 1) * hgap
                node_pos[nid] = (x, y)
        # Draw edges
        for a, b, kind in edges:
            x1, y1 = node_pos.get(a, (50, 50))
            x2, y2 = node_pos.get(b, (50, 50))
            color = "#6c6c6c"
            dash = None
            if kind == "put":
                color = "#1565c0"  # blue
            elif kind == "get":
                color = "#ef6c00"  # orange
            elif kind == "flow":
                color = "#9e9e9e"; dash = (3, 3)
            canvas.create_line(x1, y1, x2, y2, arrow=tk.LAST, fill=color, dash=dash)
        # Draw nodes
        for nid, (x, y) in node_pos.items():
            if nid.startswith("J:"):
                jid = nid[2:]
                # color by status
                latest = None
                sts = jobs_map.get(jid, [])
                if sts:
                    latest = max(sts, key=lambda s: s.getEmitTime())
                stat = ((latest.getStatus() if latest else "") or "").upper()
                fill = "#f0f7ff"
                outline = "#3a7bd5"
                if stat in (JobStatus.FAILED, JobStatus.CANCELLED):
                    fill, outline = "#ffebee", "#d32f2f"
                elif stat == JobStatus.COMPLETE:
                    fill, outline = "#e8f5e9", "#2e7d32"
                label = jid[:10] + ("…" if len(jid) > 10 else "")
                canvas.create_rectangle(x-60, y-18, x+60, y+18, fill=fill, outline=outline, width=2)
                canvas.create_text(x, y, text=label)
            else:
                key = nid[2:]
                info = data_nodes.get(key, {})
                label = (info.get("path") or key)
                short = str(label)
                if len(short) > 22:
                    short = "…" + short[-21:]
                canvas.create_oval(x-14, y-14, x+14, y+14, fill="#fff8e1", outline="#ef6c00", width=2)
                canvas.create_text(x, y-24, text=short, anchor=tk.S)
        # Optional: legend
        canvas.create_rectangle(10, 10, 230, 82, fill="#ffffff", outline="#cccccc")
        canvas.create_line(20, 30, 40, 30, arrow=tk.LAST, fill="#1565c0")
        canvas.create_text(50, 30, text="put")
        canvas.create_line(20, 50, 40, 50, arrow=tk.LAST, fill="#ef6c00")
        canvas.create_text(50, 50, text="get")
        canvas.create_line(20, 70, 40, 70, arrow=tk.LAST, fill="#9e9e9e", dash=(3,3))
        canvas.create_text(75, 70, text="job flow")

    # Redraw graph on resize
    def _on_cfg(_e):
        try:
            draw_graph()
        except Exception:
            pass
    canvas.bind("<Configure>", _on_cfg)

    def worker():
        try:
            wf = lwfManager.getWorkflow(workflow_id)
            if wf:
                try:
                    name_var.set(wf.getName() or "")
                    desc_var.set(wf.getDescription() or "")
                    # properties
                    props = wf.getProps() or {}
                    props_text.configure(state=tk.NORMAL)
                    props_text.delete(1.0, tk.END)
                    props_text.insert(tk.END, json.dumps(props, indent=2, sort_keys=True))
                    props_text.configure(state=tk.DISABLED)
                except Exception:
                    pass
            all_stats = lwfManager.getAllJobStatusesForWorkflow(workflow_id) or []
            jmap: Dict[str, List[JobStatus]] = {}
            for s in all_stats:
                jid = s.getJobContext().getJobId()
                jmap.setdefault(jid, []).append(s)
            for lst in jmap.values():
                lst.sort(key=lambda x: x.getEmitTime())
            ms = lwfManager.find({"_workflowId": workflow_id}) or []
        except Exception:
            jmap = {}
            ms = []
        def done():
            nonlocal jobs_map, metas
            jobs_map = jmap
            metas = ms
            jobs_count.set(str(len(jobs_map)))
            data_count.set(str(len(metas)))
            jobs_rebuild()
            data_rebuild()
            draw_graph()
        try:
            gui.after(0, done)  # type: ignore[attr-defined]
        except Exception:
            pass

    threading.Thread(target=worker, daemon=True).start()
