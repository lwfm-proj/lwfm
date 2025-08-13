"""
Tkinter-based GUI for lwfm job monitoring and control.
Features:
- Sortable, filterable, scrollable grid of non-terminal (running) jobs.
- Periodic refresh.
- Per-row actions: Cancel job, Show files (put/get metasheets).
- Time window filter (last hour/day/week/month/all).
"""
# pylint: disable=invalid-name, broad-exception-caught
import threading
import os
import time
import json
import math
import random
from datetime import datetime
from typing import List, Dict, Any

import tkinter as tk
from tkinter import ttk, messagebox

from lwfm.midware.LwfManager import lwfManager
from lwfm.midware._impl.SiteConfig import SiteConfig
from lwfm.base.JobStatus import JobStatus
from lwfm.base.Metasheet import Metasheet


RefreshIntervalSecDefault = 5


class JobsModel:
    def __init__(self):
        self.rows = []  # type: List[Dict[str, Any]]
        self.sort_by = "last_update"
        self.sort_asc = False
        self.filter_text = ""
        self.filter_status = "ALL"
        self.filter_time = "ALL"  # one of: ALL, 1H, 1D, 1W, 1M

    def apply_filters(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        text = (self.filter_text or "").lower()
        status_filter = self.filter_status
        out: List[Dict[str, Any]] = []
        now_ts = time.time()

        def within_window(ts: float) -> bool:
            if not ts:
                return False
            code = (self.filter_time or "ALL").upper()
            if code == "ALL":
                return True
            limits = {
                "1H": 60 * 60,
                "1D": 60 * 60 * 24,
                "1W": 60 * 60 * 24 * 7,
                "1M": 60 * 60 * 24 * 30,
            }
            window = limits.get(code)
            if window is None:
                return True
            return (now_ts - ts) <= window

        for r in rows:
            if status_filter and status_filter != "ALL" and r.get("status") != status_filter:
                continue
            if not within_window(r.get("last_update_ts") or 0):
                continue
            if text:
                hay = " ".join([str(r.get(k, "")) for k in ("job_id", "site", "status", "workflow_id")]).lower()
                if text not in hay:
                    continue
            out.append(r)
        return out

    def apply_sort(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        key = self.sort_by
        rev = not self.sort_asc
        if key == "last_update":
            return sorted(rows, key=lambda r: r.get("last_update_ts") or 0, reverse=rev)
        return sorted(rows, key=lambda r: str(r.get(key, "")), reverse=rev)


class LwfmGui(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("lwfm Jobs")
        self.geometry("1040x560")
        self.model = JobsModel()
        self.refresh_interval = int(os.environ.get("LWFM_GUI_REFRESH", str(RefreshIntervalSecDefault)))
        self._shutdown = False
        self._building = False
        # cache: job_id -> { 'ts': last_update_ts, 'has': bool }
        self._files_cache = {}
        self._setup_ui()
        self.after(200, self.refresh_async)

    def _setup_ui(self):
        # Controls frame
        ctrl = ttk.Frame(self)
        ctrl.pack(side=tk.TOP, fill=tk.X, padx=8, pady=6)

        ttk.Label(ctrl, text="Filter:").pack(side=tk.LEFT)
        self.filter_entry = ttk.Entry(ctrl, width=30)
        self.filter_entry.pack(side=tk.LEFT, padx=(4, 10))
        self.filter_entry.bind("<Return>", lambda e: self.rebuild_table())

        ttk.Label(ctrl, text="Status:").pack(side=tk.LEFT)
        self.status_combo = ttk.Combobox(
            ctrl,
            values=["ALL", "READY", "PENDING", "RUNNING", "INFO", "FINISHING"],
            width=12,
        )
        self.status_combo.set("ALL")
        self.status_combo.pack(side=tk.LEFT, padx=(4, 10))
        self.status_combo.bind("<<ComboboxSelected>>", lambda e: self.rebuild_table())

        ttk.Label(ctrl, text="Time:").pack(side=tk.LEFT)
        self.time_combo = ttk.Combobox(
            ctrl,
            values=["Last hour", "Last day", "Last week", "Last month", "All"],
            width=12,
        )
        self.time_combo.set("Last day")
        self.time_combo.pack(side=tk.LEFT, padx=(4, 10))
        self.time_combo.bind("<<ComboboxSelected>>", lambda e: self.rebuild_table())

        self.refresh_btn = ttk.Button(ctrl, text="Refresh", command=self.refresh_async)
        self.refresh_btn.pack(side=tk.LEFT)

        ttk.Label(ctrl, text="Every (s):").pack(side=tk.LEFT, padx=(10, 2))
        self.interval_entry = ttk.Entry(ctrl, width=5)
        self.interval_entry.insert(0, str(self.refresh_interval))
        self.interval_entry.pack(side=tk.LEFT)

        # Table frame containing tree and scrollbar
        table = ttk.Frame(self)
        table.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=6)

        # Treeview
        cols = ("job_id", "site", "status", "workflow_id", "last_update", "files")
        self.tree = ttk.Treeview(table, columns=cols, show="headings", selectmode="browse")
        headings = {
            "job_id": "Job ID",
            "site": "Site",
            "status": "Status",
            "workflow_id": "Workflow",
            "last_update": "Last Update",
            "files": "Files",
        }
        for cid in cols:
            self.tree.heading(cid, text=headings[cid], command=lambda c=cid: self.on_sort(c))
            # Make workflow smaller, time larger
            if cid == "workflow_id":
                width = 140
            elif cid == "last_update":
                width = 220
            elif cid == "job_id":
                width = 180
            else:
                width = 120
            if cid in ("files",):
                width = 100
                self.tree.column(cid, width=width, anchor=tk.CENTER)
            else:
                self.tree.column(cid, width=width, anchor=tk.W)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        # Color tags for statuses
        try:
            self.tree.tag_configure("status-bad", foreground="#d32f2f")    # red
            self.tree.tag_configure("status-good", foreground="#2e7d32")   # green
            self.tree.tag_configure("status-info", foreground="#1565c0")   # blue
        except Exception:
            pass
        self.tree.bind("<Button-1>", self.on_tree_click)
        self.tree.bind("<Motion>", self.on_tree_motion)

        # Scrollbar
        ysb = ttk.Scrollbar(table, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=ysb.set)
        ysb.pack(side=tk.RIGHT, fill=tk.Y)

        # Status bar
        self.status_var = tk.StringVar(value="Ready")
        sbar = ttk.Label(self, textvariable=self.status_var, anchor=tk.W)
        sbar.pack(side=tk.BOTTOM, fill=tk.X)

    def on_sort(self, column: str):
        if self.model.sort_by == column:
            self.model.sort_asc = not self.model.sort_asc
        else:
            self.model.sort_by = column
            self.model.sort_asc = True
        self.rebuild_table()

    def on_tree_click(self, event):
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            return
        row_id = self.tree.identify_row(event.y)
        col_id = self.tree.identify_column(event.x)
        if not row_id or not col_id:
            return
        col_index = int(col_id.replace('#', '')) - 1
        cols = ("job_id", "site", "status", "workflow_id", "last_update", "files")
        col_name = cols[col_index]
        row_vals = self.tree.item(row_id, "values")
        if col_name == "job_id":
            self.show_job_status(job_id=row_vals[0], _workflow_id=row_vals[3])
            return
        if col_name == "workflow_id":
            self.show_workflow_details(workflow_id=row_vals[3])
            return
        if col_name not in ("files",):
            return
        job_id = row_vals[0]
        if row_vals[5].strip():
            self.show_files(job_id)

    def on_tree_motion(self, event):
        region = self.tree.identify("region", event.x, event.y)
        if region != "cell":
            self.tree.configure(cursor="")
            return
        col_id = self.tree.identify_column(event.x)
        try:
            col_index = int(col_id.replace('#', '')) - 1
        except Exception:
            self.tree.configure(cursor="")
            return
        # Make Job ID (0), Workflow (3) and Files (5) show hand cursor
        if col_index in (0, 3, 5):
            if col_index == 5:
                row_id = self.tree.identify_row(event.y)
                if row_id:
                    vals = self.tree.item(row_id, "values")
                    if not vals[5].strip():
                        self.tree.configure(cursor="")
                        return
            self.tree.configure(cursor="hand2")
        else:
            self.tree.configure(cursor="")

    def show_workflow_details(self, workflow_id: str):
        def worker():
            try:
                wf = lwfManager.getWorkflow(workflow_id)
            except Exception:
                wf = None
            try:
                jobs = lwfManager.getJobStatusesForWorkflow(workflow_id) or []
            except Exception:
                jobs = []
            try:
                metas = lwfManager.find({"_workflowId": workflow_id}) or []
            except Exception:
                metas = []
            self.after(0, lambda: self._show_workflow_window(workflow_id, wf, jobs, metas))
        threading.Thread(target=worker, daemon=True).start()

    def _show_workflow_window(self, workflow_id: str, workflow, jobs: List[JobStatus], metas: List[Metasheet]):
        win = tk.Toplevel(self)
        title = f"Workflow {workflow_id}"
        try:
            name = workflow.getName() if workflow else None
            if name:
                title = f"Workflow {name} ({workflow_id})"
        except Exception:
            pass
        win.title(title)
        win.geometry("900x600")

        nb = ttk.Notebook(win)
        nb.pack(fill=tk.BOTH, expand=True)

        # Properties tab
        props_frame = ttk.Frame(nb)
        nb.add(props_frame, text="Properties")
        props_tree = ttk.Treeview(props_frame, columns=("key", "value"), show="headings")
        props_tree.heading("key", text="Key")
        props_tree.heading("value", text="Value")
        props_tree.column("key", width=200)
        props_tree.column("value", width=600)
        props_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ysb1 = ttk.Scrollbar(props_frame, orient=tk.VERTICAL, command=props_tree.yview)
        props_tree.configure(yscrollcommand=ysb1.set)
        ysb1.pack(side=tk.RIGHT, fill=tk.Y)

        # Populate properties
        try:
            kvs = []
            if workflow:
                kvs.append(("workflow_id", workflow.getWorkflowId()))
                kvs.append(("name", workflow.getName() or ""))
                kvs.append(("description", workflow.getDescription() or ""))
                for k, v in (workflow.getProps() or {}).items():
                    kvs.append((str(k), str(v)))
            else:
                kvs.append(("error", "Workflow not found"))
            for k, v in kvs:
                props_tree.insert("", tk.END, values=(k, v))
        except Exception:
            pass

        # Jobs tab
        jobs_frame = ttk.Frame(nb)
        nb.add(jobs_frame, text="Jobs")
        jobs_cols = ("job_id", "site", "status", "time")
        jobs_tree = ttk.Treeview(jobs_frame, columns=jobs_cols, show="headings")
        for cid, label, w in (
            ("job_id", "Job ID", 220),
            ("site", "Site", 120),
            ("status", "Status", 140),
            ("time", "Last Update", 220),
        ):
            jobs_tree.heading(cid, text=label)
            jobs_tree.column(cid, width=w)
        jobs_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ysb2 = ttk.Scrollbar(jobs_frame, orient=tk.VERTICAL, command=jobs_tree.yview)
        jobs_tree.configure(yscrollcommand=ysb2.set)
        ysb2.pack(side=tk.RIGHT, fill=tk.Y)

        # Populate jobs with latest statuses already provided
        try:
            for js in jobs:
                ctx = js.getJobContext()
                t = js.getEmitTime()
                time_str = datetime.fromtimestamp(t.timestamp()).strftime('%Y-%m-%d %H:%M:%S')
                jobs_tree.insert("", tk.END, values=(ctx.getJobId(), ctx.getSiteName(), js.getStatus(), time_str))
        except Exception:
            pass

        # Data tab
        data_frame = ttk.Frame(nb)
        nb.add(data_frame, text="Data")

        # Top split: list + scrollbar
        df_top = ttk.Frame(data_frame)
        df_top.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        data_cols = ("direction", "local", "siteobj")
        data_tree = ttk.Treeview(df_top, columns=data_cols, show="headings")
        data_tree.heading("direction", text="Dir")
        data_tree.heading("local", text="Local Path")
        data_tree.heading("siteobj", text="Site Object")
        data_tree.column("direction", width=80)
        data_tree.column("local", width=360)
        data_tree.column("siteobj", width=360)
        data_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ysb3 = ttk.Scrollbar(df_top, orient=tk.VERTICAL, command=data_tree.yview)
        data_tree.configure(yscrollcommand=ysb3.set)
        ysb3.pack(side=tk.RIGHT, fill=tk.Y)

        # Bottom: details text showing full metasheet props
        df_bottom = ttk.Frame(data_frame)
        df_bottom.pack(side=tk.BOTTOM, fill=tk.BOTH, expand=False)
        ttk.Label(df_bottom, text="Metasheet:").pack(side=tk.TOP, anchor=tk.W)
        dtext = tk.Text(df_bottom, height=10, wrap=tk.NONE)
        dxsb = ttk.Scrollbar(df_bottom, orient=tk.HORIZONTAL, command=dtext.xview)
        dysb = ttk.Scrollbar(df_bottom, orient=tk.VERTICAL, command=dtext.yview)
        dtext.configure(xscrollcommand=dxsb.set, yscrollcommand=dysb.set)
        dtext.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        dysb.pack(side=tk.RIGHT, fill=tk.Y)
        dxsb.pack(side=tk.BOTTOM, fill=tk.X)

        # Populate metasheets and map to details
        data_info_by_iid = {}
        try:
            for ms in metas:
                p = ms.getProps() or {}
                iid = data_tree.insert("", tk.END, values=(p.get("_direction", ""), p.get("_localPath", ""), p.get("_siteObjPath", "")))
                try:
                    data_info_by_iid[iid] = json.dumps(p, indent=2, sort_keys=True)
                except Exception:
                    data_info_by_iid[iid] = str(p)
        except Exception:
            pass

        def on_data_select(_event):
            sel = data_tree.selection()
            if not sel:
                return
            iid = sel[0]
            dtext.delete(1.0, tk.END)
            dtext.insert(tk.END, data_info_by_iid.get(iid, ""))

        data_tree.bind("<<TreeviewSelect>>", on_data_select)

        # Bottom controls
        btns = ttk.Frame(win)
        btns.pack(side=tk.BOTTOM, fill=tk.X)
        ttk.Button(btns, text="Close", command=win.destroy).pack(side=tk.RIGHT, padx=6, pady=6)

        # Graph tab
        graph_frame = ttk.Frame(nb)
        nb.add(graph_frame, text="Graph")
        canvas = tk.Canvas(graph_frame, background="#0f111a")
        canvas.pack(fill=tk.BOTH, expand=True)

        # Build graph data: nodes and edges
        wf_node_id = f"wf:{workflow_id}"
        nodes = {wf_node_id: {"type": "wf", "label": (workflow.getName() if workflow else "Workflow") or "Workflow"}}
        job_parent = {}
        job_nodes = set()
        for js in jobs:
            try:
                ctx = js.getJobContext()
                jid = ctx.getJobId()
                job_nodes.add(jid)
                job_parent[jid] = ctx.getParentJobId()
                nodes[f"job:{jid}"] = {"type": "job", "label": jid}
            except Exception:
                continue
        # Group metasheets by identity: files are the same if local path OR site path matches
        edges = set()
        groups: Dict[str, Dict[str, Any]] = {}  # groupKey -> { 'label': str, 'items': List[Metasheet] }
        index: Dict[str, str] = {}  # id string (local/site path) -> groupKey
        def pick_label(lp: str, sp: str) -> str:
            return (sp or lp or "data")
        for ms in metas:
            try:
                p = ms.getProps() or {}
                lp = (p.get("_localPath") or "").strip()
                sp = (p.get("_siteObjPath") or "").strip()
                g_lp = index.get(lp) if lp else None
                g_sp = index.get(sp) if sp else None
                if not g_lp and not g_sp:
                    gk = sp or lp or ms.getSheetId()
                    groups[gk] = {"label": pick_label(lp, sp), "items": [ms]}
                    if lp:
                        index[lp] = gk
                    if sp:
                        index[sp] = gk
                else:
                    gk = g_lp or g_sp or (sp or lp or ms.getSheetId())
                    if gk not in groups:
                        groups[gk] = {"label": pick_label(lp, sp), "items": []}
                    groups[gk]["items"].append(ms)
                    # If both map and differ, merge groups
                    if g_lp and g_sp and g_lp != g_sp:
                        keep = g_lp
                        drop = g_sp
                        # Move items from drop into keep
                        for item in groups.get(drop, {}).get("items", []):
                            groups[keep]["items"].append(item)
                        # Repoint indices
                        for id_str, gval in list(index.items()):
                            if gval == drop:
                                index[id_str] = keep
                        # Remove drop group
                        groups.pop(drop, None)
                        gk = keep
                    # Ensure both identifiers point to the chosen group
                    if lp:
                        index[lp] = gk
                    if sp:
                        index[sp] = gk
            except Exception:
                continue
        # Build data nodes and edges
        data_groups: Dict[str, List[Metasheet]] = {}
        for gk, meta in groups.items():
            label = meta.get("label") or "data"
            data_id = f"data:{gk}"
            nodes[data_id] = {"type": "data", "label": label}
            items: List[Metasheet] = meta.get("items", [])
            data_groups[gk] = items
            for ms in items:
                try:
                    pj = ms.getProps() or {}
                    jid = pj.get("_jobId")
                    if jid:
                        edges.add((f"job:{jid}", data_id))
                except Exception:
                    pass
        # Job edges
        for jid in job_nodes:
            parent = job_parent.get(jid)
            if parent:
                edges.add((f"job:{parent}", f"job:{jid}"))
            else:
                edges.add((wf_node_id, f"job:{jid}"))

        # Force-directed layout on the canvas
        def layout_and_draw():
            canvas.delete("all")
            w = max(200, canvas.winfo_width())
            h = max(200, canvas.winfo_height())
            cx, cy = w / 2, h / 2
            # Initial positions
            pos = {}
            vel = {}
            for nid in nodes.keys():
                if nid == wf_node_id:
                    pos[nid] = [cx, cy]
                    vel[nid] = [0.0, 0.0]
                else:
                    angle = random.random() * 2 * math.pi
                    r = min(w, h) * 0.35 * (0.6 + 0.4 * random.random())
                    pos[nid] = [cx + r * math.cos(angle), cy + r * math.sin(angle)]
                    vel[nid] = [0.0, 0.0]

            # Simple Fruchterman-Reingold style forces
            N = len(nodes)
            area = w * h
            k = math.sqrt(area / max(1, N))  # ideal distance
            iterations = min(200, 40 + 4 * N)
            for _ in range(iterations):
                disp = {nid: [0.0, 0.0] for nid in nodes.keys()}
                # Repulsion
                for a in nodes.keys():
                    ax, ay = pos[a]
                    for b in nodes.keys():
                        if a >= b:
                            continue
                        bx, by = pos[b]
                        dx = ax - bx
                        dy = ay - by
                        dist2 = dx * dx + dy * dy + 0.01
                        dist = math.sqrt(dist2)
                        force = (k * k) / dist
                        ux = dx / dist
                        uy = dy / dist
                        disp[a][0] += ux * force
                        disp[a][1] += uy * force
                        disp[b][0] -= ux * force
                        disp[b][1] -= uy * force
                # Attraction on edges
                for (u, v) in edges:
                    ux, uy = pos[u]
                    vx, vy = pos[v]
                    dx = ux - vx
                    dy = uy - vy
                    dist2 = dx * dx + dy * dy + 0.01
                    dist = math.sqrt(dist2)
                    force = (dist * dist) / k
                    nx = dx / dist
                    ny = dy / dist
                    disp[u][0] -= nx * force
                    disp[u][1] -= ny * force
                    disp[v][0] += nx * force
                    disp[v][1] += ny * force
                # Update positions
                for nid in nodes.keys():
                    if nid == wf_node_id:
                        pos[nid] = [cx, cy]
                        continue
                    dx, dy = disp[nid]
                    # limit step
                    step = 4.0
                    dl = math.sqrt(dx * dx + dy * dy) or 1.0
                    vx = (dx / dl) * min(step, dl)
                    vy = (dy / dl) * min(step, dl)
                    vel[nid][0] = (vel[nid][0] + vx) * 0.8
                    vel[nid][1] = (vel[nid][1] + vy) * 0.8
                    pos[nid][0] = min(w - 30, max(30, pos[nid][0] + vel[nid][0]))
                    pos[nid][1] = min(h - 30, max(30, pos[nid][1] + vel[nid][1]))

            # Draw edges
            edge_items = []
            for (u, v) in edges:
                x1, y1 = pos[u]
                x2, y2 = pos[v]
                edge_items.append(canvas.create_line(x1, y1, x2, y2, fill="#5f6b8a", width=1.0))

            # Draw nodes
            node_items = {}
            for nid, meta in nodes.items():
                x, y = pos[nid]
                t = meta.get("type")
                if t == "wf":
                    r = 18
                    fill = "#8e24aa"  # workflow purple
                elif t == "job":
                    r = 12
                    fill = "#1976d2"  # job blue
                else:
                    r = 10
                    fill = "#ff8f00"  # data orange
                item = canvas.create_oval(x - r, y - r, x + r, y + r, fill=fill, outline="#eaeaea", width=1.0)
                node_items[item] = nid
                # Optional subtle label near node (shortened)
                try:
                    label = meta.get("label") or ""
                    if len(label) > 18:
                        label = label[:8] + "…" + label[-8:]
                    canvas.create_text(x, y - (r + 10), text=label, fill="#d9e1f2", font=("TkDefaultFont", 9))
                except Exception:
                    pass

            # Click handling
            def on_click(event):
                # find closest node within hit radius
                x = event.x
                y = event.y
                hit = canvas.find_closest(x, y)
                if not hit:
                    return
                item = hit[0]
                nid = node_items.get(item)
                if not nid:
                    return
                try:
                    if nid.startswith("job:"):
                        jid = nid.split(":", 1)[1]
                        self.show_job_status(job_id=jid, _workflow_id=workflow_id)
                    elif nid.startswith("data:"):
                        key = nid.split(":", 1)[1]
                        group = data_groups.get(key)
                        if group:
                            # if multiple metasheets, show a chooser; else show the single sheet
                            if len(group) == 1:
                                self._show_metasheet_window(group[0])
                            else:
                                self._show_metasheet_group_window(label=nodes.get(nid, {}).get("label", key), sheets=group)
                    else:
                        # workflow node: no-op for now
                        pass
                except Exception:
                    pass

            canvas.bind("<Button-1>", on_click)

        # Draw now and on resize
        def on_resize(_event):
            layout_and_draw()
        canvas.bind("<Configure>", on_resize)
        # Initial draw
        win.after(50, layout_and_draw)

    def _show_metasheet_window(self, ms: Metasheet):
        win = tk.Toplevel(self)
        title = f"Data: {ms.getSheetId()}"
        try:
            p = ms.getProps() or {}
            title = p.get("_localPath") or p.get("_siteObjPath") or title
        except Exception:
            pass
        win.title(title)
        win.geometry("800x400")
        text = tk.Text(win, wrap=tk.NONE)
        xsb = ttk.Scrollbar(win, orient=tk.HORIZONTAL, command=text.xview)
        ysb = ttk.Scrollbar(win, orient=tk.VERTICAL, command=text.yview)
        text.configure(xscrollcommand=xsb.set, yscrollcommand=ysb.set)
        text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ysb.pack(side=tk.RIGHT, fill=tk.Y)
        xsb.pack(side=tk.BOTTOM, fill=tk.X)
        try:
            p = ms.getProps() or {}
            text.insert(tk.END, json.dumps(p, indent=2, sort_keys=True))
        except Exception as ex:
            text.insert(tk.END, f"Error rendering metasheet: {ex}")

    def _show_metasheet_group_window(self, label: str, sheets: List[Metasheet]):
        win = tk.Toplevel(self)
        win.title(f"Data: {label}")
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
        for ms in sheets:
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

    def set_status(self, text: str):
        self.status_var.set(text)

    def refresh_async(self):
        if self._building:
            return
        txt = self.interval_entry.get()
        if not txt.isdigit():
            self.refresh_interval = RefreshIntervalSecDefault
            self.interval_entry.delete(0, tk.END)
            self.interval_entry.insert(0, str(self.refresh_interval))
        else:
            self.refresh_interval = int(txt)

        self._building = True
        def worker():
            rows = self.fetch_job_rows()
            def done():
                self._building = False
                self.model.rows = rows
                self.rebuild_table()
                if not self._shutdown:
                    self.after(self.refresh_interval * 1000, self.refresh_async)
            self.after(0, done)
        threading.Thread(target=worker, daemon=True).start()

    def fetch_job_rows(self) -> List[Dict[str, Any]]:
        # Aggregate by job_id and keep only the latest status per job, while tracking
        # whether any non-INFO status has been seen (jobs with only INFO are treated as terminal for cancelability)
        latest: Dict[str, Dict[str, Any]] = {}
        workflows = lwfManager.getAllWorkflows() or []
        for wf in workflows:
            try:
                wf_id = wf.getWorkflowId()
                statuses = lwfManager.getJobStatusesForWorkflow(wf_id) or []
                for js in statuses:
                    ctx = js.getJobContext()
                    job_id = ctx.getJobId()
                    ts = js.getEmitTime().timestamp()
                    prev = latest.get(job_id)
                    # Track presence of any non-INFO status for this job
                    has_non_info = (prev.get("has_non_info") if prev else False) or (js.getStatus() != JobStatus.INFO)
                    if not prev or ts >= prev["last_update_ts"]:
                        latest[job_id] = {
                            "job_id": job_id,
                            "site": ctx.getSiteName(),
                            "status": js.getStatus(),
                            "workflow_id": ctx.getWorkflowId(),
                            "last_update": datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S'),
                            "last_update_ts": ts,
                            "has_non_info": has_non_info,
                        }
                    else:
                        # Update has_non_info on older record
                        if has_non_info and prev is not None:
                            prev["has_non_info"] = True
            except Exception:
                continue
        # Enrich with files and cancelable
        rows: List[Dict[str, Any]] = []
        for job_id, r in latest.items():
            status = r.get("status")
            is_terminal = status in (JobStatus.COMPLETE, JobStatus.FAILED, JobStatus.CANCELLED)
            # Jobs that only ever emitted INFO are treated as terminal for cancelability
            only_info = not bool(r.get("has_non_info"))
            has_files = self._has_files(job_id, r.get("last_update_ts") or 0)
            r["has_files"] = has_files
            r["cancelable"] = (not is_terminal) and (not only_info)
            rows.append(r)
        return rows

    def _has_files(self, job_id: str, last_ts: float) -> bool:
        try:
            cache = self._files_cache.get(job_id)
            if cache and cache.get("ts") == last_ts:
                return bool(cache.get("has"))
            metas: List[Metasheet] = lwfManager.find({"_jobId": job_id}) or []
            has = len(metas) > 0
            self._files_cache[job_id] = {"ts": last_ts, "has": has}
            return has
        except Exception:
            return False

    def rebuild_table(self):
        self.model.filter_text = self.filter_entry.get()
        self.model.filter_status = self.status_combo.get()
        time_label = (self.time_combo.get() or "All").lower()
        mapping = {
            "last hour": "1H",
            "last day": "1D",
            "last week": "1W",
            "last month": "1M",
            "all": "ALL",
        }
        self.model.filter_time = mapping.get(time_label, "ALL")

        rows = self.model.apply_filters(self.model.rows)
        rows = self.model.apply_sort(rows)

        for iid in self.tree.get_children():
            self.tree.delete(iid)
        for r in rows:
            files_label = "[ Files ]" if r.get("has_files") else ""
            # Determine tag by status
            stat = (r.get("status", "") or "").upper()
            if stat in (JobStatus.FAILED, JobStatus.CANCELLED):
                row_tag = "status-bad"
            elif stat == JobStatus.COMPLETE:
                row_tag = "status-good"
            elif stat == JobStatus.INFO:
                row_tag = "status-info"
            else:
                row_tag = ""
            self.tree.insert("", tk.END, values=(
                r.get("job_id", ""),
                r.get("site", ""),
                r.get("status", ""),
                r.get("workflow_id", ""),
                r.get("last_update", ""),
                files_label,
            ), tags=((row_tag,) if row_tag else ()))
        self.set_status(f"Jobs: {len(rows)} (refreshed {datetime.now().strftime('%H:%M:%S')})")

    def show_job_status(self, job_id: str, _workflow_id: str):
        def worker():
            try:
                # Fetch full history for this job directly
                filtered = lwfManager.getAllStatus(job_id) or []
                filtered.sort(key=lambda s: s.getEmitTime().timestamp())
            except Exception:
                filtered = []
            self.after(0, lambda: self._show_status_window(job_id, filtered))
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


def main():
    app = LwfmGui()
    app.mainloop()
