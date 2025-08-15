"""
Tkinter-based GUI for lwfm job monitoring and control.
"""
# pylint: disable=invalid-name, broad-exception-caught
from __future__ import annotations

import json
import os
import threading
from datetime import datetime
from typing import Any, Dict, List, Optional

import tkinter as tk
from tkinter import messagebox, ttk

from lwfm.base.JobStatus import JobStatus
from lwfm.base.WorkflowEvent import WorkflowEvent, MetadataEvent, JobEvent, NotificationEvent
from lwfm.base.Metasheet import Metasheet  # type: ignore
from lwfm.midware.LwfManager import lwfManager
from lwfm.midware._impl.SiteConfig import SiteConfig


RefreshIntervalSecDefault = 5


class LwfmGui(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("lwfm")
        self.geometry("1100x650")

        # State
        self.refresh_interval: int = RefreshIntervalSecDefault
        self._building: bool = False
        self._shutdown: bool = False
        self._files_cache: Dict[str, Dict[str, Any]] = {}

        # Top toolbar
        top = ttk.Frame(self)
        top.pack(side=tk.TOP, fill=tk.X)
        ttk.Button(top, text="View Events", command=self.view_events).pack(side=tk.LEFT, padx=6, pady=6)
        ttk.Button(top, text="Search Data", command=self.view_metasheets).pack(side=tk.LEFT, padx=6, pady=6)
        ttk.Button(top, text="Server Log", command=self.view_server_log).pack(side=tk.LEFT, padx=6, pady=6)

        # Status bar with refresh interval control
        status = ttk.Frame(self)
        status.pack(side=tk.BOTTOM, fill=tk.X)
        self.status_var = tk.StringVar(value="")
        ttk.Label(status, textvariable=self.status_var).pack(side=tk.LEFT, padx=6)
        ttk.Label(status, text="Refresh (sec):").pack(side=tk.RIGHT, padx=(0, 4))
        self.interval_entry = ttk.Entry(status, width=6)
        self.interval_entry.insert(0, str(self.refresh_interval))
        self.interval_entry.pack(side=tk.RIGHT, padx=(0, 8))

        # Jobs table
        cols = ("job_id", "site", "status", "workflow_id", "last_update", "files")
        self.tree = ttk.Treeview(self, columns=cols, show="headings")
        for c, w in zip(cols, (260, 120, 120, 200, 180, 90)):
            self.tree.heading(c, text=c.replace("_", " ").title())
            self.tree.column(c, width=w, anchor=(tk.W if c != "files" else tk.CENTER))
        self.tree.pack(side=tk.TOP, fill=tk.BOTH, expand=True)
        self.tree.bind("<Button-1>", self.on_tree_click)
        self.tree.bind("<Motion>", self.on_tree_motion)

        # Filtering controls
        filt = ttk.Frame(self)
        filt.pack(side=tk.TOP, fill=tk.X)
        ttk.Label(filt, text="Filter:").pack(side=tk.LEFT)
        self.filter_entry = ttk.Entry(filt, width=24)
        self.filter_entry.pack(side=tk.LEFT, padx=(4, 8))
        ttk.Label(filt, text="Status:").pack(side=tk.LEFT)
        self.status_combo = ttk.Combobox(filt, values=["", JobStatus.INFO, JobStatus.PENDING, JobStatus.RUNNING, JobStatus.COMPLETE, JobStatus.CANCELLED, JobStatus.FAILED], width=14)
        self.status_combo.pack(side=tk.LEFT, padx=(4, 8))
        ttk.Label(filt, text="Time:").pack(side=tk.LEFT)
        self.time_combo = ttk.Combobox(filt, values=["All", "Last hour", "Last day", "Last week", "Last month"], width=14)
        self.time_combo.current(0)
        self.time_combo.pack(side=tk.LEFT, padx=(4, 8))

        # Simple data model helpers
        class _Model:
            def __init__(self):
                self.rows: List[Dict[str, Any]] = []
                self.sort_col = "last_update"
                self.sort_asc = False
                self.filter_text = ""
                self.filter_status = ""
                self.filter_time = "ALL"

            def apply_filters(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
                f = (self.filter_text or "").lower().strip()
                st = (self.filter_status or "").upper().strip()
                out = []
                for r in rows:
                    hay = " ".join([str(r.get(k, "")) for k in ("job_id", "site", "status", "workflow_id")]).lower()
                    if f and f not in hay:
                        continue
                    if st and (str(r.get("status", "")).upper().strip() != st):
                        continue
                    out.append(r)
                return out

            def apply_sort(self, rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
                key = self.sort_col
                rev = not self.sort_asc
                return sorted(rows, key=lambda rr: str(rr.get(key, "")), reverse=rev)

        self.model = _Model()

        # Kick off first refresh
        self.after(250, self.refresh_async)

    # ===== Metasheets search dialog =====
    def view_metasheets(self):
        """Open a panel to search metasheets with AND/OR/NOT and show results in a grouped tree."""
        win = tk.Toplevel(self)
        win.title("Search Metasheets")
        win.geometry("1100x650")

        # State
        all_sheets: List[Metasheet] = []
        field_names: List[str] = []
        group_fields: List[str] = []  # f1, f2, f3 order
        leaf_by_iid: Dict[str, Metasheet] = {}

        # Helpers
        def wildcard_to_regex(pat: str) -> str:
            import re as _re
            s = "" if pat is None else str(pat)
            return "".join([".*" if c == "*" else "." if c == "?" else _re.escape(c) for c in s])

        def load_all_fields_and_data():
            nonlocal all_sheets, field_names
            try:
                all_sheets = lwfManager.find({}) or []
            except Exception:
                all_sheets = []
            names = set()
            for ms in all_sheets:
                try:
                    for k in (ms.getProps() or {}).keys():
                        names.add(str(k))
                except Exception:
                    continue
            preferred = ["_workflowId", "_jobId", "_siteName", "_direction", "_localPath", "_siteObjPath", "_sheetId"]
            rest = sorted([n for n in names if n not in preferred])
            field_names[:] = [n for n in preferred if n in names] + rest

        def sheet_matches(ms: Metasheet, must_all: List[tuple], must_any: List[tuple], must_not: List[tuple], case_sensitive: bool) -> bool:
            import re as _re
            props = ms.getProps() or {}
            def getv(k: str) -> str:
                v = props.get(k)
                return "" if v is None else str(v)
            flags = 0 if case_sensitive else _re.IGNORECASE
            for k, regex in must_all:
                if not _re.search(regex, getv(k), flags=flags):
                    return False
            for k, regex in must_not:
                if _re.search(regex, getv(k), flags=flags):
                    return False
            if must_any:
                if not any(_re.search(regex, getv(k), flags=flags) for k, regex in must_any):
                    return False
            return True

        # Layout: query builder
        top = ttk.Frame(win)
        top.pack(side=tk.TOP, fill=tk.X, padx=8, pady=6)
        qframe = ttk.Frame(top)
        qframe.pack(side=tk.LEFT, fill=tk.X, expand=True)
        case_var = tk.BooleanVar(value=False)
        show_nulls_var = tk.BooleanVar(value=False)  # default OFF

        def make_clause_block(parent, title: str):
            frm = ttk.Labelframe(parent, text=title)
            frm.pack(side=tk.TOP, fill=tk.X, padx=(0, 10), pady=(0, 6))
            rows: List[Dict[str, Any]] = []

            def add_row():
                row = ttk.Frame(frm)
                row.pack(side=tk.TOP, fill=tk.X, pady=2)
                field_cb = ttk.Combobox(row, values=field_names, width=24)
                field_cb.pack(side=tk.LEFT, padx=(0, 6))
                val_ent = ttk.Entry(row, width=28)
                val_ent.pack(side=tk.LEFT, padx=(0, 6))
                def on_del():
                    try:
                        rows.remove(item)
                    except Exception:
                        pass
                    row.destroy()
                ttk.Button(row, text="-", width=2, command=on_del).pack(side=tk.LEFT)
                item = {"field": field_cb, "value": val_ent}
                rows.append(item)

            btns = ttk.Frame(frm)
            btns.pack(side=tk.TOP, anchor=tk.W)
            ttk.Button(btns, text="+ Add", command=add_row).pack(side=tk.LEFT, pady=2)
            add_row()
            return rows

        all_rows = make_clause_block(qframe, "Must match ALL of:")
        any_rows = make_clause_block(qframe, "Must match ANY of:")
        not_rows = make_clause_block(qframe, "Must NOT match:")

        # Options (right side)
        opts = ttk.Frame(top)
        opts.pack(side=tk.RIGHT, anchor=tk.N)
        ttk.Checkbutton(opts, text="Case sensitive", variable=case_var).pack(side=tk.TOP)
        ttk.Checkbutton(opts, text="Show nulls", variable=show_nulls_var).pack(side=tk.TOP)
        search_btn_opts = ttk.Button(opts, text="Search")
        search_btn_opts.pack(side=tk.TOP, pady=(6, 0))

        # Group-by selector
        grp = ttk.Labelframe(win, text="Group by (order) f1, f2, f3…")
        grp.pack(side=tk.TOP, fill=tk.X, padx=8, pady=(0, 6))
        grp_left = ttk.Frame(grp)
        grp_left.pack(side=tk.LEFT, padx=(0, 8))
        ttk.Label(grp_left, text="Available fields:").pack(anchor=tk.W)
        avail = tk.Listbox(grp_left, height=7, exportselection=False)
        avail.pack(side=tk.LEFT, fill=tk.X)
        grp_mid = ttk.Frame(grp)
        grp_mid.pack(side=tk.LEFT, padx=8)
        def add_field():
            sel = avail.curselection()
            if not sel:
                return
            name = avail.get(sel[0])
            if name not in group_fields:
                group_fields.append(name)
                chosen.insert(tk.END, name)
        def remove_field():
            sel = chosen.curselection()
            if not sel:
                return
            idx = sel[0]
            name = chosen.get(idx)
            try:
                group_fields.remove(name)
            except ValueError:
                pass
            chosen.delete(idx)
        def move_up():
            sel = chosen.curselection()
            if not sel:
                return
            idx = sel[0]
            if idx == 0:
                return
            name = chosen.get(idx)
            chosen.delete(idx)
            chosen.insert(idx-1, name)
            chosen.selection_set(idx-1)
            group_fields[:] = list(chosen.get(0, tk.END))
        def move_down():
            sel = chosen.curselection()
            if not sel:
                return
            idx = sel[0]
            if idx >= chosen.size()-1:
                return
            name = chosen.get(idx)
            chosen.delete(idx)
            chosen.insert(idx+1, name)
            chosen.selection_set(idx+1)
            group_fields[:] = list(chosen.get(0, tk.END))
        ttk.Button(grp_mid, text=">", width=3, command=add_field).pack(pady=2)
        ttk.Button(grp_mid, text="<", width=3, command=remove_field).pack(pady=2)
        ttk.Button(grp_mid, text="▲", width=3, command=move_up).pack(pady=2)
        ttk.Button(grp_mid, text="▼", width=3, command=move_down).pack(pady=2)
        grp_right = ttk.Frame(grp)
        grp_right.pack(side=tk.LEFT)
        ttk.Label(grp_right, text="Chosen (f1→f2→…):").pack(anchor=tk.W)
        chosen = tk.Listbox(grp_right, height=7, exportselection=False)
        chosen.pack(side=tk.LEFT, fill=tk.X)

        # Results toolbar + tree
        tree_toolbar = ttk.Frame(win)
        tree_toolbar.pack(side=tk.TOP, fill=tk.X, padx=8, pady=(6, 0))
        tree_frame = ttk.Frame(win)
        tree_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=6)
        tv = ttk.Treeview(tree_frame, columns=("type",), show="tree headings")
        tv.heading("type", text="Type")
        tv.column("type", width=100, anchor=tk.W)
        tv.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ysb = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=tv.yview)
        tv.configure(yscrollcommand=ysb.set)
        ysb.pack(side=tk.RIGHT, fill=tk.Y)
        # Slightly taller rows and zebra striping + readable selection (dark mode friendly)
        try:
            style = ttk.Style(tv)
            style_name = "LwfmSearch.Treeview"
            style.configure(style_name, rowheight=22)
            # Make selected row readable on dark themes
            style.map(style_name,
                      background=[('selected', '#eaf2ff')],
                      foreground=[('selected', '#000000')])
            tv.configure(style=style_name)
            # Detect dark vs light background and choose zebra colors
            def _is_dark_color(hex_or_name: str) -> bool:
                try:
                    rgb = tv.winfo_rgb(hex_or_name)
                    r, g, b = rgb[0] / 65535.0, rgb[1] / 65535.0, rgb[2] / 65535.0
                    luma = 0.2126 * r + 0.7152 * g + 0.0722 * b
                    return luma < 0.5
                except Exception:
                    return False
            base_bg = style.lookup("Treeview", "background") or style.lookup(style_name, "background") or "#ffffff"
            dark = _is_dark_color(base_bg)
            if dark:
                odd_bg, even_bg, fg = "#2b2b2b", "#1f1f1f", "#e6e6e6"
            else:
                odd_bg, even_bg, fg = "#f6f6f6", "#ffffff", "#000000"
            tv.tag_configure("odd", background=odd_bg, foreground=fg)
            tv.tag_configure("even", background=even_bg, foreground=fg)

            # Custom indicator images for better dark-mode visibility
            def _mk_triangle(direction: str, size: int, color: str, bg: str = ""):
                img = tk.PhotoImage(width=size, height=size)
                if bg:
                    img.put(bg, to=(0, 0, size, size))
                c = size // 2
                if direction == 'right':
                    for x in range(size):
                        half = max(0, (x * c) // max(1, size - 1))
                        y0, y1 = c - half, c + half
                        for y in range(max(0, y0), min(size, y1 + 1)):
                            img.put(color, to=(x, y))
                else:
                    for y in range(size):
                        half = max(0, (y * c) // max(1, size - 1))
                        x0, x1 = c - half, c + half
                        for x in range(max(0, x0), min(size, x1 + 1)):
                            img.put(color, to=(x, y))
                return img
            try:
                ind_color = fg if dark else "#333333"
                bg_color = even_bg
                collapsed_img = _mk_triangle('right', 10, ind_color, bg_color)
                expanded_img = _mk_triangle('down', 10, ind_color, bg_color)
                win.lwf_indicator_images = (collapsed_img, expanded_img)  # keep refs
                elem_name = 'LwfmIndicator'
                try:
                    style.element_create(elem_name, 'image', collapsed_img, ('open', expanded_img))
                except Exception:
                    pass
                try:
                    base_layout = style.layout('Treeview.Item')
                    def _replace_indicator(spec):
                        new = []
                        for child in spec:
                            elem = child[0]
                            opts_child = child[1]
                            if isinstance(elem, str) and elem.endswith('.indicator'):
                                new.append((elem_name, opts_child))
                            elif isinstance(opts_child, dict) and 'children' in opts_child:
                                opts_child = {**opts_child, 'children': _replace_indicator(opts_child['children'])}
                                new.append((elem, opts_child))
                            else:
                                new.append(child)
                        return new
                    style.layout('Treeview.Item', _replace_indicator(base_layout))
                except Exception:
                    pass
            except Exception:
                pass
        except Exception:
            pass

        # Expand/Collapse all
        def _open_all(item):
            try:
                tv.item(item, open=True)
                for ch in tv.get_children(item):
                    _open_all(ch)
            except Exception:
                pass
        def _close_all(item):
            try:
                for ch in tv.get_children(item):
                    _close_all(ch)
                tv.item(item, open=False)
            except Exception:
                pass
        def expand_all():
            for r in tv.get_children(""):
                _open_all(r)
        def collapse_all():
            for r in tv.get_children(""):
                _close_all(r)
        ttk.Button(tree_toolbar, text="Expand All", command=expand_all).pack(side=tk.LEFT)
        ttk.Button(tree_toolbar, text="Collapse All", command=collapse_all).pack(side=tk.LEFT, padx=(6,0))

        # Status and actions
        bottom = ttk.Frame(win)
        bottom.pack(side=tk.BOTTOM, fill=tk.X)
        status = tk.StringVar(value="")
        ttk.Label(bottom, textvariable=status).pack(side=tk.LEFT, padx=6)

        # Build tree from results
        def rebuild_tree(results: List[Metasheet]):
            tv.delete(*tv.get_children())
            leaf_by_iid.clear()
            if not results:
                status.set("No results")
                return
            row_index = 0
            def _ins(parent, text, values):
                nonlocal row_index
                tag = "odd" if (row_index % 2) else "even"
                row_index += 1
                return tv.insert(parent, tk.END, text=text, values=values, tags=(tag,))

            from collections import defaultdict
            def make_tree():
                return defaultdict(make_tree)
            nested = make_tree()
            buckets: Dict[tuple, List[Metasheet]] = {}
            def get_val(ms: Metasheet, key: str) -> str:
                try:
                    v = (ms.getProps() or {}).get(key)
                    return str(v) if v is not None else "(missing)"
                except Exception:
                    return "(missing)"
            for ms in results:
                cursor = nested
                path_vals = []
                for gf in group_fields:
                    val = get_val(ms, gf)
                    path_vals.append(val)
                    cursor = cursor[val]
                buckets.setdefault(tuple(path_vals), []).append(ms)

            def insert_children(parent, depth, prefix):
                if depth < len(group_fields):
                    node = nested
                    for p in prefix:
                        node = node[p]
                    for val in sorted(node.keys(), key=str):
                        label = f"📁 {group_fields[depth]}: {val}"
                        iid = _ins(parent, label, ("Folder",))
                        insert_children(iid, depth+1, prefix + [val])
                else:
                    items = buckets.get(tuple(prefix), [])
                    for ms in items:
                        label = (ms.getProps() or {}).get("_localPath") or (ms.getProps() or {}).get("_siteObjPath") or ms.getSheetId()
                        iid = _ins(parent, f"📄 {label}", ("File",))
                        leaf_by_iid[iid] = ms

            if group_fields:
                insert_children("", 0, [])
            else:
                for ms in results:
                    label = (ms.getProps() or {}).get("_localPath") or (ms.getProps() or {}).get("_siteObjPath") or ms.getSheetId()
                    iid = _ins("", f"📄 {label}", ("File",))
                    leaf_by_iid[iid] = ms

            status.set(f"Results: {len(results)} metasheets")

        def on_click(event):
            row_id = tv.identify_row(event.y)
            if not row_id:
                return
            ms = leaf_by_iid.get(row_id)
            if ms:
                try:
                    self._show_metasheet_window(ms)
                except Exception:
                    pass
        tv.bind("<Double-1>", on_click)

        def do_search():
            nonlocal group_fields
            def extract(rows: List[Dict[str, Any]]) -> List[tuple]:
                out = []
                for r in rows:
                    k = (r["field"].get() or "").strip()
                    v = (r["value"].get() or "").strip()
                    if not k or v == "":
                        continue
                    out.append((k, wildcard_to_regex(v)))
                return out
            must_all = extract(all_rows)
            must_any = extract(any_rows)
            must_not = extract(not_rows)
            case_sensitive = bool(case_var.get())

            if not field_names:
                load_all_fields_and_data()
                avail.delete(0, tk.END)
                for n in field_names:
                    avail.insert(tk.END, n)

            used_fields = set([k for (k, _) in (must_all + must_any + must_not)])
            results: List[Metasheet] = []
            for ms in all_sheets:
                try:
                    if used_fields and not bool(show_nulls_var.get()):
                        props = ms.getProps() or {}
                        if any((props.get(k) is None) or (isinstance(props.get(k), str) and props.get(k).strip() == "") for k in used_fields):
                            continue
                    if sheet_matches(ms, must_all, must_any, must_not, case_sensitive):
                        results.append(ms)
                except Exception:
                    continue
            if not group_fields:
                group_fields = ["_workflowId", "_jobId"]
                try:
                    chosen.delete(0, tk.END)
                    for gf in group_fields:
                        chosen.insert(tk.END, gf)
                except Exception:
                    pass
            rebuild_tree(results)

        # Wire the right-side Search button now that do_search exists
        try:
            search_btn_opts.configure(command=do_search)
        except Exception:
            pass

        # Buttons at bottom (search + close)
        ttk.Button(bottom, text="Search", command=do_search).pack(side=tk.RIGHT, padx=6, pady=6)
        ttk.Button(bottom, text="Close", command=win.destroy).pack(side=tk.RIGHT, padx=6, pady=6)

        # Allow Enter/Return to trigger Search
        try:
            win.bind("<Return>", lambda e: do_search())
        except Exception:
            pass

        # Bootstrap load of fields and update UI pickers
        def bootstrap_fields_async():
            def worker():
                load_all_fields_and_data()
                def done():
                    avail.delete(0, tk.END)
                    for n in field_names:
                        avail.insert(tk.END, n)
                    try:
                        for r in (all_rows + any_rows + not_rows):
                            cb = r.get("field")
                            if cb:
                                cb.configure(values=field_names)
                    except Exception:
                        pass
                try:
                    win.after(0, done)
                except Exception:
                    pass
            threading.Thread(target=worker, daemon=True).start()

        bootstrap_fields_async()

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
        # Color tags for status rows
        try:
            self.tree.tag_configure("status-bad", foreground="#d32f2f")   # failed/cancelled
            self.tree.tag_configure("status-good", foreground="#2e7d32")  # complete
            # info/other left default
        except Exception:
            pass
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

    def on_tree_click(self, event):
        try:
            region = self.tree.identify("region", event.x, event.y)
            if region != "cell":
                return
            row_id = self.tree.identify_row(event.y)
            col_id = self.tree.identify_column(event.x)
            if not row_id or not col_id:
                return
            idx = int(col_id.replace('#', '')) - 1
            cols = self.tree["columns"]
            if idx < 0 or idx >= len(cols):
                return
            colname = cols[idx]
            vals = self.tree.item(row_id, "values") or []
            if not vals:
                return
            job_id = vals[0] if len(vals) >= 1 else ""
            workflow_id = vals[3] if len(vals) >= 4 else ""
            if colname == "files":
                label = vals[-1] if vals else ""
                if label:
                    self.show_files(job_id)
            elif colname in ("job_id", "status"):
                if job_id:
                    self.show_job_status(job_id, workflow_id)
            elif colname == "workflow_id":
                if workflow_id:
                    self.view_workflow(workflow_id)
        except Exception:
            pass

    def on_tree_motion(self, event):
        try:
            region = self.tree.identify("region", event.x, event.y)
            if region != "cell":
                self.tree.configure(cursor="")
                return
            row_id = self.tree.identify_row(event.y)
            col_id = self.tree.identify_column(event.x)
            cursor = ""
            if row_id and col_id:
                idx = int(col_id.replace('#', '')) - 1
                cols = self.tree["columns"]
                if 0 <= idx < len(cols) and cols[idx] == "files":
                    vals = self.tree.item(row_id, "values") or []
                    label = vals[-1] if vals else ""
                    if label:
                        cursor = "hand2"
            self.tree.configure(cursor=cursor)
        except Exception:
            try:
                self.tree.configure(cursor="")
            except Exception:
                pass

    def show_job_status(self, job_id: str, _workflow_id: str):
        def worker():
            try:
                filtered = lwfManager.getAllStatus(job_id) or []
                filtered.sort(key=lambda s: s.getEmitTime().timestamp())
            except Exception:
                filtered = []
            self.after(0, lambda: self._show_status_window(job_id, filtered))
        threading.Thread(target=worker, daemon=True).start()

    def view_workflow(self, workflow_id: str):
        win = tk.Toplevel(self)
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
        props_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=(0,8))
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
        tv_jobs.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(8,0), pady=8)
        ysb_jobs = ttk.Scrollbar(tab_jobs, orient=tk.VERTICAL, command=tv_jobs.yview)
        tv_jobs.configure(yscrollcommand=ysb_jobs.set)
        ysb_jobs.pack(side=tk.RIGHT, fill=tk.Y, padx=(0,8), pady=8)
        try:
            tv_jobs.tag_configure("status-bad", foreground="#d32f2f")
            tv_jobs.tag_configure("status-good", foreground="#2e7d32")
        except Exception:
            pass

        details_jobs = tk.Text(tab_jobs, height=10, wrap=tk.WORD)
        details_jobs.pack(side=tk.BOTTOM, fill=tk.X, padx=8, pady=(0,8))

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
        tv_data.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(8,0), pady=8)
        ysb_data = ttk.Scrollbar(tab_data, orient=tk.VERTICAL, command=tv_data.yview)
        tv_data.configure(yscrollcommand=ysb_data.set)
        ysb_data.pack(side=tk.RIGHT, fill=tk.Y, padx=(0,8), pady=8)

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
                self.show_files(job_id)
            elif cols[idx] == "actions":
                self.show_job_status(job_id, workflow_id)
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
                self._show_metasheet_window(metas[idx])
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
            self.after(0, done)

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
