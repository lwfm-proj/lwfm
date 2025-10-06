from __future__ import annotations

from typing import Any, Dict, List
import os
import tkinter as tk
from tkinter import ttk

from lwfm.base.Metasheet import Metasheet  # type: ignore
from lwfm.midware.LwfManager import lwfManager


def open_metasheets_dialog(parent: tk.Misc):
    """Open the Search Metasheets dialog (was LwfmGui.view_metasheets)."""
    win = tk.Toplevel(parent)
    win.title("Search Metasheets")
    win.geometry("1200x800")
    try:
        win.minsize(1100, 720)
    except Exception:
        pass

    # Local state
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
            field_cb = ttk.Combobox(row, values=field_names, width=24, state="readonly")
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

    rows_all = make_clause_block(qframe, "Must contain ALL")
    rows_any = make_clause_block(qframe, "Must contain ANY")
    rows_not = make_clause_block(qframe, "Must NOT contain")

    opts = ttk.Frame(top)
    opts.pack(side=tk.LEFT, padx=6)
    ttk.Checkbutton(opts, text="Case sensitive", variable=case_var).pack(side=tk.TOP, anchor=tk.W)
    ttk.Checkbutton(opts, text="Show nulls", variable=show_nulls_var).pack(side=tk.TOP, anchor=tk.W)

    # Field display order builder (group-by fields)
    grp = ttk.Labelframe(win, text="Field display / grouping order")
    grp.pack(side=tk.TOP, fill=tk.X, padx=8, pady=(0, 6))

    # Available (all) fields list
    listbox = tk.Listbox(grp, selectmode=tk.SINGLE, exportselection=False, height=8)
    listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(6, 6), pady=6)

    # Controls
    btns = ttk.Frame(grp)
    btns.pack(side=tk.LEFT, padx=6, pady=6)

    def add_field():
        sel = listbox.curselection()
        if not sel:
            return
        name = listbox.get(sel[0])
        if name in group_fields:
            return
        group_fields.append(name)
        sel_listbox.insert(tk.END, name)
        rebuild_tree([])

    def remove_field():
        sel = sel_listbox.curselection()
        if not sel:
            return
        idx = sel[0]
        if 0 <= idx < len(group_fields):
            group_fields.pop(idx)
            sel_listbox.delete(idx)
            rebuild_tree([])

    def move_up():
        sel = sel_listbox.curselection()
        if not sel:
            return
        idx = sel[0]
        if idx <= 0:
            return
        group_fields[idx-1], group_fields[idx] = group_fields[idx], group_fields[idx-1]
        # Update UI
        name = sel_listbox.get(idx)
        sel_listbox.delete(idx)
        sel_listbox.insert(idx-1, name)
        sel_listbox.selection_clear(0, tk.END)
        sel_listbox.selection_set(idx-1)
        rebuild_tree([])

    def move_down():
        sel = sel_listbox.curselection()
        if not sel:
            return
        idx = sel[0]
        if idx >= len(group_fields) - 1:
            return
        group_fields[idx+1], group_fields[idx] = group_fields[idx], group_fields[idx+1]
        # Update UI
        name = sel_listbox.get(idx)
        sel_listbox.delete(idx)
        sel_listbox.insert(idx+1, name)
        sel_listbox.selection_clear(0, tk.END)
        sel_listbox.selection_set(idx+1)
        rebuild_tree([])

    ttk.Button(btns, text="Add", command=add_field).pack(side=tk.TOP, fill=tk.X, pady=(6,2))
    ttk.Button(btns, text="Remove", command=remove_field).pack(side=tk.TOP, fill=tk.X, pady=2)
    ttk.Button(btns, text="Up", command=move_up).pack(side=tk.TOP, fill=tk.X, pady=2)
    ttk.Button(btns, text="Down", command=move_down).pack(side=tk.TOP, fill=tk.X, pady=(2,6))

    # Selected (ordered) fields list
    sel_listbox = tk.Listbox(grp, selectmode=tk.SINGLE, exportselection=False, height=8)
    sel_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(6, 6), pady=6)

    # Results tree
    res_frame = ttk.Frame(win)
    res_frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=6)
    cols = ("field", "value", "count")
    tv = ttk.Treeview(res_frame, columns=cols, show="tree headings")
    tv.heading("field", text="Field")
    tv.heading("value", text="Value")
    tv.heading("count", text="#")
    tv.column("field", width=200)
    tv.column("value", width=560)
    tv.column("count", width=80, anchor=tk.E)
    tv.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
    ysb = ttk.Scrollbar(res_frame, orient=tk.VERTICAL, command=tv.yview)
    tv.configure(yscrollcommand=ysb.set)
    ysb.pack(side=tk.RIGHT, fill=tk.Y)

    # Dark-mode-friendly indicators (placeholder)
    def _is_dark_color(hex_or_name: str) -> bool:
        try:
            if hex_or_name.startswith('#') and len(hex_or_name) == 7:
                r = int(hex_or_name[1:3], 16)
                g = int(hex_or_name[3:5], 16)
                b = int(hex_or_name[5:7], 16)
                return (0.2126*r + 0.7152*g + 0.0722*b) < 128
        except Exception:
            pass
        return False

    try:
        style = ttk.Style(tv)
        bg = style.lookup("Treeview", "background") or tv.cget("background") or "#ffffff"
        _ = _is_dark_color(bg)
    except Exception:
        pass

    def _open_all(item):
        tv.item(item, open=True)
        for ch in tv.get_children(item):
            _open_all(ch)

    def _close_all(item):
        tv.item(item, open=False)
        for ch in tv.get_children(item):
            _close_all(ch)

    def expand_all():
        for it in tv.get_children(""):
            _open_all(it)

    def collapse_all():
        for it in tv.get_children(""):
            _close_all(it)

    toolbar = ttk.Frame(win)
    toolbar.pack(side=tk.TOP, fill=tk.X, padx=8)
    ttk.Button(toolbar, text="Expand all", command=expand_all).pack(side=tk.LEFT)
    ttk.Button(toolbar, text="Collapse all", command=collapse_all).pack(side=tk.LEFT, padx=(6,0))

    def rebuild_tree(results: List[Metasheet]):
        tv.delete(*tv.get_children())
        leaf_by_iid.clear()

        def _ins(parent, text, values):
            return tv.insert(parent, tk.END, text=text, values=values)

        def make_tree():
            # Ensure default grouping fields and sync UI selection list
            if not group_fields:
                default_fields = ["_workflowId", "_jobId"]
                for f in default_fields:
                    if f not in group_fields:
                        group_fields.append(f)
                sel_listbox.delete(0, tk.END)
                for f in group_fields:
                    sel_listbox.insert(tk.END, f)

            # Header row
            _ins("", "", ("Field", "Value", "Count"))

            fields = list(group_fields)
            show_nulls = bool(show_nulls_var.get())

            def get_raw(ms: Metasheet, key: str):
                try:
                    return (ms.getProps() or {}).get(key)
                except Exception:
                    return None

            def label_for(v) -> str:
                if v is None or str(v).strip() == "":
                    return "None"
                return str(v)

            def build_level(parent, items: List[Metasheet], depth: int):
                if depth >= len(fields):
                    # Render leaves (files)
                    for ms in items:
                        p = ms.getProps() or {}
                        leaf_name = os.path.basename(p.get("_localPath", "") or "")
                        if not leaf_name:
                            leaf_name = str(p.get("_sheetId", ""))
                        leaf = _ins(parent, f"📄 {leaf_name}", (p.get("_sheetId", ""), p.get("_localPath", ""), 1))
                        leaf_by_iid[leaf] = ms
                    return

                field = fields[depth]
                # Group by this field
                from collections import defaultdict as _dd
                buckets: Dict[str, List[Metasheet]] = _dd(list)
                for ms in items:
                    v = get_raw(ms, field)
                    if (v is None or str(v).strip() == "") and not show_nulls:
                        # Skip null/empty groups if Show nulls is OFF
                        continue
                    buckets[label_for(v)].append(ms)

                # Insert child folders sorted by label
                for k in sorted(buckets.keys(), key=lambda s: (s == "None", s)):
                    lst = buckets[k]
                    node = _ins(parent, f"📁 {field}: {k}", (field, k, len(lst)))
                    build_level(node, lst, depth + 1)

            # Start at root with all results
            build_level("", results, 0)

        make_tree()

    def on_click(event):
        sel = tv.selection()
        if not sel:
            return
        iid = sel[0]
        ms = leaf_by_iid.get(iid)
        if ms:
            try:
                parent._show_metasheet_window(ms)  # type: ignore[attr-defined]
            except Exception:
                pass

    tv.bind("<Double-1>", on_click)

    def do_search():
        def extract(rows: List[Dict[str, Any]]) -> List[tuple]:
            out = []
            for r in rows:
                k = r["field"].get()
                v = r["value"].get()
                if not k or v is None:
                    continue
                out.append((k, wildcard_to_regex(v)))
            return out

        must_all = extract(rows_all)
        must_any = extract(rows_any)
        must_not = extract(rows_not)
        case_sensitive = bool(case_var.get())

        try:
            results = []
            for ms in all_sheets:
                if sheet_matches(ms, must_all, must_any, must_not, case_sensitive):
                    props = ms.getProps() or {}
                    if not show_nulls_var.get():
                        if all((v is None or str(v).strip() == "") for v in props.values()):
                            continue
                    results.append(ms)
        except Exception:
            results = []
        rebuild_tree(results)

    btns_q = ttk.Frame(win)
    btns_q.pack(side=tk.TOP, fill=tk.X, padx=8, pady=4)
    ttk.Button(btns_q, text="Search", command=do_search).pack(side=tk.LEFT)
    ttk.Button(btns_q, text="Close", command=win.destroy).pack(side=tk.RIGHT)

    # Helper to update all clause comboboxes with latest field names
    def _update_clause_field_choices(names: List[str]):
        try:
            for r in rows_all:
                r["field"]["values"] = names
            for r in rows_any:
                r["field"]["values"] = names
            for r in rows_not:
                r["field"]["values"] = names
        except Exception:
            pass

    def bootstrap_fields_async():
        def worker():
            try:
                load_all_fields_and_data()
                names = list(field_names)
            except Exception:
                names = []
            def done():
                listbox.delete(0, tk.END)
                for n in names:
                    listbox.insert(tk.END, n)
                _update_clause_field_choices(names)
                # Initialize selected list defaults if empty
                if not group_fields:
                    group_fields.extend(["_workflowId", "_jobId"])
                sel_listbox.delete(0, tk.END)
                for f in group_fields:
                    if f in names:
                        sel_listbox.insert(tk.END, f)
            parent.after(0, done)
        import threading as _t
        _t.Thread(target=worker, daemon=True).start()

    bootstrap_fields_async()
