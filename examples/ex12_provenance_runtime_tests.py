"""
Provenance and runtime trace tests (example-style).

This example verifies:
- Job parent-child relationships are recorded (triggered job has correct parentJobId).
- Data put/get are notated and attributed to the correct workflow and job context.
- The workflow dump contains jobs and metasheets with expected properties.

It prints a compact PASS/FAIL report and exits non-zero if any assertion fails.
Requires the middleware service to be running.
"""

import os
import sys
from typing import List, Optional, Dict, Any, cast

from lwfm.midware.LwfManager import lwfManager
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus
from lwfm.base.JobContext import JobContext
from lwfm.base.Workflow import Workflow
from lwfm.base.WorkflowEvent import JobEvent


def fail(msg: str) -> None:
    print(f"FAIL: {msg}")
    sys.exit(1)


def ensure_midware() -> None:
    if not lwfManager.isMidwareRunning():
        fail("Middleware service is not running. Start it and re-run this test.")


def assert_eq(actual, expected, msg_prefix: str) -> None:
    if actual != expected:
        fail(f"{msg_prefix}: expected {expected!r}, got {actual!r}")


def main() -> None:
    ensure_midware()

    # 1) Create a workflow
    wf = Workflow("provenance runtime tests")
    wf = lwfManager.putWorkflow(wf)
    if wf is None:
        fail("putWorkflow returned None")
    wf = cast(Workflow, wf)
    wf_id = wf.getWorkflowId()
    print(f"Created workflow: {wf_id}")

    # 2) Submit a root job and wait
    site = lwfManager.getSite("local")
    root_cmd = "echo ROOT_TEST > /tmp/lwfm_prov_root.txt"
    root_job_defn = JobDefn(root_cmd)
    root_status = site.getRunDriver().submit(root_job_defn, wf)
    root_status = lwfManager.wait(root_status.getJobId())
    if root_status is None or not root_status.isTerminal():
        fail("Root job did not reach a terminal state")
    root_job_id = root_status.getJobId()
    print(f"Root job complete: {root_job_id}")

    # 3) Trigger a child job that runs after root completes
    child_cmd = "echo CHILD_TEST > /tmp/lwfm_prov_child.txt"
    child_job_defn = JobDefn(child_cmd)
    child_init = lwfManager.setEvent(
        # When root job is COMPLETE, run a new job on local
        # The system should set parentJobId on the child.
        JobEvent(root_job_id, JobStatus.COMPLETE, child_job_defn, "local")
    )
    if child_init is None:
        fail("Setting child event returned None")
    child_init = cast(JobStatus, child_init)
    child_status = lwfManager.wait(child_init.getJobId())
    if child_status is None or not child_status.isTerminal():
        fail("Child job did not reach a terminal state")
    child_job_id = child_status.getJobId()
    print(f"Child job complete: {child_job_id}")

    # 4) Perform a repo.put attributed to the root job
    temp_dir = "/tmp"
    local_file1 = os.path.join(temp_dir, f"prov_put1_{child_job_id}.txt")
    with open(local_file1, "w", encoding="utf-8") as f:
        f.write("data for put1")
    # Destination must differ from source; use a distinct filename
    repo_obj_path1 = os.path.join(temp_dir, f"prov_put1_site_{child_job_id}.txt")
    ms1 = site.getRepoDriver().put(local_file1, repo_obj_path1, root_status.getJobContext(),
                                   {"case": "put1", "test": "provenance"})
    if ms1 is None:
        fail("Repo put1 metasheet is None")

    # 5) Perform a notated get attributed to the workflow
    #    (this will produce a metasheet with direction 'get' and workflow id)
    get_dest = os.path.join(temp_dir, f"prov_get1_{child_job_id}.txt")
    # Prepare a source file under /tmp to act as the site object and then perform get
    site_src = os.path.join(temp_dir, f"prov_site_src_{child_job_id}.txt")
    with open(site_src, "w", encoding="utf-8") as f:
        f.write("data for get1")
    # Perform a get and include metadata directly; attribute to the workflow via context
    wf_ctx = JobContext()
    wf_ctx.setWorkflowId(wf_id)
    get_props = {"case": "get1", "test": "provenance"}
    got_path = site.getRepoDriver().get(site_src, get_dest, wf_ctx, get_props)
    if got_path is None:
        fail("repo.get returned None")

    # 6) Dump workflow and verify structure and metadata
    dump = lwfManager.dumpWorkflow(wf_id)
    if dump is None:
        fail("dumpWorkflow returned None")

    dump = cast(Dict[str, Any], dump)
    jobs = dump.get("jobs") or []
    metasheets: List[Any] = dump.get("metasheets") or []

    # 6a) Verify parent-child relationship: find the child and confirm parent is root
    def latest_parent(job_obj) -> Optional[str]:
        # Prefer explicit parent if present in JobStatus; else attempt via JobContext
        pid = job_obj.getParentJobId() if hasattr(job_obj, "getParentJobId") else None
        if pid:
            return pid
        if hasattr(job_obj, "getJobContext") and job_obj.getJobContext() is not None:
            ctx = job_obj.getJobContext()
            return ctx.getParentJobId() if hasattr(ctx, "getParentJobId") else None
        return None

    found_link = False
    for js in jobs:
        jid = js.getJobId() if hasattr(js, "getJobId") else None
        if jid == child_job_id:
            pid = latest_parent(js)
            if pid == root_job_id:
                found_link = True
                break
    if not found_link:
        fail("Child job does not reference root job as parent")

    # 6b) Verify metasheets: workflowId is correct; put1 sheet attributed to root job
    if not metasheets:
        fail("No metasheets found for workflow dump")

    def props(ms):
        return ms.getProps() if hasattr(ms, "getProps") else {}

    def job_id_of(ms):
        return ms.getJobId() if hasattr(ms, "getJobId") else None

    # Expect at least one 'put' metasheet for repo_obj_name1 with _jobId == root_job_id
    ms_put1 = None
    for ms in metasheets:
        p = props(ms)
        # Match on props written in _notate(): _direction and _siteObjPath
        if p.get("_direction") == "put" and p.get("_siteObjPath") == repo_obj_path1:
            ms_put1 = ms
            break
    if ms_put1 is None:
        fail("Did not find expected 'put' metasheet for repo put1")
    if props(ms_put1).get("_workflowId") != wf_id:
        fail("'put' metasheet does not carry correct _workflowId")
    if job_id_of(ms_put1) != root_job_id:
        fail("'put' metasheet is not attributed to the root job (_jobId mismatch)")

    # Expect at least one 'get' metasheet for get_dest with correct workflow id
    ms_get1 = None
    for ms in metasheets:
        p = props(ms)
        # For notateGet, _localPath is set to the destination path
        if p.get("_direction") == "get" and p.get("_localPath") == get_dest:
            ms_get1 = ms
            break
    if ms_get1 is None:
        fail("Did not find expected 'get' metasheet for notateGet")
    if props(ms_get1).get("_workflowId") != wf_id:
        fail("'get' metasheet does not carry correct _workflowId")

    # 6d) Visualize jobs with parent-child and workflow IDs
    print("\n-- Job Tree (with workflow ids) --")
    # Build maps
    job_info: Dict[str, Dict[str, Any]] = {}
    children: Dict[str, list] = {}

    def parent_of(js) -> Optional[str]:
        pid = js.getParentJobId() if hasattr(js, "getParentJobId") else None
        if pid:
            return pid
        if hasattr(js, "getJobContext") and js.getJobContext() is not None:
            ctx = js.getJobContext()
            return ctx.getParentJobId() if hasattr(ctx, "getParentJobId") else None
        return None

    def wf_of(js) -> Optional[str]:
        if hasattr(js, "getJobContext") and js.getJobContext() is not None:
            return js.getJobContext().getWorkflowId()
        return None

    def status_icon_of(js) -> str:
        st = js.getStatus() if hasattr(js, "getStatus") else ""
        if st == JobStatus.COMPLETE:
            return "✅"
        if st == JobStatus.INFO:
            return "ℹ️"
        if st in (JobStatus.CANCELLED, JobStatus.FAILED):
            return "❌"
        return "⏳"

    for js in jobs:
        jid = js.getJobId() if hasattr(js, "getJobId") else None
        if not jid:
            continue
        par = parent_of(js)
        wfid = wf_of(js)
        job_info[jid] = {"parent": par, "wfid": wfid, "status": getattr(js, "getStatus", lambda: "")()}
        if par:
            children.setdefault(par, []).append(jid)

    roots = [jid for jid, info in job_info.items() if not info["parent"] or info["parent"] not in job_info]

    def print_job(jid: str, prefix: str, is_last: bool) -> None:
        icon = status_icon_of(next(js for js in jobs if hasattr(js, "getJobId") and js.getJobId() == jid))
        conn = "└── " if is_last else "├── "
        info = job_info.get(jid, {})
        print(f"{prefix}{conn}{icon} job:{jid} wf:{info.get('wfid') or '-'} parent:{info.get('parent') or '-'}")
        next_prefix = prefix + ("    " if is_last else "│   ")
        kids = children.get(jid, [])
        for i, kid in enumerate(kids):
            print_job(kid, next_prefix, i == len(kids) - 1)

    for i, r in enumerate(roots):
        print_job(r, "", i == len(roots) - 1)

    # 6e) Show runtime metadata for data operations
    print("\n-- Data operations metadata --")
    for ms in metasheets:
        p = props(ms)
        direction = p.get("_direction", "?")
        path = p.get("_siteObjPath") if direction == "put" else p.get("_localPath")
        print(f" - {direction.upper()} path:{path} wf:{p.get('_workflowId')} job:{ms.getJobId() if hasattr(ms, 'getJobId') else None}")
        print(f"   props: {p}")

    # 6c) Optional: verify we have JobStatus.INFO records for repo notation in full history
    # We will scan all job statuses; there may be an INFO status emitted for repo events.
    all_statuses = lwfManager.getAllJobStatusesForWorkflow(wf_id) or []
    info_seen = any(getattr(s, "getStatus", lambda: "")() == JobStatus.INFO for s in all_statuses)
    print("INFO status present for repo notation:" , "YES" if info_seen else "NO (optional)")

    print("\nPASS: provenance/runtime audit verified for workflow:", wf_id)


if __name__ == "__main__":
    main()
