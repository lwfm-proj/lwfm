"""
Find() capability tests (example-style).

This example verifies we can:
- Find metasheets by _jobId and _workflowId that put/get them.
- Search by custom metadata fields (exact and wildcard).
- Use AND semantics across multiple fields.

Requires the middleware service to be running.
"""

import os
import sys
from typing import List, Dict, Any, cast, NoReturn

from lwfm.midware.LwfManager import lwfManager
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus
from lwfm.base.JobContext import JobContext
from lwfm.base.Workflow import Workflow
from lwfm.base.Metasheet import Metasheet


def fail(msg: str) -> NoReturn:
    print(f"FAIL: {msg}")
    sys.exit(1)


def ensure_midware() -> None:
    if not lwfManager.isMidwareRunning():
        fail("Middleware service is not running. Start it and re-run this test.")


def props(ms: Metasheet) -> Dict[str, Any]:
    return cast(Dict[str, Any], ms.getProps() or {})


def main() -> None:
    ensure_midware()

    # 1) Create workflow and run a root job that writes a file under /tmp
    wf = Workflow("ex13 find() tests")
    wf = lwfManager.putWorkflow(wf)
    if wf is None:
        fail("putWorkflow returned None")
    wf_id = wf.getWorkflowId()
    wf = lwfManager.getWorkflow(wf_id)
    if wf is None:
        fail("getWorkflow returned None")

    site = lwfManager.getSite("local")
    temp_dir = "/tmp"
    local_file = os.path.join(temp_dir, f"ex13_local_{lwfManager.generateId()}.txt")
    with open(local_file, "w", encoding="utf-8") as f:
        f.write("hello from ex13")

    # Run a trivial job associated with the workflow (for job attribution)
    job_defn = JobDefn(f"echo 'ex13 run' >> {local_file}")
    root_status = site.getRunDriver().submit(job_defn, wf)
    root_status = lwfManager.wait(root_status.getJobId())
    if root_status.getStatus() != JobStatus.COMPLETE:
        fail("Root job did not complete")
    root_job_id = root_status.getJobId()

    # 2) Put the file to a repo path under /tmp with custom metadata
    repo_obj = os.path.join(temp_dir, f"ex13_repo_{root_job_id}.txt")
    ms_put = site.getRepoDriver().put(
        local_file,
        repo_obj,
        root_status.getJobContext(),
        {"case": "ex13-put1", "creator": "ex13", "desc": "hello file", "tag": "alpha"},
    )
    if ms_put is None:
        fail("Repo put metasheet is None")

    # 3) Perform a get (copy) with workflow-scoped context and custom metadata
    site_src = os.path.join(temp_dir, f"ex13_site_src_{root_job_id}.txt")
    with open(site_src, "w", encoding="utf-8") as f:
        f.write("data for get")
    get_dest = os.path.join(temp_dir, f"ex13_get_{root_job_id}.txt")
    wf_ctx = JobContext()
    wf_ctx.setWorkflowId(wf_id)
    got_path = site.getRepoDriver().get(
        site_src, get_dest, wf_ctx, {"case": "ex13-get1", "creator": "ex13", "desc": "hello get", "tag": "beta"}
    )
    if not got_path or not os.path.exists(got_path):
        fail("Repo get did not return a valid path")

    # 4) Queries using find()
    # a) By _jobId (should include the put metasheet attributed to the root job)
    by_job: List[Metasheet] = lwfManager.find({"_jobId": root_job_id}) or []
    if not any(props(ms).get("_siteObjPath") == repo_obj for ms in by_job):
        fail("find({_jobId}) did not return the expected put metasheet")

    # b) By _workflowId (should include both put and get metasheets)
    by_wf: List[Metasheet] = lwfManager.find({"_workflowId": wf_id}) or []
    if not any(props(ms).get("_siteObjPath") == repo_obj for ms in by_wf):
        fail("find({_workflowId}) missing put metasheet")
    if not any(props(ms).get("_localPath") == get_dest for ms in by_wf):
        fail("find({_workflowId}) missing get metasheet")

    # c) Custom exact match
    by_case_get: List[Metasheet] = lwfManager.find({"case": "ex13-get1"}) or []
    if not any(props(ms).get("_localPath") == get_dest for ms in by_case_get):
        fail("find(case=ex13-get1) did not return the get metasheet")

    # d) Wildcard match on custom field
    by_case_wild: List[Metasheet] = lwfManager.find({"case": "ex13-*"}) or []
    # Expect both put and get cases
    case_vals = {props(ms).get("case") for ms in by_case_wild}
    if not ({"ex13-put1", "ex13-get1"} <= case_vals):
        fail("find(case=ex13-*) did not return both put and get metasheets")

    # e) AND semantics with wildcard (creator exact AND tag prefix)
    by_creator_and_tag: List[Metasheet] = lwfManager.find({"creator": "ex13", "tag": "b*"}) or []
    if not any(props(ms).get("_localPath") == get_dest for ms in by_creator_and_tag):
        fail("find(creator=ex13, tag=b*) did not return the get metasheet")

    print("PASS: ex13 find() tests succeeded")


if __name__ == "__main__":
    main()
