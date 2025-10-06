"""
Example ex14: put and get on the same file in the same workflow.
This drives two metasheets that share the same site path so the graph view can
coalesce them into a single data node (files considered the same by local or site path).
"""

# pylint: disable=invalid-name

import sys

from lwfm.base.Workflow import Workflow
from lwfm.base.JobDefn import JobDefn
from lwfm.midware.LwfManager import lwfManager


def main() -> int:
    wf = Workflow("ex14 put+get same file")
    wf = lwfManager.putWorkflow(wf)
    if wf is None:
        print("Failed to put workflow")
        return 1

    site = lwfManager.getSite("local")

    # Prepare file paths under /tmp
    local_src = "/tmp/ex14_same_file_src.txt"
    site_path = "/tmp/ex14_same_file_repo.txt"  # logical site object path
    local_dst = "/tmp/ex14_same_file_dst.txt"

    # Job 1: create the source file
    j1 = site.getRunDriver().submit(JobDefn(f"echo ex14 > {local_src}"), wf)
    j1 = lwfManager.wait(j1.getJobId())
    print(f"ex14: job1 complete: {j1}")

    # Put the file using job1's context so the data node links to job1
    ms_put = site.getRepoDriver().put(local_src, site_path, j1.getJobContext(), {
        "example": "ex14",
        "op": "put",
    })
    print(f"ex14: put metasheet: {ms_put}")

    # Job 2: a lightweight job, then do a get using job2's context
    j2 = site.getRunDriver().submit(JobDefn("echo ex14-get"), wf)
    j2 = lwfManager.wait(j2.getJobId())
    print(f"ex14: job2 complete: {j2}")

    dest = site.getRepoDriver().get(site_path, local_dst, j2.getJobContext(), {
        "example": "ex14",
        "op": "get",
    })
    print(f"ex14: get dest: {dest}")

    # Reminder for graph: two files are the same if local or site path matches
    print("ex14: done. In the GUI workflow graph, the put+get should coalesce into one data node by site path.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
