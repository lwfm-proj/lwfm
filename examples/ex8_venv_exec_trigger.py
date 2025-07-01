"""
Test job triggers expressed in site endpoint shorthand.

This is a good local stand-in for an invocation to the IBM quantum cloud, wherein a job
is submitted and when the job asynchronously completes, a trigger job is run to
retrieve the results of the first job.
"""

#pylint: disable=invalid-name, broad-exception-caught

import sys

from lwfm.base.JobDefn import JobDefn
from lwfm.base.WorkflowEvent import JobEvent
from lwfm.base.JobStatus import JobStatus
from lwfm.midware.LwfManager import lwfManager, logger


if __name__ == "__main__":
    site = lwfManager.getSite("local")

    # run some job
    job_defn = JobDefn("echo 'hello world'")
    job_status_A = site.getRunDriver().submit(job_defn)
    logger.info(f"lwfm job {job_status_A.getJobId()} " + \
                f"workflow {job_status_A.getJobContext().getWorkflowId()}")

    # set a trigger to run when the above job async completes but on a different site
    # (the local venv site)
    job_status_B = lwfManager.setEvent(
        JobEvent(job_status_A.getJobId(),                       # when this job
                JobStatus.COMPLETE,                             # hits this state
                JobDefn("repo.get", JobDefn.ENTRY_TYPE_SITE,    # run this command
                    ["/tmp/ex8.in",                             # with these args
                    "/tmp/" + job_status_A.getJobId() + \
                    ".out"]),
                "local-venv",                                   # on this site
                None,                                           # with a new job id
                job_status_A.getJobContext())                   # in this workflow
    )
    if job_status_B is None:
        logger.error("Failed to set job B event on job A")
        sys.exit(1)

    # optionally we can wait for the trigger job to complete and then print the output
    job_status_B = lwfManager.wait(job_status_B.getJobId())
    logger.info(f"job B {str(job_status_B)} finished - contents of the output file:")
    out_file = f"/tmp/{job_status_A.getJobId()}.out"
    try:
        with open(out_file, "r", encoding="utf-8") as f:
            contents = f.read()
        print(f"Contents of {out_file}:\n{contents}")
    except Exception as e:
        logger.error(f"Failed to read {out_file}: {e}")
