"""
Test job triggers expressed in site endpoint shorthand. 
"""

#pylint: disable=invalid-name, broad-exception-caught

import sys

from lwfm.base.JobDefn import JobDefn
from lwfm.base.WorkflowEvent import JobEvent
from lwfm.base.JobStatus import JobStatus
from lwfm.midware.LwfManager import lwfManager, logger


if __name__ == "__main__":
    site = lwfManager.getSite("local")

    job_defn = JobDefn("echo 'hello world'")
    job_status_A = site.getRunDriver().submit(job_defn)
    logger.info(f"lwfm job {job_status_A.getJobId()}")

    job_status_B = lwfManager.setEvent(
        JobEvent(job_status_A.getJobId(),                       # when this job
                JobStatus.COMPLETE,                             # hits this state
                JobDefn("repo.get", JobDefn.ENTRY_TYPE_SITE,    # run this repo.get
                    ["/tmp/ex8.in",                             # with these args
                    "/tmp/" + job_status_A.getJobId() + \
                    ".out"]),
                "local-venv")                                   # using this site driver
    )
    if job_status_B is None:
        logger.error("Failed to set job B event on job A")
        sys.exit(1)
    logger.info(f"job B {str(job_status_B)} set to create /tmp/{job_status_A.getJobId()}.out")
    job_status_B = lwfManager.wait(job_status_B.getJobId())
    logger.info(f"job B {str(job_status_B)} finished")
    out_file = f"/tmp/{job_status_A.getJobId()}.out"
    try:
        with open(out_file, "r", encoding="utf-8") as f:
            contents = f.read()
        print(f"Contents of {out_file}:\n{contents}")
    except Exception as e:
        logger.error(f"Failed to read {out_file}: {e}")
