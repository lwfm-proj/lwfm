"""
Test job triggers expressed in site endpoint shorthand. 
"""

#pylint: disable=invalid-name

from lwfm.base.JobDefn import JobDefn
from lwfm.base.WorkflowEvent import JobEvent
from lwfm.base.JobStatus import JobStatus
from lwfm.midware.LwfManager import lwfManager, logger


if __name__ == "__main__":
    # get the site auth/run/repo/spin drivers for IBM Quantum Cloud
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
    logger.info(f"result extraction set - see job log {job_status_B.getJobId()}.log")
