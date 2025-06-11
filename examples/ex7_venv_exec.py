"""
Demonstrate alternative site endpoint invocation via lwfManager.execSiteEndpoint()
"""

from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus
from lwfm.midware.LwfManager import lwfManager, logger

if __name__ == "__main__":
    site = lwfManager.getSite("local-venv")

    jobDefn = JobDefn("run.submit", JobDefn.ENTRY_TYPE_SITE,
        ["echo 'hello world'"])
    jobDefn.setSiteName("local-venv")

    # alternative to invoking site interface
    result: JobStatus = lwfManager.execSiteEndpoint(jobDefn)
    logger.info(f"{result.getJobId()} {result.getStatus()}")

    # wait synchronously for it to finish
    result = lwfManager.wait(result.getJobId())
    logger.info(f"{result.getJobId()} {result.getStatus()}")
