"""
Demonstrate alternative site endpoint invocation via lwfManager.execSiteEndpoint()
"""

from typing import cast
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus
from lwfm.midware.LwfManager import lwfManager, logger

if __name__ == "__main__":
    site = lwfManager.getSite("local-venv")

    jobDefn = JobDefn("run.submit", JobDefn.ENTRY_TYPE_SITE,
        ["echo 'hello world'"])
    jobDefn.setSiteName("local-venv")

    # alternative to invoking site interface
    result = lwfManager.execSiteEndpoint(jobDefn)
    if result is None:
        logger.error("Failed to execute job on site endpoint")
    result = cast(JobStatus, result)
    logger.info(f"{result.getJobId()} {result.getStatus()}")

    # wait synchronously for it to finish
    result = lwfManager.wait(result.getJobId())
    logger.info(f"{result.getJobId()} {result.getStatus()}")
