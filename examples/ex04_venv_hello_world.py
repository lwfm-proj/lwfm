"""
print 'hello world' but as a Job on a local site within a virtual environment
"""

import sys
from lwfm.base.JobDefn import JobDefn
from lwfm.midware.LwfManager import lwfManager, logger

if __name__ == "__main__":
    # Get site name from command line arg, default to "local-venv"
    site_name = sys.argv[1] if len(sys.argv) > 1 else "local-venv"
    
    # only using one site for this example - construct an interface to it
    site = lwfManager.getSite(site_name)

    logger.info(f"site={site.getSiteName()} " + \
        f"toml={lwfManager.getSiteProperties(site.getSiteName())}")

    # we expect a wrapper driver, buried within is the real auth driver which
    # cannot be operated outside the venv - the wrapper handles this
    logger.info(f"venv site auth driver is of type {type(site.getAuthDriver())}")

    # call thru the wrapper to the driver in the venv to login
    site.getAuthDriver().login()
    logger.info(f"auth current {site.getAuthDriver().isAuthCurrent()}")

    # define the job - use all defaults except the actual command to execute
    jobDefn = JobDefn("echo 'hello world'")

    # submit the job to the site asynchronously, get back an initial status
    status = site.getRunDriver().submit(jobDefn)
    logger.info(f"submitted job {status.getJobId()}, " + \
        f"native: {status.getJobContext().getNativeId()}")

    # How could we tell the async job has finished? One way is to synchronously
    # wait on its end status. (Another way is asynchronous triggering, which
    # we'll demonstrate in another example.)
    # So sit here until the job is done...
    status = lwfManager.wait(status.getJobId())

    # Let's show that we can also get the result of the job later on
    status = lwfManager.getStatus(status.getJobId())
    logger.info(f"job {str(status)}")
