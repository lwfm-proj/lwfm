"""
print 'hello world' but as a Job on a local site
"""

from lwfm.base.Site import Site
from lwfm.base.JobDefn import JobDefn
from lwfm.midware.LwfManager import lwfManager, logger

if __name__ == "__main__":
    # only using one site for this example - construct an interface to it
    site = Site.getSite("local")

    # a "local" site login is a no-op; real sites will have a login mechanism
    site.getAuthDriver().login()

    # define the job - use all defaults except the actual command to execute
    jobDefn = JobDefn("echo 'hello world'")

    # submit the job to the site asynchronously, get back an initial status
    status = site.getRunDriver().submit(jobDefn)

    # How could we tell the async job has finished? One way is to synchronously
    # wait on its end status. (Another way is asynchronous triggering, which
    # we'll demonstrate in another example.)
    # So sit here until the job is done...
    status = lwfManager.wait(status.getJobId())

    # Let's show that we can also get the result of the job later on
    status = lwfManager.getStatus(status.getJobId())
    logger.info("job status from persistence", status)
