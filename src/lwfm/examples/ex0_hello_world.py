# print 'hello world' but as a Job on a local site

from lwfm.base.Site import Site
from lwfm.base.JobDefn import JobDefn
from lwfm.midware.Logger import Logger
from lwfm.midware.LwfManager import LwfManager

if __name__ == "__main__":
    # only using one site for this example - construct an interface to it
    site = Site.getSite("local")

    # a "local" site login is a no-op; real sites will have a login mechanism
    site.getAuth().login()

    # define the job - use all Job defaults except the actual command to execute
    jobDefn = JobDefn("echo 'hello world'")

    # submit the job to the site
    status = site.getRun().submit(jobDefn)
    Logger.info("hello world job is launched", status)

    # How could we tell the async job has finished? One way is to synchronously
    # wait on its end status. (Another way is asynchronous triggering, which
    # we'll demonstrate in a separate example.)
    status = LwfManager.wait(status.getJobId())
    Logger.info("hello world job is done", status)

    # Let's show that we can get the result of the job later on
    status = LwfManager.getStatus(status.getJobId())
    Logger.info("job status from persistence", status)