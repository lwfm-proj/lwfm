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

    # define the job - use all defaults except the actual command to execute
    jobDefn = JobDefn("echo 'hello world'")

    # submit the job to the site asynchronously, get back an initial status 
    status = site.getRun().submit(jobDefn)

    # How could we tell the async job has finished? One way is to synchronously
    # wait on its end status. (Another way is asynchronous triggering, which
    # we'll demonstrate in another example.)
    # So sit here until the job is done...
    status = LwfManager.wait(status.getJobId())

    # Let's show that we can also get the result of the job later on
    status = LwfManager.getStatus(status.getJobId())
    Logger.info("job status from persistence", status)

