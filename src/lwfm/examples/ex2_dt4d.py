
import time

from lwfm.base.Site import Site
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus, JobStatusValues, JobContext
from lwfm.server.JobStatusSentinelClient import JobStatusSentinelClient

# This Site name can be an argument - name maps to a Site class implementation,
# either one provided with this sdk, or one user-authored.  In this case, the latter, as "DT4D" is a
# specific Site implementation.  We've modified ~/.lwfm/sites.txt to include the following:
# dt4d=lwfm.drivers.DT4DSiteDriver.DT4DSite
siteName = "dt4d"

if __name__ == '__main__':

    # one Site for this example - construct an interface to the Site
    site = Site.getSiteInstanceFactory(siteName)
    # login to the Site
    site.getAuthDriver().login()

    # define the Job - use all Job defaults except the actual command to execute
    jobDefn = JobDefn()
    jobDefn.setName("HelloWorld")
    # dt4d uses a "ToolRepo" to store applications / workflows to be run which allows it to do digital threading
    # here we're running a simple python script as a dt4d "tool"
    jobDefn.setEntryPoint([ "/Users/212578984/src/dt4d/py4dt4d", "py4dt4d-examples", "HelloWorld", "HelloWorld" ])

    # run it "local" - dt4d defines "compute types" which are nodes prepared for special application purposes
    # "local" is also a compute type within dt4d
    # lwfm allows you to set the compute type in the JobDefn - this can be used by a Site in targeting the run
    # in whatever way they choose
    #jobDefn.setComputeType("local")
    #status = site.getRunDriver().submitJob(jobDefn)

    # for fun, let's wait until that local job finishes
    #while (not status.isTerminal()):
    #    time.sleep(15)
    #    status = site.getRunDriver().getJobStatus(status.getJobContext())

    #print("local dt4d job " + status.getId() + " with native dt4d job id = " + status.getNativeId() + " " + status.getStatus().value)

    # now run the same tool remote on a remote dt4d node of a named compute type
    jobDefn.setComputeType("Win-VDrive")
    context = JobContext()
    status = site.getRunDriver().submitJob(jobDefn, context)
    print("local dt4d job " + status.getId() + " with native dt4d job id = " + status.getNativeId() + " " + status.getStatus().value)

    # for fun, let's wait until that remote job finishes
    #status = JobStatus.getStatusObj(context.getId())
    #while (not status.isTerminal()):
    #    logging.info("current status = " + status.getStatus().value)
    #    time.sleep(15)
    #    status = JobStatus.getStatusObj(context.getId())
    #logging.info("remote job " + status.getId() + " " + status.getStatus().value)
