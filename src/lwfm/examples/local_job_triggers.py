
import logging

from lwfm.base.Site import Site
from lwfm.base.SiteFileRef import SiteFileRef, FSFileRef
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus, JobStatusValues, JobContext
from lwfm.server.JobStatusSentinelClient import JobStatusSentinelClient


if __name__ == '__main__':
    # assumes the lwfm job status service is running
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    siteName = "local"

    # One site for all jobs in this example, the local site.
    site = Site.getSiteInstanceFactory(siteName)
    site.getAuthDriver().login()

    # We'll run job C (a data move) when B finishes after A finishes - generate a job ids to trigger off
    jobContextA = JobContext()
    jobContextB = JobContext()

    # define job A - sit-in for some pre-processing
    jobDefnA = JobDefn()
    jobDefnA.setEntryPointPath("echo pwd = `pwd`")

    # define job B - sit-in for some data generating processing
    jobDefnB = JobDefn()
    jobDefnB.setEntryPointPath("echo date = `date` > /tmp/date.out")

    # define job C - put the data "into management", whatever that means for the given site
    jobDefnC = JobDefn()
    jobDefnC.setEntryPointPath("cat /tmp/date.out > /tmp/date2.out")

    # set job B to run when job A finishes - "when the job running natively on the named site and represented by the provided
    # canonical job id reaches the state specified, run the given job definition on the named target site"
    # then set C to fire when B finishes
    jssc = JobStatusSentinelClient()
    jssc.setEventHandler(jobContextA.getId(), siteName, JobStatusValues.COMPLETE.value, jobDefnB, siteName, jobContextB.getId())
    jssc.setEventHandler(jobContextB.getId(), siteName, JobStatusValues.COMPLETE.value, jobDefnC, siteName)

    # run job A which initiates the sequence
    status = site.getRunDriver().submitJob(jobDefnA, jobContextA)
