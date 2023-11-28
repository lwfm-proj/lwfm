
# demonstrate asynchronous job chaining
# assumes the lwfm job status service is running

import logging
import os
from pathlib import Path
import time

from lwfm.base.Site import Site
from lwfm.base.SiteFileRef import SiteFileRef, FSFileRef
from lwfm.base.JobDefn import JobDefn, RepoJobDefn, RepoOp
from lwfm.base.JobStatus import JobStatus, JobStatusValues, JobContext

# This Site name can be an argument - name maps to a Site class implementation,
# either one provided with this sdk, or one user-authored.
siteName = "local"

if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    # one Site for this example - construct an interface to the Site
    site = Site.getSiteInstanceFactory(siteName)
    site.getAuthDriver().login()

    # we'll have three Jobs in total, one's completion firing the next.  A -> B -> C
    # we'll run Job C (a data move) when Job B finishes after Job A finishes 

    jobContextA = JobContext()             
    jobContextB = JobContext()   
    jobContextC = JobContext()   

    # define job A - sit-in for some kind of "real" pre-processing
    jobDefnA = JobDefn()
    jobDefnA.setEntryPoint("echo Job A output pwd = `pwd`")

    # define job B - sit-in for some kind of "real" data-generating application
    dataFile = "/tmp/date.out"
    jobDefnB = JobDefn()
    jobDefnB.setEntryPoint("echo date = `date` > " + dataFile)

    # define job C - put the data "into management", whatever that means for the given site, and do it as an async Job
    # does a "put" operation need to be structured in this way?  no.  if we want to do a Repo.put() within the context
    # of an existing Job, just call Repo.put().  but since this is a common async operation, we provide a subclass of
    # JobDefn for this purpose.
    jobDefnC = RepoJobDefn()
    jobDefnC.setRepoOp(RepoOp.PUT)
    jobDefnC.setLocalRef(Path(dataFile))
    jobDefnC.setSiteRef(FSFileRef.siteFileRefFromPath(os.path.expanduser('~')))

    # set job B to run when job A finishes - i.e., "when the job running async on the named site and represented by the provided
    # canonical job id reaches the state specified, run the given job definition on the named target site in a given job context"
    # then set C to fire when B finishes
    # the "job context" is tracking the digital thread of the related jobs
    # set a trigger, a "future" - when job A gets to complete, run B
    site.getRunDriver().setEventHandler(jobContextA, JobStatusValues.COMPLETE, None, jobDefnB, jobContextB, siteName)
    # set another trigger - when job B gets to complete, run C.  note we don't really need to specify context C,
    # but we will so we can use it in this demo
    site.getRunDriver().setEventHandler(jobContextB, JobStatusValues.COMPLETE, None, jobDefnC, jobContextC, siteName)

    # run job A which initiates the A -> B -> C sequence
    status = site.getRunDriver().submitJob(jobDefnA, jobContextA)

    # for the purposes of this example, let's wait synchronously on the conclusion of job C
    status = site.getRunDriver().getJobStatus(jobContextC)
    while (not status.isTerminal()):
        time.sleep(15)
        status = site.getRunDriver().getJobStatus(jobContextC)
    logging.info("job C " + status.getJobContext().getId() + " " + status.getStatus().value)
