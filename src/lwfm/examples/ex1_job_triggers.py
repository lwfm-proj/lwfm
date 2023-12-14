
# demonstrate asynchronous job chaining
# assumes the lwfm job status service is running

import logging
from pathlib import Path

from lwfm.base.Site import Site
from lwfm.base.SiteFileRef import FSFileRef
from lwfm.base.JobDefn import JobDefn, RepoJobDefn, RepoOp
from lwfm.base.JobStatus import JobStatusValues, fetchJobStatus
from lwfm.base.WorkflowEventTrigger import JobEventTrigger 

# This Site name can be an argument - name maps to a Site class implementation,
# either one provided with this sdk, or one user-authored.
siteName = "local"

if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    # one Site for this example - construct an interface to the Site
    site = Site.getSiteInstanceFactory(siteName)
    site.getAuthDriver().login()
 
    # define job A - sit-in for some kind of "real" pre-processing
    jobDefnA = JobDefn()
    jobDefnA.setEntryPoint("echo Job A output pwd = `pwd`")

    # define job B - sit-in for some kind of "real" data-generating application
    dataFile = "/tmp/ex1_date.out"
    jobDefnB = JobDefn()
    jobDefnB.setEntryPoint("echo date = `date` > " + dataFile)

    # define job C - put the data "into management", whatever that means for the given site,
    # and do it as an async Job does a "put" operation need to be structured in this way?  no.  
    # if we want to do a Repo.put() within the context of an existing Job, just call Repo.put().  
    # but since this is a common async operation, we provide a subclass of
    # JobDefn for this purpose.
    jobDefnC = RepoJobDefn()
    jobDefnC.setRepoOp(RepoOp.PUT)
    jobDefnC.setLocalRef(Path(dataFile))
    siteFileRef = FSFileRef()
    siteFileRef.setPath("/tmp")
    siteFileRef.setName("ex1_date.out" + ".copy")
    jobDefnC.setSiteFileRef(siteFileRef)
    
    # submit job A which gives us the job id we need to set up the event handler for job B 
    statusA = site.getRunDriver().submitJob(jobDefnA)
    print("statusA says id = " + statusA.getJobId())
    print("statusA says status = " + statusA.getStatusValue())

    # for fun, read it back 
    statusA = fetchJobStatus(statusA.getJobId())
    print("from the call, status = " + statusA.getStatusValue())
        
    # when job A gets to the COMPLETE state, fire job B on the named site; registering it
    # gives us the job id we need to set up the event handler for job C
    statusB = site.getRunDriver().setWorkflowEventTrigger(
        JobEventTrigger(statusA.getJobId(), JobStatusValues.COMPLETE.value, jobDefnB, siteName))
    print("job B when it runs will have job id " + statusB.getJobId())
    
    # when job B gets to the COMPLETE state, fire job C on the named site
    statusC = site.getRunDriver().setWorkflowEventTrigger(
        JobEventTrigger(statusB.getJobId(), JobStatusValues.COMPLETE.value, jobDefnC, siteName))
    print("job C when it runs will have job id " + statusC.getJobId())

    # for the purposes of this example, let's wait synchronously on the conclusion of job C
    print("waiting for job C to complete")
    statusC = statusC.wait()
    print(statusC.toShortString())
