
# An example of managing local data.

import logging
import time
from lwfm.base.Site import Site
from lwfm.base.JobDefn import JobDefn, RepoJobDefn, RepoOp
from lwfm.base.SiteFileRef import FSFileRef
from lwfm.base.WorkflowEventTrigger import JobEventHandler
from lwfm.base.JobStatus import JobStatusValues

siteName = "local"

def example4(site: Site):
    # submit a job to create a file
    dataFile = "/tmp/ex4_date.out"
    jobDefnA = JobDefn()
    jobDefnA.setEntryPoint("echo date = `date` > " + dataFile)
    statusA = site.getRunDriver().submitJob(jobDefnA)
    statusA = statusA.wait()
    if (not statusA.isTerminalSuccess()):
        print("job A failed")
        exit(1)

    # submit a job to copy the file, "put" it to the site and place it under management
    jobDefnB = RepoJobDefn()
    jobDefnB.setRepoOp(RepoOp.PUT)
    jobDefnB.setLocalRef(dataFile)
    siteFileRef = FSFileRef()
    siteFileRef.setPath("/tmp")
    siteFileRef.setName("ex4_date.out" + ".copy")
    time_str = str(int(time.time() * 1000))
    siteFileRef.setMetadata({"myMetaField4": "myMetaValue-" + time_str})
    jobDefnB.setSiteFileRef(siteFileRef)
    statusB = site.getRunDriver().submitJob(jobDefnB, statusA.getJobContext())

    # set a data trigger on the file - a job will run on the site when the file 
    # put under management
    jobDefnC = JobDefn()
    jobDefnC.setEntryPoint("echo date = `date` > /tmp/ex4_date.out.triggered")
    statusC = site.getRunDriver().setEventHandler(
        JobEventHandler(None, JobStatusValues.INFO, jobDefnC, siteName)
    )

    # put the file, which will fire the trigger job
    statusB = statusB.wait()
    print("put job = " + statusB.toShortString())
    statusC = statusC.wait()
    print("trigger job = " + statusC.toShortString())




if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    # make a site driver for the local site and login
    site = Site.getSiteInstanceFactory(siteName)
    # a "local" Site login is generally a no-op
    site.getAuthDriver().login()

    example4(site)
