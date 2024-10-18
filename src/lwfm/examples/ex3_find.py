# An example of managing local data.

import logging
import time
from lwfm.base.Site import Site
from lwfm.base.JobDefn import JobDefn, RepoJobDefn, RepoOp
from lwfm.base.SiteFileRef import SiteFileRef, FSFileRef

siteName = "local"


def example3(site: Site):
    # submit a job to create a file
    dataFile = "/tmp/ex3_date.out"
    jobDefnA = JobDefn()
    jobDefnA.setEntryPoint("echo date = `date` > " + dataFile)
    statusA = site.getRunDriver().submitJob(jobDefnA)
    statusA = statusA.wait()
    if not statusA.isTerminalSuccess():
        print("job A failed")
        exit(1)

    # submit a job to copy the file, "put" it to the site and place it under management
    jobDefnB = RepoJobDefn()
    jobDefnB.setRepoOp(RepoOp.PUT)
    jobDefnB.setLocalRef(dataFile)
    siteFileRef = FSFileRef()
    siteFileRef.setPath("/tmp")
    siteFileRef.setName("ex3_date.out" + ".copy")
    time_str = str(int(time.time() * 1000))
    siteFileRef.setMetadata({"myMetaField3": "myMetaValue-" + time_str})
    jobDefnB.setSiteFileRef(siteFileRef)
    statusB = site.getRunDriver().submitJob(jobDefnB, statusA.getJobContext())
    statusB = statusB.wait()
    print("put job = " + statusB.toShortString())

    # "get" the file by metadata
    siteFileRef = SiteFileRef()
    siteFileRef.setMetadata({"myMetaField3": "myMetaValue-" + time_str})
    foundSiteFileRefs = site.getRepoDriver().find(siteFileRef)
    print("found site file ref = " + str(foundSiteFileRefs))


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    # make a site driver for the local site and login
    site = Site.getSiteInstanceFactory(siteName)
    # a "local" Site login is generally a no-op
    site.getAuthDriver().login()

    example3(site)
