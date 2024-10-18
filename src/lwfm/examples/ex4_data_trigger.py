# An example of managing local data.

import logging
from lwfm.base.Site import Site
from lwfm.base.JobDefn import JobDefn, RepoJobDefn, RepoOp
from lwfm.base.SiteFileRef import FSFileRef
from lwfm.src.lwfm.midware.WorkflowEventTrigger import DataEventTrigger

siteName = "local"


def _triggerFilter(metadata: dict = None) -> bool:
    if metadata["myMetaField"] == "ex4_metaflag":
        return True


def example4(site: Site):
    dataFile = "/tmp/ex4_date.out"

    # submit a job to create a file
    jobDefnA = JobDefn("echo date = `date` > " + dataFile)
    statusA = jobDefnA.submit(site).wait()  # sync fire & wait

    # submit a job to copy the file, "put" it to the site and place it under management
    # make a reference to the file to be put under management & add metadata
    metadata = {"myMetaField": "ex4_metaflag"}
    # on the remote site, the file will be put in the specified directory with the
    # specified name, and the metadata will be stored about the file
    siteFileRef = FSFileRef("/tmp", "ex4_date.out" + ".copy", metadata)
    RepoJobDefn(RepoOp.PUT, dataFile, siteFileRef).submit(
        site, statusA.getJobContext()
    )  # async fire & forget

    # set a data trigger on the file - a job will run on the site when the file
    # is put under management
    print("setting data trigger & waiting...")
    jobDefnC = JobDefn("echo date = `date` > /tmp/ex4_date.out.triggered")
    statusC = (
        site.getRunDriver()
        .setWorkflowEventTrigger(DataEventTrigger(_triggerFilter, jobDefnC, siteName))
        .wait()
    )  # sync fire & wait
    print("data trigger job C = " + statusC.toShortString())


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    # make a site driver for the local site and login
    site = Site.getSiteInstanceFactory(siteName)
    # a "local" Site login is generally a no-op
    site.getAuthDriver().login()

    example4(site)
