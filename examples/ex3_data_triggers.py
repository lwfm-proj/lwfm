"""
test data triggers
"""

from lwfm.base.Site import Site
from lwfm.base.Metasheet import Metasheet
from lwfm.midware.LwfManager import lwfManager, logger
from lwfm.base.WorkflowEvent import MetadataEvent
from lwfm.base.JobDefn import JobDefn

if __name__ == "__main__":
    site: Site = lwfManager.getSite("local")
    site.getAuthDriver().login()

    TS = lwfManager.generateId()
    # when data is put into the repo with this sampleId in the metadata, fire the job
    # on the site
    futureJobStatus = lwfManager.setEvent(
        MetadataEvent({"sampleId": TS}, JobDefn("echo hello world"), "local")
    )
    logger.info(f"job {futureJobStatus.getJobId()} set as a data event trigger")

    # now put the file with the metadata
    site.getRepoDriver().put("ex1_date.out", "/tmp/someFile-ex3.dat", None,
        Metasheet(site.getSiteName(), "/tmp/someFile-ex3.dat", {"sampleId": TS}))

    # if we want we can wait for the future job to finish
    status = lwfManager.wait(futureJobStatus.getJobId())
    logger.info("data-triggered job finished", status)
