"""
test data triggers
"""
#pylint: disable=invalid-name

import sys

from lwfm.base.Site import Site
from lwfm.midware.LwfManager import lwfManager, logger
from lwfm.base.WorkflowEvent import MetadataEvent
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobContext import JobContext

if __name__ == "__main__":
    site: Site = lwfManager.getSite("local")
    site.getAuthDriver().login()

    context = JobContext()

    sample_id = lwfManager.generateId()
    # when data is put into the repo with this sampleId in the metadata, fire the job
    # on the site as part of my workflow
    futureJobStatus = lwfManager.setEvent(
        MetadataEvent({"sampleId": sample_id}, JobDefn("echo hello world"), "local",
                      None, context.getWorkflowId())
    )
    if futureJobStatus is None:
        logger.error("Failed to set data event trigger")
        sys.exit(1)
    logger.info(f"job {futureJobStatus.getJobId()} set as a data event trigger")

    # now put the file with the metadata in this workflow context
    site.getRepoDriver().put("ex1_date.out", "/tmp/someFile-ex3.dat", context,
        {"sampleId": sample_id})

    # if we want we can wait for the future job to finish
    futureJobStatus = lwfManager.wait(futureJobStatus.getJobId())
    logger.info("data-triggered job finished")
