"""
test data triggers
"""
#pylint: disable=invalid-name

from lwfm.base.Site import Site
from lwfm.midware.LwfManager import lwfManager, logger
from lwfm.base.WorkflowEvent import MetadataEvent
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobContext import JobContext
from lwfm.base.JobStatus import JobStatus

if __name__ == "__main__":
    site: Site = lwfManager.getSite("local")

    # create a workflow context for this script and announce we're running
    context = JobContext()
    lwfManager.emitStatus(context, JobStatus.RUNNING)

    # we can use the lwfManager to generate unique identifiers, for samples, etc.
    sample_id = lwfManager.generateId()
    # when data is put into the repo with this sampleId in the metadata, fire the job
    # on the site as part of this workflow
    futureJobStatus = lwfManager.setEvent(
        MetadataEvent({"sampleId": sample_id}, JobDefn("echo hello world"), "local",
                      None, context)
    )

    # now put an example file somewhere and notate its use with our metadata
    site.getRepoDriver().put("/tmp/ex1_date.out", "/tmp/someFile-ex3.dat", context,
        {"sampleId": sample_id})

    # if we want we can wait for the future job to finish
    if futureJobStatus is not None:
        futureJobStatus = lwfManager.wait(futureJobStatus.getJobId())
        logger.info("data-triggered job finished")
        lwfManager.emitStatus(context, JobStatus.COMPLETE)
    else:
        logger.info("data-triggered job not started, check the site for the job status")
        lwfManager.emitStatus(context, JobStatus.FAILED)
