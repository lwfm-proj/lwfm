"""
demonstrate asynchronous job chaining
"""


from lwfm.base.Site import Site
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatusValues
from lwfm.base.WfEvent import JobEvent
from lwfm.base.Workflow import Workflow
from lwfm.midware.LwfManager import lwfManager
from lwfm.midware.Logger import logger

#pylint: disable = invalid-name

if __name__ == "__main__":
    site = Site.getSite("local")
    site.getAuthDriver().login()

    # define job A - sit-in for some kind of "real" pre-processing
    jobDefnA = JobDefn("echo hello world, job A output pwd = `pwd`")

    # a stand-in for some data file
    dataFile = "ex1_date.out"

    # define workflow - if one was not defined, a trivial one would be created below
    wf = Workflow()
    wf.setName("A->B->C test")
    wf.setDescription("A test of chaining three jobs together asynchronously")
    lwfManager.putWorkflow(wf)

    # submit job A
    statusA = site.getRunDriver().submit(jobDefnA, wf)    # a new job context is created
    logger.info("job A submitted")

    # when job A asynchronously reaches the COMPLETE state, fire job B
    statusB = lwfManager.setEvent(
        JobEvent(statusA.getJobId(), JobStatusValues.COMPLETE.value,
                 JobDefn("echo date = `date` > " + dataFile), "local")
    )
    logger.info(f"job B {statusB.getJobId()} set as a job event on A")
    logger.info("job B", statusB)

    # when job B asynchronously gets to the COMPLETE state, fire job C
    statusC = lwfManager.setEvent(
        JobEvent(statusB.getJobId(), JobStatusValues.COMPLETE.value,
                 JobDefn("echo " + dataFile), "local")
    )
    logger.info(f"job C {statusC.getJobId()} set as a job event on B")
    logger.info("job C", statusC)

    # for the purposes of this example, let's wait synchronously on the
    # conclusion of job C, which implies B and A also finished
    statusC = lwfManager.wait(statusC.getJobId())
    logger.info("job C finished, implying B and A also finished")

    statusA = lwfManager.getStatus(statusA.getJobId())
    logger.info("job A", statusA)
    statusB = lwfManager.getStatus(statusB.getJobId())
    logger.info("job B", statusB)
    statusC = lwfManager.getStatus(statusC.getJobId())
    logger.info("job C", statusC)


    sList = lwfManager.getAllStatus(statusC.getJobId())
    for s in sList:
        print(s)
