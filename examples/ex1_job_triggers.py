"""
demonstrate asynchronous job chaining
"""


from lwfm.base.Site import Site
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatusValues
from lwfm.base.WorkflowEvent import JobEvent
from lwfm.base.Workflow import Workflow
from lwfm.midware.LwfManager import lwfManager
from lwfm.midware.Logger import logger

#pylint: disable = invalid-name

if __name__ == "__main__":
    # get the local site and "login"
    site = Site.getSite("local")
    site.getAuthDriver().login()

    # define job A - sit-in for some kind of "real" pre-processing
    jobDefnA = JobDefn("echo hello world, job A output pwd = `pwd`")
    # a stand-in for some data file
    dataFile = "ex1_date.out"

    # define workflow - [if one was not defined, a trivial one would be created under
    # the hood on call to submit()]
    wf = Workflow()
    wf.setName("A->B->C test")
    wf.setDescription("A test of chaining three jobs together asynchronously")
    lwfManager.putWorkflow(wf)

    # submit job A
    statusA = site.getRunDriver().submit(jobDefnA, wf)
    logger.info("job A submitted")

    # when job A asynchronously reaches the COMPLETE state, fire job B
    statusB = lwfManager.setEvent(
        JobEvent(statusA.getJobId(), JobStatusValues.COMPLETE.value,
                 JobDefn("echo date = `date` > " + dataFile), "local")
    )
    logger.info(f"job B {statusB.getJobId()} set as a job event on A")

    # when job B asynchronously gets to the COMPLETE state, fire job C
    statusC = lwfManager.setEvent(
        JobEvent(statusB.getJobId(), JobStatusValues.COMPLETE.value,
                 JobDefn("echo " + dataFile), "local")
    )
    logger.info(f"job C {statusC.getJobId()} set as a job event on B")


    # for the purposes of this example, let's wait synchronously on the
    # conclusion of job C, which implies B and A also finished
    print(f"Let's wait synchronously for the chain to end on job C {statusC.getJobId()}...")
    statusC = lwfManager.wait(statusC.getJobId())
    logger.info(f"job C {statusC.getJobId()} finished, implying B and A also finished")

    # poll the final status for A, B, & C
    statusA = lwfManager.getStatus(statusA.getJobId())
    logger.info(f"job A {statusA.getJobId()}", statusA)
    statusB = lwfManager.getStatus(statusB.getJobId())
    logger.info(f"job B {statusB.getJobId()}", statusB)
    statusC = lwfManager.getStatus(statusC.getJobId())
    logger.info(f"job C {statusC.getJobId()}", statusC)
