"""
demonstrate asynchronous job chaining using a venv site
this is the same as the ex1 example, but using a site that is
configured to run jobs in a virtual environment - good test might be refactor ex1 
to take a site arg which might be venv, but the point was to keep the examples simple
"""

import sys

from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus
from lwfm.base.WorkflowEvent import JobEvent
from lwfm.base.Workflow import Workflow
from lwfm.midware.LwfManager import lwfManager, logger

#pylint: disable = invalid-name

if __name__ == "__main__":
    # get the local site and "login"
    site = lwfManager.getSite("local-venv")
    site.getAuthDriver().login()

    # define job A - sit-in for some kind of "real" pre-processing
    jobDefnA = JobDefn(">&2 echo hello world, job A output pwd = `pwd`")
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
        JobEvent(statusA.getJobId(), JobStatus.COMPLETE,
                 JobDefn("echo date = `date` > " + dataFile), "local", None, wf.getWorkflowId())
    )
    if statusB is None:
        logger.error("Failed to set job B event on job A")
        sys.exit(1)
    logger.info(f"job B {str(statusB)} set as a job event on A")

    # when job B asynchronously gets to the COMPLETE state, fire job C
    statusC = lwfManager.setEvent(
        JobEvent(statusB.getJobId(), JobStatus.COMPLETE,
                 JobDefn(">&2 echo " + dataFile), "local", None, wf.getWorkflowId())
    )
    if statusC is None:
        logger.error("Failed to set job C event on job B")
        sys.exit(1)
    logger.info(f"job C {str(statusC)} set as a job event on B")


    # for the purposes of this example, let's wait synchronously on the
    # conclusion of job C, which implies B and A also finished
    print(f"Let's wait synchronously for the chain to end on job C {statusC.getJobId()}...")
    statusC = lwfManager.wait(statusC.getJobId())
    logger.info(f"job C {statusC.getJobId()} finished, implying B and A also finished")

    # poll the final status for A, B, & C
    statusA = lwfManager.getStatus(statusA.getJobId())
    statusB = lwfManager.getStatus(statusB.getJobId())
    statusC = lwfManager.getStatus(statusC.getJobId())
    logger.info(f"job A {str(statusA)}")
    logger.info(f"job B {str(statusB)}")
    logger.info(f"job C {str(statusC)}")
