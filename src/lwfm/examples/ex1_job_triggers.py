# demonstrate asynchronous job chaining

from lwfm.midware.Logger import Logger
from lwfm.base.Site import Site
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatusValues
from lwfm.base.WfEvent import JobEvent
from lwfm.midware.LwfManager import LwfManager


if __name__ == "__main__":
    site = Site.getSite("local")
    site.getAuth().login()

    # define job A - sit-in for some kind of "real" pre-processing
    jobDefnA = JobDefn("echo hello world, job A output pwd = `pwd`")

    # a stand-in for some data file 
    dataFile = "/tmp/ex1_date.out"

    # submit job A
    statusA = site.getRun().submit(jobDefnA)    # a new job context is created
    Logger.info("job A submitted", statusA)

    # when job A asynchronously reaches the COMPLETE state, fire job B 
    futureJobIdB = LwfManager.setEvent(
        JobEvent(statusA.getJobId(), JobStatusValues.COMPLETE.value, 
                 JobDefn("echo date = `date` > " + dataFile), "local")
    )
    Logger.info(f"job B {futureJobIdB} set as a job event on A")

    # when job B asynchronously gets to the COMPLETE state, fire job C 
    futureJobIdC = LwfManager.setEvent(
        JobEvent(futureJobIdB, JobStatusValues.COMPLETE.value,
                 JobDefn("cat " + dataFile), "local")
    )
    Logger.info(f"job C {futureJobIdC} set as a job event on B")

    # for the purposes of this example, let's wait synchronously on the
    # conclusion of job C, which implies B and A also finished
    statusC = LwfManager.wait(futureJobIdC)
    Logger.info("job C finished, implying B and A also finished", statusC)
