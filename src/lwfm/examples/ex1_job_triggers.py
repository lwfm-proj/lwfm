# demonstrate asynchronous job chaining

from lwfm.midware.Logger import Logger
from lwfm.base.Site import Site
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatusValues
from lwfm.midware.LwfMonitor import LwfMonitor, JobEvent


if __name__ == "__main__":
    site = Site.getSite("local")
    site.getAuth().login()

    # define job A - sit-in for some kind of "real" pre-processing
    jobDefnA = JobDefn("echo Job A output pwd = `pwd`")

    # define job B - sit-in for some kind of "real" data-generating application
    dataFile = "/tmp/ex1_date.out"
    jobDefnB = JobDefn("echo date = `date` > " + dataFile)

    # define job C - sit-in for some kind of "real" post-processing
    jobDefnC = JobDefn("cat " + dataFile)

    # submit job A, obtaining job id we need to set up the event handler
    statusA = site.getRun().submit(jobDefnA)
    Logger.info("job A submitted", statusA)

    # when job A gets to the COMPLETE state, fire job B on the named site;
    # registering it returns the job id we need to set up the next handler
    statusB = LwfMonitor.setEvent(
        JobEvent(jobDefnB, statusA.getJobContext(), 
                 statusA.getJobId(), None, JobStatusValues.COMPLETE.value)
    )
    Logger.info("job B set as a trigger on A", statusB)

    # when job B gets to the COMPLETE state, fire job C on the named site
    statusC = LwfMonitor.setEvent(
        JobEvent(jobDefnC, statusB.getJobContext(), 
                 statusB.getJobId(), None, JobStatusValues.COMPLETE.value)
    )
    Logger.info("job C set as a trigger on B", statusC)

    # for the purposes of this example, let's wait synchronously on the
    # conclusion of job C, which implies B also finished
    statusC = statusC.wait()
    Logger.info("job C finished, implying B and A also finished", statusC)
