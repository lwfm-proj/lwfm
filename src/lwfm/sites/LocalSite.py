# LocalSiteDriver: an implementation of Site and its constituent Auth, Run, Repo 
# interfaces for a local to the user runtime environment.  Unsecure, as this is 
# local and we assume the user is themselves already.

from typing import List
import os
import multiprocessing

from lwfm.base.Site import Site, SiteAuth, SiteRun, SiteRepo, SiteSpin
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus, JobStatusValues
from lwfm.base.JobContext import JobContext
from lwfm.midware.LwfManager import LwfManager
from lwfm.midware.Logger import Logger


# *********************************************************************

SITE_NAME = "local"


# *********************************************************************


class LocalJobStatus(JobStatus):
    def __init__(self, context: JobContext = None):
        super(LocalJobStatus, self).__init__(context)
        # use default canonical status map
        self.getJobContext().setSiteName(SITE_NAME)


# **********************************************************************


class LocalSiteAuth(SiteAuth):
    # Because this is running locally, we don't need any authentication
    def login(self, force: bool = False) -> bool:
        return True

    def isAuthCurrent(self) -> bool:
        return True


# ************************************************************************


class LocalSiteRun(SiteRun):
    _pendingJobs = {}

    def getStatus(self, jobId: str) -> JobStatus:
        return LwfManager.getStatus(jobId)

    def _runJob(self, jDefn: JobDefn, jobContext: JobContext) -> None:
        # Putting the job in a new thread means we can easily run it asynchronously
        # while still emitting statuses before and after
        # Emit RUNNING status
        LwfManager.emitStatus(jobContext, LocalJobStatus, 
                              JobStatusValues.RUNNING.value)
        try:
            # This is synchronous, so we wait here until the subprocess is over.
            # Check=True raises an exception on non-zero returns
            # run a command line job
            cmd = jDefn.getEntryPoint()
            if jDefn.getJobArgs() is not None:
                for arg in jDefn.getJobArgs():
                    cmd += " " + arg
            os.system(cmd)
            # Emit success statuses
            LwfManager.emitStatus(jobContext, LocalJobStatus, 
                                  JobStatusValues.FINISHING.value)
            LwfManager.emitStatus(jobContext, LocalJobStatus, 
                                  JobStatusValues.COMPLETE.value)
        except Exception as ex:
            Logger.error("ERROR: Job failed %s" % (ex))
            # Emit FAILED status
            LwfManager.emitStatus(jobContext, LocalJobStatus, 
                                  JobStatusValues.FAILED.value)


    def submit(self, jDefn: JobDefn, useContext: JobContext = None, 
        computeType: str = None, runArgs: dict = None) -> JobStatus:
        if (useContext is None):
            useContext = JobContext()
            # we can test validity of the job defn here, reject it, or say its ready
            # if we were given a context, then we assume its ready 
            LwfManager.emitStatus(useContext, LocalJobStatus, 
                                  JobStatusValues.READY.value)
        # horse at the gate...
        LwfManager.emitStatus(useContext, LocalJobStatus, 
                              JobStatusValues.PENDING.value)
        # Run the job in a new thread so we can wrap it in a bit more code
        # this will kick the status the rest of the way to a terminal state 
        multiprocessing.Process(target=self._runJob, args=[jDefn, useContext]).start()
        Logger.info("LocalSite: submitted job %s" % (useContext.getId()))
        return LwfManager.getStatus(useContext.getId())


    def cancel(self, jobContext: JobContext) -> bool:
        # Find the locally running thread and kill it
        try:
            thread = self._pendingJobs[jobContext.getId()]
            if thread is None:
                return False
            Logger.info(
                "LocalSiteDriver.cancelJob(): calling terminate on job "
                + jobContext.getId()
            )
            thread.terminate()
            jStatus = LocalJobStatus(jobContext)
            jStatus.emit(JobStatusValues.CANCELLED.value)
            self._pendingJobs[jobContext.getId()] = None
            return True
        except Exception as ex:
            Logger.error(
                "ERROR: Could not cancel job %d: %s" % (jobContext.getId(), ex)
            )
            return False


# *************************************************************************************

class LocalSiteSpin(SiteSpin):
    
    def listComputeTypes(self) -> List[str]:
        return ["default"]


# *************************************************************************************

# TODO 
# _repoDriver = LocalSiteRepo()


class LocalSite(Site):
    # There are no required args to instantiate a local site.
    def __init__(self):
        super(LocalSite, self).__init__(
            # SITE_NAME, LocalSiteAuth(), LocalSiteRun(), _repoDriver, None
            SITE_NAME, LocalSiteAuth(), LocalSiteRun(), None, LocalSiteSpin()
        )
