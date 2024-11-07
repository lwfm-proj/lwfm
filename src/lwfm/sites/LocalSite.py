# LocalSiteDriver: an implementation of Site and its constituent Auth, Run, Repo 
# interfaces for a local to the user runtime environment.  Unsecure, as this is 
# local and we assume the user is themselves already.

from pathlib import Path
import shutil
from typing import List
import os, subprocess
import multiprocessing

from lwfm.base.Site import Site, SiteAuth, SiteRun, SiteRepo, SiteSpin
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus, JobStatusValues
from lwfm.base.JobContext import JobContext
from lwfm.base.Metasheet import Metasheet
from lwfm.midware.LwfManager import LwfManager
from lwfm.midware.Logger import Logger


# *********************************************************************


class LocalJobStatus(JobStatus):
    def __init__(self, context: JobContext = None):
        super(LocalJobStatus, self).__init__(context)
        # use default canonical status map
        self.getJobContext().setSiteName(LocalSite.SITE_NAME)


# **********************************************************************


class LocalSiteAuth(SiteAuth):
    # Because this is running locally, we don't need any authentication
    def login(self, force: bool = False) -> bool:
        return True

    def isAuthCurrent(self) -> bool:
        return True


# **********************************************************************


class LocalSiteRun(SiteRun):
    _pendingJobs = {}

    def getStatus(self, jobId: str) -> JobStatus:
        return LwfManager.getStatus(jobId)

    # at this point we have emitted initial status and have a job id
    # we're about to spawn a subprocess locally to run the job 
    # we can tell the subprocess its job id via the os environment
    def _runJob(self, jDefn: JobDefn, jobContext: JobContext) -> None:
        # Putting the job in a new thread means we can easily run it asynchronously
        # while still emitting statuses before and after
        # Emit RUNNING status
        LwfManager.emitStatus(jobContext, LocalJobStatus, 
                              JobStatusValues.RUNNING.value)
        try:
            # This is synchronous, so we wait here until the subprocess is over.
            cmd = jDefn.getEntryPoint()
            if jDefn.getJobArgs() is not None:
                for arg in jDefn.getJobArgs():
                    cmd += " " + arg
            # copy the current shell environment into the subprocess
            # and inject the job id
            env = os.environ.copy()
            env['_LWFM_JOB_ID'] = jobContext.getId()
            subprocess.run(cmd, shell=True, env=env)
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


    # this is the top of the order for running a local job - there's no expectation
    # we know our job id yet, though we may - it can be passed in directly (e.g. the LwfManager
    # might run the job from an event trigger and thus know the job id a priori), or it can 
    # be passed in sub rosa via the os environment
    def submit(self, jDefn: JobDefn, useContext: JobContext = None, 
        computeType: str = None, runArgs: dict = None) -> JobStatus:
        try:
            # this is the local Run driver - there is not (as yet) any concept of 
            # "computeType" or "runArgs" as there might be on another more complex 
            # site (e.g. HPC scheduler, cloud, etc.)

            if (useContext is None):
                # we don't know our job id - it wasn't passed in
                # check the environment 
                useContext = LwfManager.getJobContextFromEnv()
                if (useContext is None):
                    # we still don't know our job id - create a new one
                    useContext = JobContext()
                    # assert readiness
                    LwfManager.emitStatus(useContext, LocalJobStatus, 
                        JobStatusValues.READY.value)

            # if we want to, we can test validity of the job defn here, reject it
            # let's say its good and carry on

            # horse at the gate...
            LwfManager.emitStatus(useContext, LocalJobStatus, 
                                JobStatusValues.PENDING.value)
            # Run the job in a new thread so we can wrap it in a bit more code
            # this will kick the status the rest of the way to a terminal state 
            multiprocessing.Process(target=self._runJob, args=[jDefn, useContext]).start()
            Logger.info("LocalSite: submitted job %s" % (useContext.getId()))
            return LwfManager.getStatus(useContext.getId())
        except Exception as ex:
            Logger.error("ERROR: Could not submit job %s" % (ex))
            return None


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

# ************************************************************************


class LocalSiteRepo(SiteRepo):
    # For a local site, a put or get is a filesystem copy.

    def _copyFile(self, fromPath: str, toPath: str) -> bool:
        try:
            toDir, toFilename = os.path.split(toPath)
            print("toDir: " + toDir)
            print("toFilename: " + toFilename)
            shutil.copy2(fromPath, os.path.join(toDir, toFilename))
        except Exception as ex:
            Logger.error("Error copying file: " + str(ex))
            return False
        return True

    def put(self, localPath: str, siteObjPath: str,  
            metasheet: Metasheet = None) -> Metasheet:
        success = True
        if (localPath is not None) and (siteObjPath is not None):
            # copy the file from localPath to siteObjPath
            success = self._copyFile(localPath, siteObjPath)
        # now do the metadata notate    
        if (success):
            return LwfManager.notate(localPath, siteObjPath, metasheet, True)
        else:
            return None
    
    def get(self, siteObjPath: str, localPath: str) -> str:
        success = True
        if (siteObjPath is not None) and (localPath is not None):
            # copy the file from siteObjPath to localPath
            success = self._copyFile(siteObjPath, localPath)
        if (success):
            LwfManager.notate(localPath, siteObjPath)
            return localPath
        else:
            return None

    def find(self, queryRegExs: dict) -> List[Metasheet]:
        return LwfManager.find(queryRegExs)




    



# ************************************************************************

class LocalSiteSpin(SiteSpin):
    
    def listComputeTypes(self) -> List[str]:
        return ["default"]


# ************************************************************************


class LocalSite(Site):

    SITE_NAME = "local"

    # There are no required args to instantiate a local site.
    def __init__(self):
        super(LocalSite, self).__init__(
            self.SITE_NAME, LocalSiteAuth(), LocalSiteRun(), LocalSiteRepo(), LocalSiteSpin()
        )



# a trivial extension of LocalSite where any calls to a remote service, exemplified 
# by the use of LwfManager, are made asynchronously as fire & forget, perhaps by 
# using an async-savvy extension of LwfManager.
# it may elect to implement a different Run driver, perhaps one which uses MPI, but not
# one which talks to a cluster scheduler - we would suggest modeling that as 
# its own Site.
# We defer that more performant implementation for the moment and just reuse 
# LocalSite with its inherent use of HTTP. 
class InSituSite(LocalSite):

    SITE_NAME = "insitu"

    def __init__(self):
        super(InSituSite, self).__init__(
            self.SITE_NAME, LocalSiteAuth(), LocalSiteRun(), LocalSiteRepo(), LocalSiteSpin()
        )



