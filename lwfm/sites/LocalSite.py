"""
LocalSiteDriver: an implementation of Site and its constituent Auth, Run, Repo 
and (trivial) Spin interfaces for a local to the user runtime environment.  
Unsecure, as this is local and we assume the user is themselves already.
"""

#pylint: disable = missing-function-docstring, invalid-name, missing-class-docstring
#pylint: disable = broad-exception-caught

import shutil
from typing import List, Union
import os
import subprocess
import multiprocessing

from lwfm.base.Site import Site, SiteAuth, SiteRun, SiteRepo, SiteSpin
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus, JobStatusValues
from lwfm.base.JobContext import JobContext
from lwfm.base.Metasheet import Metasheet
from lwfm.base.Workflow import Workflow
from lwfm.midware.LwfManager import lwfManager
from lwfm.midware.Logger import logger


# *********************************************************************


class LocalJobStatus(JobStatus):
    def __init__(self, context: JobContext = None):
        super().__init__(context)
        # use default canonical status map inherited from the base class
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
        return lwfManager.getStatus(jobId)

    # at this point we have emitted initial status and have a job id
    # we're about to spawn a subprocess locally to run the job
    # we can tell the subprocess its job id via the os environment
    def _runJob(self, jDefn: JobDefn, jobContext: JobContext) -> None:
        # Putting the job in a new thread means we can easily run it asynchronously
        # while still emitting statuses before and after
        # Emit RUNNING status
        lwfManager.emitStatus(jobContext, LocalJobStatus,
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
            env['_LWFM_JOB_ID'] = jobContext.getJobId()
            subprocess.run(cmd, shell=True, env=env)
            # Emit success statuses
            lwfManager.emitStatus(jobContext, LocalJobStatus,
                                  JobStatusValues.FINISHING.value)
            lwfManager.emitStatus(jobContext, LocalJobStatus,
                                  JobStatusValues.COMPLETE.value)
        except Exception as ex:
            logger.error(f"ERROR: Job failed: {ex}")
            # Emit FAILED status
            lwfManager.emitStatus(jobContext, LocalJobStatus,
                                  JobStatusValues.FAILED.value)


    # this is the top of the order for running a local job - there's no expectation
    # we know our job id yet, though we may - it can be passed in directly (e.g. the lwfManager
    # might run the job from an event trigger and thus know the job id a priori), or it can
    # be passed in sub rosa via the os environment
    def submit(self, jobDefn: JobDefn, parentContext: Union[JobContext, Workflow] = None,
        computeType: str = None, runArgs: dict = None) -> JobStatus:
        try:
            # this is the local Run driver - there is not (as yet) any concept of
            # "computeType" or "runArgs" as there might be on another more complex
            # site (e.g. HPC scheduler, cloud, etc.)
            if parentContext is None:
                # we don't know our job id - it wasn't passed in
                # check the environment
                useContext = lwfManager.getJobContextFromEnv()
                if useContext is None:
                    # we still don't know our job id - create a new one
                    useContext = JobContext()
                    # assert readiness
                    lwfManager.emitStatus(useContext, LocalJobStatus,
                        JobStatusValues.READY.value)
            elif isinstance(parentContext, JobContext):
                useContext = parentContext
            elif isinstance(parentContext, Workflow):
                useContext = JobContext()
                useContext.setWorkflowId(parentContext.getWorkflowId())
                useContext.setName(parentContext.getName())

            # TODO 
            # if we want to, we can test validity of the job defn here, reject it
            # let's say its good and carry on

            # horse at the gate...
            lwfManager.emitStatus(useContext, LocalJobStatus,
                                JobStatusValues.PENDING.value)
            # Run the job in a new thread so we can wrap it in a bit more code
            # this will kick the status the rest of the way to a terminal state
            multiprocessing.Process(target=self._runJob, args=[jobDefn, useContext]).start()
            logger.info(f"LocalSite: submitted job {useContext.getJobId()}")
            return lwfManager.getStatus(useContext.getJobId())
        except Exception as ex:
            logger.error(f"ERROR: Could not submit job {ex}")
            return None


    def cancel(self, jobContext: JobContext) -> bool:
        pass
        # # Find the locally running thread and kill it
        # try:
        #     thread = self._pendingJobs[jobContext.getId()]
        #     if thread is None:
        #         return False
        #     logger.info(
        #         "LocalSiteDriver.cancelJob(): calling terminate on job "
        #         + jobContext.getId()
        #     )
        #     thread.terminate()
        #     jStatus = LocalJobStatus(jobContext)
        #     jStatus.emit(JobStatusValues.CANCELLED.value)
        #     self._pendingJobs[jobContext.getId()] = None
        #     return True
        # except Exception as ex:
        #     logger.error(
        #         "ERROR: Could not cancel job %d: %s" % (jobContext.getId(), ex)
        #     )
        #     return False

# ************************************************************************


class LocalSiteRepo(SiteRepo):
    # For a local site, a put or get is a filesystem copy.

    def _copyFile(self, fromPath: str, toPath: str) -> bool:
        try:
            toDir, toFilename = os.path.split(toPath)
            shutil.copy2(fromPath, os.path.join(toDir, toFilename))
        except Exception as ex:
            logger.error("Error copying file: " + str(ex))
            return False
        return True

    def put(self, localPath: str, siteObjPath: str,
            jobContext: JobContext = None, metasheet: Metasheet = None) -> Metasheet:
        context = jobContext
        if context is None:
            context = JobContext()
            print("put - site in context is " + context.getSiteName())
        if jobContext is None:
            # we drive job state, else we are already part of some other job
            print("*** going to emit")
            lwfManager.emitStatus(context, LocalJobStatus, JobStatusValues.RUNNING.value)
            print("*** bck from emit")
        success = True
        if (localPath is not None) and (siteObjPath is not None):
            # copy the file from localPath to siteObjPath
            success = self._copyFile(localPath, siteObjPath)
        # now do the metadata notate
        if success:
            if jobContext is None:
                lwfManager.emitStatus(context, LocalJobStatus,
                    JobStatusValues.FINISHING.value)
            sheet = lwfManager.notatePut(localPath, siteObjPath, context, metasheet)
            if jobContext is None:
                lwfManager.emitStatus(context, LocalJobStatus,
                    JobStatusValues.COMPLETE.value)
            return sheet
        if jobContext is None:
            lwfManager.emitStatus(context, LocalJobStatus,
                JobStatusValues.FAILED.value)
        return None

    def get(self, siteObjPath: str, localPath: str, jobContext: JobContext = None) -> str:
        context = jobContext
        if context is None:
            context = JobContext()
            lwfManager.emitStatus(context, LocalJobStatus, JobStatusValues.RUNNING.value)
        success = True
        if (siteObjPath is not None) and (localPath is not None):
            # copy the file from siteObjPath to localPath
            success = self._copyFile(siteObjPath, localPath)
        # now do the metadata notate
        if success:
            if jobContext is None:
                lwfManager.emitStatus(context, LocalJobStatus,
                    JobStatusValues.FINISHING.value)
            lwfManager.notateGet(localPath, siteObjPath, context)
            if jobContext is None:
                lwfManager.emitStatus(context, LocalJobStatus,
                JobStatusValues.COMPLETE.value)
            return localPath
        if jobContext is None:
            lwfManager.emitStatus(context, LocalJobStatus,
                JobStatusValues.FAILED.value)
        return None

    def find(self, queryRegExs: dict) -> List[Metasheet]:
        return lwfManager.find(queryRegExs)


# ************************************************************************

class LocalSiteSpin(SiteSpin):

    def listComputeTypes(self) -> List[str]:
        return ["default"]


# ************************************************************************


class LocalSite(Site):

    SITE_NAME = "local"

    def __init__(self):
        super().__init__(
            self.SITE_NAME,
            LocalSiteAuth(),
            LocalSiteRun(),
            LocalSiteRepo(),
            LocalSiteSpin()
        )
