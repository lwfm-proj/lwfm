"""
LocalSiteDriver: an implementation of Site and its constituent Auth, Run, Repo 
and (trivial) Spin interfaces for a local to the user runtime environment.  
Purposefully unsecure, as this is local and we assume the user is themselves already.
"""

#pylint: disable = missing-function-docstring, invalid-name, missing-class-docstring
#pylint: disable = broad-exception-caught, protected-access, broad-exception-raised
#pylint: disable = attribute-defined-outside-init

import shutil
from typing import List, Union, Optional, cast
import os
import subprocess
import multiprocessing

from lwfm.base.Site import SiteAuth, SiteRun, SiteRepo, SiteSpin
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus
from lwfm.base.JobContext import JobContext
from lwfm.base.Metasheet import Metasheet
from lwfm.base.Workflow import Workflow
from lwfm.midware.LwfManager import lwfManager, logger


# *********************************************************************


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
        return lwfManager.getStatus(jobId) # type: ignore

    # at this point we have emitted initial status and have a job id
    # we're about to spawn a subprocess locally to run the job
    # we can tell the subprocess its job id via the os environment
    def _runJob(self, jDefn: JobDefn, jobContext: JobContext) -> None:
        # Putting the job in a new thread means we can easily run it asynchronously
        # while still emitting statuses before and after
        # Emit RUNNING status
        lwfManager.emitStatus(jobContext, JobStatus.RUNNING)
        try:
            # This is synchronous, so we wait here until the subprocess is over.
            cmd = jDefn.getEntryPoint()
            if cmd is None:
                lwfManager.emitStatus(jobContext, JobStatus.FAILED)
                logger.error("ERROR: Job definition has no entry point")
                return
            if jDefn.getEntryPointType() == JobDefn.ENTRY_TYPE_SHELL or \
                jDefn.getEntryPointType() == JobDefn.ENTRY_TYPE_STRING:
                if jDefn.getJobArgs() is not None:
                    for arg in jDefn.getJobArgs():
                        cmd += " " + str(arg)
            elif jDefn.getEntryPointType() == JobDefn.ENTRY_TYPE_SITE:
                # this is a common function - delegate it to the lwfManager
                lwfManager.execSiteEndpoint(jDefn, jobContext, False)
            else:
                raise Exception("Unknown entry point type")
            # copy the current shell environment into the subprocess
            # and inject the job id
            env = os.environ.copy()
            env['_LWFM_JOB_ID'] = jobContext.getJobId()

            # Modify to redirect all output to the file or /dev/null if no file is
            # specified.
            if hasattr(self, '_output_file') and self._output_file:
                # Redirect all output to the file
                cmd = f"{cmd} > {self._output_file} 2>&1"

            subprocess.run(cmd, shell=True, env=env, check=True)
            # Emit success statuses
            lwfManager.emitStatus(jobContext, JobStatus.FINISHING)
            lwfManager.emitStatus(jobContext, JobStatus.COMPLETE)
        except Exception as ex:
            logger.error(f"*** {jDefn}")
            logger.error(f"ERROR: Job failed: {ex}")
            # Emit FAILED status
            lwfManager.emitStatus(jobContext, JobStatus.FAILED)


    def _run_with_redirect(self, jobDefn, useContext, log_file_path) -> None:
        # Store the output file path for _runJob to use
        self._output_file = log_file_path
        try:
            self._runJob(jobDefn, useContext)
        finally:
            self._output_file = None



    # this is the top of the order for running a local job - there's no expectation
    # we know our job id yet, though we may - it can be passed in directly (e.g. the lwfManager
    # might run the job from an event trigger and thus know the job id a priori), or it can
    # be passed in sub rosa via the os environment
    def submit(self, jobDefn: Union[JobDefn, str],
                parentContext: Optional[Union[JobContext, Workflow, str]]=None,
                computeType: Optional[str]=None,
                runArgs: Optional[Union[dict, str]]=None) -> JobStatus:
        # if we are passed a string, assume it is a job definition
        if isinstance(jobDefn, str):
            jobDefn = lwfManager.deserialize(jobDefn)
        if isinstance(parentContext, str):
            parentContext = lwfManager.deserialize(parentContext)
        if isinstance(runArgs, str):
            runArgs = lwfManager.deserialize(runArgs)
        try:
            # this is the local Run driver - there is not (as yet) any concept of
            # "computeType" or "runArgs" as there might be on another more complex
            # site (e.g. HPC scheduler, cloud, etc.)
            if isinstance(parentContext, JobContext):
                useContext = parentContext
            elif isinstance(parentContext, Workflow):
                useContext = JobContext()
                useContext.setWorkflowId(parentContext.getWorkflowId())
                useContext.setName(parentContext.getName() or "")
            else:
                # we don't know our job id - it wasn't passed in
                # check the environment
                useContext = lwfManager._getJobContextFromEnv()
                if useContext is None:
                    # we still don't know our job id - create a new one
                    useContext = JobContext()
                    # assert readiness
                    lwfManager.emitStatus(useContext, JobStatus.READY)

            # horse at the gate...
            lwfManager.emitStatus(useContext, JobStatus.PENDING)

            # create a log file for this job
            logFilename = lwfManager.getLogFilename(useContext)

            # Run the job in a new thread so we can wrap it in a bit more code
            # this will kick the status the rest of the way to a terminal state
            logger.info(f"LocalSite: submitting job {useContext.getJobId()}")
            multiprocessing.Process(target=self._run_with_redirect,
                args=[jobDefn, useContext, logFilename]).start()
            return lwfManager.getStatus(useContext.getJobId())
        except Exception as ex:
            logger.error(f"ERROR: Could not submit job {ex}")
            raise ex


    def cancel(self, jobContext: Union[JobContext, str]) -> bool:
        return False
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
        #     jStatus.emit(JobStatus.CANCELLED)
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
            jobContext: Optional[Union[JobContext, str]] = None,
            metasheet: Optional[Union[Metasheet, dict, str]] = None) -> Optional[Metasheet]:
        if isinstance(jobContext, str):
            jobContext = lwfManager.deserialize(jobContext)
        if isinstance(metasheet, str):
            metasheet = lwfManager.deserialize(metasheet)
        if isinstance(metasheet, dict):
            metasheet = Metasheet(self.getSiteName(), localPath, siteObjPath, metasheet)
        context: JobContext = jobContext  #type: ignore
        if context is None:
            context = JobContext()
        if jobContext is None:
            # we drive job state, else we are already part of some other job
            lwfManager.emitStatus(context, JobStatus.RUNNING)
        if isinstance(metasheet, str):
            metasheet = lwfManager.deserialize(metasheet)
        success = True
        if (localPath is not None) and (siteObjPath is not None):
            # copy the file from localPath to siteObjPath
            success = self._copyFile(localPath, siteObjPath)
        # now do the metadata notate
        if success:
            if jobContext is None:
                lwfManager.emitStatus(context, JobStatus.FINISHING)
            sheet = lwfManager._notatePut(self.getSiteName(), localPath, siteObjPath,
                                          context, metasheet)  #type: ignore
            if jobContext is None:
                lwfManager.emitStatus(context, JobStatus.COMPLETE)
            return sheet
        if jobContext is None:
            lwfManager.emitStatus(context, JobStatus.FAILED)
        return None

    def get(self, siteObjPath: str, localPath: str,
            jobContext: Optional[Union[JobContext, str]] = None) -> Optional[str]:
        if isinstance(jobContext, str):
            jobContext = lwfManager.deserialize(jobContext)
        context = jobContext
        if context is None:
            context = JobContext()
            lwfManager.emitStatus(context, JobStatus.RUNNING)
        else:
            context = cast(JobContext, context)
        success = True
        if (siteObjPath is not None) and (localPath is not None):
            # copy the file from siteObjPath to localPath
            success = self._copyFile(siteObjPath, localPath)
        # now do the metadata notate
        if success:
            if jobContext is None:
                lwfManager.emitStatus(context, JobStatus.FINISHING)
            # TODO lwfManager.notateGet(localPath, siteObjPath, context)
            if jobContext is None:
                lwfManager.emitStatus(context, JobStatus.COMPLETE)
            return localPath
        if jobContext is None:
            lwfManager.emitStatus(context, JobStatus.FAILED,
                JobStatus.FAILED)
        return None


# ************************************************************************

class LocalSiteSpin(SiteSpin):

    def listComputeTypes(self) -> List[str]:
        return ["default"]


# ************************************************************************
