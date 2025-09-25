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
import signal
import subprocess
import multiprocessing
import time

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
            logger.error(f"*** {jDefn}", context=jobContext)
            logger.error(f"ERROR: Job failed: {ex}", context=jobContext)
            # Emit FAILED status
            lwfManager.emitStatus(jobContext, JobStatus.FAILED)


    def _run_with_redirect(self, jobDefn, useContext, log_file_path) -> None:
        # Make this process a new session leader so we can kill its process group on cancel
        try:
            os.setsid()
        except Exception:
            # setsid may not be available on some platforms; continue best-effort
            pass
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
                runArgs: Optional[Union[dict, str, list, tuple]]=None) -> JobStatus:
        # if we are passed a string, assume it is a job definition
        if isinstance(jobDefn, str):
            jobDefn = lwfManager.deserialize(jobDefn)
        if isinstance(parentContext, str):
            parentContext = lwfManager.deserialize(parentContext)
        if isinstance(runArgs, str):
            runArgs = lwfManager.deserialize(runArgs)
        useContext = None
        try:
            # this is the local Run driver - there is not (as yet) any concept of
            # "computeType" as there might be on another more complex
            # site (e.g. HPC scheduler, cloud, etc.)
            if isinstance(parentContext, JobContext):
                useContext = parentContext
                useContext.setSiteName(self.getSiteName())
            elif isinstance(parentContext, Workflow):
                useContext = JobContext()
                useContext.setSiteName(self.getSiteName())
                useContext.setWorkflowId(parentContext.getWorkflowId())
                useContext.setName(parentContext.getName() or "")
            else:
                # we don't know our job id - it wasn't passed in
                # check the environment
                useContext = lwfManager._getJobContextFromEnv()
                if useContext is None:
                    # we still don't know our job id - create a new one
                    useContext = JobContext()
                    useContext.setSiteName(self.getSiteName())
                    # assert readiness
                    lwfManager.emitStatus(useContext, JobStatus.READY)
                else:
                    useContext.setSiteName(self.getSiteName())

            # if we have runArgs, append them to the entryPoint space delimited
            jobDefn = cast(JobDefn, jobDefn)
            if runArgs is not None:
                if isinstance(runArgs, dict):
                    runArgs = " ".join([f"{k}={v}" for k, v in sorted(runArgs.items())])
                elif isinstance(runArgs, (list, tuple)):
                    runArgs = " ".join(str(x) for x in runArgs)
                else:
                    runArgs = str(runArgs)
                jobDefn.setEntryPoint(str(jobDefn.getEntryPoint()) + " " + runArgs)

            # horse at the gate...
            lwfManager.emitStatus(useContext, JobStatus.PENDING)

            # put out an info message with what we know about the job to be run
            lwfManager.emitStatus(useContext, JobStatus.INFO, None,
                str({"jobDefn": str(jobDefn), "jobContext": str(useContext),
                "computeType": str(computeType), "runArgs": str(runArgs)}))

            # create a log file for this job
            logFilename = lwfManager.getLogFilename(useContext)

            # Run the job in a new thread so we can wrap it in a bit more code
            # this will kick the status the rest of the way to a terminal state
            logger.info(f"LocalSite: submitting job {useContext.getJobId()}",
                context=useContext)
            proc = multiprocessing.Process(
                target=self._run_with_redirect,
                args=[jobDefn, useContext, logFilename]
            )
            proc.start()
            # Track the spawned process PID for cancellation
            try:
                self._pendingJobs[useContext.getJobId()] = proc.pid
            except Exception:
                # Best-effort tracking; continue even if we can't record it
                pass
            return lwfManager.getStatus(useContext.getJobId())
        except Exception as ex:
            logger.error(f"ERROR: Could not submit job {ex}", context=useContext)
            raise ex


    def cancel(self, jobContext: Union[JobContext, str]) -> bool:
        # Resolve job id and optional context (avoid recursion on bad setJobId usage)
        ctx = None
        job_id: Optional[str] = None
        if isinstance(jobContext, JobContext):
            ctx = jobContext
            job_id = ctx.getJobId()
        elif isinstance(jobContext, str):
            # Treat as a jobId directly; optionally try to deserialize if it's a serialized context
            job_id = jobContext
            try:
                maybe = lwfManager.deserialize(jobContext)
                if isinstance(maybe, JobContext):
                    ctx = maybe
                    job_id = ctx.getJobId() or job_id
            except Exception:
                pass
        if not job_id:
            logger.error("LocalSite.cancel(): no job id provided", context=ctx)
            return False

        # Small grace period, then force kill if still alive
        logger.info("LocalSite.cancel(): waiting 5s to avoid a race condition", context=ctx)
        time.sleep(5)

        try:
            pid = self._pendingJobs.get(job_id)
            if not pid:
                logger.error(f"LocalSite.cancel(): no running process found for job {job_id}",
                    context=ctx)
                return False
            # Attempt graceful termination of the whole process group
            try:
                pgid = os.getpgid(pid)
                os.killpg(pgid, signal.SIGTERM)
            except ProcessLookupError:
                # Already exited
                pass
            except Exception as ex:
                logger.error(f"LocalSite.cancel(): SIGTERM failed for job {job_id}: {ex}",
                    context=ctx)

            # force kill if still alive
            try:
                os.kill(pid, 0)
                # still alive; force kill the group
                try:
                    pgid = os.getpgid(pid)
                    os.killpg(pgid, signal.SIGKILL)
                except Exception:
                    pass
            except ProcessLookupError:
                # Process is gone
                pass

            # Emit CANCELLED status
            try:
                status = lwfManager.getStatus(job_id)
                if status is None:
                    raise Exception("Could not retrieve job status")
                lwfManager.emitStatus(status.getJobContext(), JobStatus.CANCELLED)
            except Exception:
                # Best-effort status emit
                pass

            # Cleanup tracking
            try:
                self._pendingJobs.pop(job_id, None)
            except Exception:
                pass
            return True
        except Exception as ex:
            logger.error(f"LocalSite.cancel(): error cancelling job {job_id}: {ex}",
                context=ctx)
            return False

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
            jobContext: Optional[Union[JobContext, Workflow, str]] = None,
            metasheet: Optional[Union[Metasheet, dict, str]] = None) -> Optional[Metasheet]:
        if isinstance(jobContext, str):
            jobContext = lwfManager.deserialize(jobContext)
        if isinstance(jobContext, Workflow):
            _jobContext = JobContext()
            _jobContext.setWorkflowId(jobContext.getWorkflowId())
            jobContext = _jobContext
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
            jobContext: Optional[Union[JobContext, Workflow, str]] = None,
            metasheet: Optional[Union[Metasheet, dict, str]] = None) -> Optional[str]:
        if isinstance(jobContext, str):
            jobContext = lwfManager.deserialize(jobContext)
        if isinstance(jobContext, Workflow):
            _jobContext = JobContext()
            _jobContext.setWorkflowId(jobContext.getWorkflowId())
            jobContext = _jobContext
        if isinstance(metasheet, str):
            metasheet = lwfManager.deserialize(metasheet)
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
            _metasheet = None
            if metasheet is None:
                _metasheet = Metasheet(self.getSiteName(), localPath, siteObjPath, {})
            if isinstance(metasheet, dict):
                _metasheet = Metasheet(self.getSiteName(), localPath, siteObjPath, cast(dict, metasheet))
                _metasheet.setJobId(context.getJobId())
            lwfManager._notateGet(self.getSiteName(), localPath, siteObjPath, context, _metasheet)
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
