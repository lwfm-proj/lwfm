# LocalSiteDriver: an implementation of Site and its constituent Auth, Run, Repo interfaces
# for a local to the user runtime environment.  Unsecure, as this is local and we assume
# the user is themselves already.

# TODO logging
import logging
from typing import List
import os
import shutil
import multiprocessing
import pickle
import json
import math

from datetime import datetime
from pathlib import Path

from lwfm.base.Site import Site, SiteAuth, SiteRun, SiteRepo
from lwfm.base.SiteFileRef import SiteFileRef, FSFileRef
from lwfm.base.JobDefn import JobDefn, RepoJobDefn, RepoOp
from lwfm.base.JobStatus import JobStatus, JobStatusValues
from lwfm.base.JobContext import JobContext
from lwfm.midware.LwfManager import LwfManager
from lwfm.base.LwfmBase import LwfmBase
from lwfm.midware.impl.BasicMetaRepoStore import BasicMetaRepoStore
from lwfm.midware.Logger import Logger


# *********************************************************************

# TODO make more flexible with configuration
SITE_NAME = "local"
LwfmBase._shortJobIds = True


# *********************************************************************


class LocalJobStatus(JobStatus):
    def __init__(self, context: JobContext = None):
        super(LocalJobStatus, self).__init__(context)
        # use default canonical status map
        self.getJobContext().setSiteName(SITE_NAME)


# *************************************************************************************


class LocalSiteAuth(SiteAuth):
    # Because this is running locally, we don't need any authentication
    def login(self, force: bool = False) -> bool:
        return True

    def isAuthCurrent(self) -> bool:
        return True


# *************************************************************************************


class LocalSiteRun(SiteRun):
    _pendingJobs = {}

    def getStatus(self, jobId: str) -> JobStatus:
        return LwfManager.getStatus(jobId)

    def _runJob(self, jDefn: JobDefn, jobContext: JobContext) -> None:
        # Putting the job in a new thread means we can easily run it asynchronously
        # while still emitting statuses before and after
        # Emit RUNNING status
        LwfManager.emitStatus(jobContext, LocalJobStatus, JobStatusValues.RUNNING)
        try:
            # This is synchronous, so we wait here until the subprocess is over.
            # Check=True raises an exception on non-zero returns
            if isinstance(jDefn, RepoJobDefn):
                # run the repo job
                if jDefn.getRepoOp() == RepoOp.PUT:
                    _repoDriver.put(
                        jDefn.getLocalRef(),
                        jDefn.getSiteFileRef(),
                        jobContext,
                    )
                elif jDefn.getRepoOp() == RepoOp.GET:
                    _repoDriver.get(
                        jDefn.getSiteFileRef(),
                        jDefn.getLocalRef(),
                        jobContext,
                    )
                else:
                    logging.error("Unknown repo operation")
            else:
                # run a command line job
                cmd = jDefn.getEntryPoint()
                if jDefn.getJobArgs() is not None:
                    for arg in jDefn.getJobArgs():
                        cmd += " " + arg
                os.system(cmd)
            # Emit success statuses
            LwfManager.emitStatus(jobContext, LocalJobStatus, JobStatusValues.FINISHING)
            LwfManager.emitStatus(jobContext, LocalJobStatus, JobStatusValues.COMPLETE)
        except Exception as ex:
            logging.error("ERROR: Job failed %s" % (ex))
            # Emit FAILED status
            LwfManager.emitStatus(jobContext, LocalJobStatus, JobStatusValues.FAILED)


    def submit(self, jDefn: JobDefn, useContext: JobContext = None) -> JobStatus:
        if (useContext is None):
            useContext = JobContext()
            # we can test validity of the job defn here, reject it, or say its ready
            # if we were given a context, then we assume its ready 
            LwfManager.emitStatus(useContext, LocalJobStatus, JobStatusValues.READY)
        # horse at the gate...
        LwfManager.emitStatus(useContext, LocalJobStatus, JobStatusValues.PENDING)
        # Run the job in a new thread so we can wrap it in a bit more code
        # this will kick the status the rest of the way to a terminal state 
        multiprocessing.Process(target=self._runJob, args=[jDefn, useContext]).start()
        return LwfManager.getStatus(useContext.getId())




    def cancel(self, jobContext: JobContext) -> bool:
        # Find the locally running thread and kill it
        try:
            thread = self._pendingJobs[jobContext.getId()]
            if thread is None:
                return False
            logging.info(
                "LocalSiteDriver.cancelJob(): calling terminate on job "
                + jobContext.getId()
            )
            thread.terminate()
            jstatus = LocalJobStatus(jobContext)
            jstatus.emit(JobStatusValues.CANCELLED.value)
            self._pendingJobs[jobContext.getId()] = None
            return True
        except Exception as ex:
            logging.error(
                "ERROR: Could not cancel job %d: %s" % (jobContext.getId(), ex)
            )
            return False

    def listComputeTypes(self) -> List[str]:
        return ["local"]

    def getJobList(self, startTime: int, endTime: int) -> List[JobStatus]:
        statuses = []
        serializedStatuses = WorkflowEventClient().getStatuses()
        if serializedStatuses is None:
            serializedStatuses = []
        for serializedStatus in serializedStatuses:
            status = LocalJobStatus.deserialize(serializedStatus)
            startDate = datetime.fromtimestamp(math.ceil(startTime / 1000))
            endDate = datetime.fromtimestamp(math.ceil(endTime / 1000))

            def subtract_hours(date, hours):
                return date - datetime.timedelta(hours=hours)

            statusDate = subtract_hours(status.getEmitTime(), 8)

            if statusDate > startDate and statusDate < endDate:
                status.setEmitTime(statusDate)
                statuses.append(status)
        return statuses


# *************************************************************************************


class LocalSiteRepo(SiteRepo):

    _metaRepo = None

    def __init__(self):
        super(LocalSiteRepo, self).__init__()
        self._metaRepo = BasicMetaRepoStore()

    def _makeRepoInfo(
        self,
        verb: RepoOp,
        success: bool,
        fromPath: str,
        toPath: str,
        metadata: dict = {},
    ) -> str:
        metadata["_verb"] = verb.value
        metadata["_success"] = success
        metadata["_fromPath"] = fromPath
        metadata["_toPath"] = toPath
        return str(metadata)

    def _copyFile(self, fromPath, toPath, jobContext, direction, metadata=None):
        if metadata is None:
            metadata = {}
        iAmOwnJob = False
        if jobContext is None:
            iAmOwnJob = True
            jobContext = JobContext()

        jstatus = JobStatus(jobContext)
        if iAmOwnJob:
            # emit the starting job status sequence
            jstatus.emit(JobStatusValues.PENDING.value)
            jstatus.emit(JobStatusValues.RUNNING.value)

        jstatus.setNativeInfo(
            self._makeRepoInfo(direction, True, str(fromPath), str(toPath), metadata)
        )
        jstatus.emit(JobStatusValues.INFO.value)

        try:
            toDir, toFilename = os.path.split(toPath)
            shutil.copy2(fromPath, os.path.join(toDir, toFilename))
        except Exception as ex:
            logging.error("Error copying file: " + str(ex))
            if iAmOwnJob:
                jstatus.emit(JobStatusValues.FAILED.value)
            return False

        if iAmOwnJob:
            jstatus.emit(JobStatusValues.FINISHING.value)
            jstatus.emit(JobStatusValues.COMPLETE.value)
        return True

    # If we're given a context we are a child of it, else we consider ourselves our own job.
    def put(
        self, localRef: Path, siteRef: SiteFileRef, jobContext: JobContext = None
    ) -> SiteFileRef:
        fromPath = localRef
        toPath = siteRef.getPath() + os.sep + siteRef.getName()
        if not self._copyFile(
            fromPath, toPath, jobContext, RepoOp.PUT, siteRef.getMetadata()
        ):
            return None

        newSiteFileRef = FSFileRef.siteFileRefFromPath(toPath)
        newSiteFileRef.setMetadata(siteRef.getMetadata())
        self._metaRepo.notate(newSiteFileRef)
        return newSiteFileRef

    def get(
        self, siteRef: SiteFileRef, localRef: Path, jobContext: JobContext = None
    ) -> Path:
        fromPath = siteRef.getPath() + os.sep + siteRef.getName()
        toPath = localRef
        if not self._copyFile(fromPath, toPath, jobContext, RepoOp.GET):
            return False

        # return success result
        return Path(str(toPath + os.sep + siteRef.getName()))

    def find(self, siteFileRef: SiteFileRef) -> List[SiteFileRef]:
        return self._metaRepo.find(siteFileRef)


# *************************************************************************************

_repoDriver = LocalSiteRepo()


class LocalSite(Site):
    # There are no required args to instantiate a local site.
    def __init__(self):
        super(LocalSite, self).__init__(
            SITE_NAME, LocalSiteAuth(), LocalSiteRun(), _repoDriver, None
        )
