# LocalSiteDriver: an implementation of Site and its constituent Auth, Run, Repo interfaces
# for a local to the user runtime environment.  Unsecure, as this is local and we assume
# the user is themselves already.

import logging

import os
import shutil
import multiprocessing
import time
import pickle
import json
import math

from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path

from lwfm.base.Site import Site, SiteAuthDriver, SiteRunDriver, SiteRepoDriver
from lwfm.base.SiteFileRef import SiteFileRef, FSFileRef
from lwfm.base.JobDefn import JobDefn, RepoJobDefn, RepoOp
from lwfm.base.JobStatus import JobStatus, JobStatusValues, JobContext, fetchJobStatus
from lwfm.base.WorkflowEventTrigger import JobEventHandler
from lwfm.server.JobStatusSentinelClient import JobStatusSentinelClient
from lwfm.base.LwfmBase import LwfmBase
from lwfm.store import BasicMetaRepoStore


# *************************************************************************************

# TODO make more flexible with configuration
SITE_NAME = "local"
LwfmBase._shortJobIds = True


# *************************************************************************************


class LocalJobStatus(JobStatus):
    def __init__(self, jcontext: JobContext = None):
        super(LocalJobStatus, self).__init__(jcontext)
        # use default canonical status map
        self.getJobContext().setSiteName(SITE_NAME)

    def toJSON(self):
        return self.serialize()

    def serialize(self):
        out_bytes = pickle.dumps(self, 0)
        out_str = out_bytes.decode(encoding="ascii")
        return out_str

    @staticmethod
    def deserialize(s: str):
        in_json = json.dumps(s)
        in_obj = pickle.loads(json.loads(in_json).encode(encoding="ascii"))
        return in_obj


# *************************************************************************************


class LocalSiteAuthDriver(SiteAuthDriver):
    # Because this is running locally, we don't need any authentication
    def login(self, force: bool = False) -> bool:
        return True

    def isAuthCurrent(self) -> bool:
        return True


# *************************************************************************************


class LocalSiteRunDriver(SiteRunDriver):
    _pendingJobs = {}

    def _runJob(self, jdefn, jobStatus):
        # Putting the job in a new thread means we can easily run it asynchronously
        # while still emitting statuses before and after
        # Emit RUNNING status
        jobStatus.emit(JobStatusValues.RUNNING.value)
        try:
            # This is synchronous, so we wait here until the subprocess is over.
            # Check=True raises an exception on non-zero returns
            if isinstance(jdefn, RepoJobDefn):
                # run the repo job
                if jdefn.getRepoOp() == RepoOp.PUT:
                    _repoDriver.put(
                        jdefn.getLocalRef(),
                        jdefn.getSiteFileRef(),
                        jobStatus.getJobContext(),
                    )
                elif jdefn.getRepoOp() == RepoOp.GET:
                    print("in the deep logic i am a get")
                    _repoDriver.get(
                        jdefn.getSiteFileRef(),
                        jdefn.getLocalRef(),
                        jobStatus.getJobContext(),
                    )
                else:
                    logging.error("Unknown repo operation")
            else:
                # run a command line job
                cmd = jdefn.getEntryPoint()
                if jdefn.getJobArgs() is not None:
                    for arg in jdefn.getJobArgs():
                        cmd += " " + arg
                os.system(cmd)
            # Emit success statuses
            jobStatus.emit(JobStatusValues.FINISHING.value)
            jobStatus.emit(JobStatusValues.COMPLETE.value)
        except Exception as ex:
            logging.error("ERROR: Job failed %s" % (ex))
            # Emit FAILED status
            jobStatus.emit(JobStatusValues.FAILED.value)

    def submitJob(
        self, jdefn: JobDefn, parentContext: JobContext = None, fromEvent: bool = False
    ) -> JobStatus:
        if parentContext is None:
            myContext = JobContext()
        elif fromEvent:
            myContext = parentContext
        else:
            # i am a child of the context given 
            myContext = JobContext.makeChildJobContext(parentContext)
        # In local jobs, we spawn the job in a new child process
        jstatus = LocalJobStatus(myContext)

        # Let the sentinel know the job is ready unless this is from an event
        # (in which case the sentinel already knows)
        if not fromEvent:
            jstatus.emit(JobStatusValues.PENDING.value)
        # Run the job in a new thread so we can wrap it in a bit more code
        thread = multiprocessing.Process(target=self._runJob, args=[jdefn, jstatus])
        thread.start()
        self._pendingJobs[jstatus.getJobContext().getId()] = thread
        return jstatus

    def getJobStatus(self, jobContext: JobContext) -> JobStatus:
        return fetchJobStatus(jobContext.getId())

    def cancelJob(self, jobContext: JobContext) -> bool:
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

    def listComputeTypes(self) -> [str]:
        return ["local"]

    def setEventHandler(self, jeh: JobEventHandler) -> JobStatus:
        if jeh.getTargetSiteName() is None:
            jeh.setTargetSiteName("local")
        newJobId = JobStatusSentinelClient().setEventHandler(
            jeh.getJobId(),
            jeh.getStatus().value,
            jeh.getFireDefn(),
            jeh.getTargetSiteName(),
        )
        return fetchJobStatus(newJobId)

    def unsetEventHandler(self, jeh: JobEventHandler) -> bool:
        raise NotImplementedError()

    def listEventHandlers(self) -> [JobEventHandler]:
        raise NotImplementedError()

    def getJobList(self, startTime: int, endTime: int) -> [JobStatus]:
        statuses = []
        serializedStatuses = JobStatusSentinelClient().getStatuses()
        if serializedStatuses is None:
            serializedStatuses = []
        for serializedStatus in serializedStatuses:
            status = LocalJobStatus.deserialize(serializedStatus)
            startDate = datetime.fromtimestamp(math.ceil(startTime / 1000))
            endDate = datetime.fromtimestamp(math.ceil(endTime / 1000))
            statusDate = status.getEmitTime() - relativedelta(hours=8)
            print("The start date of the range: {}".format(startDate))
            print("The start date of the status: {}".format(statusDate))
            print("The end date of the range: {}".format(endDate))
            if statusDate > startDate and statusDate < endDate:
                status.setEmitTime(statusDate)
                statuses.append(status)
        return statuses


# *************************************************************************************


class LocalSiteRepoDriver(SiteRepoDriver):

    _metaRepo = None

    def __init__(self):
        super(LocalSiteRepoDriver, self).__init__()
        self._metaRepo = BasicMetaRepoStore.BasicMetaRepoStore()

    def _copyFile(self, fromPath, toPath, jobContext, direction, metadata={}):
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
            JobStatus.makeRepoInfo(direction, True, str(fromPath), str(toPath), 
                                   metadata)
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
        if not self._copyFile(fromPath, toPath, jobContext, RepoOp.PUT, 
                              siteRef.getMetadata()):
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


    def find(self, siteFileRef: SiteFileRef) -> [SiteFileRef]:
        return self._metaRepo.find(siteFileRef)


# *************************************************************************************

_repoDriver = LocalSiteRepoDriver()

class LocalSite(Site):
    # There are no required args to instantiate a local site.
    def __init__(self):
        super(LocalSite, self).__init__(
            SITE_NAME, LocalSiteAuthDriver(), LocalSiteRunDriver(), _repoDriver, None
        )


