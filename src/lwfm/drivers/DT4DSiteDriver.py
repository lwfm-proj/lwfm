
import logging
import importlib
from datetime import datetime, timezone
import time
import json
from types import SimpleNamespace
from pathlib import Path
import os
import pickle
from typing import Callable

from lwfm.base.Site import Site, SiteAuthDriver, SiteRunDriver, SiteRepoDriver
from lwfm.base.SiteFileRef import FSFileRef, SiteFileRef, RemoteFSFileRef
from lwfm.base.JobDefn import JobDefn, RepoOp
from lwfm.base.JobStatus import JobStatus, JobStatusValues, JobContext
from lwfm.base.MetaRepo import MetaRepo
from lwfm.server.JobStatusSentinelClient import JobStatusSentinelClient
from lwfm.base.JobEventHandler import JobEventHandler


from py4dt4d._internal._SecuritySvc import _SecuritySvc
from py4dt4d._internal._JobSvc import _JobSvc
from py4dt4d._internal._PyEngineUtil import _PyEngineUtil
from py4dt4d._internal._Constants import _Locations
from py4dt4d.PyEngine import PyEngine
from py4dt4d._internal._PyEngineImpl import _PyEngineImpl
from py4dt4d.Job import JobRunner
from py4dt4d.ToolRepo import ToolRepo
from py4dt4d.SimRepo import SimRepo

SERVER = _Locations.PROD_STR.value

LOCAL_COMPUTE_TYPE = "local"

JOB_SET_HANDLER_TYPE = "jobset"
DATA_HANDLER_TYPE = "data"

#************************************************************************************************************************************

# the native DT4D status strings mapped to the canonical lwfm terms
class DT4DJobStatus(JobStatus):
    def __init__(self, jobContext: JobContext):
        super(DT4DJobStatus, self).__init__(jobContext)
        self.setStatusMap({
            "UNKNOWN"     : JobStatusValues.UNKNOWN,
            "REQUESTING"  : JobStatusValues.PENDING,
            "REQUESTED"   : JobStatusValues.PENDING,
            "SUBMITTED"   : JobStatusValues.PENDING,
            "DISCOVERED"  : JobStatusValues.PENDING,
            "PENDING"     : JobStatusValues.PENDING,
            "RUNNING"     : JobStatusValues.RUNNING,
            "ANALYSIS"    : JobStatusValues.INFO,
            "MODIFY"      : JobStatusValues.INFO,
            "MOVING"      : JobStatusValues.INFO,
            "MOVED"       : JobStatusValues.INFO,
            "FINISHED"    : JobStatusValues.FINISHING,
            "COMPLETED"   : JobStatusValues.COMPLETE,
            "IMPROPER"    : JobStatusValues.FAILED,
            "FAILED"      : JobStatusValues.FAILED,
            "CANCELLED"   : JobStatusValues.CANCELLED,
            "TIMEOUT"     : JobStatusValues.CANCELLED,
        })

    def toJSON(self):
        return self.serialize()

    def serialize(self):
        out_bytes = pickle.dumps(self, 0)
        out_str = out_bytes.decode(encoding='ascii')
        return out_str

    @staticmethod
    def deserialize(s: str):
        in_json = json.dumps(s)
        in_obj = pickle.loads(json.loads(in_json).encode(encoding='ascii'))
        return in_obj


#************************************************************************************************************************************
# Auth

class DT4DSiteAuthDriver(SiteAuthDriver):
    def login(self, force: bool=False) -> bool:
        # login to DT4D
        if (not self.isAuthCurrent()):
            _SecuritySvc().freshLogin(SERVER)
        return True

    def isAuthCurrent(self) -> bool:
        # DT4D doesn't expose an "is current" endpoint - if you make a call and you're not logged in, you get prompted
        # so to avoid incessant popping of the dt4d login dialog, do a quick and dirty check on a cached token
        path = os.path.expanduser('~') + "/dt4d/tokens.txt"
        if os.path.exists(path):
            mtime = os.stat(path).st_mtime
            modified = datetime.fromtimestamp(mtime, tz=timezone.utc)
            now = datetime.now()
            delta = now.timestamp() - modified.timestamp()
            if (delta < 3000):  # TODO: completely arbitrary...
                return True
            else:
                return False
        else:
            return False

    # DT4D provides its own local token persistence
    def writeToStore(self) -> bool:
        return True

    # DT4D provides its own local token persistence
    def readFromStore(self) -> bool:
        return True


#************************************************************************************************************************************
# Run

@JobRunner
def _runRemoteJob(job, jobId, toolName, toolFile, toolClass, toolArgs, computeType, jobName, setId, timeout=0.5):
    # Run the tool on the remote computeType.
    _JobSvc(job).runRemotePyJob(job, toolName, toolFile, toolClass, toolArgs, computeType, "", setId,
                                jobName, timeout, None, None, jobId)

@JobRunner
def _job_set_event(job, jobId, toolName, toolFile, toolClass, toolArgs, computeType, waitOnSetId, jobSetNumber, setId, jobName, timeout=0):
    # Run the tool on the remote computeType.
    print("Time to register the job: toolName:" + str(toolName) + "|toolFile:" + str(toolFile) + "|toolClass:" + str(toolClass) + "|args:" + str(toolArgs) + "|computeType:" + str(computeType) + "|waitOnSetId:" + str(waitOnSetId) + "|setNum:" + str(jobSetNumber))
    _JobSvc(job).registerJob(job, toolName, toolFile, toolClass, toolArgs, computeType, "", waitOnSetId, jobSetNumber, jobType="python",
        setId=setId, jobName=jobName, triggerJobId=jobId)

@JobRunner
def _data_event(job, jobId, toolName, toolFile, toolClass, toolArgs, computeType, trigger, setId, jobName, timeout=0):
    # Run the tool on the remote computeType.
    _JobSvc(job).registerDataTrigger(job, toolName, toolFile, toolClass, toolArgs, computeType, "", trigger, jobType="python",
                                setId=setId, jobName=jobName, triggerJobId=jobId)

@JobRunner
def _unset_job_set_event(self, jobId):
    # Run the tool on the remote computeType.
    _PyEngineImpl().removeJobSetTrigger(self, jobId)

def _unset_data_event(self, jobId):
    # Run the tool on the remote computeType.
    _PyEngineImpl().removeDataTrigger(self, jobId)

def _getJobStatus(self, jobContext):
    return _getJobStatusWorker(self, jobContext)

@JobRunner
def _getAllJobs(job, startTime, endTime):
    return _JobSvc(job).queryJobStatus(startTime, endTime)

def _getJobStatusWorker(job, jobContext):
    timeNowMs = int(round(time.time() * 1000))
    startTimeMs =  timeNowMs - (99999 * 60 * 1000)
    endTimeMs = timeNowMs + int(round(99999 * 60 * 1000))
    results = _JobSvc(job).queryJobStatusByJobId(startTimeMs, endTimeMs, jobContext.getNativeId())
    stat =  _statusProcessor(results, jobContext)
    # if (stat.getParentJobId() is None):
    #     stat.setParentJobId("")
    out = stat.serialize()
    return out


class Struct:
    def __init__(self, **entries):
        self.__dict__.update(entries)

def _statusProcessor(results, context):
    status = DT4DJobStatus(context)
    currTime = 0
    currStatus = None
    for entry in results:
        x = Struct(**entry)
        if (x.dt4dReceivedTimestamp > currTime):
            currTime = x.dt4dReceivedTimestamp
            currStatus = x.status
    status.setNativeStatusStr(currStatus)
    status.getJobContext().setId(context.getId())
    return status


class DT4DSiteRunDriver(SiteRunDriver):
    def submitJob(self, jdefn: JobDefn, parentContext: JobContext = None) -> JobStatus:
        context = parentContext
        if (context is None):
            context = JobContext()
        status = DT4DJobStatus(context)
        if jdefn is None:
            status.emit("IMPROPER")
            return status

        # launch the job with DT4D
        # DT4D moodule path is python [ directory, module, file, class ]
        modulePath = jdefn.getEntryPoint()
        modulePathStr = modulePath[1] + "." + modulePath[2]

        nativeId = context.getId()  # default to lwfm id
        setId = context.getJobSetId()
        if (jdefn.getComputeType() == LOCAL_COMPUTE_TYPE):
            # run local dt4d job
            cls = getattr(importlib.import_module(modulePathStr), modulePath[3])
            try:
                # we need the native id
                jobClass = cls()
                nativeId = jobClass.getJobId()
                PyEngine().runLocal(jobClass)
            except Exception as ex:
                print("**** DT4DSiteSDriver exception while running local job " + str(ex))
        else:
            # run remote dt4d job
            #nativeId = _PyEngineUtil.generateId()
            _runRemoteJob(nativeId,
                          modulePath[0], modulePath[1], modulePath[2],
                          jdefn.getJobArgs(), jdefn.getComputeType(), jdefn.getName(), setId)

        context.setNativeId(nativeId)
        #status.setNativeId(nativeId)
        # At this point the status object we have contains the job's lwfm id and its native id.  it does not however
        # contain an accurate job status/state string - the job is being launched asynchronously, and therefore that underlying
        # runtime is going to take the job through its states.  to get the state into lwfm, we need to poll the site.
        #retval = JobStatusSentinelClient().setTerminalSentinel(context.getId(), context.getParentJobId(), context.getOriginJobId(),
        #                                                       context.getNativeId(), "dt4d")
        return status

    def getJobStatus(self, jobContext: JobContext) -> JobStatus:
        stat =  _getJobStatus(jobContext)
        status = DT4DJobStatus.deserialize(stat)
        return status

    def cancelJob(self, nativeJobId: str) -> bool:
        # not implemented
        return False


    def listComputeTypes(self) -> [str]:
        computeTypes = PyEngine.listComputeTypes(self)
        return computeTypes


    def setEventHandler(self, jdefn:JobDefn, jeh: JobEventHandler) -> JobEventHandler:
        context = jeh.getTargetContext()
        if (context is None):
            context = JobContext()
        status = DT4DJobStatus(context)
        if jeh is None:
            status.emit("IMPROPER")
            return status

        # set the handler with DT4D
        # DT4D moodule path is python [module, file, class ]
        modulePath = jdefn.getEntryPoint()

        nativeId = _PyEngineUtil.generateId()
        setId = context.getJobSetId()
        jobName = jdefn.getName()
        fireDefn = jeh.getFireDefn()
        # Getting the handler type.  There are 2 types in dt4d.  jobset handlers will fire when a job set with a specified length
        # has completed.  data handlers will fire when a file is uploaded with a given metadata set.
        handlerType = fireDefn[0]
        if handlerType.lower() == JOB_SET_HANDLER_TYPE:
            waitOnSetId = fireDefn[1]
            jobSetNumber = int(fireDefn[2])
            _job_set_event(nativeId, modulePath[0], modulePath[1], modulePath[2],
                        jdefn.getJobArgs(), jdefn.getComputeType(), waitOnSetId, jobSetNumber, setId, jobName)
        elif handlerType.lower() == DATA_HANDLER_TYPE:
            trigger = fireDefn[1]
            _data_event(nativeId, modulePath[0], modulePath[1], modulePath[2],
                        jdefn.getJobArgs(), jdefn.getComputeType(), trigger, setId, jobName)
        context.setNativeId(nativeId)
        # At this point the status object we have contains the job's lwfm id and its native id.  it does not however
        # contain an accurate job status/state string - the job is being launched asynchronously, and therefore that underlying
        # runtime is going to take the job through its states.  to get the state into lwfm, we need to poll the site.
        #retval = JobStatusSentinelClient().setTerminalSentinel(context.getId(), context.getParentJobId(), context.getOriginJobId(),
        #                                                       context.getNativeId(), "dt4d")
        return status


    def unsetEventHandler(self, jeh: JobEventHandler) -> bool:
        handlerType = jeh.getFireDefn()[0]
        print("UNSETTING " + handlerType + ": " + jeh.getId())
        if(handlerType.lower() == JOB_SET_HANDLER_TYPE):
            _unset_job_set_event(jeh.getId())
            #_PyEngineImpl.removeJobSetTrigger(self, self, jeh.getId())
        elif(handlerType.lower() == DATA_HANDLER_TYPE):
            _unset_data_event(jeh.getId())
            #_PyEngineImpl.removeDataTrigger(self, self, jeh.getId())

    def listEventHandlers(self) -> [JobEventHandler]:
        eventHandlers = PyEngine.listRegisteredJobs(self)
        eventHandlers.extend(PyEngine.listDataTriggers(self))
        return eventHandlers
        
    def getJobList(self, startTime: int, endTime: int) -> [JobStatus]:
        return _getAllJobs(startTime, endTime)


#************************************************************************************************************************************
# Repo

@JobRunner
def repoPut(job, path, metadata={}):
    return SimRepo(job).put(path, metadata)

@JobRunner
def repoGet(job, docId, path=""):
    return SimRepo(job).getByDocId(docId, path)

@JobRunner
def repoFindById(job, docId):
    return SimRepo(job).getMetadataByDocId(docId)

@JobRunner
def repoFindByMetadata(job, metadata):
    return SimRepo(job).getMetadataByMetadata(metadata)

class Dt4DSiteRepoDriver(SiteRepoDriver):

    def _getSession(self):
        authDriver = DT4DSiteAuthDriver()
        authDriver.login()
        return authDriver._session

    def put(self, localRef: Path, siteRef: SiteFileRef, jobContext: JobContext = None) -> SiteFileRef:
        # Book keeping for status emissions
        iAmAJob = False
        if (jobContext is None):
            iAmAJob = True
            jobContext = JobContext()
        status = DT4DJobStatus(jobContext)
        if (iAmAJob):
            # emit the starting job status sequence
            status.emit("PENDING")
            status.emit("RUNNING")

        # Emit our info status before hitting the API
        status.setNativeInfo(JobStatus.makeRepoInfo(RepoOp.PUT, False, str(localRef), ""))
        status.emit("MOVING")

        repoPut(localRef, siteRef.getMetadata())

        status.emit("MOVED")

        if (iAmAJob):
            # emit the successful job ending sequence
            status.emit("FINISHED")
            status.emit("COMPLETED")
        MetaRepo.notate(SiteFileRef)
        return SiteFileRef

    def get(self, siteRef: SiteFileRef, localRef: Path, jobContext: JobContext = None) -> Path:
        # Book keeping for status emissions
        iAmAJob = False
        if (jobContext is None):
            iAmAJob = True
            jobContext = JobContext()
        status = DT4DJobStatus(jobContext)
        if (iAmAJob):
            # emit the starting job status sequence
            status.emit("PENDING")
            status.emit("RUNNING")

        # Emit our info status before hitting the API
        status.setNativeInfo(JobStatus.makeRepoInfo(RepoOp.PUT, False, "", str(localRef)))
        status.emit("MOVING")

        getFile = repoGet(siteRef.getId(), localRef)

        status.emit("MOVED")

        if (iAmAJob):
            # emit the successful job ending sequence
            status.emit("FINISHED")
            status.emit("COMPLETED")
        #MetaRepo.Notate(SiteFileRef)
        return getFile

    def find(self, siteRef: SiteFileRef) -> [SiteFileRef]:
        sheets = None
        if(siteRef.getId()):
            sheets = repoFindById(siteRef.getId())
        elif(siteRef.getMetadata()):   
            sheets = repoFindByMetadata(siteRef.getMetadata())
        print(str(sheets))
        remoteRefs = []
        for sheet in sheets:
            remoteRef = FSFileRef()
            remoteRef.setId(sheet["id"])
            if "resourceName" in sheet:
                remoteRef.setName(sheet["resourceName"])
            remoteRef.setSize(sheet["fileSizeBytes"])
            remoteRef.setTimestamp(sheet["timestamp"])
            remoteRefs.append(remoteRef)
        return remoteRefs

#************************************************************************************************************************************

#_repoDriver = LocalSiteRepoDriver()

class DT4DSite(Site):
    # There are no required args to instantiate a local site.
    def __init__(self):
        super(DT4DSite, self).__init__("dt4d", DT4DSiteAuthDriver(), DT4DSiteRunDriver(), Dt4DSiteRepoDriver(), None)



#************************************************************************************************************************************


# test
if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)

    # define the DT4D site (which is known to model distinct "compute type" resources within it), and login
#    site = Site("dt4d", DT4DSiteAuthDriver(), DT4DSiteRunDriver())
#    site.getAuthDriver().login()

    # define the job
#    jdefn = JobDefn()
#    jdefn.setName("HelloWorld")
#    jdefn.setEntryPointPath([ "/Users/212578984/src/dt4d/py4dt4d", "py4dt4d-examples", "HelloWorld", "HelloWorld" ])

    # run it local
#    jdefn.setComputeType(LOCAL_COMPUTE_TYPE)
#    status = site.getRunDriver().submitJob(jdefn)
#    print("Local run status = " + str(status.getStatus()))

    # run it remote on a named node type
#    jdefn.setComputeType("Win-VDrive")
#    status = site.getRunDriver().submitJob(jdefn)
#    while (not status.isTerminal()):
#        print("Remote run status = " + str(status.getStatus()) + " ...waiting another 15 seconds for job to finish")
#        time.sleep(15)
#        status = site.getRunDriver().getJobStatus(status.getNativeId())
#    print("Remote run status = " + str(status.getStatus()))


    site = DT4DSite()
    site.getAuthDriver().login()
    context = JobContext()
    context.setId("ae19db11-d52f-4d2f-b298-2864bc7840b7")
    context.setNativeId("ae19db11-d52f-4d2f-b298-2864bc7840b7")
    status = site.getRunDriver().getJobStatus(context)
    print("*** " + str(status))
    print("Remote run status = " + str(status.getStatus()))


#************************************************************************************************************************************
