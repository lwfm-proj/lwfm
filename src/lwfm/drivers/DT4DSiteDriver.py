
import logging
import importlib
from datetime import datetime, timezone
import time
import json
from types import SimpleNamespace
from pathlib import Path
import os
import pickle

from lwfm.base.Site import Site, SiteAuthDriver, SiteRunDriver, SiteRepoDriver
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus, JobStatusValues, JobContext
from lwfm.base.MetaRepo import MetaRepo
from lwfm.server.JobStatusSentinelClient import JobStatusSentinelClient


from py4dt4d._internal._SecuritySvc import _SecuritySvc
from py4dt4d._internal._JobSvc import _JobSvc
from py4dt4d._internal._PyEngineUtil import _PyEngineUtil
from py4dt4d._internal._Constants import _Locations
from py4dt4d.PyEngine import PyEngine
from py4dt4d.Job import JobRunner
from py4dt4d.ToolRepo import ToolRepo

SERVER = _Locations.PROD_STR.value

LOCAL_COMPUTE_TYPE = "local"


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
        self.setSiteName("dt4d")


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
def _runRemoteJob(job, jobId, toolName, toolFile, toolClass, toolArgs, computeType, email, timeout=0.5):
    # Put the file referened by script into ToolRepo with the given name and auto-increment the version
    ToolRepo(job).putTool(toolFile, toolName, None)
    # Run the tool on the remote computeType.
    _JobSvc(job).runRemotePyJob(job, toolName, toolName, toolName, toolArgs, computeType, email, jobId,
                                toolName, timeout, None, None, jobId)


@JobRunner
def _getJobStatus(job, jobContext):
    return _getJobStatusWorker(job, jobContext)

def _getJobStatusWorker(job, jobContext):
    timeNowMs = int(round(time.time() * 1000))
    startTimeMs =  timeNowMs - (99999 * 60 * 1000)
    endTimeMs = timeNowMs + int(round(99999 * 60 * 1000))
    results = _JobSvc(job).queryJobStatusByJobId(startTimeMs, endTimeMs, jobContext.getNativeId())
    stat =  _statusProcessor(results, jobContext)
    if (stat.getParentJobId() is None):
        stat.setParentJobId("")
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
    status.setId(context.getId())
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
        if (jdefn.getComputeType() == LOCAL_COMPUTE_TYPE):
            # run local dt4d job
            cls = getattr(importlib.import_module(modulePathStr), modulePath[3])
            try:
                # we need the native id
                jobClass = cls()
                nativeId = jobClass.getJobId()
                PyEngine().runLocal(jobClass)
            except ex as Exception:
                print("**** DT4DSiteSDriver exception while running local job " + str(ex))
        else:
            # run remote dt4d job
            nativeId = _PyEngineUtil.generateId()
            _runRemoteJob(nativeId,
                          jdefn.getName(), modulePath[0] + "/" + modulePath[1] + "/" + modulePath[2] + ".py",
                          modulePath[3],
                          jdefn.getJobArgs(), jdefn.getComputeType(), jdefn.getNotificationEmail())

        context.setNativeId(nativeId)
        status.setNativeId(nativeId)
        # At this point the status object we have contains the job's lwfm id and its native id.  it does not however
        # contain an accurate job status/state string - the job is being launched asynchronously, and therefore that underlying
        # runtime is going to take the job through its states.  to get the state into lwfm, we need to poll the site.
        retval = JobStatusSentinelClient().setTerminalSentinel(context.getId(), context.getParentJobId(), context.getOriginJobId(),
                                                               context.getNativeId(), "dt4d")
        return status

    def getJobStatus(self, jobContext: JobContext) -> JobStatus:
        stat =  _getJobStatus(jobContext)
        status = DT4DJobStatus.deserialize(stat)
        return status

    def cancelJob(self, nativeJobId: str) -> bool:
        # not implemented
        return False


#************************************************************************************************************************************
# Repo


#************************************************************************************************************************************

#_repoDriver = LocalSiteRepoDriver()

class DT4DSite(Site):
    # There are no required args to instantiate a local site.
    def __init__(self):
        super(DT4DSite, self).__init__("dt4d", DT4DSiteAuthDriver(), DT4DSiteRunDriver(), None, None)



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
