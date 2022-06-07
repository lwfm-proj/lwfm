
# Job Status: a record of a state of the job's execution.  The job may go through many states in its lifetime - on the actual
# runtime Site the job status will be expressed in terms of their native status codes.  In lwfm, we desire canonical status
# messages so job chaining is permitted.  Its the role of the Site's Run subsystem to produce these datagrams in their
# canonical form, though we leave room to express the native info too.  There is no firm state transition / state machine for
# job status - while "PENDING" means "submitted to run but not yet running", and "COMPLETE" means "job is done done stick a fork
# in it", in truth the Site is free to emit whateever status code it desires at any moment.  Some status codes might be emitted
# more than once (e.g. "INFO").  We provide a mechanism to track the job's parent-child relationships.


from enum import Enum
import logging
from types import SimpleNamespace


from datetime import datetime
import requests
import pickle

from lwfm.base.LwfmBase import LwfmBase, _IdGenerator
from lwfm.base.JobDefn import JobDefn, RepoOp
from lwfm.server.JobStatusSentinelClient import JobStatusSentinelClient


class _JobStatusFields(Enum):
    STATUS        = "status"                         # canonical status
    NATIVE_STATUS = "nativeStatus"                   # the status code for the specific Run implementation
    EMIT_TIME     = "emitTime"
    RECEIVED_TIME = "receivedTime"
    ID            = "id"                             # canonical job id
    NATIVE_ID     = "nativeId"                       # Run implementation native job id
    NAME          = "name"                           # optional human-readable job name
    PARENT_JOB_ID = "parentJobId"                    # immediate predecessor of this job, if any - seminal job has no parent
    ORIGIN_JOB_ID = "originJobId"                    # oldest ancestor - a seminal job is its own originator
    NATIVE_INFO   = "nativeInfo"                     # any additional info the native Run wants to put in the status message
    SITE_NAME     = "siteName"                       # name of the Site which emitted the message


# The canonical set of status codes.  Run implementations will have their own sets, and they must provide a mapping into these.
class JobStatusValues(Enum):
    UNKNOWN   = "UNKNOWN"
    PENDING   = "PENDING"
    RUNNING   = "RUNNING"
    INFO      = "INFO"
    FINISHING = "FINISHING"
    COMPLETE  = "COMPLETE"
    FAILED    = "FAILED"
    CANCELLED = "CANCELLED"


#************************************************************************************************************************************
# A job runs in the context of a runtime id (two ids - one which is canonical to lwfm, and one which is native to the site), and
# references to upstream job which spawned it, to permit later navigation of the digital thread.

import traceback

class JobContext(LwfmBase):
    def __init__(self, parentContext = None):
        super(JobContext, self).__init__()
        self.setId(_IdGenerator.generateId())
        self.setNativeId(self.getId())
        if (parentContext is not None):
            self.setParentJobId(parentContext.getParentJobId())
            self.setOriginJobId(parentContext.getOriginJobId())
        else:
            self.setParentJobId(None)                   # a seminal job has no parent
            self.setOriginJobId(self.getId())           # a seminal job is its own originator
        self.setName(self.getId())

    def setId(self, idValue: str) -> None:
        LwfmBase._setArg(self, _JobStatusFields.ID.value, idValue)

    def getId(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.ID.value)

    def setNativeId(self, idValue: str) -> None:
            LwfmBase._setArg(self, _JobStatusFields.NATIVE_ID.value, idValue)

    def getNativeId(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.NATIVE_ID.value)

    def setParentJobId(self, idValue: str) -> None:
        LwfmBase._setArg(self, _JobStatusFields.PARENT_JOB_ID.value, idValue)

    def getParentJobId(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.PARENT_JOB_ID.value)

    def setOriginJobId(self, idValue: str) -> None:
        LwfmBase._setArg(self, _JobStatusFields.ORIGIN_JOB_ID.value, idValue)

    def getOriginJobId(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.ORIGIN_JOB_ID.value)

    def setName(self, name: str) -> None:
        LwfmBase._setArg(self, _JobStatusFields.NAME.value, name)

    def getName(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.NAME.value)

    def serialize(self):
        return pickle.dumps(self, 0)

    @staticmethod
    def deserialize(s: str):
        arr = bytes(s, 'ascii')
        return pickle.loads(arr)


#************************************************************************************************************************************


class JobStatus(LwfmBase):

    statusMap:          dict = None                             # maps native status to canonical status
    statusHistory:      dict = None                             # history of status messages, not copied by copy constructor
    jobContext:         JobContext = None                       # job id tracking info

    def __init__(self, jobContext: JobContext = None, args: dict = None):
        super(JobStatus, self).__init__(args)
        if (jobContext is None):
            jobContext = JobContext()
        # default map
        self.setStatusMap( {
            "UNKNOWN"   : JobStatusValues.UNKNOWN,
            "PENDING"   : JobStatusValues.PENDING,
            "RUNNING"   : JobStatusValues.RUNNING,
            "INFO"      : JobStatusValues.INFO,
            "FINISHING" : JobStatusValues.FINISHING,
            "COMPLETE"  : JobStatusValues.COMPLETE,
            "FAILED"    : JobStatusValues.FAILED,
            "CANCELLED" : JobStatusValues.CANCELLED
        })
        self.jobContext = jobContext
        if jobContext is None:
            self.jobContext = JobContext()
        self.setReceivedTime(datetime.utcnow())
        self.setStatus(JobStatusValues.UNKNOWN)


    def emit(self, status: str = None) -> bool:
        if status:
            self.setNativeStatusStr(status)
            self.setEmitTime(datetime.utcnow())
        try:
            jssc = JobStatusSentinelClient()
            jssc.emitStatus(self.getId(), self.getStatus().value, self.serialize())
            return True
        except Exception as ex:
            logging.error(str(ex))
            return False


    def getJobContext(self) -> JobContext:
        return self.jobContext

    def setJobContext(self, jobContext: JobContext) -> None:
        self.jobContext = jobContext

    def setStatus(self, status: JobStatusValues) -> None:
        LwfmBase._setArg(self, _JobStatusFields.STATUS.value, status)

    def getStatus(self) -> JobStatusValues:
        return LwfmBase._getArg(self, _JobStatusFields.STATUS.value)

    def isTerminal(self) -> bool:
        return ( (self.getStatus() == JobStatusValues.COMPLETE) or
                 (self.getStatus() == JobStatusValues.FAILED)   or
                 (self.getStatus() == JobStatusValues.CANCELLED) )

    def getStatusValue(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.STATUS.value).value

    def setNativeStatusStr(self, status: str) -> None:
        LwfmBase._setArg(self, _JobStatusFields.NATIVE_STATUS.value, status)
        self.mapNativeStatus()

    def getNativeStatusStr(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.NATIVE_STATUS.value)

    def mapNativeStatus(self) -> None:
        try:
            self.setStatus(self.statusMap[self.getNativeStatusStr()])
        except Exception as ex:
            logging.error("Unable to map the native status to canonical: {}".format(ex))
            self.setStatus(JobStatusValues.UNKNOWN)

    def getStatusMap(self) -> dict:
        return self.statusMap

    def setStatusMap(self, statusMap: dict) -> None:
        self.statusMap = statusMap

    def setEmitTime(self, emitTime: datetime) -> None:
        LwfmBase._setArg(self, _JobStatusFields.EMIT_TIME.value, emitTime.timestamp() * 1000)

    def getEmitTime(self) -> datetime:
        ms = int(LwfmBase._getArg(self, _JobStatusFields.EMIT_TIME.value))
        return datetime.utcfromtimestamp(ms//1000).replace(microsecond=ms%1000*1000)

    def setReceivedTime(self, receivedTime: datetime) -> None:
        LwfmBase._setArg(self, _JobStatusFields.RECEIVED_TIME.value, receivedTime.timestamp() * 1000)

    def getReceivedTime(self) -> datetime:
        ms = LwfmBase._getArg(self, _JobStatusFields.RECEIVED_TIME.value)
        return datetime.utcfromtimestamp(ms//1000).replace(microsecond=ms%1000*1000)

    def setId(self, idValue: str) -> None:
        self.jobContext.setId(idValue)

    def getId(self) -> str:
        return self.jobContext.getId()

    def setNativeId(self, idValue: str) -> None:
        self.jobContext.setNativeId(idValue)

    def getNativeId(self) -> str:
        return self.jobContext.getNativeId()

    def setName(self, name: str) -> None:
        self.jobContext.setName(name)

    def getName(self) -> str:
        return self.jobContext.getName()

    def setParentJobId(self, idValue: str) -> None:
        self.jobContext.setParentJobId(idValue)

    def getParentJobId(self) -> str:
        return self.jobContext.getParentJobId()

    def setOriginJobId(self, idValue: str) -> None:
        self.jobContext.setOriginJobId(idValue)

    def getOriginJobId(self) -> str:
        return self.jobContext.getOriginJobId()

    def setNativeInfo(self, info: str) -> None:
        LwfmBase._setArg(self, _JobStatusFields.NATIVE_INFO.value, info)

    def getNativeInfo(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.NATIVE_INFO.value)

    def setStatusHistory(self, history: dict) -> None:
        self._statusHistory = history

    def getStatusHistory(self) -> dict:
        return self._statusHistory

    def setSiteName(self, name: str) -> None:
        LwfmBase._setArg(self, _JobStatusFields.SITE_NAME.value, name)

    def getSiteName(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.SITE_NAME.value)

    def serialize(self):
        return pickle.dumps(self, 0)

    @staticmethod
    def deserialize(s: str):
        arr = bytes(s, 'ascii')
        return pickle.loads(arr)


    @staticmethod
    def makeRepoInfo(verb: RepoOp, success: bool, fromPath: str, toPath: str) -> str:
        return ("[" + verb.value + "," + str(success) + "," + fromPath + "," + toPath + "]")

    def toString(self) -> str:
        s = ("" + str(self.getId()) + "," + str(self.getParentJobId()) + "," + str(self.getOriginJobId()) + "," +
            str(self.getEmitTime()) + "," + str(self.getStatusValue()) + "," + str(self.getSiteName()))
        if (self.getStatus() == JobStatusValues.INFO):
            s += "," + self.getNativeInfo()
        return s


#************************************************************************************************************************************


# test
if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    status = JobStatus()
    statusMap = {
        "NODE_FAIL" : JobStatusValues.FAILED
        }
    status.setStatusMap(statusMap)
    status.setNativeStatusStr("NODE_FAIL")
    status.setEmitTime(datetime.utcnow())

    logging.info(status.serialize())
