
# Job Status: a record of a state of the job's execution.  The job may go through many states in its lifetime - on the actual
# runtime Site the job status will be expressed in terms of their native status codes.  In lwfm, we desire canonical status
# messages so job chaining is permitted.  Its the role of the Site's Run subsystem to produce these datagrams in their
# canonical form, though we leave room to express the native info too.  There is no firm state transition / state machine for
# job status - while "PENDING" means "submitted to run but not yet running", and "COMPLETE" means "job is done done stick a fork
# in it", in truth the Site is free to emit whateever status code it desires at any moment.  Some status codes might be emitted
# more than once (e.g. "INFO").  We provide a mechanism to track the job's parent-child relationships.


from enum import Enum
import logging

from datetime import datetime
import json
import requests

from lwfm.base.LwfmBase import LwfmBase, _IdGenerator
from lwfm.base.JobDefn import JobDefn
from lwfm.server.JobStatusSentinelClient import JobStatusSentinelClient


class _JobStatusFields(Enum):
    STATUS        = "status"
    NATIVE_STATUS = "nativeStatus"
    EMIT_TIME     = "emitTime"
    RECEIVED_TIME = "receivedTime"
    ID            = "id"
    NATIVE_ID     = "nativeId"
    NAME          = "name"
    PARENT_JOB_ID = "parentJobId"
    ORIGIN_JOB_ID = "originJobId"
    NATIVE_INFO   = "nativeInfo"
    SITE_NAME     = "siteName"


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
    def __init__(self):
        super(JobContext, self).__init__()
        self.setId(_IdGenerator.generateId())
        self.setNativeId(self.getId())
        self.setParentJobId(None)                       # a seminal job has no parent
        self.setOriginJobId(self.getId())               # a seminal job is its own originator

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


#************************************************************************************************************************************


class JobStatus(LwfmBase):

    # status:           JobStatusValues
    # nativeStatus:     str
    statusMap:          dict = None                             # maps native status to canonical status
    # emitTime:         datetime
    # receivedTime:     datetime
    # id:               str                                     # its possible two native systems generated the same id
    # nativeId:         str
    # name:             str                                     # optional
    # parentJobId:      str                                     # this job's direct parent, if not a seminal job
    # originJobId:      str                                     # this job's furthest ancestor, if not itself if seminal job
    statusHistory:      dict = None                             # history of status messages, not copied by copy constructor
    # nativeInfo:       str                                     # arbitrary body of info passed in the native status message
    # siteName          str                                     # the site source for this job status

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
        self.setReceivedTime(datetime.utcnow())
        self.setId(jobContext.getId())
        self.setNativeId(jobContext.getNativeId())
        self.setStatus(JobStatusValues.UNKNOWN)
        self.setParentJobId(jobContext.getParentJobId())
        self.setOriginJobId(jobContext.getOriginJobId())

    def emit(self) -> bool:
        try:
            jssc = JobStatusSentinelClient()
            jssc.emitStatus(self.getId(), self.getStatus().value)
            return True
        except Exception as ex:
            logging.error(str(ex))
            return False


    def getJobContext(self) -> JobContext:
        jobContext = JobContext()
        jobContext.setId(self.getId())
        jobContext.setNativeId(self.getNativeId())
        jobContext.setParentJobId(self.getParentJobId())
        jobContext.setOriginJobId(self.getOriginJobId())
        return jobContext

    def setStatus(self, status: JobStatusValues) -> None:
        LwfmBase._setArg(self, _JobStatusFields.STATUS.value, status)

    def getStatus(self) -> JobStatusValues:
        return LwfmBase._getArg(self, _JobStatusFields.STATUS.value)

    def isTerminal(self) -> bool:
        return ( (self.getStatus() == JobStatusValues.COMPLETE) or
                 (self.getStatus() == JobStatusValues.FAILED)   or
                 (self.getStatus() == JobStatusValues.CANCELLED) )

    def getStatusValue(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.STATUS.value)

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
        LwfmBase._setArg(self, _JobStatusFields.EMIT_TIME.value, emitTime)

    def getEmitTime(self) -> datetime:
        return LwfmBase._getArg(self, _JobStatusFields.EMIT_TIME.value)

    def setReceivedTime(self, receivedTime: datetime) -> None:
        LwfmBase._setArg(self, _JobStatusFields.RECEIVED_TIME.value, receivedTime)

    def getReceivedTime(self) -> datetime:
        return LwfmBase._getArg(self, _JobStatusFields.RECEIVED_TIME.value)

    def setId(self, idValue: str) -> None:
        LwfmBase._setArg(self, _JobStatusFields.ID.value, idValue)

    def getId(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.ID.value)

    def setNativeId(self, idValue: str) -> None:
        LwfmBase._setArg(self, _JobStatusFields.NATIVE_ID.value, idValue)

    def getNativeId(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.NATIVE_ID.value)

    def setName(self, name: str) -> None:
        LwfmBase._setArg(self, _JobStatusFields.NAME.value, name)

    def getName(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.NAME.value)

    def setParentJobId(self, idValue: str) -> None:
        LwfmBase._setArg(self, _JobStatusFields.PARENT_JOB_ID.value, idValue)

    def getParentJobId(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.PARENT_JOB_ID.value)

    def setOriginJobId(self, idValue: str) -> None:
        LwfmBase._setArg(self, _JobStatusFields.ORIGIN_JOB_ID.value, idValue)

    def getOriginJobId(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.ORIGIN_JOB_ID.value)

    def setNativeInfo(self, info: str) -> None:
        LwfmBase._setArg(self, _JobStatusFields.NATIVE_INFO.value, idValue)

    def getNativeInfo(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.NATIVE_INFO.value)

    def setStatusHistory(self, history: dict) -> None:
        self.statusHistory = history

    def getStatusHistory(self) -> dict:
        return self.statusHistory

    def setSiteName(self, name: str) -> None:
        LwfmBase._setArg(self, _JobStatusFields.SITE_NAME.value, name)

    def getSiteName(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.SITE_NAME.value)

    def toJsonString(self) -> str:
        return json.dumps(self.getArgs(), sort_keys=True, default=str)



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
    logging.info(status.toJsonString())
