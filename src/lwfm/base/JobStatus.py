
from enum import Enum
from abc import ABC, abstractmethod
import logging
import uuid

from datetime import datetime

from LwfmBase import LwfmBase


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


class JobStatusValues(Enum):
    UNKNOWN   = "UNKNOWN"
    PENDING   = "PENDING"
    RUNNING   = "RUNNING"
    INFO      = "INFO"
    FINISHING = "FINISHING"
    COMPLETE  = "COMPLETE"
    FAILED    = "FAILED"
    CANCELLED = "CANCELLED"


class _IdGenerator:
    @staticmethod
    def generateId():
        return str(uuid.uuid4())


class JobStatus(LwfmBase, ABC):

    # status:           JobStatusValues
    # nativeStatus:     str
    statusMap:          dict[str, JobStatusValues] = None       # maps native status to canonical status
    # emitTime:         datetime
    # receivedTime:     datetime
    # id:               str                                     # its possible two native systems generated the same id
    # nativeId:         str
    # name:             str                                     # optional
    # parentJobId:      str                                     # this job's direct parent, if not a seminal job
    # originJobId:      str                                     # this job's furthest ancestor, if not itself if seminal job
    statusHistory:      dict[datetime, LwfmBase] = None         # history of status messages, not copied by copy constructor
    # nativeInfo:       str                                     # arbitrary body of info passed in the native status message

    def __init__(self, args: dict[str, type] = None):
        super(JobStatus, self).__init__(args)
        # default map
        self.setStatusMap( {
            "UNKNOWN"   : JobStatusValues.UNKNOWN,
            "PENDING"   : JobStatusValues.PENDING,
            "RUNNING"   : JobStatusValues.RUNNING,
            "INFO"      : JobStatusValues.INFO,
            "FINISHING" : JobStatusValues.FINISHING,
            "COMPLETE"  : JobStatusValues.COMPLETE,
            "FAILED"    : JobStatusValues.FAILED,
        })
        self.setReceivedTime(datetime.utcnow())
        if self.getId() is None:
            self.setId(_IdGenerator.generateId())

    def setStatus(self, status: JobStatusValues) -> None:
        LwfmBase._setArg(self, _JobStatusFields.STATUS.value, status)

    def getStatus(self) -> JobStatusValues:
        return LwfmBase._getArg(self, _JobStatusFields.STATUS.value)

    def getStatusValue(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.STATUS.value).value

    def setNativeStatusString(self, status: str) -> None:
        LwfmBase._setArg(self, _JobStatusFields.NATIVE_STATUS.value, status)
        self.mapNativeStatus()

    def getNativeStatusString(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.NATIVE_STATUS.value)

    def mapNativeStatus(self) -> None:
        try:
            self.setStatus(self.statusMap[self.getNativeStatusString()])
        except Exception as ex:
            logging.error("Unable to map the native status to canonical: {}".format(ex.message))
            self.setStatus(JobStatusValues.UNKNOWN)

    def getStatusMap(self) -> dict[str, JobStatusValues]:
        return self.statusMap

    def setStatusMap(self, statusMap: dict[str, JobStatusValues]) -> None:
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

    def setStatusHistory(self, history: dict[datetime, LwfmBase]) -> None:
        self.statusHistory = history

    def getStatusHistory(self) -> dict[datetime, LwfmBase]:
        return self.statusHistory


# test
if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    status = JobStatus()
    statusMap = {
        "NODE_FAIL" : JobStatusValues.FAILED
        }
    status.setStatusMap(statusMap)
    status.setNativeStatusString("NODE_FAIL")
    logging.info(status.getId() + " " + status.getStatusValue())
