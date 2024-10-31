
from enum import Enum
from datetime import datetime

from lwfm.base.LwfmBase import LwfmBase
from lwfm.base.JobContext import JobContext

class _JobStatusFields(Enum):
    STATUS = "status"   # canonical status
    NATIVE_STATUS = (
        "nativeStatus"  # the status code for the specific Run implementation
    )
    EMIT_TIME = "emitTime"
    RECEIVED_TIME = "receivedTime"
    NATIVE_INFO = "nativeInfo"      # the site-specific status body


# The canonical set of lwfm status codes.  Run implementations will have their 
# own sets, and they must provide a mapping into these.
class JobStatusValues(Enum):
    UNKNOWN = "UNKNOWN"
    READY = "READY"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    INFO = "INFO"
    FINISHING = "FINISHING"
    COMPLETE = "COMPLETE"  # terminal state
    FAILED = "FAILED"  # terminal state
    CANCELLED = "CANCELLED"  # terminal state


# ***********************************************************************


class JobStatus(LwfmBase):
    """
    A record of a state of the job's execution.  The job may go through many
    states in its lifetime - on the actual runtime Site the job status will be 
    expressed in terms of their native status codes.  In lwfm, we desire 
    canonical status codes so job chaining is permitted across sites.  Its the
    role of the Site's Run subsystem to produce these datagrams in their 
    canonical form, though we leave room to express the native info too.  Some 
    status codes might be emitted more than once (e.g. "INFO").

    The JobStatus references the JobContext of the running job, which contains
    the id of the job and other originating information.

    Specific sites implement their own subclass of JobStatus to provide their own 
    status map - the mapping of Site-native status strings to canonical
    status strings.  Native lwfm local jobs will use a pass-thru mapping.
    """

    statusMap: dict = None  # maps native status to canonical status
    jobContext: JobContext = None  # job id tracking info

    def __init__(self, jobContext: JobContext = None):
        super(JobStatus, self).__init__(None)
        if jobContext is None:
            self.jobContext = JobContext()
        else:
            self.jobContext = jobContext
        # default map
        self.setStatusMap(
            {
                "UNKNOWN": JobStatusValues.UNKNOWN,
                "READY": JobStatusValues.READY,
                "PENDING": JobStatusValues.PENDING,
                "RUNNING": JobStatusValues.RUNNING,
                "INFO": JobStatusValues.INFO,
                "FINISHING": JobStatusValues.FINISHING,
                "COMPLETE": JobStatusValues.COMPLETE,
                "FAILED": JobStatusValues.FAILED,
                "CANCELLED": JobStatusValues.CANCELLED,
            }
        )
        self.setReceivedTime(datetime.utcnow())
        self.setStatus(JobStatusValues.UNKNOWN)

    def getJobContext(self) -> JobContext:
        return self.jobContext

    def setJobContext(self, jobContext: JobContext) -> None:
        self.jobContext = jobContext

    def getJobId(self) -> str:
        return self.jobContext.getId()

    def setStatus(self, status: JobStatusValues) -> None:
        LwfmBase._setArg(self, _JobStatusFields.STATUS.value, status)

    def getStatus(self) -> JobStatusValues:
        return LwfmBase._getArg(self, _JobStatusFields.STATUS.value)

    def getStatusValue(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.STATUS.value).value

    def setNativeStatusStr(self, status: str) -> None:
        LwfmBase._setArg(self, _JobStatusFields.NATIVE_STATUS.value, status)
        # now map the native status to a canonical
        self.mapNativeStatus()

    def getNativeStatusStr(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.NATIVE_STATUS.value)

    def setNativeStatus(self, nativeStatus: JobStatusValues) -> None:
        self.setNativeStatusStr(nativeStatus.value)

    def mapNativeStatus(self) -> None:
        try:
            self.setStatus(self.statusMap[self.getNativeStatusStr()])
        except Exception as ex:
            self.setStatus(JobStatusValues.UNKNOWN)

    def getStatusMap(self) -> dict:
        return self.statusMap

    def setStatusMap(self, statusMap: dict) -> None:
        self.statusMap = statusMap

    def setEmitTime(self, emitTime: datetime) -> None:
        LwfmBase._setArg(
            self, _JobStatusFields.EMIT_TIME.value, emitTime.timestamp() * 1000
        )

    def getEmitTime(self) -> datetime:
        try:
            ms = int(LwfmBase._getArg(self, _JobStatusFields.EMIT_TIME.value))
            return datetime.utcfromtimestamp(ms // 1000).replace(
                microsecond=ms % 1000 * 1000
            )
        except Exception as ex:
            return datetime.now()

    def setReceivedTime(self, receivedTime: datetime) -> None:
        LwfmBase._setArg(
            self, _JobStatusFields.RECEIVED_TIME.value, 
            receivedTime.timestamp() * 1000
        )

    def getReceivedTime(self) -> datetime:
        ms = int(LwfmBase._getArg(self, _JobStatusFields.RECEIVED_TIME.value))
        return datetime.utcfromtimestamp(ms // 1000).replace(
            microsecond=ms % 1000 * 1000
        )

    def setNativeInfo(self, info: str) -> None:
        LwfmBase._setArg(self, _JobStatusFields.NATIVE_INFO.value, info)

    def getNativeInfo(self) -> str:
        return LwfmBase._getArg(self, _JobStatusFields.NATIVE_INFO.value)

    def isTerminalSuccess(self) -> bool:
        return self.getStatus() == JobStatusValues.COMPLETE

    def isTerminalFailure(self) -> bool:
        return self.getStatus() == JobStatusValues.FAILED

    def isTerminalCancelled(self) -> bool:
        return self.getStatus() == JobStatusValues.CANCELLED

    def isTerminal(self) -> bool:
        return (
            self.isTerminalSuccess()
            or self.isTerminalFailure()
            or self.isTerminalCancelled()
        )


    def __str__(self):
        return f"[stat ctx:{self.getJobContext()} value:{self.getStatusValue()}]"



