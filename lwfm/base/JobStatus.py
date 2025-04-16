"""
Represents a state of a job's execution in its set of valid lifecycle states.
It is emitted within the job's context.
"""

#pylint: disable = missing-function-docstring, missing-class-docstring
#pylint: disable = invalid-name, broad-exception-caught, line-too-long

from enum import Enum
from datetime import datetime

from ..util.IdGenerator import IdGenerator

from .JobContext import JobContext

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


class JobStatus:
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

    def __init__(self, jobContext: JobContext = None):
        self._id = IdGenerator.generateId()
        self._status = JobStatusValues.UNKNOWN
        self._native_status = None
        self._emit_time = datetime.utcnow()
        self._received_time = None
        self._native_info = None
        self._context = jobContext
        self._status_map = {
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

    def getId(self) -> str:
        return self._id

    def getJobContext(self) -> JobContext:
        return self._context

    def setJobContext(self, jobContext: JobContext) -> None:
        self._context = jobContext

    def getJobId(self) -> str:
        return self._context.getId()

    def setStatus(self, status: JobStatusValues) -> None:
        self._status = status

    def getStatus(self) -> JobStatusValues:
        return self._status

    def setNativeStatusStr(self, status: str) -> None:
        self._native_status = status
        # now map the native status to a canonical
        self.mapNativeStatus()

    def getNativeStatusStr(self) -> str:
        return self._native_status

    def setNativeStatus(self, nativeStatus: str) -> None:
        self.setNativeStatusStr(nativeStatus)

    def mapNativeStatus(self) -> None:
        try:
            self._status = self._status_map[self._native_status]
        except Exception:
            self._status = JobStatusValues.UNKNOWN

    def getStatusMap(self) -> dict:
        return self._status_map

    def setStatusMap(self, statusMap: dict) -> None:
        self._status_map = statusMap

    def setEmitTime(self, emitTime: datetime) -> None:
        self._emit_time = emitTime

    def getEmitTime(self) -> datetime:
        return self._emit_time

    def setReceivedTime(self, receivedTime: datetime) -> None:
        self._received_time = receivedTime

    def getReceivedTime(self) -> datetime:
        return self._received_time

    def setNativeInfo(self, info: str) -> None:
        self._native_info = info

    def getNativeInfo(self) -> str:
        return self._native_info

    def isTerminalSuccess(self) -> bool:
        return self._status == JobStatusValues.COMPLETE

    def isTerminalFailure(self) -> bool:
        return self._status == JobStatusValues.FAILED

    def isTerminalCancelled(self) -> bool:
        return self._status == JobStatusValues.CANCELLED

    def isTerminal(self) -> bool:
        return (
            self.isTerminalSuccess()
            or self.isTerminalFailure()
            or self.isTerminalCancelled()
        )

    def __str__(self):
        return f"[stat ctx:{self._context} value:{self._status.value} info:{self._native_info}]"
