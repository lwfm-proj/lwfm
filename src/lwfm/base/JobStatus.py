"""
Represents a state of a job's execution in its set of valid lifecycle states.
It is emitted within the job's context.
"""

#pylint: disable = missing-function-docstring, missing-class-docstring
#pylint: disable = invalid-name, broad-exception-caught, line-too-long

import datetime
from typing import Optional

from lwfm.midware._impl.IdGenerator import IdGenerator

from lwfm.base.JobContext import JobContext


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

    UNKNOWN = "UNKNOWN"
    READY = "READY"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    INFO = "INFO"
    FINISHING = "FINISHING"
    COMPLETE = "COMPLETE"  # terminal state
    FAILED = "FAILED"  # terminal state
    CANCELLED = "CANCELLED"  # terminal state


    def __init__(self, jobContext: Optional[JobContext] = None):
        self._status_id = IdGenerator().generateId()
        self._status = JobStatus.UNKNOWN
        self._native_status = None
        self._emit_time = datetime.datetime.now(datetime.timezone.utc)
        self._received_time = None
        self._native_info = None
        self._context = jobContext if jobContext is not None else JobContext()
        self._status_map = {
            "UNKNOWN": JobStatus.UNKNOWN,
            "READY": JobStatus.READY,
            "PENDING": JobStatus.PENDING,
            "RUNNING": JobStatus.RUNNING,
            "INFO": JobStatus.INFO,
            "FINISHING": JobStatus.FINISHING,
            "COMPLETE": JobStatus.COMPLETE,
            "FAILED": JobStatus.FAILED,
            "CANCELLED": JobStatus.CANCELLED,
        }

    def getStatusId(self) -> str:
        return self._status_id

    def getJobContext(self) -> JobContext:
        return self._context

    def setJobContext(self, jobContext: JobContext) -> None:
        self._context = jobContext

    def getJobId(self) -> str:
        if self._context is None:
            return None # type: ignore
        return self._context.getJobId()

    def setStatus(self, status: str) -> None:
        self._status = status

    def getStatus(self) -> str:
        return self._status

    def setNativeStatusStr(self, status: str) -> None:
        self._native_status = status

    def getNativeStatusStr(self) -> Optional[str]:
        return self._native_status

    def setNativeStatus(self, nativeStatus: str) -> None:
        self.setNativeStatusStr(nativeStatus)

    def mapNativeStatus(self) -> None:
        try:
            if self._native_status is not None:
                self._status = self._status_map[self._native_status]
            else:
                self._status = JobStatus.UNKNOWN
        except Exception:
            self._status = JobStatus.UNKNOWN

    def getStatusMap(self) -> dict:
        return self._status_map

    def setStatusMap(self, statusMap: dict) -> None:
        self._status_map = statusMap

    def setEmitTime(self, emitTime: datetime.datetime) -> None:
        self._emit_time = emitTime

    def getEmitTime(self) -> datetime.datetime:
        return self._emit_time

    def setReceivedTime(self, receivedTime: datetime.datetime) -> None:
        self._received_time = receivedTime

    def getReceivedTime(self) -> Optional[datetime.datetime]:
        return self._received_time

    def setNativeInfo(self, info: str) -> None:
        self._native_info = info

    def getNativeInfo(self) -> Optional[str]:
        return self._native_info

    def isTerminalSuccess(self) -> bool:
        return self._status == JobStatus.COMPLETE

    def isTerminalFailure(self) -> bool:
        return self._status == JobStatus.FAILED

    def isTerminalCancelled(self) -> bool:
        return self._status == JobStatus.CANCELLED

    def isTerminal(self) -> bool:
        return (
            self.isTerminalSuccess()
            or self.isTerminalFailure()
            or self.isTerminalCancelled()
        )

    def isInfo(self) -> bool:
        return self._status == JobStatus.INFO

    def isPreRun(self) -> bool:
        return self._status == JobStatus.READY or self._status == JobStatus.PENDING

    def isRunning(self) -> bool:
        return self._status == JobStatus.RUNNING


    def __str__(self):
        return f"[status ctx:{self._context} value:{self._status} info:{self._native_info}]"
