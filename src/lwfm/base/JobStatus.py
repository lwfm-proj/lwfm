# Job Status: a record of a state of the job's execution.  The job may go through many
# states in its lifetime - on the actual runtime Site the job status will be expressed
# in terms of their native status codes.  In lwfm, we desire canonical status messages
# so job chaining is permitted across sites.  Its the role of the Site's Run subsystem
# to produce these datagrams in their canonical form, though we leave room to express
# the native info too.  Some status codes might be emitted more than once (e.g. "INFO").
# We provide a mechanism to track the job's parent-child relationships.


from enum import Enum
import logging

import os
import time


from datetime import datetime
import pickle
import json

from lwfm.base.LwfmBase import LwfmBase
from lwfm.base.JobContext import JobContext
from lwfm.midware.impl.WorkflowEventClient import WorkflowEventClient


class _JobStatusFields(Enum):
    STATUS = "status"  # canonical status
    NATIVE_STATUS = (
        "nativeStatus"  # the status code for the specific Run implementation
    )
    EMIT_TIME = "emitTime"
    RECEIVED_TIME = "receivedTime"
    NATIVE_INFO = "nativeInfo"


# The canonical set of status codes.  Run implementations will have their own sets, and
# they must provide a mapping into these.
class JobStatusValues(Enum):
    UNKNOWN = "UNKNOWN"
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    INFO = "INFO"
    FINISHING = "FINISHING"
    COMPLETE = "COMPLETE"  # terminal state
    FAILED = "FAILED"  # terminal state
    CANCELLED = "CANCELLED"  # terminal state


# *************************************************************************************


class JobStatus(LwfmBase):
    """
    TODO: cleanup docs

    Over the lifetime of the running job, it may emit many status messages.  (Or, more
    specifically, lwfm might poll the remote Site for an updated status of a job it is
    tracking.)

    The Job Status references the Job Context of the running job, which contains the id
    of the job and other originating information.

    The Job Status is then augmented with the updated status info.  Like job ids, which
    come in canonical lwfm and Site-specific forms (and we track both), so do job status
    strings - there's the native job status, and the mapped canonical status.

    Attributes:

    job context - the Job Context for the job, which includes the job id

    status - the current canonical status string

    native status - the current native status string

    emit time - the timestamp when the Site emitted the status - the Site driver will
        need to populate this value

    received time - the timestamp when lwfm received the Job Status from the Site; this
        can be used to study latency in status receipt (which is by polling)

    native info - the Site may inject Site-specific status information into the Job Status
        message

    status map - a constant, the mapping of Site-native status strings to canonical
        status strings; native lwfm local jobs will use the literal mapping, and Site
        drivers will implement Job Status subclasses which provide their own
        Site-to-canonical mapping.

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
            logging.error("Can't determine emit time " + str(ex))
            return datetime.now()

    def setReceivedTime(self, receivedTime: datetime) -> None:
        LwfmBase._setArg(
            self, _JobStatusFields.RECEIVED_TIME.value, receivedTime.timestamp() * 1000
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

    # zero out state-sensative fields
    def clear(self):
        zeroTime = (
            0 if os.name != "nt" else 24 * 60 * 60
        )  # Windows requires an extra day or we get an OS error
        self.setReceivedTime(datetime.fromtimestamp(zeroTime))
        self.setEmitTime(datetime.fromtimestamp(zeroTime))
        self.setNativeInfo("")

    # Send the status message to the lwfm service.
    def emit(self, status: str = None) -> bool:
        if status:
            self.setNativeStatusStr(status)
        self.setEmitTime(datetime.utcnow())
        try:
            wfec = WorkflowEventClient()
            wfec.emitStatus(
                self.getJobContext().getId(), self.getStatus().value, self.serialize()
            )
            # TODO: is there a better way to do this?
            # put a little wait in to avoid a race condition where the status is emitted
            # and then immediately queried or two status messages are emitted in rapid
            # succession and they appear out of order
            time.sleep(1)
            self.clear()
            return True
        except Exception as ex:
            logging.error(str(ex))
            return False

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

    def toJSON(self):
        return self.serialize()

    def serialize(self):
        out_bytes = pickle.dumps(self, 0)
        return out_bytes
        # out_str = out_bytes.decode(encoding='ascii')
        # return out_str

    @staticmethod
    def deserialize(s: str):
        in_json = json.dumps(s)
        in_obj = pickle.loads(json.loads(in_json).encode(encoding="ascii"))
        return in_obj

    def toShortString(self) -> str:
        return (
            ""
            + str(self.getJobContext().getId())
            + ","
            + str(self.getStatusValue())
            + ","
            + str(self.getJobContext().getSiteName())
        )

    def toString(self) -> str:
        s = (
            ""
            + str(self.getJobContext().getId())
            + ","
            + str(self.getJobContext().getParentJobId())
            + ","
            + str(self.getJobContext().getOriginJobId())
            + ","
            + str(self.getJobContext().getNativeId())
            + ","
            + str(self.getEmitTime())
            + ","
            + str(self.getStatusValue())
            + ","
            + str(self.getJobContext().getSiteName())
        )
        if self.getStatus() == JobStatusValues.INFO:
            s += "," + str(self.getNativeInfo())
        return s

    # Wait synchronously until the job reaches a terminal state, then return that state.
    # Uses a progressive sleep time to avoid polling too frequently.
    def wait(self) -> "JobStatus":  # return JobStatus when the job is done
        status = self
        increment = 3
        sum = 1
        max = 60
        maxmax = 6000
        while not status.isTerminal():
            time.sleep(sum)
            # keep increasing the sleep time until we hit max, then keep sleeping max
            if sum < max:
                sum += increment
            elif sum < maxmax:
                sum += max
            status = fetchJobStatus(status.getJobId())
        return status


@staticmethod
def fetchJobStatus(jobId: str) -> JobStatus:
    """
    Given a canonical job id, fetch the latest Job Status from the lwfm service.
    The service may need to call on the host site to obtain up-to-date status.

    Args:
        jobId (str): canonical job id

    Returns:
        JobStatus: Job Status object, or None if the job is not found
    """
    try:
        wfec = WorkflowEventClient()
        statusBlob = wfec.getStatusBlob(jobId)
        if statusBlob:
            return JobStatus.deserialize(statusBlob)
        else:
            return None
    except Exception as ex:
        logging.error(str(ex))
        return None
