
from enum import Enum

from LwfmBase import LwfmBase


class _JobStatusFields(Enum):
    STATUS = "status"
    NATIVE_STATUS = "nativeStatus"

class JobStatusValues(Enum):
    UNKNOWN   = "UNKNOWN"
    PENDING   = "PENDING"
    RUNNING   = "RUNNING"
    INFO      = "INFO"
    FINISHING = "FINISHING"
    COMPLETE  = "COMPLETE"
    FAILED    = "FAILED"


class JobStatus(LwfmBase):

    def __init__(self, args=None):
        super(JobStatus, self).__init__(args)
        # default map
        self.setStatusMap( {
            "UNKNOWN"   : JobStatusValues.UNKNOWN,
            "PENDING"   : JobStatusValues.PENDING,
            "RUNNING"   : JobStatusValues.RUNNING,
            "INFO"      : JobStatusValues.INFO,
            "FINISHING" : JobStatusValues.FINISHING,
            "COMPLETE"  : JobStatusValues.COMPLETE,
            "FAILED"    : JobStatusValues.FAILED
        })

    def setStatus(self, status):
        LwfmBase._setArg(self, _JobStatusFields.STATUS.value, status)

    def getStatus(self):
        return LwfmBase._getArg(self, _JobStatusFields.STATUS.value)

    def getStatusValue(self):
        return LwfmBase._getArg(self, _JobStatusFields.STATUS.value).value

    def setNativeStatusString(self, status):
        LwfmBase._setArg(self, _JobStatusFields.NATIVE_STATUS.value, status)
        self.mapNativeStatus()

    def getNativeStatusString(self):
        return LwfmBase._getArg(self, _JobStatusFields.NATIVE_STATUS.value)

    def mapNativeStatus(self):
        try:
            self.setStatus(self.statusMap[self.getNativeStatusString()])
        except Exception as ex:
            print(ex)
            self.setStatus(JobStatusValues.UNKNOWN)

    def getStatusMap(self):
        return self.statusMap

    def setStatusMap(self, statusMap):
        self.statusMap = statusMap



# test
if __name__ == '__main__':
    status = JobStatus()
    statusMap = {
        "NODE_FAIL" : JobStatusValues.FAILED
        }
    status.setStatusMap(statusMap)
    status.setNativeStatusString("NODE_FAIL")
    print(status.getStatusValue())
