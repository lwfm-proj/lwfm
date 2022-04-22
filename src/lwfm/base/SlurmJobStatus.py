
import logging

from JobStatus import JobStatus, JobStatusValues


class SlurmJobStatus(JobStatus):

    def __init__(self):
        super(SlurmJobStatus, self).__init__()
        self.setStatusMap({
            "PENDING"       : JobStatusValues.PENDING    ,
            "CONFIGURING"   : JobStatusValues.PENDING    ,
            "RUNNING"       : JobStatusValues.RUNNING    ,
            "COMPLETING"    : JobStatusValues.FINISHING  ,
            "STAGE_OUT"     : JobStatusValues.FINISHING  ,
            "COMPLETED"     : JobStatusValues.COMPLETE   ,
            "BOOT_FAIL"     : JobStatusValues.FAILED     ,
            "FAILED"        : JobStatusValues.FAILED     ,
            "NODE_FAIL"     : JobStatusValues.FAILED     ,
            "OUT_OF_MEMORY" : JobStatusValues.FAILED     ,
            "CANCELLED"     : JobStatusValues.CANCELLED  ,
            "PREEMPTED"     : JobStatusValues.CANCELLED  ,
            "SUSPENDED"     : JobStatusValues.CANCELLED  ,
            "DEADLINE"      : JobStatusValues.CANCELLED  ,
            "TIMEOUT"       : JobStatusValues.CANCELLED  ,
            })


# test
if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    status = SlurmJobStatus()
    status.setNativeStatusString("NODE_FAIL")
    logging.info(status.getStatusValue())
