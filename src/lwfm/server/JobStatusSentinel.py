
# The Job Status Sentinel watches for job status events and fires a JobDefn when an event of interest occurs.
# The service exposes a way to set/unset event handlers, list jobs currently being watched.
# The service must have some persistence to allow for very long running jobs.

from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus
from lwfm.base.Site import Site



#************************************************************************************************************************************

class JobStatusSentinel:

    def setEventHandler(self):
        pass

    def unsetEventHandler(self):
        pass

    def listActiveHandlers(self):
        pass



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
