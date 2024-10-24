
import datetime
import time
from enum import Enum

from lwfm.base.WfEvent import WfEvent
from lwfm.base.JobContext import JobContext
from lwfm.base.JobStatus import JobStatus
from lwfm.midware.Logger import Logger
from lwfm.midware.impl.LwfmEventClient import LwfmEventClient


# TODO docs 


# ***************************************************************************
class LwfManager:

    def __init__(self):
        super(LwfManager, self).__init__()

    def getStatus(self, jobId: str) -> JobStatus:
        """
        Given a canonical job id, fetch the latest JobStatus from the lwfm service.
        (The lwfm service may need to call on the host site to obtain up-to-date status.)

        Args:
            jobId (str): canonical job id

        Returns:
            JobStatus: or None if the job is not found
        """
        try:
            return LwfmEventClient().getStatus(jobId)
        except Exception as ex:
            Logger.error("Error fetching job status: " + str(ex))
            return None
        
    def setEvent(self, wfe: WfEvent) -> str: 
        return LwfmEventClient().setEvent(wfe)
    
    def emitStatus(self, context: JobContext, statusClass: type, 
                   nativeStatus: Enum, nativeInfo: str = None) -> None:
        try:
            status = statusClass(context)
            # forces call on setStatus() producing a mapped native status -> status
            status.setNativeStatus(nativeStatus)    
            status.setNativeInfo(nativeInfo)
            status.setEmitTime(datetime.datetime.now(datetime.UTC))
            LwfmEventClient().emitStatus(status)
        except Exception as ex:
            Logger.error("Error emitting job status: " + str(ex))


    # Wait synchronously until the job reaches a terminal state, then return that state.
    # Uses a progressive sleep time to avoid polling too frequently.
    def wait(self, jobId: str) -> JobStatus:  # return JobStatus when the job is done
        status = self
        increment = 3
        sum = 1
        max = 60
        maxMax = 6000
        status = self.getStatus(jobId)
        while not status.isTerminal():
            time.sleep(sum)
            # progressive: keep increasing the sleep time until we hit max, 
            # then keep sleeping max
            if sum < max:
                sum += increment
            elif sum < maxMax:
                sum += max
            status = self.getStatus(jobId)
        return status
    

LwfManager = LwfManager()