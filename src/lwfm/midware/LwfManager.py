
import time
from enum import Enum
from typing import List

from lwfm.base.WfEvent import WfEvent
from lwfm.base.JobContext import JobContext
from lwfm.base.JobStatus import JobStatus
from lwfm.base.LwfmBase import _IdGenerator
from lwfm.midware.impl.LwfmEventClient import LwfmEventClient

# ***************************************************************************
class LwfManager():

    _client = LwfmEventClient()

    def generateId(self):
        return _IdGenerator.generateId()

    #***********************************************************************
    # status methods

    # given a job id, get back the current status
    def getStatus(self, jobId: str) -> JobStatus:
        return self._client.getStatus(jobId)

    
    # emit a status message 
    def emitStatus(self, context: JobContext, statusClass: type, 
                   nativeStatus: str, nativeInfo: str = None) -> None:
        return self._client.emitStatus(context, statusClass, nativeStatus, nativeInfo)

    # Wait synchronously until the job reaches a terminal state, then return 
    # that state.  Uses a progressive sleep time to avoid polling too frequently.
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
    

    #***********************************************************************
    # event methods

    # register an event handler, get back the id of the future job 
    def setEvent(self, wfe: WfEvent) -> str: 
        return self._client.setEvent(wfe)
    
    def unsetEvent(self, wfe: WfEvent) -> None: 
        return self._client.unsetEvent(wfe)

    # get all active event handlers
    def getActiveWfEvents(self) -> List[WfEvent]: 
        return self._client.getActiveWfEvents()


LwfManager = LwfManager()

if __name__ == "__main__":
    for e in LwfManager.getActiveWfEvents():
        print(e)

