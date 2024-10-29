
from abc import ABC, abstractmethod
import time
from enum import Enum
from typing import List
import importlib

from lwfm.base.WfEvent import WfEvent
from lwfm.base.JobContext import JobContext
from lwfm.base.JobStatus import JobStatus

# ***************************************************************************
class LwfManager(ABC):

    # given a job id, get back the current status
    @abstractmethod
    def getStatus(self, jobId: str) -> JobStatus:
        pass 


    @abstractmethod
    def getAllStatuses(self, jobId: str) -> List[JobStatus]:
        pass
    

    # register an event handler, get back the id of the future job 
    @abstractmethod
    def setEvent(self, wfe: WfEvent) -> str: 
        pass
    

    # get all active event handlers
    @abstractmethod
    def getActiveWfEvents(self) -> List[WfEvent]: 
        pass


    # emit a status message, perhaps triggering event handlers 
    @abstractmethod
    def emitStatus(self, context: JobContext, statusClass: type, 
                   nativeStatus: Enum, nativeInfo: str = None) -> None:
        pass


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
    

    @staticmethod
    def _getInstance() -> "LwfManager":
        module = importlib.import_module(__package__ + ".impl.LwfmEventClient")
        class_ = getattr(module, "LwfmEventClient")
        inst = class_()
        return inst


LwfManager = LwfManager._getInstance()



