
import time
import os 
from typing import List

from lwfm.base.WfEvent import WfEvent
from lwfm.base.JobContext import JobContext
from lwfm.base.JobStatus import JobStatus
from lwfm.base.LwfmBase import _IdGenerator
from lwfm.base.Metasheet import Metasheet
from lwfm.midware.impl.LwfmEventClient import LwfmEventClient

# ***************************************************************************
class LwfManager():

    _client = LwfmEventClient()

    def generateId(self):
        return _IdGenerator.generateId()

    #***********************************************************************
    # status methods - job metadata 

    # given a job id, get back the current status
    def getStatus(self, jobId: str) -> JobStatus:
        return self._client.getStatus(jobId)

    def getJobContextFromEnv(self) -> JobContext:
        # see if we got passed in a job id in the os environment
        if '_LWFM_JOB_ID' in os.environ:
            status = self.getStatus(os.environ['_LWFM_JOB_ID'])
            if (status is not None):
                return status.getJobContext()
            else:
                context = JobContext()
                context.setId(os.environ['_LWFM_JOB_ID'])
                return context
        return None
    
    # emit a status message 
    def emitStatus(self, context: JobContext, statusClass: type, 
                   nativeStatus: str, nativeInfo: str = None) -> None:
        return self._client.emitStatus(context, statusClass, nativeStatus, nativeInfo)
    

    def emitRepoInfo(self, context: JobContext, metasheet: Metasheet) -> None:
        return self.emitStatus(context, JobStatus, "INFO", metasheet)


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
    # event methods - workflow metadata

    # register an event handler, get back the id of the future job 
    def setEvent(self, wfe: WfEvent) -> str: 
        return self._client.setEvent(wfe)
    
    def unsetEvent(self, wfe: WfEvent) -> None: 
        return self._client.unsetEvent(wfe)

    # get all active event handlers
    def getActiveWfEvents(self) -> List[WfEvent]: 
        return self._client.getActiveWfEvents()


    #***********************************************************************
    # repo methods - data metadata

    def notate(self, localPath: str, siteObjPath: str, metasheet: Metasheet = None, 
               isPut: bool = False) -> Metasheet:
        # do we know the job context?
        jobContext = self.getJobContextFromEnv()
        if (jobContext is not None):
            metasheet.setId(jobContext.getId()) 
        # now do the metadata notate
        args = metasheet.getArgs()
        args['_direction'] = 'put' if isPut else 'get'
        args['localPath'] = localPath
        args['siteObjPath'] = siteObjPath
        metasheet.setArgs(args)
        # persist 
        sheet = LwfmEventClient().notate(metasheet.getId(), metasheet)
        # now emit an INFO job status
        self.emitRepoInfo(jobContext, metasheet)   
        return sheet

    
    def find(self, queryRegExs: dict) -> List[Metasheet]:
        return self._client.find(queryRegExs)
    

    #***********************************************************************

LwfManager = LwfManager()

if __name__ == "__main__":
    for e in LwfManager.getActiveWfEvents():
        print(e)

