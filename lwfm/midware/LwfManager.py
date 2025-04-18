"""
LwfManager - exposes the services of the lwfm middleware to workflows and Site 
implementations.  Permits emitting and fetching job status, setting workflow
event handlers, and notating provenancial metadata.
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring
#pylint: disable = broad-exception-caught

import time
import os
from typing import List

from ..base.WfEvent import WfEvent
from ..base.JobContext import JobContext
from ..base.JobStatus import JobStatus
from ..util.IdGenerator import IdGenerator
from ..base.Metasheet import Metasheet
from .impl.LwfmEventClient import LwfmEventClient

# ***************************************************************************
class LwfManager():

    _client = LwfmEventClient()

    def generateId(self):
        return IdGenerator.generateId()

    #***********************************************************************
    # status methods - job metadata

    # given a job id, get back the current status
    def getStatus(self, jobId: str) -> JobStatus:
        return self._client.getStatus(jobId)

    def getAllStatus(self, jobId: str) -> [JobStatus]:
        return self._client.getAllStatus(jobId)


    def getJobContextFromEnv(self) -> JobContext:
        # see if we got passed in a job id in the os environment
        if '_LWFM_JOB_ID' in os.environ:
            status = self.getStatus(os.environ['_LWFM_JOB_ID'])
            if status is not None:
                return status.getJobContext()
            else:
                context = JobContext()
                context.setJobId(os.environ['_LWFM_JOB_ID'])
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
        try:
            increment = 3
            sum = 1
            max = 60
            maxMax = 6000
            status = self.getStatus(jobId)
            fakeStatus = JobStatus(JobContext())
            fakeStatus.setStatus("UNKNOWN")
            if status is None:
                status = fakeStatus
            while not status.isTerminal():
                time.sleep(sum)
                # progressive: keep increasing the sleep time until we hit max,
                # then keep sleeping max
                if sum < max:
                    sum += increment
                elif sum < maxMax:
                    sum += max
                status = self.getStatus(jobId)
                if status is None:
                    status = fakeStatus
            return status
        except Exception:
            return None


    #***********************************************************************
    # event methods - workflow metadata

    # register an event handler, get back the initial queued status of the future job
    def setEvent(self, wfe: WfEvent) -> JobStatus:
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
        if jobContext is not None:
            metasheet.setId(jobContext.getJobId())
        # now do the metadata notate
        args = metasheet.getArgs()
        args['_direction'] = 'put' if isPut else 'get'
        args['localPath'] = localPath
        args['siteObjPath'] = siteObjPath
        metasheet.setArgs(args)
        # persist
        sheet = LwfmEventClient().notate(metasheet.getSheetId(), metasheet)
        # now emit an INFO job status
        self.emitRepoInfo(jobContext, metasheet)
        return sheet


    def find(self, queryRegExs: dict) -> List[Metasheet]:
        return self._client.find(queryRegExs)


#***********************************************************************

lwfManager = LwfManager()
