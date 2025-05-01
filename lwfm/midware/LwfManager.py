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

from lwfm.base.WorkflowEvent import WorkflowEvent
from lwfm.base.JobContext import JobContext
from lwfm.base.JobStatus import JobStatus
from lwfm.util.IdGenerator import IdGenerator
from lwfm.base.Metasheet import Metasheet
from lwfm.base.Workflow import Workflow
from lwfm.midware._impl.Logger import Logger
from lwfm.midware._impl.LwfmEventClient import LwfmEventClient


# create a singleton logger
logger = Logger()

# ***************************************************************************
class LwfManager():

    _client = LwfmEventClient()

    def generateId(self):
        return IdGenerator.generateId()

    #***********************************************************************
    # logging methods

    def info(self, msg: str, status: str = None):
        logger.info(msg, status)

    def error(self, msg: str, status: str = None):
        logger.error(msg, status)


    #***********************************************************************
    # workflow methods

    def putWorkflow(self, workflow: Workflow) -> str:
        return self._client.putWorkflow(workflow)

    def getWorkflow(self, workflow_id: str) -> Workflow:
        return self._client.getWorkflow(workflow_id)


    #***********************************************************************
    # status methods

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


    # Wait synchronously until the job reaches a terminal state, then return
    # that state.  Uses a progressive sleep time to avoid polling too frequently.
    def wait(self, jobId: str) -> JobStatus:  # return JobStatus when the job is done
        try:
            increment = 3
            w_sum = 1
            w_max = 60
            maxMax = 6000
            status = self.getStatus(jobId)
            if status is not None and status.isTerminal():
                # we're done waiting
                return status
            doneWaiting = False
            while not doneWaiting:
                time.sleep(w_sum)
                # progressive: keep increasing the sleep time until we hit max,
                # then keep sleeping max
                if w_sum < w_max:
                    w_sum += increment
                elif w_sum < maxMax:
                    w_sum += w_max
                status = self.getStatus(jobId)
                if status is not None and status.isTerminal():
                    return status
        except Exception as ex:
            logger.error("Error waiting for job", ex)
            return None


    #***********************************************************************
    # event methods

    # register an event handler, get back the initial queued status of the future job
    def setEvent(self, wfe: WorkflowEvent) -> JobStatus:
        return self._client.setEvent(wfe)

    def unsetEvent(self, wfe: WorkflowEvent) -> None:
        return self._client.unsetEvent(wfe)

    # get all active event handlers
    def getActiveWfEvents(self) -> List[WorkflowEvent]:
        return self._client.getActiveWfEvents()


    #***********************************************************************
    # repo methods

    def _emitRepoInfo(self, context: JobContext, metasheet: Metasheet) -> None:
        return self.emitStatus(context, JobStatus, "INFO", metasheet)

    def _notate(self, localPath: str, siteObjPath: str,
                jobContext: JobContext,
                metasheet: Metasheet = None,
                isPut: bool = False) -> Metasheet:
        if jobContext is not None:
            metasheet.setJobId(jobContext.getJobId())
        # now do the metadata notate
        props = metasheet.getProps()
        props['_direction'] = 'put' if isPut else 'get'
        props['localPath'] = localPath
        props['siteObjPath'] = siteObjPath
        metasheet.setProps(props)
        # persist
        sheet = LwfmEventClient().notate(metasheet.getSheetId(), metasheet)
        # now emit an INFO job status
        self._emitRepoInfo(jobContext, metasheet)
        return sheet

    def notatePut(self, localPath: str, siteObjPath: str,
        jobContext: JobContext,
        metasheet: Metasheet = None) -> Metasheet:
        return self._notate(localPath, siteObjPath, jobContext, metasheet, True)

    def notateGet(self, localPath: str, siteObjPath: str,
        jobContext: JobContext,
        metasheet: Metasheet = None) -> Metasheet:
        return self._notate(localPath, siteObjPath, jobContext, metasheet, False)

    def find(self, queryRegExs: dict) -> List[Metasheet]:
        return self._client.find(queryRegExs)


#***********************************************************************

lwfManager = LwfManager()
