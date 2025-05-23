"""
LwfManager - exposes the services of the lwfm middleware to workflows and Site 
implementations.  Permits emitting and fetching job status, setting workflow
event handlers, and notating provenancial metadata.
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring
#pylint: disable = broad-exception-caught, protected-access

import time
import os
import sys
import atexit
from typing import List

from lwfm.base.WorkflowEvent import WorkflowEvent
from lwfm.base.JobContext import JobContext
from lwfm.base.JobStatus import JobStatus
from lwfm.midware._impl.IdGenerator import IdGenerator
from lwfm.base.Metasheet import Metasheet
from lwfm.base.Workflow import Workflow
from lwfm.base.Site import Site, SiteAuth, SiteRun, SiteRepo, SiteSpin
from lwfm.midware._impl.LwfmEventClient import LwfmEventClient
from lwfm.midware._impl.ObjectSerializer import ObjectSerializer
from lwfm.midware._impl.SiteConfig import SiteConfig
from lwfm.midware._impl.Logger import Logger
from lwfm.midware._impl.SvcLauncher import launchMidware

# ***************************************************************************
class LwfManager():

    _midwareProc = None

    def _tryStartMidware(self):
        print("=== Starting middleware ===")
        print(f"Working directory: {os.getcwd()}")
        print(f"Python path: {sys.path}")
        self._midwareProc = launchMidware()


    def _checkMidware(self):
        if self.isMidwareRunning():
            return
        self._tryStartMidware()


    def __init__(self):
        self._client = LwfmEventClient()
        # Register a shutdown handler to ensure clean termination
        #atexit.register(self.shutdown)
        #self._checkMidware()


    def isMidwareRunning(self) -> bool:
        return self._client.isMidwareRunning()

    def shutdown(self):
        """Shutdown the middleware cleanly if it was started by this process"""
        if self._midwareProc is not None:
            self._midwareProc.terminate()
            self._midwareProc.wait(timeout=5)
            self._midwareProc = None


    def generateId(self):
        return IdGenerator().generateId()


    def _getClient(self):
        return self._client


    def getLogFilename(self, context: JobContext) -> str:
        logDir = os.path.expanduser("~/.lwfm/logs")   # TODO move from here and make a property 
        os.makedirs(logDir, exist_ok=True)
        return os.path.join(logDir, f"{context.getJobId()}.log")


    #***********************************************************************
    # configuration methods 

    def getAllSiteProperties(self) -> dict:
        """
        Potentially useful for debugging. Returns the contents of the combined TOML.
        """
        return SiteConfig.getAllSiteProperties()


    def getSiteProperties(self, site: str) -> dict:
        """
        Get the properties for a named site.
        """
        return SiteConfig.getSiteProperties(site)


    def getSite(self, site: str = "local",
                auth_driver: SiteAuth = None,
                run_driver: SiteRun = None,
                repo_driver: SiteRepo = None,
                spin_driver: SiteSpin = None) -> 'Site':
        """
        Get a Site instance. Look it up in the site TOML, instantiate it, potentially 
        overriding its default Site Pillars with provided drivers.
        """
        return SiteConfig.getSite(site, auth_driver, run_driver, repo_driver, spin_driver)


    #***********************************************************************
    # serialization methods

    def serialize(self, obj) -> str:
        return ObjectSerializer.serialize(obj)

    def deserialize(self, s: str):
        return ObjectSerializer.deserialize(s)

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
            status.setNativeStatus("UNKNOWN", str(ex))
            return status


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
        sheet = self._client.notate(metasheet.getSheetId(), metasheet)
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
logger = Logger(lwfManager._getClient())
