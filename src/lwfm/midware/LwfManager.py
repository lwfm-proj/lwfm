"""
LwfManager - exposes the services of the lwfm middleware to workflows and Site 
implementations.  Permits emitting and fetching job status, setting workflow
event handlers, and notating provenancial metadata.
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring
#pylint: disable = broad-exception-caught, protected-access

import time
import os
from typing import List

from lwfm.base.WorkflowEvent import WorkflowEvent
from lwfm.base.JobContext import JobContext
from lwfm.base.JobStatus import JobStatus
from lwfm.base.JobDefn import JobDefn
from lwfm.midware._impl.IdGenerator import IdGenerator
from lwfm.base.Metasheet import Metasheet
from lwfm.base.Workflow import Workflow
from lwfm.base.Site import Site
from lwfm.midware._impl.LwfmEventClient import LwfmEventClient
from lwfm.midware._impl.ObjectSerializer import ObjectSerializer
from lwfm.midware._impl.SiteConfig import SiteConfig
from lwfm.midware._impl.SiteConfigBuilder import SiteConfigBuilder
from lwfm.midware._impl.Logger import Logger


# ***************************************************************************
class LwfManager:

    def __init__(self):
        self._client = LwfmEventClient()


    def isMidwareRunning(self) -> bool:
        return self._client.isMidwareRunning()


    def generateId(self):
        return IdGenerator().generateId()


    def _getClient(self):
        return self._client


    def getLogFilename(self, context: JobContext) -> str:
        basename = SiteConfig.getLogFilename()
        logDir = os.path.expanduser(basename)
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


    def getSite(self, site: str = "local") -> 'Site':
        """
        Get a Site instance. Look it up in the site TOML, instantiate it, potentially 
        overriding its default Site Pillars with provided drivers.
        """
        return SiteConfigBuilder.getSite(site)


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
    # job & status methods

    # given a job id, get back the current status
    def getStatus(self, jobId: str) -> JobStatus:
        return self._client.getStatus(jobId)


    def getAllStatus(self, jobId: str) -> [JobStatus]:
        return self._client.getAllStatus(jobId)


    def _getJobContextFromEnv(self) -> JobContext:
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
    def emitStatus(self, context: JobContext,
                   statusStr: str, nativeStatusStr: str = None,
                   nativeInfo: str = None) -> None:
        if nativeStatusStr is None:
            nativeStatusStr = statusStr
        return self._client.emitStatus(context, statusStr,
            nativeStatusStr, nativeInfo, False)


    def _emitStatusFromEvent(self, context: JobContext,
                   statusStr: str, nativeStatusStr: str = None,
                   nativeInfo: str = None) -> None:
        if nativeStatusStr is None:
            nativeStatusStr = statusStr
        return self._client.emitStatus(context, statusStr,
            nativeStatusStr, nativeInfo, True)


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
            status.setNativeStatus("UNKNOWN")
            status.setNativeInfo(str(ex))
            return status


    def execSiteEndpoint(self, jDefn: JobDefn, jobContext: JobContext = None,
        emitStatus: bool = True):
        """
        Execute a method on a site pillar object, using venv if needed.
        In essence this is an alternative means to call "site.get-pillar.get-method"().
        We can handle the emitting of job status, or not.
        """
        if jDefn is None:
            logger.error("lwfManager: can't execute site endpoint - is none")
            return False
        if jDefn.getEntryPointType() != JobDefn.ENTRY_TYPE_SITE:
            logger.error("lwfManager: can't execute site endpoint - wrong type")
            return False
        if '.' not in jDefn.getEntryPoint():
            logger.error(f"lwfManager: invalid site endpoint format: {jDefn.getEntryPoint()}")
            return False
        if jobContext is None:
            jobContext = JobContext()

        if emitStatus:
            self.emitStatus(jobContext, JobStatus.PENDING)

        siteName = jDefn.getSiteName()
        site_pillar, site_method = jDefn.getEntryPoint().split('.', 1)

        try:
            site = self.getSite(siteName)
            if site_pillar == "auth":
                site_pillar = site.getAuthDriver()
            elif site_pillar == "run":
                site_pillar = site.getRunDriver()
            elif site_pillar == "repo":
                site_pillar = site.getRepoDriver()
            elif site_pillar == "spin":
                site_pillar = site.getSpinDriver()
            method = getattr(site_pillar, site_method, None)
            if not callable(method):
                logger.error(f"lwfManager: method {site_method} not found or not callable")
                if emitStatus:
                    self.emitStatus(jobContext, JobStatus.FAILED)
                return False
            args = jDefn.getJobArgs()
            if site_method == "submit":
                newJobDefn = JobDefn(args[0], JobDefn.ENTRY_TYPE_STRING, args[1:])
                args = [newJobDefn, jobContext, jDefn.getComputeType(), args[1:]]
            # Call the method with the job arguments
            if emitStatus:
                self.emitStatus(jobContext, JobStatus.RUNNING)
            result = method(*args)
            if emitStatus:
                self.emitStatus(jobContext, JobStatus.COMPLETE)
            return result
        except Exception as ex:
            if emitStatus:
                self.emitStatus(jobContext, JobStatus.FAILED)
            logger.error("lwfManager: error executing site endpoint " + \
                f"{jDefn.getEntryPoint()}: {str(ex)}")
            return False


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
        return self.emitStatus(context, "INFO", "INFO", metasheet)

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
