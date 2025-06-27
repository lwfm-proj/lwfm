"""
LwfManager - exposes the services of the lwfm middleware to workflows and Site 
implementations.  Permits emitting and fetching job status, setting workflow
event handlers, and notating provenancial metadata.
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring
#pylint: disable = broad-exception-caught, protected-access

import time
import os
import argparse

from typing import List, Optional, Union

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
        self._context = None


    def isMidwareRunning(self) -> bool:
        return self._client.isMidwareRunning()


    def generateId(self):
        return IdGenerator().generateId()


    def _getClient(self):
        return self._client


    def setContext(self, context: JobContext) -> None:
        """
        Set the context for the lwfManager, which can be used to include job-related
        information in log messages and status updates.
        """
        self._context = context

    def getContext(self) -> Optional[JobContext]:
        """
        Get the current context of the lwfManager.
        """
        if self._context is None:
            # try to get it from the environment
            self._context = self._getJobContextFromEnv()
        return self._context

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


    def getSite(self, site: Optional[str]) -> 'Site':
        """
        Get a Site instance. Look it up in the site TOML, instantiate it, potentially 
        overriding its default Site Pillars with provided drivers.
        """
        if site is None or site == "":
            site = "local"
        return SiteConfigBuilder.getSite(site)


    #***********************************************************************
    # serialization methods

    def serialize(self, obj) -> str:
        return ObjectSerializer.serialize(obj)

    def deserialize(self, s: str):
        return ObjectSerializer.deserialize(s)

    #***********************************************************************
    # workflow methods

    def putWorkflow(self, wflow: Workflow) -> Optional[str]:
        return self._client.putWorkflow(wflow)

    def getWorkflow(self, workflow_id: str) -> Optional[Workflow]:
        return self._client.getWorkflow(workflow_id)

    def getAllWorkflows(self) -> Optional[List[Workflow]]:
        """
        Get all workflows stored in the middleware.
        
        Returns:
            List of Workflow objects, or None if no workflows are found
        """
        try:
            val = self._client.getAllWorkflows()
            if val is None:
                return None
            if isinstance(val, List):
                return val
            return [val]
        except Exception as e:
            logger.error(f"Error in LwfManager.getAllWorkflows: {e}")
            return None

    def getAllJobStatusesForWorkflow(self, workflow_id: str) -> Optional[List[JobStatus]]:
        """
        Get all job status messages for all jobs in a workflow, ordered by timestamp (newest first).
        """
        if workflow_id is None:
            return None
        try:
            return self._client.getAllJobStatusesForWorkflow(workflow_id)
        except Exception as e:
            logger.error(f"Error in LwfManager.getAllJobStatusesForWorkflow: {e}")
            return None


    #***********************************************************************
    # job & status methods

    # given a job id, get back the current status
    def getStatus(self, jobId: str) -> JobStatus:
        return self._client.getStatus(jobId) or None # type: ignore


    def getAllStatus(self, jobId: str) -> Optional[List[JobStatus]]:
        return self._client.getAllStatus(jobId)


    def _getJobContextFromEnv(self) -> Optional[JobContext]:
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
                   statusStr: str, nativeStatusStr: Optional[str] = None,
                   nativeInfo: Optional[str] = None) -> None:
        if nativeStatusStr is None:
            nativeStatusStr = statusStr
        return self._client.emitStatus(context, statusStr,
            nativeStatusStr, nativeInfo or "", False)


    def _emitStatusFromEvent(self, context: JobContext,
                   statusStr: str, nativeStatusStr: str,
                   nativeInfo: str) -> None:
        if nativeStatusStr is None:
            nativeStatusStr = statusStr
        return self._client.emitStatus(context, statusStr,
            nativeStatusStr, nativeInfo, True)


    # Wait synchronously until the job reaches a terminal state, then return
    # that state.  Uses a progressive sleep time to avoid polling too frequently.
    def wait(self, jobId: str) -> JobStatus:  # type: ignore
        status: JobStatus = self.getStatus(jobId)
        if status is None:
            return None
        if status.isTerminal():
            # we're done waiting
            return status
        try:
            increment = 3
            w_sum = 1
            w_max = 60
            maxMax = 6000
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
            if status is None:
                return None
            status.setNativeStatus("UNKNOWN")
            status.setNativeInfo(str(ex))
            return status


    def execSiteEndpoint(self, jDefn: JobDefn, jobContext: Optional[JobContext] = None,
        emitStatus: Optional[bool] = True):
        """
        Execute a method on a site pillar object, using venv if needed.
        In essence this is an alternative means to call "site.get-pillar.get-method"().
        We can handle the emitting of job status, or not.
        """
        if jDefn is None:
            logger.error("lwfManager: can't execute site endpoint - is none")
            return None
        if jDefn.getEntryPointType() != JobDefn.ENTRY_TYPE_SITE:
            logger.error("lwfManager: can't execute site endpoint - wrong type")
            return None
        entry_point = jDefn.getEntryPoint()
        if entry_point is None or '.' not in entry_point:
            logger.error(f"lwfManager: invalid site endpoint format: {entry_point}")
            return None
        if jobContext is None:
            jobContext = JobContext()

        logger.info(f"lwfManager: exec site endpoint {entry_point} job {jobContext.getJobId()}")

        if emitStatus:
            self.emitStatus(jobContext, JobStatus.PENDING)

        siteName = jDefn.getSiteName()
        site_pillar, site_method = entry_point.split('.', 1)

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
                return None
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
            return None



    def getLoggingByWorkflowId(self, workflowId: str) -> Optional[List[str]]:
        """
        Retrieve logging entries for a given workflow ID.
        
        Args:
            workflowId: The workflow ID to get logging entries for
            
        Returns:
            List of logging entries as dictionaries, or None if error
        """
        if workflowId is None:
            return None
            
        try:
            return self._client.getLoggingByWorkflowId(workflowId)
        except Exception as e:
            logger.error(f"Error in LwfManager.getLoggingByWorkflowId: {e}")
            return None

    def getLoggingByJobId(self, jobId: str) -> Optional[List[str]]:
        """
        Retrieve logging entries for a given job ID.
        
        Args:
            jobId: The job ID to get logging entries for
            
        Returns:
            List of logging entries as dictionaries, or None if error
        """
        if jobId is None:
            return None
            
        try:
            return self._client.getLoggingByJobId(jobId)
        except Exception as e:
            logger.error(f"Error in LwfManager.getLoggingByJobId: {e}")
            return None

    #***********************************************************************
    # event methods

    # register an event handler, get back the initial queued status of the future job
    def setEvent(self, wfe: WorkflowEvent) -> Optional[JobStatus]:
        return self._client.setEvent(wfe)

    def unsetEvent(self, wfe: WorkflowEvent) -> None:
        return self._client.unsetEvent(wfe)

    # get all active event handlers
    def getActiveWfEvents(self) -> Optional[List[WorkflowEvent]]:
        return self._client.getActiveWfEvents()


    #***********************************************************************
    # repo methods

    def _emitRepoInfo(self, context: JobContext, metasheet: Metasheet) -> None:
        return self.emitStatus(context, "INFO", "INFO", str(metasheet))

    def _notate(self, siteName: str, localPath: str, siteObjPath: str,
                jobContext: JobContext,
                metasheet: Metasheet,
                isPut: bool = False) -> Metasheet:
        if jobContext is not None:
            metasheet.setJobId(jobContext.getJobId())
        # now do the metadata notate
        props = metasheet.getProps()
        props['_direction'] = 'put' if isPut else 'get'
        props['_siteName'] = siteName
        props['_localPath'] = localPath
        props['_siteObjPath'] = siteObjPath
        if jobContext is not None:
            props['_workflowId'] = jobContext.getWorkflowId()
            props['_jobId'] = jobContext.getJobId()
        metasheet.setProps(props)
        # persist
        self._client.notate(metasheet)
        # TODO technically the above notate() might be None if the notate fails - conisder
        # now emit an INFO job status
        self._emitRepoInfo(jobContext, metasheet)
        return metasheet

    def _notatePut(self, siteName: str, localPath: str, siteObjPath: str,
        jobContext: JobContext,
        metasheet: Optional[Metasheet]) -> Metasheet:
        if metasheet is None:
            metasheet = Metasheet(siteName, localPath, siteObjPath)
        return self._notate(siteName, localPath, siteObjPath, jobContext, metasheet, True)

    def _notateGet(self, siteName: str, localPath: str, siteObjPath: str,
        jobContext: JobContext) -> Metasheet:
        metasheet = Metasheet(siteName, localPath, siteObjPath)
        return self._notate(siteName, localPath, siteObjPath, jobContext, metasheet, False)


    def find(self, queryRegExs: dict) -> Optional[List[Metasheet]]:
        return self._client.find(queryRegExs)


#***********************************************************************

lwfManager = LwfManager()
logger = Logger(lwfManager._getClient())

#***********************************************************************


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LwfManager CLI")
    parser.add_argument("--check", action="store_true", help="Check if middleware is running")
    parser.add_argument("--generate-id", action="store_true", help="Generate a new ID")
    parser.add_argument("--clear-events", action="store_true", help="Unset outstanding handlers")
    parser.add_argument("--status", metavar="JOB_ID", type=str, help="Get the status of a job")
    parser.add_argument("--workflows", action="store_true",
                        help="Dump out all stored workflow top-level info")
    parser.add_argument("--workflow", metavar="WORKFLOW_ID", type=str,
                        help="Print workflow and status of all its jobs")
    parser.add_argument("--logs-by-workflow", metavar="WORKFLOW_ID", type=str,
                        help="Get logs by workflow ID")
    parser.add_argument("--logs-by-job", metavar="JOB_ID", type=str,
                        help="Get logs by job ID")
    parser.add_argument("--active-events", action="store_true",
                        help="Return all active workflow events")
    parser.add_argument("--metasheets", metavar="WORKFLOW_ID", type=str,
                        help="Get all metasheets for a workflow")
    args = parser.parse_args()

    if args.check:
        running = lwfManager.isMidwareRunning()
        print(f"Middleware running: {running}")
    elif args.generate_id:
        print(lwfManager.generateId())
    elif args.clear_events:
        events: Union[List[WorkflowEvent], None] = lwfManager.getActiveWfEvents()
        if events:
            for event in events:
                lwfManager.unsetEvent(event)
            print(f"Cleared {len(events)} outstanding unsatisfied events.")
        else:
            print("No outstanding unsatisfied events to clear.")
    elif args.status:
        status = lwfManager.getStatus(args.status)
        if status is not None:
            print(f"Status for job {args.status}: {status}")
        else:
            print(f"No status found for job {args.status}")
    elif args.workflows:
        workflows = lwfManager.getAllWorkflows()
        if workflows:
            for workflow in workflows:
                print(f"{workflow}")
        else:
            print("No workflows found.")
    elif args.workflow:
        workflow = lwfManager.getWorkflow(args.workflow)
        if workflow is None:
            print(f"No workflow found with id {args.workflow}")
        else:
            print(f"{str(workflow)}")
            statuses = lwfManager.getAllJobStatusesForWorkflow(args.workflow)
            if statuses is not None:
                for status in statuses:
                    print(f"{status}")
            else:
                print(f"No job statuses found for workflow {args.workflow}")
    elif args.logs_by_workflow:
        logs = lwfManager.getLoggingByWorkflowId(args.logs_by_workflow)
        if logs:
            for log in logs:
                print("* " + log)
        else:
            print(f"No logs found for workflow {args.logs_by_workflow}")
    elif args.logs_by_job:
        logs = lwfManager.getLoggingByJobId(args.logs_by_job)
        if logs:
            for log in logs:
                print(log)
        else:
            print(f"No logs found for job {args.logs_by_job}")
    elif args.active_events:
        events = lwfManager.getActiveWfEvents()
        if events:
            for event in events:
                print(event)
        else:
            print("No active workflow events found.")
    elif args.metasheets:
        # Return all metasheets for a given workflow
        metasheets = lwfManager.find({"_workflowId": args.metasheets})
        if metasheets:
            for metasheet in metasheets:
                print(metasheet)
        else:
            print(f"No metasheets found for workflow {args.metasheets}")
    else:
        parser.print_help()
