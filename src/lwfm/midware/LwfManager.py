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

from typing import List, Optional, Union, Any, cast

import requests

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


    #***********************************************************************
    # private methods
    # These methods are not intended to be used by workflow authors, but are
    # used internally by the lwfm service, and potentially Site drivers.

    def _getClient(self):
        """
        Gets the client class which is a thin set of methods for invoking 
        REST services.
        """
        return self._client


    def _serialize(self, obj) -> str:
        """
        Serialize an object to string.
        """
        return ObjectSerializer.serialize(obj)


    def serialize(self, obj) -> str:
        """
        Serialize an object to string.
        """
        return self._serialize(obj)


    def _deserialize(self, s: str):
        """
        Deserialize a string to an object.
        """
        return ObjectSerializer.deserialize(s)


    def deserialize(self, s: str):
        """
        Deserialize a string to an object.
        """
        return self._deserialize(s)


    def _getJobContextFromEnv(self) -> Optional[JobContext]:
        # see if we got passed in a job id in the os environment
        if '_LWFM_JOB_ID' in os.environ:
            _status = self.getStatus(os.environ['_LWFM_JOB_ID'])
            if _status is not None:
                return _status.getJobContext()
            else:
                context = JobContext()
                context.setJobId(os.environ['_LWFM_JOB_ID'])
                return context
        return None


    def _emitStatusFromEvent(self, context: JobContext,
                   statusStr: str, nativeStatusStr: str,
                   nativeInfo: str) -> None:
        if nativeStatusStr is None:
            nativeStatusStr = statusStr
        return self._client.emitStatus(context, statusStr,
            nativeStatusStr, nativeInfo, True)


    def _emitRepoInfo(self, context: JobContext, _metasheet: Metasheet) -> None:
        return self.emitStatus(context, "INFO", "INFO", str(_metasheet))


    def _notate(self, siteName: str, localPath: str, siteObjPath: str,
                jobContext: Union[JobContext, str],
                _metasheet: Metasheet,
                isPut: bool = False) -> Metasheet:
        """
        Notate a metadata sheet with the given site name, local path, and site object path.
        If a job context is provided, it will be set on the metasheet.
        """
        if jobContext is None:
            jCtx: JobContext = JobContext()
        elif isinstance(jobContext, str):
            jCtx: JobContext = self.deserialize(jobContext)
            if jCtx is None:
                jCtx = JobContext()
        else:
            jCtx: JobContext = jobContext

        _metasheet.setJobId(jCtx.getJobId())
        # now do the metadata notate
        props = _metasheet.getProps()
        props['_direction'] = 'put' if isPut else 'get'
        props['_siteName'] = siteName
        props['_localPath'] = localPath
        props['_siteObjPath'] = siteObjPath
        if jobContext is not None:
            props['_workflowId'] = jCtx.getWorkflowId()
            props['_jobId'] = jCtx.getJobId()
        _metasheet.setProps(props)
        # persist
        self._client.notate(_metasheet)
        # TODO technically the above notate() might be None if the notate fails - conisder
        # now emit an INFO job status
        self._emitRepoInfo(jCtx, _metasheet)
        return _metasheet

    def _notatePut(self, siteName: str, localPath: str, siteObjPath: str,
        jobContext: JobContext,
        _metasheet: Optional[Metasheet]) -> Metasheet:
        if _metasheet is None:
            _metasheet = Metasheet(siteName, localPath, siteObjPath)
        return self._notate(siteName, localPath, siteObjPath, jobContext, _metasheet, True)

    def _notateGet(self, siteName: str, localPath: str, siteObjPath: str,
        jobContext: JobContext) -> Metasheet:
        _metasheet = Metasheet(siteName, localPath, siteObjPath)
        return self._notate(siteName, localPath, siteObjPath, jobContext, _metasheet, False)


    #***********************************************************************
    # public job context methods

    def isMidwareRunning(self) -> bool:
        """
        Is the middleware service running?
        """
        return self._client.isMidwareRunning()


    def generateId(self) -> str:
        """
        Generate a unique ID for a job or workflow, or any other purpose.
        """
        return IdGenerator().generateId()


    def setContext(self, context: JobContext) -> None:
        """
        Set the context for the lwfManager, which can be used to include job-related
        information in log messages and status updates.
        """
        self._context = context # TODO are we using this everywhere correctly?


    def getContext(self) -> Optional[JobContext]:
        """
        Get the current context of the lwfManager.
        """
        if self._context is None:
            # try to get it from the environment
            self._context = self._getJobContextFromEnv()
        return self._context


    def getLogFilename(self, context: JobContext) -> str:
        """
        Construct and return a log file name for a job context.
        """
        basename = SiteConfig.getLogFilename()
        logDir = os.path.expanduser(basename)
        os.makedirs(logDir, exist_ok=True)
        return os.path.join(logDir, f"{context.getJobId()}.log")

    def getStdoutFilename(self, context: JobContext) -> str:
        return self.getLogFilename(context)


    #***********************************************************************
    # public site configuration methods

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
    # public workflow methods

    def putWorkflow(self, wflow: Workflow) -> Optional[Workflow]:
        """
        Write a workflow object to store. Get back a workflow id.
        """
        workflowId = self._client.putWorkflow(wflow)
        if workflowId is None:
            return None
        return self.getWorkflow(workflowId)

    def getWorkflow(self, workflow_id: str) -> Optional[Workflow]:
        """
        Get a workflow by its ID.
        """
        return self._client.getWorkflow(workflow_id)


    def getAllWorkflows(self) -> Optional[List[Workflow]]:
        """
        Get all workflows stored. Workflows are relatively thin objects.
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

    def getJobStatusesForWorkflow(self, workflow_id: str) -> Optional[List[JobStatus]]:
        """
        Get the final (or current) job status messages for all jobs in a workflow, ordered by
        timestamp (newest first).
        This is useful for getting the final state of all jobs in a workflow or latest status
        of a workflow in flight.
        """
        if workflow_id is None:
            return None
        try:
            # Get all job statuses for the workflow
            all_statuses = self._client.getAllJobStatusesForWorkflow(workflow_id)
            if all_statuses is None:
                return None

            # Group statuses by job ID and keep only the latest (newest) for each job
            latest_statuses = {}
            for job_status in all_statuses:
                job_id = job_status.getJobContext().getJobId()
                if job_id not in latest_statuses:
                    latest_statuses[job_id] = job_status
                else:
                    # Compare timestamps and keep the newer one
                    if job_status.getEmitTime() > latest_statuses[job_id].getEmitTime():
                        latest_statuses[job_id] = job_status

            # Return the latest statuses as a list, sorted by timestamp (newest first)
            result = list(latest_statuses.values())
            result.sort(key=lambda x: x.getEmitTime(), reverse=True)
            return result
        except Exception as e:
            logger.error(f"Error in LwfManager.getFinalJobStatusesForWorkflow: {e}")
            return None


    def getAllJobStatusesForWorkflow(self, workflow_id: str) -> Optional[List[JobStatus]]:
        """
        Get all job status messages for all jobs in a workflow, ordered by timestamp (newest first).
        This might get large. Not tested for super large workflows.
        """
        if workflow_id is None:
            return None
        try:
            return self._client.getAllJobStatusesForWorkflow(workflow_id)
        except Exception as e:
            logger.error(f"Error in LwfManager.getAllJobStatusesForWorkflow: {e}")
            return None


    def dumpWorkflow(self, workflow_id: str) -> Optional[dict]:
        """
        Dump the workflow and its jobs, including their statuses, to a dictionary.
        This is useful for debugging and understanding the state of a workflow.
        """
        if workflow_id is None:
            return None
        try:
            workflow = self.getWorkflow(workflow_id)
            if workflow is None:
                return None
            jobs = self.getJobStatusesForWorkflow(workflow_id)
            metasheets = self.find({"_workflowId": workflow_id}) or []
            return {
                "workflow": str(workflow),
                "jobs": jobs,
                "metasheets": metasheets
            }
        except Exception as e:
            logger.error(f"Error in LwfManager.dumpWorkflow: {e}")
            return None


    def findWorkflows(self, queryRegExs: dict) -> Optional[List[Workflow]]:
        return self._client.findWorkflows(queryRegExs)




    #***********************************************************************
    # public job & status methods

    def getStatus(self, jobId: str) -> JobStatus:
        """
        Get the status of a job by its lwfm id, returning only the most recent 
        status message. This might end up doing a read on the store, or it might
        end up calling the site to get the status.
        """
        return self._client.getStatus(jobId) or None # type: ignore


    def getAllStatus(self, jobId: str) -> Optional[List[JobStatus]]:
        """
        Return all job status records in store for a given job, sorted by time.
        This is useful for getting the full history of a job.
        """
        return self._client.getAllStatus(jobId)


    def emitStatus(self, context: JobContext,
                   statusStr: str, nativeStatusStr: Optional[str] = None,
                   nativeInfo: Optional[str] = None) -> None:
        """
        Emit a status message for a job, using the provided context.
        Emitting a status will persist it in the lwfm store, potentially write it 
        to running logs and/or console, and may trigger asynchronous events.
        """
        if nativeStatusStr is None:
            nativeStatusStr = statusStr
        return self._client.emitStatus(context, statusStr,
            nativeStatusStr, nativeInfo or "", False)


    def wait(self, jobId: str) -> JobStatus:  # type: ignore
        """
        Wait synchronously until the job reaches a terminal state, then return
        that state. Uses a progressive sleep time to avoid polling too frequently.
        Jobs will run asynchronously and will record their results in store, however, 
        some user workflows might wish to wait synchronously if they know the runtimes 
        are short. This is not recommended for long-running jobs - use job triggers
        instead.
        """
        _status: JobStatus = self.getStatus(jobId)
        if _status is None:
            return None
        if _status.isTerminal():
            # we're done waiting
            return _status
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
                _status = self.getStatus(jobId)
                if _status is not None and _status.isTerminal():
                    return _status
        except Exception as ex:
            if _status is None:
                return None
            _status.setNativeStatus("UNKNOWN")
            _status.setNativeInfo(str(ex))
            return _status


    def execSiteEndpoint(self, jDefn: JobDefn, jobContext: Optional[JobContext] = None,
        emitStatus: Optional[bool] = True) -> Any:
        """
        Execute a method on a site pillar object, using venv if needed.
        In essence this is an alternative means to call "site.get-pillar.get-method"().
        We can handle the emitting of job status, or not.
        This is a convenience method, and permits a bit of shorthand in workflows.
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
            if emitStatus:
                self.emitStatus(jobContext, JobStatus.RUNNING)
            _args = jDefn.getJobArgs()
            if site_method == "submit":
                newJobDefn = JobDefn(_args[0], JobDefn.ENTRY_TYPE_STRING, _args[1:])
                runArgs = _args[1:] if len(_args) > 1 else None
                _args = [newJobDefn, jobContext, jDefn.getComputeType(), runArgs]
            elif site_method in ["get"]:
                if len(_args) == 2:
                    _args = [_args[0], _args[1], jobContext]
            elif site_method in ["put"]:
                if len(_args) == 2:
                    _args = [_args[0], _args[1], jobContext]
                elif len(_args) == 3:
                    _args = [_args[0], _args[1], jobContext, _args[3]]
            # do the actual invocation of the site driver method here, passing the
            # args and getting the return object
            result = method(*_args)
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
        Retrieve logging entries for a given workflow ID.  # TODO needs testing
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
        Retrieve logging entries for a given job ID.    # TODO needs testing
        """
        if jobId is None:
            return None
        try:
            return self._client.getLoggingByJobId(jobId)
        except Exception as e:
            logger.error(f"Error in LwfManager.getLoggingByJobId: {e}")
            return None


    def getAllLogging(self) -> Optional[List[str]]:
        """
        Retrieve all logging entries from the system.
        """
        try:
            return self._client.getAllLogging()
        except Exception as e:
            logger.error(f"Error in LwfManager.getAllLogging: {e}")
            return None


    #***********************************************************************
    # public event methods

    def setEvent(self, wfe: WorkflowEvent) -> Optional[JobStatus]:
        """
        Register a workflow event handler. This will return the initial status of the
        future job.
        """
        return self._client.setEvent(wfe)


    def unsetEvent(self, wfe: WorkflowEvent) -> None:
        """
        Unset a registered event.
        """
        return self._client.unsetEvent(wfe)


    def getActiveWfEvents(self) -> Optional[List[WorkflowEvent]]:
        """
        Return all active event handlers. This might be large for large workflows
        but the WorkflowEvent is relatively thin.
        """
        return self._client.getActiveWfEvents()


    #***********************************************************************
    # repo methods


    def notatePut(self, localPath: str, workflowId: Optional[str] = None,
        _metasheet: Optional[Union[Metasheet, dict]] = None) -> Metasheet:
        if workflowId is not None:
            jobContext = JobContext()
            jobContext.setWorkflowId(workflowId)
        else:
            jobContext = self.getContext()
            if jobContext is None:
                jobContext = JobContext()
        if _metasheet is None:
            _metasheet = Metasheet("local", localPath, "", {})
            _metasheet.setJobId(jobContext.getJobId())
        if isinstance(_metasheet, dict):
            _metasheet = Metasheet("local", localPath, "", cast(dict, _metasheet))
            _metasheet.setJobId(jobContext.getJobId())
        return self._notatePut("local", localPath, "", jobContext, _metasheet)


    def notateGet(self, localPath: str, workflowId: Optional[str] = None) -> Metasheet:
        if workflowId is not None:
            jobContext = JobContext()
            jobContext.setWorkflowId(workflowId)
        else:
            jobContext = self.getContext()
            if jobContext is None:
                jobContext = JobContext()
        return self._notateGet("local", localPath, "", jobContext)


    def find(self, queryRegExs: dict) -> Optional[List[Metasheet]]:
        return self._client.find(queryRegExs)



    #***********************************************************************
    # misc methods

    def sendEmail(self, subject: str, body: str, to: str) -> bool:
        """
        Send an email with the given subject and body. This is a convenience method
        for sending emails from workflows. 
        TODO this is not end-state, a demo hack
        """
        logger.info(f"Sending email to {to} with subject: {subject}")
        logger.info(f"api key: {SiteConfig.getSiteProperties('lwfm').get('emailKey')}")
        try:
            session = requests.Session()
            session.verify = False  # Use system certificate store
            response = session.post(
                "https://api.mailgun.net/v3/sandbox884a84ded0f443569fd09c93dcc28aa2.mailgun.org/messages",
                auth=("api", SiteConfig.getSiteProperties("lwfm").get("emailKey") or ""),
                data={"from": "Mailgun Sandbox <postmaster@sandbox884a84ded0f443569fd09c93dcc28aa2.mailgun.org>",
                    "to": to,
                    "subject": subject,
                    "text": body},
                timeout=30  # 30 second timeout
            )
            return True
        except requests.exceptions.Timeout:
            logger.error("Email request timed out after 30 seconds")
            return False
        except requests.exceptions.SSLError as e:
            logger.error(f"SSL Error (corporate firewall?): {e}")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return False




#***********************************************************************
# exposed instances

lwfManager = LwfManager()
logger = Logger(lwfManager._getClient())


#***********************************************************************
# CLI interface for LwfManager

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
    parser.add_argument("--all-logs", action="store_true",
                        help="Dump all logs from the system")
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
    elif args.all_logs:
        logs = lwfManager.getAllLogging()
        if logs:
            for log in logs:
                print(log)
        else:
            print("No logs found in the system")
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
