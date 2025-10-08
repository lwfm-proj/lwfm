"""
Run as part of the lwfm middleware.
The Event Processor watches for Job Status events and fires a JobDefn 
to a Site when an event of interest occurs.
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring
#pylint: disable = broad-exception-caught, protected-access, import-outside-toplevel
#pylint: disable = eval-used

import re
import time
import threading
from typing import List, Optional, cast
import atexit

from lwfm.base.JobStatus import JobStatus
from lwfm.base.JobContext import JobContext
from lwfm.base.JobDefn import JobDefn
from lwfm.base.Site import SiteRun
from lwfm.base.WorkflowEvent import WorkflowEvent, JobEvent, MetadataEvent, NotificationEvent
from lwfm.base.Exceptions import JobNotFoundException
from lwfm.midware._impl.Store import EventStore, JobStatusStore, LoggingStore
from lwfm.midware._impl.SiteConfig import SiteConfig
from lwfm.midware._impl.IdGenerator import IdGenerator
from lwfm.midware.LwfManager import lwfManager

# ***************************************************************************


class RemoteJobEvent(WorkflowEvent):
    def __init__(self, context):
        super().__init__(JobDefn(), context.getSiteName(), context.getJobId())
        self._native_job_id = context.getNativeId()

    def getNativeJobId(self):
        return self._native_job_id

    def __str__(self):
        return super().__str__() + f"+[remote nativeId:{self.getNativeJobId()}]"


# ***************************************************************************

class LwfmEventProcessor:
    _timer = None
    _eventHandlerMap = dict()
    _instance = None
    _instance_lock = threading.Lock()

    _infoQueue: List[JobStatus] = []

    STATUS_CHECK_INTERVAL_SECONDS_MIN = 5
    STATUS_CHECK_INTERVAL_SECONDS_MAX = 5*60
    STATUS_CHECK_INTERVAL_SECONDS_STEP = 10
    _statusCheckIntervalSeconds = STATUS_CHECK_INTERVAL_SECONDS_MIN


    def __new__(cls, *args, **kwargs):
        # Ensure only one instance exists
        with cls._instance_lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
            return cls._instance

    @classmethod
    def get_instance(cls) -> "LwfmEventProcessor":
        # Prefer explicit accessor for clarity at call sites
        return cls()

    def __init__(self):
        # Idempotent init: guard against re-initialization if constructor is called again
        if getattr(self, "_initialized", False):
            return
        # Initialize instance variables
        self._timer_lock = threading.Lock()
        self._timer = None
        self._eventHandlerMap = {}
        self._infoQueue = []
        self._statusCheckIntervalSeconds = self.STATUS_CHECK_INTERVAL_SECONDS_MIN
        self._last_wake: float = 0.0

        # Register cleanup to happen at exit
        atexit.register(self.exit)
        self._eventStore: EventStore = EventStore()
        self._jobStatusStore: JobStatusStore = JobStatusStore()
        self._loggingStore: LoggingStore = LoggingStore()
        self._timer = threading.Timer(
            self._statusCheckIntervalSeconds,
            LwfmEventProcessor.checkEventHandlers,
            (self,),
        )
        self._timer.start()
        self._initialized = True


    def exit(self):
        print("lwfm event processor exit() called")
        with self._timer_lock:
            if self._timer:
                self._timer.cancel()


    def _runAsyncOnSite(self, trigger: WorkflowEvent, context: JobContext) -> None:
        """
        Executed by the event processor in response to a job trigger being 
        satisfied.
        """
        # Deferred import to avoid circular dependencies
        from lwfm.midware.LwfManager import lwfManager
        site = lwfManager.getSite(trigger.getFireSite())
        runDriver = site.getRunDriver()
        siteName = site.getSiteName()
        siteProps = SiteConfig.getSiteProperties(siteName)
        useVenv = siteProps.get('venv', False)
        entryPointType = trigger.getFireDefn().getEntryPointType()
        if useVenv:
            if entryPointType == JobDefn.ENTRY_TYPE_SITE:
                # venv Site - use SiteConfigVenv to execute in the venv
                from lwfm.midware._impl.SiteConfigVenv import SiteConfigVenv
                from lwfm.midware._impl.ObjectSerializer import ObjectSerializer

                # Serialize the job definition and context for the venv execution
                job_defn_serialized = ObjectSerializer.serialize(trigger.getFireDefn())
                context_serialized = ObjectSerializer.serialize(context)

                # Create the command to run in the venv
                cmd = "" + \
                    "from lwfm.midware._impl.ObjectSerializer import ObjectSerializer; " + \
                    "from lwfm.midware.LwfManager import lwfManager; " + \
                    f"job_defn = ObjectSerializer.deserialize('{job_defn_serialized}'); " + \
                    f"context = ObjectSerializer.deserialize('{context_serialized}'); " + \
                    "lwfManager.execSiteEndpoint(job_defn, context, True)"
                # Execute in the venv
                site_venv = SiteConfigVenv()
                venv_path = site_venv.makeVenvPath(siteName)
                self._loggingStore.putLogging("INFO",
                                              f"Executing in venv at '{venv_path}': {cmd}",
                                              siteName,
                                              context.getWorkflowId(),
                                              context.getJobId())
                try:
                    # TODO result
                    result = site_venv.executeInProjectVenv(siteName, cmd)
                except Exception as ex:
                    self._loggingStore.putLogging(
                        "ERROR",
                        f"Venv execution failed for site '{siteName}' at '{venv_path}': {ex}",
                        siteName,
                        context.getWorkflowId(),
                        context.getJobId()
                    )
                    raise
            else:
                # venv Site - use SiteConfigVenv to execute in the venv
                from lwfm.midware._impl.SiteConfigVenv import SiteConfigVenv
                from lwfm.midware._impl.ObjectSerializer import ObjectSerializer

                # Serialize the job definition and context for the venv execution
                job_defn_serialized = ObjectSerializer.serialize(trigger.getFireDefn())
                context_serialized = ObjectSerializer.serialize(context)

                # Create the command to run in the venv
                cmd = "" + \
                    "from lwfm.midware._impl.ObjectSerializer import ObjectSerializer; " + \
                    "from lwfm.midware.LwfManager import lwfManager; " + \
                    f"job_defn = ObjectSerializer.deserialize('{job_defn_serialized}'); " + \
                    f"context = ObjectSerializer.deserialize('{context_serialized}'); " + \
                    f"site = lwfManager.getSite('{siteName}'); " + \
                    "run_driver = site.getRunDriver(); " + \
                    "run_driver.submit(job_defn, context)"
                # Execute in the venv
                site_venv = SiteConfigVenv()
                venv_path = site_venv.makeVenvPath(siteName)
                self._loggingStore.putLogging("INFO",
                                              f"Executing in venv at '{venv_path}': {cmd}",
                                              siteName,
                                              context.getWorkflowId(),
                                              context.getJobId())
                try:
                    # TODO result
                    result = site_venv.executeInProjectVenv(siteName, cmd)
                except Exception as ex:
                    self._loggingStore.putLogging(
                        "ERROR",
                        f"Venv execution failed for site '{siteName}' at '{venv_path}': {ex}",
                        siteName,
                        context.getWorkflowId(),
                        context.getJobId()
                    )
                    raise
        else:
            if entryPointType == JobDefn.ENTRY_TYPE_SITE:
                # Non-venv Site - use the run driver directly
                # Note: Comma is needed at end to make this a tuple. DO NOT REMOVE
                thread = threading.Thread(
                    target=lwfManager.execSiteEndpoint,
                    args=(
                        trigger.getFireDefn(),
                        context,
                        True,
                        ),
                    )
                thread.start()
            else:
                # Non-venv Site - use the run driver directly
                # Note: Comma is needed at end to make this a tuple. DO NOT REMOVE
                thread = threading.Thread(
                    target=cast(SiteRun, runDriver)._submitJob,
                    args=(
                        trigger.getFireDefn(),
                        context,
                        ),
                    )
                thread.start()

    def _makeJobContext(self, trigger: JobEvent, parentContext: JobContext) -> JobContext:
        newContext = JobContext()
        # Inherit all relevant ancestry first
        newContext.addParentContext(parentContext)
        # Target site as requested by the event
        newContext.setSiteName(trigger.getFireSite())
        # Preserve the preassigned job id (from setEventHandler) if provided
        assigned_job_id = trigger.getFireJobId() or IdGenerator().generateId()
        newContext.setJobId(assigned_job_id)
        newContext.setNativeId(assigned_job_id)
        # Always inherit the parent's workflow id; do NOT take workflow from the event
        newContext.setWorkflowId(parentContext.getWorkflowId())
        # Parent linkage: prefer rule job id from the event if present
        newContext.setParentJobId(trigger.getRuleJobId() or parentContext.getJobId())
        return newContext

    def _makeDataContext(self, trigger: MetadataEvent, infoContext: JobContext) -> JobContext:
        newJobContext = JobContext()
        newJobContext.addParentContext(infoContext)
        newJobContext.setSiteName(trigger.getFireSite())
        newJobContext.setJobId(trigger.getFireJobId() or IdGenerator().generateId())
        newJobContext.setNativeId(trigger.getFireJobId() or newJobContext.getJobId())
        newJobContext.setParentJobId(infoContext.getJobId())
        newJobContext.setWorkflowId(infoContext.getWorkflowId())
        return newJobContext


    def checkRemoteJobEvents(self) -> bool:
        """
        Monitor remote jobs until they reach a terminal state, or until it seems 
        rediculous...
        """
        gotOne = False
        try:
            events: List = \
                self._eventStore.getAllWfEvents("run.event.REMOTE") or []
            if events is None:
                events = []
            if len(events) > 0:
                self._loggingStore.putLogging("INFO", "Remote events: " + str(len(events)),
                                              "", "", "") # TODO add context info
            for e in events:
                try:
                    self._loggingStore.putLogging("INFO",
                        f"remote id:{e.getFireJobId()} native:{e.getNativeJobId()} " + \
                        f"site:{e.getFireSite()}",
                        "", "", "") # TODO: add context info

                    # ask the remote site to inquire status
                    # Deferred import to avoid circular dependencies
                    from lwfm.midware.LwfManager import lwfManager
                    site = lwfManager.getSite(e.getFireSite())
                    # using canonical job id
                    status = site.getRunDriver().getStatus(e.getFireJobId())
                    if status is not None and status.isTerminal():
                        # remote job is done
                        self.unsetEventHandler(e.getEventId())
                    gotOne = True
                except JobNotFoundException as ex1:
                    # Job is not found on remote site - it has likely completed and been purged
                    # Remove the event handler to stop polling
                    self._loggingStore.putLogging("INFO",
                        f"Remote job {e.getFireJobId()} not found on {e.getFireSite()}, " + \
                        "assuming terminal state and removing event handler",
                        "", "", "")
                    self.unsetEventHandler(e.getEventId())
                except Exception as ex1:
                    # For other exceptions, check if it's a job not found error by string matching
                    # (for backward compatibility with sites that don't use JobNotFoundException)
                    if "Job not found" in str(ex1) or "not found" in str(ex1).lower():
                        self._loggingStore.putLogging("INFO",
                            f"Remote job {e.getFireJobId()} not found on {e.getFireSite()}, " + \
                            "assuming terminal state and removing event handler",
                            "", "", "")
                        self.unsetEventHandler(e.getEventId())
                    else:
                        self._loggingStore.putLogging("ERROR",
                        "Exception checking remote job event: " + str(ex1),
                        "", "", "") # TODO: add context info
        except Exception as ex:
            self._loggingStore.putLogging("ERROR", "Exception checking remote pollers: " + str(ex),
                                          "", "", "") # TODO: add context info
        return gotOne


    def checkJobEvent(self, jobEvent: JobEvent) -> Optional[JobStatus]:
        """
        Consider a job waiting on (already persisted) upstream job status events
        """
        try:
            statuses: List[JobStatus] = \
                self._jobStatusStore.getJobStatuses(jobEvent.getRuleJobId()) or []
            # the statuses will be in reverse chron order
            # does the history contain the state we want to fire on?
            for s in statuses:
                if s.getStatus() == jobEvent.getRuleStatus():
                    return s
            return None
        except Exception as ex:
            self._loggingStore.putLogging("ERROR",
                "Exception checking job event: " + str(ex),
                "", "", "") # TODO add context info


    def checkJobEvents(self) -> bool:
        """
        Consider jobs waiting on (already persisted) upstream job status events
        """
        gotOne = False
        try:
            events: List[WorkflowEvent] = self._eventStore.getAllWfEvents("run.event.JOB") \
                or []
            if events is None or events == []:
                l = 0
            else:
                l = len(events)
            if l > 0:
                self._loggingStore.putLogging("INFO", "Job events:    " + str(l),
                                              "", "", "") # TODO add context info
            if l == 0:
                return False
            for e in events:
                try:
                    if isinstance(e, NotificationEvent):
                        cast_e = cast(NotificationEvent, e)
                    else:
                        cast_e = cast(JobEvent, e)
                    status = self.checkJobEvent(cast_e)
                    if status:
                        self._loggingStore.putLogging("INFO",
                            f"triggered job id:{e.getFireJobId()} on site:{e.getFireSite()}",
                            "", "", "") # TODO add context info
                        # job event satisfied - going to fire the handler
                        # get a complete updated status
                        status = self._jobStatusStore.getJobStatus(status.getJobId())
                        if status is None:
                            self._loggingStore.putLogging("ERROR",
                                "checkJobEvents: Job status not found for event: " + str(e),
                                "", "", "") # TODO add context info
                            continue
                        # remove the handler
                        self.unsetEventHandler(e.getEventId())
                        # now launch it async
                        jobContext = self._makeJobContext(cast_e, status.getJobContext())
                        self._loggingStore.putLogging("INFO", f"*** {cast_e} {status} {jobContext}",
                                                      "", "", "")
                        # treat NotificationEvent differently
                        if isinstance(cast_e, NotificationEvent):
                            lwfManager.sendEmail(cast_e.getSubject(), cast_e.getBody(),
                                                 cast_e.getTo())
                        else:
                            self._runAsyncOnSite(cast_e, jobContext)
                        gotOne = True
                except Exception as ex1:
                    self._loggingStore.putLogging("ERROR",
                                                  "Exception checking job event: " + str(ex1),
                                                  "", "", "") # TODO add context info
        except Exception as ex:
            self._loggingStore.putLogging("ERROR",
                "Exception checking job events: " + str(ex), "", "", "") # TODO add context info
        return gotOne


    def checkDataEvent(self, dataEvent: MetadataEvent, jobStatus: JobStatus) -> bool:
        if dataEvent is None:
            return False
        if jobStatus is None:
            return False
        if jobStatus.getNativeInfo() is None:
            return False
        props = eval(jobStatus.getNativeInfo() or "{}")
        for key in dataEvent.getQueryRegExs().keys():
            if key in props.keys():
                keyVal = dataEvent.getQueryRegExs()[key]
                statVal = props[key]
                # the key val might have wildcards in it
                if not re.search(keyVal, statVal):
                    return False
            else:
                return False
        return True


    # the provided INFO status message was just emitted - are there any data
    # triggers waiting on its content?
    def checkDataStatusEvent(self, status: JobStatus) -> bool:
        gotOne = False
        try:
            # TODO make a better query
            events: List[WorkflowEvent] = self._eventStore.getAllWfEvents("run.event.DATA") \
                or []
            for e in events:
                try:
                    cast_e = cast(MetadataEvent, e)
                    if self.checkDataEvent(cast_e, status):
                        self._loggingStore.putLogging("INFO",
                            f"data triggered id:{e.getFireJobId()} on site:{e.getFireSite()}",
                            e.getFireSite(),
                            status.getJobContext().getWorkflowId(),
                            status.getJobContext().getJobId())
                        # event satisfied - going to fire the handler
                        # but first, remove the handler
                        self.unsetEventHandler(e.getEventId())
                        # now launch it async
                        self._runAsyncOnSite(e,
                            self._makeDataContext(cast_e, status.getJobContext()))
                        gotOne = True
                except Exception as ex1:
                    self._loggingStore.putLogging("ERROR",
                                                  "Exception checking data event: " + str(ex1),
                                                  "", "", "") # TODO add context info
        except Exception as ex:
            self._loggingStore.putLogging("ERROR", "Exception checking data events: " + str(ex),
                                          "", "", "") # TODO add context info
        return gotOne


    def checkEventHandlers(self):
        # consider jobs waiting on (already persisted) upstream job status events
        c1 = self.checkJobEvents()

        # consider unfinished jobs running on remote sites
        c2 = self.checkRemoteJobEvents()

        # we were busy, reduce the polling interval
        if (c1) or (c2):
            self._statusCheckIntervalSeconds = self.STATUS_CHECK_INTERVAL_SECONDS_MIN
        else:
            # make the next polling progressively longer unless we were busy
            if self._statusCheckIntervalSeconds < self.STATUS_CHECK_INTERVAL_SECONDS_MAX:
                self._statusCheckIntervalSeconds += self.STATUS_CHECK_INTERVAL_SECONDS_STEP

        with self._timer_lock:
            # Timers only run once, so re-trigger it
            self._timer = threading.Timer(
                self._statusCheckIntervalSeconds,
                LwfmEventProcessor.checkEventHandlers,
                (self,),
            )
            self._timer.start()


    def wake(self) -> None:
        """
        Wake the processor immediately and reset the sleep interval
        to the minimum so subsequent loops stay fast for a bit.
        """
        with self._timer_lock:
            now = time.time()
            # Prevent excessive wake calls - require at least 30 seconds between wakes
            # unless there's been significant idle time
            if (now - getattr(self, "_last_wake", 0.0)) < 30.0:
                return
            self._last_wake = now
            # Reset interval first so the next scheduled run uses the min
            self._statusCheckIntervalSeconds = self.STATUS_CHECK_INTERVAL_SECONDS_MIN
            # Cancel any pending timer and trigger a near-immediate check
            if self._timer:
                self._timer.cancel()
            self._timer = threading.Timer(
                0.01,  # wake almost immediately
                LwfmEventProcessor.checkEventHandlers,
                (self,),
            )
            self._timer.start()


    def _getWorkflowId(self, jobId: str) -> str:
        status = self._jobStatusStore.getJobStatus(jobId)
        if status is None:
            return jobId
        return status.getJobContext().getWorkflowId()


    def _initJobEventHandler(self, wfe: JobEvent) -> JobContext:
        # set the job context under which the new job will run, it will have a
        # new id and be a child of the setting job
        # need to also determine its workflow
        newJobContext = JobContext()
        newJobContext.setSiteName(wfe.getFireSite())
        newJobContext.setParentJobId(wfe.getRuleJobId())
        newJobContext.setWorkflowId(self._getWorkflowId(wfe.getRuleJobId()))
        # fire the initial status showing the new job ready on the shelf
        # Deferred import to avoid circular dependencies
        from lwfm.midware.LwfManager import lwfManager
        lwfManager._emitStatusFromEvent(newJobContext, JobStatus.READY,
            JobStatus.READY, "")
        return newJobContext


    def _initRemoteJobHandler(self, wfe: RemoteJobEvent) -> JobContext:
        newJobContext = JobContext()
        newJobContext.setSiteName(wfe.getFireSite())
        newJobContext.setWorkflowId(wfe.getFireJobId() or IdGenerator().generateId())
        newJobContext.setNativeId(wfe.getNativeJobId())
        return newJobContext

    def _initMetadataJobHandler(self, wfe: MetadataEvent) -> JobContext:
        newJobContext = JobContext()
        newJobContext.setSiteName(wfe.getFireSite())
        newJobContext.setWorkflowId(wfe.getWorkflowId())
        newJobContext.setParentJobId(wfe.getParentId() or wfe.getFireJobId())
        # fire a status showing the new job ready on the shelf
        # Deferred import to avoid circular dependencies
        from lwfm.midware.LwfManager import lwfManager
        lwfManager.emitStatus(newJobContext, JobStatus.READY,
            JobStatus.READY)
        return newJobContext

    # Register an event handler.  When a jobId running on a job Site
    # emits a particular Job Status, fire the given JobDefn (serialized)
    # at the target Site.  Return the new job id.
    def setEventHandler(self, wfe: WorkflowEvent) -> Optional[str]:
        typeT = ""
        try:
            # set by the user to fire a job after another one - these jobs may
            # be running anywhere and are referenced by their canonical lwfm job id
            if isinstance(wfe, JobEvent):
                initialContext = self._initJobEventHandler(wfe)
                wfe.setFireJobId(initialContext.getJobId())
                typeT = "JOB"
            # set by the system when a job is launched on a remote site to track it
            elif isinstance(wfe, RemoteJobEvent):
                initialContext = self._initRemoteJobHandler(wfe)
                typeT = "REMOTE"
            # set by the user to trigger a job when data with a certain metadata is put
            elif isinstance(wfe, MetadataEvent):
                initialContext = self._initMetadataJobHandler(wfe)
                wfe.setFireJobId(initialContext.getJobId())
                typeT = "DATA"
            # unknown type
            else:
                self._loggingStore.putLogging("ERROR", "setEventHandler: Unknown type",
                                              "", "", "") # TODO add context info
                return None
            # store the event handler
            self._eventStore.putWfEvent(wfe, typeT)
            return initialContext.getJobId()
        except Exception as ex:
            self._loggingStore.putLogging("ERROR", "setEventHandler: " + str(ex),
                                          "", "", "") # TODO add context info
            return None


    def unsetEventHandler(self, handlerId: str) -> None:
        try:
            self._eventStore.deleteWfEvent(handlerId)
            return
        except Exception as ex:
            self._loggingStore.putLogging("ERROR", "unsetEventHandler: " + str(ex),
                                          "", "", "") # TODO add context info
            return


    def testDataHandler(self, jobStatus: JobStatus) -> None:
        pass
