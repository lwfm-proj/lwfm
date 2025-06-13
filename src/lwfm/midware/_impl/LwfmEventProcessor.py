"""
Run as part of the lwfm middleware.
The Event Processor watches for Job Status events and fires a JobDefn 
to a Site when an event of interest occurs.
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring
#pylint: disable = broad-exception-caught, protected-access, import-outside-toplevel

import re
import threading
from typing import List
import atexit

from lwfm.base.JobStatus import JobStatus
from lwfm.base.JobContext import JobContext
from lwfm.base.JobDefn import JobDefn
from lwfm.base.WorkflowEvent import WorkflowEvent, JobEvent, MetadataEvent
from lwfm.midware._impl.Store import EventStore, JobStatusStore, LoggingStore
from lwfm.midware._impl.SiteConfig import SiteConfig

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

    _infoQueue: List[JobStatus] = []

    STATUS_CHECK_INTERVAL_SECONDS_MIN = 1
    STATUS_CHECK_INTERVAL_SECONDS_MAX = 5*60
    STATUS_CHECK_INTERVAL_SECONDS_STEP = 5
    _statusCheckIntervalSeconds = STATUS_CHECK_INTERVAL_SECONDS_MIN


    def __init__(self):
        # Initialize instance variables
        self._timer_lock = threading.Lock()
        self._eventStore = None
        self._jobStatusStore = None
        self._loggingStore: LoggingStore = None
        self._timer = None
        self._eventHandlerMap = {}
        self._infoQueue = []
        self._statusCheckIntervalSeconds = self.STATUS_CHECK_INTERVAL_SECONDS_MIN

        # Register cleanup to happen at exit
        atexit.register(self.exit)
        self._eventStore = EventStore()
        self._jobStatusStore = JobStatusStore()
        self._loggingStore = LoggingStore()
        self._timer = threading.Timer(
            self._statusCheckIntervalSeconds,
            LwfmEventProcessor.checkEventHandlers,
            (self,),
        )
        self._timer.start()


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
                    "lwfManager.execSiteEndpoint(job_defn, context, False)"
                # Execute in the venv
                site_venv = SiteConfigVenv()
                self._loggingStore.putLogging("INFO", f"Executing in venv: {cmd}")
                site_venv.executeInProjectVenv(siteName, cmd)
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
                self._loggingStore.putLogging("INFO", f"Executing in venv: {cmd}")
                site_venv.executeInProjectVenv(siteName, cmd)
        else:
            if entryPointType == JobDefn.ENTRY_TYPE_SITE:
                # Non-venv Site - use the run driver directly
                # Note: Comma is needed at end to make this a tuple. DO NOT REMOVE
                thread = threading.Thread(
                    target=lwfManager.execSiteEndpoint,
                    args=(
                        trigger.getFireDefn(),
                        context,
                        ),
                    )
                thread.start()
            else:
                # Non-venv Site - use the run driver directly
                # Note: Comma is needed at end to make this a tuple. DO NOT REMOVE
                thread = threading.Thread(
                    target=runDriver._submitJob,
                    args=(
                        trigger.getFireDefn(),
                        context,
                        ),
                    )
                thread.start()

    def _makeJobContext(self, trigger: JobEvent, parentContext: JobContext) -> JobContext:
        newContext = JobContext()
        newContext.addParentContext(parentContext)
        newContext.setSiteName(trigger.getFireSite())
        newContext.setJobId(trigger.getFireJobId())
        newContext.setNativeId(trigger.getFireJobId())
        return newContext

    def _makeDataContext(self, trigger: MetadataEvent, infoContext: JobContext) -> JobContext:
        newJobContext = JobContext()
        newJobContext.addParentContext(infoContext)
        newJobContext.setSiteName(trigger.getFireSite())
        newJobContext.setJobId(trigger.getFireJobId())
        newJobContext.setNativeId(trigger.getFireJobId())
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
            events: List[RemoteJobEvent] = \
                self._eventStore.getAllWfEvents("run.event.REMOTE")
            if events is None:
                events = []
            self._loggingStore.putLogging("INFO", "Remote events: " + str(len(events)))
            for e in events:
                try:
                    self._loggingStore.putLogging("INFO",
                        f"remote id:{e.getFireJobId()} native:{e.getNativeJobId()} " + \
                        f"site:{e.getFireSite()}")
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
                except Exception as ex1:
                    self._loggingStore.putLogging("ERROR",
                    "Exception checking remote job event: " + str(ex1))
        except Exception as ex:
            self._loggingStore.putLogging("ERROR", "Exception checking remote pollers: " + str(ex))
        return gotOne


    def checkJobEvent(self, jobEvent: JobEvent) -> JobStatus:
        """
        Consider a job waiting on (already persisted) upstream job status events
        """
        try:
            statuses = self._jobStatusStore.getAllJobStatuses(jobEvent.getRuleJobId())
            # the statuses will be in reverse chron order
            # does the history contain the state we want to fire on?
            for s in statuses:
                if s.getStatus() == jobEvent.getRuleStatus():
                    return s
            return None
        except Exception as ex:
            self._loggingStore.putLogging("ERROR",
                "Exception checking job event: " + jobEvent.getRuleJobId() + " " + str(ex))


    def checkJobEvents(self) -> bool:
        """
        Consider jobs waiting on (already persisted) upstream job status events
        """
        gotOne = False
        try:
            events = self._eventStore.getAllWfEvents("run.event.JOB")
            if events is None:
                l = 0
            else:
                l = len(events)
            self._loggingStore.putLogging("INFO", "Job events:    " + str(l))
            if l == 0:
                return False
            for e in events:
                try:
                    status = self.checkJobEvent(e)
                    if status:
                        # job event satisfied - going to fire the handler
                        # but first, remove the handler
                        self.unsetEventHandler(e.getEventId())
                        # get a complete updated status
                        status = self._jobStatusStore.getJobStatus(status.getJobId())
                        # now launch it async
                        jobContext = self._makeJobContext(e, status.getJobContext())
                        self._runAsyncOnSite(e, jobContext)
                        gotOne = True
                except Exception as ex1:
                    self._loggingStore.putLogging("ERROR",
                                                  "Exception checking job event: " + str(ex1))
        except Exception as ex:
            self._loggingStore.putLogging("ERROR",
                "Exception checking job events: " + str(ex))
        return gotOne


    def checkDataEvent(self, dataEvent: MetadataEvent, jobStatus: JobStatus) -> bool:
        if dataEvent is None:
            return False
        if jobStatus is None:
            return False
        if jobStatus.getNativeInfo() is None:
            return False
        if jobStatus.getNativeInfo().getProps() is None:
            return False
        for key in dataEvent.getQueryRegExs().keys():
            if key in jobStatus.getNativeInfo().getProps().keys():
                keyVal = dataEvent.getQueryRegExs()[key]
                statVal = jobStatus.getNativeInfo().getProps()[key]
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
            events = self._eventStore.getAllWfEvents("run.event.DATA")
            print("Data events: " + str(len(events)))
            for e in events:
                try:
                    if self.checkDataEvent(e, status):
                        self._loggingStore.putLogging("INFO",
                            f"data triggered id:{e.getFireJobId()} on site:{e.getFireSite()}")
                        # event satisfied - going to fire the handler
                        # but first, remove the handler
                        self.unsetEventHandler(e.getEventId())
                        # now launch it async
                        self._runAsyncOnSite(e,
                            self._makeDataContext(e, status.getJobContext()))
                        gotOne = True
                except Exception as ex1:
                    self._loggingStore.putLogging("ERROR",
                                                  "Exception checking data event: " + str(ex1))
        except Exception as ex:
            self._loggingStore.putLogging("ERROR", "Exception checking data events: " + str(ex))
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
            JobStatus.READY, None)
        return newJobContext


    def _initRemoteJobHandler(self, wfe: RemoteJobEvent) -> JobContext:
        newJobContext = JobContext()
        newJobContext.setSiteName(wfe.getFireSite())
        newJobContext.setParentJobId(None)                  # TODO provenance?
        newJobContext.setWorkflowId(wfe.getFireJobId())
        newJobContext.setNativeId(wfe.getNativeJobId())
        return newJobContext

    def _initMetadataJobHandler(self, wfe: MetadataEvent) -> JobContext:
        newJobContext = JobContext()
        newJobContext.setSiteName(wfe.getFireSite())
        newJobContext.setParentJobId(None)  # we will know the parent & workflow when the
        newJobContext.setWorkflowId(None)  # metadata event occurs
        # fire a status showing the new job ready on the shelf
        # Deferred import to avoid circular dependencies
        from lwfm.midware.LwfManager import lwfManager
        lwfManager.emitStatus(newJobContext, JobStatus.READY,
            JobStatus.READY)
        return newJobContext

    # Register an event handler.  When a jobId running on a job Site
    # emits a particular Job Status, fire the given JobDefn (serialized)
    # at the target Site.  Return the new job id.
    def setEventHandler(self, wfe: WorkflowEvent) -> str:
        typeT = ""
        try:
            # set by the user to fire a job after another one - these jobs may
            # be running anywhere and are referenced by their canonical lwfm job id
            if isinstance(wfe, JobEvent):
                self._loggingStore.putLogging("INFO", "its a job event")
                initialContext = self._initJobEventHandler(wfe)
                wfe.setFireJobId(initialContext.getJobId())
                typeT = "JOB"
            # set by the system when a job is launched on a remote site to track it
            elif isinstance(wfe, RemoteJobEvent):
                self._loggingStore.putLogging("INFO", "its a remote event")
                initialContext = self._initRemoteJobHandler(wfe)
                typeT = "REMOTE"
            # set by the user to trigger a job when data with a certain metadata is put
            elif isinstance(wfe, MetadataEvent):
                initialContext = self._initMetadataJobHandler(wfe)
                wfe.setFireJobId(initialContext.getJobId())
                typeT = "DATA"
            # unknown type
            else:
                self._loggingStore.putLogging("ERROR", "setEventHandler: Unknown type")
                return None
            # store the event handler
            self._eventStore.putWfEvent(wfe, typeT)
            return initialContext.getJobId()
        except Exception as ex:
            self._loggingStore.putLogging("ERROR", "setEventHandler: " + str(ex))
            return None


    def unsetEventHandler(self, handlerId: str) -> None:
        try:
            self._eventStore.deleteWfEvent(handlerId)
            return
        except Exception as ex:
            self._loggingStore.putLogging("ERROR", "unsetEventHandler: " + str(ex))
            return


    def testDataHandler(self, jobStatus: JobStatus) -> None:
        pass
