"""
Run as part of the lwfm middleware.
The Event Processor watches for Job Status events and fires a JobDefn 
to a Site when an event of interest occurs.
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring
#pylint: disable = broad-exception-caught, protected-access

import re
import threading
from typing import List

from lwfm.base.JobStatus import JobStatus, JobStatusValues
from lwfm.base.JobContext import JobContext
from lwfm.base.WorkflowEvent import RemoteJobEvent, WorkflowEvent, JobEvent, MetadataEvent
from lwfm.base.Site import Site
from lwfm.midware.LwfManager import lwfManager
from lwfm.midware.impl.Store import EventStore, JobStatusStore, LoggingStore

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
        self._eventStore = EventStore()
        self._jobStatusStore = JobStatusStore()
        self._loggingStore = LoggingStore()
        self._loggingStore.putLogging("INFO", "LwfmEventProcessor initialized")
        self._timer = threading.Timer(
            self._statusCheckIntervalSeconds,
            LwfmEventProcessor.checkEventHandlers,
            (self,),
        )
        self._timer.start()


    def _runAsyncOnSite(self, trigger: WorkflowEvent, context: JobContext) -> None:
        site = Site.getSite(trigger.getFireSite())
        runDriver = site.getRunDriver().__class__
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


    # monitor remote jobs until they reach terminal states
    def checkRemoteJobEvents(self) -> bool:
        gotOne = False
        try:
            events: List[RemoteJobEvent] = \
                self._eventStore.getAllWfEvents("run.event.REMOTE")
            for e in events:
                try:
                    self._loggingStore.putLogging("INFO",
                        f"remote id:{e.getFireJobId()} native:{e.getNativeJobId()} site:{e.getFireSite()}")
                    # ask the remote site to inquire status
                    site = Site.getSite(e.getFireSite())
                    status = site.getRunDriver().getStatus(e.getFireJobId())   # canonical job id
                    if status.isTerminal():
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
        try:
            statuses = self._jobStatusStore.getAllJobStatuses(jobEvent.getRuleJobId())
            # the statuses will be in reverse chron order
            # does the history contain the state we want to fire on?
            for s in statuses:
                if s.getStatus().value == jobEvent.getRuleStatus():
                    return s
            return None
        except Exception as ex:
            self._loggingStore.putLogging("ERROR",
                "Exception checking job event: " + jobEvent.getRuleJobId() + " " + str(ex))


    def checkJobEvents(self) -> bool:
        gotOne = False
        try:
            events = self._eventStore.getAllWfEvents("run.event.JOB")
            if events is None:
                l = 0
            else:
                l = len(events)
            self._loggingStore.putLogging("INFO", "Job events: " + str(l))
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
            self._loggingStore.putLogging("ERROR", "Exception checking job events: " + str(ex))
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
    def checkDataEvents(self, status: JobStatus) -> bool:
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
        c1 = self.checkJobEvents()
        c2 = 0 # TODO self.checkRemoteJobEvents()

        # we were busy, reduce the polling interval
        if (c1) or (c2):
            self._statusCheckIntervalSeconds = self.STATUS_CHECK_INTERVAL_SECONDS_MIN
        else:
            # make the next polling progressively longer unless we were busy
            if self._statusCheckIntervalSeconds < self.STATUS_CHECK_INTERVAL_SECONDS_MAX:
                self._statusCheckIntervalSeconds += self.STATUS_CHECK_INTERVAL_SECONDS_STEP

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
        lwfManager.emitStatus(newJobContext, JobStatus, JobStatusValues.READY.value)
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
        lwfManager.emitStatus(newJobContext, JobStatus, JobStatusValues.READY.value)
        return newJobContext

    # Register an event handler.  When a jobId running on a job Site
    # emits a particular Job Status, fire the given JobDefn (serialized)
    # at the target Site.  Return the new job id.
    def setEventHandler(self, wfe: WorkflowEvent) -> str:
        typeT = ""
        try:
            if isinstance(wfe, JobEvent):
                initialContext = self._initJobEventHandler(wfe)
                wfe.setFireJobId(initialContext.getJobId())
                typeT = "JOB"
            elif isinstance(wfe, RemoteJobEvent):
                initialContext = self._initRemoteJobHandler(wfe)
                typeT = "REMOTE"
            elif isinstance(wfe, MetadataEvent):
                initialContext = self._initMetadataJobHandler(wfe)
                wfe.setFireJobId(initialContext.getJobId())
                typeT = "DATA"
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


    def exit(self):
        self._timer.cancel()
