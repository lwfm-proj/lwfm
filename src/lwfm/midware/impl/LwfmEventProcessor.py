
# The Workflow Event Processor watches for Job Status events and fires a JobDefn 
# to a Site when an event of interest occurs.

import threading
from typing import List

from lwfm.base.JobStatus import JobStatus, JobStatusValues
from lwfm.base.JobContext import JobContext
from lwfm.base.WfEvent import RemoteJobEvent, WfEvent, JobEvent
from lwfm.base.Site import Site
from lwfm.midware.impl.Store import EventStore, JobStatusStore, LoggingStore
from lwfm.midware.LwfManager import LwfManager

# ***************************************************************************

class LwfmEventProcessor:
    _timer = None
    _eventHandlerMap = dict()

    _infoQueue: List[JobStatus] = []
    _eventStore: EventStore = None
    _jobStatusStore: JobStatusStore = None
    _loggingStore: LoggingStore = None

    # TODO We can make this adaptive later on, for now just wait 15 sec between polls
    STATUS_CHECK_INTERVAL_SECONDS = 60

    def __init__(self):
        self._eventStore = EventStore()
        self._jobStatusStore = JobStatusStore()
        self._loggingStore = LoggingStore()
        self._timer = threading.Timer(
            self.STATUS_CHECK_INTERVAL_SECONDS,
            LwfmEventProcessor.checkEventHandlers,
            (self,),
        )
        self._timer.start()


    def _runAsyncOnSite(self, trigger: JobEvent, parentContext: JobContext) -> None:
        site = Site.getSite(trigger.getFireSite())
        runDriver = site.getRun().__class__
        newJobContext = JobContext(parentContext)
        newJobContext.setSiteName(trigger.getFireSite())
        newJobContext.setId(trigger.getFireJobId())
        newJobContext.setNativeId(trigger.getFireJobId())   # can be later overridden to actual site job id, when known 
        # Note: Comma is needed at end to make this a tuple. DO NOT REMOVE
        thread = threading.Thread(
            target=runDriver._submitJob,
             args=(
                trigger.getFireDefn(),
                newJobContext,
                ),
            )
        thread.start()



    # monitor remote jobs until they reach terminal states
    def checkRemoteJobEvents(self):
        try:
            events: List[RemoteJobEvent] = self.findAllEvents("REMOTE")
            for e in events:
                print(f"id:{e.getFireJobId()} native:{e.getNativeJobId()} site:{e.getFireSite()}")
                # ask the remote site to inquire status
                site = Site.getSite(e.getFireSite())
                status = site.getRun().getStatus(e.getFireJobId())   # canonical job id
                print(f"status: {status}")
                if (status.isTerminal()):
                    # remote job is done
                    self.unsetEventHandler(e.getId())
        except Exception as ex:
            self._loggingStore.putLogging("ERROR", "Exception checking remote pollers: " + str(ex)) 


    def checkJobEvent(self, jobEvent: JobEvent) -> JobStatus:
        try:
            statuses = self._jobStatusStore.getAllJobStatuses(jobEvent.getRuleJobId())
            # the statuses will be in reverse chron order  
            # does the history contain the state we want to fire on?
            for s in statuses:
                if (s.getStatus().value == jobEvent.getRuleStatus()):
                    return s
            return None
        except Exception as ex:
            self._loggingStore.putLogging("ERROR", 
                                          "Exception checking job event: " + jobEvent.getRuleJobId() + " " + str(ex)) 


    def checkJobEvents(self) -> None:
        try:
            events = self.findAllEvents("JOB")
            for e in events:
                s = self.checkJobEvent(e)
                if (s):
                    # job event satisfied - going to fire the handler 
                    # but first, remove the handler 
                    self.unsetEventHandler(e.getId())
                    # now launch it async 
                    self._runAsyncOnSite(e, s.getJobContext())
        except Exception as ex:
            self._loggingStore.putLogging("ERROR", "Exception checking job events: " + str(ex)) 


    def checkDataEvents(self):
        if len(self._infoQueue) > 0:
            for key in list(self._eventHandlerMap):
                if not key.endswith("INFO.dt"):
                    continue
                for infoStatus in self._infoQueue:
                    messageConsumed = False
                    trigger = self._eventHandlerMap[key]
                    try:
                        passedFilter = trigger.runTriggerFilter(
                            infoStatus.getNativeInfo()
                        )
                        if passedFilter:
                            # fire the trigger defn
                            self.fireTrigger(trigger)
                            self.unsetEventHandler(key)
                            messageConsumed = True
                    except Exception as ex:
                        self._loggingStore.putLogging("ERROR", 
                                                      "Exception checking trigger: " + str(ex))
                        continue
                    if messageConsumed:
                        self._infoQueue.remove(infoStatus)


    def checkEventHandlers(self):
        self.checkJobEvents()
        self.checkRemoteJobEvents()
        #self.checkDataEvents()

        # Timers only run once, so re-trigger it
        self._timer = threading.Timer(
            self.STATUS_CHECK_INTERVAL_SECONDS,
            LwfmEventProcessor.checkEventHandlers,
            (self,),
        )
        self._timer.start()


    def _getOriginJobId(self, jobId: str) -> str:
        status = self._jobStatusStore.getJobStatus(jobId)
        if (status is None):
            return jobId
        return status.getJobContext().getOriginJobId()


    def _initJobEventHandler(self, wfe: JobEvent) -> JobContext:
        # set the job context under which the new job will run, it will have a 
        # new id and be a child of the setting job
        # need to also determine its originator  
        newJobContext = JobContext()
        newJobContext.setSiteName(wfe.getFireSite())
        newJobContext.setParentJobId(wfe.getRuleJobId())
        newJobContext.setOriginJobId(self._getOriginJobId(wfe.getRuleJobId()))    
        # fire the initial status showing the new job ready on the shelf 
        LwfManager.emitStatus(newJobContext, JobStatus, JobStatusValues.READY.value)
        return newJobContext


    def _initRemoteJobHandler(self, wfe: RemoteJobEvent) -> JobContext:
        newJobContext = JobContext()
        newJobContext.setSiteName(wfe.getFireSite())
        newJobContext.setParentJobId(wfe.getFireJobId())
        newJobContext.setOriginJobId(wfe.getFireJobId())    
        newJobContext.setNativeId(wfe.getNativeJobId())
        return newJobContext


    def findAllEvents(self, typeT: str = None) -> List[WfEvent]:
        try:
            print(f"findAllEvents: {typeT}")
            return self._eventStore.getAllWfEvents(typeT)
        except Exception as ex:
            self._loggingStore.putLogging("ERROR", __class__.__name__ + ".findAllEvents: " + str(ex))
            return None


    # Register an event handler.  When a jobId running on a job Site
    # emits a particular Job Status, fire the given JobDefn (serialized) 
    # at the target Site.  Return the new job id.
    def setEventHandler(self, wfe: WfEvent) -> str:
        typeT = ""
        try:
            if isinstance(wfe, JobEvent):
                context = self._initJobEventHandler(wfe)
                wfe.setFireJobId(context.getId())
                typeT = "JOB"
            elif isinstance(wfe, RemoteJobEvent):
                context = self._initRemoteJobHandler(wfe)
                typeT = "REMOTE"
            else:
                self._loggingStore.putLogging("ERROR", __class__.__name__ + ".setEventHandler: Unknown type")
                return None
            # store the event handler 
            self._eventStore.putWfEvent(wfe, typeT)
            return context.getId()
        except Exception as ex:
            self._loggingStore.putLogging("ERROR", __class__.__name__ + ".setEventHandler: " + str(ex))
            return None 


    def unsetEventHandler(self, handlerId: str) -> None:
        try:
            self._eventStore.deleteWfEvent(handlerId)
            return 
        except Exception as ex:
            self._loggingStore.putLogging("ERROR", __class__.__name__ + ".unsetEventHandler: " + str(ex))
            return 
    

    def testDataHandler(self, jobStatus: JobStatus) -> None:
        pass

    
    def exit(self):
        self._timer.cancel()

