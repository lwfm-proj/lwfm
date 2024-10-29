# TODO logging vs. print

# The Workflow Event Processor watches for Job Status events and fires a JobDefn 
# to a Site when an event of interest occurs.

import threading
from typing import List

from lwfm.midware.Logger import Logger
from lwfm.base.JobStatus import JobStatus, JobStatusValues
from lwfm.base.JobContext import JobContext
from lwfm.base.WfEvent import WfEvent, JobEvent
from lwfm.base.Site import Site
from lwfm.midware.impl.Store import EventStore, JobStatusStore
from lwfm.midware.LwfManager import LwfManager

# ***************************************************************************

class LwfmEventProcessor:
    _timer = None
    _eventHandlerMap = dict()

    _infoQueue: List[JobStatus] = []
    _eventStore: EventStore = None
    _jobStatusStore: JobStatusStore = None

    # TODO
    # We can make this adaptive later on, for now just wait 15 sec between polls
    STATUS_CHECK_INTERVAL_SECONDS = 15

    def __init__(self):
        self._timer = threading.Timer(
            self.STATUS_CHECK_INTERVAL_SECONDS,
            LwfmEventProcessor.checkEventTriggers,
            (self,),
        )
        self._timer.start()
        self._eventStore = EventStore()
        self._jobStatusStore = JobStatusStore()


    def _runAsyncOnSite(self, trigger: WfEvent, jobContext: JobContext = None) -> None:
        site = Site.getSite(trigger.getFireSite())
        runDriver = site.getRun().__class__
        newJobContext = jobContext
        if (jobContext is not None):
            newJobContext = JobContext(jobContext)
            newJobContext.setSiteName(trigger.getFireSite())
            newJobContext.setId(trigger.getFireJobId())
            newJobContext.setNativeId(trigger.getFireJobId())
        # Note: Comma is needed at end to make this a tuple. DO NOT REMOVE
        thread = threading.Thread(
            target=runDriver._submitJob,
             args=(
                trigger.getFireDefn(),
                newJobContext,
                ),
            )
        thread.start()


    def getStatusBlob(self, jobId: str) -> str:
        return self._jobStatusStore.getJobStatusBlob(jobId)


    def fireTrigger(self, trigger: WfEvent) -> bool:
        try: 
            self._runAsyncOnSite(trigger)
        except Exception as ex:
            Logger.error("Could not run job: " + str(ex))
            return False
        return True


    def checkRemotePollers(self):
        try:
            pass
        except Exception as ex:
            Logger.error("Could not check remote pollers: " + str(ex))
            return False
        return True


    def checkEventTriggers(self):
        self.checkRemotePollers()
        if len(self._infoQueue) > 0:
            for key in list(self._eventHandlerMap):
                # TODO assume for now that job events will be processed as needed
                # if the key ends with "INFO.dt" then it is a data trigger
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
                            self.unsetEventTrigger(key)
                            messageConsumed = True
                    except Exception as ex:
                        Logger.error("Exception checking trigger: " + str(ex))
                        continue
                    if messageConsumed:
                        self._infoQueue.remove(infoStatus)

        # Timers only run once, so re-trigger it
        self._timer = threading.Timer(
            self.STATUS_CHECK_INTERVAL_SECONDS,
            LwfmEventProcessor.checkEventTriggers,
            (self,),
        )
        self._timer.start()


    def _initJobEventTrigger(self, wfe: JobEvent) -> JobContext:
        # set the job context under which the new job will run, it will have a 
        # new id and be a child of the setting job
        newJobContext = JobContext()
        newJobContext.setSiteName(wfe.getFireSite())
        newJobContext.setParentJobId(wfe.getRuleJobId())
        newJobContext.setOriginJobId(wfe.getRuleJobId())    
        # TODO there would be a lookup to know the parent's origin
        # fire the initial status showing the new job ready on the shelf 
        LwfManager.emitStatus(newJobContext, JobStatus, JobStatusValues.READY)
        return newJobContext


    def findAllEventTriggers(self) -> List[str]:
        return self._eventStore.getAllWfEvents()


    # Register an event handler.  When a jobId running on a job Site
    # emits a particular Job Status, fire the given JobDefn (serialized) 
    # at the target Site.  Return the new job id.
    def setEventTrigger(self, wfe: WfEvent) -> str:
        try:
            if isinstance(wfe, JobEvent):
                context = self._initJobEventTrigger(wfe)
            else:
                Logger.error(__class__.__name__ + ".setEventTrigger: Unknown type")
                return None
            # store the event handler 
            wfe.setFireJobId(context.getId())
            self._eventStore.putWfEvent(wfe)
            return context.getId()
        except Exception as ex:
            Logger.error(__class__.__name__, str(ex))
            return None 


    def unsetEventTrigger(self, handlerId: str) -> bool:
        try:
            self._eventHandlerMap.pop(handlerId)
            return True
        except Exception as ex:
            print(ex)
            return False


    #def unsetAllEventTriggers(self) -> None:
    #    self._eventHandlerMap = dict()
    

    def testDataTrigger(self, jobStatus: JobStatus) -> None:
        # TODO implement
        pass


    def testJobStatus(self, jobStatus: JobStatus) -> None:
        try:
            # is this an INFO status? inspect for user defined data events 
            if jobStatus.getStatus() == JobStatusValues.INFO:
                self.testDataTrigger(jobStatus)

            # do i have a job event for this job id and its current state?
            key = JobEvent.getJobEventKey(jobStatus.getJobId(), 
                                          jobStatus.getStatus())
            if key in self._eventHandlerMap:
                # we have a job trigger 
                jobTrigger = self._eventHandlerMap[key]
                # consume it, un-setting ASAP 
                self.unsetEventTrigger(key)

                if (jobTrigger.getFireDefn() is None) \
                    or (jobTrigger.getFireDefn() == "") \
                    or (jobTrigger.getFireSite() is None) \
                    or (jobTrigger.getFireSite() == ""):
                    return 

                self._runAsyncOnSite(jobTrigger, jobStatus.getJobContext())
                return 
        except Exception as ex:
            Logger.error("Could not prepare to run job: " + str(ex))
            return 
    
    def exit(self):
        self._timer.cancel()
