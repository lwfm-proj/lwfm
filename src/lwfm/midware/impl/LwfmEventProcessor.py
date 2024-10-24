# TODO logging vs. print

# The Workflow Event Processor watches for Job Status events and fires a JobDefn to a
# Site when an event of interest occurs.

import threading
from typing import List


from lwfm.midware.Logger import Logger
from lwfm.base.JobStatus import JobStatus, JobStatusValues
from lwfm.base.JobContext import JobContext
from lwfm.base.WfEvent import WfEvent, JobEvent
from lwfm.base.Site import Site
from lwfm.midware.LwfManager import LwfManager



# ************************************************************************************


class LwfmEventProcessor:
    _timer = None
    _eventHandlerMap = dict()

    _infoQueue: List[JobStatus] = []

    # TODO
    # We can make this adaptive later on, for now let's just wait 15 sec between polls
    STATUS_CHECK_INTERVAL_SECONDS = 15

    def __init__(self):
        self._timer = threading.Timer(
            self.STATUS_CHECK_INTERVAL_SECONDS,
            LwfmEventProcessor.checkEventTriggers,
            (self,),
        )
        self._timer.start()

    def fireTrigger(self, trigger: WfEvent) -> bool:
        site = Site.getSite(trigger.getTargetSiteName())
        runDriver = site.getRun().__class__
        jobContext = trigger.getTargetContext()
        # Note: Comma is needed after FIRE_DEFN to make this a tuple. DO NOT REMOVE
        thread = threading.Thread(
            target=runDriver._submitJob,
            args=(
                trigger.getFireDefn(),
                jobContext,
                True,
            ),
        )
        try:
            thread.start()
        except Exception as ex:
            logging.error("Could not run job: " + ex)
            return False
        return True

    def checkEventTriggers(self):
        # if there are INFO messages in queue, run through each trigger, checking the
        # readiness to fire
        print("here in checkEventTriggers len = " + str(len(self._infoQueue)))
        if len(self._infoQueue) > 0:
            for key in list(self._eventHandlerMap):
                # TODO assume for now that job events will be processed as events warrant
                # if the key ends with "INFO.dt" then it is a data trigger
                if not key.endswith("INFO.dt"):
                    continue
                for infoStatus in self._infoQueue:
                    messageConsumed = False
                    print("*** getting trigger: " + str(key))
                    trigger = self._eventHandlerMap[key]
                    print("*** got trigger: " + str(trigger))
                    try:
                        print(
                            "Checking trigger native status: "
                            + str(infoStatus.getNativeInfo())
                        )
                        passedFilter = trigger.runTriggerFilter(
                            infoStatus.getNativeInfo()
                        )
                        print("*** passed filter: " + str(passedFilter))
                        if passedFilter:
                            # fire the trigger defn
                            self.fireTrigger(trigger)
                            self.unsetEventTrigger(key)
                            messageConsumed = True
                    except Exception as ex:
                        print("Exception checking trigger: " + str(ex))
                        continue
                    if messageConsumed:
                        print("Message consumed, removing from queue")
                        self._infoQueue.remove(infoStatus)

        # Timers only run once, so retrigger it
        self._timer = threading.Timer(
            self.STATUS_CHECK_INTERVAL_SECONDS,
            LwfmEventProcessor.checkEventTriggers,
            (self,),
        )
        self._timer.start()

    def _initJobEventTrigger(self, wfe: JobEvent) -> WfEvent:
        print("**** here in _initJobEventTrigger()")
        # set the job context under which the new job will run, it will have a new id
        # and be a child of the setting job
        newJobContext = JobContext(wfe.getRuleContext()) 

        #targetJobContext = JobContext(wfe.getParentContext())  
        #wfe.setFireContext(targetJobContext)
        # fire the initial status showing the new job pending
        newStatus = JobStatus(wfe.getFireSite())
        newStatus.setStatus(JobStatusValues.PENDING)
        newStatus.emit()
        return wfe

    """     def _initDataEventTrigger(self, wfet: DataEvent) -> WfEvent:
        # TODO
        # set the job context under which the new job will run
        newJobContext = JobContext()  # will assign a new job id
        newJobContext.setSiteName(wfet.getTargetSiteName())
        wfet.setTargetContext(newJobContext)
        # fire the initial status showing the new job pending
        newStatus = JobStatus(newJobContext)
        newStatus.setStatus(JobStatusValues.PENDING)
        newStatus.emit()
        return wfet """

    # Register an event handler.  When a jobId running on a job Site
    # emits a particular Job Status, fire the given JobDefn (serialized) at the target
    # Site.  Return the new job id.
    # TODO update doc, logging 
    def setEventTrigger(self, wfe: WfEvent) -> str:
        try:
            if isinstance(wfe, JobEvent):
                wfe = self._initJobEventTrigger(wfe)
            #elif isinstance(wfe, JobSetEvent):
            #    print("Setting JobSetEventTrigger... some other day")
            #    return None
            #elif isinstance(wfe, DataEvent):
            #    wfe = self._initDataEventTrigger(wfe)
            else:
                Logger.error(__class__.__name__ + ".setEventTrigger: Unknown type")
                return None
            # store the event handler in the cache
            Logger.info("Storing event handler in cache for key: " + str(wfe.getKey()))
            self._eventHandlerMap[wfe.getKey()] = wfe
            return wfe.getTargetContext().getId()
        except Exception as ex:
            Logger.error(__class__.__name__, str(ex))
            return None 

    #def unsetEventTrigger(self, handlerId: str) -> bool:
    #    try:
    #        self._eventHandlerMap.pop(handlerId)
    #        return True
    #    except Exception as ex:
    #        print(ex)
    #        return False

    #def unsetAllEventTriggers(self) -> None:
    #    self._eventHandlerMap = dict()

    """     def listActiveTriggers(self) -> List[WfEvent]:
        handlers = []
        for key in self._eventHandlerMap:
            handlers.append(key)
        return handlers """
    """ 
    # TODO docs and logging
    def runJobTrigger(self, jobStatus: JobStatus) -> bool:
        try:
            # is this an INFO status?  if so, put it in queue for later inspection
            # for user defined data triggers
            if jobStatus.getStatus() == JobStatusValues.INFO:
                self._infoQueue.append(jobStatus)

            # here, do i have a job trigger for this job id and its current state?
            # we can check in O(1) time
            jobTrigger = JobEvent(jobStatus.getJobId(), jobStatus.getStatusValue())
            if jobTrigger.getKey() in self._eventHandlerMap:
                # we have a job trigger
                jobTrigger = self._eventHandlerMap[jobTrigger.getKey()]
                # unset the event handler ASAP to help prevent race conditions
                self.unsetEventTrigger(jobTrigger.getKey())

                if (jobTrigger.getFireDefn() is None) or (
                    jobTrigger.getFireDefn() == ""
                ):
                    # we have no defn to fire - we've just been tracking status (why? TODO)
                    return True

                # Run in a thread instead of a subprocess so we don't have to make assumptions
                # about the environment
                site = Site.getSite(jobTrigger.getTargetSiteName())
                runDriver = site.getRun().__class__
                jobContext = jobTrigger.getTargetContext()

                jobContext.setOriginJobId(jobStatus.getJobContext().getOriginJobId())
                # Note: Comma is needed after FIRE_DEFN to make this a tuple. DO NOT REMOVE
                thread = threading.Thread(
                    target=runDriver._submitJob,
                    args=(
                        jobTrigger.getFireDefn(),
                        jobContext,
                        True,
                    ),
                )
                thread.start()
                return True
        except Exception as ex:
            logging.error("Could not prepare to run job: " + str(ex))
            return False
    """

    def runJobTrigger(self, jobStatus: JobStatus) -> bool:
        pass
    
    def exit(self):
        self._timer.cancel()
