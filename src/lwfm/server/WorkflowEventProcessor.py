# TODO logging vs. print

# The Workflow Event Processor watches for Job Status events and fires a JobDefn to a 
# Site when an event of interest occurs.

import logging
import threading

from lwfm.base.JobStatus import JobStatus, JobStatusValues, JobContext, fetchJobStatus
from lwfm.base.Site import Site
from lwfm.base.WorkflowEventTrigger import (
    _JobEventTriggerFields, 
    JobEventTrigger 
)

 
# ************************************************************************************


class WorkflowEventProcessor:
    _timer = None
    _eventHandlerMap = dict()
    # TODO 
    # We can make this adaptive later on, for now let's just wait five minutes between 
    # cycling through the list
    STATUS_CHECK_INTERVAL_SECONDS = 15  # 300

    def __init__(self):
        self._timer = threading.Timer(
            self.STATUS_CHECK_INTERVAL_SECONDS,
            WorkflowEventProcessor.checkEvents,
            (self,),
        )
        self._timer.start()

    def checkEvents(self):
        # Run through each event, checking the status
        for key in list(self._eventHandlerMap):
            handler = self._eventHandlerMap[key]
            jobId = handler._getArg(_JobEventTriggerFields.JOB_ID.value)
            site = handler._getArg(_JobEventTriggerFields.JOB_SITE_NAME.value)
            # Local jobs can instantly emit their own statuses, on demand
            if site != "local":
                # Get the job's status
                runDriver = Site.getSiteInstanceFactory(site).getRunDriver()
                context = JobContext()
                context.setId(jobId)
                context.setNativeId(jobId)
                jobStatus = runDriver.getJobStatus(context)
                if (
                    handler._getArg(_JobEventTriggerFields.FIRE_DEFN.value) == ""
                ) or (
                    handler._getArg(_JobEventTriggerFields.FIRE_DEFN.value) is None
                ):
                    # if we are in a terminal state, "run the handler" which means "evict" 
                    # the job from checking in the future - we have seen its terminal state
                    # we do have a target context, which gives us the parent and origin job 
                    # ids
                    context = handler._getArg(
                        _JobEventTriggerFields.TARGET_CONTEXT.value
                    )
                    jobStatus.setParentJobId(context.getParentJobId())
                    jobStatus.setOriginJobId(context.getOriginJobId())
                    jobStatus.setNativeId(context.getNativeId())
                    jobStatus.getJobContext().setId(context.getId())
                    jobStatus.emit()
                    if jobStatus.isTerminal():
                        # evict
                        key = JobEventTrigger(
                            jobId, None, "<<TERMINAL>>", None, None, None
                        ).getKey()
                        self.unsetEventTrigger(key)
                else:
                    key = JobEventTrigger(jobId, jobStatus, None, None).getKey()
                    self.runTrigger(key, jobStatus)

        # Timers only run once, so retrigger it
        self._timer = threading.Timer(
            self.STATUS_CHECK_INTERVAL_SECONDS, WorkflowEventProcessor.checkEvents, (self,)
        )
        self._timer.start()

    # Regsiter an event handler.  When a jobId running on a job Site 
    # emits a particular Job Status, fire the given JobDefn (serialized) at the target 
    # Site.  Return the new job id.
    # TODO update doc
    def setEventTrigger(
        self, jobId: str, jobStatus: str, fireDefn: str, targetSiteName: str
    ) -> str:
        try:
            eventHandler = JobEventTrigger(
                jobId, jobStatus, fireDefn, targetSiteName
            )
            inStatus = fetchJobStatus(jobId)
            eventHandler.setJobSiteName(inStatus.getJobContext().getSiteName())
            newJobContext = JobContext()  # will assign a new job id
            newJobContext.setParentJobId(inStatus.getJobContext().getId())
            newJobContext.setOriginJobId(inStatus.getJobContext().getOriginJobId())
            newJobContext.setSiteName(targetSiteName)
            # set the job context under which the new job will run
            eventHandler.setTargetContext(newJobContext)
            eventHandler.setTargetSiteName(targetSiteName)
            # store the event handler in the cache
            self._eventHandlerMap[eventHandler.getKey()] = eventHandler
            # fire the initial status showing the new job pending
            newStatus = JobStatus(newJobContext)
            newStatus.setStatus(JobStatusValues.PENDING)
            newStatus.emit()
            return eventHandler.getTargetContext().getId()
        except Exception as ex:
            print(ex)

    def unsetEventTrigger(self, handlerId: str) -> bool:
        try:
            self._eventHandlerMap.pop(handlerId)
            return True
        except Exception as ex:
            print(ex)
            return False

    def unsetAllEventTriggers(self) -> None:
        self._eventHandlerMap = dict()

    def hasTrigger(self, id):
        return id in self._eventHandlerMap

    def listActiveTriggers(self) -> [str]:
        handlers = []
        for key in self._eventHandlerMap:
            handlers.append(key)
        return handlers

    def runTrigger(self, triggerId, jobStatus):
        try:
            # unset the event handler ASAP to help prevent race conditions
            if triggerId not in self._eventHandlerMap:
                return False
            handler = self._eventHandlerMap[triggerId]
            self.unsetEventTrigger(triggerId)

            if handler._getArg(_JobEventTriggerFields.FIRE_DEFN.value) == "":
                # we have no defn to fire - we've just been tracking status
                return True

            # Run in a thread instead of a subprocess so we don't have to make assumptions 
            # about the environment
            site = Site.getSiteInstanceFactory(
                handler._getArg(_JobEventTriggerFields.TARGET_SITE_NAME.value)
            )
            runDriver = site.getRunDriver().__class__
            jobContext = handler._getArg(
                _JobEventTriggerFields.TARGET_CONTEXT.value
            )
            jobContext.setOriginJobId(jobStatus.getJobContext().getOriginJobId())
            # Note: Comma is needed after FIRE_DEFN to make this a tuple. DO NOT REMOVE
            thread = threading.Thread(
                target=runDriver._submitJob,
                args=(
                    handler._getArg(_JobEventTriggerFields.FIRE_DEFN.value),
                    jobContext,
                    True,
                ),
            )
        except Exception as ex:
            logging.error("Could not prepare to run job: " + ex)
            return False

        try:
            thread.start()
        except Exception as ex:
            logging.error("Could not run job: " + ex)
            return False
        return True

    def exit(self):
        self._timer.cancel()
