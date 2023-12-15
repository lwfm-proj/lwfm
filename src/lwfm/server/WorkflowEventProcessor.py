# TODO logging vs. print

# The Workflow Event Processor watches for Job Status events and fires a JobDefn to a
# Site when an event of interest occurs.

import logging
import threading

from lwfm.base.JobStatus import JobStatus, JobStatusValues, JobContext, fetchJobStatus
from lwfm.base.Site import Site
from lwfm.base.WorkflowEventTrigger import (
    DataEventTrigger,
    JobEventTrigger,
    JobSetEventTrigger,
    WorkflowEventTrigger,
)


# ************************************************************************************


class WorkflowEventProcessor:
    _timer = None
    _eventHandlerMap = dict()
    # TODO
    # We can make this adaptive later on, for now let's just wait 15 sec between polls
    STATUS_CHECK_INTERVAL_SECONDS = 15  

    def __init__(self):
        self._timer = threading.Timer(
            self.STATUS_CHECK_INTERVAL_SECONDS,
            WorkflowEventProcessor.checkEventTriggers,
            (self,),
        )
        self._timer.start()


    def fireTrigger(self, trigger: WorkflowEventTrigger) -> bool:
        site = Site.getSiteInstanceFactory(
                trigger.getTargetSiteName()
            )
        runDriver = site.getRunDriver().__class__
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
        # Run through each event, checking the status
        for key in list(self._eventHandlerMap): 
            # TODO assume for now that job events will be processed as events warrant
            # if the key ends with "INFO.dt" then it is a data trigger
            if not key.endswith("INFO.dt"):
                continue
            trigger = self._eventHandlerMap[key]
            try:
                passedFilter = trigger.runTriggerFilter()
                if (passedFilter):
                    # fire the trigger defn 
                    self.fireTrigger(trigger)  
                    self.unsetEventTrigger(key)
            except Exception as ex: 
                print("Exception checking trigger: " + str(ex))
                continue
    
        # Timers only run once, so retrigger it
        self._timer = threading.Timer(
            self.STATUS_CHECK_INTERVAL_SECONDS,
            WorkflowEventProcessor.checkEventTriggers,
            (self,),
        )
        self._timer.start()

    def _initJobEventTrigger(self, wfet: JobEventTrigger) -> WorkflowEventTrigger:
        inStatus = fetchJobStatus(wfet.getJobId())
        wfet.setJobSiteName(inStatus.getJobContext().getSiteName())
        # set the job context under which the new job will run
        newJobContext = JobContext()  # will assign a new job id
        newJobContext.setParentJobId(inStatus.getJobContext().getId())
        newJobContext.setOriginJobId(inStatus.getJobContext().getOriginJobId())
        newJobContext.setSiteName(wfet.getTargetSiteName())
        wfet.setTargetContext(newJobContext)
        # fire the initial status showing the new job pending
        newStatus = JobStatus(newJobContext)
        newStatus.setStatus(JobStatusValues.PENDING)
        newStatus.emit()
        return wfet

    def _initDataEventTrigger(self, wfet: DataEventTrigger) -> WorkflowEventTrigger:
        # TODO
        # set the job context under which the new job will run
        newJobContext = JobContext()  # will assign a new job id
        newJobContext.setSiteName(wfet.getTargetSiteName())
        wfet.setTargetContext(newJobContext)
        # fire the initial status showing the new job pending
        newStatus = JobStatus(newJobContext)
        newStatus.setStatus(JobStatusValues.PENDING)
        newStatus.emit()
        return wfet

    # Regsiter an event handler.  When a jobId running on a job Site
    # emits a particular Job Status, fire the given JobDefn (serialized) at the target
    # Site.  Return the new job id.
    # TODO update doc, logging
    def setEventTrigger(self, wfet: WorkflowEventTrigger) -> str:
        try:
            # for debug  
            # perform per event trigger type initialization
            if isinstance(wfet, JobEventTrigger):
                wfet = self._initJobEventTrigger(wfet)
            elif isinstance(wfet, JobSetEventTrigger):
                print("Setting JobSetEventTrigger... some other day")
                return None
            elif isinstance(wfet, DataEventTrigger):
                wfet = self._initDataEventTrigger(wfet)
            else:
                print("Unknown type")
                return None
            # store the event handler in the cache
            print("Storing event handler in cache for key: " + str(wfet.getKey()))
            self._eventHandlerMap[wfet.getKey()] = wfet
            return wfet.getTargetContext().getId()
        except Exception as ex:
            print(ex)
            return None

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

    # TODO docs and logging
    def runJobTrigger(self, jobStatus):
        try:
            # do i have a trigger for this job id and its current state?
            # we can check in O(1) time
            jobTrigger = JobEventTrigger(
                jobStatus.getJobId(), jobStatus.getStatusValue()
            )
            if jobTrigger.getKey() in self._eventHandlerMap:
                # we have a job trigger 
                jobTrigger = self._eventHandlerMap[jobTrigger.getKey()]
                # unset the event handler ASAP to help prevent race conditions
                self.unsetEventTrigger(jobTrigger.getKey())

            if (jobTrigger.getFireDefn() is None) or (jobTrigger.getFireDefn() == ""):
                # we have no defn to fire - we've just been tracking status (why? TODO)
                return True

            # Run in a thread instead of a subprocess so we don't have to make assumptions
            # about the environment
            site = Site.getSiteInstanceFactory(
                jobTrigger.getTargetSiteName()
            )
            runDriver = site.getRunDriver().__class__
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
        except Exception as ex:
            logging.error("Could not prepare to run job: " + str(ex))
            return False

        try:
            thread.start()
        except Exception as ex:
            logging.error("Could not run job: " + ex)
            return False
        return True

    def exit(self):
        self._timer.cancel()
