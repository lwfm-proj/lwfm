
# The Job Status Sentinel watches for Job Status events and fires a JobDefn to a Site when an event of interest occurs.
# The service exposes a way to set/unset event handlers, list jobs currently being watched.
# The service must have some persistence to allow for very long running jobs.
#
# We'll implement simple event handling for now, but one could envision handlers on sets of jobs, fuzzy satisfaction, timeouts,
# recurring event handlers, scheduled jobs, etc.
#

from enum import Enum
import logging
import threading

from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus, JobStatusValues, JobContext
from lwfm.base.Site import Site
from lwfm.base.LwfmBase import LwfmBase, _IdGenerator
from lwfm.base.JobEventHandler import JobEventHandler, _JobEventHandlerFields


#************************************************************************************************************************************

class JobStatusSentinel:

    _timer = None
    _eventHandlerMap = dict()
    # We can make this adaptive later on, for now let's just wait five minutes between cycling through the list
    STATUS_CHECK_INTERVAL_SECONDS = 15 # 300

    def __init__(self):
        self._timer = threading.Timer(self.STATUS_CHECK_INTERVAL_SECONDS, JobStatusSentinel.checkEvents, (self,))
        self._timer.start()

    def checkEvents(self):
        # Run through each event, checking the status
        for key in list(self._eventHandlerMap):
            handler = self._eventHandlerMap[key]
            jobId = handler._getArg( _JobEventHandlerFields.JOB_ID.value)
            site = handler._getArg( _JobEventHandlerFields.JOB_SITE_NAME.value)
            logging.error("Checking event found for job " + jobId + " on site " + site)
            # Local jobs can instantly emit their own statuses, on demand
            if site != "local":
                logging.info("Inside if site != local")
                # Get the job's status
                runDriver = Site.getSiteInstanceFactory(site).getRunDriver()
                context = JobContext()
                context.setId(jobId)
                context.setNativeId(jobId)
                jobStatus = runDriver.getJobStatus(context)
                if ((handler._getArg( _JobEventHandlerFields.FIRE_DEFN.value) == "") or
                    (handler._getArg( _JobEventHandlerFields.FIRE_DEFN.value) is None)):
                    # if we are in a terminal state, "run the handler" which means "evict" the job from checking
                    # in the future - we have seen its terminal state
                    # we do have a target context, which gives us the parent and origin job ids
                    context = (handler._getArg( _JobEventHandlerFields.TARGET_CONTEXT.value))
                    jobStatus.setParentJobId(context.getParentJobId())
                    jobStatus.setOriginJobId(context.getOriginJobId())
                    jobStatus.setNativeId(context.getNativeId())
                    jobStatus.getJobContext().setId(context.getId())
                    jobStatus.emit()
                    if (jobStatus.isTerminal()):
                        # evict
                        key = JobEventHandler(jobId, None, "<<TERMINAL>>", None, None, None).getKey()
                        self.unsetEventHandler(key)
                else:
                    key = JobEventHandler(jobId, None, jobStatus, None, None, None).getKey()
                    self.runHandler(key, jobStatus)

        # Timers only run once, so retrigger it
        self._timer = threading.Timer(self.STATUS_CHECK_INTERVAL_SECONDS, JobStatusSentinel.checkEvents, (self,))
        self._timer.start()


    # Regsiter an event handler with the sentinel.  When a jobId running on a job Site emits a particular Job Status, fire
    # the given JobDefn (serialized) at the target Site.  Return the handler id.
    def setEventHandler(self, jobId: str, jobSiteName: str, jobStatus: str,
                        fireDefn: str, targetSiteName: str, targetContext: JobContext) -> str:
        eventHandler = JobEventHandler(jobId, jobSiteName, jobStatus, fireDefn, targetSiteName, targetContext)
        self._eventHandlerMap[eventHandler.getKey()] = eventHandler
        return eventHandler.getKey()

    def unsetEventHandler(self, handlerId: str) -> bool:
        try:
            self._eventHandlerMap.pop(handlerId)
            return True
        except:
            return False

    def unsetAllEventHandlers(self) -> None:
        self._eventHandlerMap = dict()

    def hasHandler(self, handlerId):
        return handlerId in self._eventHandlerMap

    def listActiveHandlers(self) -> [str]:
        handlers = []
        for key in self._eventHandlerMap:
            handlers.append(key)
        return handlers

    def runHandler(self, handlerId, jobStatus):
        # unset the event handler ASAP to help prevent race conditions
        if handlerId not in self._eventHandlerMap:
            return False
        handler = self._eventHandlerMap[handlerId]
        self.unsetEventHandler(handlerId)

        if (handler._getArg( _JobEventHandlerFields.FIRE_DEFN.value) == ""):
            # we have no defn to fire - we've just been tracking status
            return True

        # Run in a thread instead of a subprocess so we don't have to make assumptions about the environment
        site = Site.getSiteInstanceFactory(handler._getArg( _JobEventHandlerFields.TARGET_SITE_NAME.value))
        runDriver = site.getRunDriver().__class__
        jobContext = handler._getArg(_JobEventHandlerFields.TARGET_CONTEXT.value)
        jobContext.setOriginJobId(jobStatus.getJobContext().getOriginJobId())
        # Note: Comma is needed after FIRE_DEFN to make this a tuple. DO NOT REMOVE
        thread = threading.Thread(target = runDriver._submitJob,
                                  args = (handler._getArg( _JobEventHandlerFields.FIRE_DEFN.value),jobContext,) )
        try:
            thread.start()
        except Exception as ex:
            logging.error("Could not run job: " + ex)
            return False
        return True

    def exit(self):
        self._timer.cancel()


#************************************************************************************************************************************

# test
if __name__ == '__main__':

    # basic server list handling
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    jss = JobStatusSentinel()
    handlerId = jss.setEventHandler("123", None, JobStatusValues.INFO.value, None, None)
    logging.info(str(jss.listActiveHandlers()))
    jss.unsetEventHandler(handlerId)
    logging.info(str(jss.listActiveHandlers()))
    jss.exit()
