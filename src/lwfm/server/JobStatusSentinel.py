
# The Job Status Sentinel watches for Job Status events and fires a JobDefn to a Site when an event of interest occurs.
# The service exposes a way to set/unset event handlers, list jobs currently being watched.
# The service must have some persistence to allow for very long running jobs.
#
# We'll implement simple event handling for now, but one could envision handlers on sets of jobs, fuzzy satisfaction, timeouts,
# recurring event handlers, scheduled jobs, etc.
#

from enum import Enum
import logging

from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus, JobStatusValues
from lwfm.base.Site import Site
from lwfm.base.LwfmBase import LwfmBase, _IdGenerator

from flask import Flask


#************************************************************************************************************************************
# tracks an event being waited upon, and the information to launch the responding job when it happens

class _EventHandlerFields(Enum):
    HANDLER_ID  = "id"
    JOB_ID      = "jobId"
    JOB_SITE    = "jobSite"
    JOB_STATUS  = "jobStatus"
    FIRE_DEFN   = "fireDefn"
    TARGET_SITE = "targetSite"


class EventHandler(LwfmBase):
    def __init__(self, handlerId: str, jobId: str, jobSite: Site, jobStatus: JobStatusValues, fireDefn: JobDefn, targetSite: Site):
        super(EventHandler, self).__init__()
        LwfmBase._setArg(self, _EventHandlerFields.HANDLER_ID.value, handlerId)
        LwfmBase._setArg(self, _EventHandlerFields.JOB_ID.value, jobId)
        LwfmBase._setArg(self, _EventHandlerFields.JOB_SITE.value, jobSite)
        LwfmBase._setArg(self, _EventHandlerFields.JOB_STATUS.value, jobStatus)
        LwfmBase._setArg(self, _EventHandlerFields.FIRE_DEFN.value, fireDefn)
        LwfmBase._setArg(self, _EventHandlerFields.TARGET_SITE.value, targetSite)

    def getHandlerId(self) -> str:
        return LwfmBase._getArg(self, _EventHandlerFields.HANDLER_ID.value)

    def getKey(self):
        # We want to permit more than one event handler for the same job, but for now we'll limit it to one handler per
        # canonical job status name.
        return str(LwfmBase._getArg(self, _EventHandlerFields.JOB_ID.value) +
                   "." +
                   LwfmBase._getArg(self, _EventHandlerFields.JOB_STATUS.value).value)


#************************************************************************************************************************************

class JobStatusSentinel:

    _eventHandlerMap: dict[str,EventHandler] = dict()

    # Regsiter an event handler with the sentinel.  When a jobId running on a job Site emits a particular Job Status, fire
    # the given JobDefn at the target Site.
    def setEventHandler(self, jobId: str, jobSite: Site, jobStatus: JobStatusValues,
                        fireDefn: JobDefn, targetSite: Site) -> str:
        handlerId = _IdGenerator.generateId()
        eventHandler = EventHandler(handlerId, jobId, jobSite, jobStatus, fireDefn, targetSite)
        self._eventHandlerMap[eventHandler.getKey()] = eventHandler
        return handlerId

    def unsetEventHandler(self, handlerId: str) -> bool:
        # We are optimized to key off job id since this is the high traffic case.  So to delete an event handler we
        # have to find it first.
        for key in self._eventHandlerMap:
            eventHandler = self._eventHandlerMap[key]
            if (eventHandler.getHandlerId() == handlerId):
                self._eventHandlerMap.pop(key)
                return True
        # must not have found it
        return False

    def listActiveHandlers(self) -> [str]:
        handlers = []
        for key in self._eventHandlerMap:
            handlers.append(key)
        return handlers


#************************************************************************************************************************************
# Flask app

app = Flask(__name__)
jss = JobStatusSentinel()


@app.route('/')
def index():
  return 'Server Works!'

@app.route('/set')
def setHandler():
    handlerId = jss.setEventHandler("123", None, JobStatusValues.INFO, None, None)
    return 'set handler ' + handlerId

@app.route('/unset')
def unsetHandler():
  return str(jss.unsetEventHandler("123"))

@app.route('/list')
def listHandlers():
  return str(jss.listActiveHandlers())


#************************************************************************************************************************************

# test
if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    jss = JobStatusSentinel()
    handlerId = jss.setEventHandler("123", None, JobStatusValues.INFO, None, None)
    print(str(jss.listActiveHandlers()))
    jss.unsetEventHandler(handlerId)
    print(str(jss.listActiveHandlers()))
