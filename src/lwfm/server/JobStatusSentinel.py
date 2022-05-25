
# The Job Status Sentinel watches for Job Status events and fires a JobDefn to a Site when an event of interest occurs.
# The service exposes a way to set/unset event handlers, list jobs currently being watched.
# The service must have some persistence to allow for very long running jobs.
#
# We'll implement simple event handling for now, but one could envision handlers on sets of jobs, fuzzy satisfaction, timeouts,
# recurring event handlers, scheduled jobs, etc.
#

from enum import Enum
import logging
from flask import Flask, request
import pickle
import requests
import threading

from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus, JobStatusValues
from lwfm.base.Site import Site
from lwfm.base.LwfmBase import LwfmBase, _IdGenerator


#************************************************************************************************************************************
# tracks an event being waited upon, and the information to launch the responding job when it happens

class _EventHandlerFields(Enum):
    JOB_ID           = "jobId"
    JOB_SITE_NAME    = "jobSiteName"
    JOB_STATUS       = "jobStatus"
    FIRE_DEFN        = "fireDefn"
    TARGET_SITE_NAME = "targetSiteName"


class EventHandler(LwfmBase):
    def __init__(self, jobId: str, jobSiteName: str, jobStatus: str, fireDefn: str, targetSiteName: str):
        super(EventHandler, self).__init__()
        LwfmBase._setArg(self, _EventHandlerFields.JOB_ID.value, jobId)
        LwfmBase._setArg(self, _EventHandlerFields.JOB_SITE_NAME.value, jobSiteName)
        LwfmBase._setArg(self, _EventHandlerFields.JOB_STATUS.value, jobStatus)
        LwfmBase._setArg(self, _EventHandlerFields.FIRE_DEFN.value, fireDefn)
        LwfmBase._setArg(self, _EventHandlerFields.TARGET_SITE_NAME.value, targetSiteName)

    def getHandlerId(self) -> str:
        return self.getKey()

    def getKey(self):
        # We want to permit more than one event handler for the same job, but for now we'll limit it to one handler per
        # canonical job status name.
        return str(LwfmBase._getArg(self, _EventHandlerFields.JOB_ID.value) +
                   "." +
                   LwfmBase._getArg(self, _EventHandlerFields.JOB_STATUS.value))


#************************************************************************************************************************************

class JobStatusSentinel:

    _eventHandlerMap = dict()
    STATUS_CHECK_INTERVAL_SECONDS = 300 # We can make this adaptive later on, for now let's just wait five minutes between cycling through the list

    def __init__(self):
        timer = threading.Timer(self.STATUS_CHECK_INTERVAL_SECONDS, JobStatusSentinel.checkEvents, (self,))
        timer.start()

    def checkEvents(self):
        # Run through each event, checking the status
        for handler in self._eventHandlerMap:
            site = handler._getArg( _EventHandlerFields.JOB_SITE_NAME.value)
            # Local jobs can instantly emit their own statuses, on demand
            if site != "local":
                # Get the job's status
                jobId = handler._getArg( _EventHandlerFields.JOB_ID.value)
                runDriver = Site.getSiteInstanceFactory(site).getRunDriver()
                jobStatus = runDriver.getJobStatus(jobId)

                # We don't need to go through the Flask API to emit a status, just run directly
                key = EventHandler(jobId, None, jobStatus, None, None).getKey()
                self.runHandler(key)


        # Timers only run once, so retrigger it
        timer = threading.Timer(self.STATUS_CHECK_INTERVAL_SECONDS, JobStatusSentinel.checkEvents, (self,))
        timer.start()

    # Regsiter an event handler with the sentinel.  When a jobId running on a job Site emits a particular Job Status, fire
    # the given JobDefn (serialized) at the target Site.  Return the hander id.
    def setEventHandler(self, jobId: str, jobSiteName: str, jobStatus: str,
                        fireDefn: str, targetSiteName: str) -> str:
        eventHandler = EventHandler(jobId, jobSiteName, jobStatus, fireDefn, targetSiteName)
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

    def runHandler(self, handlerId):
        # unset the event handler ASAP to help prevent race conditions
        if handlerId not in self._eventHandlerMap:
            return False
        handler = self._eventHandlerMap[handlerId]
        self.unsetEventHandler(handlerId)

        # Run in a thread instead of a subprocess so we don't have to make assumptions about the environment
        site = Site.getSiteInstanceFactory(handler._getArg( _EventHandlerFields.TARGET_SITE_NAME.value))
        runDriver = site.getRunDriver().__class__
        # Note: Comma is needed after FIRE_DEFN to make this a tuple. DO NOT REMOVE
        thread = threading.Thread(target = runDriver._submitJob, args = (handler._getArg( _EventHandlerFields.FIRE_DEFN.value),))
        try:
            thread.start()
        except Exception as ex:
            logging.error("Could not run job: " + ex)
            return False
        return True


#************************************************************************************************************************************

class JobStatusSentinelClient:
    # TODO - do the right thing...
    _JSS_URL = "http://127.0.0.1:5000"

    def getUrl(self):
        return self._JSS_URL

    def setEventHandler(self, jobId: str, jobSiteName: str, jobStatus: str,
                        fireDefn: str, targetSiteName: str) -> str:
        payload = {}
        payload["jobId"] = jobId
        payload["jobSiteName"] = jobSiteName
        payload["jobStatus"] = jobStatus
        payload["fireDefn"] = pickle.dumps(fireDefn, 0).decode() # Use protocol 0 so we can easily convert to an ASCII string
        payload["targetSiteName"] = targetSiteName
        response = requests.post(f'{self.getUrl()}/set', payload)
        if response.ok:
            return response.text
        else:
            logging.error(response)
            return None

    def unsetEventHandler(self, handlerId: str) -> bool:
        response = requests.get(f'{self.getUrl()}/unset/{handlerId}')
        if response.ok:
            return True
        else:
            return False

    def unsetAllEventHandlers(self) -> bool:
        response = requests.get(f'{self.getUrl()}/unsetAll')
        if response.ok:
            return True
        else:
            return False

    def listActiveHandlers(self) -> [str]:
        response = requests.get(f'{self.getUrl()}/list')
        if response.ok:
            return eval(response.text)
        else:
            logging.error(response)
            return None

    def emitStatus(self, jobId, jobStatus):
        data = {'jobId' : jobId,
                'jobStatus': jobStatus}
        response = requests.post(f'{self.getUrl()}/emit', data=data)
        if response.ok:
            return None
        else:
            logging.error(response)
            return None



#************************************************************************************************************************************
# Flask app

app = Flask(__name__)
jss = JobStatusSentinel()


@app.route('/')
def index():
  return str(True)

@app.route('/emit', methods=['POST'])
def emitStatus():
    jobId = request.form['jobId']
    jobStatus = request.form['jobStatus']
    key = EventHandler(jobId, None, jobStatus, None, None).getKey()
    jss.runHandler(key) # This will check to see if the handler is in the JSS store, and run if so
    return '', 200



@app.route('/set', methods = ['POST'])
def setHandler():
    jobId = request.form['jobId']
    jobSiteName = request.form['jobSiteName']
    jobStatus = request.form['jobStatus']
    fireDefn = pickle.loads(request.form['fireDefn'].encode())
    targetSiteName = request.form['targetSiteName']

    handlerId = jss.setEventHandler(jobId, jobSiteName, jobStatus, fireDefn, targetSiteName)
    return handlerId

# unset a given handler
@app.route('/unset/<handlerId>')
def unsetHandler(handlerId : str):
    return str(jss.unsetEventHandler(handlerId))

# unset all handers
@app.route('/unsetAll')
def unsetAllHandlers():
    jss.unsetAllEventHandlers()
    return str(True)

# list the ids of all active handers
@app.route('/list')
def listHandlers():
  return str(jss.listActiveHandlers())


#************************************************************************************************************************************

# test
if __name__ == '__main__':

    # basic server list handling
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    jss = JobStatusSentinel()
    handlerId = jss.setEventHandler("123", None, JobStatusValues.INFO.value, None, None)
    print(str(jss.listActiveHandlers()))
    jss.unsetEventHandler(handlerId)
    print(str(jss.listActiveHandlers()))

    # basic client test
    jssClient = JobStatusSentinelClient()
    print("*** " + str(jssClient.listActiveHandlers()))
    print("*** " + str(jssClient.unsetAllEventHandlers()))
    handlerId = jssClient.setEventHandler("123", "nersc", "INFO", "{jobDefn}", "nersc")
    print("*** " + handlerId)
    print("*** " + str(jssClient.listActiveHandlers()))
    print("*** " + str(jssClient.unsetEventHandler(handlerId)))
