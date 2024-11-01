
# ***********************************************************************************
# Flask app

from flask import Flask, request
from lwfm.midware.impl.LwfmEventProcessor import LwfmEventProcessor
from lwfm.base.JobStatus import JobStatus
from lwfm.base.WfEvent import WfEvent
from lwfm.midware.impl.Store import JobStatusStore, LoggingStore
import logging

#************************************************************************
# startup 

app = Flask(__name__)
app.logger.disabled = True
log = logging.getLogger("werkzeug")
log.setLevel(logging.ERROR)
wfProcessor = LwfmEventProcessor()

_statusStore = JobStatusStore()
_loggingStore = LoggingStore()

print("*** service starting")


#************************************************************************
# root endpoint 

@app.route("/")
def index():
    return str(True)


#************************************************************************
# status endpoints 

@app.route("/emitStatus", methods=["POST"])
def emitStatus():
    try:
        statusBlob = request.form["statusBlob"]
        statusObj = JobStatus.deserialize(statusBlob)
        print("emitting statusObj: " + str(statusObj))
        _statusStore.putJobStatus(statusObj)
        return "", 200
    except Exception as ex:
        _loggingStore.putLogging("ERROR", "emitStatus: " + str(ex))
        return "", 400


@app.route("/status/<jobId>")
def getStatus(jobId: str):
    try:
        return _statusStore.getJobStatus(jobId).serialize()
    except Exception:
        return ""


#************************************************************************
# logging endpoints

@app.route("/emitLogging", methods=["POST"])
def emitLogging():
    try:
        level = request.form["level"]
        errorMsg = request.form["errorMsg"]
        _loggingStore.putLogging(level, errorMsg)
        return "", 200
    except Exception as ex:
        _loggingStore.putLogging("ERROR", "emitLogging: " + str(ex))
        return "", 400


#************************************************************************
# event endpoints
# set a trigger
@app.route("/setEvent", methods=["POST"])
def setHandler():
    try:
        obj = WfEvent.deserialize(request.form["eventObj"])   
        return wfProcessor.setEventHandler(obj), 200
    except Exception as ex:
        _loggingStore.putLogging("ERROR", "setEvent: " + str(ex))
        return "", 400


# unset a given handler
@app.route("/unsetEvent/<handlerId>")
def unsetHandler(handlerId: str):
    wfProcessor.unsetEventHandler(handlerId)
    return "", 200


# list the ids of all active handlers
@app.route("/listEvents")
def listHandlers():
    l = wfProcessor.findAllEventHandlers()
    return [e.serialize() for e in l], 200

#************************************************************************





