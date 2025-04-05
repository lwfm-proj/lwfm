"""
Flask app service for the lwfm middleware
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring
#pylint: disable = broad-exception-caught

import json
import logging

from flask import Flask, request
from .LwfmEventProcessor import LwfmEventProcessor
from .Store import JobStatusStore, LoggingStore, MetaRepoStore
from ...base.JobStatus import JobStatus
from ...util.ObjectSerializer import ObjectSerializer

#************************************************************************
# startup

app = Flask(__name__)
app.logger.disabled = True
log = logging.getLogger("werkzeug")
log.setLevel(logging.ERROR)
wfProcessor = LwfmEventProcessor()

_statusStore = JobStatusStore()
_loggingStore = LoggingStore()
_metaStore = MetaRepoStore()

print("*** service starting")


#************************************************************************
# root endpoint

@app.route("/")
def index():
    return str(True)


#************************************************************************
# status endpoints

def _testDataTriggers(statusObj: JobStatus):
    LwfmEventProcessor().checkDataEvents(statusObj)


@app.route("/emitStatus", methods=["POST"])
def emitStatus():
    try:
        statusBlob = request.form["statusBlob"]
        statusObj : JobStatus = ObjectSerializer.deserialize(statusBlob)
        _statusStore.putJobStatus(statusObj)
        if statusObj.getStatusValue() == "INFO":
            _testDataTriggers(statusObj)
        return "", 200
    except Exception as ex:
        _loggingStore.putLogging("ERROR", "emitStatus: " + str(ex))
        return "", 400


@app.route("/status/<jobId>")
def getStatus(jobId: str):
    try:
        s = _statusStore.getJobStatus(jobId)
        if s is not None:
            return ObjectSerializer.serialize(s)
        return ""
    except Exception:
        # TODO log something
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
        obj = ObjectSerializer.deserialize(request.form["eventObj"])
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
    l = wfProcessor.findAllEvents()
    return [e.serialize() for e in l], 200

#************************************************************************
# data endpoints

@app.route("/notate", methods=["POST"])
def notate():
    try:
        blob = request.form["data"]
        sheet = ObjectSerializer.deserialize(blob)
        _metaStore.putMetaRepo(sheet)
        return "", 200
    except Exception as ex:
        _loggingStore.putLogging("ERROR", "emitStatus: " + str(ex))
        return "", 400


@app.route("/find", methods=["POST"])
def find():
    try:
        searchDict = json.loads(request.form["searchDict"])
        l = _metaStore.find(searchDict)
        return [e.serialize() for e in l], 200
    except Exception as ex:
        _loggingStore.putLogging("ERROR", "find: " + str(ex))
        return "", 400
