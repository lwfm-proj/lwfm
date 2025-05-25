"""
Flask app service for the lwfm middleware
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring
#pylint: disable = broad-exception-caught, protected-access

import json
import logging
import atexit
import signal
import sys
from typing import List

from flask import Flask, request
from lwfm.midware._impl.LwfmEventProcessor import LwfmEventProcessor
from lwfm.midware._impl.Store import JobStatusStore, LoggingStore, WorkflowStore, MetasheetStore
from lwfm.base.Workflow import Workflow
from lwfm.base.Metasheet import Metasheet
from lwfm.base.JobStatus import JobStatus, JobStatusValues
from lwfm.midware._impl.ObjectSerializer import ObjectSerializer

#************************************************************************
# startup

app = Flask(__name__)
app.logger.disabled = True
log = logging.getLogger("werkzeug")
log.setLevel(logging.ERROR)

# Get the singleton instance of the event processor
wfProcessor = LwfmEventProcessor()


def halt():
    print("*** halting event processor")
    wfProcessor.exit()

# Register cleanup to happen at exit
atexit.register(halt)


def sigterm_handler(sig, frame):
    print("Flask app received SIGTERM, cleaning up...")
    halt()
    sys.exit(0)

signal.signal(signal.SIGTERM, sigterm_handler)


#************************************************************************
# root endpoint

@app.route("/")
def index():
    return str(True)


@app.route("/isRunning")
def isRunning():
    return str(True), 200


# curl http://host:port/shutdown
@app.route("/shutdown")
def shutdown():
    halt()
    sys.exit(0)


#************************************************************************
# workflow endpoints


@app.route("/workflow/<workflow_id>")
def getWorkflow(workflow_id: str):
    try:
        w = WorkflowStore().getWorkflow(workflow_id)
        if w is not None:
            return w.serialize()
        return "", 404
    except Exception as ex:
        LoggingStore().putLogging("ERROR", "getWorkflow: " + str(ex))
        return "", 500

@app.route("/workflow", methods=["POST"])
def putWorkflow():
    try:
        workflow : Workflow = ObjectSerializer.deserialize(request.form["workflowObj"])
        WorkflowStore().putWorkflow(workflow)
        return "", 200
    except Exception as ex:
        LoggingStore().putLogging("ERROR", "putWorkflow: " + str(ex))
        return "", 500

#************************************************************************
# status endpoints

def _testDataTriggers(statusObj: JobStatus):
    LwfmEventProcessor().checkDataEvents(statusObj)


@app.route("/emitStatus", methods=["POST"])
def emitStatus():
    try:
        statusBlob = request.form["statusBlob"]
        statusObj : JobStatus = ObjectSerializer.deserialize(statusBlob)
        JobStatusStore().putJobStatus(statusObj)
        if statusObj.getStatus() == JobStatusValues.READY.value or \
            statusObj.getStatus() == JobStatusValues.PENDING.value:
            wfId = statusObj.getJobContext().getWorkflowId()
            wf = WorkflowStore().getWorkflow(wfId)
            if wf is None:
                wf = Workflow()
                wf._setWorkflowId(wfId)
                WorkflowStore().putWorkflow(wf)
        elif statusObj.getStatus() == JobStatusValues.INFO.value:
            print("test for data triggers goes here")
            _testDataTriggers(statusObj)
        return "", 200
    except Exception as ex:
        LoggingStore().putLogging("ERROR", "emitStatus svc: " + str(ex))
        return "", 400


@app.route("/status/<jobId>")
def getStatus(jobId: str):
    try:
        s = JobStatusStore().getJobStatus(jobId)
        if s is not None:
            return ObjectSerializer.serialize(s)
        return ""
    except Exception:
        LoggingStore().putLogging("ERROR", "Unable to /getStatus() for jobId: " + jobId)
        return ""

@app.route("/statusAll/<jobId>")
def getStatusAll(jobId: str):
    try:
        s = JobStatusStore().getAllJobStatuses(jobId)
        if s is not None:
            return ObjectSerializer.serialize(s)
        return ""
    except Exception:
        LoggingStore().putLogging("ERROR", "Unable to /getStatusAll() for jobId: " + jobId)
        return ""


#************************************************************************
# logging endpoints

@app.route("/emitLogging", methods=["POST"])
def emitLogging():
    try:
        level = request.form["level"]
        errorMsg = request.form["errorMsg"]
        LoggingStore().putLogging(level, errorMsg)
        return "", 200
    except Exception as ex:
        LoggingStore().putLogging("ERROR", "emitLogging: " + str(ex))
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
        LoggingStore().putLogging("ERROR", "setEvent: " + str(ex))
        return "", 400


# unset a given handler
@app.route("/unsetEvent/<handlerId>")
def unsetHandler(handlerId: str):
    wfProcessor.unsetEventHandler(handlerId)
    return "", 200


# list the ids of all active handlers
@app.route("/listEvents")
def listHandlers():
    return


#************************************************************************
# data endpoints

@app.route("/notate", methods=["POST"])
def notate():
    try:
        blob = request.form["data"]
        sheet = ObjectSerializer.deserialize(blob)
        MetasheetStore().putMetasheet(sheet)
        return "", 200
    except Exception as ex:
        LoggingStore().putLogging("ERROR", "notate: " + str(ex))
        return "", 400


@app.route("/find", methods=["POST"])
def find():
    try:
        searchDict = json.loads(request.form["searchDict"])
        sheets: List[Metasheet] = MetasheetStore().findMetasheet(searchDict)
        return ObjectSerializer.serialize(sheets), 200
    except Exception as ex:
        LoggingStore().putLogging("ERROR", "find: " + str(ex))
        return "", 400
