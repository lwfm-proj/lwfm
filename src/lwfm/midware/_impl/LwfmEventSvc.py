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
import os
from lwfm.midware._impl.LwfmEventProcessor import LwfmEventProcessor, RemoteJobEvent
from lwfm.midware._impl.Store import JobStatusStore, LoggingStore, WorkflowStore, MetasheetStore
from lwfm.base.Workflow import Workflow
from lwfm.base.Metasheet import Metasheet
from lwfm.base.JobStatus import JobStatus
from lwfm.midware._impl.ObjectSerializer import ObjectSerializer
from lwfm.midware._impl.SiteConfig import SiteConfig
from lwfm.midware._impl.Store import EventStore

#************************************************************************
# startup

app = Flask(__name__)
app.logger.disabled = True
log = logging.getLogger("werkzeug")
log.setLevel(logging.ERROR)

# Get the singleton instance of the event processor
wfProcessor = LwfmEventProcessor.get_instance()


def halt():
    print("*** halting event processor")
    wfProcessor.exit()

# Register cleanup to happen at exit
atexit.register(halt)


def sigterm_handler(_, __):
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
    """Attempt a clean shutdown of the development server."""
    try:
        # Preferred way for Werkzeug dev server
        func = request.environ.get('werkzeug.server.shutdown')
        if func is not None:
            func()
        else:
            # Fallback: signal ourselves to terminate
            os.kill(os.getpid(), signal.SIGTERM)
        return "shutting down", 200
    except Exception:
        try:
            os.kill(os.getpid(), signal.SIGTERM)
        except Exception:
            pass
        return "shutdown attempted", 200


#************************************************************************
# workflow endpoints


@app.route("/workflow/<workflow_id>")
def getWorkflow(workflow_id: str):
    try:
        w = WorkflowStore().getWorkflow(workflow_id)
        if w is not None:
            return ObjectSerializer.serialize(w)
        return "", 404
    except Exception as ex:
        LoggingStore().putLogging("ERROR", "getWorkflow: " + str(ex),
                                  "", workflow_id, "") # TODO context
        return "", 500

@app.route("/workflow", methods=["POST"])
def putWorkflow():
    try:
        workflow : Workflow = ObjectSerializer.deserialize(request.form["workflowObj"])
        WorkflowStore().putWorkflow(workflow)
        return "", 200
    except Exception as ex:
        LoggingStore().putLogging("ERROR", "putWorkflow: " + str(ex),
                                  "", "", "") # TODO context
        return "", 500

@app.route("/workflows")
def getWorkflows():
    try:
        workflows = WorkflowStore().getAllWorkflows()
        if workflows is None:
            # First-run empty DB: return an empty list (200) to avoid client error spam
            return ObjectSerializer.serialize([]), 200
        # Serialize each workflow for transport
        serialized_workflows = ObjectSerializer.serialize(workflows)
        return serialized_workflows, 200
    except Exception as ex:
        LoggingStore().putLogging("ERROR", "getWorkflows: " + str(ex),
                                  "", "", "") # TODO context
        return "", 500


@app.route("/workflow/<workflow_id>/statuses")
def getWorkflowStatuses(workflow_id: str):
    try:
        statuses = JobStatusStore().getJobStatusesForWorkflow(workflow_id)
        if statuses is not None:
            serialized_statuses = ObjectSerializer.serialize(statuses)
            return serialized_statuses
        return "", 404
    except Exception as ex:
        LoggingStore().putLogging("ERROR", "getWorkflowStatuses: " + str(ex),
                                  "", "", "") # TODO context
        return "", 500



@app.route("/workflow/find", methods=["POST"])
def findWorkflow():
    try:
        searchDict = json.loads(request.form["searchDict"])
        workflows: List[Workflow] = WorkflowStore().findWorkflows(searchDict)
        return ObjectSerializer.serialize(workflows), 200
    except Exception as ex:
        LoggingStore().putLogging("ERROR", "findWorkflow: " + str(ex),
                                  "", "", "") # TODO context
        return "", 400


#************************************************************************
# status endpoints


@app.route("/emitStatus", methods=["POST"])
def emitStatus():
    try:
        statusBlob = request.form["statusBlob"]
        fromEvent = False
        try:
            fromEvent = request.form["fromEvent"].lower() == "true"
        except Exception:
            pass
        statusObj : JobStatus = ObjectSerializer.deserialize(statusBlob)
        JobStatusStore().putJobStatus(statusObj)

        # if this is a new job, make sure we persisted its parent workflow
        if statusObj.getStatus() == JobStatus.READY or \
            statusObj.getStatus() == JobStatus.PENDING:
            wfId = statusObj.getJobContext().getWorkflowId()
            wf = WorkflowStore().getWorkflow(wfId)
            if wf is None:
                wf = Workflow()
                wf._setWorkflowId(wfId)
                WorkflowStore().putWorkflow(wf)

        # is this site a remote site? make sure we're tracking this job
        # to completion - lwfm is responsible for that not the user
        try:
            props = SiteConfig.getSiteProperties(statusObj.getJobContext().getSiteName())
            if props is not None:
                isRemote = props.get("remote", False)
            else:
                isRemote = False
            if isRemote:
                # check if a remote job event is pending
                events = EventStore().getAllWfEvents("run.event.REMOTE")
                gotOne = False
                if events is None:
                    events = []
                for e in events:
                    if e.getFireJobId() == statusObj.getJobContext().getJobId():
                        gotOne = True
                        break
                if not gotOne and not fromEvent:
                    # lay down a new remote job tracking event
                    LoggingStore().putLogging("INFO",
                        f"laying down remote job event {statusObj.getJobId()}",
                        "", "", "") # TODO context
                    wfProcessor.setEventHandler(RemoteJobEvent(statusObj.
                          getJobContext()))
        except Exception as ex:
            LoggingStore().putLogging("ERROR",
                "Exception putting remote job event handler: " + " " + str(ex),
                "", "", "") # TODO context

        # is this an INFO? check for data triggers
        if statusObj.getStatus() == JobStatus.INFO:
            if wfProcessor.checkDataStatusEvent(statusObj):
                LoggingStore().putLogging("INFO",
                    f"Data trigger event(s) processed for job {statusObj.getJobId()}",
                    "", "", "") # TODO context

        # Wake the event processor so it reacts immediately to newly emitted status
        try:
            wfProcessor.wake()
        except Exception as ex:
            LoggingStore().putLogging("ERROR", f"wake() failed: {ex}", "", "", "")

        return "", 200
    except Exception as ex:
        LoggingStore().putLogging("ERROR", "emitStatus svc: " + str(ex),
                                  "", "", "") # TODO context
        return "", 400


@app.route("/status/<jobId>")
def getStatus(jobId: str):
    try:
        s = JobStatusStore().getJobStatus(jobId)
        if s is not None:
            return ObjectSerializer.serialize(s)
        return ""
    except Exception:
        LoggingStore().putLogging("ERROR", "Unable to /getStatus() for jobId: " + jobId,
                                  "", jobId, "") # TODO context
        return ""

@app.route("/statusAll/<jobId>")
def getStatusAll(jobId: str):
    try:
        s = JobStatusStore().getJobStatuses(jobId)
        if s is not None:
            return ObjectSerializer.serialize(s)
        return ""
    except Exception:
        LoggingStore().putLogging("ERROR", "Unable to /getStatusAll() for jobId: " + jobId,
                                  "", jobId, "") # TODO context
        return ""


#************************************************************************
# logging endpoints

@app.route("/emitLogging", methods=["POST"])
def emitLogging():
    try:
        level = request.form["level"]
        errorMsg = request.form["errorMsg"]
        site = request.form.get("site", "")
        workflowId = request.form.get("workflowId", "")
        jobId = request.form.get("jobId", "")
        LoggingStore().putLogging(level, errorMsg, site, workflowId, jobId)
        return "", 200
    except Exception as ex:
        LoggingStore().putLogging("ERROR", "emitLogging: " + str(ex),
                                  "", "", "") # TODO context
        return "", 400


@app.route("/logs/workflow/<workflow_id>")
def getLogsByWorkflow(workflow_id: str):
    try:
        logs = LoggingStore().getLogsByWorkflow(workflow_id)
        if logs is not None:
            return ObjectSerializer.serialize(logs), 200
        return "", 200
    except Exception as ex:
        LoggingStore().putLogging("ERROR", "getLogsByWorkflow: " + str(ex),
                                    "", workflow_id, "") # TODO context
        return "", 500

@app.route("/logs/job/<job_id>")
def getLogsByJob(job_id: str):
    try:
        logs = LoggingStore().getLogsByJob(job_id)
        if logs is not None:
            print("*** got logs")
            return ObjectSerializer.serialize(logs), 200
        return "", 200
    except Exception as ex:
        LoggingStore().putLogging("ERROR", "getLogsByJob: " + str(ex),
                                    "", "", job_id) # TODO context
        return "", 500

@app.route("/logs/all")
def getAllLogs():
    try:
        logs = LoggingStore().getAllLogs()
        if logs is not None:
            return ObjectSerializer.serialize(logs), 200
        return "", 200
    except Exception as ex:
        LoggingStore().putLogging("ERROR", "getAllLogs: " + str(ex),
                                    "", "", "") # TODO context
        return "", 500



#************************************************************************
# event endpoints
# set a trigger
@app.route("/setEvent", methods=["POST"])
def setHandler():
    try:
        obj = ObjectSerializer.deserialize(request.form["eventObj"])
        result = wfProcessor.setEventHandler(obj)
        if result is None:
            return "", 200
        return str(result), 200
    except Exception as ex:
        LoggingStore().putLogging("ERROR", "setEvent: " + str(ex),
                                  "", "", "") # TODO context
        return "", 400


# unset a given handler
@app.route("/unsetEvent/<handlerId>")
def unsetHandler(handlerId: str):
    wfProcessor.unsetEventHandler(handlerId)
    return "", 200


# get all active workflow events (handlers)
@app.route("/listEvents")
def listHandlers():
    try:
        # Get all active workflow events (handlers)
        events = EventStore().getAllWfEvents(None)
        if events is None:
            return "", 200
        return ObjectSerializer.serialize(events), 200
    except Exception as ex:
        print(ex)
        LoggingStore().putLogging("ERROR", "listHandlers: " + str(ex),
                                  "", "", "") # TODO context
        return "", 500


# clear all active pollers and event handlers
@app.route("/clearAllPollers", methods=["POST"])
def clearAllPollers():
    try:
        # Get all active events
        events = EventStore().getAllWfEvents(None)
        count = len(events) if events else 0

        # Clear each event handler
        if events:
            for event in events:
                wfProcessor.unsetEventHandler(event.getEventId())

        return json.dumps({"cleared": count}), 200
    except Exception as ex:
        LoggingStore().putLogging("ERROR", f"clearAllPollers: {str(ex)}", "", "", "")
        return "", 500


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
        LoggingStore().putLogging("ERROR", "notate: " + str(ex),
                                  "", "", "") # TODO context
        return "", 400


@app.route("/find", methods=["POST"])
def find():
    try:
        searchDict = json.loads(request.form["searchDict"])
        sheets: List[Metasheet] = MetasheetStore().findMetasheets(searchDict)
        return ObjectSerializer.serialize(sheets), 200
    except Exception as ex:
        LoggingStore().putLogging("ERROR", "find: " + str(ex),
                                  "", "", "") # TODO context
        return "", 400
