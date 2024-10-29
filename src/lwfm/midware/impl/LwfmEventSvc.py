# TODO logging vs. print
# ***********************************************************************************
# Flask app

from flask import Flask, request, jsonify
import pickle
from lwfm.midware.impl.LwfmEventProcessor import LwfmEventProcessor
from lwfm.base.JobStatus import JobStatus
from lwfm.midware.impl.Store import JobStatusStore
import logging

app = Flask(__name__)
app.logger.disabled = True
log = logging.getLogger("werkzeug")
log.setLevel(logging.ERROR)
wfProcessor = LwfmEventProcessor()

_statusStore = JobStatusStore()

print("*** service starting")

# most recent status
_jobStatusCache = {}

# history - to be replaced with a real DB TODO
# _jobStatusHistory = []


@app.route("/")
def index():
    return str(True)


@app.route("/emitStatus", methods=["POST"])
def emitStatus():
    try:
        statusBlob = request.form["statusBlob"]
        statusObj = JobStatus.deserialize(statusBlob)
        _statusStore.putJobStatus(statusObj)
    except Exception as ex:
        print("exception persisting status")
        print(ex)
        return "", 400
    # TODO
    try:
        # This will check to see if there is a job trigger and if so run it
        wfProcessor.testJobStatus(statusObj)
        return "", 200
    except Exception as ex:
        print("exception checking events")
        print(ex)
        return "", 400


@app.route("/status/<jobId>")
def getStatus(jobId: str):
    try:
        return wfProcessor.getStatusBlob(jobId)
    except Exception:
        return ""


@app.route("/site/jobId/<jobId>")
def getSiteByJobId(jobId: str):
    try:
        print("getSiteByJobId() jobId = " + jobId)
        status = _jobStatusCache[jobId]
        siteName = status.getJobContext().getSiteName()
        return siteName
    except Exception as ex:
        print("*** exception from getSiteByJobId() " + str(ex))
        return ""


@app.route("/all/statuses")
def getAllStatuses():
    print("Starting get statuses")
    try:
        statuses = []
        for jobId in _jobStatusCache:
            try:
                status = _jobStatusCache[jobId]
                statuses.append(status.serialize())
            except Exception as ex:
                print("*** exception from stat.serialize() " + str(ex))
                return ""
    except Exception as e:
        print("exception: " + str(e))
        return ""
    return jsonify(statuses)


@app.route("/setWorkflowEvent", methods=["POST"])
def setTrigger():
    try:
        obj = pickle.loads(request.form["triggerObj"].encode())
        return wfProcessor.setEventTrigger(obj)
    except Exception as ex:
        print(ex)  # TODO loggging
        return "", 400


# unset a given handler
@app.route("/unset/<handlerId>")
def unsetTrigger(id: str):
    return str(wfProcessor.unsetEventTrigger(id))


# unset all handlers
@app.route("/unsetAll")
def unsetAllTriggers():
    wfProcessor.unsetAllEventTriggers()
    return str(True)


# list the ids of all active handlers
@app.route("/lisEvents")
def listTriggers():
    return wfProcessor.listActiveTriggers()





