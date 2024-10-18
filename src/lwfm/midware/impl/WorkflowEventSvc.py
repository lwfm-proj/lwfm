# TODO logging vs. print
# ***********************************************************************************
# Flask app

from flask import Flask, request, jsonify
import pickle
from lwfm.midware.impl.WorkflowEventProcessor import WorkflowEventProcessor
from lwfm.base.JobStatus import JobStatus

from lwfm.store.RunStore import RunJobStatusStore
import logging

app = Flask(__name__)
app.logger.disabled = True
log = logging.getLogger("werkzeug")
log.setLevel(logging.ERROR)
wfep = WorkflowEventProcessor()

# most recent status
_jobStatusCache = {}

# history - to be replaced with a real DB TODO
# _jobStatusHistory = []


@app.route("/")
def index():
    return str(True)


@app.route("/emit", methods=["POST"])
def emitStatus():
    jobId = request.form["jobId"]
    statusBlob = request.form["statusBlob"]
    try:
        statusObj = JobStatus.deserialize(statusBlob)
    except Exception as ex:
        print("exception deserializing statusBlob")
        print(ex)
        return "", 400
    # persist it for posterity
    RunJobStatusStore().write(statusObj)
    # TODO - no...
    # store it locally for convenience
    _jobStatusCache[jobId] = statusObj
    # _jobStatusHistory.append(statusObj)
    # TODO
    try:
        # This will check to see if there is a job trigger and if so run it
        wfep.runJobTrigger(statusObj)
        return "", 200
    except Exception as ex:
        print("exception checking events")
        print(ex)
        return "", 400


@app.route("/status/<jobId>")
def getStatus(jobId: str):
    try:
        stat = _jobStatusCache[jobId]
        try:
            return stat.serialize()
        except Exception as ex:
            print("*** exception from stat.serialize() " + str(ex))
            return ""
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
                statuses.append(status.toJSON())
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
        return wfep.setEventTrigger(obj)
    except Exception as ex:
        print(ex)  # TODO loggging
        return "", 400


# unset a given handler
@app.route("/unset/<handlerId>")
def unsetTrigger(id: str):
    return str(wfep.unsetEventTrigger(id))


# unset all handers
@app.route("/unsetAll")
def unsetAllTriggers():
    wfep.unsetAllEventTriggers()
    return str(True)


# list the ids of all active handers
@app.route("/list")
def listTriggers():
    return str(wfep.listActiveTriggers())


"""
def _getStatusHistory(jobId: str) -> []:
    results = []
    for record in _jobStatusHistory:
        if (record.getJobContext().getId() == jobId):
            results.append(record)
    return results


def _buildThreadJson(jobId: str) -> str:
    # get the status history for the seminal job
    data = {}
    statusList = _getStatusHistory(jobId)
    data[jobId] = statusList
    # find all jobs which list this job as the parent
    children = _getChildren(jobId)
    for child in children:
        statusList = _getStatusHistory(child.getJobContext().getId())
        data[child.getJobContext().getId()] = statusList
        # does this child have children?
        subkids = _getChildren(child.getJobContext().getId())

def _buildWFThread(jobId: str) -> str:
    status = getStatus(jobId)
    # does the job have a parent?
    if (status.getOriginJobId() != status.getJobContext().getId()):
        # this job is not seminal - go up the tree
        threadJson = _buildThreadJson(status.getOriginJobId())
    else:
        threadJson = _buildThreadJson(status.getJobContext().getId())
    return threadJson

# get the digital thread for a given workflow - returns a JSON blob
@app.route('/wfthread/<jobId>')
def getWFThread(jobId: str):
    thread = _buildWFThread(jobId)
    return thread
"""
