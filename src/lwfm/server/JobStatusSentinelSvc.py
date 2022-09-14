
#************************************************************************************************************************************
# Flask app

from flask import Flask, request
import pickle
from lwfm.server.JobStatusSentinel import JobStatusSentinel, EventHandler
from lwfm.base.JobStatus import JobStatus, JobContext
from lwfm.store.RunStore import RunJobStatusStore
import logging

app = Flask(__name__)
jss = JobStatusSentinel()

# most recent status
_jobStatusCache = {}

# history - to be replaced with a real DB TODO
_jobStatusHistory = []

@app.route('/')
def index():
  return str(True)

@app.route('/emit', methods=['POST'])
def emitStatus():
    jobId = request.form['jobId']
    jobStatus = request.form['jobStatus']
    statusBlob = request.form['statusBlob']
    statusObj = JobStatus.deserialize(statusBlob)
    # persist it for posterity
    RunJobStatusStore().write(statusObj)
    # store it locally for convenience
    _jobStatusCache[jobId] = statusObj
    _jobStatusHistory.append(statusObj)
    key = EventHandler(jobId, None, jobStatus, None, None, None).getKey()
    jss.runHandler(key, statusObj) # This will check to see if the handler is in the JSS store, and run if so
    return '', 200

@app.route('/status/<jobId>')
def getStatus(jobId : str):
    stat = _jobStatusCache[jobId]
    return stat.serialize()

@app.route('/set', methods = ['POST'])
def setHandler():
    jobId = request.form['jobId']
    jobSiteName = request.form['jobSiteName']
    jobStatus = request.form['jobStatus']
    fireDefn = pickle.loads(request.form['fireDefn'].encode())
    targetSiteName = request.form['targetSiteName']
    targetContextStr = request.form['targetContext']
    if (targetContextStr == ""):
        targetContext = JobContext()
    else:
        targetContext = JobContext.deserialize(targetContextStr)
    targetContext.setParentJobId(jobId)
    # set the origin
    handlerId = jss.setEventHandler(jobId, jobSiteName, jobStatus, fireDefn, targetSiteName, targetContext)
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

def _getStatusHistory(jobId: str) -> []:
    results = []
    for record in _jobStatusHistory:
        if (record.getId() == jobId):
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
        statusList = _getStatusHistory(child.getId())
        data[child.getId()] = statusList
        # does this child have children?
        subkids = _getChildren(child.getId())




def _buildWFThread(jobId: str) -> str:
    status = getStatus(jobId)
    # does the job have a parent?
    if (status.getOriginJobId() != status.getId()):
        # this job is not seminal - go up the tree
        threadJson = _buildThreadJson(status.getOriginJobId())
    else:
        threadJson = _buildThreadJson(status.getId())
    return threadJson

# get the digital thread for a given workflow - returns a JSON blob
@app.route('/wfthread/<jobId>')
def getWFThread(jobId: str):
  thread = _buildWFThread(jobId)
  return thread
