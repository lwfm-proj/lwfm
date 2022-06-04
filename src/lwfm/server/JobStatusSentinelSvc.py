
#************************************************************************************************************************************
# Flask app

from flask import Flask, request
import pickle
from lwfm.server.JobStatusSentinel import JobStatusSentinel, EventHandler
from lwfm.base.JobStatus import JobStatus
from lwfm.store.RunStore import RunJobStatusStore
app = Flask(__name__)
jss = JobStatusSentinel()


# TODO: store this sensibly
_jobStatusCache = {}

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
    key = EventHandler(jobId, None, jobStatus, None, None).getKey()
    jss.runHandler(key) # This will check to see if the handler is in the JSS store, and run if so
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
    targetId = request.form['targetId']
    handlerId = jss.setEventHandler(jobId, jobSiteName, jobStatus, fireDefn, targetSiteName, targetId)
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
