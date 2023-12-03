# TODO logging vs. print 
#************************************************************************************************************************************
# Flask app

import stat
from flask import Flask, request, jsonify
import pickle
from lwfm.server.JobStatusSentinel import JobStatusSentinel
from lwfm.base.JobEventHandler import JobEventHandler
from lwfm.base.JobStatus import JobStatus, JobContext
from lwfm.store.RunStore import RunJobStatusStore
import logging

app = Flask(__name__)
app.logger.disabled = True
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)
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
    try:
        statusObj = JobStatus.deserialize(statusBlob)
    except Exception as ex:
        print("exception deserializing statusBlob")
        print(ex)
        return '', 400
    # persist it for posterity
    RunJobStatusStore().write(statusObj)
    # store it locally for convenience
    _jobStatusCache[jobId] = statusObj
    _jobStatusHistory.append(statusObj)
    # TODO 
    key = JobEventHandler(jobId, jobStatus, None, None).getKey()
    jss.runHandler(key, statusObj) # This will check to see if the handler is in the JSS store, and run if so
    return '', 200

@app.route('/status/<jobId>')
def getStatus(jobId : str):
    try:
        stat = _jobStatusCache[jobId]
        try:
            return stat.serialize()
        except Exception as ex:
            print("*** exception from stat.serialize() " + str(ex))
            return ""
    except:
        return ""

@app.route('/site/jobId/<jobId>')
def getSiteByJobId(jobId : str):
    try:
        print("getSiteByJobId() jobId = " + jobId)
        status = _jobStatusCache[jobId]
        siteName = status.getJobContext().getSiteName()
        return siteName
    except Exception as ex:
        print("*** exception from getSiteByJobId() " + str(ex))
        return ""

@app.route('/all/statuses')
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


@app.route('/set', methods = ['POST'])
def setHandler():
    jobId = request.form['jobId']
    jobStatus = request.form['jobStatus']
    targetSiteName = request.form['targetSiteName']
    try:
        fireDefn = pickle.loads(request.form['fireDefn'].encode())
    except Exception as ex:
        print(ex)   # TODO loggging 
        fireDefn = ""
    return jss.setEventHandler(jobId, jobStatus, fireDefn, targetSiteName)


@app.route('/setTerminal', methods = ['POST'])
def setTerminal():
    try:
      jobId = request.form['jobId']
      parentId = request.form['parentId']
      originId = request.form['originId']
      nativeId = request.form['nativeId']
      siteName = request.form['siteName']
      targetContext = JobContext()
    except Exception as ex:
      print(str(ex))
    targetContext.setId(jobId)
    targetContext.setParentJobId(parentId)
    targetContext.setOriginJobId(originId)
    targetContext.setNativeId(nativeId)
    jss.setEventHandler(nativeId, siteName, "<<TERMINAL>>", "", "", targetContext)
    return ""


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
