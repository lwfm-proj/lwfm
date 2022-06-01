
#************************************************************************************************************************************
# Flask app

from flask import Flask, request
import pickle
from JobStatusSentinel import JobStatusSentinel, EventHandler

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
