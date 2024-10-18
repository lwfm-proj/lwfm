#!/bin/bash
# assume we start in a folder which contains all project source folders 

# TODO do we have the python requirements?

# TODO can the python start the service if its not already running?

# TODO make an lwfm root environment variable from which everything flows 

# TODO turn this into a single component 

# basic status persisence in a file 
# TODO make a better db - use MetaRepo?
if test -e ~/.lwfm/run_job_status_store.txt; then
    echo "Job status log file exists"
else
  echo "The file does not exist - making it"
    mkdir ~/.lwfm
    touch ~/.lwfm/run_job_status_store.txt
fi

export PYTHONPATH=$PYTHONPATH:`pwd`/src

# start a service to expose the Event Handler endpoints 
export FLASK_APP=src/lwfm/server/WorkflowEventSvc
flask run -p 3000 & 

# start the MetaRepo server
#cd MetaRepo2 && uvicorn src.metarepo:app --port 8000 & 

# TODO does this work on Mac?

trap 'pkill flask; pkill tail' SIGINT
trap 'pkill flask; pkill tail' SIGKILL

# tail the crude job status log file
echo "Tailing the status log file..."
tail -f ~/.lwfm/run_job_status_store.txt & 

# tail the crude metarepo 
echo "Tailing the metarepo log file..."
tail -f ~/.lwfm/metarepo_store.txt  




