#!/usr/bin/bash
# assume we start in a folder which contains all project source folders 

# sites are defined in  ~/.lwfm/sites.txt

# basic status persisence in a file
if test -e ~/.lwfm/run_job_status_store.txt; then
    echo "Job status log file exists"
else
  echo "The file does not exist - making it"
    mkdir ~/.lwfm
    touch ~/.lwfm/run_job_status_store.txt
fi

export PYTHONPATH=lwfm/src

# start a service to expose the Event Handler endpoints 
export FLASK_APP=lwfm/src/lwfm/server/JobStatusSentinelSvc
flask run -p 3000 & 

# start the MetaRepo server
cd MetaRepo2 && uvicorn src.metarepo:app --port 8000 & 

trap 'pkill flask' SIGINT
trap 'pkill flask' SIGKILL

# tail the status log file
echo "Tailing the status log file..."
tail -f ~/.lwfm/run_job_status_store.txt 
