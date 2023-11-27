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

# start a service to expose the middleware endpoints 
export FLASK_APP=lwfm/src/lwfm/server/JobStatusSentinelSvc
flask run -p 3000 & 

# wait for the service to start
sleep 15
# run a hello world job to test
python lwfm/src/lwfm/examples/ex0_hello_world.py 

# tail the status log file
tail -f ~/.lwfm/run_job_status_store.txt



