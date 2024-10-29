#!/bin/bash
# assume we start in a folder which contains all project source folders 

# TODO do we have the python requirements?

# TODO can the python start the service if its not already running?

# TODO make an lwfm root environment variable from which everything flows 


export PYTHONPATH=$PYTHONPATH:`pwd`/src

# start a service to expose workflow API endpoints 
export FLASK_APP=src/lwfm/midware/impl/LwfmEventSvc
flask run -p 3000 

# start the MetaRepo server
#cd MetaRepo2 && uvicorn src.metarepo:app --port 8000 & 




