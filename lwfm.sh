#!/bin/bash

export PYTHONPATH=$PYTHONPATH:`pwd`/src

# start a service to expose workflow API endpoints 
export FLASK_APP=src/lwfm/midware/impl/LwfmEventSvc
flask run -p 3000 










