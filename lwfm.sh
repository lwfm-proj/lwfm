#!/bin/bash
# assume we start in a folder which contains all project source folders 

# TODO do we have the python requirements?
# TODO make an lwfm root environment variable from which everything flows 


export PYTHONPATH=$PYTHONPATH:`pwd`/src:`pwd`/../MetaRepo

# start a service to expose workflow API endpoints 
# & start the metarepo 

# TODO port as an arg
export FLASK_APP=src/lwfm/midware/impl/LwfmEventSvc
flask run -p 3000 

# TODO trap the ctrl-c and kill all the processes









