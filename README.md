# lwfm


TODO:

+ code / test
    - David: fix nersc driver to new signatures, repo as jobs
    - David: jss emit take full job status object
    - jss.getStatus() should return rich object with history


    - parent-child tracking, originator
    - triggers


+ MxN
    - what part is open source?


+ DT4D
    - DT4D run impl
    - list of sites for factory - how to add new custom ones?
    - logger - something messing it up?
    - some stunt use case...


later:
    - local driver repo put & get as async
    - trigger/handler persistence
    - run repo persistence to real db?
    - fire & wait?
    - nersc tokens are pinned to ip - test in advance for ip
    - use case: remote job spawns other jobs - how can we get them to report in?  polling for what...
    - visualize wf
    - TODO tags
    - wildcard job handlers, full handler impl
    - Local site driver subclass with ssh as run, scp as repo, with auth credentials
    - repo put/get file with a new dest name


************************************************************************************************************************************

export FLASK_APP=lwfm/server/JobStatusSentinelSvc
flask run
