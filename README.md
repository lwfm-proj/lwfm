# lwfm


TODO:

+ code / test
    - David: fix nersc driver to new signatures, repo as jobs

    - triggers
    - parent-child tracking


+ MxN
    - what part is open source?
    - what is SFAPI file transfer limit?


+ DT4D
    - DT4D run impl
    - list of sites for factory - how to add new custom ones?
    - logger - something messing it up?
    - some stunt use case...


later:
    - job status site name, history
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
