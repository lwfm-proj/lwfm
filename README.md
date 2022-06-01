# lwfm


TODO:

- triggers: listener thread & backing persistence; local endpoint to post status; local driver emit Repo job status to endpoint; fix
  Nersc driver


+ test
    - parent-child tracking, originator
    - RunStore - was it used?
    - data movement as jobs
    - put file with a new dest name
    - fire & wait?


+ MxN
    - what part is open source?


+ DT4D
    - DT4D run impl
    - list of sites for factory - how to add new custom ones?
    - logger - something messing it up?
    - some stunt use case...


later:
    - Local driver with threaded run, cancel emit cancel status
    - nersc tokens are pinned to ip - test in advance for ip
    - use case: remote job spawns other jobs - how can we get them to report in?  polling for what...
    - visualize wf
    - TODO tags
    - wildcard job handlers
    - Local site driver subclass with ssh as run, scp as repo, with auth credentials


************************************************************************************************************************************

export FLASK_APP=lwfm/server/JobStatusSentinelSvc
flask run
