# lwfm


TODO:

- triggers: listener thread & backing persistence; local endpoint to post status; local driver emit Repo job status to endpoint; fix
  Nersc driver


+ code / test
    - local driver cancel job
    - local driver repo as job
    - JobDefn should not have the ids - use a context
    - nersc driver with jobcontext
    - all status to Run Store - in getJobStatus(), or in JobStatus itself? - can the framework auto-handle the emit?
    - parent-child tracking, originator
    - data movement as jobs
    - put file with a new dest name
    - fire & wait?
    - handler persistence


+ MxN
    - what part is open source?


+ DT4D
    - DT4D run impl
    - list of sites for factory - how to add new custom ones?
    - logger - something messing it up?
    - some stunt use case...


later:
    - nersc tokens are pinned to ip - test in advance for ip
    - use case: remote job spawns other jobs - how can we get them to report in?  polling for what...
    - visualize wf
    - TODO tags
    - wildcard job handlers, full handler impl
    - Local site driver subclass with ssh as run, scp as repo, with auth credentials


************************************************************************************************************************************

export FLASK_APP=lwfm/server/JobStatusSentinelSvc
flask run
