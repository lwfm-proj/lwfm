# lwfm


TODO:

- triggers: listener thread & backing persistence; local endpoint to post status; local driver emit Repo job status to endpoint; fix
  Nersc driver


+ test
    - all status to Run Store - in getJobStatus(), or in JobStatus itself?
    - data movement as jobs
    - parent-child tracking, originator
    - put file with a new dest name
    - fire & wait?
    - local driver cancel job
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
