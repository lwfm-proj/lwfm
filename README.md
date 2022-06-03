# lwfm


TODO:

+ code / test
    - local driver repo as job

    - David: fix nersc driver to new signatures, repo as jobs
    - David: jss emit take full job status object
    - jss.getStatus() should return rich object with history
    - parent-child tracking, originator

    - triggers

    - all status to Run Store - in getJobStatus(), or in JobStatus itself? - can the framework auto-handle the emit?
    - put file with a new dest name


+ MxN
    - what part is open source?


+ DT4D
    - DT4D run impl
    - list of sites for factory - how to add new custom ones?
    - logger - something messing it up?
    - some stunt use case...


later:
    - trigger/handler persistence
    - fire & wait?
    - nersc tokens are pinned to ip - test in advance for ip
    - use case: remote job spawns other jobs - how can we get them to report in?  polling for what...
    - visualize wf
    - TODO tags
    - wildcard job handlers, full handler impl
    - Local site driver subclass with ssh as run, scp as repo, with auth credentials


************************************************************************************************************************************

export FLASK_APP=lwfm/server/JobStatusSentinelSvc
flask run
