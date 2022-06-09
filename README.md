# lwfm


TODO:

+ test
    - MxN
    - code review, documentation, delivery


+ code
    - DT4D run impl
    - list of sites for factory - how to add new custom ones?
    - logger - something messing it up?  how to log in local batch jobs?
    - JobStatus clean info fields on emit


later:
    - JobStatus history
    - triggers running other than locally?
    - David suggests: get MPI into local script
    - job extra args - is it dict or a list (see David email)
    - job status site name in persistence log
    - trigger/handler persistence
    - "full" trigger model, wildcards
    - run repo persistence to real db?
    - fire & wait?
    - nersc tokens are pinned to ip - test in advance for ip
    - use case: remote job spawns other jobs - how can we get them to report in?  polling for what...
    - visualize wf
    - TODO tags
    - Local site driver subclass with ssh as run, scp as repo, with auth credentials
    - repo put/get file with a new dest name (is this needed?)
    - admin endpoints, "site: list compute types"


************************************************************************************************************************************

export FLASK_APP=lwfm/server/JobStatusSentinelSvc
flask run
