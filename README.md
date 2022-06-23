# lwfm


TODO:

+ test
    - MxN
    - code review, documentation, delivery


+ code 0
    - JobStatus clean info fields on emit
    - JobStatus history
    - job extra args - is it dict or a list (see David email)
    - job status site name in persistence log


+ code 1
    - Full trigger model impl - fuzzy, timeouts, wildcards, persistence, etc. - triggers running other than locally
    - Spike: remote site jobs which self-spawn other jobs - how to gather their info? (see Balsam example)
    - Select Python GUI framework incl. graph rendering
    - Multi-Site Job status panel -> similar to DT4D's, but multi-Site
    - Showing trigger futures in workflow / digital thread graph view
    - run repo persistence to real db


+ code 2
    - DT4D run impl
    - DT4D logger - something in Py4 messing it up?  how to log in local batch jobs?
    - Design spike & impl - inc2 app control via DT4D GUI extended to multi-Site
    - Azure impl
    - David suggests: "get MPI into local script" - what does that mean?
    - fire & wait?
    - Local site driver subclass with ssh as run, scp as repo, with auth credentials
    - repo put/get file with a new dest name (is this needed?)
    - admin endpoints, "site: list compute types"
    - SiteFileRef with timestamp (see TODO)
    - JSS security


************************************************************************************************************************************

export FLASK_APP=lwfm/server/JobStatusSentinelSvc
flask run
