# lwfm


TODO:

- code comments

- triggers: listener thread & backing persistence


- repo actions as jobs...

- test: parent-child tracking
- test: RunStore - was it used?

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
    - wildcard job handlers


************************************************************************************************************************************

export FLASK_APP=lwfm/server/JobStatusSentinel
flask run
