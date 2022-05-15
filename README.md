# lwfm


TODO:

- triggers: listener thread & backing persistence


- repo actions as jobs...


+ test
    - parent-child tracking
    - RunStore - was it used?
    - MxN: use case - rollup report Genesis on Perlmutter & HAL (how impl HAL?)


+ DT4D
    - DT4D run impl
    - list of sites for factory - how to add new custom ones?
    - logger - something messing it up?
    - some stunt use case...


later:
    - Local driver with threaded run
    - nersc tokens are pinned to ip - test in advance for ip
    - use case: remote job spawns other jobs - how can we get them to report in?  polling for what...
    - visualize wf
    - TODO tags
    - wildcard job handlers


************************************************************************************************************************************

export FLASK_APP=lwfm/server/JobStatusSentinel
flask run
