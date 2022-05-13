# lwfm


TODO:

- triggers

- repo actions as jobs...

- JobStatusSentinel with backing persistence

- nersc run impl

- nersc tokens are pinned to ip - test in advance for ip

- logger - something messing it up?

- Site factory

- local native run

- use case: remote job spawns other jobs - how can we get them to report in?  polling for what...
- visualize wf
- module and signature comments
- TODO tags
- wildcard job handlers




************************************************************************************************************************************

export FLASK_APP=lwfm/server/JobStatusSentinel
flask run
