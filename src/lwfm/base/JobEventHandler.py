
from enum import Enum

from lwfm.base.LwfmBase import LwfmBase, _IdGenerator
from lwfm.base.JobStatus import JobStatus, JobContext

from abc import ABC, abstractmethod

# Jobs emit status, incliuding informational status.  Some status events are terminal - "finished", "cancelled".
# We can set event handlers - "when native job <j1> running on Site <s1> reaches <state>, execute JobDefn <j2> on Site <s2>".
# We can also set event handlers which fire on an INFO status message by interrogating the body of the message.
# Note the source

class _JobEventHandlerFields(Enum):
    ID               = "id"                             # handler id
    JOB_ID           = "jobId"
    JOB_SITE_NAME    = "jobSiteName"
    JOB_STATUS       = "jobStatus"
    FIRE_DEFN        = "fireDefn"
    TARGET_SITE_NAME = "targetSiteName"
    TARGET_CONTEXT   = "targetContext"


class JobEventHandler(LwfmBase):
    def __init__(self,
                 jobId: str, jobSiteName: str, jobStatus: str, fireDefn: str, targetSiteName: str, targetContext: JobContext):
        super(JobEventHandler, self).__init__(None)
        self._setId(_IdGenerator.generateId())
        LwfmBase._setArg(self, _JobEventHandlerFields.JOB_ID.value, jobId)
        LwfmBase._setArg(self, _JobEventHandlerFields.JOB_SITE_NAME.value, jobSiteName)
        LwfmBase._setArg(self, _JobEventHandlerFields.JOB_STATUS.value, jobStatus)
        LwfmBase._setArg(self, _JobEventHandlerFields.FIRE_DEFN.value, fireDefn)
        LwfmBase._setArg(self, _JobEventHandlerFields.TARGET_SITE_NAME.value, targetSiteName)
        LwfmBase._setArg(self, _JobEventHandlerFields.TARGET_CONTEXT.value, targetContext)

    def _setId(self, idValue: str) -> None:
        LwfmBase._setArg(self, _JobEventHandlerFields.ID.value, idValue)

    def getId(self) -> str:
        return LwfmBase._getArg(self, _JobHandlerFieldsFields.ID.value)


    def getJobSiteName(self) -> str:
        return LwfmBase._getArg(self, _JobHandlerFieldsFields.JOB_SITE_NAME.value)




    def getHandlerId(self) -> str:
        return self.getKey()

    def getKey(self):
        # We want to permit more than one event handler for the same job, but for now we'll limit it to one handler per
        # canonical job status name.
        return str("" + LwfmBase._getArg(self, _EventHandlerFields.JOB_ID.value) +
                   "." +
                   LwfmBase._getArg(self, _EventHandlerFields.JOB_STATUS.value))
