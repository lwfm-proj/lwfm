
from enum import Enum

from lwfm.base.LwfmBase import LwfmBase, _IdGenerator
from lwfm.base.JobStatus import JobStatus, JobContext

from abc import ABC, abstractmethod


class _JobEventHandlerFields(Enum):
    ID               = "id"                             # handler id
    JOB_ID           = "jobId"
    JOB_SITE_NAME    = "jobSiteName"
    JOB_STATUS       = "jobStatus"
    FIRE_DEFN        = "fireDefn"
    TARGET_SITE_NAME = "targetSiteName"
    TARGET_CONTEXT   = "targetContext"


class JobEventHandler(LwfmBase):
    """
    Jobs emit status, incliuding informational status.  Some status events are terminal - "finished", "cancelled" - and some
    are interim states.  Status strings in lwfm are normalized by the Site driver from the native Site status name set into the
    lwfm canonical set.
    We can set event handlers - on canonical status strings -
        "when job <j1> running on Site <s1> reaches <state>, execute job <j2> on Site <s2>"
    We can also set event handlers which fire after interrogating the body of the message.  The user must provide the implementation
    of that filter.  An example use case - a Site emits an INFO status message when data is put using the Site.Repo interface, and
    the body of the message includes the metadata used in the put.  If the metadata meets certain user-supplied filters, the
    event handler fires.

    This struct and potential derrived classes provide the means to describe the conditions under which a given job should fire
    as a result of an upstream event or events, and when it fires, where and how.

    Implementations of the Run.Registrar component provide a means to accept these JobEventHandler descriptors and monitor the
    job status message traffic to determine when to fire them.


    Some example event handler rules:
        - job or set of jobs (to reach some state (or one or a set of states) completely or set in partial within some timeframe)
        - fire once or fire many times, or fire when triggered until some TTL
        - fire on a schedule
        - fire when a job status header and/or body contains certain attributes / metadata

    Attributes of the event handler:
        - id: each is assigned a unique id
        - site: filters to just status messages from named site or list of sites, may be omitted in which case all sites are considered
        - rule function: takes a JobStatus and returns bool to indicate the rule is satisfied an the registered JobDefn should fire
        - fire defn: the JobDefn to fire if the event handler rule is satisfied
        - target site: the site on which the job defn will fire
        - target context: the JobContext for digital threading to use when the job fires, if one is provided

    To implement this functionality, the rule must be able to save state.  Registrar provides a means for the rule function
    to post back tracking information.

    """

    def __init__(self,
                 jobId: str, jobSiteName: str, jobStatus: str, fireDefn: str, targetSiteName: str,  targetContext: JobContext):
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
        return LwfmBase._getArg(self, _JobEventHandlerFields.ID.value)

    def getJobSiteName(self) -> str:
        return LwfmBase._getArg(self, _JobEventHandlerFields.JOB_SITE_NAME.value)

    def getFireDefn(self) -> str:
        return LwfmBase._getArg(self, _JobEventHandlerFields.FIRE_DEFN.value)

    def getTargetSiteName(self) -> str:
        return LwfmBase._getArg(self, _JobEventHandlerFields.TARGET_SITE_NAME.value)

    def getTargetContext(self) -> str:
        return LwfmBase._getArg(self, _JobEventHandlerFields.TARGET_CONTEXT.value)

    def getHandlerId(self) -> str:
        return self.getKey()

    def getKey(self):
        # We want to permit more than one event handler for the same job, but for now we'll limit it to one handler per
        # canonical job status name.
        return str("" + LwfmBase._getArg(self, _JobEventHandlerFields.JOB_ID.value) +
                   "." +
                   LwfmBase._getArg(self, _JobEventHandlerFields.JOB_STATUS.value))
