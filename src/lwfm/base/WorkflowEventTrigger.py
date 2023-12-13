from enum import Enum

from lwfm.base.LwfmBase import LwfmBase, _IdGenerator
from lwfm.base.JobStatus import JobContext


#************************************************************************************

class _WorkflowEventTriggerFields(Enum):
    ID = "id"                       # trigger id


class WorkflowEventTrigger(LwfmBase):
    def __init__(self):
        super(WorkflowEventTrigger, self).__init__(None)

    def _setId(self, idValue: str) -> None:
        LwfmBase._setArg(self, _WorkflowEventTriggerFields.ID.value, idValue)

    def getId(self) -> str:
        return LwfmBase._getArg(self, _WorkflowEventTriggerFields.ID.value)

#************************************************************************************

class _JobEventTriggerFields(Enum):
    # Event handling for a single job - job A reach a state implies the running of job B.  
    # When this job running on the named site reaches the given status, fire the
    # registered job defn on the named target site in the given runtime context.
    JOB_ID = "jobId"                
    JOB_SITE_NAME = "jobSiteName"
    JOB_STATUS = "jobStatus"
    FIRE_DEFN = "fireDefn"
    TARGET_SITE_NAME = "targetSiteName"
    TARGET_CONTEXT = "targetContext"


class JobEventTrigger(WorkflowEventTrigger):
    """
    Jobs emit status, incliuding informational status.  Some status events are terminal - 
    "finished", "cancelled" - and some are interim states.  Status strings in lwfm are 
    normalized by the Site driver from the native Site status name set into the
    lwfm canonical set. We can set event triggers - on canonical status strings -
        "when job <j1> running on Site <s1> reaches <state>, execute job <j2> on Site <s2>"

    We can also set event handlers which fire after interrogating the body of the message.  
    The user must provide the implementation of that filter.  An example use case - a Site 
    emits an INFO status message when data is put using the Site.Repo interface, and
    the body of the message includes the metadata used in the put.  If the metadata meets 
    certain user-supplied filters, the event trigger fires.

    This struct and potential derrived classes provide the means to describe the conditions 
    under which a given job should fire as a result of an upstream event or events, and 
    when it fires, where and how.

    Implementations of the Run subsystem provide a means to accept these WorkflowEventTrigger 
    descriptors and monitor the job status message traffic to determine when to fire them.  
    lwfm provides a reference implementation of this functionality.

    Some example event handler rules:
        - job or set of jobs (to reach some state (or one or a set of states) completely 
            or set in partial within some timeframe)
        - fire once or fire many times, or fire when triggered until some TTL
        - fire on a schedule
        - fire when a job status header and/or body contains certain attributes / metadata

    Attributes of the event trigger:
        - id: each is assigned a unique id
        - site: filters to just status messages from named site or list of sites, may be 
            omitted in which case all sites are considered
        - rule function: takes a JobStatus and returns bool to indicate the rule is 
            satisfied an the registered JobDefn should fire
        - fire defn: the JobDefn to fire if the event handler rule is satisfied
        - target site: the site on which the job defn will fire
        - target context: the JobContext for digital threading to use when the job fires, 
            if one is provided

    To implement this functionality, the rule might need to able to save state.  The Run 
    subsystem by virtue of the status message provides a means for the rule function to 
    post back tracking information.
    """

    def __init__(self, jobId: str, jobStatus: str, fireDefn: str, targetSiteName: str):
        super(JobEventTrigger, self).__init__()
        self._setId(_IdGenerator.generateId())
        LwfmBase._setArg(self, _JobEventTriggerFields.JOB_ID.value, jobId)
        LwfmBase._setArg(self, _JobEventTriggerFields.JOB_SITE_NAME.value, None)
        LwfmBase._setArg(self, _JobEventTriggerFields.JOB_STATUS.value, jobStatus)
        LwfmBase._setArg(self, _JobEventTriggerFields.FIRE_DEFN.value, fireDefn)
        LwfmBase._setArg(
            self, _JobEventTriggerFields.TARGET_SITE_NAME.value, targetSiteName
        )
        LwfmBase._setArg(self, _JobEventTriggerFields.TARGET_CONTEXT.value, None)

    def setJobId(self, idValue: str) -> None:
        LwfmBase._setArg(self, _JobEventTriggerFields.JOB_ID.value, idValue)

    def getJobId(self) -> str:
        return LwfmBase._getArg(self, _JobEventTriggerFields.JOB_ID.value)

    def getStatus(self) -> str:
        return LwfmBase._getArg(self, _JobEventTriggerFields.JOB_STATUS.value)

    def getJobSiteName(self) -> str:
        return LwfmBase._getArg(self, _JobEventTriggerFields.JOB_SITE_NAME.value)

    def setJobSiteName(self, jobSiteName: str) -> None:
        return LwfmBase._setArg(
            self, _JobEventTriggerFields.JOB_SITE_NAME.value, jobSiteName
        )

    def getFireDefn(self) -> str:
        return LwfmBase._getArg(self, _JobEventTriggerFields.FIRE_DEFN.value)

    def setFireDefn(self, fireDefn: str) -> None:
        return LwfmBase._setArg(self, _JobEventTriggerFields.FIRE_DEFN.value, fireDefn)

    def getTargetSiteName(self) -> str:
        return LwfmBase._getArg(self, _JobEventTriggerFields.TARGET_SITE_NAME.value)

    def setTargetSiteName(self, targetSiteName: str) -> None:
        return LwfmBase._setArg(
            self, _JobEventTriggerFields.TARGET_SITE_NAME.value, targetSiteName
        )

    def getTargetContext(self) -> str:
        return LwfmBase._getArg(self, _JobEventTriggerFields.TARGET_CONTEXT.value)

    def setTargetContext(self, targetContext: JobContext) -> None:
        return LwfmBase._setArg(
            self, _JobEventTriggerFields.TARGET_CONTEXT.value, targetContext
        )

    def getHandlerId(self) -> str:
        return self.getKey()

    def getKey(self):
        # TODO relax this limitation
        # We want to permit more than one event trigger for the same job, but for now 
        # we'll limit it to one trigger per canonical job status name.
        return str(
            ""
            + LwfmBase._getArg(self, _JobEventTriggerFields.JOB_ID.value)
            + "."
            + LwfmBase._getArg(self, _JobEventTriggerFields.JOB_STATUS.value)
        )

#************************************************************************************

