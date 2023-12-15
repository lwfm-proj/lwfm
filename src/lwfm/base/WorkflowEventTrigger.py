from enum import Enum
import inspect
from typing import Callable

from lwfm.base.LwfmBase import LwfmBase
from lwfm.base.JobContext import JobContext


# ************************************************************************************


class _WorkflowEventTriggerFields(Enum):
    FIRE_DEFN = "fireDefn"
    TARGET_SITE_NAME = "targetSiteName"
    TARGET_CONTEXT = "targetContext"


class WorkflowEventTrigger(LwfmBase):
    """
        - fire defn: the JobDefn to fire if the event handler rule is satisfied
    - target site: the site on which the job defn will fire
    - target context: the JobContext for digital threading to use when the job fires,
        if one is provided
    """

    def __init__(self, fireDefn, targetSiteName):
        super(WorkflowEventTrigger, self).__init__(None)
        LwfmBase._setArg(self, _WorkflowEventTriggerFields.FIRE_DEFN.value, fireDefn)
        LwfmBase._setArg(
            self, _WorkflowEventTriggerFields.TARGET_SITE_NAME.value, targetSiteName
        )
        LwfmBase._setArg(self, _WorkflowEventTriggerFields.TARGET_CONTEXT.value, None)

    def getFireDefn(self) -> str:
        return LwfmBase._getArg(self, _WorkflowEventTriggerFields.FIRE_DEFN.value)

    def setFireDefn(self, fireDefn: str) -> None:
        return LwfmBase._setArg(
            self, _WorkflowEventTriggerFields.FIRE_DEFN.value, fireDefn
        )

    def getTargetSiteName(self) -> str:
        return LwfmBase._getArg(
            self, _WorkflowEventTriggerFields.TARGET_SITE_NAME.value
        )

    def setTargetSiteName(self, targetSiteName: str) -> None:
        return LwfmBase._setArg(
            self, _WorkflowEventTriggerFields.TARGET_SITE_NAME.value, targetSiteName
        )

    def getTargetContext(self) -> str:
        return LwfmBase._getArg(self, _WorkflowEventTriggerFields.TARGET_CONTEXT.value)

    def setTargetContext(self, targetContext: JobContext) -> None:
        return LwfmBase._setArg(
            self, _WorkflowEventTriggerFields.TARGET_CONTEXT.value, targetContext
        )

    def setTriggerFilter(self, jobFilter: str) -> None:
        pass

    def getTriggerFilter(self) -> str:
        pass

    def getKey(self) -> str:
        return self.getId()


# ************************************************************************************


class _JobEventTriggerFields(Enum):
    # Event handling for a single job - job A reach a state implies the running of job B.
    # When this job running on the named site reaches the given status, fire the
    # registered job defn on the named target site in the given runtime context.
    JOB_ID = "jobId"
    JOB_SITE_NAME = "jobSiteName"
    JOB_STATUS = "jobStatus"


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


    To implement this functionality, the rule might need to able to save state.  The Run
    subsystem by virtue of the status message provides a means for the rule function to
    post back tracking information.
    """

    def __init__(
        self,
        jobId: str,
        jobStatus: str,
        fireDefn: str = None,
        targetSiteName: str = None,
    ):
        super(JobEventTrigger, self).__init__(fireDefn, targetSiteName)
        LwfmBase._setArg(self, _JobEventTriggerFields.JOB_ID.value, jobId)
        LwfmBase._setArg(self, _JobEventTriggerFields.JOB_SITE_NAME.value, None)
        LwfmBase._setArg(self, _JobEventTriggerFields.JOB_STATUS.value, jobStatus)

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

    def getKey(self) -> str:
        # TODO relax this limitation
        # We want to permit more than one event trigger for the same job, but for now
        # we'll limit it to one trigger per canonical job status name.
        return str("" + self.getJobId() + "." + str(self.getStatus()))


# ************************************************************************************


class _JobSetEventTriggerFields(Enum):
    # Event handling for a set of jobs - a set of jobs reach a state implies the running
    # of a job.  When all of the jobs in the set reach the given status, fire the
    # registered job defn on the named target site in the given runtime context.
    JOB_SET_ID = "jobSetId"
    JOB_TRIGGER_FILTER = "jobTriggerFilter"


class JobSetEventTrigger(WorkflowEventTrigger):
    def __init__(
        self, jobSetId: str, triggerFilter: str, fireDefn: str, targetSiteName: str
    ):
        super(JobSetEventTrigger, self).__init__(fireDefn, targetSiteName)
        LwfmBase._setArg(self, _JobSetEventTriggerFields.JOB_SET_ID.value, jobSetId)
        LwfmBase._setArg(
            self, _JobSetEventTriggerFields.JOB_TRIGGER_FILTER.value, triggerFilter
        )

    def setJobSetId(self, idValue: str) -> None:
        LwfmBase._setArg(self, _JobSetEventTriggerFields.JOB_SET_ID.value, idValue)

    def getJobSetId(self) -> str:
        return LwfmBase._getArg(self, _JobSetEventTriggerFields.JOB_SET_ID.value)

    def setTriggerFilter(self, triggerFilter: Callable) -> None:
        triggerFilterString = inspect.getsource(triggerFilter)
        LwfmBase._setArg(
            self,
            _JobSetEventTriggerFields.JOB_TRIGGER_FILTER.value,
            triggerFilterString,
        )

    def getTriggerFilter(self) -> str:
        return LwfmBase._getArg(
            self, _JobSetEventTriggerFields.JOB_TRIGGER_FILTER.value
        )

    def runTriggerFilter(self) -> bool:
        triggerFilterString = self.getTriggerFilter()
        return exec(triggerFilterString)


# ************************************************************************************


class _DataEventTriggerFields(Enum):
    # Event handling for a data event - data with a given metadata profile
    # is put on site X implies the running of a job on site Y.
    DATA_TRIGGER_FILTER = "dataTriggerFilter"


class DataEventTrigger(WorkflowEventTrigger):
    """
    Data events are triggered when a job running on a site emits an INFO status message
    containing metadata that matches the user-supplied filter.  The filter is a function
    that takes the metadata as input and returns a boolean indicating whether the event
    should fire.  
    """
    def __init__(self, triggerFilter: Callable, fireDefn: str, targetSiteName: str):
        super(DataEventTrigger, self).__init__(fireDefn, targetSiteName)
        self.setTriggerFilter(triggerFilter)

    def setTriggerFilter(self, triggerFilter: Callable) -> None:
        triggerFilterString = inspect.getsource(triggerFilter)
        LwfmBase._setArg(
            self, _DataEventTriggerFields.DATA_TRIGGER_FILTER.value, triggerFilterString
        )

    def getTriggerFilter(self) -> str:
        return LwfmBase._getArg(self, _DataEventTriggerFields.DATA_TRIGGER_FILTER.value)

    def runTriggerFilter(self, metadata: dict) -> bool:
        if metadata is None:
            metadata = {}
        triggerFilterString = (
            self.getTriggerFilter()
            + "\n_trigger = _triggerFilter("
            + str(metadata)
            + ")"
        )
        globals = {}
        exec(triggerFilterString, globals)
        return globals["_trigger"]

    def getKey(self) -> str:
        return str("" + self.getId() + "." + "INFO.dt")


# ************************************************************************************
