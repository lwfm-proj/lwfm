

from enum import Enum
from abc import ABC
from datetime import datetime, timezone

from lwfm.base.LwfmBase import LwfmBase
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobContext import JobContext


# ************************************************************************
class _WfEventFields(Enum):
    FIRE_DEFN = "fireDefn"
    FIRE_SITE = "fireSite"
    RECURRING = "recurring"


"""
WfEvent: "when this happens, run this defined job in this context"; where a
context includes the target runtime site 

    - TimeEvent: a time-based event - "at this time, run this defined job in
      this context"; a DailyTimeEvent is similar but recurring 
    - JobEvent: a job reaches a status; can also be used with no job id, in which
      case it fires for any job which reaches he status  
    - (JobSetEvent: a set of jobs reaches a collective state - can be on mixed sites, marked as members of the same set)
    - (DataEvent: data is posted which matches a metadata filter)
"""


class WfEvent(LwfmBase, ABC):
    """
        - fire defn: the JobDefn to fire if the event handler rule is satisfied
    - target site: the site on which the job defn will fire
    - target context: the JobContext for digital threading to use when the job fires,
        if one is provided
    """

    def __init__(self, fireDefn : JobDefn, fireSite: str, recurring: bool = False):
        super(WfEvent, self).__init__(None)
        LwfmBase._setArg(self, _WfEventFields.FIRE_DEFN.value, fireDefn)
        LwfmBase._setArg(self, _WfEventFields.FIRE_SITE.value, fireSite)
        LwfmBase._setArg(self, _WfEventFields.RECURRING.value, recurring)

    def getFireDefn(self) -> JobDefn:
        return LwfmBase._getArg(self, _WfEventFields.FIRE_DEFN.value)
    
    def getFireSite(self) -> JobContext:
        return LwfmBase._getArg(self, _WfEventFields.FIRE_SITE.value)
    
    def getRecurring(self) -> bool:
        return LwfmBase._getArg(self, _WfEventFields.RECURRING.value)


    def __str__(self):
        return f"[event defn:{str(self.getFireDefn())} " + \
            f"site:{str(self.getFireSite())} recur:{str(self.getRecurring())}]"
    
    #def setTriggerFilter(self, jobFilter: str) -> None:
    #    # TODO
    #    pass

    #def getTriggerFilter(self) -> str:
    #    # TODO
    #    pass

    def getKey(self) -> str:
        return self.getId()

# ***************************************************************************


class _TimeEventFields(Enum):
    TIMESTAMP = "timestamp"

class TimeEvent(WfEvent):
    def __init__(
        self,
        fireDefn: JobDefn,
        fireSite: str,
        timestamp: datetime
    ):
        super(TimeEvent, self).__init__(fireDefn, fireSite)
        if timestamp.tzinfo is None or timestamp.tzinfo.utcoffset(timestamp) is None:
            raise ValueError("Timestamp must be a timezone-aware datetime object")
        utc_timestamp = timestamp.astimezone(timezone.utc)
        LwfmBase._setArg(self, _TimeEventFields.TIMESTAMP.value, utc_timestamp)

    def getTimestamp(self) -> datetime:
        return LwfmBase._getArg(self, _TimeEventFields.TIMESTAMP.value)

class DailyTimeEvent(TimeEvent):
    def __init__(
        self,
        fireDefn: JobDefn,
        fireSite: str,
        timestamp: datetime
    ):
        super(DailyTimeEvent, self).__init__(fireDefn, fireSite, timestamp)
        LwfmBase._setArg(self, _WfEventFields.RECURRING.value, True)


# ***************************************************************************
class _JobEventFields(Enum):
    # Event handling for a single job - job A reach a state implies the running of job B.
    # When this job running on the named site reaches the given status, fire the
    # registered job defn on the named target site in the given runtime context.
    RULE_JOB_ID = "ruleJobId"
    RULE_STATUS = "jobStatus"


class JobEvent(WfEvent): 
    """
    Jobs emit status, including informational status.  Some status events are terminal -
    "finished", "cancelled" - and some are interim states.  Status strings in lwfm are
    normalized by the Site driver from the native Site status name set into the
    lwfm canonical set. We can set event triggers - on canonical status strings -
        "when job <j1> running on Site <s1> reaches <state>, execute job <j2> on Site <s2>"

    We can also set event handlers which fire after interrogating the body of the message.
    The user must provide the implementation of that filter.  An example use case - a Site
    emits an INFO status message when data is put using the Site.Repo interface, and
    the body of the message includes the metadata used in the put.  If the metadata meets
    certain user-supplied filters, the event trigger fires.

    This struct and potential derived classes provide the means to describe the conditions
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


    TODO (To implement this functionality, the rule might need to able to save state.  The Run
    subsystem by virtue of the status message provides a means for the rule function to
    post back tracking information.)
    """

    def __init__(
        self,
        ruleJobId: JobContext,            # when this job 
        ruleStatus: Enum,                 # reaches this status       
        fireDefn: JobDefn,                # fire this job defn
        fireSite: str                     # on this site
    ):
        super(JobEvent, self).__init__(fireDefn, fireSite)
        LwfmBase._setArg(self, _JobEventFields.RULE_JOB_ID.value, ruleJobId)
        LwfmBase._setArg(self, _JobEventFields.RULE_STATUS.value, ruleStatus)  

    def __str__(self) -> str:
        return super().__str__() + \
            f"+[jobEvent jobId:{self.getRuleJobId()} status:{self.getRuleStatus()}]"

    def getRuleJobId(self) -> str:
        return LwfmBase._getArg(self, _JobEventFields.RULE_JOB_ID.value)
    
    def getRuleStatus(self) -> Enum:
        return LwfmBase._getArg(self, _JobEventFields.RULE_STATUS.value)

    def getKey(self) -> str:
        # TODO relax this limitation
        # We want to permit more than one event trigger for the same job, but for now
        # we'll limit it to one trigger per canonical job status name.
        return str("" + self.getRuleJobId() + "." + str(self.getRuleStatus()))


# ***********************************************************************


# Event handling for a set of jobs - a set of jobs reach a state implies the running
# of a job.  When all of the jobs in the set reach the given status, fire the
# registered job defn on the named target site in the given runtime context.
# JOB_SET_ID = "jobSetId"
# JOB_TRIGGER_FILTER = "jobTriggerFilter"


# class JobSetEvent(WfEvent):
#     def __init__(
#         self, jobSetId: str, triggerFilter: str, fireDefn: str, targetSiteName: str
#     ):
#         super(JobSetEvent, self).__init__(fireDefn, targetSiteName)
#         LwfmBase._setArg(self, _JobSetEventFields.JOB_SET_ID.value, jobSetId)
#         LwfmBase._setArg(
#             self, _JobSetEventFields.JOB_TRIGGER_FILTER.value, triggerFilter
#         )

#     def setJobSetId(self, idValue: str) -> None:
#         LwfmBase._setArg(self, _JobSetEventFields.JOB_SET_ID.value, idValue)

#     def getJobSetId(self) -> str:
#         return LwfmBase._getArg(self, _JobSetEventFields.JOB_SET_ID.value)

#     def setTriggerFilter(self, triggerFilter: Callable) -> None:
#         triggerFilterString = inspect.getsource(triggerFilter)
#         LwfmBase._setArg(
#             self,
#             _JobSetEventFields.JOB_TRIGGER_FILTER.value,
#             triggerFilterString,
#         )

#     def getTriggerFilter(self) -> str:
#         return LwfmBase._getArg(self, _JobSetEventFields.JOB_TRIGGER_FILTER.value)

#     def runTriggerFilter(self) -> bool:
#         # TODO what does this do?
#         triggerFilterString = self.getTriggerFilter()
#         return exec(triggerFilterString)


# ************************************************************************************


""" class _DataEventFields(Enum):
    # Event handling for a data event - data with a given metadata profile
    # is put on site X implies the running of a job on site Y.
    DATA_TRIGGER_FILTER = "dataTriggerFilter"


class DataEvent(WfEvent):
    Data events are triggered when a job running on a site emits an INFO status
    message containing metadata that matches the user-supplied filter. The
    filter is a function that takes the metadata as input and returns a boolean
    indicating whether the event should fire. 


    def __init__(self, triggerFilter: Callable, fireDefn: str, targetSiteName: str):
        super(DataEvent, self).__init__(fireDefn, targetSiteName)
        self.setTriggerFilter(triggerFilter)

    def setTriggerFilter(self, triggerFilter: Callable) -> None:
        triggerFilterString = inspect.getsource(triggerFilter)
        LwfmBase._setArg(
            self, _DataEventFields.DATA_TRIGGER_FILTER.value, triggerFilterString
        )

    def getTriggerFilter(self) -> str:
        return LwfmBase._getArg(self, _DataEventFields.DATA_TRIGGER_FILTER.value)

    def runTriggerFilter(self, metadata: dict) -> bool:
        if metadata is None:
            metadata = {}
        triggerFilterString = (
            self.getTriggerFilter()
            + "\n_trigger = _triggerFilter("
            + str(metadata)
            + ")"
        )
        # TODO: revisit
        try:
            globals = {}
            exec(triggerFilterString, globals)
            return globals["_trigger"]
        except Exception as ex:
            return True

    def getKey(self) -> str:
        return str("" + self.getId() + "." + "INFO.dt")
 """
