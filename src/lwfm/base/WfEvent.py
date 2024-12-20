

# Different kids of workflow events 
# - job reaches a status, i.e. any job on any site reaching a canonical status
# - a remote job reaches a status, i.e. the middleware polls for it's completion
# - a data triggered event - data with a certain metadata profile is touched
# in these cases, a user-provided handler is fired

from enum import Enum

from lwfm.base.LwfmBase import LwfmBase
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobContext import JobContext


# ************************************************************************
class _WfEventFields(Enum):
    FIRE_DEFN = "fireDefn"
    FIRE_SITE = "fireSite"
    FIRE_JOB_ID = "fireJobId"


class WfEvent(LwfmBase):
    """
    fire defn: the JobDefn to fire if the event handler rule is satisfied
    fire site: the site on which the job defn will run
    fire id: the lwfm id to be used on fire 
    """

    def __init__(self, fireDefn : JobDefn, fireSite: str):
        super(WfEvent, self).__init__(None)
        LwfmBase._setArg(self, _WfEventFields.FIRE_DEFN.value, fireDefn)
        LwfmBase._setArg(self, _WfEventFields.FIRE_SITE.value, fireSite)

    def getFireDefn(self) -> JobDefn:
        return LwfmBase._getArg(self, _WfEventFields.FIRE_DEFN.value)
    
    def getFireSite(self) -> str:
        return LwfmBase._getArg(self, _WfEventFields.FIRE_SITE.value)
    
    def setFireJobId(self, fireJobId: str) -> None:
        LwfmBase._setArg(self, _WfEventFields.FIRE_JOB_ID.value, fireJobId) 

    def getFireJobId(self) -> str:
        return LwfmBase._getArg(self, _WfEventFields.FIRE_JOB_ID.value)

    def __str__(self):
        return f"[event defn:{str(self.getFireDefn())} " + \
            f"site:{str(self.getFireSite())} " + \
            f"jobId:{str(self.getFireJobId())}]"

    def getKey(self) -> str:
        return self.getId()

# ***************************************************************************

class _RemoteJobEventFields(Enum):
    NATIVE_JOB_ID   = "nativeJobId"

class RemoteJobEvent(WfEvent):
    def __init__(self, context: JobContext):
        super(RemoteJobEvent, self).__init__(JobDefn(), context.getSiteName())
        LwfmBase._setArg(self, _RemoteJobEventFields.NATIVE_JOB_ID.value, context.getNativeId())
        LwfmBase._setArg(self, _WfEventFields.FIRE_JOB_ID.value, context.getId())

    def getNativeJobId(self) -> str:
        return LwfmBase._getArg(self, _RemoteJobEventFields.NATIVE_JOB_ID.value)

    def __str__(self):
        return super().__str__() + \
            f"+[remote nativeId:{self.getNativeJobId()}]"

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
        "when job <j1> reaches <state>, execute job <j2> on Site <s>"
    """

    def __init__(
        self,
        ruleJobId: str,                   # when this job 
        ruleStatus: str,                 # reaches this status       
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
    
    def getRuleStatus(self) -> str:
        return LwfmBase._getArg(self, _JobEventFields.RULE_STATUS.value)

    def getKey(self) -> str:
        return str("" + self.getRuleJobId() + "." + str(self.getRuleStatus()))

    @staticmethod
    def getJobEventKey(jobId: str, status: Enum) -> str:
        return str(jobId) + "." + str(status)


# ***************************************************************************
class _MetadataEventFields(Enum):
    # Event handling for a single job - job A reach a state implies the running of job B.
    # When this job running on the named site reaches the given status, fire the
    # registered job defn on the named target site in the given runtime context.
    QUERY_REG_EXS = "queryRegExs"


class MetadataEvent(WfEvent):
    def __init__(self, queryRegExs: dict, fireDefn: JobDefn, fireSite: str):
        super(MetadataEvent, self).__init__(fireDefn, fireSite)
        LwfmBase._setArg(self, _MetadataEventFields.QUERY_REG_EXS.value, queryRegExs)  

    def getQueryRegExs(self) -> dict:
        return LwfmBase._getArg(self, _MetadataEventFields.QUERY_REG_EXS.value)
    
    def __str__(self):
        return super().__str__() + \
            f"+[meta dict:{self.getQueryRegExs()}]"
    
# ***************************************************************************
