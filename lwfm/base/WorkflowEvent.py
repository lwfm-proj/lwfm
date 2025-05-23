"""
Different kids of workflow events 
- job reaches a status, i.e. any job on any site reaching a canonical status
- a remote job reaches a status, i.e. the middleware polls for it's completion
- a data triggered event - data with a certain metadata profile is touched
in these cases, a user-provided handler is fired
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring

from enum import Enum

from lwfm.util.IdGenerator import IdGenerator
from lwfm.base.JobDefn import JobDefn


# ************************************************************************
class WorkflowEvent:
    def __init__(self, fireDefn=None, fireSite=None, fireJobId=None):
        self._event_id = IdGenerator.generateId()
        self._fire_defn = fireDefn
        self._fire_site = fireSite
        self._fire_job_id = fireJobId

    def getEventId(self):
        return self._event_id

    def setFireDefn(self, fireDefn):
        self._fire_defn = fireDefn

    def getFireDefn(self):
        return self._fire_defn

    def setFireSite(self, fireSite):
        self._fire_site = fireSite

    def getFireSite(self):
        return self._fire_site

    def setFireJobId(self, fireJobId):
        self._fire_job_id = fireJobId

    def getFireJobId(self):
        return self._fire_job_id

    def __str__(self):
        return f"[event defn:{str(self.getFireDefn())} site:{str(self.getFireSite())} jobId:{str(self.getFireJobId())}]"

    def getKey(self):
        return self.getEventId()

# ***************************************************************************

class RemoteJobEvent(WorkflowEvent):
    def __init__(self, context):
        super().__init__(JobDefn(), context.getSiteName(), context.getJobId())
        self._native_job_id = context.getNativeId()

    def getNativeJobId(self):
        return self._native_job_id

    def __str__(self):
        return super().__str__() + f"+[remote nativeId:{self.getNativeJobId()}]"

# ***************************************************************************

class JobEvent(WorkflowEvent):
    """
    Jobs emit status, including informational status.  Some status events are terminal -
    "finished", "cancelled" - and some are interim states. Status strings in lwfm are
    normalized by the Site driver from the native Site status name set into the
    lwfm canonical set. We can set event triggers - on canonical status strings -
        "when job <j1> reaches <state>, execute job <j2> on Site <s>"
    """
    def __init__(self, ruleJobId=None, ruleStatus=None, fireDefn=None, fireSite=None,
        fireJobId=None):
        super().__init__(fireDefn, fireSite, fireJobId)
        self._rule_job_id = ruleJobId
        self._rule_status = ruleStatus

    def setRuleJobId(self, ruleJobId):
        self._rule_job_id = ruleJobId

    def getRuleJobId(self):
        return self._rule_job_id

    def setRuleStatus(self, ruleStatus):
        self._rule_status = ruleStatus

    def getRuleStatus(self):
        return self._rule_status

    def __str__(self):
        return super().__str__() + f"+[rule jobId:{self.getRuleJobId()} status:{self.getRuleStatus()}]"

    def getKey(self):
        return str("" + self.getRuleJobId() + "." + str(self.getRuleStatus()))

    @staticmethod
    def getJobEventKey(jobId: str, status: Enum) -> str:
        return str(jobId) + "." + str(status)


# ***************************************************************************
class MetadataEvent(WorkflowEvent):
    def __init__(self, queryRegExs: dict, fireDefn: 'JobDefn', fireSite: str):
        super().__init__(fireDefn, fireSite)
        self._query_regexs = queryRegExs

    def getQueryRegExs(self) -> dict:
        return self._query_regexs

    def __str__(self):
        return super().__str__() + f"+[meta dict:{self.getQueryRegExs()}]"

# ***************************************************************************
