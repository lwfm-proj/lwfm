"""
Different kids of workflow events 
- job reaches a status, i.e. any job on any site reaching a canonical status
- a remote job reaches a status, i.e. the middleware polls for it's completion
- a data triggered event - data with a certain metadata profile is touched
in these cases, a user-provided handler is fired
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring

from enum import Enum
from typing import Optional

from lwfm.midware._impl.IdGenerator import IdGenerator
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobContext import JobContext


# ************************************************************************
class WorkflowEvent:
    """
    Base class for workflow events.
    """
    def __init__(self, fireDefn: JobDefn, fireSite: str, fireJobId: Optional[str] = None,
                 context: Optional[JobContext] = None):
        self._event_id: str = IdGenerator().generateId()
        self._fire_defn = fireDefn
        self._fire_site = fireSite
        self._fire_job_id = fireJobId or IdGenerator().generateId()
        self._context = context
        if context is None:
            context = JobContext()
        self._workflow_id: str = context.getWorkflowId() if context.getWorkflowId() else \
            IdGenerator().generateId()
        self._parent = context.getJobId()

    def getEventId(self) -> str:
        return self._event_id

    def getWorkflowId(self) -> str:
        return self._workflow_id

    def getParentId(self) -> Optional[str]:
        return self._parent

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
        return f"[event defn:{str(self.getFireDefn())} " + \
            f"site:{str(self.getFireSite())} jobId:{str(self.getFireJobId())}]"

    def getKey(self):
        return self.getEventId()


# ***************************************************************************

class JobEvent(WorkflowEvent):
    """
    Jobs emit status, including informational status.  Some status events are terminal -
    "finished", "cancelled" - and some are interim states. Status strings in lwfm are
    normalized by the Site driver from the native Site status name set into the
    lwfm canonical set. We can set event triggers - on canonical status strings, job ids:
        "when job <j1> reaches <state>, execute job <j2> on Site <s>"
    """
    def __init__(self, ruleJobId: str, ruleStatus: str,
                 fireDefn: JobDefn, fireSite: str, fireJobId: Optional[str] = None,
                 context: Optional[JobContext] = None):
        super().__init__(fireDefn, fireSite, fireJobId, context)
        self._rule_job_id: str = ruleJobId
        self._rule_status: str = ruleStatus
        if fireDefn is not None:
            fireDefn.setSiteName(fireSite)

    def setRuleJobId(self, ruleJobId):
        self._rule_job_id = ruleJobId

    def getRuleJobId(self):
        return self._rule_job_id

    def setRuleStatus(self, ruleStatus):
        self._rule_status = ruleStatus

    def getRuleStatus(self):
        return self._rule_status

    def __str__(self):
        return super().__str__() + \
            f"+[rule jobId:{self.getRuleJobId()} status:{self.getRuleStatus()}]"

    def getKey(self):
        return str(str(self.getRuleJobId()) + "." + str(self.getRuleStatus()))

    @staticmethod
    def getJobEventKey(jobId: str, status: Enum) -> str:
        return str(jobId) + "." + str(status)


# ***************************************************************************

class MetadataEvent(WorkflowEvent):
    """
    When a data element is put with a given metadata profile, fire the 
    JobDefn at the site. 
    """
    def __init__(self, queryRegExs: dict, fireDefn: JobDefn, fireSite: str,
                fireJobId: Optional[str] = None,
                context: Optional[JobContext] = None):
        super().__init__(fireDefn, fireSite, fireJobId, context)
        self._query_regexs = queryRegExs

    def getQueryRegExs(self) -> dict:
        return self._query_regexs

    def __str__(self):
        return super().__str__() + f"+[meta dict:{self.getQueryRegExs()}]"


# ***************************************************************************

class NotificationEvent(JobEvent):
    """
    A notification event is fired when a user-defined event occurs, such as
    a job reaching a certain status or a data element being put into the repository.
    """
    def __init__(self, ruleJobId: str, ruleJobStatus: str,
                 to: str, subject: str, body: str, context: Optional[JobContext] = None):
        super().__init__(ruleJobId, ruleJobStatus, JobDefn(), "local", None, context)
        self._to = to
        self._subject = subject
        self._body = body
    
    def getTo(self) -> str:
        return self._to

    def getSubject(self) -> str:
        return self._subject

    def getBody(self) -> str:
        return self._body

    def __str__(self):
        return super().__str__() + \
            f"+[to:{self.getTo()} subject:{self.getSubject()} body:{self.getBody()}]"


# ***************************************************************************
