"""
JobContext holds the identifier for the job, info about the target site, and
tracking info.
"""

#pylint: disable = invalid-name, missing-function-docstring

from typing import Optional
from lwfm.midware._impl.IdGenerator import IdGenerator


class JobContext:
    """
    The runtime execution context of the job.  It contains the id of the job and 
    references to its parent jobs, if any.  A JobStatus can reference a JobContext,
    and then augment it with updated job status information.
    """

    def __init__(self):
        self._job_id = IdGenerator().generateId()
        self._native_id = self._job_id      # important: can be set later
        self._parent_job_id = None
        self._workflow_id = self._job_id    # important: can be set later
        self._name = self._job_id           # important: can be set later
        self._compute_type = "default"
        self._site_name = "local"

    def addParentContext(self, parentContext: "JobContext") -> None:
        if parentContext is not None:
            self._parent_job_id = parentContext.getJobId()
            self._workflow_id = parentContext.getWorkflowId()
            self._site_name = parentContext.getSiteName()
            self._name = (parentContext.getName() or "") + "_" + (self._name or "")

    def setJobId(self, idValue: str) -> None:
        self._job_id = idValue

    def getJobId(self) -> str:
        return self._job_id

    def setNativeId(self, nativeId: str) -> None:
        self._native_id = nativeId

    def getNativeId(self) -> str:
        return self._native_id

    def setParentJobId(self, parentId: str) -> None:
        self._parent_job_id = parentId


    def getParentJobId(self) -> Optional[str]:
        return self._parent_job_id

    def setWorkflowId(self, workflowId: str) -> None:
        self._workflow_id = workflowId

    def getWorkflowId(self) -> str:
        return self._workflow_id

    def setName(self, name: str) -> None:
        self._name = name

    def getName(self) -> str:
        return self._name

    def setComputeType(self, computeType: str) -> None:
        self._compute_type = computeType

    def getComputeType(self) -> str:
        return self._compute_type

    def setSiteName(self, siteName: str) -> None:
        self._site_name = siteName

    def getSiteName(self) -> str:
        return self._site_name


    def __str__(self):
        return f"[job: {self.getJobId()} native:{self.getNativeId()} " + \
            f"parent: {self.getParentJobId()} wf: {self.getWorkflowId()} " + \
            f"site:{self.getSiteName()} " + \
            f"compute:{self.getComputeType()}]"
