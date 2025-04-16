"""
JobContext holds the identifier for the job, info about the target site, and
tracking info.
"""

#pylint: disable = invalid-name, missing-function-docstring

from ..util.IdGenerator import IdGenerator


class JobContext:
    """
    The runtime execution context of the job.  It contains the id of the job and 
    references to its parent jobs, if any.  A JobStatus can reference a JobContext,
    and then augment it with updated job status information.
    """

    def __init__(self, parentContext: "JobContext" = None):
        self._id = None
        self._native_id = None
        self._job_id = None
        self._parent_job_id = None
        self._origin_job_id = None
        self._name = None
        self._compute_type = "default"
        self._site_name = "local"
        self._set_id = None

        if parentContext is not None:
            self._parent_job_id = parentContext.getJobId()
            self._origin_job_id = parentContext.getOriginJobId()
            self._site_name = parentContext.getSiteName()
            self._name = (parentContext.getName() or "") + "_" + (self._name or "")
        else:
            self._id = IdGenerator.generateId()
            self._native_id = self._id
            self._origin_job_id = self._id
            self._name = self._id

    def setId(self, idValue: str) -> None:
        self._id = idValue

    def getId(self) -> str:
        return self._id

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

    def getParentJobId(self) -> str:
        return self._parent_job_id

    def setOriginJobId(self, originId: str) -> None:
        self._origin_job_id = originId

    def getOriginJobId(self) -> str:
        return self._origin_job_id

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

    def setJobSetId(self, idValue: str) -> None:
        self._set_id = idValue

    def getJobSetId(self) -> str:
        return self._set_id

    def __str__(self):
        return f"[ctx id:{self.getId()} native:{self.getNativeId()} " + \
            f"parent:{self.getParentJobId()} origin:{self.getOriginJobId()} " + \
            f"set:{self.getJobSetId()} site:{self.getSiteName()} " + \
            f"compute:{self.getComputeType()}]"
