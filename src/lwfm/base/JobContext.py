

from enum import Enum

from lwfm.base.LwfmBase import LwfmBase

class _JobContextFields(Enum):
    ID = "id"                       # canonical job id
    NATIVE_ID = "nativeId"          # Run implementation native job id
    NAME = "name"                   # optional human-readable job name
    PARENT_JOB_ID = "parentJobId"   # immediate predecessor of this job, if any - 
                                    # seminal job has no parent
    ORIGIN_JOB_ID = (
        "originJobId"               # oldest ancestor - a seminal job is its own originator
    )
    SET_ID = "setId"                # optional id of a set if the job is part of a set
    SITE_NAME = "siteName"          # name of the Site which emitted the message
    COMPUTE_TYPE = "computeType"    # a named resource on the Site, if any
 

class JobContext(LwfmBase):
    """
    TODO cleanup docs

    The runtime execution context of the job.  It contains the id of the job and references
    to its parent jobs, if any.  A Job Status can reference a Job Context, and then augument 
    it with updated job status information.

    Attributes:

    id - the lwfm id of the executing job.  This is distinct from the "native job id" below, 
         which is the id of the job on the specific Site.  
         If the Site is "lwfm local", then one might expect that the id and the native id
         are the same, else one should assume they are not.  lwfm ids are generated as uuids.
         Sites can use whatever mechanism they prefer.

    native id - the Site-generated job id

    parent job id - a lwfm generated id, the immediate parent of this job, if any; a 
        seminal job has no parent

    origin job id - the eldest parent in the job chain; a seminal job is its own originator

    name - the job can have an optional name for human consumption, else the name is the 
        lwfm job id

    site name - the job is running (or has been submitted to the Site for queuing), 
        therefore the Site name is known

    compute type - if the Site distinguishes compute types, it can be noted here

    """

    def __init__(self, parentContext: "JobContext" = None):
        super(JobContext, self).__init__(None)
        self.setNativeId(self.getId())
        self.setParentJobId(None)
        # a seminal job would have no parent - it may be set later at runtime
        self.setOriginJobId(
            self.getId()
        )  # a seminal job would be its own originator - it may be set later
        self.setName(self.getId())
        self.setComputeType("default")
        self.setSiteName("local")  # default to local
        if (parentContext is not None):
            self.setParentJobId(parentContext.getJobId())
            self.setOriginJobId(parentContext.getOriginJobId())
            self.setSiteName(parentContext.getSiteName())

    def setId(self, idValue: str) -> None:
        LwfmBase._setArg(self, _JobContextFields.ID.value, idValue)

    def getId(self) -> str:
        return LwfmBase._getArg(self, _JobContextFields.ID.value)
    
    def setJobId(self, idValue: str) -> None:
        self.setId(idValue) # alias

    def getJobId(self) -> str:
        return self.getId()    # alias

    def setNativeId(self, idValue: str) -> None:
        LwfmBase._setArg(self, _JobContextFields.NATIVE_ID.value, idValue)

    def getNativeId(self) -> str:
        return LwfmBase._getArg(self, _JobContextFields.NATIVE_ID.value)

    def setParentJobId(self, idValue: str) -> None:
        LwfmBase._setArg(self, _JobContextFields.PARENT_JOB_ID.value, idValue)

    def getParentJobId(self) -> str:
        return LwfmBase._getArg(self, _JobContextFields.PARENT_JOB_ID.value)

    def setOriginJobId(self, idValue: str) -> None:
        LwfmBase._setArg(self, _JobContextFields.ORIGIN_JOB_ID.value, idValue)

    def getOriginJobId(self) -> str:
        return LwfmBase._getArg(self, _JobContextFields.ORIGIN_JOB_ID.value)

    def setJobSetId(self, idValue: str) -> None:
        LwfmBase._setArg(self, _JobContextFields.SET_ID.value, idValue)

    def getJobSetId(self) -> str:
        return LwfmBase._getArg(self, _JobContextFields.SET_ID.value)

    def setName(self, name: str) -> None:
        LwfmBase._setArg(self, _JobContextFields.NAME.value, name)

    def getName(self) -> str:
        return LwfmBase._getArg(self, _JobContextFields.NAME.value)

    def setSiteName(self, name: str) -> None:
        LwfmBase._setArg(self, _JobContextFields.SITE_NAME.value, name)

    def getSiteName(self) -> str:
        return LwfmBase._getArg(self, _JobContextFields.SITE_NAME.value)

    def setComputeType(self, name: str) -> None:
        LwfmBase._setArg(self, _JobContextFields.COMPUTE_TYPE.value, name)

    def getComputeType(self) -> str:
        return LwfmBase._getArg(self, _JobContextFields.COMPUTE_TYPE.value)

    def setGroup(self, name: str) -> None:
        LwfmBase._setArg(self, _JobContextFields.GROUP.value, name)

    def getGroup(self) -> str:
        return LwfmBase._getArg(self, _JobContextFields.GROUP.value)

    def setUser(self, name: str) -> None:
        LwfmBase._setArg(self, _JobContextFields.USER.value, name)

    def getUser(self) -> str:
        return LwfmBase._getArg(self, _JobContextFields.USER.value)


    def __str__(self):
        return f"[ctx id:{self.getId()} native:{self.getNativeId()} " + \
            f"parent:{self.getParentJobId()} origin:{self.getOriginJobId()} " + \
            f"set:{self.getJobSetId()} site:{self.getSiteName()} compute:{self.getComputeType()}]"
    



