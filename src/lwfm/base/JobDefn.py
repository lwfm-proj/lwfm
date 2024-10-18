
# A Job Definition is the abstract representation of a job, the non-instantiated 
# description. The JobDefn will be passed to the Site's Run driver which will use the 
# args to instantiate a job from the definition.
#
# Of note is "compute type" which is a mechanism to address jobs at specific computing
# resources within the Site on which the job is run.  For example, an HPC site which 
# has CPU and CPU+GPU nodes - a workflow script can indicate that the job should be run 
# on a certain named compute configuration.  Its completely optional for a site to expose 
# a "compute type" concept - a site might have only one compute configuration.  Thanks
# to object oriented programming, a site can also model specific compute configurations 
# as subclasses of its Site class.

from enum import Enum

from pathlib import Path

from lwfm.base.LwfmBase import LwfmBase
from lwfm.base.SiteFileRef import SiteFileRef
from lwfm.base.JobContext import JobContext

class _JobDefnFields(Enum):
    NAME               = "name"         # optional - jobs do not need to be named - 
                                        #   they have ids
    COMPUTE_TYPE       = "computeType"  # some sites define addressable compute resources 
                                        #   within it
    ENTRY_POINT        = "entryPoint"   # defines the top-level "executable" command to 
                                        #   pass to the site scheduler
    JOB_ARGS           = "jobArgs"      # positional arguments to the job - an array of 
                                        #   string - the run driver will construct the 
                                        #   command line from these args
    REPO_OP            = "repoOp"       # put, get - for data movement jobs, the relative 
                                        #   direction is noted
                                        # see the RepoOp enum in this module  
    REPO_LOCAL_REF     = "repoLocalRef" # local file reference - the "from" reference for 
                                        #   a put, the "to" reference for a get
    REPO_SITE_REF      = "repoSiteRef"  # site file reference, the "to" reference for a put, 
                                        #   the "from" reference for a get
    # EXTRA_ARGS                        # TODO site schedulers vary widely - this dict 
                                        #   permits arbitrary args


class JobDefn(LwfmBase):
    """
    The static definition of a job, to be instantiated at runtime by the Site.Run subsystem.  
    The Job Defn is not presumed to be portable, though it is possible and the onus is on
    the user or the author of the Site driver.  
    Within the JobDefn will be baked arbitrary arguments, which might very well be 
    Site-specific (e.g., parameters to a specific Site HPC scheduler).  It is ultimately 
    the job of the Site Run subsystem to interpret the job defn and execute it.  The 
    standard arguments which would be needed to aid in broad portability are not specified 
    by this framework, nor are they precluded.

    Attributes:

    name - an optional name for human consumption

    compute type - we can target the job at an optional compute type on the Site, a 
        specific resource the Site provides;
        the Site might have no such concept and present only one runtime option

    entry point - a declaration of the command to run, from the perspective of the Site.  
        This can be anything from an actual command string, or a complex serialized object 
        - its entirely up to the Site how to specify and interpret the entry point -
        again, the JobDefn is not presumed to be portable across Sites

    job args - distinct from the entry point, the job might desire arbitrary arguments 
        at runtime

    """

    def __init__(self, entryPoint: str = None):
        super(JobDefn, self).__init__(None)
        self.setEntryPoint(entryPoint)

    def setName(self, name: str) -> None:
        LwfmBase._setArg(self, _JobDefnFields.NAME.value, name)

    def getName(self) -> str:
        return LwfmBase._getArg(self, _JobDefnFields.NAME.value)

    def setComputeType(self, name: str) -> None:
        LwfmBase._setArg(self, _JobDefnFields.COMPUTE_TYPE.value, name)

    def getComputeType(self) -> str:
        return LwfmBase._getArg(self, _JobDefnFields.COMPUTE_TYPE.value)

    def setEntryPoint(self, entryPoint: str) -> None:
        LwfmBase._setArg(self, _JobDefnFields.ENTRY_POINT.value, entryPoint)

    def getEntryPoint(self) -> str:
        return LwfmBase._getArg(self, _JobDefnFields.ENTRY_POINT.value)

    def setJobArgs(self, args: [str]) -> None:
        LwfmBase._setArg(self, _JobDefnFields.JOB_ARGS.value, args)

    def getJobArgs(self) -> [str]:
        return LwfmBase._getArg(self, _JobDefnFields.JOB_ARGS.value)


#************************************************************************************************************************************

# Directionality of the data movement relative to the current process.
class RepoOp(Enum):
    PUT = "put"
    GET = "get"


class RepoJobDefn(JobDefn):
    """
    Moving data between Sites is expected to be common, and potentially time consuming, 
    and thus wanted to be performed asynchronously (if at all... a multi-site execution 
    model may assist in bringing the compute to where the data already is).
    Thus wrapping a data movement as a job and setting a job event trigger to fire when 
    the data move is complete would be common.  The RepoJobDefn as a subclass of JobDefn 
    provides this convenience mechanism for wrapping a data move as an 
    independent job.
    """

    def __init__(self, repoOp: RepoOp = RepoOp.PUT, localRef: Path = None, 
                 siteFileRef: SiteFileRef = None):
        super(RepoJobDefn, self).__init__()
        self.setRepoOp(repoOp)
        if (localRef is not None):
            self.setLocalRef(localRef)
        if (siteFileRef is not None):
            self.setSiteFileRef(siteFileRef)

    def setRepoOp(self, repoOp: RepoOp) -> None:
        LwfmBase._setArg(self, _JobDefnFields.REPO_OP.value, repoOp)

    def getRepoOp(self) -> RepoOp:
        return LwfmBase._getArg(self, _JobDefnFields.REPO_OP.value)

    def setLocalRef(self, localRef: Path) -> None:
        LwfmBase._setArg(self, _JobDefnFields.REPO_LOCAL_REF.value, str(localRef))

    def getLocalRef(self) -> Path:
        return Path(LwfmBase._getArg(self, _JobDefnFields.REPO_LOCAL_REF.value))

    def setSiteFileRef(self, siteRef: SiteFileRef) -> None:
        LwfmBase._setArg(self, _JobDefnFields.REPO_SITE_REF.value, siteRef)

    def getSiteFileRef(self) -> SiteFileRef:
        return LwfmBase._getArg(self, _JobDefnFields.REPO_SITE_REF.value)



