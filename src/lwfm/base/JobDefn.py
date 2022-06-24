
# A Job Definition is the abstract representation of a job, the non-instantiated description.
# The JobDefn will be passed to the Site's Run driver which will use the args to instantiate a job from the defn.
# As time goes on, and the lwfm's refactoring of "sites" continues, additional arbitrary name=value pairs might get promoted
# to be named explicitly at the class level.  Of note is "compute type" which is a mechanism to address jobs at specific computing
# resources within the Site on which the job is run.  For example, an HPC site which has CPU and CPU+GPU nodes.

from enum import Enum
import logging

from pathlib import Path

from lwfm.base.LwfmBase import LwfmBase
from lwfm.base.SiteFileRef import SiteFileRef


class _JobDefnFields(Enum):
    NAME               = "name"                        # for human convenience
    COMPUTE_TYPE       = "computeType"                 # some sites define addressable compute resources within it
    ENTRY_POINT        = "entryPoint"                  # defines the top-level "executable" command to pass to the site scheduler
    JOB_ARGS           = "jobArgs"                     # arguments to the job - an array of string
    NOTIFICATION_EMAIL = "notificationEmail"           # some site schedulers permit direct user notification
    REPO_OP            = "repoOp"                      # put, get
    REPO_LOCAL_REF     = "repoLocalRef"                # local file reference
    REPO_SITE_REF      = "repoSiteRef"                 # site file reference
    # EXTRA_ARGS                                       # site schedulers vary widely - this dict permits arbitrary args


class JobDefn(LwfmBase):

    def __init__(self):
        super(JobDefn, self).__init__(None)

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

    def setNotificationEmail(self, email: str) -> None:
        LwfmBase._setArg(self, _JobDefnFields.NOTIFICATION_EMAIL.value, email)

    def getNotificationEmail(self) -> str:
        return LwfmBase._getArg(self, _JobDefnFields.NOTIFICATION_EMAIL.value)


#************************************************************************************************************************************

class RepoOp(Enum):
    PUT = "put"
    GET = "get"


class RepoJobDefn(JobDefn):
    def __init__(self):
        super(RepoJobDefn, self).__init__()

    def setRepoOp(self, repoOp: RepoOp) -> None:
        LwfmBase._setArg(self, _JobDefnFields.REPO_OP.value, repoOp)

    def getRepoOp(self) -> RepoOp:
        return LwfmBase._getArg(self, _JobDefnFields.REPO_OP.value)

    def setLocalRef(self, localRef: Path) -> None:
        LwfmBase._setArg(self, _JobDefnFields.REPO_LOCAL_REF.value, str(localRef))

    def getLocalRef(self) -> Path:
        return Path(LwfmBase._getArg(self, _JobDefnFields.REPO_LOCAL_REF.value))

    def setSiteRef(self, siteRef: SiteFileRef) -> None:
        LwfmBase._setArg(self, _JobDefnFields.REPO_SITE_REF.value, siteRef)

    def getSiteRef(self) -> SiteFileRef:
        return LwfmBase._getArg(self, _JobDefnFields.REPO_SITE_REF.value)


#************************************************************************************************************************************
# test
if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    jdefn = JobDefn()
