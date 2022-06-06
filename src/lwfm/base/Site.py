
# Site: Defines an abstract computing location which exposes canonical verbs for the Auth, Run, and Repo (and someday Spin)
# logical subsystems. The purpose of lwfm is to permit workflows which span Sites.


from enum import Enum
import logging
from abc import ABC, abstractmethod
from pathlib import Path


from lwfm.base.LwfmBase  import LwfmBase
from lwfm.base.JobStatus import JobStatus, JobContext
from lwfm.base.JobDefn import JobDefn

from lwfm.base.SiteFileRef import SiteFileRef


#***********************************************************************************************************************************
# Auth: an interface for a Site's user authentication and authorization functiions.  Permits a Site to provide some kind of a
# "login" be it programmatic or forced interactive.  A given Site's implmentation of Auth might squirrel away some returned
# token (should the Site use such things).  We can then provide a quicker "is the auth current" method.  We assume the implementation
# of a Site's Run and Repo subsystems will be provided access under the hood to the necessary implementation details from the
# Auth system.

class SiteAuthDriver(ABC):
    @abstractmethod
    def login(self, force: bool=False) -> bool:
        pass

    @abstractmethod
    def isAuthCurrent(self) -> bool:
        pass


#***********************************************************************************************************************************
# Run: in its most basic "MVP-0" form, the Run subsystem provides a mechanism to submit a job, cancel the job, and interrogate
# the job's status.  The submitting of a job might, for some Sites, involve a batch scheduler.  Or, for some Sites (like a "local"
# Site), the run might be immediate.  On submit, the method returns a JobStatus, which for immediate execution might be a terminal
# completion status.  A job definition (JobDefn) is a description of the job.  Its the role of the Site's Run subsystem to
# interpret the abstract JobDefn in the context of that Site.  Thus the JobDefn permits arbitrary name=value pairs which might be
# necessary to submit a job at that Site.  The JobStatus returned is canonical - the Site's own status name set is mapped into the
# lwfm canonical name set by the implementation of the Site itself.

class SiteRunDriver(ABC):

    @classmethod
    def _submitJob(cls, jdefn, jobContext = None):
        # This helper function lets threading instantiate a SiteRunDriver of the correct subtype on demand
        runDriver = cls()
        runDriver.submitJob(jdefn, jobContext)

    @abstractmethod
    def submitJob(self, jdefn: JobDefn, parentContext: JobContext = None) -> JobStatus:
        pass

    @abstractmethod
    def getJobStatus(self, jobContext: JobContext) -> JobStatus:
        pass

    @abstractmethod
    def cancelJob(self, jobContext: JobContext) -> bool:
        pass


#***********************************************************************************************************************************
# Repo: Pemmits the movement of data objects to/from the Site.  The methods require a local file reference, and a reference to the
# remote file - a SiteFileRef.  The SiteFileRef permits arbitrary name=value pairs because the Site might require them to
# complete the transaction.

class SiteRepoDriver(ABC):
    # Take the local file by path and put it to the remote site.
    # If we're given a context, we use it, if not, we consider ourselves our own job.
    @abstractmethod
    def put(self, localRef: Path, siteRef: SiteFileRef, jobContext: JobContext = None) -> SiteFileRef:
        pass

    # Get the file from the remote site and write it local, returning a path to the local.
    # If we're given a context, we use it, if not, we consider ourselves our own job.
    @abstractmethod
    def get(self, siteRef: SiteFileRef, localRef: Path, jobContext: JobContext = None) -> Path:
        pass

    # get info about the file/dir on the remote site
    @abstractmethod
    def ls(self, siteRef: SiteFileRef) -> SiteFileRef:
        pass


#************************************************************************************************************************************
# Spin: vaporware.  In theory some Sites would expose mechanisms to create (provision) and destroy various kinds of computing
# devices.  These might be single nodes, or entire turnkey cloud-bases HPC systems.  Spin operations are modeled as jobs in
# order to permit sequential workflows which spin up resources, send them jobs, and then spin them down as part of an
# autonomous operation.

#***********************************************************************************************************************************
# Site: the Site is simply a name and the getters and setters for its Auth, Run, Repo subsystems.


# LwfmBase field list
class _SiteFields(Enum):
    SITE_NAME = "siteName"


class Site(LwfmBase):

    _authDriver: SiteAuthDriver = None
    _runDriver:  SiteRunDriver  = None
    _repoDriver: SiteRepoDriver = None

    _SITES = {
        "nersc": [ "lwfm.drivers.NerscSiteDriver", "NerscSite" ],
        "perlmutter": [ "lwfm.drivers.PerlmutterSiteDriver", "PerlmutterSite" ],
        "local": [ "lwfm.drivers.LocalSiteDriver", "LocalSite" ]
    }

    @staticmethod
    def getSiteInstanceFactory(site = "local"):
        entry = Site._SITES[site]
        import importlib
        module = importlib.import_module(entry[0])
        class_ = getattr(module, entry[1])
        inst = class_()
        inst.setName(site)
        return inst

    def __init__(self, name: str, authDriver: SiteAuthDriver, runDriver: SiteRunDriver, repoDriver: SiteRepoDriver, args: dict=None):
        super(Site, self).__init__(args)
        self.setName(name)
        self.setAuthDriver(authDriver)
        self.setRunDriver(runDriver)
        self.setRepoDriver(repoDriver)

    def setName(self, name: str) -> None:
        LwfmBase._setArg(self, _SiteFields.SITE_NAME.value, name)

    def getName(self) -> str:
        return LwfmBase._getArg(self, _SiteFields.SITE_NAME.value)

    def setAuthDriver(self, authDriver: SiteAuthDriver) -> None:
        self._authDriver = authDriver

    def getAuthDriver(self) -> SiteAuthDriver:
        return self._authDriver

    def setRunDriver(self, runDriver: SiteRunDriver) -> None:
        self._runDriver = runDriver

    def getRunDriver(self) -> SiteRunDriver:
        return self._runDriver

    def setRepoDriver(self, repoDriver: SiteRepoDriver) -> None:
        self._repoDriver = repoDriver

    def getRepoDriver(self) -> SiteRepoDriver:
        return self._repoDriver


#***********************************************************************************************************************************

# test
if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    siteFoo = Site.getSiteInstanceFactory()
    logging.info(siteFoo.getName())
