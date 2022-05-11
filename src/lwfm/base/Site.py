
from enum import Enum
import logging
from abc import ABC, abstractmethod


from lwfm.base.LwfmBase  import LwfmBase
from lwfm.base.JobStatus import JobStatus
from lwfm.base.JobDefn import JobDefn



#***********************************************************************************************************************************

class SiteAuthDriver(ABC):
    @abstractmethod
    def login(self, force: bool=False) -> bool:
        pass

    @abstractmethod
    def isAuthCurrent(self) -> bool:
        pass

    @abstractmethod
    def writeToStore(self) -> bool:
        pass

    @abstractmethod
    def readFromStore(self) -> bool:
        pass


#***********************************************************************************************************************************

class SiteRunDriver(ABC):
    @abstractmethod
    def submitJob(self, jdefn: JobDefn=None) -> JobStatus:
        pass

    @abstractmethod
    def getJobStatus(self, nativeJobId: str) -> JobStatus:
        pass

    @abstractmethod
    def cancelJob(self, nativeJobId: str) -> bool:
        pass


#***********************************************************************************************************************************

class _SiteFields(Enum):
    SITE_NAME = "siteName"


class Site(LwfmBase):

    _authDriver: SiteAuthDriver = None
    _runDriver:   SiteRunDriver = None

    def __init__(self, name: str, authDriver: SiteAuthDriver, runDriver: SiteRunDriver, args: dict[str, type]=None):
        super(Site, self).__init__(args)
        self.setName(name)
        self.setAuthDriver(authDriver)
        self.setRunDriver(runDriver)

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


#***********************************************************************************************************************************

# test
if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    siteFoo = Site("foo", None, None)
    logging.info(siteFoo.getName())
