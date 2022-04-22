
from enum import Enum
import logging
from abc import ABC, abstractmethod


from lwfm.base.LwfmBase import LwfmBase


class SiteLoginDriver(ABC):
    @abstractmethod
    def login(self) -> bool:
        pass


class SiteRunDriver(ABC):
    @abstractmethod
    def submitJob(self) -> bool:
        pass

    @abstractmethod
    def getJobStatus(self) -> bool:    # TODO
        pass

    @abstractmethod
    def cancelJob(self) -> bool:
        pass




class _SiteFields(Enum):
    SITE_NAME = "siteName"


class Site(LwfmBase):

    _loginDriver: SiteLoginDriver
    _runDriver:   SiteRunDriver

    def __init__(self, name: str, loginDriver: SiteLoginDriver, runDriver: SiteRunDriver, args: dict[str, type]=None):
        super(Site, self).__init__(args)
        self.setName(name)
        self.setLoginDriver(loginDriver)
        self.setRunDriver(runDriver)

    def setName(self, name: str) -> None:
        LwfmBase._setArg(self, _SiteFields.SITE_NAME.value, name)

    def getName(self) -> str:
        return LwfmBase._getArg(self, _SiteFields.SITE_NAME.value)

    def setLoginDriver(self, loginDriver: SiteLoginDriver) -> None:
        self._loginDriver = loginDriver

    def getLoginDriver(self) -> SiteLoginDriver:
        return self._loginDriver

    def setRunDriver(self, runDriver: SiteRunDriver) -> None:
        self._runDriver = runDriver

    def getRunDriver(self) -> SiteRunDriver:
        return self._runDriver



# test
if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    siteFoo = Site("foo", None, None)
    logging.info(siteFoo.getName())
