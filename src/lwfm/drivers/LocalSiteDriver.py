
import logging

from lwfm.base.Site import Site, SiteAuthDriver, SiteRunDriver
from lwfm.base.JobStatus import JobStatus


class LocalSiteAuthDriver(SiteAuthDriver):
    def login(self, force: bool=False) -> bool:
        return True

    def isAuthCurrent(self) -> bool:
        return True

    def writeToStore(self) -> bool:
        return True

    def readFromStore(self) -> bool:
        return True


class LocalSiteRunDriver(SiteRunDriver):
    def getJobStatus(self, nativeJobId: str) -> JobStatus:
        return JobStatus()

    def cancelJob(self, nativeJobId: str) -> bool:
        return True


# test
if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    site = Site("local", LocalSiteAuthDriver(), None)
    logging.info(site.getName())
    site.getAuthDriver().login()
    logging.info(site.getAuthDriver().isAuthCurrent())
