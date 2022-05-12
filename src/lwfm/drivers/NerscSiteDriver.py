
from authlib.integrations.requests_client import OAuth2Session
from authlib.oauth2.rfc7523 import PrivateKeyJWT

import requests
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import logging

from lwfm.base.Site import Site, SiteAuthDriver, SiteRunDriver, SiteRepoDriver
from lwfm.base.SiteFileRef import SiteFileRef
from lwfm.base.JobStatus import JobStatus, JobStatusValues
from lwfm.store.AuthStore import AuthStore


class NerscJobStatus(JobStatus):
    def __init__(self):
        super(NerscJobStatus, self).__init__()
        self.setStatusMap({
            "PENDING"       : JobStatusValues.PENDING    ,
            "CONFIGURING"   : JobStatusValues.PENDING    ,
            "RUNNING"       : JobStatusValues.RUNNING    ,
            "COMPLETING"    : JobStatusValues.FINISHING  ,
            "STAGE_OUT"     : JobStatusValues.FINISHING  ,
            "COMPLETED"     : JobStatusValues.COMPLETE   ,
            "BOOT_FAIL"     : JobStatusValues.FAILED     ,
            "FAILED"        : JobStatusValues.FAILED     ,
            "NODE_FAIL"     : JobStatusValues.FAILED     ,
            "OUT_OF_MEMORY" : JobStatusValues.FAILED     ,
            "CANCELLED"     : JobStatusValues.CANCELLED  ,
            "PREEMPTED"     : JobStatusValues.CANCELLED  ,
            "SUSPENDED"     : JobStatusValues.CANCELLED  ,
            "DEADLINE"      : JobStatusValues.CANCELLED  ,
            "TIMEOUT"       : JobStatusValues.CANCELLED  ,
            })


class NerscSiteAuthDriver(SiteAuthDriver):
    _authJson: str = None
    _accessToken: str = None
    _expiresAt: int = None

    def login(self, force: bool=False) -> bool:
        if not force and self.isAuthCurrent():
            logging.debug("Auth is current - bypassing new login")
            return True

        # login again
        authProps = AuthStore().loadAuthProperties("nersc")
        if authProps is None:
            logging.error("Error locating 1st-factor of NERSC login info from user home dir")
            return False

        try:
            session = OAuth2Session(
                authProps['client_id'],
                authProps['private_key'],
                PrivateKeyJWT(authProps['token_url']),
                grant_type="client_credentials",
                token_endpoint=authProps['token_url']
            )
            self._authJson = session.fetch_token()
            logging.debug(str(self._authJson))
            self._accessToken = self._authJson['access_token']
            self._expiresAt = self._authJson['expires_at']
            return True
        except Exception as ex:
            logging.error("Error logging into Nersc: {}".format(ex))
            _authJson: str = None
            _accessToken: str = None
            _expiresAt: int = None
            return False


    def _isTokenValid(self) -> bool:
        now = datetime.utcnow().replace(tzinfo= timezone.utc).timestamp()
        if (self._expiresAt > now):
            return True
        else:
            return False


    def isAuthCurrent(self) -> bool:
        # are we already holding a valid auth token?
        if (self._accessToken is not None) and (self._isTokenValid()):
            return True
        else:
            return False

    def writeToStore(self) -> bool:
        # the token is only valid for 5 minutes - if this process exits, we'll just login again
        return True

    def readFromStore(self) -> bool:
        # a read from store would test the validity of the stored auth - since we don't store, we'll force a login
        logging.info("Stored auth does not exist or is no longer valid - logging in again")
        return self.login()

#***********************************************************************************************************************************

class NerscSiteRunDriver(SiteRunDriver):
    def submitJob(self, jdefn: JobDefn=None) -> JobStatus:
        pass
    
    def getJobStatus(self, nativeJobId: str) -> JobStatus:
        return JobStatus()

    def cancelJob(self, nativeJobId: str) -> bool:
        return True

#***********************************************************************************************************************************

class NerscSiteRepoDriver(SiteRepoDriver):
    def put(self, localRef: Path, siteRef: SiteFileRef) -> SiteFileRef:
        pass

    def get(self, siteRef: SiteFileRef, localRef: Path) -> Path:
        pass

    def ls(self, siteRef: SiteFileRef) -> SiteFileRef:
        pass    

# Force a login vs Nersc and store the token results.
if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    site = Site("nersc", NerscSiteAuthDriver(), None)
    logging.info("Forcing new login to NERSC...")
    site.getAuthDriver().login(True)
    logging.info("Is NERSC auth valid: " + str(site.getAuthDriver().isAuthCurrent()))
