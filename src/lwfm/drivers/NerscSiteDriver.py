
from authlib.integrations.requests_client import OAuth2Session
from authlib.oauth2.rfc7523 import PrivateKeyJWT

import requests
import json
import os
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

import logging

from lwfm.base.Site import Site, SiteAuthDriver, SiteRunDriver, SiteRepoDriver
from lwfm.base.SiteFileRef import SiteFileRef, RemoteFSFileRef
from lwfm.base.JobStatus import JobStatus, JobStatusValues
from lwfm.store.AuthStore import AuthStore

NERSC_BASE_URL = "https://api.nersc.gov/api/v1.2"
class NERSC_URLS(Enum):
    NERSC_PUT_URL = NERSC_BASE_URL + "/utilities/upload/"
    NERSC_GET_URL = NERSC_BASE_URL + "/utilities/download/"
    NERSC_LS_URL  = NERSC_BASE_URL + "/utilities/ls/"
    NERSC_CMD_URL = NERSC_BASE_URL + "/utilities/command/"

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
    _session = None

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
            self._session = session
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
    def _getSession(self):
        authDriver = NerscSiteAuthDriver()
        authDriver.login()
        return authDriver._session
    
    def put(self, localRef: Path, siteRef: SiteFileRef) -> SiteFileRef:
        # Construct our URL
        machine = siteRef.getHost()
        remotePath = siteRef.getPath()
        url = NERSC_URLS.NERSC_PUT_URL.value + machine + remotePath

        # Convert the file into a binary form we can send over
        with localRef.open('rb') as f:
            fileBinary = f.read()

        # Make the connection and send the file
        session = self._getSession()
        data = {"file" : fileBinary}
        r = session.put(url, data=data)
        if not r.status_code == requests.codes.ok:
            logging.error("Error uploading file")
        return SiteFileRef

    def get(self, siteRef: SiteFileRef, localRef: Path) -> Path:
        # Construct our URL
        machine = siteRef.getHost()
        remotePath = siteRef.getPath()
        url = NERSC_URLS.NERSC_GET_URL.value + machine + remotePath

        # Make the connection and grab the file
        session = self._getSession()
        r = session.get(url)
        if not r.status_code == requests.codes.ok:
            logging.error("Error downloading file")

        # Now we can write        
        with localRef.open('w', newline='') as f: # Newline argument is needed or else all newlines are doubled
            f.write(r.json()['file'])
        return localRef

    def ls(self, siteRef: SiteFileRef) -> SiteFileRef:
        machine = siteRef.getHost()
        path = siteRef.getPath()
        url = NERSC_URLS.NERSC_LS_URL.value + machine + path
        session = self._getSession()
        r = session.get(url)
        return r.json()
        

# Force a login vs Nersc and store the token results.
if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    site = Site("nersc", NerscSiteAuthDriver(), None)
    logging.info("Forcing new login to NERSC...")
    site.getAuthDriver().login(True)
    logging.info("Is NERSC auth valid: " + str(site.getAuthDriver().isAuthCurrent()))

    # Try file methods
    localRef = Path("C:/lwfm/foo.py")
    localRef2 = Path("C:/lwfm/foo2.py")
    siteRef = RemoteFSFileRef()
    siteRef._setHost("perlmutter")
    siteRef._setPath("/global/homes/a/agallojr/tmp.py")
    NerscSiteRepoDriver().put(localRef, siteRef)
    NerscSiteRepoDriver().get(siteRef, localRef2)