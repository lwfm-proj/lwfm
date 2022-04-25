
from authlib.integrations.requests_client import OAuth2Session
from authlib.oauth2.rfc7523 import PrivateKeyJWT

import requests
import json
import os
from datetime import datetime, timezone

import logging

from lwfm.base.Site import Site, SiteAuthDriver, SiteRunDriver
from lwfm.base.JobStatus import JobStatus
from lwfm.store.AuthStore import AuthStore


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
            logging.error("Error logging into Nersc: {}".format(ex.message))
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



class NerscSiteRunDriver(SiteRunDriver):
    def getJobStatus(self, nativeJobId: str) -> JobStatus:
        return JobStatus()

    def cancelJob(self, nativeJobId: str) -> bool:
        return True



# Force a login vs Nersc and store the token results.
if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    site = Site("nersc", NerscSiteAuthDriver(), None)
    logging.info("Forcing new login to NERSC...")
    site.getAuthDriver().login(True)
    logging.info("Is NERSC auth valid: " + str(site.getAuthDriver().isAuthCurrent()))
