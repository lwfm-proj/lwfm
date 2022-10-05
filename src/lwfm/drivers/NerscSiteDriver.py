
# NerscSiteDriver: an implementation of Site and its constituent Auth, Run, Repo interfaces for the NERSC Superfacility API.
# This national lab API provides an interface to interact with HPC resources such as Cori and Perlmutter.

from authlib.integrations.requests_client import OAuth2Session
from authlib.oauth2.rfc7523 import PrivateKeyJWT

import requests
import json
import time
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Callable

import logging

from lwfm.base.Site import Site, SiteAuthDriver, SiteRunDriver, SiteRepoDriver
from lwfm.base.SiteFileRef import FSFileRef, SiteFileRef, RemoteFSFileRef
from lwfm.base.JobDefn import JobDefn, RepoOp
from lwfm.base.JobStatus import JobStatus, JobStatusValues, JobContext
from lwfm.base.JobEventHandler import JobEventHandler
from lwfm.base.MetaRepo import MetaRepo
from lwfm.store.AuthStore import AuthStore


class NerscSite(Site):
    def __init__(self):
        super(NerscSite, self).__init__("nersc", NerscSiteAuthDriver(), NerscSiteRunDriver(), NerscSiteRepoDriver(), None)
        _runDriver.setMachine(None)

class PerlmutterSite(Site):
    def __init__(self):
        super(PerlmutterSite, self).__init__("nersc", NerscSiteAuthDriver(), PerlmutterSiteRunDriver(), PerlmutterSiteRepoDriver(),
                                             None)
        _runDriver.setMachine("perlmutter")

class CoriSite(Site):
    def __init__(self):
        super(CoriSite, self).__init__("nersc", NerscSiteAuthDriver(), CoriSiteRunDriver(), CoriSiteRepoDriver(), None)
        _runDriver.setMachine("cori")

NERSC_BASE_URL = "https://api.nersc.gov/api/v1.2"
class NERSC_URLS(Enum):
    NERSC_SUBMIT_URL = NERSC_BASE_URL + "/compute/jobs/"
    NERSC_STATUS_URL = NERSC_BASE_URL + "/compute/jobs/"
    NERSC_CANCEL_URL = NERSC_BASE_URL + "/compute/jobs/"
    NERSC_TASK_URL   = NERSC_BASE_URL + "/tasks/"
    NERSC_PUT_URL = NERSC_BASE_URL + "/utilities/upload/"
    NERSC_GET_URL = NERSC_BASE_URL + "/utilities/download/"
    NERSC_LS_URL  = NERSC_BASE_URL + "/utilities/ls/"
    NERSC_CMD_URL = NERSC_BASE_URL + "/utilities/command/"

class NerscJobStatus(JobStatus):
    def __init__(self, jcontext: JobContext = None):
        super(NerscJobStatus, self).__init__(jcontext)
        self.setSiteName("nersc")
        self.setStatusMap({
            "OK"            : JobStatusValues.PENDING    ,
            "NEW"           : JobStatusValues.PENDING    ,
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
            self._accessToken = self._authJson['access_token']
            self._expiresAt = self._authJson['expires_at']
            self._session = session
            return True
        except Exception as ex:
            logging.error("Error logging into Nersc: {}".format(ex))
            self._authJson = None
            self._accessToken = None
            self._expiresAt = None
            self._session = None
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


#***********************************************************************************************************************************

class NerscSiteRunDriver(SiteRunDriver):
    machine = None

    def _getSession(self):
        authDriver = NerscSiteAuthDriver()
        authDriver.login()
        return authDriver._session

    def setMachine(self, machine):
        self.machine = machine

    def submitJob(self, jdefn: JobDefn=None, parentContext: JobContext = None) -> JobStatus:
        # We can (should?) use the compute type from the JobDefn, but we should keep usage consistent with the other methods
        if self.machine is None:
            logging.error("No machine found. Please use the setMachine() method before trying to submit a NERSC job.")
            return False

        # Make sure we have a parent context
        if (parentContext is None):
            parentContext = JobContext()

        # Construct our URL
        url = NERSC_URLS.NERSC_SUBMIT_URL.value + self.machine

        # Submit the job
        session = self._getSession()
        data = {"isPath" : True,
                "job" : jdefn.getEntryPoint()}
        r = session.post(url, data=data)
        if not r.status_code == requests.codes.ok:
            logging.error("Error submitting job")
            return False
        task_id = r.json()['task_id']


        # Given a task ID, we need to get the job ID
        # It takes time to process the task, so loop through and check every few seconds until we have a job id
        status = 'new'
        while not status == 'completed':
           task_url = NERSC_URLS.NERSC_TASK_URL.value + task_id
           r = session.get(task_url)
           status = r.json()['status']
           time.sleep(2)
        j = json.loads(r.json()['result']) # 'result' is a JSON formatted string, so we need to convert again
        if j['error'] is not None:
            logging.error("Error submitting job: " + j['error'])
            return False

        # Construct our status message
        jstatus = NerscJobStatus(parentContext)
        jstatus.setNativeStatusStr(j['status'].upper())
        jstatus.setNativeId(j['jobid'])
        jstatus.setEmitTime(datetime.utcnow())
        jstatus.setSiteName(self.machine)
        return jstatus

    def getJobStatus(self, jobContext: JobContext) -> JobStatus:
        if self.machine is None:
            logging.error("No machine found. Please use the setMachine() method before trying to check a NERSC job status.")
            return False

        # Construct our URL
        url = NERSC_URLS.NERSC_STATUS_URL.value + self.machine + "/" + jobContext.getNativeId()

        # Check the status
        session = self._getSession()
        data = {"sacct" : True} # We can use either sacct or squeue for info, sacct seems to work a fail a bit less often
        r = session.get(url, data=data)
        if not r.status_code == requests.codes.ok:
            logging.error("Error getting job status")
            return False
        if not r.json()['output']:
            logging.error("Job not found.")
            return False
        j = r.json()['output'][0]

        # Construct our status message
        jstatus = NerscJobStatus()
        jstatus.setNativeStatusStr(j['state'].split(' ')[0]) # Cancelled jobs appear in the form "CANCELLED by user123", so make sure to just grab the beginning
        jstatus.setNativeId(jobContext.getNativeId())
        jstatus.setEmitTime(datetime.utcnow())
        jstatus.setSiteName(self.machine)
        return jstatus

    def cancelJob(self, jobContext: JobContext) -> bool:
        if self.machine is None:
            logging.error("No machine found. Please use the setMachine() method before trying to cancel a NERSC job.")
            return False

        # Construct our URL
        url = NERSC_URLS.NERSC_SUBMIT_URL.value + self.machine +"/" + jobContext.getNativeId()

        # Cancel the job
        session = self._getSession()
        r = session.delete(url)
        if not r.status_code == requests.codes.ok:
            logging.error("Error cancelling job")
            return False
        return True


    def listComputeTypes(self) -> [str]:
        raise NotImplementedError()


    def setEventHandler(self, jobContext: JobContext, jobStatus: JobStatusValues, statusFilter: Callable,
                        newJobDefn: JobDefn, newJobContext: JobContext, newSiteName: str) -> JobEventHandler:
        raise NotImplementedError()


    def unsetEventHandler(self, jeh: JobEventHandler) -> bool:
        raise NotImplementedError()


    def listEventHandlers(self) -> [JobEventHandler]:
        raise NotImplementedError()



class PerlmutterSiteRunDriver(NerscSiteRunDriver):
    machine = 'perlmutter'

class CoriSiteRunDriver(NerscSiteRunDriver):
    machine = 'cori'

#***********************************************************************************************************************************

class NerscSiteRepoDriver(SiteRepoDriver):
    machine = None

    def _getSession(self):
        authDriver = NerscSiteAuthDriver()
        authDriver.login()
        return authDriver._session

    def put(self, localRef: Path, siteRef: SiteFileRef, jobContext: JobContext = None) -> SiteFileRef:
        # Book keeping for status emissions
        iAmAJob = False
        if (jobContext is None):
            iAmAJob = True
            jobContext = JobContext()
        jstatus = NerscJobStatus(jobContext)
        jstatus.setSiteName(machine)
        if (iAmAJob):
            # emit the starting job status sequence
            jstatus.emit(JobStatusValues.PENDING.value)
            jstatus.emit(JobStatusValues.RUNNING.value)

        # Construct our URL
        remotePath = siteRef.getPath()
        url = NERSC_URLS.NERSC_PUT_URL.value + self.machine + remotePath

        # Emit our info status before hitting the API
        jstatus.setNativeInfo(JobStatus.makeRepoInfo(RepoOp.PUT, False, str(localRef), str(remotePath)))
        jstatus.emit(JobStatusValues.INFO.value)

        # Convert the file into a binary form we can send over
        with localRef.open('rb') as f:
            fileBinary = f.read()

        # Make the connection and send the file
        session = self._getSession()
        data = {"file" : fileBinary}
        r = session.put(url, data=data)
        if not r.status_code == requests.codes.ok:
            logging.error("Error uploading file")
            if (iAmAJob):
                jstatus.emit(JobStatusValues.FAILED.value)
            return False
        if (iAmAJob):
            # emit the successful job ending sequence
            jstatus.emit(JobStatusValues.FINISHING.value)
            jstatus.emit(JobStatusValues.COMPLETE.value)
        MetaRepo.notate(SiteFileRef)
        return SiteFileRef

    def get(self, siteRef: SiteFileRef, localRef: Path, jobContext: JobContext = None) -> Path:
        # Book keeping for status emissions
        iAmAJob = False
        if (jobContext is None):
            iAmAJob = True
            jobContext = JobContext()
        jstatus = NerscJobStatus(jobContext)
        jstatus.setSiteName(machine)
        if (iAmAJob):
            # emit the starting job status sequence
            jstatus.emit(JobStatusValues.PENDING.value)
            jstatus.emit(JobStatusValues.RUNNING.value)

        # Construct our URL
        remotePath = siteRef.getPath()
        url = NERSC_URLS.NERSC_GET_URL.value + self.machine + remotePath

        # Emit our info status before hitting the API
        jstatus.setNativeInfo(JobStatus.makeRepoInfo(RepoOp.PUT, False, remotePath, str(localRef)))
        jstatus.emit(JobStatusValues.INFO.value)

        # Make the connection and grab the file
        session = self._getSession()
        r = session.get(url)
        if not r.status_code == requests.codes.ok:
            logging.error("Error downloading file")
            if (iAmAJob):
                jstatus.emit(JobStatusValues.FAILED.value)
            return False

        # Now we can write
        with localRef.open('w', newline='') as f: # Newline argument is needed or else all newlines are doubled
            f.write(r.json()['file'])
        if (iAmAJob):
            # emit the successful job ending sequence
            jstatus.emit(JobStatusValues.FINISHING.value)
            jstatus.emit(JobStatusValues.COMPLETE.value)
        MetaRepo.Notate(SiteFileRef)
        return localRef

    def find(self, siteRef: SiteFileRef) -> [SiteFileRef]:
        path = siteRef.getPath()
        url = NERSC_URLS.NERSC_LS_URL.value + self.machine + path

        # Write. Note that we want to pass back the full ls info
        session = self._getSession()
        r = session.get(url)
        if not r.status_code == requests.codes.ok:
            logging.error("Error performing ls")
            return False

        # Superfacility returns a json object. The "entries" field is a list of dicts, where each dict
        # corresponds to a file, with name, size, and other bits of info. We only want the name.
        fileList = r.json()["entries"]
        fileList = [f["name"] for f in fileList]
        remoteRef = FSFileRef()
        remoteRef.setDirContents(fileList)
        return [remoteRef]

class PerlmutterSiteRepoDriver(NerscSiteRepoDriver):
    machine = 'perlmutter'

class CoriSiteRepoDriver(NerscSiteRepoDriver):
    machine = 'cori'

#***********************************************************************************************************************************



# Force a login vs Nersc and store the token results.
if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)
    site = Site.getSiteInstanceFactory("nersc")
    logging.info("Forcing new login to NERSC...")
    site.getAuthDriver().login(True)
    logging.info("Is NERSC auth valid: " + str(site.getAuthDriver().isAuthCurrent()))

    # Try run methods
    jdefn = JobDefn()
    jdefn.setEntryPoint("/global/homes/a/agallojr/slurm_test.sh")
    runDriver = NerscSiteRunDriver()
    runDriver.setMachine("perlmutter")
    job = runDriver.submitJob(jdefn)
    runDriver.getJobStatus(job.getNativeId())
    runDriver.cancelJob(job.getNativeId())
    runDriver.getJobStatus(job.getNativeId())

    # Try file methods
    #localRef = Path("C:/lwfm/foo.py")
    #localRef2 = Path("C:/lwfm/foo2.py")
    #siteRef = RemoteFSFileRef()
    #siteRef.setHost("perlmutter")
    #siteRef.setPath("/global/homes/a/agallojr/tmp.py")
    #NerscSiteRepoDriver().put(localRef, siteRef)
    #NerscSiteRepoDriver().get(siteRef, localRef2)
