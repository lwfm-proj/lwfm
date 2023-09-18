
# OrnlSiteDriver: an implementation of Site and its constituent Auth, Run, Repo interfaces for ORNL clusters.
# Because there is no API, we rely on Paramiko to open an SSH connection

from datetime import datetime
from pathlib import Path
from typing import Callable

import getpass
import paramiko
import threading

import logging

from lwfm.base.Site import Site, SiteAuthDriver, SiteRunDriver, SiteRepoDriver
from lwfm.base.SiteFileRef import SiteFileRef
from lwfm.base.JobDefn import JobDefn, RepoOp
from lwfm.base.JobStatus import JobStatus, JobStatusValues, JobContext
from lwfm.base.JobEventHandler import JobEventHandler


class OrnlSite(Site):
    def __init__(self):
        super(OrnlSite, self).__init__("ornl", OrnlSiteAuthDriver(), OrnlSiteRunDriver(), OrnlSiteRepoDriver(), None)

class SummitSite(Site):
    def __init__(self):
        super(SummitSite, self).__init__("summit", SummitSiteAuthDriver(), SummitSiteRunDriver(), OrnlSiteRepoDriver(),
                                             None)

class OrnlJobStatus(JobStatus):
    def __init__(self, jcontext: JobContext = None):
        super(OrnlJobStatus, self).__init__(jcontext)
        self.setStatusMap({
            "PEND"  : JobStatusValues.PENDING      ,
            "RUN"   : JobStatusValues.RUNNING      ,
            "DONE"  : JobStatusValues.COMPLETE     ,
            "EXIT"  : JobStatusValues.FAILED       ,
            "PSUSP" : JobStatusValues.CANCELLED    ,
            "USUSP" : JobStatusValues.CANCELLED    ,
            "SSUSP" : JobStatusValues.CANCELLED    ,
            })

KEEPALIVE_SECONDS = 300 # Send a message to each SSH client every n seconds to keep them alive

def _keepalive(client):
    # If we stop interacting with the machine, ORNL will eventually automatically close the connection
    # so periodically just send an empty command to keep it alive 
    client.exec_command("")
    threading.Timer(KEEPALIVE_SECONDS, _keepalive, (client,)).start()

class OrnlSiteAuthDriver(SiteAuthDriver):
    # The parent for different summit machines. To add a new machine, we need to make a child class
    # with a host identified, and a new class level "client" object 
    _host = None
    _client = None
    
    @classmethod
    def _get_client(cls, force=False):
        if cls._client is None or force:
            cls._client = paramiko.SSHClient()
            cls._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            username = input("ORNL Username: ")
            password = getpass.getpass(prompt="ORNL Passcode: ")
            cls._client.connect(cls._host, username=username, password=password)
            #threading.Timer(KEEPALIVE_SECONDS, _keepalive, (cls._client,)).start()
        return cls._client
    

    def login(self, force: bool=False) -> bool:
        # We need a class method so we can treat the connection for each endpoint as a singleton
        try:
            self.__class__._get_client(force)
            retVal = True
        except Exception as e:
            print(f"Could not log in: {e}")
            retVal = False
        return retVal

    def isAuthCurrent(self) -> bool:
        return self._client is not None

class SummitSiteAuthDriver(OrnlSiteAuthDriver):
    _host = 'summit.olcf.ornl.gov'
    _client = None # A singleton connection so we don't need to login every time we want to do something
    
class _OrnlDtnSiteAuthDriver(OrnlSiteAuthDriver):
    # DTN is the Data Transfer Node. It is used by ORNL collectively, but ONLY for file transfers.
    _host = 'dtn.olcf.ornl.gov'
    _client = None # A singleton connection so we don't need to login every time we want to do something

    


#***********************************************************************************************************************************

class OrnlSiteRunDriver(SiteRunDriver):
    authDriver = None
    machine = 'ornl'

    def _getSession(self, force=False):
        self.authDriver.login(force)
        return self.authDriver._client

    def submitJob(self, jdefn: JobDefn=None, parentContext: JobContext = None) -> JobStatus:
        # We can (should?) use the compute type from the JobDefn, but we should keep usage consistent with the other methods
        if self.machine is None:
            logging.error("No machine found. Please use one of the classes for a specific machine (eg SummitSiteRunDriver).")
            return False

        # Make sure we have a parent context
        if (parentContext is None):
            parentContext = JobContext()

        # Submit the job
        ssh = self._getSession()
        bsubFile = jdefn.getEntryPoint()
        try:
            ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(f"source /etc/profile; bsub {bsubFile}")
        except ConnectionResetError: # Our connection had to reset, so let's try to log in again
            ssh = self._getSession(force=True)
            ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(f"source /etc/profile; bsub {bsubFile}")
        except paramiko.ChannelException:
            ssh = self._getSession(force=True)
            ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(f"source /etc/profile; bsub {bsubFile}")

            
        ssh_stdout = ssh_stdout.read().decode() # Read from the stream and convert to a normal string
        ssh_stderr = ssh_stderr.read().decode()

        # Construct our status message
        jstatus = OrnlJobStatus(parentContext)
        if not ssh_stderr:
            jstatus.setNativeStatusStr("PEND")
            
            jobId = ssh_stdout[ssh_stdout.find('<')+1:ssh_stdout.find('>')]
            jstatus.getJobContext().setNativeId(jobId)
        else:
            jstatus.setStatus("EXIT")
            logging.error(f"Job {bsubFile} failed: {ssh_stderr}")
        jstatus.setEmitTime(datetime.utcnow())
        jstatus.getJobContext().setSiteName(self.machine)
        jstatus.emit()
        return jstatus

    def getJobStatus(self, jobContext: JobContext) -> JobStatus:
        if self.machine is None:
            logging.error("No machine found. Please use one of the classes for a specific machine (eg SummitSiteRunDriver).")
            return False

        # Check the status
        ssh = self._getSession()
        jobId = jobContext.getNativeId()
        try:
            ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("source /etc/profile; bjobs " + jobId + " | tail -n1 | awk '{print $3}'")
        except ConnectionResetError: # Our connection had to reset, so let's try to log in again
            ssh = self._getSession(force=True)
            ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("source /etc/profile; bjobs " + jobId + " | tail -n1 | awk '{print $3}'")
        except paramiko.ChannelException:
            ssh = self._getSession(force=True)
            ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("source /etc/profile; bjobs " + jobId + " | tail -n1 | awk '{print $3}'")
        status = ssh_stdout.read().decode().strip()

        # Construct our status message
        jstatus = OrnlJobStatus(jobContext)
        jstatus.setNativeStatusStr(status) # Cancelled jobs appear in the form "CANCELLED by user123", so make sure to just grab the beginning
        jstatus.getJobContext().setNativeId(jobId)
        jstatus.setEmitTime(datetime.utcnow())
        jstatus.getJobContext().setSiteName(self.machine)
        jstatus.emit()
        return jstatus

    def cancelJob(self, jobContext: JobContext) -> bool:
        raise NotImplementedError()


    def listComputeTypes(self) -> [str]:
        raise NotImplementedError()


    def setEventHandler(self, jobContext: JobContext, jobStatus: JobStatusValues, statusFilter: Callable,
                        newJobDefn: JobDefn, newJobContext: JobContext, newSiteName: str) -> JobEventHandler:
        raise NotImplementedError()


    def unsetEventHandler(self, jeh: JobEventHandler) -> bool:
        raise NotImplementedError()


    def listEventHandlers(self) -> [JobEventHandler]:
        raise NotImplementedError()

    def getJobList(self, startTime: int, endTime: int) -> [JobStatus]:
        raise NotImplementedError()


class SummitSiteRunDriver(OrnlSiteRunDriver):
    authDriver = SummitSiteAuthDriver()
    machine = 'summit'


#***********************************************************************************************************************************

class OrnlSiteRepoDriver(SiteRepoDriver):

    def _getSession(self):
        client = _OrnlDtnSiteAuthDriver().login()
        client = _OrnlDtnSiteAuthDriver()._client
        #scp = paramiko.SFTPClient.from_transport(client.get_transport())
        return client.open_sftp()

    def put(self, localRef: Path, siteRef: SiteFileRef, jobContext: JobContext = None) -> SiteFileRef:
        # Book keeping for status emissions
        iAmAJob = False
        if (jobContext is None):
            iAmAJob = True
            jobContext = JobContext()
        jobContext.setSiteName("ornl")
        jstatus = JobStatus(jobContext)
        if (iAmAJob):
            # emit the starting job status sequence
            #jstatus.emit(JobStatusValues.PENDING.value)
            #jstatus.emit(JobStatusValues.RUNNING.value)
            pass
        remotePath = siteRef.getPath()

        # Emit our info status before hitting the API
        jstatus.setNativeInfo(JobStatus.makeRepoInfo(RepoOp.PUT, False, str(localRef), str(remotePath)))
        #jstatus.emit(JobStatusValues.INFO.value)
        
        # Connect to the DTN, then do a put with paramiko
        try:
            sftp = self._getSession()
            sftp.put(str(localRef), remotePath)
        except Exception as e:
            logging.error(f"Error uploading file: {e}")
            if (iAmAJob):
                #jstatus.emit(JobStatusValues.FAILED.value)
                pass
            return False

        if (iAmAJob):
            # emit the successful job ending sequence
            #jstatus.emit(JobStatusValues.FINISHING.value)
            #jstatus.emit(JobStatusValues.COMPLETE.value)
            pass
        #MetaRepo.notate(siteRef)
        return siteRef

    def get(self, siteRef: SiteFileRef, localRef: Path, jobContext: JobContext = None) -> Path:
        # Book keeping for status emissions
        iAmAJob = False
        if (jobContext is None):
            iAmAJob = True
            jobContext = JobContext()
        jobContext.setSiteName("ornl")
        jstatus = JobStatus(jobContext)
        if (iAmAJob):
            # emit the starting job status sequence
            #jstatus.emit(JobStatusValues.PENDING.value)
            #jstatus.emit(JobStatusValues.RUNNING.value)
            pass
        remotePath = siteRef.getPath()

        # Emit our info status before hitting the API
        jstatus.setNativeInfo(JobStatus.makeRepoInfo(RepoOp.PUT, False, remotePath, str(localRef)))
        #jstatus.emit(JobStatusValues.INFO.value)

        # Connect to the DTN, then do a get with paramiko
        try:
            sftp = self._getSession()
            sftp.get(remotePath, str(localRef))
        except Exception as e:
            logging.error(f"Error downloading file: {e}")
            if (iAmAJob):
                jstatus.emit(JobStatusValues.FAILED.value)
            return False

        if (iAmAJob):
            # emit the successful job ending sequence
            #jstatus.emit(JobStatusValues.FINISHING.value)
            #jstatus.emit(JobStatusValues.COMPLETE.value)
            pass
        #MetaRepo.notate(siteRef)
        return localRef


    def find(self, siteRef: SiteFileRef) -> [SiteFileRef]:
        raise NotImplementedError()



#***********************************************************************************************************************************
