
# LocalSiteDriver: an implementation of Site and its constituent Auth, Run, Repo interfaces for a local to the user runtime
# environment.  Unsecure, as this is local and we assume the user is themselves already.

import logging

import os
import shutil
import subprocess

from datetime import datetime
from pathlib import Path

from lwfm.base.Site import Site, SiteAuthDriver, SiteRunDriver, SiteRepoDriver
from lwfm.base.SiteFileRef import SiteFileRef, FSFileRef
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus, JobStatusValues, JobContext


#************************************************************************************************************************************

class LocalSite(Site):
    def __init__(self):
        super(LocalSite, self).__init__("local", LocalSiteAuthDriver(), LocalSiteRunDriver(), LocalSiteRepoDriver(), None)


#************************************************************************************************************************************

class LocalJobStatus(JobStatus):
    def __init__(self, jdefn: JobDefn = JobDefn()):
        super(LocalJobStatus, self).__init__(jdefn)
        # use default canonical status map


#************************************************************************************************************************************

class LocalSiteAuthDriver(SiteAuthDriver):
    # Because this is running locally, we don't actually need any authentication
    def login(self, force: bool=False) -> bool:
        return True

    def isAuthCurrent(self) -> bool:
        return True

    def writeToStore(self) -> bool:
        return True

    def readFromStore(self) -> bool:
        return True

#***********************************************************************************************************************************
# TODO: provide mechanisms for threading

class LocalSiteRunDriver(SiteRunDriver):
    def submitJob(self, jdefn: JobDefn=None) -> JobStatus:
        # In local jobs, we spawn the job in a new child process
        jstatus = JobStatus(jdefn)
        # Construct our status message and bail out
        jstatus.setNativeStatusStr(JobStatusValues.PENDING.value)
        jstatus.setEmitTime(datetime.utcnow())
        process = jdefn.getEntryPointPath().split()
        JobStatus.emitStatus(jstatus.getId(), JobStatusValues.PENDING.value)
        #Emit RUNNING status
        JobStatus.emitStatus(jstatus.getId(), JobStatusValues.RUNNING.value)
        try:
            subprocess.run(process, check=True) # This is synchronous, so we wait here until the subprocess is over. Check=True raises an exception on non-zero return values
            #Emit FINISHING status
            JobStatus.emitStatus(jstatus.getId(), JobStatusValues.FINISHING.value)
            #Emit COMPLETE status
            JobStatus.emitStatus(jstatus.getId(), JobStatusValues.COMPLETE.value)
            jstatus.setNativeStatusStr(JobStatusValues.COMPLETE.value)
        except Exception as ex:
            logging.error("ERROR: Job failed %s" % (ex))
            #Emit FAILED status
            JobStatus.emitStatus(jstatus.getId(), JobStatusValues.FAILED.value)
            jstatus.setNativeStatusStr(JobStatusValues.FAILED.value)
        return jstatus

    def getJobStatus(self, nativeJobId: str) -> JobStatus:
        return JobStatus()    # TODO: there is no async local, so getStatus is not useful, status returned will be "unknown"

    def cancelJob(self, nativeJobId: str) -> bool:
        try:
            os.kill()
        except Exception as ex:
            logging.error("ERROR: Could not cancel job %d: %s" % (nativeJobId, ex))
        return True

#***********************************************************************************************************************************

class LocalSiteRepoDriver(SiteRepoDriver):

    def _copyFile(self, fromPath, toPath):
        try:
            shutil.copy(fromPath, toPath)
        except Exception as ex:
            logging.error("Error copying file: " + str(ex))
            return False
        return True

    def put(self, localRef: Path, siteRef: SiteFileRef, jobContext: JobContext = None) -> SiteFileRef:
        fromPath = localRef
        toPath = siteRef.getPath()
        # TODO: if jobContext is not None, emit job status
        if not self._copyFile(fromPath, toPath):
            return False
        return FSFileRef.siteFileRefFromPath(toPath + "/" + fromPath.name)

    def get(self, siteRef: SiteFileRef, localRef: Path, jobContext: JobContext = None) -> Path:
        fromPath = siteRef.getPath()
        toPath = localRef
        # TODO: if jobContext is not None, emit job status
        if not self._copyFile(fromPath, toPath):
            return False
        return Path(str(toPath) + "/" + Path(fromPath).name)

    def ls(self, siteRef: SiteFileRef) -> SiteFileRef:
        return FSFileRef.siteFileRefFromPath(siteRef.getPath())


#************************************************************************************************************************************

# test
if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    site = Site.getSiteInstanceFactory("local")
    logging.info(site.getName())
    site.getAuthDriver().login()
    logging.info(site.getAuthDriver().isAuthCurrent())

    jdefn = JobDefn()
    jdefn.setEntryPointPath("pwd")
    status = site.getRunDriver().submitJob(jdefn)
    logging.info("pwd job id = " + status.getId())
    logging.info("pwd parent job id = " + str(status.getParentJobId()))

    # ls a file
    fileRef = FSFileRef()
    fileRef.setPath(os.path.realpath(__file__))
    logging.info("name of the file is " + site.getRepoDriver().ls(fileRef).getName())
    logging.info("size of the file is " + str(site.getRepoDriver().ls(fileRef).getSize()))

    # ls a directory
    fileRef.setPath(os.path.expanduser('~'))
    fileRef = site.getRepoDriver().ls(fileRef)
    logging.info("size of the dir is " + str(fileRef.getSize()))
    logging.info("time of the dir is " + str(fileRef.getTimestamp()))
    logging.info("contents of the dir is " + str(fileRef.getDirContents()))

    # put
    localFile = os.path.realpath(__file__)
    destFileRef = FSFileRef.siteFileRefFromPath(os.path.expanduser('~'))
    copiedFileRef = site.getRepoDriver().put(Path(localFile), destFileRef)
    logging.info(copiedFileRef.getName() + " " + str(copiedFileRef.getTimestamp()))

    # get
    fileRef = FSFileRef.siteFileRefFromPath(os.path.realpath(__file__))
    destPath = Path(os.path.expanduser('~'))
    copiedPath = site.getRepoDriver().get(fileRef, destPath)
    logging.info(copiedPath)
