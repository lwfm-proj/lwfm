
# LocalSiteDriver: an implementation of Site and its constituent Auth, Run, Repo interfaces for a local to the user runtime
# environment.  Unsecure, as this is local and we assume the user is themselves already.

import logging

import os
import shutil
import multiprocessing
import time

from datetime import datetime
from pathlib import Path

from lwfm.base.Site import Site, SiteAuthDriver, SiteRunDriver, SiteRepoDriver
from lwfm.base.SiteFileRef import SiteFileRef, FSFileRef
from lwfm.base.JobDefn import JobDefn, RepoJobDefn, RepoOp
from lwfm.base.JobStatus import JobStatus, JobStatusValues, JobContext
from lwfm.server.JobStatusSentinelClient import JobStatusSentinelClient


#************************************************************************************************************************************

SITE_NAME = "local"

#************************************************************************************************************************************

class LocalJobStatus(JobStatus):
    def __init__(self, jcontext: JobContext = None):
        super(LocalJobStatus, self).__init__(jcontext)
        # use default canonical status map
        self.setSiteName(SITE_NAME)


#************************************************************************************************************************************

class LocalSiteAuthDriver(SiteAuthDriver):
    # Because this is running locally, we don't actually need any authentication
    def login(self, force: bool=False) -> bool:
        return True

    def isAuthCurrent(self) -> bool:
        return True


#***********************************************************************************************************************************


class LocalSiteRunDriver(SiteRunDriver):

    _pendingJobs = {}

    def _runJob(self, jdefn, jobStatus):
        # Putting the job in a new thread means we can easily run it asynchronously while still emitting statuses before and after
        #Emit RUNNING status
        jobStatus.emit(JobStatusValues.RUNNING.value)
        try:
            # This is synchronous, so we wait here until the subprocess is over. Check=True raises an exception on non-zero returns
            if (isinstance(jdefn, RepoJobDefn)):
                # run the repo job
                if (jdefn.getRepoOp() == RepoOp.PUT):
                    _repoDriver.put(jdefn.getLocalRef(), jdefn.getSiteRef(), jobStatus.getJobContext())
                elif (jdefn.getRepo() == RepoOp.GET):
                    _repoDriver.get(jdefn.getSiteRef(), jdefn.getLocalRef(), jobStatus.getJobContext())
                else:
                    logging.error("Unknown repo operation")
            else:
                # run a command line job
                cmd = jdefn.getEntryPoint()
                if (jdefn.getJobArgs() is not None):
                    for arg in jdefn.getJobArgs():
                        cmd += " " + arg
                os.system(cmd)
            #Emit success statuses
            jobStatus.emit(JobStatusValues.FINISHING.value)
            jobStatus.emit(JobStatusValues.COMPLETE.value)
        except Exception as ex:
            logging.error("ERROR: Job failed %s" % (ex))
            #Emit FAILED status
            jobStatus.emit(JobStatusValues.FAILED.value)

    def submitJob(self, jdefn: JobDefn, parentContext: JobContext = None) -> JobStatus:
        if (parentContext is None):
            parentContext = JobContext()
        # In local jobs, we spawn the job in a new child process
        jstatus = LocalJobStatus(parentContext)

        # Let the sentinel know the job is ready
        jstatus.emit(JobStatusValues.PENDING.value)

        # Run the job in a new thread so we can wrap it in a bit more code
        thread = multiprocessing.Process(target=self._runJob, args=[jdefn, jstatus])
        thread.start()
        self._pendingJobs[jstatus.getId()] = thread

        return jstatus

    def getJobStatus(self, jobContext: JobContext) -> JobStatus:
        status = JobStatus.deserialize(JobStatusSentinelClient().getStatusBlob(jobContext.getId()))
        return status

    def cancelJob(self, jobContext: JobContext) -> bool:
        # Find the locally running thread and kill it
        try:
            thread = self._pendingJobs[jobContext.getId()]
            if (thread is None):
                return False
            logging.info("LocalSiteDriver.cancelJob(): calling terminate on job " + jobContext.getId())
            thread.terminate()
            jstatus = LocalJobStatus(jobContext)
            jstatus.emit(JobStatusValues.CANCELLED.value)
            self._pendingJobs[jobContext.getId()] = None
            return True
        except Exception as ex:
            logging.error("ERROR: Could not cancel job %d: %s" % (jobContext.getId(), ex))
            return False


#***********************************************************************************************************************************

class LocalSiteRepoDriver(SiteRepoDriver):

    def _copyFile(self, fromPath, toPath, jobContext):
        iAmAJob = False
        if (jobContext is None):
            iAmAJob = True
            jobContext = JobContext()

        jstatus = JobStatus(jobContext)
        if (iAmAJob):
            # emit the starting job status sequence
            jstatus.emit(JobStatusValues.PENDING.value)
            jstatus.emit(JobStatusValues.RUNNING.value)

        jstatus.setNativeInfo(JobStatus.makeRepoInfo(RepoOp.PUT, False, str(fromPath), str(toPath)))
        jstatus.emit(JobStatusValues.INFO.value)

        try:
            shutil.copy(fromPath, toPath)
        except Exception as ex:
            logging.error("Error copying file: " + str(ex))
            if iAmAJob:
                jstatus.emit(JobStatusValues.FAILED.value)
            return False

        jstatus.emit(JobStatusValues.FINISHING.value)
        jstatus.emit(JobStatusValues.COMPLETE.value)
        return True

    # If we're given a context, we use it, if not, we consider ourselves our own job.
    def put(self, localRef: Path, siteRef: SiteFileRef, jobContext: JobContext = None) -> SiteFileRef:
        fromPath = localRef
        toPath = siteRef.getPath()
        if not self._copyFile(fromPath, toPath, jobContext):
           return False

        # return success result
        return FSFileRef.siteFileRefFromPath(toPath + "/" + fromPath.name)

    def get(self, siteRef: SiteFileRef, localRef: Path, jobContext: JobContext = None) -> Path:
        fromPath = siteRef.getPath()
        toPath = localRef
        if not self._copyFile(fromPath, toPath, jobContext):
           return False

        # return success result
        return Path(str(toPath) + "/" + Path(fromPath).name)


    def ls(self, siteRef: SiteFileRef) -> SiteFileRef:
        return FSFileRef.siteFileRefFromPath(siteRef.getPath())



#************************************************************************************************************************************

_repoDriver = LocalSiteRepoDriver()

class LocalSite(Site):
    # There are no required args to instantiate a local site.
    def __init__(self):
        super(LocalSite, self).__init__(SITE_NAME, LocalSiteAuthDriver(), LocalSiteRunDriver(), _repoDriver, None)



#************************************************************************************************************************************

# test
if __name__ == '__main__':
    # assumes the lwfm job status service is running
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    logging.info("***** login test")

    # on local sites, login is a no-op
    site = Site.getSiteInstanceFactory("local")
    logging.info("site is " + site.getName())
    site.getAuthDriver().login()
    logging.info("auth is current = " + str(site.getAuthDriver().isAuthCurrent()))

    logging.info("***** local job test")

    # run a local 'pwd' as a job
    jdefn = JobDefn()
    jdefn.setEntryPoint("echo")
    jdefn.setJobArgs([ "pwd = `pwd`" ])
    status = site.getRunDriver().submitJob(jdefn)
    logging.info("pwd job id = " + status.getId())
    logging.info("pwd job status = " + str(status.getStatus()))   # initial status will be pending - its async

    logging.info("***** repo tests")

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

    # put - run as a brand new job (note: this script itself is *not* a job, its just a script, so the job we
    # run here is a seminal job
    localFile = os.path.realpath(__file__)
    destFileRef = FSFileRef.siteFileRefFromPath(os.path.expanduser('~'))
    copiedFileRef = site.getRepoDriver().put(Path(localFile), destFileRef)
    logging.info(copiedFileRef.getName() + " " + str(copiedFileRef.getTimestamp()))

    # get - run as a brand new job, but this time, pre-generate the job context
    fileRef = FSFileRef.siteFileRefFromPath(os.path.realpath(__file__))
    destPath = Path(os.path.expanduser('~'))
    copiedPath = site.getRepoDriver().get(fileRef, destPath, JobContext())
    logging.info("get test: copied to: " + str(copiedPath))

    logging.info("***** check status of async job")

    # the above job was async... check its status
    while (True):
        status = site.getRunDriver().getJobStatus(status.getJobContext())
        if (status.isTerminal()):             # should have a terminal status by now...
            logging.info("pwd job status = " + str(status.getStatus()))
            break

    logging.info("***** cancel job")

    # cancel a job
    jdefn = JobDefn()
    jdefn.setEntryPoint("sleep 100")
    status = site.getRunDriver().submitJob(jdefn)
    logging.info("sleep job id = " + status.getId())
    logging.info("sleep job status = " + str(status.getStatus()))   # initial status will be pending - its async
    # wait a little bit for the job to actually start and emit a running status, then we'll cancel it
    time.sleep(10)
    site.getRunDriver().cancelJob(status.getJobContext())
    while (True):
        status = site.getRunDriver().getJobStatus(status.getJobContext())
        if (status.isTerminal()):             # should have a terminal status by now...
            logging.info("sleep job status = " + str(status.getStatus()))
            break

    logging.info("testing done")
