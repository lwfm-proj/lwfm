# IBM Quantum cloud service Site driver for lwfm.

from typing import List
import os
import multiprocessing

from lwfm.base.Site import Site, SiteAuth, SiteRun
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus, JobStatusValues
from lwfm.base.JobContext import JobContext
from lwfm.midware.LwfManager import LwfManager
from lwfm.midware.Logger import Logger
from lwfm.midware.Store import AuthStore

from qiskit_ibm_runtime import QiskitRuntimeService

# *********************************************************************

# TODO make more flexible with configuration
SITE_NAME = "ibm_quantum"


# *********************************************************************


class IBMQuantumJobStatus(JobStatus):
    def __init__(self, context: JobContext = None):
        super(IBMQuantumJobStatus, self).__init__(context)
        # use default canonical status map
        self.getJobContext().setSiteName(SITE_NAME)


# **********************************************************************


class IBMQuantumSiteAuth(SiteAuth):
    def login(self, force: bool = False) -> bool:
        authStore = AuthStore()
        token_data = authStore.getAuthForSite(SITE_NAME)
        if token_data is None:
            return False
        QiskitRuntimeService.save_account(channel="ibm_quantum", 
            token=token_data, 
            overwrite=True,
            set_as_default=True)
        Logger.info("IBM Quantum login successful")
        return True


    def isAuthCurrent(self) -> bool:
        # implied to force another call to the above 
        return False


# ************************************************************************


class IBMQuantumSiteRun(SiteRun):

    def getStatus(self, jobId: str) -> JobStatus:
        return LwfManager.getStatus(jobId)

    def _runJob(self, jDefn: JobDefn, jobContext: JobContext) -> None:
        # Putting the job in a new thread means we can easily run it asynchronously
        # while still emitting statuses before and after
        # Emit RUNNING status
        LwfManager.emitStatus(jobContext, LocalJobStatus, 
                              JobStatusValues.RUNNING)
        try:
            # This is synchronous, so we wait here until the subprocess is over.
            # Check=True raises an exception on non-zero returns
            # run a command line job
            cmd = jDefn.getEntryPoint()
            if jDefn.getJobArgs() is not None:
                for arg in jDefn.getJobArgs():
                    cmd += " " + arg
            os.system(cmd)
            # Emit success statuses
            LwfManager.emitStatus(jobContext, LocalJobStatus, 
                                  JobStatusValues.FINISHING)
            LwfManager.emitStatus(jobContext, LocalJobStatus, 
                                  JobStatusValues.COMPLETE)
        except Exception as ex:
            Logger.error("ERROR: Job failed %s" % (ex))
            # Emit FAILED status
            LwfManager.emitStatus(jobContext, LocalJobStatus, 
                                  JobStatusValues.FAILED)


    def submit(self, jDefn: JobDefn, useContext: JobContext = None) -> JobStatus:
        if (useContext is None):
            useContext = JobContext()
            # we can test validity of the job defn here, reject it, or say its ready
            # if we were given a context, then we assume its ready 
            LwfManager.emitStatus(useContext, LocalJobStatus, 
                                  JobStatusValues.READY)
        # horse at the gate...
        LwfManager.emitStatus(useContext, LocalJobStatus, 
                              JobStatusValues.PENDING)
        # Run the job in a new thread so we can wrap it in a bit more code
        # this will kick the status the rest of the way to a terminal state 
        multiprocessing.Process(target=self._runJob, args=[jDefn, useContext]).start()
        return LwfManager.getStatus(useContext.getId())


    def cancel(self, jobContext: JobContext) -> bool:
        return False


    def listComputeTypes(self) -> List[str]:
        service = QiskitRuntimeService()
        backends = service.backends()
        l = list()
        for b in backends:
            l.append(b.name)
        return l



# *************************************************************************************

class IBMQuantumSite(Site):
    def __init__(self):
        super(IBMQuantumSite, self).__init__(
            SITE_NAME, IBMQuantumSiteAuth(), IBMQuantumSiteRun(), None, None
        )

