# IBM Quantum cloud service Site driver for lwfm.

from enum import Enum
from typing import List
import io

from lwfm.base.Site import Site, SiteAuth, SiteRun, SiteRepo, SiteSpin
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus, JobStatusValues
from lwfm.base.JobContext import JobContext
from lwfm.base.WfEvent import RemoteJobEvent
from lwfm.midware.LwfManager import LwfManager
from lwfm.midware.Logger import Logger
from lwfm.midware.impl.Store import AuthStore
from qiskit import QuantumCircuit, qpy
from qiskit_ibm_runtime import SamplerV2 as Sampler, QiskitRuntimeService
from qiskit.transpiler.preset_passmanagers import generate_preset_pass_manager

# *********************************************************************

SITE_NAME = "ibm_quantum"

# the job status codes for IBM Quantum site - see constructor below for mapping
# to lwfm canonical status strings
class IBMQuantumJobStatusValues(Enum):
    INITIALIZING    = "INITIALIZING"
    QUEUED          = "QUEUED"
    VALIDATING      = "VALIDATING"
    RUNNING         = "RUNNING"
    CANCELLED       = "CANCELLED"
    DONE            = "DONE"
    ERROR           = "ERROR"
    INFO            = "INFO"


class IBMQuantumJobStatus(JobStatus):
    def __init__(self, jobContext: JobContext = None):
        super(IBMQuantumJobStatus, self).__init__(jobContext)
        # override the default status mapping for the specifics of this site
        self.setStatusMap({
            IBMQuantumJobStatusValues.INITIALIZING.value  : JobStatusValues.READY    ,
            IBMQuantumJobStatusValues.QUEUED.value        : JobStatusValues.PENDING  ,
            IBMQuantumJobStatusValues.VALIDATING.value    : JobStatusValues.PENDING  ,
            IBMQuantumJobStatusValues.RUNNING.value       : JobStatusValues.RUNNING  ,
            IBMQuantumJobStatusValues.CANCELLED.value     : JobStatusValues.CANCELLED,
            # nothing in IBM maps to lwfm FINISHING
            IBMQuantumJobStatusValues.DONE.value          : JobStatusValues.COMPLETE ,
            IBMQuantumJobStatusValues.ERROR.value         : JobStatusValues.FAILED   ,
            IBMQuantumJobStatusValues.INFO.value          : JobStatusValues.INFO     ,
            })
        self.getJobContext().setSiteName(SITE_NAME)


# **********************************************************************
# Auth - login to IBM Quantum cloud service

class IBMQuantumSiteAuth(SiteAuth):
    def login(self, force: bool = False) -> bool:
        try:
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
        except Exception as e:
            Logger.error("IBM Quantum login failed: " + str(e))
            return False


    def isAuthCurrent(self) -> bool:
        # implied to force another call to the above 
        return False


# ************************************************************************
# Run - submit a circuit, get job status

class IBMQuantumSiteRun(SiteRun):

    def getStatus(self, jobId: str) -> JobStatus:
        status = LwfManager.getStatus(jobId)
        if (status is not None and status.isTerminal()):
            return status
        # its not terminal yet, so poke the remote site 
        service = QiskitRuntimeService()
        job = service.job(status.getJobContext().getNativeId())
        status = IBMQuantumJobStatus(status.getJobContext())
        status.setNativeStatus(IBMQuantumJobStatusValues(job.status()))
        LwfManager.emitStatus(status)
        return status


    def submit(self, jDefn: JobDefn, useContext: JobContext = None,
               computeType: str = None, runArgs: dict = None) -> JobStatus:
        if (useContext is None):
            useContext = JobContext()
        
        try: 
            # transpile the circuit to match the backend
            service = QiskitRuntimeService()
            backend = service.backend(computeType)
            pm = generate_preset_pass_manager(backend=backend, optimization_level=1)
            isa_circuit = pm.run(qpy.load(io.BytesIO(jDefn.getEntryPoint())))
            # run the circuit
            sampler = Sampler(mode=backend)
            if runArgs is not None and "shots" in runArgs:
                shots = runArgs["shots"]
            else:
                shots = 10
            job = sampler.run([isa_circuit], shots=shots)
            Logger.info("running ibm quantum job id: " + job.job_id())

            useContext.setNativeId(job.job_id())
            useContext.setSiteName(SITE_NAME)
            useContext.setComputeType(computeType)
            
            print(useContext)

            # now that we have the native job id we can emit status 
            LwfManager.emitStatus(useContext, IBMQuantumJobStatus, 
                                  IBMQuantumJobStatusValues.INITIALIZING)
            # horse at the gate...
            LwfManager.emitStatus(useContext, IBMQuantumJobStatus, 
                                IBMQuantumJobStatusValues.QUEUED)
            
            # set an event handler to poll the remote job status
            LwfManager.setEvent(RemoteJobEvent(useContext))

            # capture current job info & return 
            return LwfManager.getStatus(useContext.getId())
        except Exception as ex:
            Logger.error("IBMQuantumSiteRun.submit error: " + str(ex))
            LwfManager.emitStatus(useContext, IBMQuantumJobStatus, 
                IBMQuantumJobStatusValues.ERROR, str(ex))
            return None



    def cancel(self, jobContext: JobContext) -> bool:
        # TODO implement 
        return False


# ***************************************************************************
# Repo   


# ***************************************************************************
# Spin - show the computing resource types available on this site 

class IBMQuantumSiteSpin(SiteSpin):

    def listComputeTypes(self) -> List[str]:
        service = QiskitRuntimeService()
        leastBackend = service.least_busy(simulator=False, operational=True)
        backends = service.backends()
        l = list()
        l.append(leastBackend.name)
        l.append("ibmq_qasm_simulator")
        for b in backends:
            if not b.name == leastBackend.name:
                l.append(b.name)        
        return l


# ***************************************************************************
# the site, with its constituent site pillar (auth, run, repo, spin) interfaces
# and additional site-specific methods

class IBMQuantumSite(Site):
    def __init__(self):
        super(IBMQuantumSite, self).__init__(
            SITE_NAME, IBMQuantumSiteAuth(), IBMQuantumSiteRun(), 
            None, IBMQuantumSiteSpin()
        )

    @staticmethod
    def circuit_to_JobDefn(circuit: QuantumCircuit) -> JobDefn:
        cFile = io.BytesIO()
        qpy.dump(circuit, cFile)
        return JobDefn(cFile.getvalue())
    


