# IBM Quantum cloud service Site driver for lwfm.

from enum import Enum
from typing import List
import io

from lwfm.base.Site import Site, SiteAuth, SiteRun, SiteRepo, SiteSpin
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus, JobStatusValues
from lwfm.base.JobContext import JobContext
from lwfm.base.WfEvent import RemoteJobPoller
from lwfm.midware.LwfManager import LwfManager
from lwfm.midware.Logger import Logger
from lwfm.midware.Store import AuthStore
from qiskit import QuantumCircuit, qpy
from qiskit_ibm_runtime import SamplerV2 as Sampler, QiskitRuntimeService
from qiskit.transpiler.preset_passmanagers import generate_preset_pass_manager

# *********************************************************************

# TODO make more flexible with configuration
SITE_NAME = "ibm_quantum"

class IBMQuantumJobStatusValues(Enum):
    INITIALIZING = "INITIALIZING"
    QUEUED = "QUEUED"
    VALIDATING = "VALIDATING"
    #FINISHING = "FINISHING"
    RUNNING = "RUNNING"
    CANCELLED = "CANCELLED"
    DONE = "DONE"
    ERROR = "ERROR"
    INFO = "INFO"


class IBMQuantumJobStatus(JobStatus):
    def __init__(self, context: JobContext = None):
        super(IBMQuantumJobStatus, self).__init__(context)
        self.getJobContext().setSiteName(SITE_NAME)
        # override the default status mapping
        self.setStatusMap({
            IBMQuantumJobStatusValues.INITIALIZING.value  : JobStatusValues.READY    ,
            IBMQuantumJobStatusValues.QUEUED.value        : JobStatusValues.PENDING  ,
            IBMQuantumJobStatusValues.RUNNING.value       : JobStatusValues.RUNNING  ,
            IBMQuantumJobStatusValues.CANCELLED.value     : JobStatusValues.CANCELLED,
            # nothing in IBM maps to lwfm FINISHING
            IBMQuantumJobStatusValues.DONE.value          : JobStatusValues.COMPLETE ,
            IBMQuantumJobStatusValues.ERROR.value         : JobStatusValues.FAILED   ,
            IBMQuantumJobStatusValues.INFO.value          : JobStatusValues.INFO     ,
            })


# **********************************************************************


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


class IBMQuantumSiteRun(SiteRun):

    def getStatus(self, jobId: str) -> JobStatus:
        return LwfManager.getStatus(jobId)


    def submit(self, jDefn: JobDefn, useContext: JobContext = None,
               computeType: str = None, runArgs: dict = None) -> JobStatus:
        if (useContext is None):
            useContext = JobContext()
            LwfManager.emitStatus(useContext, IBMQuantumJobStatus, 
                                  IBMQuantumJobStatusValues.INITIALIZING)
        
        try: 
            # rewrite the circuit to match the backend
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
        
            # horse at the gate...
            LwfManager.emitStatus(useContext, IBMQuantumJobStatus, 
                                IBMQuantumJobStatusValues.QUEUED)
            
            # set an event handler to poll the remote job status 
            LwfManager.setEvent(RemoteJobPoller(useContext))
        except Exception as ex:
            Logger.error("IBMQuantumSiteRun.submit error: " + str(ex))
            LwfManager.emitStatus(useContext, IBMQuantumJobStatus, 
                IBMQuantumJobStatusValues.ERROR, str(ex))

        # capture current job info & return 
        return LwfManager.getStatus(useContext.getId())


    def cancel(self, jobContext: JobContext) -> bool:
        return False


# ***************************************************************************

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
    


