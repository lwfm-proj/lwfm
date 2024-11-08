

# an lwfm example of integrating a remote site, in this case, 
# ibm quantum cloud 

# see lwfm.util for putAuth.py which can aid in injecting credentials for remote 
# sites, in this case, ibm quantum provides a token

# we're going to write a circuit, then transpile it for a specific quantum 
# backend, then run it asynchronously

# the lwfm service will monitor its progress and update the job status, including
# fetching the results at the end of the run 

from qiskit import QuantumCircuit
from lwfm.base.Site import Site
from lwfm.base.WfEvent import JobEvent 
from lwfm.base.JobDefn import JobDefn
from lwfm.midware.Logger import Logger
from lwfm.midware.LwfManager import LwfManager
from lwfm.base.JobStatus import JobStatusValues

from lwfm.sites.IBMQuantumSite import IBMQuantumSite

qubits = 3

# entangle n qubits
def circuit_n_qubit_GHZ_state(n: int) -> QuantumCircuit:
    if isinstance(n, int) and n >= 2:
        qc = QuantumCircuit(n)
        qc.h(0)
        for i in range(n-1):
            qc.cx(i, i+1)
        qc.measure_all()
    else:
        raise Exception("n is not a valid input")
    return qc


if __name__ == "__main__":
    # load the site driver for the ibm cloud site
    site = Site.getSite("ibm_quantum")

    # login, if needed
    if (site.getAuth().isAuthCurrent() == False):
        site.getAuth().login()  # fresh login

    # list the compute types supported by the site - names of quantum backends
    backends = site.getSpin().listComputeTypes()
    Logger.info("list of IBM quantum compute types: " + str(backends))

    # make a qiskit circuit 
    circuit = circuit_n_qubit_GHZ_state(qubits)

    # let's run it synchronously on a simulator compute type first
    backend = "FakeManilaV2"
    jobStatus = site.getRun().submit(IBMQuantumSite.circuit_to_JobDefn(circuit), 
                                     None, backend, {"shots": 10})
    jobStatus = LwfManager.wait(jobStatus.getJobId())

    if (not jobStatus.isTerminalSuccess()):
        Logger.error(f"simulator job failed: {jobStatus}")
        exit(1)
    else:
        Logger.info("simulator job results: " + jobStatus.getNativeInfo())
    
    # simulator job is complete, now run it on a real quantum device
    # use the first one, which is known to be least busy 
    backend = backends[0]   

    # run it, which will first transpile the circuit to the backend; 
    # pass in a QASM string and some site-specific runtime args; 
    # get back a provisional status right away, it will complete asynchronously
    jobStatus = site.getRun().submit(IBMQuantumSite.circuit_to_JobDefn(circuit), 
                                     None, backend, {"shots": 10})
    Logger.info(f"{jobStatus}")   # should be in a pending state waiting on a free QPU 

    # set a job to run when the previous job completes to mine the results
    # the below jobId being all a script needs to find all results associated with it
    resultJobId = LwfManager.setEvent(
        JobEvent(jobStatus.getJobId(), JobStatusValues.COMPLETE.value, 
            JobDefn(f"echo 'IBM job complete {jobStatus.getJobId()} {jobStatus.getJobContext().getNativeId()}'"), "local")
    )


