
import io

from qiskit import QuantumCircuit, qpy
from lwfm.base.Site import Site
from lwfm.midware.Logger import Logger
from lwfm.base.JobDefn import JobDefn

from lwfm.sites.IBMQuantumSite import IBMQuantumSite

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
    backend = backends[0]   # use the first one, which is known to be least busy 

    # make a qiskit circuit 
    qubits = 3
    circuit = circuit_n_qubit_GHZ_state(qubits)

    # run it, which will transpile the circuit to the backend; 
    # pass in a QASM string and some site-specific runtime args; 
    # get back a provisional status right away, it will complete asynchronously
    jobStatus = site.getRun().submit(IBMQuantumSite.circuit_to_JobDefn(circuit), 
                                     None, backend, {"shots": 10})
    print(f"{jobStatus}")





