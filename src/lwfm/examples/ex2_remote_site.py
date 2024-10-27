
from qiskit import QuantumCircuit
from qiskit_ibm_runtime import SamplerV2 as Sampler, QiskitRuntimeService
from lwfm.base.Site import Site
from lwfm.midware.Logger import Logger
from qiskit.transpiler.preset_passmanagers import generate_preset_pass_manager

def circuit_n_qubit_GHZ_state(n: int) -> QuantumCircuit:
    if isinstance(n, int) and n >= 2:
        qc = QuantumCircuit(n)
        qc.h(0)
        for i in range(n-1):
            qc.cx(i, i+1)
    else:
        raise Exception("n is not a valid input")
    return qc


if __name__ == "__main__":
    # load the site driver for the ibm cloud site
    site = Site.getSite("ibm_quantum")

    # we previously saved our token in the AuthStore - this send to the cloud
    if (site.getAuth().isAuthCurrent() == False):
        # fresh login 
        site.getAuth().login()

    # list the compute types supported by the site - names of quantum backends
    backends = site.getRun().listComputeTypes()
    Logger.info("list of IBM quantum compute types: " + str(backends))

    qubits = 3
    circuit = circuit_n_qubit_GHZ_state(qubits)
    circuit.measure_all()



    service = QiskitRuntimeService()
    backend = service.least_busy(simulator=False, operational=True)
    Logger.info("Least busy backend: " + backend.name)

    # rewrite the circuit to match the backend
    pm = generate_preset_pass_manager(backend=backend, optimization_level=1)
    isa_circuit = pm.run(circuit)

    sampler = Sampler(mode=backend)
    job = sampler.run([isa_circuit], shots=10)
    print(job.result())






