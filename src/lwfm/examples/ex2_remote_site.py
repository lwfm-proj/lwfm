
from qiskit import QuantumCircuit
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
    site.getAuth().login()

    # list the compute types supported by the site - names of quantum backends
    backends = site.getRun().listComputeTypes()
    Logger.info("list of IBM quantum compute types: " + str(backends))

    # we'll pick the first one at random and send a circuit to it; 
    # this will be a generic circuit, not one for a specific backend
    qubits = 5
    qc = circuit_n_qubit_GHZ_state(qubits)

    

    pm = generate_preset_pass_manager(optimization_level=1)
    isa_circuit = pm.run(qc)
    isa_operators_list = [op.apply_layout(isa_circuit.layout) for op in operators]

    name = backends[0]
    Logger.info("sending circuit to compute type: " + name)
    site.getRun().sendCircuit(qc, name, [str(i) for i in range(qubits)])

