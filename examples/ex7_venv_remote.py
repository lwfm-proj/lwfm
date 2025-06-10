"""
Test interactions with a remote IBM Quantum Cloud machine. In this test, 
our local python environment doesn't have the IBM libraries necessary to run 
the test - we will launch the test inside a venv.
"""

#pylint: disable=invalid-name

import sys

from lwfm.base.JobDefn import JobDefn
from lwfm.base.WorkflowEvent import JobEvent
from lwfm.base.JobStatus import JobStatus
from lwfm.midware.LwfManager import lwfManager, logger

quantum_circuit = """
qc = QuantumCircuit(2)
qc.h(0)
qc.cx(0, 1)
qc.measure_all()
"""

lwfm_site_name = "ibm-quantum-venv"   # the site driver code is in a venv

if __name__ == "__main__":
    # get the site auth/run/repo/spin drivers for IBM Quantum Cloud
    site = lwfManager.getSite(lwfm_site_name)

    # list the compute types (quantum machines) at this site
    qcTypes = site.getSpinDriver().listComputeTypes()
    if not qcTypes:
        logger.error("No quantum machines found - bad login credentials?")
        sys.exit(1)
    qcBackend = "ibm_brisbane"   # currently preferred machine
    if qcBackend not in qcTypes:
        qcBackend = qcTypes[0]
    logger.info(f"Using IBM backend {qcBackend}")

    # to make the point about venvs, let's assume the current one doesn't have
    # any quantum libs, and that qiskit sits in a separate site-specific venv,
    # so we'll pass in this circuit as-is; the run driver could accept many formats
    job_defn = JobDefn(quantum_circuit)
    job_status = site.getRunDriver().submit(job_defn, None, qcBackend,
        {"shots": 1024, "format": "qiskit"})
    logger.info(f"lwfm job {job_status.getJobId()} " + \
        f"is IBM job {job_status.getJobContext().getNativeId()} " + \
        f"initial status: {job_status.getStatus()}")

    # # we don't want to wait synchronously - its a cloud resource
    # # so set an event handler for the remote job completion to fetch the results
    # statusB = lwfManager.setEvent(
    #     JobEvent(job_status.getJobId(),                         # when this job
    #             JobStatus.COMPLETE,                             # hits this state
    #             JobDefn("repo.get", JobDefn.ENTRY_TYPE_SITE,    # run this repo.get
    #                 [job_status.getJobId(),                     # remote data source
    #                 "~/.lwfm/out/" + job_status.getJobId() + \
    #                 ".out"]),                                   # local data dest 
    #             lwfm_site_name)                                 # with this site driver
    # )
    # logger.info(f"result extraction set - see job log {statusB.getJobId()}.log")


#**********************************************************************************
