
from qiskit_ibm_runtime import QiskitRuntimeService
from lwfm.base.Site import Site

if __name__ == "__main__":
    # load the site driver for the ibm cloud site
    site = Site.getSite("ibm_quantum")

    # we previously saved our token in the AuthStore - this send to the cloud
    site.getAuth().login()

    service = QiskitRuntimeService()
    job = service.job('cwf6h449r49g0085sjmg')
    job_result = job.result()
    print(job_result)
    pub_result = job_result[0]
    print(f" >> Meas output register counts: {pub_result.data.meas.get_counts()}")



