# print 'hello world' but as a Job on a (local) Site
# assumes the lwfm job status service is running

import logging

from lwfm.base.Site import Site
from lwfm.base.JobDefn import JobDefn

# This Site name can be an argument - name maps to a Site class implementation,
# either one provided with this sdk, or one user-authored.
siteName = "local"

if __name__ == '__main__':

    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    # one Site for this example - construct an interface to the Site
    site = Site.getSiteInstanceFactory(siteName)
    # a "local" Site login is generally a no-op
    site.getAuthDriver().login()

    # define the Job - use all Job defaults except the actual command to execute
    jobDefn = JobDefn()
    jobDefn.setEntryPoint("echo 'hello world'")

    # submit the Job to the Site
    status = site.getRunDriver().submitJob(jobDefn)
    # the run is generally asynchronous - on a remote HPC-type Site certainly,
    # and even in a local Site the "local" driver can implement async runs (which in fact it does),
    # so expect this Job status to be "pending"
    logging.info("hello world job " + status.getJobId() + " " + status.getStatusValue())

    # how could we tell the async job has finished? one way is to synchronously wait on its end status
    # (another way is asynchronous triggering, which we'll demonstrate in a separate example)
    status = status.wait()
    logging.info("hello world job " + status.getJobId() + " " + status.getStatusValue())
