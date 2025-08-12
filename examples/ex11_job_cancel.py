"""
A long running local job to demonstrate job cancel.
"""

from lwfm.base.JobDefn import JobDefn
from lwfm.midware.LwfManager import lwfManager

if __name__ == "__main__":
    site = lwfManager.getSite("local")
    # Long-running command so we can exercise cancel()
    jobDefn = JobDefn("sleep 300")
    status = site.getRunDriver().submit(jobDefn)
    print(f"submitted the job {status}")
    print(f"cancelled the job {site.getRunDriver().cancel(status.getJobId())}")

    # Let's show that we can also get the result of the job later on
    status = lwfManager.getStatus(status.getJobId())
    print(f"job status from persistence {status}")
