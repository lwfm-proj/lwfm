"""
User: write another example, this time chain three "hello world" jobs together 
asynchronously
"""

import logging
from lwfm.base.Site import Site
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatusValues
from lwfm.base.WorkflowEventTrigger import JobEventTrigger

# Set up logging
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

# Define the Site
siteName = "local"
site = Site.getSiteInstanceFactory(siteName)
site.getAuthDriver().login()

def submit_job_and_create_trigger(jobDefn, precedingJobStatus=None):
    # Submit the Job to the Site
    if (precedingJobStatus is None):
        jobStatus = site.getRunDriver().submitJob(jobDefn)
        logging.info(f"Job {jobStatus.getJobId()} ('hello world') {jobStatus.getStatusValue()}")

    if precedingJobStatus:
        # Set up a WorkflowEventTrigger to submit this job when the preceding job completes
        jobEventTrigger = JobEventTrigger(
            jobId=precedingJobStatus.getJobId(),
            jobStatus=JobStatusValues.COMPLETE.value,
            fireDefn=jobDefn,
            targetSiteName=siteName
        )
        # Set the WorkflowEventTrigger on the site
        jobStatus = site.getRunDriver().setWorkflowEventTrigger(jobEventTrigger)

    return jobStatus

# Define and submit the first Job
firstJobDefn = JobDefn()
firstJobDefn.setEntryPoint("echo 'hello world 1'")
firstJobStatus = submit_job_and_create_trigger(firstJobDefn)

# Define and submit the second Job, triggered by the completion of the first job
secondJobDefn = JobDefn()
secondJobDefn.setEntryPoint("echo 'hello world 2'")
secondJobStatus = submit_job_and_create_trigger(secondJobDefn, firstJobStatus)

# Define and submit the third Job, triggered by the completion of the second job
thirdJobDefn = JobDefn()
thirdJobDefn.setEntryPoint("echo 'hello world 3'")
thirdJobStatus = submit_job_and_create_trigger(thirdJobDefn, secondJobStatus)

logging.info(f"Jobs are chained. Job {secondJobStatus.getJobId()} will start after Job {firstJobStatus.getJobId()} completes. Job {thirdJobStatus.getJobId()} will start after Job {secondJobStatus.getJobId()} completes.")