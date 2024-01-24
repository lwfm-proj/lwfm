"""
User: write an example of running N number of "hello world" jobs, then running another 
"hello world" job asynchronously after the entire first set of jobs completes

This code needed modification in two places from the raw askthecode output.
- the block starting at line 57 was an extra unneeded trigger
- as a result of the above, line 66 needed indentation
"""

import logging
from lwfm.base.Site import Site
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatusValues
from lwfm.base.WorkflowEventTrigger import JobEventTrigger, JobSetEventTrigger

# Set up logging
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

# Define the Site
siteName = "local"
site = Site.getSiteInstanceFactory(siteName)
site.getAuthDriver().login()

def submit_job(jobDefn, jobNum):
    # Submit the Job to the Site
    jobStatus = site.getRunDriver().submitJob(jobDefn)
    logging.info(f"Job {jobNum} (ID: {jobStatus.getJobId()}) ('hello world') {jobStatus.getStatusValue()}")
    return jobStatus

# Define the number of jobs in the first set
N = 5

# Submit the first set of N "hello world" jobs
firstSetJobStatuses = []
for i in range(N):
    jobDefn = JobDefn()
    jobDefn.setEntryPoint(f"echo 'hello world {i+1}'")
    jobStatus = submit_job(jobDefn, i+1)
    firstSetJobStatuses.append(jobStatus)

# Define the job that runs after the first set completes
finalJobDefn = JobDefn()
finalJobDefn.setEntryPoint("echo 'hello world - final job'")

# Create a unique identifier for the set of jobs
jobSetId = f"set-{firstSetJobStatuses[0].getJobId()}"

# Set up a JobSetEventTrigger to submit the final job when all jobs in the first set complete
jobSetEventTrigger = JobSetEventTrigger(
    jobSetId=jobSetId,
    triggerFilter=JobStatusValues.COMPLETE.value,
    fireDefn=finalJobDefn,
    targetSiteName=siteName
)

# Add each job in the first set to the trigger
#for jobStatus in firstSetJobStatuses:
#    jobEventTrigger = JobEventTrigger(
#        jobId=jobStatus.getJobId(),
#        jobStatus=JobStatusValues.COMPLETE.value,
#        fireDefn=finalJobDefn,
#        targetSiteName=siteName
#    )

site.getRunDriver().setWorkflowEventTrigger(jobSetEventTrigger)

# The final job will start asynchronously after all jobs in the first set complete
logging.info(f"The final job will start after all {N} jobs in the first set complete.")