"""
User: write another example, this time run a job which prints "hello world" asynchronously 
after another job similarly prints "hello world"
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

# Define the first Job
firstJobDefn = JobDefn()
firstJobDefn.setEntryPoint("echo 'hello world'")

# Submit the first Job to the Site
firstJobStatus = site.getRunDriver().submitJob(firstJobDefn)
logging.info("First 'hello world' job " + firstJobStatus.getJobId() + " " + firstJobStatus.getStatusValue())

# Define the second Job
secondJobDefn = JobDefn()
secondJobDefn.setEntryPoint("echo 'hello world'")

# Set up a WorkflowEventTrigger to submit the second job when the first job completes
jobEventTrigger = JobEventTrigger(
    jobId=firstJobStatus.getJobId(),
    jobStatus=JobStatusValues.COMPLETE.value,
    fireDefn=secondJobDefn,
    targetSiteName=siteName
)

# Set the WorkflowEventTrigger on the site
secondJobStatus = site.getRunDriver().setWorkflowEventTrigger(jobEventTrigger)

# The second job will start asynchronously after the first job completes
logging.info("Second 'hello world' job " + secondJobStatus.getJobId() + " is set to start after the first job completes.")