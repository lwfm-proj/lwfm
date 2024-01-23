
import logging

from lwfm.base import Site, JobDefn, WorkflowEventTrigger
from lwfm.base.JobStatus import JobStatusValues

# Define the "Hello World" job
class HelloWorldJob(JobDefn):
    def execute(self):
        print("Hello World")

# Initialize the local site
local_site = Site(name="LocalSite")
run_driver = local_site.getRunDriver()

# Initialize a list to hold the job IDs
job_ids = []

# Submit the first 5 "Hello World" jobs
for _ in range(5):
    job = HelloWorldJob()
    job_id = run_driver.submitJob(job)
    job_ids.append(job_id)

# Define the final "Hello World" job
final_job = HelloWorldJob()

# Define a trigger that waits for the completion of all 5 jobs
trigger = WorkflowEventTrigger(job_ids=job_ids, condition='ALL_COMPLETED')

# Submit the final job with the trigger
run_driver.setWorkflowEventTrigger(trigger, final_job)

print("Workflow with 5 'Hello World' jobs and a trigger for the final job has been submitted.")
