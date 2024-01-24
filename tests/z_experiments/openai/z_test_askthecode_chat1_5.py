"""
User: write an example of moving a simple data file, then running a "hello world" job 
asynchronously when the data move is complete

This code needed modification in three places from the raw askthecode output.
- lines 27 & 28 needed real file paths, not "/path/to/file/goes/here" placeholders, 
tho the generated code comment clearly says so...
"""

import logging
from lwfm.base.Site import Site
from lwfm.base.JobDefn import JobDefn, RepoJobDefn, RepoOp
from lwfm.base.JobStatus import JobStatusValues
from lwfm.base.WorkflowEventTrigger import JobEventTrigger
from lwfm.base.SiteFileRef import FSFileRef

# Set up logging
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

# Define the Site
siteName = "local"
site = Site.getSiteInstanceFactory(siteName)
site.getAuthDriver().login()

# Define paths for the data move
sourcePath = "/tmp/ask1-5.in"  # Update with the actual source path
destinationPath = "/tmp/out"  # Update with the actual destination path

# Define the data movement job using RepoJobDefn
dataMoveJobDefn = RepoJobDefn(
    repoOp=RepoOp.PUT,
    localRef=sourcePath,
    siteFileRef=FSFileRef(name="data.txt", path=destinationPath)
)

# Submit the data movement job to the Site
dataMoveJobStatus = site.getRunDriver().submitJob(dataMoveJobDefn)
logging.info("Data move job " + dataMoveJobStatus.getJobId() + " " + dataMoveJobStatus.getStatusValue())

# Define the "hello world" job
helloWorldJobDefn = JobDefn()
helloWorldJobDefn.setEntryPoint("echo 'hello world'")

# Set up a WorkflowEventTrigger to submit the "hello world" job when the data move job completes
jobEventTrigger = JobEventTrigger(
    jobId=dataMoveJobStatus.getJobId(),
    jobStatus=JobStatusValues.COMPLETE.value,
    fireDefn=helloWorldJobDefn,
    targetSiteName=siteName
)

# Set the WorkflowEventTrigger on the site
site.getRunDriver().setWorkflowEventTrigger(jobEventTrigger)

# The "hello world" job will start asynchronously after the data move job completes
logging.info("The 'hello world' job will start after the data move job " + dataMoveJobStatus.getJobId() + " completes.")
