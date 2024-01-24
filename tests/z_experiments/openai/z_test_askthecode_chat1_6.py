"""
User: write an example of running a "hello world" job when a piece of data is put into 
the repo with the metadata foo="bar"

This code needed modification in three places from the raw askthecode output.
- lines 26 & 27 needed real file paths, not "/path/to/file/goes/here" placeholders, 
tho the generated code comment clearly says so...
"""

import logging
from lwfm.base.Site import Site
from lwfm.base.JobDefn import JobDefn, RepoJobDefn, RepoOp
from lwfm.base.WorkflowEventTrigger import DataEventTrigger
from lwfm.base.SiteFileRef import FSFileRef

# Set up logging
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

# Define the Site
siteName = "local"
site = Site.getSiteInstanceFactory(siteName)
site.getAuthDriver().login()

# Define paths and metadata for the data put operation
sourcePath = "/tmp/ask1-5.in"  # Update with the actual source path
destinationPath = "/tmp/out"  # Update with the actual destination path
metadata = {"foo": "bar"}

# Define the data put job using RepoJobDefn
dataPutJobDefn = RepoJobDefn(
    repoOp=RepoOp.PUT,
    localRef=sourcePath,
    siteFileRef=FSFileRef(name="data.txt", path=destinationPath, metadata=metadata)
)

# Submit the data put job to the Site
dataPutJobStatus = site.getRunDriver().submitJob(dataPutJobDefn)
logging.info("Data put job " + dataPutJobStatus.getJobId() + " " + dataPutJobStatus.getStatusValue())

# Define the "hello world" job
helloWorldJobDefn = JobDefn()
helloWorldJobDefn.setEntryPoint("echo 'hello world'")

# Define a function to filter metadata
def metadata_filter(metadata):
    return metadata.get("foo") == "bar"

# Set up a DataEventTrigger to submit the "hello world" job when data with specific metadata is put into the repo
dataEventTrigger = DataEventTrigger(
    triggerFilter=metadata_filter,
    fireDefn=helloWorldJobDefn,
    targetSiteName=siteName
)

# Register the DataEventTrigger
site.getRunDriver().setWorkflowEventTrigger(dataEventTrigger)

logging.info("The 'hello world' job will start when data with metadata foo='bar' is put into the repo.")