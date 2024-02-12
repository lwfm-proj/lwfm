"""
User: please analyze the code in src/ and write an example in the python language of a 
simple job which prints "hello world"
"""

import logging
from lwfm.base.Site import Site
from lwfm.base.JobDefn import JobDefn

# Set up logging
logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

# Define the Site
siteName = "local"
site = Site.getSiteInstanceFactory(siteName)
site.getAuthDriver().login()

# Define the Job
jobDefn = JobDefn()
jobDefn.setEntryPoint("echo 'hello world'")

# Submit the Job to the Site
status = site.getRunDriver().submitJob(jobDefn)
logging.info("hello world job " + status.getJobId() + " " + status.getStatusValue())

# Wait for the job to finish and log the final status
status = status.wait()
logging.info("hello world job " + status.getJobId() + " " + status.getStatusValue())