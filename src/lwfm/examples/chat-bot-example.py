import logging
import os
from lwfm.base.Site import Site, SiteAuthDriver, SiteRunDriver, SiteRepoDriver
from lwfm.base.SiteFileRef import FSFileRef
from lwfm.base.JobDefn import JobDefn

# Create a site object
site = Site()

# Set the authentication driver
site.setAuthDriver(SiteAuthDriver())

# Set the run driver
site.setRunDriver(SiteRunDriver())

# Set the repository driver
site.setRepoDriver(SiteRepoDriver())

# Upload a text file
localFile = "/path/to/text/file.txt"
destFileRef = FSFileRef(site.getSiteName(), "/path/to/destination/file.txt")
copiedFileRef = site.getRepoDriver().put(localFile, destFileRef)
logging.info("Uploaded file: " + copiedFileRef.getName())

# Run a job to echo the contents of the uploaded file
jdefn = JobDefn()
jdefn.setEntryPoint("echo")
jdefn.setJobArgs([f"cat {copiedFileRef.getPath()}"])
status = site.getRunDriver().submitJob(jdefn)
logging.info("Job ID: " + status.getJobContext().getId())