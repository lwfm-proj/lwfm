from pathlib import Path
from lwfm.base.JobDefn import JobDefn
from lwfm.base.Site import Site
from lwfm.base.SiteFileRef import RemoteFSFileRef

from lwfm.server.JobStatusSentinelClient import JobStatusSentinelClient

localDriver = Site.getSiteInstanceFactory("local")
perlmutterDriver = Site.getSiteInstanceFactory("perlmutter")
repoDriver = perlmutterDriver.getRepoDriver()
siteRef = RemoteFSFileRef()
siteRef.setHost("perlmutter")

print("Putting source")
localRef = Path("C:/lwfm/mpiHello.cxx")
siteRef.setPath("/global/homes/a/agallojr/mxn/mpiHello.cxx")
repoDriver.put(localRef, siteRef)

print("Putting runner")
localRef = Path("C:/lwfm/mpiHello.sh")
siteRef.setPath("/global/homes/a/agallojr/mxn/mpiHello.sh")
repoDriver.put(localRef, siteRef)

print("Putting batch script")
localRef = Path("C:/lwfm/mpiBatch.sh")
siteRef.setPath("/global/homes/a/agallojr/mxn/mpiBatch.sh")
repoDriver.put(localRef, siteRef)

# We need to treat echo as a script, since subprocess can't run unix commands if unix=True isn't enabled
# and that's fine anyway, since we shouldn't assume unix
print("Putting echo script")
localRef = Path("C:/lwfm/echo.sh")
siteRef.setPath("/global/homes/a/agallojr/mxn/echo.sh")
repoDriver.put(localRef, siteRef)

print("Running Remote")
runDriver = perlmutterDriver.getRunDriver()
jdefn = JobDefn()
jdefn.setEntryPointPath("sbatch ~/mxn/mpiBatch.sh")
remoteJobId = runDriver.submitJob(jdefn)

print("Running Local")
localRunDriver = localDriver.getRunDriver()
jdefn = JobDefn()
jdefn.setEntryPointPath("~/mxn/mpiHello.sh")
localJobId = localRunDriver.submitJob(jdefn)

print("Setting triggers")
remoteJobSuccess = JobDefn()
jdefn.setEntryPointPath("~/mxn/echo.sh 'remote job succeeded'")
localJobSuccess = JobDefn()
jdefn.setEntryPointPath("~/mxn/echo.sh 'local job succeeded'")
remoteJobFailed = JobDefn()
jdefn.setEntryPointPath("~/mxn/echo.sh 'remote job failed'")
localJobFailed = JobDefn()
jdefn.setEntryPointPath("~/mxn/echo.sh 'local job failed'")
JobStatusSentinelClient().setEventHandler(remoteJobId, "perlmutter", "COMPLETE", remoteJobSuccess, "local")
JobStatusSentinelClient().setEventHandler(localJobId, "local", "COMPLETE", localJobSuccess, "local")
JobStatusSentinelClient().setEventHandler(remoteJobId, "perlmutter", "FAILED", remoteJobFailed, "local")
JobStatusSentinelClient().setEventHandler(localJobId, "local", "FAILED", localJobFailed, "local")