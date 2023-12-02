# An example using the Local Site Driver to downloads a python file and input file, runs a job that will execute the 
# python file using the input file, and then once the job completes it will upload the file.  

# The python file used here simply takes a file with a list of numbers, multiplies by a specified number, and then writes 
# the multiplied numbers to an output file.

import logging
import time

from lwfm.base.Site import Site
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus, JobStatusValues
from lwfm.server.JobStatusSentinelClient import JobStatusSentinelClient

siteName = "local"
pythonFile = "C:\\Users\\gr80m\\projects\\resources\\multiply.py"
inputFile = "C:\\Users\\gr80m\\projects\\resources\\numbers.txt"
inputDest = "C:Users\\gr80m\\projects\\test_walled_garden\\input"
outputFile = "C:\\Users\\gr80m\\projects\\test_walled_garden\\output\\output.txt"
outputDest = "C:\\Users\\gr80m\\projects\\resources\\doe_output\\multiplied_numbers.txt"
multiplier = 5

if __name__ == '__main__':

    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    # one Site for this example - construct an interface to the Site
    site = Site.getSiteInstanceFactory(siteName)
    # a "local" Site login is generally a no-op
    site.getAuthDriver().login()

    logging.info("login successful")

    # uploading the python file which will be executed during the job
    fileRef = FSFileRef()
    file = os.path.realpath(pythonFile)
    fileRef = FSFileRef.siteFileRefFromPath(file)
    destFileRef = FSFileRef.siteFileRefFromPath(inputDest)
    file_path = Path(file)
    self.site.getRepoDriver().put(file_path, destFileRef, self.jobContext)
    print(file + " Successfully uploaded")

    # uploading the input file that will be passed in as an argument to the python script.
    fileRef = FSFileRef()
    file = os.path.realpath(filePath)
    fileRef = FSFileRef.siteFileRefFromPath(file)
    if "metadata" in repoDict:
        fileRef.setMetadata(repoDict["metadata"])
    destFileRef = FSFileRef.siteFileRefFromPath(inputDest)
    file_path = Path(file)
    self.site.getRepoDriver().put(file_path, destFileRef, self.jobContext)
    print(file + " Successfully uploaded")

    # what named computing resources are available on this site?
    logging.info("compute types = " + str(site.getRunDriver().listComputeTypes()))

    # define the Job - use all Job defaults except the actual command to execute
    jobDefn = JobDefn()

    # The entry point is the command line execution, here we are executing our python file, passing in out input file, multiplier number, and where it will write its output.
    jobDefn.setEntryPoint(" python C:Users\\gr80m\\projects\\test_walled_garden\\multiply.py C:\\Users\\gr80m\\projects\\test_walled_garden\\input\numbers.txt " + str(multiplier) + " C:\\Users\\gr80m\\projects\\test_walled_garden\\output\\output.txt")

    # submit the Job to the Site
    status = site.getRunDriver().submitJob(jobDefn)
    # the run is generally asynchronous - on a remote HPC-type Site certainly,
    # and even in a local Site the "local" driver can implement async runs (which in fact it does),
    # so expect this Job status to be "pending"
    logging.info("job " + status.getJobContext().getId() + " " + status.getStatus().value)

    # how could we tell the async job has finished? one way is to synchronously wait on its end status
    # (another way is asynchronous triggering, which we'll demonstrate in a separate example)
    context = status.getJobContext()
    status = site.getRunDriver().getJobStatus(context)
    while (not status.isTerminal()):
        time.sleep(15)
        status = site.getRunDriver().getJobStatus(context)
    logging.info("job " + status.getJobContext().getId() + " " + status.getStatus().value)

    fileRef = FSFileRef()

    # now that the job has complete, download the output file to our output file destination
    filePath = outputFile
    fileRef.setPath(filePath)

    fileDestination = outputDest
    
    destPath = Path(fileDestination)
    self.site.getRepoDriver().get(fileRef, destPath, self.jobContext)
    print("File has been Successfully downloaded.")
