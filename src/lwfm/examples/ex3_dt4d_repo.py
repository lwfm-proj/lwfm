
import os
from lwfm.base.Site import Site
from lwfm.base.SiteFileRef import FSFileRef
from py4dt4d.PyEngine import PyEngineUtil
from lwfm.base.JobStatus import JobStatus, JobStatusValues, JobContext
from pathlib import Path

siteName = "dt4d"

if __name__ == '__main__':
    #This test case demonstrates putting a file to the DT4D repo, getting the sheet by the metadata we uploaded with it
    #and then downloading the file using the sheet we get back

    site = Site.getSiteInstanceFactory(siteName)
    repoDriver = site.getRepoDriver()

    uuid = PyEngineUtil.generateId()

    resourceName = "testFile" + uuid
    metadata = {"foo" + uuid: "bar" + uuid}

    putFileRef = FSFileRef()
    putFileRef.setName(resourceName)
    putFileRef.setMetadata(metadata)

    filePath = os.path.abspath(__file__)

    repoDriver.put(filePath, putFileRef)

    getFileRef = FSFileRef()
    getFileRef.setMetadata(metadata)

    files = repoDriver.find(getFileRef)

    #There should only be one file with the metadata we provided so we can pass in the first file in the list to download.
    getFilePath = Path(repoDriver.get(files[0], "/retrieved_file"))

    if getFilePath.is_file():
      print("Successfully retrieved the file: " + str(getFilePath))
    else:
      raise Exception("Failed to retrieved the file.")