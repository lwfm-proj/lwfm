from lwfm.base.Site import Site
from lwfm.base.SiteFileRef import FSFileRef
from py4dt4d.PyEngine import PyEngineUtil
from lwfm.base.JobStatus import JobStatus, JobStatusValues, JobContext
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobEventHandler import JobEventHandler
from pathlib import Path

siteName = "dt4d"

if __name__ == '__main__':
    #This test case demonstrates putting a file to the DT4D repo, getting the sheet by the metadata we uploaded with it
    #and then downloading the file using the sheet we get back

    site = Site.getSiteInstanceFactory(siteName)
    runDriver = site.getRunDriver()

    #print("COMPUTE TYPES: " + str(runDriver.listComputeTypes()))

    #handlers = runDriver.listEventHandlers()
    #handlerIds = []

    #for handler in handlers:
    #  handlerIds.append(handler["triggerJobId"])

    #print("EVENT HANDLER IDS: " + str(handlerIds))

    jobDefn = JobDefn()

    jobDefn.setName("HelloWorld-jobSet-trigger")
    jobDefn.setComputeType("Win-VDrive")
    # dt4d uses a "ToolRepo" to store applications / workflows to be run which allows it to do digital threading
    # here we're running a simple python script as a dt4d "tool"
    jobDefn.setEntryPoint(["HelloWorld", "HelloWorld", "HelloWorld"])
    context = JobContext()
    jobSetTriggerId = context.getId()
    uuid = PyEngineUtil.generateId()
    print("Test UUID: " + uuid)
    jeh = JobEventHandler("", "dt4d", "", ["jobset", uuid,"1"], "dt4d", context)
    runDriver.setEventHandler(jobDefn, jeh)

    context = JobContext()
    dataTriggerId = context.getId()
    jeh = JobEventHandler("", "dt4d", "", ["data", {"lwfm-data-trigger-test": uuid}], "dt4d", context)
    jobDefn.setName("HelloWorld-data-trigger")
    runDriver.setEventHandler(jobDefn, jeh)

    context = JobContext()
    context.setId(uuid)
    context.setJobSetId(uuid)
    jobDefn.setName("HelloWorld")
    jobDefn.setComputeType("Win-VDrive")
    runDriver.submitJob(jobDefn, context)

    repoDriver = site.getRepoDriver()
    resourceName = "testFile" + uuid

    putFileRef = FSFileRef()
    putFileRef.setName(resourceName)
    putFileRef.setMetadata({"lwfm-data-trigger-test": uuid})

    filePath = Path(__file__)

    repoDriver.put(filePath, putFileRef)