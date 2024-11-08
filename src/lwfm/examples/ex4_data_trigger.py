
from lwfm.base.Site import Site
from lwfm.base.Metasheet import Metasheet
from lwfm.midware.Logger import Logger
from lwfm.base.LwfmBase import _IdGenerator
from lwfm.midware.LwfManager import LwfManager
from lwfm.base.WfEvent import MetadataEvent
from lwfm.base.JobDefn import JobDefn

if __name__ == "__main__":
    site = Site.getSite("local")
    site.getAuth().login()

    sampleId = _IdGenerator.generateId()

    # when data is put into the repo with this sampleId, fire the job 
    futureJobId = LwfManager.setEvent(
        MetadataEvent({"sampleId": sampleId}, JobDefn("echo hello world"), "local")
    ) 
    Logger.info(f"job {futureJobId} set as a data event trigger")
    
    # now put the file with the metadata 
    metadata = {"foo": "bar", "hello": "world", "sampleId": sampleId}
    site.getRepo().put("file.dat", "/tmp/file.dat", Metasheet(metadata))

    # if we want we can wait for the future job to finish
    status = LwfManager.wait(futureJobId)
    Logger.info("data-triggered job finished", status)

