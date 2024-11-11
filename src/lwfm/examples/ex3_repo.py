# print 'hello world' but as a Job on a local site

from lwfm.base.Site import Site
from lwfm.base.Metasheet import Metasheet
from lwfm.midware.Logger import Logger
from lwfm.base.LwfmBase import _IdGenerator

if __name__ == "__main__":
    site = Site.getSite("local")
    site.getAuth().login()

    sampleId = _IdGenerator.generateId()
    Logger.info(f"sampleId: {sampleId}")
    
    metadata = {"foo": "bar", "hello": "world", "sampleId": sampleId}
    site.getRepo().put("someFile.dat", "/tmp/someFile.dat", Metasheet(metadata))

    sheets = site.getRepo().find({"foo": "*", "sampleId": sampleId})
    for s in sheets:
        Logger.info(f"{s}")

    

