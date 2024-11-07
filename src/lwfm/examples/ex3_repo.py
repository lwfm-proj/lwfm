# print 'hello world' but as a Job on a local site

from lwfm.base.Site import Site
from lwfm.base.JobDefn import JobDefn
from lwfm.base.Metasheet import Metasheet
from lwfm.midware.Logger import Logger
from lwfm.midware.LwfManager import LwfManager
from lwfm.base.LwfmBase import _IdGenerator

if __name__ == "__main__":
    # only using one site for this example - construct an interface to it
    site = Site.getSite("local")

    # a "local" site login is a no-op; real sites will have a login mechanism
    site.getAuth().login()

    sampleId = _IdGenerator.generateId()

    metadata = {"foo": "bar", "hello": "world", "sampleId": sampleId}
    site.getRepo().put("file.dat", "/tmp/file.dat", Metasheet(metadata))

    sheets = site.getRepo().find({"foo": "*", "sampleId": sampleId})
    for s in sheets:
        Logger.info(f"{s}")

    

