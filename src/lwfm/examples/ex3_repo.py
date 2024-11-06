# print 'hello world' but as a Job on a local site

from lwfm.base.Site import Site
from lwfm.base.JobDefn import JobDefn
from lwfm.base.Metasheet import Metasheet
from lwfm.midware.Logger import Logger
from lwfm.midware.LwfManager import LwfManager

if __name__ == "__main__":
    # only using one site for this example - construct an interface to it
    site = Site.getSite("local")

    # a "local" site login is a no-op; real sites will have a login mechanism
    site.getAuth().login()

    metadata = {"foo": "bar", "hello": "world"}
    site.getRepo().put("", "", Metasheet(metadata))

    sheets = site.getRepo().find({"foo": "*", "hello": "world"})
    for s in sheets:
        print(s)

        