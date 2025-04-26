"""
example of data management
"""

from lwfm.base.Site import Site
from lwfm.base.Metasheet import Metasheet

if __name__ == "__main__":
    site: Site = Site.getSite("local")
    site.getAuthDriver().login()

    metadata = {"foo": "bar", "hello": "world", "sampleId": 12345}
    site.getRepoDriver().put("ex1_date.out", "/tmp/someFile.dat",
        None, # autogen a new job for this repo activity
        Metasheet(site.getSiteName(), "/tmp/someFile.dat", metadata))

    #sheets = site.getRepoDriver().find({"foo": "bar", "sampleId": 12345})
    #if not sheets:
    #    for s in sheets:
    #        logger.info(f"{s}")
    #else:
    #    logger.info("No results")
