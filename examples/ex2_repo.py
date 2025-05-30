"""
example of data management
"""

from lwfm.base.Site import Site
from lwfm.base.Metasheet import Metasheet
from lwfm.util.IdGenerator import IdGenerator
from lwfm.midware.Logger import logger

if __name__ == "__main__":
    site: Site = Site.getSite("local")
    site.getAuthDriver().login()

    ts = IdGenerator.generateId()
    metadata = {"foo": "bar", "hello": "world", "sampleId": ts}
    site.getRepoDriver().put("ex1_date.out", "/tmp/someFile.dat",
        None, # autogen a new job for this repo activity if no JobContext provided
        Metasheet(site.getSiteName(), "/tmp/someFile.dat", metadata))

    clause = {"sampleId": ts}
    logger.info(f"finding {clause}")
    sheets = site.getRepoDriver().find(clause)
    if not sheets:
        logger.info("No results")
    else:
        for s in sheets:
            logger.info(f"{s}")
    clause = {"foo": "b*"}
    logger.info(f"finding {clause}")
    sheets = site.getRepoDriver().find(clause)
    if not sheets:
        logger.info("No results")
    else:
        for s in sheets:
            logger.info(f"{s}")
