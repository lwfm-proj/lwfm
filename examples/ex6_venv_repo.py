"""
example of data management
"""

#pylint: disable = invalid-name

from lwfm.base.Site import Site
from lwfm.midware.LwfManager import logger, lwfManager

if __name__ == "__main__":
    site: Site = lwfManager.getSite("local-venv")
    site.getAuthDriver().login()

    ts = lwfManager.generateId()
    metadata = {"foo": "bar", "hello": "world", "sampleId": ts}
    site.getRepoDriver().put("ex1_date.out", "/tmp/someFile.dat",
        None, # autogen a new job for this repo activity if no JobContext provided
        metadata)

    clause = {"sampleId": ts}
    logger.info(f"finding {clause}")
    sheets = lwfManager.find(clause)
    if not sheets:
        logger.info("No results")
    else:
        for s in sheets:
            logger.info(f"{s}")
    clause = {"foo": "b*"}
    logger.info(f"finding {clause}")
    sheets = lwfManager.find(clause)
    if not sheets:
        logger.info("No results")
    else:
        for s in sheets:
            logger.info(f"{s}")
