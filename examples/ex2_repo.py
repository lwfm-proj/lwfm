"""
example of data management
"""

#pylint: disable = invalid-name

from lwfm.base.Site import Site
from lwfm.base.JobContext import JobContext
from lwfm.midware.LwfManager import logger, lwfManager

if __name__ == "__main__":
    site: Site = lwfManager.getSite("local")
    site.getAuthDriver().login()

    # treat this script's activities like a traceable job
    context = JobContext()
    context.setSiteName(site.getSiteName())
    lwfManager.setContext(context)

    ts = lwfManager.generateId()
    metadata = {"foo": "bar", "hello": "world", "sampleId": ts}
    site.getRepoDriver().put("ex1_date.out", "/tmp/someFile.dat", None, metadata)

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
