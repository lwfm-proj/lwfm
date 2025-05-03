"""
A basic dictionary to hold metadata about data objects under management by lwfm
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring

from lwfm.midware._impl.IdGenerator import IdGenerator

class Metasheet:
    """
    A collection of name=value pairs for a blob of data on some site at some url.
    """

    def __init__(self, siteName: str, siteUrl: str, props: dict = None):
        self._sheet_id = IdGenerator().generateId()
        self._job_id = self._sheet_id
        self._siteName = siteName
        self._siteUrl = siteUrl
        self._props = props

    def __str__(self):
        return f"{self._props}"

    def getSheetId(self) -> str:
        return self._sheet_id

    def getJobId(self) -> str:
        return self._job_id

    def setJobId(self, jobId: str) -> None:
        self._job_id = jobId

    def getSiteName(self) -> str:
        return self._siteName

    def setSiteName(self, siteName: str) -> None:
        self._siteName = siteName

    def getSiteUrl(self) -> str:
        return self._siteUrl

    def setSiteUrl(self, siteUrl: str) -> None:
        self._siteUrl = siteUrl

    def getProps(self) -> dict:
        return self._props

    def setProps(self, props: dict) -> None:
        self._props = props
