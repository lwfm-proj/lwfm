
from enum import Enum
import logging

from LwfmBase import LwfmBase


class _SiteFields(Enum):
    SITE_NAME = "siteName"


class Site(LwfmBase):
    def __init__(self, args: dict[str, type]=None):
        super(Site, self).__init__(args)

    def setName(self, name: str) -> None:
        LwfmBase._setArg(self, _SiteFields.SITE_NAME.value, name)

    def getName(self) -> str:
        return LwfmBase._getArg(self, _SiteFields.SITE_NAME.value)



# test
if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    siteFoo = Site()
    siteFoo.setName("foo")
    logging.info(siteFoo.getName())
