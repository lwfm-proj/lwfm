
from enum import Enum

from LwfmBase import LwfmBase


class _SiteFields(Enum):
    SITE_NAME = "siteName"


class Site(LwfmBase):
    def __init__(self, args=None):
        super(Site, self).__init__(args)

    def setName(self, name):
        LwfmBase._setArg(self, _SiteFields.SITE_NAME.value, name)

    def getName(self):
        return LwfmBase._getArg(self, _SiteFields.SITE_NAME.value)



# test
if __name__ == '__main__':
    siteFoo = Site()
    siteFoo.setName("foo")
    print(siteFoo.getName())
