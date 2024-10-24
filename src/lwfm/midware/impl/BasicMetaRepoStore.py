import os
from typing import List

from lwfm.base.SiteFileRef import SiteFileRef

# ******************************************************************************


class BasicMetaRepoStore:

    storeFile = os.path.expanduser("~") + "/.lwfm/metarepo_store.txt"

    def __init__(self):
        super(BasicMetaRepoStore, self).__init__()

    #def notate(self, siteFileRef: SiteFileRef):
    #    metasheet = {}
    #    metasheet["raw"] = siteFileRef.getArgs()
    #    file_object = open(self.storeFile, "a+")
    #    file_object.write(str(metasheet) + "\n")
    #    file_object.close()

    #def find(self, fileRef: SiteFileRef) -> List[SiteFileRef]:
    #    fileList = []
    #    with open(self.storeFile, "r") as file:
    #        for line in file:
    #            if str(fileRef.getMetadata()) in line:
    #                fileList.append(line)
    #    return fileList
