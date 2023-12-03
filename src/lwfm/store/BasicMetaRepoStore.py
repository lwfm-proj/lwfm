import os

from lwfm.base.MetaRepo import MetaRepo
from lwfm.base.SiteFileRef import SiteFileRef

#************************************************************************************************************************************


class BasicMetaRepoStore(MetaRepo):
    def __init__(self):
        super(BasicMetaRepoStore, self).__init__()

    def notate(self, 
        siteFileRef: SiteFileRef
    ):
        metasheet = {}
        metasheet["raw"] = siteFileRef.getArgs()
        #metasheet["userMetadata"] = siteFileRef.getMetadata()
        file_object = open(os.path.expanduser('~') + '/.lwfm/metarepo_store.txt', 'a+')
        file_object.write(str(metasheet) + "\n")
        file_object.close()


    def find(self, fileRef: SiteFileRef):
        fileList = []

        metaRepo = MetaRepo._getMetaRepo()
        for file in metaRepo:
            if fileRef.getId() is not None and file.getId() != fileRef.getId():
                continue
            if fileRef.getName() is not None and file.getName() != fileRef.getName():
                continue
            if (
                fileRef.getMetadata() is not None
                and file.getMetadata() != fileRef.getMetadata()
            ):
                continue
            fileList.append(file)
        return fileList
