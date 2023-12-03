from abc import ABC, abstractmethod
from lwfm.base.SiteFileRef import SiteFileRef


class MetaRepo(ABC):
    @abstractmethod
    def notate(self, 
        fileRef: SiteFileRef,
        siteClass=None,
        siteMetadata=None,
        targetClass=None,
        targetMetadata=None
    ):
        pass

    @abstractmethod
    def find(self, fileRef):
        pass


class BasicMetaRepoImpl(MetaRepo):
    def notate(self, 
        siteFileRef: SiteFileRef,
        siteClass=None,
        siteMetadata=None,
        targetClass=None,
        targetMetadata=None
    ):      
        metasheet = {}
        metasheet["raw"] = siteFileRef.getArgs()
        print(metasheet)


    def find(self, fileRef):
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
