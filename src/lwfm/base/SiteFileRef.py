
# SiteFileRef: an abstract representation of a data object (a "file") on a Site.  The 
# Site's Repo subsystem might back that with a normal filesystem, or it might back it 
# with something fancier like an object store.  Its the role of the Site's Repo
# subsystem to interpret the SiteFileRef in its own implementation-detail terms.


from datetime import datetime
from enum import Enum
import os

from lwfm.base.LwfmBase import LwfmBase

# TODO rethink 

#************************************************************************************

# A reference to a file object on the site, not the file object itself.
# This might be a filesystem on a remote machine, or some other kind of managed repo.

class _SiteFileRefFields(Enum):
    ID        = "id"            # sometimes the data entity has an id distinct from its name
    NAME      = "filename"      # full name, whatever that means in the context of this 
                                # managed repo 
    SIZE      = "size"  
    TIMESTAMP = "timestamp"     # the create timestamp, or last edit - this is up to the 
                                # Repo driver 
    IS_FILE   = "isFile"
    METADATA  = "metadata"      # a dict of name=value metadata pairs about this data entity

class SiteFileRef(LwfmBase):
    """
    A reference to a data entity - a file, or other managed data object.
    """

    def __init__(self):
        super(SiteFileRef, self).__init__(None)

    def getName(self) -> str:
        return LwfmBase._getArg(self, _SiteFileRefFields.NAME.value)

    def setName(self, name: str) -> None:
        LwfmBase._setArg(self, _SiteFileRefFields.NAME.value, name)

    def getSize(self) -> int:
        return LwfmBase._getArg(self, _SiteFileRefFields.SIZE.value)

    def setSize(self, size: int) -> None:
        LwfmBase._setArg(self, _SiteFileRefFields.SIZE.value, size)

    def getTimestamp(self) -> datetime:
        return LwfmBase._getArg(self, _SiteFileRefFields.TIMESTAMP.value)

    def setTimestamp(self, tstamp: datetime) -> None:
        LwfmBase._setArg(self, _SiteFileRefFields.TIMESTAMP.value, tstamp)

    def getMetadata(self) -> dict:
        return LwfmBase._getArg(self, _SiteFileRefFields.METADATA.value)

    def setMetadata(self, metadata: dict) -> None:
        LwfmBase._setArg(self, _SiteFileRefFields.METADATA.value, metadata)

    # path can mean many things... file system path, with or without host prefix, a 
    # link to some "repo", just the id, a metadata tuple, etc.
    def getPath(self) -> str:
        pass

    def setPath(self, path: str) -> None:
        pass

    def isFile(self) -> bool:
        return LwfmBase._getArg(self, _SiteFileRefFields.IS_FILE.value)

    def setIsFile(self, isFile: bool) -> None:
        LwfmBase._setArg(self, _SiteFileRefFields.IS_FILE.value, isFile)

    def toString(self) -> str:
        return self.getArgs().toString()


#*********************************************************************************

class _FSFileRefFields(Enum):
    PATH          = "path"
    DIR_CONTENTS  = "dirContents"   # if a directory, the list of files within


class FSFileRef(SiteFileRef):
    """
    Since plain filesystem files are a common data entity, this subclass provides 
    convenience mechanisms.
    """
    def __init__(self, path: str = None, name: str = None, metadata: dict = None):
        super(FSFileRef, self).__init__()
        if (path is not None):
            self.setPath(path)
        if (name is not None):
            self.setName(name)
        if (metadata is not None):
            self.setMetadata(metadata)

    def setName(self, name: str) -> None:
        super().setName(name)
        super().setId(name)
            
    def getPath(self) -> str:
        return LwfmBase._getArg(self, _FSFileRefFields.PATH.value)

    def setPath(self, path: str) -> None:
        LwfmBase._setArg(self, _FSFileRefFields.PATH.value, path)

    def getDirContents(self) -> [str]:
        if (self.isFile()):
            return self.getName()
        else:
            return LwfmBase._getArg(self, _FSFileRefFields.DIR_CONTENTS.value)

    def setDirContents(self, contents: [str]) -> None:
        LwfmBase._setArg(self, _FSFileRefFields.DIR_CONTENTS.value, contents)

    @staticmethod
    def siteFileRefFromPath(path: str) -> SiteFileRef:
        fileRef = FSFileRef()
        fileRef.setId(path)
        fileRef.setName(path)
        fileRef.setPath(path)
        # TODO - messes up serialization
        #fileRef.setTimestamp(datetime.fromtimestamp(os.path.getmtime(path)))
        if (os.path.isfile(path)):
            fileRef.setIsFile(True)
            fileRef.setSize(os.path.getsize(path))
        else:
            fileRef.setIsFile(False)
            files = os.listdir(path)
            fileRef.setDirContents(files)
            fileRef.setSize(len(files))
        return fileRef


#********************************************************************************

class _RemoteFSFileRefFields(Enum):
    HOST      = "host"


class RemoteFSFileRef(FSFileRef):
    """
    Convenience subclass for files on a remote filesystem.
    """

    def getHost(self) -> str:
        return LwfmBase._getArg(self, _RemoteFSFileRefFields.HOST.value)

    def setHost(self, host: str) -> None:
        LwfmBase._setArg(self, _RemoteFSFileRefFields.HOST.value, host)

#*********************************************************************************

class S3FileRef(SiteFileRef):
    """
    A data object file in an s3 bucket.
    """

    def getPath(self) -> str:
        raise NotImplementedError()

    def setPath(self, path: str) -> None:
        raise NotImplementedError()
