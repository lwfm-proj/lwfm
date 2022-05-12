
from abc import ABC, abstractmethod
import logging
from datetime import datetime
from enum import Enum

from lwfm.base.LwfmBase import LwfmBase


#************************************************************************************************************************************

# A reference to a file object on the site, not the file object itself.
# This might be a filesystem on a remote machine, or some other kind of managed repo.
# Methods with _abc are "protected" and should not be called by clients but only by the underlying implementations.
class SiteFileRef(ABC):
    # some files have identifiers other than a name, some might just return the name
    @abstractmethod
    def getId(self) -> str:
        pass

    @abstractmethod
    def _setId(self, id: str) -> None:
        pass

    # the common name of the file
    @abstractmethod
    def getName(self) -> str:
        pass

    @abstractmethod
    def _setName(self, name: str) -> None:
        pass

    # path can mean many things... file system path, with or without host prefix, a link to some "repo", just the id,
    # a metadata tuple, etc.
    @abstractmethod
    def getPath(self) -> str:
        pass

    @abstractmethod
    def _setPath(self, path: str) -> None:
        pass

    # TODO what are the units?
    @abstractmethod
    def getSize(self) -> int:
        pass

    @abstractmethod
    def _setSize(self, size: int) -> None:
        pass

    @abstractmethod
    def getTimestamp(self) -> datetime:
        pass

    @abstractmethod
    def _setTimestamp(self, tstamp: datetime) -> None:
        pass


#************************************************************************************************************************************

class _RemoteFSFileRefFields(Enum):
    NAME      = "name"
    PATH      = "path"
    SIZE      = "size"
    TIMESTAMP = "timestamp"
    HOST      = "host"


class RemoteFSFileRef(SiteFileRef, LwfmBase):
    def getId(self) -> str:
        return self.getName()

    def _setId(self, id: str) -> None:
        self._setName(id)

    def getName(self) -> str:
        return LwfmBase._getArg(self, _RemoteFSFileRefFields.NAME.value)

    def _setName(self, name: str) -> None:
        LwfmBase._setArg(self, _RemoteFSFileRefFields.NAME.value, name)

    def getPath(self) -> str:
        return LwfmBase._getArg(self, _RemoteFSFileRefFields.PATH.value)

    def _setPath(self, path: str) -> None:
        LwfmBase._setArg(self, _RemoteFSFileRefFields.PATH.value, path)

    def getSize(self) -> int:
        return LwfmBase._getArg(self, _RemoteFSFileRefFields.SIZE.value)

    def _setSize(self, size: int) -> None:
        LwfmBase._setArg(self, _RemoteFSFileRefFields.SIZE.value, size)

    def getTimestamp(self) -> datetime:
        return LwfmBase._getArg(self, _RemoteFSFileRefFields.TIMESTAMP.value)

    def _setTimestamp(self, tstamp: datetime) -> None:
        LwfmBase._setArg(self, _RemoteFSFileRefFields.TIMESTAMP.value, tstamp)

    def getHost(self) -> str:
        return LwfmBase._getArg(self, _RemoteFSFileRefFields.HOST.value)

    def _setHost(self, host: str) -> None:
        LwfmBase._setArg(self, _RemoteFSFileRefFields.HOST.value, host)


#************************************************************************************************************************************
