from abc import ABC, abstractmethod
from lwfm.base.SiteFileRef import SiteFileRef

class MetaRepo(ABC):
    """
    Interface to the MetaRepo middleware component.  There can be many implementations of
    the interface from cheap flat files to fancy DBs.
    """

    def __init__(self):
        pass

    @abstractmethod
    def notate(self, 
        fileRef: SiteFileRef,
    ) -> str:
        pass

    @abstractmethod
    def find(self, fileRef) -> list[dict]:
        pass

