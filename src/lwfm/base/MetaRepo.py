from abc import ABC, abstractmethod
from lwfm.base.SiteFileRef import SiteFileRef

class MetaRepo(ABC):

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

