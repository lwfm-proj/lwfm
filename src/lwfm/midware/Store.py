from abc import ABC
from typing import List
from tinydb import TinyDB, Query
import os

from lwfm.base.LwfmBase import _IdGenerator

# ****************************************************************************
_DB_FILE = os.path.join(os.path.expanduser("~"), ".lwfm", "store.json")

class Store(ABC):
    _db = TinyDB(_DB_FILE)
        
    def makeRecord(self, siteName: str, pillar: str, key: str, doc: str) -> dict:
        return {
            "id": _IdGenerator().generateId(),
            "site": siteName,
            "pillar": pillar,
            "key": key,
            "doc": doc
        }

    def _get(self, siteName: str, pillar: str, key: str) -> List[dict]:
        Q = Query()
        return self._db.search((Q.site == siteName) & (Q.pillar == pillar) & (Q.key == key))

    def _put(self, siteName: str, pillar: str, key: str, doc: str) -> None:
        record = self.makeRecord(siteName, pillar, key, doc)
        self._db.insert(record)
        return

# ****************************************************************************

class AuthStore(Store):
    def __init__(self):
        super(AuthStore, self).__init__()

    def getAuthForSite(self, siteName: str) -> str:
        return self._get(siteName, "auth", "auth")[0]["doc"]

    def putAuthForSite(self, siteName: str, doc: str) -> None:
        self._put(siteName, "auth", "auth", doc)

# ****************************************************************************

class LoggingStore(Store):
    def __init__(self):
        super(LoggingStore, self).__init__()

    def putLogging(self, level: str, doc: str) -> None:
        self._put("local", "run.log", _IdGenerator().generateId(), doc)


# ****************************************************************************

