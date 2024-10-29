from abc import ABC
from typing import List
from tinydb import TinyDB, Query
from tinydb.table import Document
import os

from lwfm.base.LwfmBase import _IdGenerator

# ****************************************************************************
_DB_FILE = os.path.join(os.path.expanduser("~"), ".lwfm", "store.json")

class Store(ABC):
    _db = TinyDB(_DB_FILE)
        
    def _get(self, siteName: str, pillar: str, key: str) -> List[dict]:
        Q = Query()
        return self._db.search((Q.site == siteName) & (Q.pillar == pillar) & (Q.key == key))

    def _put(self, siteName: str, pillar: str, key: str, doc: str) -> None:
        id = _IdGenerator().generateInteger()
        record = {
            "id": id,
            "site": siteName,
            "pillar": pillar,
            "key": key,
            "doc": doc
        }
        self._db.insert(Document(record, doc_id=id))
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

class EventStore(Store):
    def __init__(self):
        super(EventStore, self).__init__()

    def putWfEvent(self, datum: "WfEvent") -> bool: # type: ignore
        self._put(datum.getFireSite(), "run.event", 
                  datum.getId(), datum.__str__())


# ****************************************************************************

class JobStatusStore(Store):
    def __init__(self):
        super(JobStatusStore, self).__init__()

    def putJobStatus(self, datum: "JobStatus") -> None: # type: ignore
        self._put(datum.getJobContext().getSiteName(), 
                  "run.status", datum.getJobId(), datum.__str__())


# ****************************************************************************

