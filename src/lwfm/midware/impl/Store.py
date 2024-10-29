from abc import ABC
from typing import List
from tinydb import TinyDB, Query
from tinydb.table import Document
import os
import time

from lwfm.base.LwfmBase import _IdGenerator

# ****************************************************************************
_DB_FILE = os.path.join(os.path.expanduser("~"), ".lwfm", "store.json")

class Store(ABC):
    _db = TinyDB(_DB_FILE)
        
    def _get(self, siteName: str, pillar: str, key: str) -> List[dict]:
        Q = Query()
        if (key is None) or (key == ""):
            return self._db.search((Q.site == siteName) & (Q.pillar == pillar))
        else:
            return self._db.search((Q.site == siteName) & (Q.pillar == pillar) & (Q.key == key))


    def _put(self, siteName: str, pillar: str, key: str, doc: str) -> None:
        id = _IdGenerator().generateInteger()
        record = {
            "db_id": id,
            "site": siteName,
            "pillar": pillar,
            "key": key,
            "timestamp": time.perf_counter_ns(),
            "doc": doc
        }
        self._db.insert(Document(record, doc_id=id))
        return


    def _sortMostRecent(self, docs: List[dict]) -> List[dict]:
        return sorted(docs, key=lambda x: x['timestamp'], reverse=True)


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

    def getAllWfEvents(self) -> List[dict]: # type: ignore
        return self._sortMostRecent(self._get("local", "run.event"))


# ****************************************************************************

class JobStatusStore(Store):
    def __init__(self):
        super(JobStatusStore, self).__init__()

    def putJobStatus(self, datum: "JobStatus") -> None: # type: ignore
        self._put(datum.getJobContext().getSiteName(), 
                  "run.status", datum.getJobId(), datum.serialize())

    # return the most recent status record for this jobId
    def getJobStatusBlob(self, jobId: str) -> str: 
        try:
            return self._sortMostRecent(self._get("local", "run.status", jobId))[0]["doc"]   
        except Exception as e:
            print("Error in getJobStatusBlob: " + str(e))
            return ""


# ****************************************************************************

