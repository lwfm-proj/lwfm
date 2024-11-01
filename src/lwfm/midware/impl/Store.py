from abc import ABC
from typing import List
from tinydb import TinyDB, Query
from tinydb.table import Document
import os
import time

from lwfm.base.LwfmBase import _IdGenerator
from lwfm.base.JobStatus import JobStatus
from lwfm.base.WfEvent import WfEvent

# ****************************************************************************
_DB_FILE = os.path.join(os.path.expanduser("~"), ".lwfm", "store.json")


class Store(ABC):
    _db = TinyDB(_DB_FILE)
        
    # returns list of raw records, containing header fields and the "doc" field
    def _get(self, siteName: str, pillar: str, key: str = None) -> List[dict]:
        Q = Query()
        if (siteName is None) or (siteName == ""):
            return self._db.search((Q.pillar == pillar) & (Q.key == key))
        elif (key is None) or (key == ""):
            return self._db.search((Q.site == siteName) & (Q.pillar == pillar))
        else:
            return self._db.search((Q.site == siteName) & (Q.pillar == pillar) & (Q.key == key))


    def _put(self, siteName: str, pillar: str, key: str, doc: str) -> None:
        id = _IdGenerator().generateInteger()
        ts = time.perf_counter_ns()
        if (key is None) or (key == ""):
            key = ts
        record = {
            "db_id": id,
            "site": siteName,
            "pillar": pillar,
            "key": key,
            "timestamp": ts,
            "doc": doc                              # the data, serialized object, etc
        }
        self._db.insert(Document(record, doc_id=id))
        return


    def _sortMostRecent(self, docs: List[dict]) -> List[dict]:
        return sorted(docs, key=lambda x: x['timestamp'], reverse=True)


# ****************************************************************************

class AuthStore(Store):
    def __init__(self):
        super(AuthStore, self).__init__()

    # return the site-specific auth blob for this site
    def getAuthForSite(self, siteName: str) -> str:
        return self._get(siteName, "auth", "auth")[0]["doc"]

    # set the site-specific auth blob for this site
    def putAuthForSite(self, siteName: str, doc: str) -> None:
        self._put(siteName, "auth", "auth", doc)


# ****************************************************************************

class LoggingStore(Store):
    def __init__(self):
        super(LoggingStore, self).__init__()

    # put a record in the logging store
    def putLogging(self, level: str, doc: str) -> None:
        self._put("local", "run.log." + level, None, doc)


# ****************************************************************************

class EventStore(Store):
    _loggingStore: LoggingStore = None

    def __init__(self):
        super(EventStore, self).__init__()
        self._loggingStore = LoggingStore()

    def putWfEvent(self, datum: WfEvent, typeT: str) -> bool: 
        try: 
            self._put(datum.getFireSite(), "run.event." + typeT, 
                      datum.getId(), datum.serialize())
            return True
        except Exception as e:
            self._loggingStore.putLogging("ERROR", "Error in putWfEvent: " + str(e))
            return False

    def getAllWfEvents(self, typeT: str = None) -> List[WfEvent]: 
        if typeT is None:
            t = "run.event"
        else:
            t = "run.event." + typeT
        blobs = self._sortMostRecent(self._get(None, t))
        return [WfEvent.deserialize(blob["doc"]) for blob in blobs]

    def deleteAllWfEvents(self) -> None:
        q = Query()
        self._db.remove(q.pillar == 'run.event')

    def deleteWfEvent(self, eventId: str) -> bool:
        try: 
            q = Query()
            self._db.remove(q.key == eventId)
            return True
        except Exception as e:
            self._loggingStore.putLogging("ERROR", "Error in deleteWfEvent: " + str(e))
            return False


# ****************************************************************************

class JobStatusStore(Store):
    _loggingStore: LoggingStore = None

    def __init__(self):
        super(JobStatusStore, self).__init__()
        self._loggingStore = LoggingStore()

    def putJobStatus(self, datum: JobStatus) -> None: 
        self._put(datum.getJobContext().getSiteName(), 
                  "run.status", datum.getJobId(), datum.serialize())

    # return the most recent status record for this jobId
    def getJobStatus(self, jobId: str) -> JobStatus: 
        try:
            results = self._get("local", "run.status", jobId)
            if len(results) == 0:
                return None
            return JobStatus.deserialize(self._sortMostRecent(results)[0]["doc"])
        except Exception as e:
            self._loggingStore.putLogging("ERROR", "Error in getJobStatus: " + str(e))
            return None

    def getAllJobStatuses(self, jobId: str) -> List[JobStatus]:
        try:
            blobs = self._sortMostRecent(self._get(None, "run.status", jobId))
            return [JobStatus.deserialize(blob["doc"]) for blob in blobs]
        except Exception as e:
            self._loggingStore.putLogging("ERROR", "Error in getAllJobStatuses: " + str(e))
            return None
        
# ****************************************************************************
# testing 

if __name__ == "__main__":
    eStore = EventStore()
    sStore = JobStatusStore()

    #events = eStore.getAllWfEvents("JOB")
    #for e in events:
    #    print(e)    

    #events = eStore.getAllWfEvents("REMOTE")
    #for e in events:
    #    print(e)   

    statuses = sStore.getAllJobStatuses("def2eca8-734e-4bdf-bedf-fa784f951503")
    for s in statuses:
        print(s)

    





    

