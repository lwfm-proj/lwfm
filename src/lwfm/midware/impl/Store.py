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

    def getAllAuth(self) -> List[str]:
        Q = Query()
        results = self._db.search((Q.pillar == "auth"))
        if (results is not None): 
            blobs = self._sortMostRecent(results)
            return [({ "site": blob["site"], "auth": blob["doc"] }) for blob in blobs]
        return None
    
    # return the site-specific auth blob for this site
    def getAuthForSite(self, siteName: str) -> str:
        Q = Query()
        result = self._db.search((Q.site == siteName) & (Q.pillar == "auth") & (Q.key == "auth"))
        if (result is not None): 
            return result[0]["doc"]
        return None

    # set the site-specific auth blob for this site
    def putAuthForSite(self, siteName: str, doc: str) -> None:
        self._put(siteName, "auth", "auth", doc)


# ****************************************************************************

class LoggingStore(Store):
    def __init__(self):
        super(LoggingStore, self).__init__()

    def getAllLogging(self, level: str) -> List[str]:
        Q = Query()
        results = self._db.search((Q.pillar == level))
        if (results is not None): 
            blobs = self._sortMostRecent(results)
            return [({ "ts": blob["timestamp"], "log": blob["doc"] }) for blob in blobs]
        return None

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
        Q = Query()
        results = self._db.search((Q.pillar == t))
        if (results is not None):
            blobs = self._sortMostRecent(results)
            return [WfEvent.deserialize(blob["doc"]) for blob in blobs]
        return None

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

    def _getAllJobStatuses(self) -> List[JobStatus]:
        try:
            Q = Query()
            results = self._db.search((Q.pillar == "run.status"))
            if (results is not None): 
                blobs = self._sortMostRecent(results)
                return [JobStatus.deserialize(blob["doc"]) for blob in blobs]
            return None
        except Exception as e:
            self._loggingStore.putLogging("ERROR", "Error in getAllJobStatuses: " + str(e))
            return None        


    def getAllJobStatuses(self, jobId: str) -> List[JobStatus]:
        if (jobId is None):
            return self._getAllJobStatuses()
        try:
            Q = Query()
            results = self._db.search((Q.pillar == "run.status") & (Q.key == jobId))
            if (results is not None): 
                blobs = self._sortMostRecent(results)
                return [JobStatus.deserialize(blob["doc"]) for blob in blobs]
            return None
        except Exception as e:
            self._loggingStore.putLogging("ERROR", "Error in getAllJobStatuses: " + str(e))
            return None
        
    def getJobStatus(self, jobId: str) -> JobStatus:    
        try:
            statuses = self.getAllJobStatuses(jobId)
            if (statuses is not None):
                return statuses[0]
            else:
                return None
        except Exception as e:
            self._loggingStore.putLogging("ERROR", "Error in getJobStatus: " + str(e))
            return None


        
# ****************************************************************************
# testing 

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python Store.py <type>")
        sys.exit(1)
    if (sys.argv[1] == "auth"):
        authStore = AuthStore()
        print(authStore.getAllAuth())
    elif (sys.argv[1] == "run.log.ERROR") or (sys.argv[1] == "run.log.INFO"):
        logStore = LoggingStore()
        for log in logStore.getAllLogging(sys.argv[1]):
            print(log)
    elif (sys.argv[1] == "run.status"):
        statusStore = JobStatusStore()
        for status in statusStore.getAllJobStatuses(None):
            print(status)


    









    





    

