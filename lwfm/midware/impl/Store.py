"""
Data stores for job status, metadata, logging, and workflow events.
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring
#pylint: disable = broad-exception-caught

import os
import time

from typing import List
from tinydb import TinyDB, Query
from tinydb.table import Document


from ...util.IdGenerator import IdGenerator
from ...base.JobStatus import JobStatus
from ...base.Metasheet import Metasheet
from ...base.WfEvent import WfEvent
from ...util.ObjectSerializer import ObjectSerializer



# ****************************************************************************
_DB_FILE = os.path.join(os.path.expanduser("~"), ".lwfm", "lwfm.repo")



class Store():
    _db = TinyDB(_DB_FILE)

    def _put(self, siteName: str, pillar: str, key: str, mydoc: str,
             collapse_doc: bool = False) -> None:
        try:
            db_id = IdGenerator().generateInteger()
            ts = time.perf_counter_ns()
            if (key is None) or (key == ""):
                key = ts
            baseRecord = {
                "_db_id": db_id,
                "_site": siteName,
                "_pillar": pillar,
                "_key": key,
                "_timestamp": ts
            }
            if collapse_doc:
                record = {**baseRecord, **mydoc}
            else:
                record = baseRecord
                record["_doc"] = mydoc    # the data, serialized object, etc
            print(f"* insert {record}")
            self._db.insert(Document(record, doc_id=db_id))
            return
        except Exception as ex:
            print("Error in _put: " + str(ex))


    def _sortMostRecent(self, docs: List[dict]) -> List[dict]:
        return sorted(docs, key=lambda x: x['_timestamp'], reverse=True)


# ****************************************************************************

class AuthStore(Store):

    def getAllAuth(self) -> List[str]:
        Q = Query()
        results = self._db.search((Q._pillar == "auth"))
        if results is not None:
            blobs = self._sortMostRecent(results)
            return [({ "site": blob["_site"], "auth": blob["_doc"] }) for blob in blobs]
        return None

    # return the site-specific auth blob for this site
    def getAuthForSite(self, siteName: str) -> str:
        Q = Query()
        result = self._db.search((Q._site == siteName) & (Q._pillar == "auth") & (Q._key == "auth"))
        if result is not None:
            return result[0]["_doc"]
        return None

    # set the site-specific auth blob for this site
    def putAuthForSite(self, siteName: str, doc: str) -> None:
        self._put(siteName, "auth", "auth", doc)


# ****************************************************************************

class LoggingStore(Store):

    def getAllLogging(self, level: str) -> List[str]:
        Q = Query()
        results = self._db.search((Q._pillar == level))
        if results is not None:
            blobs = self._sortMostRecent(results)
            return [({ "ts": blob["_timestamp"], "log": blob["_doc"] }) for blob in blobs]
        return None

    # put a record in the logging store
    def putLogging(self, level: str, mydoc: str) -> None:
        self._put("local", "run.log." + level, None, mydoc)


# ****************************************************************************

class EventStore(Store):
    _loggingStore: LoggingStore = None

    def __init__(self):
        super().__init__()
        self._loggingStore = LoggingStore()

    def putWfEvent(self, datum: WfEvent, typeT: str) -> bool:
        try:
            self._put(datum.getFireSite(), "run.event." + typeT,
                      datum.getEventId(), ObjectSerializer.serialize(datum))
            return True
        except Exception as e:
            self._loggingStore.putLogging("ERROR", "Error in putWfEvent: " + str(e))
            return False

    def getAllWfEvents(self, typeT: str = None) -> List[WfEvent]:
        Q = Query()
        results = self._db.search((Q._pillar == typeT))
        if results is not None:
            blobs = self._sortMostRecent(results)
            return [ObjectSerializer.deserialize(blob["_doc"]) for blob in blobs]
        return None

    def deleteAllWfEvents(self) -> None:
        q = Query()
        self._db.remove(q._pillar == 'run.event')

    def deleteWfEvent(self, eventId: str) -> bool:
        try:
            q = Query()
            self._db.remove(q._key == eventId)
            return True
        except Exception as e:
            self._loggingStore.putLogging("ERROR", "Error in deleteWfEvent: " + str(e))
            return False


# ****************************************************************************

class JobStatusStore(Store):
    _loggingStore: LoggingStore = None

    def __init__(self):
        super().__init__()
        self._loggingStore = LoggingStore()

    def putJobStatus(self, datum: JobStatus) -> None:
        self._put(datum.getJobContext().getSiteName(),
                  "run.status", datum.getJobId(), ObjectSerializer.serialize(datum))

    def _getAllJobStatuses(self) -> List[JobStatus]:
        try:
            Q = Query()
            results = self._db.search((Q._pillar == "run.status"))
            if results is not None:
                blobs = self._sortMostRecent(results)
                return [ObjectSerializer.deserialize(blob["_doc"]) for blob in blobs]
            return None
        except Exception as e:
            self._loggingStore.putLogging("ERROR", "Error in getAllJobStatuses: " + str(e))
            return None


    def getAllJobStatuses(self, jobId: str) -> List[JobStatus]:
        if jobId is None:
            return self._getAllJobStatuses()
        try:
            Q = Query()
            results = self._db.search((Q._pillar == "run.status") & (Q._key == jobId))
            if results is not None:
                blobs = self._sortMostRecent(results)
                return [ObjectSerializer.deserialize(blob["_doc"]) for blob in blobs]
            return None
        except Exception as e:
            self._loggingStore.putLogging("ERROR", "Error in getAllJobStatuses: " + str(e))
            return None

    def getJobStatus(self, jobId: str) -> JobStatus:
        try:
            statuses = self.getAllJobStatuses(jobId)
            if (statuses is not None) and (len(statuses) > 0):
                return statuses[0]
            else:
                return None
        except Exception as e:
            self._loggingStore.putLogging("ERROR", "Error in getJobStatus: " + str(e))
            return None



# ****************************************************************************
# MetaRepo Store

class MetaRepoStore(Store):
    _loggingStore: LoggingStore = None

    def __init__(self):
        super().__init__()
        self._loggingStore = LoggingStore()

    def putMetaRepo(self, datum: Metasheet) -> None:
        self._put("None", "repo.meta", datum.getId(), datum.getArgs(), True)

    def getAllMetasheets(self) -> List[Metasheet]:
        Q = Query()
        results = self._db.search((Q._pillar == "repo.meta"))
        if results is not None:
            return [Metasheet(blob) for blob in results]

    def find(self, queryRegExs: dict) -> List[Metasheet]:
        try:
            qStr = None
            for (k, v) in queryRegExs.items():
                if qStr is None:
                    qStr = "where('" + k + "') == '" + v + "'"
                else:
                    qStr = qStr + " and where('" + k + "') == '" + v + "'"
            blobs = self._db.search(eval(qStr))
            if blobs is not None:
                return [Metasheet(blob) for blob in blobs]
            return None
        except Exception as e:
            self._loggingStore.putLogging("ERROR", "Error in find: " + str(e))
            return None


# ****************************************************************************
# testing

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python Store.py <type>")
        sys.exit(1)
    if sys.argv[1] == "auth":
        authStore = AuthStore()
        print(authStore.getAllAuth())
    elif (sys.argv[1] == "run.log.ERROR") or (sys.argv[1] == "run.log.INFO"):
        logStore = LoggingStore()
        for log in logStore.getAllLogging(sys.argv[1]):
            print(log)
    elif sys.argv[1] == "run.status":
        statusStore = JobStatusStore()
        for status in statusStore.getAllJobStatuses(None):
            print(status)
    elif sys.argv[1] == "repo.meta":
        metaStore = MetaRepoStore()
        for meta in metaStore.getAllMetasheets():
            print(meta)
    elif sys.argv[1].startswith("run.event"):
        eventStore = EventStore()
        for event in eventStore.getAllWfEvents(sys.argv[1]):
            print(event)
    elif sys.argv[1] == "all":
        store = Store()
        for doc in store._db.all():
            print(f"*** {doc}")
    else:
        print("Unknown type: " + sys.argv[1])
