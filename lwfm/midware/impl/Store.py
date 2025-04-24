"""
Data stores for job status, metadata, logging, and workflow events.
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring
#pylint: disable = broad-exception-caught, global-statement

import os
import time
import datetime

from typing import List
import sqlite3

from lwfm.base.JobStatus import JobStatus
from lwfm.base.WfEvent import WfEvent
from lwfm.base.Workflow import Workflow
from lwfm.util.ObjectSerializer import ObjectSerializer


# ****************************************************************************
_DB_FILE = os.path.join(os.path.expanduser("~"), ".lwfm", "lwfm.db")
_SCHEMA_CREATED = False

class Store:
    def __init__(self):
        global _SCHEMA_CREATED
        if not _SCHEMA_CREATED:
            self.createSchema()
            _SCHEMA_CREATED = True

    def getDBFilePath(self) -> str:
        return _DB_FILE

    def createSchema(self) -> None:
        db = sqlite3.connect(_DB_FILE)
        cur = db.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS AuthStore ( " \
            "id INTEGER PRIMARY KEY, "\
            "ts INTEGER, "\
            "site TEXT, " \
            "pillar TEXT, " \
            "key TEXT, " \
            "data TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS WorkflowStore ( " \
            "id INTEGER PRIMARY KEY, "\
            "ts INTEGER, "\
            "site TEXT, " \
            "pillar TEXT, " \
            "key TEXT, " \
            "data TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS LoggingStore ( " \
            "id INTEGER PRIMARY KEY, "\
            "ts INTEGER, "\
            "site TEXT, " \
            "pillar TEXT, " \
            "key TEXT, " \
            "data TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS EventStore ( " \
            "id INTEGER PRIMARY KEY, "\
            "ts INTEGER, "\
            "site TEXT, " \
            "pillar TEXT, " \
            "key TEXT, " \
            "data TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS JobStatusStore ( " \
            "id INTEGER PRIMARY KEY, "\
            "ts INTEGER, "\
            "site TEXT, " \
            "pillar TEXT, " \
            "key TEXT, " \
            "data TEXT)")
        db.commit()
        db.close()

    def _put(self, table: str, siteName: str, pillar: str, key: str, data: str) -> None:
        import time as _time
        max_retries = 5
        delay = 0.1  # seconds
        ts = time.perf_counter_ns()
        if (key is None) or (key == ""):
            key = ts
        for attempt in range(max_retries):
            try:
                db = sqlite3.connect(_DB_FILE)
                db.cursor().execute(
                    "INSERT INTO " + table + " (ts, site, pillar, key, data) VALUES (?, ?, ?, ?, ?)",
                    (ts, siteName, pillar, key, data)
                )
                db.commit()
                db.close()
                return
            except sqlite3.OperationalError as ex:
                if "database is locked" in str(ex):
                    if attempt < max_retries - 1:
                        _time.sleep(delay)
                        continue
                print(f"Error in _put: for {siteName} {pillar} {key} {ex}")
                break
            except Exception as ex:
                print(f"Error in _put: for {siteName} {pillar} {key} {ex}")
                break
            finally:
                if db:
                    db.close()
                break


# ****************************************************************************

class AuthStore(Store):

    # return the site-specific auth blob for this site
    def getAuthForSite(self, siteName: str) -> str:
        try:
            db = sqlite3.connect(_DB_FILE)
            cur = db.cursor()
            res = cur.execute(f"SELECT data FROM AuthStore WHERE pillar='auth' and " \
                f"site='{siteName}' and key='auth' order by ts desc")
            result = res.fetchone()
            if result is not None:
                result = result[0]
            db.close()
            return result
        except Exception as ex:
            print(f"Error in getAuthForSite: {ex}")
            return None
        finally:
            if db:
                db.close()

    # set the site-specific auth blob for this site
    def putAuthForSite(self, siteName: str, doc: str) -> None:
        self._put("AuthStore", siteName, "auth", "auth", doc)


# ****************************************************************************


class WorkflowStore(Store):

    # return the site-specific auth blob for this site
    def getWorkflow(self, workflow_id: str) -> Workflow:
        try:
            db = sqlite3.connect(_DB_FILE)
            cur = db.cursor()
            res = cur.execute(f"SELECT data FROM WorkflowStore WHERE pillar='run.wf' and " \
                f"site='local' and key='{workflow_id}' order by ts desc")
            result = res.fetchone()
            if result is not None:
                result = ObjectSerializer.deserialize(result[0])
            db.close()
            return result
        except Exception as e:
            print(f"Error in getWorkflow: {e}")
            return None
        finally:
            if db:
                db.close()

    # set the site-specific auth blob for this site
    def putWorkflow(self, workflow: Workflow) -> None:
        self._put("WorkflowStore", "local", "run.wf", workflow.getWorkflowId(),
            ObjectSerializer.serialize(workflow))


# ****************************************************************************

class LoggingStore(Store):

    _ECHO_STDIO = True

    # put a record in the logging store
    def putLogging(self, level: str, mydoc: str) -> None:
        if self._ECHO_STDIO:
            print(f"{datetime.datetime.now()} {level} {mydoc}")
        self._put("LoggingStore", "local", "run.log." + level, None, mydoc)


# ****************************************************************************

class EventStore(Store):

    def putWfEvent(self, datum: WfEvent, typeT: str) -> None:
        self._put("EventStore", datum.getFireSite(), "run.event." + typeT,
            datum.getEventId(), ObjectSerializer.serialize(datum))

    def getAllWfEvents(self, typeT: str = None) -> List[WfEvent]:
        try:
            db = sqlite3.connect(_DB_FILE)
            cur = db.cursor()
            if typeT is not None:
                res = cur.execute(
                    "SELECT data FROM EventStore WHERE pillar=? ORDER BY ts DESC",
                    ("run.event." + typeT,)
                )
            else:
                res = cur.execute(
                    "SELECT data FROM EventStore WHERE pillar LIKE 'run.event.%' ORDER BY ts DESC"
                )
            rows = res.fetchall()
            if rows:
                result = [ObjectSerializer.deserialize(row[0]) for row in rows]
            else:
                result = None
            db.close()
            return result
        except Exception as e:
            print(f"Error in getAllWfEvents: {e}")
            return None
        finally:
            if db:
                db.close()

    def deleteWfEvent(self, eventId: str) -> None:
        import time as _time
        max_retries = 5
        delay = 0.1  # seconds
        for attempt in range(max_retries):
            try:
                db = sqlite3.connect(_DB_FILE)
                cur = db.cursor()
                cur.execute(
                    "DELETE FROM EventStore WHERE key=? AND pillar LIKE 'run.event.%'",
                    (eventId,)
                )
                db.commit()
                db.close()
                return
            except sqlite3.OperationalError as ex:
                if db:
                    db.close()
                if "database is locked" in str(ex):
                    if attempt < max_retries - 1:
                        _time.sleep(delay)
                        continue
                print(f"Error in deleteWfEvent: {ex}")
                break
            except Exception as ex:
                if db:
                    db.close()
                print(f"Error in deleteWfEvent: {ex}")
                break
            finally:
                if db:
                    db.close()


# ****************************************************************************

class JobStatusStore(Store):

    def putJobStatus(self, datum: JobStatus) -> None:
        self._put("JobStatusStore", datum.getJobContext().getSiteName(),
                  "run.status", datum.getJobId(), ObjectSerializer.serialize(datum))

    def getAllJobStatuses(self, jobId: str) -> List[JobStatus]:
        if jobId is None:
            return None
        try:
            db = sqlite3.connect(_DB_FILE)
            cur = db.cursor()
            res = cur.execute(
                "SELECT data FROM JobStatusStore WHERE pillar=? AND key=? ORDER BY ts DESC",
                ("run.status", jobId)
            )
            rows = res.fetchall()
            if rows:
                result = [ObjectSerializer.deserialize(row[0]) for row in rows]
            else:
                result = None
            db.close()
            return result
        except Exception as e:
            # Optionally log error if you have a logging mechanism
            print(f"Error in getAllJobStatuses: {e}")
            return None
        finally:
            if db:
                db.close()

    def getJobStatus(self, jobId: str) -> JobStatus:
        try:
            db = sqlite3.connect(_DB_FILE)
            cur = db.cursor()
            res = cur.execute(
                "SELECT data FROM JobStatusStore WHERE pillar=? AND key=? ORDER BY ts DESC LIMIT 1",
                ("run.status", jobId)
            )
            row = res.fetchone()
            if row:
                result = ObjectSerializer.deserialize(row[0])
            else:
                result = None
            db.close()
            return result
        except Exception as e:
            print(f"Error in getJobStatus: {e}")
            return None
        finally:
            if db:
                db.close()


# ****************************************************************************
# MetaRepo Store

class MetaRepoStore(Store):
    pass
    # def putMetaRepo(self, datum: Metasheet) -> None:
    #     self._put("None", "repo.meta", datum.getSheetId(), datum.getProps())

    # def getAllMetasheets(self) -> List[Metasheet]:
    #     Q = Query()
    #     results = self._db.search((Q._pillar == "repo.meta"))
    #     if results is not None:
    #         return [Metasheet(blob) for blob in results]

    # def find(self, queryRegExs: dict) -> List[Metasheet]:
    #     try:
    #         qStr = None
    #         for (k, v) in queryRegExs.items():
    #             if qStr is None:
    #                 qStr = "where('" + k + "') == '" + v + "'"
    #             else:
    #                 qStr = qStr + " and where('" + k + "') == '" + v + "'"
    #         blobs = self._db.search(eval(qStr))
    #         if blobs is not None:
    #             return [Metasheet(blob) for blob in blobs]
    #         return None
    #     except Exception as e:
    #         self._loggingStore.putLogging("ERROR", "Error in find: " + str(e))
    #         return None


# ****************************************************************************
# test code

if __name__ == "__main__":
    # Minimal test code for Store and its subclasses
    print("Testing Store and subclasses with sqlite backend")

    # Test LoggingStore
    log_store = LoggingStore()
    log_store.putLogging("INFO", "This is a test log entry.")

    print("Test complete.")
