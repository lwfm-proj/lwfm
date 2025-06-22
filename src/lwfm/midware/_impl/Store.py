"""
Data stores for job status, metadata, logging, and workflow events.
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring
#pylint: disable = broad-exception-caught, global-statement

import os
import time as _time
import datetime
import json
import re

from typing import List, Optional
import sqlite3

from lwfm.base.JobStatus import JobStatus
from lwfm.base.WorkflowEvent import WorkflowEvent
from lwfm.base.Workflow import Workflow
from lwfm.base.Metasheet import Metasheet
from lwfm.midware._impl.ObjectSerializer import ObjectSerializer



def regexp(expr, item):
    if item is None:
        return False
    return re.search(expr, item) is not None


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
        cur.execute("CREATE TABLE IF NOT EXISTS MetasheetStore ( " \
            "id INTEGER PRIMARY KEY, "\
            "ts INTEGER, "\
            "site TEXT, " \
            "pillar TEXT, " \
            "key TEXT, " \
            "data TEXT)")
        db.commit()
        db.close()

    def _put(self, table: str, siteName: str, pillar: str, key: Optional[str], data: str) -> None:
        max_retries = 5
        delay = 0.1  # seconds
        ts = _time.perf_counter_ns()
        if (key is None) or (key == ""):
            key = str(ts)
        db = None
        for attempt in range(max_retries):
            try:
                db = sqlite3.connect(_DB_FILE)
                db.cursor().execute(
                    "INSERT INTO " + table + \
                    " (ts, site, pillar, key, data) VALUES (?, ?, ?, ?, ?)",
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


# ****************************************************************************

class WorkflowStore(Store):

    # return the site-specific auth blob for this site
    def getWorkflow(self, workflow_id: str) -> Optional[Workflow]:
        db = None
        try:
            db = sqlite3.connect(_DB_FILE)
            cur = db.cursor()
            results = cur.execute(f"SELECT data FROM WorkflowStore WHERE pillar='run.wf' and " \
                f"site='local' and key='{workflow_id}' order by ts desc")
            result = results.fetchone()
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

    def putWfEvent(self, datum: WorkflowEvent, typeT: str) -> None:
        self._put("EventStore", datum.getFireSite(), "run.event." + typeT,
            datum.getEventId(), ObjectSerializer.serialize(datum))

    def getAllWfEvents(self, typeT: Optional[str]) -> Optional[List[WorkflowEvent]]:
        db = None
        try:
            db = sqlite3.connect(_DB_FILE)
            cur = db.cursor()
            if typeT is not None:
                results = cur.execute(
                    "SELECT data FROM EventStore WHERE pillar=? ORDER BY ts DESC",
                    (typeT,)
                )
            else:
                results = cur.execute(
                    "SELECT data FROM EventStore ORDER BY ts DESC"
                )
            rows = results.fetchall()
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
        max_retries = 5
        delay = 0.1  # seconds
        db = None
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
        if datum is None or datum.getJobContext() is None:
            self._put("LoggingStore", "local", "run.log." + "ERROR", None,
                "Store.putJobStatus called with None datum")
            return
        jobId = datum.getJobId()
        if jobId is None:
            self._put("LoggingStore", "local", "run.log." + "ERROR", None,
                "Store.putJobStatus called with datum that has no job ID")
            return
        self._put("JobStatusStore", datum.getJobContext().getSiteName(),
                  "run.status", jobId, ObjectSerializer.serialize(datum))

    def getAllJobStatuses(self, jobId: str) -> Optional[List[JobStatus]]:
        if jobId is None:
            return None
        db = None
        try:
            db = sqlite3.connect(_DB_FILE)
            cur = db.cursor()
            results = cur.execute(
                "SELECT data FROM JobStatusStore WHERE pillar=? AND key=? ORDER BY ts DESC",
                ("run.status", jobId)
            )
            rows = results.fetchall()
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

    def getJobStatus(self, jobId: str) -> Optional[JobStatus]:
        db = None
        try:
            db = sqlite3.connect(_DB_FILE)
            cur = db.cursor()
            results = cur.execute(
                "SELECT data FROM JobStatusStore WHERE pillar=? AND key=? ORDER BY ts DESC LIMIT 1",
                ("run.status", jobId)
            )
            row = results.fetchone()
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
# Metasheet Store

class MetasheetStore(Store):

    def putMetasheet(self, datum: Metasheet) -> None:
        keys = datum.getProps()
        if keys is None:
            keys = {}
        keys["jobId"] = datum.getJobId()
        keys["site"] = datum.getSiteName()
        keys["url"] = datum.getSiteUrl()
        keys["sheetId"] = datum.getSheetId()
        self._put("MetasheetStore", datum.getSiteName(), "repo.meta",
            json.dumps(keys), ObjectSerializer.serialize(datum))

    def findMetasheet(self, queryRegExs: dict) -> List[Metasheet]:
        db = None
        try:
            db = sqlite3.connect(_DB_FILE)
            db.create_function("REGEXP", 2, lambda expr,
                val: re.search(expr, val or "") is not None)
            cur = db.cursor()

            # Build SQL WHERE clause
            where_clauses = []
            params = []
            for k, regex in queryRegExs.items():
                # This pattern matches the JSON key and value as a substring
                # e.g. "jobId": "abc123"
                pattern = f'"{k}"\\s*:\\s*"[^"]*{regex}[^"]*"'
                where_clauses.append('key REGEXP ?')
                params.append(pattern)

            sql = "SELECT key, data FROM MetasheetStore"
            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)

            cur.execute(sql, params)
            results = []
            for _, data in cur.fetchall():
                # Deserialize as needed for your Metasheet constructor
                ms = ObjectSerializer.deserialize(data)
                results.append(ms)
            return results
        except Exception as e:
            LoggingStore().putLogging("ERROR", f"Error in findMetasheet: {e}")
            return []
        finally:
            if db:
                db.close()
