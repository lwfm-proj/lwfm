"""
Data stores for job status, metadata, logging, and workflow events.

Little effort is being made here to optimize the database schema or queries.
This is a reference implementation.

The Store is a singleton in the lwfm instance, parked behind a REST service
LwfmEventSvc that is used by LwfmEventClient to access the store. Workflow authors
(i.e users) are not aware of these layers, iteracting exclusively with Site(s) and 
the lwfManager.

Authors of Site drivers will similarly interact solely with the Site parent classes
and the lwfManager.

Thus these service layers should be refactorable freely.

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


# ****************************************************************************
_DB_FILE = os.path.join(os.path.expanduser("~"), ".lwfm", "lwfm.db")
_SCHEMA_CREATED = False

class Store:
    def __init__(self):
        global _SCHEMA_CREATED
        if not _SCHEMA_CREATED:
            # Be resilient on first run: ensure DB dir exists and handle errors gracefully
            try:
                self.createSchema()
                _SCHEMA_CREATED = True
            except Exception as ex:
                # Don't raise here to avoid spamming logs on periodic GUI refresh.
                # We'll retry schema creation on the next Store instantiation.
                print(f"Error creating schema: {ex}")

    def getDBFilePath(self) -> str:
        """
        Get the path to the database file.
        """
        return _DB_FILE

    def createSchema(self) -> None:
        """
        Create the database schema if it does not exist.
        """
        # Ensure parent directory exists on first run (e.g., ~/.lwfm)
        db_dir = os.path.dirname(_DB_FILE)
        try:
            if db_dir and not os.path.isdir(db_dir):
                os.makedirs(db_dir, exist_ok=True)
        except Exception as ex:
            # If we cannot create the directory, surface a clear message and bail early
            raise RuntimeError(f"Unable to create DB directory '{db_dir}': {ex}") from ex

        db = sqlite3.connect(_DB_FILE)
        cur = db.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS WorkflowStore ( " \
            # pk is generated id
            "id INTEGER PRIMARY KEY, "\
            "ts INTEGER, "\
            "site TEXT, " \
            "pillar TEXT, " \
            "workflowId TEXT, " \
            # key = workflowId
            "key TEXT, " \
            "data TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS LoggingStore ( " \
            # pk is generated id
            "id INTEGER PRIMARY KEY, "\
            "ts INTEGER, "\
            "site TEXT, " \
            "pillar TEXT, " \
            "workflowId TEXT, " \
            # key = jobId
            "key TEXT, " \
            "data TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS EventStore ( " \
            # pk is generated id
            "id INTEGER PRIMARY KEY, "\
            "ts INTEGER, "\
            "site TEXT, " \
            "pillar TEXT, " \
            "workflowId TEXT, " \
            # key = eventId  TODO
            "key TEXT, " \
            "data TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS JobStatusStore ( " \
            # pk is generated id
            "id INTEGER PRIMARY KEY, "\
            "ts INTEGER, "\
            "site TEXT, " \
            "pillar TEXT, " \
            "workflowId TEXT, " \
            # key = jobId
            "key TEXT, " \
            "data TEXT)")
        cur.execute("CREATE TABLE IF NOT EXISTS MetasheetStore ( " \
            # pk is generated id
            "id INTEGER PRIMARY KEY, "\
            "ts INTEGER, "\
            "site TEXT, " \
            "pillar TEXT, " \
            "workflowId TEXT, " \
            # key = jobId
            "key TEXT, " \
            "data TEXT)")
        db.commit()
        db.close()

    def _put(self, store: str, siteName: str, pillar: str, workflowId: Optional[str] = None,
             key: Optional[str] = None, data: Optional[str] = None) -> None:
        """
        Generalized write method for all stores.
        """
        max_retries = 5
        delay = 0.1  # seconds
        ts = _time.perf_counter_ns()
        if (key is None) or (key == "") or not key:
            key = str(ts)
        if workflowId is None:
            workflowId = "" # Default to empty string if no workflowId provided
        if data is None:
            data = ""
        db = None
        for attempt in range(max_retries):
            try:
                db = sqlite3.connect(_DB_FILE)
                db.cursor().execute(
                    "INSERT INTO " + store + \
                    " (ts, site, pillar, workflowId, key, data) VALUES (?, ?, ?, ?, ?, ?)",
                    (ts, siteName, pillar, workflowId, key, data)
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
            results = cur.execute("SELECT data FROM WorkflowStore WHERE pillar='run.wf' and " \
                "workflowId=? order by ts desc", (workflow_id,))
            result = results.fetchone()
            if result is not None:
                result = ObjectSerializer.deserialize(result[0])
            db.close()
            return result
        except Exception as e:
            print(f"Error in getWorkflow: {e}") # TODO logging
            return None
        finally:
            if db:
                db.close()

    def getAllWorkflows(self) -> Optional[List[Workflow]]:
        """
        Get all workflows stored in the database, ordered by timestamp (newest first).
        Workflows are relatively lightweight objects, so this should be efficient enough 
        vs. returning just the ids.
        :return: List of Workflow objects or None if no workflows found
        """
        db = None
        try:
            db = sqlite3.connect(_DB_FILE)
            cur = db.cursor()
            results = cur.execute("SELECT data FROM WorkflowStore WHERE " \
                                  "pillar='run.wf' " \
                                  "ORDER BY ts DESC")
            rows = results.fetchall()
            if rows:
                result = [ObjectSerializer.deserialize(row[0]) for row in rows]
            else:
                result = None
            db.close()
            return result
        except Exception as e:
            print(f"Error in getAllWorkflows: {e}") # TODO logging
            return None
        finally:
            if db:
                db.close()


    def putWorkflow(self, workflow: Workflow) -> None:
        if workflow is None:
            return
        self._put("WorkflowStore", "local", "run.wf", workflow.getWorkflowId(),
                json.dumps(workflow.getProps()), ObjectSerializer.serialize(workflow))



    def findWorkflows(self, queryRegExs: dict) -> List[Workflow]:
        db = None
        try:
            db = sqlite3.connect(_DB_FILE)
            db.create_function("REGEXP", 2, lambda expr,
                val: re.search(expr, val or "") is not None)
            cur = db.cursor()

            # Build SQL WHERE clause
            where_clauses = []
            params = []
            pattern = ""
            for k, regex in queryRegExs.items():
                # This pattern matches the JSON key and value as a substring
                # e.g. "jobId": "abc123"
                pattern = f'"{k}"\\s*:\\s*"[^"]*{regex}[^"]*"'
                where_clauses.append('key REGEXP ?')
                params.append(pattern)

            # Use a subquery to get only the most recent version of each workflow
            # by grouping by workflowId and selecting MAX(ts)
            sql = """SELECT w1.workflowId, w1.data 
                     FROM WorkflowStore w1 
                     INNER JOIN (
                         SELECT workflowId, MAX(ts) as max_ts 
                         FROM WorkflowStore 
                         WHERE pillar='run.wf'
                         GROUP BY workflowId
                     ) w2 ON w1.workflowId = w2.workflowId AND w1.ts = w2.max_ts
                     WHERE w1.pillar='run.wf'"""

            if where_clauses:
                sql += " AND " + " AND ".join([f"w1.{clause}" for clause in where_clauses])

            cur.execute(sql, params)
            results = []
            for _, data in cur.fetchall():
                # Deserialize the workflow object
                wf = ObjectSerializer.deserialize(data)
                results.append(wf)
            return results
        except Exception as e:
            LoggingStore().putLogging("ERROR", f"Error in findWorkflows: {e}",
                                      "", "", "")
            return []
        finally:
            if db:
                db.close()



# ****************************************************************************

class LoggingStore(Store):

    # Enable or disable echoing log messages to standard output
    # This can be useful for debugging or development purposes
    # Set to False to disable echoing to stdout.
    _ECHO_STDIO = True

    def putLogging(self, level: str, mydoc: str,
                   site: str, workflowId: str, jobId: str) -> None:
        """
        Put a log message in the store.
        :param level: The log level (e.g., "INFO", "ERROR", etc)
        :param mydoc: The log message to store
        :return: None
        """
        if self._ECHO_STDIO:
            print(f"{datetime.datetime.now()} {site} {jobId} {level} {mydoc}")
        self._put("LoggingStore", site, "run.log." + level, workflowId, jobId, mydoc)
        
        # Also append to job log file if jobId is present
        if jobId:
            try:
                from lwfm.midware._impl.SiteConfig import SiteConfig
                log_dir = os.path.expanduser(SiteConfig.getLogFilename())
                os.makedirs(log_dir, exist_ok=True)
                log_path = os.path.join(log_dir, f"{jobId}.log")
                with open(log_path, 'a', encoding='utf-8') as f:
                    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    f.write(f"[{timestamp}] [{level}] {mydoc}\n")
            except Exception as ex:
                # Don't fail the logging operation if file write fails
                if self._ECHO_STDIO:
                    print(f"Warning: Could not write to job log file: {ex}")


    def getLogsByWorkflow(self, workflowId: Optional[str] = None) -> Optional[List[str]]:
        """
        Get all log messages for a specific workflow, ordered by timestamp (newest first).
        """
        db = None
        try:
            db = sqlite3.connect(_DB_FILE)
            cur = db.cursor()
            if workflowId is not None:
                results = cur.execute(
                    "SELECT data FROM LoggingStore WHERE workflowId=? ORDER BY ts DESC",
                    (workflowId,)
                )
                rows = results.fetchall()
                if rows:
                    result = [row[0] for row in rows]
                else:
                    result = None
                db.close()
                return result
        except Exception as e:
            print(f"Error in getAllLogsByWorkflow: {e}")
            return None
        finally:
            if db:
                db.close()


    def getLogsByJob(self, jobId: Optional[str] = None) -> Optional[List[str]]:
        """
        Get all log messages for a specific job, ordered by timestamp (newest first).
        :param site: The site name
        :param jobId: The job ID to filter logs by
        :return: List of log messages or None if no logs found
        """
        db = None
        try:
            db = sqlite3.connect(_DB_FILE)
            cur = db.cursor()
            results = cur.execute(
                "SELECT data FROM LoggingStore WHERE key=? ORDER BY ts DESC",
                (jobId,)
            )
            rows = results.fetchall()
            if rows:
                result = [row[0] for row in rows]
            else:
                result = None
            db.close()
            return result
        except Exception as e:
            print(f"Error in getAllLogsByJob: {e}")
            return None
        finally:
            if db:
                db.close()


    def getAllLogs(self) -> Optional[List[str]]:
        """
        Get all log messages from the system, ordered by timestamp (newest first).
        :return: List of all log messages or None if no logs found
        """
        db = None
        try:
            db = sqlite3.connect(_DB_FILE)
            cur = db.cursor()
            results = cur.execute(
                "SELECT ts, site, pillar, workflowId, key, data FROM LoggingStore ORDER BY ts DESC"
            )
            rows = results.fetchall()
            if rows:
                result = []
                for row in rows:
                    ts, site, pillar, workflowId, key, data = row
                    # Convert timestamp to readable format
                    readable_time = datetime.datetime.fromtimestamp(ts / 1e9).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                    log_entry = f"{readable_time} | {site} | {pillar} | WF:{workflowId} | Job:{key} | {data}"
                    result.append(log_entry)
            else:
                result = None
            db.close()
            return result
        except Exception as e:
            print(f"Error in getAllLogs: {e}")
            return None
        finally:
            if db:
                db.close()


# ****************************************************************************

class EventStore(Store):

    def putWfEvent(self, datum: WorkflowEvent, typeT: str) -> None:
        self._put("EventStore", datum.getFireSite(), "run.event." + typeT,
            datum.getWorkflowId(),
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
        """
        Put a job status record in the store.
        """
        if datum is None or datum.getJobContext() is None:
            self._put("LoggingStore", "local", "run.log." + "ERROR",
                    datum.getJobContext().getWorkflowId(),
                    None,
                    "Store.putJobStatus called with None datum")
            return
        jobId = datum.getJobId()
        if jobId is None:
            self._put("LoggingStore", "local", "run.log." + "ERROR",
                    datum.getJobContext().getWorkflowId(),
                    None,
                    "Store.putJobStatus called with datum that has no job ID")
            return
        self._put("JobStatusStore", datum.getJobContext().getSiteName(),
                  "run.status", datum.getJobContext().getWorkflowId(), jobId, 
                  ObjectSerializer.serialize(datum))
        
        # Also append to job log file
        try:
            from lwfm.midware._impl.SiteConfig import SiteConfig
            log_dir = os.path.expanduser(SiteConfig.getLogFilename())
            os.makedirs(log_dir, exist_ok=True)
            log_path = os.path.join(log_dir, f"{jobId}.log")
            with open(log_path, 'a', encoding='utf-8') as f:
                timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                status_line = f"[{timestamp}] [STATUS] {datum.getStatus()}"
                if datum.getNativeStatusStr():
                    status_line += f" (native: {datum.getNativeStatusStr()})"
                f.write(status_line + "\n")
                if datum.getNativeInfo():
                    # Indent native info for readability
                    f.write(f"  Details: {datum.getNativeInfo()}\n")
        except Exception as ex:
            # Don't fail the status operation if file write fails
            print(f"Warning: Could not write status to job log file: {ex}")


    def _getAllJobStatuses(self) -> Optional[List[JobStatus]]:
        """
        Get all job status messages, ordered by timestamp (newest first).
        For developer debugging. 
        """
        db = None
        try:
            db = sqlite3.connect(_DB_FILE)
            cur = db.cursor()
            results = cur.execute(
                "SELECT data FROM JobStatusStore WHERE pillar=? ORDER BY ts DESC",
                ("run.status", )
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



    def getJobStatuses(self, jobId: str) -> Optional[List[JobStatus]]:
        """
        Get all job status messages for a specific job, ordered by timestamp (newest first).
        """
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
        """
        Get the most recent job status for a specific job.
        """
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




    def getJobStatusesForWorkflow(self, workflow_id: str) -> Optional[List[JobStatus]]:
        """
        Get all job status messages for all jobs in a workflow, ordered by timestamp (newest first).
        """
        if workflow_id is None:
            return None

        db = None
        try:
            # Build SQL query to get all job statuses for these job IDs
            db = sqlite3.connect(_DB_FILE)
            cur = db.cursor()

            results = cur.execute(
                "SELECT data FROM JobStatusStore WHERE pillar='run.status' " + \
                "AND workflowId=? ORDER BY ts DESC",
                (workflow_id,)
            )

            rows = results.fetchall()
            if rows:
                result = [ObjectSerializer.deserialize(row[0]) for row in rows]
            else:
                result = None

            db.close()
            return result
        except Exception as e:
            LoggingStore().putLogging("ERROR", f"Error in getAllJobStatusesForWorkflow: {e}",
                                      "", workflow_id, "") # TODO add context
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
        keys["_sheetId"] = datum.getSheetId()
        self._put("MetasheetStore", datum.getSiteName(), "repo.meta",
            datum.getProps().get("_workflowId", ""),
            json.dumps(keys), ObjectSerializer.serialize(datum))


    def findMetasheets(self, queryRegExs: dict) -> List[Metasheet]:
        db = None
        try:
            db = sqlite3.connect(_DB_FILE)
            db.create_function("REGEXP", 2, lambda expr,
                val: re.search(expr, val or "") is not None)
            cur = db.cursor()

            # Build SQL WHERE clause
            where_clauses = []
            params = []
            pattern = ""
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
            LoggingStore().putLogging("ERROR", f"Error in findMetasheet: {e}",
                                      "", "", "")
            return []
        finally:
            if db:
                db.close()



if __name__ == "__main__":
    store = JobStatusStore()
    statuses = store._getAllJobStatuses()
    if statuses:
        for status in statuses:
            print(status)
    else:
        print("No job statuses found.")
