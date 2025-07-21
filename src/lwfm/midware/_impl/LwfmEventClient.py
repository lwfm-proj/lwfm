"""
implementation behind the LwfManager - use it instead
this gives the HTTP calls to the lwfm middleware to account for the case
where its not running on the same machine as the workflow
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring
#pylint: disable = broad-exception-caught, logging-not-lazy, logging-fstring-interpolation

from typing import List, Optional, cast
import os
import datetime

import logging # don't use the lwfm Logger here else circular import

import json
import requests

from lwfm.base.JobStatus import JobStatus
from lwfm.base.JobContext import JobContext
from lwfm.base.Metasheet import Metasheet
from lwfm.base.WorkflowEvent import WorkflowEvent
from lwfm.base.Workflow import Workflow
from lwfm.midware._impl.ObjectSerializer import ObjectSerializer
from lwfm.midware._impl.SiteConfig import SiteConfig
from lwfm.midware._impl.SvcLauncher import SvcLauncher

class LwfmEventClient:
    _REST_TIMEOUT = 100


    def getUrl(self) -> str:
        return self._SERVICE_URL


    # is the middleware running?
    def isMidwareRunning(self) -> bool:
        return SvcLauncher.isMidwareRunning(self.getUrl())


    def __init__(self) -> None:
        if os.getenv("LWFM_SERVICE_URL") is not None:
            self._SERVICE_URL: str = os.getenv("LWFM_SERVICE_URL", "127.0.0.1")
        else:
            host = SiteConfig.getSiteProperties("lwfm").get("host") or "127.0.0.1"
            port = SiteConfig.getSiteProperties("lwfm").get("port") or "3000"
            self._SERVICE_URL = f"http://{host}:{port}"


    #***********************************************************************
    # workflow methods

    def getWorkflow(self, workflow_id: str) -> Optional[Workflow]:
        response = requests.get(f"{self.getUrl()}/workflow/{workflow_id}",
            timeout=self._REST_TIMEOUT)
        try:
            if response.ok:
                if (response.text is not None) and (len(response.text) > 0):
                    workflow = ObjectSerializer.deserialize(response.text)
                    return workflow
                return None
            self.emitLogging("ERROR", f"response not ok: {response.text}")
            return None
        except Exception as ex:
            self.emitLogging("ERROR", "getWorkflow error: " + str(ex))
            return None

    def getAllWorkflows(self) -> Optional[List[Workflow]]:
        """
        Get all workflows from the service.
        """
        try:
            response = requests.get(f"{self.getUrl()}/workflows",
                timeout=self._REST_TIMEOUT)
            if response.ok:
                if (response.text is not None) and (len(response.text) > 0):
                    workflows = ObjectSerializer.deserialize(response.text)
                    return cast(List[Workflow], workflows)
                return None
            self.emitLogging("ERROR", f"getAllWorkflows error: {response.text}")
            return None
        except Exception as ex:
            self.emitLogging("ERROR", "getAllWorkflows error: " + str(ex))
            return None

    def putWorkflow(self, workflow: Workflow) -> Optional[str]:
        payload = {}
        payload["workflowObj"] = ObjectSerializer.serialize(workflow)
        response = requests.post(f"{self.getUrl()}/workflow", payload,
            timeout=self._REST_TIMEOUT)
        if not response.ok:
            self.emitLogging("ERROR", f"putWorkflow error: {response.text}")
            return
        return workflow.getWorkflowId()



    def getAllJobStatusesForWorkflow(self, workflow_id: str) -> Optional[List[JobStatus]]:
        """
        Get all job status messages for all jobs in a workflow.
        """
        try:
            url = f"{self.getUrl()}/workflow/{workflow_id}/statuses"
            response = requests.get(url, timeout=self._REST_TIMEOUT)
            if response.status_code == 200:
                return ObjectSerializer.deserialize(response.text)
            elif response.status_code == 404:
                return None
            else:
                return None
        except Exception:
            return None


    #***********************************************************************
    # status methods

    def getStatus(self, jobId: str) -> Optional[JobStatus]:
        response = requests.get(f"{self.getUrl()}/status/{jobId}",
            timeout=self._REST_TIMEOUT)
        try:
            if response.ok:
                if (response.text is not None) and (len(response.text) > 0):
                    status = ObjectSerializer.deserialize(response.text)
                    return status
                return None
            self.emitLogging("ERROR", f"response not ok: {response.text}")
            return None
        except Exception as ex:
            self.emitLogging("ERROR", "getStatus error: " + str(ex))
            return None


    def getAllStatus(self, jobId: str) -> Optional[List[JobStatus]]:
        response = requests.get(f"{self.getUrl()}/statusAll/{jobId}",
            timeout=self._REST_TIMEOUT)
        try:
            if response.ok:
                if (response.text is not None) and (len(response.text) > 0):
                    statusList = ObjectSerializer.deserialize(response.text)
                    return statusList
                return None
            self.emitLogging("ERROR", f"response not ok: {response.text}")
            return None
        except Exception as ex:
            self.emitLogging("ERROR", "getAllStatus error: " + str(ex))
            return None


    # emit a status message, perhaps triggering event handlers
    def emitStatus(self, context: JobContext,
                   statusStr: str, nativeStatusStr: str, nativeInfo: str = "",
                   fromEvent: bool = False) -> None:
        try:
            status: JobStatus = JobStatus(context)
            status.setStatus(statusStr)
            status.setNativeStatus(nativeStatusStr)
            status.setNativeInfo(nativeInfo)
            status.setEmitTime(datetime.datetime.now(datetime.UTC))
            statusBlob = ObjectSerializer.serialize(status)
            data = {"statusBlob": statusBlob, "fromEvent": str(fromEvent)}
            response = requests.post(f"{self.getUrl()}/emitStatus", data=data,
                timeout=self._REST_TIMEOUT)
            if response.ok:
                return
            self.emitLogging("ERROR", f"emitStatus error: {response}")
            return
        except Exception as ex:
            self.emitLogging("ERROR", "Error emitting job status: " + str(ex))
            return

    #***********************************************************************
    # event methods

    def setEvent(self, wfe: WorkflowEvent) -> Optional[JobStatus]:
        payload = {}
        payload["eventObj"] = ObjectSerializer.serialize(wfe)
        response = requests.post(f"{self.getUrl()}/setEvent", payload,
            timeout=self._REST_TIMEOUT)
        if response.ok:
            # return the initial status of the registered job
            return self.getStatus(response.text)
        self.emitLogging("ERROR", "setEvent error: " + response.text)
        return None

    def unsetEvent(self, wfe: WorkflowEvent) -> None:
        if wfe is None or wfe.getEventId() is None:
            self.emitLogging("ERROR", "unsetEvent called with None or empty eventId")
            return
        response = requests.get(f"{self.getUrl()}/unsetEvent/{wfe.getEventId()}",
            timeout=self._REST_TIMEOUT)
        if response.ok:
            # TODO should return a terminal status
            return
        self.emitLogging("ERROR", "unsetEvent error: " + response.text)
        return

    def getActiveWfEvents(self) -> Optional[List[WorkflowEvent]]:
        response = requests.get(f"{self.getUrl()}/listEvents",
            timeout=self._REST_TIMEOUT)
        if response.ok and response.text is not None and len(response.text) > 0:
            # deserialize the list of WorkflowEvent objects
            return ObjectSerializer.deserialize(response.text)
        return None


    #***********************************************************************
    # logging methods

    def emitLogging(self, level: str, doc: str,
                    site: Optional[str] = "",
                    workflowId: Optional[str] = "",
                    jobId: Optional[str] = "") -> None:
        try:
            data = {"level": level,
                    "errorMsg": doc,
                    "site": site,
                    "workflowId": workflowId,
                    "jobId": jobId}
            response = requests.post(f"{self.getUrl()}/emitLogging", data,
                timeout=self._REST_TIMEOUT)
            if response.ok:
                return
            # use the plain logger when logging logging errors
            logging.error(f"emitLogging error: {response.text}")
            return
        except Exception as ex:
            # use the plain logger when logging logging errors
            logging.error("error emitting logging: " + str(ex))


    def getLoggingByWorkflowId(self, workflowId: str) -> Optional[List[str]]:
        """
        Retrieve logging entries for a given workflow ID.
        """
        try:
            response = requests.get(f"{self.getUrl()}/logs/workflow/{workflowId}",
                                    timeout=self._REST_TIMEOUT)
            if response.ok and response.text:
                return cast(List[str], ObjectSerializer.deserialize(response.text))
            if response.status_code == 404:
                self.emitLogging("ERROR", f"getLoggingByWorkflowId error: {response.text}")
            return None 
        except Exception as ex:
            self.emitLogging("ERROR", f"getLoggingByWorkflowId exception: {str(ex)}")
        return None


    def getLoggingByJobId(self, jobId: str) -> Optional[List[str]]:
        """
        Retrieve logging entries for a given job ID.
        """
        try:
            response = requests.get(f"{self.getUrl()}/logs/job/{jobId}",
                                    timeout=self._REST_TIMEOUT)
            if response.ok and response.text:
                return cast(List[str], ObjectSerializer.deserialize(response.text))
            if response.status_code == 404:
                self.emitLogging("ERROR", f"getLoggingByJobId error: {response.text}")
            return None
        except Exception as ex:
            self.emitLogging("ERROR", f"getLoggingByJobId exception: {str(ex)}")
        return None


    def getAllLogging(self) -> Optional[List[str]]:
        """
        Retrieve all logging entries from the system.
        """
        try:
            response = requests.get(f"{self.getUrl()}/logs/all",
                                    timeout=self._REST_TIMEOUT)
            if response.ok and response.text:
                return cast(List[str], ObjectSerializer.deserialize(response.text))
            if response.status_code == 404:
                self.emitLogging("ERROR", f"getAllLogging error: {response.text}")
            return None
        except Exception as ex:
            self.emitLogging("ERROR", f"getAllLogging exception: {str(ex)}")
        return None



    #***********************************************************************
    # repo methods


    def notate(self, metasheet: Optional[Metasheet]):
        # call to the service to put metasheet for this put
        try:
            data = {"data": ObjectSerializer.serialize(metasheet)}
            response = requests.post(f"{self.getUrl()}/notate", data,
                timeout=self._REST_TIMEOUT)
            if response.ok:
                return
            # use the plain logger when logging logging errors
            logging.error(f"notate error: {response.text}")
            return
        except Exception as ex:
            # use the plain logger when logging logging errors
            logging.error("error notating: " + str(ex))
        return metasheet

    def find(self, queryRegExs: dict) -> Optional[List[Metasheet]]:
        # call to the service to find metasheets
        try:
            data = {"searchDict": json.dumps(queryRegExs)}
            response = requests.post(f"{self.getUrl()}/find", data,
                timeout=self._REST_TIMEOUT)
            if response.ok:
                sheets: List[Metasheet] = ObjectSerializer.deserialize(response.text)
                return sheets
            # use the plain logger when logging logging errors
            logging.error(f"find error: {response.text}")
        except Exception as ex:
            # use the plain logger when logging logging errors
            logging.error("error finding: " + str(ex))
        return None
