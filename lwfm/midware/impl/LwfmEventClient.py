"""
implementation behind the LwfManager - use it instead
this gives the HTTP calls to the lwfm middleware to account for the case
where its not running on the same machine as the workflow
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring
#pylint: disable = broad-exception-caught, logging-not-lazy

from typing import List
import os
import datetime

import logging # don't use the lwfm Logger here else circular import

import json
import requests

from lwfm.base.JobStatus import JobStatus
from lwfm.base.JobContext import JobContext
from lwfm.base.Metasheet import Metasheet
from lwfm.base.WfEvent import WfEvent
from lwfm.base.Workflow import Workflow
from lwfm.util.ObjectSerializer import ObjectSerializer

class LwfmEventClient:
    _SERVICE_URL = "http://127.0.0.1:3000"
    _REST_TIMEOUT = 100

    if os.getenv("LWFM_SERVICE_URL") is not None:
        _SERVICE_URL = os.getenv("LWFM_SERVICE_URL")

    def getUrl(self):
        return self._SERVICE_URL

    #***********************************************************************
    # workflow methods

    def getWorkflow(self, workflow_id: str) -> Workflow:
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

    def putWorkflow(self, workflow: Workflow) -> str:
        payload = {}
        payload["workflowObj"] = ObjectSerializer.serialize(workflow)
        response = requests.post(f"{self.getUrl()}/workflow", payload,
            timeout=self._REST_TIMEOUT)
        if not response.ok:
            self.emitLogging("ERROR", f"putWorkflow error: {response.text}")
            return
        return workflow.getWorkflowId()


    #***********************************************************************
    # status methods

    def getStatus(self, jobId: str) -> JobStatus:
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


    def getAllStatus(self, jobId: str) -> [JobStatus]:
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
    def emitStatus(self, context: JobContext, statusClass: type,
                   nativeStatus: str, nativeInfo: str = None) -> None:
        try:
            status = statusClass(context)
            # forces call on setStatus() producing a mapped native status -> status
            status.setNativeStatus(nativeStatus)
            status.setNativeInfo(nativeInfo)
            status.setEmitTime(datetime.datetime.now(datetime.UTC))
            statusBlob = ObjectSerializer.serialize(status)
            data = {"statusBlob": statusBlob}
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

    def setEvent(self, wfe: WfEvent) -> JobStatus:
        payload = {}
        payload["eventObj"] = ObjectSerializer.serialize(wfe)
        response = requests.post(f"{self.getUrl()}/setEvent", payload,
            timeout=self._REST_TIMEOUT)
        if response.ok:
            # return the initial status of the registered job
            return self.getStatus(response.text)
        else:
            self.emitLogging("ERROR", "setEvent error: " + response.text)
            return None

    def unsetEvent(self, wfe: WfEvent) -> None:
        payload = {}
        payload["eventObj"] = wfe.serialize()
        response = requests.post(f"{self.getUrl()}/unsetEvent", payload,
            timeout=self._REST_TIMEOUT)
        if response.ok:
            # TODO should return a terminal status  
            return
        else:
            self.emitLogging("ERROR", "unsetEvent error: " + response.text)
            return

    def getActiveWfEvents(self) -> List[WfEvent]:
        response = requests.get(f"{self.getUrl()}/listEvents",
            timeout=self._REST_TIMEOUT)
        if response.ok:
            l = json.loads(response.text)
            return [ObjectSerializer.deserialize(blob) for blob in l]
        self.emitLogging("ERROR", "getActiveWfEvents error: " + str(response.text))
        return None


    #***********************************************************************
    # logging methods

    def emitLogging(self, level: str, doc: str) -> None:
        try:
            data = {"level": level,
                    "errorMsg": doc}
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


    #***********************************************************************
    # repo methods


    def notate(self, jobId: str, metasheet: Metasheet = None) -> Metasheet:
        # call to the service to put metasheet for this put
        try:
            data = {"jobId": jobId,
                    "data": ObjectSerializer.serialize(metasheet)}
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

    def find(self, queryRegExs: dict) -> List[Metasheet]:
        # call to the service to find metasheets
        try:
            data = {"searchDict": json.dumps(queryRegExs)}
            response = requests.post(f"{self.getUrl()}/find", data,
                timeout=self._REST_TIMEOUT)
            if response.ok:
                l = json.loads(response.text)
                return [ObjectSerializer.deserialize(blob) for blob in l]
            # use the plain logger when logging logging errors
            logging.error(f"find error: {response.text}")
        except Exception as ex:
            # use the plain logger when logging logging errors
            logging.error("error finding: " + str(ex))
        return None


    #***********************************************************************
