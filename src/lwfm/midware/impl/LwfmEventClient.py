
# implementation behind the LwfManager - use it instead
# this gives the HTTP calls to the lwfm middleware to account for the case
# where its not running on the same machine as the workflow 

import json
from pathlib import Path
import requests
from typing import List
import os 
import datetime
import logging    # don't use the lwfm Logger here else circular import

from lwfm.base.JobStatus import JobStatus
from lwfm.base.JobContext import JobContext
from lwfm.base.Metasheet import Metasheet
from lwfm.base.WfEvent import WfEvent

class LwfmEventClient():
    _SERVICE_URL = "http://127.0.0.1:3000"
    if os.getenv("LWFM_SERVICE_URL") is not None:
        _SERVICE_URL = os.getenv("LWFM_SERVICE_URL")    

    def getUrl(self):
        return self._SERVICE_URL

    #***********************************************************************
    # status methods

    def getStatus(self, jobId: str) -> JobStatus:
        response = requests.get(f"{self.getUrl()}/status/{jobId}")
        try:
            if response.ok:
                if (response.text is not None) and (len(response.text) > 0):
                    status = JobStatus.deserialize(response.text)
                    return status
                else:
                    return None
            else:
                self.emitLogging("ERROR", f"response not ok: {response.text}")    
                return None
        except Exception as ex:
            self.emitLogging("ERROR", "getStatus error: " + str(ex))
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
            statusBlob = status.serialize()
            data = {"statusBlob": statusBlob}
            response = requests.post(f"{self.getUrl()}/emitStatus", data=data)
            if response.ok:
                return
            else:
                self.emitLogging("ERROR", f"emitStatus error: {response}")
                return
        except Exception as ex:
            self.emitLogging("ERROR", "Error emitting job status: " + str(ex))
            return

    #***********************************************************************
    # event methods

    def setEvent(self, wfe: WfEvent) -> str:
        payload = {}
        payload["eventObj"] = wfe.serialize()
        response = requests.post(f"{self.getUrl()}/setEvent", payload)
        if response.ok:
            # return the job id of the registered job
            return response.text
        else:
            self.emitLogging("ERROR", "setEvent error: " + response.text)
            return None
        
    def unsetEvent(self, wfe: WfEvent) -> None:
        payload = {}
        payload["eventObj"] = wfe.serialize()
        response = requests.post(f"{self.getUrl()}/unsetEvent", payload)
        if response.ok:
            # return the job id of the registered job
            return 
        else:
            self.emitLogging("ERROR", "unsetEvent error: " + response.text)
            return 

    def getActiveWfEvents(self) -> List[WfEvent]:
        response = requests.get(f"{self.getUrl()}/listEvents")
        if response.ok:
            l = json.loads(response.text)
            return [WfEvent.deserialize(blob) for blob in l]
        else:
            self.emitLogging("ERROR", "getActiveWfEvents error: " + str(response.text))
            return None


    #***********************************************************************
    # logging methods

    def emitLogging(self, level: str, doc: str) -> None: 
        try:
            data = {"level": level, 
                    "errorMsg": doc}
            response = requests.post(f"{self.getUrl()}/emitLogging", data)
            if response.ok:
                return
            else:
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
                    "data": metasheet.serialize()}
            response = requests.post(f"{self.getUrl()}/notate", data)
            if response.ok:
                return
            else:
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
            response = requests.post(f"{self.getUrl()}/find", data)
            if response.ok:
                l = json.loads(response.text)
                return [Metasheet.deserialize(blob) for blob in l]
            else:
                # use the plain logger when logging logging errors
                logging.error(f"find error: {response.text}")
        except Exception as ex:
            # use the plain logger when logging logging errors
            logging.error("error notating: " + str(ex))
        return None
    

    #***********************************************************************

