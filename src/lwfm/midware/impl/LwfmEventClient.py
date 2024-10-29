import pickle
import json
import requests
from typing import List
from enum import Enum
import datetime

from lwfm.base.JobStatus import JobStatus
from lwfm.base.JobContext import JobContext
from lwfm.base.WfEvent import WfEvent
from lwfm.midware.LwfManager import LwfManager
from lwfm.midware.Logger import Logger


class LwfmEventClient(LwfManager):
    # TODO url of the actual service we expose to our little gaggle 
    _JSS_URL = "http://127.0.0.1:3000"

    def getUrl(self):
        return self._JSS_URL

    #***********************************************************************

    def getAllStatuses(self, jobId: str) -> List[JobStatus]:
        response = requests.get(f"{self.getUrl()}/all/statuses")
        if response.ok:
            if response.text == "":
                return None
            else:
                statuses = json.loads(str(response.text))
                return statuses
        else:
            return None


    def getStatus(self, jobId: str) -> JobStatus:
        response = requests.get(f"{self.getUrl()}/status/{jobId}")
        try:
            if response.ok:
                return JobStatus.deserialize(response.text)
            else:
                print("response not ok")
                return None
        except Exception as ex:
            Logger.error("getStatusBlob error: " + str(ex))
            return None


    def setEvent(self, wfe: WfEvent) -> str:
        payload = {}
        payload["triggerObj"] = pickle.dumps(wfe, 0).decode()
        response = requests.post(f"{self.getUrl()}/setWorkflowEvent", payload)
        if response.ok:
            # this is the job id of the registered job
            return response.text
        else:
            Logger.error("setEvent error: " + response.text)
            return None
        

    def getActiveWfEvents(self) -> List[WfEvent]:
        response = requests.get(f"{self.getUrl()}/listEvents")
        if response.ok:
            blobs = response.text.split("\n")
            return [WfEvent.deserialize(blob) for blob in blobs]
        else:
            Logger.error("listActiveEventTriggers error: " + str(response))
            return None


    # emit a status message, perhaps triggering event handlers 
    def emitStatus(self, context: JobContext, statusClass: type, 
                   nativeStatus: Enum, nativeInfo: str = None) -> None:
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
                Logger.error(f"emitStatus error: {response}")
                return
        except Exception as ex:
            Logger.error("Error emitting job status: " + str(ex))


    #***********************************************************************


    #def unsetEventTrigger(self, handlerId: str) -> bool:
    #    response = requests.get(f"{self.getUrl()}/unset/{handlerId}")
    #    if response.ok:
            return True
    #    else:
    #        return False

    #def unsetAllEventTriggers(self) -> bool:
    #    response = requests.get(f"{self.getUrl()}/unsetAll")
    #    if response.ok:
    #        return True
    #    else:
    #        return False




    def getSiteName(self, jobId):
        response = requests.get(f"{self.getUrl()}/site/jobId/{jobId}")
        if response.ok:
            return response.text
        else:
            return None

    def getWorkflowUrl(self, jobContext) -> str:
        return "" + self.getUrl() + "/wfthread/" + jobContext.getId()
