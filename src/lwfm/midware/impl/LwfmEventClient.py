import pickle
import logging  # TODO logging
import json
import requests
from typing import List


from lwfm.base.JobContext import JobContext
from lwfm.base.JobStatus import JobStatus
from lwfm.base.WfEvent import WfEvent



class LwfmEventClient:
    # TODO url of the actual service we expose to our little gaggle 
    _JSS_URL = "http://127.0.0.1:3000"

    def getUrl(self):
        return self._JSS_URL


    def getStatus(self, jobId: str) -> JobStatus:
        response = requests.get(f"{self.getUrl()}/status/{jobId}")
        try:
            if response.ok:
                if response.text == "":
                    print("got blank response")
                    return None
                else:
                    return JobStatus.deserialize(response.text) 
            else:
                print("response not ok")
                return None
        except Exception as ex:
            logging.error("getStatusBlob error: " + ex)
            return None
        
    # TODO - docs
    def setEvent(self, wfe: WfEvent) -> str:
        payload = {}
        payload["triggerObj"] = pickle.dumps(wfe, 0).decode()
        response = requests.post(f"{self.getUrl()}/setWorkflowEvent", payload)
        if response.ok:
            # this is the job id of the registered job
            return response.text
        else:
            logging.error("setEvent error: " + response.text)
            return None

    def unsetEventTrigger(self, handlerId: str) -> bool:
        response = requests.get(f"{self.getUrl()}/unset/{handlerId}")
        if response.ok:
            return True
        else:
            return False

    def unsetAllEventTriggers(self) -> bool:
        response = requests.get(f"{self.getUrl()}/unsetAll")
        if response.ok:
            return True
        else:
            return False

    def listActiveEventTriggers(self) -> List[str]:
        response = requests.get(f"{self.getUrl()}/list")
        if response.ok:
            return eval(response.text)
        else:
            logging.error("listActiveEventTriggers error: " + response)
            return None

    def emitStatus(self, status: JobStatus):
        try: 
            statusBlob = status.serialize()
            data = {"statusBlob": statusBlob}
            response = requests.post(f"{self.getUrl()}/emitStatus", data=data)
            if response.ok:
                return
            else:
                logging.error("emitStatus error: " + response)
                return
        except Exception as ex:
            logging.error("emitStatus error: " + ex)
            raise ex



    def getStatuses(self):
        response = requests.get(f"{self.getUrl()}/all/statuses")
        if response.ok:
            if response.text == "":
                return None
            else:
                statuses = json.loads(str(response.text))
                return statuses
        else:
            return None

    def getSiteName(self, jobId):
        response = requests.get(f"{self.getUrl()}/site/jobId/{jobId}")
        if response.ok:
            return response.text
        else:
            return None

    def getWorkflowUrl(self, jobContext) -> str:
        return "" + self.getUrl() + "/wfthread/" + jobContext.getId()
