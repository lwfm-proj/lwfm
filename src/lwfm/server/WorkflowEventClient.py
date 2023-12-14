import pickle
import logging
import json
import requests

from lwfm.base.WorkflowEventTrigger import WorkflowEventTrigger


class WorkflowEventClient:
    _JSS_URL = "http://127.0.0.1:3000"

    def getUrl(self):
        return self._JSS_URL

    # TODO - docs
    def setEventTrigger(self, wfet: WorkflowEventTrigger) -> str:
        payload = {}
        print("about to pickle - " + str(wfet.getTriggerFilter()))
        payload["triggerObj"] = pickle.dumps(wfet, 0).decode()
        response = requests.post(f"{self.getUrl()}/setWorkflowEvent", payload)
        if response.ok:
            # this is the job id of the registered job
            return response.text
        else:
            logging.error(response.text)
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

    def listActiveEventTriggers(self) -> [str]:
        response = requests.get(f"{self.getUrl()}/list")
        if response.ok:
            return eval(response.text)
        else:
            logging.error(response)
            return None

    def emitStatus(self, jobId, jobStatus, statusBlob):
        data = {"jobId": jobId, "jobStatus": jobStatus, "statusBlob": statusBlob}
        response = requests.post(f"{self.getUrl()}/emit", data=data)
        if response.ok:
            return
        else:
            logging.error(response)
            return

    def getStatusBlob(self, jobId: str) -> str:
        response = requests.get(f"{self.getUrl()}/status/{jobId}")
        try:
            if response.ok:
                if response.text == "":
                    return None
                else:
                    return response.text
            else:
                return None
        except Exception as ex:
            logging.error(ex)
            return None

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

