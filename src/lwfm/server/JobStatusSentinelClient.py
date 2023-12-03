import pickle
import logging
import json
import requests 

from lwfm.base.JobDefn import JobDefn

class JobStatusSentinelClient:
    _JSS_URL = "http://127.0.0.1:3000"

    def getUrl(self):
        return self._JSS_URL


    def setTerminalSentinel(self, jobId: str, parentId: str, originId: str, nativeId: str, siteName: str) -> str:
        payload = {}
        payload["jobId"] = jobId
        if (parentId is not None):
            payload["parentId"] = parentId
        else:
            payload["parentId"] = ""
        payload["originId"] = originId
        payload["nativeId"] = nativeId
        payload["siteName"] = siteName
        response = requests.post(f'{self.getUrl()}/setTerminal', payload)
        if response.ok:
            return response.text
        else:
            logging.error(response.text)
            return response.text


    # TODO - docs 
    def setEventHandler(self, jobId: str, jobStatus: str, fireDefn: JobDefn, targetSiteName: str) -> str:
        payload = {}
        payload["jobId"] = jobId
        payload["jobStatus"] = jobStatus
        if (fireDefn is not None) and (targetSiteName is not None):
            payload["fireDefn"] = pickle.dumps(fireDefn, 0).decode() # Use protocol 0 so we can easily convert to an ASCII string
            payload["targetSiteName"] = targetSiteName
        else:
            payload["fireDefn"] = ""
            payload["targetSiteName"] = ""
        response = requests.post(f'{self.getUrl()}/set', payload)
        if response.ok:
            # this is the job id of the registered job 
            return response.text
        else:
            logging.error(response.text)
            return None

    def unsetEventHandler(self, handlerId: str) -> bool:
        response = requests.get(f'{self.getUrl()}/unset/{handlerId}')
        if response.ok:
            return True
        else:
            return False

    def unsetAllEventHandlers(self) -> bool:
        response = requests.get(f'{self.getUrl()}/unsetAll')
        if response.ok:
            return True
        else:
            return False

    def listActiveHandlers(self) -> [str]:
        response = requests.get(f'{self.getUrl()}/list')
        if response.ok:
            return eval(response.text)
        else:
            logging.error(response)
            return None

    def emitStatus(self, jobId, jobStatus, statusBlob):
        data = {'jobId' : jobId,
                'jobStatus': jobStatus,
                'statusBlob': statusBlob}
        response = requests.post(f'{self.getUrl()}/emit', data=data)
        if response.ok:
            return 
        else:
            logging.error(response)
            return   

    def getStatusBlob(self, jobId: str) -> str:
        response = requests.get(f'{self.getUrl()}/status/{jobId}')
        try:
            if response.ok:
                if (response.text == ""):
                    return None
                else:
                    return response.text
            else:
                return None
        except Exception as ex:
            logging.error(ex)
            return None

    def getStatuses(self):
        response = requests.get(f'{self.getUrl()}/all/statuses')
        if response.ok:
            if (response.text == ""):
                return None
            else:
                statuses =  json.loads(str(response.text))
                return statuses
        else:
            return None      

    def getSiteName(self, jobId):
        response = requests.get(f'{self.getUrl()}/site/jobId/{jobId}')
        if response.ok:
            return response.text
        else:
            return None

    def getWorkflowUrl(self, jobContext) -> str:
        return "" + self.getUrl() + "/wfthread/" + jobContext.getId()


#************************************************************************************************************************************

# test
if __name__ == '__main__':
    pass 

