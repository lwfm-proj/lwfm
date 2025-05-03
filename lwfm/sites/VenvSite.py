"""
Base class for a Site which runs its functions in a virtual environment. This allows the 
Site to run with its own set of dependencies independent of the global environment.
"""

#pylint: disable = invalid-name, broad-exception-caught

from typing import List, Union

import subprocess
import os

from lwfm.base.Site import Site, SiteAuth, SiteRun, SiteRepo, SiteSpin
from lwfm.base.JobContext import JobContext
from lwfm.base.JobStatus import JobStatus
from lwfm.base.Metasheet import Metasheet
from lwfm.base.Workflow import Workflow
from lwfm.base.JobDefn import JobDefn
from lwfm.midware.LwfManager import logger


def _runInVenv(venv_path: str, script_path_cmd: str) -> None:
    """
    venv_path = "/path/to/your/venv"
    script_path_cmd = "import example_class; obj = example_class.MyClass('example');
#        obj.my_method('hello', 'world')"
    """
    try:
        if os.name == "nt":
            python_executable = os.path.join(venv_path, "Scripts", "python.exe")
        else:
            python_executable = os.path.join(venv_path, "bin", "python")
        process = subprocess.Popen([python_executable, "-c", script_path_cmd])
        process.wait()
        if process.returncode != 0:
            logger.error(f"_runInVenv: subprocess failed with return code: {process.returncode}")
    except Exception as ex:
        logger.error(f"_runInVenv: an error occurred: {ex}")


class VenvSiteAuth(SiteAuth):
    def login(self, force: bool = False) -> bool:
        return True

    def isAuthCurrent(self) -> bool:
        return True



class VenvSiteRun(SiteRun):
    """
    """
    def submit(self, jobDefn: 'JobDefn', 
        parentContext: Union[JobContext, Workflow] = None,
        computeType: str = None, runArgs: dict = None) -> JobStatus:
        pass

    def getStatus(self, jobId: str) -> JobStatus:
        pass

    def cancel(self, jobContext: JobContext) -> bool:
        pass



class VenvSiteRepo(SiteRepo):
    """
    """
    def put(
        self,
        localPath: str,
        siteObjPath: str,
        jobContext: JobContext = None,
        metasheet: Metasheet = None
    ) -> Metasheet:
        pass

    def get(
        self,
        siteObjPath: str,
        localPath: str,
        jobContext: JobContext = None
    ) -> str:
        pass

    def find(self, queryRegExs: dict) -> List[Metasheet]:
        pass


class VenvSiteSpin(SiteSpin):
    """
    """
    def listComputeTypes(self) -> List[str]:
        pass


class VenvSite(Site):
    """
    A Site driver for running jobs in a virtual environment.
    """
    SITE_NAME = "venv"

    def __init__(self):
        super().__init__(
            self.SITE_NAME,
            VenvSiteAuth(),
            VenvSiteRun(),
            VenvSiteRepo(),
            VenvSiteSpin()
        )
