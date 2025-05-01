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


def _runInVenv(venv_path: str, script_path: str) -> None:
    """
    venv_path = "/path/to/your/venv"
    script_path = "/path/to/your/script.py"
    """
    try:
        if os.name == "nt":
            python_executable = os.path.join(venv_path, "Scripts", "python.exe")
        else:
            python_executable = os.path.join(venv_path, "bin", "python")
        process = subprocess.Popen([python_executable, script_path])
        process.wait()
        if process.returncode == 0:
            print("Subprocess completed successfully.")
        else:
            print(f"Subprocess failed with return code: {process.returncode}")
    except Exception as ex:
        print(f"An error occurred: {ex}")

# python -c "import example_class; obj = example_class.MyClass('example');
#        obj.my_method('hello', 'world')"


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
