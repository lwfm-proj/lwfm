"""
Base class for a Site which runs its functions in a virtual environment. This allows the 
Site to run with its own set of dependencies independent of the global environment.
"""

#pylint: disable = invalid-name, broad-exception-caught, too-few-public-methods
#pylint: disable = missing-function-docstring, missing-class-docstring

from typing import List, Union
from abc import ABC

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
    A private helper method to run a command in a virtual environment. This is used
    to run canonical Site methods.

    venv_path = "/path/to/your/venv"
    script_path_cmd = "import example_class; obj = example_class.MyClass('example');
#        obj.my_method('hello', 'world')"
    """
    try:
        # construct the path to the python executable in the virtual environment
        if os.name == "nt":    # windows
            python_executable = os.path.join(venv_path, "Scripts", "python.exe")
        else:   # a real OS
            python_executable = os.path.join(venv_path, "bin", "python")
        # execute a semicolon separated command in the virtual environment; this
        # includes an import statement and the method call
        process = subprocess.Popen([python_executable, "-c", script_path_cmd])
        # wait synchronously for the process to finish - if the underlying method
        # being called is async, then this will return right away, else we wait
        process.wait()
        if process.returncode != 0:
            # something bad happened in the subprocess
            logger.error(f"_runInVenv: subprocess failed with return code: {process.returncode}")
    except Exception as ex:
        # something really bad happened
        logger.error(f"_runInVenv: an error occurred: {ex}")


class VenvSiteAuth(SiteAuth):
    """
    An Auth driver for running jobs in a virtual environment. For a default local
    site, this is a no-op.
    """

    def __init__(self):
        super().__init__()
        self._authDriver = None

    def setAuthDriver(self, authDriver: SiteAuth) -> None:
        self._authDriver = authDriver

    def getAuthDriver(self) -> SiteAuth:
        return self._authDriver

    def login(self, force: bool = False) -> bool:
        return self._authDriver.login(force)

    def isAuthCurrent(self) -> bool:
        return self._authDriver.isAuthCurrent()



class VenvSiteRun(SiteRun):
    """
    A Run driver for running jobs in a virtual environment.
    """

    def __init__(self) -> None:
        super().__init__()
        self._runDriver = None

    def setRunDriver(self, runDriver: SiteRun) -> None:
        self._runDriver = runDriver

    def getRunDriver(self) -> SiteRun:
        return self._runDriver

    def submit(self, jobDefn: 'JobDefn',
        parentContext: Union[JobContext, Workflow] = None,
        computeType: str = None, runArgs: dict = None) -> JobStatus:
        return self._runDriver.submit(jobDefn, parentContext, computeType, runArgs)

    def getStatus(self, jobId: str) -> JobStatus:
        return self._runDriver.getStatus(jobId)

    def cancel(self, jobContext: JobContext) -> bool:
        return self._runDriver.cancel(jobContext)



class VenvSiteRepo(SiteRepo):
    """
    A Repo driver for running jobs in a virtual environment.
    """

    def __init__(self) -> None:
        super().__init__()
        self._repoDriver = None

    def setRepoDriver(self, repoDriver: SiteRepo) -> None:
        self._repoDriver = repoDriver

    def getRepoDriver(self) -> SiteRepo:
        return self._repoDriver

    def put(
        self,
        localPath: str,
        siteObjPath: str,
        jobContext: JobContext = None,
        metasheet: Metasheet = None
    ) -> Metasheet:
        return self._repoDriver.put(localPath, siteObjPath, jobContext, metasheet)

    def get(
        self,
        siteObjPath: str,
        localPath: str,
        jobContext: JobContext = None
    ) -> str:
        return self._repoDriver.get(siteObjPath, localPath, jobContext)

    def find(self, queryRegExs: dict) -> List[Metasheet]:
        return self._repoDriver.find(queryRegExs)


class VenvSiteSpin(SiteSpin):
    def __init__(self) -> None:
        super().__init__()
        self._spinDriver = None

    def setSpinDriver(self, spinDriver: SiteSpin) -> None:
        self._spinDriver = spinDriver

    def getSpinDriver(self) -> SiteSpin:
        return self._spinDriver

    def listComputeTypes(self) -> List[str]:
        return self._spinDriver.listComputeTypes()


class VenvSite(Site, ABC):
    """
    A Site driver for running jobs in a virtual environment.
    """
