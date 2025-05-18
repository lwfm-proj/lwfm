"""
Abstract class for a Site which runs its functions in a virtual environment. This allows the 
Site to run with its own set of dependencies independent of the global environment.
"""

#pylint: disable = invalid-name, broad-exception-caught, too-few-public-methods
#pylint: disable = missing-function-docstring, missing-class-docstring

from typing import List, Union
from abc import ABC

import subprocess
import os

from lwfm.base.Site import Site, SiteAuth, SiteRun, SiteRepo, SiteSpin, SitePillar
from lwfm.base.JobContext import JobContext
from lwfm.base.JobStatus import JobStatus
from lwfm.base.Metasheet import Metasheet
from lwfm.base.Workflow import Workflow
from lwfm.base.JobDefn import JobDefn
from lwfm.midware.LwfManager import logger, lwfManager
from lwfm.sites.LocalSite import LocalSite


# *********************************************************************************
# internals

def _makeVenvPath() -> str:   # TODO
    """
    A private helper method to construct the path to the virtual environment.
    This is used to run commands in the virtual environment.
    """
    # construct the path to the virtual environment
    # if os.name == "nt":    # windows
    #     return os.path.join(os.getcwd(), ".venv", "Scripts")
    # else:   # a real OS
    #     return os.path.join(os.getcwd(), ".venv", "bin")
    return "./.venv"


def _executeInProjectVenv(script_path_cmd: str = None) -> str:
    """
    A private helper method to run a command in a virtual environment. This is used
    to run canonical Site methods.

    venv_path = "/path/to/your/venv"
    script_path_cmd = "import example_class; obj = example_class.MyClass('example');
#        obj.my_method('hello', 'world')"
    """
    if script_path_cmd is None:
        raise ValueError("script_path_cmd is required")

    proj_path = _makeVenvPath()

    print(f"_executeInProjectVenv: executing in {proj_path}")
    print(f"_executeInProjectVenv: executing command: {script_path_cmd}")

    try:
        # construct the path to the python executable in the virtual environment
        if os.name == "nt":    # windows
            python_executable = os.path.join(proj_path, "Scripts", "python.exe")
        else:   # a real OS
            python_executable = os.path.join(proj_path, "bin", "python")
        # execute a semicolon separated command in the virtual environment; this
        # includes an import statement and the method call
        process = subprocess.Popen([python_executable, "-c", script_path_cmd],
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE,
                                    text=True
                                    )
        # read the output and error streams
        stdout, stderr = process.communicate()

        if process.returncode != 0:
            # something bad happened in the subprocess
            logger.error("_executeInProjectVenv: subproc failed with return code: " + \
                         f"{process.returncode}")
            logger.error(f"_executeInProjectVenv: stderr: {stderr}")
            logger.error(f"_executeInProjectVenv: stdout: {stdout}")
            raise RuntimeError()
        if stdout:
            # if the command was successful, return the output
            return stdout
        return None
    except Exception as ex:
        # something really bad happened
        logger.error(f"_runInVenv: an error occurred: {ex}")

def _getClassName(obj: object) -> str:
    if obj is None:
        return "None"
    return obj.__class__.__name__

def _getPackageName(obj: object) -> str:
    if obj is None:
        return "None"
    return f"{obj.__class__.__module__}"


def _makeSerializeCommandString() -> str:
    """
    A private helper method to serialize an object into a string. This is used
    to pass objects between the main process and the subprocess.
    """
    # construct the command string to execute in the virtual environment
    return "from lwfm.midware.LwfManager import lwfManager; " + \
           "obj = lwfManager.serialize(obj); " + \
           "print(obj)"

def _makeObjDriverCommandString(sitePillar: SitePillar) -> str:
    """
    A private helper method to import a class from a module. This is used
    to pass objects between the main process and the subprocess.
    """
    # construct the command string to execute in the virtual environment
    pkgName = _getPackageName(sitePillar)
    className = _getClassName(sitePillar)
    return f"from {pkgName} import {className}; " + \
           f"driver = {className}(); "



def _makeObjWrapper(obj: object) -> str:
    return f"'{lwfManager.serialize(obj)}'"


# *********************************************************************************
# site pillar wrappers

class VenvSiteAuthWrapper(SiteAuth):
    """
    An Auth driver for running jobs in a virtual environment. For a default local
    site, this is a no-op.
    """

    def __init__(self, realAuthDriver: SiteAuth) -> None:
        super().__init__()
        self._realAuthDriver = realAuthDriver

    def login(self, force: bool = False) -> bool:
        retVal = _executeInProjectVenv(
            _makeObjDriverCommandString(self._realAuthDriver) + \
            f"obj = driver.login({force}); " + \
            f"{_makeSerializeCommandString()}"
        )
        return lwfManager.deserialize(retVal)

    def isAuthCurrent(self) -> bool:
        retVal = _executeInProjectVenv(
            _makeObjDriverCommandString(self._realAuthDriver) + \
            "obj = driver.isAuthCurrent(); " + \
            f"{_makeSerializeCommandString()}"
        )
        return lwfManager.deserialize(retVal)



class VenvSiteRunWrapper(SiteRun):
    """
    A Run driver for running jobs in a virtual environment.
    """

    def __init__(self, realRunDriver: SiteRun) -> None:
        super().__init__()
        self._realRunDriver = realRunDriver

    def submit(self, jobDefn: Union['JobDefn', str],
        parentContext: Union[JobContext, Workflow, str] = None,
        computeType: str = None, runArgs: Union[dict, str] = None) -> JobStatus:
        retVal = _executeInProjectVenv(
            _makeObjDriverCommandString(self._realRunDriver) + \
            f"obj = driver.submit({_makeObjWrapper(jobDefn)}, " +\
            f"{_makeObjWrapper(parentContext)}, '{computeType}', " + \
            f"{_makeObjWrapper(runArgs)}); " + \
            f"{_makeSerializeCommandString()}"
        )
        return lwfManager.deserialize(retVal)

    def getStatus(self, jobId: str) -> JobStatus:
        retVal = _executeInProjectVenv(
            _makeObjDriverCommandString(self._realRunDriver) + \
            f"obj = driver.getStatus('{jobId}'); " + \
            f"{_makeSerializeCommandString()}"
        )
        return lwfManager.deserialize(retVal)

    def cancel(self, jobContext: Union[JobContext, str]) -> bool:
        retVal = _executeInProjectVenv(
            _makeObjDriverCommandString(self._realRunDriver) + \
            f"obj = driver.cancel({_makeObjWrapper(jobContext)}); " + \
            f"{_makeSerializeCommandString()}"
        )
        return lwfManager.deserialize(retVal)



class VenvSiteRepoWrapper(SiteRepo):
    """
    A Repo driver for running jobs in a virtual environment.
    """

    def __init__(self, realRepoDriver: SiteRepo) -> None:
        super().__init__()
        self._realRepoDriver = realRepoDriver

    def put(
        self,
        localPath: str,
        siteObjPath: str,
        jobContext: Union[JobContext, str] = None,
        metasheet: Union[Metasheet, str] = None
    ) -> Metasheet:
        retVal = _executeInProjectVenv(
            _makeObjDriverCommandString(self._realRepoDriver) + \
            f"obj = driver.put('{localPath}', '{siteObjPath}', " + \
            f"{_makeObjWrapper(jobContext)}, {_makeObjWrapper(metasheet)}); " + \
            f"{_makeSerializeCommandString()}"
        )
        return lwfManager.deserialize(retVal)
    
    def get(
        self,
        siteObjPath: str,
        localPath: str,
        jobContext: Union[JobContext, str] = None
    ) -> str:
        retVal = _executeInProjectVenv(
            _makeObjDriverCommandString(self._realRepoDriver) + \
            f"obj = driver.get('{siteObjPath}', '{localPath}', {_makeObjWrapper(jobContext)}); " + \
            f"{_makeSerializeCommandString()}"
        )
        return lwfManager.deserialize(retVal)
    
    def find(self, queryRegExs: dict) -> List[Metasheet]:
        retVal = _executeInProjectVenv(
            _makeObjDriverCommandString(self._realRepoDriver) + \
            f"obj = driver.find({_makeObjWrapper(queryRegExs)}); " + \
            f"{_makeSerializeCommandString()}"
        )
        return lwfManager.deserialize(retVal)

class VenvSiteSpinWrapper(SiteSpin):
    def __init__(self, realSpinDriver: SiteSpin) -> None:
        super().__init__()
        self._realSpinDriver = realSpinDriver

    def listComputeTypes(self) -> List[str]:
        retVal = _executeInProjectVenv(
            _makeObjDriverCommandString(self._realSpinDriver) + \
            "obj = driver.listComputeTypes(); " + \
            f"{_makeSerializeCommandString()}"
        )
        return lwfManager.deserialize(retVal)


class VenvSite(Site, ABC):
    """
    A Site driver for running jobs in a virtual environment.
    """

    def __init__(self, site_name: str = None,
                    auth_driver: SiteAuth = None,
                    run_driver: SiteRun = None,
                    repo_driver: SiteRepo = None,
                    spin_driver: SiteSpin = None):
        # TODO use a LocalSite when the driver is not available - see also LocalVenvSite
        self.localSite = LocalSite()
        if site_name is not None:
            self.localSite.setSiteName(site_name)
        else:
            self.localSite.setSiteName("local-venv")   # TODO make a constant
        self._realAuthDriver = auth_driver or self.localSite.getAuthDriver()
        self._realRunDriver = run_driver or self.localSite.getRunDriver()
        self._realRepoDriver = repo_driver or self.localSite.getRepoDriver()
        self._realSpinDriver = spin_driver or self.localSite.getSpinDriver()
        super().__init__(site_name,
                         VenvSiteAuthWrapper(self._realAuthDriver),
                         VenvSiteRunWrapper(self._realRunDriver),
                         VenvSiteRepoWrapper(self._realRepoDriver),
                         VenvSiteSpinWrapper(self._realSpinDriver))
