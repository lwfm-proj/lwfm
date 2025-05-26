"""
Abstract class for a Site which runs its functions in a virtual environment. This allows the 
Site to run with its own set of dependencies independent of the global environment.

The use case is when a Site has dependencies that are not available in or conflict with the global
environment. For example, a Site for a quantum computer might be pinned to an older version of the
qiskit library, while the global environment has a newer version. In this case, the Site can run in
its own virtual environment with the older version of qiskit. (Dealing with the interim
representation of the qiskit circuit is handled differently... via QASM or other quantum circuit
representation - this class just deals with the library dependencies of Sites.)

In general, you can take a Site class and wrap it in a VenvSite class to run it in a virtual
environment. We show an example of this in the LocalVenvSite class, which ends up being very little
code - basically a contructor. The heavy lift is in this class. Here each of the "pillars" are
represented - Auth, Run, Repo, and Spin. Each method of each pillar is represented. The
implementation is to wrap the Site method in a subprocess and execute it in the virtual
environment. Thus each public method here tends to be rather cookie cutter.

Here's an example of gthe VenvSite's Auth pillar login() method:

    def login(self, force: bool = False) -> bool:
        retVal = _executeInProjectVenv(
            _makeSiteDriverCommandString(self._realAuthDriver) +    <-- wrap the real auth driver 
            f"obj = driver.login({force}); " +                      <-- call login() with args
            f"{_makeSerializeReturnString()}"                       <-- serialize subproc result 
        )
        return lwfManager.deserialize(retVal)                       <-- return deserialized result

The user will call login() on the instantied Site Pillar object passing it the normal arguments
(e.g. in the case of login() the "force" argument). The method will construct a command string to
call login() on a real Site object (self._realAuthDriver), passing in the user's argument.
The results of the call might return an arbitrary object (in the case of login its just a bool,
but in general a Site method might return any object) - so we serialize it so it can be returned
by the subprocess running the virtual env. The result is then deserialized in the original process
and returned to the user.

The string created above and passed into _executeInProjectVenv() looks something like this:

    from lwfm.sites.LocalSite import LocalSiteAuth; 
    driver = LocalSiteAuth(); 
    obj = driver.login(False); 
    from lwfm.midware.LwfManager import lwfManager; 
    obj = lwfManager.serialize(obj); 
    print(obj)

All this gets passed to "python -c" and the result is serialized back to the invoker.

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

def _makeVenvPath(siteName: str) -> str:
    """
    Construct the path to the virtual environment used to run the Site driver.
    """
    props = lwfManager.getSiteProperties(siteName)
    if props['venv']:
        return os.path.expanduser(props['venv'])
    return os.path.join(os.getcwd(), ".venv")



def _executeInProjectVenv(siteName: str, script_path_cmd: str = None) -> str:
    """
    Run a command in a virtual environment, used to run canonical Site methods.
    Arbitrary scripts can subsequently be run via Site.Run.submit().
    """
    if script_path_cmd is None:
        raise ValueError("script_path_cmd is required")

    venv_path = _makeVenvPath(siteName)
    if os.name == "nt":    # windows
        python_executable = os.path.join(venv_path, "Scripts", "python.exe")
    else:   # a real OS
        python_executable = os.path.join(venv_path, "bin", "python")

    logger.info(f"_executeInProjectVenv: executing in venv {venv_path} cmd: {script_path_cmd}")

    try:
        # Execute the command, making sure to capture only the last line as the
        # serialized return value
        # This will allow earlier prints to go to a file without disrupting the
        # return value
        modified_cmd = script_path_cmd
        if "driver.submit(" in script_path_cmd:
            # Modify script to make sure the serialized result is the only thing printed at the end
            modified_cmd = script_path_cmd.replace("print(obj)",
                "import sys; sys.stdout.write('RESULT_MARKER: ' + obj)")

        # execute a semicolon separated command in the virtual environment; this
        # includes an import statement and the method call
        process = subprocess.Popen([python_executable, "-c", modified_cmd],
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
            # Check if we marked the result
            if "RESULT_MARKER: " in stdout:
                # Extract only the serialized part
                result_lines = stdout.split("RESULT_MARKER: ")
                return result_lines[-1].strip()
            # Otherwise return the whole output
            return stdout

        # if stdout:
        #     # if the command was successful, return the output
        #     return stdout
        return None
    except Exception as ex:
        # something really bad happened
        logger.error(f"_runInVenv: an error occurred: {ex}")


def _getClassName(obj: object) -> str:
    """
    Get the class name for an object.
    """
    if obj is None:
        return "None"
    return obj.__class__.__name__


def _getPackageName(obj: object) -> str:
    """
    Get the package name for an object.
    """
    if obj is None:
        return "None"
    return f"{obj.__class__.__module__}"


def _makeSerializeReturnString() -> str:
    """
    Serialize an object into a string. This is used to pass objects back from the venv
    subprocess back to the main process (see discussion above).
    """
    # construct the command string to execute in the virtual environment
    return "from lwfm.midware.LwfManager import lwfManager; " + \
           "obj = lwfManager.serialize(obj); " + \
           "print(obj)"


def _makeSiteDriverCommandString(sitePillar: SitePillar) -> str:
    """
    Import a class from a module and instantiate it.
    """
    # construct the command string to execute in the virtual environment
    pkgName = _getPackageName(sitePillar)
    className = _getClassName(sitePillar)
    return f"from {pkgName} import {className}; " + \
           f"driver = {className}(); "


def _makeArgWrapper(obj: object) -> str:
    """
    Any argument we want to pass to a Site method might be a complex object, so we
    serialize it to pass it to the subprocess running the venv.
    """
    return f"'{lwfManager.serialize(obj)}'"


# *********************************************************************************
# site pillar wrappers

class VenvSiteAuthWrapper(SiteAuth):
    """
    An Auth driver for running jobs in a virtual environment. For a default local
    site, this is a no-op.
    """

    def __init__(self, siteName: str, realAuthDriver: SiteAuth) -> None:
        super().__init__()
        self._siteName = siteName
        self._realAuthDriver = realAuthDriver

    def login(self, force: bool = False) -> bool:
        retVal = _executeInProjectVenv(
            self._siteName,
            _makeSiteDriverCommandString(self._realAuthDriver) + \
            f"obj = driver.login({force}); " + \
            f"{_makeSerializeReturnString()}"
        )
        return lwfManager.deserialize(retVal)

    def isAuthCurrent(self) -> bool:
        retVal = _executeInProjectVenv(
            self._siteName,
            _makeSiteDriverCommandString(self._realAuthDriver) + \
            "obj = driver.isAuthCurrent(); " + \
            f"{_makeSerializeReturnString()}"
        )
        return lwfManager.deserialize(retVal)


# *********************************************************************************

class VenvSiteRunWrapper(SiteRun):
    """
    A Run driver for running jobs in a virtual environment.
    """

    def __init__(self, siteName: str, realRunDriver: SiteRun) -> None:
        super().__init__()
        self._siteName = siteName
        self._realRunDriver = realRunDriver

    def submit(self, jobDefn: Union['JobDefn', str],
        parentContext: Union[JobContext, Workflow, str] = None,
        computeType: str = None, runArgs: Union[dict, str] = None) -> JobStatus:
        retVal = _executeInProjectVenv(
            self._siteName,
            _makeSiteDriverCommandString(self._realRunDriver) + \
            f"obj = driver.submit({_makeArgWrapper(jobDefn)}, " +\
            f"{_makeArgWrapper(parentContext)}, '{computeType}', " + \
            f"{_makeArgWrapper(runArgs)}); " + \
            f"{_makeSerializeReturnString()}"
        )
        print("retVal = " + retVal)
        return lwfManager.deserialize(retVal)

    def getStatus(self, jobId: str) -> JobStatus:
        retVal = _executeInProjectVenv(
            self._siteName,
            _makeSiteDriverCommandString(self._realRunDriver) + \
            f"obj = driver.getStatus('{jobId}'); " + \
            f"{_makeSerializeReturnString()}"
        )
        return lwfManager.deserialize(retVal)

    def cancel(self, jobContext: Union[JobContext, str]) -> bool:
        retVal = _executeInProjectVenv(
            self._siteName,
            _makeSiteDriverCommandString(self._realRunDriver) + \
            f"obj = driver.cancel({_makeArgWrapper(jobContext)}); " + \
            f"{_makeSerializeReturnString()}"
        )
        return lwfManager.deserialize(retVal)


# *********************************************************************************

class VenvSiteRepoWrapper(SiteRepo):
    """
    A Repo driver for running jobs in a virtual environment.
    """

    def __init__(self, siteName: str, realRepoDriver: SiteRepo) -> None:
        super().__init__()
        self._siteName = siteName
        self._realRepoDriver = realRepoDriver

    def put(
        self,
        localPath: str,
        siteObjPath: str,
        jobContext: Union[JobContext, str] = None,
        metasheet: Union[Metasheet, str] = None
    ) -> Metasheet:
        retVal = _executeInProjectVenv(
            self._siteName,
            _makeSiteDriverCommandString(self._realRepoDriver) + \
            f"obj = driver.put('{localPath}', '{siteObjPath}', " + \
            f"{_makeArgWrapper(jobContext)}, {_makeArgWrapper(metasheet)}); " + \
            f"{_makeSerializeReturnString()}"
        )
        return lwfManager.deserialize(retVal)

    def get(
        self,
        siteObjPath: str,
        localPath: str,
        jobContext: Union[JobContext, str] = None
    ) -> str:
        retVal = _executeInProjectVenv(
            self._siteName,
            _makeSiteDriverCommandString(self._realRepoDriver) + \
            f"obj = driver.get('{siteObjPath}', '{localPath}', {_makeArgWrapper(jobContext)}); " + \
            f"{_makeSerializeReturnString()}"
        )
        return lwfManager.deserialize(retVal)

    def find(self, queryRegExs: dict) -> List[Metasheet]:
        retVal = _executeInProjectVenv(
            self._siteName,
            _makeSiteDriverCommandString(self._realRepoDriver) + \
            f"obj = driver.find({_makeArgWrapper(queryRegExs)}); " + \
            f"{_makeSerializeReturnString()}"
        )
        return lwfManager.deserialize(retVal)


# *********************************************************************************

class VenvSiteSpinWrapper(SiteSpin):
    def __init__(self, siteName: str, realSpinDriver: SiteSpin) -> None:
        super().__init__()
        self._siteName = siteName
        self._realSpinDriver = realSpinDriver

    def listComputeTypes(self) -> List[str]:
        retVal = _executeInProjectVenv(
            self._siteName,
            _makeSiteDriverCommandString(self._realSpinDriver) + \
            "obj = driver.listComputeTypes(); " + \
            f"{_makeSerializeReturnString()}"
        )
        return lwfManager.deserialize(retVal)


# *********************************************************************************

class VenvSite(Site, ABC):
    """
    A Site driver for running jobs in a virtual environment.
    """

    _DEFAULT_SITE_NAME = "local-venv"

    def __init__(self, site_name: str = None,
                    auth_driver: SiteAuth = None,
                    run_driver: SiteRun = None,
                    repo_driver: SiteRepo = None,
                    spin_driver: SiteSpin = None):
        self.localSite = LocalSite()
        if site_name is not None:
            self.localSite.setSiteName(site_name)
        else:
            self.localSite.setSiteName(self._DEFAULT_SITE_NAME)
        self._realAuthDriver = auth_driver or self.localSite.getAuthDriver()
        self._realRunDriver = run_driver or self.localSite.getRunDriver()
        self._realRepoDriver = repo_driver or self.localSite.getRepoDriver()
        self._realSpinDriver = spin_driver or self.localSite.getSpinDriver()
        super().__init__(site_name,
                         VenvSiteAuthWrapper(self.localSite.getSiteName(), self._realAuthDriver),
                         VenvSiteRunWrapper(self.localSite.getSiteName(), self._realRunDriver),
                         VenvSiteRepoWrapper(self.localSite.getSiteName(), self._realRepoDriver),
                         VenvSiteSpinWrapper(self.localSite.getSiteName(), self._realSpinDriver))
