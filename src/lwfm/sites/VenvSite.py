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
        retVal = self._siteConfigVenv.executeInProject(
            self._siteConfigVenv.makeSiteDriverCommandString(self._realAuthDriver) +
            <-- wrap the real auth driver 
            f"obj = driver.login({force}); " +                      <-- call login() with args
            f"{self._siteConfigVenv.makeSerializeReturnString()}"
            <-- serialize subproc result 
        )
        return lwfManager.deserialize(retVal)                       <-- return deserialized result

The user will call login() on the instantied Site Pillar object passing it the normal arguments
(e.g. in the case of login() the "force" argument). The method will construct a command string to
call login() on a real Site object (self._realAuthDriver), passing in the user's argument.
The results of the call might return an arbitrary object (in the case of login its just a bool,
but in general a Site method might return any object) - so we serialize it so it can be returned
by the subprocess running the virtual env. The result is then deserialized in the original process
and returned to the user.

The string created above and passed into self._siteConfigVenv.executeInProject() looks
something like this:

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


from lwfm.base.JobContext import JobContext
from lwfm.base.JobStatus import JobStatus
from lwfm.base.Metasheet import Metasheet
from lwfm.base.Workflow import Workflow
from lwfm.base.JobDefn import JobDefn
from lwfm.base.Site import Site, SiteAuth, SiteRun, SiteRepo, SiteSpin
from lwfm.sites.LocalSite import LocalSite
from lwfm.midware.LwfManager import lwfManager
from lwfm.midware._impl.SiteConfigVenv import SiteConfigVenv



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
        self._siteConfigVenv = SiteConfigVenv()

    def login(self, force: bool = False) -> bool:
        retVal = self._siteConfigVenv.executeInProjectVenv(
            self._siteName,
            self._siteConfigVenv.makeSiteDriverCommandString(self._realAuthDriver,
                self._siteName) + \
                f"obj = driver.login({force}); " + \
                f"{self._siteConfigVenv.makeSerializeReturnString()}"
        )
        return lwfManager.deserialize(retVal)

    def isAuthCurrent(self) -> bool:
        retVal = self._siteConfigVenv.executeInProjectVenv(
            self._siteName,
            self._siteConfigVenv.makeSiteDriverCommandString(self._realAuthDriver,
                self._siteName) + \
                "obj = driver.isAuthCurrent(); " + \
                f"{self._siteConfigVenv.makeSerializeReturnString()}"
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
        self._siteConfigVenv = SiteConfigVenv()


    def submit(self, jobDefn: Union['JobDefn', str],
        parentContext: Union[JobContext, Workflow, str] = None,
        computeType: str = None, runArgs: Union[dict, str] = None) -> JobStatus:
        retVal = self._siteConfigVenv.executeInProjectVenv(
            self._siteName,
            self._siteConfigVenv.makeSiteDriverCommandString(self._realRunDriver,
                self._siteName) + \
                f"obj = driver.submit({self._siteConfigVenv.makeArgWrapper(jobDefn)}, " +\
                f"{self._siteConfigVenv.makeArgWrapper(parentContext)}, '{computeType}', " + \
                f"{self._siteConfigVenv.makeArgWrapper(runArgs)}); " + \
                f"{self._siteConfigVenv.makeSerializeReturnString()}"
        )
        print("retVal = " + retVal)
        return lwfManager.deserialize(retVal)

    def getStatus(self, jobId: str) -> JobStatus:
        retVal = self._siteConfigVenv.executeInProjectVenv(
            self._siteName,
            self._siteConfigVenv.makeSiteDriverCommandString(self._realRunDriver,
                self._siteName) + \
                f"obj = driver.getStatus('{jobId}'); " + \
                f"{self._siteConfigVenv.makeSerializeReturnString()}"
        )
        return lwfManager.deserialize(retVal)

    def cancel(self, jobContext: Union[JobContext, str]) -> bool:
        retVal = self._siteConfigVenv.executeInProjectVenv(
            self._siteName,
            self._siteConfigVenv.makeSiteDriverCommandString(self._realRunDriver,
                self._siteName) + \
                f"obj = driver.cancel({self._siteConfigVenv.makeArgWrapper(jobContext)}); " + \
                f"{self._siteConfigVenv.makeSerializeReturnString()}"
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
        self._siteConfigVenv = SiteConfigVenv()


    def put(
        self,
        localPath: str,
        siteObjPath: str,
        jobContext: Union[JobContext, str] = None,
        metasheet: Union[Metasheet, str] = None
    ) -> Metasheet:
        retVal = self._siteConfigVenv.executeInProjectVenv(
            self._siteName,
            self._siteConfigVenv.makeSiteDriverCommandString(self._realRepoDriver,
                self._siteName) + \
                f"obj = driver.put('{localPath}', '{siteObjPath}', " + \
                f"{self._siteConfigVenv.makeArgWrapper(jobContext)}, {self._siteConfigVenv.makeArgWrapper(metasheet)}); " + \
                f"{self._siteConfigVenv.makeSerializeReturnString()}"
        )
        return lwfManager.deserialize(retVal)

    def get(
        self,
        siteObjPath: str,
        localPath: str,
        jobContext: Union[JobContext, str] = None
    ) -> str:
        retVal = self._siteConfigVenv.executeInProjectVenv(
            self._siteName,
            self._siteConfigVenv.makeSiteDriverCommandString(self._realRepoDriver,
                self._siteName) + \
                f"obj = driver.get('{siteObjPath}', '{localPath}', {self._siteConfigVenv.makeArgWrapper(jobContext)}); " + \
                f"{self._siteConfigVenv.makeSerializeReturnString()}"
        )
        return lwfManager.deserialize(retVal)

    def find(self, queryRegExs: dict) -> List[Metasheet]:
        retVal = self._siteConfigVenv.executeInProjectVenv(
            self._siteName,
            self._siteConfigVenv.makeSiteDriverCommandString(self._realRepoDriver,
                self._siteName) + \
                f"obj = driver.find({self._siteConfigVenv.makeArgWrapper(queryRegExs)}); " + \
                f"{self._siteConfigVenv.makeSerializeReturnString()}"
        )
        return lwfManager.deserialize(retVal)


# *********************************************************************************

class VenvSiteSpinWrapper(SiteSpin):
    def __init__(self, siteName: str, realSpinDriver: SiteSpin) -> None:
        super().__init__()
        self._siteName = siteName
        self._realSpinDriver = realSpinDriver
        self._siteConfigVenv = SiteConfigVenv()


    def listComputeTypes(self) -> List[str]:
        retVal = self._siteConfigVenv.executeInProjectVenv(
            self._siteName,
            self._siteConfigVenv.makeSiteDriverCommandString(self._realSpinDriver,
                self._siteName) + \
                "obj = driver.listComputeTypes(); " + \
                f"{self._siteConfigVenv.makeSerializeReturnString()}"
        )
        return lwfManager.deserialize(retVal)


# *********************************************************************************

class VenvSite(Site, ABC):
    """
    A Site driver for running jobs in a virtual environment.
    """

    _DEFAULT_SITE_NAME = "local-venv-base"

    def __init__(self, site_name: str = None,
                    auth_driver: SiteAuth = None,
                    run_driver: SiteRun = None,
                    repo_driver: SiteRepo = None,
                    spin_driver: SiteSpin = None):
        # Any pillars not covered by user drivers will be covered by those from LocalSite.
        self.localSite = LocalSite()
        self.localSite.setSiteName(site_name or self._DEFAULT_SITE_NAME)
        self.setSiteName(site_name or self._DEFAULT_SITE_NAME)

        # The idea here is there is a venv wrapper driver (self._authDriver), defined
        # above in this module, and a real driver, which does the work while wrapped
        # in the venv wrapper. If the caller does't provide a driver, the real driver
        # will be the one from LocalSite.
        if not auth_driver:
            self._realAuthDriver = self.localSite.getAuthDriver()
            self._realAuthDriver.setSite(self.localSite)
        else:
            self._realAuthDriver = auth_driver
            self._realAuthDriver.setSite(self)
        if not run_driver:
            self._realRunDriver = self.localSite.getRunDriver()
            self._realRunDriver.setSite(self.localSite)
        else:
            self._realRunDriver = run_driver
            self._realRunDriver.setSite(self)
        if not repo_driver:
            self._realRepoDriver = self.localSite.getRepoDriver()
            self._realRepoDriver.setSite(self.localSite)
        else:
            self._realRepoDriver = repo_driver
            self._realRepoDriver.setSite(self)
        if not spin_driver:
            self._realSpinDriver = self.localSite.getSpinDriver()
            self._realSpinDriver.setSite(self.localSite)
        else:
            self._realSpinDriver = spin_driver
            self._realSpinDriver.setSite(self)
        super().__init__(self.getSiteName(),
            VenvSiteAuthWrapper(self._realAuthDriver.getSite().getSiteName(),
                self._realAuthDriver),
            VenvSiteRunWrapper(self._realRunDriver.getSite().getSiteName(),
                self._realRunDriver),
            VenvSiteRepoWrapper(self._realRepoDriver.getSite().getSiteName(),
                self._realRepoDriver),
            VenvSiteSpinWrapper(self._realSpinDriver.getSite().getSiteName(),
                self._realSpinDriver))
