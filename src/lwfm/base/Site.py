"""
Site: Defines an abstract computing location which exposes canonical verbs for
the Auth, Run, and Repo and Spin logical subsystems. The purpose
of lwfm is to permit workflows which span Sites. A new Site would inherit or
implement a driver for each of the subsystems. An application will instantiate
a Site driver using the Site factory method.

The Auth portion of the Site is responsible for user authentication and
authorization. The Run sub-interface provides verbs such as job submit and job
cancel. The Repo portion provides verbs for managing files and metadata.
The Spin portion provides verbs for spinning up and spinning down computing 
resources from a cafeteria of options.

The Site factory method returns the Python class which implements the
interfaces for the named Site. ~/.lwfm/sites.toml can be used to augment the
list of sites provided here with a user's own custom Site implementations. 
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring
#pylint: disable = broad-exception-caught, broad-exception-raised

from abc import ABC, abstractmethod

import json
from typing import List, TYPE_CHECKING, Union

from lwfm.base.JobContext import JobContext
from lwfm.base.JobStatus import JobStatus
from lwfm.base.Metasheet import Metasheet
from lwfm.base.Workflow import Workflow

from lwfm.midware._impl.ObjectSerializer import ObjectSerializer
from lwfm.midware._impl.SiteConfigVenv import SiteConfigVenv

# Only import for type checking, not at runtime
if TYPE_CHECKING:
    from lwfm.base.JobDefn import JobDefn



# ***************************************************************************
class SitePillar(ABC):
    _siteName : str = None

    def setSiteName(self, siteName: str) -> None:
        self._siteName = siteName

    def getSiteName(self) -> str:
        return self._siteName


# ***************************************************************************

class SiteAuth(SitePillar):
    """
    Auth: an interface for a Site's user authentication and authorization
    functions.  Permits a Site to provide some kind of a "login" be it
    programmatic, forced interactive, etc.  A given Site's implementation of
    Auth might squirrel away some returned token (should the Site use such
    things). It could conceptually then provide a quicker "is the auth current"
    method. We assume the implementation of a Site's Run and Repo subsystems
    will, if needed, be provided access under the hood to the necessary
    implementation details from the Auth subsystem.
    """

    @abstractmethod
    def login(self, force: bool = False) -> bool:
        """
        Login to the Site using the Site's own mechanism for authentication and
        caching.

        Params:
            force - if true, forces a login even if the Site detects that the 
                current login is still viable
        Returns:
            bool - true if success, else false

        Example:
            site = Site.getSite(siteName)
            site.getAuth().login()
        """


    @abstractmethod
    def isAuthCurrent(self) -> bool:
        """
        Is the currently cached Site login info still viable?

        Returns:
            bool - true if the login is still viable, else false; the Site might
            not cache, and thus always return false

        Example:
            site = Site.getSite(siteName)
            site.getAuth().login()
            site.getAuth().isAuthCurrent()
        """


# **************************************************************************


class SiteRun(SitePillar):
    """
    Run: in its most basic form, the Run subsystem provides a mechanism to
    submit a job, cancel the job, and interrogate the job's status.  The
    submitting of a job might, for some Sites, involve a batch scheduler.
    Or, for some Sites (like a "local" Site), the run might be immediate.
    On submit of a job the method returns a JobStatus.  A job definition
    (JobDefn) is a description of the job.  Its the role of the Site's Run
    subsystem to interpret the abstract JobDefn in the context of that
    Site.  Thus the JobDefn permits arbitrary name=value pairs which might
    be necessary to submit a job at that Site.  The JobStatus returned is
    canonical - the Site's own status name set is mapped into the lwfm
    canonical name set by the implementation of the Site.Run itself.
    """

    @classmethod
    def _submitJob(cls, jobDefn, parentContext=None, computeType=None, runArgs=None,
        inVenv: bool = False, siteName: str  = None, realRunDriver: 'SiteRun' = None):
        """
        This helper function, not a member of the public interface, lets Python
        threading instantiate a SiteRunDriver of the correct subtype on demand.
        It is used, for example, by the lwfm middleware's event handler mechanism
        to reflectively instantiate a Site Run driver of the correct subtype,
        and then call its submitJob() method.
        """
        if inVenv:
            runDriver = cls(siteName, realRunDriver)
        else:
            runDriver = cls()
        runDriver.submit(jobDefn, parentContext, computeType, runArgs)

    @abstractmethod
    def submit(self, jobDefn: Union['JobDefn', str],
        parentContext: Union[JobContext, Workflow, str] = None,
        computeType: str = None, runArgs: Union[dict, str] = None) -> JobStatus:
        """
        Submit the job for execution on this Site. It is an implementation
        detail of the Site what that means - everything from pseudo-immediate
        command line execution, to scheduling on an HPC system. The caller
        should assume the run is asynchronous. We would assume all Sites would
        implement this method. Note that "compute type" is an optional member
        of JobDefn, and might be used by the Site to direct the execution of
        the job.

        [We note that both the JobDefn and the JobContext potentially contain
        a reference to a compute type. Since the Job Context is historical,
        and provided to give that historical context to the job we're about to
        run, its strongly suggested that Site.Run implementations use the
        compute type named in the JobDef, if the concept is present at all on
        the Site. We note also that compute type and Site are relatively
        interchangeable - one can model a compute resource as a compute type,
        or as its own Site, perhaps with a complete inherited Site driver
        implementation.]

        Params:
            jobDefn - the definition of the job to run, might include the name
                of a script, include arguments for the run, etc.
            parentContext - [optional - if none provided, one will be assigned
                and managed by the lwfm framework] information about the
                current JobContext which might be running, thus the job we are
                submitting will be tracked in the digital thread
        Returns:
            JobStatus - preliminary status, containing a JobContext including
                the canonical and Site-specific native job id
        """

    @abstractmethod
    def getStatus(self, jobId: str) -> JobStatus:
        """
        Check the status of a job running on this Site.  The JobContext is an 
        attribute of the JobStatus, and contains the canonical and Site-specific 
        native job id. The implementation of getJobStatus() may use any portion
        of the JobContext to obtain the status of the job, as needed by the Site.

        Parameters:
            jobContext - the context of the executing job, including the native
                job id
        Returns:
            JobStatus - the current known status of the job
        """

    @abstractmethod
    def cancel(self, jobContext: Union[JobContext, str]) -> bool:
        """
        Cancel the job, which might be in a queued state, or might already be running.
        Its up to to the Site how to handle the cancel, including not doing it.  The
        JobContext is obtained from the JobStatus returned by the call to
        submitJob(), or any call to getJobStatus().

        Params:
            jobContext - the context of the job, including the Site-native job id
        Returns:
            bool - true if a successful cancel, else false; callers should invoke the
                getJobStatus() method to obtain final terminal status (e.g., the job
                might have completed successfully prior to the cancel being received,
                the cancel might not be instantaneous, etc.)
        """


# ****************************************************************************

class SiteRepo(SitePillar):

    # The siteObjPath can be many things depending on the site.  For a local site,
    # its just a path.  For a remote site, its a reference to an object in some
    # remote repository which might be filesystem, or use some URI, or whatever
    # the site wants.  Some sites may provide no mechanism at all.
    # The siteObjPath can also be blank, which is intended to mean the
    # data is not moved anywhere by instead is just being checked into management
    # in place with metadata.

    # ask the site to store this local file at a path as an object at the site reference
    # and return the metasheet
    @abstractmethod
    def put(
        self,
        localPath: str,
        siteObjPath: str,
        jobContext: Union[JobContext, str] = None,
        metasheet: Union[Metasheet, str] = None
    ) -> Metasheet:
        pass


    # ask the site to fetch an object by reference and write it locally to a path,
    # returning the local path where written
    @abstractmethod
    def get(
        self,
        siteObjPath: str,
        localPath: str,
        jobContext: Union[JobContext, str] = None
    ) -> str:
        pass

    # find metasheets by query
    @abstractmethod
    def find(self, queryRegExs: Union[dict, str]) -> List[Metasheet]:
        pass


# *****************************************************************************
# Spin: mostly vaporware.  In theory some Sites would expose mechanisms to create
# (provision) and destroy various kinds of computing devices.  These might be
# single nodes, or entire turnkey cloud-bases HPC systems.

class SiteSpin(SitePillar):

    @abstractmethod
    def listComputeTypes(self) -> List[str]:
        """
        List the compute types supported by the Site.  Compute types are specific
        runtime resources (if any) within the Site. The Site might not have any concept
        and return an empty list, or a list of one singular type of the Site.  Or, the
        Site might front a number of resources, and return a list of names.

        Returns:
            List[str] - a list of names, potentially empty or None
        """



# *********************************************************************************


def makeSiteNameCommandString(siteName: str) -> str:
    """
    Create a command string to get the appropriate site implementation directly,
    avoiding the potential recursion from lwfManager.getSite().
    """
    return "from lwfm.midware.LwfManager import lwfManager; " + \
        "props = lwfManager.getSiteProperties('" + siteName + "'); " + \
        "class_name = props['class'].split('.')[-1]; " + \
        "module_name = '.'.join(props['class'].split('.')[:-1]); " + \
        "site_module = __import__(module_name, fromlist=['*']); " + \
        "site_class = getattr(site_module, class_name); " + \
        f"site = site_class(); site.setSiteName('{siteName}'); "


def makeSiteDriverCommandString(sitePillar: SitePillar, siteName: str) -> str:
    """
    Import a driver class from a module and instantiate it.
    """
    # construct the command string to execute in the virtual environment
    driverStr = ""
    if isinstance(sitePillar, SiteAuth):
        driverStr = "driver = site.getAuthDriver()"
    elif isinstance(sitePillar, SiteRun):
        driverStr = "driver = site.getRunDriver()"
    elif isinstance(sitePillar, SiteRepo):
        driverStr = "driver = site.getRepoDriver()"
    elif isinstance(sitePillar, SiteSpin):
        driverStr = "driver = site.getSpinDriver()"
    else:
        driverStr = "driver = site.getAuthDriver()"
    return makeSiteNameCommandString(siteName) + \
        driverStr + "; "


# *********************************************************************************
# site pillar wrappers

class _VenvSiteAuthWrapper(SiteAuth):
    """
    An Auth driver for running jobs in a virtual environment. For a default local
    site, this is a no-op.
    """

    def __init__(self, siteName: str, realAuthDriver: str) -> None:
        super().__init__()
        self._siteName = siteName
        self._realAuthDriver = json.dumps(realAuthDriver)
        self._siteConfigVenv = SiteConfigVenv()

    def login(self, force: bool = False) -> bool:
        retVal = self._siteConfigVenv.executeInProjectVenv(
            self._siteName,
            "from lwfm.midware._impl.ObjectSerializer import ObjectSerializer; " + \
            f"driver = ObjectSerializer.deserialize('{self._realAuthDriver}'); " + \
            f"obj = driver.login({force}); " + \
            f"{self._siteConfigVenv.makeSerializeReturnString()}"
        )
        return ObjectSerializer.deserialize(retVal)

    def isAuthCurrent(self) -> bool:
        retVal = self._siteConfigVenv.executeInProjectVenv(
            self._siteName,
            "from lwfm.midware._impl.ObjectSerializer import ObjectSerializer; " + \
            f"driver = ObjectSerializer.deserialize('{self._realAuthDriver}'); " + \
            "obj = driver.isAuthCurrent(); " + \
            f"{self._siteConfigVenv.makeSerializeReturnString()}"
        )
        return ObjectSerializer.deserialize(retVal)


# *********************************************************************************

class _VenvSiteRunWrapper(SiteRun):
    """
    A Run driver for running jobs in a virtual environment.
    """

    def __init__(self, siteName: str, realRunDriver: str) -> None:
        super().__init__()
        self._siteName = siteName
        self._realRunDriver = json.dumps(realRunDriver)
        self._siteConfigVenv = SiteConfigVenv()


    def submit(self, jobDefn: Union['JobDefn', str],
        parentContext: Union[JobContext, Workflow, str] = None,
        computeType: str = None, runArgs: Union[dict, str] = None) -> JobStatus:
        retVal = self._siteConfigVenv.executeInProjectVenv(
            self._siteName,
            "from lwfm.midware._impl.ObjectSerializer import ObjectSerializer; " + \
            f"driver = ObjectSerializer.deserialize('{self._realRunDriver}'); " + \
            f"obj = driver.submit({self._siteConfigVenv.makeArgWrapper(jobDefn)}, " +\
            f"{self._siteConfigVenv.makeArgWrapper(parentContext)}, '{computeType}', " + \
            f"{self._siteConfigVenv.makeArgWrapper(runArgs)}); " + \
            f"{self._siteConfigVenv.makeSerializeReturnString()}"
        )
        return ObjectSerializer.deserialize(retVal)

    def getStatus(self, jobId: str) -> JobStatus:
        retVal = self._siteConfigVenv.executeInProjectVenv(
            self._siteName,
            "from lwfm.midware._impl.ObjectSerializer import ObjectSerializer; " + \
            f"driver = ObjectSerializer.deserialize('{self._realRunDriver}'); " + \
            f"obj = driver.getStatus('{jobId}'); " + \
            f"{self._siteConfigVenv.makeSerializeReturnString()}"
        )
        return ObjectSerializer.deserialize(retVal)

    def cancel(self, jobContext: Union[JobContext, str]) -> bool:
        retVal = self._siteConfigVenv.executeInProjectVenv(
            self._siteName,
            "from lwfm.midware._impl.ObjectSerializer import ObjectSerializer; " + \
            f"driver = ObjectSerializer.deserialize('{self._realRunDriver}'); " + \
            f"obj = driver.cancel({self._siteConfigVenv.makeArgWrapper(jobContext)}); " + \
            f"{self._siteConfigVenv.makeSerializeReturnString()}"
        )
        return ObjectSerializer.deserialize(retVal)


# *********************************************************************************

class _VenvSiteRepoWrapper(SiteRepo):
    """
    A Repo driver for running jobs in a virtual environment.
    """

    def __init__(self, siteName: str, realRepoDriver: str) -> None:
        super().__init__()
        self._siteName = siteName
        self._realRepoDriver = json.dumps(realRepoDriver)
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
            "from lwfm.midware._impl.ObjectSerializer import ObjectSerializer; " + \
            f"driver = ObjectSerializer.deserialize('{self._realRepoDriver}'); " + \
            f"obj = driver.put('{localPath}', '{siteObjPath}', " + \
            f"{self._siteConfigVenv.makeArgWrapper(jobContext)}, " + \
            f"{self._siteConfigVenv.makeArgWrapper(metasheet)}); " + \
            f"{self._siteConfigVenv.makeSerializeReturnString()}"
        )
        return ObjectSerializer.deserialize(retVal)

    def get(
        self,
        siteObjPath: str,
        localPath: str,
        jobContext: Union[JobContext, str] = None
    ) -> str:
        retVal = self._siteConfigVenv.executeInProjectVenv(
            self._siteName,
            "from lwfm.midware._impl.ObjectSerializer import ObjectSerializer; " + \
            f"driver = ObjectSerializer.deserialize('{self._realRepoDriver}'); " + \
            f"obj = driver.get('{siteObjPath}', '{localPath}', " + \
            f"{self._siteConfigVenv.makeArgWrapper(jobContext)}); " + \
            f"{self._siteConfigVenv.makeSerializeReturnString()}"
        )
        return ObjectSerializer.deserialize(retVal)

    def find(self, queryRegExs: dict) -> List[Metasheet]:
        retVal = self._siteConfigVenv.executeInProjectVenv(
            self._siteName,
            "from lwfm.midware._impl.ObjectSerializer import ObjectSerializer; " + \
            f"driver = ObjectSerializer.deserialize('{self._realRepoDriver}'); " + \
            f"obj = driver.find({self._siteConfigVenv.makeArgWrapper(queryRegExs)}); " + \
            f"{self._siteConfigVenv.makeSerializeReturnString()}"
        )
        return ObjectSerializer.deserialize(retVal)


# *********************************************************************************

class _VenvSiteSpinWrapper(SiteSpin):
    def __init__(self, siteName: str, realSpinDriver: str) -> None:
        super().__init__()
        self._siteName = siteName
        self._realSpinDriver = json.dumps(realSpinDriver)
        self._siteConfigVenv = SiteConfigVenv()


    def listComputeTypes(self) -> List[str]:
        retVal = self._siteConfigVenv.executeInProjectVenv(
            self._siteName,
            "from lwfm.midware._impl.ObjectSerializer import ObjectSerializer; " + \
            f"driver = ObjectSerializer.deserialize('{self._realSpinDriver}'); " + \
            "obj = driver.listComputeTypes(); " + \
            f"{self._siteConfigVenv.makeSerializeReturnString()}"
        )
        return ObjectSerializer.deserialize(retVal)


# *********************************************************************************
# Site: the Site is simply a name and the getters and setters for its Auth, Run,
# Repo subsystems.
#
# The getSite() factory utility method returns the Python class which implements the
# interfaces for the named Site. ~/.lwfm/sites.toml can be used to augment the
# list of sites provided here with a user's own custom Site implementations. In the
# event of a name collision between the user's sites.txt and those hardcoded here,
# the user's sites.txt config trumps.


class Site:
    def __init__(self, site_name: str = None,
                auth_driver: Union[SiteAuth, str] = None,
                run_driver: Union[SiteRun, str] = None,
                repo_driver: Union[SiteRepo, str] = None,
                spin_driver: Union[SiteSpin, str] = None):
        self._site_name = site_name
        self._auth_driver = auth_driver
        self._run_driver = run_driver
        self._repo_driver = repo_driver
        self._spin_driver = spin_driver
        self._remote = False
        self._venv = False

    def getSiteName(self):
        return self._site_name

    def setSiteName(self, name):
        self._site_name = name

    def getAuthDriver(self):
        if self.isVenv():
            return _VenvSiteAuthWrapper(self.getSiteName(), self._auth_driver)
        return self._auth_driver

    def setAuthDriver(self, driver):
        self._auth_driver = driver

    def getRunDriver(self):
        if self.isVenv():
            return _VenvSiteRunWrapper(self.getSiteName(), self._run_driver)
        return self._run_driver

    def setRunDriver(self, driver):
        self._run_driver = driver

    def getRepoDriver(self):
        if self.isVenv():
            return _VenvSiteRepoWrapper(self.getSiteName(), self._repo_driver)
        return self._repo_driver

    def setRepoDriver(self, driver):
        self._repo_driver = driver

    def getSpinDriver(self):
        if self.isVenv():
            return _VenvSiteSpinWrapper(self.getSiteName(), self._spin_driver)
        return self._spin_driver

    def setSpinDriver(self, driver):
        self._spin_driver = driver

    def isRemote(self) -> bool:
        return self._remote

    def setRemote(self, remote):
        self._remote = remote

    def isVenv(self) -> bool:
        return self._venv

    def setVenv(self, venv: bool):
        self._venv = venv
