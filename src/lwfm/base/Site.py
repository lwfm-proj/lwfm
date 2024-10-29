"""
Site: Defines an abstract computing location which exposes canonical verbs for
the Auth, Run, and Repo (and optionally Spin) logical subsystems. The purpose
of lwfm is to permit workflows which span Sites. A new Site would inherit or
implement a driver for each of the subsystems. An application will instantiate
a Site driver using the Site factory method.

The Auth portion of the Site is responsible for user authentication and
authorization. The Run sub-interface provides verbs such as job submit and job
cancel. The Repo portion provides verbs for managing files and directories.
The Spin portion is vaporware, and would provide verbs for spinning up and
spinning down computing resources.

The Site factory method returns the Python class which implements the
interfaces for the named Site. ~/.lwfm/sites.txt can be used to augment the
list of sites provided here with a user's own custom Site implementations. In
the event of a name collision between the user's sites.txt and those hardcoded
here, the user's sites.txt config trumps.
"""

from enum import Enum
from abc import ABC, abstractmethod
from pathlib import Path
import os
from typing import List
import importlib

from lwfm.base.LwfmBase import LwfmBase
from lwfm.base.JobContext import JobContext
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobStatus import JobStatus
from lwfm.midware.Logger import Logger


# ***************************************************************************
class SitePillar(ABC):
    pass

# TODO 
class SiteFileRef:
    pass

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
        pass

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
        pass


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
    def _submitJob(cls, jobDefn, parentContext=None, computeType=None, runArgs=None):
        """
        This helper function, not a member of the public interface, lets Python
        threading instantiate a SiteRunDriver of the correct subtype on demand.
        It is used, for example, by the lwfm middleware's event handler mechanism
        to reflectively instantiate a Site Run driver of the correct subtype,
        and then call its submitJob() method.
        """
        runDriver = cls()
        runDriver.submit(jobDefn, parentContext)

    @abstractmethod
    def submit(self, jobDefn: JobDefn, parentContext: JobContext = None, 
        computeType: str = None, runArgs: dict = None) -> JobStatus:
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
        pass

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
        pass

    @abstractmethod
    def cancel(self, jobContext: JobContext) -> bool:
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
        pass


# ****************************************************************************


class SiteRepo(SitePillar):
    """
    Repo: Permits the movement of data objects to/from the Site.  The methods require a
    local file reference, and a reference to the remote file - a SiteFileRef.  The
    SiteFileRef permits arbitrary name=value pairs because the Site might require them
    to complete the transaction.
    """

    @abstractmethod
    def put(
        self,
        localPath: Path,
        siteFileRef: SiteFileRef,
        jobContext: JobContext = None,
    ) -> SiteFileRef:
        """
        Take the local file by path and put it to the remote Site.  This might be
        implemented by the Site as a simple filesystem copy, or it might be a checkin
        to a managed service - that's up to the Site.  If we're given a context,
        we use it, if not, we consider ourselves our own job.

        Params:
            localPath - a local file object
            siteFileRef - a reference to an abstract "file" entity on the Site -
                this is the target of the put operation
            jobContext - if we have a job context we wish to use (e.g. we are already
                inside a job and wish to indicate the digital thread parent-child
                relationships) then pass the context in, else the put operation will be
                performed as its own seminal job
        Returns:
            SiteFileRef - a reference to the entity put on the Site; the Site might also
                raise any kind of exception depending on the error case
        """
        pass

    @abstractmethod
    def get(
        self,
        siteFileRef: SiteFileRef,
        localPath: Path,
        jobContext: JobContext = None,
    ) -> Path:
        """
        Get the file from the remote site and write it local, returning a path to the local.
        If we're given a context, we use it, if not, we consider ourselves our own job.

        Params:
            siteFileRef - a reference to a data entity on the Site, the source of the get
            localPath - a local file object, the destination of the get
            jobContext - if we have a job context we wish to use (e.g. we are already
                inside a job and wish to indicate the
                digital thread parent-child relationships) then pass the context in,
                else the put operation will be performed as its own seminal job
        Returns:
            Path - the reference to the local location of the gotten file; during the
                get, the Site might also raise any kind of
                exception depending on the error case
        """
        pass

    @abstractmethod
    def find(self, siteFileRef: SiteFileRef) -> List[SiteFileRef]:
        """
        Get info about the file/dir on the remote site

        Params:
            siteFileRef - a reference to an abstract "file" entity on the Site, may be
                specialized partially (e.g. wildcards) though
                it is up to the Site to determine how to implement this search
        Returns:
            [SiteFileRef] - the instantiated file reference(s) (not the file, but the
                references), including the size, timestamp info, and other arbitrary
                metadata; may be a single file reference, or a list, or none
        """
        pass


# *****************************************************************************
# Spin: vaporware.  In theory some Sites would expose mechanisms to create 
# (provision) and destroy various kinds of computing devices.  These might be
# single nodes, or entire turnkey cloud-bases HPC systems.  Spin operations are
# modeled as jobs in order to permit sequential workflows which spin up resources,
# send them jobs, and then spin them down as part of a autonomous operation.  
# Basic verbs include: show cafeteria, spin up, spin down.  Spins would be 
# wrapped as Jobs allowing normal status interrogation.

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
        pass


# ****************************************************************************
# Site: the Site is simply a name and the getters and setters for its Auth, Run,
# Repo subsystems.
#
# The getSite() factory utility method returns the Python class which implements the
# interfaces for the named Site. ~/.lwfm/sites.txt can be used to augment the list
# of sites provided here with a user's own custom Site implementations. In the
# event of a name collision between the user's sites.txt and those hardcoded here,
# the user's sites.txt config trumps.


# LwfmBase field list
class _SiteFields(Enum):
    SITE_NAME = "siteName"


class Site(LwfmBase):
    _authDriver: SiteAuth = None
    _runDriver: SiteRun = None
    _repoDriver: SiteRepo = None
    _spinDriver: SiteSpin = None

    # pre-defined Sites and their associated driver implementations, each which
    # implements Auth, Run, Repo, [Spin]  these mappings can be extended in 
    # the ~/.lwfm/sites.txt configuration
    _SITES = {"local": "lwfm.sites.LocalSite.LocalSite", 
              "ibm_quantum": "lwfm.sites.IBMQuantumSite.IBMQuantumSite"}

    @staticmethod
    def _getSiteEntry(site: str):
        siteSet = Site._SITES
        # is there a local site config?
        path = os.path.expanduser("~") + "/.lwfm/sites.txt"
        # Check whether the specified path exists or not
        if os.path.exists(path):
            Logger.info("Loading custom site configs from ~/.lwfm/sites.txt")
            with open(path) as f:
                for line in f:
                    name, var = line.split("=")
                    name = name.strip()
                    var = var.strip()
                    Logger.info(
                        "Registering driver " + var + " for site " + name
                    )
                    siteSet[name] = var
        else:
            Logger.info(
                "No custom ~/.lwfm/sites.txt - using built-in site configs"
            )
        fullPath = siteSet[site]
        Logger.info("Obtaining site driver " + fullPath + " for " + site)
        if fullPath is not None:
            # parse the path into package and class parts for convenience
            xPackage = fullPath.rsplit(".", 1)[0]
            xClass = fullPath.rsplit(".", 1)[1]
            return [xPackage, xClass]
        else:
            return None

    @staticmethod
    def getSite(site: str = "local"):
        try:
            entry = Site._getSiteEntry(site)
            module = importlib.import_module(entry[0])
            class_ = getattr(module, str(entry[1]))
            inst = class_()
            inst.setName(site)
            return inst
        except Exception as ex:
            Logger.error(
                "Cannot instantiate Site for " + str(site) + " {}".format(ex)
            )

    def __init__(
        self,
        name: str,
        authDriver: SiteAuth,
        runDriver: SiteRun,
        repoDriver: SiteRepo,
        spinDriver: SiteSpin,
        args: dict = None,
    ):
        super(Site, self).__init__(args)
        self.setName(name)
        self.setAuth(authDriver)
        self.setRun(runDriver)
        self.setRepo(repoDriver)
        self.setSpin(spinDriver)

    def setName(self, name: str) -> None:
        LwfmBase._setArg(self, _SiteFields.SITE_NAME.value, name)

    def getName(self) -> str:
        return LwfmBase._getArg(self, _SiteFields.SITE_NAME.value)

    def setAuth(self, authDriver: SiteAuth) -> None:
        self._authDriver = authDriver

    def getAuth(self) -> SiteAuth:
        return self._authDriver

    def setRun(self, runDriver: SiteRun) -> None:
        self._runDriver = runDriver

    def getRun(self) -> SiteRun:
        return self._runDriver

    def setRepo(self, repoDriver: SiteRepo) -> None:
        self._repoDriver = repoDriver

    def getRepo(self) -> SiteRepo:
        return self._repoDriver
    
    def setSpin(self, spinDriver: SiteSpin) -> None:
        self._spinDriver = spinDriver

    def getSpin(self) -> SiteSpin:
        return self._spinDriver
    
    
