
# Site: Defines an abstract computing location which exposes canonical verbs for the Auth, Run, and Repo (and optionally Spin)
# logical subsystems. The purpose of lwfm is to permit workflows which span Sites.


from enum import Enum
import logging
from abc import ABC, abstractmethod
from typing import Callable
from pathlib import Path
import os


from lwfm.base.LwfmBase  import LwfmBase
from lwfm.base.JobStatus import JobStatus, JobStatusValues, JobContext
from lwfm.base.JobDefn import JobDefn
from lwfm.base.JobEventHandler import JobEventHandler
from lwfm.base.SiteFileRef import SiteFileRef


#***********************************************************************************************************************************

class SiteAuthDriver(ABC):
    """
    Auth: an interface for a Site's user authentication and authorization functiions.  Permits a Site to provide some kind of a
    "login" be it programmatic or forced interactive.  A given Site's implmentation of Auth might squirrel away some returned
    token (should the Site use such things).  We can then provide a quicker "is the auth current" method.  We assume the
    implementation of a Site's Run and Repo subsystems will be provided access under the hood to the necessary implementation
    details from the Auth system.
    """

    @abstractmethod
    def login(self, force: bool=False) -> bool:
        """
        Login to the Site using the Site's own mechanism for authenication and caching.

        Params:
            force - if true, forces a login even if the Site detects that the current login is still viable
        Returns:
            bool - true if success, else false
        """
        pass


    @abstractmethod
    def isAuthCurrent(self) -> bool:
        """
        Is the currently cached Site login info still viable?

        Returns:
            bool - true if the login is still viable, else false; the Site might not cache, and thus always return false
        """
        pass



#************************************************************************************************************************************

class SiteRunDriver(ABC):
    """
    Run: in its most basic "MVP-0" form, the Run subsystem provides a mechanism to submit a job, cancel the job, and interrogate
    the job's status.  The submitting of a job might, for some Sites, involve a batch scheduler.  Or, for some Sites (like a "local"
    Site), the run might be immediate.  On submit, the method returns a JobStatus, which for immediate execution might be a terminal
    completion status.  A job definition (JobDefn) is a description of the job.  Its the role of the Site's Run subsystem to
    interpret the abstract JobDefn in the context of that Site.  Thus the JobDefn permits arbitrary name=value pairs which might be
    necessary to submit a job at that Site.  The JobStatus returned is canonical - the Site's own status name set is mapped into the
    lwfm canonical name set by the implementation of the Site itself.
    """

    @classmethod
    def _submitJob(cls, jdefn, jobContext = None):
        # This helper function, not a member of the public interface, lets Python threading instantiate a
        # SiteRunDriver of the correct subtype on demand
        runDriver = cls()
        runDriver.submitJob(jdefn, jobContext)


    @abstractmethod
    def submitJob(self, jobDefn: JobDefn, parentContext: JobContext = None) -> JobStatus:
        """
        Submit the job for execution on this Site.  It is an implementation detail of the Site what that means - everything
        from pseudo-immediate command line execution, to scheduling on an HPC system.  The caller should assume the run is
        asynchronous.  We would assume all Sites would implement this method.  Note that "compute type" is an optional member
        of JobDefn, and might be used by the Site to direct the execution of the job.

        [We note that both the JobDefn and the JobContext potentially contain a reference to a compute type.  Since the Job Context
        is historical, and provided to give that historical context to the job we're about to run, its strongly suggested that
        Site.Run implementations use the compute type named in the JobDef, if the concept is present at all on the Site.  We note also
        that compute type and Site are relatively interchangeable - one can model a compute resource as a compute type, or as its
        own Site, perhaps with a complete inherited Site driver imp]ementation.  e.g. NerscSiteDriver has two trivially subclassed
        Sites - one for Cori (rest in peace) and one for Perlmutter.  This could have been impleted as one Site with two compute
        types.  The Site driver author is invited to use whichever model fits them best.]

        Params:
            jobDefn - the definition of the job to run, might include the name of a script, include arguments for the run, etc.
            parentContext - information about the current JobContext which might be running, thus the job we are submitting will be
                a child in the digital thread; this is an optional argument - if none, the submitted job is considered semminal
        Returns:
            JobStatus - preliminary status, containing a JobContext including the Site-specific native job id
        """
        pass


    @abstractmethod
    def getJobStatus(self, jobContext: JobContext) -> JobStatus:
        """
        Check the status of a job running on this Site.  The Site might raise a NotImplementedError if it does not implement
        such a check, though this would be unusual.

        Params:
            jobContext - the context of the executing job, including the native job id
        Returns:
            JobStatus - the current known status of the job
        """
        pass


    @abstractmethod
    def cancelJob(self, jobContext: JobContext) -> bool:
        """
        Cancel the job, which might be in a queued, state, or might already be running.  Its up to to the Site how to
        handle the cancel, including raising a NotImplementedError.

        Params:
            jobContext - the context of the job, including the Site-native job id
        Returns:
            bool - true if a successful cancel, else false, or throw exception; callers should invoke the getJobStatus() method
                to obtain final status (e.g., the job might have completed successfully prior to the cancel being receieved, etc.)
        """
        pass


    @abstractmethod
    def listComputeTypes(self) -> [str]:
        """
        List the compute types supported by the Site.  The Site might not have any concept, and thus might raise a
        NotImplementedError, or return an empty list, or a list of one singular type of the Site.  Or, the Site might front
        a number of resources, and return a list of names.

        Returns:
            [str] - a list of names, potentially empty or None, or a raise of NotImplementedError
        """
        pass


    @abstractmethod
    def setEventHandler(self, jobContext: JobContext, jobStatus: JobStatusValues, statusFilter: Callable,
                        newJobDefn: JobDefn, newJobContext: JobContext, newSiteName: str) -> JobEventHandler:
        """
        Set a job to be submitted when a prior job event occurs.
        A Site does not need to have a concept of these event handlers (most won't) and is free to throw a NotImplementedError.
        The local lwfm site will provide an implementation through its own Site.Run interface, and thus permit
        cross-Site workflow job chaining.
        Asking a Site to run a job on a Site other than itself (siteName = None) is free to raise a NotImplementedError, though
        it might be possible in some cases, and the lwfm Local site will permit it.

        Params:
            jobContext - information about the job we're waiting on including the native Site job id
            jobStatus - the status string, from the enum set of canonical strings, on which we're waiting
            statusFilter - a function which returns boolean success / failure after parsing the content of the status message in detail
            newJobDefn - the job to be submitted to this Site if the handler fires
            newJobContext - the job context to run the job under, if not provided, reverts to the triggering job being the parent
            newSiteName - run the job on the named site - this can be None to run on self
        Returns:
            JobEventHandler
        """
        pass


    @abstractmethod
    def unsetEventHandler(self, jeh: JobEventHandler) -> bool:
        """
        Unset an event handler.

        Params:
            JobEventHandler - a previously set handler
        Returns:
            bool - success, fail, or raise NotImplementedError if the Site has no concept of event handlers
        """
        pass


    @abstractmethod
    def listEventHandlers(self) -> [JobEventHandler]:
        """
        List the JobEventHandler registrations the Site is holding, or an empty list, or raise NotImplementedError if the
        Site doesn't support event handlers.
        """
        pass


#***********************************************************************************************************************************

class SiteRepoDriver(ABC):
    """
    Repo: Pemmits the movement of data objects to/from the Site.  The methods require a local file reference, and a reference to the
    remote file - a SiteFileRef.  The SiteFileRef permits arbitrary name=value pairs because the Site might require them to
    complete the transaction.
    """

    @abstractmethod
    def put(self, localPath: Path, siteFileRef: SiteFileRef, jobContext: JobContext = None) -> SiteFileRef:
        """
        Take the local file by path and put it to the remote Site.  This might be implemented by the Site as a simple
        filesystem copy, or it might be a checkin to a managed service - that's up to the Site.  If we're given a context,
        we use it, if not, we consider ourselves our own job.

        Params:
            localPath - a local file object
            siteFileRef - a reference to an abstract "file" entity on the Site - this is the target of the put operation
            jobContext - if we have a job context we wish to use (e.g. we are already inside a job and wish to indicate the
                digital thread parent-child relationships) then pass the context in, else the put operation will be performed
                as its own seminal job
        Returns:
            SiteFileRef - a refernce to the entity put on the Site; the Site might also raise any kind of exception depending on
                the error case
        """
        pass


    @abstractmethod
    def get(self, siteFileRef: SiteFileRef, localPath: Path, jobContext: JobContext = None) -> Path:
        """
        Get the file from the remote site and write it local, returning a path to the local.
        If we're given a context, we use it, if not, we consider ourselves our own job.

        Params:
            siteFileRef - a reference to a data entity on the Site, the source of the get
            localPath - a local file object, the destination of the get
            jobContext - if we have a job context we wish to use (e.g. we are already inside a job and wish to indicate the
                digital thread parent-child relationships) then pass the context in, else the put operation will be performed
                as its own seminal job
        Returns:
            Path - the reference to the local location of the gotten file; during the get, the Site might also raise any kind of
                exception depending on the error case
        """
        pass


    @abstractmethod
    def find(self, siteFileRef: SiteFileRef) -> [SiteFileRef]:
        """
        Get info about the file/dir on the remote site

        Params:
            siteFileRef - a reference to an abstract "file" entity on the Site, may be specialized partially (e.g. wildcards) though
                it is up to the Site to determine how to implement this search
        Returns:
            [SiteFileRef] - the instantiated file reference(s) (not the file, but the references), including the size, timestamp
                info, and other arbitrary metadata; may be a single file reference, or a list, or none
        """
        pass


#************************************************************************************************************************************
# Spin: vaporware.  In theory some Sites would expose mechanisms to create (provision) and destroy various kinds of computing
# devices.  These might be single nodes, or entire turnkey cloud-bases HPC systems.  Spin operations are modeled as jobs in
# order to permit sequential workflows which spin up resources, send them jobs, and then spin them down as part of an
# autonomous operation.  Basic verbs include: show cafeteria, spin up, spin down.  Spins would be wrapped as Jobs allowing normal
# status interogation.


#***********************************************************************************************************************************
# Site: the Site is simply a name and the getters and setters for its Auth, Run, Repo subsystems.
#
# The Site factory utility method returns the Python class which implements the interfaces for the named Site.
# ~/.lwfm/sites.txt can be used to augment the list of sites provided here with a user's own custom Site implementations.
# In the event of a name collision between the user's sites.txt and those hardcoded here, the user's sites.txt config trumps.


# LwfmBase field list
class _SiteFields(Enum):
    SITE_NAME = "siteName"


class Site(LwfmBase):

    _authDriver: SiteAuthDriver = None
    _runDriver:  SiteRunDriver  = None
    _repoDriver: SiteRepoDriver = None

    # pre-defined Sites and their associated driver implementations, each which implements Auth, Run, Repo, [Spin]
    # these mappings can be extended in the ~/.lwfm/sites.txt configuration
    _SITES = {
        "local":      "lwfm.drivers.LocalSiteDriver.LocalSite",
        "nersc":      "lwfm.drivers.NerscSiteDriver.NerscSite",
        "cori":       "lwfm.drivers.NerscSiteDriver.CoriSite",
        "perlmutter": "lwfm.drivers.NerscSiteDriver.PerlmutterSite",
        "dt4d":       "lwfm.drivers.DT4DSiteDriver.DT4DSite",
    }

    @staticmethod
    def _getSiteEntry(site: str):
        siteSet = Site._SITES
        # is there a local site config?
        path = os.path.expanduser('~') + "/.lwfm/sites.txt"
        # Check whether the specified path exists or not
        if os.path.exists(path):
            logging.info("Loading custom site configs from ~/.lwfm/sites.txt")
            with open(path) as f:
                for line in f:
                    name, var = line.split("=")
                    name = name.strip()
                    var = var.strip()
                    logging.info("Registering driver " + var + " for site " + name)
                    siteSet[name] = var
        else:
            logging.info("No custom ~/.lwfm/sites.txt - using built-in site configs")
        fullPath = siteSet[site]
        logging.info("Obtaining driver " + fullPath + " for site " + site)
        if fullPath is not None:
            # parse the path into package and class parts for convenience
            xpackage = fullPath.rsplit('.', 1)[0]
            xclass = fullPath.rsplit('.', 1)[1]
            return [ xpackage, xclass ]
        else:
            return None

    @staticmethod
    def getSiteInstanceFactory(site: str = "local"):
        try:
            entry = Site._getSiteEntry(site)
            logging.info("Processing site config entry " + str(entry))
            import importlib
            module = importlib.import_module(entry[0])
            class_ = getattr(module, str(entry[1]))
            inst = class_()
            inst.setName(site)
            return inst
        except Exception as ex:
            logging.error("Cannot instantiate Site for " + site + " {}".format(ex))


    def __init__(self, name: str, authDriver: SiteAuthDriver, runDriver: SiteRunDriver, repoDriver: SiteRepoDriver, args: dict=None):
        super(Site, self).__init__(args)
        self.setName(name)
        self.setAuthDriver(authDriver)
        self.setRunDriver(runDriver)
        self.setRepoDriver(repoDriver)

    def setName(self, name: str) -> None:
        LwfmBase._setArg(self, _SiteFields.SITE_NAME.value, name)

    def getName(self) -> str:
        return LwfmBase._getArg(self, _SiteFields.SITE_NAME.value)

    def setAuthDriver(self, authDriver: SiteAuthDriver) -> None:
        self._authDriver = authDriver

    def getAuthDriver(self) -> SiteAuthDriver:
        return self._authDriver

    def setRunDriver(self, runDriver: SiteRunDriver) -> None:
        self._runDriver = runDriver

    def getRunDriver(self) -> SiteRunDriver:
        return self._runDriver

    def setRepoDriver(self, repoDriver: SiteRepoDriver) -> None:
        self._repoDriver = repoDriver

    def getRepoDriver(self) -> SiteRepoDriver:
        return self._repoDriver


#***********************************************************************************************************************************

# test
if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    siteFoo = Site.getSiteInstanceFactory("local")
    logging.info(siteFoo.getName())
