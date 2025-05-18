"""
Local Site to run in a virual environment with its own set of dependencies, 
independent of the global environment.
"""

#pylint: disable = invalid-name

from lwfm.base.Site import SiteAuth, SiteRun, SiteRepo, SiteSpin
from lwfm.sites.VenvSite import VenvSite
from lwfm.sites.LocalSite import LocalSite


class LocalVenvSite(VenvSite):
    """
    A Site driver for running jobs in a virtual environment.
    """
    SITE_NAME = "local-venv"

    def __init__(self, site_name: str = None,
                    authDriver: SiteAuth = None,
                    runDriver: SiteRun = None,
                    repoDriver: SiteRepo = None,
                    spinDriver: SiteSpin = None,
                 ) -> None:
        # going to use a private local site for some pillars if none are not provided;
        # (truthfully the invoker can pass in any Site pillar implementation, not just
        # one from the local site - this might be convenient, or allow for a chimera...)
        self.localSite = LocalSite()
        # TODO think thru this... how does an arbitrary name work? must be in toml
        if site_name is not None:
            self.localSite.setSiteName(site_name)
        else:
            self.localSite.setSiteName(self.SITE_NAME)

        self._realAuthDriver = authDriver or self.localSite.getAuthDriver()
        self._realRunDriver = runDriver or self.localSite.getRunDriver()
        self._realRepoDriver = repoDriver or self.localSite.getRepoDriver()
        self._realSpinDriver = spinDriver or self.localSite.getSpinDriver()
        super().__init__(
            site_name or self.SITE_NAME,
            self._realAuthDriver,
            self._realRunDriver,
            self._realRepoDriver,
            self._realSpinDriver
            )
