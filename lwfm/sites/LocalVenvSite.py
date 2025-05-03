"""
Local Site to run with its own set of dependencies independent of the global environment.
"""

#pylint: disable = invalid-name

from lwfm.sites.VenvSite import VenvSite, VenvSiteAuth, VenvSiteRun, VenvSiteRepo, VenvSiteSpin
from lwfm.sites.LocalSite import LocalSite


class LocalVenvSite(VenvSite):
    """
    A Site driver for running jobs in a virtual environment.
    """
    SITE_NAME = "local-venv"

    def __init__(self):
        # Initialize the base class with the site name and the pillars

        # going to use a private local site for some pillars
        self.localSite = LocalSite()
        self.localSite.setSiteName(self.SITE_NAME)

        auth = VenvSiteAuth()
        auth.setAuthDriver(self.localSite.getAuthDriver())
        run = VenvSiteRun()
        run.setRunDriver(self.localSite.getRunDriver())
        repo = VenvSiteRepo()
        repo.setRepoDriver(self.localSite.getRepoDriver())
        spin = VenvSiteSpin()
        spin.setSpinDriver(self.localSite.getSpinDriver())
        super().__init__(
            self.SITE_NAME, auth, run, repo, spin
        )
