"""
Site configuration management for lwfm.
Handles loading site configuration from TOML files and creating site instances.
"""

#pylint: disable = broad-exception-caught, broad-exception-raised, invalid-name

import os
import tomllib

from typing import Dict, Any

class SiteConfig:
    """
    Manages site configuration from TOML files and provides methods to access 
    site properties and instantiate site objects.
    """

    @staticmethod
    def _getSiteToml() -> dict:
        """
        Load the default and user site configurations from TOML.
        These hardcoded default values (below) can be overriden and extended by 
        using a ~/.lwfm/sites.toml.
        """
        siteToml = """
        [lwfm]
        host = "127.0.0.1"
        port = "3000"

        [local]
        auth = "lwfm.sites.LocalSite.LocalSiteAuth"
        run  = "lwfm.sites.LocalSite.LocalSiteRun"     
        repo = "lwfm.sites.LocalSite.LocalSiteRepo"
        spin = "lwfm.sites.LocalSite.LocalSiteSpin" 
        remote = false

        [local-venv]
        venv = "/tmp"
        auth = "lwfm.sites.LocalSite.LocalSiteAuth"
        run  = "lwfm.sites.LocalSite.LocalSiteRun"     
        repo = "lwfm.sites.LocalSite.LocalSiteRepo"
        spin = "lwfm.sites.LocalSite.LocalSiteSpin" 
        remote = false
        """

        USER_TOML = os.path.expanduser("~") + "/.lwfm/sites.toml"

        siteSet = tomllib.loads(siteToml)
        # is there a local site config? it can define any custom site, or override
        # a site driver which ships with lwfm
        # Check whether the specified path exists or not
        if os.path.exists(USER_TOML):
            with open(USER_TOML, "rb") as f:
                siteSetUser = tomllib.load(f)
            siteSet.update(siteSetUser)
        return siteSet

    @staticmethod
    def getAllSiteProperties() -> dict:
        """
        Potentially useful for debugging. Returns the contents of the combined TOML.
        """
        return SiteConfig._getSiteToml()

    @staticmethod
    def getSiteProperties(site: str) -> Dict[str, Any]:
        """
        Get the properties for a named site.
        """
        siteSet = SiteConfig._getSiteToml()
        return siteSet.get(site) or {}


    @staticmethod
    def getLogFilename() -> str:
        """ Get path to the log files. """
        return "~/.lwfm/logs"
