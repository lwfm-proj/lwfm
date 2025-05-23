"""
Site configuration management for lwfm.
Handles loading site configuration from TOML files and creating site instances.
"""

#pylint: disable = broad-exception-caught

import importlib
import os
import tomllib

from typing import Dict, Any, TYPE_CHECKING

#if TYPE_CHECKING:
#    from lwfm.base.Site import Site


class SiteConfig:
    """
    Manages site configuration from TOML files and provides methods to access 
    site properties and instantiate site objects.
    """
    
    @staticmethod
    def _getSiteToml() -> dict:
        """Load the default and user site configurations from TOML."""
        siteToml = """
        [lwfm]
        host = "127.0.0.1"
        port = "3000"

        [local]
        class = "lwfm.sites.LocalSite.LocalSite"

        [local-venv]
        class = "lwfm.sites.LocalVenvSite.LocalVenvSite"
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
        return siteSet.get(site)

    @staticmethod
    def getSite(site: str = "local",
                auth_driver = None,
                run_driver = None,
                repo_driver = None,
                spin_driver = None) -> 'Site':
        """
        Get a Site instance. Look it up in the site TOML, instantiate it, potentially 
        overriding its default Site Pillars with provided drivers.
        """
        try:
            siteObj = SiteConfig.getSiteProperties(site)
            if siteObj is None:
                raise Exception(f"Cannot find site {site}")
            class_name = siteObj.get("class")
            module = importlib.import_module(class_name.rsplit(".", 1)[0])
            class_ = getattr(module, str(class_name.rsplit(".", 1)[1]))
            inst = class_(site, auth_driver, run_driver, repo_driver, spin_driver)
            return inst
        except Exception as ex:
            print(f"Cannot instantiate Site for {site} {ex}")
            raise ex
