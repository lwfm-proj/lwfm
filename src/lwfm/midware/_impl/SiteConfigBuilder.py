#pylint: disable=invalid-name, broad-exception-raised
"""
Site builder
"""

import importlib


from lwfm.base.Site import Site, SiteAuth, SiteRun, SiteRepo, SiteSpin
from lwfm.midware._impl.ObjectSerializer import ObjectSerializer
from lwfm.midware._impl.SiteConfigVenv import SiteConfigVenv
from lwfm.midware._impl.SiteConfig import SiteConfig

class SiteConfigBuilder:
    """
    site builder
    """

    @staticmethod
    def getSite(site: str = "local",
                auth_driver: SiteAuth = None,
                run_driver:  SiteRun = None,
                repo_driver: SiteRepo = None,
                spin_driver: SiteSpin = None) -> 'Site':
        """
        Get a Site instance. Look it up in the site TOML, instantiate it, potentially 
        overriding its default Site Pillars with provided drivers.
        """
        try:
            siteObj = SiteConfig.getSiteProperties(site)
            if siteObj is None:
                raise Exception(f"Cannot find site {site}")
            # if this is a venv site, we need to be inside the venv to
            # construct the site instance
            venv = siteObj.get("venv")
            venvSite = siteObj.get("venvSite")
            siteInst: Site = None
            if venv is not None and venvSite is not None:
                realSiteObj = SiteConfig.getSiteProperties(venvSite)
                venvHelper = SiteConfigVenv()
                builderStr = "import importlib; " + \
                    f"class_name = \"{realSiteObj.get("class")}\"; " + \
                    "module = importlib.import_module(class_name.rsplit(\".\", 1)[0]); " + \
                    "class_= getattr(module, str(class_name.rsplit(\".\", 1)[1])); " + \
                    f"siteInst = class_('{site}'); " + \
                    "from lwfm.midware.LwfManager import lwfManager; " + \
                    f"{venvHelper.makeSerializeReturnString('siteInst')}"
                siteInstStr = venvHelper.executeInProjectVenv(site, builderStr)
                siteInst = ObjectSerializer.deserialize(siteInstStr)
            else:
                class_name = siteObj.get("class")
                module = importlib.import_module(class_name.rsplit(".", 1)[0])
                class_ = getattr(module, str(class_name.rsplit(".", 1)[1]))
                siteInst = class_(site,
                    auth_driver, run_driver, repo_driver, spin_driver)

            siteInst.setRemote(siteObj.get("remote", False))
            auth = siteInst.getAuthDriver()
            if auth:
                auth.setSite(siteInst)
            run = siteInst.getRunDriver()
            if run:
                run.setSite(siteInst)
            repo = siteInst.getRepoDriver()
            if repo:
                repo.setSite(siteInst)
            spin = siteInst.getSpinDriver()
            if spin:
                spin.setSite(siteInst)
            return siteInst
        except Exception as ex:
            print(ex)
            raise ex
