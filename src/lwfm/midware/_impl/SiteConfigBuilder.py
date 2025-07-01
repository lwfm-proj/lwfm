#pylint: disable=invalid-name, broad-exception-raised
"""
Site builder
"""

import importlib
from typing import Optional, cast

from lwfm.base.Site import Site, SiteAuth, SiteRun, SiteRepo, SiteSpin, SitePillar
from lwfm.midware._impl.SiteConfigVenv import SiteConfigVenv
from lwfm.midware._impl.SiteConfig import SiteConfig

class SiteConfigBuilder:
    """
    site builder
    """

    @staticmethod
    def getPillarInstance(pillar_class: Optional[str] = None) -> Optional[SitePillar]:
        """
        instantiate the pillar class
        """
        if pillar_class is None:
            return None
        module = importlib.import_module(pillar_class.rsplit(".", 1)[0])
        class_ = getattr(module, str(pillar_class.rsplit(".", 1)[1]))
        pillar_driver = class_()
        return pillar_driver


    @staticmethod
    def getVenvPillarInstance(siteName: str, pillar_class: Optional[str]) -> Optional[str]:
        """
        instantiate the pillar class from inside a venv 
        """
        if pillar_class is None:
            return None
        venvHelper = SiteConfigVenv()
        builderStr = \
            "import importlib; " + \
            f"class_name = '{pillar_class}'; " + \
            "module = importlib.import_module(class_name.rsplit(\".\", 1)[0]); " + \
            "class_= getattr(module, str(class_name.rsplit(\".\", 1)[1])); " + \
            "pillarInst = class_(); " + \
            f"pillarInst.setSiteName('{siteName}'); " + \
            "from lwfm.midware._impl.ObjectSerializer import ObjectSerializer; " + \
            f"{venvHelper.makeSerializeReturnString('pillarInst')}"
        return venvHelper.executeInProjectVenv(siteName, builderStr)


    @staticmethod
    def getSite(site: str = "local") -> 'Site':
        """
        Get a Site instance. Look it up in the site TOML, instantiate it, potentially 
        overriding its default Site Pillars with provided drivers.
        """
        try:
            siteObj = SiteConfig.getSiteProperties(site)
            if siteObj is None:
                raise Exception(f"Cannot find site {site}")
            siteInst: Site
            # if this is a venv site, we need to be inside the venv to
            # construct the site instance
            venv = siteObj.get("venv")
            if venv is not None:
                # we need to pop into the venv to make the pillar classes
                # these will be kept serialized in the caller who is outside
                # the venv
                # a regular Site object will be returned, and it will be marked
                # as being for venv wrapper use
                auth_driver = \
                    SiteConfigBuilder.getVenvPillarInstance(site,
                    siteObj.get("auth"))
                run_driver = \
                    SiteConfigBuilder.getVenvPillarInstance(site,
                    siteObj.get("run"))
                repo_driver = \
                    SiteConfigBuilder.getVenvPillarInstance(site,
                    siteObj.get("repo"))
                spin_driver = \
                    SiteConfigBuilder.getVenvPillarInstance(site,
                    siteObj.get("spin"))
                siteInst = Site(site, auth_driver, run_driver, repo_driver, spin_driver)
                siteInst.setVenv(True)
            else:
                auth_driver = SiteConfigBuilder.getPillarInstance(siteObj.get("auth"))
                run_driver  = SiteConfigBuilder.getPillarInstance(siteObj.get("run"))
                repo_driver = SiteConfigBuilder.getPillarInstance(siteObj.get("repo"))
                spin_driver = SiteConfigBuilder.getPillarInstance(siteObj.get("spin"))
                if auth_driver is not None:
                    auth_driver.setSiteName(site)
                if run_driver is not None:
                    run_driver.setSiteName(site)
                if repo_driver is not None:
                    repo_driver.setSiteName(site)
                if spin_driver is not None:
                    spin_driver.setSiteName(site)
                siteInst = Site(site,
                                cast(SiteAuth, auth_driver),
                                cast(SiteRun, run_driver),
                                cast(SiteRepo, repo_driver),
                                cast(SiteSpin, spin_driver))
                siteInst.setVenv(False)
            siteInst.setRemote(siteObj.get("remote", False))
            return siteInst
        except Exception as ex:
            print(ex)
            raise ex
