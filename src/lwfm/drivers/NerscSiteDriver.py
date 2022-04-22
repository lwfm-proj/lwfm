
import logging

from lwfm.base.Site import Site, SiteLoginDriver, SiteRunDriver

class NerscSiteLoginDriver(SiteLoginDriver):
    def login(self) -> bool:
        return True

class NerscSiteRunDriver(SiteRunDriver):




# test
if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    site = Site("perlmutter", NerscSiteLoginDriver(), None)
    logging.info(site.getName())
