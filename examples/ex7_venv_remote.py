"""
Test interactions with a remote IBM Quantum Cloud machine. In this test, 
our local python environment doesn't have the IBM libraries necessary to run 
the test - we will launch the test inside a venv.
"""

from lwfm.midware.LwfManager import lwfManager, logger

if __name__ == "__main__":
    # get inside the venv of this site, get the handle to the remote site, and "login"
    site = lwfManager.getSite("ibm-quantum-venv")

    logger.info(f"site={site.getSiteName()} " + \
        f"toml={lwfManager.getSiteProperties(site.getSiteName())}")

    #isLoggedIn = site.getAuthDriver().login()
    #print(f"We're logged in {isLoggedIn}")

    qcTypes = site.getSpinDriver().listComputeTypes()
    print(f"QC types: {qcTypes}")
