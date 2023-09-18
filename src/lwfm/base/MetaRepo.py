import requests


# This communicates with a MetaRepo2 API. The user will have to set the domain
# first, at which point notate and find will remain available. If the user has NOT
# set the domain, no errors are issued, because this is a valid state. You may not
# want to be using an external Metarepo in a particular workflow

_domain = None

class MetaRepo:
    # We put everything in classes because ???

    @staticmethod
    def setDomain(domain):
        # Before we can set a file, we need to pick a domain
        global _domain
        _domain = domain
        
    @staticmethod
    def getDomain():
        global _domain
        return _domain

    @staticmethod
    def notate(fileRef, siteClass=None, siteMetadata=None, targetClass=None, targetMetadata=None, token=None):
        # Given a fileRef, add it to the MetaRepo
        # The user must supply a site and target information, so Metarepo knows how to
        # process the metasheet
        global _domain
        if _domain is None:
            return
        
        metasheet = {}
        metasheet["docSetId"] = [] # No concept of set IDs in lwfm for the moment, but should be added
        metasheet["displayName"] = fileRef.getName()
        metasheet["userMetadata"] = fileRef.getMetadata()

        metasheet["siteClass"] = siteClass
        metasheet["siteMetadata"] = siteMetadata
        
        metasheet["targetClass"] = targetClass
        metasheet["targetMetadata"] = targetMetadata
        
        r = requests.post(f"http://{_domain}/notate", json=metasheet,
                         headers={"Authorization": f"Bearer {token}"})
        if not r.status_code == 200:
            print(r.text)
        
    @staticmethod
    def find(fileRef):
        global _domain
        if _domain is None:
            return

        fileList = []

        metaRepo = MetaRepo._getMetaRepo()
        for file in metaRepo:
            if fileRef.getId() is not None       and file.getId() != fileRef.getId():
                continue
            if fileRef.getName() is not None     and file.getName() != fileRef.getName():
                continue
            if fileRef.getMetadata() is not None and file.getMetadata() != fileRef.getMetadata():
                continue
            fileList.append(file)
        return fileList
    
