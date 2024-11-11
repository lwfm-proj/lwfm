
# convenience tool to stick the auth token for a site into the lwfm store 

from lwfm.midware.impl.Store import AuthStore

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Usage: python putAuth.py <siteName> <doc>")
        sys.exit(1)
    siteName = sys.argv[1]
    doc = sys.argv[2]
    authStore = AuthStore()
    authStore.putAuthForSite(siteName, doc)



