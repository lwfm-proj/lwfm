
# Provides a quick and dirty way for a Site's Auth subsystem to squirrel away 
# validated user information.  Its likely this information has some time-to-live, 
# and thus we can keep it around for a bit rather than force unnecessary logins.  
# The Auth API includes an "am I still authenticated" endpoint which the Site Auth 
# is free to implement any way desired. This utility is provided here as a 
# convenience only.

import os

class AuthStore():

    def storeAuthProperties(self, site: str="", props: dict={}) -> bool:
        path = os.path.expanduser('~') + "/.lwfm/" + site + "/auth.txt"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, 'w') as f:
            for key, value in props.items():
                f.write('%s = %s\n' % (key, value))
        return True


    def loadAuthProperties(self, site: str="") -> dict:
        path = os.path.expanduser('~') + "/.lwfm/" + site + "/auth.txt"
        # Check whether the specified path exists or not
        isExist = os.path.exists(path)
        if (not isExist):
            return None
        myVars = {}
        with open(path) as f:
            name = None
            for line in f:
                # We need to be able to have multiline vars for private keys
                line = line.strip()
                # Also allow an equal at the end, eg if we're using base64
                if line.count("=") == 1 and not line[-1] == "=": 
                    name, var = line.split("=")
                    # We have to strip again because there might have been 
                    # whitespace around the =
                    name = name.strip() 
                    myVars[name] = var.strip()
                else:
                    myVars[name] += "\n" + line
        return myVars

