
import logging
import os


class AuthStore():
    def loadAuthProperties(self, site: str="") -> dict:
        path = os.path.expanduser('~') + "/.lwfm/" + site + "/auth.txt"
        # Check whether the specified path exists or not
        isExist = os.path.exists(path)
        if (not isExist):
            return None
        myvars = {}
        with open(path) as myfile:
            name = None
            for line in myfile:
                # We need to be able to have multiline vars for private keys
                # TODO: This is a quick hack, we need to do something better
                if line.count("=") == 1:
                    name, var = line.split("=")
                    name = name.strip()
                    myvars[name] = var.strip()
                else:
                    var = line.strip()
                    myvars[name] += "\n" + var
        return myvars
