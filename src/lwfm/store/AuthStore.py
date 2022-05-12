
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
            for line in myfile:
                name, var = line.partition("=")[::2]
                myvars[name.strip()] = str(var.strip())
        return myvars
