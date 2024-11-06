

from lwfm.base.LwfmBase import LwfmBase

# good enough for now - LwfmBase includes an id for the sheet and a place to 
# stick an arbitrary dict, some of which will come from the user's call, and
# some can be stuck there by the lwfm framework
class Metasheet(LwfmBase):
    def __init__(self, args: dict = None):
        super(Metasheet, self).__init__(args)

    def __str__(self):
        return f"{self.getArgs()}"

