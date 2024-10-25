import os
from typing import List


# ******************************************************************************


class BasicMetaRepoStore:

    storeFile = os.path.expanduser("~") + "/.lwfm/metarepo_store.txt"

    def __init__(self):
        super(BasicMetaRepoStore, self).__init__()
