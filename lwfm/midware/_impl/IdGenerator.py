"""
UUID generator used to give jobs lwfm ids which obviates collisions between
job sites.  Other objects in the system may also use this generator.
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring

import uuid
import random
import sys

class IdGenerator:
    def generateId(self) -> str:
        # return str(uuid.uuid4())[:8]  # short form
        return str(uuid.uuid4())        # long form

    def generateInteger(self) -> int:
        max_int = sys.maxsize
        return random.randint(1, max_int)
