"""
UUID generator used to give jobs lwfm ids which obviates collisions between
job sites.  Other objects in the system may also use this generator.
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring

import uuid
import random
import sys

class IdGenerator:
    @staticmethod
    def generateId() -> str:
        return str(uuid.uuid4())[:8]

    @staticmethod
    def generateInteger() -> int:
        max_int = sys.maxsize
        return random.randint(1, max_int)
