"""
UUID generator used to give jobs lwfm ids which obviates collisions between
job sites.  Other objects in the system may also use this generator.
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring

import uuid

class IdGenerator:
    def _generateIdWorker(self, short: bool = True) -> str:
        if short:
            return str(uuid.uuid4())[:8]  # short form
        return str(uuid.uuid4())        # long form

    def generateIdShort(self) -> str:
        """
        Generate a short-form unique id.
        """
        return self._generateIdWorker(short=True)

    def generateIdLong(self) -> str:
        """
        Generate a long-form unique id.
        """
        return self._generateIdWorker(short=False)

    def generateId(self) -> str:
        """
        Generate a unique ID for a job or workflow, or any other purpose.
        This is the method called by default.
        """
        return self._generateIdWorker(short=True) # TODO get this from site.toml config
