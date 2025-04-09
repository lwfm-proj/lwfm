"""
A collection of jobs and their associated (meta)information.
"""

#pylint: disable = invalid-name, missing-function-docstring

from ..util.IdGenerator import IdGenerator

class Workflow():
    """
    A collection of jobs and their associated (meta)information.
    """

    _id = None
    _name = None

    def __init__(self, name: str = None):
        self._id = IdGenerator.generateId()
        self._name = name

    def setId(self, idValue: str) -> None:
        self._id = idValue

    def getId(self) -> str:
        return self._id

    def setName(self, name: str) -> None:
        self._name = name

    def getName(self) -> str:
        return self._name
