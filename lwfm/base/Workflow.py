"""
A collection of jobs and their associated (meta)information.
"""

#pylint: disable = invalid-name, missing-function-docstring

from ..util.IdGenerator import IdGenerator

class Workflow():
    """
    A collection of jobs and their associated (meta)information.
    """

    def __init__(self, name: str = None, description: str = None):
        self._id = IdGenerator.generateId()
        self._name = name
        self._description = description

    def setId(self, idValue: str) -> None:
        self._id = idValue

    def getId(self) -> str:
        return self._id

    def setName(self, name: str) -> None:
        self._name = name

    def getName(self) -> str:
        return self._name

    def setDescription(self, description: str) -> None:
        self._description = description

    def getDescription(self) -> str:
        return self._description
