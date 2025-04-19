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
        self._workflow_id = IdGenerator.generateId()
        self._name = name
        self._description = description
        self._props = {}

    def _setWorkflowId(self, idValue: str) -> None:
        self._workflow_id = idValue

    def getWorkflowId(self) -> str:
        return self._workflow_id

    def setName(self, name: str) -> None:
        self._name = name

    def getName(self) -> str:
        return self._name

    def setDescription(self, description: str) -> None:
        self._description = description

    def getDescription(self) -> str:
        return self._description

    def getProps(self) -> dict:
        return self._props

    def setProps(self, props: dict) -> None:
        self._props = props
