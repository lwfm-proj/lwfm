"""
A basic dictionary to hold metadata about data objects under management by lwfm
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring

from ..util.IdGenerator import IdGenerator

# good enough for now - LwfmBase includes an id for the sheet and a place to 
# stick an arbitrary dict, some of which will come from the user's call, and
# some can be stuck there by the lwfm framework
class Metasheet():
    """
    A collection of name=value pairs for a blob of data.
    """

    _id = None
    _props = {}


    def __init__(self, props: dict = None):
        self._id = IdGenerator.generateId()
        self._props = props

    def __str__(self):
        return f"{self._props}"

    def getId(self) -> str:
        return self._id

    def getProps(self) -> dict:
        return self._props

    def setProps(self, props: dict) -> None:
        self._props = props
