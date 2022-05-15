
from abc import ABC, abstractmethod
import logging
import uuid


# UUID generator used to give jobs lwfm ids which obviates collisions between job sites.  Other objects in the system
# may also benefit from this generator.
class _IdGenerator:
    @staticmethod
    def generateId():
        return str(uuid.uuid4())


# Many base classes extend LwfmBase to permit the passing of arbitrary name=value maps in addition to the fixed parameters
# specified by various classes in the object model.
class LwfmBase(ABC):

    args: dict = None    # most class attributes backed by getters and setters are handled as values in this dict

    def __init__(self, args: dict = None):
        self.setArgs(args)

    def _setArg(self, name: str, value: type) -> None:
        self.args[name] = value

    def _getArg(self, name: str) -> type:
        return self.args.get(name, None)

    def getArgs(self) -> dict:
        return self.args

    def setArgs(self, args: dict=None):
        if args is None:
            args = dict()
        self.args = dict(args)


#************************************************************************************************************************************

# test
if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    base = LwfmBase()
    base._setArg("foo", "bar")
    logging.info(base._getArg("foo"))
