
from abc import ABC
import uuid
import pickle
import json


# Many base classes extend LwfmBase to permit the passing of arbitrary name=value
# maps in addition to the fixed parameters specified by various classes in the 
# object model.  This aids in generalization and serialization.  
# I don't doubt there's a better way... 

class LwfmBase(ABC):

    _shortJobIds = True # TODO set to false to use long ids in a production environment

    id: str = None

    args: dict = None   # most class attributes backed by getters and setters are 
                        # handled as values in this dict

    def __init__(self, args: dict):
        if (args is None):
            args = {}
        self.setArgs(args)
        self._setId(_IdGenerator.generateId())

    def _setArg(self, name: str, value: type) -> None:
        self.args[name] = value

    def _getArg(self, name: str) -> type:
        return self.args.get(name, None)

    def _setId(self, idValue: str) -> None:
        self._setArg("id", idValue)

    def setId(self, idValue: str) -> None:
        self._setArg("id", idValue)

    def getId(self) -> str:
        return self._getArg("id")

    def getArgs(self) -> dict:
        return self.args

    def setArgs(self, args: dict):
        if args is None:
            args = dict()
        self.args = dict(args)

    def serialize(self):
        out_bytes = pickle.dumps(self, 0)
        out_str = out_bytes.decode(encoding="ascii")
        return out_str

    @staticmethod
    def deserialize(s: str):
        in_json = json.dumps(s)
        in_obj = pickle.loads(json.loads(in_json).encode(encoding="ascii"))
        return in_obj

# UUID generator used to give jobs lwfm ids which obviates collisions between 
# job sites.  Other objects in the system may also use this generator.
class _IdGenerator:
    @staticmethod
    def generateId():
        if (LwfmBase._shortJobIds):
            return str(uuid.uuid4())[:8]
        else:
            return str(uuid.uuid4())

