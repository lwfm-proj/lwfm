
from abc import ABC, abstractmethod
import logging


class LwfmBase(ABC):

    args: dict[str, type] = None    # most class attributes backed by getters and setters are handled as values in this dict

    def __init__(self, args: dict[str, type] = None):
        if args is None:
            args = dict()
        self.args = dict(args)

    def _setArg(self, name: str, value: type) -> None:
        self.args[name] = value

    def _getArg(self, name: str) -> type:
        return self.args.get(name, None)

    def getArgs(self) -> dict[str, type]:
        return args


# test
if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    base = LwfmBase()
    base._setArg("foo", "bar")
    logging.info(base._getArg("foo"))
