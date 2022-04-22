
from abc import ABC, abstractmethod


class LwfmBase(ABC):
    def __init__(self, args=None):
        if args is None:
            args = dict()
        self.args = dict(args)

    def _setArg(self, name, value):
        self.args[name] = value

    def _getArg(self, name):
        return self.args.get(name)


# test
if __name__ == '__main__':
    base = LwfmBase()
    base._setArg("foo", "bar")
    print(base._getArg("foo"))
