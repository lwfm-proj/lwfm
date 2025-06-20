"""
Regularize handling of object (de)serialization.
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring

import base64
import dill

class ObjectSerializer:

    @staticmethod
    def serialize(obj):
        out_bytes = dill.dumps(obj, protocol=dill.HIGHEST_PROTOCOL)
        out_str = base64.b64encode(out_bytes).decode('ascii')
        return out_str

    @staticmethod
    def deserialize(s: str):
        if s is None:
            return None
        out_bytes = base64.b64decode(s.encode('ascii'))
        return dill.loads(out_bytes)
