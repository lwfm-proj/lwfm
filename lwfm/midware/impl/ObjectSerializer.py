"""
Regularize handling of object (de)serialization.
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring

import pickle
import base64

class ObjectSerializer:

    @staticmethod
    def serialize(obj):
        out_bytes = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
        out_str = base64.b64encode(out_bytes).decode('ascii')
        return out_str

    @staticmethod
    def deserialize(s: str):
        out_bytes = base64.b64decode(s.encode('ascii'))
        return pickle.loads(out_bytes)
