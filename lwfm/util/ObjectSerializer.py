"""
Regularize handling of object (de)serialization.
"""

#pylint: disable = invalid-name, missing-class-docstring, missing-function-docstring

import pickle
import json

class ObjectSerializer:

    @staticmethod
    def serialize(obj):
        out_bytes = pickle.dumps(obj, 0)
        out_str = out_bytes.decode(encoding="ascii")
        return out_str

    @staticmethod
    def deserialize(s: str):
        in_json = json.dumps(s)
        return pickle.loads(json.loads(in_json).encode(encoding="ascii"))
