"""
A Job Definition is the abstract representation of a job, the non-instantiated 
description. The JobDefn will be passed to the Site's Run driver which will use the 
args to instantiate a job from the definition.
"""

#pylint: disable = missing-function-docstring, invalid-name

from typing import List

from lwfm.midware._impl.IdGenerator import IdGenerator


class JobDefn:
    """
    The static definition of a job, to be instantiated at runtime by the Site.Run 
    subsystem. The JobDefn is not presumed to be portable, though it is possible 
    and the onus is on the user or the author of the Site driver.  
    Within the JobDefn will be baked arbitrary arguments, which might very well be 
    Site-specific (e.g., parameters to a specific Site HPC scheduler).  It is 
    ultimately the job of the Site Run subsystem to interpret the job defn and 
    execute it.  The standard arguments which would be needed to aid in broad 
    portability are not specified by this framework, nor are they precluded.

    Attributes:

    name - an optional name for human consumption

    entry point - a declaration of the command to run, from the perspective of the Site.  
        This can be anything from an actual command string, or a complex serialized object 
        - its entirely up to the Site how to specify and interpret the entry point -
        again, the JobDefn is not presumed to be portable across Sites

    job args - distinct from the entry point, the job might desire arbitrary arguments 
        at runtime
    """

    def __init__(self, entryPoint: str = None):
        self._defn_id = IdGenerator().generateId()
        self.setEntryPoint(entryPoint)
        self.setName("")
        self.setJobArgs([])

    def getDefnId(self) -> str:
        return self._defn_id

    def setName(self, name: str) -> None:
        self._name = name

    def getName(self) -> str:
        return self._name

    def setEntryPoint(self, entryPoint: str) -> None:
        self._entryPoint = entryPoint

    def getEntryPoint(self) -> str:
        return self._entryPoint

    def setJobArgs(self, args: List[str]) -> None:
        self._jobArgs = args

    def getJobArgs(self) -> List[str]:
        return self._jobArgs


#****************************************************************************
