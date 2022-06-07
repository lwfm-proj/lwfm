import logging
import os
from abc import ABC, abstractmethod

from lwfm.base.JobStatus import JobStatus, JobStatusValues
import json

#************************************************************************************************************************************


class RunStore(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def write(self, datum: type) -> bool:
        pass


#************************************************************************************************************************************
# We might optionally record locally in a write-once read-never fashion every observed status message.

class RunJobStatusStore(RunStore):
    def __init__(self):
        super(RunJobStatusStore, self).__init__()

    def write(self, datum: JobStatus) -> bool:
        #s = datum.serialize()
        #file_object = open(os.path.expanduser('~') + '/.lwfm/run_job_status_store.bin', 'ba+')
        #file_object.write(s)
        #file_object.close()
        #s = datum.serialize()
        file_object = open(os.path.expanduser('~') + '/.lwfm/run_job_status_store.txt', 'a+')
        file_object.write(datum.toString() + "\n")
        file_object.close()


#************************************************************************************************************************************

# A "job status handler map" is a mapping of a canonical JobStatus state value to any callable handler function which takes a
# JobStatus arg.  Not all states need be represented with handlers.
# jobStatusHandlerMap: dict[JobStatusValues, callable]

# Terminal job states will execute the handler function one time. When the job moves to a terminal state, any remaining handlers
# for that job will be evicted.
# Non-terminal job states (e.g. INFO states) will execute the handler function once per unique occurance of the state.
# Only one handler may exist for each state.

# A "job status handler dictionary" is a set of "job status handler maps" indexed by jobId. This allows a O(1) lookup of the handler
# map for a given job, and O(1) lookup of the specific callable for that job state value.

# Changes to the "job status handler dictionary" should be written to a backing persistence.  Changes include: adding of a new
# handler map for a specific job, removing a handler map (when the job moves to a terminal state), updating the last instance
# identifier for a job & job status for non-terminal states (e.g. keeping track of the last and/or every "INFO" seen to notice a
# unique instance).
#
# So our conceptual data structure is: jobStatusHandlerDict: dict[str,dict[JobStatusValues,callable]]
# Plus the list of distinct JobStatus records (their ids) for non-terminal states which were previously seen and handled.
# This permits us to not be listening for job status coninuously (e.g. our laptop lid is closed) and then at some point
# catch up, executing the handlers for state changes not previously seen and handled.
#
# Conceptually, the key str to this handler dict could be a wildcard: "handler map for all jobs", "handler map for all jobs for a
# given site".
#

class RunEventStore(RunStore):
    # maintain an in-memory map of event listeners

    def __init__(self):
        super(RunEventStore, self).__init__()


#************************************************************************************************************************************
