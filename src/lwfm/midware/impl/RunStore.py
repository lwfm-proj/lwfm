import os
from abc import ABC, abstractmethod

from lwfm.base.JobStatus import JobStatus

class RunStore(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def write(self, datum: type) -> bool:
        pass

class RunJobStatusStore(RunStore):
    def __init__(self):
        super(RunJobStatusStore, self).__init__()

    def write(self, datum: JobStatus) -> bool:
        file_object = open(os.path.expanduser('~') + '/.lwfm/run_job_status_store.txt', 'a+')
        file_object.write(datum.__str__() + "\n")
        file_object.close()


class RunEventStore(RunStore):
    # maintain an in-memory map of event listeners

    def __init__(self):
        super(RunEventStore, self).__init__()

