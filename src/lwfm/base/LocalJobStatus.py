
import logging

from lwfm.base.JobStatus import JobStatus


class LocalJobStatus(JobStatus):

    def __init__(self):
        super(LocalJobStatus, self).__init__()
        # use default canonical status map


# test
if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    status = LocalJobStatus()
    status.setNativeStatusString("FAILED")
    logging.info(status.getStatusValue())
