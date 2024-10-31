"""
This class provides a wrapper around Python's built-in logging module,
offering simplified logging methods with optional JobContext integration.
It initializes a logger with INFO level by default and provides methods
to set the logging level and log messages at different severity levels.
"""

import logging
import datetime

from lwfm.midware.impl.LwfmEventClient import LwfmEventClient


class Logger:
    _logger = None
    _lwfmClient = None

    # create a singleton logger
    def __init__(self):
        logging.basicConfig()
        self._logger = logging.getLogger()
        self._logger.setLevel(logging.INFO)
        self._lwfmClient = LwfmEventClient()

    def _getTimestamp(self) -> str:
        current_time = datetime.datetime.now(datetime.timezone.utc)
        formatted_time = current_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        return formatted_time

    def _buildMsg(self, msg: str, status: str) -> str:
        if (status is None):
            status = ""
        if (msg is None):
            msg = ""
        out = " {} [{}] {}".format(
            self._getTimestamp(),
            status,
            msg,
        )
        return out

    def setLevel(self, level) -> None:
        """
        Set the logging level.

        :param level: the level to set (e.g. logging.INFO, logging.DEBUG, etc.)
        :type level: int
        """
        self._logger.setLevel(level)

    def info(self, msg: str, status: str = None) -> None:
        """
        Log an informational message.

        :param msg: the message to log
        :type msg: str
        :param jobStatus: the job status info to add to the log message (optional)
        :type jobStatus: JobStatus
        """
        out = self._buildMsg(msg, status)
        self._logger.info(out)
        self._lwfmClient.emitLogging("INFO", out)

    def error(self, msg: str, status: str = None) -> None:
        """
        Log an error message.

        :param msg: the message to log
        :type msg: str
        :param status: the job status info to add to the log message (optional)
        :type status: JobStatus
        """
        out = self._buildMsg(msg, status)
        self._logger.error(out)
        self._lwfmClient.emitLogging("ERROR", out)



# create a singleton logger
Logger = Logger()
