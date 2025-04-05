"""
This class provides a wrapper around Python's built-in logging module,
offering simplified logging methods with optional JobContext integration.
It persists to the lwfm store.
"""

#pylint: disable = missing-class-docstring, invalid-name, missing-function-docstring

import logging
import datetime

from .impl.LwfmEventClient import LwfmEventClient


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
        self._logger.setLevel(level)

    def info(self, msg: str, status: str = None) -> None:
        out = self._buildMsg(msg, status)
        self._logger.info(out)
        self._lwfmClient.emitLogging("INFO", out)

    def error(self, msg: str, status: str = None) -> None:
        out = self._buildMsg(msg, status)
        self._logger.error(out)
        self._lwfmClient.emitLogging("ERROR", out)



# create a singleton logger
logger = Logger()
