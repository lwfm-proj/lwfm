"""
This class provides a wrapper around Python's built-in logging module,
offering simplified logging methods with optional JobContext integration.
It persists to the lwfm store.
"""

#pylint: disable = missing-class-docstring, invalid-name, missing-function-docstring

import logging
import datetime
from typing import Optional

from lwfm.midware._impl.LwfmEventClient import LwfmEventClient


class Logger:
    def __init__(self, client: LwfmEventClient):
        logging.basicConfig()
        self._logger = logging.getLogger()
        # should suppress most 3rd party libraries' logging output below INFO level
        self._logger.setLevel(logging.INFO)
        self._lwfmClient = client

    def _getTimestamp(self) -> str:
        current_time = datetime.datetime.now(datetime.timezone.utc)
        formatted_time = current_time.strftime("%Y%m%dT%H:%M:%SZ")
        return formatted_time

    def _buildMsg(self, msg: str = "", status: str = "") -> str:
        out = f"{self._getTimestamp()} [{status}] {msg}"
        return out

    def setLevel(self, level) -> None:
        self._logger.setLevel(level)

    def _generateLog(self, level: str, msg: str, status: Optional[str] = None) -> str:
        safe_status = status if status is not None else ""
        out = self._buildMsg(msg, safe_status)
        self._lwfmClient.emitLogging(safe_status, out)
        self._lwfmClient.emitLogging(level, out)
        return out

    def debug(self, msg: str, status: Optional[str] = None) -> None:
        safe_status = status if status is not None else ""
        out = self._generateLog("DEBUG", msg, safe_status)
        self._logger.debug(out)

    def info(self, msg: str, status: Optional[str] = None) -> None:
        safe_status = status if status is not None else ""
        out = self._generateLog("INFO", msg, safe_status)
        self._logger.info(out)

    def warning(self, msg: str, status: Optional[str] = None) -> None:
        safe_status = status if status is not None else ""
        out = self._generateLog("WARNING", msg, safe_status)
        self._logger.warning(out)

    def error(self, msg: str, status: Optional[str] = None) -> None:
        safe_status = status if status is not None else ""
        out = self._generateLog("ERROR", msg, safe_status)
        self._logger.error(out)

    def critical(self, msg: str, status: Optional[str] = None) -> None:
        safe_status = status if status is not None else ""
        out = self._generateLog("CRITICAL", msg, safe_status)
        self._logger.critical(out)
