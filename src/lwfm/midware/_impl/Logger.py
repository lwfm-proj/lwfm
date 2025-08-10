"""
This class provides a wrapper around Python's built-in logging module,
offering simplified logging methods with optional JobContext integration.
It persists to the lwfm store.
"""

#pylint: disable = missing-class-docstring, invalid-name, missing-function-docstring
#pylint: disable = unused-argument

import logging
import datetime
from typing import Optional

from lwfm.base.JobContext import JobContext
from lwfm.midware._impl.LwfmEventClient import LwfmEventClient


class Logger:
    def __init__(self, client: LwfmEventClient):
        logging.basicConfig()
        self._logger = logging.getLogger()
        # should suppress most 3rd party libraries' logging output below INFO level
        self._logger.setLevel(logging.INFO)
        self._lwfmClient = client
        self._context = None

    def _getTimestamp(self) -> str:
        current_time = datetime.datetime.now(datetime.timezone.utc)
        formatted_time = current_time.strftime("%Y%m%dT%H:%M:%SZ")
        return formatted_time

    def _buildMsg(self, msg: str = "", site: Optional[str] = None,
                workflowId: Optional[str] = None,
                jobId: Optional[str] = None) -> str:
        out = f"{self._getTimestamp()} {msg}"
        if site is not None:
            out += f" site={site}"
        if workflowId is not None:
            out += f" wfId={workflowId}"
        if jobId is not None:
            out += f" jobId={jobId}"
        return out

    def setContext(self, context: JobContext) -> None:
        """
        Set the context for the logger, which can be used to include job-related
        information in log messages.
        """
        self._context = context

    def getContext(self) -> Optional[JobContext]:
        """
        Get the current context of the logger.
        """
        return self._context

    def setLevel(self, level) -> None:
        self._logger.setLevel(level)

    def _generateLog(self, level: str, msg: str, context: Optional[JobContext] = None) -> str:
        if context is not None:
            site = context.getSiteName()
            workflowId = context.getWorkflowId()
            jobId = context.getJobId()
        elif self._context is not None:
            site = self._context.getSiteName()
            workflowId = self._context.getWorkflowId()
            jobId = self._context.getJobId()
        else:
            site = None
            workflowId = None
            jobId = None
        out = self._buildMsg(msg, site, workflowId, jobId)
        self._lwfmClient.emitLogging(level, out, site or "", workflowId or "", jobId or "")
        return out

    def debug(self, msg: str, *args, context: Optional[JobContext] = None, **kwargs) -> None:
        if args:
            msg = msg % args
        out = self._generateLog("DEBUG", msg, context)
        self._logger.debug(out)

    def info(self, msg: str, *args, context: Optional[JobContext] = None, **kwargs) -> None:
        if args:
            msg = msg % args
        out = self._generateLog("INFO", msg, context)
        self._logger.info(out)

    def warning(self, msg: str, *args, context: Optional[JobContext] = None, **kwargs) -> None:
        if args:
            msg = msg % args
        out = self._generateLog("WARNING", msg, context)
        self._logger.warning(out)

    def error(self, msg: str, *args, context: Optional[JobContext] = None, **kwargs) -> None:
        if args:
            msg = msg % args  # or msg.format(...) depending on style
        out = self._generateLog("ERROR", msg, context)
        self._logger.error(out)

    def critical(self, msg: str, *args, context: Optional[JobContext] = None, **kwargs) -> None:
        if args:
            msg = msg % args
        out = self._generateLog("CRITICAL", msg, context)
        self._logger.critical(out)
