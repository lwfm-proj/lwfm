"""
This class provides a wrapper around Python's built-in logging module,
offering simplified logging methods with optional JobContext integration.
It persists to the lwfm store.
"""

#pylint: disable = missing-class-docstring, invalid-name, missing-function-docstring
#pylint: disable = unused-argument

import logging
import datetime
import time
from typing import Optional, Union, List

from lwfm.base.JobContext import JobContext
from lwfm.base.Workflow import Workflow
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

    def setContext(self, context: Union[JobContext, Workflow]) -> None:
        """
        Set the context for the logger, which can be used to include job-related
        information in log messages.
        """
        self._context = context

    def getContext(self) -> Optional[Union[JobContext, Workflow]]:
        """
        Get the current context of the logger.
        """
        return self._context

    def setLevel(self, level) -> None:
        self._logger.setLevel(level)

    def _generateLog(self, level: str, msg: str,
        context: Optional[Union[JobContext, Workflow]] = None) -> str:
        if context is None:
            context = self._context
        site = None
        workflowId = None
        jobId = None
        if context is not None:
            if isinstance(context, JobContext):
                site = context.getSiteName()
                workflowId = context.getWorkflowId()
                jobId = context.getJobId()
            elif isinstance(context, Workflow):
                site = None
                workflowId = context.getWorkflowId()
                jobId = context.getWorkflowId()
        out = self._buildMsg(msg, site, workflowId, jobId)
        self._lwfmClient.emitLogging(level, out, site or "", workflowId or "", jobId or "")
        return out


    def debug(self, msg: str, *args, context: Optional[Union[JobContext, Workflow]] = None,
        **kwargs) -> None:
        if args:
            msg = msg % args
        out = self._generateLog("DEBUG", msg, context)
        self._logger.debug(out)

    def info(self, msg: str, *args, context: Optional[Union[JobContext, Workflow]] = None,
        **kwargs) -> None:
        if args:
            msg = msg % args
        out = self._generateLog("INFO", msg, context)
        self._logger.info(out)

    def infoWithSplits(self, msg: str, baseTimes: List[float], *args,
        context: Optional[Union[JobContext, Workflow]] = None, **kwargs) -> None:
        """
        Log an info message with timing splits prepended.

        Prepends timing information in the format:
        [HH:MM:SS] [T+X.XXs] [C+Y.YYs] [ΔZ.ZZs] message

        Where:
        - HH:MM:SS: Current time
        - T+X.XXs: Time elapsed since baseTimes[0] (e.g., workflow start)
        - C+Y.YYs: Time elapsed since baseTimes[1] (e.g., case start) - optional
        - ΔZ.ZZs: Time elapsed since baseTimes[-1] (delta from last checkpoint)

        Parameters
        ----------
        msg : str
            The message to log (supports % formatting with *args)
        baseTimes : List[float]
            List of baseline times (from time.time()). Typically:
            - baseTimes[0]: workflow start time
            - baseTimes[1]: case start time (optional)
            - baseTimes[-1]: last checkpoint time
        *args
            Arguments for % formatting of msg
        context : Optional[Union[JobContext, Workflow]]
            Optional context for the log message
        **kwargs
            Additional keyword arguments (for compatibility)

        Example
        -------
        >>> import time
        >>> workflow_start = time.time()
        >>> case_start = time.time()
        >>> time.sleep(1)
        >>> last_time = time.time()
        >>> logger.infoWithSplits("Processing complete", [workflow_start, case_start, last_time])
        # Logs: [14:30:45] [T+5.23s] [C+1.15s] [Δ1.00s] Processing complete
        """
        if args:
            msg = msg % args

        currentTime = time.time()
        timestamp = datetime.datetime.now().strftime("%H:%M:%S")

        # Build timing prefix based on number of base times provided
        if len(baseTimes) == 0:
            # No base times, just log normally
            self.info(msg, context=context)
            return

        # Always calculate cumulative time from first base time (workflow start)
        cumulative = currentTime - baseTimes[0]
        timingPrefix = f"[{timestamp}] [T+{cumulative:.2f}s]"

        # If we have a second base time (case start), add case cumulative
        if len(baseTimes) >= 2:
            caseCumulative = currentTime - baseTimes[1]
            timingPrefix += f" [C+{caseCumulative:.2f}s]"

        # Always calculate delta from last time in the list
        delta = currentTime - baseTimes[-1]
        timingPrefix += f" [Δ{delta:.2f}s]"

        # Prepend timing info to message
        enhancedMsg = f"{timingPrefix} {msg}"

        # Use the existing info method
        self.info(enhancedMsg, context=context)

    def warning(self, msg: str, *args, context: Optional[Union[JobContext, Workflow]] = None,
        **kwargs) -> None:
        if args:
            msg = msg % args
        out = self._generateLog("WARNING", msg, context)
        self._logger.warning(out)

    def error(self, msg: str, *args, context: Optional[Union[JobContext, Workflow]] = None,
        **kwargs) -> None:
        if args:
            msg = msg % args  # or msg.format(...) depending on style
        out = self._generateLog("ERROR", msg, context)
        self._logger.error(out)

    def critical(self, msg: str, *args, context: Optional[Union[JobContext, Workflow]] = None,
        **kwargs) -> None:
        if args:
            msg = msg % args
        out = self._generateLog("CRITICAL", msg, context)
        self._logger.critical(out)
