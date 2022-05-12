
from enum import Enum
import logging
import uuid

from datetime import datetime

from lwfm.base.LwfmBase import LwfmBase


class _JobDefnFields(Enum):
    NAME               = "name"
    COMPUTE_TYPE       = "computeType"
    ENTRY_POINT_PATH   = "entryPointPath"
    NOTIFICATION_EMAIL = "notificationEmail"


class JobDefn(LwfmBase):

    def __init__(self, args: dict=None):
        super(JobDefn, self).__init__(args)

    def setName(self, name: str) -> None:
        LwfmBase._setArg(self, _JobDefnFields.NAME.value, name)

    def getName(self) -> str:
        return LwfmBase._getArg(self, _JobDefnFields.NAME.value)

    def setComputeType(self, name: str) -> None:
        LwfmBase._setArg(self, _JobDefnFields.COMPUTE_TYPE.value, name)

    def getComputeType(self) -> str:
        return LwfmBase._getArg(self, _JobDefnFields.COMPUTE_TYPE.value)

    def setEntryPointPath(self, entryPointPath: [str]) -> None:
        LwfmBase._setArg(self, _JobDefnFields.ENTRY_POINT_PATH.value, entryPointPath)

    def getEntryPointPath(self) -> [str]:
        return LwfmBase._getArg(self, _JobDefnFields.ENTRY_POINT_PATH.value)

    def setNotificationEmail(self, email: str) -> None:
        LwfmBase._setArg(self, _JobDefnFields.NOTIFICATION_EMAIL.value, email)

    def getNotificationEmail(self) -> str:
        return LwfmBase._getArg(self, _JobDefnFields.NOTIFICATION_EMAIL.value)

    def setExtraArgs(self, args: dict=None) -> None:
        LwfmBase.setArgs(self, args)

    def getExtraArgs(self) -> dict:
        return LwfmBase.getArgs(self)
