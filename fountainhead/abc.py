from __future__ import annotations
from typing import Any, Optional
from abc import ABCMeta, abstractmethod
from datetime import datetime


class Storage(metaclass=ABCMeta):
    @abstractmethod
    async def write(self, key: datetime, topic: str, content: Any):
        ...

    async def read(self, topic: str, name: str):
        ...

    async def list_topic(
        self, topic: str, start: Optional[datetime], end: Optional[datetime]
    ):
        ...
