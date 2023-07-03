import logging
from datetime import datetime
from pickle import loads
from typing import Any, AsyncIterator, Optional

import asyncstdlib

from .pubsub import PubSubManager
from .abc import Storage


def load_time_stamp_and_value(time_stamp_and_value):
    time_stamp, value = time_stamp_and_value
    return time_stamp, loads(value)


class Server:
    def __init__(self, storage: Storage) -> None:
        self.storage = storage
        self.pub_sub_manager = PubSubManager()

    async def write_event(
        self,
        topic: str,
        event: bytes,
        time_stamp: Optional[datetime] = None,
    ):
        time_stamp = time_stamp or datetime.now()
        await self.storage.write(str(time_stamp.timestamp()), topic, event)
        logging.info(f"Saved event from {self} under: {topic}{time_stamp}")
        self.pub_sub_manager.broadcast_to_subscriptions(topic, time_stamp)
        return time_stamp

    async def read_event(self, time_stamp: datetime):
        return await self.storage.read(time_stamp)

    async def read_events(
        self,
        topic: str,
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        time_stamps_only: bool = False,
    ) -> AsyncIterator[Any]:
        udpates = self.pub_sub_manager.subscribe(topic)
        existing_tags = self.storage.list_topic(topic, start, end)
        async for time_stamp in asyncstdlib.chain(existing_tags, udpates):
            if end and datetime.now() > end:
                break
            yield (
                time_stamp
                if time_stamps_only
                else (
                    time_stamp,
                    await self.read_event(time_stamp),
                )
            )
