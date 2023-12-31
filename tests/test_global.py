import contextlib
import sys
import tempfile
from pickle import dumps, loads
from typing import AsyncIterator

import anyio
import asyncstdlib
import pytest

from fountainhead.server import Server


if sys.version_info < (3, 10):
    from asyncstdlib import anext


@contextlib.asynccontextmanager
async def create_test_client_session() -> AsyncIterator[Server]:
    with tempfile.TemporaryDirectory() as event_folder:
        yield Server(event_folder)


@pytest.mark.anyio
async def test_read_and_write():
    async with create_test_client_session() as client_session:
        topic, original_value = "topic/subtopic", dumps([123])
        time_stamp = await client_session.write_event(
            topic, original_value, None, False
        )
        returned_value = await client_session.read_event(topic, time_stamp)
        assert original_value is not returned_value
        assert original_value == returned_value


@pytest.mark.anyio
async def test_subscription():
    async with create_test_client_session() as client_session:
        topic = "topic/subtopic"
        nb_events = 10

        async def read_events():
            async for index, (_time_stamp, value) in asyncstdlib.enumerate(
                client_session.read_events(topic)
            ):
                assert loads(value) == {"value": index}
                if index == nb_events - 1:
                    break

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(read_events)
            for i in range(nb_events):
                value = dumps({"value": i})
                await client_session.write_event(topic, value)
