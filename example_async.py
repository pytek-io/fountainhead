import argparse
import logging
import random
from datetime import datetime, timedelta
from pickle import dumps, loads

import anyio
from asyncstdlib import scoped_iter

from fountainhead import create_async_client, Server
from rmy import cancel_task_group_on_signal


async def write_events(client, topic):
    while True:
        time_stamp = await client.write_event(
            topic, dumps({"origin": "sftp", "s3": "fdsljd"})
        )
        logging.info(f"Saved {topic} event at {time_stamp}")
        await anyio.sleep(random.random() * 1)


async def subscribe_to_events(server: Server, topic: str):
    start = datetime.now() - timedelta(minutes=100)
    async with scoped_iter(server.read_events(topic, start)) as events:
        async for time_stamp, topic, content in events:
            print(f"Received {topic} {time_stamp} {loads(content)}")


async def main_async(args):
    async with create_async_client(args.host, args.port) as server:
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(cancel_task_group_on_signal, task_group)
            for i in range(1):
                topic = f"uploads/client_{i}"
                task_group.start_soon(write_events, server, topic)
                task_group.start_soon(subscribe_to_events, server, topic)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Client test")
    parser.add_argument(
        "host", type=str, help="server host name", nargs="?", default="localhost"
    )
    parser.add_argument("port", type=int, help="tcp port", nargs="?", default=8765)
    args = parser.parse_args()
    anyio.run(main_async, args)
