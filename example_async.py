import argparse
import random
from datetime import datetime, timedelta
from pickle import dumps, loads

import anyio
from asyncstdlib import scoped_iter

from fountainhead import create_async_client, Server
from rmy import cancel_task_group_on_signal
from itertools import cycle

TOPICS = [f"client_{i}" for i in range(2)]


async def write_events(client):
    topics = cycle(TOPICS)
    while True:
        topic = next(topics)
        time_stamp = await client.write_event(
            topic, dumps({"origin": "sftp", "s3": "fdsljd"})
        )
        print(f"Saved {topic} event at {time_stamp}")
        await anyio.sleep(random.random() * 1)


async def subscribe_to_events(server: Server, topic: str):
    start = datetime.now() - timedelta(minutes=100)
    existing_tags, updates = await server.read_events(topic, start)
    async for x in existing_tags:
        print(f"Received: {topic} {x}")
    print("-" * 30)
    async for x in updates:
        print(f"Received: {topic} {x}")

    #         async for time_stamp, topic, data in asyncstdlib.chain():
    #         if end and datetime.now() > end:
    #             break
    #         yield time_stamp, topic, data

    # async with scoped_iter(server.read_events(topic, start)) as events:
    #     async for time_stamp, topic, content in events:
    #         print(f"Received {topic} {time_stamp} {loads(content)}")


async def main_async(args):
    async with create_async_client(args.host, args.port) as server:
        async with anyio.create_task_group() as task_group:
            task_group.start_soon(cancel_task_group_on_signal, task_group)
            task_group.start_soon(write_events, server)
            for topic in TOPICS:
                task_group.start_soon(subscribe_to_events, server, topic)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="Client test")
    parser.add_argument(
        "host", type=str, help="server host name", nargs="?", default="localhost"
    )
    parser.add_argument("port", type=int, help="tcp port", nargs="?", default=8765)
    args = parser.parse_args()
    anyio.run(main_async, args)
