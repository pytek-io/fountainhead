import argparse
import logging
import asyncio
from rmy.server import start_tcp_server
from fountainhead.server import Server
from fountainhead.storage.db import DBStorage
from sqlalchemy.ext.asyncio import create_async_engine


async def main(engine):
    db = DBStorage(engine)
    await db.initialize()
    await start_tcp_server(args.port, Server(db))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(prog="Fountainhead", description="store events")
    parser.add_argument(
        "folder", type=str, nargs="?", help="Stored events location", default="events"
    )
    parser.add_argument("port", type=int, help="tcp port to use", nargs="?", default=8765)
    args = parser.parse_args()
    engine = create_async_engine("sqlite+aiosqlite:///test.sqlite")
    asyncio.run(main(engine))
