import asyncio
from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from fountainhead.storage.db import DBStorage
from pickle import dumps


async def main():
    engine = create_async_engine("sqlite+aiosqlite:///test.sqlite")
    db = DBStorage(engine)
    await db.initialize()
    key = await db.write(datetime.now(), "topic", dumps("content"))
    print("hello", await db.read(key))
    print(await db.list_topic("topic", None, None))

if __name__ == "__main__":
    asyncio.run(main())
