from datetime import datetime
from typing import Any, Optional

import sqlalchemy as sqla
from sqlalchemy.ext.asyncio import AsyncSession

from fountainhead.abc import Storage

metadata = sqla.MetaData()

id_col = sqla.Column("id", sqla.TIMESTAMP(), primary_key=True)
topic_col = sqla.Column("topic", sqla.String(255), nullable=False)
data_col = sqla.Column("data", sqla.BLOB())
Record = sqla.Table("Record", metadata, id_col, topic_col, data_col)


class DBStorage(Storage):
    def __init__(self, engine) -> None:
        self.engine = engine
        self.metadata = sqla.MetaData()

    async def initialize(self):
        async with self.engine.connect() as connection:
            await connection.run_sync(metadata.drop_all)
            await connection.run_sync(metadata.create_all)

    async def write(self, key: datetime, topic: str, content: Any):
        key = datetime.now()
        async with AsyncSession(self.engine) as session:
            async with session.begin():
                await session.execute(
                    Record.insert(), [{"id": key, "topic": topic, "data": content}]
                )
        return key

    async def read(self, key: datetime):
        async with self.engine.connect() as connection:
            return (
                await connection.execute(Record.select().where(id_col == key))
            ).fetchall()

    async def list_topic(
        self,
        topic: str,
        start: Optional[datetime],
        end: Optional[datetime],
    ):
        conditions = [topic_col.regexp_match(topic)]
        if start:
            conditions.append(id_col >= start)
        if end:
            conditions.append(id_col <= end)
        async with self.engine.connect() as connection:
            for value in (await connection.execute(Record.select().where(*conditions))).fetchall():
                yield value
