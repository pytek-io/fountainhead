from .server import Server
import contextlib
import rmy
from typing import AsyncIterator, Iterator

@contextlib.asynccontextmanager
async def create_async_client(host_name: str, port: int) -> AsyncIterator[Server]:
    async with rmy.connect(host_name, port) as client:
        yield await client.fetch_remote_object()


@contextlib.contextmanager
def create_sync_client(host_name: str, port: int) -> Iterator[Server]:
    with rmy.create_sync_client(host_name, port) as client:
        yield client.fetch_remote_object()
