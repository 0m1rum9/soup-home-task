from typing import Annotated
import asyncpg
from contextlib import asynccontextmanager
from asyncpg.connection import Connection
from fastapi import Depends, FastAPI, Request
from soup_internship.settings import get_settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await asyncpg.create_pool(dsn=get_settings().dsn)

    yield

    await app.state.pool.close()


async def get_db(request: Request):
    async with request.app.state.pool.acquire() as connection:
        yield connection


DatabaseDep = Annotated[Connection, Depends(get_db)]
