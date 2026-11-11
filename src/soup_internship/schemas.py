from typing import Annotated
from uuid import UUID
from pydantic import BaseModel, Field


class JobCreate(BaseModel):
    uuid: UUID


class JobResponse(BaseModel):
    id: int
    uuid: UUID
    status: str


class JobsResponse(BaseModel):
    data: list[JobResponse]
    next_cursor: int | None
