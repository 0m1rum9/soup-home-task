from typing import Annotated
from uuid import UUID
from fastapi import FastAPI, Path, Query, Response
from soup_internship.db import DatabaseDep
from soup_internship.schemas import JobCreate, JobsResponse
from soup_internship.db import lifespan
from starlette.responses import JSONResponse
import uvicorn


app = FastAPI(lifespan=lifespan)


@app.get("/jobs", response_model=JobsResponse)
async def root(
    db: DatabaseDep,
    limit: Annotated[int, Query(ge=1, le=100)] = 100,
    cursor: Annotated[int, Query(ge=0)] = 0,
):
    res = await db.fetch(
        "SELECT * FROM jobs WHERE id > $1 ORDER BY id ASC LIMIT $2", cursor, limit
    )

    jobs = [dict(i) for i in res]
    return {"data": jobs, "next_cursor": jobs[-1]["id"] if len(jobs) > 0 else None}


@app.post("/jobs")
async def create_job(db: DatabaseDep, job: JobCreate):
    res = await db.fetchval(
        "INSERT INTO jobs(uuid) VALUES($1) ON CONFLICT (uuid) DO NOTHING RETURNING uuid",
        job.uuid,
    )
    if res is None:
        return JSONResponse({}, status_code=200)

    return JSONResponse({}, status_code=201)


@app.get("/jobs/{uuid}")
async def status_uuid(db: DatabaseDep, uuid: Annotated[UUID, Path()]):
    status = await db.fetchval("SELECT status FROM jobs WHERE uuid=$1", uuid)

    if status is None:
        return JSONResponse({"detail": "not found"}, status_code=404)

    return {"status": status}


@app.post("/jobs/{uuid}/cancel")
async def cancel_job(db: DatabaseDep, uuid: Annotated[UUID, Path()]):
    await db.execute(
        "UPDATE jobs SET status='cancelled' WHERE uuid=$1 AND status IN ('queued', 'running')",
        uuid,
    )
    status = await db.fetchval("SELECT status FROM jobs WHERE uuid=$1", uuid)

    if status is None:
        return JSONResponse({"detail": "job not found"}, status_code=404)
    elif status in ("succeeded", "failed"):
        return JSONResponse(
            {"detail": "cannot cancel succeeded or failed job"}, status_code=409
        )

    return Response(status_code=204)


def main():
    uvicorn.run(app="soup_internship.main:app", host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
