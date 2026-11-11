import asyncio
import random
import uuid
import asyncpg
from asyncpg.connection import Connection
from soup_internship.settings import get_settings as get_api_db_settings

Job = uuid.UUID


async def process(conn: Connection, worker_uuid: uuid.UUID, job: Job):
    await do_side_effect(conn, worker_uuid, job)
    await asyncio.sleep(random.uniform(0.05, 0.9))  # imitating work
    await do_final_effect_and_mark_as_succeeded(conn, worker_uuid, job)


# side effect is like a log_table of that the job after startup is being run and doing something
async def do_side_effect(conn: Connection, worker_uuid: uuid.UUID, job: Job):
    await conn.execute(
        """
                           INSERT INTO side_effect
                           (worker_uuid, job_uuid, message)
                           VALUES ($1, $2, 'RUNNING');
                       """,
        worker_uuid,
        job,
    )


async def do_final_effect_and_mark_as_succeeded(
    conn: Connection, worker_uuid: uuid.UUID, job: Job
):
    async with conn.transaction():
        job_uuid = await conn.fetchval(
            """
                           UPDATE jobs
                           SET status='succeeded', lease_expires_at=null
                           WHERE status IN ('running')
                           AND worker_uuid=$1
                           AND uuid=$2
                           AND clock_timestamp() < lease_expires_at
                           RETURNING uuid;
                           """,
            worker_uuid,
            job,
        )
        if job_uuid is None:
            return
        await conn.execute(
            """
                            INSERT INTO final_effect(worker_uuid, job_uuid, message)
                            VALUES($1, $2, 'FINISHED');
                           """,
            worker_uuid,
            job,
        )


async def start(worker_uuid: uuid.UUID):
    delay = 0.05
    conn = await asyncpg.connect(dsn=get_api_db_settings().dsn)
    while True:
        await reap(conn)
        job = await claim(conn, worker_uuid)

        if job is None:
            await asyncio.sleep(delay)
            delay = min(delay * 2.0, 1.0)
            continue

        delay = 0.05
        await process(conn, worker_uuid, job)


async def reap(conn: Connection, max_attempts: int = 3):
    await conn.execute(
        """
                        UPDATE jobs
                        SET status = CASE WHEN attempts + 1 > $1 THEN 'failed' ELSE 'queued' END,
                            attempts = attempts + 1,
                            worker_uuid = NULL,
                            lease_expires_at = NULL
                        WHERE status = 'running' AND lease_expires_at < clock_timestamp()
                        RETURNING uuid, status, attempts;
                       """,
        max_attempts,
    )


async def claim(conn: Connection, worker_uuid: uuid.UUID) -> Job | None:
    res = await conn.fetchval(
        """
            WITH selected AS (
                SELECT * FROM jobs
                WHERE status IN('queued') AND worker_uuid IS NULL AND lease_expires_at IS NULL
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            ) UPDATE jobs
            SET status='running', worker_uuid=$1, lease_expires_at=now() + interval '30s'
            WHERE id IN (SELECT id FROM selected)
            RETURNING uuid;
        """,
        worker_uuid,
    )
    return res


def main():

    worker_uuid = uuid.uuid4()

    asyncio.run(start(worker_uuid))


if __name__ == "__main__":
    main()
