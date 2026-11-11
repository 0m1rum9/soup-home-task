#!/usr/bin/env python3
import argparse
import asyncio
import json
import random
import subprocess
import sys
import traceback
import uuid
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path

import asyncpg
import httpx

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = [
    "docker",
    "compose",
    "-f",
    "docker-compose.yml",
    "-f",
    "chaos/docker-compose.chaos.yml",
]


class Tee:
    def __init__(self, path):
        self.file = path.open("w", encoding="utf-8")
        self.streams = (sys.__stdout__, self.file)

    def write(self, text):
        for stream in self.streams:
            stream.write(text)
            stream.flush()

    def flush(self):
        for stream in self.streams:
            stream.flush()


def command(*args, check=True, show=True):
    process = subprocess.run(args, cwd=ROOT, text=True, capture_output=True)
    if show and process.stdout:
        print(process.stdout, end="")
    if show and process.stderr:
        print(process.stderr, end="", file=sys.stderr)
    if check and process.returncode:
        raise RuntimeError(f"command failed ({process.returncode}): {' '.join(args)}")
    return process.stdout


def env_file():
    values = {}
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def compose(*args, check=True, show=True):
    return command(*COMPOSE, *args, check=check, show=show)


async def db_connect():
    values = env_file()
    return await asyncpg.connect(
        host="127.0.0.1",
        port=55432,
        user=values["DB_USER"],
        password=values["DB_PASSWORD"],
        database=values["DB_NAME"],
    )


async def running_workers():
    return [
        item
        for item in compose(
            "ps", "--status", "running", "-q", "worker", show=False
        ).splitlines()
        if item
    ]


async def reset(conn):
    compose("stop", "worker")
    await conn.execute("TRUNCATE jobs, side_effect, final_effect RESTART IDENTITY")
    compose("up", "-d", "--scale", "worker=8")
    await asyncio.sleep(2)
    workers = await running_workers()
    if len(workers) != 8:
        raise RuntimeError(f"expected 8 running workers, got {len(workers)}")


async def load(args, rng, posts, cancels, kills):
    accepted, create_done, stop_kills = [], asyncio.Event(), asyncio.Event()
    accepted_set = set()
    lock, limiter = asyncio.Lock(), asyncio.Semaphore(40)
    job_ids = [str(uuid.uuid4()) for _ in range(args.jobs)]
    cancel_conn, disrupt_conn = await db_connect(), await db_connect()

    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:8000", timeout=15
    ) as client:

        async def post_job(job_uuid):
            async with limiter:
                response = await client.post("/jobs", json={"uuid": job_uuid})
            async with lock:
                posts[job_uuid].append(response.status_code)
                if response.status_code in (200, 201) and job_uuid not in accepted_set:
                    accepted_set.add(job_uuid)
                    accepted.append(job_uuid)

        async def create_all():
            await asyncio.gather(*(post_job(job_uuid) for job_uuid in job_ids))
            repeats = []
            for job_uuid in job_ids:
                if rng.random() < 0.20:
                    repeats.extend(
                        [post_job(job_uuid) for _ in range(rng.randint(2, 3))]
                    )
            await asyncio.gather(*repeats)
            create_done.set()

        async def cancel_all():
            while len(accepted) < max(1, args.jobs // 20) and not create_done.is_set():
                await asyncio.sleep(0.05)
            for _ in range(round(args.jobs * 0.08)):
                await asyncio.sleep(rng.uniform(0, 0.25))
                running = await cancel_conn.fetch(
                    "SELECT uuid::text FROM jobs WHERE status='running'"
                )
                choices = (
                    [row["uuid"] for row in running]
                    if running and rng.random() < 0.5
                    else accepted
                )
                if not choices:
                    continue
                job_uuid = rng.choice(choices)
                async with limiter:
                    response = await client.post(f"/jobs/{job_uuid}/cancel")
                cancels[job_uuid].append(response.status_code)

        async def disrupt():
            started = asyncio.get_running_loop().time()
            while (
                not stop_kills.is_set()
                and asyncio.get_running_loop().time() - started < args.kill_max_seconds
            ):
                queued = await disrupt_conn.fetchval(
                    "SELECT count(*) FROM jobs WHERE status='queued'"
                )
                if create_done.is_set() and not queued:
                    return
                if not queued:
                    await asyncio.sleep(0.1)
                    continue
                workers = await running_workers()
                if workers:
                    worker = rng.choice(workers)
                    if args.freeze:
                        print(f"FREEZE {worker}")
                        command("docker", "pause", worker, show=False)
                        try:
                            await asyncio.sleep(45)
                        finally:
                            command("docker", "unpause", worker, show=False)
                        kills.append(
                            {"container": worker, "action": "freeze", "seconds": 45}
                        )
                    else:
                        print(f"KILL {worker}")
                        command("docker", "kill", "-s", "SIGKILL", worker, show=False)
                        command("docker", "start", worker, show=False)
                        kills.append({"container": worker, "action": "SIGKILL"})
                await asyncio.sleep(2)

        disrupt_task = asyncio.create_task(disrupt())
        try:
            await asyncio.gather(create_all(), cancel_all())
        finally:
            stop_kills.set()
            try:
                await disrupt_task
            finally:
                await cancel_conn.close()
                await disrupt_conn.close()
    return accepted


async def settle(conn, timeout):
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        active = await conn.fetchval(
            "SELECT count(*) FROM jobs WHERE status IN ('queued', 'running')"
        )
        if not active:
            return
        if asyncio.get_running_loop().time() >= deadline:
            print(f"settle timeout with {active} active jobs")
            return
        await asyncio.sleep(1)


def invariant(name, violations):
    result = {
        "status": "PASS" if not violations else "FAIL",
        "violations": len(violations),
        "examples": violations[:5],
    }
    print(
        f"{name} {result['status']} violations={result['violations']} examples={result['examples']}"
    )
    return result


async def check(conn, accepted, posts, cancels, check_only):
    rows = await conn.fetch("""
        SELECT j.uuid::text, j.status, j.attempts, count(DISTINCT f.job_uuid) AS finals,
               count(s.job_uuid) AS sides
        FROM jobs j LEFT JOIN final_effect f ON f.job_uuid=j.uuid
        LEFT JOIN side_effect s ON s.job_uuid=j.uuid
        GROUP BY j.uuid, j.status, j.attempts
    """)
    data = {row["uuid"]: dict(row) for row in rows}
    results = {}
    if not check_only:
        results["I1"] = invariant(
            "I1", [job_uuid for job_uuid in accepted if job_uuid not in data]
        )
        i2 = [job_uuid for job_uuid in accepted if posts[job_uuid].count(201) != 1]
        i2 += [
            job_uuid
            for job_uuid, codes in posts.items()
            if any(code not in (200, 201) for code in codes)
        ]
        results["I2"] = invariant("I2", list(dict.fromkeys(i2)))
    active = [
        job_uuid
        for job_uuid, row in data.items()
        if row["status"] in ("queued", "running")
    ]
    results["I3"] = invariant("I3", active)
    i4 = []
    for job_uuid, row in data.items():
        if (row["status"] == "succeeded" and row["finals"] != 1) or (
            row["status"] != "succeeded" and row["finals"] != 0
        ):
            i4.append(job_uuid)
    for job_uuid, codes in cancels.items():
        status = data.get(job_uuid, {}).get("status")
        if (204 in codes and status != "cancelled") or (
            409 in codes and status not in ("succeeded", "failed")
        ):
            i4.append(job_uuid)
    results["I4"] = invariant("I4", list(dict.fromkeys(i4)))
    i5 = [
        job_uuid
        for job_uuid, row in data.items()
        if (row["status"] == "succeeded" and row["sides"] < 1)
        or row["sides"] > row["attempts"] + 1
    ]
    results["I5"] = invariant("I5", i5)
    histogram = Counter(row["sides"] for row in data.values() if row["sides"])
    repeated = sum(count for times, count in histogram.items() if times > 1)
    print("INFO repeated_side_effect_jobs=", repeated)
    print("INFO side_effect_histogram=", dict(sorted(histogram.items())))
    return results, data


async def metrics(conn):
    failed = await conn.fetchval("SELECT count(*) FROM jobs WHERE status='failed'")
    retries = await conn.fetchval("SELECT count(*) FROM jobs WHERE attempts >= 1")
    containers = compose("ps", "-aq", "worker", show=False).split()
    restarts = 0
    if containers:
        restarts = sum(
            int(value)
            for value in command(
                "docker", "inspect", "-f", "{{.RestartCount}}", *containers, show=False
            ).split()
        )
    logs = compose("logs", "worker", check=False, show=False)
    errors = sum(
        "Traceback" in line or "SerializationError" in line
        for line in logs.splitlines()
    )
    result = {
        "failed": failed,
        "attempts_ge_1": retries,
        "worker_restarts": restarts,
        "worker_log_errors": errors,
    }
    print("INFO metrics=", result)
    return result


async def main(args):
    rng = random.Random(args.seed)
    print(f"seed={args.seed}")
    conn = await db_connect()
    posts, cancels, kills, accepted = defaultdict(list), defaultdict(list), [], []
    try:
        if not args.check_only:
            await reset(conn)
            accepted = await load(args, rng, posts, cancels, kills)
            await settle(conn, args.settle_timeout)
        results, _ = await check(conn, accepted, posts, cancels, args.check_only)
        extra = await metrics(conn)
        status = (
            "PASS"
            if all(item["status"] == "PASS" for item in results.values())
            else "FAIL"
        )
        return status, {
            "parameters": vars(args),
            "seed": args.seed,
            "status": status,
            "invariants": results,
            "kills": kills,
            "metrics": extra,
        }
    finally:
        await conn.close()


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs", type=int, default=10000)
    parser.add_argument(
        "--seed", type=int, default=random.SystemRandom().randrange(2**63)
    )
    parser.add_argument("--kill-max-seconds", type=int, default=900)
    parser.add_argument("--settle-timeout", type=int, default=240)
    parser.add_argument("--check-only", action="store_true")
    parser.add_argument("--freeze", action="store_true")
    args = parser.parse_args()
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    directory = ROOT / "runs" / f"{stamp}_seed{args.seed}"
    suffix = 1
    while directory.exists():
        directory = ROOT / "runs" / f"{stamp}_seed{args.seed}_{suffix}"
        suffix += 1
    directory.mkdir(parents=True)
    original_out, original_err = sys.stdout, sys.stderr
    tee = Tee(directory / "raw.log")
    sys.stdout = sys.stderr = tee
    status, result = (
        "INVALID",
        {
            "parameters": vars(args),
            "seed": args.seed,
            "status": "INVALID",
            "invariants": {},
            "kills": [],
        },
    )
    try:
        status, result = asyncio.run(main(args))
    except BaseException:
        traceback.print_exc()
    result["status"] = status
    (directory / "result.json").write_text(
        json.dumps(result, indent=2) + "\n", encoding="utf-8"
    )
    with (ROOT / "runs" / "INDEX.tsv").open("a", encoding="utf-8") as index:
        index.write(f"{directory.name}\t{status}\t{args.seed}\n")
    print(f"RESULT {status} {directory}")
    sys.stdout, sys.stderr = original_out, original_err
    return {"PASS": 0, "FAIL": 1, "INVALID": 2}[status]


if __name__ == "__main__":
    raise SystemExit(run())
