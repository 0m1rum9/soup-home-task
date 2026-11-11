CREATE TABLE IF NOT EXISTS jobs(
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    uuid UUID NOT NULL UNIQUE,
    status TEXT CHECK(status in ('queued', 'running', 'failed', 'succeeded', 'cancelled')) DEFAULT 'queued',
    worker_uuid UUID NULL,
    lease_expires_at TIMESTAMP DEFAULT NULL,
    attempts SMALLINT DEFAULT 0
);


CREATE TABLE IF NOT EXISTS final_effect(
    worker_uuid UUID NOT NULL,
    job_uuid UUID NOT NULL UNIQUE,
    message TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS side_effect(
    worker_uuid UUID NOT NULL,
    job_uuid UUID NOT NULL,
    message TEXT NOT NULL
);
