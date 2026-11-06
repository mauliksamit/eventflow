-- Dimensional warehouse (Kimball-style star schema) plus pipeline metadata.
-- Event-level history stays in the lake as Parquet. The warehouse holds what analysts query:
-- conformed dimensions, daily and hourly facts, and individual stars.
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS mart;

-- ---------- Dimensions ----------
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key      INT PRIMARY KEY,           -- yyyymmdd
    full_date     DATE NOT NULL UNIQUE,
    day_of_week   SMALLINT NOT NULL,
    day_name      TEXT NOT NULL,
    iso_week      SMALLINT NOT NULL,
    month         SMALLINT NOT NULL,
    year          SMALLINT NOT NULL,
    is_weekend    BOOLEAN NOT NULL
);

-- Type 2: a rename opens a new version, so facts keep the name the repo had at the time.
CREATE TABLE IF NOT EXISTS dw.dim_repo (
    repo_key      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    repo_id       BIGINT NOT NULL,
    repo_name     TEXT NOT NULL,
    owner         TEXT NOT NULL,
    valid_from    DATE NOT NULL,
    valid_to      DATE NOT NULL DEFAULT '9999-12-31',
    is_current    BOOLEAN NOT NULL DEFAULT true,
    UNIQUE (repo_id, valid_from)
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_repo_current ON dw.dim_repo (repo_id) WHERE is_current;

-- Type 1: logins can change, but only the latest matters for labelling.
CREATE TABLE IF NOT EXISTS dw.dim_actor (
    actor_id      BIGINT PRIMARY KEY,
    login         TEXT NOT NULL,
    actor_class   TEXT NOT NULL,             -- human | bot | ai_agent
    first_seen    TIMESTAMPTZ NOT NULL,
    last_seen     TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS dw.dim_event_type (
    event_type    TEXT PRIMARY KEY,
    id_series     TEXT NOT NULL              -- ref | collab
);

-- GitHub's own published incidents: the ground truth for feed anomaly evaluation.
CREATE TABLE IF NOT EXISTS dw.dim_incident (
    incident_id   TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    impact        TEXT NOT NULL,
    status        TEXT NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL,
    resolved_at   TIMESTAMPTZ,
    components    TEXT[] NOT NULL
);

-- ---------- Facts ----------
-- Grain: one row per repo per day with any captured activity. Range partitioned by month.
CREATE TABLE IF NOT EXISTS dw.fact_repo_daily (
    date_key      INT NOT NULL REFERENCES dw.dim_date(date_key),
    repo_key      BIGINT NOT NULL,
    events        INT NOT NULL,
    stars         INT NOT NULL,
    forks         INT NOT NULL,
    pushes        INT NOT NULL,
    prs_opened    INT NOT NULL,
    issues_opened INT NOT NULL,
    releases      INT NOT NULL,
    actors        INT NOT NULL,
    PRIMARY KEY (date_key, repo_key)
) PARTITION BY RANGE (date_key);

-- Grain: one row per star captured.
CREATE TABLE IF NOT EXISTS dw.fact_star (
    event_id      BIGINT PRIMARY KEY,
    date_key      INT NOT NULL REFERENCES dw.dim_date(date_key),
    repo_key      BIGINT NOT NULL,
    actor_id      BIGINT NOT NULL,
    starred_at    TIMESTAMPTZ NOT NULL
);

-- Grain: one row per hour, event type and actor class.
CREATE TABLE IF NOT EXISTS dw.fact_type_hourly (
    hour_ts       TIMESTAMPTZ NOT NULL,
    event_type    TEXT NOT NULL REFERENCES dw.dim_event_type(event_type),
    actor_class   TEXT NOT NULL,
    events        BIGINT NOT NULL,
    PRIMARY KEY (hour_ts, event_type, actor_class)
);

-- Grain: one row per hour and id sequence. How much of GitHub the archive actually saw.
CREATE TABLE IF NOT EXISTS dw.fact_feed_hourly (
    hour_ts       TIMESTAMPTZ NOT NULL,
    id_series     TEXT NOT NULL,
    events        BIGINT NOT NULL,
    est_total     DOUBLE PRECISION,
    capture_rate  DOUBLE PRECISION,
    PRIMARY KEY (hour_ts, id_series)
);

-- Grain: one row per minute and id sequence.
CREATE TABLE IF NOT EXISTS dw.fact_feed_minute (
    minute_ts     TIMESTAMPTZ NOT NULL,
    id_series     TEXT NOT NULL,
    events        INT NOT NULL,
    id_median     BIGINT NOT NULL,
    PRIMARY KEY (minute_ts, id_series)
);

-- ---------- Operations ----------
CREATE TABLE IF NOT EXISTS ops.ingest_files (
    dt            DATE NOT NULL,
    hour          SMALLINT NOT NULL,
    object_key    TEXT NOT NULL,
    bytes         BIGINT NOT NULL,
    line_count    BIGINT NOT NULL,
    sha256        TEXT NOT NULL,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dt, hour)
);

CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id        TEXT PRIMARY KEY,
    dt            DATE NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    status        TEXT NOT NULL,
    stats         JSONB
);

CREATE TABLE IF NOT EXISTS ops.dq_results (
    run_id        TEXT NOT NULL,
    dt            DATE NOT NULL,
    check_name    TEXT NOT NULL,
    severity      TEXT NOT NULL,
    passed        BOOLEAN NOT NULL,
    observed      DOUBLE PRECISION,
    threshold     DOUBLE PRECISION,
    details       TEXT,
    PRIMARY KEY (run_id, check_name)
);

-- The commit point of Write-Audit-Publish: the load step only reads the run recorded here.
CREATE TABLE IF NOT EXISTS ops.published_partitions (
    dt            DATE PRIMARY KEY,
    run_id        TEXT NOT NULL,
    published_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops.schema_fields (
    event_type    TEXT NOT NULL,             -- '*' means a top-level field
    field         TEXT NOT NULL,
    first_seen    DATE NOT NULL,
    last_seen     DATE NOT NULL,
    PRIMARY KEY (event_type, field)
);

CREATE TABLE IF NOT EXISTS ops.feed_anomalies (
    window_start  TIMESTAMPTZ NOT NULL,
    id_series     TEXT NOT NULL,
    velocity      DOUBLE PRECISION NOT NULL,
    baseline      DOUBLE PRECISION NOT NULL,
    drop_ratio    DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (window_start, id_series)
);
