-- Dimensional warehouse (Kimball-style star schema).
-- Facts hold measurable events at a declared grain; dimensions hold descriptive
-- attributes, denormalized on purpose so analysts join once per dimension.
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS mart;

-- ---------- Dimensions ----------
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key      INT PRIMARY KEY,           -- yyyymmdd, a smart key that sorts and partitions well
    full_date     DATE NOT NULL UNIQUE,
    day_of_week   SMALLINT NOT NULL,
    day_name      TEXT NOT NULL,
    week_of_year  SMALLINT NOT NULL,
    month         SMALLINT NOT NULL,
    year          SMALLINT NOT NULL,
    is_weekend    BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_id   BIGINT PRIMARY KEY,
    city          TEXT,
    signup_date   DATE,
    first_seen_ts TIMESTAMPTZ,                -- first clickstream event observed
    primary_device TEXT
);

-- Denormalized: category name is copied onto the product row (no snowflaking).
CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_id    BIGINT PRIMARY KEY,
    product_name  TEXT NOT NULL,
    category_name TEXT NOT NULL,
    list_price    NUMERIC(10, 2) NOT NULL
);

-- ---------- Facts ----------
-- Grain: one row per clickstream event (after de-duplication).
CREATE TABLE IF NOT EXISTS dw.fact_events (
    event_id      TEXT PRIMARY KEY,
    date_key      INT NOT NULL REFERENCES dw.dim_date(date_key),
    customer_id   BIGINT NOT NULL REFERENCES dw.dim_customer(customer_id),
    product_id    BIGINT REFERENCES dw.dim_product(product_id),
    session_id    TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    device        TEXT NOT NULL,
    event_ts      TIMESTAMPTZ NOT NULL
);

-- Grain: one row per user session (30-minute inactivity rule, computed in Spark).
CREATE TABLE IF NOT EXISTS dw.fact_sessions (
    session_id    TEXT PRIMARY KEY,
    date_key      INT NOT NULL REFERENCES dw.dim_date(date_key),
    customer_id   BIGINT NOT NULL REFERENCES dw.dim_customer(customer_id),
    start_ts      TIMESTAMPTZ NOT NULL,
    end_ts        TIMESTAMPTZ NOT NULL,
    n_events      INT NOT NULL,
    converted     BOOLEAN NOT NULL            -- session contained a checkout event
);

-- Grain: one row per order line.
CREATE TABLE IF NOT EXISTS dw.fact_order_items (
    order_id      BIGINT NOT NULL,
    line_no       INT NOT NULL,
    date_key      INT NOT NULL REFERENCES dw.dim_date(date_key),
    customer_id   BIGINT NOT NULL REFERENCES dw.dim_customer(customer_id),
    product_id    BIGINT NOT NULL REFERENCES dw.dim_product(product_id),
    status        TEXT NOT NULL,
    quantity      INT NOT NULL,
    revenue       NUMERIC(12, 2) NOT NULL,
    PRIMARY KEY (order_id, line_no)
);

-- Grain: one row per HTTP request in the server access logs.
CREATE TABLE IF NOT EXISTS dw.fact_requests (
    date_key      INT NOT NULL REFERENCES dw.dim_date(date_key),
    request_ts    TIMESTAMPTZ NOT NULL,
    method        TEXT NOT NULL,
    path_group    TEXT NOT NULL,
    status_code   SMALLINT NOT NULL,
    bytes_sent    INT NOT NULL,
    latency_ms    REAL NOT NULL
);

-- ---------- Operations / monitoring ----------
CREATE TABLE IF NOT EXISTS ops.pipeline_runs (
    run_id        TEXT PRIMARY KEY,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    status        TEXT NOT NULL DEFAULT 'running',
    stats         JSONB
);

CREATE TABLE IF NOT EXISTS ops.dq_results (
    run_id        TEXT NOT NULL,
    check_name    TEXT NOT NULL,
    severity      TEXT NOT NULL,              -- critical | warn
    passed        BOOLEAN NOT NULL,
    observed      DOUBLE PRECISION,
    threshold     DOUBLE PRECISION,
    details       TEXT,
    checked_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, check_name)
);

CREATE TABLE IF NOT EXISTS ops.watermarks (
    source        TEXT PRIMARY KEY,
    high_water    TIMESTAMPTZ NOT NULL
);
