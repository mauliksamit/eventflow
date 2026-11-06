# eventflow

A batch data pipeline that collects log events from three sources, lands them in an S3/Parquet
data lake, models them into a PostgreSQL star schema warehouse, and serves daily metrics,
data quality results and a dashboard. It covers the full path end to end:
ingestion, transformation at scale, data modeling, data quality, monitoring and query tuning.

```mermaid
flowchart LR
  A[Clickstream JSON<br/>web, iOS, Android] --> R
  B[Web server<br/>access logs] --> R
  C[(OLTP PostgreSQL<br/>orders, 3NF)] -- incremental extract<br/>watermark --> R
  R[Raw zone<br/>local or S3<br/>dt= partitions] --> S[PySpark<br/>parse, validate, de-dupe,<br/>sessionize]
  S --> Q[Quarantine<br/>bad records + reason]
  S --> P[Curated zone<br/>Parquet, partitioned by event_date]
  P -- COPY + upserts --> W[(Warehouse<br/>star schema)]
  W --> M[Marts<br/>materialized views]
  M --> D[Dashboard]
  W --> DQ[Data quality checks<br/>ops.dq_results]
```

## Results from a full run

| Metric | Value |
|---|---:|
| Clickstream lines read | 2,021,627 |
| Quarantined (malformed JSON, missing fields, unknown event types) | 8,211 (0.41%) |
| Duplicate deliveries removed | 19,540 |
| Events loaded into `fact_events` | 1,993,876 |
| Sessions built (30-minute inactivity rule) | 172,156 |
| Access-log lines read / quarantined | 994,000 / 1,977 |
| Order lines (incremental extract from OLTP) | 10,282 |
| Spark transform time (local, 2 cores) | 87.3 s |
| Warehouse load time (COPY + upserts) | 39.8 s |
| Re-run on the same input | identical row counts in every fact table (idempotent) |

The pipeline found every problem injected into the synthetic data: ~0.4% malformed clickstream records and ~0.2% garbled log lines were quarantined with a reason; ~1% duplicate deliveries were removed; late events landed in the partition of the day they happened; and the hourly 5xx check flagged the injected outage (7.7% errors at 2026-10-10 15:00 UTC vs a ~0.5% baseline).

**Query tuning** (`docs/query_tuning.md`, median of 5 `EXPLAIN ANALYZE` runs):

| Query | Before (ms) | After (ms) | Change |
|---|---:|---:|---|
| Last 50 events for one customer (support/debugging lookup) | 200.54 | 0.02 | B-tree index on (customer_id, event_ts DESC) |
| Revenue by category for the last 7 days | 2.65 | 0.06 | read pre-aggregated materialized view |
| 5xx errors by endpoint during a 2-hour incident window | 69.93 | 2.0 | BRIN index on request_ts |
| Daily funnel: sessions, active users, conversion | 127.89 | 0.03 | read pre-aggregated materialized view |

![Dashboard](docs/dashboard.png)

## What each part does

| Stage | Code | Key ideas |
|---|---|---|
| Generate sources | `eventflow/generate.py` | Zipf-like users, mixed time zones, duplicates, late and malformed records, an injected outage |
| Extract | `eventflow/extract.py` | Incremental extract from the OLTP database using a high-water mark on `updated_at` |
| Land | `eventflow/storage.py` | Raw zone kept immutable on S3 (boto3), partitioned by arrival date, re-downloadable for backfills |
| Transform | `eventflow/transform.py` | Schema-on-read for JSON, regex parsing of logs, UTC normalization, de-duplication, 30-minute sessionization with window functions, quarantine with reasons, idempotent partition overwrite |
| Load | `eventflow/load.py` | Bulk `COPY` into staging, set-based `ON CONFLICT` upserts into facts and dimensions in one transaction |
| Model | `sql/oltp_schema.sql`, `sql/dw_schema.sql` | Normalized (3NF) source vs. dimensional star schema: `fact_events`, `fact_sessions`, `fact_order_items`, `fact_requests`; `dim_date`, `dim_customer`, `dim_product` |
| Quality and monitoring | `eventflow/quality.py` | Row reconciliation (raw = valid + quarantined), quarantine and duplicate rates, freshness, referential integrity, hourly 5xx anomaly; every run and check is recorded in `ops.*` |
| Serve | `sql/marts.sql`, `eventflow/report.py` | Daily KPI, category revenue and hourly ops marts; static HTML dashboard |
| Tune | `eventflow/tune.py` | Before/after benchmark of report queries with indexes, BRIN and materialized views |
| Orchestrate | `scripts/run_pipeline.sh` | Bash orchestration with per-step timing, retries for transient errors, non-zero exit on failed data quality |

## Data model

- **Grain**: `fact_events` = one clickstream event; `fact_sessions` = one user session;
  `fact_order_items` = one order line; `fact_requests` = one HTTP request.
- **Dimensions** are denormalized on purpose (category name copied onto `dim_product`), so analysts join once per dimension.
- The **OLTP source** is normalized to 3NF: customers, categories, products, orders, order_items.

## Run it

Requirements: Python 3.10+, Java 17+ (for Spark), PostgreSQL 14+.

```bash
pip install -r requirements.txt
createdb eventflow
export EVENTFLOW_PG_DSN=postgresql://postgres:postgres@localhost:5432/eventflow
make small      # ~2% of full size, about a minute
make pipeline   # full size
make tune       # query tuning benchmark -> docs/query_tuning.md
make test
```

Land raw data in S3 instead of only on disk: `EVENTFLOW_STORAGE=s3 EVENTFLOW_S3_BUCKET=<bucket>` (uses your AWS credentials).

## Production version on AWS

S3 for the raw and curated zones, AWS Glue or EMR for the Spark job, Redshift for the warehouse
(`COPY` from S3), Step Functions or Amazon MWAA (Airflow) for orchestration, CloudWatch alarms on
failed runs and data quality checks, and Kinesis Data Firehose if logs had to be collected in real time.

## Limitations

- Data is synthetic, generated to look like a real store's traffic, with known problems injected.
- Runs as a single-machine Spark job (local mode); the same code runs on a cluster.
- `dim_customer` is a Type 1 dimension (history is overwritten); Type 2 history tracking is not implemented.
