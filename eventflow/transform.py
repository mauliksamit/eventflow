"""PySpark transformations: raw zone -> curated Parquet (the data lake) + quarantine.

Each step is a pure function on DataFrames so it can be unit tested on tiny inputs.
Writes use dynamic partition overwrite, which makes re-running a day idempotent:
the same input always produces the same output partitions, with no duplicates.
"""
from __future__ import annotations

import json
import time

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import Config, get_config

VALID_EVENT_TYPES = ["page_view", "search", "add_to_cart", "checkout"]
SESSION_GAP_SECONDS = 30 * 60

CLICK_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("user_id", T.LongType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("product_id", T.LongType()),
    T.StructField("device", T.StringType()),
    T.StructField("app_version", T.StringType()),
    T.StructField("ts", T.StringType()),
])

ACCESS_RE = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) [^"]*" (\d{3}) (\d+) ([\d.]+) "([^"]*)"$'


def spark_session(app: str = "eventflow") -> SparkSession:
    return (SparkSession.builder.appName(app).master("local[*]")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


def parse_clickstream(lines: DataFrame) -> tuple[DataFrame, DataFrame]:
    """lines: DataFrame with a single string column `value`. Returns (valid, quarantine)."""
    parsed = lines.select(F.col("value").alias("raw"), F.from_json("value", CLICK_SCHEMA).alias("j"))
    parsed = parsed.select("raw", "j.*").withColumn(
        "event_ts", F.to_timestamp("ts", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
    reason = (F.when(F.col("event_id").isNull(), "unparseable_json")
              .when(F.col("user_id").isNull(), "missing_user_id")
              .when(~F.col("event_type").isin(VALID_EVENT_TYPES), "invalid_event_type")
              .when(F.col("event_ts").isNull(), "bad_timestamp"))
    tagged = parsed.withColumn("reject_reason", reason)
    valid = (tagged.filter(F.col("reject_reason").isNull())
             .select("event_id", "user_id", "event_type", "product_id", "device", "app_version", "event_ts")
             .withColumn("event_date", F.to_date("event_ts")))
    quarantine = tagged.filter(F.col("reject_reason").isNotNull()).select("raw", "reject_reason")
    return valid, quarantine


def dedupe_events(df: DataFrame) -> DataFrame:
    """At-least-once delivery means the same event_id can arrive twice; keep one copy."""
    return df.dropDuplicates(["event_id"])


def sessionize(df: DataFrame, gap_seconds: int = SESSION_GAP_SECONDS) -> DataFrame:
    """Assign session_id: a new session starts after `gap_seconds` of user inactivity."""
    w = Window.partitionBy("user_id").orderBy("event_ts", "event_id")
    prev = F.lag("event_ts").over(w)
    gap = F.col("event_ts").cast("long") - prev.cast("long")
    is_new = F.when(prev.isNull() | (gap > gap_seconds), 1).otherwise(0)
    return (df.withColumn("_new", is_new)
            .withColumn("_seq", F.sum("_new").over(w.rowsBetween(Window.unboundedPreceding, 0)))
            .withColumn("session_id", F.concat_ws("-", F.lit("s"), F.col("user_id").cast("string"), F.col("_seq").cast("string")))
            .drop("_new", "_seq"))


def build_sessions(events: DataFrame) -> DataFrame:
    return (events.groupBy("session_id", "user_id")
            .agg(F.min("event_ts").alias("start_ts"), F.max("event_ts").alias("end_ts"),
                 F.count("*").alias("n_events"),
                 F.max(F.when(F.col("event_type") == "checkout", 1).otherwise(0)).cast("boolean").alias("converted"))
            .withColumn("event_date", F.to_date("start_ts")))


def parse_access_logs(lines: DataFrame) -> tuple[DataFrame, DataFrame]:
    g = lambda i: F.regexp_extract("value", ACCESS_RE, i)  # noqa: E731
    parsed = lines.select(
        F.col("value").alias("raw"),
        F.to_timestamp(g(2), "dd/MMM/yyyy:HH:mm:ss Z").alias("request_ts"),
        g(3).alias("method"), g(4).alias("path"),
        g(5).cast("int").alias("status_code"), g(6).cast("int").alias("bytes_sent"),
        (g(7).cast("double") * 1000).cast("float").alias("latency_ms"))
    ok = F.col("request_ts").isNotNull() & F.col("status_code").isNotNull()
    valid = (parsed.filter(ok)
             .withColumn("path_group", F.regexp_replace("path", r"^/p/\d+$", "/p/:id"))
             .withColumn("event_date", F.to_date("request_ts"))
             .drop("raw", "path"))
    quarantine = parsed.filter(~ok).select("raw", F.lit("unparseable_log_line").alias("reject_reason"))
    return valid, quarantine


def latest_order_lines(orders: DataFrame) -> DataFrame:
    """An order can be extracted several times as its status changes; keep the newest version."""
    w = Window.partitionBy("order_id", "line_no").orderBy(F.col("updated_at").desc())
    return (orders.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
            .withColumn("order_date", F.to_date("created_at")))


def run(cfg: Config | None = None, spark: SparkSession | None = None) -> dict:
    cfg = cfg or get_config()
    spark = spark or spark_session()
    t0 = time.time()
    stats: dict = {}

    lines = spark.read.text(str(cfg.raw / "clickstream"))
    stats["clickstream_raw_lines"] = lines.count()
    valid, bad = parse_clickstream(lines.select("value"))
    valid = valid.cache()
    stats["clickstream_valid"] = valid.count()
    deduped = dedupe_events(valid)
    events = sessionize(deduped).cache()
    stats["clickstream_deduped"] = events.count()
    stats["duplicates_removed"] = stats["clickstream_valid"] - stats["clickstream_deduped"]
    events.write.mode("overwrite").partitionBy("event_date").parquet(str(cfg.curated / "events"))
    sessions = build_sessions(events)
    sessions.write.mode("overwrite").partitionBy("event_date").parquet(str(cfg.curated / "sessions"))
    stats["sessions"] = spark.read.parquet(str(cfg.curated / "sessions")).count()
    bad.write.mode("overwrite").json(str(cfg.quarantine / "clickstream"))
    stats["clickstream_quarantined"] = spark.read.json(str(cfg.quarantine / "clickstream")).count()

    logs = spark.read.text(str(cfg.raw / "access_logs"))
    stats["access_raw_lines"] = logs.count()
    lvalid, lbad = parse_access_logs(logs.select("value"))
    lvalid.write.mode("overwrite").partitionBy("event_date").parquet(str(cfg.curated / "requests"))
    stats["access_valid"] = spark.read.parquet(str(cfg.curated / "requests")).count()
    lbad.write.mode("overwrite").json(str(cfg.quarantine / "access_logs"))
    stats["access_quarantined"] = stats["access_raw_lines"] - stats["access_valid"]

    orders = (spark.read.option("header", True).option("inferSchema", True).csv(str(cfg.raw / "orders"))
              .withColumn("created_at", F.to_timestamp("created_at"))
              .withColumn("updated_at", F.to_timestamp("updated_at")))
    orders = latest_order_lines(orders)
    orders.write.mode("overwrite").parquet(str(cfg.curated / "orders"))
    stats["order_lines"] = spark.read.parquet(str(cfg.curated / "orders")).count()

    stats["transform_seconds"] = round(time.time() - t0, 1)
    cfg.runs.mkdir(parents=True, exist_ok=True)
    (cfg.runs / "transform_stats.json").write_text(json.dumps(stats, indent=2))
    return stats


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
