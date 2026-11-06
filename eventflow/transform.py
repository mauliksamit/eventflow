"""PySpark: one day of raw GH Archive JSON -> curated Parquet and quarantine.

Every function takes and returns DataFrames so it can be unit tested on a few rows. run_day only
writes to a new run-scoped location; nothing is visible to the warehouse until the audit passes
and quality.publish() records that location in ops.published_partitions.
"""
from __future__ import annotations

import time
from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import Config
from .lake import Lake

# GitHub numbers these event families from two unrelated id sequences. The archive can lose one
# family entirely, so capture has to be measured per sequence.
REF_EVENTS = ["PushEvent", "CreateEvent", "DeleteEvent"]
KNOWN_EVENTS = REF_EVENTS + [
    "CommitCommentEvent", "DiscussionEvent", "ForkEvent", "GollumEvent", "IssueCommentEvent", "IssuesEvent",
    "MemberEvent", "PublicEvent", "PullRequestEvent", "PullRequestReviewCommentEvent", "PullRequestReviewEvent",
    "ReleaseEvent", "SponsorshipEvent", "WatchEvent",
]
TOP_LEVEL_FIELDS = ["actor", "created_at", "id", "org", "payload", "public", "repo", "type"]

# Lower bound: an agent running under a person's account is counted as human.
AI_AGENTS = [
    "amazon-q-developer[bot]", "chatgpt-codex-connector[bot]", "claude[bot]", "coderabbitai[bot]", "copilot",
    "copilot-pull-request-reviewer[bot]", "copilot-swe-agent[bot]", "cursor[bot]", "cursoragent",
    "devin-ai-integration[bot]", "gemini-code-assist[bot]", "google-labs-jules[bot]", "greptile-apps[bot]",
    "qodo-merge-pro[bot]", "sourcery-ai[bot]",
]
BOT_PATTERN = r"(?i)(\[bot\]$|-bot$|^github-actions$|^dependabot$|^renovate$)"

EVENT_SCHEMA = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("type", T.StringType()),
    T.StructField("actor", T.StructType([T.StructField("id", T.LongType()), T.StructField("login", T.StringType())])),
    T.StructField("repo", T.StructType([T.StructField("id", T.LongType()), T.StructField("name", T.StringType())])),
    T.StructField("public", T.BooleanType()),
    T.StructField("created_at", T.StringType()),
    T.StructField("payload", T.StructType([T.StructField("action", T.StringType())])),
])


def spark_session(cfg: Config, app: str = "eventflow") -> SparkSession:
    b = (SparkSession.builder.appName(app).master(cfg.spark_master)
         .config("spark.driver.memory", cfg.spark_driver_memory)
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")  # bad timestamps become NULL, not a crash
         .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
         .config("spark.sql.files.maxPartitionBytes", str(128 << 20))
         .config("spark.ui.enabled", "false"))
    if cfg.lake.startswith("s3://"):
        b = (b.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.maximum", "64")
             .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                     "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"))
        if cfg.s3_endpoint:
            b = (b.config("spark.hadoop.fs.s3a.endpoint", cfg.s3_endpoint)
                 .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(cfg.s3_endpoint.startswith("https")).lower()))
    return b.getOrCreate()


def actor_class(login: Column) -> Column:
    return (F.when(F.lower(login).isin(AI_AGENTS), "ai_agent")
            .when(login.rlike(BOT_PATTERN), "bot")
            .otherwise("human"))


def parse_events(lines: DataFrame, day: date, sample_fraction: float = 0.01) -> DataFrame:
    """Parse JSON lines (`value`) and tag every row, valid or not, with reject_reason.

    Nothing is dropped here, so valid + rejected always accounts for every input line.
    """
    p = lines.withColumn("j", F.from_json("value", EVENT_SCHEMA)).select("value", "j.*")
    p = (p.withColumn("event_id", F.col("id").cast("long"))
          .withColumn("created_ts", F.to_timestamp("created_at", "yyyy-MM-dd'T'HH:mm:ssXXX"))
          .withColumn("actor_id", F.col("actor.id")).withColumn("actor_login", F.col("actor.login"))
          .withColumn("repo_id", F.col("repo.id")).withColumn("repo_name", F.col("repo.name"))
          .withColumn("action", F.col("payload.action")))
    reason = (F.when(F.col("id").isNull() & F.col("type").isNull(), "unparseable_json")
              .when(F.col("event_id").isNull(), "bad_event_id")
              .when(F.col("type").isNull(), "missing_event_type")
              .when(F.col("actor_id").isNull() | F.col("actor_login").isNull(), "missing_actor")
              .when(F.col("repo_id").isNull() | F.col("repo_name").isNull(), "missing_repo")
              .when(F.col("created_ts").isNull(), "bad_timestamp")
              .when(F.to_date("created_ts") != F.lit(str(day)).cast("date"), "outside_partition"))
    return (p.withColumn("reject_reason", reason)
            .withColumn("raw", F.when(F.col("reject_reason").isNotNull(), F.col("value")))
            .withColumn("sample", F.when(F.rand(7) < sample_fraction, F.col("value")))
            .select("event_id", F.col("type").alias("event_type"), "actor_id", "actor_login", "repo_id", "repo_name",
                    "action", "created_ts", "reject_reason", "raw", "sample"))


def valid_events(tagged: DataFrame) -> DataFrame:
    """Valid rows with one copy per event id, plus the derived columns everything else uses."""
    return (tagged.filter(F.col("reject_reason").isNull())
            .drop("reject_reason", "raw", "sample")
            .dropDuplicates(["event_id"])
            .withColumn("hour_ts", F.date_trunc("hour", "created_ts"))
            .withColumn("actor_class", actor_class(F.col("actor_login")))
            .withColumn("id_series", F.when(F.col("event_type").isin(REF_EVENTS), "ref").otherwise("collab")))


def feed_hourly(events: DataFrame) -> DataFrame:
    """Capture rate per hour and id sequence.

    Ids advance for every event GitHub records, seen or not, so the span between the 1st and 99th
    percentile estimates true volume regardless of how little of it the archive caught.
    """
    q = F.percentile_approx("event_id", [0.01, 0.99], 100000)
    return (events.groupBy("hour_ts", "id_series").agg(F.count("*").alias("events"), q.alias("q"))
            .withColumn("est_total", (F.col("q")[1] - F.col("q")[0]) / 0.98)
            .withColumn("capture_rate", F.when(F.col("est_total") > 0, F.col("events") / F.col("est_total")))
            .drop("q"))


def feed_minute(events: DataFrame) -> DataFrame:
    """Median event id per minute. The minute-to-minute difference is GitHub's real event throughput."""
    return (events.withColumn("minute_ts", F.date_trunc("minute", "created_ts"))
            .groupBy("minute_ts", "id_series")
            .agg(F.count("*").alias("events"), F.percentile_approx("event_id", 0.5, 10000).alias("id_median")))


def type_hourly(events: DataFrame) -> DataFrame:
    return events.groupBy("hour_ts", "event_type", "actor_class").agg(F.count("*").alias("events"))


def repo_daily(events: DataFrame) -> DataFrame:
    def n(cond: Column) -> Column:
        return F.sum(F.when(cond, 1).otherwise(0))
    t, a = F.col("event_type"), F.col("action")
    return events.groupBy("repo_id").agg(
        F.max_by("repo_name", "created_ts").alias("repo_name"),
        F.count("*").alias("events"),
        n(t == "WatchEvent").alias("stars"), n(t == "ForkEvent").alias("forks"), n(t == "PushEvent").alias("pushes"),
        n((t == "PullRequestEvent") & (a == "opened")).alias("prs_opened"),
        n((t == "IssuesEvent") & (a == "opened")).alias("issues_opened"),
        n(t == "ReleaseEvent").alias("releases"),
        F.countDistinct("actor_id").alias("actors"))


def actor_daily(events: DataFrame) -> DataFrame:
    return events.groupBy("actor_id").agg(
        F.max_by("actor_login", "created_ts").alias("actor_login"),
        F.max_by("actor_class", "created_ts").alias("actor_class"),
        F.min("created_ts").alias("first_seen"), F.max("created_ts").alias("last_seen"),
        F.count("*").alias("events"))


def stars(events: DataFrame) -> DataFrame:
    return (events.filter(F.col("event_type") == "WatchEvent")
            .select("event_id", "repo_id", "actor_id", "created_ts", "hour_ts"))


def observed_fields(tagged: DataFrame) -> DataFrame:
    """Field names seen on a sample of raw events, for schema drift detection."""
    sample = tagged.filter(F.col("sample").isNotNull()).select("event_type", "sample")
    top = sample.select(F.lit("*").alias("event_type"), F.explode(F.json_object_keys("sample")).alias("field"))
    payload = sample.select("event_type", F.explode(F.json_object_keys(F.get_json_object("sample", "$.payload"))).alias("field"))
    total = (sample.groupBy("event_type").count()
             .unionByName(sample.select(F.lit("*").alias("event_type")).groupBy("event_type").count()))
    return (top.unionByName(payload).groupBy("event_type", "field").count().withColumnRenamed("count", "seen")
            .join(total, "event_type").withColumn("share", F.col("seen") / F.col("count")).drop("seen", "count"))


OUTPUTS = {"repo_daily": repo_daily, "actor_daily": actor_daily, "stars": stars, "feed_hourly": feed_hourly,
           "feed_minute": feed_minute, "type_hourly": type_hourly}


def run_day(cfg: Config, spark: SparkSession, day: date, run_id: str) -> dict:
    """Write one day of curated output under a run-scoped path and return its stats."""
    lake, t0 = Lake(cfg), time.time()
    base = f"dt={day}/run={run_id}"
    tagged_uri, events_uri = lake.uri(f"_work/{base}/tagged"), lake.uri(f"curated/events/{base}")

    lines = spark.read.option("basePath", lake.uri(f"raw/gharchive/dt={day}")).text(lake.uri(f"raw/gharchive/dt={day}"))
    hours = lines.select("hr").distinct().count()
    parse_events(lines.select("value"), day).write.mode("overwrite").parquet(tagged_uri)
    tagged = spark.read.parquet(tagged_uri)

    valid_events(tagged).write.mode("overwrite").parquet(events_uri)
    events = spark.read.parquet(events_uri)
    for name, fn in OUTPUTS.items():
        fn(events).coalesce(4).write.mode("overwrite").parquet(lake.uri(f"curated/{name}/{base}"))

    bad = tagged.filter(F.col("reject_reason").isNotNull())
    bad.select("raw", "reject_reason").coalesce(1).write.mode("overwrite").json(lake.uri(f"quarantine/{base}"))

    counts = tagged.agg(F.count("*").alias("rows"), F.count("reject_reason").alias("rejected")).first()
    by_type = {r["event_type"]: r["count"] for r in events.groupBy("event_type").count().collect()}
    series = {r["id_series"]: r for r in spark.read.parquet(lake.uri(f"curated/feed_hourly/{base}")).groupBy("id_series")
              .agg(F.sum("events").alias("events"), F.sum("est_total").alias("est_total")).collect()}
    stats = {
        "dt": str(day), "run_id": run_id, "hours_present": hours, "parsed_rows": counts["rows"],
        "rejected": counts["rejected"], "valid_unique": sum(by_type.values()),
        "reject_reasons": {r["reject_reason"]: r["count"] for r in bad.groupBy("reject_reason").count().collect()},
        "events_by_type": by_type,
        "unknown_event_types": sorted(set(by_type) - set(KNOWN_EVENTS)),
        "capture_ref": series["ref"]["events"] / series["ref"]["est_total"] if "ref" in series else 0.0,
        "capture_collab": series["collab"]["events"] / series["collab"]["est_total"] if "collab" in series else 0.0,
        "observed_fields": [r.asDict() for r in observed_fields(tagged).collect()],
    }
    stats["duplicates_removed"] = stats["parsed_rows"] - stats["rejected"] - stats["valid_unique"]
    stats["transform_seconds"] = round(time.time() - t0, 1)
    lake.delete_prefix(f"_work/dt={day}/")
    return stats
