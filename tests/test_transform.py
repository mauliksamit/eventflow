import json
from datetime import datetime

from pyspark.sql import Row

from eventflow.transform import dedupe_events, latest_order_lines, parse_access_logs, parse_clickstream, sessionize


def ev(eid, user, ts, etype="page_view", pid=1):
    return json.dumps({"event_id": eid, "user_id": user, "event_type": etype, "product_id": pid,
                       "device": "web", "app_version": "5.4.0", "ts": ts})


def lines(spark, values):
    return spark.createDataFrame([Row(value=v) for v in values])


def test_parse_clickstream_routes_bad_records_to_quarantine(spark):
    good = ev("e1", 7, "2026-10-01T10:00:00.000-06:00")
    missing_user = json.dumps({"event_id": "e2", "event_type": "page_view", "ts": "2026-10-01T10:00:00.000Z"})
    bad_type = ev("e3", 7, "2026-10-01T10:00:00.000Z", etype="pageview_v2")
    truncated = good[:25]
    valid, quarantine = parse_clickstream(lines(spark, [good, missing_user, bad_type, truncated]))
    assert [r.event_id for r in valid.collect()] == ["e1"]
    reasons = sorted(r.reject_reason for r in quarantine.collect())
    assert reasons == ["invalid_event_type", "missing_user_id", "unparseable_json"]


def test_timestamps_are_normalized_to_utc(spark):
    # 23:30 in UTC-6 on Oct 1 is 05:30 UTC on Oct 2, so the event belongs to the Oct 2 partition.
    valid, _ = parse_clickstream(lines(spark, [ev("e1", 1, "2026-10-01T23:30:00.000-06:00")]))
    # Check inside Spark (session time zone is UTC); collect() would convert to the machine's local zone.
    row = valid.selectExpr("event_date", "hour(event_ts) AS h").collect()[0]
    assert str(row.event_date) == "2026-10-02"
    assert row.h == 5


def test_dedupe_keeps_one_copy_per_event_id(spark):
    raw = [ev("e1", 1, "2026-10-01T10:00:00.000Z")] * 3 + [ev("e2", 1, "2026-10-01T10:01:00.000Z")]
    valid, _ = parse_clickstream(lines(spark, raw))
    assert dedupe_events(valid).count() == 2


def test_sessionize_splits_after_30_minutes_of_inactivity(spark):
    raw = [ev("a", 1, "2026-10-01T10:00:00.000Z"), ev("b", 1, "2026-10-01T10:20:00.000Z"),
           ev("c", 1, "2026-10-01T10:51:00.000Z"),   # 31 min gap -> new session
           ev("d", 2, "2026-10-01T10:05:00.000Z")]
    valid, _ = parse_clickstream(lines(spark, raw))
    rows = {r.event_id: r.session_id for r in sessionize(valid).collect()}
    assert rows["a"] == rows["b"] != rows["c"]
    assert rows["d"] not in (rows["a"], rows["c"])


def test_parse_access_logs(spark):
    ok = '10.0.0.1 - - [10/Oct/2026:15:04:05 +0000] "POST /checkout HTTP/1.1" 503 312 0.912 "okhttp/4.12"'
    product = '10.0.0.2 - - [10/Oct/2026:15:04:06 +0000] "GET /p/1234 HTTP/1.1" 200 5120 0.045 "curl/8.5"'
    garbled = "\x00\x17 garbled 10.0.0.3 - - ["
    valid, quarantine = parse_access_logs(lines(spark, [ok, product, garbled]))
    rows = sorted(valid.collect(), key=lambda r: r.request_ts)
    assert rows[0].status_code == 503 and rows[0].path_group == "/checkout"
    assert abs(rows[0].latency_ms - 912.0) < 0.01
    assert rows[1].path_group == "/p/:id"
    assert quarantine.count() == 1


def test_latest_order_lines_keeps_newest_status(spark):
    df = spark.createDataFrame([
        Row(order_id=1, line_no=1, status="placed", created_at=datetime(2026, 10, 1, 9), updated_at=datetime(2026, 10, 1, 9)),
        Row(order_id=1, line_no=1, status="shipped", created_at=datetime(2026, 10, 1, 9), updated_at=datetime(2026, 10, 2, 9)),
    ])
    out = latest_order_lines(df).collect()
    assert len(out) == 1 and out[0].status == "shipped"
