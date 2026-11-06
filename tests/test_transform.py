import json
from datetime import date

from pyspark.sql import Row

from eventflow.transform import feed_hourly, feed_minute, parse_events, repo_daily, valid_events

DAY = date(2026, 10, 1)


def ev(eid="100", etype="WatchEvent", ts="2026-10-01T10:00:00Z", login="octo", repo="a/b", action="started", **over):
    doc = {"id": eid, "type": etype, "actor": {"id": 7, "login": login}, "repo": {"id": 9, "name": repo},
           "payload": {"action": action}, "public": True, "created_at": ts}
    doc.update(over)
    return json.dumps(doc)


def parse(spark, values):
    return parse_events(spark.createDataFrame([Row(value=v) for v in values]), DAY)


def reasons(spark, values):
    return sorted(r.reject_reason for r in parse(spark, values).filter("reject_reason IS NOT NULL").collect())


def test_valid_event_is_not_rejected(spark):
    assert reasons(spark, [ev()]) == []


def test_every_bad_shape_gets_a_specific_reason(spark):
    no_repo_name = {**json.loads(ev()), "repo": {"id": 9}}
    bad = ['{"id": "1", "tru', ev(eid="abc"), ev(actor=None), json.dumps(no_repo_name),
           ev(ts="not-a-date"), ev(ts="2026-10-02T00:00:00Z")]
    assert reasons(spark, bad) == sorted(["unparseable_json", "bad_event_id", "missing_actor", "missing_repo",
                                          "bad_timestamp", "outside_partition"])


def test_missing_event_type_is_rejected_not_passed_through(spark):
    # `NULL NOT IN (...)` is NULL, not true, so this needs an explicit null check.
    doc = json.loads(ev())
    del doc["type"]
    assert reasons(spark, [json.dumps(doc)]) == ["missing_event_type"]


def test_unknown_event_type_is_kept_so_new_github_events_are_not_lost(spark):
    assert reasons(spark, [ev(etype="BrandNewEvent")]) == []


def test_nothing_is_dropped_by_parsing(spark):
    assert parse(spark, [ev(), '{"broken', "", ev(eid="abc")]).count() == 4


def test_duplicate_ids_collapse_to_one_event(spark):
    assert valid_events(parse(spark, [ev(), ev(), ev(eid="101")])).count() == 2


def test_timezone_offsets_are_normalized_before_partition_check(spark):
    # 23:30 at -06:00 on Sep 30 is 05:30 UTC on Oct 1, inside the partition.
    assert reasons(spark, [ev(ts="2026-09-30T23:30:00-06:00")]) == []


def test_actors_are_classified(spark):
    rows = [ev(eid="1", login="octo"), ev(eid="2", login="dependabot[bot]"), ev(eid="3", login="Copilot"),
            ev(eid="4", login="cursor[bot]"), ev(eid="5", login="robot")]
    got = {r.event_id: r.actor_class for r in valid_events(parse(spark, rows)).collect()}
    assert got == {1: "human", 2: "bot", 3: "ai_agent", 4: "ai_agent", 5: "human"}


def test_event_types_map_to_their_id_sequence(spark):
    rows = [ev(eid="1", etype="PushEvent"), ev(eid="2", etype="CreateEvent"), ev(eid="3", etype="WatchEvent")]
    got = {r.event_id: r.id_series for r in valid_events(parse(spark, rows)).collect()}
    assert got == {1: "ref", 2: "ref", 3: "collab"}


def test_capture_rate_uses_id_span_not_event_count(spark):
    # 101 events spread evenly over ids 0..1,000,000 means about 0.01% of events were captured.
    rows = [ev(eid=str(i * 10_000), ts=f"2026-10-01T10:{i % 60:02d}:00Z") for i in range(101)]
    out = feed_hourly(valid_events(parse(spark, rows))).collect()[0]
    assert out.events == 101
    assert abs(out.est_total - 1_000_000) / 1_000_000 < 0.03
    assert 0.00009 < out.capture_rate < 0.00011


def test_capture_is_measured_per_id_sequence(spark):
    # Ids 6 billion apart must not read as one enormous span.
    rows = [ev(eid="1000", etype="WatchEvent"), ev(eid="2000", etype="WatchEvent"),
            ev(eid="6000001000", etype="PushEvent"), ev(eid="6000002000", etype="PushEvent")]
    out = {r.id_series: r for r in feed_hourly(valid_events(parse(spark, rows))).collect()}
    assert out["ref"].est_total < 2000 and out["collab"].est_total < 2000


def test_feed_minute_median_ignores_a_stray_id(spark):
    rows = [ev(eid=str(i), ts="2026-10-01T10:00:30Z") for i in (10, 11, 12, 13, 9_000_000)]
    out = feed_minute(valid_events(parse(spark, rows))).collect()[0]
    assert out.id_median in (11, 12, 13)


def test_repo_daily_counts_and_latest_name(spark):
    rows = [ev(eid="1", repo="old/name", ts="2026-10-01T09:00:00Z"),
            ev(eid="2", repo="new/name", ts="2026-10-01T11:00:00Z"),
            ev(eid="3", etype="PullRequestEvent", action="opened"),
            ev(eid="4", etype="PullRequestEvent", action="closed")]
    out = repo_daily(valid_events(parse(spark, rows))).collect()[0]
    assert (out.stars, out.prs_opened, out.events, out.repo_name) == (2, 1, 4, "new/name")
