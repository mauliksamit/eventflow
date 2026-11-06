from eventflow.config import Config
from eventflow.quality import audit, blocking

STATS = {
    "hours_present": 24, "parsed_rows": 1000, "rejected": 0, "valid_unique": 990, "reject_reasons": {},
    "events_by_type": {"PushEvent": 600, "WatchEvent": 390}, "unknown_event_types": [],
    "capture_ref": 0.01, "capture_collab": 0.01,
    "observed_fields": [{"event_type": "*", "field": "id", "share": 1.0}],
}
CFG = Config()


def checks(**over):
    return {c.name: c for c in audit({**STATS, **over}, 1000, CFG)}


def test_healthy_day_passes_everything():
    assert all(c.passed for c in checks().values())


def test_missing_hour_blocks_publication():
    assert not checks(hours_present=23)["source_hours_complete"].passed
    assert blocking(audit({**STATS, "hours_present": 23}, 1000, CFG))


def test_lost_rows_block_publication():
    # Spark read fewer lines than ingest counted, for example a truncated object in the lake.
    assert [c.name for c in blocking(audit({**STATS, "parsed_rows": 999}, 1000, CFG))] == ["rows_reconcile_to_manifest"]


def test_high_quarantine_rate_blocks_publication():
    assert not checks(rejected=2)["quarantine_rate"].passed


def test_source_degradation_warns_but_does_not_block():
    got = audit({**STATS, "events_by_type": {"WatchEvent": 990}, "capture_ref": 0.0}, 1000, CFG)
    assert {"ref_stream_present", "ref_capture_floor"} <= {c.name for c in got if not c.passed}
    assert not blocking(got)


def test_unknown_event_type_warns():
    assert not checks(unknown_event_types=["NewEvent"])["no_unknown_event_types"].passed


def test_schema_drift_is_only_flagged_against_a_baseline():
    fields = [{"event_type": "PushEvent", "field": "ref", "share": 1.0}]
    first = audit({**STATS, "observed_fields": fields}, 1000, CFG, set())
    assert next(c for c in first if c.name == "no_new_payload_fields").passed
    later = audit({**STATS, "observed_fields": fields}, 1000, CFG, {("PushEvent", "head")})
    assert next(c for c in later if c.name == "no_new_payload_fields").details == "PushEvent.ref"
