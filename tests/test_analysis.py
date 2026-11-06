import numpy as np
import pandas as pd

from eventflow.analysis import Scored, choose_threshold, drop_ratios, find_episodes, match, null_distribution, velocity

T0 = pd.Timestamp("2026-10-05", tz="UTC")


def minutes(rate_per_s, n=240, events=100):
    idx = pd.date_range(T0, periods=n, freq="1min")
    return pd.DataFrame({"events": events, "id_median": np.arange(n) * rate_per_s * 60.0}, index=idx)


def windows(values):
    return pd.Series(values, index=pd.date_range(T0, periods=len(values), freq="15min"), dtype=float)


def incident(iid="a", start="2026-10-05 12:00", end="2026-10-05 13:00"):
    return {"incident_id": iid, "started_at": pd.Timestamp(start, tz="UTC"), "resolved_at": pd.Timestamp(end, tz="UTC")}


def test_velocity_recovers_the_id_rate():
    v = velocity(minutes(500)).velocity.dropna()
    assert abs(v.median() - 500) < 1


def test_velocity_ignores_the_feed_going_quiet():
    # Few captured events say nothing about GitHub, only about the archive.
    m = minutes(500)
    m.loc[m.index[60:120], "events"] = 1
    v = velocity(m).velocity
    assert v[m.index[60]:m.index[119]].isna().all()


def test_velocity_ignores_a_capture_burst():
    m = minutes(500, events=20)
    m.loc[m.index[60:75], "events"] = 2000
    v = velocity(m).velocity
    assert np.isnan(v[m.index[60]])


def test_a_drop_does_not_lower_its_own_baseline():
    idx = pd.date_range(T0 - pd.Timedelta("21D"), periods=96 * 28, freq="15min")
    v = pd.Series(1000.0, index=idx)
    v.iloc[-4:] = 400.0
    out = drop_ratios(v)
    assert out.baseline.iloc[-1] > 900
    assert out.drop_ratio.iloc[-1] < 0.5


def test_episode_needs_two_consecutive_windows():
    assert find_episodes(windows([1, 0.5, 1, 0.5, 1]), 0.7).empty
    ep = find_episodes(windows([1, 0.5, 0.4, 1]), 0.7)
    assert len(ep) == 1 and ep.windows[0] == 2 and ep.min_ratio[0] == 0.4
    assert ep.started_at[0] == T0 + pd.Timedelta("15min")
    assert ep.ended_at[0] == T0 + pd.Timedelta("45min")


def test_match_allows_an_hour_before_the_posting():
    inc = pd.DataFrame([incident()])
    def ep(start, end):
        return pd.DataFrame([{"started_at": pd.Timestamp(start, tz="UTC"), "ended_at": pd.Timestamp(end, tz="UTC")}])
    assert match(ep("2026-10-05 10:45", "2026-10-05 11:15"), inc)[0] == "a"
    assert match(ep("2026-10-05 13:15", "2026-10-05 13:45"), inc)[0] == "a"
    assert match(ep("2026-10-05 09:00", "2026-10-05 10:00"), inc)[0] is None
    assert match(ep("2026-10-05 14:00", "2026-10-05 15:00"), inc)[0] is None


def test_a_threshold_that_flags_nothing_scores_zero():
    s = Scored(0.5, 0, 0, 10, 0)
    assert (s.precision, s.recall, s.f1) == (0.0, 0.0, 0.0)


def test_threshold_ties_go_to_the_stricter_value():
    r = windows([1.0] * 40 + [0.4, 0.4] + [1.0] * 40)
    inc = pd.DataFrame([incident("a", "2026-10-05 10:00", "2026-10-05 11:00")])
    assert choose_threshold(r, inc) == 0.60


def test_null_is_seeded_and_finds_what_chance_finds():
    ep = pd.DataFrame([{"started_at": T0 + pd.Timedelta(hours=h), "ended_at": T0 + pd.Timedelta(hours=h, minutes=30)}
                       for h in range(0, 240, 24)])
    inc = pd.DataFrame([incident()])
    a = null_distribution(ep, inc, T0, pd.Timedelta("20D"))
    b = null_distribution(ep, inc, T0, pd.Timedelta("20D"))
    assert (a == b).all() and a.min() >= 0 and a.max() <= len(ep)
