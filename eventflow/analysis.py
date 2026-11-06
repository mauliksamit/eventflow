"""Can GitHub's own event ids reveal an incident? Scored against GitHub's published incidents.

Ids advance for every event GitHub records, whether or not the archive caught it, so the id rate
measures the platform rather than the feed. The detector compares each 15 minute rate to a
trailing level, after removing the weekly cycle, and flags sustained drops. The threshold is
chosen on one period and scored on a later one, and a shifted baseline shows what chance alone
would match.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass

import numpy as np
import pandas as pd
import psycopg

from .config import Config

log = logging.getLogger("eventflow.analysis")

WINDOW = pd.Timedelta("15min")
MIN_WINDOW_EVENTS = 30     # too few ids to place the median
MAX_CAPTURE_JUMP = 5.0     # capture moving this much between windows is the feed, not GitHub
TRAILING = "6h"            # long enough to ride out a drop, short enough to follow the day
MIN_WINDOWS = 2
MATCH_BEFORE = pd.Timedelta("60min")
MATCH_AFTER = pd.Timedelta("30min")
THRESHOLDS = (0.60, 0.65, 0.70, 0.75, 0.80, 0.85)
TRAIN_END = pd.Timestamp("2026-10-01", tz="UTC")
PLATFORM = {"Git Operations", "Issues", "Pull Requests", "API Requests", "Webhooks"}
NULL_DRAWS = 2000
MIN_DAYS = 14              # the weekly cycle cannot be estimated from less
MIN_DAILY_WINDOWS = 48     # half a day of usable windows, or the daily median is a few stray hours


def velocity(minutes: pd.DataFrame) -> pd.DataFrame:
    """15 minute id rate for one sequence, from (minute_ts index, events, id_median)."""
    m = minutes[minutes.events >= 3]
    median = m.id_median.resample(WINDOW).median()
    events = m.events.resample(WINDOW).sum()
    v = (median - median.shift(2)) / (2 * WINDOW.total_seconds())
    jump = (events / events.shift(1)).clip(lower=1e-9)
    jump = np.maximum(jump, 1 / jump)
    usable = (events >= MIN_WINDOW_EVENTS) & (events.shift(2) >= MIN_WINDOW_EVENTS) & (v > 0)
    usable &= (jump < MAX_CAPTURE_JUMP) & (jump.shift(1) < MAX_CAPTURE_JUMP)
    return pd.DataFrame({"velocity": v.where(usable), "events": events})


def drop_ratios(v: pd.Series) -> pd.DataFrame:
    """Each window against its trailing level, with the hour-of-week cycle divided out."""
    t = v.index
    how = t.day_of_week * 96 + t.hour * 4 + t.minute // 15
    weekly = v.rolling("7D", center=True, min_periods=48).median()
    cycle = (v / weekly).groupby(how).transform("median")
    adjusted = v / cycle
    # closed="left" keeps the current window out of its own baseline.
    level = adjusted.rolling(TRAILING, closed="left", min_periods=8).median()
    return pd.DataFrame({"velocity": v, "baseline": level * cycle, "drop_ratio": adjusted / level})


def find_episodes(ratios: pd.Series, threshold: float) -> pd.DataFrame:
    """Runs of at least MIN_WINDOWS consecutive windows below threshold."""
    low = ratios < threshold
    run = (low != low.shift()).cumsum()
    rows = []
    for _, w in ratios[low].groupby(run[low]):
        if len(w) >= MIN_WINDOWS:
            rows.append({"started_at": w.index[0], "ended_at": w.index[-1] + WINDOW, "windows": len(w),
                         "min_ratio": float(w.min())})
    return pd.DataFrame(rows, columns=["started_at", "ended_at", "windows", "min_ratio"])


def overlaps(start, end, incident) -> bool:
    """An episode may begin up to an hour before the posting, since posting lags the failure."""
    return start <= incident.resolved_at + MATCH_AFTER and end >= incident.started_at - MATCH_BEFORE


def match(episodes: pd.DataFrame, incidents: pd.DataFrame) -> pd.Series:
    """The first incident each episode overlaps, or None."""
    ordered = list(incidents.sort_values("started_at").itertuples())
    return pd.Series([next((i.incident_id for i in ordered if overlaps(e.started_at, e.ended_at, i)), None)
                      for e in episodes.itertuples()], index=episodes.index, dtype=object)


def null_distribution(episodes: pd.DataFrame, incidents: pd.DataFrame, start: pd.Timestamp, span: pd.Timedelta,
                      seed: int = 7) -> np.ndarray:
    """Matches expected by chance: slide all episodes by one offset, wrapping around the period."""
    rng = np.random.default_rng(seed)
    draws = []
    for _ in range(NULL_DRAWS):
        offset = pd.Timedelta(hours=float(rng.uniform(24, span.total_seconds() / 3600 - 24)))
        slid = episodes.copy()
        length = episodes.ended_at - episodes.started_at
        slid["started_at"] = start + (episodes.started_at - start + offset) % span
        slid["ended_at"] = slid.started_at + length
        draws.append(int(match(slid, incidents).notna().sum()))
    return np.array(draws)


@dataclass
class Scored:
    threshold: float
    episodes: int
    matched: int
    incidents: int
    recalled: int

    @property
    def precision(self) -> float:
        return self.matched / self.episodes if self.episodes else 0.0

    @property
    def recall(self) -> float:
        return self.recalled / self.incidents if self.incidents else 0.0

    @property
    def f1(self) -> float:
        p, r = self.precision, self.recall
        return 2 * p * r / (p + r) if p + r else 0.0

    def row(self) -> dict:
        return {**vars(self), "precision": round(self.precision, 3), "recall": round(self.recall, 3)}


def score(ratios: pd.Series, incidents: pd.DataFrame, threshold: float) -> Scored:
    ep = find_episodes(ratios, threshold)
    matched = int(match(ep, incidents).notna().sum()) if len(ep) else 0
    recalled = sum(any(overlaps(e.started_at, e.ended_at, i) for e in ep.itertuples()) for i in incidents.itertuples())
    return Scored(threshold, len(ep), matched, len(incidents), recalled)


def choose_threshold(ratios: pd.Series, incidents: pd.DataFrame) -> float:
    """Best F1 on the training period; ties go to the stricter threshold."""
    return max((score(ratios, incidents, t) for t in THRESHOLDS), key=lambda s: (s.f1, -s.threshold)).threshold


def weekday_rate(v: pd.Series) -> pd.Series:
    """Median id rate per weekday, leaving out days with too little coverage to trust."""
    weekdays = v[v.index.day_of_week < 5].dropna()
    days = weekdays.resample("D")
    return days.median().where(days.count() >= MIN_DAILY_WINDOWS).dropna()


def weekday_growth(v: pd.Series) -> dict:
    """Change in the weekday id rate, first ten usable weekdays against the last ten."""
    daily = weekday_rate(v)
    head, tail = daily.iloc[:10].median(), daily.iloc[-10:].median()
    return {"first_ids_per_s": round(float(head)), "last_ids_per_s": round(float(tail)),
            "first_days": [str(daily.index[0].date()), str(daily.index[9].date())],
            "last_days": [str(daily.index[-10].date()), str(daily.index[-1].date())],
            "change": round(float(tail / head - 1), 3)}


def load_inputs(conn) -> tuple[dict[str, pd.DataFrame], pd.DataFrame]:
    conn.execute("SET TIME ZONE 'UTC'")
    with conn.cursor() as cur:
        cur.execute("SELECT minute_ts, id_series, events, id_median FROM dw.fact_feed_minute ORDER BY 1")
        minutes = pd.DataFrame(cur.fetchall(), columns=["minute_ts", "id_series", "events", "id_median"])
        cur.execute("""SELECT incident_id, name, impact, started_at, coalesce(resolved_at, started_at), components
                       FROM dw.dim_incident ORDER BY started_at""")
        incidents = pd.DataFrame(cur.fetchall(), columns=["incident_id", "name", "impact", "started_at", "resolved_at",
                                                          "components"])
    minutes["minute_ts"] = pd.to_datetime(minutes.minute_ts, utc=True)
    minutes["id_median"] = minutes.id_median.astype("float64")
    for c in ("started_at", "resolved_at"):
        incidents[c] = pd.to_datetime(incidents[c], utc=True)
    return {s: g.set_index("minute_ts").sort_index() for s, g in minutes.groupby("id_series")}, incidents


def strata(incidents: pd.DataFrame) -> dict[str, pd.DataFrame]:
    severe = incidents.impact.isin(["critical", "major"])
    platform = incidents.components.map(lambda c: bool(PLATFORM & set(c)))
    return {"all": incidents, "critical_or_major": incidents[severe], "platform": incidents[platform],
            "platform_critical_or_major": incidents[severe & platform]}


def analyze(cfg: Config) -> dict:
    with psycopg.connect(cfg.pg_dsn) as conn:
        by_series, incidents = load_inputs(conn)
        days = by_series["collab"].index.normalize().nunique() if "collab" in by_series else 0
        if days < MIN_DAYS or incidents.empty:
            log.info("skipping analysis: %d days loaded, %d needed", days, MIN_DAYS)
            return {"skipped": True, "days": int(days)}
        ratios = {s: drop_ratios(velocity(m).velocity.dropna()) for s, m in by_series.items()}
        result: dict = {"growth": {s: weekday_growth(r.velocity) for s, r in ratios.items()}}

        # The collab sequence is the only one present in every hour. Nothing before the first
        # published incident can be scored.
        collab = ratios["collab"].drop_ratio.dropna()
        collab = collab[collab.index >= incidents.started_at.min() - MATCH_BEFORE]
        train, test = collab[collab.index < TRAIN_END], collab[collab.index >= TRAIN_END]
        inc_train = incidents[incidents.started_at < TRAIN_END]
        inc_test = incidents[incidents.started_at >= TRAIN_END]
        chosen = choose_threshold(train, inc_train)
        result["threshold"] = chosen
        result["grid"] = [score(train, inc_train, t).row() for t in THRESHOLDS]
        result["train"] = {k: score(train, v, chosen).row() for k, v in strata(inc_train).items()}
        result["test"] = {k: score(test, v, chosen).row() for k, v in strata(inc_test).items()}
        # For the record only: the test period at every threshold. None of this picks anything.
        result["test_grid"] = [score(test, inc_test, t).row() for t in THRESHOLDS]

        held_out = find_episodes(test, chosen)
        null = null_distribution(held_out, inc_test, test.index[0], test.index[-1] - test.index[0] + WINDOW)
        observed = int(match(held_out, inc_test).notna().sum())
        result["null"] = {"observed": observed, "episodes": len(held_out), "null_mean": round(float(null.mean()), 1),
                          "null_p95": float(np.quantile(null, 0.95)),
                          "p": round(float((1 + (null >= observed).sum()) / (1 + NULL_DRAWS)), 4)}

        episodes = find_episodes(collab, chosen)
        episodes["incident_id"] = match(episodes, incidents)
        info = incidents.set_index("incident_id")
        episodes["lead_minutes"] = [(info.started_at[i] - s).total_seconds() / 60 if i else None
                                    for i, s in zip(episodes.incident_id, episodes.started_at)]
        result["detections"] = [
            {"started_at": str(e.started_at), "ended_at": str(e.ended_at), "min_ratio": round(e.min_ratio, 3),
             "incident": info.name[e.incident_id] if e.incident_id else None,
             "impact": info.impact[e.incident_id] if e.incident_id else None,
             "lead_minutes": None if pd.isna(e.lead_minutes) else round(e.lead_minutes)}
            for e in episodes.itertuples()]

        with conn.cursor() as cur:
            cur.execute("TRUNCATE ops.feed_anomalies, ops.feed_episodes")
            for s, r in ratios.items():
                low = r[r.drop_ratio < chosen].dropna()
                cur.executemany("INSERT INTO ops.feed_anomalies VALUES (%s, %s, %s, %s, %s)",
                                [(x.Index.to_pydatetime(), s, x.velocity, x.baseline, x.drop_ratio)
                                 for x in low.itertuples()])
            cur.executemany("INSERT INTO ops.feed_episodes VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            [("collab", e.started_at.to_pydatetime(), e.ended_at.to_pydatetime(), e.windows, e.min_ratio,
                              e.incident_id, None if pd.isna(e.lead_minutes) else e.lead_minutes)
                             for e in episodes.itertuples()])
            cur.execute("INSERT INTO ops.analysis_results (result) VALUES (%s)", (json.dumps(result, default=str),))
    log.info("threshold %s: %s", chosen, result["null"])
    return result
