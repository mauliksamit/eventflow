"""Charts and the numbers quoted in the docs. Everything is read back from the warehouse.

Writes docs/results.json, docs/figures/*.png and docs/dashboard.html.
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import psycopg

from .analysis import PLATFORM, drop_ratios, load_inputs, velocity, weekday_rate
from .config import Config

DOCS = Path(__file__).resolve().parent.parent / "docs"
BLUE, ORANGE = "#2a78d6", "#eb6834"
INK, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#e8e7e3", "#fcfcfb"
INCIDENT_FILL = "rgba(82, 81, 78, 0.14)"
FONT = "Inter, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif"


def _style(fig: go.Figure, title: str, subtitle: str, height: int, legend: bool = True) -> go.Figure:
    fig.update_layout(
        title={"text": f"<b>{title}</b><br><span style='font-size:13px;color:{MUTED}'>{subtitle}</span>",
               "x": 0, "xanchor": "left"},
        template="none", height=height, width=980, paper_bgcolor=SURFACE, plot_bgcolor=SURFACE,
        font={"family": FONT, "color": INK, "size": 13}, margin={"l": 64, "r": 24, "t": 88, "b": 48},
        hovermode="x unified", showlegend=legend,
        legend={"orientation": "h", "x": 0, "y": -0.12, "font": {"color": MUTED}})
    fig.update_xaxes(showgrid=False, linecolor=GRID, tickcolor=GRID, tickfont={"color": MUTED}, tickformat="%b %-d")
    fig.update_yaxes(gridcolor=GRID, zeroline=False, linecolor=GRID, tickfont={"color": MUTED})
    return fig


def fig_capture(daily: pd.DataFrame) -> go.Figure:
    """Same unit on one log axis: the gap between the two lines is the capture rate."""
    d = daily[daily.id_series == "collab"].sort_values("dt")
    fig = go.Figure()
    fig.add_scatter(x=d.dt, y=d.events_estimated, name="Ids allocated by GitHub", mode="lines",
                    line={"color": BLUE, "width": 2}, hovertemplate="%{y:,.0f}")
    fig.add_scatter(x=d.dt, y=d.events_captured, name="Captured in the archive", mode="lines",
                    line={"color": ORANGE, "width": 2}, hovertemplate="%{y:,.0f}")
    fig.update_yaxes(type="log", title="Per day (log scale)")
    swing = d.events_captured.max() / d.events_captured.min()
    return _style(fig, f"The archive's volume swings {swing:.0f}×. GitHub's ids do not.",
                  "Pull requests, issues, comments, stars and forks, per day", 420)


def fig_coverage(gaps: pd.DataFrame) -> go.Figure:
    """Each hour of the period. Empty cells are hours with no push events in the archive."""
    g = gaps[gaps.id_series == "ref"].copy()
    g["day"] = pd.to_datetime(g.hour_ts, utc=True).dt.date
    g["hour"] = pd.to_datetime(g.hour_ts, utc=True).dt.hour
    grid = g.pivot(index="hour", columns="day", values="events").reindex(range(24))
    fig = go.Figure(go.Heatmap(
        x=[str(c) for c in grid.columns], y=grid.index, z=grid.where(grid > 0).values, xgap=1, ygap=1,
        colorscale=[[0, "#cfe2f7"], [0.5, "#5b9be0"], [1, "#134a8e"]],
        colorbar={"title": "Events", "thickness": 10, "outlinewidth": 0},
        hovertemplate="%{x} %{y}:00 UTC<br>%{z:,.0f} events<extra></extra>", hoverongaps=False))
    fig.update_yaxes(title="Hour (UTC)", autorange="reversed", dtick=6, showgrid=False)
    return _style(fig, "Push events vanish from the archive for hours at a time",
                  "Push, create and delete events captured per hour. Blank cells have none.", 400, legend=False)


def fig_growth(ratios: dict[str, pd.DataFrame]) -> go.Figure:
    fig = go.Figure()
    names = {"collab": ("Pull requests, issues, stars and comments", BLUE), "ref": ("Pushes, branches and tags", ORANGE)}
    for series, (label, color) in names.items():
        daily = weekday_rate(ratios[series].velocity)
        fig.add_scatter(x=daily.index, y=daily, name=label, mode="lines+markers", line={"color": color, "width": 2},
                        marker={"size": 5}, hovertemplate="%{y:,.0f} ids/s")
    fig.update_yaxes(title="Ids per second", rangemode="tozero")
    return _style(fig, "GitHub's ids kept climbing while the archive's counts fell",
                  "Median id rate on weekdays, whether or not the events reached the archive", 400)


def fig_incident(ratios: dict[str, pd.DataFrame], incidents: pd.DataFrame, episodes: pd.DataFrame) -> go.Figure:
    r = ratios["collab"]["2026-09-13 05:00":"2026-09-13 13:00"]
    inc = incidents[incidents.name == "Incident with several GitHub Services"].iloc[0]
    ep = episodes[(episodes.started_at >= r.index[0]) & (episodes.started_at <= r.index[-1])].iloc[0]
    fig = go.Figure()
    fig.add_vrect(x0=inc.started_at, x1=inc.resolved_at, fillcolor=INCIDENT_FILL, line_width=0,
                  annotation_text="Published incident", annotation_position="top right",
                  annotation_font={"color": MUTED, "size": 12})
    fig.add_scatter(x=r.index, y=r.baseline, name="Expected", mode="lines", line={"color": MUTED, "width": 2, "dash": "dot"},
                    hovertemplate="%{y:,.0f} ids/s")
    fig.add_scatter(x=r.index, y=r.velocity, name="Observed", mode="lines", line={"color": BLUE, "width": 2},
                    hovertemplate="%{y:,.0f} ids/s")
    lead = (inc.started_at - ep.started_at).total_seconds() / 60
    fig.add_annotation(x=ep.started_at, y=r.velocity[ep.started_at], text=f"Drop begins {lead:.0f} min before the posting",
                       ax=-110, ay=44, arrowcolor=MUTED, arrowwidth=1, font={"color": INK, "size": 12})
    fig.update_yaxes(title="Ids per second", rangemode="tozero")
    fig.update_xaxes(tickformat="%H:%M")
    return _style(fig, "The clearest catch: September 13, 2026",
                  "Id rate on a critical incident across API, Issues, Pull Requests, Actions and Pages (UTC)", 400)


def _numbers(conn, result: dict, tune: list[dict]) -> dict:
    def one(sql: str) -> tuple:
        return conn.execute(sql).fetchone()

    lines, gb = one("SELECT sum(line_count), sum(bytes) / 1e9 FROM ops.ingest_files")
    dups, rejected, valid, secs = one("""SELECT sum(duplicates), sum(rejected), sum(valid_unique), sum(transform_seconds)
                                         FROM mart.pipeline_daily""")
    present, total = one("SELECT count(*) FILTER (WHERE events > 0), count(*) FROM mart.feed_gaps WHERE id_series = 'ref'")
    lo, hi = one("""SELECT min(c), max(c) FROM (SELECT sum(events_captured) / sum(events_estimated) AS c
                    FROM mart.feed_daily WHERE id_series = 'collab' GROUP BY dt) x""")
    daily_lo, daily_hi = one("SELECT min(events_captured), max(events_captured) FROM mart.feed_daily WHERE id_series = 'collab'")
    q = one("""SELECT sum(from_new_accounts), sum(dated_stars), count(*) FILTER (WHERE dated_stars >= 100),
                      count(*) FILTER (WHERE dated_stars >= 100 AND from_new_accounts >= 0.5 * dated_stars)
               FROM mart.repo_stars""")
    naive = {r[0] for r in conn.execute("SELECT repo_id FROM mart.repo_stars ORDER BY stars_captured DESC LIMIT 20").fetchall()}
    weighted = {r[0] for r in conn.execute("""SELECT repo_id FROM mart.repo_stars WHERE stargazers >= 30
                                              ORDER BY stars_normalized DESC LIMIT 20""").fetchall()}
    mix_low, mix_high, star_lo, star_hi, pr_lo, pr_hi = one("""
        SELECT min(capture_rate), max(capture_rate), min(star_share), max(star_share),
               min(pull_request_share), max(pull_request_share) FROM mart.capture_mix""")
    mix = dict(conn.execute("SELECT actor_class, sum(events) FROM mart.actor_mix_weekly GROUP BY 1").fetchall())
    mix_total = sum(mix.values())
    onset = conn.execute("""SELECT e.started_at, e.ended_at, e.min_ratio, e.lead_minutes, i.name, i.impact, i.started_at
                            FROM ops.feed_episodes e JOIN dw.dim_incident i USING (incident_id)
                            WHERE i.components && %s AND e.min_ratio < 0.7 AND e.lead_minutes > 0
                            ORDER BY 1""", (list(PLATFORM),)).fetchall()
    return {
        "platform_catches": [{"drop_began": str(o[0]), "drop_ended": str(o[1]), "min_ratio": round(o[2], 3),
                              "lead_minutes": round(o[3]), "incident": o[4], "impact": o[5], "posted": str(o[6])}
                             for o in onset],
        "days": one("SELECT count(*) FROM ops.published_partitions")[0],
        "hours": one("SELECT count(*) FROM ops.ingest_files")[0],
        "raw_lines": int(lines), "raw_gb": round(float(gb), 1), "unique_events": int(valid), "duplicates": int(dups),
        "quarantined": int(rejected), "reconciles": int(lines) == int(valid) + int(dups) + int(rejected),
        "transform_minutes": round(float(secs) / 60, 1),
        "repos": one("SELECT count(DISTINCT repo_id) FROM dw.dim_repo")[0],
        "repo_renames": one("SELECT count(*) FROM dw.dim_repo WHERE NOT is_current")[0],
        "actors": one("SELECT count(*) FROM dw.dim_actor")[0], "stars": one("SELECT count(*) FROM dw.fact_star")[0],
        "fact_repo_daily_rows": one("SELECT count(*) FROM dw.fact_repo_daily")[0],
        "incidents": one("SELECT count(*) FROM dw.dim_incident")[0],
        "ref_hours_present": present, "ref_hours_total": total, "ref_hours_missing_share": round(1 - present / total, 3),
        "collab_capture_daily_min": round(float(lo), 5), "collab_capture_daily_max": round(float(hi), 5),
        "collab_captured_daily_min": int(daily_lo), "collab_captured_daily_max": int(daily_hi),
        "new_account_stars": int(q[0]), "dated_stars": int(q[1]), "new_account_star_share": round(int(q[0]) / int(q[1]), 3),
        "repos_100_stars": q[2], "repos_majority_new_accounts": q[3],
        "top20_overlap_naive_vs_weighted": len(naive & weighted),
        "capture_mix": {"capture_range": round(float(mix_high) / float(mix_low), 1),
                        "star_share": [round(float(star_lo), 3), round(float(star_hi), 3)],
                        "pull_request_share": [round(float(pr_lo), 3), round(float(pr_hi), 3)]},
        "actor_mix": {k: round(float(v) / float(mix_total), 3) for k, v in mix.items()},
        "analysis": result, "tuning": tune,
    }


def build(cfg: Config, tune: list[dict] | None = None) -> str:
    (DOCS / "figures").mkdir(parents=True, exist_ok=True)
    with psycopg.connect(cfg.pg_dsn) as conn:
        conn.execute("SET TIME ZONE 'UTC'")
        by_series, incidents = load_inputs(conn)
        ratios = {s: drop_ratios(velocity(m).velocity.dropna()) for s, m in by_series.items()}
        result = conn.execute("SELECT result FROM ops.analysis_results ORDER BY run_at DESC LIMIT 1").fetchone()[0]
        with conn.cursor() as cur:
            cur.execute("SELECT dt, id_series, events_captured, events_estimated FROM mart.feed_daily")
            daily = pd.DataFrame(cur.fetchall(), columns=["dt", "id_series", "events_captured", "events_estimated"])
            cur.execute("SELECT hour_ts, id_series, events FROM mart.feed_gaps")
            gaps = pd.DataFrame(cur.fetchall(), columns=["hour_ts", "id_series", "events"])
            cur.execute("SELECT started_at, ended_at FROM ops.feed_episodes")
            episodes = pd.DataFrame(cur.fetchall(), columns=["started_at", "ended_at"])
        for c in ("events_captured", "events_estimated"):
            daily[c] = daily[c].astype(float)
        gaps["events"] = gaps.events.astype(float)
        for c in ("started_at", "ended_at"):
            episodes[c] = pd.to_datetime(episodes[c], utc=True)
        previous = DOCS / "results.json"
        if tune is None and previous.exists():
            tune = json.loads(previous.read_text()).get("tuning", [])
        numbers = _numbers(conn, result, tune or [])

    figures = {"capture": fig_capture(daily), "coverage": fig_coverage(gaps), "growth": fig_growth(ratios),
               "incident": fig_incident(ratios, incidents, episodes)}
    for name, fig in figures.items():
        fig.write_image(DOCS / "figures" / f"{name}.png", scale=2)
    body = "".join(f.to_html(full_html=False, include_plotlyjs="cdn" if i == 0 else False)
                   for i, f in enumerate(figures.values()))
    html = (f"<!DOCTYPE html><html lang='en'><head><meta charset='utf-8'><title>eventflow</title></head>"
            f"<body style='background:{SURFACE};margin:32px auto;max-width:980px'>{body}</body></html>")
    (DOCS / "dashboard.html").write_text(html, encoding="utf-8")
    (DOCS / "results.json").write_text(json.dumps(numbers, indent=2, default=str) + "\n", encoding="utf-8")
    return str(DOCS)
