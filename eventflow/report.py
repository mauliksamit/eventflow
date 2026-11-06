"""Build a static HTML dashboard from the reporting marts (no server needed to view it)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import psycopg
from plotly.subplots import make_subplots

from .config import Config, get_config


def _df(conn, sql: str) -> pd.DataFrame:
    cur = conn.execute(sql)
    return pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])


def build_dashboard(out: Path = Path("docs/dashboard.html"), cfg: Config | None = None) -> Path:
    cfg = cfg or get_config()
    with psycopg.connect(cfg.pg_dsn) as conn:
        k = _df(conn, "SELECT * FROM mart.daily_kpis ORDER BY full_date")
        c = _df(conn, """SELECT category_name, sum(revenue) AS revenue FROM mart.category_revenue_daily
                         GROUP BY 1 ORDER BY 2 DESC""")
        h = _df(conn, """SELECT hour, sum(requests) AS requests,
                                sum(error_rate_5xx * requests) / sum(requests) AS error_rate_5xx
                         FROM mart.hourly_ops GROUP BY 1 ORDER BY 1""")
        dq = _df(conn, """SELECT check_name, severity, passed, observed, threshold, details
                          FROM ops.dq_results WHERE run_id = (SELECT run_id FROM ops.pipeline_runs
                                                              ORDER BY started_at DESC LIMIT 1)
                          ORDER BY severity, check_name""")
    for col in ["revenue", "conversion_rate", "error_rate_5xx", "p95_latency_ms", "avg_session_min"]:
        if col in k:
            k[col] = k[col].astype(float)
    c["revenue"] = c["revenue"].astype(float)
    h["error_rate_5xx"] = h["error_rate_5xx"].astype(float)

    fig = make_subplots(rows=3, cols=2, vertical_spacing=0.12, subplot_titles=(
        "Daily active users", "Revenue (CAD, excl. cancelled)", "Session conversion rate",
        "Revenue by category", "Hourly 5xx error rate", "p95 request latency (ms)"))
    fig.add_trace(go.Scatter(x=k.full_date, y=k.active_users, mode="lines+markers"), 1, 1)
    fig.add_trace(go.Bar(x=k.full_date, y=k.revenue), 1, 2)
    fig.add_trace(go.Scatter(x=k.full_date, y=k.conversion_rate, mode="lines+markers"), 2, 1)
    fig.add_trace(go.Bar(x=c.category_name, y=c.revenue), 2, 2)
    fig.add_trace(go.Scatter(x=h.hour, y=h.error_rate_5xx, mode="lines"), 3, 1)
    fig.add_trace(go.Scatter(x=k.full_date, y=k.p95_latency_ms, mode="lines+markers"), 3, 2)
    fig.update_layout(height=1000, showlegend=False, title="eventflow – daily KPIs", template="plotly_white")

    dq_html = dq.to_html(index=False, classes="dq", border=0, na_rep="", float_format=lambda x: f"{x:,.6g}")
    html = f"""<!doctype html><html><head><meta charset="utf-8"><title>eventflow dashboard</title>
<style>body{{font-family:system-ui,sans-serif;margin:24px}} table.dq{{border-collapse:collapse}}
table.dq td,table.dq th{{padding:4px 10px;border-bottom:1px solid #ddd;text-align:left}}</style></head><body>
{fig.to_html(full_html=False, include_plotlyjs="cdn")}
<h2>Data quality – latest run</h2>{dq_html}</body></html>"""
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html)
    return out
