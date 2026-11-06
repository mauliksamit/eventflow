-- Reporting layer: daily KPIs that dashboards and analysts read.
-- Materialized views precompute the expensive joins/aggregations once per pipeline run;
-- REFRESH MATERIALIZED VIEW is the last step of every run.
DROP MATERIALIZED VIEW IF EXISTS mart.daily_kpis;
CREATE MATERIALIZED VIEW mart.daily_kpis AS
WITH s AS (
    SELECT date_key, count(*) AS sessions, count(DISTINCT customer_id) AS active_users,
           avg(extract(epoch FROM end_ts - start_ts)) / 60 AS avg_session_min,
           avg(converted::int) AS conversion_rate
    FROM dw.fact_sessions GROUP BY date_key
), o AS (
    SELECT date_key, count(DISTINCT order_id) AS orders, sum(revenue) AS revenue
    FROM dw.fact_order_items WHERE status <> 'cancelled' GROUP BY date_key
), r AS (
    SELECT date_key, count(*) AS requests,
           avg((status_code >= 500)::int) AS error_rate_5xx,
           percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_latency_ms
    FROM dw.fact_requests GROUP BY date_key
)
SELECT d.full_date, s.active_users, s.sessions, round(s.avg_session_min::numeric, 2) AS avg_session_min,
       round(s.conversion_rate::numeric, 4) AS conversion_rate, o.orders, o.revenue,
       r.requests, round(r.error_rate_5xx::numeric, 4) AS error_rate_5xx,
       round(r.p95_latency_ms::numeric, 1) AS p95_latency_ms
FROM dw.dim_date d
JOIN s USING (date_key)
LEFT JOIN o USING (date_key)
LEFT JOIN r USING (date_key)
ORDER BY d.full_date;

DROP MATERIALIZED VIEW IF EXISTS mart.category_revenue_daily;
CREATE MATERIALIZED VIEW mart.category_revenue_daily AS
SELECT d.full_date, p.category_name, sum(f.revenue) AS revenue, sum(f.quantity) AS units
FROM dw.fact_order_items f
JOIN dw.dim_date d USING (date_key)
JOIN dw.dim_product p USING (product_id)
WHERE f.status <> 'cancelled'
GROUP BY 1, 2;
CREATE INDEX ON mart.category_revenue_daily (full_date);

DROP MATERIALIZED VIEW IF EXISTS mart.hourly_ops;
CREATE MATERIALIZED VIEW mart.hourly_ops AS
SELECT date_trunc('hour', request_ts) AS hour, path_group, count(*) AS requests,
       avg((status_code >= 500)::int) AS error_rate_5xx,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms) AS p95_latency_ms
FROM dw.fact_requests GROUP BY 1, 2;
