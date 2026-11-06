-- Reporting layer, rebuilt from the warehouse. Times are UTC.
SET TIME ZONE 'UTC';

-- What the archive captured each day against what GitHub actually recorded.
DROP TABLE IF EXISTS mart.feed_daily;
CREATE TABLE mart.feed_daily AS
SELECT hour_ts::date AS dt, id_series, count(*) AS hours_present, sum(events) AS events_captured,
       sum(est_total)::bigint AS events_estimated, sum(events) / nullif(sum(est_total), 0) AS capture_rate
FROM dw.fact_feed_hourly GROUP BY 1, 2;

-- Every hour of the source, including those in which an id sequence is missing altogether.
DROP TABLE IF EXISTS mart.feed_gaps;
CREATE TABLE mart.feed_gaps AS
SELECT h.hour_ts, s.id_series, coalesce(f.events, 0) AS events, f.capture_rate
FROM (SELECT DISTINCT hour_ts FROM dw.fact_feed_hourly) h
CROSS JOIN (VALUES ('ref'), ('collab')) s (id_series)
LEFT JOIN dw.fact_feed_hourly f ON f.hour_ts = h.hour_ts AND f.id_series = s.id_series;

-- Account ids are handed out in order, so the highest id seen by a given day dates any account.
DROP TABLE IF EXISTS mart.account_watermark;
CREATE TABLE mart.account_watermark AS
SELECT dt, max(top) OVER (ORDER BY dt) AS max_actor_id
FROM (SELECT first_seen::date AS dt, max(actor_id) AS top FROM dw.dim_actor GROUP BY 1) d;
ALTER TABLE mart.account_watermark ADD PRIMARY KEY (dt);

-- Capture is a relative index, because ids also cover activity that never becomes public.
-- The best-covered day is the reference, so a weight says how many stars one captured star
-- stands for compared with that day. Repo totals are for ranking: one star caught in a thin
-- hour carries a large weight. from_new_account is NULL in the first week, before there is
-- a watermark to compare against.
DROP TABLE IF EXISTS mart.star_weighted;
CREATE TABLE mart.star_weighted AS
SELECT s.event_id, s.date_key, r.repo_id, s.actor_id, s.starred_at,
       greatest(1, (SELECT max(capture_rate) FROM mart.feed_daily WHERE id_series = 'collab')
                   / nullif(f.capture_rate, 0)) AS weight,
       s.actor_id > w.max_actor_id AS from_new_account
FROM dw.fact_star s
JOIN dw.dim_repo r USING (repo_key)
JOIN dw.dim_date d USING (date_key)
LEFT JOIN dw.fact_feed_hourly f ON f.hour_ts = date_trunc('hour', s.starred_at) AND f.id_series = 'collab'
LEFT JOIN mart.account_watermark w ON w.dt = d.full_date - 7;

-- Repos by stars, named as they are today.
DROP TABLE IF EXISTS mart.repo_stars;
CREATE TABLE mart.repo_stars AS
SELECT s.repo_id, c.repo_name, count(*) AS stars_captured, round(sum(s.weight)) AS stars_normalized,
       count(DISTINCT s.actor_id) AS stargazers, count(*) FILTER (WHERE s.from_new_account) AS from_new_accounts,
       count(s.from_new_account) AS dated_stars
FROM mart.star_weighted s
JOIN dw.dim_repo c ON c.repo_id = s.repo_id AND c.is_current
GROUP BY 1, 2;

-- Does the mix of events change with capture? If loss were selective, reweighting would be wrong.
DROP TABLE IF EXISTS mart.capture_mix;
CREATE TABLE mart.capture_mix AS
WITH hourly AS (
    SELECT f.hour_ts, f.capture_rate, f.events,
           sum(t.events) FILTER (WHERE t.event_type = 'WatchEvent') AS stars,
           sum(t.events) FILTER (WHERE t.event_type = 'PullRequestEvent') AS pull_requests
    FROM dw.fact_feed_hourly f
    JOIN dw.fact_type_hourly t ON t.hour_ts = f.hour_ts
    JOIN dw.dim_event_type e ON e.event_type = t.event_type AND e.id_series = 'collab'
    WHERE f.id_series = 'collab'
    GROUP BY 1, 2, 3)
SELECT band, avg(capture_rate) AS capture_rate, sum(events) AS events,
       sum(stars) / sum(events) AS star_share, sum(pull_requests) / sum(events) AS pull_request_share
FROM (SELECT *, ntile(8) OVER (ORDER BY capture_rate) AS band FROM hourly) x
GROUP BY band;

DROP TABLE IF EXISTS mart.actor_mix_weekly;
CREATE TABLE mart.actor_mix_weekly AS
SELECT date_trunc('week', t.hour_ts)::date AS week, t.actor_class, sum(t.events) AS events
FROM dw.fact_type_hourly t
JOIN dw.dim_event_type e USING (event_type)
WHERE e.id_series = 'collab'     -- the only sequence captured in every hour
GROUP BY 1, 2;

DROP TABLE IF EXISTS mart.pipeline_daily;
CREATE TABLE mart.pipeline_daily AS
SELECT p.dt, (r.stats ->> 'parsed_rows')::bigint AS parsed_rows, (r.stats ->> 'valid_unique')::bigint AS valid_unique,
       (r.stats ->> 'duplicates_removed')::bigint AS duplicates, (r.stats ->> 'rejected')::bigint AS rejected,
       (r.stats ->> 'transform_seconds')::numeric AS transform_seconds,
       (SELECT count(*) FROM ops.dq_results q WHERE q.run_id = p.run_id AND NOT q.passed) AS warnings
FROM ops.published_partitions p JOIN ops.pipeline_runs r USING (run_id);
