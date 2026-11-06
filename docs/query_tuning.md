# Query tuning

Measured on the loaded warehouse: 29.3M rows in `fact_repo_daily`, 1.2M in `fact_star`, 13.1M in `dim_repo`. Each time is the median of five warm runs on PostgreSQL 16, so these compare plans rather than disk caches. Run it with `python -m eventflow.cli tune`, and see `tuning` in [results.json](results.json) for the plans.

| Query | Before | After | Speedup | Added | Decision |
|---|---:|---:|---:|---:|---|
| One repo's daily history across renames | 286.02 ms | 0.77 ms | 370.8× | 928 MB | Kept |
| A repo's newest stargazers | 39.45 ms | 2.32 ms | 17.0× | 39 MB | Kept |
| Everything one account starred | 33.12 ms | 1.55 ms | 21.3× | 39 MB | Kept |
| Stars per minute around an incident | 36.07 ms | 4.09 ms | 8.8× | under 0.1 MB | Kept |
| One day of hourly counts by event type | 0.35 ms | 0.25 ms | 1.4× | 3 MB | Rejected |

An index stays only if it at least doubles the speed of the query it serves. Every index also adds to each daily load, and the tool drops any that miss the bar.

## What moved the numbers

**Repo history.** A repo can have several rows in `dim_repo` after renames, so the query resolves a name to a `repo_id`, then to all of its surrogate keys. Before tuning that meant a parallel sequential scan of 13M names, and the fact table's primary key starts with `date_key`, so it could not help find one repo. Indexes on `repo_name` and `repo_id`, plus `(repo_key, date_key)` on the fact, turn it into index lookups across the monthly partitions. This is the expensive one, at 928 MB and 49 seconds to build, and it is the query the warehouse exists for.

**Stars around an incident.** `fact_star` is loaded in time order, so a BRIN index on `starred_at` gives most of a B-tree's benefit for almost no space. That correlation would not hold for a table with heavy out-of-order loads, and the numbers should be rechecked if backfills become common.

**The rejected index.** A covering index on `fact_type_hourly` did produce an index-only scan, and on an earlier run the same query came out slower than without it. The table is small enough that the primary key already answers this in under a millisecond, so there was nothing to gain.

## Why these are not in the base schema

Dimensions load through `ON CONFLICT` upserts and facts through delete-then-insert by day, so every index adds to the daily load. Indexes belong to a query someone actually runs, which is why they are created here with measurements attached.
