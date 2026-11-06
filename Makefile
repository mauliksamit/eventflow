PY ?= python3
START ?= 2026-08-20
END ?= 2026-11-05
export EVENTFLOW_PG_DSN ?= postgresql://postgres:postgres@localhost:5432/eventflow
export EVENTFLOW_LAKE ?= data/lake

.PHONY: install test lint e2e backfill analyze tune report

install:
	$(PY) -m pip install -r requirements.txt

test:
	$(PY) -m pytest -q

lint:
	ruff check eventflow tests dags

# One real archive day: the happy path, a blocked publish and a reload.
e2e:
	PYTHON=$(PY) scripts/e2e.sh

# The published dataset: 78 days, 31.7 GB compressed.
backfill:
	$(PY) -m eventflow.cli init
	$(PY) -m eventflow.cli status
	$(PY) -m eventflow.cli run --start $(START) --end $(END)

analyze:
	$(PY) -m eventflow.cli marts
	$(PY) -m eventflow.cli analyze

tune:
	$(PY) -m eventflow.cli tune

report:
	$(PY) -m eventflow.cli report
