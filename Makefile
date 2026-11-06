PY ?= python3
export EVENTFLOW_PG_DSN ?= postgresql://postgres:postgres@localhost:5432/eventflow

.PHONY: install test lint pipeline small tune report

install:
	$(PY) -m pip install -r requirements.txt

test:
	$(PY) -m pytest -q

lint:
	ruff check eventflow tests

# Full run: ~2M clickstream events, ~1M access-log lines, 14 days
pipeline:
	scripts/run_pipeline.sh

# Fast run for development and CI (~2% of full size)
small:
	EVENTFLOW_SCALE=0.02 EVENTFLOW_DATA_DIR=data/small scripts/run_pipeline.sh

tune:
	$(PY) -m eventflow.cli tune

report:
	$(PY) -m eventflow.cli report
