.PHONY: install data ingest profile economics features analyze risk training-snapshots model experiments monitor memo dashboard charts report validate test coverage lint format-check typecheck security quality pipeline production snapshot all release clean

PY ?= ./.venv/bin/python
MOD = $(PY) -m churn
MPLCONFIGDIR ?= ./.cache/matplotlib
LOCK ?= requirements.lock
SOURCE_ADAPTER ?= csv
SOURCE_ARGS ?=

install:
	python -m venv .venv
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -c $(LOCK) -e ".[dev,charts]"

data:
	$(MOD).generate

ingest:
	$(MOD).ingest --adapter $(SOURCE_ADAPTER) $(SOURCE_ARGS)

profile:
	$(MOD).profile

economics:
	$(MOD).economics

features:
	$(MOD).features

analyze:
	$(MOD).analyze

risk:
	$(MOD).risk

training-snapshots:
	$(MOD).snapshots

model:
	$(MOD).modeling

experiments:
	$(MOD).experiments

monitor:
	$(MOD).monitor

memo:
	$(MOD).memo

dashboard:
	$(MOD).dashboard

charts:
	MPLCONFIGDIR=$(MPLCONFIGDIR) $(MOD).graphs

# Builds the static chart pack first, then the narrative PDF that embeds it.
report: charts
	$(MOD).report

validate:
	$(MOD).contracts
	$(MOD).validate

test:
	$(PY) -m unittest discover -s tests -p "test_*.py" -v

coverage:
	$(PY) -m coverage run -m unittest discover -s tests -p "test_*.py"
	$(PY) -m coverage report

lint:
	$(PY) -m ruff check src tests

format-check:
	$(PY) -m ruff format --check src tests

typecheck:
	$(PY) -m mypy

security:
	$(PY) -m bandit -q -r src -c pyproject.toml
	$(PY) -m pip_audit --skip-editable

quality: lint format-check typecheck coverage security

pipeline: profile economics features analyze risk training-snapshots model experiments monitor dashboard validate
	$(MOD).dashboard

production: ingest pipeline

snapshot:
	$(MOD).snapshot

# Validate inspects the first render; the recipe then publishes a final render
# with the current validation summary embedded.
all: data pipeline

release: all memo report snapshot

clean:
	rm -rf data/raw data/processed outputs/tables outputs/models outputs/snapshots
	rm -f outputs/dashboard/*.html outputs/graphs/*.png outputs/reports/*.pdf
	rm -f index.html docs/index.html
