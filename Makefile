.PHONY: install data profile features analyze risk dashboard charts report validate test coverage lint security quality all release clean

PY ?= ./.venv/bin/python
MOD = $(PY) -m churn
MPLCONFIGDIR ?= ./.cache/matplotlib

install:
	python -m venv .venv
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -e ".[dev,charts]"

data:
	$(MOD).generate

profile:
	$(MOD).profile

features:
	$(MOD).features

analyze:
	$(MOD).analyze

risk:
	$(MOD).risk

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

security:
	$(PY) -m bandit -q -r src -c pyproject.toml
	$(PY) -m pip_audit --skip-editable

quality: lint coverage security

# Validate inspects the first render; the recipe then publishes a final render
# with the current validation summary embedded.
all: data profile features analyze risk dashboard validate
	$(MOD).dashboard

release: all report

clean:
	rm -rf data/raw data/processed outputs/tables
	rm -f outputs/dashboard/*.html outputs/graphs/*.png outputs/reports/*.pdf
	rm -f index.html docs/index.html
