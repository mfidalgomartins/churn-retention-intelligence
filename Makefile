.PHONY: install data profile features analyze risk dashboard validate test all clean

PY ?= ./.venv/bin/python
MOD = $(PY) -m churn

install:
	python -m venv .venv
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -e .

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

validate:
	$(MOD).contracts
	$(MOD).validate

test:
	$(PY) -m unittest discover -s tests -p "test_*.py" -v

# validate inspects the dashboard HTML, so dashboard must run first;
# then we re-render so the released dashboard embeds the validation results.
all: data profile features analyze risk dashboard validate dashboard

clean:
	rm -rf data/raw data/processed outputs/tables outputs/dashboard/*.html
	rm -f index.html docs/index.html
