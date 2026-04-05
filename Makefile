.PHONY: install generate profile features analyze risk charts dashboard validate test qa-release all

PYTHON := ./.venv/bin/python

install:
	python -m venv .venv
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt

generate:
	$(PYTHON) src/data_generation/generate_synthetic_data.py

profile:
	$(PYTHON) src/data_profiling/profile_data_quality.py

features:
	$(PYTHON) src/feature_engineering/create_retention_features.py

analyze:
	$(PYTHON) src/churn_analysis/run_main_analysis.py

risk:
	$(PYTHON) src/risk_scoring/build_risk_scores.py

charts:
	$(PYTHON) src/visualization/build_chart_pack.py

dashboard:
	$(PYTHON) src/dashboard_builder/build_executive_dashboard.py

validate:
	$(PYTHON) src/validation/validate_data_contracts.py
	$(PYTHON) src/validation/run_final_validation.py

test:
	$(PYTHON) -m unittest discover -s tests -p "test_*.py" -v

qa-release: validate test
	@echo "QA release gates executed: validation + tests."

all: generate profile features analyze risk charts dashboard validate
