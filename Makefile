.PHONY: install data profile features analysis risk charts dashboard validate test all

PY := ./.venv/bin/python

install:
	python -m venv .venv
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r requirements.txt

data:
	$(PY) src/data_generation/generate_synthetic_data.py

profile:
	$(PY) src/data_profiling/profile_data_quality.py

features:
	$(PY) src/feature_engineering/create_retention_features.py

analysis:
	$(PY) src/churn_analysis/run_main_analysis.py

risk:
	$(PY) src/risk_scoring/build_risk_scores.py

charts:
	$(PY) src/visualization/build_chart_pack.py

dashboard:
	$(PY) src/dashboard_builder/build_executive_dashboard.py

validate:
	$(PY) src/validation/validate_data_contracts.py
	$(PY) src/validation/run_final_validation.py

test:
	$(PY) -m unittest discover -s tests -p "test_*.py" -v

all: data profile features analysis risk charts dashboard validate
