"""Data-contract gate.

Reads config/contracts/data_contracts.json and verifies that every declared
dataset exists, has its required columns, has rows, and has a unique non-null
primary key. Writes a check log and (if any failed) an issues log.
"""
from __future__ import annotations

import csv
import json
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from typing import Any

from churn.common import outputs_tables_dir, project_root


@dataclass
class Check:
    dataset: str
    check_name: str
    status: str
    severity: str
    evidence: str


def _load_csv(path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return rows, list(reader.fieldnames or [])


def _write_csv(path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def evaluate_dataset(name: str, cfg: dict, root) -> list[Check]:
    rel = str(cfg.get("path", ""))
    pk = str(cfg.get("primary_key", ""))
    required_cols = [str(c) for c in cfg.get("required_columns", [])]
    path = root / rel
    exists = path.exists()

    checks: list[Check] = [Check(
        dataset=name, check_name="dataset_exists",
        status="PASS" if exists else "FAIL",
        severity="info" if exists else "blocker",
        evidence=f"path={rel}; exists={exists}",
    )]
    if not exists:
        return checks

    rows, cols = _load_csv(path)
    missing = sorted(set(required_cols) - set(cols))
    checks.append(Check(
        dataset=name, check_name="required_columns_present",
        status="PASS" if not missing else "FAIL",
        severity="info" if not missing else "blocker",
        evidence=f"missing_columns={missing}",
    ))
    checks.append(Check(
        dataset=name, check_name="row_count_nonzero",
        status="PASS" if rows else "FAIL",
        severity="info" if rows else "blocker",
        evidence=f"row_count={len(rows)}",
    ))

    if pk and pk in cols:
        null_pk = sum(1 for r in rows if not str(r.get(pk, "")).strip())
        dup_pk = sum(c - 1 for c in Counter(str(r.get(pk, "")) for r in rows).values() if c > 1)
        checks.append(Check(
            dataset=name, check_name="primary_key_not_null",
            status="PASS" if null_pk == 0 else "FAIL",
            severity="info" if null_pk == 0 else "blocker",
            evidence=f"primary_key={pk}; null_rows={null_pk}",
        ))
        checks.append(Check(
            dataset=name, check_name="primary_key_unique",
            status="PASS" if dup_pk == 0 else "FAIL",
            severity="info" if dup_pk == 0 else "blocker",
            evidence=f"primary_key={pk}; duplicate_rows={dup_pk}",
        ))
    else:
        checks.append(Check(
            dataset=name, check_name="primary_key_declared_and_present",
            status="FAIL", severity="blocker",
            evidence=f"primary_key={pk}; available_columns={cols}",
        ))
    return checks


def main() -> int:
    root = project_root()
    contract = json.loads((root / "config" / "contracts" / "data_contracts.json").read_text(encoding="utf-8"))
    datasets: dict[str, dict[str, Any]] = contract.get("datasets", {})

    all_checks: list[Check] = []
    for name, cfg in datasets.items():
        all_checks.extend(evaluate_dataset(name, cfg, root))

    fields = ["dataset", "check_name", "status", "severity", "evidence"]
    rows = [asdict(c) for c in all_checks]
    issues = [asdict(c) for c in all_checks if c.status != "PASS"]

    out = outputs_tables_dir()
    _write_csv(out / "data_contract_checks.csv", rows, fields)
    _write_csv(out / "data_contract_issues.csv", issues, fields)

    fails = sum(1 for c in all_checks if c.status == "FAIL")
    print(f"Contract gate: {len(all_checks)} checks, {fails} failed.")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
