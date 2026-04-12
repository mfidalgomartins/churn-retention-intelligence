from __future__ import annotations

import csv
import json
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class ContractCheck:
    dataset: str
    check_name: str
    status: str
    severity: str
    evidence: str


def load_csv(path: Path) -> tuple[list[dict[str, str]], list[str]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return rows, list(reader.fieldnames or [])


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> int:
    project_root = Path(__file__).resolve().parents[2]
    contract_path = project_root / "config" / "contracts" / "data_contracts.json"
    outputs_dir = project_root / "outputs" / "tables"
    docs_dir = project_root / "docs" / "governance"

    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    datasets: dict[str, dict[str, Any]] = contract.get("datasets", {})

    checks: list[ContractCheck] = []

    for dataset_name, cfg in datasets.items():
        rel_path = str(cfg.get("path", ""))
        pk = str(cfg.get("primary_key", ""))
        required_columns = [str(c) for c in cfg.get("required_columns", [])]

        path = project_root / rel_path
        exists = path.exists()
        checks.append(
            ContractCheck(
                dataset=dataset_name,
                check_name="dataset_exists",
                status="PASS" if exists else "FAIL",
                severity="blocker" if not exists else "info",
                evidence=f"path={rel_path}; exists={exists}",
            )
        )

        if not exists:
            continue

        rows, cols = load_csv(path)
        row_count = len(rows)

        missing_cols = sorted(set(required_columns) - set(cols))
        checks.append(
            ContractCheck(
                dataset=dataset_name,
                check_name="required_columns_present",
                status="PASS" if not missing_cols else "FAIL",
                severity="blocker" if missing_cols else "info",
                evidence=f"missing_columns={missing_cols}",
            )
        )

        checks.append(
            ContractCheck(
                dataset=dataset_name,
                check_name="row_count_nonzero",
                status="PASS" if row_count > 0 else "FAIL",
                severity="blocker" if row_count == 0 else "info",
                evidence=f"row_count={row_count}",
            )
        )

        if pk and pk in cols:
            null_pk = sum(1 for r in rows if not str(r.get(pk, "")).strip())
            dup_pk = sum(c - 1 for c in Counter(str(r.get(pk, "")) for r in rows).values() if c > 1)
            checks.append(
                ContractCheck(
                    dataset=dataset_name,
                    check_name="primary_key_not_null",
                    status="PASS" if null_pk == 0 else "FAIL",
                    severity="blocker" if null_pk > 0 else "info",
                    evidence=f"primary_key={pk}; null_rows={null_pk}",
                )
            )
            checks.append(
                ContractCheck(
                    dataset=dataset_name,
                    check_name="primary_key_unique",
                    status="PASS" if dup_pk == 0 else "FAIL",
                    severity="blocker" if dup_pk > 0 else "info",
                    evidence=f"primary_key={pk}; duplicate_rows={dup_pk}",
                )
            )
        else:
            checks.append(
                ContractCheck(
                    dataset=dataset_name,
                    check_name="primary_key_declared_and_present",
                    status="FAIL",
                    severity="blocker",
                    evidence=f"primary_key={pk}; available_columns={cols}",
                )
            )

    check_rows = [
        {
            "dataset": c.dataset,
            "check_name": c.check_name,
            "status": c.status,
            "severity": c.severity,
            "evidence": c.evidence,
        }
        for c in checks
    ]
    issue_rows = [
        {
            "dataset": c.dataset,
            "check_name": c.check_name,
            "status": c.status,
            "severity": c.severity,
            "evidence": c.evidence,
        }
        for c in checks
        if c.status != "PASS"
    ]

    write_csv(
        outputs_dir / "data_contract_checks.csv",
        check_rows,
        ["dataset", "check_name", "status", "severity", "evidence"],
    )
    write_csv(
        outputs_dir / "data_contract_issues.csv",
        issue_rows,
        ["dataset", "check_name", "status", "severity", "evidence"],
    )

    pass_count = sum(1 for c in checks if c.status == "PASS")
    fail_count = sum(1 for c in checks if c.status == "FAIL")

    docs_dir.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Data Contract Validation Report",
        "",
        f"- Total checks: **{len(checks)}**",
        f"- PASS: **{pass_count}**",
        f"- FAIL: **{fail_count}**",
        "",
        "## Contract Checks",
        "",
        "| Dataset | Check | Status | Severity | Evidence |",
        "|---|---|---|---|---|",
    ]
    for r in check_rows:
        lines.append(f"| {r['dataset']} | {r['check_name']} | {r['status']} | {r['severity']} | {r['evidence']} |")

    (docs_dir / "data_contract_validation_report.md").write_text("\n".join(lines), encoding="utf-8")

    print("Data contract validation completed.")
    print("Checks:", len(checks), "| PASS:", pass_count, "| FAIL:", fail_count)
    return 1 if fail_count > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
