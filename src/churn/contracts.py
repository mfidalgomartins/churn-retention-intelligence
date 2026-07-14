"""Data-contract gate.

Reads config/contracts/data_contracts.json and verifies that every declared
dataset exists, has its required columns, has rows, and has a unique non-null
primary key. Writes a check log and (if any failed) an issues log.
"""

from __future__ import annotations

import csv
import json
import math
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date
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


def _cell(value: str | None) -> str:
    return str(value or "").strip()


def _sample(values: list[str], limit: int = 5) -> list[str]:
    return sorted(set(values))[:limit]


def _missing_constraint_check(
    dataset: str,
    check_name: str,
    required: list[str],
    cols: list[str],
) -> Check | None:
    missing = sorted(set(required) - set(cols))
    if not missing:
        return None
    return Check(
        dataset=dataset,
        check_name=check_name,
        status="FAIL",
        severity="blocker",
        evidence=f"missing_constraint_columns={missing}",
    )


def _allowed_value_checks(
    dataset: str, rows: list[dict[str, str]], cols: list[str], cfg: dict
) -> list[Check]:
    checks: list[Check] = []
    allowed_values: dict[str, list[str]] = cfg.get("allowed_values", {})
    for col, allowed in allowed_values.items():
        missing_check = _missing_constraint_check(dataset, f"allowed_values:{col}", [col], cols)
        if missing_check:
            checks.append(missing_check)
            continue

        allowed_set = {str(v) for v in allowed}
        nullable = col in {str(value) for value in cfg.get("nullable_columns", [])}
        invalid = [
            value
            for row in rows
            if (value := _cell(row.get(col))) not in allowed_set and not (nullable and not value)
        ]
        checks.append(
            Check(
                dataset=dataset,
                check_name=f"allowed_values:{col}",
                status="PASS" if not invalid else "FAIL",
                severity="info" if not invalid else "blocker",
                evidence=f"allowed={sorted(allowed_set)}; invalid_rows={len(invalid)}; sample={_sample(invalid)}",
            )
        )
    return checks


def _numeric_range_checks(
    dataset: str, rows: list[dict[str, str]], cols: list[str], cfg: dict
) -> list[Check]:
    checks: list[Check] = []
    ranges: dict[str, dict[str, float]] = cfg.get("numeric_ranges", {})
    nullable_columns = {str(value) for value in cfg.get("nullable_columns", [])}
    for col, bounds in ranges.items():
        missing_check = _missing_constraint_check(dataset, f"numeric_range:{col}", [col], cols)
        if missing_check:
            checks.append(missing_check)
            continue

        lower = bounds.get("min")
        upper = bounds.get("max")
        invalid: list[str] = []
        for row in rows:
            raw = _cell(row.get(col))
            if col in nullable_columns and not raw:
                continue
            try:
                value = float(raw)
            except ValueError:
                invalid.append(raw)
                continue
            if not math.isfinite(value):
                invalid.append(raw)
                continue
            if (lower is not None and value < float(lower)) or (
                upper is not None and value > float(upper)
            ):
                invalid.append(raw)

        checks.append(
            Check(
                dataset=dataset,
                check_name=f"numeric_range:{col}",
                status="PASS" if not invalid else "FAIL",
                severity="info" if not invalid else "blocker",
                evidence=f"min={lower}; max={upper}; invalid_rows={len(invalid)}; sample={_sample(invalid)}",
            )
        )
    return checks


def _unique_column_checks(
    dataset: str,
    rows: list[dict[str, str]],
    cols: list[str],
    cfg: dict,
) -> list[Check]:
    checks: list[Check] = []
    for col in [str(value) for value in cfg.get("unique_columns", [])]:
        check_name = f"unique:{col}"
        missing_check = _missing_constraint_check(dataset, check_name, [col], cols)
        if missing_check:
            checks.append(missing_check)
            continue
        values = [_cell(row.get(col)) for row in rows]
        duplicates = sum(count - 1 for count in Counter(values).values() if count > 1)
        checks.append(
            Check(
                dataset=dataset,
                check_name=check_name,
                status="PASS" if duplicates == 0 else "FAIL",
                severity="info" if duplicates == 0 else "blocker",
                evidence=f"column={col}; duplicate_rows={duplicates}",
            )
        )
    return checks


def _parse_iso_date(value: str) -> date:
    return date.fromisoformat(value[:10])


def _date_order_checks(
    dataset: str, rows: list[dict[str, str]], cols: list[str], cfg: dict
) -> list[Check]:
    checks: list[Check] = []
    for rule in cfg.get("date_order_checks", []):
        start_col = str(rule["start_column"])
        end_col = str(rule["end_column"])
        allow_blank_end = bool(rule.get("allow_blank_end", False))
        check_name = f"date_order:{start_col}_lte_{end_col}"

        missing_check = _missing_constraint_check(dataset, check_name, [start_col, end_col], cols)
        if missing_check:
            checks.append(missing_check)
            continue

        invalid = 0
        order_failures = 0
        for row in rows:
            start_raw = _cell(row.get(start_col))
            end_raw = _cell(row.get(end_col))
            if allow_blank_end and not end_raw:
                continue
            try:
                start = _parse_iso_date(start_raw)
                end = _parse_iso_date(end_raw)
            except ValueError:
                invalid += 1
                continue
            if end < start:
                order_failures += 1

        failures = invalid + order_failures
        checks.append(
            Check(
                dataset=dataset,
                check_name=check_name,
                status="PASS" if failures == 0 else "FAIL",
                severity="info" if failures == 0 else "blocker",
                evidence=(
                    f"invalid_dates={invalid}; order_failures={order_failures}; "
                    f"allow_blank_end={allow_blank_end}"
                ),
            )
        )
    return checks


def _foreign_key_checks(
    dataset: str,
    rows: list[dict[str, str]],
    cols: list[str],
    cfg: dict,
    root,
    all_configs: dict[str, dict[str, Any]] | None,
) -> list[Check]:
    checks: list[Check] = []
    if not all_configs:
        return checks

    for rule in cfg.get("foreign_keys", []):
        col = str(rule["column"])
        ref_dataset = str(rule["references_dataset"])
        ref_col = str(rule["references_column"])
        check_name = f"foreign_key:{col}->{ref_dataset}.{ref_col}"

        missing_check = _missing_constraint_check(dataset, check_name, [col], cols)
        if missing_check:
            checks.append(missing_check)
            continue

        ref_cfg = all_configs.get(ref_dataset)
        if not ref_cfg:
            checks.append(
                Check(
                    dataset=dataset,
                    check_name=check_name,
                    status="FAIL",
                    severity="blocker",
                    evidence=f"reference_dataset_missing={ref_dataset}",
                )
            )
            continue

        ref_path = root / str(ref_cfg.get("path", ""))
        if not ref_path.exists():
            checks.append(
                Check(
                    dataset=dataset,
                    check_name=check_name,
                    status="FAIL",
                    severity="blocker",
                    evidence=f"reference_path_missing={ref_path}",
                )
            )
            continue

        ref_rows, ref_cols = _load_csv(ref_path)
        if ref_col not in ref_cols:
            checks.append(
                Check(
                    dataset=dataset,
                    check_name=check_name,
                    status="FAIL",
                    severity="blocker",
                    evidence=f"reference_column_missing={ref_col}; available_columns={ref_cols}",
                )
            )
            continue

        reference_values = {_cell(row.get(ref_col)) for row in ref_rows}
        missing_values = [
            _cell(row.get(col))
            for row in rows
            if _cell(row.get(col)) and _cell(row.get(col)) not in reference_values
        ]
        checks.append(
            Check(
                dataset=dataset,
                check_name=check_name,
                status="PASS" if not missing_values else "FAIL",
                severity="info" if not missing_values else "blocker",
                evidence=f"missing_rows={len(missing_values)}; sample={_sample(missing_values)}",
            )
        )
    return checks


def evaluate_dataset(
    name: str,
    cfg: dict,
    root,
    all_configs: dict[str, dict[str, Any]] | None = None,
) -> list[Check]:
    rel = str(cfg.get("path", ""))
    pk = str(cfg.get("primary_key", ""))
    required_cols = [str(c) for c in cfg.get("required_columns", [])]
    path = root / rel
    exists = path.exists()

    checks: list[Check] = [
        Check(
            dataset=name,
            check_name="dataset_exists",
            status="PASS" if exists else "FAIL",
            severity="info" if exists else "blocker",
            evidence=f"path={rel}; exists={exists}",
        )
    ]
    if not exists:
        return checks

    rows, cols = _load_csv(path)
    missing = sorted(set(required_cols) - set(cols))
    checks.append(
        Check(
            dataset=name,
            check_name="required_columns_present",
            status="PASS" if not missing else "FAIL",
            severity="info" if not missing else "blocker",
            evidence=f"missing_columns={missing}",
        )
    )
    checks.append(
        Check(
            dataset=name,
            check_name="row_count_nonzero",
            status="PASS" if rows else "FAIL",
            severity="info" if rows else "blocker",
            evidence=f"row_count={len(rows)}",
        )
    )

    if pk and pk in cols:
        null_pk = sum(1 for r in rows if not str(r.get(pk, "")).strip())
        dup_pk = sum(c - 1 for c in Counter(str(r.get(pk, "")) for r in rows).values() if c > 1)
        checks.append(
            Check(
                dataset=name,
                check_name="primary_key_not_null",
                status="PASS" if null_pk == 0 else "FAIL",
                severity="info" if null_pk == 0 else "blocker",
                evidence=f"primary_key={pk}; null_rows={null_pk}",
            )
        )
        checks.append(
            Check(
                dataset=name,
                check_name="primary_key_unique",
                status="PASS" if dup_pk == 0 else "FAIL",
                severity="info" if dup_pk == 0 else "blocker",
                evidence=f"primary_key={pk}; duplicate_rows={dup_pk}",
            )
        )
    else:
        checks.append(
            Check(
                dataset=name,
                check_name="primary_key_declared_and_present",
                status="FAIL",
                severity="blocker",
                evidence=f"primary_key={pk}; available_columns={cols}",
            )
        )
    checks.extend(_allowed_value_checks(name, rows, cols, cfg))
    checks.extend(_numeric_range_checks(name, rows, cols, cfg))
    checks.extend(_unique_column_checks(name, rows, cols, cfg))
    checks.extend(_date_order_checks(name, rows, cols, cfg))
    checks.extend(_foreign_key_checks(name, rows, cols, cfg, root, all_configs))
    return checks


def main() -> int:
    root = project_root()
    contract = json.loads(
        (root / "config" / "contracts" / "data_contracts.json").read_text(encoding="utf-8")
    )
    datasets: dict[str, dict[str, Any]] = contract.get("datasets", {})

    all_checks: list[Check] = []
    for name, cfg in datasets.items():
        all_checks.extend(evaluate_dataset(name, cfg, root, datasets))

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
