"""Validated source adapters for publishing production inputs to the raw layer."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from churn.common import project_root

RAW_PREFIX = "raw_"
IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def load_pipeline_config(root: Path | None = None) -> dict[str, Any]:
    root = root or project_root()
    return yaml.safe_load((root / "config" / "pipeline.yml").read_text(encoding="utf-8"))


def load_contracts(root: Path | None = None) -> dict[str, dict[str, Any]]:
    root = root or project_root()
    payload = json.loads(
        (root / "config" / "contracts" / "data_contracts.json").read_text(encoding="utf-8")
    )
    return {
        name: config for name, config in payload["datasets"].items() if name.startswith(RAW_PREFIX)
    }


def load_contract_version(root: Path | None = None) -> int:
    root = root or project_root()
    path = root / "config" / "contracts" / "data_contracts.json"
    return int(json.loads(path.read_text(encoding="utf-8"))["version"]) if path.is_file() else 1


def source_name(contract_name: str) -> str:
    return contract_name.removeprefix(RAW_PREFIX)


class SourceAdapter(ABC):
    """Read complete raw source tables without publishing partial results."""

    @abstractmethod
    def read(self, contracts: dict[str, dict[str, Any]]) -> dict[str, pd.DataFrame]:
        raise NotImplementedError


class CsvSourceAdapter(SourceAdapter):
    def __init__(self, source_directory: Path):
        self.source_directory = source_directory.resolve()

    def read(self, contracts: dict[str, dict[str, Any]]) -> dict[str, pd.DataFrame]:
        frames: dict[str, pd.DataFrame] = {}
        for contract_name, contract in contracts.items():
            filename = Path(str(contract["path"])).name
            path = self.source_directory / filename
            if not path.is_file():
                raise FileNotFoundError(f"Required source file is missing: {path}")
            frames[contract_name] = pd.read_csv(path, dtype=str, keep_default_na=False)
        return frames


class PostgreSqlSourceAdapter(SourceAdapter):
    def __init__(self, dsn: str, schema: str, tables: dict[str, str]):
        if not dsn:
            raise ValueError("PostgreSQL DSN is empty")
        self.dsn = dsn
        self.schema = _validated_identifier(schema)
        self.tables = {name: _validated_identifier(value) for name, value in tables.items()}

    def read(self, contracts: dict[str, dict[str, Any]]) -> dict[str, pd.DataFrame]:
        try:
            from sqlalchemy import MetaData, Table, create_engine, select
        except ImportError as exc:  # pragma: no cover - optional production dependency
            raise RuntimeError(
                'PostgreSQL ingestion requires `pip install -e ".[warehouse]"`.'
            ) from exc

        engine = create_engine(self.dsn, pool_pre_ping=True)
        frames: dict[str, pd.DataFrame] = {}
        try:
            with engine.connect() as connection:
                metadata = MetaData(schema=self.schema)
                for contract_name, contract in contracts.items():
                    name = source_name(contract_name)
                    table = self.tables.get(name)
                    if table is None:
                        raise ValueError(f"Missing PostgreSQL table mapping for {name}")
                    columns = [
                        _validated_identifier(column) for column in contract["required_columns"]
                    ]
                    source_table = Table(table, metadata, autoload_with=connection)
                    missing = sorted(set(columns) - set(source_table.columns.keys()))
                    if missing:
                        raise ValueError(
                            f"PostgreSQL table {self.schema}.{table} is missing {missing}"
                        )
                    statement = select(*(source_table.c[column] for column in columns))
                    frames[contract_name] = pd.read_sql_query(
                        statement, connection, dtype=str
                    ).fillna("")
        finally:
            engine.dispose()
        return frames


def _validated_identifier(value: str) -> str:
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


def validate_source_frames(
    frames: dict[str, pd.DataFrame],
    contracts: dict[str, dict[str, Any]],
) -> None:
    errors: list[str] = []
    for name, contract in contracts.items():
        frame = frames.get(name)
        if frame is None:
            errors.append(f"{name}: dataset not returned by adapter")
            continue
        required = list(contract["required_columns"])
        missing = sorted(set(required) - set(frame.columns))
        if missing:
            errors.append(f"{name}: missing columns {missing}")
            continue
        if frame.empty:
            errors.append(f"{name}: dataset is empty")
        primary_key = str(contract["primary_key"])
        key = frame[primary_key].astype(str).str.strip()
        if key.eq("").any() or key.duplicated().any():
            errors.append(f"{name}: primary key {primary_key} is blank or duplicated")
        for column, allowed in contract.get("allowed_values", {}).items():
            invalid = set(frame[column].astype(str)) - {str(value) for value in allowed}
            if invalid:
                errors.append(f"{name}: {column} has invalid values {sorted(invalid)[:5]}")
        for column, bounds in contract.get("numeric_ranges", {}).items():
            numeric = pd.to_numeric(frame[column], errors="coerce")
            invalid = numeric.isna()
            if "min" in bounds:
                invalid |= numeric.lt(float(bounds["min"]))
            if "max" in bounds:
                invalid |= numeric.gt(float(bounds["max"]))
            if invalid.any():
                errors.append(f"{name}: {column} has {int(invalid.sum())} invalid numeric values")
        for rule in contract.get("date_order_checks", []):
            start = pd.to_datetime(frame[rule["start_column"]], errors="coerce")
            end_raw = frame[rule["end_column"]].astype(str)
            end = pd.to_datetime(end_raw, errors="coerce")
            invalid_end = end.isna() & end_raw.ne("")
            invalid = start.isna() | invalid_end | (end.notna() & end.lt(start))
            if invalid.any():
                errors.append(f"{name}: invalid date order in {int(invalid.sum())} rows")

    for name, contract in contracts.items():
        frame = frames.get(name)
        if frame is None:
            continue
        for rule in contract.get("foreign_keys", []):
            reference = frames.get(str(rule["references_dataset"]))
            if reference is None:
                continue
            invalid = ~frame[str(rule["column"])].isin(reference[str(rule["references_column"])])
            if invalid.any():
                errors.append(
                    f"{name}: foreign key {rule['column']} has {int(invalid.sum())} misses"
                )

    if errors:
        raise ValueError("Source contract validation failed:\n- " + "\n- ".join(errors))


def _frame_bytes(frame: pd.DataFrame, columns: list[str]) -> bytes:
    return frame[columns].to_csv(index=False, lineterminator="\n").encode("utf-8")


def publish_frames(
    frames: dict[str, pd.DataFrame],
    contracts: dict[str, dict[str, Any]],
    root: Path,
    adapter_name: str,
    ingested_at: datetime | None = None,
    reference_date: str | None = None,
) -> dict[str, Any]:
    target_dir = root / "data" / "raw"
    target_dir.mkdir(parents=True, exist_ok=True)
    payloads: dict[Path, bytes] = {}
    datasets: dict[str, Any] = {}
    for name, contract in contracts.items():
        payload = _frame_bytes(frames[name], list(contract["required_columns"]))
        path = root / str(contract["path"])
        payloads[path] = payload
        datasets[source_name(name)] = {
            "rows": len(frames[name]),
            "sha256": hashlib.sha256(payload).hexdigest(),
        }

    timestamp = ingested_at or datetime.now(UTC)
    resolved_reference_date = pd.Timestamp(reference_date or timestamp.date()).date().isoformat()
    manifest = {
        "adapter": adapter_name,
        "contract_version": load_contract_version(root),
        "ingested_at_utc": timestamp.astimezone(UTC).isoformat(),
        "reference_date": resolved_reference_date,
        "datasets": datasets,
    }
    manifest_path = target_dir / "_ingestion_manifest.json"
    payloads[manifest_path] = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()

    staged: list[tuple[Path, Path]] = []
    try:
        for target, payload in payloads.items():
            with tempfile.NamedTemporaryFile(dir=target.parent, delete=False) as handle:
                handle.write(payload)
                staged.append((Path(handle.name), target))
        for temporary, target in staged:
            os.replace(temporary, target)
    finally:
        for temporary, _target in staged:
            temporary.unlink(missing_ok=True)
    return manifest


def verify_ingestion_manifest(
    root: Path | None = None,
    contracts: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    root = root or project_root()
    manifest_path = root / "data" / "raw" / "_ingestion_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Ingestion manifest is missing: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    contracts = contracts or load_contracts(root)
    errors: list[str] = []
    expected_contract_version = load_contract_version(root)
    if int(manifest.get("contract_version", -1)) != expected_contract_version:
        errors.append(
            "contract version mismatch "
            f"(manifest={manifest.get('contract_version')}, current={expected_contract_version})"
        )
    try:
        manifest_reference_date = pd.Timestamp(str(manifest["reference_date"]))
    except (KeyError, TypeError, ValueError):
        errors.append("reference_date is missing or invalid")
    else:
        if pd.isna(manifest_reference_date):
            errors.append("reference_date is missing or invalid")
    for contract_name, contract in contracts.items():
        name = source_name(contract_name)
        record = manifest.get("datasets", {}).get(name)
        path = root / str(contract["path"])
        if record is None or not path.is_file():
            errors.append(f"{name}: file or manifest entry missing")
            continue
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != str(record.get("sha256", "")):
            errors.append(f"{name}: checksum mismatch")
        row_count = len(pd.read_csv(path, usecols=[str(contract["primary_key"])]))
        if row_count != int(record.get("rows", -1)):
            errors.append(
                f"{name}: row count mismatch (manifest={record.get('rows')}, file={row_count})"
            )
    if errors:
        raise ValueError(
            "Published raw batch does not match its manifest:\n- " + "\n- ".join(errors)
        )
    return manifest


def build_adapter(
    adapter_name: str,
    config: dict[str, Any],
    source_directory: Path | None = None,
) -> SourceAdapter:
    if adapter_name == "csv":
        env_name = str(config["csv"]["source_directory_env"])
        configured_directory = os.environ.get(env_name)
        directory = source_directory or (
            Path(configured_directory) if configured_directory else None
        )
        if directory is None:
            raise ValueError(f"Set {env_name} or pass --source-directory for CSV ingestion")
        return CsvSourceAdapter(directory)
    if adapter_name == "postgresql":
        settings = config["postgresql"]
        env_name = str(settings["dsn_env"])
        dsn = os.environ.get(env_name, "")
        if not dsn:
            raise ValueError(f"Set {env_name} for PostgreSQL ingestion")
        return PostgreSqlSourceAdapter(dsn, str(settings["schema"]), dict(settings["tables"]))
    raise ValueError(f"Unsupported adapter: {adapter_name}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--adapter", choices=["csv", "postgresql"])
    parser.add_argument("--source-directory", type=Path)
    args = parser.parse_args()

    root = project_root()
    config = load_pipeline_config(root)
    adapter_name = args.adapter or str(config["default_adapter"])
    reference_date = os.environ.get("CHURN_REFERENCE_DATE")
    if not reference_date:
        raise ValueError("Set CHURN_REFERENCE_DATE to the production batch cutoff date")
    try:
        reference_date = pd.Timestamp(reference_date).date().isoformat()
    except ValueError as exc:
        raise ValueError("CHURN_REFERENCE_DATE must be a valid ISO date") from exc
    adapter = build_adapter(adapter_name, config, args.source_directory)
    contracts = load_contracts(root)
    frames = adapter.read(contracts)
    validate_source_frames(frames, contracts)
    manifest = publish_frames(
        frames,
        contracts,
        root,
        adapter_name,
        reference_date=reference_date,
    )
    total_rows = sum(int(item["rows"]) for item in manifest["datasets"].values())
    print(f"Ingested {len(frames)} datasets ({total_rows:,} rows) via {adapter_name}.")


if __name__ == "__main__":
    main()
