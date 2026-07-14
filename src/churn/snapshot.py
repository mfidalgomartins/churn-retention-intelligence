"""Create deterministic, immutable pipeline snapshots with a checksum manifest."""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import io
import json
import os
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from churn.common import REFERENCE_DATE, project_root
from churn.ingest import load_pipeline_config

ZIP_TIMESTAMP = (1980, 1, 1, 0, 0, 0)


def discover_snapshot_files(root: Path, config: dict[str, Any]) -> list[Path]:
    settings = config["snapshot"]
    paths: set[Path] = set()
    for pattern in settings["include"]:
        paths.update(path for path in root.glob(str(pattern)) if path.is_file())
    excluded = [str(pattern) for pattern in settings.get("exclude", [])]
    return sorted(
        path
        for path in paths
        if not any(
            fnmatch.fnmatch(path.relative_to(root).as_posix(), pattern) for pattern in excluded
        )
    )


def build_manifest(root: Path, files: list[Path], as_of: str) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    for path in files:
        payload = path.read_bytes()
        entries.append(
            {
                "path": path.relative_to(root).as_posix(),
                "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
    source_manifest = root / "data" / "raw" / "_ingestion_manifest.json"
    source_adapter = "synthetic"
    if source_manifest.is_file():
        source_adapter = str(json.loads(source_manifest.read_text(encoding="utf-8"))["adapter"])
    content_digest = hashlib.sha256(
        json.dumps(entries, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()
    return {
        "schema_version": 1,
        "as_of_date": as_of,
        "source_adapter": source_adapter,
        "content_sha256": content_digest,
        "file_count": len(entries),
        "total_bytes": sum(int(entry["bytes"]) for entry in entries),
        "files": entries,
    }


def build_snapshot_bytes(root: Path, manifest: dict[str, Any]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(
        buffer,
        mode="w",
        compression=zipfile.ZIP_DEFLATED,
        compresslevel=9,
    ) as archive:
        manifest_payload = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()
        _write_zip_entry(archive, "manifest.json", manifest_payload)
        for entry in manifest["files"]:
            path = root / str(entry["path"])
            _write_zip_entry(archive, str(entry["path"]), path.read_bytes())
    return buffer.getvalue()


def _write_zip_entry(archive: zipfile.ZipFile, name: str, payload: bytes) -> None:
    info = zipfile.ZipInfo(name, date_time=ZIP_TIMESTAMP)
    info.compress_type = zipfile.ZIP_DEFLATED
    info.external_attr = 0o100644 << 16
    archive.writestr(info, payload, compress_type=zipfile.ZIP_DEFLATED, compresslevel=9)


def publish_snapshot(root: Path, as_of: str) -> tuple[Path, Path]:
    config = load_pipeline_config(root)
    files = discover_snapshot_files(root, config)
    if not files:
        raise ValueError("Snapshot has no eligible files; run the pipeline first")
    manifest = build_manifest(root, files, as_of)
    snapshot_id = f"{as_of}-{manifest['content_sha256'][:12]}"
    output_dir = root / "outputs" / "snapshots"
    output_dir.mkdir(parents=True, exist_ok=True)
    archive_path = output_dir / f"retention-snapshot-{snapshot_id}.zip"
    manifest_path = output_dir / f"retention-snapshot-{snapshot_id}.manifest.json"
    archive_payload = build_snapshot_bytes(root, manifest)
    manifest_payload = (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode()

    for path, payload in ((archive_path, archive_payload), (manifest_path, manifest_payload)):
        if path.exists() and path.read_bytes() != payload:
            raise FileExistsError(f"Immutable snapshot collision: {path}")
        if not path.exists():
            with tempfile.NamedTemporaryFile(dir=output_dir, delete=False) as handle:
                handle.write(payload)
                temporary = Path(handle.name)
            try:
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
    return archive_path, manifest_path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", default=REFERENCE_DATE.date().isoformat())
    args = parser.parse_args()
    archive, manifest = publish_snapshot(project_root(), args.as_of)
    print(f"Snapshot published: {archive.relative_to(project_root())}")
    print(f"Manifest published: {manifest.relative_to(project_root())}")


if __name__ == "__main__":
    main()
