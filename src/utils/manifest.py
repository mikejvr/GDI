import json
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List

from src.utils.io_sorted import read_shards_sorted

def compute_file_hash(path: Path, algo: str = "sha256") -> str:
    hasher = hashlib.new(algo)
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def generate_phase1_manifest(data_dir: Path, output_dir: Path) -> Dict[str, Any]:
    # Resolve to absolute paths
    data_dir = Path(data_dir).resolve()
    output_dir = Path(output_dir).resolve()
    logs_dir = output_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    manifest_shards = []
    validation_results = []

    # The repo root is the parent of data_dir (assuming data_dir is e.g., /.../nexus/data)
    repo_root = data_dir.parent

    for shard_path in read_shards_sorted(data_dir):
        # Compute relative path from repo root (e.g., "data/shard_xxx.json")
        try:
            rel_path = shard_path.relative_to(repo_root)
        except ValueError:
            # Fallback: just use the absolute path as string (should not happen)
            rel_path = str(shard_path)

        file_hash = compute_file_hash(shard_path)

        # Basic validation (schema check will be separate)
        try:
            with shard_path.open("r") as f:
                data = json.load(f)
            valid = True
            error = None
        except Exception as e:
            valid = False
            error = str(e)

        manifest_shards.append({
            "path": str(rel_path),
            "hash": file_hash,
            "size_bytes": shard_path.stat().st_size,
            "last_modified_utc": datetime.fromtimestamp(shard_path.stat().st_mtime, tz=timezone.utc).isoformat()
        })

        validation_results.append({
            "path": str(rel_path),
            "valid": valid,
            "error": error,
            "hash_matches": None
        })

    manifest_path = output_dir / "phase1_manifest.json"
    with manifest_path.open("w") as f:
        json.dump({
            "version": "1.0",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "shards": manifest_shards
        }, f, indent=2)

    hash_log = logs_dir / "phase1_hashes.json"
    with hash_log.open("w") as f:
        json.dump({
            "version": "1.0",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "shards": manifest_shards
        }, f, indent=2)

    val_log = logs_dir / "phase1_validation.json"
    with val_log.open("w") as f:
        json.dump({
            "version": "1.0",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "results": validation_results
        }, f, indent=2)

    return {"manifest_path": str(manifest_path), "logs": str(logs_dir)}
