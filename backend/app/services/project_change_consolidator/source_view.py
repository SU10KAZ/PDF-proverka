"""Read-only source of a consolidation: a completed V3 run or a frozen bundle.

Allowlist loader: the V3 result, the Miner region outputs (miner_results or
the Miner checkpoint), the semantic map and the source package.  Nothing
else is opened; a path that names review/audit/evaluation material is refused
outright, so evaluation labels cannot reach a model payload through here.
Every file is hash-checked; the result sha256 is the binding key of a shadow run.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .identity import HintTable, canonical_hints

# Whole tokens between non-letters: "..._quality_review/", "OPUS_64_AUDIT.json" are refused, while the
# installation root ("auditmanager/corpus-audits") is not a review file and passes.
FORBIDDEN_PATH_TOKENS = re.compile(
    r"(?<![a-z])(?:reviews?|audit|evaluations?|source[_-]?first|grouping|truth|golden|labels?)(?![a-z])",
    re.IGNORECASE)


class BundleError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code


def sha256_file(path: Path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _guard(path: Path) -> Path:
    path = Path(path)
    if FORBIDDEN_PATH_TOKENS.search(str(path)):
        raise BundleError("FORBIDDEN_SOURCE_PATH", f"{path} is not an allowed consolidation source")
    return path


def _read_verified(path: Path, expected: str | None) -> tuple[dict[str, Any], str]:
    path = _guard(path)
    data = path.read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    if expected is not None and digest != expected:
        raise BundleError("SOURCE_SHA_MISMATCH", f"{path}: {digest} != {expected}")
    return json.loads(data.decode("utf-8")), digest


@dataclass
class SourceBundle:
    pair_id: str
    session_id: str
    source_run_id: str
    result: dict[str, Any]
    result_sha256: str
    region_sources: list[tuple[str, list[dict[str, Any]]]]
    hint_method: str
    semantic_map: dict[str, Any]
    source_package_dir: Path
    files: list[dict[str, Any]]
    page_sha256: dict[str, str] | None = None
    _pages: dict[tuple[str, int], dict[str, Any]] = field(default_factory=dict)

    def hint_table(self) -> HintTable:
        return canonical_hints(self.result.get("unresolved_hints") or [], self.region_sources,
                               method=self.hint_method, source_files=self.files,
                               source_run_id=self.source_run_id, result_sha256=self.result_sha256)

    def page(self, side: str, page_no: int) -> dict[str, Any] | None:
        """page.json of the run's source package (hash-checked when a manifest is known)."""
        key = (side, int(page_no))
        if key not in self._pages:
            rel = f"{side.lower()}/p{int(page_no):03d}/page.json"
            path = _guard(self.source_package_dir / rel)
            if not path.is_file():
                self._pages[key] = None  # type: ignore[assignment]
            else:
                data = path.read_bytes()
                if self.page_sha256 is not None:
                    expected = self.page_sha256.get(rel)
                    if expected is None or hashlib.sha256(data).hexdigest() != expected:
                        raise BundleError("SOURCE_PACKAGE_SHA_MISMATCH", rel)
                self._pages[key] = json.loads(data.decode("utf-8"))
        return self._pages[key]

    def block(self, side: str, page_no: int, block_id: str) -> dict[str, Any] | None:
        record = self.page(side, page_no)
        if not record:
            return None
        found = [b for b in record.get("blocks") or [] if b.get("block_id") == block_id]
        return found[0] if len(found) == 1 else None

    def identity(self) -> dict[str, Any]:
        return {"pair_id": self.pair_id, "session_id": self.session_id, "source_run_id": self.source_run_id,
                "source_result_sha256": self.result_sha256, "hint_method": self.hint_method,
                "files": self.files, "source_package_dir": str(self.source_package_dir),
                "source_package_pages_verified": self.page_sha256 is not None}


def _region_sources(kind: str, value: dict[str, Any]) -> list[tuple[str, list[dict[str, Any]]]]:
    if kind == "miner_results":
        return [(str(r["region_id"]), list(r.get("unresolved_hints") or [])) for r in value.get("regions") or []]
    if kind == "miner_checkpoint":
        return [(str(r["region_id"]), list((r.get("result") or {}).get("unresolved_hints") or []))
                for r in value.get("regions") or []]
    raise BundleError("HINT_REGION_SOURCE_MISSING", f"unknown region source kind {kind!r}")


def load_frozen_bundle(spec: dict[str, Any]) -> SourceBundle:
    """Frozen research bundle with explicit files and expected sha256 (never discovered by name).

    ``spec``: {source_run_id, result{path,sha256}, hint_regions{kind,path,sha256},
    semantic_map{path,sha256}, source_package{dir, page_sha256{rel: sha}}}
    """
    result, result_sha = _read_verified(Path(spec["result"]["path"]), spec["result"]["sha256"])
    if str(result.get("run_id")) != str(spec["source_run_id"]):
        raise BundleError("SOURCE_RUN_MISMATCH", f"result run_id {result.get('run_id')} != {spec['source_run_id']}")
    hr = spec["hint_regions"]
    regions_value, regions_sha = _read_verified(Path(hr["path"]), hr["sha256"])
    semantic_map, map_sha = _read_verified(Path(spec["semantic_map"]["path"]), spec["semantic_map"]["sha256"])
    package = spec["source_package"]
    files = [{"role": "result", "path": str(spec["result"]["path"]), "sha256": result_sha},
             {"role": hr["kind"], "path": str(hr["path"]), "sha256": regions_sha},
             {"role": "semantic_map", "path": str(spec["semantic_map"]["path"]), "sha256": map_sha},
             {"role": "source_package", "path": str(package["dir"]),
              "sha256": hashlib.sha256(json.dumps(package["page_sha256"], sort_keys=True).encode()).hexdigest()}]
    return SourceBundle(
        pair_id=str(result["pair_id"]), session_id=str(result.get("session_id") or ""),
        source_run_id=str(result["run_id"]), result=result, result_sha256=result_sha,
        region_sources=_region_sources(hr["kind"], regions_value), hint_method=(
            "MINER_RESULTS_REGION_ORDER" if hr["kind"] == "miner_results" else "MINER_CHECKPOINT_REGION_ORDER"),
        semantic_map=semantic_map, source_package_dir=_guard(Path(package["dir"])), files=files,
        page_sha256=dict(package["page_sha256"]))


def load_completed_run(session_id: str, pair_id: str, run_id: str) -> SourceBundle:
    """A completed, validated V3 run of the production run store (read-only)."""
    from backend.app.services.project_change_v3 import run_storage

    manifest = run_storage.validate(session_id, pair_id, run_id)  # COMPLETED_FROZEN + artifact hashes
    directory = run_storage.run_dir(session_id, pair_id, run_id)
    expected = manifest["artifacts"]["project_change_v3_result"]["sha256"]
    result, result_sha = _read_verified(directory / "project_change_v3_result.json", expected)
    map_expected = manifest["artifacts"]["project_change_v3_semantic_map"]["sha256"]
    semantic_map, map_sha = _read_verified(directory / "project_change_v3_semantic_map.json", map_expected)
    miner_path = directory / "project_change_v3_miner_results.json"
    if not miner_path.is_file():
        raise BundleError("HINT_REGION_SOURCE_MISSING", str(miner_path))
    miner, miner_sha = _read_verified(miner_path, None)
    if str(miner.get("run_id")) != str(run_id):
        raise BundleError("SOURCE_RUN_MISMATCH", "miner results belong to another run")
    files = [{"role": "result", "path": str(directory / "project_change_v3_result.json"), "sha256": result_sha},
             {"role": "miner_results", "path": str(miner_path), "sha256": miner_sha},
             {"role": "semantic_map", "path": str(directory / "project_change_v3_semantic_map.json"),
              "sha256": map_sha},
             {"role": "source_package", "path": str(directory / "project_change_v3" / "source"), "sha256": ""}]
    return SourceBundle(
        pair_id=pair_id, session_id=session_id, source_run_id=run_id, result=result, result_sha256=result_sha,
        region_sources=_region_sources("miner_results", miner), hint_method="MINER_RESULTS_REGION_ORDER",
        semantic_map=semantic_map, source_package_dir=directory / "project_change_v3" / "source", files=files)
