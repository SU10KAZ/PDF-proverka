"""Contract shared by Gemma enrichment producers and pipeline guards."""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from block_markdown import strip_gemma_enrichment_sections


GEMMA_ENRICHMENT_SCHEMA_VERSION = 2
GEMMA_ENRICHMENT_STAGE = "gemma_enrichment"

GEMMA_BASE_PROFILE = "gemma_100_base"
GEMMA_HIGH_DETAIL_PROFILE = "gemma_300_high_detail"
STAGE02_PROFILE = "stage02_100"

GEMMA_BASE_BLOCKS_DIRNAME = "blocks_gemma_100"
GEMMA_HIGH_DETAIL_BLOCKS_DIRNAME = "blocks_gemma_300"
STAGE02_BLOCKS_DIRNAME = "blocks_stage02_100"

# Backward-compatible aliases for callers that historically meant
# "the production Gemma crop source of truth".
GEMMA_BLOCKS_DIRNAME = GEMMA_BASE_BLOCKS_DIRNAME

GEMMA_BASE_CROP_POLICY = {
    "profile": GEMMA_BASE_PROFILE,
    "dpi": 100,
    "min_long_side": 800,
    "compact": False,
    "skip_small": False,
}
GEMMA_HIGH_DETAIL_CROP_POLICY = {
    "profile": GEMMA_HIGH_DETAIL_PROFILE,
    "dpi": 300,
    "min_long_side": 800,
    "compact": False,
    "skip_small": False,
}
STAGE02_CROP_POLICY = {
    "profile": STAGE02_PROFILE,
    "dpi": 100,
    "min_long_side": 800,
    "compact": False,
    "skip_small": False,
}

# Backward-compatible alias for older imports.
GEMMA_ENRICHMENT_CROP_POLICY = GEMMA_BASE_CROP_POLICY

FINAL_PROFILES = {GEMMA_BASE_PROFILE, GEMMA_HIGH_DETAIL_PROFILE, "none"}
BASE_STATUSES = {"ok", "partial_ok", "failed", "missing"}
HIGH_DETAIL_STATUSES = {"not_needed", "ok", "partial_ok", "failed", "missing", "skipped_large_block"}
COVERAGE_STATUSES = {"ok", "partial", "missing_gemma_enrichment", "high_detail_skipped_large_block"}
WARNING_LARGE_BLOCK = "high_detail_large_block"

ENRICHMENT_MARKER_RE = re.compile(
    r"^<!--\s*ENRICHMENT:\s*(?P<model>\S+)\s*@\s*(?P<ts>\S+)\s+"
    r"blocks=(?P<ok>\d+)/(?P<total>\d+).*?-->\s*\n",
    re.MULTILINE,
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")


def normalize_crop_policy(policy: dict[str, Any] | None) -> dict[str, Any]:
    policy = policy or {}
    profile = str(policy.get("profile") or "").strip()
    min_long_side = (
        policy.get("min_long_side")
        if policy.get("min_long_side") is not None
        else policy.get("min_long_side_px")
    )
    if min_long_side in (None, "", 0):
        min_long_side = 800
    return {
        "profile": profile,
        "dpi": int(policy.get("dpi") or 0),
        "min_long_side": int(min_long_side or 0),
        "compact": bool(policy.get("compact", False)),
        "skip_small": bool(policy.get("skip_small", True)),
    }


def gemma_base_crop_policy() -> dict[str, Any]:
    return dict(GEMMA_BASE_CROP_POLICY)


def gemma_high_detail_crop_policy() -> dict[str, Any]:
    return dict(GEMMA_HIGH_DETAIL_CROP_POLICY)


def gemma_enrichment_crop_policy() -> dict[str, Any]:
    return gemma_base_crop_policy()


def stage02_crop_policy() -> dict[str, Any]:
    return dict(STAGE02_CROP_POLICY)


def _output_dir(project_dir: Path | str, dirname: str) -> Path:
    return Path(project_dir) / "_output" / dirname


def gemma_base_blocks_dir(project_dir: Path | str) -> Path:
    return _output_dir(project_dir, GEMMA_BASE_BLOCKS_DIRNAME)


def gemma_base_blocks_index_path(project_dir: Path | str) -> Path:
    return gemma_base_blocks_dir(project_dir) / "index.json"


def gemma_high_detail_blocks_dir(project_dir: Path | str) -> Path:
    return _output_dir(project_dir, GEMMA_HIGH_DETAIL_BLOCKS_DIRNAME)


def gemma_high_detail_blocks_index_path(project_dir: Path | str) -> Path:
    return gemma_high_detail_blocks_dir(project_dir) / "index.json"


def gemma_blocks_dir(project_dir: Path | str) -> Path:
    return gemma_base_blocks_dir(project_dir)


def gemma_blocks_index_path(project_dir: Path | str) -> Path:
    return gemma_base_blocks_index_path(project_dir)


def stage02_blocks_dir(project_dir: Path | str) -> Path:
    return _output_dir(project_dir, STAGE02_BLOCKS_DIRNAME)


def stage02_blocks_index_path(project_dir: Path | str) -> Path:
    return stage02_blocks_dir(project_dir) / "index.json"


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _canonical_json_bytes(data: Any) -> bytes:
    return json.dumps(
        data,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, OSError):
        return {}


def blocks_index_hash(index_path: Path) -> str:
    if not index_path.exists():
        return ""
    data = load_json(index_path)
    if data:
        return sha256_bytes(_canonical_json_bytes(data))
    try:
        return sha256_bytes(index_path.read_bytes())
    except OSError:
        return ""


def crop_policy_from_index(index: dict[str, Any]) -> dict[str, Any]:
    return normalize_crop_policy({
        "profile": index.get("profile"),
        "dpi": index.get("dpi") if index.get("dpi") is not None else 100,
        "min_long_side": index.get("min_long_side"),
        "compact": index.get("compact", False),
        "skip_small": index.get("skip_small", True),
    })


def crop_index_matches_policy(index_path: Path, policy: dict[str, Any]) -> bool:
    index = load_json(index_path)
    if not index:
        return False
    expected = normalize_crop_policy(policy)
    actual = crop_policy_from_index(index)
    if not actual.get("profile") and expected.get("profile"):
        actual["profile"] = expected["profile"]
    return actual == expected


def strip_gemma_enrichment(md_text: str) -> str:
    """Return the canonical source MD without Gemma marker/enriched sections."""
    md_text = ENRICHMENT_MARKER_RE.sub("", md_text, count=1)
    return strip_gemma_enrichment_sections(md_text)


def md_hash_before_enrichment(md_path: Path) -> str:
    if not md_path.exists():
        return ""
    try:
        return sha256_text(strip_gemma_enrichment(md_path.read_text(encoding="utf-8")))
    except OSError:
        return ""


def coverage_ratio(blocks_ok: int, blocks_total: int) -> float:
    if blocks_total <= 0:
        return 0.0
    return round(max(0, blocks_ok) / blocks_total, 6)


def _image_blocks_from_index(index_path: Path) -> list[dict[str, Any]]:
    index = load_json(index_path)
    blocks = index.get("blocks")
    if not isinstance(blocks, list):
        return []
    return [
        block for block in blocks
        if isinstance(block, dict) and (block.get("block_type") or "image").lower() == "image"
    ]


def _covered_from_block_record(block: dict[str, Any]) -> bool:
    return str(block.get("final_profile") or "none") != "none"


def _derive_summary_status(blocks: list[dict[str, Any]], blocks_total: int) -> str:
    if blocks_total <= 0:
        return "no_blocks"
    if not blocks:
        return "partial"
    if all(
        str(block.get("coverage_status") or "") == "ok"
        and str(block.get("final_profile") or "") == GEMMA_BASE_PROFILE
        for block in blocks
    ):
        return "ok"
    if all(str(block.get("coverage_status") or "") == "ok" for block in blocks):
        return "ok"
    return "partial"


def _default_summary_blocks(
    project_dir: Path,
    *,
    blocks_total: int,
    blocks_ok: int,
    uncovered_block_ids: list[str],
) -> list[dict[str, Any]]:
    base_blocks = _image_blocks_from_index(gemma_base_blocks_index_path(project_dir))
    block_ids = [str(block.get("block_id") or "") for block in base_blocks if block.get("block_id")]
    while len(block_ids) < blocks_total:
        block_ids.append(f"AUTO-{len(block_ids) + 1:03d}")

    uncovered = set(uncovered_block_ids)
    out: list[dict[str, Any]] = []
    ok_assigned = 0
    for block_id in block_ids[:blocks_total]:
        if block_id in uncovered:
            out.append({
                "block_id": block_id,
                "base_status": "failed",
                "high_detail_status": "not_needed",
                "final_profile": "none",
                "coverage_status": "missing_gemma_enrichment",
                "warnings": [],
            })
            continue
        if ok_assigned < blocks_ok:
            ok_assigned += 1
            out.append({
                "block_id": block_id,
                "base_status": "ok",
                "high_detail_status": "not_needed",
                "final_profile": GEMMA_BASE_PROFILE,
                "coverage_status": "ok",
                "warnings": [],
            })
        else:
            out.append({
                "block_id": block_id,
                "base_status": "failed",
                "high_detail_status": "not_needed",
                "final_profile": "none",
                "coverage_status": "missing_gemma_enrichment",
                "warnings": [],
            })
    return out


def _summary_blocks_or_default(
    project_dir: Path,
    *,
    blocks_total: int,
    blocks_ok: int,
    extra: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    extra = extra or {}
    blocks = extra.get("blocks")
    if isinstance(blocks, list):
        return blocks
    uncovered = extra.get("uncovered_block_ids") or []
    if not isinstance(uncovered, list):
        uncovered = []
    return _default_summary_blocks(
        project_dir,
        blocks_total=blocks_total,
        blocks_ok=blocks_ok,
        uncovered_block_ids=[str(item) for item in uncovered if item],
    )


def _count_high_detail_skipped_large(blocks: list[dict[str, Any]]) -> int:
    return sum(1 for block in blocks if block.get("high_detail_status") == "skipped_large_block")


def _large_block_skipped_ids(blocks: list[dict[str, Any]]) -> list[str]:
    return [
        str(block.get("block_id") or "")
        for block in blocks
        if block.get("high_detail_status") == "skipped_large_block" and block.get("block_id")
    ]


def _uncovered_ids(blocks: list[dict[str, Any]]) -> list[str]:
    return [
        str(block.get("block_id") or "")
        for block in blocks
        if block.get("block_id") and not _covered_from_block_record(block)
    ]


def build_gemma_summary(
    *,
    status: str,
    project_dir: Path,
    md_path: Path,
    model: str,
    blocks_total: int,
    blocks_ok: int,
    blocks_failed: int,
    blocks_skipped: int = 0,
    created_at: str | None = None,
    crop_policy: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    project_dir = Path(project_dir)
    extra = dict(extra or {})
    base_policy = normalize_crop_policy(extra.get("base_crop_policy") or crop_policy or GEMMA_BASE_CROP_POLICY)
    high_detail_policy = normalize_crop_policy(extra.get("high_detail_crop_policy") or GEMMA_HIGH_DETAIL_CROP_POLICY)
    base_index_path = gemma_base_blocks_index_path(project_dir)
    high_detail_index_path = gemma_high_detail_blocks_index_path(project_dir)
    block_records = _summary_blocks_or_default(
        project_dir,
        blocks_total=int(blocks_total or 0),
        blocks_ok=int(blocks_ok or 0),
        extra=extra,
    )

    covered_blocks = sum(1 for block in block_records if _covered_from_block_record(block))
    uncovered_block_ids = _uncovered_ids(block_records)
    large_block_skipped_ids = _large_block_skipped_ids(block_records)
    high_detail_candidates = int(extra.get("high_detail_candidates") or 0)
    high_detail_ok = int(extra.get("high_detail_ok") or 0)
    high_detail_skipped_large = int(
        extra.get("high_detail_skipped_large")
        if extra.get("high_detail_skipped_large") is not None
        else _count_high_detail_skipped_large(block_records)
    )
    derived_status = _derive_summary_status(block_records, int(blocks_total or 0))
    base_hash = blocks_index_hash(base_index_path)
    high_detail_hash = extra.get("high_detail_blocks_index_hash")
    if high_detail_hash is None:
        high_detail_hash = blocks_index_hash(high_detail_index_path) or None

    summary = {
        "schema_version": GEMMA_ENRICHMENT_SCHEMA_VERSION,
        "stage": GEMMA_ENRICHMENT_STAGE,
        "status": status or derived_status,
        "model": model,
        "base_profile": base_policy["profile"] or GEMMA_BASE_PROFILE,
        "base_crop_policy": base_policy,
        "base_blocks_index_hash": base_hash,
        "high_detail_profile": high_detail_policy["profile"] or GEMMA_HIGH_DETAIL_PROFILE,
        "high_detail_crop_policy": high_detail_policy,
        "high_detail_blocks_index_hash": high_detail_hash,
        "md_hash_before_enrichment": md_hash_before_enrichment(md_path),
        "blocks_total": int(blocks_total or 0),
        "base_blocks_ok": int(blocks_ok or 0),
        "high_detail_candidates": high_detail_candidates,
        "high_detail_ok": high_detail_ok,
        "high_detail_skipped_large": high_detail_skipped_large,
        "blocks_failed": int(blocks_failed or 0),
        "blocks_skipped": int(blocks_skipped or 0),
        "coverage_ratio": coverage_ratio(covered_blocks, int(blocks_total or 0)),
        "uncovered_block_ids": uncovered_block_ids,
        "large_block_skipped_ids": large_block_skipped_ids,
        "created_at": created_at or utc_now_iso(),
        "blocks": block_records,
        # Compatibility aliases used by older status/report code.
        "crop_policy": base_policy,
        "blocks_index_hash": base_hash,
        "blocks_ok": covered_blocks,
    }
    if "uncovered_blocks" in extra:
        summary["uncovered_blocks"] = extra["uncovered_blocks"]
    if extra:
        for key, value in extra.items():
            if key in summary:
                continue
            summary[key] = value
    if not summary.get("status"):
        summary["status"] = derived_status
    return summary


def _validation_error(
    *,
    reason_code: str,
    reason: str,
    summary: dict[str, Any] | None = None,
    metadata_valid: bool = False,
) -> dict[str, Any]:
    payload = {
        "valid": False,
        "metadata_valid": metadata_valid,
        "reason_code": reason_code,
        "reason": reason,
    }
    if summary is not None:
        payload["summary"] = summary
    return payload


def _normalize_block_record(block: dict[str, Any]) -> dict[str, Any]:
    return {
        "block_id": str(block.get("block_id") or ""),
        "base_status": str(block.get("base_status") or ""),
        "high_detail_status": str(block.get("high_detail_status") or ""),
        "final_profile": str(block.get("final_profile") or ""),
        "coverage_status": str(block.get("coverage_status") or ""),
        "warnings": block.get("warnings") if isinstance(block.get("warnings"), list) else [],
    }


def validate_gemma_summary(
    project_dir: Path,
    *,
    md_path: Path | None = None,
    summary: dict[str, Any] | None = None,
    min_coverage: float = 0.0,
    require_marker: bool = False,  # Deprecated: marker is human-readable only.
) -> dict[str, Any]:
    del require_marker
    project_dir = Path(project_dir)
    out_dir = project_dir / "_output"
    summary_path = out_dir / "gemma_enrichment_summary.json"
    if summary is None:
        summary = load_json(summary_path)

    if not summary:
        return _validation_error(
            reason_code="missing_summary",
            reason="gemma_enrichment_summary.json отсутствует",
        )

    if summary.get("schema_version") != GEMMA_ENRICHMENT_SCHEMA_VERSION:
        return _validation_error(
            reason_code="schema_mismatch",
            reason="schema_version summary устарел или отсутствует",
            summary=summary,
        )

    if summary.get("stage") != GEMMA_ENRICHMENT_STAGE:
        return _validation_error(
            reason_code="stage_mismatch",
            reason="summary относится не к gemma_enrichment",
            summary=summary,
        )

    expected_base_policy = normalize_crop_policy(GEMMA_BASE_CROP_POLICY)
    actual_base_policy = normalize_crop_policy(summary.get("base_crop_policy") or summary.get("crop_policy"))
    if not actual_base_policy.get("profile"):
        actual_base_policy["profile"] = expected_base_policy["profile"]
    if actual_base_policy != expected_base_policy:
        return _validation_error(
            reason_code="crop_policy_mismatch",
            reason="base_crop_policy summary не совпадает с Gemma base production policy",
            summary=summary,
        )

    base_index_path = gemma_base_blocks_index_path(project_dir)
    current_base_hash = blocks_index_hash(base_index_path)
    if not current_base_hash:
        return _validation_error(
            reason_code="missing_blocks",
            reason=f"_output/{GEMMA_BASE_BLOCKS_DIRNAME}/index.json отсутствует",
            summary=summary,
        )
    if not crop_index_matches_policy(base_index_path, expected_base_policy):
        return _validation_error(
            reason_code="crop_policy_mismatch",
            reason=f"текущий {GEMMA_BASE_BLOCKS_DIRNAME}/index.json не соответствует Gemma base crop policy",
            summary=summary,
        )
    expected_hash = summary.get("base_blocks_index_hash") or summary.get("blocks_index_hash")
    if expected_hash != current_base_hash:
        return _validation_error(
            reason_code="blocks_index_hash_mismatch",
            reason=f"{GEMMA_BASE_BLOCKS_DIRNAME}/index.json изменился после Gemma enrichment",
            summary=summary,
        )

    high_detail_hash = summary.get("high_detail_blocks_index_hash")
    if high_detail_hash:
        expected_high_policy = normalize_crop_policy(GEMMA_HIGH_DETAIL_CROP_POLICY)
        high_detail_index_path = gemma_high_detail_blocks_index_path(project_dir)
        current_high_hash = blocks_index_hash(high_detail_index_path)
        if not current_high_hash:
            return _validation_error(
                reason_code="missing_high_detail_blocks",
                reason=f"_output/{GEMMA_HIGH_DETAIL_BLOCKS_DIRNAME}/index.json отсутствует",
                summary=summary,
            )
        if not crop_index_matches_policy(high_detail_index_path, expected_high_policy):
            return _validation_error(
                reason_code="high_detail_crop_policy_mismatch",
                reason=f"текущий {GEMMA_HIGH_DETAIL_BLOCKS_DIRNAME}/index.json не соответствует Gemma high-detail crop policy",
                summary=summary,
            )
        if high_detail_hash != current_high_hash:
            return _validation_error(
                reason_code="high_detail_blocks_index_hash_mismatch",
                reason=f"{GEMMA_HIGH_DETAIL_BLOCKS_DIRNAME}/index.json изменился после Gemma enrichment",
                summary=summary,
            )

    if md_path is not None:
        current_md_hash = md_hash_before_enrichment(md_path)
        if summary.get("md_hash_before_enrichment") != current_md_hash:
            return _validation_error(
                reason_code="md_hash_mismatch",
                reason="MD изменился после Gemma enrichment",
                summary=summary,
            )

    blocks_total = int(summary.get("blocks_total") or 0)
    base_blocks_ok = int(summary.get("base_blocks_ok") or 0)
    blocks_failed = int(summary.get("blocks_failed") or 0)
    if blocks_total < 0 or base_blocks_ok < 0 or blocks_failed < 0:
        return _validation_error(
            reason_code="coverage_counts_invalid",
            reason="base_blocks_ok/blocks_total/blocks_failed некорректны",
            summary=summary,
            metadata_valid=True,
        )

    blocks = summary.get("blocks")
    if not isinstance(blocks, list):
        return _validation_error(
            reason_code="blocks_missing",
            reason="summary.blocks отсутствует",
            summary=summary,
            metadata_valid=True,
        )
    if len(blocks) != blocks_total:
        return _validation_error(
            reason_code="blocks_total_mismatch",
            reason="blocks_total не совпадает с числом block decisions",
            summary=summary,
            metadata_valid=True,
        )

    base_index_blocks = _image_blocks_from_index(base_index_path)
    base_index_ids = {
        str(block.get("block_id") or "")
        for block in base_index_blocks
        if block.get("block_id")
    }
    block_ids: list[str] = []
    covered_count = 0
    normalized_blocks: list[dict[str, Any]] = []
    for raw_block in blocks:
        if not isinstance(raw_block, dict):
            return _validation_error(
                reason_code="final_decisions_missing",
                reason="summary.blocks содержит некорректный элемент",
                summary=summary,
                metadata_valid=True,
            )
        block = _normalize_block_record(raw_block)
        block_id = block["block_id"]
        if not block_id:
            return _validation_error(
                reason_code="final_decisions_missing",
                reason="в summary.blocks есть запись без block_id",
                summary=summary,
                metadata_valid=True,
            )
        if block_id in block_ids:
            return _validation_error(
                reason_code="duplicate_block_id",
                reason=f"дублирующийся block_id в summary.blocks: {block_id}",
                summary=summary,
                metadata_valid=True,
            )
        block_ids.append(block_id)
        if block["base_status"] not in BASE_STATUSES:
            return _validation_error(
                reason_code="final_decisions_missing",
                reason=f"base_status некорректен для {block_id}",
                summary=summary,
                metadata_valid=True,
            )
        if block["high_detail_status"] not in HIGH_DETAIL_STATUSES:
            return _validation_error(
                reason_code="final_decisions_missing",
                reason=f"high_detail_status некорректен для {block_id}",
                summary=summary,
                metadata_valid=True,
            )
        if block["final_profile"] not in FINAL_PROFILES:
            return _validation_error(
                reason_code="final_decisions_missing",
                reason=f"final_profile отсутствует или некорректен для {block_id}",
                summary=summary,
                metadata_valid=True,
            )
        if block["coverage_status"] not in COVERAGE_STATUSES:
            return _validation_error(
                reason_code="final_decisions_missing",
                reason=f"coverage_status отсутствует или некорректен для {block_id}",
                summary=summary,
                metadata_valid=True,
            )
        if block["final_profile"] == "none" and block["coverage_status"] != "missing_gemma_enrichment":
            return _validation_error(
                reason_code="final_decisions_missing",
                reason=f"final_profile=none требует coverage_status=missing_gemma_enrichment ({block_id})",
                summary=summary,
                metadata_valid=True,
            )
        if block["high_detail_status"] == "skipped_large_block" and block["final_profile"] == "none":
            return _validation_error(
                reason_code="final_decisions_missing",
                reason=f"skipped_large_block без fallback base enrichment недопустим ({block_id})",
                summary=summary,
                metadata_valid=True,
            )
        if _covered_from_block_record(block):
            covered_count += 1
        normalized_blocks.append(block)

    if base_index_ids and set(block_ids) != base_index_ids:
        return _validation_error(
            reason_code="blocks_total_mismatch",
            reason=f"summary.blocks не совпадает с _output/{GEMMA_BASE_BLOCKS_DIRNAME}/index.json",
            summary=summary,
            metadata_valid=True,
        )

    ratio = float(summary.get("coverage_ratio") or 0.0)
    expected_ratio = coverage_ratio(covered_count, blocks_total)
    if round(ratio, 6) != expected_ratio:
        return _validation_error(
            reason_code="coverage_ratio_mismatch",
            reason="coverage_ratio не соответствует final block decisions",
            summary=summary,
            metadata_valid=True,
        )

    uncovered_ids = _uncovered_ids(normalized_blocks)
    if list(summary.get("uncovered_block_ids") or []) != uncovered_ids:
        return _validation_error(
            reason_code="uncovered_blocks_missing",
            reason="uncovered_block_ids не соответствует final block decisions",
            summary=summary,
            metadata_valid=True,
        )

    large_block_ids = _large_block_skipped_ids(normalized_blocks)
    if list(summary.get("large_block_skipped_ids") or []) != large_block_ids:
        return _validation_error(
            reason_code="large_block_skipped_ids_mismatch",
            reason="large_block_skipped_ids не соответствует final block decisions",
            summary=summary,
            metadata_valid=True,
        )

    if min_coverage > 0 and ratio < min_coverage:
        return {
            "valid": False,
            "metadata_valid": True,
            "reason_code": "low_coverage",
            "reason": f"coverage ниже required threshold ({ratio:.3f} < {min_coverage:.3f})",
            "coverage_ratio": ratio,
            "summary": summary,
        }

    has_partial = any(block.get("coverage_status") != "ok" for block in normalized_blocks)
    result = {
        "valid": True,
        "metadata_valid": True,
        "reason_code": "ok",
        "reason": "summary валиден",
        "coverage_ratio": ratio,
        "summary": summary,
        "status": "partial" if has_partial else ("no_blocks" if blocks_total == 0 else "ok"),
        "blocks_ok": covered_count,
        "blocks_total": blocks_total,
        "uncovered_block_ids": uncovered_ids,
        "large_block_skipped_ids": large_block_ids,
    }
    return result
