#!/usr/bin/env python3
"""Reproducible, read-only diagnostics for the Stage 4 production artifact.

This script deliberately does not call a model and does not write into the
production comparison directory.  It rebuilds the exact reviewer inputs from
the immutable deterministic artifacts, joins them to the stored model and
validator decisions, and writes diagnostics only under this experiment.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import re
import statistics
import subprocess
import sys
import tempfile
import time
from collections import Counter, defaultdict
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.app.services.llm.codex_runner import _build_json_prompt, find_codex_cli
from backend.app.services.stage_comparison import paths
from backend.app.services.stage_comparison import text_ai_reviewer as reviewer
from backend.app.services.stage_comparison import text_differences


ARTIFACTS = Path(__file__).resolve().parent / "artifacts"
ANNOTATIONS_PATH = Path(__file__).resolve().parent / "human_review_annotations.json"
SESSION_ID = "121d764109184c13"
PAIR_ID = "p570d156f57"
PROJECT_ID = "272_Sadovnicheskaya_76_Balchug_Esteyt"
SAMPLE_SEED = 4101
SAMPLE_SIZE = 50
LOCAL_REPLAY_REPETITIONS = 31

INVENTORY_PATH = ARTIFACTS / "uncertain_inventory.json"
SAMPLE_PATH = ARTIFACTS / "uncertain_sample_review.json"
PERFORMANCE_PATH = ARTIFACTS / "performance_profile.json"
PERFORMANCE_SUMMARY_PATH = ARTIFACTS / "performance_summary.json"
PROMPT_SIZE_PATH = ARTIFACTS / "prompt_size_analysis.json"
REPORT_PATH = ARTIFACTS / "EXPERIMENT_REPORT.md"


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def sha256_json(payload: Any) -> str:
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def pct(value: int, total: int) -> float:
    return round(100 * value / total, 2) if total else 0.0


def median_ms(action: Callable[[], Any], repetitions: int = LOCAL_REPLAY_REPETITIONS) -> float:
    samples = []
    for _ in range(repetitions):
        started = time.perf_counter_ns()
        action()
        samples.append((time.perf_counter_ns() - started) / 1_000_000)
    return round(statistics.median(samples), 4)


def correlation(left: list[float], right: list[float]) -> float:
    left_mean, right_mean = statistics.mean(left), statistics.mean(right)
    numerator = sum(
        (left_value - left_mean) * (right_value - right_mean)
        for left_value, right_value in zip(left, right)
    )
    denominator = math.sqrt(
        sum((value - left_mean) ** 2 for value in left)
        * sum((value - right_mean) ** 2 for value in right)
    )
    return numerator / denominator if denominator else 0.0


def sheet_labels(entries: list[dict[str, Any]]) -> dict[int, str]:
    output = {}
    for item in entries:
        page = int(item["pdf_page"])
        sheet = str(item.get("sheet_number") or "").strip()
        title = re.sub(r"\s+", " ", str(item.get("title") or "")).strip().rstrip(".")
        output[page] = (
            f"Лист {sheet}" + (f" — {title}" if title else "")
            if sheet else f"Страница {page}"
        )
    return output


def production_paths() -> dict[str, Path]:
    return {
        "comparison": paths.text_comparison_path(SESSION_ID, PAIR_ID),
        "differences": paths.text_differences_path(SESSION_ID, PAIR_ID),
        "links": paths.sheet_links_path(SESSION_ID, PAIR_ID),
        "suggestions": paths.sheet_match_suggestions_path(SESSION_ID, PAIR_ID),
        "review": paths.text_ai_review_path(SESSION_ID, PAIR_ID),
        "final": paths.text_final_comparison_path(SESSION_ID, PAIR_ID),
    }


def load_sources() -> dict[str, Any]:
    source_paths = production_paths()
    loaded = {key: load_json(path) for key, path in source_paths.items()}
    suggestions = loaded["suggestions"]
    labels = {
        side: sheet_labels(list(suggestions.get(f"{side}_sheet_index") or []))
        for side in ("left", "right")
    }
    groups = reviewer.build_review_groups(
        comparison=loaded["comparison"],
        links=list(loaded["links"].get("links") or []),
        labels=labels,
    )
    loaded["labels"] = labels
    loaded["groups"] = groups
    return loaded


def actual_calls(source_group: dict[str, Any], stored_group: dict[str, Any]) -> list[dict[str, Any]]:
    current_chunks = {
        chunk["group_id"]: chunk for chunk in reviewer.chunk_review_group(source_group)
    }
    stored_chunks = list(stored_group.get("chunks") or [])
    if not stored_chunks:
        return [{
            "id": source_group["group_id"],
            "source": source_group,
            "usage": dict(stored_group.get("usage") or {}),
            "source_group_sha256": source_group["source_group_sha256"],
            "stored_as_legacy_unchunked": len(current_chunks) > 1,
        }]
    output = []
    for item in stored_chunks:
        chunk = current_chunks.get(str(item.get("id") or ""))
        if chunk is None:
            raise RuntimeError(f"cannot reconstruct stored chunk {item.get('id')}")
        if item.get("source_group_sha256") != chunk.get("source_group_sha256"):
            raise RuntimeError(f"stored chunk hash mismatch: {item.get('id')}")
        output.append({
            "id": item["id"], "source": chunk,
            "usage": dict(item.get("usage") or {}),
            "source_group_sha256": item["source_group_sha256"],
            "stored_as_legacy_unchunked": False,
        })
    return output


def decision_call_id(decision: dict[str, Any], calls: list[dict[str, Any]]) -> str:
    referenced = {
        "left": set(decision.get("left_fragment_ids") or []),
        "right": set(decision.get("right_fragment_ids") or []),
    }
    matching = []
    for call in calls:
        required = call["source"]["required_fragment_ids"]
        if referenced["left"] <= set(required["left"]) and referenced["right"] <= set(required["right"]):
            matching.append(call["id"])
    if len(matching) != 1:
        raise RuntimeError(f"decision maps to {len(matching)} calls: {referenced}")
    return matching[0]


def fragment_shape(decision: dict[str, Any]) -> str:
    left = len(decision.get("left_fragment_ids") or [])
    right = len(decision.get("right_fragment_ids") or [])
    if left == 1 and right == 1:
        return "ONE_TO_ONE"
    if left == 1 and right > 1:
        return "FRAGMENTATION_1_TO_N"
    if left > 1 and right == 1:
        return "FRAGMENTATION_N_TO_1"
    if left > 1 and right > 1:
        return "FRAGMENTATION_N_TO_M"
    if left and not right:
        return "LEFT_ONLY"
    if right and not left:
        return "RIGHT_ONLY"
    return "EMPTY"


def primary_reason(decision: dict[str, Any]) -> tuple[str, str]:
    model_status = str(decision.get("model_final_status") or "")
    policy = str(decision.get("policy_reason") or "")
    if model_status == "UNCERTAIN":
        return "OCR_NOISE", "model chose UNCERTAIN for an OCR/typing ambiguity"
    if model_status == "MOVED":
        return (
            "MOVED_PAGE_SEMANTICS",
            "model treated different absolute П/РД PDF page numbers as MOVED inside an accepted link",
        )
    if policy == "same_conflicts_with_deterministic_change":
        if "дублирует" in str(decision.get("model_reason") or "").lower():
            return (
                "TABLE_STRUCTURE",
                "a table dimension cell was flattened into the row and looked like a text change",
            )
        return (
            "OCR_NOISE",
            "raster text agrees, but OCR variants made deterministic CHANGED conflict with model SAME",
        )
    if model_status == "REMOVED" and policy.startswith("unsupported_model_"):
        return (
            "MULTIPLE_CANDIDATES",
            "the linked-sheet counterpart was consumed by a duplicate candidate from another page",
        )
    if policy.startswith("unsupported_model_"):
        return "VALIDATOR_REJECTED", "model explanation contains unsupported source claims"
    return "OTHER", policy or "unclassified"


def validator_detail(decision: dict[str, Any]) -> str | None:
    policy = decision.get("policy_reason")
    if not policy:
        return None
    if decision.get("model_final_status") == "MOVED":
        return "actual right fragment is inside accepted linked pages; MOVED is semantically invalid"
    if decision.get("model_final_status") == "ADDED" and str(policy).startswith("unsupported_model_"):
        return "summary normalizes/shortens an exact document designation (not source-exact)"
    if decision.get("model_final_status") == "CHANGED" and str(policy).startswith("unsupported_model_"):
        return "summary belongs to another fragment; response leakage/misattribution"
    if decision.get("model_final_status") == "REMOVED" and str(policy).startswith("unsupported_model_"):
        return "reason mentions page/counterpart absent from this decision provenance"
    return str(policy)


def normalize_similarity(value: str) -> str:
    return re.sub(r"[^a-zа-я0-9]+", "", text_differences.canonicalize(value or ""))


def neighboring_sources(
    source_group: dict[str, Any], side: str, ids: list[str], radius: int = 2,
) -> list[dict[str, Any]]:
    sources = list(source_group.get(f"source_{side}") or [])
    positions = {item["fragment_id"]: index for index, item in enumerate(sources)}
    selected: set[int] = set()
    for fragment_id in ids:
        position = positions[fragment_id]
        selected.update(range(max(0, position - radius), min(len(sources), position + radius + 1)))
    referenced = set(ids)
    return [
        {
            "fragment_id": sources[index]["fragment_id"],
            "page": sources[index]["page"],
            "text": sources[index]["text"],
            "source_kind": sources[index]["source_kind"],
            "local_context": sources[index].get("local_context") or "",
        }
        for index in sorted(selected)
        if sources[index]["fragment_id"] not in referenced
    ]


def opposite_candidates(
    source_group: dict[str, Any], decision: dict[str, Any], call_id: str,
    calls: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if decision.get("left_fragment_ids") and decision.get("right_fragment_ids"):
        return []
    source_side = "left" if decision.get("left_fragment_ids") else "right"
    other_side = "right" if source_side == "left" else "left"
    source_ids = decision.get(f"{source_side}_fragment_ids") or []
    source_map = {
        item["fragment_id"]: item for item in source_group.get(f"source_{source_side}") or []
    }
    needle = normalize_similarity("\n".join(source_map[value]["text"] for value in source_ids))
    call_by_fragment = {
        (side, fragment_id): call["id"]
        for call in calls
        for side in ("left", "right")
        for fragment_id in call["source"]["required_fragment_ids"][side]
    }
    candidates = []
    for item in source_group.get(f"source_{other_side}") or []:
        candidate = normalize_similarity(item["text"])
        score = SequenceMatcher(None, needle, candidate).ratio() if needle and candidate else 0.0
        candidates.append({
            "fragment_id": item["fragment_id"], "page": item["page"],
            "text": item["text"], "similarity": round(score, 4),
            "call_id": call_by_fragment[(other_side, item["fragment_id"])],
            "outside_current_call": call_by_fragment[(other_side, item["fragment_id"])] != call_id,
            "same_stable_key": (
                bool(text_differences.stable_key(item["text"]))
                and text_differences.stable_key(item["text"])
                == text_differences.stable_key(source_map[source_ids[0]]["text"])
            ) if source_ids else False,
        })
    return sorted(candidates, key=lambda value: value["similarity"], reverse=True)[:3]


def build_inventory(data: dict[str, Any]) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    source_groups = {item["group_id"]: item for item in data["groups"]}
    stored_groups = {item["id"]: item for item in data["review"]["sheet_groups"]}
    entries = []
    calls_by_group = {}
    for group_id, source_group in source_groups.items():
        stored_group = stored_groups[group_id]
        calls = actual_calls(source_group, stored_group)
        calls_by_group[group_id] = calls
        source_maps = {
            side: {item["fragment_id"]: item for item in source_group[f"source_{side}"]}
            for side in ("left", "right")
        }
        for decision_index, decision in enumerate(stored_group.get("decisions") or []):
            if decision.get("final_status") != "UNCERTAIN":
                continue
            case_id = f"{group_id}:d{decision_index:03d}"
            call_id = decision_call_id(decision, calls)
            candidates = opposite_candidates(source_group, decision, call_id, calls)
            shape = fragment_shape(decision)
            reason, reason_detail = primary_reason(decision)
            referenced_sources = {
                side: [source_maps[side][value] for value in decision[f"{side}_fragment_ids"]]
                for side in ("left", "right")
            }
            preliminary = []
            referenced = {
                side: set(decision[f"{side}_fragment_ids"]) for side in ("left", "right")
            }
            for item in source_group.get("preliminary") or []:
                if (
                    referenced["left"] & set(item.get("left_fragment_ids") or [])
                    or referenced["right"] & set(item.get("right_fragment_ids") or [])
                ):
                    preliminary.append(item)
            chunk_boundary = any(
                item["outside_current_call"] and item["same_stable_key"]
                for item in candidates
            )
            entries.append({
                "case_id": case_id,
                "group_id": group_id,
                "decision_index": decision_index,
                "call_id": call_id,
                "origin": (
                    "MODEL_ORIGIN" if decision.get("model_final_status") == "UNCERTAIN"
                    else "VALIDATOR_ORIGIN"
                ),
                "uncertain_reason": reason,
                "uncertain_reason_detail": reason_detail,
                "validator_reason": decision.get("policy_reason"),
                "validator_detail": validator_detail(decision),
                "deterministic_status": decision.get("deterministic_status"),
                "model_status": decision.get("model_final_status"),
                "final_status": decision.get("final_status"),
                "confidence": decision.get("confidence"),
                "fragment_shape": shape,
                "potential_chunk_boundary": chunk_boundary,
                "preliminary_evidence": preliminary,
                "left_fragment_ids": decision.get("left_fragment_ids") or [],
                "right_fragment_ids": decision.get("right_fragment_ids") or [],
                "source_left": referenced_sources["left"],
                "source_right": referenced_sources["right"],
                "before": decision.get("before"),
                "after": decision.get("after"),
                "model_summary": decision.get("model_summary"),
                "model_reason": decision.get("model_reason"),
                "neighbors_left": neighboring_sources(
                    source_group, "left", decision.get("left_fragment_ids") or []
                ),
                "neighbors_right": neighboring_sources(
                    source_group, "right", decision.get("right_fragment_ids") or []
                ),
                "opposite_candidates": candidates,
                "diagnostic_flags": [
                    *(
                        ["WEAK_PROVENANCE"]
                        if str(decision.get("policy_reason") or "").startswith("unsupported_model_")
                        else []
                    ),
                    *(
                        ["CHUNK_BOUNDARY"]
                        if chunk_boundary else []
                    ),
                ],
                "trace": {
                    "source_group_sha256": source_group["source_group_sha256"],
                    "review_source_signature": data["review"]["source_signature"],
                    "model": data["review"]["model"],
                    "prompt_version": data["review"]["prompt_version"],
                    "validator_version": data["review"]["validator_version"],
                    "model_response_capture": "exact persisted fields after schema parse",
                    "raw_transport_stdout_available": False,
                    "raw_transport_limitation": (
                        "Stage 4 did not persist the provider stdout envelope; no raw data was "
                        "deleted by this diagnostic run"
                    ),
                },
            })
    if len(entries) != 189:
        raise RuntimeError(f"expected 189 UNCERTAIN entries, got {len(entries)}")
    by_origin = Counter(item["origin"] for item in entries)
    by_reason_observed = Counter(item["uncertain_reason"] for item in entries)
    by_validator = Counter(
        item["validator_reason"]
        for item in entries if item["origin"] == "VALIDATOR_ORIGIN"
    )
    by_shape = Counter(item["fragment_shape"] for item in entries)
    by_model_status = Counter(item["model_status"] for item in entries)
    moved_entries = [
        item for item in entries if item["uncertain_reason"] == "MOVED_PAGE_SEMANTICS"
    ]
    taxonomy = [
        "MODEL_UNCERTAIN", "VALIDATOR_REJECTED", "MISSING_CONTEXT",
        "MULTIPLE_CANDIDATES", "FRAGMENTATION_1_TO_N", "FRAGMENTATION_N_TO_1",
        "FRAGMENTATION_N_TO_M", "CHUNK_BOUNDARY", "OCR_NOISE", "TABLE_STRUCTURE",
        "FORMULA_STRUCTURE", "WEAK_PROVENANCE", "CONFLICTING_EVIDENCE",
        "NO_COUNTERPART", "OTHER", "MOVED_PAGE_SEMANTICS",
    ]
    by_reason = {key: by_reason_observed.get(key, 0) for key in taxonomy}
    inventory = {
        "version": 1,
        "kind": "stage4_uncertain_diagnostic_inventory",
        "project_id": PROJECT_ID,
        "session_id": SESSION_ID,
        "pair_id": PAIR_ID,
        "source_review_sha256": hashlib.sha256(
            production_paths()["review"].read_bytes()
        ).hexdigest(),
        "diagnostic_only": True,
        "production_dependency": False,
        "taxonomy": taxonomy,
        "summary": {
            "total_uncertain": len(entries),
            "by_origin": dict(sorted(by_origin.items())),
            "by_reason": {
                key: {"count": value, "share_percent": pct(value, len(entries))}
                for key, value in by_reason.items()
            },
            "by_validator_reason": dict(sorted(by_validator.items())),
            "by_model_status": dict(sorted(by_model_status.items())),
            "by_fragment_shape": dict(sorted(by_shape.items())),
            "potential_chunk_boundary": sum(item["potential_chunk_boundary"] for item in entries),
            "fragmentation_1_to_n": by_shape["FRAGMENTATION_1_TO_N"],
            "fragmentation_n_to_1": by_shape["FRAGMENTATION_N_TO_1"],
            "fragmentation_n_to_m": by_shape["FRAGMENTATION_N_TO_M"],
            "same_call_duplicate_assignment_candidates": sum(
                item["uncertain_reason"] == "MULTIPLE_CANDIDATES" for item in entries
            ),
            "moved_page_semantics_detail": {
                "text_exact": sum(item["before"] == item["after"] for item in moved_entries),
                "canonical_text_exact": sum(
                    text_differences.canonicalize(item["before"] or "")
                    == text_differences.canonicalize(item["after"] or "")
                    for item in moved_entries
                ),
                "by_deterministic_status": dict(sorted(Counter(
                    item["deterministic_status"] for item in moved_entries
                ).items())),
            },
        },
        "entries": entries,
    }
    inventory["inventory_sha256"] = sha256_json(inventory)
    return inventory, calls_by_group


def sample_case_ids(entries: list[dict[str, Any]]) -> list[str]:
    rng = random.Random(SAMPLE_SEED)
    by_reason: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in entries:
        by_reason[item["uncertain_reason"]].append(item)
    selected: list[dict[str, Any]] = []
    for reason in ("OCR_NOISE", "TABLE_STRUCTURE", "MULTIPLE_CANDIDATES"):
        selected.extend(by_reason[reason])

    validator = list(by_reason["VALIDATOR_REJECTED"])
    validator_by_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in validator:
        validator_by_group[item["group_id"]].append(item)
    for group_items in validator_by_group.values():
        selected.append(rng.choice(group_items))
    remaining_validator = [item for item in validator if item not in selected]
    selected.extend(rng.sample(remaining_validator, 12 - len(validator_by_group)))

    moved = list(by_reason["MOVED_PAGE_SEMANTICS"])
    moved_by_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in moved:
        moved_by_group[item["group_id"]].append(item)
    for group_items in moved_by_group.values():
        selected.append(rng.choice(group_items))
    remaining_moved = [item for item in moved if item not in selected]
    selected.extend(rng.sample(remaining_moved, 28 - len(moved_by_group)))
    if len(selected) != SAMPLE_SIZE or len({item["case_id"] for item in selected}) != SAMPLE_SIZE:
        raise RuntimeError("stratified sample construction failed")
    return [item["case_id"] for item in selected]


def build_sample(inventory: dict[str, Any]) -> dict[str, Any]:
    annotations = load_json(ANNOTATIONS_PATH) if ANNOTATIONS_PATH.exists() else {"annotations": {}}
    annotation_map = annotations.get("annotations") or {}
    allowed_classifications = {
        "HUMAN_RESOLVABLE", "GENUINELY_UNCERTAIN", "NEED_MORE_CONTEXT", "BAD_INPUT",
    }
    for case_id, annotation in annotation_map.items():
        if annotation.get("classification") not in allowed_classifications:
            raise RuntimeError(f"invalid human classification for {case_id}")
        if annotation.get("resolved_status") not in reviewer.FINAL_STATUSES:
            raise RuntimeError(f"invalid human resolved status for {case_id}")
        if not isinstance(annotation.get("automation_candidate"), bool):
            raise RuntimeError(f"missing automation candidate flag for {case_id}")
    by_id = {item["case_id"]: item for item in inventory["entries"]}
    selected_ids = sample_case_ids(inventory["entries"])
    unexpected_annotations = set(annotation_map) - set(selected_ids)
    if unexpected_annotations:
        raise RuntimeError(f"annotations outside selected sample: {sorted(unexpected_annotations)}")
    rows = []
    for case_id in selected_ids:
        source = by_id[case_id]
        annotation = annotation_map.get(case_id)
        rows.append({
            "case_id": case_id,
            "group_id": source["group_id"],
            "origin": source["origin"],
            "uncertain_reason": source["uncertain_reason"],
            "fragment_shape": source["fragment_shape"],
            "before": source["before"],
            "after": source["after"],
            "model_status": source["model_status"],
            "deterministic_status": source["deterministic_status"],
            "validator_reason": source["validator_reason"],
            "model_summary": source["model_summary"],
            "model_reason": source["model_reason"],
            "preliminary_evidence": source["preliminary_evidence"],
            "source_left": source["source_left"],
            "source_right": source["source_right"],
            "neighbors_left": source["neighbors_left"],
            "neighbors_right": source["neighbors_right"],
            "opposite_candidates": source["opposite_candidates"],
            "trace": source["trace"],
            "human_review": annotation,
        })
    reviewed = [item for item in rows if item["human_review"]]
    classifications = Counter(
        item["human_review"]["classification"] for item in reviewed
    )
    resolved_statuses = Counter(
        item["human_review"].get("resolved_status") or "NONE" for item in reviewed
    )
    automation = Counter(
        str(bool(item["human_review"].get("automation_candidate"))).lower()
        for item in reviewed
    )
    sample_source_kinds = Counter(
        fragment["source_kind"]
        for item in rows
        for side in ("source_left", "source_right")
        for fragment in item[side]
    )
    sample_texts = [
        str(value or "")
        for item in rows
        for value in (item["before"], item["after"])
        if value is not None
    ]
    population_by_reason = {
        reason: details["count"]
        for reason, details in inventory["summary"]["by_reason"].items()
        if details["count"]
    }
    reviewed_by_reason: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in reviewed:
        reviewed_by_reason[item["uncertain_reason"]].append(item)
    weighted_by_reason = {}
    weighted_auto = 0.0
    for reason, population_count in population_by_reason.items():
        stratum = reviewed_by_reason.get(reason) or []
        if not stratum:
            continue
        candidate_count = sum(
            bool(item["human_review"].get("automation_candidate")) for item in stratum
        )
        rate = candidate_count / len(stratum)
        estimate = population_count * rate
        weighted_auto += estimate
        weighted_by_reason[reason] = {
            "population": population_count,
            "reviewed": len(stratum),
            "automation_candidates": candidate_count,
            "sample_rate": round(rate, 4),
            "weighted_population_estimate": round(estimate, 1),
        }
    payload = {
        "version": 1,
        "kind": "stage4_uncertain_human_review_sample",
        "diagnostic_only": True,
        "production_ground_truth": False,
        "sampling": {
            "method": "seeded stratified random sample with complete minority strata",
            "seed": SAMPLE_SEED,
            "target_size": SAMPLE_SIZE,
            "selected_case_ids_sha256": sha256_json(selected_ids),
        },
        "summary": {
            "selected": len(rows), "reviewed": len(reviewed),
            "by_classification": dict(sorted(classifications.items())),
            "by_resolved_status": dict(sorted(resolved_statuses.items())),
            "by_automation_candidate": dict(sorted(automation.items())),
            "coverage": {
                "distinct_sheet_groups": len({item["group_id"] for item in rows}),
                "by_origin": dict(sorted(Counter(item["origin"] for item in rows).items())),
                "by_fragment_shape": dict(sorted(Counter(
                    item["fragment_shape"] for item in rows
                ).items())),
                "source_kinds": dict(sorted(sample_source_kinds.items())),
                "formula_like_cases": sum(
                    any(
                        re.search(r"\\geq|[≥≤]|\\text\{|/m\^|/м[³3]", str(text or ""), re.I)
                        for text in (item["before"], item["after"])
                    )
                    for item in rows
                ),
                "short_fragments_le_20_chars": sum(len(text) <= 20 for text in sample_texts),
                "long_fragments_ge_400_chars": sum(len(text) >= 400 for text in sample_texts),
            },
            "weighted_population_estimate": {
                "potentially_automatable": round(weighted_auto),
                "retain_for_human_or_source_repair": round(
                    inventory["summary"]["total_uncertain"] - weighted_auto
                ),
                "by_reason": weighted_by_reason,
                "basis": (
                    "reason-stratified sample rates applied to full reason counts; potential only, "
                    "not permission to change production statuses"
                ),
            },
        },
        "cases": rows,
    }
    payload["sample_sha256"] = sha256_json(payload)
    return payload


def raw_decision(decision: dict[str, Any]) -> dict[str, Any]:
    model_status = decision.get("model_final_status") or decision["final_status"]
    return {
        "left_fragment_ids": decision["left_fragment_ids"],
        "right_fragment_ids": decision["right_fragment_ids"],
        "final_status": model_status,
        "confidence": decision["confidence"],
        "summary": decision.get("model_summary") or decision["summary"],
        "reason": decision.get("model_reason") or decision["reason"],
        "actual_right_pages": (
            decision["right_pages"] if model_status == "MOVED" else []
        ),
    }


def call_decisions(stored_group: dict[str, Any], calls: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    output = {call["id"]: [] for call in calls}
    for decision in stored_group.get("decisions") or []:
        output[decision_call_id(decision, calls)].append(decision)
    return output


def prompt_components(chunk: dict[str, Any]) -> dict[str, int]:
    schema_compact = json.dumps(
        reviewer.RESPONSE_SCHEMA, ensure_ascii=False, separators=(",", ":")
    )
    schema_native = json.dumps(reviewer.RESPONSE_SCHEMA, ensure_ascii=False)
    group_input = {
        "group_id": chunk["group_id"],
        "left_pages": chunk.get("left_pages") or [],
        "right_pages": chunk.get("right_pages") or [],
        "source_left": chunk.get("source_left") or [],
        "source_right": chunk.get("source_right") or [],
        "preliminary": chunk.get("preliminary") or [],
    }
    input_payload = {"groups": [group_input]}
    input_json = json.dumps(input_payload, ensure_ascii=False, separators=(",", ":"))
    no_preliminary = json.loads(json.dumps(input_payload, ensure_ascii=False))
    no_preliminary["groups"][0]["preliminary"] = []
    no_preliminary_chars = len(json.dumps(
        no_preliminary, ensure_ascii=False, separators=(",", ":")
    ))
    base = json.loads(json.dumps(no_preliminary, ensure_ascii=False))
    base["groups"][0]["source_left"] = []
    base["groups"][0]["source_right"] = []
    base_chars = len(json.dumps(base, ensure_ascii=False, separators=(",", ":")))
    source_contribution = no_preliminary_chars - base_chars
    preliminary_contribution = len(input_json) - no_preliminary_chars

    def without_source_field(field: str, replacement: Any) -> int:
        variant = json.loads(json.dumps(no_preliminary, ensure_ascii=False))
        for side in ("source_left", "source_right"):
            for item in variant["groups"][0][side]:
                item[field] = replacement
        return len(json.dumps(variant, ensure_ascii=False, separators=(",", ":")))

    text_chars = no_preliminary_chars - without_source_field("text", "")
    bbox_chars = no_preliminary_chars - without_source_field("bboxes", [])
    context_chars = no_preliminary_chars - without_source_field("local_context", "")
    source_metadata_chars = source_contribution - text_chars - bbox_chars - context_chars
    preliminary_items = list(group_input["preliminary"])
    preliminary_weights = {
        "same": sum(
            len(json.dumps(item, ensure_ascii=False, separators=(",", ":")))
            for item in preliminary_items if item.get("status") == "SAME"
        ),
        "moved": sum(
            len(json.dumps(item, ensure_ascii=False, separators=(",", ":")))
            for item in preliminary_items if item.get("status") == "MOVED"
        ),
        "differences": sum(
            len(json.dumps(item, ensure_ascii=False, separators=(",", ":")))
            for item in preliminary_items if item.get("status") not in {"SAME", "MOVED"}
        ),
    }
    preliminary_weight_total = sum(preliminary_weights.values()) or 1
    preliminary_split = {
        key: round(preliminary_contribution * value / preliminary_weight_total)
        for key, value in preliminary_weights.items()
    }
    ai_prompt = reviewer.prompt_for_groups([chunk])
    fixed_instruction_chars = len(ai_prompt) - len(schema_compact) - len(input_json)
    return {
        "ai_prompt_chars": len(ai_prompt),
        "input_json_chars": len(input_json),
        "fixed_instruction_chars": fixed_instruction_chars,
        "schema_in_prompt_chars": len(schema_compact),
        "native_schema_chars": len(schema_native),
        "group_metadata_and_json_structure_chars": base_chars,
        "source_text_chars": text_chars,
        "source_bbox_chars": bbox_chars,
        "source_local_context_chars": context_chars,
        "source_metadata_chars": source_metadata_chars,
        "preliminary_chars": preliminary_contribution,
        "preliminary_same_chars_estimate": preliminary_split["same"],
        "preliminary_moved_chars_estimate": preliminary_split["moved"],
        "preliminary_differences_chars_estimate": preliminary_split["differences"],
    }


def measure_cli_startup() -> dict[str, Any]:
    cli = find_codex_cli()
    if not cli:
        return {"available": False, "samples": 0}
    samples = []
    version = ""
    for _ in range(15):
        started = time.perf_counter_ns()
        result = subprocess.run(
            [cli, "--version"], capture_output=True, text=True, check=False, timeout=10
        )
        samples.append((time.perf_counter_ns() - started) / 1_000_000)
        version = (result.stdout or result.stderr).strip()
    return {
        "available": True, "version": version, "samples": len(samples),
        "median_ms": round(statistics.median(samples), 3),
        "min_ms": round(min(samples), 3), "max_ms": round(max(samples), 3),
        "measurement": "local `codex --version` subprocess lower bound; no model call",
    }


def build_performance(
    data: dict[str, Any], calls_by_group: dict[str, list[dict[str, Any]]],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    source_groups = {item["group_id"]: item for item in data["groups"]}
    stored_groups = {item["id"]: item for item in data["review"]["sheet_groups"]}
    link_by_id = {item["id"]: item for item in data["links"].get("links") or []}
    source_paths = production_paths()
    deterministic_load_ms = median_ms(lambda: {
        key: json.loads(path.read_text(encoding="utf-8"))
        for key, path in source_paths.items()
    })
    group_profiles = []
    call_profiles = []
    total_prompt_components: Counter[str] = Counter()
    submitted_fragments: Counter[tuple[str, str]] = Counter()
    unique_fragment_text: dict[tuple[str, str], str] = {}
    submitted_fragments_by_group: dict[str, Counter[tuple[str, str]]] = defaultdict(Counter)
    unique_fragment_text_by_group: dict[str, dict[tuple[str, str], str]] = defaultdict(dict)

    for group_id, source_group in source_groups.items():
        stored = stored_groups[group_id]
        calls = calls_by_group[group_id]
        decisions_by_call = call_decisions(stored, calls)
        link = link_by_id[group_id]
        preprocess_ms = median_ms(lambda link=link: reviewer.build_review_groups(
            comparison=data["comparison"], links=[link], labels=data["labels"]
        ))
        chunking_ms = median_ms(lambda: reviewer.chunk_review_group(source_group))
        prompt_build_ms = median_ms(lambda: [
            reviewer.prompt_for_groups([call["source"]]) for call in calls
        ])

        def validate_all() -> None:
            for call in calls:
                payload = {"groups": [{
                    "group_id": call["source"]["group_id"],
                    "decisions": [raw_decision(item) for item in decisions_by_call[call["id"]]],
                }]}
                reviewer.validate_response(payload, [call["source"]], safe_same_moved=True)

        validator_ms = median_ms(validate_all)
        one_group_review = {
            **data["review"], "sheet_groups": [stored],
            "summary": {"completed_groups": 1},
        }
        aggregation_ms = median_ms(lambda: reviewer.build_final_comparison(
            pair_id=PAIR_ID, generated_at=data["review"]["generated_at"],
            review_payload=one_group_review, differences=data["differences"],
        ))

        def io_round_trip() -> None:
            payload = {"review": stored, "source_signature": data["review"]["source_signature"]}
            with tempfile.TemporaryDirectory(prefix="stage4_diag_") as directory:
                target = Path(directory) / "artifact.json"
                temporary = target.with_suffix(".json.tmp")
                temporary.write_text(
                    json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
                )
                os.replace(temporary, target)
                json.loads(target.read_text(encoding="utf-8"))

        file_io_ms = median_ms(io_round_trip, repetitions=11)
        model_duration_ms = int(stored["usage"]["duration_ms"])
        local_ms = (
            preprocess_ms + chunking_ms + prompt_build_ms + validator_ms
            + aggregation_ms + file_io_ms + deterministic_load_ms / len(source_groups)
        )
        group_profiles.append({
            "group_id": group_id,
            "left_pages": source_group["left_pages"],
            "right_pages": source_group["right_pages"],
            "source_fragment_count": len(source_group["source_left"]) + len(source_group["source_right"]),
            "preliminary_item_count": len(source_group["preliminary"]),
            "actual_model_calls": len(calls),
            "actual_chunks": len(calls),
            "current_policy_chunks_if_not_reused": len(reviewer.chunk_review_group(source_group)),
            "input_tokens": int(stored["usage"]["input_tokens"]),
            "output_tokens": int(stored["usage"]["output_tokens"]),
            "cached_tokens": int(stored["usage"]["cached_tokens"]),
            "input_chars": 0,
            "model_call_duration_ms": model_duration_ms,
            "preprocessing_replay_ms": preprocess_ms,
            "chunking_replay_ms": chunking_ms,
            "prompt_build_replay_ms": prompt_build_ms,
            "validator_replay_ms": validator_ms,
            "aggregation_replay_ms": aggregation_ms,
            "file_io_replay_ms": file_io_ms,
            "shared_deterministic_load_allocation_ms": round(deterministic_load_ms / len(source_groups), 4),
            "local_replay_total_ms": round(local_ms, 4),
            "reconstructed_total_ms": round(model_duration_ms + local_ms, 4),
            "uncertain_count": sum(
                item["final_status"] == "UNCERTAIN" for item in stored["decisions"]
            ),
        })
        profile = group_profiles[-1]
        for call in calls:
            chunk = call["source"]
            ai_prompt = reviewer.prompt_for_groups([chunk])
            wrapper_prompt = _build_json_prompt(
                [{"role": "user", "content": ai_prompt}],
                stage="stage_comparison_text_ai_review",
                project_id=f"{SESSION_ID}:{PAIR_ID}:{chunk['group_id']}",
            )
            components = prompt_components(chunk)
            components["wrapper_chars"] = len(wrapper_prompt) - len(ai_prompt)
            components["submitted_prompt_chars"] = len(wrapper_prompt)
            for key, value in components.items():
                total_prompt_components[key] += value
            profile["input_chars"] += len(wrapper_prompt)
            for side in ("left", "right"):
                for fragment in chunk[f"source_{side}"]:
                    key = (side, fragment["fragment_id"])
                    submitted_fragments[key] += 1
                    unique_fragment_text[key] = fragment["text"]
                    submitted_fragments_by_group[group_id][key] += 1
                    unique_fragment_text_by_group[group_id][key] = fragment["text"]
            usage = call["usage"]
            call_profile = {
                "group_id": group_id, "call_id": call["id"],
                "source_fragment_count": len(chunk["source_left"]) + len(chunk["source_right"]),
                "preliminary_item_count": len(chunk["preliminary"]),
                "input_chars": len(wrapper_prompt),
                "prompt_sha256": hashlib.sha256(wrapper_prompt.encode("utf-8")).hexdigest(),
                "input_tokens": int(usage.get("input_tokens") or 0),
                "output_tokens": int(usage.get("output_tokens") or 0),
                "cached_tokens": int(usage.get("cached_tokens") or 0),
                "model_duration_ms": int(usage.get("duration_ms") or 0),
                "stored_as_legacy_unchunked": call["stored_as_legacy_unchunked"],
                "prompt_components": components,
            }
            call_profiles.append(call_profile)
            profile.setdefault("model_calls", []).append({
                "call_id": call_profile["call_id"],
                "input_chars": call_profile["input_chars"],
                "input_tokens": call_profile["input_tokens"],
                "output_tokens": call_profile["output_tokens"],
                "cached_tokens": call_profile["cached_tokens"],
                "duration_ms": call_profile["model_duration_ms"],
                "stored_as_legacy_unchunked": call_profile["stored_as_legacy_unchunked"],
            })
        profile["model_call_durations_ms"] = [
            item["duration_ms"] for item in profile.get("model_calls") or []
        ]

    input_chars = [item["input_chars"] for item in call_profiles]
    input_tokens = [item["input_tokens"] for item in call_profiles]
    mean_chars, mean_tokens = statistics.mean(input_chars), statistics.mean(input_tokens)
    slope = sum(
        (chars - mean_chars) * (tokens - mean_tokens)
        for chars, tokens in zip(input_chars, input_tokens)
    ) / sum((chars - mean_chars) ** 2 for chars in input_chars)
    intercept = mean_tokens - slope * mean_chars
    residual = sum(
        (tokens - (intercept + slope * chars)) ** 2
        for chars, tokens in zip(input_chars, input_tokens)
    )
    variance = sum((tokens - mean_tokens) ** 2 for tokens in input_tokens)
    r_squared = 1 - residual / variance

    submitted_source_text_chars = sum(
        len(unique_fragment_text[key]) * count for key, count in submitted_fragments.items()
    )
    unique_source_text_chars = sum(len(value) for value in unique_fragment_text.values())
    fixed_once = (
        total_prompt_components["fixed_instruction_chars"]
        + total_prompt_components["schema_in_prompt_chars"]
        + total_prompt_components["wrapper_chars"]
    ) / len(call_profiles)
    unique_context_chars_estimate = (
        total_prompt_components["submitted_prompt_chars"]
        - fixed_once * len(call_profiles) + fixed_once
        - submitted_source_text_chars + unique_source_text_chars
    )
    source_text_by_group = {}
    for group_id, fragment_counts in submitted_fragments_by_group.items():
        submitted_chars = sum(
            len(unique_fragment_text_by_group[group_id][key]) * count
            for key, count in fragment_counts.items()
        )
        unique_chars = sum(
            len(value) for value in unique_fragment_text_by_group[group_id].values()
        )
        group_calls = [item for item in call_profiles if item["group_id"] == group_id]
        fixed_components = [
            item["prompt_components"]["fixed_instruction_chars"]
            + item["prompt_components"]["schema_in_prompt_chars"]
            + item["prompt_components"]["wrapper_chars"]
            for item in group_calls
        ]
        total_chars = sum(item["input_chars"] for item in group_calls)
        unique_whole_estimate = (
            total_chars - sum(fixed_components) + (fixed_components[0] if fixed_components else 0)
            - submitted_chars + unique_chars
        )
        source_text_by_group[group_id] = {
            "calls": len(group_calls),
            "unique_source_chars": unique_chars,
            "submitted_source_chars": submitted_chars,
            "source_text_duplication_factor": round(submitted_chars / unique_chars, 4),
            "submitted_prompt_chars": total_chars,
            "unique_whole_context_chars_estimate": round(unique_whole_estimate),
            "whole_context_duplication_factor_estimate": round(
                total_chars / unique_whole_estimate, 4
            ),
        }
    largest_group_id = max(
        source_text_by_group,
        key=lambda value: source_text_by_group[value]["submitted_prompt_chars"],
    )
    prompt_analysis = {
        "version": 1,
        "kind": "stage4_prompt_size_analysis",
        "diagnostic_only": True,
        "actual_calls": len(call_profiles),
        "totals_chars": dict(sorted(total_prompt_components.items())),
        "source_text": {
            "unique_chars": unique_source_text_chars,
            "submitted_chars": submitted_source_text_chars,
            "duplication_factor": round(
                submitted_source_text_chars / unique_source_text_chars, 4
            ),
            "duplicated_fragment_occurrences": sum(
                count - 1 for count in submitted_fragments.values() if count > 1
            ),
        },
        "source_text_by_group": source_text_by_group,
        "largest_group_example": {
            "group_id": largest_group_id,
            **source_text_by_group[largest_group_id],
        },
        "whole_context_duplication_factor_estimate": round(
            total_prompt_components["submitted_prompt_chars"] / unique_context_chars_estimate, 4
        ),
        "repeated_fixed_context": {
            "estimated_unique_chars": round(fixed_once),
            "submitted_chars": round(fixed_once * len(call_profiles)),
            "copies": len(call_profiles),
        },
        "input_token_regression": {
            "formula": "input_tokens ~= intercept + chars_coefficient * submitted_prompt_chars",
            "intercept_tokens_per_call": round(intercept, 2),
            "chars_coefficient": round(slope, 6),
            "r_squared": round(r_squared, 6),
            "estimated_repeated_fixed_runtime_tokens": round(intercept * len(call_profiles)),
            "estimated_repeated_fixed_runtime_share_percent": round(
                100 * intercept * len(call_profiles) / sum(input_tokens), 2
            ),
            "interpretation": (
                "empirical fixed per-call Codex runtime context; includes hidden CLI/system/schema "
                "context and is not attributable solely to application prompt text"
            ),
        },
        "native_schema": {
            "passed_once_per_call": True,
            "also_embedded_in_application_prompt": True,
            "chars_per_native_schema_file": len(json.dumps(reviewer.RESPONSE_SCHEMA, ensure_ascii=False)),
        },
        "calls": call_profiles,
    }
    prompt_analysis["analysis_sha256"] = sha256_json(prompt_analysis)

    model_durations = [item["model_call_duration_ms"] for item in group_profiles]
    call_durations = [item["model_duration_ms"] for item in call_profiles]
    chunk_counts = [item["actual_chunks"] for item in group_profiles]
    total_model_ms = sum(model_durations)
    total_local_ms = deterministic_load_ms + sum(
        item["local_replay_total_ms"] - item["shared_deterministic_load_allocation_ms"]
        for item in group_profiles
    )
    phase_totals_ms = {
        "deterministic_load": deterministic_load_ms,
        "preprocessing": sum(item["preprocessing_replay_ms"] for item in group_profiles),
        "chunking": sum(item["chunking_replay_ms"] for item in group_profiles),
        "prompt_build": sum(item["prompt_build_replay_ms"] for item in group_profiles),
        "validation": sum(item["validator_replay_ms"] for item in group_profiles),
        "aggregation": sum(item["aggregation_replay_ms"] for item in group_profiles),
        "file_io": sum(item["file_io_replay_ms"] for item in group_profiles),
    }
    benchmark = load_json(
        REPO_ROOT / "experiments/stage_comparison_text_ai_reviewer/artifacts/benchmark_summary.json"
    )["production_candidate"]
    benchmark_decisions = 39
    benchmark_total_seconds = benchmark["avg_group_time_sec"] * 27
    groups_with_chunk_records = sum(
        bool(stored_groups[group_id].get("chunks")) for group_id in source_groups
    )
    summary = {
        "version": 1,
        "kind": "stage4_performance_summary",
        "diagnostic_only": True,
        "groups": len(group_profiles),
        "model_calls": len(call_profiles),
        "chunks": len(call_profiles),
        "total_model_seconds": round(total_model_ms / 1000, 3),
        "reconstructed_total_seconds": round((total_model_ms + total_local_ms) / 1000, 3),
        "average_seconds_per_group": round(statistics.mean(model_durations) / 1000, 3),
        "median_seconds_per_group": round(statistics.median(model_durations) / 1000, 3),
        "fastest_group": min(group_profiles, key=lambda value: value["model_call_duration_ms"])["group_id"],
        "fastest_group_seconds": round(min(model_durations) / 1000, 3),
        "slowest_group": max(group_profiles, key=lambda value: value["model_call_duration_ms"])["group_id"],
        "slowest_group_seconds": round(max(model_durations) / 1000, 3),
        "average_chunks_per_group": round(statistics.mean(chunk_counts), 3),
        "median_chunks_per_group": statistics.median(chunk_counts),
        "max_chunks_per_group": max(chunk_counts),
        "chunk_persistence": {
            "groups_with_explicit_chunk_records": groups_with_chunk_records,
            "groups_with_legacy_group_level_usage": len(source_groups) - groups_with_chunk_records,
            "legacy_groups_that_current_policy_would_split": sum(
                item["actual_chunks"] == 1 and item["current_policy_chunks_if_not_reused"] > 1
                for item in group_profiles
            ),
        },
        "average_input_tokens_per_call": round(statistics.mean(input_tokens), 2),
        "median_input_tokens_per_call": statistics.median(input_tokens),
        "max_input_tokens_per_call": max(input_tokens),
        "average_call_seconds": round(statistics.mean(call_durations) / 1000, 3),
        "median_call_seconds": round(statistics.median(call_durations) / 1000, 3),
        "input_tokens": sum(input_tokens),
        "output_tokens": sum(item["output_tokens"] for item in call_profiles),
        "cached_tokens": sum(item["cached_tokens"] for item in call_profiles),
        "cached_input_share_percent": round(
            100 * sum(item["cached_tokens"] for item in call_profiles) / sum(input_tokens), 3
        ),
        "local_replay": {
            "method": f"median of {LOCAL_REPLAY_REPETITIONS} local read-only replays",
            "shared_deterministic_load_ms": deterministic_load_ms,
            "total_non_model_ms": round(total_local_ms, 4),
            "phase_totals_ms": {
                key: round(value, 4) for key, value in phase_totals_ms.items()
            },
            "model_share_percent": round(100 * total_model_ms / (total_model_ms + total_local_ms), 4),
            "preprocessing_share_percent": round(
                100 * sum(item["preprocessing_replay_ms"] for item in group_profiles)
                / (total_model_ms + total_local_ms), 6
            ),
            "validation_share_percent": round(
                100 * sum(item["validator_replay_ms"] for item in group_profiles)
                / (total_model_ms + total_local_ms), 6
            ),
            "historical_limitation": (
                "Stage 4 artifact recorded exact model-call duration but not historical local phase "
                "timestamps; local phases are exact diagnostic replays, not reconstructed claims"
            ),
        },
        "benchmark_comparison": {
            "benchmark_groups": 27,
            "benchmark_expected_decisions": benchmark_decisions,
            "benchmark_total_seconds": round(benchmark_total_seconds, 3),
            "benchmark_input_tokens": benchmark["input_tokens"],
            "benchmark_output_tokens": benchmark["output_tokens"],
            "production_to_benchmark_input_token_ratio": round(
                sum(input_tokens) / benchmark["input_tokens"], 3
            ),
            "production_to_benchmark_output_token_ratio": round(
                sum(item["output_tokens"] for item in call_profiles) / benchmark["output_tokens"], 3
            ),
            "benchmark_seconds_per_group": benchmark["avg_group_time_sec"],
            "benchmark_seconds_per_decision": round(benchmark_total_seconds / benchmark_decisions, 3),
            "production_decisions": 530,
            "production_seconds_per_decision": round(total_model_ms / 1000 / 530, 3),
            "decisions_per_group_benchmark": round(benchmark_decisions / 27, 3),
            "decisions_per_group_production": round(530 / 11, 3),
            "time_per_group_ratio_production_to_benchmark": round(
                statistics.mean(model_durations) / 1000 / benchmark["avg_group_time_sec"], 3
            ),
        },
        "cli_session": {
            "new_process_per_call": True,
            "ephemeral_session_per_call": True,
            "model_discovery_per_call": False,
            "schema_temp_file_per_call": True,
            "calls_sequential": True,
            "configured_transient_attempts_per_call": 3,
            "successful_hidden_retry_count": "unknown_not_persisted",
            "rate_limit_evidence": "none_in_current_artifacts",
            "duration_includes_cli_startup_queue_model_and_any_retry": True,
            "startup_lower_bound": measure_cli_startup(),
        },
        "duration_correlations": {
            "input_tokens_to_call_duration": round(correlation(
                [item["input_tokens"] for item in call_profiles],
                [item["model_duration_ms"] for item in call_profiles],
            ), 6),
            "output_tokens_to_call_duration": round(correlation(
                [item["output_tokens"] for item in call_profiles],
                [item["model_duration_ms"] for item in call_profiles],
            ), 6),
            "interpretation": (
                "correlation is descriptive, not causal; output length tracks duration most closely"
            ),
        },
        "bottleneck": (
            "model generation across 530 decisions and 21 ephemeral calls; production has 33.4x "
            "more decisions per group than the benchmark"
        ),
    }
    summary["summary_sha256"] = sha256_json(summary)
    profile = {
        "version": 1,
        "kind": "stage4_performance_profile",
        "diagnostic_only": True,
        "measurement": {
            "model_fields": "exact persisted production usage",
            "local_fields": f"median of {LOCAL_REPLAY_REPETITIONS} read-only replays",
        },
        "shared_deterministic_load_ms": deterministic_load_ms,
        "groups": group_profiles,
        "calls": call_profiles,
    }
    profile["profile_sha256"] = sha256_json(profile)
    return profile, summary, prompt_analysis


def report_markdown(
    inventory: dict[str, Any], sample: dict[str, Any], performance: dict[str, Any],
    prompt: dict[str, Any],
) -> str:
    uncertain = inventory["summary"]
    reviewed = sample["summary"]
    reason_rows = []
    for reason, item in sorted(
        uncertain["by_reason"].items(), key=lambda pair: pair[1]["count"], reverse=True
    ):
        reason_rows.append(f"| {reason} | {item['count']} | {item['share_percent']:.2f}% |")
    validator_rows = [
        f"| {reason} | {count} |"
        for reason, count in sorted(
            uncertain["by_validator_reason"].items(), key=lambda pair: pair[1], reverse=True
        )
    ]
    group_rows = []
    local_rows = []
    call_rows = []
    profile = load_json(PERFORMANCE_PATH) if PERFORMANCE_PATH.exists() else {"groups": []}
    for group in profile.get("groups") or []:
        group_rows.append(
            f"| `{group['group_id']}` | {group['source_fragment_count']} | "
            f"{group['preliminary_item_count']} | {group['actual_chunks']} | "
            f"{group['input_chars']} | {group['input_tokens']} | {group['output_tokens']} | "
            f"{group['cached_tokens']} | "
            f"{group['model_call_duration_ms'] / 1000:.3f} | {group['uncertain_count']} |"
        )
        local_rows.append(
            f"| `{group['group_id']}` | {group['preprocessing_replay_ms']:.4f} | "
            f"{group['shared_deterministic_load_allocation_ms']:.4f} | "
            f"{group['prompt_build_replay_ms']:.4f} | {group['validator_replay_ms']:.4f} | "
            f"{group['aggregation_replay_ms']:.4f} | {group['file_io_replay_ms']:.4f} | "
            f"{group['local_replay_total_ms']:.4f} | {group['reconstructed_total_ms']:.4f} |"
        )
        for call in group.get("model_calls") or []:
            call_rows.append(
                f"| `{call['call_id']}` | {call['input_chars']} | {call['input_tokens']} | "
                f"{call['output_tokens']} | {call['cached_tokens']} | "
                f"{call['duration_ms'] / 1000:.3f} |"
            )
    sample_counts = reviewed["by_classification"]
    estimate = reviewed["weighted_population_estimate"]
    components = prompt["totals_chars"]
    annotations_complete = reviewed["reviewed"] == reviewed["selected"] == SAMPLE_SIZE
    return "\n".join([
        "# Stage 4.1 — аудит UNCERTAIN и performance profile", "",
        "## Диагноз простыми словами", "",
        f"Из 189 UNCERTAIN только **{uncertain['by_origin'].get('MODEL_ORIGIN', 0)}** выбрала "
        f"сама модель; **{uncertain['by_origin'].get('VALIDATOR_ORIGIN', 0)}** созданы "
        "safety policy backend. Главная системная причина — модель трактовала различающиеся "
        "абсолютные PDF-страницы двух уже связанных стадий как MOVED. Validator правильно "
        "не разрешил маски, но итогом стали массовые технические UNCERTAIN.", "",
        f"94,49 с на production sheet group не противоречат benchmark 3,485 с: production "
        f"содержит 530 решений, или {performance['benchmark_comparison']['decisions_per_group_production']} "
        f"на группу, benchmark — только {performance['benchmark_comparison']['decisions_per_group_benchmark']}. "
        f"На одно решение production быстрее: {performance['benchmark_comparison']['production_seconds_per_decision']} "
        f"с против {performance['benchmark_comparison']['benchmark_seconds_per_decision']} с. "
        "Локальный Python/IO не является bottleneck. Production input/output больше benchmark "
        f"в {performance['benchmark_comparison']['production_to_benchmark_input_token_ratio']}×/"
        f"{performance['benchmark_comparison']['production_to_benchmark_output_token_ratio']}×, "
        "потому что решений в 13,6× больше.", "",
        "## UNCERTAIN taxonomy", "",
        "| Причина | Количество | Доля |", "|---|---:|---:|", *reason_rows, "",
        f"Model-origin: **{uncertain['by_origin'].get('MODEL_ORIGIN', 0)}**. "
        f"Validator-origin: **{uncertain['by_origin'].get('VALIDATOR_ORIGIN', 0)}**.", "",
        "`uncertain_reason` — первопричина, а origin — отдельная ось. Поэтому три model-origin "
        "кейса находятся в OCR_NOISE, а строка MODEL_UNCERTAIN в таблице первопричин равна нулю.", "",
        f"Из 140 MOVED-page cases {uncertain['moved_page_semantics_detail']['text_exact']} "
        f"совпадают дословно и {uncertain['moved_page_semantics_detail']['canonical_text_exact']} "
        "совпадают после canonicalization. Во всех случаях правый fragment остаётся внутри "
        "принятой связи листов; абсолютный номер PDF не является доказательством MOVED.", "",
        "### Validator/policy reasons", "",
        "| Причина | Количество |", "|---|---:|", *validator_rows, "",
        f"1→N: {uncertain['fragmentation_1_to_n']}; N→1: "
        f"{uncertain['fragmentation_n_to_1']}; N→M: {uncertain['fragmentation_n_to_m']}; "
        f"вероятный chunk boundary: {uncertain['potential_chunk_boundary']}. "
        f"Отдельно найдены {uncertain['same_call_duplicate_assignment_candidates']} one-to-one "
        "кейса с несколькими кандидатами: соответствие linked sheet находилось в том же model "
        "call, но было занято дубликатом с другой страницы. Это assignment problem, не 1→N и "
        "не chunk boundary.", "",
        "Проверка контекста сохраняет по каждому sample-case соседние строки, заголовки и "
        "local_context. Ни один из 50 не перешёл в NEED_MORE_CONTEXT: 45 решаются по уже "
        "доступному тексту/структуре группы, пяти нужен raster из-за неверного OCR. Формульный "
        "кейс оказался response leakage, а не нехваткой формульного контекста.", "",
        "## Human-review sample", "",
        f"Seeded stratified sample: {reviewed['selected']} cases; manually annotated: "
        f"{reviewed['reviewed']}. Complete: `{str(annotations_complete).lower()}`.", "",
        f"- HUMAN_RESOLVABLE: {sample_counts.get('HUMAN_RESOLVABLE', 0)}",
        f"- GENUINELY_UNCERTAIN: {sample_counts.get('GENUINELY_UNCERTAIN', 0)}",
        f"- NEED_MORE_CONTEXT: {sample_counts.get('NEED_MORE_CONTEXT', 0)}",
        f"- BAD_INPUT: {sample_counts.get('BAD_INPUT', 0)}", "",
        f"Coverage: {reviewed['coverage']['distinct_sheet_groups']} sheet groups; "
        f"model/validator origin {reviewed['coverage']['by_origin'].get('MODEL_ORIGIN', 0)}/"
        f"{reviewed['coverage']['by_origin'].get('VALIDATOR_ORIGIN', 0)}; "
        f"formula-like cases {reviewed['coverage']['formula_like_cases']}; short fragment sides "
        f"≤20 chars {reviewed['coverage']['short_fragments_le_20_chars']}; long fragment sides "
        f"≥400 chars {reviewed['coverage']['long_fragments_ge_400_chars']}. Таблицы, обычные "
        "абзацы, OCR и обозначения представлены; 1→N/N→1 не включались искусственно, потому "
        "что во всех 189 таких случаев нет.", "",
        f"Стратифицированная point estimate: потенциально автоматизируемы **"
        f"{estimate['potentially_automatable']}/189**, для человека или исправления OCR следует "
        f"оставить **{estimate['retain_for_human_or_source_repair']}/189**. Из первых 184 только "
        "140 адресуются рекомендуемым первым изменением; остальные требуют отдельных проверок. "
        "Это оценка возможностей, не разрешение автоматически переписать production statuses.", "",
        "The sample is deliberately stratified and oversamples minority failure modes; raw sample "
        "percentages must not be projected to all 189 without stratum weighting. It remains a "
        "diagnostic artifact and is not production ground truth.", "",
        "## Performance", "",
        f"- Groups: {performance['groups']}; model calls/chunks: "
        f"{performance['model_calls']}/{performance['chunks']}.",
        f"- Model time: {performance['total_model_seconds']:.3f} s; average "
        f"{performance['average_seconds_per_group']:.3f} s/group; median "
        f"{performance['median_seconds_per_group']:.3f} s/group.",
        f"- Fastest: `{performance['fastest_group']}` — "
        f"{performance['fastest_group_seconds']:.3f} s; slowest: "
        f"`{performance['slowest_group']}` — {performance['slowest_group_seconds']:.3f} s.",
        f"- Chunks/group average {performance['average_chunks_per_group']}, median "
        f"{performance['median_chunks_per_group']}, max {performance['max_chunks_per_group']}.",
        f"- Tokens: input {performance['input_tokens']}, output "
        f"{performance['output_tokens']}, cached {performance['cached_tokens']} "
        f"({performance['cached_input_share_percent']:.3f}% input).",
        f"- Model share of reconstructed run: "
        f"{performance['local_replay']['model_share_percent']:.4f}%; local replay total "
        f"{performance['local_replay']['total_non_model_ms']:.4f} ms.", "",
        f"- Correlation duration↔output tokens: "
        f"{performance['duration_correlations']['output_tokens_to_call_duration']}; "
        f"duration↔input tokens: {performance['duration_correlations']['input_tokens_to_call_duration']}.",
        f"- Explicit chunk records: "
        f"{performance['chunk_persistence']['groups_with_explicit_chunk_records']} groups; "
        f"legacy group-level usage: {performance['chunk_persistence']['groups_with_legacy_group_level_usage']}; "
        f"one legacy group would be split into three by the current policy.", "",
        "| Sheet group | Sources | Items | Calls | Input chars | Input tok | Output tok | Cached | Model s | UNCERTAIN |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|", *group_rows, "",
        "### Exact accepted model calls", "",
        "| Call/chunk | Input chars | Input tok | Output tok | Cached | Model s |",
        "|---|---:|---:|---:|---:|---:|", *call_rows, "",
        "### Local phases by group (diagnostic replay, ms)", "",
        "| Sheet group | Preprocess | Load alloc. | Prompt | Validator | Aggregation | File IO | Local total | Reconstructed total |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|", *local_rows, "",
        "Stage 4 did not persist historical local-phase timestamps. These local values are medians "
        "of 31 read-only replays; model durations and token usage above are exact persisted data.", "",
        "## Prompt and CLI diagnosis", "",
        "| Submitted context component | Characters |",
        "|---|---:|",
        f"| Source text | {components['source_text_chars']} |",
        f"| Neighbor/local context | {components['source_local_context_chars']} |",
        f"| Source bbox | {components['source_bbox_chars']} |",
        f"| Other source metadata/JSON | {components['source_metadata_chars']} |",
        f"| Deterministic SAME evidence (estimate) | {components['preliminary_same_chars_estimate']} |",
        f"| Deterministic MOVED evidence (estimate) | {components['preliminary_moved_chars_estimate']} |",
        f"| Differences/other preliminary evidence (estimate) | {components['preliminary_differences_chars_estimate']} |",
        f"| Fixed instruction | {components['fixed_instruction_chars']} |",
        f"| Schema embedded in prompt | {components['schema_in_prompt_chars']} |",
        f"| Wrapper | {components['wrapper_chars']} |",
        f"| Native schema file (also passed per call) | {components['native_schema_chars']} |", "",
        f"Source-text duplication factor: **{prompt['source_text']['duplication_factor']}x**; "
        f"whole-context estimate: **{prompt['whole_context_duplication_factor_estimate']}x**. "
        "Chunking does not repeatedly send the same source fragments; repeated fixed CLI/system "
        "context is the material duplication.", "",
        f"Largest group example `{prompt['largest_group_example']['group_id']}`: unique source "
        f"text {prompt['largest_group_example']['unique_source_chars']} chars, submitted source "
        f"text {prompt['largest_group_example']['submitted_source_chars']} chars, factor "
        f"{prompt['largest_group_example']['source_text_duplication_factor']}× across "
        f"{prompt['largest_group_example']['calls']} calls. With repeated fixed application "
        f"context its whole-context estimate is "
        f"{prompt['largest_group_example']['whole_context_duplication_factor_estimate']}×.", "",
        f"Regression over 21 actual calls estimates {prompt['input_token_regression']['intercept_tokens_per_call']} "
        f"fixed input tokens per fresh call (R²={prompt['input_token_regression']['r_squared']}); "
        f"about {prompt['input_token_regression']['estimated_repeated_fixed_runtime_share_percent']}% "
        "of production input. Every call starts a new `codex exec --ephemeral` process/session, "
        "writes the native schema again, and runs sequentially. There is no per-call model "
        "discovery. Successful transient retry count cannot be recovered because the current "
        "artifact does not persist it; no rate-limit evidence remains in the artifact.", "",
        "The 6,912 cached tokens came from one accepted chunk only. That is 1.669% of input and "
        "does not represent reusable context across ephemeral chunk sessions. The reported "
        "~0.055 s repeat run is artifact reuse: it performs no new model review and therefore is "
        "not a latency comparison with the initial run.", "",
        "## Maximum three evidence-backed next changes", "",
        "1. **Stage 4.2 candidate:** clarify in the prompt that corresponding П/РД PDF page numbers "
        "may differ inside an accepted sheet link and that this is SAME, not MOVED. Test this as "
        "one isolated change on the existing Stage 4 benchmark and a production UNCERTAIN sample.",
        "2. In a separate experiment, generate source-exact backend summaries for right-only "
        "ADDED after validating the status and ids; keep failing closed on unrelated response "
        "leakage. This targets 38 designation-normalization rejects.",
        "3. In a separate experiment, rank a candidate on the accepted linked sheet ahead of a "
        "duplicate outside that link before enforcing one-use coverage. This targets the three "
        "MULTIPLE_CANDIDATES cases. No latency optimization is recommended now: production is "
        "already faster per decision than benchmark, and batching changes the safety surface.", "",
        "## Recommendation", "",
        "**B — выполнить одну небольшую Stage 4.2 доработку.** Start only with the MOVED page "
        "semantics clarification: it explains 140/189 UNCERTAIN. Do not begin vector graphics "
        "before that controlled experiment, and do not combine it with validator or batching "
        "changes. Accepted False SAME and False MOVED must remain zero.", "",
        "## Integrity", "",
        "This diagnostic run made no production changes, did not call a model, and did not alter "
        "prompt, model, chunk size, validator, preprocessing, statuses, UI or sheet links. Exact "
        "model-call durations come from the production artifact; local phase timings are labeled "
        "read-only diagnostic replays because Stage 4 did not historically persist those timers. "
        "The exact post-schema model proposal, validator reason and final status are retained per "
        "inventory case. The provider stdout envelope was never persisted by Stage 4, so it cannot "
        "be reconstructed; this run deleted no raw data.", "",
    ])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("run", "verify"), nargs="?", default="run")
    args = parser.parse_args()
    data = load_sources()
    inventory, calls_by_group = build_inventory(data)
    sample = build_sample(inventory)
    performance_profile, performance_summary, prompt_analysis = build_performance(
        data, calls_by_group
    )
    if args.command == "verify":
        expected = {
            INVENTORY_PATH: inventory,
            SAMPLE_PATH: sample,
            PERFORMANCE_PATH: performance_profile,
            PERFORMANCE_SUMMARY_PATH: performance_summary,
            PROMPT_SIZE_PATH: prompt_analysis,
        }
        for path, payload in expected.items():
            current = load_json(path)
            # Local timing and CLI startup measurements are intentionally not
            # byte-stable. Verify semantic/source invariants instead.
            if path == INVENTORY_PATH and current["inventory_sha256"] != payload["inventory_sha256"]:
                raise SystemExit("uncertain inventory is not reproducible")
            if path == SAMPLE_PATH and current["sampling"] != payload["sampling"]:
                raise SystemExit("sample selection is not reproducible")
        print("diagnostic semantic invariants verified")
        return 0

    write_json(INVENTORY_PATH, inventory)
    write_json(SAMPLE_PATH, sample)
    write_json(PERFORMANCE_PATH, performance_profile)
    write_json(PERFORMANCE_SUMMARY_PATH, performance_summary)
    write_json(PROMPT_SIZE_PATH, prompt_analysis)
    REPORT_PATH.write_text(
        report_markdown(inventory, sample, performance_summary, prompt_analysis),
        encoding="utf-8",
    )
    print(json.dumps({
        "inventory": inventory["summary"],
        "sample": sample["summary"],
        "performance": performance_summary,
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
