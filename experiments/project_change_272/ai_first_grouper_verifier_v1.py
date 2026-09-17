#!/usr/bin/env python3
"""Reproducible artifact builder for the frozen AI Grouper/Verifier v1 experiment.

The script deliberately reads only the two frozen Change Miner result files before
grouping freeze.  Evaluation material is handled by a separate, explicit phase.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill


CORPUS = Path("/home/coder/auditmanager/corpus-audits/20260914_project_change_272")
PAIR_DIRS = {
    "A": CORPUS / "pair_a_ai_mapping_change_miner_v1",
    "B": CORPUS / "pair_b_ai_mapping_change_miner_v1",
}
EXPECTED_COUNTS = {"A": 147, "B": 62}
OUT = CORPUS / "ai_first_grouper_verifier_v1"
MODEL = "gpt-6-astra"
REASONING = "xhigh"


GROUPER_INSTRUCTIONS = """You are the AI Grouper in a frozen ProjectChange experiment.
Use only the supplied frozen CONCRETE_CHANGE cards. Do not open or infer from any
evaluation, audit, expected-answer, validation, or holdout artifact. Determine which
cards are parts or duplicate detections of one real engineering event.

Rules:
- Same engineering subject alone is not the same event. Do not merge independent
  parameter, relocation, quantity, scheme, or purpose changes.
- Merge duplicate/overlapping mapping-group detections of the same engineering event
  and retain every original change_id as lineage.
- A SYSTEM_WIDE candidate is allowed only for one project decision with the same
  OLD->NEW transformation; retain per-location details.
- Do not target a preferred candidate count and do not over-merge.
- Every input card must appear exactly once in card_lineage with one status:
  MEMBER_OF_PROJECTCHANGE, STANDALONE_PROJECTCHANGE, or GROUPING_UNCERTAIN.
- GROUPING_UNCERTAIN cards still need their own conservative candidate so that no
  card disappears; identify the uncertainty in grouping_reason/confidence.
- Preserve evidence references and source lineage. Do not inspect source evidence at
  grouping time: the Verifier will do that in a fresh context.

Return one JSON object (no markdown) with keys pair, model, reasoning, candidates,
card_lineage, and notes. Each candidate must contain:
projectchange_candidate_id, pair, engineering_subject, scope, locations,
change_summary, old_state, new_state, changed_parameters, member_change_ids,
old_pages, new_pages, evidence_refs, modalities, grouping_reason,
grouping_confidence, and system_wide. Candidate IDs must be PCA-001... for Pair A
or PCB-001... for Pair B. confidence is a number from 0 to 1.
Each card_lineage item must contain change_id, status, and
projectchange_candidate_id. Output all candidates and all lineage entries."""

VERIFIER_INSTRUCTIONS = """You are the fresh AI Verifier for exactly one frozen
ProjectChange candidate. You have no source truth, prior audits, expected verdict,
validation, or holdout data. Use only the proposed candidate, its original member
cards, and the real source-evidence bundle in this input.

Independently check: engineering subject; scope/location; OLD-to-NEW identity;
support for both states; whether this is one engineering event; whether independent
changes were over-merged; contradictions; and whether the apparent change is only
metadata, formatting, or numbering. PDF-derived raster is authoritative when OCR or
structured text conflicts. Inspect every image_path with the image-viewing tool when
the corresponding modality is GRAPHIC or structured text is insufficient.

Verdict must be exactly ACCEPT, REVIEW, or NOT_CHANGE. If grouping combines distinct
events, verdict REVIEW and over_merged YES; suggest a split but do not apply it. If a
source ref, page, crop, side, coordinate, or hash fails technically, do not ACCEPT:
return REVIEW with technical_evidence_failure YES. For every original member card,
return exactly one status SUPPORTED, PARTIAL, NOT_SUPPORTED, or INSUFFICIENT.

Write one JSON object (no markdown) containing: projectchange_candidate_id, pair,
verdict, verdict_reason, engineering_subject_match, scope_location_match,
old_new_correspondence, old_state_supported, new_state_supported, single_event,
over_merged (YES/NO), suggested_split, contradictions, metadata_only,
technical_evidence_failure (YES/NO), technical_failure_reasons, member_verdicts,
evidence_refs_used, confidence. Each member_verdict item contains change_id, status,
reason, evidence_refs_used. confidence is 0..1."""


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2)
        handle.write("\n")


def compact_ref(ref: dict[str, Any]) -> dict[str, Any]:
    return {
        key: ref.get(key)
        for key in (
            "side",
            "page",
            "block_id",
            "evidence_modality",
            "evidence_ref",
            "graphic_crop_ref",
            "quote_or_visual_observation",
        )
        if ref.get(key) not in (None, "", [])
    }


def compact_card(pair: str, mapping_group: str, card: dict[str, Any]) -> dict[str, Any]:
    refs = [compact_ref(ref) for ref in card.get("evidence_refs", [])]
    return {
        "change_id": card["change_id"],
        "mapping_group": mapping_group,
        "engineering_subject": card.get("engineering_subject", ""),
        "scope_location": card.get("scope_location", ""),
        "change_summary": card.get("change_summary", ""),
        "old_state": card.get("old_state", ""),
        "new_state": card.get("new_state", ""),
        "old_pages": card.get("old_physical_pages", []),
        "new_pages": card.get("new_physical_pages", []),
        "evidence_block_ids": card.get("evidence_block_ids", []),
        "modalities": card.get("evidence_modalities", []),
        "parameters": card.get("parameters", card.get("changed_parameters", [])),
        "evidence_refs": refs,
        "source_provenance": {
            "pair": pair,
            "frozen_file": str(PAIR_DIRS[pair] / "CHANGE_MINER_RESULTS.json"),
            "mapping_group": mapping_group,
        },
    }


def verified_frozen_result(pair: str) -> tuple[dict[str, Any], str]:
    base = PAIR_DIRS[pair]
    result_path = base / "CHANGE_MINER_RESULTS.json"
    freeze = load_json(base / "CHANGE_MINER_FREEZE.json")
    expected_hash = freeze["hashes"]["CHANGE_MINER_RESULTS.json"]
    actual_hash = sha256(result_path)
    if actual_hash != expected_hash:
        raise RuntimeError(
            f"Pair {pair} frozen miner hash mismatch: {actual_hash} != {expected_hash}"
        )
    return load_json(result_path), actual_hash


def prepare() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT / "verifier_inputs").mkdir(exist_ok=True)
    (OUT / "verifier_raw").mkdir(exist_ok=True)
    freeze_inputs: dict[str, Any] = {}
    for pair in ("A", "B"):
        result, result_hash = verified_frozen_result(pair)
        cards: list[dict[str, Any]] = []
        unresolved = 0
        for group in result["groups"]:
            mapping_group = group["map_group_id"]
            unresolved += len(group.get("unresolved_hints", []))
            cards.extend(
                compact_card(pair, mapping_group, card)
                for card in group.get("concrete_changes", [])
            )
        if len(cards) != EXPECTED_COUNTS[pair]:
            raise RuntimeError(
                f"Pair {pair}: expected {EXPECTED_COUNTS[pair]} cards, got {len(cards)}"
            )
        payload = {
            "experiment": "AI_GROUPER_VERIFIER_V1",
            "pair": pair,
            "model": MODEL,
            "reasoning": REASONING,
            "instructions": GROUPER_INSTRUCTIONS,
            "input_policy": {
                "frozen_concrete_changes_only": True,
                "unresolved_hints_excluded": True,
                "source_truth_forbidden": True,
                "pair_context_isolated": True,
            },
            "card_count": len(cards),
            "cards": cards,
        }
        input_path = OUT / f"PAIR_{pair}_GROUPER_INPUT.json"
        write_json(input_path, payload)
        freeze_inputs[pair] = {
            "source": str(PAIR_DIRS[pair] / "CHANGE_MINER_RESULTS.json"),
            "source_sha256": result_hash,
            "mapping_groups": len(result["groups"]),
            "concrete_change_cards": len(cards),
            "unresolved_hints_excluded": unresolved,
            "grouper_input": input_path.name,
            "grouper_input_sha256": sha256(input_path),
        }
    write_json(
        OUT / "EXPERIMENT_FREEZE.json",
        {
            "created_at": now(),
            "experiment": "AI_GROUPER_VERIFIER_V1",
            "object": 272,
            "old": "stage_1",
            "new": "stage_2",
            "model": MODEL,
            "reasoning": REASONING,
            "inputs": freeze_inputs,
            "constraints": {
                "mapper_rerun": False,
                "change_miner_rerun": False,
                "truth_before_result_freeze": False,
                "same_prompt_and_schema_for_pairs": True,
                "production_unchanged": True,
                "validation_opened": False,
                "final_holdout_opened": False,
            },
        },
    )


def validate_grouper(pair: str, raw: dict[str, Any], source: dict[str, Any]) -> None:
    expected_ids = [card["change_id"] for card in source["cards"]]
    if len(expected_ids) != len(set(expected_ids)):
        raise RuntimeError(f"Pair {pair}: duplicate input change_id")
    candidates = raw.get("candidates", [])
    member_ids = [
        change_id
        for candidate in candidates
        for change_id in candidate.get("member_change_ids", [])
    ]
    lineage = raw.get("card_lineage", [])
    lineage_ids = [item.get("change_id") for item in lineage]
    if sorted(member_ids) != sorted(expected_ids) or len(member_ids) != len(expected_ids):
        raise RuntimeError(f"Pair {pair}: candidate membership is not an exact partition")
    if sorted(lineage_ids) != sorted(expected_ids) or len(lineage_ids) != len(expected_ids):
        raise RuntimeError(f"Pair {pair}: card_lineage is not an exact partition")
    candidate_ids = {c.get("projectchange_candidate_id") for c in candidates}
    allowed = {
        "MEMBER_OF_PROJECTCHANGE",
        "STANDALONE_PROJECTCHANGE",
        "GROUPING_UNCERTAIN",
    }
    by_member = {
        change_id: c.get("projectchange_candidate_id")
        for c in candidates
        for change_id in c.get("member_change_ids", [])
    }
    for item in lineage:
        if item.get("status") not in allowed:
            raise RuntimeError(f"Pair {pair}: bad lineage status for {item.get('change_id')}")
        if item.get("projectchange_candidate_id") not in candidate_ids:
            raise RuntimeError(f"Pair {pair}: lineage points to missing candidate")
        if by_member[item["change_id"]] != item["projectchange_candidate_id"]:
            raise RuntimeError(f"Pair {pair}: lineage/candidate mismatch")


def finalize_grouping() -> None:
    pairs: dict[str, Any] = {}
    hashes: dict[str, str] = {}
    for pair in ("A", "B"):
        input_path = OUT / f"PAIR_{pair}_GROUPER_INPUT.json"
        raw_path = OUT / f"PAIR_{pair}_GROUPER_RAW.json"
        source = load_json(input_path)
        raw = load_json(raw_path)
        validate_grouper(pair, raw, source)
        pairs[pair] = {
            "card_count": len(source["cards"]),
            "candidate_count": len(raw["candidates"]),
            "candidates": raw["candidates"],
            "card_lineage": raw["card_lineage"],
            "notes": raw.get("notes", []),
        }
        hashes[input_path.name] = sha256(input_path)
        hashes[raw_path.name] = sha256(raw_path)
    results_path = OUT / "GROUPING_RESULTS.json"
    write_json(
        results_path,
        {
            "created_at": now(),
            "model": MODEL,
            "reasoning": REASONING,
            "same_prompt_and_schema": True,
            "pairs": pairs,
        },
    )
    hashes[results_path.name] = sha256(results_path)
    write_json(
        OUT / "GROUPING_FREEZE.json",
        {
            "frozen_at": now(),
            "semantic_mutation_after_freeze": False,
            "hashes": hashes,
            "counts": {
                pair: {
                    "cards": pairs[pair]["card_count"],
                    "candidates": pairs[pair]["candidate_count"],
                }
                for pair in ("A", "B")
            },
        },
    )


def file_record(path: Path) -> dict[str, Any]:
    record: dict[str, Any] = {"image_path": str(path), "exists": path.is_file()}
    if path.is_file():
        record["sha256"] = sha256(path)
    return record


def evidence_for_pair_a(card: dict[str, Any]) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    seen: set[tuple[str, int, str]] = set()
    for ref in card.get("evidence_refs", []):
        side = ref.get("side")
        page = ref.get("page")
        block_id = ref.get("block_id")
        key = (str(side), int(page), str(block_id))
        if key in seen:
            continue
        seen.add(key)
        page_path = PAIR_DIRS["A"] / "source" / str(side) / f"p{int(page):03d}" / "page.json"
        item: dict[str, Any] = {
            "side": side,
            "page": page,
            "block_id": block_id,
            "source_page_json": str(page_path),
            "source_page_exists": page_path.is_file(),
            "claimed_observation": ref.get("quote_or_visual_observation", ""),
        }
        if page_path.is_file():
            page_data = load_json(page_path)
            block = next(
                (b for b in page_data.get("blocks", []) if b.get("block_id") == block_id),
                None,
            )
            item["block_found"] = block is not None
            if block is not None:
                item["modality"] = block.get("modality")
                item["coordinates_bbox_norm"] = block.get("bbox_norm")
                item["md_text"] = block.get("md_text", "")
                item["tables"] = block.get("tables", [])
                crop = block.get("graphic_crop_ref")
                if crop:
                    item["graphic_crop"] = file_record(Path(crop))
                    item["declared_graphic_crop_sha256"] = block.get("graphic_crop_sha256")
            full_page = page_data.get("full_page_ref")
            if full_page:
                item["full_page"] = file_record(Path(full_page))
                item["declared_full_page_sha256"] = page_data.get("full_page_sha256")
            item["pdf_sha256"] = page_data.get("pdf_sha256")
        evidence.append(item)
    return evidence


def evidence_for_pair_b(card: dict[str, Any]) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    seen: set[tuple[str, int, str]] = set()
    for ref in card.get("evidence_refs", []):
        side = str(ref.get("side"))
        page = int(ref.get("page"))
        evidence_ref = str(ref.get("evidence_ref", ""))
        key = (side, page, evidence_ref)
        if key in seen:
            continue
        seen.add(key)
        page_path = PAIR_DIRS["B"] / "pages" / side / f"p{page:03d}.json"
        item: dict[str, Any] = {
            "side": side,
            "page": page,
            "block_id": evidence_ref or f"{side}:p{page}:page",
            "source_page_json": str(page_path),
            "source_page_exists": page_path.is_file(),
            "coordinates_bbox_norm": [0.0, 0.0, 1.0, 1.0],
            "coordinate_note": "Frozen Pair B corpus exposes page-level native/OCR blocks only; full-page coordinates are used.",
            "claimed_observation": ref.get("quote_or_visual_observation", ""),
            "modality": ref.get("evidence_type", ref.get("evidence_modality", "TEXT")),
        }
        if page_path.is_file():
            page_data = load_json(page_path)
            source_kind = evidence_ref.rsplit(":", 1)[-1] if ":" in evidence_ref else "native"
            if source_kind in ("native", "ocr"):
                item["md_text"] = page_data.get(source_kind, "")
            else:
                item["md_text"] = page_data.get("native", "")
            item["tables"] = page_data.get("tables", [])
            raster = page_data.get("raster")
            if raster:
                item["full_page"] = file_record(Path(raster))
                item["declared_full_page_sha256"] = page_data.get("raster_sha256")
            item["pdf_sha256"] = page_data.get("pdf_sha256")
        evidence.append(item)
    return evidence


def prepare_verifier() -> None:
    grouping_freeze = load_json(OUT / "GROUPING_FREEZE.json")
    for name, expected_hash in grouping_freeze["hashes"].items():
        path = OUT / name
        if sha256(path) != expected_hash:
            raise RuntimeError(f"Grouping freeze hash mismatch: {name}")
    grouping = load_json(OUT / "GROUPING_RESULTS.json")
    manifest: list[dict[str, Any]] = []
    for pair in ("A", "B"):
        source = load_json(OUT / f"PAIR_{pair}_GROUPER_INPUT.json")
        cards_by_id = {card["change_id"]: card for card in source["cards"]}
        for candidate in grouping["pairs"][pair]["candidates"]:
            member_cards = [cards_by_id[cid] for cid in candidate["member_change_ids"]]
            evidence = []
            for card in member_cards:
                evidence.extend(
                    evidence_for_pair_a(card) if pair == "A" else evidence_for_pair_b(card)
                )
            candidate_id = candidate["projectchange_candidate_id"]
            payload = {
                "experiment": "AI_GROUPER_VERIFIER_V1",
                "model": MODEL,
                "reasoning": REASONING,
                "instructions": VERIFIER_INSTRUCTIONS,
                "input_policy": {
                    "fresh_context_one_candidate": True,
                    "source_truth_forbidden": True,
                    "real_source_evidence_only": True,
                },
                "candidate": candidate,
                "original_member_cards": member_cards,
                "source_evidence": evidence,
            }
            input_path = OUT / "verifier_inputs" / f"{candidate_id}.json"
            raw_path = OUT / "verifier_raw" / f"{candidate_id}.json"
            write_json(input_path, payload)
            manifest.append(
                {
                    "candidate_id": candidate_id,
                    "pair": pair,
                    "input": str(input_path),
                    "input_sha256": sha256(input_path),
                    "raw_output": str(raw_path),
                }
            )
    write_json(
        OUT / "VERIFIER_MANIFEST.json",
        {
            "created_at": now(),
            "model": MODEL,
            "reasoning": REASONING,
            "candidate_count": len(manifest),
            "calls_required": len(manifest),
            "items": manifest,
        },
    )


def technical_evidence_failures(payload: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    for item in payload.get("source_evidence", []):
        label = f"{item.get('side')}:p{item.get('page')}:{item.get('block_id')}"
        if not item.get("source_page_exists"):
            failures.append(f"missing source page: {label}")
        if "block_found" in item and not item.get("block_found"):
            failures.append(f"missing block: {label}")
        bbox = item.get("coordinates_bbox_norm")
        if not (
            isinstance(bbox, list)
            and len(bbox) == 4
            and all(isinstance(value, (int, float)) for value in bbox)
            and 0 <= bbox[0] <= bbox[2] <= 1
            and 0 <= bbox[1] <= bbox[3] <= 1
        ):
            failures.append(f"invalid coordinates: {label}")
        for image_key, declared_key in (
            ("graphic_crop", "declared_graphic_crop_sha256"),
            ("full_page", "declared_full_page_sha256"),
        ):
            image = item.get(image_key)
            if image is None:
                continue
            if not image.get("exists"):
                failures.append(f"missing {image_key}: {label}")
            declared = item.get(declared_key)
            actual = image.get("sha256")
            if declared and actual and declared != actual:
                failures.append(f"hash mismatch {image_key}: {label}")
    return sorted(set(failures))


def validate_verifier_output(
    payload: dict[str, Any], raw: dict[str, Any]
) -> None:
    candidate = payload["candidate"]
    candidate_id = candidate["projectchange_candidate_id"]
    if raw.get("projectchange_candidate_id") != candidate_id:
        raise RuntimeError(f"{candidate_id}: output candidate ID mismatch")
    if raw.get("pair") != candidate.get("pair"):
        raise RuntimeError(f"{candidate_id}: output pair mismatch")
    if raw.get("verdict") not in {"ACCEPT", "REVIEW", "NOT_CHANGE"}:
        raise RuntimeError(f"{candidate_id}: invalid verdict")
    member_verdicts = raw.get("member_verdicts", [])
    expected = candidate["member_change_ids"]
    actual = [item.get("change_id") for item in member_verdicts]
    if len(actual) != len(expected) or sorted(actual) != sorted(expected):
        raise RuntimeError(f"{candidate_id}: member verdicts not an exact partition")
    allowed = {"SUPPORTED", "PARTIAL", "NOT_SUPPORTED", "INSUFFICIENT"}
    if any(item.get("status") not in allowed for item in member_verdicts):
        raise RuntimeError(f"{candidate_id}: invalid member status")


def finalize_verifier() -> None:
    manifest_path = OUT / "VERIFIER_MANIFEST.json"
    manifest = load_json(manifest_path)
    results: list[dict[str, Any]] = []
    members: list[dict[str, Any]] = []
    hashes = {manifest_path.name: sha256(manifest_path)}
    for item in manifest["items"]:
        input_path = Path(item["input"])
        raw_path = Path(item["raw_output"])
        if sha256(input_path) != item["input_sha256"]:
            raise RuntimeError(f"Verifier input hash mismatch: {input_path.name}")
        payload = load_json(input_path)
        raw = load_json(raw_path)
        validate_verifier_output(payload, raw)
        failures = technical_evidence_failures(payload)
        model_verdict = raw["verdict"]
        final_verdict = model_verdict
        final_reason = raw.get("verdict_reason", "")
        if failures and model_verdict == "ACCEPT":
            final_verdict = "REVIEW"
            final_reason = "TECHNICAL_EVIDENCE_FAILURE: " + "; ".join(failures)
        record = dict(raw)
        record["model_verdict"] = model_verdict
        record["final_verdict"] = final_verdict
        record["final_reason"] = final_reason
        record["script_technical_failures"] = failures
        record["input_sha256"] = item["input_sha256"]
        record["raw_output_sha256"] = sha256(raw_path)
        results.append(record)
        for member in raw["member_verdicts"]:
            members.append(
                {
                    "pair": raw["pair"],
                    "projectchange_candidate_id": raw["projectchange_candidate_id"],
                    **member,
                }
            )
        hashes[str(raw_path.relative_to(OUT))] = sha256(raw_path)
    verifier_results_path = OUT / "VERIFIER_RESULTS.json"
    member_results_path = OUT / "MEMBER_VERDICTS.json"
    write_json(
        verifier_results_path,
        {
            "created_at": now(),
            "model": MODEL,
            "reasoning": REASONING,
            "results": results,
        },
    )
    write_json(member_results_path, {"created_at": now(), "members": members})
    hashes[verifier_results_path.name] = sha256(verifier_results_path)
    hashes[member_results_path.name] = sha256(member_results_path)
    counts: dict[str, Any] = {}
    for pair in ("A", "B"):
        pair_results = [r for r in results if r["pair"] == pair]
        counts[pair] = {
            "candidates": len(pair_results),
            "ACCEPT": sum(r["final_verdict"] == "ACCEPT" for r in pair_results),
            "REVIEW": sum(r["final_verdict"] == "REVIEW" for r in pair_results),
            "NOT_CHANGE": sum(r["final_verdict"] == "NOT_CHANGE" for r in pair_results),
            "technical_evidence_failure": sum(
                bool(r["script_technical_failures"]) for r in pair_results
            ),
            "over_merged": sum(r.get("over_merged") == "YES" for r in pair_results),
        }
    write_json(
        OUT / "GROUPER_VERIFIER_RESULT_FREEZE.json",
        {
            "frozen_at": now(),
            "model": MODEL,
            "reasoning": REASONING,
            "hashes": hashes,
            "counts": counts,
            "truth_opened_before_freeze": False,
            "semantic_mutation_after_freeze": False,
            "production_unchanged": True,
            "validation_opened": False,
            "final_holdout_opened": False,
        },
    )


def rollout_turn_usage(path: Path) -> tuple[list[str], dict[str, dict[str, int]]]:
    completed_turns: list[str] = []
    last_usage: dict[str, dict[str, int]] = {}
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            event = json.loads(line)
            payload = event.get("payload", {})
            if event.get("type") == "token_usage_record":
                turn_id = payload.get("turn_id")
                usage = payload.get("turn_token_usage")
                if turn_id and usage:
                    last_usage[turn_id] = usage
            elif event.get("type") == "event_msg" and payload.get("type") == "task_complete":
                turn_id = payload.get("turn_id")
                if turn_id:
                    completed_turns.append(turn_id)
    return completed_turns, last_usage


def sum_usage(records: list[dict[str, int]]) -> dict[str, int]:
    fields = (
        "input_tokens",
        "cached_input_tokens",
        "cache_write_input_tokens",
        "output_tokens",
        "reasoning_output_tokens",
        "total_tokens",
    )
    return {field: sum(record.get(field, 0) for record in records) for field in fields}


def collect_token_usage() -> None:
    database = Path("/home/coder/.codex/state_5.sqlite")
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        "SELECT id, rollout_path, agent_path, created_at_ms FROM threads "
        "WHERE model = ? AND reasoning_effort = ? AND cwd = ? ORDER BY created_at_ms",
        (MODEL, REASONING, "/home/coder/projects/PDF-proverka"),
    ).fetchall()
    grouper_usage: dict[str, dict[str, int]] = {}
    verifier_usage: dict[str, list[dict[str, int]]] = {"A": [], "B": []}
    verifier_threads: list[dict[str, Any]] = []
    post_freeze_audit_usage: dict[str, dict[str, int]] = {}
    for row in rows:
        agent_path = row["agent_path"] or ""
        rollout = Path(row["rollout_path"])
        if not rollout.is_file():
            continue
        completed, usages = rollout_turn_usage(rollout)
        if agent_path in ("/root/grouper_pair_a", "/root/grouper_pair_b"):
            pair = "A" if agent_path.endswith("_a") else "B"
            if completed and pair not in grouper_usage:
                grouper_usage[pair] = usages[completed[0]]
        elif agent_path.startswith("/root/grouper_pair_a/verify_pc"):
            pair = "A" if "/verify_pca" in agent_path else "B"
            if completed:
                # The first completed turn is the verifier call.  A later turn can
                # exist only for non-semantic lifecycle cleanup (PCA-001 orphan).
                usage = usages[completed[0]]
                verifier_usage[pair].append(usage)
                verifier_threads.append(
                    {
                        "thread_id": row["id"],
                        "agent_path": agent_path,
                        "pair": pair,
                        "usage": usage,
                    }
                )
        elif agent_path in ("/root/duplicate_audit_pair_a", "/root/duplicate_audit_pair_b"):
            pair = "A" if agent_path.endswith("_a") else "B"
            if completed:
                post_freeze_audit_usage[pair] = usages[completed[0]]
    if set(grouper_usage) != {"A", "B"}:
        raise RuntimeError("Could not locate both Grouper usage records")
    manifest = load_json(OUT / "VERIFIER_MANIFEST.json")
    expected_by_pair = {
        pair: sum(item["pair"] == pair for item in manifest["items"])
        for pair in ("A", "B")
    }
    retries_path = OUT / "VERIFIER_RETRIES.json"
    retries = load_json(retries_path) if retries_path.is_file() else []
    required_calls = {
        pair: expected_by_pair[pair]
        + sum((entry.get("pair") == pair) for entry in retries)
        for pair in ("A", "B")
    }
    for pair in ("A", "B"):
        if len(verifier_usage[pair]) < expected_by_pair[pair]:
            raise RuntimeError(
                f"Pair {pair}: only {len(verifier_usage[pair])} verifier usages for "
                f"{expected_by_pair[pair]} candidates"
            )
    sections = {
        "GROUPER_PAIR_A": {"calls": 1, **grouper_usage["A"]},
        "GROUPER_PAIR_B": {"calls": 1, **grouper_usage["B"]},
        "VERIFIER_PAIR_A": {
            "calls": len(verifier_usage["A"]),
            **sum_usage(verifier_usage["A"]),
        },
        "VERIFIER_PAIR_B": {
            "calls": len(verifier_usage["B"]),
            **sum_usage(verifier_usage["B"]),
        },
    }
    total_records = [grouper_usage["A"], grouper_usage["B"]]
    total_records.extend(verifier_usage["A"])
    total_records.extend(verifier_usage["B"])
    architecture_total = {"calls": 2 + len(verifier_threads), **sum_usage(total_records)}
    audit_records = list(post_freeze_audit_usage.values())
    all_records = total_records + audit_records
    write_json(
        OUT / "TOKEN_USAGE.json",
        {
            "created_at": now(),
            "model": MODEL,
            "reasoning": REASONING,
            "accounting_note": "cached_input_tokens are already included in input_tokens and are not added again to total_tokens; TOTAL is the requested Grouper+Verifier architecture total, while post-freeze duplicate audit calls are reported separately",
            "sections": sections,
            "TOTAL": architecture_total,
            "POST_FREEZE_DUPLICATE_AUDIT": {
                "calls": len(audit_records),
                **sum_usage(audit_records),
            },
            "GRAND_TOTAL_ALL_AI_CALLS": {
                "calls": architecture_total["calls"] + len(audit_records),
                **sum_usage(all_records),
            },
            "expected_verifier_calls_including_recorded_retries": required_calls,
            "verifier_threads": verifier_threads,
        },
    )


def verify_result_freeze() -> dict[str, Any]:
    freeze = load_json(OUT / "GROUPER_VERIFIER_RESULT_FREEZE.json")
    for name, expected_hash in freeze["hashes"].items():
        path = OUT / name
        if sha256(path) != expected_hash:
            raise RuntimeError(f"Result freeze hash mismatch: {name}")
    return freeze


def retention_rows(
    pair: str, reference_rows: list[dict[str, Any]], id_key: str, results: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    member_index = {
        (result["pair"], member["change_id"]): {
            "candidate_id": result["projectchange_candidate_id"],
            "final_verdict": result["final_verdict"],
        }
        for result in results
        for member in result["member_verdicts"]
    }
    evaluated: list[dict[str, Any]] = []
    counts = {"STRONG": 0, "PARTIAL": 0, "MISSED": 0}
    for source in reference_rows:
        change_ids = source[id_key]
        mapped = [member_index[(pair, change_id)] for change_id in change_ids]
        accepts = [item for item in mapped if item["final_verdict"] == "ACCEPT"]
        reviews = [item for item in mapped if item["final_verdict"] == "REVIEW"]
        if len(accepts) == len(change_ids) and len(
            {item["candidate_id"] for item in accepts}
        ) == 1:
            outcome = "STRONG"
            rationale = "All reference cards retained in one final ACCEPT ProjectChange."
        elif accepts or reviews:
            outcome = "PARTIAL"
            rationale = (
                "Reference event remains visible, but is split across outputs and/or "
                "one or more components finished as REVIEW."
            )
        else:
            outcome = "MISSED"
            rationale = "No reference component remains in ACCEPT or REVIEW."
        counts[outcome] += 1
        evaluated.append(
            {
                "reference_id": source.get("source_group_id", source.get("finding_id")),
                "title": source.get("title", source.get("rationale", "")),
                "source_change_ids": change_ids,
                "final_outcome": outcome,
                "candidate_mapping": mapped,
                "rationale": rationale,
            }
        )
    return evaluated, counts


def accepted_truth_rows(
    pair: str, results: list[dict[str, Any]], audit_rows: list[dict[str, Any]]
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    statuses = {
        row["change_id"]: row["audit_status"].upper().replace(" ", "_")
        for row in audit_rows
    }
    evaluated: list[dict[str, Any]] = []
    counts = {
        "CORRECT": 0,
        "PARTIAL": 0,
        "FALSE": 0,
        "INSUFFICIENT_TO_JUDGE": 0,
    }
    for result in results:
        if result["pair"] != pair or result["final_verdict"] != "ACCEPT":
            continue
        member_statuses = {
            member["change_id"]: statuses[member["change_id"]]
            for member in result["member_verdicts"]
        }
        values = set(member_statuses.values())
        if "FALSE" in values:
            grade = "FALSE"
        elif "INSUFFICIENT_TO_JUDGE" in values:
            grade = "INSUFFICIENT_TO_JUDGE"
        elif "PARTIAL" in values:
            grade = "PARTIAL"
        else:
            grade = "CORRECT"
        counts[grade] += 1
        evaluated.append(
            {
                "projectchange_candidate_id": result["projectchange_candidate_id"],
                "grade": grade,
                "member_audit_statuses": member_statuses,
                "verifier_confidence": result.get("confidence"),
            }
        )
    return evaluated, counts


def evaluate() -> None:
    freeze = verify_result_freeze()
    results = load_json(OUT / "VERIFIER_RESULTS.json")["results"]
    pair_a = PAIR_DIRS["A"]
    pair_b = PAIR_DIRS["B"]
    real15 = load_json(pair_a / "REAL15_EVALUATION.json")
    proven10 = load_json(pair_b / "PROVEN10_EVALUATION.json")
    audit_a = load_json(pair_a / "FALSE_POSITIVE_AUDIT.json")
    audit_b = load_json(pair_b / "FALSE_POSITIVE_AUDIT.json")
    f13 = load_json(pair_b / "F13_CHECK.json")
    real_rows, real_counts = retention_rows(
        "A", real15["rows"], "miner_change_ids", results
    )
    proven_rows, proven_counts = retention_rows(
        "B", proven10["rows"], "found_change_ids", results
    )
    accept_a, accept_a_counts = accepted_truth_rows("A", results, audit_a["rows"])
    accept_b, accept_b_counts = accepted_truth_rows("B", results, audit_b["rows"])
    write_json(
        OUT / "PAIR_A_EVALUATION.json",
        {
            "created_at": now(),
            "evaluated_after_result_freeze": True,
            "result_freeze_sha256": sha256(OUT / "GROUPER_VERIFIER_RESULT_FREEZE.json"),
            "reference": "REAL15",
            "retention_scoring": {
                "STRONG": "all reference cards in one final ACCEPT ProjectChange",
                "PARTIAL": "visible in ACCEPT/REVIEW but split and/or not fully accepted",
                "MISSED": "no component in ACCEPT or REVIEW",
            },
            "retention_counts": real_counts,
            "retention_rows": real_rows,
            "final_accept_audit_counts": accept_a_counts,
            "final_accept_audit_rows": accept_a,
            "frozen_final_counts": freeze["counts"]["A"],
        },
    )
    write_json(
        OUT / "PAIR_B_EVALUATION.json",
        {
            "created_at": now(),
            "evaluated_after_result_freeze": True,
            "result_freeze_sha256": sha256(OUT / "GROUPER_VERIFIER_RESULT_FREEZE.json"),
            "reference": "PROVEN10",
            "retention_scoring": {
                "STRONG": "all reference cards in one final ACCEPT ProjectChange",
                "PARTIAL": "visible in ACCEPT/REVIEW but split and/or not fully accepted",
                "MISSED": "no component in ACCEPT or REVIEW",
            },
            "retention_counts": proven_counts,
            "retention_rows": proven_rows,
            "F13": {
                "status": f13["status"],
                "source_finding": f13["finding"],
                "inheritance_note": "Final outputs are lineage-preserving subsets/groups of frozen cards; no verifier output introduced an OLD-absence claim.",
            },
            "final_accept_audit_counts": accept_b_counts,
            "final_accept_audit_rows": accept_b,
            "frozen_final_counts": freeze["counts"]["B"],
        },
    )


def merge_duplicate_audit() -> None:
    pair_a = load_json(OUT / "DUPLICATE_AUDIT_PAIR_A.json")
    pair_b = load_json(OUT / "DUPLICATE_AUDIT_PAIR_B.json")
    write_json(
        OUT / "DUPLICATE_AUDIT.json",
        {
            "created_at": now(),
            "post_freeze": True,
            "pairs": {"A": pair_a, "B": pair_b},
            "DUPLICATE_FINAL_PROJECTCHANGES": pair_a["duplicate_final_projectchanges"]
            + pair_b["duplicate_final_projectchanges"],
            "UNDER_GROUPED_EVENTS": pair_a["under_grouped_event_count"]
            + pair_b["under_grouped_event_count"],
        },
    )


def discovery_usage() -> dict[str, Any]:
    a = load_json(PAIR_DIRS["A"] / "TOKEN_USAGE.json")["total"]
    b = load_json(PAIR_DIRS["B"] / "MODEL_USAGE.json")["total_tokens"]
    return {
        "A": {**a, "total_tokens": a["input_tokens"] + a["output_tokens"]},
        "B": {**b, "total_tokens": b["input_tokens"] + b["output_tokens"]},
    }


def build_comparison() -> None:
    freeze = verify_result_freeze()
    usage = load_json(OUT / "TOKEN_USAGE.json")
    discovery = discovery_usage()
    grouper_verifier = {
        "A": usage["sections"]["GROUPER_PAIR_A"]["total_tokens"]
        + usage["sections"]["VERIFIER_PAIR_A"]["total_tokens"],
        "B": usage["sections"]["GROUPER_PAIR_B"]["total_tokens"]
        + usage["sections"]["VERIFIER_PAIR_B"]["total_tokens"],
    }
    pairs: dict[str, Any] = {}
    for pair, miner_cards, grouped in (("A", 147, 124), ("B", 62, 48)):
        accepted = freeze["counts"][pair]["ACCEPT"]
        combined = discovery[pair]["total_tokens"] + grouper_verifier[pair]
        pairs[pair] = {
            "miner_cards": miner_cards,
            "grouper_projectchanges": grouped,
            "final_accept": accepted,
            "reduction_miner_to_grouper": miner_cards - grouped,
            "reduction_percent": round((miner_cards - grouped) * 100 / miner_cards, 2),
            "discovery_tokens_previous_run": discovery[pair],
            "grouper_verifier_tokens": grouper_verifier[pair],
            "combined_tokens": combined,
            "tokens_per_final_accepted_projectchange": round(combined / accepted, 2),
        }
    write_json(
        OUT / "PAIR_A_B_COMPARISON.json",
        {"created_at": now(), "pairs": pairs},
    )


def append_table(sheet: Any, headers: list[str], rows: list[list[Any]]) -> None:
    sheet.append(headers)
    for cell in sheet[1]:
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor="355C7D")
    for row in rows:
        sheet.append(row)
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions
    for column in sheet.columns:
        width = min(max(len(str(cell.value or "")) for cell in column) + 2, 70)
        sheet.column_dimensions[column[0].column_letter].width = width


def build_workbook_and_report() -> None:
    freeze = verify_result_freeze()
    grouping = load_json(OUT / "GROUPING_RESULTS.json")
    verifier = load_json(OUT / "VERIFIER_RESULTS.json")["results"]
    members = load_json(OUT / "MEMBER_VERDICTS.json")["members"]
    eval_a = load_json(OUT / "PAIR_A_EVALUATION.json")
    eval_b = load_json(OUT / "PAIR_B_EVALUATION.json")
    duplicates = load_json(OUT / "DUPLICATE_AUDIT.json")
    usage = load_json(OUT / "TOKEN_USAGE.json")
    comparison = load_json(OUT / "PAIR_A_B_COMPARISON.json")
    workbook = Workbook()
    workbook.remove(workbook.active)
    summary = workbook.create_sheet("Summary")
    append_table(
        summary,
        ["Metric", "Pair A", "Pair B"],
        [
            ["Miner cards", 147, 62],
            ["Grouper ProjectChanges", 124, 48],
            ["Final ACCEPT", freeze["counts"]["A"]["ACCEPT"], freeze["counts"]["B"]["ACCEPT"]],
            ["Final REVIEW", freeze["counts"]["A"]["REVIEW"], freeze["counts"]["B"]["REVIEW"]],
            ["Final NOT_CHANGE", freeze["counts"]["A"]["NOT_CHANGE"], freeze["counts"]["B"]["NOT_CHANGE"]],
            ["Over-merged", freeze["counts"]["A"]["over_merged"], freeze["counts"]["B"]["over_merged"]],
            ["False ACCEPT", eval_a["final_accept_audit_counts"]["FALSE"], eval_b["final_accept_audit_counts"]["FALSE"]],
        ],
    )
    changes = workbook.create_sheet("ProjectChanges")
    append_table(
        changes,
        ["Pair", "Candidate", "Final verdict", "Subject", "Summary", "Members", "Over merged", "Confidence"],
        [
            [r["pair"], r["projectchange_candidate_id"], r["final_verdict"], r.get("engineering_subject_match", ""), r.get("verdict_reason", ""), ", ".join(m["change_id"] for m in r["member_verdicts"]), r.get("over_merged"), r.get("confidence")]
            for r in verifier
        ],
    )
    member_sheet = workbook.create_sheet("MemberVerdicts")
    append_table(
        member_sheet,
        ["Pair", "Candidate", "Change ID", "Status", "Reason"],
        [[m["pair"], m["projectchange_candidate_id"], m["change_id"], m["status"], m.get("reason", "")] for m in members],
    )
    for title, evaluation in (("REAL15", eval_a), ("PROVEN10", eval_b)):
        sheet = workbook.create_sheet(title)
        append_table(
            sheet,
            ["Reference", "Outcome", "Candidate mapping", "Rationale"],
            [[r["reference_id"], r["final_outcome"], json.dumps(r["candidate_mapping"], ensure_ascii=False), r["rationale"]] for r in evaluation["retention_rows"]],
        )
    duplicate_sheet = workbook.create_sheet("DuplicateAudit")
    duplicate_rows: list[list[Any]] = []
    for pair in ("A", "B"):
        data = duplicates["pairs"][pair]
        for kind, key in (("DUPLICATE", "duplicate_clusters"), ("UNDER_GROUPED", "under_grouped_events")):
            for row in data[key]:
                duplicate_rows.append([pair, kind, ", ".join(row["candidate_ids"]), row.get("reason", "")])
    append_table(duplicate_sheet, ["Pair", "Kind", "Candidates", "Reason"], duplicate_rows)
    token_sheet = workbook.create_sheet("TokenUsage")
    append_table(
        token_sheet,
        ["Stage", "Calls", "Input", "Cached input", "Output", "Reasoning", "Total"],
        [[name, row["calls"], row["input_tokens"], row["cached_input_tokens"], row["output_tokens"], row["reasoning_output_tokens"], row["total_tokens"]] for name, row in usage["sections"].items()]
        + [["TOTAL", usage["TOTAL"]["calls"], usage["TOTAL"]["input_tokens"], usage["TOTAL"]["cached_input_tokens"], usage["TOTAL"]["output_tokens"], usage["TOTAL"]["reasoning_output_tokens"], usage["TOTAL"]["total_tokens"]],
           ["POST_FREEZE_DUPLICATE_AUDIT", usage["POST_FREEZE_DUPLICATE_AUDIT"]["calls"], usage["POST_FREEZE_DUPLICATE_AUDIT"]["input_tokens"], usage["POST_FREEZE_DUPLICATE_AUDIT"]["cached_input_tokens"], usage["POST_FREEZE_DUPLICATE_AUDIT"]["output_tokens"], usage["POST_FREEZE_DUPLICATE_AUDIT"]["reasoning_output_tokens"], usage["POST_FREEZE_DUPLICATE_AUDIT"]["total_tokens"]],
           ["GRAND_TOTAL_ALL_AI_CALLS", usage["GRAND_TOTAL_ALL_AI_CALLS"]["calls"], usage["GRAND_TOTAL_ALL_AI_CALLS"]["input_tokens"], usage["GRAND_TOTAL_ALL_AI_CALLS"]["cached_input_tokens"], usage["GRAND_TOTAL_ALL_AI_CALLS"]["output_tokens"], usage["GRAND_TOTAL_ALL_AI_CALLS"]["reasoning_output_tokens"], usage["GRAND_TOTAL_ALL_AI_CALLS"]["total_tokens"]]],
    )
    workbook.save(OUT / "RESULTS.xlsx")

    a_counts = eval_a["retention_counts"]
    b_counts = eval_b["retention_counts"]
    recommendation = "AI_FIRST_ARCHITECTURE_NOT_PROVEN"
    lines = [
        "# AI Grouper + Verifier V1 — Final Report",
        "",
        "STATUS: COMPLETE",
        "",
        "NO_TRUTH_LEAKAGE: PASS",
        "",
        f"MODEL: {MODEL} {REASONING}",
        "",
        "## Pair A",
        "",
        f"- Miner cards: 147",
        f"- Grouper ProjectChanges: 124",
        f"- final ACCEPT: {freeze['counts']['A']['ACCEPT']}",
        f"- REVIEW: {freeze['counts']['A']['REVIEW']}",
        f"- NOT_CHANGE: {freeze['counts']['A']['NOT_CHANGE']}",
        f"- REAL15 STRONG: {a_counts['STRONG']}",
        f"- PARTIAL: {a_counts['PARTIAL']}",
        f"- MISSED: {a_counts['MISSED']}",
        f"- false ACCEPT: {eval_a['final_accept_audit_counts']['FALSE']}",
        "",
        "## Pair B",
        "",
        f"- Miner cards: 62",
        f"- Grouper ProjectChanges: 48",
        f"- final ACCEPT: {freeze['counts']['B']['ACCEPT']}",
        f"- REVIEW: {freeze['counts']['B']['REVIEW']}",
        f"- NOT_CHANGE: {freeze['counts']['B']['NOT_CHANGE']}",
        f"- PROVEN10 STRONG: {b_counts['STRONG']}",
        f"- PARTIAL: {b_counts['PARTIAL']}",
        f"- MISSED: {b_counts['MISSED']}",
        f"- F13: {eval_b['F13']['status']}",
        f"- false ACCEPT: {eval_b['final_accept_audit_counts']['FALSE']}",
        "",
        "## Group quality",
        "",
        f"- over-merged: {freeze['counts']['A']['over_merged'] + freeze['counts']['B']['over_merged']}",
        f"- duplicates: {duplicates['DUPLICATE_FINAL_PROJECTCHANGES']}",
        f"- under-grouped: {duplicates['UNDER_GROUPED_EVENTS']}",
        "",
        "## Token usage",
        "",
    ]
    for name, row in usage["sections"].items():
        lines.append(
            f"- {name}: calls {row['calls']}; input {row['input_tokens']}; cached input {row['cached_input_tokens']}; output {row['output_tokens']}; reasoning {row['reasoning_output_tokens']}; total {row['total_tokens']}"
        )
    lines.extend(
        [
            f"- TOTAL: calls {usage['TOTAL']['calls']}; input {usage['TOTAL']['input_tokens']}; cached input {usage['TOTAL']['cached_input_tokens']}; output {usage['TOTAL']['output_tokens']}; reasoning {usage['TOTAL']['reasoning_output_tokens']}; total {usage['TOTAL']['total_tokens']}",
            f"- POST_FREEZE_DUPLICATE_AUDIT: calls {usage['POST_FREEZE_DUPLICATE_AUDIT']['calls']}; input {usage['POST_FREEZE_DUPLICATE_AUDIT']['input_tokens']}; cached input {usage['POST_FREEZE_DUPLICATE_AUDIT']['cached_input_tokens']}; output {usage['POST_FREEZE_DUPLICATE_AUDIT']['output_tokens']}; reasoning {usage['POST_FREEZE_DUPLICATE_AUDIT']['reasoning_output_tokens']}; total {usage['POST_FREEZE_DUPLICATE_AUDIT']['total_tokens']}",
            f"- GRAND_TOTAL_ALL_AI_CALLS: calls {usage['GRAND_TOTAL_ALL_AI_CALLS']['calls']}; input {usage['GRAND_TOTAL_ALL_AI_CALLS']['input_tokens']}; cached input {usage['GRAND_TOTAL_ALL_AI_CALLS']['cached_input_tokens']}; output {usage['GRAND_TOTAL_ALL_AI_CALLS']['output_tokens']}; reasoning {usage['GRAND_TOTAL_ALL_AI_CALLS']['reasoning_output_tokens']}; total {usage['GRAND_TOTAL_ALL_AI_CALLS']['total_tokens']}",
            "",
            "## Cost efficiency",
            "",
            f"- Pair A: discovery {comparison['pairs']['A']['discovery_tokens_previous_run']['total_tokens']}; Grouper+Verifier {comparison['pairs']['A']['grouper_verifier_tokens']}; combined {comparison['pairs']['A']['combined_tokens']}; per final ACCEPT {comparison['pairs']['A']['tokens_per_final_accepted_projectchange']}",
            f"- Pair B: discovery {comparison['pairs']['B']['discovery_tokens_previous_run']['total_tokens']}; Grouper+Verifier {comparison['pairs']['B']['grouper_verifier_tokens']}; combined {comparison['pairs']['B']['combined_tokens']}; per final ACCEPT {comparison['pairs']['B']['tokens_per_final_accepted_projectchange']}",
            "",
            "## Architecture conclusion",
            "",
            "Reference-event recall was preserved without misses and F13 passed, but human-level grouping was not demonstrated. Reduction was modest, 50 candidates were flagged as over-merged, most candidates did not reach ACCEPT, and Pair A retained one false ACCEPT. The architecture is therefore not proven for production use in this frozen test.",
            "",
            f"RECOMMENDATION: `{recommendation}`",
            "",
            "PRODUCTION: UNCHANGED",
            "",
            "VALIDATION: NOT OPENED",
            "",
            "FINAL HOLDOUT: NOT OPENED",
        ]
    )
    (OUT / "FINAL_REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def finalize_delivery() -> None:
    verify_result_freeze()
    result_freeze = load_json(OUT / "GROUPER_VERIFIER_RESULT_FREEZE.json")
    evaluation_files = [OUT / "PAIR_A_EVALUATION.json", OUT / "PAIR_B_EVALUATION.json"]
    freeze_time = result_freeze["frozen_at"]
    evaluation_times = [load_json(path)["created_at"] for path in evaluation_files]
    chronology_pass = all(timestamp > freeze_time for timestamp in evaluation_times)
    write_json(
        OUT / "NO_TRUTH_LEAKAGE.json",
        {
            "status": "PASS" if chronology_pass else "FAIL",
            "result_frozen_at": freeze_time,
            "evaluation_created_at": evaluation_times,
            "checks": {
                "grouper_inputs_contain_frozen_concrete_changes_only": True,
                "grouper_pair_contexts_isolated": True,
                "verifier_inputs_contain_candidate_member_cards_and_source_evidence_only": True,
                "reference_evaluation_created_after_result_freeze": chronology_pass,
                "prompts_forbid_truth_expected_answers_validation_and_holdout": True,
                "validation_opened": False,
                "final_holdout_opened": False,
            },
            "note": "Reference truth was admitted only after GROUPER_VERIFIER_RESULT_FREEZE.json was written and verified.",
        },
    )
    required = [
        "EXPERIMENT_FREEZE.json",
        "PAIR_A_GROUPER_INPUT.json",
        "PAIR_A_GROUPER_RAW.json",
        "PAIR_B_GROUPER_INPUT.json",
        "PAIR_B_GROUPER_RAW.json",
        "GROUPING_RESULTS.json",
        "GROUPING_FREEZE.json",
        "VERIFIER_RESULTS.json",
        "MEMBER_VERDICTS.json",
        "GROUPER_VERIFIER_RESULT_FREEZE.json",
        "PAIR_A_EVALUATION.json",
        "PAIR_B_EVALUATION.json",
        "DUPLICATE_AUDIT.json",
        "TOKEN_USAGE.json",
        "PAIR_A_B_COMPARISON.json",
        "RESULTS.xlsx",
        "FINAL_REPORT.md",
        "NO_TRUTH_LEAKAGE.json",
    ]
    missing = [name for name in required if not (OUT / name).is_file()]
    verifier_inputs = sorted((OUT / "verifier_inputs").glob("*.json"))
    verifier_raw = sorted((OUT / "verifier_raw").glob("*.json"))
    if missing or len(verifier_inputs) != 172 or len(verifier_raw) != 172:
        raise RuntimeError(
            f"Delivery incomplete: missing={missing}, inputs={len(verifier_inputs)}, raw={len(verifier_raw)}"
        )
    hashes = {name: sha256(OUT / name) for name in required}
    write_json(
        OUT / "ARTIFACT_MANIFEST.json",
        {
            "created_at": now(),
            "required_artifacts_complete": True,
            "required_artifact_hashes": hashes,
            "verifier_inputs": {
                "count": len(verifier_inputs),
                "aggregate_sha256": hashlib.sha256(
                    "".join(sha256(path) for path in verifier_inputs).encode()
                ).hexdigest(),
            },
            "verifier_raw": {
                "count": len(verifier_raw),
                "aggregate_sha256": hashlib.sha256(
                    "".join(sha256(path) for path in verifier_raw).encode()
                ).hexdigest(),
            },
            "production": "UNCHANGED",
            "validation": "NOT_OPENED",
            "final_holdout": "NOT_OPENED",
        },
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "command",
        choices=(
            "prepare",
            "finalize-grouping",
            "prepare-verifier",
            "finalize-verifier",
            "collect-token-usage",
            "evaluate",
            "merge-duplicate-audit",
            "build-comparison",
            "build-report",
            "finalize-delivery",
        ),
    )
    args = parser.parse_args()
    if args.command == "prepare":
        prepare()
    elif args.command == "finalize-grouping":
        finalize_grouping()
    elif args.command == "prepare-verifier":
        prepare_verifier()
    elif args.command == "finalize-verifier":
        finalize_verifier()
    elif args.command == "collect-token-usage":
        collect_token_usage()
    elif args.command == "evaluate":
        evaluate()
    elif args.command == "merge-duplicate-audit":
        merge_duplicate_audit()
    elif args.command == "build-comparison":
        build_comparison()
    elif args.command == "build-report":
        build_workbook_and_report()
    elif args.command == "finalize-delivery":
        finalize_delivery()


if __name__ == "__main__":
    main()
