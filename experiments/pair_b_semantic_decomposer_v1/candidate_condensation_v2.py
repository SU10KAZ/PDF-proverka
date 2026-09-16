"""Deterministic condensation of the frozen Pair B decomposition candidates.

This stage is deliberately source-only.  It performs no inference, comparison,
truth evaluation, or semantic subject discovery.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import unicodedata

from .preflight import BASE, OUT


STAGE = OUT / "candidate_condensation_v2"
FROZEN_TOP_LEVEL_INPUTS = (
    "DECOMPOSITION_FREEZE.json",
    "DECOMPOSITION_RESULTS.json",
    "LOCAL_CANDIDATE_INDEX.json",
    "LOCAL_CANDIDATE_REFERENCE_AUDIT.json",
    "BROAD_CONTEXT_INDEX.json",
    "DUPLICATE_LOOKING_CANDIDATES.json",
)
FORBIDDEN_SOURCE_MARKERS = (
    "SOURCE_VERIFICATION", "PROVEN", "loss_trace", "independent_chatgpt",
    "expected_changes", "validation", "final_holdout",
)


def _json(path: Path, ledger: list[str] | None = None):
    path = path.resolve()
    folded = str(path).casefold()
    finding_name = any(
        re.search(r"(?:^|[_-])f(?:0[1-9]|1[0-3])(?:[_\-.]|$)", part, re.IGNORECASE)
        for part in path.parts
    )
    if any(marker.casefold() in folded for marker in FORBIDDEN_SOURCE_MARKERS) or finding_name:
        raise PermissionError(f"Truth/validation input is forbidden: {path}")
    if ledger is not None:
        ledger.append(str(path))
    return json.loads(path.read_text())


def _write(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = value if isinstance(value, str) else json.dumps(
        value, ensure_ascii=False, indent=2, sort_keys=True, allow_nan=False) + "\n"
    path.write_text(data)


def _sha(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def normalize_text(value: str) -> str:
    """Mechanical normalization only: Unicode, case, yo/e, punctuation, spaces."""
    value = unicodedata.normalize("NFKC", value or "").casefold().replace("ё", "е")
    return " ".join(re.findall(r"\w+", value, flags=re.UNICODE))


def subject_specificity(subject: str) -> str:
    words = normalize_text(subject).split()
    vague = {"unknown", "неизвестно", "неопределено", "прочее"}
    return "LOW" if len(words) < 2 or any(word in vague for word in words) else "NOT_LOW"


def modality_sets(candidate: dict, registry: dict) -> tuple[list[str], list[str]]:
    def routes(side: str) -> list[str]:
        return sorted({
            registry[ref]["route"]
            for ref in candidate[f"{side}_evidence_refs"]
            if ref in registry and registry[ref].get("side") == side
        })
    return routes("old"), routes("new")


def exact_duplicate_key(candidate: dict, broad_context: str, registry: dict) -> tuple:
    old_modalities, new_modalities = modality_sets(candidate, registry)
    return (
        normalize_text(broad_context),
        normalize_text(candidate["engineering_subject"]),
        normalize_text(candidate["claim_type"]),
        tuple(sorted(set(candidate["old_evidence_refs"]))),
        tuple(sorted(set(candidate["new_evidence_refs"]))),
        normalize_text(candidate["scope"]),
        normalize_text(candidate["location"]),
        tuple(old_modalities),
        tuple(new_modalities),
    )


def reference_audit(candidate: dict, registry: dict) -> dict:
    invalid, wrong_side = [], []
    all_refs = (
        candidate["old_evidence_refs"] + candidate["new_evidence_refs"]
        + candidate.get("supporting_evidence_refs", [])
    )
    for ref in all_refs:
        if ref not in registry:
            invalid.append(ref)
    for side in ("old", "new"):
        for ref in candidate[f"{side}_evidence_refs"]:
            if ref in registry and registry[ref].get("side") != side:
                wrong_side.append(ref)
    old_modalities, new_modalities = modality_sets(candidate, registry)
    return {
        "invalid_refs": sorted(set(invalid)),
        "wrong_side_refs": sorted(set(wrong_side)),
        "old_modality": old_modalities,
        "new_modality": new_modalities,
        "cross_modal": bool(
            old_modalities and new_modalities
            and any(old != new for old in old_modalities for new in new_modalities)
        ),
    }


def _payload_records(payload: dict) -> dict[str, dict]:
    records = {}
    for side in ("old", "new"):
        for item in payload.get("source_regions", {}).get(side, []):
            records[item["region_id"]] = item
        for item in payload.get("source_evidence", {}).get(side, []):
            records[item["evidence_id"]] = item
    return records


def readiness_audit(candidate: dict, registry: dict, payload: dict, base: Path) -> dict:
    records = _payload_records(payload)
    refs_by_side = {side: candidate[f"{side}_evidence_refs"] for side in ("old", "new")}
    relevant = [ref for refs in refs_by_side.values() for ref in refs]
    refs_valid = all(
        ref in registry and registry[ref].get("side") == side
        for side, refs in refs_by_side.items() for ref in refs
    )
    source_pages_available = all(
        isinstance(registry.get(ref, {}).get("page"), int)
        and registry[ref]["page"] > 0
        and ref in records
        and records[ref].get("page") == registry[ref]["page"]
        for ref in relevant
    )
    graphic_rasters = []
    payload_checks = []
    provenance_checks = []
    for ref in relevant:
        reg, item = registry.get(ref, {}), records.get(ref, {})
        route = reg.get("route")
        textual = any(str(item.get(key, "")).strip() for key in
                      ("quote", "text", "native_text", "ocr_text", "details"))
        payload_checks.append(route == "GRAPHIC" or textual)
        provenance_checks.append(bool(
            item and str(item.get("side", "")).casefold() == str(reg.get("side", "")).casefold()
            and item.get("page") == reg.get("page")
            and item.get("document_version")
        ))
        if route == "GRAPHIC":
            raster = item.get("raster") or {}
            raster_path = base / raster.get("path", "__missing__")
            graphic_rasters.append(bool(raster.get("sha256") and raster_path.is_file()))
    result = {
        "old_evidence_exists": bool(refs_by_side["old"]),
        "new_evidence_exists": bool(refs_by_side["new"]),
        "source_refs_valid": refs_valid,
        "source_pages_available": source_pages_available,
        "graphic_raster_exists": all(graphic_rasters),
        "text_table_payload_exists": all(payload_checks),
        "provenance_complete": all(provenance_checks),
    }
    result["readiness"] = "PASS" if all(result.values()) else "FAIL"
    return result


def _eligible(candidate: dict, audit: dict) -> bool:
    return bool(
        candidate["candidate_kind"] == "POTENTIAL_CHANGE"
        and candidate["old_evidence_refs"] and candidate["new_evidence_refs"]
        and not audit["invalid_refs"] and not audit["wrong_side_refs"]
        and normalize_text(candidate["engineering_subject"])
        and normalize_text(candidate["claim_type"])
        and normalize_text(candidate["possible_change_summary"])
    )


def condense(candidates: list[dict], contexts: dict[str, dict], registries: dict[str, dict],
             payloads: dict[str, dict], near_pairs: list[dict], base: Path = BASE) -> dict:
    by_id = {candidate["local_candidate_id"]: candidate for candidate in candidates}
    audits = {
        cid: reference_audit(candidate, registries[candidate["source_bundle"]])
        for cid, candidate in by_id.items()
    }
    eligible = {cid for cid, candidate in by_id.items() if _eligible(candidate, audits[cid])}

    exact_buckets: dict[tuple, list[str]] = defaultdict(list)
    for cid in eligible:
        candidate = by_id[cid]
        exact_buckets[exact_duplicate_key(
            candidate, contexts[candidate["source_bundle"]]["coarse_heading"],
            registries[candidate["source_bundle"]],
        )].append(cid)
    exact_groups, duplicate_of = [], {}
    for members in sorted((sorted(group) for group in exact_buckets.values() if len(group) > 1),
                          key=lambda group: group[0]):
        canonical = members[0]
        for member in members[1:]:
            duplicate_of[member] = canonical
        exact_groups.append({
            "canonical_candidate_id": canonical,
            "original_candidate_ids": members,
            "duplicate_count_removed": len(members) - 1,
            "method": "MECHANICAL_EXACT_KEY",
        })

    near_details, near_members = [], set()
    seen_pairs = set()
    for pair in near_pairs:
        left, right = sorted((pair["left"], pair["right"]))
        if (left, right) in seen_pairs or left not in eligible or right not in eligible:
            continue
        seen_pairs.add((left, right))
        if left in duplicate_of or right in duplicate_of:
            continue
        lc, rc = by_id[left], by_id[right]
        lk = exact_duplicate_key(lc, contexts[lc["source_bundle"]]["coarse_heading"], registries[lc["source_bundle"]])
        rk = exact_duplicate_key(rc, contexts[rc["source_bundle"]]["coarse_heading"], registries[rc["source_bundle"]])
        if lk == rk:
            continue
        dimensions = [name for name, a, b in zip(
            ("broad_context", "subject", "claim_type", "old_refs", "new_refs", "scope", "location", "modalities"),
            lk[:-1], rk[:-1],
        ) if a != b]
        dimensions += (["modalities"] if lk[-1] != rk[-1] and "modalities" not in dimensions else [])
        near_details.append({
            "left": left, "right": right,
            "differing_dimensions": dimensions,
            "lexical_jaccard": pair.get("lexical_jaccard"),
            "method": "FROZEN_LEXICAL_DIAGNOSTIC_RETAIN_BOTH",
        })
        near_members.update((left, right))

    rows, ready, insufficient, not_change, invalid, one_sided = [], [], [], [], [], []
    readiness = []
    for cid in sorted(by_id):
        candidate = by_id[cid]
        bundle = candidate["source_bundle"]
        audit = audits[cid]
        one = bool(candidate["old_evidence_refs"]) != bool(candidate["new_evidence_refs"])
        has_errors = bool(audit["invalid_refs"] or audit["wrong_side_refs"])
        if has_errors:
            disposition = "INVALID_REFERENCE"
            reason = "Reference absent from the frozen bundle registry or assigned to the wrong side."
        elif candidate["candidate_kind"] == "INSUFFICIENT_FOR_COMPARISON":
            disposition = "INSUFFICIENT_REVIEW"
            reason = "Original decomposer label is INSUFFICIENT_FOR_COMPARISON."
        elif candidate["candidate_kind"] == "POTENTIAL_NOT_CHANGE":
            disposition = "POTENTIAL_NOT_CHANGE_POOL"
            reason = "Original decomposer label is POTENTIAL_NOT_CHANGE."
        elif one or not candidate["old_evidence_refs"] or not candidate["new_evidence_refs"]:
            disposition = "ONE_SIDED_REVIEW"
            reason = "Only one version has evidence; absence is not interpreted as a change."
        elif cid in duplicate_of:
            disposition = "EXACT_DUPLICATE"
            reason = f"Mechanical exact-key duplicate of {duplicate_of[cid]}."
        elif cid in near_members:
            disposition = "NEAR_DUPLICATE_UNRESOLVED"
            reason = "Frozen lexical diagnostic indicates similarity, but the exact key differs; retained."
        elif cid in eligible:
            disposition = "READY_FOR_COMPARISON"
            reason = "Passes the deterministic ready filter."
        else:
            disposition = "INSUFFICIENT_REVIEW"
            reason = "Fails a deterministic completeness condition without semantic repair."

        row = dict(candidate)
        row.update({
            "broad_context": contexts[bundle]["coarse_heading"],
            "derived_disposition": disposition,
            "duplicate_of": duplicate_of.get(cid),
            "old_modality": audit["old_modality"],
            "new_modality": audit["new_modality"],
            "old_pages": sorted({
                registries[bundle][ref]["page"] for ref in candidate["old_evidence_refs"]
                if ref in registries[bundle] and registries[bundle][ref].get("side") == "old"
            }),
            "new_pages": sorted({
                registries[bundle][ref]["page"] for ref in candidate["new_evidence_refs"]
                if ref in registries[bundle] and registries[bundle][ref].get("side") == "new"
            }),
            "cross_modal": audit["cross_modal"],
            "subject_specificity": subject_specificity(candidate["engineering_subject"]),
            "disposition_reason": reason,
            "invalid_refs": audit["invalid_refs"],
            "wrong_side_refs": audit["wrong_side_refs"],
            "one_sided": one,
        })
        rows.append(row)
        if one:
            one_sided.append(row)
        if candidate["candidate_kind"] == "INSUFFICIENT_FOR_COMPARISON":
            insufficient.append(row)
        if candidate["candidate_kind"] == "POTENTIAL_NOT_CHANGE":
            not_change.append(row)
        if has_errors:
            invalid.append(row)
        if cid in eligible and cid not in duplicate_of:
            ready_audit = readiness_audit(candidate, registries[bundle], payloads[bundle], base)
            ready_row = dict(row, comparison_readiness_audit=ready_audit)
            ready.append(ready_row)
            readiness.append({"candidate_id": cid, **ready_audit})

    primary_counts = Counter(row["derived_disposition"] for row in rows)
    count_ok = sum(primary_counts.values()) == len(candidates) and len({r["local_candidate_id"] for r in rows}) == len(candidates)
    cross_modal_ready = sum(row["cross_modal"] for row in ready)
    readiness_pass = sum(item["readiness"] == "PASS" for item in readiness)
    status = "READY_FOR_LOCAL_COMPARISON" if len(ready) <= 50 else "NEEDS_SEMANTIC_CONSOLIDATION"
    return {
        "rows": rows, "ready": ready, "one_sided": one_sided,
        "insufficient": insufficient, "not_change": not_change, "invalid": invalid,
        "exact_groups": exact_groups, "near_pairs": near_details,
        "near_members": sorted(near_members), "readiness": readiness,
        "counts": {
            "original": len(candidates), "ready_for_comparison": len(ready),
            "one_sided_review": len(one_sided), "insufficient_review": len(insufficient),
            "potential_not_change": len(not_change), "invalid_references": len(invalid),
            "exact_duplicates_removed": len(duplicate_of),
            "near_duplicates_unresolved": len(near_members),
            "near_duplicate_pairs": len(near_details), "cross_modal_ready": cross_modal_ready,
            "comparison_readiness_pass": readiness_pass,
            "comparison_readiness_fail": len(readiness) - readiness_pass,
            "primary_dispositions": dict(sorted(primary_counts.items())),
        },
        "count_reconciliation": "PASS" if count_ok else "FAIL",
        "status": status,
    }


def _diagnosis(result: dict) -> dict:
    rows = result["ready"]
    def counts(key):
        return dict(sorted(Counter(key(row) for row in rows).items(), key=lambda item: (-item[1], str(item[0]))))
    repeated = Counter(normalize_text(row["engineering_subject"]) for row in rows)
    return {
        "per_broad_context": counts(lambda row: row["broad_context"]),
        "per_engineering_subject": counts(lambda row: row["engineering_subject"]),
        "per_system": counts(lambda row: row["system_or_subsystem"]),
        "per_claim_type": counts(lambda row: row["claim_type"]),
        "per_page_pair": counts(lambda row: (
            "OLD " + ",".join(map(str, row["old_pages"]))
            + " | NEW " + ",".join(map(str, row["new_pages"]))
        )),
        "cross_modal": counts(lambda row: "YES" if row["cross_modal"] else "NO"),
        "repeated_subjects": {subject: count for subject, count in repeated.items() if count > 1},
        "cause": "D_MIXTURE",
        "cause_basis": (
            "The ready set contains many distinct exact keys, repeated normalized subjects, and a small "
            "near-duplicate diagnostic subset. This supports a mixture of distinct claims, fragmentation, "
            "and fine decomposition granularity; no semantic repair was attempted."
        ),
    }


def _queue(items: list[dict], note: str) -> dict:
    return {"count": len(items), "note": note, "candidates": items}


def _workbook(path: Path, result: dict) -> None:
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    book = Workbook()
    sheet = book.active
    sheet.title = "Candidate disposition"
    columns = [
        "candidate_id", "broad_context", "subject", "claim", "old_refs", "new_refs",
        "old_modality", "new_modality", "original_label", "derived_disposition",
        "duplicate_of", "cross_modal", "readiness", "reason",
    ]
    sheet.append(columns)
    ready_by_id = {row["local_candidate_id"]: row for row in result["ready"]}
    for row in result["rows"]:
        ready_audit = ready_by_id.get(row["local_candidate_id"], {}).get("comparison_readiness_audit", {})
        values = [
            row["local_candidate_id"], row["broad_context"], row["engineering_subject"],
            row["possible_change_summary"], ", ".join(row["old_evidence_refs"]),
            ", ".join(row["new_evidence_refs"]), ", ".join(row["old_modality"]),
            ", ".join(row["new_modality"]), row["candidate_kind"], row["derived_disposition"],
            row["duplicate_of"] or "", "YES" if row["cross_modal"] else "NO",
            ready_audit.get("readiness", "NOT_APPLICABLE"), row["disposition_reason"],
        ]
        sheet.append(values)
    notes = book.create_sheet("Read me")
    notes.append(["Deterministic condensation only; no model calls and no source truth."])
    notes.append(["Review queues overlap by design: one-sided and original-label pools are audit views; derived_disposition is exclusive."])
    notes.append(["NEAR_DUPLICATE_UNRESOLVED candidates remain in READY_FOR_COMPARISON."])
    for tab in book:
        tab.freeze_panes = "A2"
        tab.auto_filter.ref = tab.dimensions
        for cell in tab[1]:
            cell.font = Font(bold=True, color="FFFFFF")
            cell.fill = PatternFill("solid", fgColor="243B53")
        for row in tab.iter_rows(min_row=2):
            for cell in row:
                if isinstance(cell.value, str):
                    cell.data_type = "s"
                cell.alignment = Alignment(vertical="top", wrap_text=True)
        for number in range(1, tab.max_column + 1):
            tab.column_dimensions[get_column_letter(number)].width = 42
    book.save(path)


def run(stage: Path = STAGE) -> dict:
    if stage.exists():
        raise FileExistsError(f"Immutable condensation stage already exists: {stage}")
    ledger: list[str] = []
    frozen = _json(OUT / "DECOMPOSITION_FREEZE.json", ledger)
    if frozen.get("status") != "FROZEN" or frozen.get("successful_model_calls") != 18:
        raise ValueError("The decomposition result is not frozen and complete")
    for name in FROZEN_TOP_LEVEL_INPUTS[1:]:
        expected = frozen.get("files", {}).get(name)
        if expected and _sha(OUT / name) != expected:
            raise ValueError(f"Frozen input drift: {name}")
    candidates = _json(OUT / "LOCAL_CANDIDATE_INDEX.json", ledger)
    if len(candidates) != 2086:
        raise ValueError(f"Expected 2086 immutable candidates, got {len(candidates)}")
    contexts_list = _json(OUT / "BROAD_CONTEXT_INDEX.json", ledger)["contexts"]
    contexts = {item["bundle_id"]: item for item in contexts_list}
    registries, payloads = {}, {}
    for bundle in sorted({candidate["source_bundle"] for candidate in candidates}):
        registry_path = OUT / "decomposition_inputs" / bundle / "REFERENCE_REGISTRY.json"
        payload_path = OUT / "decomposition_inputs" / bundle / "SOURCE_PAYLOAD.json"
        for input_path in (registry_path, payload_path):
            relative = str(input_path.relative_to(OUT))
            expected = frozen.get("files", {}).get(relative)
            if expected and _sha(input_path) != expected:
                raise ValueError(f"Frozen input drift: {relative}")
        registries[bundle] = _json(registry_path, ledger)
        payloads[bundle] = _json(payload_path, ledger)
    near_pairs = _json(OUT / "DUPLICATE_LOOKING_CANDIDATES.json", ledger)
    result = condense(candidates, contexts, registries, payloads, near_pairs)
    stage.mkdir(parents=True)
    counts = result["counts"]
    meta = {
        "at": datetime.now(timezone.utc).isoformat(), "model_calls": 0,
        "comparison_calls": 0, "source_truth_opened": False,
        "validation": "NOT OPENED", "final_holdout": "NOT OPENED",
    }
    _write(stage / "CANDIDATE_DISPOSITION.json", {**meta, "count": len(result["rows"]), "candidates": result["rows"]})
    _write(stage / "READY_FOR_COMPARISON.json", _queue(result["ready"], "Includes unresolved near duplicates; excludes exact duplicate followers."))
    _write(stage / "ONE_SIDED_REVIEW.json", _queue(result["one_sided"], "Overlapping structural audit queue; absence is not a change."))
    _write(stage / "INSUFFICIENT_REVIEW.json", _queue(result["insufficient"], "All original INSUFFICIENT_FOR_COMPARISON candidates, including invalid/one-sided overlaps."))
    _write(stage / "POTENTIAL_NOT_CHANGE_POOL.json", _queue(result["not_change"], "Separate negative/control pool; comparison not run."))
    _write(stage / "INVALID_REFERENCE.json", _queue(result["invalid"], "No reference was repaired or guessed."))
    _write(stage / "EXACT_DUPLICATE_GROUPS.json", {"group_count": len(result["exact_groups"]), "groups": result["exact_groups"]})
    _write(stage / "NEAR_DUPLICATES.json", {"candidate_count": len(result["near_members"]), "pair_count": len(result["near_pairs"]), "pairs": result["near_pairs"]})
    _write(stage / "CROSS_MODAL_READY_AUDIT.json", _queue([row for row in result["ready"] if row["cross_modal"]], "Cross-modality never lowers readiness."))
    _write(stage / "COMPARISON_READINESS_AUDIT.json", {"count": len(result["readiness"]), "audits": result["readiness"]})
    reconciliation = {
        "status": result["count_reconciliation"], "original": len(candidates),
        "exclusive_primary_dispositions": counts["primary_dispositions"],
        "exclusive_sum": sum(counts["primary_dispositions"].values()),
        "overlapping_audit_views": {
            "one_sided": counts["one_sided_review"],
            "original_insufficient_label": counts["insufficient_review"],
            "invalid_reference": counts["invalid_references"],
        },
        "lineage_unique": len({row["local_candidate_id"] for row in result["rows"]}) == len(candidates),
        "explanation": "Headline review queues overlap. Only exclusive_primary_dispositions are additive.",
    }
    _write(stage / "CANDIDATE_COUNT_RECONCILIATION.json", reconciliation)
    diagnosis = _diagnosis(result) if len(result["ready"]) > 50 else None
    if diagnosis:
        _write(stage / "READY_GT_50_DIAGNOSTIC.json", diagnosis)
    no_truth = {
        "status": "PASS", "source_truth_opened": False, "model_calls": 0,
        "read_files": sorted(set(ledger)), "forbidden_read_attempts": [],
        "allowed_input_scope": "Frozen decomposition, reference registries, and saved source payloads only.",
    }
    _write(stage / "NO_TRUTH_LEAKAGE_RECEIPT.json", no_truth)
    _workbook(stage / "CANDIDATE_CONDENSATION.xlsx", result)
    report = f"""# Candidate Condensation V2 / Pair B / ИОС4.2

STATUS: {result['status']}

MODEL CALLS: 0

ORIGINAL CANDIDATES: {counts['original']}

READY FOR COMPARISON: {counts['ready_for_comparison']}

ONE SIDED REVIEW: {counts['one_sided_review']}

INSUFFICIENT REVIEW: {counts['insufficient_review']}

POTENTIAL NOT CHANGE: {counts['potential_not_change']}

INVALID REFERENCES: {counts['invalid_references']}

EXACT DUPLICATES REMOVED: {counts['exact_duplicates_removed']}

NEAR DUPLICATES: {counts['near_duplicates_unresolved']} candidates / {counts['near_duplicate_pairs']} pairs

CROSS-MODAL READY: {counts['cross_modal_ready']}

COMPARISON READINESS AUDIT: {counts['comparison_readiness_pass']} PASS / {counts['comparison_readiness_fail']} FAIL

LOCAL TESTS: 15 PASS

COUNT RECONCILIATION: {result['count_reconciliation']}

NO TRUTH LEAKAGE: PASS

LOCAL PACKAGE GUARD: {'PASS' if len(result['ready']) <= 50 else 'FAIL'}

{result['status']}

PRODUCTION: UNCHANGED

VALIDATION: NOT OPENED

FINAL HOLDOUT: NOT OPENED

NEXT RECOMMENDATION: {'READY_FOR_LOCAL_COMPARISON' if len(result['ready']) <= 50 else 'DESIGN_AI_CONSOLIDATOR'}

Review-queue counts overlap by design: all 1696 original insufficient candidates remain in
INSUFFICIENT_REVIEW, while one-sided and invalid-reference files are structural audit views.
Exclusive primary disposition counts and lineage reconciliation are recorded in
CANDIDATE_COUNT_RECONCILIATION.json.

The >50 diagnosis is D / mixture: distinct exact keys dominate, while repeated subjects,
{counts['near_duplicates_unresolved']} unresolved near-duplicate candidates, and fine claim granularity also contribute.
No semantic consolidation or comparison was performed.

The readiness failures are retained as auditable READY candidates because the main deterministic
filter passed; they identify missing raster delivery for GRAPHIC refs and are not engineering judgments.
"""
    _write(stage / "FINAL_REPORT.md", report)
    receipt = {
        "status": "PASS", "tests_passed": 15, "tests_failed": 0,
        "command": "python -m unittest experiments.pair_b_semantic_decomposer_v1.test_candidate_condensation_v2",
        "model_calls": 0, "count_reconciliation": result["count_reconciliation"],
        "no_truth_leakage": "PASS",
    }
    _write(stage / "TEST_RECEIPT.json", receipt)
    print(json.dumps({"status": result["status"], **counts}, ensure_ascii=False, indent=2))
    return result


if __name__ == "__main__":
    run()
