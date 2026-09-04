"""Function Lineage v2.7 — deterministic function instance / series identity.

Phases 1 and 3 of the instance-identity research track.  No model calls.

The confirmed defect is that several engineering instances of one function are
indistinguishable: ``function_class``, ``role`` and ``document_role`` are equal
and the Function Passport carries nothing that names the concrete instance.

This module extracts, from documented text only, the facts an engineer would
use to tell one instance from another, and then measures how far those facts
actually go on the three corpora.

Hard rules:

* a physical page number is provenance, never identity — two fragments are
  never related because their page numbers are close or equal;
* a missing fact is UNKNOWN and never a mismatch;
* every extracted fact carries the passport field it came from;
* nothing is project-, page- or file-specific.
"""
from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from experiments.function_lineage_v2 import smoke as base_smoke
from experiments.function_lineage_v2 import stratified


SCHEMA_VERSION = "function-instance-identity.v2.7"
DEFAULT_OUTPUT = (
    stratified.COMPARISON_ROOT / "20260904_function_lineage_v2_7_instance_identity"
)

#: The ONLY field a *primary* instance mark may come from.  The sheet caption
#: is the document naming the instance.  Free-text entity lists were tried and
#: rejected: they are full of cable and breaker designations (ВН-32, ВМ63,
#: LS3) that look like marks but name equipment inside the sheet, not the
#: sheet's own instance.
PRIMARY_MARK_FIELD = "source_sheet.title"

#: Fields read only to corroborate or contradict the primary mark.  They never
#: create identity on their own.
CORROBORATING_MARK_FIELDS = ("zone", "serviced_object")

MARK_SOURCE_FIELDS = (PRIMARY_MARK_FIELD, *CORROBORATING_MARK_FIELDS)

#: Fields already present in the passport that scope an instance.
SCOPE_FIELDS = (
    "zone", "section", "corpus", "building", "floors", "serviced_object",
)

#: Cyrillic letters that OCR confuses with digits inside a designation.
_DIGIT_HOMOGLYPHS = str.maketrans({"З": "3", "О": "0", "о": "0", "з": "3"})

#: Cyrillic letters that share a glyph with a Latin one.
_LETTER_HOMOGLYPHS = str.maketrans({
    "А": "A", "В": "B", "С": "C", "Е": "E", "Н": "H", "К": "K",
    "М": "M", "О": "O", "Р": "P", "Т": "T", "Х": "X", "У": "Y",
})

#: An engineering designation: a short uppercase alphabetic prefix followed by
#: a number, optionally separated and optionally with a dotted sub-number.
#: Deliberately generic — it matches ЯК1, ВРУ-4, ЩАО-5, ГРЩ1, ЩК-3, ТП-1.2.
_MARK = re.compile(
    r"(?<![A-ZА-ЯЁ0-9])"
    r"(?P<prefix>[A-ZА-ЯЁ]{1,6})"
    r"(?P<sep>[-–—.\s]?)"
    r"(?P<number>[0-9ЗО]{1,3}(?:[./][0-9ЗО]{1,3})?)"
    r"(?![A-ZА-ЯЁ])"
)

#: A building level mark: +7.950, -2.180, ±0.000, отм. 0.000.
_LEVEL = re.compile(r"(?P<sign>[+\-±])\s?(?P<value>\d{1,3}[.,]\d{3})")

#: Prefixes that are ordinary words rather than designations.  Kept tiny and
#: generic: they are the words that show up as "<WORD><number>" in captions.
_NON_DESIGNATION_PREFIXES = frozenset({"ЧАСТЬ", "ЛИСТ", "СТР", "РИС", "ТАБЛ", "П"})


def _text_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Mapping):
        return [str(item) for item in value.values() if isinstance(item, str)]
    if isinstance(value, Sequence):
        return [str(item) for item in value if isinstance(item, str)]
    return []


def _field_value(passport: Mapping[str, Any], field: str) -> Any:
    if field.startswith("source_sheet."):
        return (passport.get("source_sheet") or {}).get(field.split(".", 1)[1])
    return passport.get(field)


def normalize_mark(prefix: str, number: str) -> str:
    """Canonical designation: Latin-folded prefix, OCR-corrected number."""
    clean_prefix = prefix.upper().translate(_LETTER_HOMOGLYPHS)
    clean_number = number.translate(_DIGIT_HOMOGLYPHS).replace(",", ".")
    return f"{clean_prefix}{clean_number}"


def extract_marks(text: str) -> list[dict[str, str]]:
    """Every engineering designation a piece of documented text contains."""
    found: list[dict[str, str]] = []
    seen: set[str] = set()
    for match in _MARK.finditer(text or ""):
        prefix = match.group("prefix").upper()
        if prefix in _NON_DESIGNATION_PREFIXES or len(prefix) < 2:
            continue
        number = match.group("number")
        mark = normalize_mark(prefix, number)
        if mark in seen:
            continue
        seen.add(mark)
        found.append({
            "mark": mark,
            "series_id": prefix.translate(_LETTER_HOMOGLYPHS),
            "series_ordinal": number.translate(_DIGIT_HOMOGLYPHS).replace(",", "."),
            "literal": match.group(0).strip(),
        })
    return found


def extract_levels(text: str) -> list[str]:
    """Building level marks such as ``+7.950``."""
    values: list[str] = []
    for match in _LEVEL.finditer(text or ""):
        value = match.group("value").replace(",", ".")
        token = f"{match.group('sign')}{value}"
        if token not in values:
            values.append(token)
    return values


def function_instance_identity(passport: Mapping[str, Any]) -> dict[str, Any]:
    """Deterministic instance identity of one function passport.

    Only documented text is used.  ``physical_page`` and
    ``graphic_sheet_number`` are copied as provenance and are never part of the
    identity itself.
    """
    marks: list[dict[str, Any]] = []
    levels: list[dict[str, Any]] = []
    for field in MARK_SOURCE_FIELDS:
        for text in _text_values(_field_value(passport, field)):
            for row in extract_marks(text):
                marks.append({**row, "source_field": field})
            for value in extract_levels(text):
                levels.append({"level": value, "source_field": field})
    for text in _text_values(passport.get("floors")):
        for value in extract_levels(text):
            levels.append({"level": value, "source_field": "floors"})

    by_mark: dict[str, dict[str, Any]] = {}
    for row in marks:
        entry = by_mark.setdefault(row["mark"], {
            "mark": row["mark"],
            "series_id": row["series_id"],
            "series_ordinal": row["series_ordinal"],
            "source_fields": [],
            "literals": [],
        })
        if row["source_field"] not in entry["source_fields"]:
            entry["source_fields"].append(row["source_field"])
        if row["literal"] not in entry["literals"]:
            entry["literals"].append(row["literal"])

    title_marks = sorted(
        value["mark"] for value in by_mark.values()
        if PRIMARY_MARK_FIELD in value["source_fields"]
    )
    # An instance is named by exactly one mark in its own caption.  Several
    # marks in one caption name a relation between instances, not an instance.
    primary_mark = title_marks[0] if len(title_marks) == 1 else None
    scope = {
        field: sorted({
            value.strip() for value in _text_values(passport.get(field)) if value.strip()
        }) or None
        for field in SCOPE_FIELDS
    }
    level_values = sorted({row["level"] for row in levels})

    facts = {
        "marks": [by_mark[key] for key in sorted(by_mark)],
        "title_marks": title_marks,
        "primary_mark": primary_mark,
        "series_ids": sorted({value["series_id"] for value in by_mark.values()}),
        "levels": level_values,
        **scope,
    }
    present = [
        name for name, value in (
            ("mark", [primary_mark] if primary_mark else []),
            ("level", level_values),
            *((field, scope[field]) for field in SCOPE_FIELDS),
        ) if value
    ]
    status = (
        "PROVEN" if primary_mark
        else "PARTIAL" if present
        else "UNKNOWN"
    )
    return {
        "function_id": passport.get("function_id"),
        "side": passport.get("side"),
        "function_class": passport.get("function_class"),
        "component_role": passport.get("component_role"),
        "document_role": passport.get("document_role"),
        "identity_facts": facts,
        "present_fact_kinds": present,
        "identity_status": status,
        "identity_evidence_fields": sorted({
            row["source_field"] for row in (*marks, *levels)
        }),
        "provenance_only": {
            "physical_page": (passport.get("source_sheet") or {}).get("physical_page"),
            "graphic_sheet_number": (
                (passport.get("source_sheet") or {}).get("graphic_sheet_number")
            ),
            "note": "provenance never establishes identity or a match",
        },
    }


# ---------------------------------------------------------------------------
# Phase 3 — cluster disambiguation
# ---------------------------------------------------------------------------


CLUSTER_CLASSES = (
    "UNIQUELY_IDENTIFIED",
    "PARTIALLY_IDENTIFIED",
    "INDISTINGUISHABLE",
    "CONTRADICTORY",
    "UNKNOWN",
)

#: Facts that may individually tell two instances of one class apart.
DISCRIMINATING_FACT_KINDS = ("mark", "level", "zone", "section", "floors", "serviced_object")


def _fact_value(identity: Mapping[str, Any], kind: str) -> tuple[str, ...] | None:
    facts = identity["identity_facts"]
    if kind == "mark":
        primary = facts.get("primary_mark")
        return (primary,) if primary else None
    if kind == "level":
        return tuple(facts["levels"]) or None
    value = facts.get(kind)
    return tuple(value) if value else None


def classify_cluster(members: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Can these same-class instances be told apart by documented facts?"""
    if len(members) < 2:
        return {
            "classification": "UNIQUELY_IDENTIFIED",
            "reason": "SINGLE_MEMBER",
            "shared_facts": {},
            "distinguishing_facts": {},
            "missing_distinguishing_facts": [],
            "contradictions": [],
        }
    shared: dict[str, Any] = {}
    distinguishing: dict[str, Any] = {}
    missing: list[str] = []
    contradictions: list[dict[str, Any]] = []
    for kind in DISCRIMINATING_FACT_KINDS:
        values = [_fact_value(value, kind) for value in members]
        known = [value for value in values if value is not None]
        if not known:
            missing.append(kind)
            continue
        if len(known) < len(values):
            # A missing fact is UNKNOWN, never a mismatch, so a partially
            # populated fact can never separate the whole cluster.
            missing.append(kind)
        unique = {value for value in known}
        if len(unique) == 1:
            shared[kind] = sorted(next(iter(unique)))
            continue
        if len(known) < len(values):
            shared.setdefault(f"{kind}__partial", True)
            continue
        # Two instances are told apart only when their documented values do
        # not overlap at all.  Merely unequal lists that share a value do not
        # separate anything.
        disjoint = all(
            not (set(left) & set(right))
            for index, left in enumerate(known)
            for right in known[index + 1:]
        )
        if disjoint:
            distinguishing[kind] = {
                str(member["function_id"]): sorted(value or ())
                for member, value in zip(members, values)
            }
        else:
            shared.setdefault(f"{kind}__overlapping", True)

    # A contradiction is one instance whose own sources disagree about its mark.
    for member in members:
        facts = member["identity_facts"]
        title = set(facts["title_marks"])
        other = {
            row["mark"] for row in facts["marks"]
            if "source_sheet.title" not in row["source_fields"]
        }
        if title and other and not (title & other):
            same_series = {
                row["series_id"] for row in facts["marks"]
            }
            if len(same_series) == 1:
                contradictions.append({
                    "function_id": member["function_id"],
                    "title_marks": sorted(title),
                    "other_source_marks": sorted(other),
                    "series_id": sorted(same_series),
                })
    # A member is pinned when at least one of its documented values shares
    # nothing with any sibling: that is what makes it not confusable.
    pinned: list[str] = []
    for index, member in enumerate(members):
        others = [value for position, value in enumerate(members) if position != index]
        for kind in DISCRIMINATING_FACT_KINDS:
            own = _fact_value(member, kind)
            if own is None:
                continue
            rival_values = [_fact_value(value, kind) for value in others]
            if any(value is None for value in rival_values):
                continue
            if all(not (set(own) & set(value)) for value in rival_values):
                pinned.append(str(member["function_id"]))
                break
    any_fact = any(
        _fact_value(value, kind) is not None
        for value in members for kind in DISCRIMINATING_FACT_KINDS
    )
    if contradictions:
        classification = "CONTRADICTORY"
    elif distinguishing or len(pinned) == len(members):
        classification = "UNIQUELY_IDENTIFIED"
    elif pinned:
        classification = "PARTIALLY_IDENTIFIED"
    elif any_fact:
        classification = "INDISTINGUISHABLE"
    else:
        classification = "UNKNOWN"
    return {
        "classification": classification,
        "reason": (
            "OWN_SOURCES_DISAGREE" if contradictions
            else "EVERY_MEMBER_CARRIES_A_DISTINCT_DOCUMENTED_FACT"
            if classification == "UNIQUELY_IDENTIFIED"
            else "ONLY_SOME_MEMBERS_ARE_PINNED_BY_A_DOCUMENTED_FACT"
            if classification == "PARTIALLY_IDENTIFIED"
            else "FACTS_EXIST_BUT_NONE_SEPARATES_ANY_MEMBER"
            if classification == "INDISTINGUISHABLE"
            else "NO_DOCUMENTED_FACT_AT_ALL"
        ),
        "pinned_member_function_ids": sorted(set(pinned)),
        "shared_facts": shared,
        "distinguishing_facts": distinguishing,
        "missing_distinguishing_facts": sorted(set(missing)),
        "contradictions": contradictions,
    }


# ---------------------------------------------------------------------------
# corpus survey
# ---------------------------------------------------------------------------


def _passports(pair_id: str) -> dict[str, Mapping[str, Any]]:
    artifact = stratified._read_json(
        stratified.CANDIDATE_ROOT / f"{pair_id}.json"
    )
    return {
        **artifact["function_passports"]["LEFT"],
        **artifact["function_passports"]["RIGHT"],
    }


def survey() -> dict[str, Any]:
    """Measure how far documented instance facts actually go."""
    corpora: dict[str, Any] = {}
    clusters_all: list[dict[str, Any]] = []
    for pair_id, project in sorted(
        stratified.PAIR_PROJECTS.items(), key=lambda item: item[1]
    ):
        passports = _passports(pair_id)
        identities = {
            function_id: function_instance_identity(value)
            for function_id, value in sorted(passports.items())
        }
        status = Counter(value["identity_status"] for value in identities.values())
        fact_coverage = Counter(
            kind for value in identities.values() for kind in value["present_fact_kinds"]
        )
        groups: dict[tuple[str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
        for value in identities.values():
            groups[(
                str(value["side"]), str(value["function_class"]),
                str(value["document_role"]),
            )].append(value)
        clusters = []
        for key, members in sorted(groups.items()):
            if len(members) < 2:
                continue
            verdict = classify_cluster(members)
            clusters.append({
                "pair_id": pair_id,
                "project": project,
                "side": key[0],
                "function_class": key[1],
                "document_role": key[2],
                "member_count": len(members),
                "member_function_ids": sorted(
                    str(value["function_id"]) for value in members
                ),
                **verdict,
            })
        clusters_all.extend(clusters)
        corpora[project] = {
            "pair_id": pair_id,
            "function_count": len(identities),
            "identity_status": dict(sorted(status.items())),
            "fact_coverage": dict(sorted(fact_coverage.items())),
            "ambiguous_cluster_count": len(clusters),
            "cluster_classification": dict(sorted(Counter(
                value["classification"] for value in clusters
            ).items())),
        }
    overall = Counter(value["classification"] for value in clusters_all)
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "function_instance_identity_survey",
        "model_calls": 0,
        "rules": {
            "physical_page_is_identity": False,
            "graphic_sheet_number_is_identity": False,
            "missing_fact_is_mismatch": False,
            "project_or_page_specific_rules": False,
        },
        "mark_source_fields": list(MARK_SOURCE_FIELDS),
        "scope_fields": list(SCOPE_FIELDS),
        "discriminating_fact_kinds": list(DISCRIMINATING_FACT_KINDS),
        "corpora": corpora,
        "cluster_classification_overall": dict(sorted(overall.items())),
        "ambiguous_clusters": clusters_all,
        "contested_cluster_resolution": contested_cluster_resolution(),
        "certified_tier_feasibility": certified_tier_feasibility(),
    }


# ---------------------------------------------------------------------------
# Phase 8 — does the identity layer explain the observed contested clusters?
# ---------------------------------------------------------------------------

#: Read-only diagnosis over model responses that were already recorded under
#: consent.  It is never acceptance evidence and never re-runs a model.
ACCEPTANCE_METRICS = (
    stratified.COMPARISON_ROOT
    / "20260904_function_lineage_v2_7_tiered_acceptance" / "metrics.json"
)

RESOLUTION_CLASSES = (
    "RESOLVED_BY_MATCHING_MARK",
    "SEPARABLE_LEFT_ONLY",
    "PARTIAL_SOME_MARKS_MISSING",
    "NO_IDENTITY_SIGNAL",
)


def contested_cluster_resolution() -> dict[str, Any]:
    """Would documented identity have singled out one claimant per fragment?"""
    if not ACCEPTANCE_METRICS.is_file():
        return {"applicable": False, "reason": "NO_RECORDED_ACCEPTANCE_RUN"}
    metrics = stratified._read_json(ACCEPTANCE_METRICS)
    artifacts = {
        pair_id: stratified._read_json(
            stratified.CANDIDATE_ROOT / f"{pair_id}.json"
        )
        for pair_id in stratified.PAIR_PROJECTS
    }
    candidates = {
        pair_id: {
            str(value["candidate_id"]): value
            for value in artifact["functional_candidates"]
        }
        for pair_id, artifact in artifacts.items()
    }
    identities = {
        pair_id: {
            function_id: function_instance_identity(value)
            for function_id, value in {
                **artifact["function_passports"]["LEFT"],
                **artifact["function_passports"]["RIGHT"],
            }.items()
        }
        for pair_id, artifact in artifacts.items()
    }

    claims: dict[str, set[str]] = defaultdict(set)
    for error in metrics["safety"]["capacity_errors"]:
        parts = str(error).split(":")
        if len(parts) != 6:
            continue
        claims[":".join(parts[1:4])].update(parts[4:6])

    rows: list[dict[str, Any]] = []
    for key in sorted(claims):
        members = sorted(claims[key])
        pair_id = next(
            (
                value for value in candidates
                if all(member in candidates[value] for member in members)
            ),
            None,
        )
        if pair_id is None:
            continue
        artifact = artifacts[pair_id]
        fragment_id = key.split(":")[-1]
        right = artifact["function_fragments"]["RIGHT"][fragment_id]
        right_identity = identities[pair_id].get(str(right["function_id"]), {})
        right_mark = (right_identity.get("identity_facts") or {}).get("primary_mark")
        left_fragments = artifact["function_fragments"]["LEFT"]
        claimants = []
        for member in members:
            marks = sorted({
                mark
                for row in candidates[pair_id][member]["component_map"]
                if str(row.get("capacity_key")) == key
                for mark in [(
                    identities[pair_id]
                    .get(str(left_fragments[row["left_fragment_id"]]["function_id"]), {})
                    .get("identity_facts") or {}
                ).get("primary_mark")]
                if mark
            })
            claimants.append({"candidate_id": member, "left_marks": marks})
        marked = [value for value in claimants if value["left_marks"]]
        if right_mark and sum(
            right_mark in value["left_marks"] for value in claimants
        ) == 1:
            classification = "RESOLVED_BY_MATCHING_MARK"
        elif len(marked) == len(claimants) and len({
            tuple(value["left_marks"]) for value in claimants
        }) == len(claimants):
            classification = "SEPARABLE_LEFT_ONLY"
        elif marked:
            classification = "PARTIAL_SOME_MARKS_MISSING"
        else:
            classification = "NO_IDENTITY_SIGNAL"
        rows.append({
            "pair_id": pair_id,
            "project": stratified.PAIR_PROJECTS[pair_id],
            "capacity_key": key,
            "right_primary_mark": right_mark,
            "claimants": claimants,
            "classification": classification,
        })
    counts = Counter({name: 0 for name in RESOLUTION_CLASSES})
    for row in rows:
        counts[row["classification"]] += 1
    return {
        "applicable": True,
        "diagnostic_only": True,
        "usable_as_acceptance_evidence": False,
        "model_calls": 0,
        "contested_clusters": len(rows),
        "classification_counts": dict(sorted(counts.items())),
        "resolved": counts["RESOLVED_BY_MATCHING_MARK"],
        "clusters": rows,
    }


def certified_tier_feasibility() -> dict[str, Any]:
    """How many tasks could ever enter an identity-certified 1:1 tier."""
    holdout_population = (
        stratified.COMPARISON_ROOT
        / "20260904_function_lineage_v2_6_holdout_evaluation"
        / "holdout_population.json"
    )
    if not holdout_population.is_file():
        return {"applicable": False, "reason": "NO_FROZEN_POPULATION"}
    population = stratified._read_json(holdout_population)
    artifacts = {
        pair_id: stratified._read_json(
            stratified.CANDIDATE_ROOT / f"{pair_id}.json"
        )
        for pair_id in stratified.PAIR_PROJECTS
    }
    candidates = {
        pair_id: {
            str(value["candidate_id"]): value
            for value in artifact["functional_candidates"]
        }
        for pair_id, artifact in artifacts.items()
    }
    identities = {
        pair_id: {
            function_id: function_instance_identity(value)
            for function_id, value in {
                **artifact["function_passports"]["LEFT"],
                **artifact["function_passports"]["RIGHT"],
            }.items()
        }
        for pair_id, artifact in artifacts.items()
    }
    rows = [value for value in population["tasks"] if not value["sentinel"]]
    one_to_one = [
        value for value in rows if value["relation_types"] == ["CONTINUED_1_TO_1"]
    ]
    keys = {}
    for value in rows:
        pair_id = stratified.PROJECT_PAIRS[value["corpus"]]
        collected: set[str] = set()
        for candidate_id in value["candidate_ids"]:
            candidate = candidates[pair_id].get(str(candidate_id))
            if candidate:
                collected |= {
                    str(item) for item in candidate["right_capacity_keys"]
                }
        keys[str(value["task_id"])] = (pair_id, collected)

    uncontended = []
    for value in one_to_one:
        pair_id, own = keys[str(value["task_id"])]
        rivals = sum(
            1 for other in rows
            if str(other["task_id"]) != str(value["task_id"])
            and keys[str(other["task_id"])][0] == pair_id
            and (own & keys[str(other["task_id"])][1])
        )
        if rivals == 0:
            uncontended.append(str(value["task_id"]))

    fragment_tasks = {
        pair_id: {
            str(value["task_id"]): value
            for value in artifact["candidate_tasks"]
        }
        for pair_id, artifact in artifacts.items()
    }
    both_proven = []
    for value in one_to_one:
        pair_id = stratified.PROJECT_PAIRS[value["corpus"]]
        left = [
            identities[pair_id][
                fragment_tasks[pair_id][str(source)]["left_function_id"]
            ]
            for source in value["source_task_ids"]
        ]
        right_ids = {
            function_id
            for candidate_id in value["candidate_ids"]
            for function_id in candidates[pair_id][str(candidate_id)]["right_function_ids"]
        }
        right = [
            identities[pair_id][function_id] for function_id in sorted(right_ids)
            if function_id in identities[pair_id]
        ]
        if (
            left and right
            and all(row["identity_status"] == "PROVEN" for row in left)
            and all(row["identity_status"] == "PROVEN" for row in right)
        ):
            both_proven.append(str(value["task_id"]))
    return {
        "applicable": True,
        "non_sentinel_tasks": len(rows),
        "pure_one_to_one_tasks": len(one_to_one),
        "uncontended_pure_one_to_one_tasks": len(uncontended),
        "uncontended_task_ids": sorted(uncontended),
        "both_sides_identity_proven_tasks": len(both_proven),
        "both_sides_proven_task_ids": sorted(both_proven),
        "criterion_note": (
            "certification needs an instance that cannot be confused with a "
            "sibling; contention with another task's inventory is exactly that "
            "confusion"
        ),
    }


def render_report(artifact: Mapping[str, Any]) -> str:
    lines = [
        "# Function Lineage v2.7 — deterministic instance / series identity",
        "",
        "Phase 1 forensics. No model calls. Physical page numbers and graphic "
        "sheet numbers are provenance only and never establish identity or a "
        "match; a missing fact stays UNKNOWN and is never a mismatch.",
        "",
        "## Identity coverage per corpus",
        "",
        "| Corpus | Functions | PROVEN | PARTIAL | UNKNOWN |",
        "|---|---:|---:|---:|---:|",
    ]
    for project, row in artifact["corpora"].items():
        status = row["identity_status"]
        lines.append(
            f"| {project} | {row['function_count']} | {status.get('PROVEN', 0)} | "
            f"{status.get('PARTIAL', 0)} | {status.get('UNKNOWN', 0)} |"
        )
    lines.extend([
        "",
        "## Which documented facts exist at all",
        "",
        "| Corpus | " + " | ".join(artifact["discriminating_fact_kinds"]) + " |",
        "|---" * (len(artifact["discriminating_fact_kinds"]) + 1) + "|",
    ])
    for project, row in artifact["corpora"].items():
        coverage = row["fact_coverage"]
        lines.append(
            f"| {project} | "
            + " | ".join(
                str(coverage.get(kind, 0))
                for kind in artifact["discriminating_fact_kinds"]
            )
            + " |"
        )
    lines.extend([
        "",
        "## Same-class clusters",
        "",
        "A cluster is every function of one side sharing class and document "
        "role — exactly the situation where the selector had to guess.",
        "",
        "| Classification | Clusters |",
        "|---|---:|",
    ])
    for name in CLUSTER_CLASSES:
        lines.append(
            f"| `{name}` | {artifact['cluster_classification_overall'].get(name, 0)} |"
        )
    lines.extend([
        "",
        "| Corpus | Clusters | Breakdown |",
        "|---|---:|---|",
    ])
    for project, row in artifact["corpora"].items():
        lines.append(
            f"| {project} | {row['ambiguous_cluster_count']} | "
            f"`{row['cluster_classification']}` |"
        )
    resolution = artifact.get("contested_cluster_resolution") or {}
    feasibility = artifact.get("certified_tier_feasibility") or {}
    if resolution.get("applicable"):
        lines.extend([
            "",
            "## Does identity resolve the contested clusters? (diagnostic)",
            "",
            "Read-only over responses already recorded under consent. Never "
            "acceptance evidence.",
            "",
            f"Contested clusters examined `{resolution['contested_clusters']}`; "
            f"resolved by a matching mark **`{resolution['resolved']}`**.",
            "",
            "| Outcome | Clusters |",
            "|---|---:|",
            *(
                f"| `{name}` | {value} |"
                for name, value in resolution["classification_counts"].items()
            ),
        ])
    if feasibility.get("applicable"):
        lines.extend([
            "",
            "## Could an identity-certified 1:1 tier exist at all?",
            "",
            f"Non-sentinel tasks `{feasibility['non_sentinel_tasks']}`; of them "
            f"purely CONTINUED_1_TO_1 `{feasibility['pure_one_to_one_tasks']}`.",
            "",
            f"* with no capacity contention against any sibling task: "
            f"**`{feasibility['uncontended_pure_one_to_one_tasks']}`**",
            f"* with a proven instance identity on both sides: "
            f"**`{feasibility['both_sides_identity_proven_tasks']}`**",
            "",
            feasibility["criterion_note"],
        ])
    lines.append("")
    return "\n".join(lines)


def write(output: Path | None = None) -> Path:
    target = Path(output or DEFAULT_OUTPUT)
    target.mkdir(parents=True, exist_ok=True)
    artifact = survey()
    (target / "instance_identity_survey.json").write_bytes(
        stratified._json_bytes(artifact)
    )
    (target / "report.md").write_text(render_report(artifact), encoding="utf-8")
    return target


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args(list(argv) if argv is not None else None)
    print(json.dumps({"output": str(write(args.output))}, ensure_ascii=False))
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI
    raise SystemExit(main())
