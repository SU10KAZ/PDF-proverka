"""Candidate-v4 adapter for the unchanged bounded AI Sheet Matcher selector.

Only candidate construction and the evidence payload are replaced.  Output
shape, local-then-map review, Pass A/B unanimity, human priority, and the
fail-closed verifier remain inherited from ``ai_sheet_matcher-research.v1``.
"""
from __future__ import annotations

import re
import statistics
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from experiments.ai_sheet_matcher.core import (
    CONCRETE_DECISIONS,
    PROJECT_CONFIG,
    SENTINEL_OPTION_IDS,
    ProjectDataset,
    canonical_json,
    digest,
    selector_schema,
    stable_id,
    verify_selector_response,
)
from experiments.candidate_v4.core import (
    ALGORITHM_VERSION as CANDIDATE_GENERATOR_VERSION,
    CandidateV4Dataset,
    _group_audit as generator_group_audit,
    build_candidate_v4_dataset,
)


ALGORITHM_VERSION = "ai-sheet-matcher-v4-repeat.v1"
CONTRACT_VERSION = "bounded-sheet-selector.v1"
GROUP_SHORTLIST_LIMIT = 16
_WORD_RE = re.compile(r"[a-zа-яё0-9][a-zа-яё0-9._-]*", re.IGNORECASE)


@dataclass
class V4SelectorDataset:
    """Selector-compatible dataset plus immutable v4 source objects."""

    selector: ProjectDataset
    candidate_v4: CandidateV4Dataset
    page_candidates: dict[str, dict[str, Any]]
    group_candidates: dict[str, dict[str, Any]]
    group_shortlists: dict[int, list[dict[str, Any]]]
    comparison_left_pages: list[int]


def _tokens(values: Any) -> set[str]:
    if isinstance(values, Mapping):
        values = list(values.values())
    if not isinstance(values, (list, tuple, set)):
        values = [values]
    return set(_WORD_RE.findall(" ".join(str(value) for value in values).casefold()))


def _jaccard(left: set[str], right: set[str]) -> float:
    return len(left & right) / len(left | right) if left | right else 0.0


def _flatten(items: Iterable[Any]) -> list[Any]:
    output: list[Any] = []
    for item in items:
        if isinstance(item, (list, tuple, set)):
            output.extend(item)
        else:
            output.append(item)
    return output


def group_support_vector(dataset: CandidateV4Dataset, group: Mapping[str, Any]) -> dict[str, Any]:
    """Seven-priority deterministic group rank; no evaluation map is read."""
    left_pages = [int(page) for page in group["left_pages"]]
    right_pages = [int(page) for page in group["right_pages"]]
    left_functions = [
        function for page in left_pages
        for function in dataset.function_passports["LEFT"][page]
    ]
    right_functions = [
        function for page in right_pages
        for function in dataset.function_passports["RIGHT"][page]
    ]
    left_classes = {
        str(function["function_class"]) for function in left_functions
        if function.get("function_class") != "GENERAL_DOCUMENT_FUNCTION"
    }
    covered = set(str(value) for value in group.get("covered_functions") or [])
    covered_left = covered & left_classes

    left_object = _tokens(_flatten(
        dataset.sheet_passports["LEFT"][page].get(key) or []
        for page in left_pages for key in ("object_corpus", "zone")
    ))
    right_object = _tokens(_flatten(
        dataset.sheet_passports["RIGHT"][page].get(key) or []
        for page in right_pages for key in ("object_corpus", "zone")
    ))
    left_systems = _tokens(_flatten(function.get("systems") or [] for function in left_functions))
    right_systems = _tokens(_flatten(function.get("systems") or [] for function in right_functions))
    coverage_map = group.get("component_coverage") or {}
    covered_component_pages: set[int] = set()
    for value in coverage_map.values():
        if isinstance(value, list):
            covered_component_pages.update(int(page) for page in value if isinstance(page, int))
    grounds = [str(value).casefold() for value in group.get("why_group_exists") or []]

    left_entities = _tokens(_flatten(
        dataset.sheet_passports["LEFT"][page].get("entities") or [] for page in left_pages
    ))
    right_entities = _tokens(_flatten(
        dataset.sheet_passports["RIGHT"][page].get("entities") or [] for page in right_pages
    ))
    left_topology = _tokens(_flatten(
        dataset.sheet_passports["LEFT"][page].get("topology_hints") or [] for page in left_pages
    ))
    right_topology = _tokens(_flatten(
        dataset.sheet_passports["RIGHT"][page].get("topology_hints") or [] for page in right_pages
    ))
    return {
        "function_coverage_ratio": round(
            len(covered_left) / len(left_classes) if left_classes else float(bool(covered)), 6,
        ),
        "function_coverage_count": len(covered_left),
        "object_zone_overlap": round(_jaccard(left_object, right_object), 6),
        "system_family_overlap": round(_jaccard(left_systems, right_systems), 6),
        "complementary_coverage_count": len(coverage_map),
        "complementary_page_count": len(covered_component_pages),
        "complementary_ground": int(any(
            "complement" in value or "set cover" in value for value in grounds
        )),
        "entity_overlap": round(_jaccard(left_entities, right_entities), 6),
        "topology_overlap": round(_jaccard(left_topology, right_topology), 6),
        "neighbor_context": sum(
            "neighbor" in value or "lineage" in value for value in grounds
        ),
        "right_sequence": int(all(
            right == left + 1 for left, right in zip(right_pages, right_pages[1:])
        )),
        "v4_group_score_tiebreak": float(group.get("group_score") or 0.0),
    }


def _support_sort_key(item: Mapping[str, Any]) -> tuple[Any, ...]:
    vector = item["shortlist_support"]
    priority = (
        vector["function_coverage_ratio"],
        vector["function_coverage_count"],
        vector["object_zone_overlap"],
        vector["system_family_overlap"],
        vector["complementary_coverage_count"],
        vector["complementary_page_count"],
        vector["complementary_ground"],
        vector["entity_overlap"],
        vector["topology_overlap"],
        vector["neighbor_context"],
        vector["right_sequence"],
        vector["v4_group_score_tiebreak"],
    )
    return tuple(-float(value) for value in priority) + (
        str(item["relation_type"]),
        tuple(int(page) for page in item["right_pages"]),
        str(item["candidate_group_id"]),
    )


def build_group_shortlists(
    dataset: CandidateV4Dataset,
    left_pages: Sequence[int],
    *,
    limit: int = GROUP_SHORTLIST_LIMIT,
) -> dict[int, list[dict[str, Any]]]:
    """Rank bounded groups without consulting human/reference evaluation data."""
    universe = set(int(page) for page in left_pages)
    scored: dict[str, dict[str, Any]] = {}
    for raw in dataset.group_candidates:
        group_left = {int(page) for page in raw["left_pages"]}
        if not group_left or not group_left <= universe:
            continue
        group_id = str(raw["candidate_group_id"])
        scored[group_id] = {
            **dict(raw),
            "shortlist_support": group_support_vector(dataset, raw),
        }
    provisional: dict[int, list[str]] = {}
    for left_page in sorted(universe):
        applicable = [
            row for row in scored.values() if left_page in set(row["left_pages"])
        ]
        provisional[left_page] = [
            str(row["candidate_group_id"])
            for row in sorted(applicable, key=_support_sort_key)[:limit]
        ]

    # A multi-LEFT group is atomic.  It is exposed only if it independently
    # survives the bound for every participating LEFT task.
    eligible = {
        group_id for group_id, group in scored.items()
        if all(group_id in provisional[left] for left in group["left_pages"])
    }
    output: dict[int, list[dict[str, Any]]] = {}
    for left_page in sorted(universe):
        output[left_page] = [
            {
                **scored[group_id],
                "shortlist_rank": rank,
                "shortlist_limit": limit,
            }
            for rank, group_id in enumerate(
                (value for value in provisional[left_page] if value in eligible), 1
            )
        ]
    return output


def _candidate_option(raw: Mapping[str, Any]) -> dict[str, Any]:
    candidate_id = str(raw["candidate_id"])
    return {
        "option_id": candidate_id,
        "pair_id": str(raw["pair_id"]),
        "decision_type": "MATCH_1_TO_1",
        "left_pages": [int(raw["left_physical_page"])],
        "right_pages": [int(raw["right_physical_page"])],
        "evidence_refs": sorted(set(str(value) for value in raw.get("evidence_refs") or [])),
        "deterministic_evidence": {
            "candidate_source": CANDIDATE_GENERATOR_VERSION,
            "candidate_kind": "RIGHT_PAGE",
            "v4_rank": int(raw["rank"]),
            "ranking_score": raw.get("ranking_score"),
            "which_channels_found": list(raw.get("which_channels_found") or []),
            "channel_ranks": dict(raw.get("channel_ranks") or {}),
            "channel_scores": dict(raw.get("channel_scores") or {}),
            "left_function_matches": dict(raw.get("left_function_matches") or {}),
            "explicit_contradictions": list(raw.get("explicit_contradictions") or []),
            "generator_object_id": candidate_id,
        },
    }


def _group_option(raw: Mapping[str, Any]) -> dict[str, Any]:
    group_id = str(raw["candidate_group_id"])
    return {
        "option_id": group_id,
        "pair_id": str(raw["pair_id"]),
        "decision_type": str(raw["relation_type"]),
        "left_pages": sorted(int(page) for page in raw["left_pages"]),
        "right_pages": sorted(int(page) for page in raw["right_pages"]),
        "evidence_refs": sorted(set(str(value) for value in raw.get("evidence_refs") or [])),
        "deterministic_evidence": {
            "candidate_source": CANDIDATE_GENERATOR_VERSION,
            "candidate_kind": "RIGHT_PAGE_GROUP",
            "covered_functions": list(raw.get("covered_functions") or []),
            "component_coverage": dict(raw.get("component_coverage") or {}),
            "why_group_exists": list(raw.get("why_group_exists") or []),
            "group_score": raw.get("group_score"),
            "generator_object_id": group_id,
        },
    }


def _all_evidence(dataset: CandidateV4Dataset) -> dict[str, dict[str, Any]]:
    output: dict[str, dict[str, Any]] = {}
    for side in ("LEFT", "RIGHT"):
        for sheet in dataset.sheet_passports[side].values():
            for evidence_id, raw in (sheet.get("evidence_catalog") or {}).items():
                normalized = dict(raw)
                # The unchanged v1 verifier names this field ``page``; v4's
                # evidence catalog calls it ``physical_page``.
                normalized["page"] = normalized.get("physical_page")
                output[str(evidence_id)] = normalized
    return output


def build_v4_selector_dataset(
    repo_root: Path,
    pair_id: str,
    *,
    group_limit: int = GROUP_SHORTLIST_LIMIT,
) -> V4SelectorDataset:
    candidate_v4 = build_candidate_v4_dataset(repo_root, pair_id)
    base = candidate_v4.base
    focus = sorted(int(page) for page in PROJECT_CONFIG[pair_id]["focus_left_pages"])
    shortlists = build_group_shortlists(candidate_v4, focus, limit=group_limit)
    page_candidates: dict[str, dict[str, Any]] = {}
    group_candidates: dict[str, dict[str, Any]] = {}
    options: dict[str, dict[str, Any]] = {}
    option_ids: dict[int, list[str]] = {page: [] for page in focus}

    for left_page in focus:
        for raw in candidate_v4.candidate_sets[left_page]["candidates"]:
            candidate_id = str(raw["candidate_id"])
            page_candidates[candidate_id] = dict(raw)
            options[candidate_id] = _candidate_option(raw)
            option_ids[left_page].append(candidate_id)
    for left_page in focus:
        for raw in shortlists[left_page]:
            group_id = str(raw["candidate_group_id"])
            group_candidates[group_id] = dict(raw)
            options.setdefault(group_id, _group_option(raw))
            option_ids[left_page].append(group_id)

    tasks = []
    for left_page in focus:
        page_ids = sorted(
            (value for value in option_ids[left_page] if value in page_candidates),
            key=lambda value: (int(page_candidates[value]["rank"]), value),
        )
        group_ids = sorted(
            (value for value in option_ids[left_page] if value in group_candidates),
            key=lambda value: (
                next(
                    int(row["shortlist_rank"]) for row in shortlists[left_page]
                    if row["candidate_group_id"] == value
                ),
                value,
            ),
        )
        tasks.append({
            "task_id": stable_id("task_", ALGORITHM_VERSION, pair_id, left_page),
            "left_page": left_page,
            "left_function_ids": [
                str(value["function_id"])
                for value in candidate_v4.function_passports["LEFT"][left_page]
            ],
            "page_candidate_ids": page_ids,
            "group_candidate_ids": group_ids,
            "option_ids": [*page_ids, *group_ids, *SENTINEL_OPTION_IDS],
        })
    signature = digest({
        "algorithm": ALGORITHM_VERSION,
        "candidate_v4_input_signature": candidate_v4.input_signature,
        "group_shortlist_limit": group_limit,
        "tasks": tasks,
        "options": options,
    })
    selector = replace(
        base,
        evidence_catalog=_all_evidence(candidate_v4),
        top10={
            page: [dict(item) for item in candidate_v4.candidate_sets[page]["candidates"]]
            for page in focus
        },
        deep_top10={},
        tasks=tasks,
        options=options,
        input_signature=signature,
    )
    return V4SelectorDataset(
        selector=selector,
        candidate_v4=candidate_v4,
        page_candidates=page_candidates,
        group_candidates=group_candidates,
        group_shortlists=shortlists,
        comparison_left_pages=focus,
    )


def subset_selector_dataset(
    dataset: V4SelectorDataset,
    left_pages: Iterable[int],
) -> V4SelectorDataset:
    """Create a fallback subset without reopening already closed TEXT tasks."""
    selected = {int(page) for page in left_pages}
    universe = set(dataset.comparison_left_pages)
    if not selected <= universe:
        raise ValueError("fallback subset contains an unknown LEFT page")
    tasks = []
    for raw_task in dataset.selector.tasks:
        if int(raw_task["left_page"]) not in selected:
            continue
        task = dict(raw_task)
        allowed = []
        for option_id in task["option_ids"]:
            if option_id in SENTINEL_OPTION_IDS:
                allowed.append(option_id)
                continue
            option = dataset.selector.options[option_id]
            if set(int(page) for page in option["left_pages"]) <= selected:
                allowed.append(option_id)
        task["option_ids"] = allowed
        task["page_candidate_ids"] = [
            value for value in task.get("page_candidate_ids") or [] if value in allowed
        ]
        task["group_candidate_ids"] = [
            value for value in task.get("group_candidate_ids") or [] if value in allowed
        ]
        tasks.append(task)
    used_ids = {
        str(option_id) for task in tasks for option_id in task["option_ids"]
        if option_id not in SENTINEL_OPTION_IDS
    }
    options = {key: value for key, value in dataset.selector.options.items() if key in used_ids}
    signature = digest({
        "parent": dataset.selector.input_signature,
        "fallback_left_pages": sorted(selected),
        "tasks": tasks,
        "options": options,
    })
    selector = replace(
        dataset.selector,
        tasks=tasks,
        options=options,
        input_signature=signature,
    )
    return V4SelectorDataset(
        selector=selector,
        candidate_v4=dataset.candidate_v4,
        page_candidates={key: value for key, value in dataset.page_candidates.items() if key in used_ids},
        group_candidates={key: value for key, value in dataset.group_candidates.items() if key in used_ids},
        group_shortlists={
            page: [
                row for row in dataset.group_shortlists[page]
                if set(int(value) for value in row["left_pages"]) <= selected
            ]
            for page in sorted(selected)
        },
        comparison_left_pages=sorted(selected),
    )


def _take(values: Any, limit: int = 12, *, char_limit: int = 360) -> list[Any]:
    if not isinstance(values, list):
        return []
    return [
        (value[:char_limit] if isinstance(value, str) and len(value) > char_limit else value)
        for value in values[:limit]
    ]


def _sheet_view(sheet: Mapping[str, Any]) -> dict[str, Any]:
    provenance_refs = sorted({
        str(ref) for refs in (sheet.get("provenance") or {}).values()
        for ref in (refs if isinstance(refs, list) else [])
    })
    return {
        "document_version_id": sheet.get("document_version_id"),
        "side": sheet.get("side"),
        "physical_page": sheet.get("physical_page"),
        "graphic_sheet_number": sheet.get("graphic_sheet_number"),
        "page_kind": sheet.get("_page_kind"),
        "title": sheet.get("title"),
        "document_subtype": _take(sheet.get("document_subtype"), 8),
        "discipline": sheet.get("discipline"),
        "systems": _take(sheet.get("systems"), 12),
        "object_corpus": _take(sheet.get("object_corpus"), 10),
        "zone": _take(sheet.get("zone"), 8),
        "floors": _take(sheet.get("floors"), 8),
        "consumers": _take(sheet.get("consumers"), 5),
        "equipment": _take(sheet.get("equipment"), 6),
        "source": _take(sheet.get("source"), 5),
        "receivers": _take(sheet.get("receivers"), 5),
        "entities": _take(sheet.get("entities"), 24),
        "topology_hints": _take(sheet.get("topology_hints"), 6),
        "stamp_fields": dict(sheet.get("stamp_fields") or {}),
        "neighboring_sheets": list(sheet.get("neighboring_sheets") or []),
        "toc_references": _take(sheet.get("toc_references"), 8),
        "evidence_refs": list(sheet.get("text_evidence_references") or []),
        "provenance_evidence_refs": provenance_refs,
    }


def _function_view(function: Mapping[str, Any]) -> dict[str, Any]:
    provenance_refs = sorted({
        str(ref) for refs in (function.get("provenance") or {}).values()
        for ref in (refs if isinstance(refs, list) else [])
    })
    return {
        "function_id": function.get("function_id"),
        "source_sheet_refs": list(function.get("source_sheet_refs") or []),
        "function_class": function.get("function_class"),
        "fragment_text": _take(function.get("fragment_text"), 6),
        "evidence_refs": list(function.get("evidence_refs") or []),
        "provenance_evidence_refs": provenance_refs,
        "shared_sheet_passport_ref": (
            f"{function.get('source_sheet_refs', [{}])[0].get('physical_page')}"
            if function.get("source_sheet_refs") else None
        ),
    }


def _page_candidate_view(dataset: V4SelectorDataset, raw: Mapping[str, Any]) -> dict[str, Any]:
    left_page = int(raw["left_physical_page"])
    right_page = int(raw["right_physical_page"])
    functions = dataset.candidate_v4.function_passports
    return {
        "candidate_id": raw["candidate_id"],
        "decision_type": "MATCH_1_TO_1",
        "left_physical_page": left_page,
        "left_function_ids": list(raw.get("left_function_ids") or []),
        "right_physical_page": right_page,
        "right_graphic_sheet_number": raw.get("right_graphic_sheet_number"),
        "right_sheet_passport_ref": f"RIGHT:{right_page}",
        "right_function_passport_refs": [
            value["function_id"] for value in functions["RIGHT"][right_page]
        ],
        "rank": raw.get("rank"),
        "ranking_score": raw.get("ranking_score"),
        "retrieval_channels": list(raw.get("which_channels_found") or []),
        "retrieval_channel_evidence": {
            name: [
                (raw.get("channel_ranks") or {}).get(name),
                (raw.get("channel_scores") or {}).get(name),
            ]
            for name in ("FUNCTION", "ENTITY", "OBJECT_ZONE", "TOPOLOGY", "TITLE_STAMP", "NEIGHBOR_TOC")
        },
        "functional_evidence": {
            "left_function_matches": dict(raw.get("left_function_matches") or {}),
            "right_function_classes": [value["function_class"] for value in functions["RIGHT"][right_page]],
        },
        "field_evidence_refs": {
            "sheet_sides": [f"LEFT:{left_page}", f"RIGHT:{right_page}"],
            "entity_evidence": ["entities"],
            "object_zone_evidence": ["object_corpus", "zone"],
            "topology_evidence": ["topology_hints"],
            "stamp_title_evidence": ["title", "stamp_fields"],
            "neighbor_toc_evidence": ["neighboring_sheets", "toc_references"],
        },
        "contradictions": list(raw.get("explicit_contradictions") or []),
        "provenance": {
            "generator_version": CANDIDATE_GENERATOR_VERSION,
            "evidence_refs": list(raw.get("evidence_refs") or []),
        },
    }


def _group_candidate_view(dataset: V4SelectorDataset, raw: Mapping[str, Any]) -> dict[str, Any]:
    group_id = str(raw["candidate_group_id"])
    left_pages = [int(page) for page in raw["left_pages"]]
    right_pages = [int(page) for page in raw["right_pages"]]
    return {
        "candidate_group_id": group_id,
        "decision_type": raw["relation_type"],
        "left_pages": left_pages,
        "left_function_ids": list(raw.get("left_function_ids") or []),
        "right_pages": right_pages,
        "right_sheet_passport_refs": [f"RIGHT:{page}" for page in right_pages],
        "covered_functions": list(raw.get("covered_functions") or []),
        "component_coverage": dict(raw.get("component_coverage") or {}),
        "why_group_exists": list(raw.get("why_group_exists") or []),
        "shortlist_support": {
            "function_coverage": [
                (raw.get("shortlist_support") or {}).get("function_coverage_ratio"),
                (raw.get("shortlist_support") or {}).get("function_coverage_count"),
            ],
            "object_zone": (raw.get("shortlist_support") or {}).get("object_zone_overlap"),
            "system_family": (raw.get("shortlist_support") or {}).get("system_family_overlap"),
            "complementary_coverage": [
                (raw.get("shortlist_support") or {}).get("complementary_coverage_count"),
                (raw.get("shortlist_support") or {}).get("complementary_page_count"),
                (raw.get("shortlist_support") or {}).get("complementary_ground"),
            ],
            "entities": (raw.get("shortlist_support") or {}).get("entity_overlap"),
            "topology": (raw.get("shortlist_support") or {}).get("topology_overlap"),
            "neighbor_context": [
                (raw.get("shortlist_support") or {}).get("neighbor_context"),
                (raw.get("shortlist_support") or {}).get("right_sequence"),
            ],
        },
        "shortlist_ranks_by_left": {
            str(page): next(
                row["shortlist_rank"] for row in dataset.group_shortlists[page]
                if row["candidate_group_id"] == group_id
            )
            for page in left_pages
        },
        "group_score": raw.get("group_score"),
        "provenance": {
            "generator_version": raw.get("generator_version"),
            "evidence_refs": list(raw.get("evidence_refs") or []),
        },
    }


def build_selector_payload(dataset: V4SelectorDataset) -> dict[str, Any]:
    selector = dataset.selector
    left_pages = sorted(int(task["left_page"]) for task in selector.tasks)
    right_pages = sorted({
        int(page) for option in selector.options.values() for page in option["right_pages"]
    })
    left_functions = [
        function for page in left_pages
        for function in dataset.candidate_v4.function_passports["LEFT"][page]
    ]
    right_functions = [
        function for page in right_pages
        for function in dataset.candidate_v4.function_passports["RIGHT"][page]
    ]
    payload: dict[str, Any] = {
        "contract_version": CONTRACT_VERSION,
        "experiment_algorithm": ALGORITHM_VERSION,
        "project": selector.project,
        "pair_id": selector.pair_id,
        "run_id": selector.run_id,
        "direction": "LEFT_TO_RIGHT",
        "candidate_generator": CANDIDATE_GENERATOR_VERSION,
        "policy": {
            "primary_rule": "same engineering function, not same PDF page number",
            "priority": [
                "function", "served object or zone", "system position", "stable entities",
                "equipment or loads", "neighbor relations", "stamp", "contents",
                "visual structure", "PDF page number as weak signal only",
            ],
            "no_arbitrary_pages": True,
            "no_new_candidate_or_group_ids": True,
            "no_new_evidence": True,
            "no_new_values_or_entity_names": True,
            "no_model_composed_groups": True,
            "fail_closed": True,
        },
        "document_context": {
            "left_version_id": dataset.candidate_v4.sheet_passports["LEFT"][1].get("document_version_id"),
            "right_version_id": dataset.candidate_v4.sheet_passports["RIGHT"][1].get("document_version_id"),
            "left_page_count": selector.page_counts["LEFT"],
            "right_page_count": selector.page_counts["RIGHT"],
        },
        "contents_context": selector.contents_context,
        "sheet_passports": {
            "LEFT": [_sheet_view(dataset.candidate_v4.sheet_passports["LEFT"][page]) for page in left_pages],
            "RIGHT": [_sheet_view(dataset.candidate_v4.sheet_passports["RIGHT"][page]) for page in right_pages],
        },
        "function_passports": {
            "LEFT": [_function_view(value) for value in left_functions],
            "RIGHT": [_function_view(value) for value in right_functions],
        },
        "page_candidates": [
            _page_candidate_view(dataset, raw)
            for raw in sorted(
                dataset.page_candidates.values(),
                key=lambda value: (value["left_physical_page"], value["rank"], value["candidate_id"]),
            )
        ],
        "group_candidates": [
            _group_candidate_view(dataset, raw)
            for raw in sorted(
                dataset.group_candidates.values(),
                key=lambda value: (value["left_pages"], value["right_pages"], value["candidate_group_id"]),
            )
        ],
        "sentinel_options": [
            {"option_id": "NO_ANALOG", "decision_type": "NO_ANALOG"},
            {"option_id": "NEED_MORE_EVIDENCE", "decision_type": "NEED_MORE_EVIDENCE"},
        ],
        "tasks": selector.tasks,
        "required_reasoning_sequence": [
            "choose local_option_id independently for each LEFT function-passport-bearing task",
            "review the whole proposed document map for competition, legal merges, splits, distributed functions, and sheet/function distinction",
            "return map_option_id after that document-map review; use NEED_MORE_EVIDENCE for unresolved conflict",
        ],
    }
    payload["payload_signature"] = digest(payload)
    return payload


def build_selector_prompt(
    dataset: V4SelectorDataset,
    *,
    mode: str,
    image_manifest: Sequence[str] = (),
) -> tuple[str, dict[str, Any]]:
    if mode not in {"TEXT", "VISION_FALLBACK"}:
        raise ValueError("mode must be TEXT or VISION_FALLBACK")
    payload = build_selector_payload(dataset)
    vision_note = (
        "The attached renders are exactly image_manifest and cover only fallback LEFT tasks and their shortlisted RIGHT options."
        if mode == "VISION_FALLBACK"
        else "No images are available in this TEXT arm; use only the supplied text evidence."
    )
    prompt = "\n".join([
        "You are a bounded engineering-sheet selector in a read-only research experiment.",
        "Return only the JSON object required by the output schema.",
        "Never invent a page, candidate ID, group ID, evidence, value, sheet number, entity, or group.",
        "For every task, local_option_id and map_option_id must be one of that task's option_ids.",
        "First decide locally. Then review the full document map and set map_option_id after checking shared RIGHT competition and declared cardinality.",
        "A shared RIGHT is legal only through a declared MERGED_N_TO_1 or FUNCTION_DISTRIBUTED group; do not impose unconditional 1-to-1 assignment.",
        "NEW SHEET is not NEW FUNCTION and REMOVED SHEET is not REMOVED FUNCTION.",
        "Choose NEED_MORE_EVIDENCE whenever supplied evidence does not prove the same engineering function or a map conflict remains.",
        "NO_ANALOG is appropriate only when bounded candidates affirmatively prove no analogue; absence from top-10 alone is not proof.",
        "PDF page proximity is not a primary argument and is intentionally omitted from candidate evidence.",
        vision_note,
        "image_manifest=" + canonical_json(list(image_manifest)),
        "payload=" + canonical_json(payload),
    ])
    return prompt, payload


def verify_v4_selector_response(
    dataset: V4SelectorDataset,
    payload_signature: str,
    response: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Run the v1 verifier, then bind every option back to an actual v4 object."""
    result = verify_selector_response(dataset.selector, payload_signature, response)
    response_selections = response.get("selections") if isinstance(response, Mapping) else []
    by_task = {
        str(value.get("task_id")): value
        for value in response_selections or [] if isinstance(value, Mapping)
    }
    task_by_id = {str(task["task_id"]): task for task in dataset.selector.tasks}
    extra_global: list[str] = []
    for task_id, task_result in result["task_results"].items():
        errors = set(task_result["errors"])
        selection = by_task.get(task_id) or {}
        for field in ("local_option_id", "map_option_id"):
            option_id = str(selection.get(field) or "")
            if not option_id or option_id in SENTINEL_OPTION_IDS:
                continue
            option = dataset.selector.options.get(option_id)
            if option is None:
                continue
            source_kind = option.get("deterministic_evidence", {}).get("candidate_kind")
            if source_kind == "RIGHT_PAGE":
                raw = dataset.page_candidates.get(option_id)
                if raw is None or raw.get("candidate_id") != option_id:
                    errors.add("V4_PAGE_CANDIDATE_NOT_FOUND")
                elif int(raw["left_physical_page"]) != int(task_by_id[task_id]["left_page"]):
                    errors.add("V4_PAGE_CANDIDATE_LEFT_MISMATCH")
            elif source_kind == "RIGHT_PAGE_GROUP":
                raw = dataset.group_candidates.get(option_id)
                if raw is None or raw.get("candidate_group_id") != option_id:
                    errors.add("V4_GROUP_CANDIDATE_NOT_FOUND")
                elif raw.get("generator_version") != CANDIDATE_GENERATOR_VERSION:
                    errors.add("V4_GROUP_GENERATOR_MISMATCH")
            else:
                errors.add("UNKNOWN_V4_CANDIDATE_KIND")
            for evidence_id in option.get("evidence_refs") or []:
                evidence = dataset.selector.evidence_catalog.get(str(evidence_id))
                if not evidence:
                    errors.add("MISSING_EVIDENCE_REF")
                    continue
                page = evidence.get("physical_page")
                side = evidence.get("side")
                if side == "LEFT" and page not in option["left_pages"]:
                    errors.add("EVIDENCE_PAGE_MISMATCH")
                if side == "RIGHT" and page not in option["right_pages"]:
                    errors.add("EVIDENCE_PAGE_MISMATCH")
            if option.get("decision_type") not in CONCRETE_DECISIONS:
                errors.add("RELATION_TYPE_NOT_ALLOWED")
            if option.get("pair_id") != dataset.selector.pair_id:
                errors.add("CANDIDATE_PAIR_MISMATCH")
        task_result["errors"] = sorted(errors)
        task_result["ok"] = not task_result["errors"] and not result["global_errors"]
    result["ok"] = not result["global_errors"] and all(
        value["ok"] for value in result["task_results"].values()
    )
    result["candidate_generator_verified"] = CANDIDATE_GENERATOR_VERSION
    result["engineer_mapping_write_possible"] = False
    result["engineer_priority_gate"] = "DEFERRED_TO_UNCHANGED_AGGREGATION_MATERIALIZATION_GATE"
    result["model_supplied_evidence_possible"] = False
    if extra_global:
        result["global_errors"] = sorted(set(result["global_errors"]) | set(extra_global))
    return result


def build_group_audit(datasets: Sequence[V4SelectorDataset]) -> dict[str, Any]:
    projects = []
    all_cases: list[dict[str, Any]] = []
    for dataset in datasets:
        cases = []
        for raw in generator_group_audit(dataset.candidate_v4):
            group_id = raw.get("candidate_group_id")
            ranks = {
                str(page): next(
                    (row["shortlist_rank"] for row in dataset.group_shortlists.get(int(page), [])
                     if row["candidate_group_id"] == group_id),
                    None,
                )
                for page in raw["left_pages"]
            }
            present = bool(group_id) and all(value is not None for value in ranks.values())
            case = {
                **raw,
                "present_after_shortlist": present,
                "shortlist_ranks_by_left": ranks,
                "evaluation_only": True,
            }
            cases.append(case)
            all_cases.append(case)
        counts = [len(value) for value in dataset.group_shortlists.values()]
        projects.append({
            "project": dataset.selector.project,
            "pair_id": dataset.selector.pair_id,
            "generator_group_count": len(dataset.candidate_v4.group_candidates),
            "shortlist_limit_per_left": GROUP_SHORTLIST_LIMIT,
            "shortlist_count_min": min(counts) if counts else 0,
            "shortlist_count_max": max(counts) if counts else 0,
            "shortlist_count_median": statistics.median(counts) if counts else 0,
            "evaluation_case_count": len(cases),
            "evaluation_hits_after_shortlist": sum(value["present_after_shortlist"] for value in cases),
            "group_recall_after_shortlist": (
                round(sum(value["present_after_shortlist"] for value in cases) / len(cases), 6)
                if cases else None
            ),
            "cases": cases,
        })
    return {
        "kind": "candidate_v4_group_shortlist_audit",
        "schema_version": "group-audit.v1",
        "algorithm_version": ALGORITHM_VERSION,
        "candidate_generator": CANDIDATE_GENERATOR_VERSION,
        "shortlist_limit_per_left": GROUP_SHORTLIST_LIMIT,
        "ranking_priority": [
            "function coverage", "serviced object / zone", "system family",
            "complementary coverage", "entities", "topology", "neighboring context",
        ],
        "reference_used_for_shortlist": False,
        "reference_used_for_evaluation_only": True,
        "input_signatures": {
            dataset.selector.pair_id: dataset.selector.input_signature for dataset in datasets
        },
        "projects": projects,
        "summary": {
            "evaluation_case_count": len(all_cases),
            "evaluation_hits_after_shortlist": sum(value["present_after_shortlist"] for value in all_cases),
            "group_recall_after_shortlist": (
                round(sum(value["present_after_shortlist"] for value in all_cases) / len(all_cases), 6)
                if all_cases else None
            ),
        },
    }


def candidate_support_score(option: Mapping[str, Any]) -> float:
    evidence = option.get("deterministic_evidence") or {}
    value = evidence.get("ranking_score")
    if value is None:
        value = evidence.get("group_score")
    return float(value or 0.0)


def close_support_left_pages(dataset: V4SelectorDataset, *, absolute_gap: float = 0.05) -> set[int]:
    output: set[int] = set()
    for task in dataset.selector.tasks:
        scores = sorted((
            candidate_support_score(dataset.selector.options[option_id])
            for option_id in task["option_ids"] if option_id not in SENTINEL_OPTION_IDS
        ), reverse=True)
        if len(scores) >= 2 and scores[0] - scores[1] <= absolute_gap:
            output.add(int(task["left_page"]))
    return output


def output_schema(dataset: V4SelectorDataset, payload_signature: str) -> dict[str, Any]:
    return selector_schema(dataset.selector, payload_signature)
