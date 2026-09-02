"""Bounded, fail-closed primitives for the AI Sheet Matcher research spike.

The experiment deliberately reads frozen production artifacts but never writes
to a pair directory.  Production Sheet Matcher v3 remains the only candidate
generator: the experiment merely gives stable IDs to its top-10 pages and to
bounded groups composed from those pages.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


ALGORITHM_VERSION = "ai-sheet-matcher-research.v1"
PRODUCTION_ALGORITHM = "production-sheet-matcher.v3"
SESSION_ID = "7cccec69bb0b4327"
DECISION_TYPES = frozenset({
    "MATCH_1_TO_1",
    "SPLIT_1_TO_N",
    "MERGED_N_TO_1",
    "FUNCTION_DISTRIBUTED",
    "NO_ANALOG",
    "NEED_MORE_EVIDENCE",
})
CONCRETE_DECISIONS = DECISION_TYPES - {"NO_ANALOG", "NEED_MORE_EVIDENCE"}
SENTINEL_OPTION_IDS = ("NO_ANALOG", "NEED_MORE_EVIDENCE")

PROJECT_CONFIG: dict[str, dict[str, Any]] = {
    "p19cd7f695a": {
        "project": "ИОС 1.1",
        "run_id": "prun_494b90e814257a693d77f627",
        "baseline": {"HIGH": 1, "POSSIBLE": 11, "UNKNOWN": 70},
        "focus_left_pages": [*range(24, 32), *range(37, 53)],
        "reference_cases": [
            {
                "left_pages": [31], "right_pages": [29], "expected_mode": "ALL",
                "name": "ВРУ автостоянки ↔ ВРУ-А",
            },
            {
                "left_pages": [27, 28], "right_pages": [27], "expected_mode": "ALL",
                "name": "ВРУ-3: несколько старых листов ↔ один новый",
            },
            {
                "left_pages": [29, 30], "right_pages": [28], "expected_mode": "ALL",
                "name": "ВРУ-4: несколько старых листов ↔ один новый",
            },
            {
                "left_pages": [52], "right_pages": [21, 22, 23], "expected_mode": "ALL",
                "name": "ГРЩ: один старый лист ↔ несколько новых",
            },
        ],
    },
    "pb02de74a81": {
        "project": "ИОС 3.1",
        "run_id": "prun_f69ec835d1e06081e63a68cd",
        "baseline": {"HIGH": 0, "POSSIBLE": 5, "UNKNOWN": 44},
        "focus_left_pages": [11, 12, 13, 14, 15],
        "reference_cases": [
            {
                "left_pages": [11], "right_pages": [14], "expected_mode": "ALL",
                "name": "Внутренние системы водоотведения — корпус 1",
            },
            {
                "left_pages": [12], "right_pages": [15], "expected_mode": "ALL",
                "name": "Внутренние системы водоотведения — корпус 2",
            },
            {
                "left_pages": [13], "right_pages": [16], "expected_mode": "ALL",
                "name": "Внутренние системы водоотведения — корпус 3",
            },
            {
                "left_pages": [14], "right_pages": [17], "expected_mode": "ALL",
                "name": "Внутренние системы водоотведения — корпус 4",
            },
            {
                "left_pages": [15], "right_pages": [13], "expected_mode": "ALL",
                "name": "Водоотведение со стилобата",
            },
        ],
    },
    "pe336037597": {
        "project": "ИОС 2.1",
        "run_id": "prun_68a933afd5b2bb083189d3c6",
        "baseline": {"HIGH": 0, "POSSIBLE": 7, "UNKNOWN": 94},
        "focus_left_pages": [16, 17, 18, 19, 20, 21, 51],
        # These are explicitly hypotheses, not ground truth.  ANY means that
        # the case asks whether at least one named alternative was retrieved.
        "reference_cases": [
            {
                "left_pages": [16], "right_pages": [26, 28], "expected_mode": "ANY",
                "name": "Корпус 1 — возможное распределение функции",
            },
            {
                "left_pages": [17], "right_pages": [27], "expected_mode": "ALL",
                "name": "Корпус 2 — функциональная гипотеза",
            },
            {
                "left_pages": [18], "right_pages": [24], "expected_mode": "ALL",
                "name": "Корпус 3 — функциональная гипотеза",
            },
            {
                "left_pages": [19], "right_pages": [25, 30], "expected_mode": "ANY",
                "name": "Корпус 4 — конкурирующие варианты",
            },
            {
                "left_pages": [20], "right_pages": [29, 30], "expected_mode": "ANY",
                "name": "LEFT 20 — конкуренция RIGHT 29/30",
            },
            {
                "left_pages": [21], "right_pages": [29, 30], "expected_mode": "ANY",
                "name": "LEFT 21 — конкуренция RIGHT 29/30",
            },
            {
                "left_pages": [51], "right_pages": [29, 63], "expected_mode": "ANY",
                "name": "LEFT 51 — конкуренция RIGHT 29/63",
            },
        ],
    },
}

_PAGE_RE = re.compile(r"^## Page (\d+)\s*$", re.MULTILINE)
_STAMP_RE = re.compile(r"^> \*\*Stamp:\*\* (.+)$", re.MULTILINE)
_BOLD_FIELD_RE = re.compile(r"^\*\*([^*:\n]+):\*\*\s*(.+)$", re.MULTILINE)
_HEADING_RE = re.compile(r"^#{1,6}\s+(.+)$", re.MULTILINE)
_IMAGE_META_RE = re.compile(r"^\*\*\[IMAGE\]\*\*\s*\|\s*(.+)$", re.MULTILINE)


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def digest(value: Any) -> str:
    raw = value if isinstance(value, bytes) else canonical_json(value).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def stable_id(prefix: str, *parts: Any) -> str:
    return prefix + digest(parts)[:20]


def read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"expected object in {path}")
    return value


def split_markdown_pages(text: str) -> dict[int, str]:
    matches = list(_PAGE_RE.finditer(text))
    pages: dict[int, str] = {}
    for index, match in enumerate(matches):
        end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        pages[int(match.group(1))] = text[match.end():end].strip()
    return pages


def _unique(values: Iterable[str], *, limit: int = 80) -> list[str]:
    output: list[str] = []
    seen: set[str] = set()
    for raw in values:
        value = " ".join(str(raw or "").split())
        key = value.casefold()
        if value and key not in seen:
            seen.add(key)
            output.append(value)
        if len(output) >= limit:
            break
    return output


def _parse_pipe_fields(value: str) -> dict[str, str]:
    fields: dict[str, str] = {}
    for part in value.split("|"):
        key, separator, content = part.partition(":")
        if separator and key.strip():
            fields[key.strip()] = " ".join(content.split())
    return fields


def _page_excerpt(body: str, *, limit: int = 6500) -> tuple[str, bool, int]:
    lines = []
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith(("> **Created:**", "> **Crop:**", "### BLOCK")):
            continue
        lines.append(line.rstrip())
    value = "\n".join(lines).strip()
    if len(value) <= limit:
        return value, False, len(value)
    head = value[: limit * 3 // 4]
    tail = value[-limit // 4 :]
    return head + "\n[… middle omitted by bounded passport …]\n" + tail, True, len(value)


def build_passport(
    *,
    pair_id: str,
    side: str,
    page: int,
    body: str,
    page_count: int,
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Build a passport only from literal fields already present in markdown."""
    stamp_match = _STAMP_RE.search(body)
    stamp_text = stamp_match.group(1).strip() if stamp_match else ""
    stamp = _parse_pipe_fields(stamp_text)

    fields: dict[str, list[str]] = {}
    for match in _BOLD_FIELD_RE.finditer(body):
        fields.setdefault(match.group(1).strip(), []).append(match.group(2).strip())
    for match in _IMAGE_META_RE.finditer(body):
        for key, value in _parse_pipe_fields(match.group(1)).items():
            fields.setdefault(key, []).append(value)

    excerpt, truncated, source_length = _page_excerpt(body)
    headings = _unique([
        *(_HEADING_RE.findall(body)),
        *(value for value in re.findall(r"^\*\*([^*\n]{3,160})\*\*$", body, re.MULTILINE)),
    ], limit=24)
    evidence_prefix = f"ev_{pair_id}_{side}_{page}"
    evidence = {
        f"{evidence_prefix}_text": {
            "evidence_id": f"{evidence_prefix}_text",
            "side": side,
            "page": page,
            "kind": "page_text_excerpt",
            "source": f"pair.{side.lower()}.document.md#page={page}",
            "content_sha256": hashlib.sha256(body.encode("utf-8")).hexdigest(),
        },
        f"{evidence_prefix}_stamp": {
            "evidence_id": f"{evidence_prefix}_stamp",
            "side": side,
            "page": page,
            "kind": "stamp_text",
            "source": f"pair.{side.lower()}.document.md#page={page}",
            "content_sha256": hashlib.sha256(stamp_text.encode("utf-8")).hexdigest(),
        },
    }
    field_refs: dict[str, list[str]] = {}
    for key, values in fields.items():
        ref = f"{evidence_prefix}_{re.sub(r'[^a-z0-9]+', '_', key.casefold())[:30]}"
        evidence[ref] = {
            "evidence_id": ref,
            "side": side,
            "page": page,
            "kind": "literal_markdown_field",
            "field": key,
            "source": f"pair.{side.lower()}.document.md#page={page}",
            "content_sha256": digest(values),
        }
        field_refs.setdefault(key, []).append(ref)

    def values(*names: str) -> list[str]:
        return _unique(value for name in names for value in fields.get(name, []))

    passport = {
        "side": side,
        "pdf_page": page,
        "sheet_number": stamp.get("Sheet") or None,
        "sheet_title": stamp.get("Name") or None,
        "sheet_types": values("Type"),
        "large_headings": headings,
        "systems": values("System", "Systems"),
        "served_object_or_zone": values("Zone", "Object"),
        "floor_or_level": values("Level"),
        "characteristic_rooms": values("Rooms"),
        "equipment": values("Equipment"),
        "entities": values("Entities"),
        "incoming_connections": values("Incoming", "Incoming connections"),
        "outgoing_connections": values("Outgoing", "Outgoing connections"),
        "functional_signals": _unique([
            *(values("Summary", "Purpose", "Function")),
            *([stamp.get("Name", "")] if stamp.get("Name") else []),
        ], limit=12),
        "topology": values("Topology"),
        "stamp_text": stamp_text or None,
        "page_text_excerpt": excerpt,
        "page_text_source_length": source_length,
        "page_text_truncated": truncated,
        "neighbor_pages": [candidate for candidate in (page - 1, page + 1) if 1 <= candidate <= page_count],
        "evidence_refs": sorted(evidence),
        "field_evidence_refs": field_refs,
    }
    return passport, evidence


def _pair_dir(repo_root: Path, pair_id: str) -> Path:
    return repo_root / "comparison" / "sessions" / SESSION_ID / "pairs" / pair_id


def _candidate_rows(payload: Mapping[str, Any]) -> dict[int, dict[str, Any]]:
    return {
        int(item["left_page"]): dict(item)
        for item in payload.get("candidate_search") or []
    }


def _relation_type(left_pages: Sequence[int], right_pages: Sequence[int]) -> str:
    if len(left_pages) == 1 and len(right_pages) == 1:
        return "MATCH_1_TO_1"
    if len(left_pages) == 1 and len(right_pages) > 1:
        return "SPLIT_1_TO_N"
    if len(left_pages) > 1 and len(right_pages) == 1:
        return "MERGED_N_TO_1"
    return "FUNCTION_DISTRIBUTED"


@dataclass(frozen=True)
class ProjectDataset:
    pair_id: str
    project: str
    run_id: str
    pair_dir: Path
    pair: dict[str, Any]
    baseline: dict[str, int]
    page_counts: dict[str, int]
    passports: dict[str, dict[int, dict[str, Any]]]
    evidence_catalog: dict[str, dict[str, Any]]
    frozen_top5: dict[int, list[dict[str, Any]]]
    top10: dict[int, list[dict[str, Any]]]
    deep_top10: dict[int, list[dict[str, Any]]]
    human_links: list[dict[str, Any]]
    human_by_left: dict[int, dict[str, Any]]
    reference_cases: list[dict[str, Any]]
    tasks: list[dict[str, Any]]
    options: dict[str, dict[str, Any]]
    contents_context: dict[str, list[dict[str, Any]]]
    source_hashes: dict[str, str]
    input_signature: str


def _hash_sources(pair_dir: Path) -> dict[str, str]:
    paths = [
        pair_dir / "pair.json",
        pair_dir / "sheet_links.json",
        pair_dir / "production" / "state.json",
        pair_dir / "production" / "sheet_relations.json",
    ]
    return {
        str(path.relative_to(pair_dir)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in paths
    }


def _toc_context(passports: Mapping[int, Mapping[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for page, passport in passports.items():
        haystack = " ".join([
            str(passport.get("sheet_title") or ""),
            " ".join(passport.get("large_headings") or []),
            str(passport.get("page_text_excerpt") or "")[:2000],
        ]).casefold()
        if "содержан" not in haystack:
            continue
        output.append({
            "pdf_page": page,
            "sheet_title": passport.get("sheet_title"),
            "text_excerpt": str(passport.get("page_text_excerpt") or "")[:5000],
            "evidence_refs": passport.get("evidence_refs") or [],
        })
    return output[:3]


def _option(
    *,
    pair_id: str,
    decision_type: str,
    left_pages: Sequence[int],
    right_pages: Sequence[int],
    evidence_refs: Iterable[str],
    deterministic_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    identity = {
        "pair_id": pair_id,
        "decision_type": decision_type,
        "left_pages": sorted(set(int(page) for page in left_pages)),
        "right_pages": sorted(set(int(page) for page in right_pages)),
    }
    return {
        "option_id": stable_id("cand_", identity),
        **identity,
        "evidence_refs": sorted(set(evidence_refs)),
        "deterministic_evidence": dict(deterministic_evidence),
    }


def _add_option(
    option: dict[str, Any],
    *,
    options: dict[str, dict[str, Any]],
    option_ids_by_left: dict[int, set[str]],
) -> None:
    option_id = str(option["option_id"])
    options.setdefault(option_id, option)
    for left_page in option["left_pages"]:
        if left_page in option_ids_by_left:
            option_ids_by_left[left_page].add(option_id)


def _build_options(
    *,
    pair_id: str,
    focus_left_pages: Sequence[int],
    passports: Mapping[str, Mapping[int, Mapping[str, Any]]],
    top10: Mapping[int, Sequence[Mapping[str, Any]]],
    deep_top10: Mapping[int, Sequence[Mapping[str, Any]]],
    evidence_catalog: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    options: dict[str, dict[str, Any]] = {}
    option_ids_by_left = {int(page): set(SENTINEL_OPTION_IDS) for page in focus_left_pages}
    deep_by_pair = {
        (left_page, int(item["right_page"])): item
        for left_page, rows in deep_top10.items()
        for item in rows
    }
    ranks = {
        left_page: {int(item["right_page"]): rank for rank, item in enumerate(rows, 1)}
        for left_page, rows in top10.items()
    }

    for left_page in focus_left_pages:
        for candidate in top10[left_page]:
            right_page = int(candidate["right_page"])
            det_ref = f"ev_{pair_id}_production_{left_page}_{right_page}"
            evidence_catalog[det_ref] = {
                "evidence_id": det_ref,
                "kind": "production_sheet_matcher_candidate",
                "left_page": left_page,
                "right_page": right_page,
                "source": "production/sheet_relations.json#candidate_search",
                "signals": candidate.get("signals") or {},
                "deep": deep_by_pair.get((left_page, right_page), {}),
            }
            refs = [
                f"ev_{pair_id}_LEFT_{left_page}_text",
                f"ev_{pair_id}_RIGHT_{right_page}_text",
                det_ref,
            ]
            option = _option(
                pair_id=pair_id,
                decision_type="MATCH_1_TO_1",
                left_pages=[left_page],
                right_pages=[right_page],
                evidence_refs=refs,
                deterministic_evidence={
                    "production_rank": ranks[left_page][right_page],
                    "production_score": candidate.get("score"),
                    "production_signals": candidate.get("signals") or {},
                },
            )
            _add_option(option, options=options, option_ids_by_left=option_ids_by_left)

        candidate_pages = sorted(ranks[left_page])
        windows: list[tuple[int, tuple[int, ...]]] = []
        for size in (2, 3):
            for start in range(len(candidate_pages) - size + 1):
                group = tuple(candidate_pages[start:start + size])
                if group[-1] - group[0] != size - 1:
                    continue
                windows.append((sum(ranks[left_page][page] for page in group), group))
        for _rank_sum, group in sorted(windows)[:4]:
            refs = [f"ev_{pair_id}_LEFT_{left_page}_text"]
            refs.extend(f"ev_{pair_id}_RIGHT_{page}_text" for page in group)
            refs.extend(f"ev_{pair_id}_production_{left_page}_{page}" for page in group)
            option = _option(
                pair_id=pair_id,
                decision_type="SPLIT_1_TO_N",
                left_pages=[left_page],
                right_pages=group,
                evidence_refs=refs,
                deterministic_evidence={
                    "construction": "contiguous_right_pages_from_production_top10",
                    "production_ranks": {str(page): ranks[left_page][page] for page in group},
                },
            )
            _add_option(option, options=options, option_ids_by_left=option_ids_by_left)

    focus = sorted(set(int(page) for page in focus_left_pages))
    for left_a, left_b in zip(focus, focus[1:]):
        if left_b != left_a + 1:
            continue
        common_right = sorted(
            set(ranks[left_a]) & set(ranks[left_b]),
            key=lambda page: (ranks[left_a][page] + ranks[left_b][page], page),
        )
        for right_page in common_right[:2]:
            refs = [
                f"ev_{pair_id}_LEFT_{left_a}_text",
                f"ev_{pair_id}_LEFT_{left_b}_text",
                f"ev_{pair_id}_RIGHT_{right_page}_text",
                f"ev_{pair_id}_production_{left_a}_{right_page}",
                f"ev_{pair_id}_production_{left_b}_{right_page}",
            ]
            option = _option(
                pair_id=pair_id,
                decision_type="MERGED_N_TO_1",
                left_pages=[left_a, left_b],
                right_pages=[right_page],
                evidence_refs=refs,
                deterministic_evidence={
                    "construction": "shared_right_page_in_both_production_top10_sets",
                    "production_ranks": {
                        str(left_a): ranks[left_a][right_page],
                        str(left_b): ranks[left_b][right_page],
                    },
                },
            )
            _add_option(option, options=options, option_ids_by_left=option_ids_by_left)

        union_right = sorted(set(ranks[left_a]) | set(ranks[left_b]))
        distributed: list[tuple[int, tuple[int, int]]] = []
        for right_a, right_b in zip(union_right, union_right[1:]):
            if right_b != right_a + 1:
                continue
            group = (right_a, right_b)
            if not any(page in ranks[left_a] for page in group):
                continue
            if not any(page in ranks[left_b] for page in group):
                continue
            score = sum(
                min(ranks[left].get(page, 99) for page in group)
                for left in (left_a, left_b)
            )
            distributed.append((score, group))
        for _score, group in sorted(distributed)[:2]:
            refs = [
                f"ev_{pair_id}_LEFT_{left_a}_text",
                f"ev_{pair_id}_LEFT_{left_b}_text",
                *(f"ev_{pair_id}_RIGHT_{page}_text" for page in group),
            ]
            for left in (left_a, left_b):
                refs.extend(
                    f"ev_{pair_id}_production_{left}_{page}"
                    for page in group if page in ranks[left]
                )
            option = _option(
                pair_id=pair_id,
                decision_type="FUNCTION_DISTRIBUTED",
                left_pages=[left_a, left_b],
                right_pages=group,
                evidence_refs=refs,
                deterministic_evidence={
                    "construction": "adjacent_left_and_right_pages_from_production_top10_union",
                },
            )
            _add_option(option, options=options, option_ids_by_left=option_ids_by_left)

    tasks = []
    for left_page in focus:
        option_ids = sorted(
            option_ids_by_left[left_page],
            key=lambda item: (
                1 if item in SENTINEL_OPTION_IDS else 0,
                options.get(item, {}).get("decision_type", item),
                options.get(item, {}).get("right_pages", []),
                item,
            ),
        )
        tasks.append({
            "task_id": stable_id("task_", pair_id, left_page),
            "left_page": left_page,
            "option_ids": option_ids,
        })
    return tasks, options


def build_project_dataset(repo_root: Path, pair_id: str) -> ProjectDataset:
    if pair_id not in PROJECT_CONFIG:
        raise ValueError(f"unknown benchmark pair: {pair_id}")
    config = PROJECT_CONFIG[pair_id]
    pair_dir = _pair_dir(repo_root, pair_id)
    pair = read_json(pair_dir / "pair.json")
    frozen = read_json(pair_dir / "production" / "sheet_relations.json")
    if frozen.get("algorithm_version") != PRODUCTION_ALGORITHM:
        raise ValueError("frozen production matcher version changed")

    # This call is read-only and is the exact production candidate generator.
    # top_k=10 is an experimental retrieval setting; no production threshold or
    # artifact is changed.
    from backend.app.services.stage_comparison.production_orchestrator import (  # noqa: PLC0415
        _production_sheet_indexes,
    )
    from backend.app.services.stage_comparison.sheet_matcher import match_sheets  # noqa: PLC0415

    indexes = _production_sheet_indexes(pair)
    regenerated = match_sheets(
        indexes["left"], indexes["right"], top_k=10,
        generated_at=frozen.get("generated_at") or "frozen",
    )
    frozen_rows = _candidate_rows(frozen)
    regenerated_rows = _candidate_rows(regenerated)
    frozen_top5 = {
        page: [dict(item) for item in row.get("top_candidates") or []]
        for page, row in frozen_rows.items()
    }
    top10 = {
        page: [dict(item) for item in row.get("top_candidates") or []]
        for page, row in regenerated_rows.items()
    }
    deep_top10 = {
        page: [dict(item) for item in row.get("deep_candidates") or []]
        for page, row in regenerated_rows.items()
    }

    passports: dict[str, dict[int, dict[str, Any]]] = {"LEFT": {}, "RIGHT": {}}
    evidence_catalog: dict[str, dict[str, Any]] = {}
    page_counts: dict[str, int] = {}
    for side_key, pair_key in (("LEFT", "left"), ("RIGHT", "right")):
        document = pair[pair_key]
        markdown_path = Path(str(document["md_path"]))
        page_sections = split_markdown_pages(markdown_path.read_text(encoding="utf-8"))
        page_counts[side_key] = len(indexes[pair_key])
        for page in range(1, page_counts[side_key] + 1):
            passport, evidence = build_passport(
                pair_id=pair_id,
                side=side_key,
                page=page,
                body=page_sections.get(page, ""),
                page_count=page_counts[side_key],
            )
            passports[side_key][page] = passport
            evidence_catalog.update(evidence)

    links_payload = read_json(pair_dir / "sheet_links.json")
    human_links = [dict(item) for item in links_payload.get("links") or []]
    human_by_left: dict[int, dict[str, Any]] = {}
    for link in human_links:
        normalized = {
            "link_id": link.get("id"),
            "left_pages": sorted(int(page) for page in link.get("left_pages") or []),
            "right_pages": sorted(int(page) for page in link.get("right_pages") or []),
            "decision_type": _relation_type(
                link.get("left_pages") or [], link.get("right_pages") or [],
            ),
            "source": link.get("source"),
            "reason": list(link.get("reason") or []),
        }
        for left_page in normalized["left_pages"]:
            if left_page in human_by_left:
                raise ValueError(f"overlapping saved human links at LEFT {left_page}")
            human_by_left[left_page] = normalized

    tasks, options = _build_options(
        pair_id=pair_id,
        focus_left_pages=config["focus_left_pages"],
        passports=passports,
        top10=top10,
        deep_top10=deep_top10,
        evidence_catalog=evidence_catalog,
    )
    source_hashes = _hash_sources(pair_dir)
    signature_payload = {
        "algorithm": ALGORITHM_VERSION,
        "pair_id": pair_id,
        "production_input_signature": frozen.get("input_signature"),
        "top10_input_signature": regenerated.get("input_signature"),
        "source_hashes": source_hashes,
        "tasks": tasks,
        "options": options,
    }
    return ProjectDataset(
        pair_id=pair_id,
        project=config["project"],
        run_id=config["run_id"],
        pair_dir=pair_dir,
        pair=pair,
        baseline=dict(config["baseline"]),
        page_counts=page_counts,
        passports=passports,
        evidence_catalog=evidence_catalog,
        frozen_top5=frozen_top5,
        top10=top10,
        deep_top10=deep_top10,
        human_links=human_links,
        human_by_left=human_by_left,
        reference_cases=[dict(item) for item in config["reference_cases"]],
        tasks=tasks,
        options=options,
        contents_context={
            "LEFT": _toc_context(passports["LEFT"]),
            "RIGHT": _toc_context(passports["RIGHT"]),
        },
        source_hashes=source_hashes,
        input_signature=digest(signature_payload),
    )


def _candidate_view(
    dataset: ProjectDataset,
    left_page: int,
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    output = []
    for rank, row in enumerate(rows, 1):
        right_page = int(row["right_page"])
        passport = dataset.passports["RIGHT"][right_page]
        output.append({
            "rank": rank,
            "right_page": right_page,
            "sheet_number": passport.get("sheet_number"),
            "sheet_title": passport.get("sheet_title"),
            "sheet_types": passport.get("sheet_types") or [],
            "served_object_or_zone": passport.get("served_object_or_zone") or [],
            "score": row.get("score"),
            "signals": row.get("signals") or {},
        })
    return output


def _production_relation_is_exact(
    relations: Sequence[Mapping[str, Any]],
    left_pages: Sequence[int],
    right_pages: Sequence[int],
) -> tuple[bool, str | None]:
    expected_left = sorted(int(page) for page in left_pages)
    expected_right = sorted(int(page) for page in right_pages)
    for relation in relations:
        if (
            sorted(int(page) for page in relation.get("left_pages") or []) == expected_left
            and sorted(int(page) for page in relation.get("right_pages") or []) == expected_right
        ):
            return True, str(relation.get("status") or "") or None
    return False, None


def build_candidate_recall(datasets: Sequence[ProjectDataset]) -> dict[str, Any]:
    projects: list[dict[str, Any]] = []
    aggregate_cases: list[dict[str, Any]] = []
    for dataset in datasets:
        frozen = read_json(dataset.pair_dir / "production" / "sheet_relations.json")
        relation_rows = list(frozen.get("relations") or [])
        cases: list[dict[str, Any]] = []
        by_identity: dict[tuple[tuple[int, ...], tuple[int, ...]], dict[str, Any]] = {}

        for link in dataset.human_links:
            left_pages = tuple(sorted(int(page) for page in link.get("left_pages") or []))
            right_pages = tuple(sorted(int(page) for page in link.get("right_pages") or []))
            for left_page in left_pages:
                key = ((left_page,), right_pages)
                case = {
                    "case_id": stable_id("recall_", dataset.pair_id, "human", left_page, right_pages),
                    "source_types": ["human_confirmed"],
                    "source_ref": link.get("id"),
                    "name": "saved engineer-accepted sheet link",
                    "left_pages": list(left_pages),
                    "audit_left_page": left_page,
                    "expected_right_pages": list(right_pages),
                    "expected_mode": "ALL",
                    "reference_is_ground_truth": True,
                }
                by_identity[key] = case
                cases.append(case)

        for reference in dataset.reference_cases:
            left_pages = tuple(sorted(int(page) for page in reference["left_pages"]))
            right_pages = tuple(sorted(int(page) for page in reference["right_pages"]))
            for left_page in left_pages:
                key = ((left_page,), right_pages)
                existing = by_identity.get(key)
                if existing is not None:
                    existing["source_types"].append("reference_hypothesis")
                    existing["name"] = reference["name"]
                    continue
                case = {
                    "case_id": stable_id("recall_", dataset.pair_id, "reference", left_page, right_pages),
                    "source_types": ["reference_hypothesis"],
                    "source_ref": None,
                    "name": reference["name"],
                    "left_pages": list(left_pages),
                    "audit_left_page": left_page,
                    "expected_right_pages": list(right_pages),
                    "expected_mode": reference.get("expected_mode", "ALL"),
                    "reference_is_ground_truth": False,
                }
                by_identity[key] = case
                cases.append(case)

        for case in cases:
            left_page = int(case["audit_left_page"])
            expected = set(int(page) for page in case["expected_right_pages"])
            top5_pages = [int(item["right_page"]) for item in dataset.frozen_top5[left_page]]
            top10_pages = [int(item["right_page"]) for item in dataset.top10[left_page]]
            match = expected.issubset if case["expected_mode"] == "ALL" else lambda values: bool(expected & set(values))
            hit5 = bool(match(set(top5_pages)))
            hit10 = bool(match(set(top10_pages)))
            exact, status = _production_relation_is_exact(
                relation_rows, case["left_pages"], case["expected_right_pages"],
            )
            if not hit10:
                problem = "CANDIDATE_GENERATION_PROBLEM"
            elif case["reference_is_ground_truth"] and not (exact and status == "HIGH"):
                problem = "CANDIDATE_SELECTION_PROBLEM"
            elif case["reference_is_ground_truth"]:
                problem = "ALREADY_HIGH"
            else:
                problem = "REFERENCE_AUDIT_ONLY"
            case.update({
                "left_passport_summary": {
                    key: dataset.passports["LEFT"][left_page].get(key)
                    for key in ("sheet_number", "sheet_title", "sheet_types", "served_object_or_zone")
                },
                "current_top5_candidates": _candidate_view(
                    dataset, left_page, dataset.frozen_top5[left_page],
                ),
                "regenerated_top10_candidates": _candidate_view(
                    dataset, left_page, dataset.top10[left_page],
                ),
                "expected_ranks": {
                    str(right): (top10_pages.index(right) + 1 if right in top10_pages else None)
                    for right in sorted(expected)
                },
                "recall_at_5": hit5,
                "recall_at_10": hit10,
                "production_exact_relation": exact,
                "production_exact_status": status,
                "problem_class": problem,
            })

        def summary(selected: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
            total = len(selected)
            return {
                "case_count": total,
                "candidate_recall_at_5": round(sum(bool(item["recall_at_5"]) for item in selected) / total, 6) if total else None,
                "candidate_recall_at_10": round(sum(bool(item["recall_at_10"]) for item in selected) / total, 6) if total else None,
                "candidate_generation_problem_count": sum(item["problem_class"] == "CANDIDATE_GENERATION_PROBLEM" for item in selected),
                "candidate_selection_problem_count": sum(item["problem_class"] == "CANDIDATE_SELECTION_PROBLEM" for item in selected),
            }

        human_cases = [item for item in cases if "human_confirmed" in item["source_types"]]
        reference_cases = [item for item in cases if "reference_hypothesis" in item["source_types"]]
        project = {
            "project": dataset.project,
            "pair_id": dataset.pair_id,
            "run_id": dataset.run_id,
            "production_algorithm": PRODUCTION_ALGORITHM,
            "human_confirmation_relation_count": len(dataset.human_links),
            "human_confirmation_left_case_count": len(human_cases),
            "summary_all_unique_cases": summary(cases),
            "summary_human_confirmed": summary(human_cases),
            "summary_reference_hypotheses": summary(reference_cases),
            "cases": cases,
        }
        projects.append(project)
        aggregate_cases.extend(cases)

    total = len(aggregate_cases)
    return {
        "kind": "ai_sheet_matcher_candidate_recall",
        "schema_version": "candidate-recall.v1",
        "algorithm_version": ALGORITHM_VERSION,
        "candidate_generator": PRODUCTION_ALGORITHM,
        "candidate_generator_mutated": False,
        "top5_source": "frozen production artifact",
        "top10_source": "same production matcher regenerated read-only with top_k=10",
        "input_signature": digest([dataset.input_signature for dataset in datasets]),
        "summary": {
            "case_count": total,
            "candidate_recall_at_5": round(sum(bool(item["recall_at_5"]) for item in aggregate_cases) / total, 6),
            "candidate_recall_at_10": round(sum(bool(item["recall_at_10"]) for item in aggregate_cases) / total, 6),
            "candidate_generation_problem_count": sum(item["problem_class"] == "CANDIDATE_GENERATION_PROBLEM" for item in aggregate_cases),
            "candidate_selection_problem_count": sum(item["problem_class"] == "CANDIDATE_SELECTION_PROBLEM" for item in aggregate_cases),
        },
        "projects": projects,
    }


def _passport_prompt_view(
    dataset: ProjectDataset,
    side: str,
    page: int,
) -> dict[str, Any]:
    passport = dict(dataset.passports[side][page])
    neighbors = []
    for neighbor_page in passport.pop("neighbor_pages", []):
        neighbor = dataset.passports[side][neighbor_page]
        neighbors.append({
            "pdf_page": neighbor_page,
            "sheet_number": neighbor.get("sheet_number"),
            "sheet_title": neighbor.get("sheet_title"),
            "functional_signals": neighbor.get("functional_signals") or [],
            "served_object_or_zone": neighbor.get("served_object_or_zone") or [],
            "text_excerpt": str(neighbor.get("page_text_excerpt") or "")[:800],
            "evidence_refs": neighbor.get("evidence_refs") or [],
        })
    passport["neighbors"] = neighbors
    return passport


def build_selector_payload(dataset: ProjectDataset) -> dict[str, Any]:
    left_pages = sorted({int(task["left_page"]) for task in dataset.tasks})
    right_pages = sorted({
        int(page)
        for task in dataset.tasks
        for option_id in task["option_ids"]
        if option_id not in SENTINEL_OPTION_IDS
        for page in dataset.options[option_id]["right_pages"]
    })
    option_ids = sorted({
        option_id
        for task in dataset.tasks
        for option_id in task["option_ids"]
        if option_id not in SENTINEL_OPTION_IDS
    })
    payload = {
        "contract_version": "bounded-sheet-selector.v1",
        "project": dataset.project,
        "pair_id": dataset.pair_id,
        "run_id": dataset.run_id,
        "direction": "LEFT_TO_RIGHT",
        "candidate_generator": PRODUCTION_ALGORITHM,
        "policy": {
            "primary_rule": "same engineering function, not same PDF page number",
            "priority": [
                "function", "served object or zone", "system position", "stable entities",
                "equipment or loads", "neighbor relations", "stamp", "contents",
                "visual structure", "PDF page number as weak signal only",
            ],
            "no_arbitrary_pages": True,
            "no_new_evidence": True,
            "no_new_values_or_entity_names": True,
            "fail_closed": True,
        },
        "contents_context": dataset.contents_context,
        "sheet_passports": {
            "LEFT": [_passport_prompt_view(dataset, "LEFT", page) for page in left_pages],
            "RIGHT": [_passport_prompt_view(dataset, "RIGHT", page) for page in right_pages],
        },
        # The verifier retains full evidence refs.  The selector needs only the
        # immutable choice geometry and compact production signals; repeating
        # hundreds of long evidence-ref lists would crowd out page content.
        "options": [
            {
                "option_id": dataset.options[option_id]["option_id"],
                "decision_type": dataset.options[option_id]["decision_type"],
                "left_pages": dataset.options[option_id]["left_pages"],
                "right_pages": dataset.options[option_id]["right_pages"],
                "deterministic_evidence": dataset.options[option_id]["deterministic_evidence"],
            }
            for option_id in option_ids
        ],
        "sentinel_options": [
            {"option_id": "NO_ANALOG", "decision_type": "NO_ANALOG"},
            {"option_id": "NEED_MORE_EVIDENCE", "decision_type": "NEED_MORE_EVIDENCE"},
        ],
        "tasks": dataset.tasks,
        "required_reasoning_sequence": [
            "choose local_option_id independently for each LEFT task",
            "review the whole proposed map for conflicts, splits, merges, sequences, moved and distributed functions",
            "return map_option_id after that document-map review",
        ],
    }
    payload["payload_signature"] = digest(payload)
    return payload


def build_selector_prompt(dataset: ProjectDataset, *, mode: str) -> tuple[str, dict[str, Any]]:
    if mode not in {"TEXT", "VISION_TEXT"}:
        raise ValueError("mode must be TEXT or VISION_TEXT")
    payload = build_selector_payload(dataset)
    vision_note = (
        "The attached images are ordered exactly as image_manifest; use them only as additional evidence."
        if mode == "VISION_TEXT"
        else "No images are available in this arm; use only the supplied text evidence."
    )
    left_pages = [int(task["left_page"]) for task in dataset.tasks]
    right_pages = sorted({
        int(page)
        for option in dataset.options.values()
        for page in option["right_pages"]
        if any(int(task["left_page"]) in option["left_pages"] for task in dataset.tasks)
    })
    image_manifest = [
        *(f"LEFT_page_{page:03d}.jpg" for page in left_pages),
        *(f"RIGHT_page_{page:03d}.jpg" for page in right_pages),
    ] if mode == "VISION_TEXT" else []
    prompt = "\n".join([
        "You are a bounded engineering-sheet selector in a read-only research experiment.",
        "Return only the JSON object required by the output schema.",
        "Never invent a page, option, evidence, value, sheet number, or entity name.",
        "For every task, local_option_id and map_option_id must be one of that task's option_ids.",
        "First decide locally. Then review the full document map and set map_option_id after checking shared RIGHT conflicts and cardinality.",
        "Choose NEED_MORE_EVIDENCE whenever the supplied evidence does not prove the same engineering function.",
        "NO_ANALOG is appropriate only when the bounded candidates affirmatively show no analogue; absence from top-10 alone is not proof.",
        vision_note,
        "image_manifest=" + canonical_json(image_manifest),
        "payload=" + canonical_json(payload),
    ])
    return prompt, payload


def selector_schema(dataset: ProjectDataset, payload_signature: str) -> dict[str, Any]:
    task_ids = [str(task["task_id"]) for task in dataset.tasks]
    option_ids = sorted({
        option_id for task in dataset.tasks for option_id in task["option_ids"]
    })
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "payload_signature": {"type": "string", "enum": [payload_signature]},
            "selections": {
                "type": "array",
                "minItems": len(task_ids),
                "maxItems": len(task_ids),
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "properties": {
                        "task_id": {"type": "string", "enum": task_ids},
                        "local_option_id": {"type": "string", "enum": option_ids},
                        "map_option_id": {"type": "string", "enum": option_ids},
                    },
                    "required": ["task_id", "local_option_id", "map_option_id"],
                },
            },
        },
        "required": ["payload_signature", "selections"],
    }


def _valid_cardinality(option: Mapping[str, Any]) -> bool:
    decision = option.get("decision_type")
    left = list(option.get("left_pages") or [])
    right = list(option.get("right_pages") or [])
    if decision == "MATCH_1_TO_1":
        return len(left) == len(right) == 1
    if decision == "SPLIT_1_TO_N":
        return len(left) == 1 and len(right) > 1
    if decision == "MERGED_N_TO_1":
        return len(left) > 1 and len(right) == 1
    if decision == "FUNCTION_DISTRIBUTED":
        return bool(left) and len(right) > 1
    return False


def verify_selector_response(
    dataset: ProjectDataset,
    payload_signature: str,
    response: Mapping[str, Any] | None,
) -> dict[str, Any]:
    task_by_id = {str(task["task_id"]): task for task in dataset.tasks}
    task_errors: dict[str, list[str]] = {task_id: [] for task_id in task_by_id}
    global_errors: list[str] = []
    selections_by_task: dict[str, dict[str, str]] = {}
    if not isinstance(response, Mapping):
        global_errors.append("MODEL_FAILURE_OR_NON_OBJECT")
    else:
        if set(response) != {"payload_signature", "selections"}:
            global_errors.append("UNEXPECTED_RESPONSE_FIELD")
        if response.get("payload_signature") != payload_signature:
            global_errors.append("PAYLOAD_SIGNATURE_MISMATCH")
        selections = response.get("selections")
        if not isinstance(selections, list):
            global_errors.append("SELECTIONS_NOT_ARRAY")
            selections = []
        for raw in selections:
            if not isinstance(raw, Mapping):
                global_errors.append("SELECTION_NOT_OBJECT")
                continue
            task_id = str(raw.get("task_id") or "")
            if task_id not in task_by_id:
                global_errors.append("INVENTED_TASK_ID")
                continue
            if set(raw) != {"task_id", "local_option_id", "map_option_id"}:
                task_errors[task_id].append("UNEXPECTED_SELECTION_FIELD")
            if task_id in selections_by_task:
                task_errors[task_id].append("DUPLICATE_TASK_SELECTION")
                continue
            selections_by_task[task_id] = {
                "local_option_id": str(raw.get("local_option_id") or ""),
                "map_option_id": str(raw.get("map_option_id") or ""),
            }
        for task_id, task in task_by_id.items():
            selection = selections_by_task.get(task_id)
            if selection is None:
                task_errors[task_id].append("MISSING_TASK_SELECTION")
                continue
            allowed = set(str(item) for item in task["option_ids"])
            for field in ("local_option_id", "map_option_id"):
                option_id = selection[field]
                if option_id not in allowed:
                    task_errors[task_id].append(f"{field.upper()}_NOT_BOUND_TO_TASK")
                    continue
                if option_id in SENTINEL_OPTION_IDS:
                    continue
                option = dataset.options.get(option_id)
                if option is None:
                    task_errors[task_id].append("INVENTED_CANDIDATE_ID")
                    continue
                if option.get("pair_id") != dataset.pair_id:
                    task_errors[task_id].append("CANDIDATE_PAIR_MISMATCH")
                if int(task["left_page"]) not in option.get("left_pages", []):
                    task_errors[task_id].append("CANDIDATE_LEFT_BINDING_MISMATCH")
                if not _valid_cardinality(option):
                    task_errors[task_id].append("INVALID_CARDINALITY")
                if any(not 1 <= int(page) <= dataset.page_counts["LEFT"] for page in option.get("left_pages") or []):
                    task_errors[task_id].append("INVALID_LEFT_PAGE")
                if any(not 1 <= int(page) <= dataset.page_counts["RIGHT"] for page in option.get("right_pages") or []):
                    task_errors[task_id].append("INVALID_RIGHT_PAGE")
                for evidence_ref in option.get("evidence_refs") or []:
                    if evidence_ref not in dataset.evidence_catalog:
                        task_errors[task_id].append("MISSING_EVIDENCE_REF")
                        continue
                    evidence = dataset.evidence_catalog[evidence_ref]
                    evidence_side = evidence.get("side")
                    evidence_page = evidence.get("page")
                    if evidence_side == "LEFT" and evidence_page not in option.get("left_pages", []):
                        task_errors[task_id].append("EVIDENCE_PAGE_MISMATCH")
                    if evidence_side == "RIGHT" and evidence_page not in option.get("right_pages", []):
                        task_errors[task_id].append("EVIDENCE_PAGE_MISMATCH")
                    if evidence.get("kind") == "production_sheet_matcher_candidate":
                        if evidence.get("left_page") not in option.get("left_pages", []):
                            task_errors[task_id].append("EVIDENCE_PAGE_MISMATCH")
                        if evidence.get("right_page") not in option.get("right_pages", []):
                            task_errors[task_id].append("EVIDENCE_PAGE_MISMATCH")

    # Whole-map consistency is checked on map_option_id, after local choices.
    selected_concrete: dict[str, list[str]] = {}
    for task_id, selection in selections_by_task.items():
        option_id = selection["map_option_id"]
        if option_id not in SENTINEL_OPTION_IDS and option_id in dataset.options:
            selected_concrete.setdefault(option_id, []).append(task_id)
    for option_id, selected_tasks in selected_concrete.items():
        option = dataset.options[option_id]
        required_tasks = {
            str(task["task_id"])
            for task in dataset.tasks
            if int(task["left_page"]) in option["left_pages"]
        }
        selected_set = set(selected_tasks)
        if selected_set != required_tasks:
            for task_id in selected_set | required_tasks:
                if task_id in task_errors:
                    task_errors[task_id].append("INCOMPLETE_GROUP_SELECTION")

    option_items = [(option_id, dataset.options[option_id]) for option_id in selected_concrete]
    for index, (option_a_id, option_a) in enumerate(option_items):
        for option_b_id, option_b in option_items[index + 1:]:
            if option_a_id == option_b_id:
                continue
            if set(option_a["left_pages"]) & set(option_b["left_pages"]):
                affected = selected_concrete[option_a_id] + selected_concrete[option_b_id]
                for task_id in affected:
                    task_errors[task_id].append("LEFT_MAP_CONFLICT")
            if set(option_a["right_pages"]) & set(option_b["right_pages"]):
                affected = selected_concrete[option_a_id] + selected_concrete[option_b_id]
                for task_id in affected:
                    task_errors[task_id].append("RIGHT_MAP_CONFLICT")

    for task_id in task_errors:
        task_errors[task_id] = sorted(set(task_errors[task_id]))
    task_results = {
        task_id: {
            "ok": not errors and not global_errors,
            "errors": [*global_errors, *errors],
            "selection": selections_by_task.get(task_id),
        }
        for task_id, errors in task_errors.items()
    }
    return {
        "ok": not global_errors and all(not errors for errors in task_errors.values()),
        "global_errors": sorted(set(global_errors)),
        "task_results": task_results,
        "direction": "LEFT_TO_RIGHT",
        "invented_evidence_possible": False,
    }


def _human_option_match(dataset: ProjectDataset, left_page: int, option: Mapping[str, Any]) -> bool | None:
    human = dataset.human_by_left.get(left_page)
    if human is None:
        return None
    return (
        option.get("decision_type") == human["decision_type"]
        and sorted(option.get("left_pages") or []) == human["left_pages"]
        and sorted(option.get("right_pages") or []) == human["right_pages"]
    )


def aggregate_decisions(
    dataset: ProjectDataset,
    *,
    mode: str,
    run_records: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Combine Pass A/B and three cold runs; human priority is the final gate."""
    by_key = {
        (int(item["cold_run"]), str(item["pass_name"])): item
        for item in run_records
        if item.get("pair_id") == dataset.pair_id and item.get("mode") == mode
    }
    task_by_id = {str(task["task_id"]): task for task in dataset.tasks}
    decisions = []
    cold_maps: list[dict[str, str | None]] = []
    for cold_run in (1, 2, 3):
        cold_map: dict[str, str | None] = {}
        for task_id in task_by_id:
            records = [by_key.get((cold_run, pass_name)) for pass_name in ("A", "B")]
            choices: list[str] = []
            valid = True
            for record in records:
                if not record:
                    valid = False
                    continue
                result = (record.get("verification") or {}).get("task_results", {}).get(task_id, {})
                selection = result.get("selection") or {}
                if not result.get("ok") or not selection.get("map_option_id"):
                    valid = False
                    continue
                choices.append(str(selection["map_option_id"]))
            cold_map[task_id] = choices[0] if valid and len(choices) == 2 and len(set(choices)) == 1 else None
        cold_maps.append(cold_map)

    for task_id, task in task_by_id.items():
        left_page = int(task["left_page"])
        pass_a = []
        pass_b = []
        for cold_run in (1, 2, 3):
            for pass_name, target in (("A", pass_a), ("B", pass_b)):
                record = by_key.get((cold_run, pass_name))
                result = ((record or {}).get("verification") or {}).get("task_results", {}).get(task_id, {})
                selection = result.get("selection") or {}
                target.append({
                    "cold_run": cold_run,
                    "option_id": selection.get("map_option_id"),
                    "local_option_id": selection.get("local_option_id"),
                    "verified": bool(result.get("ok")),
                    "errors": list(result.get("errors") or []),
                })
        cold_choices = [cold_map[task_id] for cold_map in cold_maps]
        stable_option_id = (
            str(cold_choices[0])
            if all(choice is not None for choice in cold_choices) and len(set(cold_choices)) == 1
            else None
        )
        materialization_allowed = False
        unsupported = False
        verifier_status = "BLOCKED_STABILITY"
        final_status = "UNRESOLVED"
        reason = "Pass A/B or cold runs disagreed, failed, or produced a map conflict."
        selected_option: dict[str, Any] | None = None
        if stable_option_id in SENTINEL_OPTION_IDS:
            verifier_status = "PASS_BOUNDED_SENTINEL"
            final_status = "UNRESOLVED"
            reason = (
                "All six passes selected NEED_MORE_EVIDENCE; no relation is materialized."
                if stable_option_id == "NEED_MORE_EVIDENCE"
                else "All six passes selected NO_ANALOG, but bounded top-10 is not exhaustive; no relation is materialized."
            )
        elif stable_option_id is not None:
            selected_option = dataset.options.get(stable_option_id)
            if selected_option is None:
                verifier_status = "BLOCKED_INVENTED_CANDIDATE"
                reason = "Stable ID is not a prebuilt candidate."
            else:
                human_match = _human_option_match(dataset, left_page, selected_option)
                if human_match is True:
                    materialization_allowed = True
                    verifier_status = "PASS_HUMAN_PRIORITY"
                    final_status = "STABLE_AUTO"
                    reason = "Six-pass unanimity, bounded verifier pass, and no conflict with the saved engineer decision."
                elif human_match is False:
                    unsupported = True
                    verifier_status = "BLOCKED_HUMAN_DECISION_CONFLICT"
                    final_status = "HUMAN_REVIEW"
                    reason = "Stable bounded selection conflicts with the saved engineer decision; human decision wins."
                else:
                    unsupported = True
                    verifier_status = "BLOCKED_HUMAN_SUPPORT_MISSING"
                    final_status = "HUMAN_REVIEW"
                    reason = "Stable bounded selection has no human-confirmed mapping; manual audit is required."

        candidate_evidence = []
        for option_id in task["option_ids"]:
            if option_id in SENTINEL_OPTION_IDS:
                continue
            option = dataset.options[option_id]
            candidate_evidence.append({
                "candidate_id": option_id,
                "decision_type": option["decision_type"],
                "left_pages": option["left_pages"],
                "right_pages": option["right_pages"],
                "evidence_refs": option["evidence_refs"],
                "deterministic_evidence": option["deterministic_evidence"],
            })
        decisions.append({
            "project": dataset.project,
            "pair_id": dataset.pair_id,
            "run_id": dataset.run_id,
            "mode": mode,
            "left_page": left_page,
            "task_id": task_id,
            "candidate_ids": list(task["option_ids"]),
            "candidate_evidence": candidate_evidence,
            "pass_A": pass_a,
            "pass_B": pass_b,
            "cold_run_unanimous_options": cold_choices,
            "selected_option_id": stable_option_id,
            "selected_option": selected_option,
            "final_status": final_status,
            "verifier_status": verifier_status,
            "materialization_allowed": materialization_allowed,
            "unsafe_or_unsupported_decision": unsupported,
            "confidence": "UNANIMOUS_6_OF_6" if stable_option_id is not None else "NOT_STABLE",
            "reason": reason,
        })

    pairwise = []
    for index_a, index_b in ((0, 1), (0, 2), (1, 2)):
        same = sum(cold_maps[index_a][task_id] == cold_maps[index_b][task_id] for task_id in task_by_id)
        pairwise.append({
            "cold_runs": [index_a + 1, index_b + 1],
            "overlap": round(same / len(task_by_id), 6) if task_by_id else None,
            "same_task_count": same,
        })
    stability = {
        "project": dataset.project,
        "pair_id": dataset.pair_id,
        "run_id": dataset.run_id,
        "mode": mode,
        "cold_run_count": 3,
        "passes_per_cold_run": 2,
        "cold_map_signatures": [digest(cold_map) for cold_map in cold_maps],
        "pairwise_overlap": pairwise,
        "stable_task_count": sum(item["selected_option_id"] is not None for item in decisions),
        "disagreement_or_failure_count": sum(item["selected_option_id"] is None for item in decisions),
    }
    return decisions, stability


def production_sources_unchanged(dataset: ProjectDataset) -> bool:
    return dataset.source_hashes == _hash_sources(dataset.pair_dir)


def decision_metrics(decisions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    concrete = [
        item for item in decisions
        if isinstance(item.get("selected_option"), Mapping)
    ]
    unique_detected: dict[str, str] = {}
    unique_materialized: dict[str, str] = {}
    for item in concrete:
        option = item["selected_option"]
        unique_detected[str(option["option_id"])] = str(option["decision_type"])
        if item.get("materialization_allowed"):
            unique_materialized[str(option["option_id"])] = str(option["decision_type"])
    return {
        "task_count": len(decisions),
        "stable_auto_decisions": sum(bool(item.get("materialization_allowed")) for item in decisions),
        "human_review": sum(item.get("final_status") == "HUMAN_REVIEW" for item in decisions),
        "unresolved": sum(item.get("final_status") == "UNRESOLVED" for item in decisions),
        "unsafe_or_unsupported_decisions": sum(bool(item.get("unsafe_or_unsupported_decision")) for item in decisions),
        "unsupported_auto_matches": sum(
            bool(item.get("materialization_allowed"))
            and bool(item.get("unsafe_or_unsupported_decision"))
            for item in decisions
        ),
        "detected_relations": {
            kind: sum(value == kind for value in unique_detected.values())
            for kind in sorted(CONCRETE_DECISIONS)
        },
        "materialized_relations": {
            kind: sum(value == kind for value in unique_materialized.values())
            for kind in sorted(CONCRETE_DECISIONS)
        },
    }
