"""What a function documents, and where on its page those things are printed.

Two kinds of documented material, and they come from different places.

*Documented values* are the passport's own fields — the same frozen field list
V1's reassessment used, minus ``systems`` (a bag of words).  They are what the
model-written passport says.

*Fragment evidence* is older and rawer: the Markdown rows the deterministic
extractor attributed to the fragment before any passport existed —
``Квартиры 1к | 13 | 14,0 | 182,00``.  Table rows are printed cell by cell, so a
row is split into its cells and each cell is located on its own.

Locating never uses a distance.  A needle is located when a printed string of
the page *equals* it after the shared normalization, or contains it when the
needle is long enough to be more than a word; the containers that own those
printed strings are the containers V1 attributed them to through a drawn cell,
box or leader.  A needle printed in two containers is recorded and votes for
neither.
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from backend.app.pipeline.stages.block_grounding import electrical_load_table as production_values
from experiments.function_lineage_v2 import instance_identity as production_marks
from experiments.pdf_evidence_v1.reassessment import BOUND_FIELDS
from experiments.pdf_evidence_v1.textnorm import MIN_COMPARABLE, normalize

from .contract import EXCLUDED_VALUE_FIELDS

#: Below this many normalized characters a needle may match a printed string by
#: accident.  Reported with a curve, never presented as a tuned single truth.
MIN_DISCRIMINATING_CHARS = 8
#: A fragment whose located rows all lie in one container certifies only when
#: at least this many distinct rows were located there.  Same rule: a curve.
MIN_LOCATED_SEGMENTS = 2
SEGMENT_LENGTHS = (4, 6, 8, 12)
SEGMENT_COUNTS = (1, 2, 3, 4)

#: Quantity facets a passport and a container may both state about one named
#: thing.  A single value on each side that differs is a contradiction.
COMPARABLE_FACETS = (
    "installed_power_kw",
    "demand_active_power_kw",
    "maximum_calculated_current_a",
)


@dataclass
class Location:
    needle: str
    printed: bool
    label_ids: tuple[str, ...] = ()
    containers: tuple[str, ...] = ()


@dataclass
class PageIndex:
    """A page folded once: every printed string with the container that owns it."""

    entries: list[tuple[str, str, str | None]] = field(default_factory=list)
    owner_of_label: dict[str, str] = field(default_factory=dict)
    caption_label_ids: set[str] = field(default_factory=set)
    bbox_of_label: dict[str, tuple[float, float, float, float]] = field(default_factory=dict)
    labels_of_assembly: dict[str, tuple[str, ...]] = field(default_factory=dict)


def build_index(page: Any, assemblies: Sequence[Any]) -> PageIndex:
    index = PageIndex()
    for assembly in assemblies:
        index.labels_of_assembly[assembly.assembly_id] = tuple(assembly.member_label_ids)
        for label_id in assembly.member_label_ids:
            index.owner_of_label[label_id] = assembly.assembly_id
            row = page.labels_by_id.get(label_id)
            if row is not None and assembly.table_ids and row.get("cell") is not None:
                if int(row["cell"][0]) == 0:
                    index.caption_label_ids.add(label_id)
    for label_id, row in page.labels_by_id.items():
        index.entries.append((normalize(row["text"]), label_id, index.owner_of_label.get(label_id)))
        box = row.get("bbox")
        if box:
            index.bbox_of_label[label_id] = tuple(float(value) for value in box)
    return index


def locate(
    index: PageIndex,
    needle: str,
    *,
    minimum_chars: int = MIN_DISCRIMINATING_CHARS,
    restrict_to: Iterable[str] | None = None,
    exclude_labels: Iterable[str] = (),
) -> Location:
    """Where a normalized needle is printed, and which containers own it."""
    folded = normalize(needle)
    if len(folded) < MIN_COMPARABLE:
        return Location(needle=folded, printed=False)
    allowed = set(restrict_to) if restrict_to is not None else None
    excluded = set(exclude_labels)
    label_ids: list[str] = []
    containers: set[str] = set()
    for text, label_id, owner in index.entries:
        if label_id in excluded:
            continue
        if allowed is not None and label_id not in allowed:
            continue
        if text == folded or (len(folded) >= minimum_chars and folded in text):
            label_ids.append(label_id)
            if owner:
                containers.add(owner)
    return Location(
        needle=folded, printed=bool(label_ids),
        label_ids=tuple(label_ids), containers=tuple(sorted(containers)),
    )


def documented_values(passport: Mapping[str, Any]) -> list[tuple[str, str]]:
    """The passport's documented values, in V1's frozen field order, minus bags of words."""
    out: list[tuple[str, str]] = []
    for name in BOUND_FIELDS:
        if name in EXCLUDED_VALUE_FIELDS:
            continue
        raw = passport.get(name)
        if raw is None:
            continue
        values = [raw] if isinstance(raw, str) else list(raw)
        for value in values:
            text = str(value).strip()
            if text:
                out.append((name, text))
    return out


def fragment_segments(fragments: Sequence[Mapping[str, Any]]) -> list[str]:
    """Distinct normalized cells of the fragment's raw evidence rows, in order."""
    seen: set[str] = set()
    out: list[str] = []
    for fragment in fragments:
        for snippet in fragment.get("evidence_snippets") or []:
            for piece in str(snippet).replace("\n", "|").split("|"):
                folded = normalize(piece)
                if len(folded) < MIN_COMPARABLE or folded in seen:
                    continue
                seen.add(folded)
                out.append(folded)
    return out


def primary_mark_of(passport: Mapping[str, Any]) -> str | None:
    facts = production_marks.function_instance_identity(passport)
    value = facts["identity_facts"].get("primary_mark")
    return str(value) if value else None


def marks_of(text: str) -> set[str]:
    return {str(row["mark"]) for row in production_marks.extract_marks(str(text))}


def passport_quantities(passport: Mapping[str, Any]) -> dict[str, set[float]]:
    """Prefixed quantities the passport's text states, through the production parser."""
    out: dict[str, set[float]] = defaultdict(set)
    texts: list[str] = []
    for value in passport.values():
        if isinstance(value, str):
            texts.append(value)
        elif isinstance(value, list):
            texts.extend(str(item) for item in value if isinstance(item, str))
        elif isinstance(value, dict):
            texts.extend(str(item) for item in value.values() if isinstance(item, str))
    for text in texts:
        for row in production_values.parse_values(text):
            if row.get("reading") != "PREFIXED":
                continue
            facet = str(row["facet_ref"])
            if facet in COMPARABLE_FACETS:
                out[facet].update(float(value) for value in row["values"])
    return {key: out[key] for key in sorted(out)}


__all__ = [
    "COMPARABLE_FACETS",
    "MIN_DISCRIMINATING_CHARS",
    "MIN_LOCATED_SEGMENTS",
    "SEGMENT_COUNTS",
    "SEGMENT_LENGTHS",
    "Location",
    "PageIndex",
    "build_index",
    "documented_values",
    "fragment_segments",
    "locate",
    "marks_of",
    "passport_quantities",
    "primary_mark_of",
]
