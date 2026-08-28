"""Пакет доказательств — единственный вход ИИ-аналитика.

Модель не видит PDF, не видит репозиторий и не ходит в файлы. Она видит ровно
то, что детерминированный слой уже установил про этот элемент: пару листов и
чем она доказана, значения из Stage 3, окно соседних строк вокруг фрагмента с
обеих сторон и то, что производитель фактов о нём знает.

Это не экономия токенов, а условие проверяемости: всё, что модель вернёт как
факт документа, обязано дословно найтись в пакете, и верификатор это
проверяет. Дать модели больше — значит потерять возможность её поймать.

Пакеты собираются в памяти прямо во время прогона, из тех же артефактов,
которые конвейер и так построил. Ничего с диска не дочитывается.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Iterable, Mapping

from ..production_artifacts import content_signature, stable_id
from . import schemas

PACKAGE_VERSION = "stage-comparison-ai-evidence.v1"

#: Сколько строк документа показывать вокруг фрагмента с каждой стороны.
CONTEXT_WINDOW = 6
#: Предел длины одной строки контекста: длинное примечание не должно вытеснять
#: из пакета сам изменившийся ряд таблицы.
CONTEXT_CHAR_LIMIT = 400


def scope_ref_for_group(group: Mapping[str, Any]) -> str:
    """Тот же ключ, что чеканит производитель фактов, — иначе связи не будет."""
    return stable_id(
        "text_scope_",
        group.get("id"),
        sorted(int(page) for page in group.get("left_pages") or []),
        sorted(int(page) for page in group.get("right_pages") or []),
    )


@dataclass
class EvidenceItem:
    """Один элемент, требующий разрешения."""

    item_id: str = ""
    atom_id: str = ""
    scope_ref: str = ""
    source: str = "TEXT"

    # что установил детерминированный Stage 3
    stage3_bucket: str = ""
    direction: str = ""
    before_value: str | None = None
    after_value: str | None = None

    # где это физически лежит
    left_pages: list[int] = field(default_factory=list)
    right_pages: list[int] = field(default_factory=list)
    locations: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

    # окно соседних строк документа; текущая строка помечена «»»
    left_context: list[str] = field(default_factory=list)
    right_context: list[str] = field(default_factory=list)

    # что уже установил детерминированный слой про сам элемент
    deterministic_state: dict[str, Any] = field(default_factory=dict)

    # ссылки на доказательства, которыми модель обязана оперировать
    evidence_refs: list[dict[str, Any]] = field(default_factory=list)

    evidence_digest: str = ""

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)

    def model_view(self) -> dict[str, Any]:
        """Ровно то, что уходит в промпт: без внутренних ссылок и провенанса."""
        return {
            "item_id": self.item_id,
            "stage3_bucket": self.stage3_bucket,
            "deterministic_direction": self.direction,
            "before_value": self.before_value,
            "after_value": self.after_value,
            "left_pages": self.left_pages,
            "right_pages": self.right_pages,
            "left_context": self.left_context,
            "right_context": self.right_context,
            "deterministic_state": self.deterministic_state,
        }


@dataclass
class EvidencePackage:
    """Партия элементов одной пары листов."""

    package_version: str = PACKAGE_VERSION
    schema_version: str = schemas.SCHEMA_VERSION
    relation_id: str = ""
    sheet_relation: dict[str, Any] = field(default_factory=dict)
    items: list[EvidenceItem] = field(default_factory=list)

    def digest(self) -> str:
        return content_signature({
            "package_version": self.package_version,
            "schema_version": self.schema_version,
            "sheet_relation": self.sheet_relation,
            "items": [item.model_view() for item in self.items],
        })

    def model_view(self) -> dict[str, Any]:
        return {
            "sheet_relation": self.sheet_relation,
            "items": [item.model_view() for item in self.items],
        }


# ── Сборка ────────────────────────────────────────────────────────────────

def _fragment_index(
    preparation: Mapping[str, Any],
) -> tuple[dict[str, tuple[str, int, int]], dict[tuple[str, int], list[dict[str, Any]]]]:
    """Фрагменты по страницам в порядке чтения и позиция каждого из них."""
    pages: dict[tuple[str, int], list[dict[str, Any]]] = {}
    raw = preparation.get("fragments")
    if not isinstance(raw, Mapping):
        return {}, {}
    for key, side in (("left", "LEFT"), ("right", "RIGHT")):
        for fragment in raw.get(key) or []:
            if not isinstance(fragment, Mapping):
                continue
            page = int(fragment.get("pdf_page") or 0)
            pages.setdefault((side, page), []).append(dict(fragment))
    for values in pages.values():
        values.sort(key=lambda value: (
            int(value.get("order") or 0), str(value.get("id") or "")
        ))
    position: dict[str, tuple[str, int, int]] = {}
    for (side, page), values in pages.items():
        for index, fragment in enumerate(values):
            position[str(fragment.get("id") or "")] = (side, page, index)
    return position, pages


def _context_lines(
    position: Mapping[str, tuple[str, int, int]],
    pages: Mapping[tuple[str, int], list[dict[str, Any]]],
    fragment_id: str,
    *,
    window: int = CONTEXT_WINDOW,
) -> list[str]:
    located = position.get(fragment_id)
    if located is None:
        return []
    side, page, index = located
    values = pages.get((side, page)) or []
    low = max(0, index - window)
    high = min(len(values), index + window + 1)
    output = []
    for cursor in range(low, high):
        text = " ".join(str(values[cursor].get("text") or "").split())
        if len(text) > CONTEXT_CHAR_LIMIT:
            text = text[:CONTEXT_CHAR_LIMIT] + "…"
        marker = "»" if cursor == index else " "
        output.append(f"{marker} {text}")
    return output


def _sheet_labels(sheet_relations: Mapping[str, Any]) -> dict[str, dict[int, str]]:
    raw = sheet_relations.get("sheet_labels")
    output: dict[str, dict[int, str]] = {"LEFT": {}, "RIGHT": {}}
    if not isinstance(raw, Mapping):
        return output
    for side in ("LEFT", "RIGHT"):
        values = raw.get(side)
        if not isinstance(values, Mapping):
            continue
        for page, label in values.items():
            try:
                output[side][int(page)] = str(label)
            except (TypeError, ValueError):
                continue
    return output


def _relation_view(
    relation: Mapping[str, Any],
    labels: Mapping[str, Mapping[int, str]],
) -> dict[str, Any]:
    """Пара листов так, как её должен прочитать аналитик: названия, не хеши."""
    left_pages = sorted({int(page) for page in relation.get("left_pages") or []})
    right_pages = sorted({int(page) for page in relation.get("right_pages") or []})
    stamp = relation.get("stamp_identity")
    return {
        "left_sheets": [
            {"page": page, "title": labels["LEFT"].get(page)} for page in left_pages
        ],
        "right_sheets": [
            {"page": page, "title": labels["RIGHT"].get(page)} for page in right_pages
        ],
        "relation_type": relation.get("relation_type"),
        "status": relation.get("status"),
        "confidence": relation.get("confidence"),
        "proved_by": relation.get("primary_source"),
        "reason_codes": sorted(
            str(value) for value in relation.get("reason_codes") or []
        ),
        "stamp_identity": dict(stamp) if isinstance(stamp, Mapping) else None,
    }


def _locations(item: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    provenance = item.get("provenance")
    source_atom = (
        provenance.get("source_atom") if isinstance(provenance, Mapping) else None
    )
    raw = source_atom.get("locations") if isinstance(source_atom, Mapping) else None
    output: dict[str, list[dict[str, Any]]] = {"LEFT": [], "RIGHT": []}
    if not isinstance(raw, Mapping):
        return output
    for side in ("LEFT", "RIGHT"):
        values = raw.get(side)
        if isinstance(values, (list, tuple)):
            output[side] = [dict(value) for value in values if isinstance(value, Mapping)]
    return output


def build_packages(
    *,
    review_items: Iterable[Mapping[str, Any]],
    preparation: Mapping[str, Any],
    sheet_relations: Mapping[str, Any],
    comparison_groups: Iterable[Mapping[str, Any]],
    batch_size: int,
) -> list[EvidencePackage]:
    """Собрать пакеты, сгруппированные по паре листов.

    Партия внутри одной пары листов — это не только экономия вызовов: элементы
    одной таблицы объясняют друг друга, и модель, которая видит рядом «02.41
    Кладовая» и «02.42 Кладовая», перестаёт принимать соседнюю строку за то же
    помещение.
    """
    position, pages = _fragment_index(preparation)
    labels = _sheet_labels(sheet_relations)
    relations_by_id = {
        str(relation.get("relation_id") or ""): relation
        for relation in sheet_relations.get("relations") or []
        if isinstance(relation, Mapping)
    }
    scope_to_relation = {
        scope_ref_for_group(group): str(group.get("id") or "")
        for group in comparison_groups
    }

    grouped: dict[str, list[EvidenceItem]] = {}
    for item in review_items:
        if not isinstance(item, Mapping):
            continue
        locations = _locations(item)
        provenance = item.get("provenance")
        source_atom = (
            provenance.get("source_atom")
            if isinstance(provenance, Mapping)
            else {}
        )
        source_atom = source_atom if isinstance(source_atom, Mapping) else {}
        left_context: list[str] = []
        for location in locations["LEFT"]:
            left_context += _context_lines(
                position, pages, str(location.get("fragment_id") or "")
            )
        right_context: list[str] = []
        for location in locations["RIGHT"]:
            right_context += _context_lines(
                position, pages, str(location.get("fragment_id") or "")
            )
        evidence = EvidenceItem(
            item_id=str(item.get("review_evidence_id") or ""),
            atom_id=str(item.get("atom_id") or ""),
            scope_ref=str(item.get("scope_ref") or ""),
            source=str(item.get("source") or "TEXT"),
            stage3_bucket=str(source_atom.get("stage3_bucket") or ""),
            direction=str(item.get("direction") or ""),
            before_value=item.get("before_value"),
            after_value=item.get("after_value"),
            left_pages=sorted({
                int(value["page"]) for value in locations["LEFT"]
                if isinstance(value.get("page"), int)
            }),
            right_pages=sorted({
                int(value["page"]) for value in locations["RIGHT"]
                if isinstance(value.get("page"), int)
            }),
            locations=locations,
            left_context=left_context,
            right_context=right_context,
            deterministic_state={
                "dimension": item.get("dimension"),
                "outcome": item.get("outcome"),
                "reason_codes": sorted(
                    str(value) for value in item.get("reason_codes") or []
                ),
                "structured_fact": bool(source_atom.get("structured_fact")),
                "source_atom_outcome": (
                    provenance.get("source_atom_outcome")
                    if isinstance(provenance, Mapping)
                    else None
                ),
            },
            evidence_refs=[
                dict(value) for value in item.get("evidence_refs") or []
                if isinstance(value, Mapping)
            ],
        )
        evidence.evidence_digest = content_signature(evidence.model_view())
        relation_id = scope_to_relation.get(evidence.scope_ref, "")
        grouped.setdefault(relation_id, []).append(evidence)

    packages: list[EvidencePackage] = []
    for relation_id in sorted(grouped):
        relation = relations_by_id.get(relation_id) or {}
        view = _relation_view(relation, labels) if relation else {}
        items = sorted(grouped[relation_id], key=lambda value: value.item_id)
        for start in range(0, len(items), max(1, batch_size)):
            packages.append(EvidencePackage(
                relation_id=relation_id,
                sheet_relation=view,
                items=items[start:start + max(1, batch_size)],
            ))
    return packages


__all__ = [
    "CONTEXT_WINDOW",
    "EvidenceItem",
    "EvidencePackage",
    "PACKAGE_VERSION",
    "build_packages",
    "scope_ref_for_group",
]
