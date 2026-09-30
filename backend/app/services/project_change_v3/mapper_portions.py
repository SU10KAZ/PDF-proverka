"""Mapper по порциям (engine 3.10.0).

Claude Code CLI передаёт в одном запросе не больше 100 изображений и не больше
24 МиБ изображений; лишние он молча выбрасывает.  Сборка из многих проектов
(9 корпусов РД — 304 графических фрагмента, 86 МБ) в один вызов Mapper не
помещается, и раньше движок честно отказывался (``assembly_mapper_envelope_exceeded``).

Теперь, если один вызов не помещается, Mapper вызывается порциями:

* сторона OLD передаётся ЦЕЛИКОМ в каждую порцию — Mapper всегда видит весь
  исходный проект и может сопоставить любую его страницу;
* сторона NEW делится на порции по проектам сборки (корпус целиком, пока он
  помещается вместе с OLD); проект, который не помещается сам, режется по
  страницам; без сборки порции — подряд идущие страницы;
* каждая порция проверяется точным планом транспорта (``plan_claude``) до
  отправки: ничего не обрезается молча;
* ответ каждой порции проверяется так же, как ответ одного вызова
  (``validate_map`` на страницах порции), затем порции сливаются: области
  перенумеровываются с префиксом порции (``P03.A-R002``), NEW-страницы без пары
  объединяются, а OLD-страница считается без пары, только если её не взяла ни
  одна порция.  Объединённая карта проходит ту же финальную проверку покрытия.

Промпт Mapper не меняется.  К данным порции добавляется поле ``portion`` —
какая часть NEW передана.  Пара, которая помещается в один вызов, идёт прежним
путём байт в байт.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

PORTIONING_VERSION = "projectchange_v3_mapper_portions/1"

PageRecord = dict[str, Any]


class PortioningImpossible(RuntimeError):
    """Даже одна страница NEW вместе со стороной OLD не помещается в вызов."""


@dataclass
class MapperPortion:
    index: int
    total: int
    label: str
    new_pages: list[int]
    units: list[str] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        return {"index": self.index, "total": self.total, "label": self.label,
                "new_pages": list(self.new_pages), "units": list(self.units)}


def new_units(structure: list[PageRecord], origin_sources: list[dict[str, Any]] | None) -> list[tuple[str, list[int]]]:
    """Смысловые единицы стороны NEW: проекты сборки, иначе отдельные страницы.

    ``origin_sources`` — ``assembly_origin.json["sources"]`` стороны NEW
    (``document_code``, ``page_start``, ``page_end``).  Страницы вне любого
    проекта сборки становятся отдельными единицами.
    """
    pages = sorted(p["physical_page"] for p in structure if p["side"] == "NEW")
    units: list[tuple[str, list[int]]] = []
    covered: set[int] = set()
    for source in origin_sources or []:
        try:
            start, end = int(source["page_start"]), int(source["page_end"])
        except (KeyError, TypeError, ValueError):
            continue
        member = [p for p in pages if start <= p <= end and p not in covered]
        if member:
            units.append((str(source.get("document_code") or f"стр. {start}–{end}"), member))
            covered.update(member)
    for page in pages:
        if page not in covered:
            units.append((f"стр. {page}", [page]))
    units.sort(key=lambda unit: unit[1][0])
    return units


def plan_portions(
    structure: list[PageRecord],
    units: list[tuple[str, list[int]]],
    fits: Callable[[list[PageRecord]], bool],
) -> list[MapperPortion]:
    """Жадно собрать порции NEW, каждая вместе со всей стороной OLD помещается в вызов."""
    old = [p for p in structure if p["side"] == "OLD"]
    by_page = {p["physical_page"]: p for p in structure if p["side"] == "NEW"}

    def pages_of(numbers: Iterable[int]) -> list[PageRecord]:
        return [by_page[n] for n in numbers]

    groups: list[tuple[list[str], list[int]]] = []
    current_labels: list[str] = []
    current_pages: list[int] = []

    def close() -> None:
        nonlocal current_labels, current_pages
        if current_pages:
            groups.append((current_labels, current_pages))
        current_labels, current_pages = [], []

    for label, unit_pages in units:
        if fits(old + pages_of(current_pages + unit_pages)):
            current_labels.append(label)
            current_pages.extend(unit_pages)
            continue
        close()
        if fits(old + pages_of(unit_pages)):
            current_labels, current_pages = [label], list(unit_pages)
            continue
        # Проект не помещается даже один: режем его по страницам.
        for page in unit_pages:
            if fits(old + pages_of(current_pages + [page])):
                if not current_labels or current_labels[-1] != label:
                    current_labels.append(label)
                current_pages.append(page)
                continue
            close()
            if not fits(old + pages_of([page])):
                raise PortioningImpossible(
                    f"страница NEW {page} вместе со всей стороной OLD не помещается в один вызов модели"
                )
            current_labels, current_pages = [label], [page]
    close()
    total = len(groups)
    return [
        MapperPortion(index=i, total=total, label=_label(labels, pages), new_pages=pages, units=labels)
        for i, (labels, pages) in enumerate(groups, start=1)
    ]


def _label(labels: list[str], pages: list[int]) -> str:
    span = f"стр. NEW {pages[0]}–{pages[-1]}" if len(pages) > 1 else f"стр. NEW {pages[0]}"
    return f"{', '.join(labels)} ({span})"


def portion_structure(structure: list[PageRecord], portion: MapperPortion) -> list[PageRecord]:
    wanted = set(portion.new_pages)
    return [p for p in structure if p["side"] == "OLD" or p["physical_page"] in wanted]


def portion_data(pair_id: str, structure: list[PageRecord], portion: MapperPortion) -> dict[str, Any]:
    """Данные вызова: как у одного вызова, плюс описание порции."""
    return {
        "pair": pair_id,
        "pages": portion_structure(structure, portion),
        "portion": {
            "index": portion.index,
            "total": portion.total,
            "new_scope": portion.label,
            "note": (
                f"Это порция {portion.index} из {portion.total}. OLD передан целиком, NEW — только "
                f"{portion.label}. Сопоставляй OLD только со страницами NEW этой порции. Страницы OLD, "
                "которым в этой порции нет пары, укажи в unmatched_old: их могут сопоставить другие порции."
            ),
        },
    }


def merge_portion_maps(
    pair_id: str,
    portion_maps: list[dict[str, Any]],
    portions: list[MapperPortion],
    structure: list[PageRecord],
) -> dict[str, Any]:
    """Слить ответы порций в одну карту той же схемы, что у одного вызова."""
    regions: list[dict[str, Any]] = []
    unmatched_new: set[int] = set()
    notes: list[str] = []
    for portion, answer in zip(portions, portion_maps, strict=True):
        prefix = f"P{portion.index:02d}."
        for region in answer.get("regions") or []:
            merged = copy.deepcopy(region)
            merged["region_id"] = prefix + str(region["region_id"])
            regions.append(merged)
        unmatched_new.update(int(p) for p in answer.get("unmatched_new") or [])
        notes.extend(f"[порция {portion.index}/{portion.total}: {portion.label}] {note}"
                     for note in answer.get("coverage_notes") or [])
    all_old = {p["physical_page"] for p in structure if p["side"] == "OLD"}
    mapped_old = {int(p) for region in regions for p in region["old_pages"]}
    return {
        "pair": pair_id,
        "regions": regions,
        "unmatched_old": sorted(all_old - mapped_old),
        "unmatched_new": sorted(unmatched_new),
        "coverage_notes": notes,
    }


def split_region_for_envelope(
    region: dict[str, Any],
    fits: Callable[[dict[str, Any]], bool],
) -> list[dict[str, Any]]:
    """Разделить область, чей вызов Miner не помещается в транспорт, на части по страницам NEW.

    Страницы OLD области идут в каждую часть целиком.  Ссылки на важные блоки
    остаются только те, что лежат на страницах части.  Область, которая
    помещается, возвращается как есть (одна, без изменений).
    """
    if fits(region):
        return [region]
    new_pages = [int(p) for p in region["new_pages"]]
    chunks: list[list[int]] = []
    current: list[int] = []
    for page in new_pages:
        if fits(_sub_region(region, current + [page], 0, 0)):
            current.append(page)
            continue
        if current:
            chunks.append(current)
        if not fits(_sub_region(region, [page], 0, 0)):
            raise PortioningImpossible(
                f"область {region['region_id']}: страница NEW {page} вместе со всеми страницами OLD области "
                "не помещается в один вызов Miner"
            )
        current = [page]
    if current:
        chunks.append(current)
    return [_sub_region(region, chunk, i, len(chunks)) for i, chunk in enumerate(chunks, start=1)]


def _sub_region(region: dict[str, Any], new_pages: list[int], index: int, total: int) -> dict[str, Any]:
    part = copy.deepcopy(region)
    part["new_pages"] = list(new_pages)
    keep = {("OLD", int(p)) for p in region["old_pages"]} | {("NEW", int(p)) for p in new_pages}
    for key in ("important_text_blocks", "important_table_blocks", "important_graphic_blocks"):
        part[key] = [ref for ref in region.get(key) or []
                     if (ref.get("side"), int(ref.get("physical_page") or 0)) in keep]
    if total:
        part["region_id"] = f"{region['region_id']}.{index}"
        part["scope"] = f"{region.get('scope', '')} (часть {index} из {total}: стр. NEW {new_pages[0]}–{new_pages[-1]})"
    return part


__all__ = [
    "MapperPortion",
    "PORTIONING_VERSION",
    "PortioningImpossible",
    "merge_portion_maps",
    "new_units",
    "plan_portions",
    "portion_data",
    "portion_structure",
    "split_region_for_envelope",
]
