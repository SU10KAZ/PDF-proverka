"""Mapper по порциям (Claude с 3.10.0, Codex с 3.13.0).

Claude Code CLI передаёт в одном запросе не больше 100 изображений и не больше
24 МиБ изображений; лишние он молча выбрасывает.  Сборка из многих проектов
(9 корпусов РД — 304 графических фрагмента, 86 МБ) в один вызов Mapper не
помещается, и раньше движок честно отказывался (``assembly_mapper_envelope_exceeded``).

Теперь, если один вызов не помещается, Mapper вызывается порциями:

* сначала сторона OLD передаётся ЦЕЛИКОМ в каждую порцию; если она не
  помещается даже с одной страницей NEW, OLD тоже делится. Каждая часть OLD
  проходит всю сторону NEW, поэтому ни одно сочетание страниц не теряется;
* сторона NEW делится на порции по проектам сборки (корпус целиком, пока он
  помещается вместе с OLD); проект, который не помещается сам, режется по
  страницам; без сборки порции — подряд идущие страницы;
* каждая порция проверяется планом транспорта Claude или оценкой общего
  контекста Codex до отправки: ничего не обрезается молча;
* ответ каждой порции проверяется так же, как ответ одного вызова
  (``validate_map`` на страницах порции), затем порции сливаются: области
  перенумеровываются с префиксом порции (``P03.A-R002``), страница любой стороны
  считается без пары только тогда, когда её не взяла ни одна порция.
  Объединённая карта проходит ту же финальную проверку покрытия.

Промпт Mapper не меняется.  К данным порции добавляется поле ``portion`` —
какие части OLD/NEW переданы. Пара, которая помещается в один вызов, идёт прежним
путём байт в байт.
"""
from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable

PORTIONING_VERSION = "projectchange_v3_mapper_portions/2"

PageRecord = dict[str, Any]


class PortioningImpossible(RuntimeError):
    """Источники не помещаются в вызов даже после разбиения по страницам."""


@dataclass
class MapperPortion:
    index: int
    total: int
    label: str
    new_pages: list[int]
    units: list[str] = field(default_factory=list)
    old_pages: list[int] | None = None  # None retains the original whole-OLD path.

    def summary(self) -> dict[str, Any]:
        result = {"index": self.index, "total": self.total, "label": self.label,
                  "new_pages": list(self.new_pages), "units": list(self.units)}
        if self.old_pages is not None:
            result["old_pages"] = list(self.old_pages)
        return result


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
    """Preserve the NEW-only plan when possible; otherwise partition OLD too.

    Every OLD group is compared against ALL NEW units. Recursion finishes at
    individual OLD pages; the NEW planner already goes down to one NEW page.
    Thus an oversized page pair is refused before any provider call, while
    every feasible OLD×NEW combination remains covered exactly once.
    """
    old = sorted((p for p in structure if p["side"] == "OLD"), key=lambda p: p["physical_page"])
    new = [p for p in structure if p["side"] == "NEW"]

    def split(old_group: list[PageRecord]) -> list[MapperPortion]:
        try:
            portions = _plan_new_portions(old_group + new, units, fits)
        except PortioningImpossible:
            if len(old_group) <= 1:
                raise
            middle = len(old_group) // 2
            return split(old_group[:middle]) + split(old_group[middle:])
        if len(old_group) != len(old):
            for portion in portions:
                portion.old_pages = [p["physical_page"] for p in old_group]
        return portions

    portions = split(old)
    for index, portion in enumerate(portions, start=1):
        portion.index, portion.total = index, len(portions)
    validate_portion_coverage(structure, portions)
    return portions


def validate_portion_coverage(structure: list[PageRecord], portions: list[MapperPortion]) -> None:
    """Refuse gaps/overlaps in the OLD×NEW search space before model calls or merge."""
    old = {p["physical_page"] for p in structure if p["side"] == "OLD"}
    new = {p["physical_page"] for p in structure if p["side"] == "NEW"}
    covered = {page: set() for page in old}
    for portion in portions:
        old_scope = old if portion.old_pages is None else set(portion.old_pages)
        new_scope = set(portion.new_pages)
        if not old_scope or not new_scope or not old_scope <= old or not new_scope <= new:
            raise PortioningImpossible("порция Mapper содержит пустую сторону или неизвестную страницу")
        for page in old_scope:
            if covered[page] & new_scope:
                raise PortioningImpossible("сочетание страниц OLD↔NEW повторяется в плане Mapper")
            covered[page].update(new_scope)
    if not old or not new or any(pages != new for pages in covered.values()):
        raise PortioningImpossible("план Mapper не покрывает все сочетания страниц OLD↔NEW")


def _plan_new_portions(
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
                old_scope = ", ".join(str(p["physical_page"]) for p in old)
                raise PortioningImpossible(
                    f"страницы OLD [{old_scope}] и NEW {page} не помещаются в один вызов модели; "
                    "исходные данные не обрезаны"
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
    old = None if portion.old_pages is None else set(portion.old_pages)
    return [p for p in structure
            if (p["side"] == "OLD" and (old is None or p["physical_page"] in old))
            or (p["side"] == "NEW" and p["physical_page"] in wanted)]


def portion_data(pair_id: str, structure: list[PageRecord], portion: MapperPortion) -> dict[str, Any]:
    """Данные вызова: как у одного вызова, плюс описание порции."""
    data = {
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
    if portion.old_pages is not None:
        data["portion"].update(
            old_pages=list(portion.old_pages),
            note=(
                f"Это порция {portion.index} из {portion.total}. Обе стороны переданы частично: "
                f"OLD — страницы {', '.join(map(str, portion.old_pages))}; NEW — только {portion.label}. "
                "Сопоставляй только переданные страницы OLD и NEW. Каждая часть OLD сравнивается "
                "со всеми частями NEW в отдельных порциях. Страницы без пары в этой порции "
                "укажи в unmatched_old / unmatched_new соответственно: их могут сопоставить "
                "другие порции. Отсутствие пары здесь не означает отсутствие пары во всём документе."
            ),
        )
    return data


def merge_portion_maps(
    pair_id: str,
    portion_maps: list[dict[str, Any]],
    portions: list[MapperPortion],
    structure: list[PageRecord],
) -> dict[str, Any]:
    """Слить ответы порций в одну карту той же схемы, что у одного вызова."""
    from .validate import validate_map

    validate_portion_coverage(structure, portions)
    regions: list[dict[str, Any]] = []
    notes: list[str] = []
    for portion, answer in zip(portions, portion_maps, strict=True):
        validate_map(pair_id, answer, portion_structure(structure, portion))
        prefix = f"P{portion.index:02d}."
        for region in answer.get("regions") or []:
            merged = copy.deepcopy(region)
            merged["region_id"] = prefix + str(region["region_id"])
            regions.append(merged)
        notes.extend(f"[порция {portion.index}/{portion.total}: {portion.label}] {note}"
                     for note in answer.get("coverage_notes") or [])
    all_old = {p["physical_page"] for p in structure if p["side"] == "OLD"}
    all_new = {p["physical_page"] for p in structure if p["side"] == "NEW"}
    mapped_old = {int(p) for region in regions for p in region["old_pages"]}
    mapped_new = {int(p) for region in regions for p in region["new_pages"]}
    return {
        "pair": pair_id,
        "regions": regions,
        "unmatched_old": sorted(all_old - mapped_old),
        "unmatched_new": sorted(all_new - mapped_new),
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
    "validate_portion_coverage",
]
