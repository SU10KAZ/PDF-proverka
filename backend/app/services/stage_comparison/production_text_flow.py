"""Deterministic Stage 2/3 preparation for explicit production scopes.

Unlike the legacy document flow this module accepts comparison groups directly.
A PAGE selection therefore starts immediately and never depends on persisted
Sheet Matcher approval or a parent relation.
"""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable, Mapping

from .production_artifacts import (
    content_signature,
    file_content_identity,
    stable_id,
    utc_now,
)
from . import (
    recognition_coverage,
    room_schedule,
    sheet_matching,
    text_comparison,
    text_differences,
)
from .text_fragment_cache import load_or_extract_document_fragments


PREPARATION_KIND = "stage_comparison_text_preparation"
PREPARATION_SCHEMA_VERSION = "text-preparation.v2"
PREPARATION_VERSION = "production-text-preparation-v2"


def normalize_comparison_groups(
    values: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    groups = []
    for value in values:
        left = sorted({int(page) for page in value.get("left_pages") or []})
        right = sorted({int(page) for page in value.get("right_pages") or []})
        if not left or not right or min([*left, *right]) < 1:
            raise ValueError("comparison group requires positive LEFT and RIGHT pages")
        relation_type = str(value.get("relation_type") or "MATCHED")
        group_id = str(value.get("id") or value.get("relation_id") or "").strip()
        if not group_id:
            group_id = stable_id("scope_", "LEFT_TO_RIGHT", left, right, relation_type)
        groups.append({
            "id": group_id,
            "left_pages": left,
            "right_pages": right,
            "relation_type": relation_type,
            # ``normalize_comparison_groups`` is intentionally idempotent:
            # Stage 3 receives the already-normalized Stage 2 preparation
            # groups.  Do not erase the confidence recorded on that first
            # pass merely because the source field is now named
            # ``relation_status``.
            "relation_status": value.get("relation_status", value.get("status")),
        })
    groups.sort(key=lambda group: (group["left_pages"], group["right_pages"], group["id"]))
    if len({group["id"] for group in groups}) != len(groups):
        raise ValueError("duplicate comparison group id")
    return groups


def split_two_up_table_rows(
    fragments: list[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Разложить сдвоенную строку экспликации на отдельные помещения.

    Лист печатают в две колонки, и одна строка документа несёт два помещения:
    «02.1 Рампа 185,03 B2 02.42 Коридор 44,10».  Пока эта строка едина, Stage 3
    сопоставляет её с правой строкой только по первому помещению, а второе —
    02.42 — уходит на правой стороне в «добавлено», хотя оно там было всегда, и
    настоящее изменение его площади (44,10 → 44,14) не видит никто.

    Резать можно только там, где документ сам доказал разметку: заголовок
    таблицы объявил колонки, а строка целиком разложилась на полные помещения.
    Строка, у которой остался хвост, не режется вовсе.

    У частей остаются рамки и страница исходной строки: факт действительно
    происходит из неё, и подсветка доказательства обязана вести туда же.
    """
    output: list[dict[str, Any]] = []
    widths: dict[str, int] = {}
    for fragment in fragments:
        if fragment.get("source_kind") != "table_row":
            continue
        units = room_schedule.header_units(fragment)
        if units:
            group = str(fragment.get("source_group") or "")
            widths[group] = max(widths.get(group, 0), max(units))
    for fragment in fragments:
        width = (
            widths.get(str(fragment.get("source_group") or ""))
            if fragment.get("source_kind") == "table_row"
            else None
        )
        parts = [
            text_comparison.canonicalize_text(str(value))
            for value in fragment.get("location_parts") or []
        ]
        units = room_schedule.row_units(parts, width) if width else None
        if not units or len(units) < 2:
            output.append(dict(fragment))
            continue
        raw = list(fragment.get("location_parts") or [])
        position = 0
        for index, unit in enumerate(units):
            piece = deepcopy(dict(fragment))
            piece["location_parts"] = raw[position:position + len(unit)]
            position += len(unit)
            piece["text"] = " ".join(str(value) for value in piece["location_parts"])
            piece["canonical_text"] = text_comparison.canonicalize_text(piece["text"])
            piece["id"] = f"{fragment['id']}#{index}"
            piece["char_count"] = len(piece["text"])
            piece["order"] = int(fragment.get("order") or 0) * 100 + index
            piece["split_from"] = {
                "fragment_id": str(fragment["id"]),
                "unit_index": index,
                "unit_count": len(units),
                "rule": "two_up_room_schedule_row",
            }
            output.append(piece)
    return output


def _append_native_fallback(
    fragments: list[dict[str, Any]],
    *,
    pdf_path: Path,
    pages: set[int],
    side: str,
    fitz: Any,
) -> dict[str, Any]:
    """Дочитать страницу из вектор-слоя PDF там, где Markdown не дал ничего.

    Резерв включается ровно для той страницы, где Markdown не выдал НИ ОДНОЙ
    текстовой единицы. Это не «второе мнение» и не улучшение полноты вообще:
    там, где Markdown что-то прочитал, он и остаётся единственным источником
    содержания, а нативный слой — независимой проверкой этого чтения.

    Смысл узкого условия в том, что независимой проверки для самого резерва
    не существует: источник и проверяющий — один и тот же текстовый слой.
    Поэтому резерв применяется только там, где альтернатива ему — не более
    слабое доказательство, а полное молчание.

    Возвращает отчёт по страницам: он попадает в артефакт подготовки, чтобы
    происхождение каждой единицы было видно, а не восстанавливалось догадкой.
    """
    read_pages = {int(fragment["pdf_page"]) for fragment in fragments}
    empty_pages = sorted(page for page in pages if page not in read_pages)
    report = {
        "applied": False,
        "pages": [],
        "fragments": 0,
        "markdown_fragments": len(fragments),
    }
    if not empty_pages:
        return report
    recovered = text_comparison.native_page_fragments(
        pdf_path, empty_pages, side, fitz=fitz
    )
    if not recovered:
        report["pages"] = empty_pages
        return report
    fragments.extend(recovered)
    fragments.sort(
        key=lambda fragment: (
            int(fragment["pdf_page"]),
            int(fragment.get("order") or 0),
            str(fragment["id"]),
        )
    )
    report.update({
        "applied": True,
        "pages": sorted({int(item["pdf_page"]) for item in recovered}),
        "fragments": len(recovered),
    })
    return report


def prepare_text_scope(
    pair: Mapping[str, Any],
    comparison_groups: Iterable[Mapping[str, Any]],
    *,
    sheet_indexes: Mapping[str, list[dict[str, Any]]] | None = None,
    fitz: Any,
    generated_at: str | None = None,
    document_cache_dir: Path | None = None,
) -> dict[str, Any]:
    """Extract located text fragments only for pages in the explicit scope.

    When ``document_cache_dir`` is supplied, a content-addressed complete
    document extraction is reused and then projected onto the requested
    groups.  Production passes it only for DOCUMENT runs.  PAGE runs leave it
    unset and the PDF extractor is invoked for selected pages alone.
    """
    groups = normalize_comparison_groups(comparison_groups)
    selected = {
        "left": {page for group in groups for page in group["left_pages"]},
        "right": {page for group in groups for page in group["right_pages"]},
    }
    fragments: dict[str, list[dict[str, Any]]] = {}
    documents: dict[str, Any] = {}
    document_cache_signatures: dict[str, str] = {}
    # Нативный текстовый слой выбранных страниц — независимый от Markdown
    # контроль полноты распознавания. Он НЕ становится вторым источником
    # фактов: сравнение по-прежнему идёт только по Markdown, а этот индекс
    # отвечает ровно на один вопрос — «а есть ли это в документе на самом
    # деле». Без него «строки справа нет» означает лишь «мы её не прочитали».
    recognition_index: dict[str, dict[str, Any]] = {}
    native_fallback: dict[str, dict[str, Any]] = {}
    for side, stage in (("left", "stage_1"), ("right", "stage_2")):
        document = pair.get(side)
        if not isinstance(document, Mapping):
            raise ValueError(f"pair.{side} document required")
        pdf_path = Path(str(document.get("pdf_path") or ""))
        markdown_path = Path(str(document.get("md_path") or ""))
        if not markdown_path.is_file():
            markdown_path = pdf_path.parent / "document.md"
        if not pdf_path.is_file():
            raise FileNotFoundError(pdf_path)
        if not markdown_path.is_file():
            raise FileNotFoundError(markdown_path)
        with fitz.open(str(pdf_path)) as pdf:
            page_count = int(pdf.page_count)
        if selected[side] and max(selected[side]) > page_count:
            raise ValueError(f"{side} page outside document")
        index = list((sheet_indexes or {}).get(side) or [])
        if not index:
            index = sheet_matching.placeholder_sheet_index(page_count)
        if document_cache_dir is not None:
            extracted, cache_signature = load_or_extract_document_fragments(
                stage=stage,
                document=document,
                markdown_path=markdown_path,
                pdf_path=pdf_path,
                sheet_index=index,
                fitz=fitz,
                cache_dir=Path(document_cache_dir),
                generated_at=generated_at,
            )
            document_cache_signatures[side.upper()] = cache_signature
        else:
            extracted = text_comparison.extract_document_fragments(
                stage=stage,
                markdown_path=markdown_path,
                pdf_path=pdf_path,
                sheet_index=index,
                fitz=fitz,
                selected_pages=selected[side],
            )
        fragments[side] = split_two_up_table_rows(sorted(
            (
                fragment for fragment in extracted
                if int(fragment["pdf_page"]) in selected[side]
                and not text_differences.is_graphic_description(fragment)
            ),
            key=lambda fragment: (
                int(fragment["pdf_page"]),
                int(fragment.get("order") or 0),
                str(fragment["id"]),
            ),
        ))
        native_fallback[side.upper()] = _append_native_fallback(
            fragments[side],
            pdf_path=pdf_path,
            pages=selected[side],
            side=side,
            fitz=fitz,
        )
        documents[side.upper()] = {
            "pdf": file_content_identity(pdf_path),
            "markdown": file_content_identity(markdown_path),
            "version_id": document.get("version_id"),
        }
        recognition_index[side.upper()] = recognition_coverage.build_native_index(
            str(pdf_path), selected[side], fitz=fitz,
        )
    input_signature = content_signature({
        "producer": PREPARATION_VERSION,
        "pair_id": pair.get("id"),
        "groups": groups,
        "documents": documents,
    })
    return {
        "kind": PREPARATION_KIND,
        "schema_version": PREPARATION_SCHEMA_VERSION,
        "version": 1,
        "pair_id": pair.get("id"),
        "direction": "LEFT_TO_RIGHT",
        "input_signature": input_signature,
        "generated_at": generated_at or utc_now(),
        "comparison_groups": groups,
        "fragments": fragments,
        "documents": documents,
        # Индекс не входит в input_signature подготовки намеренно: подпись
        # описывает, ЧТО сравнивается (пара, области, документы), а не чем это
        # проверяется. Иначе появление проверки протухило бы каждый уже
        # сохранённый прогон, не изменив ни одного сравниваемого текста.
        "recognition_index": recognition_index,
        "recognition_contract_version": recognition_coverage.CONTRACT_VERSION,
        # Происхождение единиц: где сработал резерв нативного слоя и на
        # скольких единицах. Как и индекс полноты, в подпись подготовки не
        # входит — это описание того, ЧЕМ прочитано, а не ЧТО сравнивается.
        "fragment_sources": native_fallback,
        "extraction": {
            "mode": (
                "DOCUMENT_CACHE"
                if document_cache_dir is not None
                else "SCOPED_PAGES"
            ),
            "selected_pages": {
                side.upper(): sorted(pages) for side, pages in selected.items()
            },
            "document_cache_signatures": document_cache_signatures,
        },
        "constraints": {
            "uses_model": False,
            "parent_relation_required": False,
            "sheet_matcher_is_gate": False,
        },
    }


def build_text_differences_from_preparation(
    preparation: Mapping[str, Any],
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    if (
        preparation.get("kind") != PREPARATION_KIND
        or preparation.get("schema_version") != PREPARATION_SCHEMA_VERSION
    ):
        raise ValueError("production text preparation artifact required")
    fragments = preparation.get("fragments")
    if not isinstance(fragments, Mapping):
        raise ValueError("text preparation fragments required")
    left_all = list(fragments.get("left") or [])
    right_all = list(fragments.get("right") or [])
    groups = []
    totals = {
        "sheet_groups_with_differences": 0,
        "changed": 0,
        "removed": 0,
        "added": 0,
        "model_ambiguity": 0,
        "model_failures": 0,
    }
    for group in normalize_comparison_groups(preparation.get("comparison_groups") or []):
        left_pages, right_pages = set(group["left_pages"]), set(group["right_pages"])
        result = text_differences.compare_group(
            [item for item in left_all if int(item["pdf_page"]) in left_pages],
            [item for item in right_all if int(item["pdf_page"]) in right_pages],
        )
        if not any(result[bucket] for bucket in ("changed", "removed", "added")):
            continue
        groups.append({
            "id": group["id"],
            "left_pages": group["left_pages"],
            "right_pages": group["right_pages"],
            "left_labels": [f"Страница {page}" for page in group["left_pages"]],
            "right_labels": [f"Страница {page}" for page in group["right_pages"]],
            "relation_type": group["relation_type"],
            "relation_status": group["relation_status"],
            "changed": result["changed"],
            "removed": result["removed"],
            "added": result["added"],
            "deterministic_same": result["same"],
            "deterministic_ambiguities": result["ambiguous"],
            "ambiguity_count": result["ambiguity_count"],
            "exact_equivalents": result["exact_equivalents"],
        })
        totals["sheet_groups_with_differences"] += 1
        for bucket in ("changed", "removed", "added"):
            totals[bucket] += len(result[bucket])
        totals["model_ambiguity"] += int(result["ambiguity_count"])
    artifact = {
        "version": text_differences.VERSION,
        "kind": text_differences.KIND,
        "pair_id": preparation.get("pair_id"),
        "algorithm": "production_scope_" + text_differences.ALGORITHM,
        "production_path": "STAGE_2_3_DIRECT_SCOPE",
        "generated_at": generated_at or utc_now(),
        "source_signature": content_signature({
            "preparation": preparation.get("input_signature"),
            "algorithm": text_differences.ALGORITHM,
        }),
        "sheet_groups": groups,
        "summary": totals,
        "model": {"used": False, "failures": 0, "reason": "deterministic_production_flow"},
        "constraints": {
            "factual_differences_only": True,
            "graphics_analyzed": False,
            "engineering_findings_created": False,
            "one_row_per_sheet_group": False,
            "parent_relation_required": False,
            "sheet_matcher_is_gate": False,
        },
    }
    # Полнота считается ПОСЛЕ корзин, потому что вердикт по расхождению
    # зависит от того, что именно объявлено удалённым или добавленным.
    # Ключ верхнего уровня: stage3_content_signature покрывает элементы
    # доказательств, а не оболочку артефакта, поэтому добавление проверки не
    # объявляет протухшей ни одну сохранённую валидацию Stage 4.
    artifact["recognition_coverage"] = (
        recognition_coverage.build_recognition_coverage(preparation, artifact)
    )
    return artifact


__all__ = [
    "PREPARATION_KIND",
    "PREPARATION_SCHEMA_VERSION",
    "PREPARATION_VERSION",
    "build_text_differences_from_preparation",
    "normalize_comparison_groups",
    "prepare_text_scope",
]
