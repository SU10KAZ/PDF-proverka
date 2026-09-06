"""Production orchestration for additive Stage Comparison.

This is the only new-flow coordinator.  It keeps literal LEFT -> RIGHT input,
runs TEXT and GRAPHIC independently, adapts their atomic facts into the
accepted G2.4.5/G2.4.6 modules, and persists review/final-report artifacts.
The legacy Stage 5 and Stage 5.3 services are intentionally not imported.
"""
from __future__ import annotations

import copy
import importlib
import os
import re
import threading
import time
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence
from uuid import uuid4

from backend.app.services.common.blocks_json import load_blocks_json

from . import (
    function_lineage_shadow,
    production_store,
    sheet_matching,
    sheet_scope_policy,
    store,
)
from .ai import gateway as ai_gateway
from .ai import resolution as ai_resolution
from .ai import routing as ai_routing
from .ai import settings as ai_settings
from .ai_v2 import settings as ai_v2_settings
from .ai_v2.engine import WholeDocumentAnalyst
from .ai_v2.materialization import materialize_verified_resolutions
from .ai_v31 import settings as ai_question_closure_settings
from .ai_v31.production import (
    failure_artifact as question_closure_failure_artifact,
    fast_signature as question_closure_fast_signature,
    run_production_question_closure,
)
from .engineer_review import build_engineer_decisions, build_final_report, review_rows
from .human_review_orchestrator import (
    blocked_target_id,
    build_human_review_plan,
    build_human_review_view,
    empty_human_review_decisions,
    mode_target_id,
    unproven_target_id,
    update_human_review_decisions as apply_human_review_decision_updates,
)
from .preliminary_report import (
    build_preliminary_report,
    change_is_review,
    describe_change,
)
from .evidence_navigation import (
    build_evidence_availability_index,
    build_evidence_navigation,
    build_inline_evidence_navigation,
)
from backend.app.pipeline.stages.block_grounding.electrical_table_diff import (
    compare_match as compare_electrical_match,
)
from .graphic_comparison.mode2 import (
    DirectPageComparisonError,
    compare_selected_pages,
    validate_direct_page_comparison_result,
)
from .production_artifacts import (
    content_signature,
    file_content_identity,
    stable_id,
    utc_now,
)
from .production_text_flow import (
    build_text_differences_from_preparation,
    prepare_text_scope,
)
from .production_text_evidence import (
    ProductionTextEvidenceConflictError,
    build_production_text_evidence,
    empty_production_text_evidence,
    evidence_is_publishable,
)
from .sheet_content_fingerprint import (
    build_sheet_content_fingerprint,
    has_meaningful_content,
)
from .function_lineage_source import extract_page_sources as extract_function_lineage_sources
from .sheet_identity import extract_sheet_identities
from .sheet_matcher import match_sheets, page_selection_suggestions
from . import sheet_matcher_flags
from . import sheet_passport
from .text_atom_builder import (
    BUILDER_VERSION as TEXT_ATOM_BUILDER_VERSION,
    KIND as TEXT_ATOMS_KIND,
    SCHEMA_VERSION as TEXT_ATOMS_SCHEMA_VERSION,
    build_text_atoms,
)
from .text_fact_producer import (
    PRODUCER_VERSION as TEXT_FACT_PRODUCER_VERSION,
    produce_text_facts,
)
from .text_semantic_validation import (
    KIND as SEMANTIC_KIND,
    SCHEMA_VERSION as SEMANTIC_SCHEMA_VERSION,
    build_semantic_validation,
    stage3_content_signature,
)
from .unified_change_synthesizer import (
    canonical_synthesis_digest,
    ledger_to_graphic_atoms,
    synthesize_unified_changes,
    validate_synthesis,
)
from .unified_change_synthesizer.normalization import (
    load_table_diff_to_graphic_atoms,
)
from .unified_entity_bridge.document_binding import (
    document_identity_is_complete,
    pair_documents_from_pair_artifact,
)


STATE_KIND = "stage_comparison_production_state"
STATE_SCHEMA_VERSION = "production-comparison-state.v1"
CHANGES_KIND = "stage_comparison_production_changes"
CHANGES_SCHEMA_VERSION = "production-changes.v1"
QUESTIONS_KIND = "stage_comparison_review_questions"
QUESTIONS_SCHEMA_VERSION = "review-questions.v1"
ANSWERS_KIND = "stage_comparison_review_answers"
ANSWERS_SCHEMA_VERSION = "review-answers.v1"
INPUT_MODES = frozenset({"PAGE", "DOCUMENT"})
PUBLISHED_STATUSES = frozenset({"COMPLETED", "PARTIAL"})
ACTIVE_RUN_STATUSES = frozenset({"RUNNING", "UPDATING"})
#: Отменённый прогон — не упавший. Разница видна инженеру и в журнале: у
#: отказа есть причина в коде, у отмены — человек, который её нажал.
CANCELLED_STATUS = "CANCELLED"
PAGE_MATERIALIZING_ACTIONS = frozenset({
    "REPLACE",
    "COMPARE_ADDITIONALLY",
    "ADD_TO_GROUP",
})
SOURCE_SNAPSHOT_KIND = "stage_comparison_production_source_snapshot"
SOURCE_SNAPSHOT_SCHEMA_VERSION = "production-source-snapshot.v1"
PAGE_GRAPHIC_BUNDLE_KIND = "stage_comparison_page_graphic_bundle"
PAGE_GRAPHIC_BUNDLE_SCHEMA_VERSION = "page-graphic-bundle.v1"
DOCUMENT_GRAPHIC_BUNDLE_KIND = "stage_comparison_document_graphic_bundle"
DOCUMENT_GRAPHIC_BUNDLE_SCHEMA_VERSION = "document-graphic-bundle.v1"
DOCUMENT_GRAPHIC_GROUP_STATUSES = frozenset({
    "COMPLETED",
    "NOT_APPLICABLE",
    "REVIEW_REQUIRED",
    "CHECK_BLOCKED",
})
PROGRESS_ACTIVITY_WARNING_ENV = (
    "STAGE_COMPARISON_ACTIVITY_WARNING_THRESHOLD_SEC"
)
DEFAULT_PROGRESS_ACTIVITY_WARNING_SEC = 120
_TEXT_PROGRESS_CALLBACK: ContextVar[Callable[..., None] | None] = ContextVar(
    "stage_comparison_text_progress_callback",
    default=None,
)
_GRAPHIC_PROGRESS_CALLBACK: ContextVar[Callable[..., None] | None] = ContextVar(
    "stage_comparison_graphic_progress_callback",
    default=None,
)
#: Ручка текущего прогона для кода, который её не получает аргументом.
_RUN_CONTROL: ContextVar[Any] = ContextVar(
    "stage_comparison_run_control", default=None
)


class ProductionStateConflictError(production_store.ProductionConflictError):
    """A human write targets stale comparison sources or a stale revision."""


class ProductionProgressPublicationError(Exception):
    """Progress persistence failed; never classify it as branch evidence."""


def _fitz():
    try:
        import fitz
    except ImportError as exc:  # pragma: no cover - production dependency
        raise RuntimeError("PyMuPDF is required") from exc
    return fitz


def _positive_pages(values: Iterable[Any], side: str) -> list[int]:
    pages: list[int] = []
    for value in values:
        if not isinstance(value, int) or isinstance(value, bool) or value < 1:
            raise ValueError(f"{side}_pages must contain positive integers")
        pages.append(value)
    if len(pages) != len(set(pages)):
        raise ValueError(f"duplicate_{side}_page")
    return sorted(pages)


def _block_ids(values: Iterable[Any], side: str) -> list[str]:
    output = []
    for value in values:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{side}_block_ids must contain non-empty strings")
        output.append(value.strip())
    if len(output) != len(set(output)):
        raise ValueError(f"duplicate_{side}_block_id")
    return sorted(output)


#: Ключи запроса, описывающие ИСХОДНЫЕ ДАННЫЕ прогона: какие документы, какие
#: страницы, какая область сравнения. Только они отвечают на вопрос «изменился
#: ли вход» — и только они входят в подпись источников.
SOURCE_REQUEST_KEYS = (
    "input_mode",
    "left_pages",
    "right_pages",
    "left_block_ids",
    "right_block_ids",
)
#: Ключи запроса, описывающие КОНФИГУРАЦИЮ АНАЛИЗА: в каком режиме посчитан
#: результат. Это отдельная ось: смена режима по умолчанию не меняет ни PDF, ни
#: версии документов, ни выбор листов, — а значит не имеет права объявить
#: прежние прогоны устаревшими.
ANALYSIS_CONFIG_KEYS = ("ai_mode",)

# Read-only export ownership for the eight user-facing stages.  The export
# endpoint reads these already-persisted artifacts; it never starts a producer
# or reshapes an algorithm result.
PRODUCTION_STAGE_RESULT_ARTIFACTS: dict[str, tuple[str, ...]] = {
    "selection": (),
    "sheets": ("sheet_relations",),
    "content": (
        "text_preparation",
        "text_differences",
        "text_fact_production",
        "text_semantic_validation",
        "text_atoms",
        "graphic_ledger",
        "page_graphic_bundle",
        "document_graphic_bundle",
        "direct_page_mode2",
        "electrical_table_changes",
        "document_inconsistencies",
    ),
    "objects": ("entity_relations", "bound_atoms", "effective_bound_atoms"),
    "questions": (
        "review_questions",
        "review_answers",
        "human_review_plan",
        "human_review_decisions",
        "ai_question_closure",
    ),
    "synthesis": (
        "automatic_unified_synthesis",
        "review_application",
        "unified_synthesis",
        "ai_routing_inventory",
        "ai_resolutions",
        "ai_table_identity",
        "ai_v2_run",
        "ai_v2_materialization",
        "preliminary_report",
    ),
    "review": ("engineer_decisions",),
    "report": ("final_report",),
}
PRODUCTION_STAGE_RESULT_STATE_KEYS: dict[str, tuple[str, ...]] = {
    "selection": (),
    "sheets": ("sheet_matching", "sheet_scope"),
    "content": ("text", "graphic", "source_snapshot"),
    "objects": ("entity_matching", "entity_binding", "effective_entity_binding"),
    "questions": ("review_questions", "question_closure", "human_review"),
    "synthesis": (
        "automatic_unified_synthesis",
        "review_application",
        "unified_synthesis",
        "ai_resolution",
        "preliminary_report",
    ),
    "review": ("engineer_decisions",),
    "report": ("final_report",),
}
PRODUCTION_STAGE_RESULT_INPUT_ARTIFACTS: dict[str, tuple[str, ...]] = {
    "selection": (),
    "sheets": (),
    "content": ("sheet_relations",),
    "objects": ("text_atoms", "graphic_ledger"),
    "questions": ("sheet_relations", "entity_relations", "unified_synthesis"),
    "synthesis": (
        "text_atoms",
        "graphic_ledger",
        "entity_relations",
        "review_answers",
        "human_review_decisions",
    ),
    "review": ("review_questions", "human_review_plan", "unified_synthesis"),
    "report": ("unified_synthesis", "engineer_decisions", "human_review_decisions"),
}
PRODUCTION_STAGE_RESULT_LABELS = {
    "selection": (1, "Выбор сравнения"),
    "sheets": (2, "Сопоставление листов"),
    "content": (3, "Анализ содержимого"),
    "objects": (4, "Сопоставление объектов"),
    "questions": (5, "Вопросы инженеру"),
    "synthesis": (6, "Синтез изменений"),
    "review": (7, "Проверка инженером"),
    "report": (8, "Итоговый отчёт"),
}
_STAGE_EXPORT_SECRET_KEYS = frozenset({
    "api_key",
    "authorization",
    "auth_token",
    "access_token",
    "refresh_token",
    "password",
    "passwd",
    "secret",
    "client_secret",
    "private_key",
    "cookie",
    "session_cookie",
    "token",
})
_STAGE_EXPORT_BINARY_KEYS = frozenset({
    "binary",
    "binary_data",
    "blob",
    "bytes",
    "image_base64",
    "pdf_base64",
    "raster_base64",
    "raw_image",
})
_STAGE_EXPORT_EVIDENCE_KEYS = frozenset({
    "evidence",
    "evidence_ref",
    "evidence_refs",
    "source_evidence",
    "provenance",
    "sources",
    "locations",
    "source_artifact",
})
_STAGE_EXPORT_REASON_KEYS = frozenset({
    "reason",
    "reasons",
    "reason_code",
    "reason_codes",
    "review_reason",
    "skip_reason",
    "failure_reason",
    "error",
    "errors",
    "human_reasons",
    "blocked_reason",
    "not_applicable_reason",
    "fallback_message",
    "failure",
    "skip",
    "skipped",
    "review_required",
    "not_applicable",
    "blocked",
    "fallback_used",
})
_STAGE_EXPORT_SERVER_PATH = re.compile(
    r"(?<![A-Za-z0-9:])/(?:home|tmp|var|srv|opt|root|mnt|usr)(?:/[^\s,;)\]}]+)+"
)
_STAGE_EXPORT_BEARER = re.compile(
    r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]{8,}"
)
_STAGE_EXPORT_INLINE_SECRET = re.compile(
    r"(?i)\b(api[_-]?key|access[_-]?token|refresh[_-]?token|auth[_-]?token|token|password|secret)"
    r"(\s*[:=]\s*)([^\s,;&]+)"
)
_STAGE_EXPORT_BASE64 = re.compile(r"[A-Za-z0-9+/=\r\n]+")


def normalize_run_request(
    *,
    input_mode: str,
    left_pages: Iterable[Any] = (),
    right_pages: Iterable[Any] = (),
    left_block_ids: Iterable[Any] = (),
    right_block_ids: Iterable[Any] = (),
    ai_mode: str | None = None,
) -> dict[str, Any]:
    mode = str(input_mode or "").upper()
    if mode not in INPUT_MODES:
        raise ValueError("input_mode must be PAGE or DOCUMENT")
    request = {
        "input_mode": mode,
        "left_pages": _positive_pages(left_pages, "left"),
        "right_pages": _positive_pages(right_pages, "right"),
        "left_block_ids": _block_ids(left_block_ids, "left"),
        "right_block_ids": _block_ids(right_block_ids, "right"),
        # Глубина анализа — параметр ЭТОГО прогона, а не состояние машины.
        # Клиент присылает пожелание, разрешает его сервер: «глубокая проверка»
        # на установке без критика означала бы тихую деградацию.
        "ai_mode": ai_settings.run_mode_label(
            ai_settings.resolve_run_mode(ai_mode)
        ),
    }
    if mode == "PAGE":
        if len(request["left_pages"]) != 1 or len(request["right_pages"]) != 1:
            raise ValueError("PAGE mode requires exactly one LEFT and one RIGHT page")
    elif request["left_pages"] or request["right_pages"]:
        raise ValueError("DOCUMENT mode resolves pages through Sheet Matcher")
    return request


def source_request(request: Mapping[str, Any]) -> dict[str, Any]:
    """Часть запроса, отвечающая на вопрос «изменился ли вход?».

    Ровно те ключи, что были в запросе до появления режимов глубины, — поэтому
    подпись прогона, сделанного тогда, пересчитывается в прежнее значение, а не
    объявляется устаревшей из-за нового поля.
    """
    return {
        key: copy.deepcopy(request[key])
        for key in SOURCE_REQUEST_KEYS
        if key in request
    }


def analysis_config(request: Mapping[str, Any]) -> dict[str, Any]:
    """Часть запроса, отвечающая на вопрос «в каком режиме это посчитано?».

    Отсутствие режима сохраняется как отсутствие: прогон, выполненный до
    появления режимов глубины, обязан читаться как «режим не записан», а не как
    «Быстро». Придуманный задним числом режим — неверный аудитный след.
    """
    stored = request.get("ai_mode") if isinstance(request, Mapping) else None
    return {
        "ai_mode": ai_settings.run_mode_label(stored) if stored else None,
        "recorded": bool(stored),
    }


def analysis_config_signature(request: Mapping[str, Any]) -> str:
    """Подпись конфигурации анализа — отдельная от подписи исходных данных."""
    return content_signature({
        "flow": "stage-comparison-analysis-config-v1",
        "config": analysis_config(request),
    })


def restore_selection(selection: Mapping[str, Any]) -> dict[str, Any]:
    """Прочитать сохранённый выбор прогона, не применяя сегодняшнюю политику.

    Прогон уже состоялся. Если установке позже запретили «глубокую проверку»,
    прежний результат не перестаёт относиться к своим документам: политика
    сервера ограничивает ЗАПУСК анализа, а не чтение уже посчитанного.
    """
    normalized = normalize_run_request(
        input_mode=selection.get("input_mode"),
        left_pages=selection.get("left_pages") or (),
        right_pages=selection.get("right_pages") or (),
        left_block_ids=selection.get("left_block_ids") or (),
        right_block_ids=selection.get("right_block_ids") or (),
    )
    stored = selection.get("ai_mode")
    if stored:
        normalized["ai_mode"] = ai_settings.run_mode_label(stored)
    else:
        normalized.pop("ai_mode", None)
    return normalized


def _resolved_document_paths(document: Mapping[str, Any]) -> dict[str, Path]:
    """Resolve the exact configured-or-fallback inputs read by producers."""
    pdf = Path(str(document.get("pdf_path") or ""))
    markdown = Path(str(document.get("md_path") or ""))
    if not markdown.is_file():
        markdown = pdf.parent / "document.md"
    html = Path(str(document.get("html_path") or ""))
    if not html.is_file():
        html = pdf.parent / "ocr.html"
    return {
        "pdf": pdf,
        "markdown": markdown,
        "html": html,
        "blocks": pdf.parent / "blocks.json",
    }


def _input_signature(
    pair: Mapping[str, Any],
    request: Mapping[str, Any],
    *,
    page_groups: Iterable[Mapping[str, Any]] | None = None,
) -> str:
    documents: dict[str, Any] = {}
    for side in ("left", "right"):
        document = pair.get(side) or {}
        resolved = _resolved_document_paths(document)
        documents[side] = {
            key: file_content_identity(path)
            for key, path in resolved.items()
        } | {
            "document_code": document.get("document_code"),
            "version_id": document.get("version_id"),
        }
    signature_payload = {
        "flow": "stage-comparison-production-v1",
        "pair_id": pair.get("id"),
        # ТОЛЬКО исходные данные. Конфигурация анализа (глубина ИИ) живёт в
        # отдельной подписи: иначе смена режима по умолчанию объявляла бы
        # устаревшими прогоны, у которых не изменились ни PDF, ни версии
        # документов, ни выбор сторон, ни область сравнения.
        "request": source_request(request),
        "documents": documents,
    }
    if page_groups is not None:
        signature_payload["page_scope"] = {
            "groups": copy.deepcopy(list(page_groups)),
        }
    return content_signature(signature_payload)


#: Причина, по которой опубликованный прогон перестал быть текущим.
STALE_SOURCES_CHANGED = "SOURCES_CHANGED"
STALE_MANUAL_PAGE_PAIRING_CHANGED = "MANUAL_PAGE_PAIRING_CHANGED"


def manual_page_pairing(session_id: str, pair_id: str) -> dict[str, Any] | None:
    """Отпечаток РУЧНОЙ пары страниц — отдельная ось исходных данных.

    Когда человек сам собрал пару листов, он задал область сравнения. Прогон,
    посчитанный до этого, относится к другой области, и выдавать его за
    текущий нельзя.  В отпечаток идёт только ручная часть связей:
    автоматические подсказки — производная расчёта, а не ввод человека, и
    их пересчёт не должен объявлять прогон устаревшим.

    Возвращается ``None``, если ручной пары нет вовсе.
    """
    try:
        links = store.load_sheet_links(session_id, pair_id)
    except (OSError, ValueError, KeyError):
        # Нечитаемая связка — не доказательство изменения. Молча объявлять
        # прогон устаревшим по ошибке чтения хуже, чем не заметить правку.
        return None
    if not isinstance(links, Mapping):
        return None
    manual = [
        {
            "id": str(link.get("id") or ""),
            "left_pages": sorted(
                int(page) for page in link.get("left_pages") or []
            ),
            "right_pages": sorted(
                int(page) for page in link.get("right_pages") or []
            ),
        }
        for link in links.get("links") or []
        if isinstance(link, Mapping) and str(link.get("source") or "") == "manual"
    ]
    unlinked = sorted(
        int(page) for page in links.get("unlinked_left_pages") or []
    )
    if not manual and not unlinked:
        return None
    manual.sort(key=lambda link: (link["left_pages"], link["right_pages"], link["id"]))
    return {
        "digest": content_signature({
            "axis": "manual-page-pairing-v1",
            "links": manual,
            "unlinked_left_pages": unlinked,
        }),
        "updated_at": links.get("updated_at"),
    }


def _manual_pairing_stale_reason(
    session_id: str, pair_id: str, state: Mapping[str, Any]
) -> str | None:
    """Сверить ручную пару страниц прогона с той, что действует сейчас."""
    if str(state.get("input_mode") or "") != "PAGE":
        return None
    current = manual_page_pairing(session_id, pair_id)
    current_digest = current["digest"] if current else None
    source_scope = state.get("source_scope")
    if isinstance(source_scope, Mapping) and "manual_page_pairing" in source_scope:
        recorded = source_scope.get("manual_page_pairing")
        if recorded == current_digest:
            return None
        return STALE_MANUAL_PAGE_PAIRING_CHANGED
    # Прогон старше самой оси: записанного отпечатка нет. Объявлять устаревшим
    # всё подряд нельзя — это ровно та ошибка, из-за которой появление режимов
    # глубины однажды обнулило все прошлые прогоны. Единственное доступное
    # доказательство правки — время: человек тронул связки ПОСЛЕ прогона.
    if current is None:
        return None
    changed_at = _parse_timestamp(current.get("updated_at"))
    started_at = _parse_timestamp(state.get("started_at"))
    if changed_at is None or started_at is None:
        return None
    if changed_at > started_at:
        return STALE_MANUAL_PAGE_PAIRING_CHANGED
    return None


def _parse_timestamp(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or ""))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _page_count(pdf_path: Path) -> int:
    with _fitz().open(str(pdf_path)) as document:
        return int(document.page_count)


def _validate_page_bounds(
    pair: Mapping[str, Any],
    request: Mapping[str, Any],
    comparison_groups: Iterable[Mapping[str, Any]] | None = None,
) -> None:
    if request["input_mode"] != "PAGE":
        return
    for side in ("left", "right"):
        document = pair.get(side) or {}
        pdf_path = Path(str(document.get("pdf_path") or ""))
        count = _page_count(pdf_path)
        pages = set(request[f"{side}_pages"])
        for group in comparison_groups or []:
            pages.update(int(page) for page in group.get(f"{side}_pages") or [])
        if pages and max(pages) > count:
            raise ValueError(f"{side}_page_out_of_range")


def _production_sheet_indexes(
    pair: Mapping[str, Any],
    *,
    with_sheet_identity: bool = True,
    sheet_matcher_v4: bool | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Read compact existing OCR/index facts without running a comparator.

    ``sheet_matcher_v4`` selects the v4 index (stamp axis rule + sheet
    passport from the Markdown page body); ``None`` reads the feature flag.
    With v3 the records are byte-identical to the pre-flag production index.
    """
    if sheet_matcher_v4 is None:
        sheet_matcher_v4 = sheet_matcher_flags.v4_enabled()
    indexes: dict[str, list[dict[str, Any]]] = {}
    for side in ("left", "right"):
        document = pair.get(side) or {}
        resolved = _resolved_document_paths(document)
        pdf_path = resolved["pdf"]
        if not pdf_path.is_file():
            raise FileNotFoundError(pdf_path)
        html_path = resolved["html"]
        records: list[dict[str, Any]] = []
        if html_path.is_file():
            try:
                records = sheet_matching.extract_sheet_index_from_results_html(
                    html_path.read_text(encoding="utf-8")
                )
            except (OSError, UnicodeDecodeError):
                records = []
        count = _page_count(pdf_path)
        by_page = {int(item["pdf_page"]): dict(item) for item in records}
        placeholders = sheet_matching.placeholder_sheet_index(count)
        records = [
            dict(by_page.get(page, placeholder))
            for page, placeholder in enumerate(placeholders, 1)
        ]
        markdown_path = resolved["markdown"]
        semantics: dict[int, str] = {}
        lineage_sources: dict[int, dict[str, Any]] = {}
        markdown = ""
        if markdown_path.is_file():
            try:
                markdown = markdown_path.read_text(encoding="utf-8")
                semantics = sheet_matching.extract_page_semantics_from_markdown(markdown)
            except (OSError, UnicodeDecodeError):
                semantics = {}
                markdown = ""
            else:
                try:
                    lineage_sources = extract_function_lineage_sources(markdown)
                except Exception:  # noqa: BLE001 - never affect Sheet Matcher v3
                    lineage_sources = {}
        # The sheet states its own identity in the stamp, and that line is in
        # the PDF text layer of both sides in directly comparable form.  It is
        # read here, once per side, from the file already opened for the page
        # count: ~1.5 s for a whole set, no model, no OCR.  A document whose
        # text layer cannot be read loses only its identities, not the run.
        identities = {}
        if with_sheet_identity:
            try:
                identities = extract_sheet_identities(
                    str(pdf_path), axis_preposition=sheet_matcher_v4,
                )
            except Exception:  # noqa: BLE001 - identity is an optional signal
                identities = {}
        for record in records:
            page = int(record["pdf_page"])
            lineage_source = lineage_sources.get(page)
            if lineage_source is not None:
                # Sheet Matcher v3 ignores this namespaced field.  It is a
                # compact, deterministic Function Lineage input only.
                record["function_lineage_source"] = lineage_source
            identity = identities.get(page)
            if identity is not None:
                record["sheet_identity"] = identity.to_dict()
            semantic = semantics.get(page)
            if not semantic:
                continue
            fingerprint = build_sheet_content_fingerprint(
                semantic,
                title=str(record.get("title") or ""),
            )
            if has_meaningful_content(fingerprint):
                record["content_fingerprint"] = fingerprint
        # Sheet Matcher v4: паспорт листа из тела страницы Markdown
        # (sheet-passport.v1).  Строки Summary/Entities есть у ~28 % страниц
        # корпуса; тело страницы — у всех.  Только положительные факты,
        # общедокументные термины удалены по частоте.  Сбой паспорта не
        # роняет прогон: лист просто остаётся с тем, что у него уже есть.
        if sheet_matcher_v4 and markdown:
            try:
                passports = sheet_passport.build_passports(
                    sheet_passport.page_bodies_from_markdown(markdown),
                    titles={
                        int(item["pdf_page"]): item.get("title") for item in records
                    },
                )
                sheet_passport.extend_sheet_index(
                    records, passports, source="MARKDOWN_BODY", mode="MERGE",
                )
            except Exception:  # noqa: BLE001 - passport is an optional positive signal
                pass
        indexes[side] = records
    return indexes


def _run_sheet_matcher(
    pair: Mapping[str, Any],
    *,
    algorithm: str | None = None,
) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    """Index and match with ONE algorithm: the index and the matcher agree."""
    algorithm = sheet_matcher_flags.resolve_algorithm(algorithm)
    v4 = algorithm == sheet_matcher_flags.ALGORITHM_V4
    indexes = _production_sheet_indexes(pair, sheet_matcher_v4=v4)
    return (
        match_sheets(indexes["left"], indexes["right"], algorithm=algorithm),
        indexes,
    )


SHEET_MATCHER_V4_SHADOW_KIND = "stage_comparison_sheet_matcher_v4_shadow"
SHEET_MATCHER_V4_SHADOW_SCHEMA_VERSION = "sheet-matcher-v4-shadow.v1"
SHEET_MATCHER_V4_SHADOW_DISABLED = "SHADOW_DISABLED"
SHEET_MATCHER_V4_SHADOW_V4_IS_PRODUCTION = "V4_IS_PRODUCTION"
SHEET_MATCHER_V4_SHADOW_PAIR_NOT_ALLOWED = "PAIR_NOT_ALLOWED"
SHEET_MATCHER_V4_SHADOW_RUN_NOT_ALLOWED = "RUN_NOT_ALLOWED"
SHEET_MATCHER_V4_SHADOW_EXECUTED = "SHADOW_EXECUTED"
SHEET_MATCHER_V4_SHADOW_FAILED = "SHADOW_FAILED"


def _sheet_matcher_v4_shadow_gate(
    *, pair_id: str, run_id: str, input_mode: str,
) -> dict[str, Any]:
    """Fail-closed gate of the v4 shadow; allowlist identifiers are not exposed.

    The shadow exists to compare v4 against the production v3 on the same
    pair, so it only runs when v3 is the production algorithm, only for
    DOCUMENT scope (PAGE scope is the user's own selection) and only for an
    explicitly allowlisted pair or run.  Empty allowlists mean nobody.
    """
    armed = sheet_matcher_flags.shadow_enabled()
    v4_production = sheet_matcher_flags.v4_enabled()
    document_mode = str(input_mode) == "DOCUMENT"
    pair_allowlist = sheet_matcher_flags.shadow_pair_allowlist()
    run_allowlist = sheet_matcher_flags.shadow_run_allowlist()
    pair_allowed = str(pair_id) in pair_allowlist
    run_allowed = str(run_id) in run_allowlist
    allowed = (
        armed and not v4_production and document_mode
        and (pair_allowed or run_allowed)
    )
    reason: str | None = None
    if not armed or not document_mode or (not pair_allowlist and not run_allowlist):
        reason = SHEET_MATCHER_V4_SHADOW_DISABLED
    elif v4_production:
        reason = SHEET_MATCHER_V4_SHADOW_V4_IS_PRODUCTION
    elif not allowed:
        reason = (
            SHEET_MATCHER_V4_SHADOW_PAIR_NOT_ALLOWED
            if pair_allowlist
            else SHEET_MATCHER_V4_SHADOW_RUN_NOT_ALLOWED
        )
    return {
        "allowed": allowed,
        "diagnostic_reason": reason,
        "feature_enabled": armed,
        "v4_is_production": v4_production,
        "document_mode": document_mode,
        "pair_allowlist_configured": bool(pair_allowlist),
        "run_allowlist_configured": bool(run_allowlist),
        "pair_allowed": pair_allowed,
        "run_allowed": run_allowed,
        "executed": False,
    }


def _sheet_relation_status_by_left_page(
    sheet_relations: Mapping[str, Any],
) -> dict[int, str]:
    statuses: dict[int, str] = {}
    for relation in sheet_relations.get("relations") or []:
        if not relation.get("left_pages"):
            continue
        two_sided = bool(relation.get("right_pages"))
        label = str(relation.get("status") or "UNKNOWN")
        for page in relation["left_pages"]:
            statuses[int(page)] = label if two_sided else f"UNMATCHED_{label}"
    return statuses


def build_sheet_matcher_v4_shadow(
    *,
    pair_id: str,
    run_id: str,
    production_sheet_relations: Mapping[str, Any],
    shadow_sheet_relations: Mapping[str, Any],
    gate: Mapping[str, Any],
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Diagnostic artifact: v4 next to the production v3 on the same pair.

    Carries the whole v4 result so the downstream analysis (questions,
    review noise) can be replayed offline without a second full comparison.
    Nothing here is read by the production flow.
    """
    production_status = _sheet_relation_status_by_left_page(production_sheet_relations)
    shadow_status = _sheet_relation_status_by_left_page(shadow_sheet_relations)
    transitions: dict[str, int] = {}
    for page in sorted(set(production_status) | set(shadow_status)):
        key = f"{production_status.get(page, 'ABSENT')}->{shadow_status.get(page, 'ABSENT')}"
        transitions[key] = transitions.get(key, 0) + 1
    return {
        "kind": SHEET_MATCHER_V4_SHADOW_KIND,
        "schema_version": SHEET_MATCHER_V4_SHADOW_SCHEMA_VERSION,
        "pair_id": pair_id,
        "run_id": run_id,
        "generated_at": generated_at or utc_now(),
        "shadow_status": "COMPLETED",
        "diagnostic_reason": SHEET_MATCHER_V4_SHADOW_EXECUTED,
        "gate": dict(gate),
        "uses_model": False,
        "affects_production": False,
        "production": {
            "algorithm_version": production_sheet_relations.get("algorithm_version"),
            "input_signature": production_sheet_relations.get("input_signature"),
            "relation_counts": _sheet_relation_counts(production_sheet_relations),
        },
        "shadow": {
            "algorithm_version": shadow_sheet_relations.get("algorithm_version"),
            "input_signature": shadow_sheet_relations.get("input_signature"),
            "relation_counts": _sheet_relation_counts(shadow_sheet_relations),
            "ambiguous_high_demoted": (
                shadow_sheet_relations.get("diagnostics") or {}
            ).get("ambiguous_high_demoted"),
        },
        "left_page_status_transitions": transitions,
        "sheet_relations": copy.deepcopy(dict(shadow_sheet_relations)),
    }


def _sheet_matcher_v4_shadow_failure(
    *, pair_id: str, run_id: str, gate: Mapping[str, Any], reason_code: str,
) -> dict[str, Any]:
    return {
        "kind": SHEET_MATCHER_V4_SHADOW_KIND,
        "schema_version": SHEET_MATCHER_V4_SHADOW_SCHEMA_VERSION,
        "pair_id": pair_id,
        "run_id": run_id,
        "generated_at": utc_now(),
        "shadow_status": "FAILED",
        "diagnostic_reason": SHEET_MATCHER_V4_SHADOW_FAILED,
        # Exception messages may contain server paths; only the type is kept.
        "reason_code": reason_code,
        "gate": dict(gate),
        "uses_model": False,
        "affects_production": False,
    }


def _record_sheet_matcher_v4_shadow_diagnostic(
    session_id: str,
    pair_id: str,
    run_id: str,
    diagnostic: Mapping[str, Any],
) -> None:
    """Best-effort run-bound state update; diagnostics cannot fail the run."""
    def update(existing: Any) -> Any:
        if not isinstance(existing, Mapping) or existing.get("run_id") != run_id:
            return existing
        value = dict(existing)
        value["sheet_matcher_v4_shadow"] = copy.deepcopy(dict(diagnostic))
        value["revision"] = int(value.get("revision") or 0) + 1
        value["updated_at"] = utc_now()
        return value

    try:
        production_store.mutate_artifact(
            session_id, pair_id, "state", update, default={}
        )
    except Exception:  # noqa: BLE001 - shadow diagnostics are non-critical
        pass


def _maybe_run_sheet_matcher_v4_shadow(
    session_id: str,
    pair_id: str,
    *,
    run_id: str,
    input_mode: str,
    pair: Mapping[str, Any],
    production_sheet_relations: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Compute v4 as a shadow of the production v3 for an allowlisted pair.

    Runs only the index + matcher (seconds, no model), never the comparison.
    The result is a separate diagnostic artifact and a state note; the
    production ``sheet_relations``, scope, questions, synthesis and report
    are not touched.  Any failure is recorded and swallowed.
    """
    gate = _sheet_matcher_v4_shadow_gate(
        pair_id=pair_id, run_id=run_id, input_mode=input_mode,
    )
    if not gate["allowed"]:
        return None
    try:
        shadow_sheet_relations, _indexes = _run_sheet_matcher(
            pair, algorithm=sheet_matcher_flags.ALGORITHM_V4,
        )
        artifact = build_sheet_matcher_v4_shadow(
            pair_id=pair_id,
            run_id=run_id,
            production_sheet_relations=production_sheet_relations,
            shadow_sheet_relations=shadow_sheet_relations,
            gate=gate,
        )
    except Exception as exc:  # noqa: BLE001 - shadow cannot fail production
        artifact = _sheet_matcher_v4_shadow_failure(
            pair_id=pair_id, run_id=run_id, gate=gate,
            reason_code=type(exc).__name__,
        )
    try:
        production_store.save_artifact(
            session_id, pair_id, "sheet_matcher_v4_shadow", artifact
        )
    except Exception:  # noqa: BLE001 - diagnostics persistence is isolated
        artifact = {
            **artifact,
            "shadow_status": "FAILED",
            "diagnostic_reason": SHEET_MATCHER_V4_SHADOW_FAILED,
        }
    diagnostic = {
        **gate,
        "executed": artifact.get("shadow_status") == "COMPLETED",
        "diagnostic_reason": artifact.get("diagnostic_reason"),
        "artifact": "sheet_matcher_v4_shadow",
        "production_relation_counts": (artifact.get("production") or {}).get("relation_counts"),
        "shadow_relation_counts": (artifact.get("shadow") or {}).get("relation_counts"),
    }
    _record_sheet_matcher_v4_shadow_diagnostic(session_id, pair_id, run_id, diagnostic)
    return diagnostic


def _function_lineage_manual_mappings(
    answers: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    """Read only explicitly namespaced human functional decisions.

    Existing sheet decisions are documentary.  Treating them as functional
    ground truth would recreate the exact DOCUMENT_LINK/ANALOGUE namespace
    collision this contour is designed to prevent.
    """
    if not isinstance(answers, Mapping):
        return []
    mappings: list[dict[str, Any]] = []
    for raw in answers.get("decisions") or []:
        if not isinstance(raw, Mapping):
            continue
        context = raw.get("context") if isinstance(raw.get("context"), Mapping) else {}
        namespace = raw.get("relation_namespace") or context.get("relation_namespace")
        if namespace not in {
            function_lineage_shadow.RELATION_FUNCTIONAL_ANALOGUE,
            function_lineage_shadow.RELATION_FUNCTION_LINEAGE,
        }:
            continue
        mappings.append({
            "mapping_id": raw.get("decision_id") or raw.get("id"),
            "relation_namespace": namespace,
            "left_pages": copy.deepcopy(
                raw.get("left_pages") or context.get("left_pages") or []
            ),
            "right_pages": copy.deepcopy(
                raw.get("right_pages") or context.get("right_pages") or []
            ),
        })
    return mappings


FUNCTION_LINEAGE_SHADOW_DISABLED = "SHADOW_DISABLED"
FUNCTION_LINEAGE_PAIR_NOT_ALLOWED = "PAIR_NOT_ALLOWED"
FUNCTION_LINEAGE_RUN_NOT_ALLOWED = "RUN_NOT_ALLOWED"
FUNCTION_LINEAGE_SHADOW_EXECUTED = "SHADOW_EXECUTED"
FUNCTION_LINEAGE_SHADOW_FAILED = "SHADOW_FAILED"


def _function_lineage_shadow_gate(
    *, pair_id: str, run_id: str, ai_mode: str,
) -> dict[str, Any]:
    """Resolve the fail-closed production gate without exposing allowlist IDs."""
    standard = ai_settings.normalize_mode(ai_mode) == ai_settings.MODE_STANDARD
    armed = ai_settings.function_lineage_shadow_enabled()
    pair_allowlist = ai_settings.function_lineage_shadow_pair_allowlist()
    run_allowlist = ai_settings.function_lineage_shadow_run_allowlist()
    pair_allowed = str(pair_id) in pair_allowlist
    run_allowed = str(run_id) in run_allowlist
    allowed = standard and armed and (pair_allowed or run_allowed)

    reason: str | None = None
    if not standard or not armed or (not pair_allowlist and not run_allowlist):
        reason = FUNCTION_LINEAGE_SHADOW_DISABLED
    elif not allowed:
        # A pair allowlist is the primary rollout boundary when configured.
        # The booleans below preserve the complete OR-gate diagnostic when
        # both lists are configured and neither identifier matches.
        reason = (
            FUNCTION_LINEAGE_PAIR_NOT_ALLOWED
            if pair_allowlist
            else FUNCTION_LINEAGE_RUN_NOT_ALLOWED
        )

    return {
        "allowed": allowed,
        "diagnostic_reason": reason,
        "standard_mode": standard,
        "feature_enabled": armed,
        "pair_allowlist_configured": bool(pair_allowlist),
        "run_allowlist_configured": bool(run_allowlist),
        "pair_allowed": pair_allowed,
        "run_allowed": run_allowed,
        "executed": False,
    }


def _record_function_lineage_shadow_diagnostic(
    session_id: str,
    pair_id: str,
    run_id: str,
    diagnostic: Mapping[str, Any],
) -> None:
    """Best-effort run-bound state update; diagnostics cannot fail the run."""
    def update(existing: Any) -> Any:
        if not isinstance(existing, Mapping) or existing.get("run_id") != run_id:
            return existing
        value = dict(existing)
        value["function_lineage_shadow"] = copy.deepcopy(dict(diagnostic))
        value["revision"] = int(value.get("revision") or 0) + 1
        value["updated_at"] = utc_now()
        return value

    try:
        production_store.mutate_artifact(
            session_id, pair_id, "state", update, default={}
        )
    except Exception:  # noqa: BLE001 - shadow diagnostics are non-critical
        pass


def _maybe_run_function_lineage_shadow(
    session_id: str,
    pair_id: str,
    *,
    run_id: str,
    ai_mode: str,
    indexes: Mapping[str, Sequence[Mapping[str, Any]]],
    sheet_relations: Mapping[str, Any],
    answers: Mapping[str, Any] | None = None,
    cancel: Any = None,
    gate: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Run and persist shadow artifacts without affecting the main flow."""
    resolved_gate = dict(gate) if gate is not None else _function_lineage_shadow_gate(
        pair_id=pair_id, run_id=run_id, ai_mode=ai_mode
    )
    if not resolved_gate.get("allowed"):
        return None
    try:
        artifacts = function_lineage_shadow.run_shadow(
            pair_id=pair_id,
            run_id=run_id,
            sheet_indexes=indexes,
            sheet_relations=sheet_relations,
            manual_mappings=_function_lineage_manual_mappings(answers),
            cancel=cancel,
        )
    except Exception as exc:  # noqa: BLE001 - shadow cannot fail production
        artifacts = function_lineage_shadow.failure_artifacts(
            pair_id=pair_id,
            run_id=run_id,
            # Exception messages may contain server paths or credentials.
            reason_code=type(exc).__name__,
        )
    diagnostic_reason = (
        FUNCTION_LINEAGE_SHADOW_EXECUTED
        if (artifacts.get("function_lineage_map") or {}).get("shadow_status")
        == "COMPLETED"
        else FUNCTION_LINEAGE_SHADOW_FAILED
    )
    for artifact in artifacts.values():
        artifact["diagnostic_reason"] = diagnostic_reason
    persistence_failed = False
    for name in (
        "document_link_map", "function_lineage_map", "derived_sheet_map",
    ):
        try:
            production_store.save_artifact(
                session_id, pair_id, name, artifacts[name]
            )
        except Exception:  # noqa: BLE001 - diagnostics persistence is isolated
            persistence_failed = True
            continue
    result = dict(artifacts["function_lineage_map"])
    if persistence_failed:
        result["diagnostic_reason"] = FUNCTION_LINEAGE_SHADOW_FAILED
    return result


def _run_text_branch(
    pair: Mapping[str, Any],
    pair_id: str,
    groups: list[dict[str, Any]],
    indexes: Mapping[str, list[dict[str, Any]]],
    existing_semantic: Mapping[str, Any] | None,
    *,
    document_cache_dir: Path | None = None,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    progress_callback = _TEXT_PROGRESS_CALLBACK.get()
    if progress_callback is not None:
        progress_callback(
            substage="text_preparation",
            message="Подготовка текста…",
        )
    preparation = prepare_text_scope(
        pair,
        groups,
        sheet_indexes=indexes,
        fitz=_fitz(),
        document_cache_dir=document_cache_dir,
    )
    if progress_callback is not None:
        progress_callback(
            substage="text_difference_search",
            message="Поиск различий в тексте…",
        )
    differences = build_text_differences_from_preparation(preparation)
    if progress_callback is not None:
        progress_callback(
            substage="text_difference_validation",
            message="Проверка найденных различий…",
        )
    fact_production = produce_text_facts(differences, preparation)
    stage3_signature = stage3_content_signature(differences)
    fact_production_signature = fact_production.get("input_signature")
    if (
        isinstance(existing_semantic, Mapping)
        and existing_semantic.get("kind") == SEMANTIC_KIND
        and existing_semantic.get("schema_version") == SEMANTIC_SCHEMA_VERSION
        and existing_semantic.get("stage3_signature") == stage3_signature
        and existing_semantic.get("text_fact_production_signature")
        == fact_production_signature
    ):
        semantic = dict(existing_semantic)
    else:
        # Stage 4 remains a closed validator.  The deterministic producer is
        # its explicit governed input; neither stage invokes a model or
        # guesses facts from ambiguous narrative text.
        semantic = build_semantic_validation(
            differences,
            fact_production.get("facts") or [],
            not_applicable_source_evidence=(
                fact_production.get("not_applicable_source_evidence") or []
            ),
        )
        semantic["text_fact_production_signature"] = fact_production_signature
        semantic["provenance"] = {
            **dict(semantic.get("provenance") or {}),
            "fact_source": TEXT_FACT_PRODUCER_VERSION,
            "text_fact_production_signature": fact_production_signature,
        }
    if progress_callback is not None:
        progress_callback(
            substage="text_change_formation",
            message="Формирование текстовых изменений…",
        )
    atoms = build_text_atoms(
        differences,
        semantic,
        artifact_ref=f"production/{pair_id}/text_differences.json",
    )
    return preparation, differences, fact_production, semantic, atoms


def _text_stage_summary(
    preparation: Mapping[str, Any],
    differences: Mapping[str, Any],
    fact_production: Mapping[str, Any],
    semantic: Mapping[str, Any],
    atom_artifact: Mapping[str, Any],
) -> dict[str, Any]:
    """Expose real TEXT pipeline counters without treating placeholders as facts."""
    fragments = preparation.get("fragments") or {}
    left_fragments = list(fragments.get("left") or []) if isinstance(
        fragments, Mapping
    ) else []
    right_fragments = list(fragments.get("right") or []) if isinstance(
        fragments, Mapping
    ) else []
    difference_summary = dict(differences.get("summary") or {})
    facts = [
        value
        for value in fact_production.get("facts") or []
        if isinstance(value, Mapping)
    ]
    not_applicable = [
        value
        for value in fact_production.get("not_applicable_source_evidence") or []
        if isinstance(value, Mapping)
    ]
    unresolved = list(fact_production.get("unresolved_source_evidence") or [])
    atoms = [
        value
        for value in atom_artifact.get("atoms") or []
        if isinstance(value, Mapping)
    ]
    automatic_atoms = sum(
        value.get("review_status") != "REVIEW_REQUIRED" for value in atoms
    )
    review_required_atoms = len(atoms) - automatic_atoms

    reason_counts: dict[str, int] = {}

    def add_reason(reason: Any) -> None:
        code = str(reason or "").strip()
        if code:
            reason_counts[code] = reason_counts.get(code, 0) + 1

    for fact in facts:
        requirement = (fact.get("provenance") or {}).get("review_requirement")
        if isinstance(requirement, Mapping):
            for reason in requirement.get("reason_codes") or []:
                add_reason(reason)
    for item in not_applicable:
        add_reason(item.get("reason_code"))
    if unresolved:
        reason_counts["unresolved_text_structure"] = len(unresolved)

    # An unresolved Stage 3 evidence item still creates an intentionally
    # review-only placeholder atom.  Such a placeholder must never upgrade
    # the TEXT source to VALID by its mere presence.
    if automatic_atoms:
        source_state = "VALID"
    elif facts or unresolved:
        source_state = "REVIEW_REQUIRED"
    elif not_applicable:
        source_state = "NOT_APPLICABLE"
    else:
        source_state = "ABSENT"

    semantic_diagnostics = dict(semantic.get("diagnostics") or {})
    delta_count = sum(
        int(difference_summary.get(bucket) or 0)
        for bucket in ("changed", "removed", "added")
    )
    return {
        "status": "COMPLETED",
        "source_state": source_state,
        "atoms": len(atoms),
        "deltas": delta_count,
        "automatic_atoms": automatic_atoms,
        "review_required": review_required_atoms,
        "review_required_atoms": review_required_atoms,
        "not_applicable": len(not_applicable),
        "unresolved": len(unresolved),
        "reason_counts": dict(sorted(reason_counts.items())),
        "input_signature": atom_artifact.get("input_signature"),
        "preparation": {
            "status": "COMPLETED",
            "groups": len(preparation.get("comparison_groups") or []),
            "fragments": len(left_fragments) + len(right_fragments),
            "left_fragments": len(left_fragments),
            "right_fragments": len(right_fragments),
            "extraction": copy.deepcopy(preparation.get("extraction") or {}),
            "input_signature": preparation.get("input_signature"),
        },
        "deterministic_diff": {
            "status": "COMPLETED",
            "groups": int(
                difference_summary.get("sheet_groups_with_differences") or 0
            ),
            "changed": int(difference_summary.get("changed") or 0),
            "removed": int(difference_summary.get("removed") or 0),
            "added": int(difference_summary.get("added") or 0),
            "source_signature": differences.get("source_signature"),
        },
        "fact_production": {
            "status": "COMPLETED",
            "facts": len(facts),
            "automatic": sum(
                fact.get("outcome") != "REVIEW_REQUIRED" for fact in facts
            ),
            "review_required": sum(
                fact.get("outcome") == "REVIEW_REQUIRED" for fact in facts
            ),
            "not_applicable": len(not_applicable),
            "unresolved": len(unresolved),
            "reason_counts": dict(sorted(reason_counts.items())),
            "input_signature": fact_production.get("input_signature"),
        },
        "semantic_validation": {
            "status": "COMPLETED",
            "facts": int(semantic_diagnostics.get("facts") or len(facts)),
            "automatic": sum(
                fact.get("outcome") != "REVIEW_REQUIRED" for fact in facts
            ),
            "review_required": sum(
                fact.get("outcome") == "REVIEW_REQUIRED" for fact in facts
            ),
            "not_applicable": int(
                semantic_diagnostics.get("not_applicable_source_evidence")
                or len(not_applicable)
            ),
            "unresolved": int(
                semantic_diagnostics.get("unresolved_source_evidence")
                or len(unresolved)
            ),
            "reason_counts": dict(sorted(reason_counts.items())),
            "input_signature": semantic.get("input_signature"),
        },
        "text_atoms": {
            "status": "COMPLETED",
            "atoms": len(atoms),
            "automatic": automatic_atoms,
            "review_required": review_required_atoms,
            "not_applicable": len(not_applicable),
            "unresolved": len(unresolved),
            "reason_counts": dict(sorted(reason_counts.items())),
            "input_signature": atom_artifact.get("input_signature"),
        },
    }


def _text_error_reason(error: Exception) -> str:
    if isinstance(error, FileNotFoundError):
        return "TEXT_SOURCE_MISSING"
    if isinstance(error, UnicodeDecodeError):
        return "TEXT_SOURCE_DECODING_FAILED"
    if isinstance(error, ValueError):
        return "TEXT_PIPELINE_VALIDATION_FAILED"
    if isinstance(error, OSError):
        return "TEXT_SOURCE_READ_FAILED"
    return "TEXT_EXTRACTION_UNAVAILABLE"


def _direct_page_sources(
    pair: Mapping[str, Any], request: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    descriptors = pair_documents_from_pair_artifact(dict(pair))
    sources: list[dict[str, Any]] = []
    for side, descriptor_key in (("left", "LEFT"), ("right", "RIGHT")):
        document = pair.get(side) or {}
        pdf_path = Path(str(document.get("pdf_path") or ""))
        source = {
            "document": descriptors[descriptor_key],
            "pdf_path": str(pdf_path),
            "blocks_path": str(pdf_path.parent / "blocks.json"),
            "page_index_0based": request[f"{side}_pages"][0] - 1,
        }
        block_ids = request[f"{side}_block_ids"]
        if len(block_ids) == 1:
            source["block_id"] = block_ids[0]
        elif len(block_ids) > 1:
            raise DirectPageComparisonError(
                f"{descriptor_key}: direct PAGE MODE 2 accepts one block"
            )
        sources.append(source)
    return sources[0], sources[1]


def _prepared_graphic_block_ids(
    document: Mapping[str, Any],
    pages: Iterable[int],
    side: str,
) -> list[str]:
    pdf_path = Path(str(document.get("pdf_path") or ""))
    blocks_path = pdf_path.parent / "blocks.json"
    if not blocks_path.is_file():
        return []
    payload = load_blocks_json(blocks_path)
    if payload is None:
        raise ValueError(f"invalid_{side}_blocks_json")
    page_indexes = {int(page) - 1 for page in pages}
    block_ids = sorted({
        str(record.get("block_id") or record.get("id") or "")
        for record in payload.get("blocks") or []
        if isinstance(record, Mapping)
        and record.get("page_index") in page_indexes
        and str(record.get("block_type") or "").casefold() in {"image", "graphic"}
        and str(record.get("block_id") or record.get("id") or "")
    })
    return block_ids


def _graphic_block_ids_in_sheet_scope(
    document: Mapping[str, Any],
    block_ids: Iterable[str],
    pages: Iterable[int],
    side: str,
) -> list[str]:
    """Bind an explicit DOCUMENT graphic selection to effective sheet pages."""
    selected = {str(block_id) for block_id in block_ids}
    page_indexes = {int(page) - 1 for page in pages}
    if not selected or not page_indexes:
        return []
    pdf_path = Path(str(document.get("pdf_path") or ""))
    payload = load_blocks_json(pdf_path.parent / "blocks.json")
    if payload is None:
        raise ValueError(f"invalid_{side}_blocks_json")
    return sorted({
        str(record.get("block_id") or record.get("id") or "")
        for record in payload.get("blocks") or []
        if isinstance(record, Mapping)
        and record.get("page_index") in page_indexes
        and str(record.get("block_type") or "").casefold() in {"image", "graphic"}
        and str(record.get("block_id") or record.get("id") or "") in selected
    })


def _page_graphic_group_id(group: Mapping[str, Any]) -> str:
    return stable_id(
        "pgraphic_group_",
        sorted({int(page) for page in group.get("left_pages") or []}),
        sorted({int(page) for page in group.get("right_pages") or []}),
        str(group.get("id") or ""),
        length=28,
    )


def _page_graphic_evidence_ref(group_id: str, change_id: str) -> str:
    return stable_id(
        "pgraphic_evidence_", group_id, change_id, length=30
    )


def _build_page_graphic_bundle(
    entries: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    groups = []
    for entry in entries:
        group = _normalize_page_groups([entry.get("group") or {}])[0]
        group_id = _page_graphic_group_id(group)
        ledger = entry.get("ledger")
        change_refs = []
        if isinstance(ledger, Mapping):
            # The adapter performs the canonical ledger validation here; the
            # bundle never weakens or rewrites an individual ledger schema.
            ledger_to_graphic_atoms(ledger)
            change_refs = [
                {
                    "source_change_id": str(change.get("change_id") or ""),
                    "evidence_ref": _page_graphic_evidence_ref(
                        group_id, str(change.get("change_id") or "")
                    ),
                }
                for change in sorted(
                    ledger.get("changes") or [],
                    key=lambda item: str((item or {}).get("change_id") or ""),
                )
                if isinstance(change, Mapping) and change.get("change_id")
            ]
        groups.append({
            "group_id": group_id,
            "group": group,
            "status": str(entry.get("status") or "CHECK_BLOCKED"),
            "source_state": str(
                entry.get("source_state") or "CHECK_BLOCKED"
            ),
            "reason_code": entry.get("reason_code"),
            "ledger": copy.deepcopy(dict(ledger))
            if isinstance(ledger, Mapping)
            else None,
            "change_refs": change_refs,
        })
    groups.sort(key=lambda item: (
        item["group"]["left_pages"],
        item["group"]["right_pages"],
        item["group_id"],
    ))
    core = {
        "kind": PAGE_GRAPHIC_BUNDLE_KIND,
        "schema_version": PAGE_GRAPHIC_BUNDLE_SCHEMA_VERSION,
        "version": 1,
        "direction": "LEFT_TO_RIGHT",
        "mode": "MODE_2",
        "groups": groups,
        "diagnostics": {
            "groups_total": len(groups),
            "groups_completed": sum(
                item["status"] == "COMPLETED" for item in groups
            ),
            "groups_blocked": sum(
                item["status"] != "COMPLETED" for item in groups
            ),
            "changes": sum(len(item["change_refs"]) for item in groups),
            "legacy_ledger_read": False,
        },
    }
    return {**core, "input_signature": content_signature(core)}


def _validate_page_graphic_bundle(payload: Mapping[str, Any]) -> dict[str, Any]:
    if (
        payload.get("kind") != PAGE_GRAPHIC_BUNDLE_KIND
        or payload.get("schema_version") != PAGE_GRAPHIC_BUNDLE_SCHEMA_VERSION
        or payload.get("version") != 1
        or payload.get("direction") != "LEFT_TO_RIGHT"
        or payload.get("mode") != "MODE_2"
        or not isinstance(payload.get("groups"), list)
    ):
        raise ProductionStateConflictError("PAGE graphic bundle is malformed")
    core = {
        key: copy.deepcopy(value)
        for key, value in payload.items()
        if key != "input_signature"
    }
    if payload.get("input_signature") != content_signature(core):
        raise ProductionStateConflictError("PAGE graphic bundle digest changed")
    seen_groups: set[str] = set()
    seen_evidence: set[str] = set()
    for record in payload.get("groups") or []:
        if not isinstance(record, Mapping):
            raise ProductionStateConflictError("PAGE graphic bundle group is malformed")
        group = _normalize_page_groups([record.get("group") or {}])[0]
        group_id = str(record.get("group_id") or "")
        if not group_id or group_id != _page_graphic_group_id(group):
            raise ProductionStateConflictError("PAGE graphic bundle group id changed")
        if group_id in seen_groups:
            raise ProductionStateConflictError("PAGE graphic bundle group is duplicated")
        seen_groups.add(group_id)
        ledger = record.get("ledger")
        status = str(record.get("status") or "")
        if status == "COMPLETED" and not isinstance(ledger, Mapping):
            raise ProductionStateConflictError(
                "completed PAGE graphic bundle group has no ledger"
            )
        if ledger is not None and not isinstance(ledger, Mapping):
            raise ProductionStateConflictError("PAGE graphic bundle ledger is malformed")
        source_change_ids = []
        if isinstance(ledger, Mapping):
            try:
                ledger_to_graphic_atoms(ledger)
            except (TypeError, ValueError) as exc:
                raise ProductionStateConflictError(
                    "PAGE graphic bundle ledger failed validation"
                ) from exc
            source_change_ids = sorted(
                str(change.get("change_id") or "")
                for change in ledger.get("changes") or []
                if isinstance(change, Mapping) and change.get("change_id")
            )
        expected_refs = [
            {
                "source_change_id": change_id,
                "evidence_ref": _page_graphic_evidence_ref(group_id, change_id),
            }
            for change_id in source_change_ids
        ]
        if record.get("change_refs") != expected_refs:
            raise ProductionStateConflictError(
                "PAGE graphic bundle evidence index changed"
            )
        for ref in expected_refs:
            evidence_ref = ref["evidence_ref"]
            if evidence_ref in seen_evidence:
                raise ProductionStateConflictError(
                    "PAGE graphic bundle evidence ref is duplicated"
                )
            seen_evidence.add(evidence_ref)
    return copy.deepcopy(dict(payload))


def _normalize_document_graphic_group(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("DOCUMENT graphic group must be an object")
    left_pages = _positive_pages(value.get("left_pages") or [], "left")
    right_pages = _positive_pages(value.get("right_pages") or [], "right")
    if not left_pages or not right_pages:
        raise ValueError("DOCUMENT graphic group requires both sides")
    relation_type = str(value.get("relation_type") or "MATCHED").upper()
    relation_status = str(
        value.get("status", value.get("relation_status")) or "UNKNOWN"
    ).upper()
    return {
        "id": str(value.get("id") or value.get("relation_id") or ""),
        "left_pages": left_pages,
        "right_pages": right_pages,
        "relation_type": relation_type,
        "relation_status": relation_status,
    }


def _document_graphic_group_id(group: Mapping[str, Any]) -> str:
    normalized = _normalize_document_graphic_group(group)
    return stable_id(
        "dgraphic_group_",
        normalized["left_pages"],
        normalized["right_pages"],
        normalized["relation_type"],
        normalized["id"],
        length=28,
    )


def _document_graphic_evidence_ref(group_id: str, change_id: str) -> str:
    return stable_id("dgraphic_evidence_", group_id, change_id, length=30)


def _document_bundle_diagnostics(
    groups: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    records = list(groups)
    router_counts = {
        route: sum(str(item.get("route") or "") == route for item in records)
        for route in (
            "MODE_1_APPLICABLE",
            "MODE_2_REQUIRED",
            "VISION_REQUIRED",
            "NO_GRAPHIC_COMPARISON",
        )
    }
    return {
        "groups_total": len(records),
        "confident_1to1_groups": sum(
            item.get("eligible_confident_1to1") is True for item in records
        ),
        "groups_completed": sum(
            item.get("status") == "COMPLETED" for item in records
        ),
        "groups_not_applicable": sum(
            item.get("status") == "NOT_APPLICABLE" for item in records
        ),
        "groups_review_required": sum(
            item.get("status") == "REVIEW_REQUIRED" for item in records
        ),
        "groups_blocked": sum(
            item.get("status") == "CHECK_BLOCKED" for item in records
        ),
        "changes": sum(len(item.get("change_refs") or []) for item in records),
        "router": {
            "runs": sum(item.get("router_called") is True for item in records),
            **router_counts,
            "FAILED": sum(
                item.get("router_called") is True
                and item.get("status") == "CHECK_BLOCKED"
                and not item.get("route")
                for item in records
            ),
        },
        "uses_model": False,
        "legacy_first_match_used": False,
    }


def _build_document_graphic_bundle(
    entries: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    groups = []
    for entry in entries:
        group = _normalize_document_graphic_group(entry.get("group") or {})
        group_id = _document_graphic_group_id(group)
        ledger = entry.get("ledger")
        change_refs = []
        if isinstance(ledger, Mapping):
            # Validate every independently routed result before it can enter
            # the signed aggregate.  One invalid group is represented by its
            # fail-closed entry by the caller; it never weakens another group.
            ledger_to_graphic_atoms(ledger)
            change_refs = [
                {
                    "source_change_id": str(change.get("change_id") or ""),
                    "evidence_ref": _document_graphic_evidence_ref(
                        group_id, str(change.get("change_id") or "")
                    ),
                }
                for change in sorted(
                    ledger.get("changes") or [],
                    key=lambda item: str((item or {}).get("change_id") or ""),
                )
                if isinstance(change, Mapping) and change.get("change_id")
            ]
        groups.append({
            "group_id": group_id,
            "group": group,
            "eligible_confident_1to1": bool(
                entry.get("eligible_confident_1to1")
            ),
            "status": str(entry.get("status") or "CHECK_BLOCKED"),
            "source_state": str(entry.get("source_state") or "CHECK_BLOCKED"),
            "reason_code": entry.get("reason_code"),
            "review_required": bool(entry.get("review_required")),
            "required_action": entry.get("required_action"),
            "selection_source": entry.get("selection_source"),
            "left_block_ids": sorted({
                str(value) for value in entry.get("left_block_ids") or []
                if str(value)
            }),
            "right_block_ids": sorted({
                str(value) for value in entry.get("right_block_ids") or []
                if str(value)
            }),
            "router_called": bool(entry.get("router_called")),
            "route": entry.get("route"),
            "mode": entry.get("mode"),
            "ledger": copy.deepcopy(dict(ledger))
            if isinstance(ledger, Mapping)
            else None,
            "change_refs": change_refs,
        })
    groups.sort(key=lambda item: (
        item["group"]["left_pages"],
        item["group"]["right_pages"],
        item["group_id"],
    ))
    core = {
        "kind": DOCUMENT_GRAPHIC_BUNDLE_KIND,
        "schema_version": DOCUMENT_GRAPHIC_BUNDLE_SCHEMA_VERSION,
        "version": 1,
        "direction": "LEFT_TO_RIGHT",
        "scope": "DOCUMENT",
        "groups": groups,
        "diagnostics": _document_bundle_diagnostics(groups),
    }
    return {**core, "input_signature": content_signature(core)}


def _validate_document_graphic_bundle(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    if (
        payload.get("kind") != DOCUMENT_GRAPHIC_BUNDLE_KIND
        or payload.get("schema_version") != DOCUMENT_GRAPHIC_BUNDLE_SCHEMA_VERSION
        or payload.get("version") != 1
        or payload.get("direction") != "LEFT_TO_RIGHT"
        or payload.get("scope") != "DOCUMENT"
        or not isinstance(payload.get("groups"), list)
    ):
        raise ProductionStateConflictError("DOCUMENT graphic bundle is malformed")
    core = {
        key: copy.deepcopy(value)
        for key, value in payload.items()
        if key != "input_signature"
    }
    if payload.get("input_signature") != content_signature(core):
        raise ProductionStateConflictError("DOCUMENT graphic bundle digest changed")
    seen_groups: set[str] = set()
    seen_evidence: set[str] = set()
    for record in payload.get("groups") or []:
        if not isinstance(record, Mapping):
            raise ProductionStateConflictError(
                "DOCUMENT graphic bundle group is malformed"
            )
        try:
            group = _normalize_document_graphic_group(record.get("group") or {})
        except (TypeError, ValueError) as exc:
            raise ProductionStateConflictError(
                "DOCUMENT graphic bundle group scope is malformed"
            ) from exc
        group_id = str(record.get("group_id") or "")
        if not group_id or group_id != _document_graphic_group_id(group):
            raise ProductionStateConflictError(
                "DOCUMENT graphic bundle group id changed"
            )
        if group_id in seen_groups:
            raise ProductionStateConflictError(
                "DOCUMENT graphic bundle group is duplicated"
            )
        seen_groups.add(group_id)
        status = str(record.get("status") or "")
        if status not in DOCUMENT_GRAPHIC_GROUP_STATUSES:
            raise ProductionStateConflictError(
                "DOCUMENT graphic bundle group status is unsupported"
            )
        ledger = record.get("ledger")
        if status == "COMPLETED" and not isinstance(ledger, Mapping):
            raise ProductionStateConflictError(
                "completed DOCUMENT graphic bundle group has no ledger"
            )
        if ledger is not None and not isinstance(ledger, Mapping):
            raise ProductionStateConflictError(
                "DOCUMENT graphic bundle ledger is malformed"
            )
        source_change_ids = []
        if isinstance(ledger, Mapping):
            try:
                ledger_to_graphic_atoms(ledger)
            except (TypeError, ValueError) as exc:
                raise ProductionStateConflictError(
                    "DOCUMENT graphic bundle ledger failed validation"
                ) from exc
            source_change_ids = sorted(
                str(change.get("change_id") or "")
                for change in ledger.get("changes") or []
                if isinstance(change, Mapping) and change.get("change_id")
            )
        expected_refs = [
            {
                "source_change_id": change_id,
                "evidence_ref": _document_graphic_evidence_ref(
                    group_id, change_id
                ),
            }
            for change_id in source_change_ids
        ]
        if record.get("change_refs") != expected_refs:
            raise ProductionStateConflictError(
                "DOCUMENT graphic bundle evidence index changed"
            )
        for ref in expected_refs:
            evidence_ref = ref["evidence_ref"]
            if evidence_ref in seen_evidence:
                raise ProductionStateConflictError(
                    "DOCUMENT graphic bundle evidence ref is duplicated"
                )
            seen_evidence.add(evidence_ref)
    if payload.get("diagnostics") != _document_bundle_diagnostics(
        payload.get("groups") or []
    ):
        raise ProductionStateConflictError(
            "DOCUMENT graphic bundle diagnostics changed"
        )
    return copy.deepcopy(dict(payload))


def _load_table_atoms(
    session_id: str,
    pair_id: str,
    graphic_atoms: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Атомы сравнения таблиц нагрузок для того же синтеза.

    Область сравнения берётся у уже построенных графических атомов: изменения
    таблиц относятся к тем же двум листам, и своя область сделала бы их
    несопоставимыми с изменениями графа при группировке.
    """
    payload = production_store.load_artifact(
        session_id, pair_id, "electrical_table_changes"
    )
    if not isinstance(payload, Mapping) or not payload.get("changes"):
        return []
    scope_ref = next(
        (str(atom.get("scope_ref")) for atom in graphic_atoms if atom.get("scope_ref")),
        "",
    )
    if not scope_ref:
        return []
    return list(
        load_table_diff_to_graphic_atoms(payload, scope_ref=scope_ref).get("atoms") or []
    )


def _graphic_atoms_from_source(payload: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if payload is None:
        return []
    kind = payload.get("kind")
    if kind not in {PAGE_GRAPHIC_BUNDLE_KIND, DOCUMENT_GRAPHIC_BUNDLE_KIND}:
        return list(ledger_to_graphic_atoms(payload).get("atoms") or [])
    if kind == PAGE_GRAPHIC_BUNDLE_KIND:
        bundle = _validate_page_graphic_bundle(payload)
        provenance_group_key = "page_graphic_group_id"
    else:
        bundle = _validate_document_graphic_bundle(payload)
        provenance_group_key = "document_graphic_group_id"
    atoms = []
    for record in bundle["groups"]:
        ledger = record.get("ledger")
        if not isinstance(ledger, Mapping):
            continue
        refs = {
            item["source_change_id"]: item["evidence_ref"]
            for item in record.get("change_refs") or []
        }
        group_id = record["group_id"]
        for value in ledger_to_graphic_atoms(ledger).get("atoms") or []:
            atom = copy.deepcopy(dict(value))
            original_atom_id = str(atom.get("atom_id") or "")
            original_evidence_ref = str(atom.get("evidence_ref") or "")
            atom["atom_id"] = stable_id(
                "graphic_atom_", group_id, original_atom_id, length=30
            )
            atom["evidence_ref"] = refs[original_evidence_ref]
            provenance = dict(atom.get("provenance") or {})
            provenance.update({
                provenance_group_key: group_id,
                "source_atom_id": original_atom_id,
                "source_evidence_ref": original_evidence_ref,
            })
            atom["provenance"] = provenance
            if kind == DOCUMENT_GRAPHIC_BUNDLE_KIND:
                atom["source_artifact"] = {
                    "kind": DOCUMENT_GRAPHIC_BUNDLE_KIND,
                    "schema_version": DOCUMENT_GRAPHIC_BUNDLE_SCHEMA_VERSION,
                    "artifact_ref": f"sha256:{bundle['input_signature']}",
                }
            atoms.append(atom)
    atoms.sort(key=lambda item: str(item.get("atom_id") or ""))
    if len({item["atom_id"] for item in atoms}) != len(atoms):
        raise ProductionStateConflictError("graphic bundle atom is duplicated")
    return atoms


def _document_graphic_entry(
    session_id: str,
    pair_id: str,
    pair: Mapping[str, Any],
    group_value: Mapping[str, Any],
    *,
    explicit_left_ids: Iterable[str] = (),
    explicit_right_ids: Iterable[str] = (),
) -> dict[str, Any]:
    """Route one confident 1:1 sheet without borrowing another group's blocks."""
    group = _normalize_document_graphic_group(group_value)
    base = {
        "group": group,
        "eligible_confident_1to1": False,
        "status": "CHECK_BLOCKED",
        "source_state": "CHECK_BLOCKED",
        "reason_code": None,
        "review_required": False,
        "required_action": None,
        "selection_source": (
            "CLIENT_BLOCK_IDS"
            if list(explicit_left_ids) or list(explicit_right_ids)
            else "SERVER_MATCHED_PAGES"
        ),
        "left_block_ids": [],
        "right_block_ids": [],
        "router_called": False,
        "route": None,
        "mode": None,
        "ledger": None,
    }
    if len(group["left_pages"]) != 1 or len(group["right_pages"]) != 1:
        return {
            **base,
            "status": "NOT_APPLICABLE",
            "source_state": "NOT_APPLICABLE",
            "reason_code": "grouped_graphic_comparison_not_supported",
            "review_required": True,
            "required_action": "CONFIRM_GROUPED_SHEET_WITHOUT_GRAPHIC_COMPARISON",
        }
    if group["relation_status"] != "HIGH":
        return {
            **base,
            "status": "REVIEW_REQUIRED",
            "source_state": "REVIEW_REQUIRED",
            "reason_code": "sheet_relation_requires_review",
            "review_required": True,
            "required_action": "CONFIRM_SHEET_RELATION",
        }

    base["eligible_confident_1to1"] = True
    try:
        if base["selection_source"] == "CLIENT_BLOCK_IDS":
            left_ids = _graphic_block_ids_in_sheet_scope(
                pair.get("left") or {},
                explicit_left_ids,
                group["left_pages"],
                "LEFT",
            )
            right_ids = _graphic_block_ids_in_sheet_scope(
                pair.get("right") or {},
                explicit_right_ids,
                group["right_pages"],
                "RIGHT",
            )
        else:
            left_ids = _prepared_graphic_block_ids(
                pair.get("left") or {}, group["left_pages"], "left"
            )
            right_ids = _prepared_graphic_block_ids(
                pair.get("right") or {}, group["right_pages"], "right"
            )
    except (FileNotFoundError, OSError, UnicodeDecodeError, ValueError) as exc:
        return {
            **base,
            "reason_code": type(exc).__name__,
            "review_required": True,
            "required_action": "VERIFY_PREPARED_GRAPHIC_INPUT",
        }
    base["left_block_ids"] = list(left_ids)
    base["right_block_ids"] = list(right_ids)
    if len(left_ids) > 1 or len(right_ids) > 1:
        return {
            **base,
            "status": "REVIEW_REQUIRED",
            "source_state": "REVIEW_REQUIRED",
            "reason_code": "ambiguous_prepared_graphic_blocks",
            "review_required": True,
            "required_action": "SELECT_PREPARED_BLOCK_IDS",
        }
    if len(left_ids) != 1 or len(right_ids) != 1:
        return {
            **base,
            "status": "NOT_APPLICABLE",
            "source_state": "NOT_APPLICABLE",
            "reason_code": (
                "NO_CLIENT_GRAPHIC_BLOCK_IN_EFFECTIVE_SHEET_SCOPE"
                if base["selection_source"] == "CLIENT_BLOCK_IDS"
                else "no_prepared_graphic_block_on_matched_sheet"
            ),
        }

    try:
        ledger = store.run_graphic_comparison(
            session_id,
            pair_id,
            list(left_ids),
            list(right_ids),
            persist=False,
        )
        # Reject a malformed Router result inside this group.  The aggregate
        # caller keeps routing the remaining independent sheets.
        ledger_to_graphic_atoms(ledger)
    except (
        FileNotFoundError,
        KeyError,
        OSError,
        RuntimeError,
        TypeError,
        ValueError,
    ) as exc:
        return {
            **base,
            "router_called": True,
            "reason_code": type(exc).__name__,
            "review_required": True,
            "required_action": "RETRY_OR_REVIEW_GRAPHIC_GROUP",
        }

    route = str(ledger.get("route") or "")
    routing = (ledger.get("diagnostics") or {}).get("routing") or {}
    common = {
        **base,
        "router_called": True,
        "route": route or None,
        "mode": ledger.get("mode"),
        "ledger": ledger,
        "reason_code": routing.get("reason_code") or "GRAPHIC_ROUTE_UNAVAILABLE",
    }
    if route == "MODE_1_APPLICABLE":
        return {
            **common,
            "status": "COMPLETED",
            "source_state": "VALID" if ledger.get("changes") else "ABSENT",
        }
    if route == "MODE_2_REQUIRED":
        return {
            **common,
            "status": "NOT_APPLICABLE",
            "source_state": "NOT_APPLICABLE",
            "review_required": True,
            "required_action": "RUN_MODE_2_OR_REVIEW",
        }
    if route == "VISION_REQUIRED":
        return {
            **common,
            "status": "CHECK_BLOCKED",
            "source_state": "CHECK_BLOCKED",
            "review_required": True,
            "required_action": "RUN_VISION_OR_REVIEW",
        }
    if route == "NO_GRAPHIC_COMPARISON":
        return {
            **common,
            "status": "NOT_APPLICABLE",
            "source_state": "NOT_APPLICABLE",
        }
    return {
        **common,
        "review_required": True,
        "required_action": "REVIEW_GRAPHIC_ROUTE",
    }


def _document_graphic_stage(bundle: Mapping[str, Any]) -> dict[str, Any]:
    validated = _validate_document_graphic_bundle(bundle)
    groups = validated["groups"]
    diagnostics = validated["diagnostics"]
    has_results = diagnostics["groups_completed"] > 0
    has_unresolved = (
        diagnostics["groups_review_required"] > 0
        or diagnostics["groups_blocked"] > 0
    )
    changes = diagnostics["changes"]
    if has_unresolved:
        status = "CHECK_BLOCKED"
        source_state = "CHECK_BLOCKED"
    elif has_results:
        status = "COMPLETED"
        source_state = "VALID" if changes else "ABSENT"
    else:
        status = "NOT_APPLICABLE"
        source_state = "NOT_APPLICABLE"
    routes = sorted({
        str(item.get("route")) for item in groups if item.get("route")
    })
    reasons = sorted({
        str(item.get("reason_code"))
        for item in groups
        if item.get("reason_code")
    })
    selections = sorted({
        str(item.get("selection_source"))
        for item in groups
        if item.get("selection_source")
    })
    not_checked = (
        diagnostics["groups_not_applicable"]
        + diagnostics["groups_review_required"]
        + diagnostics["groups_blocked"]
    )
    engineer_questions = [
        {
            "question_id": stable_id(
                "gquestion_",
                item["group_id"],
                item.get("reason_code"),
                item.get("required_action"),
                item.get("left_block_ids") or [],
                item.get("right_block_ids") or [],
                length=28,
            ),
            "category": "GRAPHIC",
            "question_type": (
                "GRAPHIC_BLOCK_SELECTION"
                if item.get("required_action") == "SELECT_PREPARED_BLOCK_IDS"
                else "GRAPHIC_GROUP_REVIEW"
            ),
            "group_id": item["group_id"],
            "reason_code": item.get("reason_code"),
            "required_action": item.get("required_action"),
            "left_block_ids": list(item.get("left_block_ids") or []),
            "right_block_ids": list(item.get("right_block_ids") or []),
        }
        for item in groups
        if item.get("review_required")
    ]
    return {
        "status": status,
        "source_state": source_state,
        "mode": "DOCUMENT_GRAPHIC_BUNDLE",
        "route": routes[0] if len(routes) == 1 else "MULTI_ROUTE" if routes else None,
        "routes": routes,
        "changes": changes,
        "selection_source": (
            selections[0] if len(selections) == 1 else "MIXED" if selections else None
        ),
        "reason_code": reasons[0] if len(reasons) == 1 else (
            "document_graphic_groups_require_attention" if reasons else None
        ),
        "reason_codes": reasons,
        "groups_total": diagnostics["groups_total"],
        "groups_confident_1to1": diagnostics["confident_1to1_groups"],
        "groups_completed": diagnostics["groups_completed"],
        "groups_not_applicable": diagnostics["groups_not_applicable"],
        "groups_review_required": diagnostics["groups_review_required"],
        "groups_blocked": diagnostics["groups_blocked"],
        "router_runs": diagnostics["router"]["runs"],
        "mode1_groups": diagnostics["router"]["MODE_1_APPLICABLE"],
        "mode2_groups": diagnostics["router"]["MODE_2_REQUIRED"],
        "vision_groups": diagnostics["router"]["VISION_REQUIRED"],
        "no_graphic_comparison_groups": diagnostics["router"][
            "NO_GRAPHIC_COMPARISON"
        ],
        "router_failed_groups": diagnostics["router"]["FAILED"],
        "coverage": (
            "PARTIAL" if has_results and not_checked
            else "CHECKED" if has_results and not not_checked
            else "NOT_CHECKED"
        ),
        "review_required": sum(
            item.get("review_required") is True for item in groups
        ),
        "engineer_questions": engineer_questions,
        "engineer_question_count": len(engineer_questions),
        "group_results": [
            {
                "group_id": item["group_id"],
                "group": copy.deepcopy(item["group"]),
                "status": item["status"],
                "source_state": item["source_state"],
                "reason_code": item.get("reason_code"),
                "review_required": item.get("review_required"),
                "required_action": item.get("required_action"),
                "selection_source": item.get("selection_source"),
                "left_block_count": len(item.get("left_block_ids") or []),
                "right_block_count": len(item.get("right_block_ids") or []),
                "left_block_ids": list(item.get("left_block_ids") or [])
                if item.get("review_required") else [],
                "right_block_ids": list(item.get("right_block_ids") or [])
                if item.get("review_required") else [],
                "router_called": item.get("router_called"),
                "route": item.get("route"),
                "mode": item.get("mode"),
                "changes": len(item.get("change_refs") or []),
            }
            for item in groups
        ],
        "artifact_kind": DOCUMENT_GRAPHIC_BUNDLE_KIND,
        "bundle_input_signature": validated["input_signature"],
        "parent_relation_required": False,
    }


DOCUMENT_INCONSISTENCIES_KIND = "stage_comparison_document_inconsistencies"
DOCUMENT_INCONSISTENCIES_SCHEMA_VERSION = "document-inconsistencies.v1"


def _save_electrical_table_changes(
    session_id: str,
    pair_id: str,
    result: Mapping[str, Any],
) -> dict[str, Any]:
    """Сохранить сравнение таблиц нагрузок отдельным артефактом.

    Значения мощностей и токов подписаны у колонок листа, а не у аппаратов, и
    узел графа щита для них есть не всегда: у АУКРМ левого листа его нет вовсе.
    Поэтому сравнение таблиц живёт своим артефактом, а в синтез приходит теми
    же атомами источника GRAPHIC, что и изменения графа.
    """
    diagnostics = result.get("diagnostics")
    payload = (
        diagnostics.get("electrical_table_diff")
        if isinstance(diagnostics, Mapping)
        else None
    )
    if not isinstance(payload, Mapping):
        payload = {
            "contract_version": "electrical-table-diff.v1",
            "producer": "electrical-table-diff-v1",
            "changes": [],
            "unchanged": [],
            "blocked": [],
            "unproven": [],
            "counts": {},
            "diagnostics": {"reason": "load_table_diff_absent"},
        }
    artifact = {
        "kind": "stage_comparison_electrical_table_changes",
        "schema_version": str(payload.get("contract_version") or "electrical-table-diff.v1"),
        "version": 1,
        "pair_id": pair_id,
        "generated_at": utc_now(),
        **{
            key: copy.deepcopy(payload.get(key))
            for key in ("changes", "unchanged", "blocked", "unproven", "counts", "diagnostics")
        },
        "constraints": {"uses_model": False, "is_deterministic": True},
    }
    production_store.save_artifact(
        session_id, pair_id, "electrical_table_changes", artifact
    )
    return artifact


def _save_document_inconsistencies(
    session_id: str,
    pair_id: str,
    result: Mapping[str, Any],
) -> dict[str, Any]:
    """Сохранить внутренние противоречия листов отдельным артефактом.

    Это НЕ изменения. «1QF1 стоит во второй секции» — ошибка самого чертежа:
    обозначение относит аппарат к первой секции, а геометрия шин — ко второй.
    Показать это как «было → стало» нельзя: на другом листе такого аппарата в
    таком виде не было вовсе, и любая пара значений оказалась бы выдуманной.

    Отдельный файл делает разделение физическим, а не соглашением: перечень
    изменений просто не может их случайно втянуть.
    """
    comparison = result.get("comparison_result")
    items = list(
        (comparison or {}).get("document_inconsistencies") or []
        if isinstance(comparison, Mapping)
        else []
    )
    payload = {
        "kind": DOCUMENT_INCONSISTENCIES_KIND,
        "schema_version": DOCUMENT_INCONSISTENCIES_SCHEMA_VERSION,
        "version": 1,
        "pair_id": pair_id,
        "generated_at": utc_now(),
        "items": items,
        "counts": {
            "total": len(items),
            "LEFT": sum(1 for item in items if item.get("side") == "LEFT"),
            "RIGHT": sum(1 for item in items if item.get("side") == "RIGHT"),
        },
        "constraints": {
            "uses_model": False,
            "is_a_change_between_versions": False,
        },
    }
    production_store.save_artifact(
        session_id, pair_id, "document_inconsistencies", payload
    )
    return payload


def _run_graphic_branch(
    session_id: str,
    pair_id: str,
    pair: Mapping[str, Any],
    request: Mapping[str, Any],
    comparison_groups: list[Mapping[str, Any]],
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    progress_callback = _GRAPHIC_PROGRESS_CALLBACK.get()
    recent_durations_ms: list[int] = []

    def report_group(
        *,
        processed: int | None,
        total: int | None,
        group: Mapping[str, Any],
        message: str,
    ) -> None:
        if progress_callback is None:
            return
        progress_callback(
            processed=processed,
            total=total,
            unit="groups" if total is not None else None,
            current_item={
                "group_id": group.get("id") or group.get("relation_id"),
                "left_pages": list(group.get("left_pages") or []),
                "right_pages": list(group.get("right_pages") or []),
            },
            recent_unit_durations_ms=list(recent_durations_ms),
            message=message,
        )

    if request["input_mode"] == "PAGE":
        # Direct MODE 2 has a calibrated one-page-per-side contract.  A PAGE
        # review action may create several groups or a grouped 1:N/N:1 scope;
        # never silently feed only the first page to that comparator.
        if any(
            len(group.get("left_pages") or []) != 1
            or len(group.get("right_pages") or []) != 1
            for group in comparison_groups
        ) or not comparison_groups:
            return None, {
                "status": "NOT_APPLICABLE",
                "source_state": "NOT_APPLICABLE",
                "mode": "MODE_2_REQUIRED",
                "changes": 0,
                "reason_code": "GROUPED_PAGE_CARDINALITY_REQUIRES_NEW_COMPARATOR",
                "parent_relation_required": False,
            }
        scope_changed = any(
            list(group.get("left_pages") or []) != request.get("left_pages")
            or list(group.get("right_pages") or []) != request.get("right_pages")
            for group in comparison_groups
        )
        if scope_changed and (
            request.get("left_block_ids") or request.get("right_block_ids")
        ):
            return None, {
                "status": "NOT_APPLICABLE",
                "source_state": "NOT_APPLICABLE",
                "mode": "MODE_2_REQUIRED",
                "changes": 0,
                "reason_code": "PAGE_ACTION_INVALIDATES_EXPLICIT_BLOCK_SCOPE",
                "parent_relation_required": False,
            }
        if len(comparison_groups) > 1:
            entries = []
            group_total = len(comparison_groups)
            for index, group in enumerate(comparison_groups):
                report_group(
                    processed=index,
                    total=group_total,
                    group=group,
                    message=(
                        "Структурное сравнение графической группы "
                        f"{index + 1} из {group_total}…"
                    ),
                )
                unit_started = time.perf_counter()
                direct_request = copy.deepcopy(dict(request))
                direct_request["left_pages"] = list(
                    group.get("left_pages") or []
                )
                direct_request["right_pages"] = list(
                    group.get("right_pages") or []
                )
                try:
                    left, right = _direct_page_sources(pair, direct_request)
                    result = validate_direct_page_comparison_result(
                        compare_selected_pages(left, right)
                    )
                    ledger = result["graphic_change_ledger"]
                    entries.append({
                        "group": group,
                        "status": "COMPLETED",
                        "source_state": (
                            "VALID" if ledger.get("changes") else "ABSENT"
                        ),
                        "reason_code": None,
                        "ledger": ledger,
                    })
                except (
                    DirectPageComparisonError,
                    FileNotFoundError,
                    OSError,
                    RuntimeError,
                    ValueError,
                ) as exc:
                    entries.append({
                        "group": group,
                        "status": "CHECK_BLOCKED",
                        "source_state": "CHECK_BLOCKED",
                        "reason_code": type(exc).__name__,
                        "ledger": None,
                    })
                recent_durations_ms.append(
                    max(0, int((time.perf_counter() - unit_started) * 1000))
                )
                recent_durations_ms[:] = recent_durations_ms[-5:]
                report_group(
                    processed=index + 1,
                    total=group_total,
                    group=group,
                    message=(
                        "Обработано графических групп: "
                        f"{index + 1} из {group_total}."
                    ),
                )
            bundle = _validate_page_graphic_bundle(
                _build_page_graphic_bundle(entries)
            )
            production_store.save_artifact(
                session_id, pair_id, "page_graphic_bundle", bundle
            )
            completed = [
                item for item in bundle["groups"]
                if item["status"] == "COMPLETED"
            ]
            blocked = [
                item for item in bundle["groups"]
                if item["status"] != "COMPLETED"
            ]
            changes = sum(len(item["change_refs"]) for item in completed)
            return bundle, {
                "status": "CHECK_BLOCKED" if blocked else "COMPLETED",
                "source_state": (
                    "CHECK_BLOCKED" if blocked
                    else "VALID" if changes
                    else "ABSENT"
                ),
                "mode": "MODE_2",
                "changes": changes,
                "groups_total": len(bundle["groups"]),
                "groups_completed": len(completed),
                "groups_blocked": len(blocked),
                "coverage": (
                    "PARTIAL" if blocked and completed
                    else "NOT_CHECKED" if blocked
                    else "CHECKED"
                ),
                "group_results": [
                    {
                        "group_id": item["group_id"],
                        "group": copy.deepcopy(item["group"]),
                        "status": item["status"],
                        "source_state": item["source_state"],
                        "reason_code": item.get("reason_code"),
                        "changes": len(item["change_refs"]),
                    }
                    for item in bundle["groups"]
                ],
                "artifact_kind": PAGE_GRAPHIC_BUNDLE_KIND,
                "bundle_input_signature": bundle["input_signature"],
                "parent_relation_required": False,
            }
        group = comparison_groups[0]
        report_group(
            processed=None,
            total=None,
            group=group,
            message="Выполняется одно структурное сравнение страниц…",
        )
        direct_request = copy.deepcopy(dict(request))
        direct_request["left_pages"] = list(group.get("left_pages") or [])
        direct_request["right_pages"] = list(group.get("right_pages") or [])
        try:
            left, right = _direct_page_sources(pair, direct_request)
            result = validate_direct_page_comparison_result(
                compare_selected_pages(left, right)
            )
            production_store.save_artifact(
                session_id, pair_id, "direct_page_mode2", result
            )
            ledger = production_store.save_graphic_ledger(
                session_id, pair_id, result["graphic_change_ledger"]
            )
            inconsistencies = _save_document_inconsistencies(
                session_id, pair_id, result
            )
            table_changes = _save_electrical_table_changes(
                session_id, pair_id, result
            )
            return ledger, {
                "status": "COMPLETED",
                "source_state": "VALID" if ledger.get("changes") else "ABSENT",
                "mode": result.get("mode"),
                "changes": len(ledger.get("changes") or []),
                # Внутренние противоречия листа считаются отдельно от
                # изменений: это ошибка чертежа, а не расхождение редакций.
                "document_inconsistencies": len(inconsistencies["items"]),
                "electrical_table_changes": len(table_changes.get("changes") or []),
                "parent_relation_required": False,
            }
        except (DirectPageComparisonError, FileNotFoundError, OSError, ValueError) as exc:
            # Direct MODE 2 is intentionally narrow.  A page outside that
            # calibrated shape remains a valid TEXT comparison, not a failed
            # production run and not an invitation to invent a comparator.
            return None, {
                "status": "NOT_APPLICABLE",
                "source_state": "NOT_APPLICABLE",
                "mode": "MODE_2",
                "changes": 0,
                "reason_code": type(exc).__name__,
                "parent_relation_required": False,
            }
    left_ids = list(request["left_block_ids"])
    right_ids = list(request["right_block_ids"])
    if bool(left_ids) != bool(right_ids):
        raise ValueError("DOCUMENT graphic block ids are required on both sides")
    entries = []
    group_total = len(comparison_groups)
    determinate = group_total > 1
    for index, group in enumerate(comparison_groups):
        report_group(
            processed=index if determinate else None,
            total=group_total if determinate else None,
            group=group,
            message=(
                f"Сравнение графической группы {index + 1} из {group_total}…"
                if determinate
                else "Выполняется одно графическое сравнение…"
            ),
        )
        unit_started = time.perf_counter()
        entries.append(_document_graphic_entry(
            session_id,
            pair_id,
            pair,
            group,
            explicit_left_ids=left_ids,
            explicit_right_ids=right_ids,
        ))
        recent_durations_ms.append(
            max(0, int((time.perf_counter() - unit_started) * 1000))
        )
        recent_durations_ms[:] = recent_durations_ms[-5:]
        report_group(
            processed=index + 1 if determinate else None,
            total=group_total if determinate else None,
            group=group,
            message=(
                f"Обработано графических групп: {index + 1} из {group_total}."
                if determinate
                else "Графическое сравнение завершено."
            ),
        )
    bundle = _validate_document_graphic_bundle(
        _build_document_graphic_bundle(entries)
    )
    production_store.save_artifact(
        session_id, pair_id, "document_graphic_bundle", bundle
    )
    return bundle, _document_graphic_stage(bundle)


def _entity_records(
    text_atoms: Iterable[Mapping[str, Any]],
    graphic_atoms: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Expose side-specific, exact atom aliases to the final Entity Matcher."""
    module = importlib.import_module(
        "backend.app.services.stage_comparison.entity_matcher"
    )
    return module.entity_records_from_atoms(text_atoms, graphic_atoms)


def _run_entity_matcher(
    text_atoms: list[dict[str, Any]],
    graphic_atoms: list[dict[str, Any]],
) -> dict[str, Any]:
    module = importlib.import_module(
        "backend.app.services.stage_comparison.entity_matcher"
    )
    left, right = _entity_records(text_atoms, graphic_atoms)
    return module.match_entities(left, right)


def _bind_synthesis_atoms(
    text_atoms: list[dict[str, Any]],
    graphic_atoms: list[dict[str, Any]],
    entity_relations: Mapping[str, Any],
) -> dict[str, Any]:
    module = importlib.import_module(
        "backend.app.services.stage_comparison.entity_matcher"
    )
    return module.bind_atoms_to_entity_relations(
        text_atoms, graphic_atoms, entity_relations
    )


def _build_synthesis_candidates(
    text_atoms: list[dict[str, Any]],
    graphic_atoms: list[dict[str, Any]],
    entity_relations: Mapping[str, Any],
    *,
    source_valid: bool = False,
    coverage_by_side: Mapping[str, Any] | None = None,
    document_binding_state: str = "DOCUMENT_BINDING_UNKNOWN",
) -> list[dict[str, Any]]:
    module = importlib.import_module(
        "backend.app.services.stage_comparison.entity_matcher"
    )
    artifact = module.build_text_graphic_synthesis_candidates(
        text_atoms,
        graphic_atoms,
        entity_relations,
        source_valid=source_valid,
        coverage_by_side=coverage_by_side,
        document_binding_state=document_binding_state,
    )
    return list(artifact.get("candidates") or [])


def _empty_questions(
    sheet_relations: Mapping[str, Any],
    entity_relations: Mapping[str, Any],
    synthesis: Mapping[str, Any],
) -> dict[str, Any]:
    signature = content_signature({
        "sheet_relations": sheet_relations.get("input_signature"),
        "entity_relations": entity_relations.get("input_signature"),
        "synthesis": canonical_synthesis_digest(synthesis),
    })
    return {
        "kind": QUESTIONS_KIND,
        "schema_version": QUESTIONS_SCHEMA_VERSION,
        "version": 1,
        "revision": 1,
        "input_signature": signature,
        "generated_at": utc_now(),
        "questions": [],
        "counts": {"SHEET": 0, "ENTITY": 0, "CHANGE": 0, "total": 0},
    }


def _sheet_suggestion_questions(
    sheet_suggestions: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    labels = {
        "COMPARE_ADDITIONALLY": "Сравнить дополнительно",
        "REPLACE": "Заменить выбранный лист",
        "ADD_TO_GROUP": "Добавить в группу сравнения",
        "IGNORE": "Игнорировать рекомендацию",
    }
    questions = []
    for suggestion in (sheet_suggestions or {}).get("suggestions") or []:
        if not isinstance(suggestion, Mapping):
            continue
        suggestion_id = str(suggestion.get("suggestion_id") or "")
        relation_id = str(suggestion.get("relation_id") or "")
        suggested_scope = _suggested_page_scope(suggestion)
        if not suggestion_id or suggested_scope is None:
            continue
        actions = [
            str(action) for action in suggestion.get("actions") or []
            if str(action) in labels
        ]
        identity = {
            "suggestion_id": suggestion_id,
            "selected_left_pages": suggestion.get("selected_left_pages"),
            "selected_right_pages": suggestion.get("selected_right_pages"),
            "suggested_left_pages": suggestion.get("suggested_left_pages"),
            "suggested_right_pages": suggestion.get("suggested_right_pages"),
            "relation_id": relation_id,
        }
        question_id = stable_id("hquestion_", "SHEET", "PAGE_SUGGESTION", suggestion_id, length=24)
        questions.append({
            "question_id": question_id,
            "category": "SHEET",
            "question_type": "PAGE_SUGGESTION_ACTION",
            "prompt": (
                "Как поступить с рекомендацией Sheet Matcher для выбранной пары страниц?"
            ),
            "answer_options": [
                {"code": action, "label": labels[action]} for action in actions
            ],
            "dependencies": [{
                "kind": "SHEET_RELATION",
                "artifact_kind": "stage_comparison_sheet_relations",
                "ref": relation_id,
            }],
            "dependency_refs": [relation_id],
            "context": {**identity, "suggestion_id": suggestion_id},
            "input_signature": content_signature({
                "producer": "production-page-suggestion-review-v1",
                "sheet_suggestions_input_signature": (
                    sheet_suggestions or {}
                ).get("input_signature"),
                "identity": identity,
                "actions": actions,
            }),
            # Рекомендация, а не вопрос: инженер выбрал пару страниц сам, и
            # анализ этой пары ничего не ждёт. Ответить на неё можно, но она
            # не задерживает работу и не считается непроверенной находкой.
            "advisory": True,
            "blocking": False,
            "status": "PENDING",
        })
    return sorted(questions, key=lambda item: item["question_id"])


def _suggested_page_scope(
    suggestion: Mapping[str, Any],
) -> tuple[list[int], list[int]] | None:
    """Return a materializable suggestion or reject an incomplete relation."""
    try:
        left = _positive_pages(
            suggestion.get("suggested_left_pages") or [], "left"
        )
        right = _positive_pages(
            suggestion.get("suggested_right_pages") or [], "right"
        )
    except (TypeError, ValueError):
        return None
    if not left or not right:
        return None
    return left, right


def _filter_page_suggestions(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Exclude UNCERTAIN/partial candidates that cannot become page scope."""
    result = copy.deepcopy(dict(payload))
    suggestions = [
        item for item in result.get("suggestions") or []
        if isinstance(item, Mapping)
    ]
    valid = [item for item in suggestions if _suggested_page_scope(item)]
    result["suggestions"] = valid
    diagnostics = dict(result.get("diagnostics") or {})
    diagnostics["excluded_non_materializable_suggestions"] = (
        len(suggestions) - len(valid)
    )
    result["diagnostics"] = diagnostics
    return result


def _selected_page_group(request: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "left_pages": list(request.get("left_pages") or []),
        "right_pages": list(request.get("right_pages") or []),
        "relation_type": "USER_SELECTED",
    }


def _normalize_page_groups(
    values: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if isinstance(values, (str, bytes, Mapping)):
        raise ValueError("PAGE comparison groups must be an array")
    groups = []
    for value in values:
        if not isinstance(value, Mapping):
            raise ValueError("PAGE comparison group must be an object")
        left = _positive_pages(value.get("left_pages") or [], "left")
        right = _positive_pages(value.get("right_pages") or [], "right")
        if not left or not right:
            raise ValueError("PAGE comparison group requires both sides")
        relation_type = str(value.get("relation_type") or "USER_SELECTED")
        group = {
            "left_pages": left,
            "right_pages": right,
            "relation_type": relation_type,
        }
        group_id = value.get("id") or value.get("relation_id")
        if group_id:
            group["id"] = str(group_id)
        groups.append(group)
    if not groups:
        raise ValueError("PAGE comparison groups must not be empty")
    keys = [
        (tuple(item["left_pages"]), tuple(item["right_pages"]))
        for item in groups
    ]
    if len(keys) != len(set(keys)):
        raise ValueError("duplicate PAGE comparison group")
    return groups


def _page_action_projection(
    request: Mapping[str, Any],
    sheet_suggestions: Mapping[str, Any] | None,
    answers: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Materialize at most one version-current PAGE scope action."""
    questions = {
        str(question.get("question_id") or ""): question
        for question in _sheet_suggestion_questions(sheet_suggestions)
    }
    suggestions = {
        str(item.get("suggestion_id") or ""): item
        for item in (sheet_suggestions or {}).get("suggestions") or []
        if isinstance(item, Mapping)
    }
    decisions = []
    for decision in (answers or {}).get("decisions") or []:
        if not isinstance(decision, Mapping):
            continue
        question = questions.get(str(decision.get("question_id") or ""))
        if not question or (
            decision.get("question_input_signature")
            != question.get("input_signature")
        ):
            continue
        action = str(decision.get("answer") or "")
        suggestion_id = str(
            (question.get("context") or {}).get("suggestion_id") or ""
        )
        suggestion = suggestions.get(suggestion_id)
        if not suggestion or action not in {
            "IGNORE", *PAGE_MATERIALIZING_ACTIONS
        }:
            continue
        decisions.append({
            "action": action,
            "decision_id": str(decision.get("decision_id") or ""),
            "question_id": str(decision.get("question_id") or ""),
            "suggestion_id": suggestion_id,
            "suggestion": suggestion,
        })

    materializing = [
        item for item in decisions
        if item["action"] in PAGE_MATERIALIZING_ACTIONS
    ]
    if len(materializing) > 1:
        raise ValueError("multiple materializing PAGE suggestion actions are ambiguous")

    automatic_groups = [_selected_page_group(request)]
    effective_groups = copy.deepcopy(automatic_groups)
    active = materializing[0] if materializing else None
    if active is not None:
        suggested_scope = _suggested_page_scope(active["suggestion"])
        if suggested_scope is None:
            raise ValueError("PAGE suggestion scope is incomplete")
        suggested_left, suggested_right = suggested_scope
        suggested_group = {
            "id": str(active["suggestion"].get("relation_id") or ""),
            "left_pages": suggested_left,
            "right_pages": suggested_right,
            "relation_type": str(
                active["suggestion"].get("relation_type") or "SUGGESTED"
            ),
        }
        action = active["action"]
        if action == "REPLACE":
            effective_groups = [suggested_group]
        elif action == "COMPARE_ADDITIONALLY":
            effective_groups = [*automatic_groups]
            existing = {
                (
                    tuple(group["left_pages"]),
                    tuple(group["right_pages"]),
                )
                for group in effective_groups
            }
            suggested_key = (tuple(suggested_left), tuple(suggested_right))
            if suggested_key not in existing:
                effective_groups.append(suggested_group)
        else:  # ADD_TO_GROUP
            effective_groups = [{
                "left_pages": sorted({
                    *automatic_groups[0]["left_pages"], *suggested_left
                }),
                "right_pages": sorted({
                    *automatic_groups[0]["right_pages"], *suggested_right
                }),
                "relation_type": "USER_GROUPED",
            }]

    automatic_signature = _sheet_scope_signature(automatic_groups)
    effective_signature = _sheet_scope_signature(effective_groups)
    active_decision_id = str((active or {}).get("decision_id") or "")
    outcomes = []
    for item in sorted(
        decisions,
        key=lambda value: (value["suggestion_id"], value["question_id"]),
    ):
        outcomes.append({
            "suggestion_id": item["suggestion_id"],
            "question_id": item["question_id"],
            "decision_id": item["decision_id"],
            "action": item["action"],
            "state": (
                "MATERIALIZED"
                if item["decision_id"] == active_decision_id
                and item["action"] in PAGE_MATERIALIZING_ACTIONS
                else "IGNORED"
            ),
        })
    return {
        "groups": _normalize_page_groups(effective_groups),
        "automatic_signature": automatic_signature,
        "effective_signature": effective_signature,
        "scope_changed": effective_signature != automatic_signature,
        "scope_applied": active is not None,
        "decision_ids": sorted({
            item["decision_id"] for item in decisions if item["decision_id"]
        }),
        "action_outcomes": outcomes,
        "action_state": (
            "MATERIALIZED" if active is not None
            else "IGNORED" if decisions
            else "NONE"
        ),
    }


def _suggestion_actions(
    sheet_suggestions: Mapping[str, Any] | None,
    answers: Mapping[str, Any] | None,
) -> dict[str, str]:
    """Project persisted, still-current PAGE actions for reloadable UI state."""
    questions = {
        str(question.get("question_id") or ""): question
        for question in _sheet_suggestion_questions(sheet_suggestions)
    }
    projected: dict[str, str] = {}
    for decision in (answers or {}).get("decisions") or []:
        if not isinstance(decision, Mapping):
            continue
        question = questions.get(str(decision.get("question_id") or ""))
        if not question or (
            decision.get("question_input_signature")
            != question.get("input_signature")
        ):
            continue
        suggestion_id = str(
            (question.get("context") or {}).get("suggestion_id") or ""
        )
        action = str(decision.get("answer") or "")
        if suggestion_id and action:
            projected[suggestion_id] = action
    return dict(sorted(projected.items()))


def _suggestion_action_semantics(
    state: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    sheet_scope = ((state or {}).get("stages") or {}).get("sheet_scope") or {}
    return {
        "state": str(sheet_scope.get("page_action_state") or "NONE"),
        "scope_applied": bool(sheet_scope.get("scope_applied")),
        "pipeline_rerun": bool(sheet_scope.get("pipeline_rerun")),
        "generation_was_materialized": bool(
            sheet_scope.get("generation_was_materialized")
        ),
        "this_update_reran": bool(sheet_scope.get("this_update_reran")),
        "generation_run_id": (state or {}).get("run_id"),
        "effective_page_groups": copy.deepcopy(
            sheet_scope.get("effective_page_groups") or []
        ),
        "outcomes": copy.deepcopy(sheet_scope.get("page_action_outcomes") or []),
    }


def _merge_suggestion_questions(
    queue: Mapping[str, Any],
    sheet_suggestions: Mapping[str, Any] | None,
    answers: Mapping[str, Any] | None,
    review_module: Any,
) -> dict[str, Any]:
    custom = _sheet_suggestion_questions(sheet_suggestions)
    all_questions = [
        *[dict(item) for item in queue.get("questions") or []],
        *custom,
    ]
    all_questions.sort(key=lambda item: (
        {"SHEET": 0, "ENTITY": 1, "CHANGE": 2}.get(item.get("category"), 9),
        str(item.get("question_id") or ""),
    ))
    by_id = {str(item["question_id"]): item for item in all_questions}
    decisions = [
        item for item in (answers or {}).get("decisions") or []
        if isinstance(item, Mapping)
    ]
    resolved = {
        str(decision.get("question_id"))
        for decision in decisions
        if not review_module.decision_is_stale(
            decision, by_id.get(str(decision.get("question_id") or ""))
        )
        and (
            getattr(
                review_module,
                "_decision_resolves_question",
                lambda _decision, _question: True,
            )(
                decision,
                by_id.get(str(decision.get("question_id") or "")),
            )
        )
    }
    pending = [
        item for item in all_questions if item["question_id"] not in resolved
    ]
    category_counts = {
        category: sum(item.get("category") == category for item in pending)
        for category in ("SHEET", "ENTITY", "CHANGE")
    }
    question_signatures = {
        item["question_id"]: item["input_signature"] for item in all_questions
    }
    result = dict(queue)
    result.update({
        "input_signature": content_signature({
            "producer": "production-review-queue-with-page-suggestions-v1",
            "base_queue": queue.get("input_signature"),
            "question_signatures": question_signatures,
        }),
        "questions": pending,
        "question_signatures": question_signatures,
        "resolved_question_ids": sorted(resolved),
        "counts": {
            "total": len(pending),
            "pending": len(pending),
            "resolved_unchanged": len(resolved),
            "stale_decisions": sum(
                review_module.decision_is_stale(
                    decision, by_id.get(str(decision.get("question_id") or ""))
                )
                for decision in decisions
            ),
            "by_category": category_counts,
            **category_counts,
        },
    })
    advisory = [item for item in pending if item.get("advisory")]
    result["counts"]["advisory"] = len(advisory)
    # Обязательными считаются только вопросы, без ответа на которые анализ
    # неполон. Рекомендации сопоставителя к ним не относятся.
    result["counts"]["blocking"] = len(pending) - len(advisory)
    result.setdefault("diagnostics", {})["page_suggestion_questions"] = len(custom)
    return result


def _build_review_questions(
    *,
    sheet_relations: Mapping[str, Any],
    sheet_suggestions: Mapping[str, Any] | None,
    entity_relations: Mapping[str, Any],
    synthesis: Mapping[str, Any],
    answers: Mapping[str, Any] | None,
    ai_resolutions: Mapping[str, Any] | None = None,
    input_mode: str = "DOCUMENT",
) -> dict[str, Any]:
    """Собрать очередь вопросов инженеру.

    В режиме «страница ↔ страницу» пару выбрал сам человек, и вопросы о том,
    как сопоставитель соотнёс ОСТАЛЬНЫЕ листы обоих документов, к его выбору
    отношения не имеют. Сопоставитель здесь совещательный: его предложения
    приходят отдельной, необязательной строкой, а не одиннадцатью вопросами о
    листах, которых инженер не выбирал.
    """
    include_sheet_questions = str(input_mode).upper() != "PAGE"
    try:
        module = importlib.import_module(
            "backend.app.services.stage_comparison.review_queue"
        )
    except ModuleNotFoundError:
        return _empty_questions(sheet_relations, entity_relations, synthesis)
    builder = getattr(module, "build_review_queue", None) or getattr(
        module, "build_review_questions", None
    )
    if builder is None:
        return _empty_questions(sheet_relations, entity_relations, synthesis)
    try:
        base = builder(
            sheet_relations,
            entity_relations,
            synthesis,
            human_decisions=None,
            ai_resolutions=ai_resolutions,
            include_sheet_questions=include_sheet_questions,
        )
        answered = lambda: builder(  # noqa: E731 — одна и та же форма вызова
            sheet_relations,
            entity_relations,
            synthesis,
            human_decisions=answers,
            ai_resolutions=ai_resolutions,
            include_sheet_questions=include_sheet_questions,
        )
    except TypeError:
        # Сборка очереди без этого параметра: в PAGE-режиме тогда действует
        # прежнее поведение, и это видно по отсутствию флага, а не молча.
        base = builder(
            sheet_relations, entity_relations, synthesis,
            human_decisions=None, ai_resolutions=ai_resolutions,
        )
        answered = lambda: builder(  # noqa: E731
            sheet_relations, entity_relations, synthesis,
            human_decisions=answers, ai_resolutions=ai_resolutions,
        )
    if not sheet_suggestions:
        return answered()
    return _merge_suggestion_questions(
        base, sheet_suggestions, answers, module
    )


def _ai_resolution_stage(artifact: Mapping[str, Any] | None) -> dict[str, Any]:
    """Карточка этапа для конвейера: сколько закрыто и сколько осталось людям."""
    diagnostics = (artifact or {}).get("diagnostics") or {}
    total = int(diagnostics.get("input_items") or 0)
    resolved = int(diagnostics.get("ai_resolved") or 0)
    mode = str((artifact or {}).get("mode") or ai_settings.MODE_OFF)
    runtime_ready = diagnostics.get("runtime_ready")
    layer_error = str(diagnostics.get("layer_error") or "")
    cancelled = bool(diagnostics.get("cancelled"))
    mode_completeness = str(diagnostics.get("mode_completeness") or "COMPLETE")
    unrecovered_calls = (
        int(diagnostics.get("model_failures") or 0)
        + int(diagnostics.get("model_timeouts") or 0)
    )
    if mode == ai_settings.MODE_OFF:
        status = "NOT_APPLICABLE"
    elif cancelled:
        # Инженер сам нажал «остановить»: это не отказ и тем более не «готово».
        status = "CANCELLED"
    elif runtime_ready is False:
        # Слой обещал разбор и не смог его начать. «Готово» здесь было бы
        # неправдой: ни один элемент не разобран, и инженер должен это видеть.
        status = "PARTIAL"
    elif layer_error:
        # Слой упал целиком. Артефакт пустой, элементы уехали человеку.
        status = "PARTIAL"
    elif mode_completeness != "COMPLETE":
        # «Глубокая проверка» без состоявшегося критика — это не та проверка,
        # которую обещали инженеру.
        status = "PARTIAL"
    elif unrecovered_calls:
        # Отказ или таймаут, переживший повторы: эти элементы разбора не
        # получили. Отдельная строка причин объясняет, сколько именно.
        status = "PARTIAL"
    else:
        status = "COMPLETED"
    return {
        "status": status,
        "mode": mode,
        "layer_error": layer_error,
        "cancelled": cancelled,
        "run_mode": ai_settings.run_mode_label(mode),
        "runtime_ready": True if runtime_ready is None else bool(runtime_ready),
        "runtime_problems": list(diagnostics.get("runtime_problems") or []),
        "total": total,
        "processed": total,
        "ai_resolved": resolved,
        "human_required": int(diagnostics.get("human_required") or 0),
        "verifier_rejected": int(diagnostics.get("verifier_rejected") or 0),
        "critic_rejected": int(diagnostics.get("critic_rejected") or 0),
        "model_calls": int(diagnostics.get("model_calls") or 0),
        "model_failures": int(diagnostics.get("model_failures") or 0),
        "model_timeouts": int(diagnostics.get("model_timeouts") or 0),
        "cache_hits": int(((diagnostics.get("cache") or {}).get("hits")) or 0),
        # Разбор по чертежу инженер видит отдельной строкой прогресса, поэтому
        # его счётчики обязаны доехать до карточки этапа, а не остаться только
        # в артефакте.
        "vision_items": int(diagnostics.get("vision_items") or 0),
        "vision_calls": int(diagnostics.get("vision_calls") or 0),
        # Глубокий режим обещает дополнительную проверку. Если провести её не
        # удалось, инженер обязан видеть «выполнено частично», а не «готово».
        "critic_required": int(diagnostics.get("critic_required") or 0),
        "critic_unavailable": int(diagnostics.get("critic_unavailable") or 0),
        # Ответ критика, не выполнивший контракт, — тоже несостоявшаяся
        # проверка, и прятать её за общим счётчиком нельзя: «не ответил» и
        # «ответил не по форме» чинятся разными руками.
        "critic_invalid": int(diagnostics.get("critic_invalid") or 0),
        "mode_completeness": str(
            diagnostics.get("mode_completeness") or "COMPLETE"
        ),
        "duration_ms": int(diagnostics.get("duration_ms") or 0),
        "human_reasons": dict(diagnostics.get("human_reasons") or {}),
        "budgets_hit": list(diagnostics.get("budgets_hit") or []),
    }


def _load_load_tables(session_id: str, pair_id: str) -> dict[str, Any]:
    """Прочитанные строки таблиц нагрузок обеих сторон.

    Живут внутри артефакта графической ветки: отдельного файла у них нет, а
    вопрос об идентичности без них не построить.
    """
    mode2 = production_store.load_artifact(session_id, pair_id, "direct_page_mode2")
    diagnostics = (mode2 or {}).get("diagnostics")
    tables = (
        diagnostics.get("electrical_load_tables")
        if isinstance(diagnostics, Mapping)
        else None
    )
    return dict(tables) if isinstance(tables, Mapping) else {}


def _build_routing_inventory(
    session_id: str,
    pair_id: str,
    *,
    synthesis: Mapping[str, Any] | None,
    preparation: Mapping[str, Any] | None,
    mode: str,
) -> dict[str, Any]:
    """Инвентаризация нерешённого — и в «Быстро» тоже, без единого вызова.

    Она объясняет, почему элемент НЕ уехал модели, и это объяснение обязано
    существовать в обоих режимах: иначе разницу между ними видно только по
    двум прогонам подряд.
    """
    inventory = ai_routing.build_inventory(
        synthesis=synthesis,
        preparation=preparation,
        electrical_table_changes=production_store.load_artifact(
            session_id, pair_id, "electrical_table_changes"
        ),
        document_inconsistencies=production_store.load_artifact(
            session_id, pair_id, "document_inconsistencies"
        ),
        load_tables=_load_load_tables(session_id, pair_id),
        change_is_review=change_is_review,
        change_describe=describe_change,
        pair_id=pair_id,
        mode=mode,
        generated_at=utc_now(),
    )
    production_store.save_artifact(
        session_id, pair_id, "ai_routing_inventory", inventory
    )
    return inventory


def _empty_identity_artifact(pair_id: str, mode: str) -> dict[str, Any]:
    return {
        "kind": "stage_comparison_ai_table_identity",
        "schema_version": "ai-table-identity.v1",
        "version": 1,
        "pair_id": pair_id,
        "mode": mode,
        "generated_at": utc_now(),
        "resolutions": [],
        "derived_changes": [],
        "derived_unchanged": [],
        "derived_blocked": [],
        "resolved_row_ids": [],
        "diagnostics": {
            "questions": 0,
            "batches": 0,
            "identity_resolved": 0,
            "human_required": 0,
            "human_reasons": {},
            "derived_changes": 0,
            "uses_model": False,
        },
        "constraints": {
            # Тождество предложено моделью, значения посчитаны правилами.
            "identity_from_model": True,
            "values_from_model": False,
        },
    }


def _run_table_identity(
    session_id: str,
    pair_id: str,
    *,
    inventory: Mapping[str, Any],
    mode: str,
    publish_progress: Any,
) -> dict[str, Any]:
    """Разобрать тождество строк таблиц — второй проход ИИ-слоя.

    Отдельный проход, а не расширение текстового: у него другой вопрос,
    другая схема, другой верификатор и другой артефакт. Смешать их значило бы
    сложить в один счётчик «модель не смогла прочитать левый лист» и «модель
    не смогла выбрать пару», которые чинятся разными руками.
    """
    if mode == ai_settings.MODE_OFF:
        artifact = _empty_identity_artifact(pair_id, mode)
        production_store.save_artifact(
            session_id, pair_id, "ai_table_identity", artifact
        )
        return artifact
    if not ai_routing.eligible_ids(inventory, ai_routing.KIND_TABLE_UNPROVEN) and (
        not ai_routing.eligible_ids(inventory, ai_routing.KIND_TABLE_BLOCKED)
    ):
        artifact = _empty_identity_artifact(pair_id, mode)
        production_store.save_artifact(
            session_id, pair_id, "ai_table_identity", artifact
        )
        return artifact

    load_tables = _load_load_tables(session_id, pair_id)
    control = _RUN_CONTROL.get()
    cache_dir = production_store.artifact_path(
        session_id, pair_id, "ai_resolutions"
    ).parent / "ai_response_cache"
    layer = ai_resolution.AiResolutionLayer(
        cache_dir=cache_dir,
        cancel=control.cancel_token if control is not None else None,
        run_id=control.run_id if control is not None else "",
        mode=mode,
    )
    started = time.perf_counter()
    publish_progress(
        current_stage="unified_synthesis",
        current_substage="ai_table_identity",
        message="ИИ-разбор строк таблиц…",
        stage_key="ai_table_identity",
        stage_status="RUNNING",
    )
    try:
        section = layer.resolve_identity(
            inventory=inventory,
            load_tables=load_tables,
            contradictions=load_tables,
            compare_match=compare_electrical_match,
            taken_rows=ai_routing.matched_row_ids(
                production_store.load_artifact(
                    session_id, pair_id, "electrical_table_changes"
                )
            ),
        )
    except Exception as exc:  # noqa: BLE001 — проход не роняет прогон
        ai_gateway.kill_live_processes(layer.run_id)
        artifact = _empty_identity_artifact(pair_id, mode)
        artifact["diagnostics"]["layer_error"] = type(exc).__name__
        production_store.save_artifact(
            session_id, pair_id, "ai_table_identity", artifact
        )
        return artifact

    artifact = {
        **_empty_identity_artifact(pair_id, mode),
        **{key: value for key, value in section.items() if key != "diagnostics"},
        "diagnostics": {**section["diagnostics"], "uses_model": True},
    }
    production_store.save_artifact(
        session_id, pair_id, "ai_table_identity", artifact
    )
    publish_progress(
        current_stage="unified_synthesis",
        current_substage="ai_table_identity",
        message="ИИ-разбор строк таблиц завершён.",
        duration_ms=max(0, int((time.perf_counter() - started) * 1000)),
        stage_key="ai_table_identity",
        stage_status="COMPLETED",
        stage_update=dict(artifact["diagnostics"]),
    )
    return artifact


def _run_ai_resolution(
    session_id: str,
    pair_id: str,
    *,
    synthesis: Mapping[str, Any],
    preparation: Mapping[str, Any] | None,
    sheet_relations: Mapping[str, Any],
    comparison_groups: Iterable[Mapping[str, Any]],
    publish_progress: Any,
    pair: Mapping[str, Any] | None = None,
    graphic_route: str | None = None,
    ai_mode: str | None = None,
    routing_inventory: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Разрешить неоднозначные расхождения моделью — или честно не разрешить.

    Слой врезан РОВНО между детерминированным синтезом и построением вопросов:
    ниже по течению всё работает так же, как если бы типизированный ответ дал
    человек. Выключенный слой (режим OFF) пишет пустой артефакт и не делает ни
    одного вызова — поведение системы совпадает со сборкой без ИИ.

    Отказ слоя не роняет прогон: элементы возвращаются человеку с причиной.
    """
    all_review_items = [
        item for item in synthesis.get("review_items") or []
        if isinstance(item, Mapping)
    ]
    # Маршрут решает инвентаризация, а не «всё, что осталось». Элемент, у
    # которого противоположной стороны нет в прочитанном виде, модель может
    # только отклонить — и на паре ГРЩ ровно это и произошло: одиннадцать
    # примечаний правого листа против левого, где текстового слоя в этом
    # месте нет вовсе, стоили семи обращений и ста тридцати пяти секунд ради
    # одиннадцати отказов.
    eligible = set(ai_routing.eligible_ids(
        routing_inventory or {}, ai_routing.KIND_TEXT_REVIEW,
    )) if routing_inventory is not None else None
    review_items = (
        all_review_items if eligible is None
        else [
            item for item in all_review_items
            if str(item.get("review_evidence_id") or "") in eligible
        ]
    )
    mode = ai_settings.normalize_mode(ai_mode) if ai_mode else ai_settings.mode()
    if mode == ai_settings.MODE_OFF or not review_items or not preparation:
        # Режим обязан доехать до артефакта даже когда разбирать было нечего:
        # «глубокая проверка» без единого неоднозначного элемента — это всё
        # ещё глубокая проверка, а не прогон в режиме «Быстро».
        artifact = ai_resolution.empty_artifact(mode=mode)
        production_store.save_artifact(
            session_id, pair_id, "ai_resolutions", artifact
        )
        return artifact

    # Среда проверяется ДО первого вызова. Отсутствующий CLI, модель, которой
    # эта версия не знает, или сессия, у которой осталась оболочка, обязаны
    # остановить слой честно, а не превратиться в четыреста отказов модели на
    # четырёхстах элементах.
    try:
        deep = mode == ai_settings.MODE_DEEP
        runtime = ai_gateway.validate_runtime(
            require_vision=deep, deep=deep, mode=mode,
        )
    except Exception as exc:  # noqa: BLE001 — проверка не роняет прогон
        runtime = {"ok": False, "problems": [type(exc).__name__], "checks": {}}
    if not runtime.get("ok"):
        artifact = ai_resolution.unavailable_artifact(
            review_items, runtime=runtime, mode=mode,
        )
        production_store.save_artifact(
            session_id, pair_id, "ai_resolutions", artifact
        )
        unavailable_stage = _ai_resolution_stage(artifact)
        publish_progress(
            current_stage="unified_synthesis",
            current_substage="ai_resolution",
            message="ИИ-анализ не запущен: среда не готова.",
            stage_key="ai_resolution",
            # Статус берётся у самого этапа, а не назначается на месте вызова:
            # слой обещал разбор и не смог его начать — это «частично».
            stage_status=unavailable_stage["status"],
            stage_update=unavailable_stage,
        )
        return artifact

    # Сессии прошлых прогонов, переживших падение бэкенда, держат соединение
    # с провайдером и жгут лимит подписки. Убираем их до старта своих.
    ai_gateway.reap_orphaned_processes(
        keep_run_id=(_RUN_CONTROL.get().run_id if _RUN_CONTROL.get() else "")
    )
    started_at = utc_now()
    started_perf = time.perf_counter()
    publish_progress(
        current_stage="unified_synthesis",
        current_substage="ai_resolution",
        message="ИИ-анализ текста…",
        processed=0,
        total=len(review_items),
        unit="ai_items",
        stage_key="ai_resolution",
        stage_status="RUNNING",
        stage_started_at=started_at,
        stage_update={
            "mode": mode,
            "run_mode": ai_settings.run_mode_label(mode),
            "total": len(review_items),
        },
    )

    def report(payload: Mapping[str, Any]) -> None:
        publish_progress(
            current_stage="unified_synthesis",
            current_substage="ai_resolution",
            message="ИИ-анализ текста…",
            processed=int(payload.get("processed") or 0),
            total=int(payload.get("total") or 0),
            unit="ai_items",
            duration_ms=max(0, int((time.perf_counter() - started_perf) * 1000)),
            stage_key="ai_resolution",
            stage_status="RUNNING",
            stage_started_at=started_at,
            stage_update={
                "mode": mode,
                "run_mode": ai_settings.run_mode_label(mode),
                "total": int(payload.get("total") or 0),
                "processed": int(payload.get("processed") or 0),
                "ai_resolved": int(payload.get("resolved") or 0),
                "human_required": int(payload.get("human") or 0),
            },
        )

    cache_dir = production_store.artifact_path(
        session_id, pair_id, "ai_resolutions"
    ).parent / "ai_response_cache"
    pdf_paths: dict[str, str] = {}
    if isinstance(pair, Mapping):
        for side, key in (("LEFT", "left"), ("RIGHT", "right")):
            document = pair.get(key)
            path = (
                str(document.get("pdf_path") or "")
                if isinstance(document, Mapping)
                else ""
            )
            if path and Path(path).is_file():
                pdf_paths[side] = path
    control = _RUN_CONTROL.get()
    layer = ai_resolution.AiResolutionLayer(
        cache_dir=cache_dir,
        progress=report,
        pdf_paths=pdf_paths,
        graphic_route=graphic_route,
        # Отмена прогона обязана доходить до живых CLI-сессий: без этого
        # «остановить» означало бы «перестать ждать», а сотни процессов
        # продолжали бы жечь лимит подписки.
        cancel=control.cancel_token if control is not None else None,
        run_id=control.run_id if control is not None else "",
        mode=mode,
    )
    try:
        artifact = layer.resolve(
            review_items=review_items,
            preparation=preparation,
            sheet_relations=sheet_relations,
            comparison_groups=list(comparison_groups),
            retrieved={
                str(entry.get("item_id") or ""):
                    (entry.get("routing_payload") or {}).get("retrieved") or {}
                for entry in (routing_inventory or {}).get("items") or ()
                if isinstance(entry, Mapping)
                and (entry.get("routing_payload") or {}).get("retrieved")
            },
        )
    except Exception as exc:  # noqa: BLE001 — слой не имеет права ронять прогон
        ai_gateway.kill_live_processes(layer.run_id)
        # Режим прогона обязан пережить отказ слоя: артефакт упавшей
        # «глубокой проверки», записанный как «Быстро», объясняет не тот
        # прогон, к которому приложен.
        artifact = ai_resolution.empty_artifact(mode=mode)
        artifact["diagnostics"]["layer_error"] = type(exc).__name__
    production_store.save_artifact(
        session_id, pair_id, "ai_resolutions", artifact
    )
    stage = _ai_resolution_stage(artifact)
    publish_progress(
        current_stage="unified_synthesis",
        current_substage="ai_resolution",
        message=(
            "ИИ-анализ текста завершён."
            if stage["status"] == "COMPLETED"
            else "ИИ-анализ текста выполнен не полностью."
        ),
        processed=stage["processed"],
        total=stage["total"],
        unit="ai_items",
        duration_ms=max(0, int((time.perf_counter() - started_perf) * 1000)),
        stage_key="ai_resolution",
        # Исход объявляет этап, а не место вызова. Жёсткое «COMPLETED» здесь
        # перекрывало и отказ среды, и упавший слой, и несостоявшегося критика.
        stage_status=stage["status"],
        stage_started_at=started_at,
        stage_completed_at=utc_now(),
        stage_update=stage,
    )
    return artifact


def _sheet_comparison_groups(
    relations: Mapping[str, Any] | None,
) -> list[dict[str, Any]]:
    """Return the exact DOCUMENT groups that producer branches may consume.

    Only a proven pair is compared.  A relation the matcher merely proposed
    (``POSSIBLE``/``UNCERTAIN``) already has its own Stage 5 sheet question;
    letting it in here would publish hundreds of findings derived from a pair
    of drawings nobody confirmed — which is what produced 814 «added» rows on
    the ЭОМ pair from four relations at confidence 0.29-0.34.
    """
    groups = []
    for relation in (relations or {}).get("relations") or []:
        if not isinstance(relation, Mapping):
            continue
        if not sheet_scope_policy.is_effective(relation):
            continue
        left_pages = sorted({int(page) for page in relation.get("left_pages") or []})
        right_pages = sorted({
            int(page) for page in relation.get("right_pages") or []
        })
        groups.append({
            "id": relation.get("relation_id"),
            "left_pages": left_pages,
            "right_pages": right_pages,
            "relation_type": str(relation.get("relation_type") or "MATCHED"),
            "status": relation.get("status"),
        })
    groups.sort(key=lambda item: (
        item["left_pages"],
        item["right_pages"],
        str(item.get("id") or ""),
    ))
    return groups


def _sheet_relation_counts(
    relations: Mapping[str, Any] | None,
) -> dict[str, int]:
    """Publish status and cardinality counters used by the pipeline UI."""
    counts: dict[str, int] = {}
    for relation in (relations or {}).get("relations") or []:
        if not isinstance(relation, Mapping):
            continue
        status = str(relation.get("status") or "UNKNOWN").upper()
        counts[status] = counts.get(status, 0) + 1
        relation_type = str(relation.get("relation_type") or "").upper()
        if relation_type in {"SPLIT", "MERGED"}:
            counts[relation_type] = counts.get(relation_type, 0) + 1
    return dict(sorted(counts.items()))


def _initial_pipeline_stages(input_mode: str) -> dict[str, dict[str, Any]]:
    """Expose a truthful transient state while the synchronous run is active."""
    return {
        "sheet_matching": {
            "status": "RUNNING" if input_mode == "DOCUMENT" else "PENDING_ADVISORY",
            "relations": 0,
            "relation_counts": {},
        },
        "sheet_scope": {"status": "NOT_STARTED", "groups": 0},
        "text": {"status": "NOT_STARTED", "atoms": 0, "deltas": 0},
        "graphic": {"status": "NOT_STARTED", "changes": 0},
    }


def _sheet_scope_signature(groups: Iterable[Mapping[str, Any]]) -> str:
    """Identify producer scope while ignoring confidence-only status changes."""
    normalized = [
        {
            "id": str(group.get("id") or group.get("relation_id") or ""),
            "left_pages": sorted({int(page) for page in group.get("left_pages") or []}),
            "right_pages": sorted({
                int(page) for page in group.get("right_pages") or []
            }),
            "relation_type": str(group.get("relation_type") or "MATCHED"),
        }
        for group in groups
    ]
    normalized.sort(key=lambda item: (
        item["left_pages"],
        item["right_pages"],
        item["id"],
    ))
    return content_signature({"direction": "LEFT_TO_RIGHT", "groups": normalized})


def _materialized_sheet_scope(
    automatic_relations: Mapping[str, Any],
    application: Mapping[str, Any],
) -> dict[str, Any]:
    """Project only complete, non-stale SHEET decisions onto branch scope.

    An incomplete OTHER or UNSURE decision must roll an older override back to
    the automatic relation rather than materializing a partial replacement.
    """
    effective = application.get("effective_sheet_relations")
    effective_by_id = {
        str(item.get("relation_id") or ""): item
        for item in (effective or {}).get("relations") or []
        if isinstance(item, Mapping)
    }
    materialized_relations = []
    decision_ids = []
    for automatic in automatic_relations.get("relations") or []:
        if not isinstance(automatic, Mapping):
            continue
        relation_id = str(automatic.get("relation_id") or "")
        candidate = effective_by_id.get(relation_id)
        decision = (
            candidate.get("human_decision")
            if isinstance(candidate, Mapping)
            else None
        )
        if (
            isinstance(candidate, Mapping)
            and isinstance(decision, Mapping)
            and not candidate.get("review_required")
        ):
            materialized = copy.deepcopy(dict(candidate))
            decision_id = str(decision.get("decision_id") or "")
            if decision_id:
                decision_ids.append(decision_id)
        else:
            materialized = copy.deepcopy(dict(automatic))
        materialized_relations.append(materialized)

    automatic_groups = _sheet_comparison_groups(automatic_relations)
    effective_groups = _sheet_comparison_groups({"relations": materialized_relations})
    automatic_signature = _sheet_scope_signature(automatic_groups)
    effective_signature = _sheet_scope_signature(effective_groups)
    pending = sheet_scope_policy.pending_relations(materialized_relations)
    return {
        "groups": effective_groups,
        "automatic_signature": automatic_signature,
        "effective_signature": effective_signature,
        "scope_changed": effective_signature != automatic_signature,
        "scope_applied": bool(decision_ids),
        "decision_ids": sorted(set(decision_ids)),
        # Pairs the matcher proposed but nobody confirmed.  They are not
        # compared, and the pipeline card says so instead of silently
        # shrinking the scope.
        "pending_confirmation": pending,
    }


def _apply_sheet_scope_diagnostics(
    application: Mapping[str, Any],
    projection: Mapping[str, Any],
    *,
    run_id: str,
    pipeline_rerun: bool,
    this_update_reran: bool | None = None,
    rerun_question_ids: Iterable[str] = (),
) -> dict[str, Any]:
    result = copy.deepcopy(dict(application))
    diagnostics = dict(result.get("diagnostics") or {})
    update_reran = (
        bool(pipeline_rerun)
        if this_update_reran is None
        else bool(this_update_reran)
    )
    rerun_ids = {str(value) for value in rerun_question_ids if value}
    outcomes = copy.deepcopy(projection.get("action_outcomes") or [])
    for outcome in outcomes:
        if not isinstance(outcome, dict):
            continue
        question_was_updated = (
            str(outcome.get("question_id") or "") in rerun_ids
        )
        if rerun_ids:
            # Mutation IDs are already reduced to actions that entered,
            # changed, or left the materialized PAGE scope.  Do not infer
            # causality from the final state: a batch may contain unrelated
            # repeated IGNORE answers next to the real scope change.
            outcome_reran = update_reran and question_was_updated
        else:
            # A manual full generation has no answer-mutation IDs.  Attribute
            # that generation to its one active materialized action only.
            outcome_reran = (
                update_reran and outcome.get("state") == "MATERIALIZED"
            )
        outcome["scope_applied"] = outcome.get("state") == "MATERIALIZED"
        outcome["pipeline_rerun"] = bool(outcome_reran)
        outcome["this_update_reran"] = bool(outcome_reran)
    diagnostics.update({
        "scope_applied": bool(projection.get("scope_applied")),
        "pipeline_rerun": bool(pipeline_rerun),
        "generation_was_materialized": bool(projection.get("scope_applied")),
        "this_update_reran": update_reran,
        "sheet_scope_changed": bool(projection.get("scope_changed")),
        "automatic_sheet_scope_signature": projection.get(
            "automatic_signature"
        ),
        "materialized_sheet_scope_signature": projection.get(
            "effective_signature"
        ),
        "sheet_scope_decision_ids": list(projection.get("decision_ids") or []),
        "generation_run_id": run_id,
        "effective_page_groups": copy.deepcopy(
            projection.get("groups") or []
        ),
        "page_action_state": str(projection.get("action_state") or "NONE"),
        "page_action_outcomes": outcomes,
    })
    result["diagnostics"] = diagnostics
    return result


def _refresh_decisions(
    session_id: str,
    pair_id: str,
    synthesis: Mapping[str, Any],
) -> dict[str, Any]:
    return production_store.mutate_artifact(
        session_id,
        pair_id,
        "engineer_decisions",
        lambda existing: build_engineer_decisions(
            synthesis,
            existing=existing if isinstance(existing, Mapping) else None,
        ),
        default={},
    )


def _preliminary_evidence_availability(
    synthesis: Mapping[str, Any] | None,
    source_snapshot: Mapping[str, Any],
    electrical_table_changes: Mapping[str, Any] | None,
    *,
    materialized_graphic_ledger: Mapping[str, Any] | None = None,
) -> dict[str, bool]:
    """Project the canonical evidence resolver into report-safe booleans."""
    if synthesis is None:
        return {}
    text = source_snapshot.get("text")
    graphic = source_snapshot.get("graphic")
    text_atoms = text.get("artifact") if isinstance(text, Mapping) else None
    graphic_ledger = (
        materialized_graphic_ledger
        if isinstance(materialized_graphic_ledger, Mapping)
        else graphic.get("ledger") if isinstance(graphic, Mapping) else None
    )
    return build_evidence_availability_index(
        synthesis=synthesis,
        text_atoms=text_atoms if isinstance(text_atoms, Mapping) else None,
        graphic_ledger=(
            graphic_ledger if isinstance(graphic_ledger, Mapping) else None
        ),
        electrical_table_changes=electrical_table_changes,
        documents={"LEFT": "LEFT", "RIGHT": "RIGHT"},
    )


def _human_review_evidence_availability(
    plan: Mapping[str, Any] | None,
    *,
    table_changes: Mapping[str, Any] | None,
    load_tables: Mapping[str, Any] | None,
    inconsistencies: Mapping[str, Any] | None,
) -> dict[str, bool]:
    """Expose HRO-only evidence targets to the preliminary-report UI."""
    if not isinstance(plan, Mapping):
        return {}
    target_ids: set[str] = set()
    for collection, identity_key in (
        (plan.get("groups") or (), "group_id"),
        (plan.get("standalone_questions") or (), "question_id"),
        (plan.get("ai_closed_questions") or (), "question_id"),
        (plan.get("metadata_changes") or (), "target_id"),
        (plan.get("text_requirement_changes") or (), "target_id"),
        (plan.get("missing_evidence") or (), "target_id"),
    ):
        for value in collection:
            if not isinstance(value, Mapping):
                continue
            identity = str(value.get(identity_key) or "")
            if identity:
                target_ids.add(identity)
            target_ids.update(
                str(target_id)
                for target_id in value.get("affected_target_ids") or ()
                if target_id
            )
    target_ids.update(
        str(value.get("inconsistency_id") or value.get("row_id") or "")
        for value in (inconsistencies or {}).get("items") or ()
        if isinstance(value, Mapping)
    )
    availability: dict[str, bool] = {}
    for target_id in sorted(target_ids):
        if not target_id:
            continue
        inline = _inline_human_review_evidence(
            target_id,
            plan=plan,
            table_changes=table_changes,
            load_tables=load_tables,
            inconsistencies=inconsistencies,
        )
        if inline is not None:
            availability[target_id] = True
    return availability


def _persist_preliminary_report(
    session_id: str,
    pair_id: str,
    synthesis: Mapping[str, Any] | None,
    source_snapshot: Mapping[str, Any],
    *,
    human_review_plan: Mapping[str, Any] | None = None,
    materialized_graphic_ledger: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Собрать и сохранить предварительный отчёт.

    Он строится ДО проверки инженером и отвечает на вопрос «что система
    нашла», а не «какие внутренние атомы существуют». Итоговый отчёт при этом
    не меняется: он по-прежнему собирается только из подтверждённых находок.
    """
    table_changes = production_store.load_artifact(
        session_id, pair_id, "electrical_table_changes"
    )
    inconsistencies = production_store.load_artifact(
        session_id, pair_id, "document_inconsistencies"
    )
    evidence_availability = _preliminary_evidence_availability(
        synthesis,
        source_snapshot,
        table_changes,
        materialized_graphic_ledger=materialized_graphic_ledger,
    )
    evidence_availability.update(_human_review_evidence_availability(
        human_review_plan,
        table_changes=table_changes,
        load_tables=_load_load_tables(session_id, pair_id),
        inconsistencies=inconsistencies,
    ))
    report = build_preliminary_report(
        pair_id=pair_id,
        synthesis=synthesis,
        document_inconsistencies=inconsistencies,
        electrical_table_changes=table_changes,
        ai_table_identity=production_store.load_artifact(
            session_id, pair_id, "ai_table_identity"
        ),
        human_review_plan=human_review_plan,
        evidence_availability=evidence_availability,
        generated_at=utc_now(),
    )
    production_store.save_artifact(session_id, pair_id, "preliminary_report", report)
    return report


def _persist_deterministic_human_review(
    session_id: str,
    pair_id: str,
    *,
    synthesis: Mapping[str, Any],
    decisions: Mapping[str, Any],
    text_preparation: Mapping[str, Any] | None,
    run_id: str,
    generation_input_signature: str,
) -> dict[str, Any]:
    """Build the production HRO read model directly from FAST artifacts.

    AI artifacts are deliberately not inputs to this projection.  Candidate
    modes may publish a replacement plan only after their independently
    verified materialization has completed.
    """
    plan = build_human_review_plan(
        pair_id=pair_id,
        synthesis=synthesis,
        engineer_decisions=decisions,
        electrical_table_changes=production_store.load_artifact(
            session_id, pair_id, "electrical_table_changes"
        ),
        text_preparation=text_preparation,
        document_inconsistencies=production_store.load_artifact(
            session_id, pair_id, "document_inconsistencies"
        ),
        resolved_row_ids=(),
        generated_at=utc_now(),
    )
    plan = {
        **plan,
        "generation_run_id": run_id,
        "generation_input_signature": generation_input_signature,
    }
    production_store.save_artifact(
        session_id, pair_id, "human_review_plan", plan
    )
    production_store.mutate_artifact(
        session_id,
        pair_id,
        "human_review_decisions",
        lambda existing: (
            dict(existing)
            if isinstance(existing, Mapping)
            and existing.get("input_signature") == plan.get("input_signature")
            else empty_human_review_decisions(plan)
        ),
        default={},
    )
    return plan


def _ai_v2_candidate_requested(ai_mode: Any) -> bool:
    """STANDARD becomes the controlled v2 candidate only behind its flag."""
    return (
        ai_v2_settings.enabled()
        and ai_settings.normalize_mode(ai_mode) == ai_settings.MODE_STANDARD
    )


def _question_closure_artifacts(
    session_id: str,
    pair_id: str,
    *,
    human_review_plan: Mapping[str, Any],
    engineer_decisions: Mapping[str, Any],
    preliminary_report: Mapping[str, Any],
) -> dict[str, Mapping[str, Any]]:
    """Freeze only existing FAST/HRO inputs; never invoke the general v3 flow."""
    artifact_names = (
        "direct_page_mode2",
        "document_inconsistencies",
        "electrical_table_changes",
        "unified_synthesis",
        "text_preparation",
        "sheet_relations",
        "ai_routing_inventory",
        "text_atoms",
        "bound_atoms",
        "entity_relations",
    )
    artifacts = {
        name: production_store.load_artifact(session_id, pair_id, name) or {}
        for name in artifact_names
    }
    artifacts.update({
        "graphic_change_ledger": production_store.load_artifact(
            session_id, pair_id, "graphic_ledger"
        ) or {},
        "human_review_plan": dict(human_review_plan),
        "engineer_decisions": dict(engineer_decisions),
        "preliminary_report": dict(preliminary_report),
    })
    return artifacts


def _run_ai_question_closure_candidate(
    session_id: str,
    pair_id: str,
    *,
    human_review_plan: Mapping[str, Any],
    engineer_decisions: Mapping[str, Any],
    preliminary_report: Mapping[str, Any],
    run_id: str,
    generation_input_signature: str,
    publish_progress: Callable[..., Mapping[str, Any]],
) -> dict[str, Any]:
    """Publish a closed plan only after the complete two-pass proof exists."""
    started_at = utc_now()
    started_perf = time.perf_counter()
    baseline = int(
        (human_review_plan.get("summary") or {}).get(
            "mandatory_human_interactions"
        ) or 0
    )
    publish_progress(
        current_stage="preliminary_report",
        current_substage="question_closure_eligibility",
        message="Автоматический анализ завершён",
        stage_key="question_closure",
        stage_status="RUNNING",
        stage_started_at=started_at,
        stage_update={
            "feature_flag": ai_question_closure_settings.FEATURE_FLAG,
            "hro_before": baseline,
            "fallback_preserves_fast_hro": True,
        },
    )
    artifacts = _question_closure_artifacts(
        session_id,
        pair_id,
        human_review_plan=human_review_plan,
        engineer_decisions=engineer_decisions,
        preliminary_report=preliminary_report,
    )
    frozen_fast = question_closure_fast_signature(artifacts)
    frozen_decisions = content_signature(engineer_decisions)
    human_decisions = production_store.load_artifact(
        session_id, pair_id, "human_review_decisions"
    ) or {}
    frozen_human_decisions = content_signature(human_decisions)
    publish_progress(
        current_stage="review_questions",
        current_substage="question_closure_selector",
        message=(
            "Проверяем, можно ли автоматически снять часть уточняющих вопросов"
        ),
        stage_key="question_closure",
        stage_status="RUNNING",
        stage_started_at=started_at,
    )
    try:
        result = run_production_question_closure(
            artifacts=artifacts,
            hro_plan=human_review_plan,
            human_decisions=human_decisions,
            pair_id=pair_id,
            cache_dir=production_store.artifact_path(
                session_id, pair_id, "ai_question_closure"
            ).parent / "ai_question_closure_cache",
            run_id=run_id,
        )
        state = production_store.load_artifact(session_id, pair_id, "state") or {}
        current_plan = production_store.load_artifact(
            session_id, pair_id, "human_review_plan"
        ) or {}
        current_human_decisions = production_store.load_artifact(
            session_id, pair_id, "human_review_decisions"
        ) or {}
        current_decisions = production_store.load_artifact(
            session_id, pair_id, "engineer_decisions"
        ) or {}
        current_artifacts = _question_closure_artifacts(
            session_id,
            pair_id,
            human_review_plan=human_review_plan,
            engineer_decisions=engineer_decisions,
            preliminary_report=preliminary_report,
        )
        if (
            state.get("run_id") != run_id
            or state.get("input_signature") != generation_input_signature
            or current_plan.get("input_signature")
            != human_review_plan.get("input_signature")
            or content_signature(current_human_decisions) != frozen_human_decisions
            or content_signature(current_decisions) != frozen_decisions
            or question_closure_fast_signature(current_artifacts) != frozen_fast
        ):
            raise ProductionStateConflictError(
                "Question Closure generation became stale before publication"
            )
        plan = dict(result["human_review_plan"])
        plan["generation_run_id"] = run_id
        plan["generation_input_signature"] = generation_input_signature
        production_store.save_artifact(
            session_id, pair_id, "ai_question_closure", result
        )
        production_store.save_artifact(
            session_id, pair_id, "human_review_plan", plan
        )
        hro_after = int(result.get("hro_after") or baseline)
        completed_at = utc_now()
        stage = {
            "status": (
                "COMPLETED" if result.get("status") == "COMPLETED"
                else "NOT_APPLICABLE"
            ),
            "feature_flag": ai_question_closure_settings.FEATURE_FLAG,
            "hro_before": baseline,
            "hro_after": hro_after,
            "closed": len(result.get("closed_questions") or ()),
            "model_calls": int(result.get("model_calls") or 0),
            "eligibility_duration_ms": int(
                result.get("eligibility_duration_ms") or 0
            ),
            "pass_1_duration_ms": int(result.get("pass_1_duration_ms") or 0),
            "pass_2_duration_ms": int(result.get("pass_2_duration_ms") or 0),
            "duration_ms": int(result.get("duration_ms") or 0),
            "unsupported_closures": int(result.get("unsupported_closures") or 0),
            "selector_disagreement": any(
                value.get("two_pass_unanimous") is False
                for value in result.get("outcomes") or ()
            ),
            "fallback_used": not bool(result.get("closed_questions")),
            "fast_preserved": True,
            "engineer_approvals_untouched": True,
            "completed_at": completed_at,
        }
        publish_progress(
            current_stage="review_questions",
            current_substage="human_review_projection",
            message=f"Требуется {hro_after} уточнений инженера",
            processed=hro_after,
            total=hro_after,
            unit="questions",
            duration_ms=stage["duration_ms"],
            stage_key="question_closure",
            stage_status=stage["status"],
            stage_started_at=started_at,
            stage_completed_at=completed_at,
            stage_update=stage,
        )
        return {"succeeded": True, "artifact": result, "plan": plan, "stage": stage}
    except ProductionRunCancelled:
        raise
    except Exception as exc:  # noqa: BLE001 - closure must never break FAST/HRO
        duration_ms = max(0, int((time.perf_counter() - started_perf) * 1000))
        failure = question_closure_failure_artifact(
            pair_id=pair_id,
            hro_plan=human_review_plan,
            reason=exc,
            duration_ms=duration_ms,
        )
        production_store.save_artifact(
            session_id, pair_id, "ai_question_closure", failure
        )
        completed_at = utc_now()
        stage = {
            "status": "FALLBACK",
            "feature_flag": ai_question_closure_settings.FEATURE_FLAG,
            "hro_before": baseline,
            "hro_after": baseline,
            "closed": 0,
            "model_calls": int(failure.get("model_calls") or 0),
            "duration_ms": duration_ms,
            "unsupported_closures": 0,
            "fallback_used": True,
            "fallback_message": failure["fallback_message"],
            "fast_preserved": True,
            "engineer_approvals_untouched": True,
            "completed_at": completed_at,
        }
        publish_progress(
            current_stage="review_questions",
            current_substage="question_closure_fallback",
            message=f"Требуется {baseline} уточнений инженера",
            processed=baseline,
            total=baseline,
            unit="questions",
            duration_ms=duration_ms,
            stage_key="question_closure",
            stage_status="FALLBACK",
            stage_started_at=started_at,
            stage_completed_at=completed_at,
            stage_update=stage,
        )
        return {
            "succeeded": False,
            "artifact": failure,
            "plan": dict(human_review_plan),
            "stage": stage,
        }


def _run_ai_v2_candidate(
    session_id: str,
    pair_id: str,
    *,
    synthesis: Mapping[str, Any],
    decisions: Mapping[str, Any],
    source_snapshot: Mapping[str, Any],
    fast_preliminary_report: Mapping[str, Any],
    publish_progress: Callable[..., Mapping[str, Any]],
) -> dict[str, Any]:
    """Run the accepted LOW/three-session candidate over frozen FAST output.

    Nothing is published until verifier and deterministic materialization both
    finish.  Therefore any timeout, crash or malformed response leaves the
    already persisted FAST synthesis/report/decisions byte-for-byte usable.
    """
    started_at = utc_now()
    started_perf = time.perf_counter()
    publish_progress(
        current_stage="unified_synthesis",
        current_substage="ai_v2_analysis",
        message="Автоматический анализ завершён",
        stage_key="ai_resolution",
        stage_status="RUNNING",
        stage_started_at=started_at,
        stage_update={
            "candidate": "AI_ANALYST_V2",
            "run_mode": "STANDARD",
            "fallback_preserves_fast": True,
        },
    )
    control = _RUN_CONTROL.get()
    try:
        runtime = ai_gateway.validate_runtime(
            require_vision=False,
            deep=False,
            mode=ai_settings.MODE_STANDARD,
        )
        if not runtime.get("ok"):
            raise RuntimeError("AI_V2_RUNTIME_UNAVAILABLE")
        publish_progress(
            current_stage="unified_synthesis",
            current_substage="ai_v2_analysis",
            message="ИИ анализирует неоднозначные места",
            stage_key="ai_resolution",
            stage_status="RUNNING",
            stage_started_at=started_at,
        )
        text_snapshot = source_snapshot.get("text") or {}
        graphic_snapshot = source_snapshot.get("graphic") or {}
        artifacts: dict[str, Mapping[str, Any]] = {
            "state": production_store.load_artifact(session_id, pair_id, "state") or {},
            "direct_page_mode2": production_store.load_artifact(
                session_id, pair_id, "direct_page_mode2"
            ) or {},
            "document_inconsistencies": production_store.load_artifact(
                session_id, pair_id, "document_inconsistencies"
            ) or {},
            "electrical_table_changes": production_store.load_artifact(
                session_id, pair_id, "electrical_table_changes"
            ) or {},
            "unified_synthesis": dict(synthesis),
            "text_preparation": production_store.load_artifact(
                session_id, pair_id, "text_preparation"
            ) or {},
            "sheet_relations": production_store.load_artifact(
                session_id, pair_id, "sheet_relations"
            ) or {},
            "ai_routing_inventory": production_store.load_artifact(
                session_id, pair_id, "ai_routing_inventory"
            ) or {},
            "preliminary_report": dict(fast_preliminary_report),
            "engineer_decisions": dict(decisions),
            "text_atoms": (
                text_snapshot.get("artifact")
                if isinstance(text_snapshot, Mapping) else {}
            ) or {},
            "bound_atoms": production_store.load_artifact(
                session_id, pair_id, "effective_bound_atoms"
            ) or {},
            "graphic_change_ledger": (
                graphic_snapshot.get("ledger")
                if isinstance(graphic_snapshot, Mapping) else {}
            ) or {},
            "entity_relations": production_store.load_artifact(
                session_id, pair_id, "entity_relations"
            ) or {},
        }
        cache_dir = production_store.artifact_path(
            session_id, pair_id, "ai_v2_run"
        ).parent / "ai_v2_response_cache"
        analyst = WholeDocumentAnalyst(
            artifacts=artifacts,
            pair_id=pair_id,
            effort="low",
            cache_dir=cache_dir,
            cancel=control.cancel_token if control is not None else None,
            run_id=control.run_id if control is not None else "",
        )
        run = analyst.run()
        production_store.save_artifact(session_id, pair_id, "ai_v2_run", run)
        diagnostics = run.get("diagnostics") or {}
        if int(diagnostics.get("model_failures") or 0) or int(
            diagnostics.get("model_timeouts") or 0
        ):
            raise RuntimeError("AI_V2_MODEL_INCOMPLETE")
        if int(diagnostics.get("unsupported_published") or 0):
            raise RuntimeError("AI_V2_UNSUPPORTED_PUBLICATION")
        publish_progress(
            current_stage="unified_synthesis",
            current_substage="ai_v2_verification",
            message="Проверяем выводы ИИ",
            processed=int(diagnostics.get("ai_resolved_verified") or 0),
            total=int(diagnostics.get("routed") or 0),
            unit="ai_items",
            duration_ms=max(0, int((time.perf_counter() - started_perf) * 1000)),
            stage_key="ai_resolution",
            stage_status="RUNNING",
            stage_started_at=started_at,
        )
        materialization = materialize_verified_resolutions(
            artifacts=artifacts,
            run=run,
            pair_id=pair_id,
            manual_audit=None,
            human_entity_relations=artifacts.get("entity_relations"),
        )
        materialization_diagnostics = materialization.get("diagnostics") or {}
        if int(materialization_diagnostics.get("unsupported_materialized") or 0):
            raise RuntimeError("AI_V2_UNSUPPORTED_MATERIALIZATION")
        plan = {
            **materialization["human_review_plan"],
            "generation_run_id": control.run_id if control is not None else None,
            "generation_input_signature": (
                artifacts.get("state") or {}
            ).get("input_signature"),
        }
        materialization = {**materialization, "human_review_plan": plan}
        production_store.save_artifact(
            session_id, pair_id, "ai_v2_materialization", materialization
        )
        production_store.save_artifact(
            session_id,
            pair_id,
            "human_review_plan",
            plan,
        )
        production_store.save_artifact(
            session_id,
            pair_id,
            "document_inconsistencies",
            materialization["document_inconsistencies"],
        )
        production_store.mutate_artifact(
            session_id,
            pair_id,
            "human_review_decisions",
            lambda existing: (
                dict(existing)
                if isinstance(existing, Mapping)
                and existing.get("input_signature")
                == plan.get("input_signature")
                else empty_human_review_decisions(
                    plan
                )
            ),
            default={},
        )
        completed_at = utc_now()
        stage = {
            "status": "COMPLETED",
            "candidate": "AI_ANALYST_V2",
            "run_mode": "STANDARD",
            "processed": int(diagnostics.get("routed") or 0),
            "total": int(diagnostics.get("routed") or 0),
            "ai_resolved": int(diagnostics.get("ai_resolved_verified") or 0),
            "human_required": int(diagnostics.get("human_required") or 0),
            "verifier_rejected": int(diagnostics.get("verifier_rejected") or 0),
            "model_calls": int(diagnostics.get("model_calls") or 0),
            "sessions": int(diagnostics.get("sessions") or 0),
            "cache_hits": int((diagnostics.get("cache") or {}).get("hits") or 0),
            "duration_ms": max(
                0, int((time.perf_counter() - started_perf) * 1000)
            ),
            "fallback_used": False,
            "fallback_message": None,
            "completed_at": completed_at,
        }
        publish_progress(
            current_stage="review_questions",
            current_substage="human_review_projection",
            message="Формируем вопросы инженеру",
            processed=int(
                (materialization["human_review_plan"].get("summary") or {}).get(
                    "mandatory_human_interactions"
                ) or 0
            ),
            total=int(
                (materialization["human_review_plan"].get("summary") or {}).get(
                    "mandatory_human_interactions"
                ) or 0
            ),
            unit="questions",
            duration_ms=stage["duration_ms"],
            stage_key="ai_resolution",
            stage_status="COMPLETED",
            stage_started_at=started_at,
            stage_completed_at=completed_at,
            stage_update=stage,
        )
        return {
            "succeeded": True,
            "run": run,
            "materialization": materialization,
            "stage": stage,
        }
    except ProductionRunCancelled:
        raise
    except Exception as exc:  # noqa: BLE001 - this layer must preserve FAST
        failed_at = utc_now()
        failure = {
            "kind": "stage_comparison_ai_v2_failure",
            "schema_version": "stage-comparison-ai-v2-failure.v1",
            "version": 1,
            "pair_id": pair_id,
            "generated_at": failed_at,
            "reason_code": type(exc).__name__,
            "fallback_used": True,
            "fallback_message": (
                "Расширенный анализ не удалось завершить. "
                "Автоматические результаты сохранены."
            ),
        }
        if production_store.load_artifact(session_id, pair_id, "ai_v2_run") is None:
            production_store.save_artifact(
                session_id, pair_id, "ai_v2_run", failure
            )
        stage = {
            "status": "PARTIAL",
            "candidate": "AI_ANALYST_V2",
            "run_mode": "STANDARD",
            "processed": 0,
            "total": 0,
            "model_calls": 0,
            "duration_ms": max(
                0, int((time.perf_counter() - started_perf) * 1000)
            ),
            "fallback_used": True,
            "fallback_message": failure["fallback_message"],
            "completed_at": failed_at,
        }
        publish_progress(
            current_stage="preliminary_report",
            current_substage="ai_v2_fallback",
            message=failure["fallback_message"],
            stage_key="ai_resolution",
            stage_status="PARTIAL",
            stage_started_at=started_at,
            stage_completed_at=failed_at,
            stage_update=stage,
        )
        return {"succeeded": False, "failure": failure, "stage": stage}


def _persist_latest_final_report(
    session_id: str,
    pair_id: str,
    synthesis: Mapping[str, Any],
    decisions: Mapping[str, Any],
) -> dict[str, Any]:
    """Converge the derived report if another process updates decisions."""
    current = dict(decisions)
    report: dict[str, Any] = {}
    for _attempt in range(5):
        report = build_final_report(synthesis, current, object_ref=None)
        production_store.save_artifact(
            session_id, pair_id, "final_report", report
        )
        latest = production_store.load_artifact(
            session_id, pair_id, "engineer_decisions"
        )
        if not latest or (
            latest.get("revision") == current.get("revision")
            and latest.get("input_signature") == current.get("input_signature")
        ):
            return report
        current = latest
    return report


def _artifact_state(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    return {
        "present": bool(payload),
        "input_signature": (payload or {}).get("input_signature"),
    }


def _progress_activity_warning_sec() -> int:
    """Return the soft no-activity warning threshold for a new generation."""
    raw = os.environ.get(PROGRESS_ACTIVITY_WARNING_ENV, "").strip()
    if not raw:
        return DEFAULT_PROGRESS_ACTIVITY_WARNING_SEC
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_PROGRESS_ACTIVITY_WARNING_SEC
    return (
        value
        if 1 <= value <= 86_400
        else DEFAULT_PROGRESS_ACTIVITY_WARNING_SEC
    )


def _duration_ms_since(started_at: Any) -> int | None:
    """Best-effort persisted elapsed time; never makes a run fail."""
    if not isinstance(started_at, str) or not started_at:
        return None
    try:
        started = datetime.fromisoformat(started_at)
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        elapsed = datetime.now(timezone.utc) - started.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None
    return max(0, int(elapsed.total_seconds() * 1000))


def _review_question_stage(
    questions: Mapping[str, Any],
) -> dict[str, Any]:
    """Publish pending/answered/all counts without changing queue semantics."""
    counts = dict(questions.get("counts") or {})
    pending = int(counts.get("pending", len(questions.get("questions") or [])) or 0)
    answered = int(counts.get("resolved_unchanged") or 0)
    # Этап ждёт только обязательных ответов. Рекомендация сопоставителя на
    # ручной паре страниц — это предложение, а не незакрытый вопрос.
    blocking = int(counts.get("blocking", pending) or 0)
    return {
        "status": "NEEDS_REVIEW" if blocking else "COMPLETED",
        "blocking": blocking,
        "advisory": int(counts.get("advisory") or 0),
        # Historical field: it has always meant the current pending projection.
        "questions": pending,
        "pending": pending,
        "answered": answered,
        "total": pending + answered,
        "counts": counts,
        **_artifact_state(questions),
    }


#: Исходы, которые публикует САМ этап, разобравшись в собственном результате.
#: Обобщённый статус прогресса не имеет права их перекрыть: «готово» поверх
#: «частично» или «отменено» — это не индикация, а неверный результат.
TERMINAL_STAGE_STATUSES = frozenset({
    "PARTIAL",
    "FAILED",
    "CHECK_BLOCKED",
    "CANCELLED",
    "NEEDS_REVIEW",
    "NOT_APPLICABLE",
    "HUMAN_REQUIRED",
    "BLOCKED",
    "SKIPPED",
    "NOT_CHECKED",
})


def _declared_stage_status(stage_update: Mapping[str, Any] | None) -> str | None:
    """Терминальный статус, объявленный самим этапом, если он его объявил."""
    if not isinstance(stage_update, Mapping):
        return None
    declared = str(stage_update.get("status") or "").strip().upper()
    return declared if declared in TERMINAL_STAGE_STATUSES else None


def _publish_progress_event(
    session_id: str,
    pair_id: str,
    run_id: str,
    *,
    current_stage: str | None,
    current_substage: str | None,
    message: str | None,
    processed: int | None = None,
    total: int | None = None,
    unit: str | None = None,
    duration_ms: int | None = None,
    run_duration_ms: int | None = None,
    current_item: Mapping[str, Any] | None = None,
    recent_unit_durations_ms: Iterable[int] = (),
    stage_key: str | None = None,
    stage_status: str | None = None,
    stage_started_at: str | None = None,
    stage_completed_at: str | None = None,
    stage_update: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist one real progress event for exactly one active generation.

    The pair-level lock serializes normal writers.  The run-id check is still
    required so a delayed callback can never overwrite a newer generation.
    A rejected event returns the current state byte-for-byte (including its
    revision and activity timestamp).
    """
    observed_at = utc_now()
    recent_durations = [
        max(0, int(value)) for value in recent_unit_durations_ms
    ][-5:]

    def update(existing: Any) -> Any:
        if not isinstance(existing, Mapping):
            return existing
        if (
            existing.get("run_id") != run_id
            or existing.get("status") != "RUNNING"
        ):
            return existing
        state = copy.deepcopy(dict(existing))
        state.update({
            "current_stage": current_stage,
            "current_substage": current_substage,
            "message": message,
            "processed": processed,
            "total": total,
            "unit": unit,
            "current_item": copy.deepcopy(dict(current_item))
            if isinstance(current_item, Mapping)
            else None,
            "recent_unit_durations_ms": recent_durations,
            "last_activity_at": observed_at,
            "duration_ms": max(0, int(
                run_duration_ms
                if run_duration_ms is not None
                else duration_ms
            ))
            if run_duration_ms is not None or duration_ms is not None
            else None,
            "updated_at": observed_at,
            "revision": int(existing.get("revision") or 0) + 1,
        })
        if stage_key:
            stages = copy.deepcopy(state.get("stages") or {})
            stage = dict(stages.get(stage_key) or {})
            # Единственный источник статуса этапа — сам этап. Обобщённый
            # статус прогресса заполняет пропуск, но не имеет права поднять
            # объявленный этапом терминальный исход до «готово».
            declared = _declared_stage_status(stage_update)
            if isinstance(stage_update, Mapping):
                stage.update(copy.deepcopy(dict(stage_update)))
            effective_status = declared or stage_status or None
            if effective_status:
                stage["status"] = effective_status
            progress = dict(stage.get("progress") or {})
            progress.update({
                "status": effective_status or progress.get("status") or "RUNNING",
                "started_at": stage_started_at or progress.get("started_at"),
                "last_activity_at": observed_at,
                "current_stage": current_stage,
                "current_substage": current_substage,
                "message": message,
                "processed": processed,
                "total": total,
                "unit": unit,
                "current_item": copy.deepcopy(dict(current_item))
                if isinstance(current_item, Mapping)
                else None,
                "recent_unit_durations_ms": recent_durations,
                "duration_ms": max(0, int(duration_ms))
                if duration_ms is not None
                else None,
            })
            if stage_completed_at:
                progress["completed_at"] = stage_completed_at
            stage["progress"] = progress
            stages[stage_key] = stage
            state["stages"] = stages
        return state

    value = production_store.mutate_artifact(
        session_id,
        pair_id,
        "state",
        update,
        default={},
    )
    return dict(value) if isinstance(value, Mapping) else {}


def _empty_text_atoms(
    *, run_id: str, generation_input_signature: str, source_state: str
) -> dict[str, Any]:
    """Return an explicit empty raw TEXT artifact for this generation."""
    input_signature = content_signature({
        "producer": TEXT_ATOM_BUILDER_VERSION,
        "run_id": run_id,
        "generation_input_signature": generation_input_signature,
        "source_state": source_state,
    })
    return {
        "kind": TEXT_ATOMS_KIND,
        "schema_version": TEXT_ATOMS_SCHEMA_VERSION,
        "version": 1,
        "input_signature": input_signature,
        "generated_at": utc_now(),
        "atoms": [],
        "diagnostics": {
            "stage3_evidence": 0,
            "atoms": 0,
            "unresolved_source_evidence": [],
            "one_property_per_atom": True,
            "legacy_stage5_used": False,
            "legacy_stage53_used": False,
            "source_state": source_state,
        },
        "provenance": {
            "producer": TEXT_ATOM_BUILDER_VERSION,
            "stage3_signature": None,
            "stage3_full_signature": None,
            "stage3_full_signature_version": None,
            "stage4_signature": None,
            "generation_input_signature": generation_input_signature,
        },
    }


def _build_source_snapshot(
    *,
    run_id: str,
    generation_input_signature: str,
    text_artifact: Mapping[str, Any],
    text_source_state: str,
    graphic_ledger: Mapping[str, Any] | None,
    graphic_source_state: str,
) -> dict[str, Any]:
    """Freeze raw producer output before any human-dependent projection."""
    text_copy = copy.deepcopy(dict(text_artifact))
    graphic_copy = (
        copy.deepcopy(dict(graphic_ledger))
        if isinstance(graphic_ledger, Mapping)
        else None
    )
    core = {
        "kind": SOURCE_SNAPSHOT_KIND,
        "schema_version": SOURCE_SNAPSHOT_SCHEMA_VERSION,
        "version": 1,
        "run_id": run_id,
        "generation_input_signature": generation_input_signature,
        "text": {
            "source_state": text_source_state,
            "content_digest": content_signature(text_copy),
            "artifact": text_copy,
        },
        "graphic": {
            "source_state": graphic_source_state,
            "content_digest": content_signature(graphic_copy),
            # ``None`` is an explicit, versioned absence.  It prevents an old
            # successful ledger from leaking into a blocked later generation.
            "ledger": graphic_copy,
        },
    }
    return {**core, "input_signature": content_signature(core)}


def _validate_source_snapshot(
    payload: Mapping[str, Any], state: Mapping[str, Any]
) -> dict[str, Any]:
    """Verify the snapshot belongs byte-for-byte to the published run."""
    if (
        payload.get("kind") != SOURCE_SNAPSHOT_KIND
        or payload.get("schema_version") != SOURCE_SNAPSHOT_SCHEMA_VERSION
        or payload.get("version") != 1
        or payload.get("run_id") != state.get("run_id")
        or payload.get("generation_input_signature") != state.get("input_signature")
    ):
        raise ProductionStateConflictError(
            "production source snapshot generation does not match state"
        )
    text_source = payload.get("text")
    graphic_source = payload.get("graphic")
    if not isinstance(text_source, Mapping) or not isinstance(
        graphic_source, Mapping
    ):
        raise ProductionStateConflictError("production source snapshot is malformed")
    text_artifact = text_source.get("artifact")
    graphic_ledger = graphic_source.get("ledger")
    if not isinstance(text_artifact, Mapping) or (
        graphic_ledger is not None and not isinstance(graphic_ledger, Mapping)
    ):
        raise ProductionStateConflictError("production source snapshot is malformed")
    if text_source.get("content_digest") != content_signature(text_artifact):
        raise ProductionStateConflictError("production TEXT snapshot digest changed")
    if graphic_source.get("content_digest") != content_signature(graphic_ledger):
        raise ProductionStateConflictError("production GRAPHIC snapshot digest changed")
    if (
        isinstance(graphic_ledger, Mapping)
        and graphic_ledger.get("kind") == PAGE_GRAPHIC_BUNDLE_KIND
    ):
        _validate_page_graphic_bundle(graphic_ledger)
    if (
        isinstance(graphic_ledger, Mapping)
        and graphic_ledger.get("kind") == DOCUMENT_GRAPHIC_BUNDLE_KIND
    ):
        _validate_document_graphic_bundle(graphic_ledger)
    core = {key: copy.deepcopy(value) for key, value in payload.items() if key != "input_signature"}
    actual = content_signature(core)
    expected = (
        ((state.get("stages") or {}).get("source_snapshot") or {})
        .get("input_signature")
    )
    if not expected or payload.get("input_signature") != actual or expected != actual:
        raise ProductionStateConflictError(
            "production source snapshot digest does not match state"
        )
    return copy.deepcopy(dict(payload))


def _load_published_source_snapshot(
    session_id: str, pair_id: str, state: Mapping[str, Any]
) -> dict[str, Any]:
    payload = production_store.load_artifact(
        session_id, pair_id, "source_snapshot"
    )
    if payload is None:
        raise ProductionStateConflictError("published source snapshot is missing")
    return _validate_source_snapshot(payload, state)


def _write_state(
    session_id: str,
    pair_id: str,
    value: Mapping[str, Any],
) -> dict[str, Any]:
    def update(existing: Any) -> dict[str, Any]:
        revision = int((existing or {}).get("revision") or 0) + 1 if isinstance(existing, Mapping) else 1
        return {**dict(value), "revision": revision, "updated_at": utc_now()}

    return production_store.mutate_artifact(
        session_id, pair_id, "state", update, default={}
    )


def _run_production_comparison_impl(
    session_id: str,
    pair_id: str,
    *,
    input_mode: str,
    left_pages: Iterable[Any] = (),
    right_pages: Iterable[Any] = (),
    left_block_ids: Iterable[Any] = (),
    right_block_ids: Iterable[Any] = (),
    ai_mode: str | None = None,
    review_answers_override: Mapping[str, Any] | None = None,
    page_groups_override: Iterable[Mapping[str, Any]] | None = None,
    page_scope_rerun: bool = False,
    page_rerun_question_ids: Iterable[str] = (),
    interrupted_run: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run the complete additive flow; TEXT and GRAPHIC fail independently."""
    pair = store.get_pair_for_production(session_id, pair_id)
    request = normalize_run_request(
        input_mode=input_mode,
        left_pages=left_pages,
        right_pages=right_pages,
        ai_mode=ai_mode,
        left_block_ids=left_block_ids,
        right_block_ids=right_block_ids,
    )
    function_lineage_run_mode = ai_settings.normalize_mode(request.get("ai_mode"))
    answers = (
        copy.deepcopy(dict(review_answers_override))
        if isinstance(review_answers_override, Mapping)
        else production_store.load_artifact(
            session_id, pair_id, "review_answers"
        )
    )
    page_groups = None
    if request["input_mode"] == "PAGE":
        page_groups = _normalize_page_groups(
            page_groups_override
            if page_groups_override is not None
            else [_selected_page_group(request)]
        )
        if page_groups_override is None and isinstance(answers, Mapping):
            previous_state = production_store.load_artifact(
                session_id, pair_id, "state"
            )
            previous_scope = (
                previous_state.get("generation_scope")
                if isinstance(previous_state, Mapping)
                and previous_state.get("status") in PUBLISHED_STATUSES
                # Сравнивается ИСТОЧНИК, а не конфигурация анализа: область
                # страниц прошлого прогона относится к тем же документам
                # независимо от того, в каком режиме его считали (и был ли
                # режим записан вообще).
                and source_request(previous_state.get("selection") or {})
                == source_request(request)
                else None
            )
            previous_groups = (
                previous_scope.get("page_groups")
                if isinstance(previous_scope, Mapping)
                else None
            )
            previous_suggestions = (
                previous_state.get("sheet_suggestions")
                if isinstance(previous_state, Mapping)
                and isinstance(previous_state.get("sheet_suggestions"), Mapping)
                else None
            )
            if previous_groups and previous_suggestions:
                normalized_previous_groups = _normalize_page_groups(
                    previous_groups
                )
                previous_sources_are_current = _input_signature(
                    pair,
                    request,
                    page_groups=normalized_previous_groups,
                ) == previous_state.get("input_signature")
                if previous_sources_are_current:
                    reused_projection = _page_action_projection(
                        request, previous_suggestions, answers
                    )
                    page_groups = reused_projection["groups"]
                    page_scope_rerun = bool(
                        reused_projection.get("scope_applied")
                    )
    elif page_groups_override is not None:
        raise ValueError("page_groups_override is supported only in PAGE mode")
    _validate_page_bounds(pair, request, page_groups)
    signature = _input_signature(pair, request, page_groups=page_groups)
    run_pairing = (
        manual_page_pairing(session_id, pair_id)
        if request["input_mode"] == "PAGE"
        else None
    )
    started_at = utc_now()
    run_started_perf = time.perf_counter()
    run_id = stable_id(
        "prun_", pair_id, signature, started_at, uuid4().hex, length=24
    )
    function_lineage_shadow_diagnostic = _function_lineage_shadow_gate(
        pair_id=pair_id,
        run_id=run_id,
        ai_mode=function_lineage_run_mode,
    )
    # Ручка прогона появляется вместе с его идентификатором: раньше отменять
    # нечего, позже — уже поздно, самый долгий этап может начаться первым.
    control = _register_run(session_id, pair_id, run_id)
    _RUN_CONTROL.set(control)
    base_state = {
        "kind": STATE_KIND,
        "schema_version": STATE_SCHEMA_VERSION,
        "version": 1,
        "session_id": session_id,
        "pair_id": pair_id,
        "run_id": run_id,
        "direction": "LEFT_TO_RIGHT",
        "input_mode": request["input_mode"],
        "selection": copy.deepcopy(request),
        "generation_scope": {
            "page_groups": copy.deepcopy(page_groups or []),
        },
        # Ось исходных данных, которую не видит подпись документов: ручная
        # пара страниц живёт в связках листов, а не в PDF и не в запросе.
        "source_scope": (
            {"manual_page_pairing": (run_pairing or {}).get("digest")}
            if request["input_mode"] == "PAGE"
            else {}
        ),
        "input_signature": signature,
        # Две оси, а не одна: «изменился ли вход» и «в каком режиме считали».
        "analysis_config": analysis_config(request),
        "analysis_config_signature": analysis_config_signature(request),
        "function_lineage_shadow": copy.deepcopy(
            function_lineage_shadow_diagnostic
        ),
        "status": "RUNNING",
        "progress": 0,
        "stale": False,
        "started_at": started_at,
        "last_activity_at": started_at,
        "current_stage": None,
        "current_substage": None,
        "message": "Production-анализ запущен.",
        "processed": None,
        "total": None,
        "unit": None,
        "current_item": None,
        "recent_unit_durations_ms": [],
        "duration_ms": 0,
        "stages": _initial_pipeline_stages(request["input_mode"]),
        "constraints": {
            "new_flow": True,
            "legacy_stage5_used": False,
            "legacy_stage53_used": False,
            "parent_relation_required": False,
            "sheet_matcher_is_page_gate": False,
            "activity_warning_threshold_sec": (
                _progress_activity_warning_sec()
            ),
        },
    }
    if isinstance(interrupted_run, Mapping) and interrupted_run.get("run_id"):
        base_state["recovered_from_interrupted_run"] = {
            "run_id": interrupted_run.get("run_id"),
            "status": "INTERRUPTED",
            "previous_status": interrupted_run.get("status"),
            "started_at": interrupted_run.get("started_at"),
            "last_activity_at": interrupted_run.get("last_activity_at"),
            "input_signature": interrupted_run.get("input_signature"),
            "interrupted_at": started_at,
        }
    _write_state(session_id, pair_id, base_state)

    progress_snapshots: dict[str, dict[str, Any]] = {}

    def publish_progress(
        *,
        current_stage: str | None,
        current_substage: str | None,
        message: str | None,
        processed: int | None = None,
        total: int | None = None,
        unit: str | None = None,
        duration_ms: int | None = None,
        current_item: Mapping[str, Any] | None = None,
        recent_unit_durations_ms: Iterable[int] = (),
        stage_key: str | None = None,
        stage_status: str | None = None,
        stage_started_at: str | None = None,
        stage_completed_at: str | None = None,
        stage_update: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        run_duration_ms = max(
            0, int((time.perf_counter() - run_started_perf) * 1000)
        )
        stage_duration_ms = duration_ms
        if stage_duration_ms is None and stage_started_at:
            # A just-started stage must not inherit elapsed time from all
            # earlier stages. Later callbacks pass their real stage elapsed.
            stage_duration_ms = 0
        state = _publish_progress_event(
            session_id,
            pair_id,
            run_id,
            current_stage=current_stage,
            current_substage=current_substage,
            message=message,
            processed=processed,
            total=total,
            unit=unit,
            duration_ms=stage_duration_ms,
            run_duration_ms=run_duration_ms,
            current_item=current_item,
            recent_unit_durations_ms=recent_unit_durations_ms,
            stage_key=stage_key,
            stage_status=stage_status,
            stage_started_at=stage_started_at,
            stage_completed_at=stage_completed_at,
            stage_update=stage_update,
        )
        if stage_key and state.get("run_id") == run_id:
            stage = (state.get("stages") or {}).get(stage_key) or {}
            if isinstance(stage.get("progress"), Mapping):
                progress_snapshots[stage_key] = copy.deepcopy(
                    dict(stage["progress"])
                )
        return state

    review_module = importlib.import_module(
        "backend.app.services.stage_comparison.review_queue"
    )
    sheet_suggestions = None
    sheet_scope_projection: dict[str, Any] | None = None
    text_started_at: str | None = None
    text_started_perf: float | None = None
    if request["input_mode"] == "PAGE":
        # The user-selected PAGE scope exists before Sheet Matcher.  Load only
        # the text index needed by Stage 2 here; candidate matching itself runs
        # after both main branches and is advisory.
        text_started_at = utc_now()
        text_started_perf = time.perf_counter()
        publish_progress(
            current_stage="content_analysis",
            current_substage="text_preparation",
            message="Подготовка данных выбранных страниц…",
            stage_key="text",
            stage_status="RUNNING",
            stage_started_at=text_started_at,
        )
        try:
            # PAGE scope is the user's own selection; the matcher is advisory
            # here and never sees these records, so no stamp is read.
            indexes = _production_sheet_indexes(pair, with_sheet_identity=False)
            page_index_error = None
        except (FileNotFoundError, OSError, ValueError, RuntimeError) as exc:
            indexes = {"left": [], "right": []}
            page_index_error = exc
        sheet_relations = match_sheets([], [])
        sheet_status = "PENDING_ADVISORY"
        groups = copy.deepcopy(page_groups or [])
    else:
        sheet_started_at = utc_now()
        sheet_started_perf = time.perf_counter()
        publish_progress(
            current_stage="sheet_matching",
            current_substage="sheet_candidate_matching",
            message="Сопоставление листов документов…",
            stage_key="sheet_matching",
            stage_status="RUNNING",
            stage_started_at=sheet_started_at,
        )
        _raise_if_cancelled(control)
        sheet_relations, indexes = _run_sheet_matcher(pair)
        sheet_status = "COMPLETED"
        sheet_completed_at = utc_now()
        sheet_units = len(indexes.get("left") or [])
        production_store.save_artifact(
            session_id, pair_id, "sheet_relations", sheet_relations
        )
        # Sheet Matcher v4 shadow: allowlisted pairs only, v3 stays the
        # production result above.  Flag-OFF runs skip this call entirely.
        if sheet_matcher_flags.shadow_enabled():
            sheet_matcher_v4_shadow_diagnostic = _maybe_run_sheet_matcher_v4_shadow(
                session_id,
                pair_id,
                run_id=run_id,
                input_mode=str(request["input_mode"]),
                pair=pair,
                production_sheet_relations=sheet_relations,
            )
            if sheet_matcher_v4_shadow_diagnostic is not None:
                # The run writes ``base_state`` at the end; a note stored only
                # through mutate_artifact would be overwritten by that write.
                base_state["sheet_matcher_v4_shadow"] = copy.deepcopy(
                    sheet_matcher_v4_shadow_diagnostic
                )
        publish_progress(
            current_stage="sheet_matching",
            current_substage="sheet_scope",
            message="Сопоставление листов завершено.",
            processed=sheet_units,
            total=sheet_units,
            unit="left_sheets",
            duration_ms=max(
                0, int((time.perf_counter() - sheet_started_perf) * 1000)
            ),
            stage_key="sheet_matching",
            stage_status="COMPLETED",
            stage_started_at=sheet_started_at,
            stage_completed_at=sheet_completed_at,
            stage_update={
                "relations": len(sheet_relations.get("relations") or []),
                "relation_counts": _sheet_relation_counts(sheet_relations),
            },
        )
        sheet_only_questions = _build_review_questions(
            sheet_relations=sheet_relations,
            sheet_suggestions=None,
            entity_relations={},
            synthesis={},
            answers=None,
        )
        sheet_only_application = review_module.apply_human_decisions(
            sheet_only_questions,
            answers or {"decisions": [], "input_signature": None},
            sheet_relations=sheet_relations,
        )
        sheet_scope_projection = _materialized_sheet_scope(
            sheet_relations, sheet_only_application
        )
        groups = list(sheet_scope_projection["groups"])
        text_started_at = utc_now()
        text_started_perf = time.perf_counter()

    # Shadow lineage observes the completed candidate layer but cannot replace
    # ``groups`` or ``sheet_relations``.  PAGE keeps its existing advisory
    # matcher position; only the enabled STANDARD shadow builds an additional
    # private candidate view before content comparison.  FAST, DEEP, flag-OFF,
    # and non-allowlisted runs do not execute this block and therefore make
    # zero new model calls.
    if function_lineage_shadow_diagnostic["allowed"]:
        shadow_sheet_relations = sheet_relations
        if request["input_mode"] == "PAGE":
            try:
                shadow_sheet_relations = match_sheets(
                    indexes.get("left") or [], indexes.get("right") or []
                )
            except Exception:  # noqa: BLE001 - shadow input is non-critical
                shadow_sheet_relations = match_sheets([], [])
        function_lineage_result = _maybe_run_function_lineage_shadow(
            session_id,
            pair_id,
            run_id=run_id,
            ai_mode=function_lineage_run_mode,
            indexes=indexes,
            sheet_relations=shadow_sheet_relations,
            answers=answers,
            cancel=control.cancel_token,
            gate=function_lineage_shadow_diagnostic,
        )
        function_lineage_shadow_diagnostic = {
            **function_lineage_shadow_diagnostic,
            "diagnostic_reason": (
                (function_lineage_result or {}).get("diagnostic_reason")
                or FUNCTION_LINEAGE_SHADOW_FAILED
            ),
            "executed": function_lineage_result is not None,
        }
        base_state["function_lineage_shadow"] = copy.deepcopy(
            function_lineage_shadow_diagnostic
        )
        _record_function_lineage_shadow_diagnostic(
            session_id,
            pair_id,
            run_id,
            function_lineage_shadow_diagnostic,
        )
        _raise_if_cancelled(control)

    text_atoms: list[dict[str, Any]] = []
    # Ветка текста может отказать целиком; ИИ-слою тогда не с чем работать, и
    # он обязан это увидеть, а не упасть на несуществующем имени.
    preparation: dict[str, Any] | None = None
    text_stage: dict[str, Any]
    atom_artifact = _empty_text_atoms(
        run_id=run_id,
        generation_input_signature=signature,
        source_state="CHECK_BLOCKED",
    )
    existing_semantic = production_store.load_artifact(
        session_id, pair_id, "text_semantic_validation"
    )
    if text_started_at is None or text_started_perf is None:
        raise RuntimeError("TEXT progress invariant was not initialized")

    def report_text_boundary(*, substage: str, message: str) -> None:
        try:
            publish_progress(
                current_stage="content_analysis",
                current_substage=substage,
                message=message,
                duration_ms=max(
                    0, int((time.perf_counter() - text_started_perf) * 1000)
                ),
                stage_key="text",
                stage_status="RUNNING",
                stage_started_at=text_started_at,
            )
        except Exception as exc:
            raise ProductionProgressPublicationError(
                "TEXT progress publication failed"
            ) from exc

    _raise_if_cancelled(control)
    try:
        document_cache_dir = (
            production_store.artifact_path(
                session_id, pair_id, "text_preparation"
            ).parent
            / "text_fragment_cache"
            if request["input_mode"] == "DOCUMENT"
            else None
        )
        text_progress_token = _TEXT_PROGRESS_CALLBACK.set(
            report_text_boundary
        )
        try:
            (
                preparation,
                differences,
                fact_production,
                semantic,
                atom_artifact,
            ) = _run_text_branch(
                pair,
                pair_id,
                groups,
                indexes,
                existing_semantic,
                document_cache_dir=document_cache_dir,
            )
        finally:
            _TEXT_PROGRESS_CALLBACK.reset(text_progress_token)
        production_store.save_artifact(
            session_id, pair_id, "text_preparation", preparation
        )
        production_store.save_artifact(
            session_id, pair_id, "text_differences", differences
        )
        production_store.save_artifact(
            session_id, pair_id, "text_fact_production", fact_production
        )
        production_store.save_artifact(
            session_id, pair_id, "text_semantic_validation", semantic
        )
        production_store.save_artifact(
            session_id, pair_id, "text_atoms", atom_artifact
        )
        text_atoms = list(atom_artifact.get("atoms") or [])
        text_stage = _text_stage_summary(
            preparation,
            differences,
            fact_production,
            semantic,
            atom_artifact,
        )
    except (FileNotFoundError, OSError, UnicodeDecodeError, ValueError, RuntimeError) as exc:
        text_atoms = []
        atom_artifact = _empty_text_atoms(
            run_id=run_id,
            generation_input_signature=signature,
            source_state="CHECK_BLOCKED",
        )
        production_store.save_artifact(
            session_id, pair_id, "text_atoms", atom_artifact
        )
        text_stage = {
            "status": "CHECK_BLOCKED",
            "source_state": "CHECK_BLOCKED",
            "atoms": 0,
            "deltas": 0,
            "automatic_atoms": 0,
            "review_required": 0,
            "review_required_atoms": 0,
            "not_applicable": 0,
            "unresolved": 0,
            "reason_code": _text_error_reason(exc),
            "error_type": type(exc).__name__,
        }

    text_completed_at = utc_now()
    text_duration_ms = max(
        0, int((time.perf_counter() - text_started_perf) * 1000)
    )
    text_terminal_count = (
        int(text_stage.get("deltas") or 0)
        if text_stage.get("status") == "COMPLETED"
        else None
    )
    text_progress_state = publish_progress(
        current_stage="content_analysis",
        current_substage="text_change_formation",
        message=(
            "Текстовый анализ завершён."
            if text_stage.get("status") == "COMPLETED"
            else "Текстовый анализ завершён с ограничениями."
        ),
        processed=text_terminal_count,
        total=text_terminal_count,
        unit="differences" if text_terminal_count is not None else None,
        duration_ms=text_duration_ms,
        stage_key="text",
        stage_status=str(text_stage.get("status") or "CHECK_BLOCKED"),
        stage_started_at=text_started_at,
        stage_completed_at=text_completed_at,
        stage_update=text_stage,
    )
    persisted_text_stage = (
        (text_progress_state.get("stages") or {}).get("text") or {}
    )
    if isinstance(persisted_text_stage.get("progress"), Mapping):
        text_stage["progress"] = copy.deepcopy(
            dict(persisted_text_stage["progress"])
        )

    graphic_ledger = None
    graphic_started_at = utc_now()
    graphic_started_perf = time.perf_counter()
    graphic_group_count = len(groups)
    graphic_determinate = graphic_group_count > 1
    publish_progress(
        current_stage="content_analysis",
        current_substage="graphic_method_selection",
        message="Выбор метода графического сравнения…",
        processed=0 if graphic_determinate else None,
        total=graphic_group_count if graphic_determinate else None,
        unit="groups" if graphic_determinate else None,
        stage_key="graphic",
        stage_status="RUNNING",
        stage_started_at=graphic_started_at,
    )

    def report_graphic_progress(
        *,
        processed: int | None,
        total: int | None,
        unit: str | None,
        current_item: Mapping[str, Any] | None,
        recent_unit_durations_ms: Iterable[int],
        message: str,
    ) -> None:
        try:
            publish_progress(
                current_stage="content_analysis",
                current_substage=(
                    "graphic_structural_comparison"
                    if request["input_mode"] == "PAGE"
                    else "graphic_group_comparison"
                ),
                message=message,
                processed=processed,
                total=total,
                unit=unit,
                duration_ms=max(
                    0, int((time.perf_counter() - graphic_started_perf) * 1000)
                ),
                current_item=current_item,
                recent_unit_durations_ms=recent_unit_durations_ms,
                stage_key="graphic",
                stage_status="RUNNING",
                stage_started_at=graphic_started_at,
            )
        except Exception as exc:
            raise ProductionProgressPublicationError(
                "GRAPHIC progress publication failed"
            ) from exc

    _raise_if_cancelled(control)
    try:
        graphic_progress_token = _GRAPHIC_PROGRESS_CALLBACK.set(
            report_graphic_progress
        )
        try:
            graphic_ledger, graphic_stage = _run_graphic_branch(
                session_id, pair_id, pair, request, groups
            )
        finally:
            _GRAPHIC_PROGRESS_CALLBACK.reset(graphic_progress_token)
    except (FileNotFoundError, OSError, UnicodeDecodeError, ValueError, RuntimeError) as exc:
        graphic_stage = {
            "status": "CHECK_BLOCKED",
            "source_state": "CHECK_BLOCKED",
            "changes": 0,
            "reason_code": type(exc).__name__,
        }

    graphic_completed_at = utc_now()
    graphic_duration_ms = max(
        0, int((time.perf_counter() - graphic_started_perf) * 1000)
    )
    latest_graphic_progress = progress_snapshots.get("graphic") or {}
    graphic_progress_state = publish_progress(
        current_stage="content_analysis",
        current_substage=(
            "graphic_structural_comparison"
            if request["input_mode"] == "PAGE"
            else "graphic_group_comparison"
        ),
        message=(
            "Графический анализ завершён."
            if graphic_stage.get("status") == "COMPLETED"
            else "Графический анализ завершён с ограничениями."
        ),
        processed=graphic_group_count if graphic_determinate else None,
        total=graphic_group_count if graphic_determinate else None,
        unit="groups" if graphic_determinate else None,
        duration_ms=graphic_duration_ms,
        recent_unit_durations_ms=(
            latest_graphic_progress.get("recent_unit_durations_ms") or []
        ),
        stage_key="graphic",
        stage_status=str(graphic_stage.get("status") or "CHECK_BLOCKED"),
        stage_started_at=graphic_started_at,
        stage_completed_at=graphic_completed_at,
        stage_update=graphic_stage,
    )
    persisted_graphic_stage = (
        (graphic_progress_state.get("stages") or {}).get("graphic") or {}
    )
    if isinstance(persisted_graphic_stage.get("progress"), Mapping):
        graphic_stage["progress"] = copy.deepcopy(
            dict(persisted_graphic_stage["progress"])
        )

    if request["input_mode"] == "PAGE":
        sheet_started_at = utc_now()
        sheet_started_perf = time.perf_counter()
        publish_progress(
            current_stage="sheet_matching",
            current_substage="page_sheet_advisory",
            message="Проверка рекомендаций сопоставления листов…",
            stage_key="sheet_matching",
            stage_status="RUNNING",
            stage_started_at=sheet_started_at,
        )
        try:
            if indexes.get("left") or indexes.get("right"):
                sheet_relations = match_sheets(
                    indexes.get("left") or [], indexes.get("right") or []
                )
            else:
                # Also keeps injectable/test index providers possible without
                # making their result a prerequisite for either main branch.
                sheet_relations, _unused_indexes = _run_sheet_matcher(pair)
            sheet_status = "COMPLETED"
        except (FileNotFoundError, OSError, ValueError, RuntimeError) as exc:
            sheet_relations = match_sheets([], [])
            sheet_status = "NOT_APPLICABLE"
            reason = page_index_error or exc
            sheet_relations.setdefault("diagnostics", {})["reason_code"] = (
                type(reason).__name__
            )
        sheet_completed_at = utc_now()
        sheet_units = len(indexes.get("left") or [])
        production_store.save_artifact(
            session_id, pair_id, "sheet_relations", sheet_relations
        )
        publish_progress(
            current_stage="sheet_matching",
            current_substage="page_sheet_advisory",
            message=(
                "Рекомендации сопоставления листов готовы."
                if sheet_status == "COMPLETED"
                else "Рекомендации сопоставления листов недоступны."
            ),
            processed=sheet_units if sheet_status == "COMPLETED" else None,
            total=sheet_units if sheet_status == "COMPLETED" else None,
            unit="left_sheets" if sheet_status == "COMPLETED" else None,
            duration_ms=max(
                0, int((time.perf_counter() - sheet_started_perf) * 1000)
            ),
            stage_key="sheet_matching",
            stage_status=sheet_status,
            stage_started_at=sheet_started_at,
            stage_completed_at=sheet_completed_at,
            stage_update={
                "relations": len(sheet_relations.get("relations") or []),
                "relation_counts": _sheet_relation_counts(sheet_relations),
            },
        )
        sheet_suggestions = _filter_page_suggestions(
            page_selection_suggestions(
                request["left_pages"], request["right_pages"], sheet_relations
            )
        )
        page_projection = _page_action_projection(
            request, sheet_suggestions, answers
        )
        actual_scope_signature = _sheet_scope_signature(groups)
        if actual_scope_signature != page_projection["effective_signature"]:
            if page_groups_override is not None:
                raise ProductionStateConflictError(
                    "PAGE action scope does not match its review decision"
                )
            # A manual full rerun can encounter a still-current saved action
            # only after the advisory matcher has rebuilt its questions.  The
            # first pass is never published; restart the generation under the
            # exact materialized scope and bind that scope into its signature.
            return _run_production_comparison_impl(
                session_id,
                pair_id,
                **request,
                review_answers_override=answers,
                page_groups_override=page_projection["groups"],
                page_scope_rerun=True,
                interrupted_run=interrupted_run,
            )
        sheet_scope_projection = page_projection

    graphic_atoms: list[dict[str, Any]] = []
    if graphic_ledger is not None:
        graphic_atoms = _graphic_atoms_from_source(graphic_ledger)
        graphic_atoms.extend(
            _load_table_atoms(session_id, pair_id, graphic_atoms)
        )
    source_snapshot = _build_source_snapshot(
        run_id=run_id,
        generation_input_signature=signature,
        text_artifact=atom_artifact,
        text_source_state=str(text_stage["source_state"]),
        graphic_ledger=graphic_ledger,
        graphic_source_state=str(graphic_stage["source_state"]),
    )
    production_store.save_artifact(
        session_id, pair_id, "source_snapshot", source_snapshot
    )

    entity_started_at = utc_now()
    entity_started_perf = time.perf_counter()
    publish_progress(
        current_stage="entity_matching",
        current_substage="entity_matching",
        message="Сопоставление объектов…",
        stage_key="entity_matching",
        stage_status="RUNNING",
        stage_started_at=entity_started_at,
    )
    _raise_if_cancelled(control)
    entity_relations = _run_entity_matcher(text_atoms, graphic_atoms)
    production_store.save_artifact(
        session_id, pair_id, "entity_relations", entity_relations
    )
    bound_atoms = _bind_synthesis_atoms(
        text_atoms, graphic_atoms, entity_relations
    )
    production_store.save_artifact(
        session_id, pair_id, "bound_atoms", bound_atoms
    )
    entity_completed_at = utc_now()
    entity_relation_count = len(entity_relations.get("relations") or [])
    publish_progress(
        current_stage="entity_matching",
        current_substage="entity_binding",
        message="Сопоставление объектов завершено.",
        processed=entity_relation_count,
        total=entity_relation_count,
        unit="relations",
        duration_ms=max(
            0, int((time.perf_counter() - entity_started_perf) * 1000)
        ),
        stage_key="entity_matching",
        stage_status="COMPLETED",
        stage_started_at=entity_started_at,
        stage_completed_at=entity_completed_at,
        stage_update={
            "relations": entity_relation_count,
            **_artifact_state(entity_relations),
        },
    )
    synthesis_text_atoms = list(bound_atoms.get("text_atoms") or [])
    synthesis_graphic_atoms = list(bound_atoms.get("graphic_atoms") or [])
    descriptors = pair_documents_from_pair_artifact(dict(pair))
    binding_proven = all(
        document_identity_is_complete(descriptors[side])
        for side in ("LEFT", "RIGHT")
    )
    semantic_mode2_checked = (
        graphic_stage.get("status") == "COMPLETED"
        and graphic_stage.get("mode") == "MODE_2"
    )
    synthesis_started_at = utc_now()
    synthesis_started_perf = time.perf_counter()
    publish_progress(
        current_stage="unified_synthesis",
        current_substage="automatic_synthesis",
        message="Синтез автоматических изменений…",
        stage_key="unified_synthesis",
        stage_status="RUNNING",
        stage_started_at=synthesis_started_at,
    )
    candidates = _build_synthesis_candidates(
        synthesis_text_atoms,
        synthesis_graphic_atoms,
        entity_relations,
        source_valid=semantic_mode2_checked,
        coverage_by_side=(
            {"LEFT": "CHECKED", "RIGHT": "CHECKED"}
            if semantic_mode2_checked
            else {"LEFT": "NOT_CHECKED", "RIGHT": "NOT_CHECKED"}
        ),
        document_binding_state=(
            "DOCUMENT_BINDING_PROVEN"
            if semantic_mode2_checked and binding_proven
            else "DOCUMENT_BINDING_UNPROVEN"
        ),
    )
    source_states = {
        "TEXT": "VALID" if synthesis_text_atoms else text_stage["source_state"],
        "GRAPHIC": (
            "VALID" if synthesis_graphic_atoms else graphic_stage["source_state"]
        ),
    }
    _raise_if_cancelled(control)
    automatic_synthesis = synthesize_unified_changes(
        text_atoms=synthesis_text_atoms,
        graphic_atoms=synthesis_graphic_atoms,
        candidates=candidates,
        source_states=source_states,
    )
    automatic_synthesis = validate_synthesis(automatic_synthesis)
    production_store.save_artifact(
        session_id,
        pair_id,
        "automatic_unified_synthesis",
        automatic_synthesis,
    )
    publish_progress(
        current_stage="unified_synthesis",
        current_substage="review_application",
        message="Применение актуальных ответов перед синтезом…",
        duration_ms=max(
            0, int((time.perf_counter() - synthesis_started_perf) * 1000)
        ),
        stage_key="unified_synthesis",
        stage_status="RUNNING",
        stage_started_at=synthesis_started_at,
    )
    _raise_if_cancelled(control)
    requested_ai_mode = ai_settings.normalize_mode(request.get("ai_mode")) if (
        request.get("ai_mode")
    ) else ai_settings.mode()
    use_ai_v2_candidate = _ai_v2_candidate_requested(requested_ai_mode)
    routing_inventory = _build_routing_inventory(
        session_id,
        pair_id,
        synthesis=automatic_synthesis,
        preparation=preparation,
        mode=requested_ai_mode,
    )
    # Public launch modes have a deliberately narrow contract.  STANDARD is
    # FAST + HRO + Question Closure; the legacy general analyst is not part of
    # that route.  DEEP keeps its existing backend semantics for old callers,
    # but is not offered by the unified launch UI until it has a separate safe
    # implementation.
    general_ai_mode = (
        requested_ai_mode
        if requested_ai_mode == ai_settings.MODE_DEEP
        else ai_settings.MODE_FAST
    )
    ai_resolutions = _run_ai_resolution(
        session_id,
        pair_id,
        synthesis=automatic_synthesis,
        preparation=preparation,
        sheet_relations=sheet_relations,
        comparison_groups=groups,
        publish_progress=publish_progress,
        pair=pair,
        graphic_route=(
            "VISION_REQUIRED"
            if "VISION_REQUIRED" in (graphic_stage.get("routes") or [])
            else str(graphic_stage.get("route") or "") or None
        ),
        ai_mode=(ai_settings.MODE_FAST if use_ai_v2_candidate else general_ai_mode),
        routing_inventory=routing_inventory,
    )
    _raise_if_cancelled(control)
    _run_table_identity(
        session_id,
        pair_id,
        inventory=routing_inventory,
        mode=(
            ai_settings.MODE_OFF
            if use_ai_v2_candidate
            else ai_settings.normalize_mode(general_ai_mode)
        ),
        publish_progress=publish_progress,
    )
    base_questions = _build_review_questions(
        sheet_relations=sheet_relations,
        sheet_suggestions=sheet_suggestions,
        entity_relations=entity_relations,
        synthesis=automatic_synthesis,
        answers=None,
        ai_resolutions=ai_resolutions,
        input_mode=request["input_mode"],
    )
    application = review_module.apply_human_decisions(
        base_questions,
        answers or {"decisions": [], "input_signature": None},
        sheet_relations=sheet_relations,
        entity_relations=entity_relations,
        synthesis=automatic_synthesis,
        ai_resolutions=ai_resolutions,
    )
    if request["input_mode"] == "DOCUMENT":
        sheet_scope_projection = _materialized_sheet_scope(
            sheet_relations, application
        )
        application = _apply_sheet_scope_diagnostics(
            application,
            sheet_scope_projection,
            run_id=run_id,
            pipeline_rerun=(
                review_answers_override is not None
                or bool(sheet_scope_projection["scope_changed"])
            ),
        )
    else:
        assert sheet_scope_projection is not None
        application = _apply_sheet_scope_diagnostics(
            application,
            sheet_scope_projection,
            run_id=run_id,
            pipeline_rerun=page_scope_rerun,
            this_update_reran=page_scope_rerun,
            rerun_question_ids=page_rerun_question_ids,
        )
    publish_progress(
        current_stage="unified_synthesis",
        current_substage="effective_synthesis",
        message="Синтез изменений с учётом актуальных ответов…",
        duration_ms=max(
            0, int((time.perf_counter() - synthesis_started_perf) * 1000)
        ),
        stage_key="unified_synthesis",
        stage_status="RUNNING",
        stage_started_at=synthesis_started_at,
    )
    projection_state = {
        **base_state,
        "stages": {
            "text": text_stage,
            "graphic": graphic_stage,
            "source_snapshot": {
                "status": "COMPLETED",
                **_artifact_state(source_snapshot),
            },
        },
    }
    synthesis, effective_bound_atoms = _rebuild_dependent_synthesis(
        session_id,
        pair_id,
        pair,
        projection_state,
        automatic_synthesis,
        application,
        entity_relations,
        source_snapshot=source_snapshot,
    )
    production_store.save_artifact(
        session_id, pair_id, "effective_bound_atoms", effective_bound_atoms
    )
    production_store.save_artifact(
        session_id, pair_id, "review_application", application
    )
    synthesis = production_store.save_unified_synthesis(
        session_id, pair_id, synthesis
    )
    synthesis_completed_at = utc_now()
    synthesis_target_count = (
        len(synthesis.get("changes") or [])
        + len(synthesis.get("review_items") or [])
    )
    publish_progress(
        current_stage="unified_synthesis",
        current_substage="effective_synthesis",
        message="Синтез изменений завершён.",
        processed=synthesis_target_count,
        total=synthesis_target_count,
        unit="atomic_targets",
        duration_ms=max(
            0, int((time.perf_counter() - synthesis_started_perf) * 1000)
        ),
        stage_key="unified_synthesis",
        stage_status="COMPLETED",
        stage_started_at=synthesis_started_at,
        stage_completed_at=synthesis_completed_at,
        stage_update={
            "changes": len(synthesis.get("changes") or []),
            "review_items": len(synthesis.get("review_items") or []),
            "input_signature": canonical_synthesis_digest(synthesis),
            "present": True,
        },
    )
    decisions = _refresh_decisions(session_id, pair_id, synthesis)
    preliminary_report: dict[str, Any] | None = None
    human_review_plan = _persist_deterministic_human_review(
        session_id,
        pair_id,
        synthesis=synthesis,
        decisions=decisions,
        text_preparation=preparation,
        run_id=run_id,
        generation_input_signature=signature,
    )
    ai_v2_stage: dict[str, Any] | None = None
    if use_ai_v2_candidate:
        # Publish a complete FAST read model before the first model call.  It
        # is both useful progress and the exact fallback if v2 cannot finish.
        fast_preliminary = _persist_preliminary_report(
            session_id,
            pair_id,
            synthesis,
            source_snapshot,
            human_review_plan=human_review_plan,
        )
        candidate = _run_ai_v2_candidate(
            session_id,
            pair_id,
            synthesis=synthesis,
            decisions=decisions,
            source_snapshot=source_snapshot,
            fast_preliminary_report=fast_preliminary,
            publish_progress=publish_progress,
        )
        ai_v2_stage = dict(candidate["stage"])
        if candidate.get("succeeded"):
            materialization = candidate["materialization"]
            synthesis = production_store.save_unified_synthesis(
                session_id, pair_id, materialization["unified_synthesis"]
            )
            decisions = _refresh_decisions(session_id, pair_id, synthesis)
            human_review_plan = dict(materialization["human_review_plan"])
            preliminary_report = dict(materialization["preliminary_report"])
            production_store.save_artifact(
                session_id, pair_id, "preliminary_report", preliminary_report
            )
        else:
            preliminary_report = dict(fast_preliminary)
    question_closure_stage: dict[str, Any] | None = None
    if (
        requested_ai_mode == ai_settings.MODE_STANDARD
        and ai_question_closure_settings.enabled()
    ):
        # Preliminary Report is the immutable FAST hand-off.  The closure
        # layer may replace only the HRO projection that follows it.
        if preliminary_report is None:
            preliminary_report = _persist_preliminary_report(
                session_id,
                pair_id,
                synthesis,
                source_snapshot,
                human_review_plan=human_review_plan,
            )
        closure = _run_ai_question_closure_candidate(
            session_id,
            pair_id,
            human_review_plan=human_review_plan,
            engineer_decisions=decisions,
            preliminary_report=preliminary_report,
            run_id=run_id,
            generation_input_signature=signature,
            publish_progress=publish_progress,
        )
        question_closure_stage = dict(closure["stage"])
        human_review_plan = dict(closure["plan"])
    questions_started_at = utc_now()
    questions_started_perf = time.perf_counter()
    publish_progress(
        current_stage="review_questions",
        current_substage="question_generation",
        message="Публикация вопросов инженеру…",
        stage_key="review_questions",
        stage_status="RUNNING",
        stage_started_at=questions_started_at,
    )
    _raise_if_cancelled(control)
    questions = _build_review_questions(
        sheet_relations=sheet_relations,
        sheet_suggestions=sheet_suggestions,
        entity_relations=entity_relations,
        synthesis=automatic_synthesis,
        answers=answers,
        ai_resolutions=ai_resolutions,
        input_mode=request["input_mode"],
    )
    if sheet_suggestions:
        question_by_suggestion = {
            str((question.get("context") or {}).get("suggestion_id") or ""): question.get("question_id")
            for question in _sheet_suggestion_questions(sheet_suggestions)
        }
        for suggestion in sheet_suggestions.get("suggestions") or []:
            if isinstance(suggestion, dict):
                suggestion["question_id"] = question_by_suggestion.get(
                    str(suggestion.get("suggestion_id") or "")
                )
    production_store.save_artifact(
        session_id, pair_id, "review_questions", questions
    )
    questions_completed_at = utc_now()
    question_stage = _review_question_stage(questions)
    question_progress_state = publish_progress(
        current_stage="review_questions",
        current_substage="question_generation",
        message=(
            "Вопросы инженеру сформированы."
            if question_stage["pending"]
            else "Вопросов, требующих ответа, нет."
        ),
        processed=question_stage["answered"],
        total=question_stage["total"],
        unit="questions",
        duration_ms=max(
            0, int((time.perf_counter() - questions_started_perf) * 1000)
        ),
        stage_key="review_questions",
        stage_status=question_stage["status"],
        stage_started_at=questions_started_at,
        stage_completed_at=questions_completed_at,
        stage_update=question_stage,
    )
    persisted_question_stage = (
        (question_progress_state.get("stages") or {}).get("review_questions")
        or {}
    )
    if isinstance(persisted_question_stage.get("progress"), Mapping):
        question_stage["progress"] = copy.deepcopy(
            dict(persisted_question_stage["progress"])
        )
    preliminary_started_at = utc_now()
    preliminary_started_perf = time.perf_counter()
    publish_progress(
        current_stage="preliminary_report",
        current_substage="preliminary_projection",
        message="Сборка предварительного отчёта…",
        stage_key="preliminary_report",
        stage_status="RUNNING",
        stage_started_at=preliminary_started_at,
    )
    if preliminary_report is None:
        preliminary_report = _persist_preliminary_report(
            session_id,
            pair_id,
            synthesis,
            source_snapshot,
            human_review_plan=human_review_plan,
        )
    preliminary_counts = dict((preliminary_report.get("summary") or {}).get("counts") or {})
    preliminary_completed_at = utc_now()
    preliminary_progress_state = publish_progress(
        current_stage="preliminary_report",
        current_substage="preliminary_projection",
        message="Предварительный отчёт готов.",
        processed=int(preliminary_counts.get("automatic") or 0),
        total=int(preliminary_counts.get("changes") or 0),
        unit="report_items",
        duration_ms=max(
            0, int((time.perf_counter() - preliminary_started_perf) * 1000)
        ),
        stage_key="preliminary_report",
        stage_status="COMPLETED",
        stage_started_at=preliminary_started_at,
        stage_completed_at=preliminary_completed_at,
        stage_update={
            "counts": preliminary_counts,
            "content_digest": content_signature(preliminary_report),
            **_artifact_state(preliminary_report),
        },
    )
    preliminary_stage = (
        (preliminary_progress_state.get("stages") or {}).get("preliminary_report") or {}
    )
    final_report_started_at = utc_now()
    final_report_started_perf = time.perf_counter()
    publish_progress(
        current_stage="final_report",
        current_substage="approved_report_projection",
        message="Обновление итогового отчёта…",
        stage_key="final_report",
        stage_status="RUNNING",
        stage_started_at=final_report_started_at,
    )
    final_report = _persist_latest_final_report(
        session_id, pair_id, synthesis, decisions
    )
    final_report_completed_at = utc_now()
    approved_count = len(final_report.get("approved_atomic_changes") or [])
    final_report_progress_state = publish_progress(
        current_stage="final_report",
        current_substage="approved_report_projection",
        message="Итоговый отчёт обновлён.",
        processed=approved_count,
        total=approved_count,
        unit="approved_changes",
        duration_ms=max(
            0, int((time.perf_counter() - final_report_started_perf) * 1000)
        ),
        stage_key="final_report",
        stage_status="COMPLETED",
        stage_started_at=final_report_started_at,
        stage_completed_at=final_report_completed_at,
        stage_update={
            "approved": approved_count,
            "content_digest": content_signature(final_report),
            **_artifact_state(final_report),
        },
    )
    persisted_final_report_stage = (
        (final_report_progress_state.get("stages") or {}).get("final_report")
        or {}
    )
    final_report_progress = (
        copy.deepcopy(dict(persisted_final_report_stage["progress"]))
        if isinstance(persisted_final_report_stage.get("progress"), Mapping)
        else None
    )

    # Верхний статус прогона обязан наследовать честный исход этапов. Раньше он
    # смотрел только на текст и графику, поэтому упавший ИИ-слой давал
    # «Готово» на всём прогоне рядом с «ИИ-анализ не выполнен» на карточке.
    # «Неприменимо» у ИИ-слоя (режим «Быстро») ничего не опускает: этот режим
    # ничего и не обещал.
    ai_resolution_stage = ai_v2_stage or _ai_resolution_stage(ai_resolutions)
    partial = any(
        stage.get("status") in {"CHECK_BLOCKED", "NOT_APPLICABLE", "NOT_CHECKED"}
        for stage in (text_stage, graphic_stage)
    ) or ai_resolution_stage["status"] in {"PARTIAL", "FAILED", "CANCELLED"}
    completed_at = utc_now()
    run_duration_ms = max(
        0, int((time.perf_counter() - run_started_perf) * 1000)
    )
    final_state = {
        **base_state,
        "status": "PARTIAL" if partial else "COMPLETED",
        "progress": 100,
        "completed_at": completed_at,
        "last_activity_at": completed_at,
        "current_stage": None,
        "current_substage": None,
        "message": None,
        "processed": None,
        "total": None,
        "unit": None,
        "current_item": None,
        "recent_unit_durations_ms": [],
        "duration_ms": run_duration_ms,
        "sheet_suggestions": sheet_suggestions,
        "stages": {
            "sheet_matching": {
                "status": sheet_status,
                "relations": len(sheet_relations.get("relations") or []),
                "relation_counts": _sheet_relation_counts(sheet_relations),
                **_artifact_state(sheet_relations),
                **(
                    {"progress": progress_snapshots["sheet_matching"]}
                    if "sheet_matching" in progress_snapshots
                    else {}
                ),
            },
            "sheet_scope": {
                "status": "COMPLETED",
                "groups": len((sheet_scope_projection or {}).get("groups") or []),
                "pending_confirmation": len(
                    (sheet_scope_projection or {}).get("pending_confirmation") or []
                ),
                "pending_confirmation_groups": copy.deepcopy(
                    (sheet_scope_projection or {}).get("pending_confirmation") or []
                ),
                "input_signature": (sheet_scope_projection or {}).get(
                    "effective_signature"
                ),
                "automatic_input_signature": (
                    sheet_scope_projection or {}
                ).get("automatic_signature"),
                "scope_applied": bool(
                    (sheet_scope_projection or {}).get("scope_applied")
                ),
                "pipeline_rerun": bool(
                    (application.get("diagnostics") or {}).get(
                        "pipeline_rerun"
                    )
                ),
                "generation_was_materialized": bool(
                    (application.get("diagnostics") or {}).get(
                        "generation_was_materialized"
                    )
                ),
                "this_update_reran": bool(
                    (application.get("diagnostics") or {}).get(
                        "this_update_reran"
                    )
                ),
                "effective_page_groups": copy.deepcopy(
                    (sheet_scope_projection or {}).get("groups") or []
                ),
                "page_action_state": str(
                    (sheet_scope_projection or {}).get("action_state") or "NONE"
                ),
                "page_action_outcomes": copy.deepcopy(
                    (application.get("diagnostics") or {}).get(
                        "page_action_outcomes"
                    ) or []
                ),
            },
            "text": text_stage,
            "graphic": graphic_stage,
            "source_snapshot": {
                "status": "COMPLETED",
                **_artifact_state(source_snapshot),
            },
            "entity_matching": {
                "status": "COMPLETED",
                "relations": len(entity_relations.get("relations") or []),
                **_artifact_state(entity_relations),
                **(
                    {"progress": progress_snapshots["entity_matching"]}
                    if "entity_matching" in progress_snapshots
                    else {}
                ),
            },
            "entity_binding": {
                "status": "COMPLETED",
                "bound_atoms": len(
                    (bound_atoms.get("diagnostics") or {}).get("bound_atom_ids") or []
                ),
                **_artifact_state(bound_atoms),
            },
            "effective_entity_binding": {
                "status": "COMPLETED",
                "bound_atoms": len(
                    (effective_bound_atoms.get("diagnostics") or {}).get(
                        "bound_atom_ids"
                    )
                    or []
                ),
                **_artifact_state(effective_bound_atoms),
            },
            "ai_resolution": {
                **ai_resolution_stage,
                **(
                    {"progress": progress_snapshots["ai_resolution"]}
                    if "ai_resolution" in progress_snapshots
                    else {}
                ),
            },
            "question_closure": (
                question_closure_stage
                if question_closure_stage is not None
                else {
                    "status": "DISABLED",
                    "feature_flag": ai_question_closure_settings.FEATURE_FLAG,
                    "hro_before": int(
                        ((human_review_plan or {}).get("summary") or {}).get(
                            "mandatory_human_interactions"
                        ) or 0
                    ),
                    "hro_after": int(
                        ((human_review_plan or {}).get("summary") or {}).get(
                            "mandatory_human_interactions"
                        ) or 0
                    ),
                    "closed": 0,
                    "model_calls": 0,
                    "fallback_used": False,
                    "fast_preserved": True,
                    "engineer_approvals_untouched": True,
                }
            ),
            "human_review": {
                "status": (
                    "NEEDS_REVIEW"
                    if human_review_plan
                    and int((human_review_plan.get("summary") or {}).get(
                        "mandatory_human_interactions"
                    ) or 0)
                    else "NOT_APPLICABLE"
                ),
                "total": int(
                    ((human_review_plan or {}).get("summary") or {}).get(
                        "mandatory_human_interactions"
                    ) or 0
                ),
                "pending": int(
                    ((human_review_plan or {}).get("summary") or {}).get(
                        "mandatory_human_interactions"
                    ) or 0
                ),
                "answered": 0,
                "clarification_is_not_final_approval": True,
            },
            "review_questions": question_stage,
            "review_application": {
                "status": "COMPLETED",
                "applied_decisions": len(
                    application.get("applied_decision_ids") or []
                ),
                **_artifact_state(application),
            },
            "automatic_unified_synthesis": {
                "status": "COMPLETED",
                "changes": len(automatic_synthesis.get("changes") or []),
                "review_items": len(
                    automatic_synthesis.get("review_items") or []
                ),
                "input_signature": canonical_synthesis_digest(
                    automatic_synthesis
                ),
                "present": True,
            },
            "unified_synthesis": {
                "status": "COMPLETED",
                "changes": len(synthesis.get("changes") or []),
                "review_items": len(synthesis.get("review_items") or []),
                "input_signature": canonical_synthesis_digest(synthesis),
                "present": True,
                **(
                    {"progress": progress_snapshots["unified_synthesis"]}
                    if "unified_synthesis" in progress_snapshots
                    else {}
                ),
            },
            "engineer_decisions": {
                "status": "READY",
                "counts": decisions.get("counts") or {},
                "revision": int(decisions.get("revision") or 0),
                "content_digest": content_signature(decisions),
                **_artifact_state(decisions),
            },
            "preliminary_report": {
                "status": "READY",
                "counts": preliminary_counts,
                "content_digest": content_signature(preliminary_report),
                **_artifact_state(preliminary_report),
                **(
                    {"progress": copy.deepcopy(dict(preliminary_stage["progress"]))}
                    if isinstance(preliminary_stage.get("progress"), Mapping)
                    else {}
                ),
            },
            "final_report": {
                "status": "READY",
                "approved": len(final_report.get("approved_atomic_changes") or []),
                "content_digest": content_signature(final_report),
                **_artifact_state(final_report),
                **(
                    {"progress": final_report_progress}
                    if final_report_progress is not None
                    else {}
                ),
            },
        },
    }
    latest_pair = store.get_pair_for_production(session_id, pair_id)
    if _input_signature(
        latest_pair, request, page_groups=page_groups
    ) != signature:
        raise ProductionStateConflictError(
            "production sources changed during comparison"
        )
    if isinstance(review_answers_override, Mapping):
        production_store.save_artifact(
            session_id,
            pair_id,
            "review_answers",
            review_answers_override,
        )
    return _write_state(session_id, pair_id, final_state)


def _run_production_comparison_locked(
    session_id: str,
    pair_id: str,
    *,
    input_mode: str,
    left_pages: Iterable[Any] = (),
    right_pages: Iterable[Any] = (),
    left_block_ids: Iterable[Any] = (),
    right_block_ids: Iterable[Any] = (),
    ai_mode: str | None = None,
    review_answers_override: Mapping[str, Any] | None = None,
    page_groups_override: Iterable[Mapping[str, Any]] | None = None,
    page_scope_rerun: bool = False,
    page_rerun_question_ids: Iterable[str] = (),
    interrupted_run: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Run under an already-held pair lock and fail the generation closed."""
    try:
        return _run_production_comparison_impl(
            session_id,
            pair_id,
            input_mode=input_mode,
            left_pages=left_pages,
            right_pages=right_pages,
            left_block_ids=left_block_ids,
            right_block_ids=right_block_ids,
            # Глубина анализа — параметр ЭТОГО прогона. Потерять её здесь
            # значит молча вернуться к переменной окружения: инженер выбрал
            # «глубокую проверку», а установка отработала в своём режиме и
            # ничем об этом не сообщила.
            ai_mode=ai_mode,
            review_answers_override=review_answers_override,
            page_groups_override=page_groups_override,
            page_scope_rerun=page_scope_rerun,
            page_rerun_question_ids=page_rerun_question_ids,
            interrupted_run=interrupted_run,
        )
    except ProductionRunCancelled:
        # Отмена — не отказ. Записать её как FAILED значило бы показать
        # инженеру ошибку там, где он сам нажал «остановить», и заодно
        # потерять причину в журнале.
        control = active_run_control(session_id, pair_id)
        killed = ai_gateway.kill_live_processes(
            control.run_id if control is not None else ""
        )
        current = production_store.load_artifact(session_id, pair_id, "state")
        cancelled_at = utc_now()
        cancelled = {
            **(dict(current) if isinstance(current, Mapping) else {}),
            "status": CANCELLED_STATUS,
            "progress": 100,
            "cancelled_at": cancelled_at,
            "cancelled_by": control.requested_by if control is not None else None,
            "cancelled_stage": (current or {}).get("current_stage"),
            "cancelled_substage": (current or {}).get("current_substage"),
            "killed_model_sessions": killed,
            "last_activity_at": cancelled_at,
            "current_stage": None,
            "current_substage": None,
            "message": "Анализ остановлен по запросу.",
            "processed": None,
            "total": None,
            "unit": None,
            "current_item": None,
            "recent_unit_durations_ms": [],
            "duration_ms": _duration_ms_since((current or {}).get("started_at")),
            "reason_code": "cancelled_by_request",
        }
        _write_state(session_id, pair_id, cancelled)
        return cancelled
    except Exception as exc:
        current = production_store.load_artifact(session_id, pair_id, "state")
        if isinstance(current, Mapping) and current.get("status") in {
            "RUNNING",
            "UPDATING",
        }:
            failed_at = utc_now()
            failed_stage = current.get("current_stage")
            failed_substage = current.get("current_substage")
            stages = copy.deepcopy(current.get("stages") or {})
            for stage in stages.values():
                if not isinstance(stage, dict):
                    continue
                stage_progress = stage.get("progress")
                if not isinstance(stage_progress, Mapping):
                    continue
                if stage_progress.get("status") != "RUNNING":
                    continue
                progress = copy.deepcopy(dict(stage_progress))
                progress.update({
                    "status": "FAILED",
                    "completed_at": failed_at,
                    "last_activity_at": failed_at,
                    "message": "Этап завершён ошибкой.",
                    "duration_ms": _duration_ms_since(
                        progress.get("started_at")
                    ),
                })
                stage["progress"] = progress
                if stage.get("status") == "RUNNING":
                    stage["status"] = "FAILED"
            failed = {
                **current,
                "status": "FAILED",
                "progress": 100,
                "failed_at": failed_at,
                "failed_stage": failed_stage,
                "failed_substage": failed_substage,
                "last_activity_at": failed_at,
                "current_stage": None,
                "current_substage": None,
                "message": None,
                "processed": None,
                "total": None,
                "unit": None,
                "current_item": None,
                "recent_unit_durations_ms": [],
                "duration_ms": _duration_ms_since(current.get("started_at")),
                "stages": stages,
                # Never persist an exception message here: file locators are
                # private and some dependency errors include absolute paths.
                "reason_code": type(exc).__name__,
            }
            _write_state(session_id, pair_id, failed)
        raise
    finally:
        _release_run(active_run_control(session_id, pair_id))
        _RUN_CONTROL.set(None)


class ProductionRunCancelled(RuntimeError):
    """Прогон остановлен снаружи. Это не отказ и не ошибка конвейера."""


@dataclass
class _RunControl:
    """Ручка живого прогона: единственное, за что его можно взять снаружи."""

    session_id: str
    pair_id: str
    run_id: str
    cancel_token: Any
    requested_by: str | None = None
    requested_at: str | None = None

    @property
    def cancelled(self) -> bool:
        return bool(self.cancel_token.cancelled)


_RUN_CONTROLS: dict[tuple[str, str], _RunControl] = {}
_RUN_CONTROLS_LOCK = threading.Lock()


def _register_run(session_id: str, pair_id: str, run_id: str) -> _RunControl:
    control = _RunControl(
        session_id=session_id,
        pair_id=pair_id,
        run_id=run_id,
        cancel_token=ai_gateway.CancelToken(),
    )
    with _RUN_CONTROLS_LOCK:
        _RUN_CONTROLS[(session_id, pair_id)] = control
    return control


def _release_run(control: _RunControl | None) -> None:
    if control is None:
        return
    with _RUN_CONTROLS_LOCK:
        current = _RUN_CONTROLS.get((control.session_id, control.pair_id))
        if current is control:
            _RUN_CONTROLS.pop((control.session_id, control.pair_id), None)


def active_run_control(session_id: str, pair_id: str) -> _RunControl | None:
    with _RUN_CONTROLS_LOCK:
        return _RUN_CONTROLS.get((session_id, pair_id))


def _raise_if_cancelled(control: _RunControl | None) -> None:
    """Проверка на границе этапа. Внутри этапа отмену несёт CancelToken."""
    if control is not None and control.cancelled:
        raise ProductionRunCancelled("stage comparison run cancelled")


def cancel_production_comparison(
    session_id: str,
    pair_id: str,
    *,
    requested_by: str | None = None,
) -> dict[str, Any]:
    """Остановить живой прогон сравнения этой пары.

    Замок пары НЕ берётся намеренно: он неблокирующий и занят самим прогоном,
    поэтому попытка его взять вернула бы 409 вместо отмены. Отмена работает
    через токен и метку процессов, а не через захват ресурса.

    Дочерние CLI-сессии убиваются по метке ИМЕННО ЭТОГО прогона: параллельная
    пара в очереди — обычный режим, и снести её вызовы заодно нельзя.
    """
    control = active_run_control(session_id, pair_id)
    if control is None:
        state = production_store.load_artifact(session_id, pair_id, "state")
        status = str((state or {}).get("status") or "NOT_STARTED")
        return {
            "cancelled": False,
            "reason_code": (
                "run_not_owned_by_this_process"
                if status in ACTIVE_RUN_STATUSES
                else "no_active_run"
            ),
            "status": status,
            "run_id": (state or {}).get("run_id"),
        }
    control.requested_by = requested_by
    control.requested_at = utc_now()
    control.cancel_token.cancel()
    killed = ai_gateway.kill_live_processes(control.run_id)
    return {
        "cancelled": True,
        "reason_code": "cancel_requested",
        "run_id": control.run_id,
        "killed_model_sessions": killed,
        "requested_by": requested_by,
        "requested_at": control.requested_at,
    }


def run_production_comparison(
    session_id: str,
    pair_id: str,
    *,
    input_mode: str,
    left_pages: Iterable[Any] = (),
    right_pages: Iterable[Any] = (),
    left_block_ids: Iterable[Any] = (),
    right_block_ids: Iterable[Any] = (),
    ai_mode: str | None = None,
) -> dict[str, Any]:
    """Run production comparison and never leave a failed run as RUNNING."""
    with production_store.production_pair_lock(session_id, pair_id):
        previous = production_store.load_artifact(
            session_id, pair_id, "state"
        )
        interrupted_run = (
            copy.deepcopy(previous)
            if isinstance(previous, Mapping)
            and str(previous.get("status") or "") in ACTIVE_RUN_STATUSES
            else None
        )
        return _run_production_comparison_locked(
            session_id,
            pair_id,
            input_mode=input_mode,
            left_pages=left_pages,
            right_pages=right_pages,
            left_block_ids=left_block_ids,
            right_block_ids=right_block_ids,
            ai_mode=ai_mode,
            interrupted_run=interrupted_run,
        )


def _empty_state(session_id: str, pair_id: str) -> dict[str, Any]:
    return {
        "kind": STATE_KIND,
        "schema_version": STATE_SCHEMA_VERSION,
        "version": 1,
        "revision": 0,
        "session_id": session_id,
        "pair_id": pair_id,
        "direction": "LEFT_TO_RIGHT",
        "input_mode": None,
        "selection": None,
        "input_signature": None,
        "analysis_config": analysis_config({}),
        "analysis_config_signature": analysis_config_signature({}),
        "status": "NOT_STARTED",
        "progress": 0,
        "stale": False,
        "stale_reason": None,
        "started_at": None,
        "last_activity_at": None,
        "current_stage": None,
        "current_substage": None,
        "message": None,
        "processed": None,
        "total": None,
        "unit": None,
        "current_item": None,
        "recent_unit_durations_ms": [],
        "duration_ms": None,
        "runner_active": False,
        "orphaned_run": False,
        "run_recoverable": False,
        "stages": {},
        "constraints": {
            "new_flow": True,
            "legacy_stage5_used": False,
            "legacy_stage53_used": False,
            "parent_relation_required": False,
            "sheet_matcher_is_page_gate": False,
            "activity_warning_threshold_sec": (
                _progress_activity_warning_sec()
            ),
        },
    }


def get_production_state(session_id: str, pair_id: str) -> dict[str, Any]:
    """Read state and compute source staleness without starting a producer."""
    pair = store.get_pair_for_production(session_id, pair_id)
    state = production_store.load_artifact(session_id, pair_id, "state")
    if not state:
        return _empty_state(session_id, pair_id)
    state_status = str(state.get("status") or "")
    runner_active = False
    if state_status in ACTIVE_RUN_STATUSES:
        runner_active = production_store.production_pair_runner_active(
            session_id, pair_id
        )
        if not runner_active:
            # Close the narrow race where a producer publishes terminal state
            # or acquires the pair lock between our first state read and the
            # non-blocking lock probe.
            latest = production_store.load_artifact(
                session_id, pair_id, "state"
            )
            if isinstance(latest, Mapping) and (
                latest.get("run_id") != state.get("run_id")
                or latest.get("revision") != state.get("revision")
                or latest.get("status") != state.get("status")
            ):
                state = dict(latest)
                state_status = str(state.get("status") or "")
            runner_active = (
                production_store.production_pair_runner_active(
                    session_id, pair_id
                )
                if state_status in ACTIVE_RUN_STATUSES
                else False
            )
    orphaned_run = state_status in ACTIVE_RUN_STATUSES and not runner_active
    public = copy.deepcopy(state)
    public["runner_active"] = runner_active
    public["orphaned_run"] = orphaned_run
    public["run_recoverable"] = orphaned_run
    for key, default in {
        "current_stage": None,
        "current_substage": None,
        "message": None,
        "processed": None,
        "total": None,
        "unit": None,
        "current_item": None,
        "recent_unit_durations_ms": [],
        "last_activity_at": None,
        "duration_ms": None,
    }.items():
        public.setdefault(key, copy.deepcopy(default))
    constraints = dict(public.get("constraints") or {})
    constraints.setdefault(
        "activity_warning_threshold_sec",
        _progress_activity_warning_sec(),
    )
    public["constraints"] = constraints
    request = public.get("selection")
    public.setdefault("analysis_config", analysis_config(
        request if isinstance(request, Mapping) else {}
    ))
    public.setdefault("analysis_config_signature", analysis_config_signature(
        request if isinstance(request, Mapping) else {}
    ))
    stale = True
    if isinstance(request, Mapping):
        try:
            normalized = restore_selection(request)
            generation_scope = public.get("generation_scope")
            page_groups = (
                generation_scope.get("page_groups")
                if isinstance(generation_scope, Mapping)
                and normalized.get("input_mode") == "PAGE"
                else None
            )
            stale = _input_signature(
                pair, normalized, page_groups=page_groups
            ) != public.get("input_signature")
        except (TypeError, ValueError):
            stale = True
    stale_reason = STALE_SOURCES_CHANGED if stale else None
    if not stale:
        # Область сравнения меняется не только правкой PDF: человек мог
        # пересобрать пару страниц руками уже после прогона.
        manual_reason = _manual_pairing_stale_reason(session_id, pair_id, public)
        if manual_reason:
            stale = True
            stale_reason = manual_reason
    public["stale"] = stale
    public["stale_reason"] = stale_reason
    suggestions = (
        public.get("sheet_suggestions")
        if isinstance(public.get("sheet_suggestions"), Mapping)
        else None
    )
    answers = production_store.load_artifact(
        session_id, pair_id, "review_answers"
    )
    public["suggestion_actions"] = _suggestion_actions(suggestions, answers)
    public["suggestion_action_semantics"] = _suggestion_action_semantics(public)
    if stale:
        for stage in (public.get("stages") or {}).values():
            if isinstance(stage, dict):
                stage["stale"] = True
    return public


def _sanitize_stage_export(
    value: Any,
    *,
    path: str,
    omissions: list[dict[str, str]],
) -> Any:
    """Make a persisted diagnostic safe for an engineer's clipboard."""
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for raw_key, item in value.items():
            key = str(raw_key)
            safe_key = key
            if key.startswith("file://") or os.path.isabs(key):
                name = Path(key.removeprefix("file://")).name
                safe_key = (
                    f"[server path omitted]/{name}"
                    if name
                    else "[server path omitted]"
                )
                omissions.append({
                    "json_path": f"{path}.[mapping key]",
                    "reason": "absolute_server_path",
                })
            else:
                safe_key, path_count = _STAGE_EXPORT_SERVER_PATH.subn(
                    "[server path omitted]", key
                )
                if path_count:
                    omissions.append({
                        "json_path": f"{path}.[mapping key]",
                        "reason": "absolute_server_path",
                    })
            if safe_key in result:
                safe_key = f"{safe_key} [{len(result) + 1}]"
            normalized_key = safe_key.strip().lower().replace("-", "_")
            child_path = f"{path}.{safe_key}"
            is_secret = (
                normalized_key in _STAGE_EXPORT_SECRET_KEYS
                or normalized_key.endswith(("_password", "_secret", "_api_key", "_token"))
            )
            is_binary = (
                normalized_key in _STAGE_EXPORT_BINARY_KEYS
                or normalized_key.endswith(("_bytes", "_blob", "_base64"))
            )
            if is_secret or is_binary:
                reason = "secret" if is_secret else "binary_data"
                omissions.append({"json_path": child_path, "reason": reason})
                result[safe_key] = f"[{reason} omitted]"
                continue
            result[safe_key] = _sanitize_stage_export(
                item,
                path=child_path,
                omissions=omissions,
            )
        return result
    if isinstance(value, (list, tuple)):
        return [
            _sanitize_stage_export(
                item,
                path=f"{path}[{index}]",
                omissions=omissions,
            )
            for index, item in enumerate(value)
        ]
    if isinstance(value, (bytes, bytearray, memoryview)):
        omissions.append({"json_path": path, "reason": "binary_data"})
        return "[binary_data omitted]"
    if isinstance(value, str):
        if value.startswith("data:"):
            omissions.append({"json_path": path, "reason": "binary_data"})
            return "[binary_data omitted]"
        if len(value) >= 1024 and _STAGE_EXPORT_BASE64.fullmatch(value):
            omissions.append({"json_path": path, "reason": "binary_data"})
            return "[binary_data omitted]"
        redacted, bearer_count = _STAGE_EXPORT_BEARER.subn(
            "Bearer [secret omitted]", value
        )
        redacted, inline_count = _STAGE_EXPORT_INLINE_SECRET.subn(
            lambda match: f"{match.group(1)}{match.group(2)}[secret omitted]",
            redacted,
        )
        if bearer_count or inline_count:
            omissions.append({"json_path": path, "reason": "secret"})
        if redacted.startswith("file://") or os.path.isabs(redacted):
            name = Path(redacted.removeprefix("file://")).name
            omissions.append({"json_path": path, "reason": "absolute_server_path"})
            return f"[server path omitted]/{name}" if name else "[server path omitted]"
        redacted, count = _STAGE_EXPORT_SERVER_PATH.subn(
            "[server path omitted]", redacted
        )
        if count:
            omissions.append({"json_path": path, "reason": "absolute_server_path"})
        return redacted
    return value


def _stage_export_artifact_identity(artifact: Mapping[str, Any]) -> dict[str, Any]:
    keys = (
        "kind",
        "schema_version",
        "version",
        "input_signature",
        "source_signature",
        "generation_input_signature",
        "content_digest",
    )
    return {key: copy.deepcopy(artifact[key]) for key in keys if key in artifact}


def _stage_export_document_name(document: Any) -> str:
    value = document if isinstance(document, Mapping) else {}
    for key in ("filename", "document_code", "name"):
        name = str(value.get(key) or "").strip()
        if name:
            return name
    return Path(str(value.get("pdf_path") or "")).name


def _stage_export_status(
    stage_id: str,
    stage_state: Mapping[str, Any],
    state: Mapping[str, Any],
) -> str:
    if stage_id == "selection":
        return "COMPLETED" if isinstance(state.get("selection"), Mapping) else "NOT_STARTED"
    if stage_id == "review":
        decisions = stage_state.get("engineer_decisions") or {}
        counts = decisions.get("counts") if isinstance(decisions, Mapping) else {}
        if isinstance(counts, Mapping) and int(counts.get("PENDING_REVIEW") or 0) > 0:
            return "NEEDS_REVIEW"
    aliases = {
        "READY": "COMPLETED",
        "REVIEW_REQUIRED": "NEEDS_REVIEW",
        "CHECK_BLOCKED": "PARTIAL",
        "BLOCKED": "PARTIAL",
        "DISABLED": "NOT_APPLICABLE",
    }
    statuses = [
        aliases.get(str(item.get("status") or "").upper(), str(item.get("status") or "").upper())
        for item in stage_state.values()
        if isinstance(item, Mapping) and item.get("status")
    ]
    for candidate in (
        "FAILED",
        "RUNNING",
        "CANCELLED",
        "PARTIAL",
        "NEEDS_REVIEW",
        "COMPLETED",
        "NOT_APPLICABLE",
    ):
        if candidate in statuses:
            return candidate
    return statuses[0] if statuses else "NOT_STARTED"


def _stage_export_field_paths(value: Any, keys: frozenset[str], path: str = "$.outputs") -> list[str]:
    paths: list[str] = []
    if isinstance(value, Mapping):
        for raw_key, item in value.items():
            key = str(raw_key)
            child_path = f"{path}.{key}"
            normalized = key.strip().lower().replace("-", "_")
            if (
                normalized in keys
                or "provenance" in normalized
                or normalized.startswith("evidence_")
                or normalized.endswith("_evidence")
            ):
                paths.append(child_path)
            paths.extend(_stage_export_field_paths(item, keys, child_path))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            paths.extend(_stage_export_field_paths(item, keys, f"{path}[{index}]"))
    return paths


def _stage_export_reason_values(value: Any, path: str = "$.outputs") -> dict[str, Any]:
    reasons: dict[str, Any] = {}
    if isinstance(value, Mapping):
        for raw_key, item in value.items():
            key = str(raw_key)
            child_path = f"{path}.{key}"
            if key.strip().lower().replace("-", "_") in _STAGE_EXPORT_REASON_KEYS:
                reasons[child_path] = copy.deepcopy(item)
            reasons.update(_stage_export_reason_values(item, child_path))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            reasons.update(_stage_export_reason_values(item, f"{path}[{index}]"))
    return reasons


def get_production_stage_result(
    session_id: str,
    pair_id: str,
    run_id: str,
    stage_id: str,
) -> dict[str, Any]:
    """Return one stage only when the pair still owns the requested run."""
    normalized = str(stage_id or "").strip().lower()
    if normalized not in PRODUCTION_STAGE_RESULT_ARTIFACTS:
        raise ValueError(f"unsupported production stage: {stage_id}")
    requested_run_id = str(run_id or "").strip()
    if not requested_run_id:
        raise ValueError("run_id is required")

    pair = store.get_pair_for_production(session_id, pair_id)
    state = production_store.load_artifact(session_id, pair_id, "state") or {}
    if state.get("run_id") != requested_run_id:
        raise ProductionStateConflictError(
            "requested production run is not current for this pair"
        )
    stages = state.get("stages") if isinstance(state.get("stages"), Mapping) else {}
    artifact_names = PRODUCTION_STAGE_RESULT_ARTIFACTS[normalized]
    raw_artifacts: dict[str, Any] = {}
    missing_artifacts: list[str] = []
    for name in artifact_names:
        artifact = production_store.load_artifact(session_id, pair_id, name)
        if artifact is None:
            missing_artifacts.append(name)
        else:
            raw_artifacts[name] = artifact

    stage_state = {
        key: copy.deepcopy(stages[key])
        for key in PRODUCTION_STAGE_RESULT_STATE_KEYS[normalized]
        if key in stages and isinstance(stages[key], Mapping)
    }
    number, label = PRODUCTION_STAGE_RESULT_LABELS[normalized]
    upstream_artifacts: dict[str, Any] = {}
    for name in PRODUCTION_STAGE_RESULT_INPUT_ARTIFACTS[normalized]:
        artifact = production_store.load_artifact(session_id, pair_id, name)
        if isinstance(artifact, Mapping):
            upstream_artifacts[name] = _stage_export_artifact_identity(artifact)

    inputs: dict[str, Any] = {
        "selection": copy.deepcopy(state.get("selection") or {}),
        "analysis_config": copy.deepcopy(state.get("analysis_config") or {}),
        "upstream_artifacts": upstream_artifacts,
    }
    outputs: dict[str, Any] = {
        "stage_state": stage_state,
        "artifacts": raw_artifacts,
    }
    if normalized == "selection":
        inputs["documents"] = {
            "pair_id": pair.get("id"),
            "left": copy.deepcopy(pair.get("left") or {}),
            "right": copy.deepcopy(pair.get("right") or {}),
        }
        outputs["selection"] = copy.deepcopy(state.get("selection") or {})
    elif normalized == "sheets":
        outputs["sheet_suggestions"] = copy.deepcopy(
            state.get("sheet_suggestions") or {}
        )
        # Shadow artifacts are conditional and run-bound.  Keeping them out of
        # the ordinary stage ownership tuple makes a flag-OFF export identical
        # to the pre-lineage export and prevents stale artifacts from an older
        # run from surfacing in a newer FAST run.
        shadow_artifacts: dict[str, Any] = {}
        for name in (
            "document_link_map", "function_lineage_map", "derived_sheet_map",
        ):
            artifact = production_store.load_artifact(session_id, pair_id, name)
            if isinstance(artifact, Mapping) and artifact.get("run_id") == requested_run_id:
                shadow_artifacts[name] = artifact
        if "function_lineage_map" in shadow_artifacts:
            outputs["function_lineage_shadow"] = shadow_artifacts

    # A new run publishes state before replacing its artifacts.  Rechecking
    # after all reads prevents a request racing that publication from returning
    # a mixture of two generations.
    current_state = production_store.load_artifact(session_id, pair_id, "state") or {}
    if current_state.get("run_id") != requested_run_id:
        raise ProductionStateConflictError(
            "requested production run changed while it was being exported"
        )

    omissions: list[dict[str, str]] = []
    safe_inputs = _sanitize_stage_export(inputs, path="$.inputs", omissions=omissions)
    safe_outputs = _sanitize_stage_export(outputs, path="$.outputs", omissions=omissions)
    result: dict[str, Any] = {
        "schema_version": "stage-comparison-stage-result-export.v1",
        "stage": {"id": normalized, "number": number, "label": label},
        "run_id": requested_run_id,
        "pair_id": str(pair.get("id") or pair_id),
        "documents": {
            "LEFT": _stage_export_document_name(pair.get("left")),
            "RIGHT": _stage_export_document_name(pair.get("right")),
        },
        "status": _stage_export_status(normalized, stage_state, state),
        "inputs": safe_inputs,
        "outputs": safe_outputs,
        "evidence_provenance": {
            "included_in_outputs": True,
            "json_paths": _stage_export_field_paths(
                safe_outputs, _STAGE_EXPORT_EVIDENCE_KEYS
            ),
        },
        "reasons": _stage_export_reason_values(safe_outputs),
        "diagnostics": {
            "run_status": state.get("status", "NOT_STARTED"),
            "input_signature": state.get("input_signature"),
            "missing_artifacts": missing_artifacts,
            "omissions": omissions,
        },
    }
    return result


def _published_synthesis(
    session_id: str,
    pair_id: str,
    state: Mapping[str, Any],
    *,
    for_write: bool = False,
) -> dict[str, Any] | None:
    """Load only the synthesis generation published by a completed state."""
    status = str(state.get("status") or "")
    if status not in PUBLISHED_STATUSES:
        if for_write:
            raise ProductionStateConflictError(
                f"production run is not published ({status or 'NOT_STARTED'})"
            )
        return None
    if for_write and state.get("stale"):
        raise ProductionStateConflictError(
            "production sources changed; rerun required"
        )
    synthesis = production_store.load_artifact(
        session_id, pair_id, "unified_synthesis"
    )
    if synthesis is None:
        raise ProductionStateConflictError("published synthesis is missing")
    validated = validate_synthesis(synthesis)
    actual = canonical_synthesis_digest(validated)
    stage = (state.get("stages") or {}).get("unified_synthesis") or {}
    expected = stage.get("input_signature")
    if not expected or actual != expected:
        raise ProductionStateConflictError(
            "published synthesis generation does not match state"
        )
    return validated


def _review_source_synthesis(
    session_id: str,
    pair_id: str,
    state: Mapping[str, Any],
    effective_synthesis: Mapping[str, Any],
) -> dict[str, Any]:
    """Return immutable automatic synthesis used to version review questions."""
    payload = production_store.load_artifact(
        session_id, pair_id, "automatic_unified_synthesis"
    )
    expected = (
        ((state.get("stages") or {}).get("automatic_unified_synthesis") or {})
        .get("input_signature")
    )
    if payload is None:  # compatibility with a short-lived pre-contract run
        if expected:
            raise ProductionStateConflictError(
                "published automatic synthesis is missing"
            )
        return validate_synthesis(dict(effective_synthesis))
    automatic = validate_synthesis(payload)
    actual = canonical_synthesis_digest(automatic)
    if expected and expected != actual:
        raise ProductionStateConflictError(
            "automatic synthesis generation does not match state"
        )
    return automatic


def _apply_completed_change_resolutions(
    text_atoms: list[dict[str, Any]],
    graphic_atoms: list[dict[str, Any]],
    source_synthesis: Mapping[str, Any],
    application: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Apply complete CHANGE answers to exact dependent atom copies.

    Review-evidence dependencies address one atom.  Contested dependencies
    address a group of already-synthesized changes, so their selected change
    ids are first translated back to the exact evidence atom ids.
    """
    review_targets = {
        str(item.get("review_evidence_id") or ""): str(item.get("atom_id") or "")
        for item in source_synthesis.get("review_items") or []
        if isinstance(item, Mapping)
    }
    changes_by_id = {
        str(item.get("change_id") or ""): item
        for item in source_synthesis.get("changes") or []
        if isinstance(item, Mapping)
    }
    contested_by_id = {
        str(item.get("group_id") or ""): item
        for item in source_synthesis.get("contested_groups") or []
        if isinstance(item, Mapping)
    }

    def change_atom_ids(change_id: str) -> set[str]:
        change = changes_by_id.get(change_id)
        if not isinstance(change, Mapping):
            return set()
        return {
            str(evidence.get("atom_id") or "")
            for evidence in change.get("evidence_refs") or []
            if isinstance(evidence, Mapping) and evidence.get("atom_id")
        }

    resolutions: dict[str, Mapping[str, Any]] = {}
    excluded_atom_ids: set[str] = set()
    for item in application.get("change_resolutions") or []:
        if not isinstance(item, Mapping) or not item.get("resolution_complete"):
            continue
        for dependency in item.get("dependency_refs") or []:
            dependency_ref = str(dependency)
            atom_id = review_targets.get(dependency_ref)
            if atom_id:
                resolutions[atom_id] = item
                continue
            group = contested_by_id.get(dependency_ref)
            if not isinstance(group, Mapping):
                raise ProductionStateConflictError(
                    "complete CHANGE resolution has no published dependency"
                )
            offered_ids = {
                str(value) for value in group.get("change_ids") or [] if value
            }
            all_atom_ids = {
                atom_id
                for change_id in offered_ids
                for atom_id in change_atom_ids(change_id)
            }
            if not all_atom_ids:
                raise ProductionStateConflictError(
                    "contested CHANGE dependency has no source atoms"
                )
            if item.get("resolution") == "REJECTED":
                excluded_atom_ids.update(all_atom_ids)
                continue
            typed = item.get("typed_resolution")
            selected_ids = {
                str(value)
                for value in (
                    (typed or {}).get("selected_change_ids")
                    if isinstance(typed, Mapping)
                    else []
                )
                or ((item.get("decision") or {}).get("selected_refs") or [])
                if value
            }
            if (
                not selected_ids
                or not selected_ids < offered_ids
            ):
                raise ProductionStateConflictError(
                    "contested CHANGE resolution must select a proper offered subset"
                )
            excluded_atom_ids.update(
                atom_id
                for change_id in offered_ids - selected_ids
                for atom_id in change_atom_ids(change_id)
            )
            for change_id in selected_ids:
                for selected_atom_id in change_atom_ids(change_id):
                    resolutions[selected_atom_id] = item

    def resolve(values: list[dict[str, Any]]) -> list[dict[str, Any]]:
        output = []
        for source in values:
            atom_id = str(source.get("atom_id") or "")
            if atom_id in excluded_atom_ids:
                continue
            resolution = resolutions.get(atom_id)
            if resolution is None:
                output.append(dict(source))
                continue
            if resolution.get("resolution") == "REJECTED":
                continue
            typed = resolution.get("typed_resolution")
            atom = copy.deepcopy(dict(source))
            if isinstance(typed, Mapping):
                for field in (
                    "dimension",
                    "subject_ref",
                    "project_entity_ref",
                    "facet_ref",
                    "direction",
                    "outcome",
                    "before_value",
                    "after_value",
                ):
                    if field in typed:
                        atom[field] = copy.deepcopy(typed[field])
            if not atom.get("subject_ref") and atom.get("project_entity_ref"):
                atom["subject_ref"] = atom["project_entity_ref"]
            atom["review_status"] = "CONFIRMED"
            provenance = dict(atom.get("provenance") or {})
            record = {
                "resolution": resolution.get("resolution"),
                "question_id": resolution.get("question_id"),
                "decision_id": (
                    (resolution.get("decision") or {}).get("decision_id")
                    if isinstance(resolution.get("decision"), Mapping)
                    else None
                ),
                "application_signature": application.get("input_signature"),
            }
            if resolution.get("source") == "AI":
                # Провенанс — единственный след того, КТО изменил атом.
                # Записать машинное разрешение под ключом человека значит
                # солгать аудиту, поэтому ключ отдельный.
                ai_details = resolution.get("ai_resolution")
                provenance["ai_change_resolution"] = {
                    **record,
                    **(dict(ai_details) if isinstance(ai_details, Mapping) else {}),
                }
            else:
                provenance["human_change_resolution"] = record
            atom["provenance"] = provenance
            output.append(atom)
        return output

    return resolve(text_atoms), resolve(graphic_atoms)


def _assert_change_resolutions_materialized(
    effective_synthesis: Mapping[str, Any],
    source_synthesis: Mapping[str, Any],
    application: Mapping[str, Any],
) -> None:
    """Fail closed when a nominally complete answer did not change its target."""
    review_targets = {
        str(item.get("review_evidence_id") or ""): str(item.get("atom_id") or "")
        for item in source_synthesis.get("review_items") or []
        if isinstance(item, Mapping)
    }
    source_changes = {
        str(item.get("change_id") or ""): item
        for item in source_synthesis.get("changes") or []
        if isinstance(item, Mapping)
    }
    source_contests = {
        str(item.get("group_id") or ""): item
        for item in source_synthesis.get("contested_groups") or []
        if isinstance(item, Mapping)
    }

    def source_change_atoms(change_ids: Iterable[str]) -> set[str]:
        return {
            str(evidence.get("atom_id") or "")
            for change_id in change_ids
            for evidence in (source_changes.get(str(change_id)) or {}).get(
                "evidence_refs"
            )
            or []
            if isinstance(evidence, Mapping) and evidence.get("atom_id")
        }

    surfaced_atoms = {
        str(evidence.get("atom_id") or "")
        for change in effective_synthesis.get("changes") or []
        if isinstance(change, Mapping)
        for evidence in change.get("evidence_refs") or []
        if isinstance(evidence, Mapping) and evidence.get("atom_id")
    }
    remaining_review_atoms = {
        str(item.get("atom_id") or "")
        for item in effective_synthesis.get("review_items") or []
        if isinstance(item, Mapping) and item.get("atom_id")
    }
    effective_changes_by_atom = {
        str(evidence.get("atom_id") or ""): change
        for change in effective_synthesis.get("changes") or []
        if isinstance(change, Mapping)
        for evidence in change.get("evidence_refs") or []
        if isinstance(evidence, Mapping) and evidence.get("atom_id")
    }
    for resolution in application.get("change_resolutions") or []:
        if not isinstance(resolution, Mapping) or not resolution.get(
            "resolution_complete"
        ):
            continue
        for raw_dependency in resolution.get("dependency_refs") or []:
            dependency = str(raw_dependency)
            review_atom = review_targets.get(dependency)
            if review_atom:
                if resolution.get("resolution") == "REJECTED":
                    if review_atom in surfaced_atoms | remaining_review_atoms:
                        raise ProductionStateConflictError(
                            "rejected CHANGE evidence remained materialized"
                        )
                elif (
                    review_atom not in surfaced_atoms
                    or review_atom in remaining_review_atoms
                    or effective_changes_by_atom[review_atom].get("review_status")
                    != "CONFIRMED"
                    or effective_changes_by_atom[review_atom].get("outcome")
                    == "REVIEW_REQUIRED"
                ):
                    raise ProductionStateConflictError(
                        "confirmed CHANGE evidence did not become an atomic change"
                    )
                continue
            contest = source_contests.get(dependency)
            if not isinstance(contest, Mapping):
                raise ProductionStateConflictError(
                    "complete CHANGE resolution has no published dependency"
                )
            offered = {
                str(value) for value in contest.get("change_ids") or [] if value
            }
            offered_atoms = source_change_atoms(offered)
            if resolution.get("resolution") == "REJECTED":
                if offered_atoms & (surfaced_atoms | remaining_review_atoms):
                    raise ProductionStateConflictError(
                        "rejected contested CHANGE group remained materialized"
                    )
                continue
            typed = resolution.get("typed_resolution")
            selected = {
                str(value)
                for value in (
                    (typed or {}).get("selected_change_ids")
                    if isinstance(typed, Mapping)
                    else []
                )
                or ((resolution.get("decision") or {}).get("selected_refs") or [])
                if value
            }
            selected_atoms = source_change_atoms(selected)
            removed_atoms = source_change_atoms(offered - selected)
            if (
                not selected_atoms
                or not selected_atoms <= surfaced_atoms
                or removed_atoms & (surfaced_atoms | remaining_review_atoms)
                or any(
                    effective_changes_by_atom[atom_id].get("review_status")
                    != "CONFIRMED"
                    for atom_id in selected_atoms
                )
            ):
                raise ProductionStateConflictError(
                    "contested CHANGE selection was not materialized exactly"
                )


def _rebuild_dependent_synthesis(
    session_id: str,
    pair_id: str,
    pair: Mapping[str, Any],
    state: Mapping[str, Any],
    source_synthesis: Mapping[str, Any],
    application: Mapping[str, Any],
    automatic_entity_relations: Mapping[str, Any],
    *,
    source_snapshot: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Project automatic synthesis plus every current non-stale answer."""
    has_entity_override = bool(
        ((application.get("effective_entity_relations") or {}).get("diagnostics") or {})
        .get("human_decision_overrides_applied")
    )
    has_change_resolution = any(
        isinstance(item, Mapping) and item.get("resolution_complete")
        for item in application.get("change_resolutions") or []
    )
    snapshot = (
        _validate_source_snapshot(source_snapshot, state)
        if isinstance(source_snapshot, Mapping)
        else _load_published_source_snapshot(session_id, pair_id, state)
    )
    text_source = snapshot["text"]
    graphic_source = snapshot["graphic"]
    text_artifact = text_source["artifact"]
    text_atoms = [
        dict(item) for item in (text_artifact or {}).get("atoms") or []
        if isinstance(item, Mapping)
    ]
    ledger = graphic_source.get("ledger")
    graphic_atoms = _graphic_atoms_from_source(ledger)
    text_atoms, graphic_atoms = _apply_completed_change_resolutions(
        text_atoms, graphic_atoms, source_synthesis, application
    )
    effective_entity = application.get("effective_entity_relations")
    relations = (
        effective_entity
        if isinstance(effective_entity, Mapping)
        else automatic_entity_relations
    )
    bound = _bind_synthesis_atoms(text_atoms, graphic_atoms, relations)
    bound_text = list(bound.get("text_atoms") or [])
    bound_graphic = list(bound.get("graphic_atoms") or [])

    if not has_entity_override and not has_change_resolution:
        # The identity projection is the immutable automatic payload itself.
        # Saving the freshly automatic bound atoms also rolls back an older
        # entity override without introducing a second synthesis identity.
        return validate_synthesis(dict(source_synthesis)), bound

    graphic_stage = (state.get("stages") or {}).get("graphic") or {}
    semantic_mode2_checked = (
        graphic_stage.get("status") == "COMPLETED"
        and graphic_stage.get("mode") == "MODE_2"
    )
    descriptors = pair_documents_from_pair_artifact(dict(pair))
    binding_proven = all(
        document_identity_is_complete(descriptors[side])
        for side in ("LEFT", "RIGHT")
    )
    candidates = _build_synthesis_candidates(
        bound_text,
        bound_graphic,
        relations,
        source_valid=semantic_mode2_checked,
        coverage_by_side=(
            {"LEFT": "CHECKED", "RIGHT": "CHECKED"}
            if semantic_mode2_checked
            else {"LEFT": "NOT_CHECKED", "RIGHT": "NOT_CHECKED"}
        ),
        document_binding_state=(
            "DOCUMENT_BINDING_PROVEN"
            if semantic_mode2_checked and binding_proven
            else "DOCUMENT_BINDING_UNPROVEN"
        ),
    )
    source_states = {
        "TEXT": "VALID" if bound_text else text_source.get("source_state", "ABSENT"),
        "GRAPHIC": (
            "VALID" if bound_graphic else graphic_source.get("source_state", "ABSENT")
        ),
    }
    rebuilt = synthesize_unified_changes(
        text_atoms=bound_text,
        graphic_atoms=bound_graphic,
        candidates=candidates,
        source_states=source_states,
    )
    _assert_change_resolutions_materialized(
        rebuilt, source_synthesis, application
    )
    return rebuilt, bound


def _publish_derived_state(
    session_id: str,
    pair_id: str,
    state: Mapping[str, Any],
    synthesis: Mapping[str, Any],
    decisions: Mapping[str, Any],
    report: Mapping[str, Any],
) -> dict[str, Any]:
    stages = copy.deepcopy(state.get("stages") or {})
    stages["unified_synthesis"] = {
        **dict(stages.get("unified_synthesis") or {}),
        "status": "COMPLETED",
        "changes": len(synthesis.get("changes") or []),
        "review_items": len(synthesis.get("review_items") or []),
        "input_signature": canonical_synthesis_digest(synthesis),
        "present": True,
    }
    stages["engineer_decisions"] = {
        **dict(stages.get("engineer_decisions") or {}),
        "status": "READY",
        "counts": decisions.get("counts") or {},
        "revision": int(decisions.get("revision") or 0),
        "content_digest": content_signature(decisions),
        **_artifact_state(decisions),
    }
    stages["final_report"] = {
        **dict(stages.get("final_report") or {}),
        "status": "READY",
        "approved": len(report.get("approved_atomic_changes") or []),
        "content_digest": content_signature(report),
        **_artifact_state(report),
    }
    return _write_state(session_id, pair_id, {**dict(state), "stages": stages})


def _empty_decisions_for(synthesis: Mapping[str, Any]) -> dict[str, Any]:
    return build_engineer_decisions(synthesis)


def _published_decisions(
    session_id: str,
    pair_id: str,
    state: Mapping[str, Any],
    synthesis: Mapping[str, Any],
) -> dict[str, Any]:
    """Load only decisions bound to the currently published state."""
    payload = production_store.load_artifact(
        session_id, pair_id, "engineer_decisions"
    )
    stage = (state.get("stages") or {}).get("engineer_decisions") or {}
    if payload is None:
        if stage.get("present"):
            raise ProductionStateConflictError(
                "published engineer decisions are missing"
            )
        return _empty_decisions_for(synthesis)
    expected_synthesis = canonical_synthesis_digest(synthesis)
    if payload.get("input_signature") != expected_synthesis:
        raise ProductionStateConflictError(
            "engineer decisions do not match published synthesis"
        )
    expected_revision = stage.get("revision")
    if expected_revision is not None and int(payload.get("revision") or 0) != int(
        expected_revision
    ):
        raise ProductionStateConflictError(
            "engineer decisions revision does not match state"
        )
    expected_digest = stage.get("content_digest")
    if expected_digest and content_signature(payload) != expected_digest:
        raise ProductionStateConflictError(
            "engineer decisions digest does not match state"
        )
    return payload


def _enrich_rows_with_sheet_references(
    session_id: str,
    pair_id: str,
    rows: list[dict[str, Any]],
) -> None:
    """Дать каждой находке название и номер листа, а не только страницу PDF.

    Инженер ищет «лист 7», а не «страницу 29»: номер из штампа и номер
    страницы в файле совпадают редко, и таблица, показывающая только второе,
    заставляет держать это соответствие в голове.
    """
    review_module = importlib.import_module(
        "backend.app.services.stage_comparison.review_queue"
    )
    sheet_relations = production_store.load_artifact(
        session_id, pair_id, "sheet_relations"
    )
    if not isinstance(sheet_relations, Mapping):
        return
    for row in rows:
        change = row.get("change")
        change = change if isinstance(change, Mapping) else {}
        provenance = change.get("provenance")
        source_atom = (
            provenance.get("source_atom") if isinstance(provenance, Mapping) else None
        )
        locations = (
            source_atom.get("locations") if isinstance(source_atom, Mapping) else None
        )
        pages = {"LEFT": set(), "RIGHT": set()}
        if isinstance(locations, Mapping):
            for side in ("LEFT", "RIGHT"):
                for value in locations.get(side) or []:
                    if isinstance(value, Mapping) and isinstance(value.get("page"), int):
                        pages[side].add(int(value["page"]))
        if isinstance(provenance, Mapping):
            for atom in provenance.get("source_atoms") or []:
                nested = atom.get("provenance") if isinstance(atom, Mapping) else None
                nested_locations = (
                    nested.get("locations") if isinstance(nested, Mapping) else None
                )
                if not isinstance(nested_locations, Mapping):
                    continue
                for side in ("LEFT", "RIGHT"):
                    for value in nested_locations.get(side) or []:
                        if (
                            isinstance(value, Mapping)
                            and isinstance(value.get("page"), int)
                        ):
                            pages[side].add(int(value["page"]))
        for side, key in (("LEFT", "left_sheets"), ("RIGHT", "right_sheets")):
            row[key] = [
                review_module.sheet_reference(sheet_relations, side, page)
                for page in sorted(pages[side])
            ]


def get_production_changes(session_id: str, pair_id: str) -> dict[str, Any]:
    """Read review rows; no producer artifact is written by this GET."""
    state = get_production_state(session_id, pair_id)
    synthesis = _published_synthesis(session_id, pair_id, state)
    if synthesis is None:
        return {
            "kind": CHANGES_KIND,
            "schema_version": CHANGES_SCHEMA_VERSION,
            "version": 1,
            "revision": 0,
            "input_signature": None,
            "stale": state["stale"],
            "available": False,
            "run_status": state.get("status"),
            "summary": {
                "total": 0,
                "APPROVED": 0,
                "PENDING_REVIEW": 0,
                "REJECTED": 0,
            },
            "rows": [],
        }
    decisions = _published_decisions(
        session_id, pair_id, state, synthesis
    )
    rows = review_rows(synthesis, decisions)
    _enrich_rows_with_sheet_references(session_id, pair_id, rows)
    counts = {"APPROVED": 0, "PENDING_REVIEW": 0, "REJECTED": 0}
    for row in rows:
        decision = (row.get("engineer_decision") or {}).get("decision")
        if decision in counts:
            counts[decision] += 1
    return {
        "kind": CHANGES_KIND,
        "schema_version": CHANGES_SCHEMA_VERSION,
        "version": 1,
        "revision": int(decisions.get("revision") or 0),
        "input_signature": canonical_synthesis_digest(synthesis),
        "stale": state["stale"],
        "available": True,
        "run_status": state.get("status"),
        "summary": {"total": len(rows), **counts},
        "rows": rows,
    }


def get_production_text_evidence(
    session_id: str,
    pair_id: str,
) -> dict[str, Any]:
    """Read the current generation's exact TEXT viewer evidence.

    This GET never acquires a producer lock and never creates or refreshes an
    artifact.  A non-published or blocked TEXT branch is represented as an
    honest empty payload so an older generation cannot leak through while a
    new run is being produced.
    """
    state = get_production_state(session_id, pair_id)
    if not evidence_is_publishable(state):
        return empty_production_text_evidence(state)

    source_snapshot = _load_published_source_snapshot(
        session_id, pair_id, state
    )
    text_differences = production_store.load_artifact(
        session_id, pair_id, "text_differences"
    )
    if text_differences is None:
        raise ProductionStateConflictError(
            "published production Stage 3 text differences are missing"
        )
    synthesis = _published_synthesis(session_id, pair_id, state)
    if synthesis is None:  # guarded by ``evidence_is_publishable``
        raise ProductionStateConflictError("published synthesis is missing")
    try:
        payload = build_production_text_evidence(
            state=state,
            source_snapshot=source_snapshot,
            text_differences=text_differences,
            synthesis=synthesis,
            synthesis_input_signature=canonical_synthesis_digest(synthesis),
        )
    except ProductionTextEvidenceConflictError as exc:
        raise ProductionStateConflictError(str(exc)) from exc

    # Detect a full rerun or a review-driven synthesis publication racing this
    # multi-artifact read.  Returning a mixed but individually valid payload
    # would violate the endpoint's generation-bound contract.
    latest = production_store.load_artifact(session_id, pair_id, "state")
    if not isinstance(latest, Mapping) or any(
        latest.get(key) != state.get(key)
        for key in ("run_id", "input_signature", "revision", "status")
    ):
        raise ProductionStateConflictError(
            "production generation changed while TEXT evidence was read"
        )
    return payload


def get_review_questions(session_id: str, pair_id: str) -> dict[str, Any]:
    state = get_production_state(session_id, pair_id)
    synthesis = _published_synthesis(session_id, pair_id, state)
    if synthesis is None:
        return {
            "kind": QUESTIONS_KIND,
            "schema_version": QUESTIONS_SCHEMA_VERSION,
            "version": 1,
            "revision": 0,
            "input_signature": None,
            "questions": [],
            "counts": {"SHEET": 0, "ENTITY": 0, "CHANGE": 0, "total": 0},
            "stale": state["stale"],
            "available": False,
            "run_status": state.get("status"),
            "suggestion_actions": {},
            "suggestion_action_semantics": _suggestion_action_semantics(state),
        }
    review_synthesis = _review_source_synthesis(
        session_id, pair_id, state, synthesis
    )
    questions = production_store.load_artifact(
        session_id, pair_id, "review_questions"
    )
    if questions is None:
        questions = {
            "kind": QUESTIONS_KIND,
            "schema_version": QUESTIONS_SCHEMA_VERSION,
            "version": 1,
            "revision": 0,
            "input_signature": None,
            "questions": [],
            "counts": {"SHEET": 0, "ENTITY": 0, "CHANGE": 0, "total": 0},
        }
    answers = production_store.load_artifact(
        session_id, pair_id, "review_answers"
    )
    response = {
        **questions,
        "revision": int((answers or {}).get("revision") or 0),
        "stale": state["stale"],
        "available": True,
        "run_status": state.get("status"),
        "suggestion_actions": dict(state.get("suggestion_actions") or {}),
        "suggestion_action_semantics": _suggestion_action_semantics(state),
    }
    last_application = production_store.load_artifact(
        session_id, pair_id, "review_application"
    )
    if last_application is not None:
        response["last_application"] = last_application
    if answers:
        review_module = importlib.import_module(
            "backend.app.services.stage_comparison.review_queue"
        )
        sheet_relations = production_store.load_artifact(
            session_id, pair_id, "sheet_relations"
        )
        entity_relations = production_store.load_artifact(
            session_id, pair_id, "entity_relations"
        )
        ai_resolutions = production_store.load_artifact(
            session_id, pair_id, "ai_resolutions"
        )
        base_queue = _build_review_questions(
            sheet_relations=sheet_relations or {},
            sheet_suggestions=(
                state.get("sheet_suggestions")
                if isinstance(state.get("sheet_suggestions"), Mapping)
                else None
            ),
            entity_relations=entity_relations or {},
            synthesis=review_synthesis,
            answers=None,
            ai_resolutions=ai_resolutions,
            input_mode=str(state.get("input_mode") or "DOCUMENT"),
        )
        current_application = review_module.apply_human_decisions(
            base_queue,
            answers,
            sheet_relations=sheet_relations,
            entity_relations=entity_relations,
            synthesis=review_synthesis,
            ai_resolutions=ai_resolutions,
        )
        persisted_diagnostics = (
            last_application.get("diagnostics")
            if isinstance(last_application, Mapping)
            else None
        )
        if (
            isinstance(persisted_diagnostics, Mapping)
            and persisted_diagnostics.get("generation_run_id")
            == state.get("run_id")
        ):
            diagnostics = dict(current_application.get("diagnostics") or {})
            for field in (
                "scope_applied",
                "pipeline_rerun",
                "generation_was_materialized",
                "this_update_reran",
                "sheet_scope_changed",
                "automatic_sheet_scope_signature",
                "materialized_sheet_scope_signature",
                "sheet_scope_decision_ids",
                "generation_run_id",
                "effective_page_groups",
                "page_action_state",
                "page_action_outcomes",
            ):
                if field in persisted_diagnostics:
                    diagnostics[field] = copy.deepcopy(
                        persisted_diagnostics[field]
                    )
            current_application["diagnostics"] = diagnostics
        response["application"] = current_application
    return response


def update_engineer_decisions(
    session_id: str,
    pair_id: str,
    *,
    updates: list[Mapping[str, Any]],
    author: str,
    expected_input_signature: str | None = None,
    expected_revision: int | None = None,
) -> dict[str, Any]:
    with production_store.production_pair_lock(session_id, pair_id):
        return _update_engineer_decisions_locked(
            session_id,
            pair_id,
            updates=updates,
            author=author,
            expected_input_signature=expected_input_signature,
            expected_revision=expected_revision,
        )


def _update_engineer_decisions_locked(
    session_id: str,
    pair_id: str,
    *,
    updates: list[Mapping[str, Any]],
    author: str,
    expected_input_signature: str | None = None,
    expected_revision: int | None = None,
) -> dict[str, Any]:
    if not expected_input_signature or expected_revision is None:
        raise ProductionStateConflictError(
            "expected_input_signature and expected_revision are required"
        )
    state = get_production_state(session_id, pair_id)
    synthesis = _published_synthesis(
        session_id, pair_id, state, for_write=True
    )
    assert synthesis is not None
    synthesis_signature = canonical_synthesis_digest(synthesis)
    if expected_input_signature != synthesis_signature:
        raise ProductionStateConflictError("production input signature changed")
    current = _published_decisions(session_id, pair_id, state, synthesis)
    if int(current.get("revision") or 0) != expected_revision:
        raise ProductionStateConflictError("engineer decisions revision changed")
    decisions = build_engineer_decisions(
        synthesis,
        existing=current,
        updates=[{**dict(update), "author": author} for update in updates],
    )
    report = build_final_report(synthesis, decisions, object_ref=None)

    previous_status = str(state.get("status") or "COMPLETED")
    updating_state = _write_state(
        session_id,
        pair_id,
        {
            **state,
            "status": "UPDATING",
            "progress": 100,
            "updating_at": utc_now(),
        },
    )
    production_store.save_artifact(
        session_id, pair_id, "engineer_decisions", decisions
    )
    production_store.save_artifact(
        session_id, pair_id, "final_report", report
    )
    publication_state = {
        **updating_state,
        "status": previous_status,
        "progress": 100,
    }
    _publish_derived_state(
        session_id,
        pair_id,
        publication_state,
        synthesis,
        decisions,
        report,
    )
    return get_production_changes(session_id, pair_id)


def update_review_answers(
    session_id: str,
    pair_id: str,
    *,
    answers: list[Mapping[str, Any]],
    author: str,
    expected_input_signature: str | None = None,
    expected_revision: int | None = None,
) -> dict[str, Any]:
    with production_store.production_pair_lock(session_id, pair_id):
        return _update_review_answers_locked(
            session_id,
            pair_id,
            answers=answers,
            author=author,
            expected_input_signature=expected_input_signature,
            expected_revision=expected_revision,
        )


def _update_review_answers_locked(
    session_id: str,
    pair_id: str,
    *,
    answers: list[Mapping[str, Any]],
    author: str,
    expected_input_signature: str | None = None,
    expected_revision: int | None = None,
) -> dict[str, Any]:
    if not expected_input_signature or expected_revision is None:
        raise ProductionStateConflictError(
            "expected_input_signature and expected_revision are required"
        )
    state = get_production_state(session_id, pair_id)
    synthesis = _published_synthesis(
        session_id, pair_id, state, for_write=True
    )
    assert synthesis is not None
    review_synthesis = _review_source_synthesis(
        session_id, pair_id, state, synthesis
    )
    questions = production_store.load_artifact(
        session_id, pair_id, "review_questions"
    )
    if questions is None:
        raise KeyError("review_questions_not_found")
    review_module = importlib.import_module(
        "backend.app.services.stage_comparison.review_queue"
    )
    sheet_relations = production_store.load_artifact(
        session_id, pair_id, "sheet_relations"
    )
    entity_relations = production_store.load_artifact(
        session_id, pair_id, "entity_relations"
    )
    # Reconstruct the full deterministic queue so an already-resolved answer
    # can itself be revised.  The persisted/public queue remains the pending
    # projection and automatic source artifacts are never mutated.
    sheet_suggestions = (
        state.get("sheet_suggestions")
        if isinstance(state.get("sheet_suggestions"), Mapping)
        else None
    )
    ai_resolutions = production_store.load_artifact(
        session_id, pair_id, "ai_resolutions"
    )
    base_questions = _build_review_questions(
        sheet_relations=sheet_relations or {},
        sheet_suggestions=sheet_suggestions,
        entity_relations=entity_relations or {},
        synthesis=review_synthesis,
        answers=None,
        ai_resolutions=ai_resolutions,
        input_mode=str(state.get("input_mode") or "DOCUMENT"),
    )
    input_signature = str(base_questions.get("input_signature") or "")
    if expected_input_signature is not None and expected_input_signature != input_signature:
        raise ProductionStateConflictError("review questions input signature changed")
    normalized = []
    for value in answers:
        item = {
            "question_id": str(value.get("question_id") or ""),
            "answer": value.get("answer"),
            "comment": value.get("comment"),
        }
        for field in ("selected_refs", "explicit_candidate", "typed_resolution"):
            if field in value and value.get(field) is not None:
                item[field] = copy.deepcopy(value.get(field))
        normalized.append(item)
    if any(not item["question_id"] for item in normalized):
        raise ValueError("review answer question_id is required")
    if len({item["question_id"] for item in normalized}) != len(normalized):
        raise ValueError("duplicate review answer update")
    question_by_id = {
        str(item.get("question_id") or ""): item
        for item in base_questions.get("questions") or []
        if isinstance(item, Mapping)
    }
    materializing_page_updates = [
        item for item in normalized
        if item.get("answer") in PAGE_MATERIALIZING_ACTIONS
        and (
            question_by_id.get(item["question_id"]) or {}
        ).get("question_type") == "PAGE_SUGGESTION_ACTION"
    ]
    if len(materializing_page_updates) > 1:
        raise ValueError(
            "multiple materializing PAGE suggestion actions are ambiguous"
        )

    current_answers = production_store.load_artifact(
        session_id, pair_id, "review_answers"
    ) or {}
    current_revision = int(current_answers.get("revision") or 0)
    if expected_revision != current_revision:
        raise ProductionStateConflictError("review answers revision changed")
    current_page_actions: dict[str, str] = {}
    for decision in current_answers.get("decisions") or []:
        if not isinstance(decision, Mapping):
            continue
        question_id = str(decision.get("question_id") or "")
        question = question_by_id.get(question_id)
        if (
            not isinstance(question, Mapping)
            or question.get("question_type") != "PAGE_SUGGESTION_ACTION"
            or review_module.decision_is_stale(decision, question)
        ):
            continue
        current_page_actions[question_id] = str(decision.get("answer") or "")
    page_scope_mutation_question_ids = []
    for item in normalized:
        question_id = item["question_id"]
        question = question_by_id.get(question_id) or {}
        if question.get("question_type") != "PAGE_SUGGESTION_ACTION":
            continue
        previous_action = current_page_actions.get(question_id, "")
        next_action = str(item.get("answer") or "")
        if (
            previous_action != next_action
            and (
                previous_action in PAGE_MATERIALIZING_ACTIONS
                or next_action in PAGE_MATERIALIZING_ACTIONS
            )
        ):
            page_scope_mutation_question_ids.append(question_id)
    # Build the entire proposal in memory first.  A materialization failure
    # must not persist/suppress an answer while the old effective synthesis is
    # still published.
    answer_artifact = review_module.build_human_decisions(
        base_questions,
        normalized,
        previous=current_answers,
        author=author,
    )
    application = review_module.apply_human_decisions(
        base_questions,
        answer_artifact,
        sheet_relations=sheet_relations,
        entity_relations=entity_relations,
        synthesis=review_synthesis,
        ai_resolutions=ai_resolutions,
    )
    sheet_scope_stage = (
        (state.get("stages") or {}).get("sheet_scope") or {}
    )
    sheet_projection: dict[str, Any] | None = None
    if (
        state.get("input_mode") == "DOCUMENT"
        and isinstance(sheet_relations, Mapping)
    ):
        sheet_projection = _materialized_sheet_scope(
            sheet_relations, application
        )
        current_scope_signature = sheet_scope_stage.get("input_signature")
        if not current_scope_signature:
            # Compatibility with generations published before materialized
            # scope became explicit: those branches used automatic relations.
            current_scope_signature = sheet_projection["automatic_signature"]
        if current_scope_signature != sheet_projection["effective_signature"]:
            selection = state.get("selection")
            if not isinstance(selection, Mapping):
                raise ProductionStateConflictError(
                    "published DOCUMENT selection is missing"
                )
            request = normalize_run_request(**dict(selection))
            _write_state(
                session_id,
                pair_id,
                {
                    **state,
                    "status": "UPDATING",
                    "progress": 100,
                    "updating_at": utc_now(),
                },
            )
            rerun_state = _run_production_comparison_locked(
                session_id,
                pair_id,
                **request,
                review_answers_override=answer_artifact,
            )
            response = get_review_questions(session_id, pair_id)
            persisted_application = production_store.load_artifact(
                session_id, pair_id, "review_application"
            )
            return {
                **response,
                "state": rerun_state,
                "revision": int(answer_artifact.get("revision") or 0),
                "stale": False,
                "application": persisted_application
                or response.get("application")
                or application,
            }

        application = _apply_sheet_scope_diagnostics(
            application,
            sheet_projection,
            run_id=str(state.get("run_id") or ""),
            pipeline_rerun=bool(sheet_scope_stage.get("pipeline_rerun")),
        )
    elif state.get("input_mode") == "PAGE":
        selection = state.get("selection")
        if not isinstance(selection, Mapping):
            raise ProductionStateConflictError(
                "published PAGE selection is missing"
            )
        request = normalize_run_request(**dict(selection))
        sheet_projection = _page_action_projection(
            request, sheet_suggestions, answer_artifact
        )
        pair = store.get_pair_for_production(session_id, pair_id)
        _validate_page_bounds(pair, request, sheet_projection["groups"])
        current_scope_signature = sheet_scope_stage.get("input_signature")
        if not current_scope_signature:
            generation_scope = state.get("generation_scope")
            current_groups = (
                generation_scope.get("page_groups")
                if isinstance(generation_scope, Mapping)
                else [_selected_page_group(request)]
            )
            current_scope_signature = _sheet_scope_signature(current_groups)
        if current_scope_signature != sheet_projection["effective_signature"]:
            _write_state(
                session_id,
                pair_id,
                {
                    **state,
                    "status": "UPDATING",
                    "progress": 100,
                    "updating_at": utc_now(),
                },
            )
            rerun_state = _run_production_comparison_locked(
                session_id,
                pair_id,
                **request,
                review_answers_override=answer_artifact,
                page_groups_override=sheet_projection["groups"],
                page_scope_rerun=True,
                page_rerun_question_ids=page_scope_mutation_question_ids,
            )
            response = get_review_questions(session_id, pair_id)
            persisted_application = production_store.load_artifact(
                session_id, pair_id, "review_application"
            )
            return {
                **response,
                "state": rerun_state,
                "revision": int(answer_artifact.get("revision") or 0),
                "stale": False,
                "application": persisted_application
                or response.get("application")
                or application,
            }
        application = _apply_sheet_scope_diagnostics(
            application,
            sheet_projection,
            run_id=str(state.get("run_id") or ""),
            pipeline_rerun=bool(sheet_scope_stage.get("pipeline_rerun")),
            this_update_reran=False,
        )
    else:
        current_scope_signature = str(
            sheet_scope_stage.get("input_signature") or ""
        )
        application = _apply_sheet_scope_diagnostics(
            application,
            {
                "automatic_signature": current_scope_signature,
                "effective_signature": current_scope_signature,
                "scope_changed": False,
                "scope_applied": False,
                "decision_ids": [],
            },
            run_id=str(state.get("run_id") or ""),
            pipeline_rerun=False,
        )
    pair = store.get_pair_for_production(session_id, pair_id)
    rebuilt, effective_bound_atoms = _rebuild_dependent_synthesis(
        session_id,
        pair_id,
        pair,
        state,
        review_synthesis,
        application,
        entity_relations or {},
    )
    synthesis_changed = (
        canonical_synthesis_digest(rebuilt)
        != canonical_synthesis_digest(synthesis)
    )
    updated_questions = _build_review_questions(
        sheet_relations=sheet_relations or {},
        sheet_suggestions=sheet_suggestions,
        entity_relations=entity_relations or {},
        synthesis=review_synthesis,
        answers=answer_artifact,
        ai_resolutions=ai_resolutions,
        input_mode=str(state.get("input_mode") or "DOCUMENT"),
    )

    previous_status = str(state.get("status") or "COMPLETED")
    updating_state = _write_state(
        session_id,
        pair_id,
        {
            **state,
            "status": "UPDATING",
            "progress": 100,
            "updating_at": utc_now(),
        },
    )
    production_store.save_artifact(
        session_id, pair_id, "effective_bound_atoms", effective_bound_atoms
    )
    if synthesis_changed:
        synthesis = production_store.save_unified_synthesis(
            session_id, pair_id, rebuilt
        )
    production_store.save_artifact(
        session_id, pair_id, "review_application", application
    )
    production_store.save_artifact(
        session_id, pair_id, "review_answers", answer_artifact
    )
    production_store.save_artifact(
        session_id, pair_id, "review_questions", updated_questions
    )

    publication_stages = copy.deepcopy(updating_state.get("stages") or {})
    publication_stages["effective_entity_binding"] = {
        "status": "COMPLETED",
        "bound_atoms": len(
            (effective_bound_atoms.get("diagnostics") or {}).get(
                "bound_atom_ids"
            )
            or []
        ),
        **_artifact_state(effective_bound_atoms),
    }
    updated_question_stage = _review_question_stage(updated_questions)
    existing_question_progress = (
        publication_stages.get("review_questions") or {}
    ).get("progress")
    if isinstance(existing_question_progress, Mapping):
        question_progress = copy.deepcopy(dict(existing_question_progress))
        question_progress.update({
            "status": updated_question_stage["status"],
            "processed": updated_question_stage["answered"],
            "total": updated_question_stage["total"],
            "unit": "questions",
            "message": (
                "Ожидаются ответы инженера."
                if updated_question_stage["pending"]
                else "Все вопросы инженеру обработаны."
            ),
        })
        updated_question_stage["progress"] = question_progress
    publication_stages["review_questions"] = updated_question_stage
    publication_stages["review_application"] = {
        "status": "COMPLETED",
        "applied_decisions": len(application.get("applied_decision_ids") or []),
        **_artifact_state(application),
    }
    if sheet_projection is not None:
        publication_stages["sheet_scope"] = {
            **dict(publication_stages.get("sheet_scope") or {}),
            "status": "COMPLETED",
            "groups": len(sheet_projection.get("groups") or []),
            "input_signature": sheet_projection.get("effective_signature"),
            "automatic_input_signature": sheet_projection.get(
                "automatic_signature"
            ),
            "scope_applied": bool(sheet_projection.get("scope_applied")),
            "pipeline_rerun": bool(
                (application.get("diagnostics") or {}).get("pipeline_rerun")
            ),
            "generation_was_materialized": bool(
                (application.get("diagnostics") or {}).get(
                    "generation_was_materialized"
                )
            ),
            "this_update_reran": bool(
                (application.get("diagnostics") or {}).get(
                    "this_update_reran"
                )
            ),
            "effective_page_groups": copy.deepcopy(
                sheet_projection.get("groups") or []
            ),
            "page_action_state": str(
                sheet_projection.get("action_state") or "NONE"
            ),
            "page_action_outcomes": copy.deepcopy(
                (application.get("diagnostics") or {}).get(
                    "page_action_outcomes"
                ) or []
            ),
        }
    publication_state = {
        **updating_state,
        "status": previous_status,
        "progress": 100,
        "stages": publication_stages,
    }
    if synthesis_changed:
        decisions = _refresh_decisions(session_id, pair_id, synthesis)
        report = _persist_latest_final_report(
            session_id, pair_id, synthesis, decisions
        )
        state = _publish_derived_state(
            session_id,
            pair_id,
            publication_state,
            synthesis,
            decisions,
            report,
        )
    else:
        state = _write_state(session_id, pair_id, publication_state)
    public_state = get_production_state(session_id, pair_id)
    return {
        **updated_questions,
        "state": public_state,
        "revision": int(answer_artifact.get("revision") or 0),
        "stale": False,
        "application": application,
        "suggestion_actions": _suggestion_actions(
            sheet_suggestions, answer_artifact
        ),
        "suggestion_action_semantics": _suggestion_action_semantics(public_state),
    }


def get_preliminary_report(session_id: str, pair_id: str) -> dict[str, Any]:
    """Предварительный отчёт анализа — читаемая проекция найденного.

    В отличие от итогового, он доступен СРАЗУ после анализа и не ждёт решений
    инженера. Как и итоговый, пересобирается на чтении из текущего синтеза:
    сохранённая копия — кэш, а не источник истины.
    """
    state = get_production_state(session_id, pair_id)
    synthesis = _published_synthesis(session_id, pair_id, state)
    if synthesis is None:
        empty = build_preliminary_report(pair_id=pair_id, synthesis=None)
        return {
            **empty,
            "stale": state["stale"],
            "available": False,
            "run_status": state.get("status"),
        }
    table_changes = production_store.load_artifact(
        session_id, pair_id, "electrical_table_changes"
    )
    source_snapshot = _load_published_source_snapshot(
        session_id, pair_id, state
    )
    materialization = production_store.load_artifact(
        session_id, pair_id, "ai_v2_materialization"
    )
    plan = production_store.load_artifact(
        session_id, pair_id, "human_review_plan"
    )
    materialized_ledger = None
    current_plan = None
    if isinstance(plan, Mapping) and (
        plan.get("generation_run_id") == state.get("run_id")
        and plan.get("generation_input_signature") == state.get("input_signature")
    ):
        current_plan = plan
        materialized_plan = (
            materialization.get("human_review_plan")
            if isinstance(materialization, Mapping) else None
        )
        if isinstance(materialized_plan, Mapping) and (
            materialized_plan.get("generation_run_id") == state.get("run_id")
            and materialized_plan.get("generation_input_signature")
            == state.get("input_signature")
            and materialized_plan.get("input_signature")
            == current_plan.get("input_signature")
        ):
            candidate = materialization.get("materialized_graphic_ledger")
            if isinstance(candidate, Mapping):
                materialized_ledger = candidate
    report = build_preliminary_report(
        pair_id=pair_id,
        synthesis=synthesis,
        document_inconsistencies=production_store.load_artifact(
            session_id, pair_id, "document_inconsistencies"
        ),
        electrical_table_changes=table_changes,
        ai_table_identity=production_store.load_artifact(
            session_id, pair_id, "ai_table_identity"
        ),
        human_review_plan=current_plan,
        evidence_availability={
            **_preliminary_evidence_availability(
                synthesis,
                source_snapshot,
                table_changes,
                materialized_graphic_ledger=materialized_ledger,
            ),
            **_human_review_evidence_availability(
                current_plan,
                table_changes=table_changes,
                load_tables=_load_load_tables(session_id, pair_id),
                inconsistencies=production_store.load_artifact(
                    session_id, pair_id, "document_inconsistencies"
                ),
            ),
        },
        generated_at=utc_now(),
    )
    return {
        **report,
        "stale": state["stale"],
        "available": True,
        "run_status": state.get("status"),
    }


def get_human_review(session_id: str, pair_id: str) -> dict[str, Any]:
    """Deterministic clarification read model; final approval stays Stage 7."""
    state = get_production_state(session_id, pair_id)
    ai_stage = ((state.get("stages") or {}).get("ai_resolution") or {})
    base = {
        "kind": "stage_comparison_human_review_view",
        "schema_version": "human-review-view.v1",
        "available": False,
        "stale": bool(state.get("stale")),
        "run_status": state.get("status"),
        "summary": {
            "interactions_total": 0,
            "interactions_answered": 0,
            "interactions_pending": 0,
        },
        "review_groups": [],
        "standalone_questions": [],
        "closed_questions": [],
        "metadata_changes": [],
        "text_requirement_changes": [],
        "missing_evidence": [],
        "document_inconsistencies": [],
        "atomic_targets": [],
        "input_signature": None,
        "revision": 0,
        "failure": (
            {
                "message": ai_stage.get("fallback_message"),
                "fast_results_preserved": True,
            }
            if ai_stage.get("fallback_used") else None
        ),
    }
    plan = production_store.load_artifact(
        session_id, pair_id, "human_review_plan"
    )
    if not isinstance(plan, Mapping) or (
        plan.get("generation_run_id") != state.get("run_id")
        or plan.get("generation_input_signature") != state.get("input_signature")
    ):
        return base
    synthesis = _published_synthesis(session_id, pair_id, state)
    if synthesis is None:
        return base
    engineer_decisions = _published_decisions(
        session_id, pair_id, state, synthesis
    )
    human_decisions = production_store.load_artifact(
        session_id, pair_id, "human_review_decisions"
    )
    view = build_human_review_view(
        plan,
        synthesis=synthesis,
        engineer_decisions=engineer_decisions,
        human_decisions=human_decisions,
        document_inconsistencies=production_store.load_artifact(
            session_id, pair_id, "document_inconsistencies"
        ),
    )
    return {
        **view,
        "available": not bool(state.get("stale")),
        "stale": bool(state.get("stale")),
        "run_status": state.get("status"),
        "failure": None,
    }


def update_human_review_answers(
    session_id: str,
    pair_id: str,
    *,
    updates: Iterable[Mapping[str, Any]],
    author: str,
    expected_input_signature: str,
    expected_revision: int,
) -> dict[str, Any]:
    """Persist group/standalone clarifications with optimistic concurrency."""
    updates = list(updates)
    with production_store.production_pair_lock(session_id, pair_id):
        state = get_production_state(session_id, pair_id)
        if state.get("stale"):
            raise ProductionStateConflictError("production result is stale")
        plan = production_store.load_artifact(
            session_id, pair_id, "human_review_plan"
        )
        if not isinstance(plan, Mapping) or (
            plan.get("generation_run_id") != state.get("run_id")
            or plan.get("generation_input_signature") != state.get("input_signature")
        ):
            raise ProductionStateConflictError("human review plan is not current")
        if plan.get("input_signature") != expected_input_signature:
            raise ProductionStateConflictError("human review input changed")

        closed_by_id = {
            str(value.get("question_id") or ""): value
            for value in plan.get("ai_closed_questions") or ()
            if isinstance(value, Mapping)
        }
        reopen_updates = [
            value for value in updates
            if str(value.get("interaction_id") or "") in closed_by_id
            and str((value.get("answer") or {}).get("answer_id") or "")
            == "REOPEN_FOR_HUMAN"
        ]
        if reopen_updates and len(reopen_updates) != len(updates):
            raise ValueError(
                "reopen must be submitted separately from clarification answers"
            )
        if reopen_updates:
            current = production_store.load_artifact(
                session_id, pair_id, "human_review_decisions"
            ) or empty_human_review_decisions(plan)
            if int(current.get("revision") or 0) != int(expected_revision):
                raise production_store.ProductionConflictError(
                    "human review revision changed"
                )
            reopened_ids = {
                str(value.get("interaction_id") or "")
                for value in reopen_updates
            }
            reopened = [closed_by_id[value] for value in sorted(reopened_ids)]
            updated_plan = copy.deepcopy(dict(plan))
            updated_plan["ai_closed_questions"] = [
                copy.deepcopy(value)
                for value in plan.get("ai_closed_questions") or ()
                if str((value or {}).get("question_id") or "") not in reopened_ids
            ]
            standalone = list(updated_plan.get("standalone_questions") or ())
            for value in sorted(
                reopened, key=lambda item: int(item.get("original_position") or 0)
            ):
                question = copy.deepcopy(dict(value))
                position = int(question.get("original_position") or 0)
                for key in (
                    "closure", "closed_at", "original_position", "status",
                    "can_reopen", "history_message",
                ):
                    question.pop(key, None)
                standalone.insert(max(0, min(position, len(standalone))), question)
            updated_plan["standalone_questions"] = standalone
            history = list(updated_plan.get("ai_question_closure_history") or ())
            now = utc_now()
            for value in reopened:
                history.append({
                    "question_id": value.get("question_id"),
                    "action": "REOPENED_BY_HUMAN",
                    "author": author,
                    "created_at": now,
                    "previous_closure": copy.deepcopy(value.get("closure") or {}),
                })
            updated_plan["ai_question_closure_history"] = history
            summary = dict(updated_plan.get("summary") or {})
            summary["standalone_human_questions"] = len(standalone)
            summary["mandatory_human_interactions"] = (
                len(updated_plan.get("groups") or ()) + len(standalone)
            )
            summary["ai_question_closure_closed"] = len(
                updated_plan.get("ai_closed_questions") or ()
            )
            updated_plan["summary"] = summary
            overrides = list(current.get("closure_overrides") or ())
            overrides.extend({
                "question_id": value.get("question_id"),
                "action": "REOPEN_FOR_HUMAN",
                "author": author,
                "created_at": now,
            } for value in reopened)
            saved = {
                **dict(current),
                "generated_at": now,
                "revision": int(current.get("revision") or 0) + 1,
                "closure_overrides": overrides,
            }
            production_store.save_artifact(
                session_id, pair_id, "human_review_plan", updated_plan
            )
            production_store.save_artifact(
                session_id, pair_id, "human_review_decisions", saved
            )
            production_store.mutate_artifact(
                session_id,
                pair_id,
                "ai_question_closure",
                lambda existing: {
                    **dict(existing or {}),
                    "human_overrides": [
                        *list((existing or {}).get("human_overrides") or ()),
                        *overrides[-len(reopened):],
                    ],
                },
                default={},
            )
            plan = updated_plan
            updates = []

        def mutate(existing: Any) -> dict[str, Any]:
            current = (
                dict(existing) if isinstance(existing, Mapping)
                else empty_human_review_decisions(plan)
            )
            if int(current.get("revision") or 0) != int(expected_revision):
                raise production_store.ProductionConflictError(
                    "human review revision changed"
                )
            return apply_human_review_decision_updates(
                plan,
                current,
                updates=updates,
                author=author,
            )

        if updates:
            saved = production_store.mutate_artifact(
                session_id,
                pair_id,
                "human_review_decisions",
                mutate,
                default=empty_human_review_decisions(plan),
            )
        elif not reopen_updates:
            saved = production_store.load_artifact(
                session_id, pair_id, "human_review_decisions"
            ) or empty_human_review_decisions(plan)
        total = int((plan.get("summary") or {}).get(
            "mandatory_human_interactions"
        ) or 0)
        answered = len(saved.get("group_decisions") or ()) + len(
            saved.get("standalone_answers") or ()
        )

        def update_state(existing: Any) -> dict[str, Any]:
            current = dict(existing) if isinstance(existing, Mapping) else {}
            stages = copy.deepcopy(current.get("stages") or {})
            stages["human_review"] = {
                **dict(stages.get("human_review") or {}),
                "status": "COMPLETED" if answered >= total else "NEEDS_REVIEW",
                "total": total,
                "answered": answered,
                "pending": max(0, total - answered),
                "clarification_is_not_final_approval": True,
            }
            if reopen_updates:
                stages["question_closure"] = {
                    **dict(stages.get("question_closure") or {}),
                    "hro_after": total,
                    "closed": len(plan.get("ai_closed_questions") or ()),
                    "human_override_applied": True,
                    "engineer_approvals_untouched": True,
                }
            return {**current, "stages": stages, "updated_at": utc_now()}

        production_store.mutate_artifact(
            session_id, pair_id, "state", update_state, default={}
        )
    return get_human_review(session_id, pair_id)


def get_final_report(session_id: str, pair_id: str) -> dict[str, Any]:
    state = get_production_state(session_id, pair_id)
    synthesis = _published_synthesis(session_id, pair_id, state)
    if synthesis is None:
        return {
            "kind": "stage_comparison_approved_changes_report",
            "schema_version": "approved-changes-report.v1",
            "version": 1,
            "direction": "LEFT_TO_RIGHT",
            "input_signature": None,
            "approved_atomic_changes": [],
            "summary": {"approved": 0},
            "stale": state["stale"],
            "available": False,
            "run_status": state.get("status"),
        }
    decisions = _published_decisions(
        session_id, pair_id, state, synthesis
    )
    # Final is a read-only projection of the current locked decisions.  The
    # persisted copy is a cache/audit artifact only, so a crash between the
    # decision write and cache refresh can never expose a rejected finding.
    report = build_final_report(synthesis, decisions, object_ref=None)
    return {
        **report,
        "stale": state["stale"],
        "available": True,
        "run_status": state.get("status"),
    }


def _current_v2_artifacts(
    session_id: str, pair_id: str, state: Mapping[str, Any]
) -> tuple[Mapping[str, Any] | None, Mapping[str, Any] | None]:
    plan = production_store.load_artifact(session_id, pair_id, "human_review_plan")
    materialization = production_store.load_artifact(
        session_id, pair_id, "ai_v2_materialization"
    )
    if not isinstance(plan, Mapping):
        return None, None
    if (
        plan.get("generation_run_id") != state.get("run_id")
        or plan.get("generation_input_signature") != state.get("input_signature")
    ):
        return None, None
    if not isinstance(materialization, Mapping):
        return plan, None
    materialized_plan = materialization.get("human_review_plan")
    if not isinstance(materialized_plan, Mapping) or (
        materialized_plan.get("generation_run_id") != state.get("run_id")
        or materialized_plan.get("generation_input_signature")
        != state.get("input_signature")
        or materialized_plan.get("input_signature") != plan.get("input_signature")
    ):
        return plan, None
    return plan, materialization


def _selected_default_pages(mode2: Mapping[str, Any] | None) -> dict[str, int]:
    pages: dict[str, int] = {}
    for side in ("LEFT", "RIGHT"):
        source = ((mode2 or {}).get("sources") or {}).get(side)
        page_index = source.get("page_index_0based") if isinstance(source, Mapping) else None
        if isinstance(page_index, int):
            pages[side] = page_index + 1
    return pages


def _inline_human_review_evidence(
    target_id: str,
    *,
    plan: Mapping[str, Any] | None,
    table_changes: Mapping[str, Any] | None,
    load_tables: Mapping[str, Any] | None,
    inconsistencies: Mapping[str, Any] | None,
) -> tuple[dict[str, list[dict[str, Any]]], str] | None:
    """Resolve report-only targets without pretending they are Stage-7 atoms."""
    evidence: dict[str, list[dict[str, Any]]] = {"LEFT": [], "RIGHT": []}
    source_mode = "HUMAN_REVIEW"
    if isinstance(plan, Mapping):
        for group in plan.get("groups") or ():
            if not isinstance(group, Mapping):
                continue
            wanted = target_id == str(group.get("group_id") or "")
            for value in group.get("evidence_refs") or ():
                if not isinstance(value, Mapping):
                    continue
                if not wanted and target_id != str(value.get("target_id") or ""):
                    continue
                for side in evidence:
                    for record in (value.get("evidence") or {}).get(side) or ():
                        if isinstance(record, Mapping):
                            evidence[side].append(dict(record))
            if any(evidence.values()):
                return evidence, (
                    "HUMAN_REVIEW_MODE_GROUP" if wanted
                    else "HUMAN_REVIEW_MODE_ATOM"
                )
        for question in [
            *list(plan.get("standalone_questions") or ()),
            *list(plan.get("ai_closed_questions") or ()),
        ]:
            if not isinstance(question, Mapping) or target_id != str(
                question.get("question_id") or ""
            ):
                continue
            closure_evidence = (question.get("closure") or {}).get("evidence")
            if isinstance(closure_evidence, Mapping):
                for side in evidence:
                    evidence[side].extend(
                        copy.deepcopy(value)
                        for value in closure_evidence.get(side) or ()
                        if isinstance(value, Mapping)
                    )
                if any(evidence.values()):
                    return evidence, "AI_QUESTION_CLOSURE"
            select = next((
                value for value in question.get("allowed_answers") or ()
                if isinstance(value, Mapping)
                and value.get("answer_id") == "SELECT_ROW_PAIR"
            ), None)
            if isinstance(select, Mapping):
                row_ids = {
                    "LEFT": set(select.get("left_row_ids") or ()),
                    "RIGHT": set(select.get("right_row_ids") or ()),
                }
                for side in evidence:
                    table = (load_tables or {}).get(side)
                    table_page = table.get("page_index") if isinstance(table, Mapping) else None
                    for row in (table or {}).get("rows") or ():
                        if not isinstance(row, Mapping) or row.get("row_id") not in row_ids[side]:
                            continue
                        located = dict(row)
                        located.pop("page", None)
                        located["page_index"] = table_page if isinstance(table_page, int) else 0
                        evidence[side].append(located)
                if any(evidence.values()):
                    return evidence, "HUMAN_REVIEW_TABLE_ROW_CHOICE"

        # Informational missing-evidence groups keep their own public identity.
        missing = next((
            value for value in plan.get("missing_evidence") or ()
            if isinstance(value, Mapping)
            and target_id == str(value.get("target_id") or "")
        ), None)
        if isinstance(missing, Mapping):
            affected = set(str(value) for value in missing.get("affected_target_ids") or ())
            for record in (table_changes or {}).get("blocked") or ():
                if not isinstance(record, Mapping) or blocked_target_id(record) not in affected:
                    continue
                for side in evidence:
                    value = (record.get("evidence") or {}).get(side)
                    if isinstance(value, Mapping):
                        evidence[side].append(dict(value))
            for record in (table_changes or {}).get("unproven") or ():
                if not isinstance(record, Mapping) or unproven_target_id(record) not in affected:
                    continue
                side = str(record.get("side") or "")
                table = (load_tables or {}).get(side)
                table_page = table.get("page_index") if isinstance(table, Mapping) else None
                for row in (table or {}).get("rows") or ():
                    if isinstance(row, Mapping) and row.get("row_id") == record.get("row_id"):
                        located = dict(row)
                        located.pop("page", None)
                        located["page_index"] = table_page if isinstance(table_page, int) else 0
                        evidence[side].append(located)
            if any(evidence.values()):
                return evidence, "MISSING_EVIDENCE_SOURCE"

        # Text requirements and metadata are report-only classifications, but
        # their source fragments are still first-class viewer evidence.  Use
        # the plan's exact source span so navigation keeps the public raw text
        # and the normalized OCR boxes instead of returning a bare atom ref.
        for collection, inline_mode in (
            ("text_requirement_changes", "TEXT_REQUIREMENT_SOURCE"),
            ("metadata_changes", "DOCUMENT_METADATA_SOURCE"),
        ):
            informational = next((
                value for value in plan.get(collection) or ()
                if isinstance(value, Mapping)
                and target_id == str(value.get("target_id") or "")
            ), None)
            if not isinstance(informational, Mapping):
                continue
            source = informational.get("source_evidence")
            if not isinstance(source, Mapping):
                continue
            side = str(source.get("side") or "").upper()
            if side not in evidence:
                continue
            raw_text = source.get("raw_text")
            source_region = informational.get("source_region")
            block_ids = (
                source_region.get("source_block_ids") or ()
                if isinstance(source_region, Mapping) else ()
            )
            block_id = next((str(value) for value in block_ids if value), None)
            records: list[dict[str, Any]] = []
            for span in source.get("text_spans") or ():
                if not isinstance(span, Mapping):
                    continue
                base = {
                    "source": "TEXT",
                    "page": span.get("page") or source.get("page"),
                    "fragment_id": span.get("fragment_id"),
                    "block_id": block_id,
                    "raw_text": raw_text,
                    "bounded_absence": informational.get("bounded_absence"),
                }
                boxes = [
                    value for value in span.get("bboxes") or ()
                    if isinstance(value, Mapping)
                ]
                if not boxes:
                    records.append(base)
                    continue
                for box in boxes:
                    x = box.get("x")
                    y = box.get("y")
                    width = box.get("width")
                    height = box.get("height")
                    if not all(isinstance(value, (int, float)) for value in (
                        x, y, width, height,
                    )):
                        continue
                    records.append({
                        **base,
                        "bbox": [x, y, x + width, y + height],
                        "coordinate_space": "NORMALIZED_PAGE_TOP_LEFT",
                    })
            if not records:
                records.append({
                    "source": "TEXT",
                    "page": source.get("page"),
                    "block_id": block_id,
                    "raw_text": raw_text,
                    "bounded_absence": informational.get("bounded_absence"),
                })
            evidence[side].extend(records)
            return evidence, inline_mode

    inconsistency = next((
        value for value in (inconsistencies or {}).get("items") or ()
        if isinstance(value, Mapping)
        and target_id == str(value.get("inconsistency_id") or value.get("row_id") or "")
    ), None)
    if isinstance(inconsistency, Mapping):
        side = str(inconsistency.get("side") or "RIGHT")
        record = dict(inconsistency.get("evidence") or {})
        record["block_id"] = inconsistency.get("block_id")
        record["row_id"] = inconsistency.get("row_id")
        evidence.setdefault(side, []).append(record)
        source_mode = "DOCUMENT_INCONSISTENCY"
    return (evidence, source_mode) if any(evidence.values()) else None


def get_change_evidence(
    session_id: str,
    pair_id: str,
    target_id: str,
) -> dict[str, Any]:
    """Build a safe viewer payload from stored artifacts only."""
    state = get_production_state(session_id, pair_id)
    synthesis = _published_synthesis(
        session_id, pair_id, state, for_write=True
    )
    assert synthesis is not None
    source_snapshot = _load_published_source_snapshot(
        session_id, pair_id, state
    )
    text_atoms = source_snapshot["text"]["artifact"]
    ledger = source_snapshot["graphic"]["ledger"]
    plan, materialization = _current_v2_artifacts(
        session_id, pair_id, state
    )
    if isinstance(materialization, Mapping) and isinstance(
        materialization.get("materialized_graphic_ledger"), Mapping
    ):
        ledger = materialization["materialized_graphic_ledger"]
    table_changes = production_store.load_artifact(
        session_id, pair_id, "electrical_table_changes"
    )
    load_tables = _load_load_tables(session_id, pair_id)
    inconsistencies = production_store.load_artifact(
        session_id, pair_id, "document_inconsistencies"
    )
    mode2 = production_store.load_artifact(
        session_id, pair_id, "direct_page_mode2"
    )
    documents = {
        "LEFT": {"document_ref": "LEFT"},
        "RIGHT": {"document_ref": "RIGHT"},
    }
    aliases = [target_id]
    if isinstance(plan, Mapping):
        question = next((
            value for value in [
                *list(plan.get("standalone_questions") or ()),
                *list(plan.get("ai_closed_questions") or ()),
            ]
            if isinstance(value, Mapping)
            and target_id == str(value.get("question_id") or "")
        ), None)
        if isinstance(question, Mapping):
            aliases.extend(str(value) for value in question.get("affected_target_ids") or ())

    def resolve(page_sizes: Mapping[str, Any] | None = None) -> dict[str, Any]:
        inline = _inline_human_review_evidence(
            target_id,
            plan=plan,
            table_changes=table_changes,
            load_tables=load_tables,
            inconsistencies=inconsistencies,
        )
        if inline is not None:
            evidence, source_mode = inline
            return build_inline_evidence_navigation(
                target_id,
                evidence=evidence,
                documents=documents,
                page_sizes=page_sizes,
                default_pages=_selected_default_pages(mode2),
                source_mode=source_mode,
            )
        for alias in aliases:
            try:
                payload = build_evidence_navigation(
                    alias,
                    synthesis=synthesis,
                    text_atoms=text_atoms,
                    graphic_ledger=ledger,
                    electrical_table_changes=table_changes,
                    documents=documents,
                    page_sizes=page_sizes,
                )
                payload["target_id"] = target_id
                return payload
            except KeyError:
                pass
        raise KeyError("evidence target not found")

    initial = resolve()
    page_sizes: dict[str, dict[int, dict[str, float]]] = {"LEFT": {}, "RIGHT": {}}
    for public_side, store_side in (("LEFT", "left"), ("RIGHT", "right")):
        pages = sorted({
            int(location["page"])
            for location in initial["sides"][public_side]
            if isinstance(location.get("page"), int)
        })
        for page in pages:
            info = store.page_info_payload(
                session_id, pair_id, store_side, page
            )
            page_sizes[public_side][page] = {
                "width": float(info["width"]),
                "height": float(info["height"]),
            }
    payload = resolve(page_sizes)
    for side, locations in payload["sides"].items():
        for location in locations:
            page = location.get("page")
            if location.get("page_size") is None and page in page_sizes[side]:
                location["page_size"] = copy.deepcopy(page_sizes[side][page])
    payload["input_signature"] = content_signature({
        "target_id": target_id,
        "synthesis": canonical_synthesis_digest(synthesis),
        "sides": payload["sides"],
        "trace": payload["trace"],
    })
    return payload


__all__ = [
    "ANALYSIS_CONFIG_KEYS",
    "ANSWERS_KIND",
    "ANSWERS_SCHEMA_VERSION",
    "CHANGES_KIND",
    "CHANGES_SCHEMA_VERSION",
    "ProductionStateConflictError",
    "SOURCE_REQUEST_KEYS",
    "STATE_KIND",
    "STATE_SCHEMA_VERSION",
    "analysis_config",
    "analysis_config_signature",
    "get_change_evidence",
    "get_final_report",
    "get_human_review",
    "get_production_changes",
    "get_production_state",
    "get_production_text_evidence",
    "get_review_questions",
    "normalize_run_request",
    "restore_selection",
    "run_production_comparison",
    "source_request",
    "update_engineer_decisions",
    "update_human_review_answers",
    "update_review_answers",
]
