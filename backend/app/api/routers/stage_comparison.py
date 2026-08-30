"""API for source upload, sheet matching and the tiled PDF viewer."""
from __future__ import annotations

import json
import logging
from typing import Any, Literal

from fastapi import APIRouter, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import Response
from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.app.services.stage_comparison import objects as objects_mod
from backend.app.services.stage_comparison import stage_upload as stage_upload_mod
from backend.app.services.stage_comparison import store
from backend.app.services.stage_comparison import production_orchestrator as production
from backend.app.services.stage_comparison import production_store
from backend.app.core import portal_auth
from backend.app.services.common import user_service


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/stage-comparison", tags=["stage-comparison"])


class CreateSessionRequest(BaseModel):
    stage_a_path: str = Field(min_length=1)
    stage_b_path: str = Field(min_length=1)


class CreatePairRequest(BaseModel):
    left_pdf: str = Field(min_length=1)
    right_pdf: str = Field(min_length=1)


class ConfirmedDocumentPairRequest(BaseModel):
    left_pdf: str = Field(min_length=1)
    right_pdf: str = Field(min_length=1)


class SaveDocumentPairingRequest(BaseModel):
    left_order: list[str | None]
    right_order: list[str | None]
    confirmed_pairs: list[ConfirmedDocumentPairRequest] = Field(default_factory=list)


class SheetLinkRequest(BaseModel):
    id: str | None = None
    left_pages: list[int]
    right_pages: list[int]
    source: str = "manual"
    confidence: str = "manual"
    reason: list[str] = Field(default_factory=list)


class SaveSheetLinksRequest(BaseModel):
    links: list[SheetLinkRequest] = Field(default_factory=list)
    unlinked_left_pages: list[int] = Field(default_factory=list)


class GraphicComparisonRequest(BaseModel):
    """References to upstream-prepared blocks; no inline bbox override exists."""

    model_config = ConfigDict(extra="forbid")

    left_block_ids: list[str] = Field(default_factory=list)
    right_block_ids: list[str] = Field(default_factory=list)


class ProductionRunRequest(BaseModel):
    """Client-controlled IDs only; source paths and geometry stay server-side."""

    model_config = ConfigDict(extra="forbid")

    input_mode: Literal["PAGE", "DOCUMENT"]
    left_pages: list[int] = Field(default_factory=list)
    right_pages: list[int] = Field(default_factory=list)
    left_block_ids: list[str] = Field(default_factory=list)
    right_block_ids: list[str] = Field(default_factory=list)
    #: Глубина анализа этого прогона. Пожелание клиента; что из него
    #: действительно разрешено, решает сервер. Без значения действует
    #: настройка установки, чтобы поведение не менялось молча.
    ai_mode: Literal["FAST", "STANDARD", "DEEP"] | None = None


class ProductionDecisionUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_id: str = Field(min_length=1)
    decision: Literal["PENDING_REVIEW", "APPROVED", "REJECTED"]
    # Accepted for old/new UI compatibility but always replaced by the
    # authenticated server identity before persistence.
    author: str | None = None
    comment: str | None = None
    reason_code: str | None = None


class SaveProductionDecisionsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    updates: list[ProductionDecisionUpdate] = Field(default_factory=list)
    expected_input_signature: str = Field(min_length=1)
    expected_revision: int = Field(ge=0)


class ProductionExplicitCandidate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    right_entity_ref: str | None = Field(default=None, min_length=1)
    project_entity_ref: str | None = Field(default=None, min_length=1)
    left_pages: list[int] = Field(default_factory=list)
    right_pages: list[int] = Field(default_factory=list)
    relation_type: Literal["MATCHED", "SPLIT", "MERGED"] | None = None


class ProductionTypedResolution(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dimension: Literal[
        "PRINCIPLE",
        "METHOD",
        "OPERATION",
        "STRUCTURE",
        "CONNECTION",
        "TYPE",
        "PARAMETER",
        "QUANTITY",
        "SPACE",
    ] | None = None
    subject_ref: str | None = Field(default=None, min_length=1)
    project_entity_ref: str | None = Field(default=None, min_length=1)
    facet_ref: str | None = Field(default=None, min_length=1)
    direction: Literal[
        "ADDED",
        "REMOVED",
        "REPLACED",
        "INCREASED",
        "DECREASED",
        "ALTERED",
    ] | None = None
    outcome: Literal["MATERIAL_CHANGE", "DETAIL_ONLY"] | None = None
    before_value: Any = None
    after_value: Any = None
    # ``None`` is intentional: an omitted contested selection must stay
    # omitted instead of being synthesized as an empty (and misleading)
    # typed resolution by Pydantic.
    selected_change_ids: list[str] | None = None

    @model_validator(mode="after")
    def reject_semantically_empty_resolution(self):
        values = self.model_dump(exclude_none=True)
        if not any(
            value not in ([], {})
            and (not isinstance(value, str) or value.strip())
            for value in values.values()
        ):
            raise ValueError("typed_resolution must not be semantically empty")
        return self


class ProductionReviewAnswer(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question_id: str = Field(min_length=1)
    answer: str = Field(min_length=1)
    # See ProductionDecisionUpdate.author.
    author: str | None = None
    comment: str | None = None
    selected_refs: list[str] = Field(default_factory=list)
    explicit_candidate: ProductionExplicitCandidate | None = None
    typed_resolution: ProductionTypedResolution | None = None


class SaveProductionAnswersRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    answers: list[ProductionReviewAnswer] = Field(default_factory=list)
    expected_input_signature: str = Field(min_length=1)
    expected_revision: int = Field(ge=0)


def _engineer_author(request: Request) -> str:
    """Best-effort server identity; a request body can never choose author."""
    settings = portal_auth.get_settings()
    username = portal_auth.request_username(request, settings) if settings.enabled else None
    matched = user_service.get_user_by_login(username) if username else None
    if matched:
        return str(matched.get("id") or matched.get("login") or username)
    if username:
        return username
    current = user_service.get_current_user()
    if current:
        return str(current.get("id") or current.get("login") or "local-engineer")
    return "local-engineer"


@router.get("/objects")
async def list_comparison_objects():
    return objects_mod.list_objects()


@router.post("/objects/{object_id}/stages/{stage_name}/upload")
async def upload_stage_archive(object_id: str, stage_name: str, file: UploadFile = File(...)):
    if stage_name not in stage_upload_mod.VALID_STAGES:
        raise HTTPException(400, "Разрешены только stage_1 и stage_2")
    try:
        return await run_in_threadpool(
            stage_upload_mod.replace_stage_from_zip, object_id, stage_name, file.file, file.filename,
        )
    except stage_upload_mod.StageUploadError as exc:
        raise HTTPException(400, str(exc)) from exc
    except OSError as exc:
        logger.exception("stage archive upload failed: %s/%s", object_id, stage_name)
        raise HTTPException(500, f"Не удалось сохранить архив стадии: {exc}") from exc


@router.post("/objects/{object_id}/stages/{stage_name}/upload-folder")
async def upload_stage_folder(
    object_id: str,
    stage_name: str,
    files: list[UploadFile] = File(...),
    relative_paths: str = Form("[]"),
    folder_name: str = Form(""),
    retain_backup: bool = Form(True),
):
    if stage_name not in stage_upload_mod.VALID_STAGES:
        raise HTTPException(400, "Разрешены только stage_1 и stage_2")
    try:
        paths = json.loads(relative_paths or "[]")
    except json.JSONDecodeError as exc:
        raise HTTPException(422, "Некорректный список путей файлов") from exc
    if not files or not isinstance(paths, list) or len(paths) != len(files):
        raise HTTPException(422, "Количество файлов и относительных путей не совпадает")
    uploads = [(upload.file, str(paths[index] or upload.filename or "")) for index, upload in enumerate(files)]
    try:
        return await run_in_threadpool(
            stage_upload_mod.replace_stage_from_folder,
            object_id,
            stage_name,
            uploads,
            folder_name,
            retain_backup,
        )
    except stage_upload_mod.StageUploadError as exc:
        raise HTTPException(400, str(exc)) from exc
    except OSError as exc:
        logger.exception("stage folder upload failed: %s/%s", object_id, stage_name)
        raise HTTPException(500, f"Не удалось сохранить папку стадии: {exc}") from exc


@router.post("/sessions")
async def create_session(request: CreateSessionRequest):
    try:
        store.assert_path_in_allowlist(request.stage_a_path)
        store.assert_path_in_allowlist(request.stage_b_path)
        session, warnings = await run_in_threadpool(
            store.create_session, request.stage_a_path, request.stage_b_path,
        )
    except PermissionError as exc:
        raise HTTPException(403, str(exc)) from exc
    except OSError as exc:
        raise HTTPException(400, f"Ошибка доступа к папкам: {exc}") from exc
    return {**session, "session_id": session["id"], "warnings": warnings}


@router.get("/sessions")
async def list_sessions():
    return {"sessions": await run_in_threadpool(store.list_sessions)}


@router.get("/sessions/{session_id}")
async def get_session(session_id: str):
    session = await run_in_threadpool(store.get_session, session_id)
    if session is None:
        raise HTTPException(404, "Сессия не найдена")
    return session


@router.post("/sessions/{session_id}/pairs")
async def create_pair(session_id: str, request: CreatePairRequest):
    try:
        return await run_in_threadpool(
            store.create_pair, session_id, request.left_pdf, request.right_pdf,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.put("/sessions/{session_id}/document-pairing")
async def save_document_pairing(session_id: str, request: SaveDocumentPairingRequest):
    try:
        return await run_in_threadpool(
            store.save_document_pairing,
            session_id,
            request.left_order,
            request.right_order,
            [pair.model_dump() for pair in request.confirmed_pairs],
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.post("/sessions/{session_id}/document-pairing/suggest")
async def suggest_document_pairing(session_id: str):
    try:
        return await run_in_threadpool(store.suggest_document_pairing, session_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/sessions/{session_id}/pairs/{pair_id}")
async def get_pair(session_id: str, pair_id: str):
    pair = await run_in_threadpool(store.get_pair_view, session_id, pair_id)
    if pair is None:
        raise HTTPException(404, "Пара не найдена")
    return pair


# Additive NEW FLOW.  Legacy Stage 5/5.3 endpoints below remain unchanged.
@router.post("/sessions/{session_id}/pairs/{pair_id}/production/run")
async def run_production_comparison(
    session_id: str,
    pair_id: str,
    request: ProductionRunRequest,
):
    try:
        return await run_in_threadpool(
            production.run_production_comparison,
            session_id,
            pair_id,
            **request.model_dump(),
        )
    except production_store.ProductionConflictError as exc:
        raise HTTPException(409, str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except (FileNotFoundError, ValueError, OSError, UnicodeDecodeError) as exc:
        raise HTTPException(
            400,
            f"Не удалось запустить production-сравнение ({type(exc).__name__})",
        ) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("production stage comparison failed")
        raise HTTPException(500, "Ошибка production-сравнения") from exc


@router.get("/production/ai-modes")
async def get_production_ai_modes():
    """Какие режимы анализа разрешает эта установка и что выбрано по умолчанию."""
    settings = production.ai_settings
    return {
        "modes": [
            {"code": "FAST", "label": "Быстро"},
            {"code": "STANDARD", "label": "Стандартно"},
            {"code": "DEEP", "label": "Глубокая проверка"},
        ],
        "allowed": list(settings.allowed_run_modes()),
        "default": settings.run_mode_label(settings.mode()),
    }


@router.post("/sessions/{session_id}/pairs/{pair_id}/production/cancel")
async def cancel_production_comparison(
    session_id: str,
    pair_id: str,
    http_request: Request,
):
    """Остановить идущий анализ этой пары.

    Замок пары намеренно не берётся: он неблокирующий и занят самим прогоном,
    поэтому попытка его захватить вернула бы 409 вместо отмены.
    """
    try:
        return await run_in_threadpool(
            production.cancel_production_comparison,
            session_id,
            pair_id,
            requested_by=_engineer_author(http_request),
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("production stage comparison cancel failed")
        raise HTTPException(500, "Не удалось остановить анализ") from exc


@router.get("/sessions/{session_id}/pairs/{pair_id}/production/state")
async def get_production_state(session_id: str, pair_id: str):
    try:
        return await run_in_threadpool(
            production.get_production_state, session_id, pair_id
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get(
    "/sessions/{session_id}/pairs/{pair_id}/production/text-evidence"
)
async def get_production_text_evidence(session_id: str, pair_id: str):
    try:
        return await run_in_threadpool(
            production.get_production_text_evidence, session_id, pair_id
        )
    except production_store.ProductionConflictError as exc:
        raise HTTPException(409, str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(
            409, f"Некорректный production TEXT evidence: {exc}"
        ) from exc


@router.get("/sessions/{session_id}/pairs/{pair_id}/production/changes")
async def get_production_changes(session_id: str, pair_id: str):
    try:
        return await run_in_threadpool(
            production.get_production_changes, session_id, pair_id
        )
    except production_store.ProductionConflictError as exc:
        raise HTTPException(409, str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(409, f"Некорректный production-артефакт: {exc}") from exc


@router.put(
    "/sessions/{session_id}/pairs/{pair_id}/production/decisions"
)
async def save_production_decisions(
    http_request: Request,
    session_id: str,
    pair_id: str,
    request: SaveProductionDecisionsRequest,
):
    updates = [
        update.model_dump(exclude={"author"}) for update in request.updates
    ]
    try:
        return await run_in_threadpool(
            production.update_engineer_decisions,
            session_id,
            pair_id,
            updates=updates,
            author=_engineer_author(http_request),
            expected_input_signature=request.expected_input_signature,
            expected_revision=request.expected_revision,
        )
    except production_store.ProductionConflictError as exc:
        raise HTTPException(409, str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get(
    "/sessions/{session_id}/pairs/{pair_id}/production/questions"
)
async def get_production_questions(session_id: str, pair_id: str):
    try:
        return await run_in_threadpool(
            production.get_review_questions, session_id, pair_id
        )
    except production_store.ProductionConflictError as exc:
        raise HTTPException(409, str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.put(
    "/sessions/{session_id}/pairs/{pair_id}/production/answers"
)
async def save_production_answers(
    http_request: Request,
    session_id: str,
    pair_id: str,
    request: SaveProductionAnswersRequest,
):
    answers = [
        answer.model_dump(exclude={"author"}, exclude_none=True)
        for answer in request.answers
    ]
    try:
        return await run_in_threadpool(
            production.update_review_answers,
            session_id,
            pair_id,
            answers=answers,
            author=_engineer_author(http_request),
            expected_input_signature=request.expected_input_signature,
            expected_revision=request.expected_revision,
        )
    except production_store.ProductionConflictError as exc:
        raise HTTPException(409, str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/sessions/{session_id}/pairs/{pair_id}/production/preliminary-report")
async def get_production_preliminary_report(session_id: str, pair_id: str):
    """Предварительный отчёт: что найдено, до проверки инженером."""
    try:
        return await run_in_threadpool(
            production.get_preliminary_report, session_id, pair_id
        )
    except production_store.ProductionConflictError as exc:
        raise HTTPException(409, str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/sessions/{session_id}/pairs/{pair_id}/production/final-report")
async def get_production_final_report(session_id: str, pair_id: str):
    try:
        return await run_in_threadpool(
            production.get_final_report, session_id, pair_id
        )
    except production_store.ProductionConflictError as exc:
        raise HTTPException(409, str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get(
    "/sessions/{session_id}/pairs/{pair_id}/production/changes/{target_id}/evidence"
)
async def get_production_change_evidence(
    session_id: str,
    pair_id: str,
    target_id: str,
):
    try:
        return await run_in_threadpool(
            production.get_change_evidence, session_id, pair_id, target_id
        )
    except production_store.ProductionConflictError as exc:
        raise HTTPException(409, str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except (FileNotFoundError, ValueError, OSError) as exc:
        raise HTTPException(
            400, f"Не удалось открыть evidence ({type(exc).__name__})"
        ) from exc


@router.post("/sessions/{session_id}/pairs/{pair_id}/sheet-match-suggestions")
async def rebuild_sheet_match_suggestions(session_id: str, pair_id: str):
    try:
        return await run_in_threadpool(store.run_sheet_matching, session_id, pair_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(400, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except (OSError, UnicodeDecodeError) as exc:
        raise HTTPException(400, f"Не удалось прочитать HTML-оглавление: {exc}") from exc


@router.get("/sessions/{session_id}/pairs/{pair_id}/sheet-matches")
async def get_sheet_matches(session_id: str, pair_id: str):
    try:
        return await run_in_threadpool(store.get_sheet_matching_state, session_id, pair_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.put("/sessions/{session_id}/pairs/{pair_id}/sheet-links")
async def save_sheet_links(session_id: str, pair_id: str, request: SaveSheetLinksRequest):
    try:
        return await run_in_threadpool(
            store.save_sheet_links,
            session_id,
            pair_id,
            [link.model_dump() for link in request.links],
            request.unlinked_left_pages,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.get("/sessions/{session_id}/pairs/{pair_id}/sheet-link-repairs")
async def get_sheet_link_repairs(session_id: str, pair_id: str):
    try:
        return await run_in_threadpool(store.get_sheet_link_repairs_state, session_id, pair_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.post("/sessions/{session_id}/pairs/{pair_id}/sheet-link-repairs/{repair_id}/undo")
async def undo_sheet_link_repair(session_id: str, pair_id: str, repair_id: str):
    try:
        return await store.undo_sheet_link_repair(session_id, pair_id, repair_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except (FileNotFoundError, ValueError, OSError, UnicodeDecodeError) as exc:
        raise HTTPException(400, f"Не удалось отменить исправление связей: {exc}") from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("sheet-link repair undo failed")
        raise HTTPException(500, f"Ошибка отмены исправления связей: {exc}") from exc


@router.post("/sessions/{session_id}/pairs/{pair_id}/text-comparison")
async def rebuild_text_comparison(session_id: str, pair_id: str):
    try:
        return await run_in_threadpool(store.run_text_comparison, session_id, pair_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except (FileNotFoundError, ValueError, OSError, UnicodeDecodeError) as exc:
        raise HTTPException(400, f"Не удалось сравнить текст: {exc}") from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("deterministic text comparison failed")
        raise HTTPException(500, f"Ошибка сравнения текста: {exc}") from exc


@router.get("/sessions/{session_id}/pairs/{pair_id}/text-comparison")
async def get_text_comparison(session_id: str, pair_id: str):
    try:
        payload = await run_in_threadpool(
            store.get_text_comparison_state, session_id, pair_id
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    return payload or {"version": 1, "pair_id": pair_id, "status": "not_started"}


@router.get("/sessions/{session_id}/pairs/{pair_id}/text-exclusions")
async def get_text_exclusions(session_id: str, pair_id: str):
    try:
        payload = await run_in_threadpool(
            store.get_text_exclusions_state, session_id, pair_id
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    return payload or {"version": 1, "pair_id": pair_id, "status": "not_started"}


@router.post("/sessions/{session_id}/pairs/{pair_id}/graphic-comparison")
async def rebuild_graphic_comparison(
    session_id: str, pair_id: str, request: GraphicComparisonRequest,
):
    try:
        return await run_in_threadpool(
            store.run_graphic_comparison,
            session_id,
            pair_id,
            request.left_block_ids,
            request.right_block_ids,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except (FileNotFoundError, ValueError, OSError, UnicodeDecodeError) as exc:
        raise HTTPException(400, f"Не удалось сравнить графические блоки: {exc}") from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("production graphic comparison failed")
        raise HTTPException(500, f"Ошибка сравнения графики: {exc}") from exc


@router.get("/sessions/{session_id}/pairs/{pair_id}/graphic-comparison")
async def get_graphic_comparison(session_id: str, pair_id: str):
    try:
        payload = await run_in_threadpool(
            store.get_graphic_change_ledger_state, session_id, pair_id,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    return payload or {
        "schema_version": "graphic-change-ledger.v1",
        "status": "not_started",
        "pair_id": pair_id,
    }


@router.post("/sessions/{session_id}/pairs/{pair_id}/text-differences")
async def rebuild_text_differences(session_id: str, pair_id: str):
    try:
        return await run_in_threadpool(
            store.run_text_differences, session_id, pair_id
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except (FileNotFoundError, ValueError, OSError, UnicodeDecodeError) as exc:
        raise HTTPException(400, f"Не удалось определить расхождения текста: {exc}") from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("deterministic text differences failed")
        raise HTTPException(500, f"Ошибка анализа расхождений текста: {exc}") from exc


@router.get("/sessions/{session_id}/pairs/{pair_id}/text-differences")
async def get_text_differences(session_id: str, pair_id: str):
    try:
        payload = await run_in_threadpool(
            store.get_text_differences_state, session_id, pair_id
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    return payload or {"version": 1, "pair_id": pair_id, "status": "not_started"}


@router.post("/sessions/{session_id}/pairs/{pair_id}/text-ai-review")
async def rebuild_text_ai_review(session_id: str, pair_id: str):
    try:
        return await store.run_text_ai_review(session_id, pair_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except (FileNotFoundError, ValueError, OSError, UnicodeDecodeError) as exc:
        raise HTTPException(400, f"Не удалось выполнить ИИ-ревизию текста: {exc}") from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("AI text review failed")
        raise HTTPException(500, f"Ошибка ИИ-ревизии текста: {exc}") from exc


@router.get("/sessions/{session_id}/pairs/{pair_id}/text-ai-review")
async def get_text_ai_review(session_id: str, pair_id: str):
    try:
        payload = await run_in_threadpool(store.get_text_ai_review_state, session_id, pair_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    return payload or {"version": 1, "pair_id": pair_id, "status": "not_started"}


@router.get("/sessions/{session_id}/pairs/{pair_id}/text-final-comparison")
async def get_text_final_comparison(session_id: str, pair_id: str):
    try:
        payload = await run_in_threadpool(
            store.get_text_final_comparison_state, session_id, pair_id
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    return payload or {"version": 1, "pair_id": pair_id, "status": "not_started"}


@router.post("/sessions/{session_id}/pairs/{pair_id}/text-change-summary")
async def rebuild_text_change_summary(session_id: str, pair_id: str):
    try:
        return await store.run_project_change_summary(session_id, pair_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except (FileNotFoundError, ValueError, OSError, UnicodeDecodeError) as exc:
        raise HTTPException(400, f"Не удалось сформировать основные изменения: {exc}") from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("project change summary failed")
        raise HTTPException(500, f"Ошибка агрегации основных изменений: {exc}") from exc


@router.get("/sessions/{session_id}/pairs/{pair_id}/text-change-summary")
async def get_text_change_summary(session_id: str, pair_id: str):
    try:
        payload = await run_in_threadpool(
            store.get_project_change_summary_state, session_id, pair_id
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    return payload or {"version": 1, "pair_id": pair_id, "status": "not_started"}


@router.post("/sessions/{session_id}/pairs/{pair_id}/high-level-project-changes")
async def rebuild_high_level_project_changes(session_id: str, pair_id: str):
    try:
        return await store.run_high_level_project_changes(session_id, pair_id)
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except (FileNotFoundError, ValueError, OSError, UnicodeDecodeError) as exc:
        raise HTTPException(400, f"Не удалось синтезировать основные изменения: {exc}") from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("high-level project change synthesis failed")
        raise HTTPException(500, f"Ошибка синтеза основных изменений: {exc}") from exc


@router.get("/sessions/{session_id}/pairs/{pair_id}/high-level-project-changes")
async def get_high_level_project_changes(session_id: str, pair_id: str):
    try:
        payload = await run_in_threadpool(
            store.get_high_level_project_changes_state, session_id, pair_id
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    return payload or {"version": 1, "pair_id": pair_id, "status": "not_started"}


@router.get("/sessions/{session_id}/pairs/{pair_id}/text-entities")
async def get_text_entities(session_id: str, pair_id: str):
    """Return an existing lightweight artifact; GET never starts a producer."""
    try:
        payload = await run_in_threadpool(
            store.get_text_entities_state, session_id, pair_id
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    return payload or {
        "schema_version": "text-entities.v1",
        "pair_id": pair_id,
        "status": "not_started",
    }


@router.get("/sessions/{session_id}/pairs/{pair_id}/page-thumb")
async def get_page_thumb(
    request: Request,
    session_id: str,
    pair_id: str,
    side: str = Query(..., pattern="^(left|right)$"),
    page: int = Query(1, ge=1),
    width: int = Query(160, ge=64, le=400),
):
    try:
        payload = await run_in_threadpool(
            store.page_thumbnail_payload, session_id, pair_id, side, page, width
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("page-thumb render failed")
        raise HTTPException(500, f"Ошибка миниатюры страницы: {exc}") from exc

    # Миниатюра меняется только вместе с PDF, поэтому кэшируем надолго: полоса
    # прокручивается туда-обратно, и каждый повторный проход иначе стоил бы
    # десятки перерисовок.
    headers = {"Cache-Control": "private, max-age=86400", "ETag": payload["etag"]}
    if request.headers.get("if-none-match") == payload["etag"]:
        return Response(status_code=304, headers=headers)
    return Response(payload["body"], media_type="image/png", headers=headers)


@router.get("/sessions/{session_id}/pairs/{pair_id}/page-info")
async def get_page_info(
    session_id: str,
    pair_id: str,
    side: str = Query(..., pattern="^(left|right)$"),
    page: int = Query(1, ge=1),
):
    try:
        return await run_in_threadpool(
            store.page_info_payload, session_id, pair_id, side, page
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("page-info read failed")
        raise HTTPException(500, f"Ошибка параметров страницы: {exc}") from exc


@router.get("/sessions/{session_id}/pairs/{pair_id}/text-search")
async def search_pdf_text(
    session_id: str,
    pair_id: str,
    side: str = Query(..., pattern="^(left|right)$"),
    query: str = Query(..., min_length=1, max_length=200),
):
    try:
        return await run_in_threadpool(
            store.pdf_text_search_payload, session_id, pair_id, side, query
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("PDF text search failed")
        raise HTTPException(500, f"Ошибка поиска по PDF: {exc}") from exc


@router.get("/sessions/{session_id}/pairs/{pair_id}/page-preview")
async def get_page_preview(
    request: Request,
    session_id: str,
    pair_id: str,
    side: str = Query(..., pattern="^(left|right)$"),
    page: int = Query(1, ge=1),
    width: int = Query(1400, ge=640, le=2400),
):
    try:
        payload = await run_in_threadpool(
            store.page_preview_payload, session_id, pair_id, side, page, width
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("page-preview render failed")
        raise HTTPException(500, f"Ошибка preview страницы: {exc}") from exc

    headers = {"Cache-Control": "private, max-age=86400", "ETag": payload["etag"]}
    if request.headers.get("if-none-match") == payload["etag"]:
        return Response(status_code=304, headers=headers)
    return Response(payload["body"], media_type="image/png", headers=headers)


@router.get("/sessions/{session_id}/pairs/{pair_id}/page-tile")
async def get_page_tile(
    request: Request,
    session_id: str,
    pair_id: str,
    side: str = Query(..., pattern="^(left|right)$"),
    page: int = Query(1, ge=1),
    level: int = Query(0, ge=0, le=6),
    x: int = Query(0, ge=0),
    y: int = Query(0, ge=0),
):
    try:
        payload = await run_in_threadpool(
            store.page_tile_payload, session_id, pair_id, side, page, level, x, y
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("page-tile render failed")
        raise HTTPException(500, f"Ошибка тайла страницы: {exc}") from exc

    headers = {"Cache-Control": "private, max-age=86400", "ETag": payload["etag"]}
    if request.headers.get("if-none-match") == payload["etag"]:
        return Response(status_code=304, headers=headers)
    return Response(payload["body"], media_type="image/png", headers=headers)


@router.get("/sessions/{session_id}/pairs/{pair_id}/page-svg")
async def get_page_svg(
    request: Request,
    session_id: str,
    pair_id: str,
    side: str = Query(..., pattern="^(left|right)$"),
    page: int = Query(1, ge=1),
):
    accept_gzip = "gzip" in (request.headers.get("accept-encoding") or "").lower()
    try:
        payload = await run_in_threadpool(
            store.page_svg_payload, session_id, pair_id, side, page, accept_gzip
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception("page-svg render failed")
        raise HTTPException(500, f"Ошибка векторного рендера страницы: {exc}") from exc

    headers = {"Cache-Control": "private, max-age=3600", "ETag": payload["etag"]}
    # Просмотрщик листает страницы туда-обратно; 304 экономит мегабайты вектора.
    if request.headers.get("if-none-match") == payload["etag"]:
        return Response(status_code=304, headers=headers)
    if payload["encoding"]:
        headers["Content-Encoding"] = payload["encoding"]
        headers["Vary"] = "Accept-Encoding"
    return Response(payload["body"], media_type="image/svg+xml", headers=headers)
