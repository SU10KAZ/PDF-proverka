"""API for source upload, sheet matching and the tiled PDF viewer."""
from __future__ import annotations

import json
import logging
from typing import Any, Literal

from fastapi import APIRouter, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import Response
from pydantic import BaseModel, ConfigDict, Field, model_validator

from backend.app.services.stage_comparison import document_groups
from backend.app.services.stage_comparison import objects as objects_mod
from backend.app.services.stage_comparison import stage_upload as stage_upload_mod
from backend.app.services.stage_comparison import store
from backend.app.services.stage_comparison import production_orchestrator as production
from backend.app.services.stage_comparison import production_store
from backend.app.services.stage_comparison import decision_registry
from backend.app.services.stage_comparison import human_contour
from backend.app.services.stage_comparison import human_presentation
from backend.app.core import portal_auth
from backend.app.services.common import user_service
from backend.app.services.project_change_v3 import expert_review as pc_expert_review


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/stage-comparison", tags=["stage-comparison"])


class CreateSessionRequest(BaseModel):
    stage_a_path: str = Field(min_length=1)
    stage_b_path: str = Field(min_length=1)


class CreatePairRequest(BaseModel):
    left_pdf: str = Field(min_length=1)
    right_pdf: str = Field(min_length=1)
    composition_completeness: Literal["COMPLETE", "INCOMPLETE", "UNKNOWN"] = "UNKNOWN"


class DocumentGroupMembersRequest(BaseModel):
    left_document_code: str = Field(min_length=1)
    member_codes: list[str] = Field(min_length=1, max_length=32)


class CompositionCompletenessRequest(BaseModel):
    value: Literal["COMPLETE", "INCOMPLETE", "UNKNOWN"]


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
    #: Модель ProjectChange V3 этого прогона (``astra`` / ``opus55``). Разрешённый
    #: список задаёт установка (PROJECT_COMPARISON_V3_MODEL_CHOICES); без значения —
    #: модель установки по умолчанию.
    model_profile: str | None = Field(default=None, max_length=32)
    #: Досборка V3: взять карту, ответы Miner и разбора unmatched у упавшего прогона
    #: этой пары и продолжить с Dedupe (без повторных вызовов Mapper/Miner).
    resume_from_run_id: str | None = Field(default=None, pattern=r"^[0-9a-f]{32}$")
    resume_model_policy: Literal["same_model", "remaining_stages"] = "same_model"


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
    # Human Contour v1 binds authority to this stable semantic identity.
    # ``question_id`` remains only for legacy compatibility and cross-checking.
    domain_key: str | None = Field(
        default=None,
        pattern=r"^dk1_[a-z_]+_[0-9a-f]{32}$",
    )
    answer: str = Field(min_length=1)
    # See ProductionDecisionUpdate.author.
    author: str | None = None
    comment: str | None = None
    selected_refs: list[str] = Field(default_factory=list)
    explicit_candidate: ProductionExplicitCandidate | None = None
    typed_resolution: ProductionTypedResolution | None = None
    lock: bool = False


class SaveProductionAnswersRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    answers: list[ProductionReviewAnswer] = Field(default_factory=list)
    expected_input_signature: str = Field(min_length=1)
    expected_revision: int = Field(ge=0)


class HumanReviewAtomOverride(BaseModel):
    model_config = ConfigDict(extra="forbid")

    target_id: str = Field(min_length=1)
    answer: dict[str, Any]
    comment: str | None = None


class HumanReviewUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    interaction_id: str = Field(min_length=1)
    answer: dict[str, Any]
    comment: str | None = None
    overrides: list[HumanReviewAtomOverride] = Field(default_factory=list)


class SaveHumanReviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    updates: list[HumanReviewUpdate] = Field(default_factory=list)
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


def _authorized_human_contour_author(request: Request) -> str:
    """Require a signed portal session mapped to an authorized employee."""
    settings = portal_auth.get_settings()
    if not settings.enabled:
        raise HTTPException(503, "Human Contour требует включённой авторизации портала")
    username = portal_auth.request_username(request, settings)
    if not username:
        raise HTTPException(401, "Not authenticated")
    employee = user_service.get_user_by_login(username)
    if not human_contour.authorized_employee(employee):
        raise HTTPException(403, "Пользователь не уполномочен принимать решения")
    return str(employee.get("id") or employee.get("login") or username)


@router.get("/objects")
async def list_comparison_objects():
    return objects_mod.list_objects()


def _document_group_error(exc: document_groups.DocumentGroupError) -> HTTPException:
    status = {
        "ASSEMBLIES_DISABLED": 409,
        "GROUP_NOT_FOUND": 404,
        "GROUP_BUSY": 409,
        "GROUPS_CORRUPT": 500,
    }.get(exc.code, 400)
    return HTTPException(status, {"code": exc.code, "message": str(exc)})


async def _run_document_groups(func, *args):
    try:
        return await run_in_threadpool(func, *args)
    except document_groups.DocumentGroupError as exc:
        raise _document_group_error(exc) from exc
    except stage_upload_mod.StageUploadError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get("/objects/{object_id}/document-groups")
async def list_document_groups(object_id: str):
    return await _run_document_groups(document_groups.list_groups, object_id)


@router.post("/objects/{object_id}/document-groups")
async def add_document_group_members(object_id: str, request: DocumentGroupMembersRequest):
    return await _run_document_groups(
        document_groups.add_members, object_id, request.left_document_code, request.member_codes,
    )


@router.delete("/objects/{object_id}/document-groups/{group_id}/members/{document_code}")
async def remove_document_group_member(object_id: str, group_id: str, document_code: str):
    return await _run_document_groups(document_groups.remove_member, object_id, group_id, document_code)


@router.post("/objects/{object_id}/document-groups/{group_id}/rebuild")
async def rebuild_document_group(object_id: str, group_id: str):
    return await _run_document_groups(document_groups.rebuild, object_id, group_id)


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
            request.composition_completeness,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc


@router.put("/sessions/{session_id}/pairs/{pair_id}/composition-completeness")
async def save_composition_completeness(session_id: str, pair_id: str, request: CompositionCompletenessRequest):
    try:
        return await run_in_threadpool(store.save_composition_completeness, session_id, pair_id, request.value)
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
    if request.resume_model_policy == "remaining_stages" and (
        not request.resume_from_run_id or not request.model_profile
    ):
        raise HTTPException(400, "Для смены модели досборки нужны исходный прогон и выбранная модель")
    if request.model_profile:
        from backend.app.services.project_change_v3.contracts import resolve_profile
        try:
            resolve_profile(request.model_profile)
        except ValueError as exc:
            raise HTTPException(400, f"Модель недоступна на этой установке: {request.model_profile}") from exc
    if request.resume_from_run_id:
        from backend.app.services.project_change_v3 import resume as v3_resume
        try:
            await run_in_threadpool(v3_resume.load_donor, session_id, pair_id, request.resume_from_run_id)
        except ValueError as exc:  # ResumeRefused, or an unsafe id
            raise HTTPException(400, f"Досборка невозможна: {exc}") from exc
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
        "controlled_v2_standard": production.ai_v2_settings.enabled(),
        # Модели ProjectChange V3, из которых инженер выбирает перед запуском.
        "models": _v3_model_choices(),
    }


def _v3_model_choices() -> dict[str, Any]:
    from backend.app.services.project_change_v3 import contracts as v3_contracts
    try:
        choices = v3_contracts.profile_choices()
    except ValueError:
        logger.exception("PROJECT_COMPARISON_V3_MODEL_CHOICES is invalid")
        choices = [v3_contracts.MODEL_PROFILES[v3_contracts.DEFAULT_PROFILE_KEY]]
    return {
        "default": v3_contracts.DEFAULT_PROFILE_KEY,
        "items": [{"code": p.key, "label": p.label, "model": p.model} for p in choices],
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
    "/sessions/{session_id}/pairs/{pair_id}/production/stages/{stage_id}/result"
)
async def get_production_stage_result(
    session_id: str,
    pair_id: str,
    stage_id: str,
    run_id: str = Query(min_length=1),
):
    """Download one stage from the explicitly requested persisted run."""
    try:
        return await run_in_threadpool(
            production.get_production_stage_result,
            session_id,
            pair_id,
            run_id,
            stage_id,
        )
    except production_store.ProductionConflictError as exc:
        raise HTTPException(409, str(exc)) from exc
    except (KeyError, ValueError) as exc:
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


@router.post('/objects/{object_id}/project-change-expert-review')
def save_project_change_expert_review(object_id: str, body: pc_expert_review.ReviewRequest, request: Request):
    try:
        return pc_expert_review.save(object_id, body.updates, actor=_engineer_author(request))
    except pc_expert_review.ReviewConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    except (LookupError, ValueError, OSError) as exc:
        raise HTTPException(404, 'Изменение или опубликованный запуск недоступны') from exc


@router.get('/sessions/{session_id}/pairs/{pair_id}/runs/{run_id}/project-changes')
def get_run_project_changes(session_id: str, pair_id: str, run_id: str):
    from backend.app.services.project_change_v3 import presentation, run_storage
    try:
        with run_storage.selected(session_id, pair_id, run_id):
            result = presentation.pair_changes(session_id, pair_id)
        if not result['available']:
            raise KeyError('run unavailable')
        return result
    except (ValueError, KeyError, OSError) as exc:
        raise HTTPException(404, 'Run unavailable') from exc


# Consolidator V1 — read-only «Исходные | Итоговые»: only COMPLETED shadow runs bound to
# their source by source_run_id + sha256; nothing is generated here (0 model calls).
@router.get('/sessions/{session_id}/pairs/{pair_id}/consolidated')
def get_pair_consolidations(session_id: str, pair_id: str):
    from backend.app.services.project_change_consolidator import view
    try:
        items = view.pair_consolidations(session_id, pair_id)
    except (ValueError, KeyError, OSError) as exc:
        raise HTTPException(404, 'Pair unavailable') from exc
    return {'schema': 'projectchange-consolidations/1', 'available': bool(items), 'consolidations': items}


@router.get('/sessions/{session_id}/pairs/{pair_id}/consolidated/{source_run_id}/{consolidator_run_id}')
def get_consolidated_view(session_id: str, pair_id: str, source_run_id: str, consolidator_run_id: str):
    from fastapi.responses import JSONResponse
    from backend.app.services.project_change_consolidator import view
    try:
        return JSONResponse(view.consolidated_view(session_id, pair_id, source_run_id, consolidator_run_id))
    except (view.ViewUnavailable, ValueError, KeyError, OSError) as exc:
        raise HTTPException(404, 'Consolidated result unavailable') from exc


# Excel-отчёт «Итоговые · Карточки ИТ · Краткая сводка»: «Итоговые» — из той же итоговой сводки,
# разбор по корзинам — загруженный инженером (иначе шаблон). 0 вызовов моделей.
def _consolidated_or_404(session_id: str, pair_id: str, source_run_id: str, consolidator_run_id: str) -> dict:
    from backend.app.services.project_change_consolidator import view
    try:
        return view.consolidated_view(session_id, pair_id, source_run_id, consolidator_run_id)
    except (view.ViewUnavailable, ValueError, KeyError, OSError) as exc:
        raise HTTPException(404, 'Consolidated result unavailable') from exc


def _report_labels(session_id: str, pair_id: str) -> tuple[str, str]:
    """(шифр документа, название объекта) для шапки отчёта."""
    from pathlib import Path

    pair = store._load_pair(session_id, pair_id) or {}
    code = str((pair.get('left') or {}).get('document_code') or (pair.get('right') or {}).get('document_code') or '')
    meta = store._load_session_meta(session_id) or {}
    label = ''
    stage_path = meta.get('stage_a_path') or ''
    if stage_path:  # .../objects/<папка объекта>/comparison/stage_1
        try:
            label = json.loads((Path(stage_path).parents[1] / 'object.json').read_text(encoding='utf-8')).get('display_name') or ''
        except (OSError, ValueError, IndexError):
            label = ''
    return code or pair_id, label or session_id


@router.get('/sessions/{session_id}/pairs/{pair_id}/consolidated/{source_run_id}/{consolidator_run_id}/report.xlsx')
def get_consolidated_report(session_id: str, pair_id: str, source_run_id: str, consolidator_run_id: str, request: Request):
    from urllib.parse import quote

    from backend.app.services.project_change_consolidator import it_analysis, report_xlsx
    data = _consolidated_or_404(session_id, pair_id, source_run_id, consolidator_run_id)
    code, label = _report_labels(session_id, pair_id)
    analysis = it_analysis.load(session_id, pair_id, data)
    body = report_xlsx.build_report(data, document_code=code, object_label=label, analysis=analysis,
                                    base_url=str(request.base_url))
    name = f"{report_xlsx.short_code(code)}_ИТ_{'четыре_корзины' if analysis else 'шаблон'}.xlsx"
    return Response(body, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                    headers={'Content-Disposition': f"attachment; filename=\"report.xlsx\"; filename*=UTF-8''{quote(name)}",
                             'Cache-Control': 'no-store'})


@router.get('/sessions/{session_id}/pairs/{pair_id}/consolidated/{source_run_id}/{consolidator_run_id}/it-analysis')
def get_it_analysis_status(session_id: str, pair_id: str, source_run_id: str, consolidator_run_id: str):
    from backend.app.services.project_change_consolidator import it_analysis
    return it_analysis.status(session_id, pair_id,
                              _consolidated_or_404(session_id, pair_id, source_run_id, consolidator_run_id))


@router.post('/sessions/{session_id}/pairs/{pair_id}/consolidated/{source_run_id}/{consolidator_run_id}/it-analysis')
async def upload_it_analysis(session_id: str, pair_id: str, source_run_id: str, consolidator_run_id: str,
                             request: Request, file: UploadFile = File(...)):
    from backend.app.services.project_change_consolidator import it_analysis
    data = await file.read(it_analysis.MAX_UPLOAD_BYTES + 1)
    author = _engineer_author(request)
    consolidated = await run_in_threadpool(_consolidated_or_404, session_id, pair_id, source_run_id, consolidator_run_id)
    try:
        return await run_in_threadpool(it_analysis.save, session_id, pair_id, consolidated, data,
                                       filename=file.filename or 'report.xlsx', author=author)
    except it_analysis.AnalysisError as exc:
        raise HTTPException(422, {'message': 'Разбор не принят', 'errors': exc.errors}) from exc


@router.get('/sessions/{session_id}/pairs/{pair_id}/consolidated/{source_run_id}/evidence/{evidence_id}/crop')
def get_consolidated_evidence_crop(session_id: str, pair_id: str, source_run_id: str, evidence_id: str):
    from fastapi import Response

    from backend.app.services.project_change_consolidator import view
    try:
        data = view.evidence_crop(session_id, pair_id, source_run_id, evidence_id)
    except (view.ViewUnavailable, ValueError, KeyError, OSError) as exc:
        raise HTTPException(404, 'Evidence unavailable') from exc
    return Response(data, media_type='image/png', headers={'Cache-Control': 'no-store'})


@router.get("/sessions/{session_id}/pairs/{pair_id}/production/changes")
async def get_production_changes(session_id: str, pair_id: str):
    try:
        return await run_in_threadpool(
            human_presentation.get_production_changes, session_id, pair_id
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
            human_presentation.get_review_questions, session_id, pair_id
        )
    except production_store.ProductionConflictError as exc:
        raise HTTPException(409, str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.get(
    "/sessions/{session_id}/pairs/{pair_id}/production/questions/{domain_key}"
)
async def get_production_question(
    session_id: str, pair_id: str, domain_key: str
):
    try:
        return await run_in_threadpool(
            production.get_review_question_by_domain_key,
            session_id,
            pair_id,
            domain_key,
        )
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
        if human_contour.enabled():
            return await run_in_threadpool(
                human_presentation.update_atomic_question_answers,
                session_id,
                pair_id,
                answers=answers,
                author=_authorized_human_contour_author(http_request),
                expected_input_signature=request.expected_input_signature,
                expected_revision=request.expected_revision,
            )
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
    except decision_registry.RegistryConflict as exc:
        raise HTTPException(409, str(exc)) from exc
    except human_contour.HumanContourUnavailable as exc:
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


@router.get("/sessions/{session_id}/pairs/{pair_id}/production/human-review")
async def get_production_human_review(session_id: str, pair_id: str):
    try:
        return await run_in_threadpool(
            production.get_human_review, session_id, pair_id
        )
    except production_store.ProductionConflictError as exc:
        raise HTTPException(409, str(exc)) from exc
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc


@router.put(
    "/sessions/{session_id}/pairs/{pair_id}/production/human-review/answers"
)
async def save_production_human_review(
    http_request: Request,
    session_id: str,
    pair_id: str,
    request: SaveHumanReviewRequest,
):
    try:
        return await run_in_threadpool(
            production.update_human_review_answers,
            session_id,
            pair_id,
            updates=[value.model_dump() for value in request.updates],
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
