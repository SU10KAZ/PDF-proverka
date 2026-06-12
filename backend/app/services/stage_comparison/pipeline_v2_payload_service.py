# -*- coding: utf-8 -*-
"""Pipeline V2 — read-only выдача UI payload по готовым артефактам.

Сервис для endpoint'а ``GET /api/stage-comparison/pipeline-v2/{sid}/ui-payload``:
находит артефакты Pipeline V2 в дереве comparison-сессии и отдаёт готовый
``pipeline_v2_ui_payload.json`` либо собирает payload на лету через
:mod:`pipeline_v2_ui_payload` из ``pipeline_v2_summary.json`` (+ опц. diff /
delta explanation / graphic отчёты).

Жёсткие гарантии read-only:

* НИЧЕГО не запускает: ни Pipeline V2, ни внешние модели, ни фоновые задачи;
* НИЧЕГО не пишет на диск (без кеша; директории не создаются — резолв путей
  идёт без ``mkdir``, в отличие от ``paths.session_dir``);
* существующие артефакты/статусы/comparison_result не изменяются;
* битые артефакты дают fail-soft ответ ``status=error``, а не исключение.

Path convention (готова к будущей runtime-структуре):

* session-level: ``comparison/sessions/<sid>/pipeline_v2/``
* pair-level:    ``comparison/sessions/<sid>/pairs/<pid>/pipeline_v2/``

Файлы внутри — стандартные имена dry-run (``pipeline_v2_summary.json``,
``entity_diff_report.json``, ``delta_explanation_report.json``,
``left/right_graphic_descriptor_report.json``) плюс опциональный готовый
``pipeline_v2_ui_payload.json``.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Optional

from backend.app.services.stage_comparison.paths import (
    _safe_id,
    comparison_root_path,
    sessions_root_path,
)
from backend.app.services.stage_comparison.pipeline_v2_ui_payload import (
    PAYLOAD_KIND,
    build_pipeline_v2_ui_payload,
)
from backend.app.services.stage_comparison.pipeline_v2_block_link_preview import (
    REPORT_KIND as BLOCK_LINK_PREVIEW_KIND,
    build_block_link_preview,
)
from backend.app.services.stage_comparison.pipeline_v2_entity_alignment_preview import (
    REPORT_KIND as ENTITY_ALIGNMENT_KIND,
    build_entity_alignment_preview_report,
)
from backend.app.services.stage_comparison.pipeline_v2_entity_alignment_detail import (
    build_entity_alignment_detail,
)
from backend.app.services.stage_comparison.pipeline_v2_link_validation import (
    REPORT_KIND as LINK_VALIDATION_KIND,
)
from backend.app.services.stage_comparison.pipeline_v2_link_validation_detail import (
    build_link_validation_detail,
)
from backend.app.services.stage_comparison.pipeline_v2_exclusion_preview import (
    REPORT_KIND as EXCLUSION_PREVIEW_KIND,
)
from backend.app.services.stage_comparison import (
    pipeline_v2_grounding_detail as _grounding_detail_mod,
    pipeline_v2_exclusion_review_overrides as _excl_review_mod,
)

PIPELINE_V2_DIRNAME = "pipeline_v2"
UI_PAYLOAD_FILENAME = "pipeline_v2_ui_payload.json"
SUMMARY_FILENAME = "pipeline_v2_summary.json"
ENTITY_DIFF_FILENAME = "entity_diff_report.json"
DELTA_EXPLANATION_FILENAME = "delta_explanation_report.json"
LEFT_GRAPHIC_FILENAME = "left_graphic_descriptor_report.json"
RIGHT_GRAPHIC_FILENAME = "right_graphic_descriptor_report.json"
LEFT_MODEL_FILENAME = "left_normalized_document_model.json"
RIGHT_MODEL_FILENAME = "right_normalized_document_model.json"
BLOCK_MATCHING_FILENAME = "block_matching_report.json"
VISUAL_GATE_FILENAME = "visual_equivalence_gate_report.json"
BLOCK_LINK_PREVIEW_FILENAME = "block_link_preview_report.json"
GROUNDED_EVIDENCE_FILENAME = "grounded_evidence_report.json"
GRAPHIC_MATCHED_FILENAME = "graphic_descriptor_matched_report.json"
ENTITY_ALIGNMENT_PREVIEW_FILENAME = "entity_alignment_preview_report.json"
ENTITY_MAPPING_OVERRIDES_FILENAME = "entity_mapping_overrides.json"
LINK_VALIDATION_FILENAME = "link_validation_report.json"
EXCLUSION_PREVIEW_FILENAME = "exclusion_preview_v2_report.json"
EXCLUSION_REVIEW_OVERRIDES_FILENAME = "exclusion_review_overrides.json"
SKIP_READINESS_FILENAME = "skip_readiness_report.json"

NOT_FOUND_MESSAGE = "Pipeline V2 artifacts not found for this session."
BLP_NOT_FOUND_MESSAGE = "Pipeline V2 block link preview artifacts not found."
EAP_NOT_FOUND_MESSAGE = "Entity alignment preview report not found for this pair."


# ─── резолв путей (БЕЗ mkdir — endpoint read-only) ───────────────────────────


def resolve_session_dir(session_id: str) -> Path:
    """Директория сессии без создания (в отличие от paths.session_dir).

    Используются «чистые» резолверы ``*_path()`` — даже корень comparison/
    не создаётся, один GET к несуществующей сессии не материализует дерево.
    ``ValueError`` при невалидном id (path traversal обрезается ``_safe_id``).
    """
    return sessions_root_path() / _safe_id(session_id)


def pipeline_v2_artifacts_dir(session_id: str,
                              pair_id: Optional[str] = None) -> Path:
    """Каталог артефактов Pipeline V2 (session- или pair-level), без mkdir."""
    base = resolve_session_dir(session_id)
    if pair_id:
        return base / "pairs" / _safe_id(pair_id) / PIPELINE_V2_DIRNAME
    return base / PIPELINE_V2_DIRNAME


def list_pairs_with_artifacts(session_id: str) -> list[str]:
    """pair_id пар, у которых есть pipeline_v2 артефакты (для discovery)."""
    pairs_dir = resolve_session_dir(session_id) / "pairs"
    if not pairs_dir.is_dir():
        return []
    out: list[str] = []
    try:
        children = sorted(pairs_dir.iterdir())
    except OSError:
        return []
    for child in children:
        art = child / PIPELINE_V2_DIRNAME
        if not art.is_dir():
            continue
        if (art / UI_PAYLOAD_FILENAME).is_file() or (art / SUMMARY_FILENAME).is_file():
            out.append(child.name)
    return out


# ─── чтение артефактов (fail-soft) ───────────────────────────────────────────


def _read_json(path: Path) -> tuple[Optional[Any], Optional[str]]:
    """(данные, ошибка) — никогда не бросает наружу.

    ``except Exception`` сознательно широкий: json.loads на патологическом
    входе может бросить и RecursionError — endpoint обязан остаться fail-soft.
    """
    try:
        if not path.is_file():
            return None, None
        return json.loads(path.read_text(encoding="utf-8")), None
    except Exception as exc:  # noqa: BLE001 — fail-soft по контракту
        return None, f"{path.name}: {type(exc).__name__}: {exc}"


_JSON_SAFE_MAX_DEPTH = 200


def _json_safe(value: Any, hits: list[int], depth: int = 0) -> Any:
    """Заменить не-финитные float'ы (NaN/Inf из artifact-JSON) на None.

    json.loads принимает NaN/Infinity, а сериализация ответа на них падает —
    payload должен оставаться строгим JSON. Патологически глубокая вложенность
    обрезается (None + warning-hit): иначе она прошла бы санитайзер, но
    уронила бы сериализацию ответа в RecursionError → 500.
    """
    if depth > _JSON_SAFE_MAX_DEPTH:
        hits.append(1)
        return None
    if isinstance(value, float) and not math.isfinite(value):
        hits.append(1)
        return None
    if isinstance(value, dict):
        return {k: _json_safe(v, hits, depth + 1) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v, hits, depth + 1) for v in value]
    return value


def _sanitize_payload(payload: Optional[dict],
                      warnings: list[str]) -> Optional[dict]:
    if payload is None:
        return None
    hits: list[int] = []
    out = _json_safe(payload, hits)
    if hits:
        warnings.append(f"non-finite numeric values sanitized: {len(hits)}")
    return out


def _relative_to_comparison_root(path: Path) -> str:
    try:
        return str(path.relative_to(comparison_root_path()))
    except ValueError:
        return str(path)


def _envelope(status: str, *, session_id: str, pair_id: Optional[str],
              message: str = "", source: Optional[str] = None,
              payload: Optional[dict] = None,
              artifacts_dir: Optional[Path] = None,
              warnings: Optional[list[str]] = None,
              available_pairs: Optional[list[str]] = None) -> dict:
    out: dict[str, Any] = {
        "status": status,
        "available": payload is not None,
        "session_id": session_id,
        "pair_id": pair_id,
        "source": source,
        "message": message,
        "payload": payload,
        "warnings": warnings or [],
    }
    if artifacts_dir is not None:
        out["artifacts_dir"] = _relative_to_comparison_root(artifacts_dir)
    if available_pairs is not None:
        out["available_pairs"] = available_pairs
    return out


def _build_from_artifacts(art_dir: Path, session_id: str,
                          pair_id: Optional[str],
                          warnings: list[str]) -> Optional[dict]:
    """Собрать payload из артефактов dry-run; None — если нет даже summary."""
    summary, err = _read_json(art_dir / SUMMARY_FILENAME)
    if err:
        warnings.append(err)
    if not isinstance(summary, dict):
        if summary is not None:
            # файл есть, но это не объект — это error, а не not_found
            warnings.append(f"{SUMMARY_FILENAME}: expected JSON object, "
                            f"got {type(summary).__name__}")
        return None

    diff, err = _read_json(art_dir / ENTITY_DIFF_FILENAME)
    if err:
        warnings.append(err)
    de, err = _read_json(art_dir / DELTA_EXPLANATION_FILENAME)
    if err:
        warnings.append(err)
    left_g, err = _read_json(art_dir / LEFT_GRAPHIC_FILENAME)
    if err:
        warnings.append(err)
    right_g, err = _read_json(art_dir / RIGHT_GRAPHIC_FILENAME)
    if err:
        warnings.append(err)
    # grounded evidence (optional) — per-delta badges/cards; отсутствие не
    # ломает payload (build_pipeline_v2_ui_payload деградирует на counts/нет)
    ge, err = _read_json(art_dir / GROUNDED_EVIDENCE_FILENAME)
    if err:
        warnings.append(err)

    gdr = None
    if isinstance(left_g, dict) or isinstance(right_g, dict):
        gdr = {"left": left_g if isinstance(left_g, dict) else None,
               "right": right_g if isinstance(right_g, dict) else None}
    return build_pipeline_v2_ui_payload(
        summary,
        diff if isinstance(diff, dict) else None,
        de if isinstance(de, dict) else None,
        graphic_descriptor_reports=gdr,
        grounded_evidence_report=ge if isinstance(ge, dict) else None,
    )


# ─── основной read-only вход ─────────────────────────────────────────────────


def discover_pipeline_v2_payload(session_id: str,
                                 pair_id: Optional[str] = None) -> dict:
    """Найти/собрать UI payload для сессии (read-only, fail-soft).

    Статусы ответа:

    * ``ok`` — готовый payload с диска или собран из полного набора отчётов;
    * ``partial`` — собран из summary, но diff/explanation отчётов не хватает
      (карточки деградированы, counters из summary);
    * ``not_found`` — артефактов нет (ни готового payload, ни summary);
      в ответе ``available_pairs`` — пары сессии, у которых артефакты есть;
    * ``error`` — артефакты есть, но непригодны (битый JSON и т.п.).

    ``ValueError`` пробрасывается только на невалидный session_id/pair_id
    (router переводит в HTTP 400). Всё остальное — JSON-ответ, не исключение.
    """
    # валидация id ДО fail-soft обёртки: ValueError → HTTP 400
    art_dir = pipeline_v2_artifacts_dir(session_id, pair_id)
    try:
        return _discover(art_dir, session_id, pair_id)
    except Exception as exc:  # noqa: BLE001 — endpoint не должен дать 500
        return _envelope("error", session_id=session_id, pair_id=pair_id,
                         warnings=[f"{type(exc).__name__}: {exc}"],
                         message="Pipeline V2 artifacts could not be read.")


def _discover(art_dir: Path, session_id: str, pair_id: Optional[str]) -> dict:
    warnings: list[str] = []

    # 1) готовый payload — отдать как есть
    ready_path = art_dir / UI_PAYLOAD_FILENAME
    ready, err = _read_json(ready_path)
    if isinstance(ready, dict) and ready.get("kind") == PAYLOAD_KIND:
        return _envelope("ok", session_id=session_id, pair_id=pair_id,
                         source="ready_payload",
                         payload=_sanitize_payload(ready, warnings),
                         artifacts_dir=art_dir, warnings=warnings,
                         message="Готовый pipeline_v2_ui_payload.json.")
    if err:
        warnings.append(err)
    elif isinstance(ready, dict):
        warnings.append(f"{UI_PAYLOAD_FILENAME}: unexpected kind "
                        f"{ready.get('kind')!r} — rebuilt from artifacts")
    elif ready is not None:
        # валидный JSON, но не объект (список/строка/число)
        warnings.append(f"{UI_PAYLOAD_FILENAME}: expected JSON object, "
                        f"got {type(ready).__name__} — rebuilt from artifacts")

    # 2) собрать из артефактов dry-run
    try:
        payload = _build_from_artifacts(art_dir, session_id, pair_id, warnings)
    except Exception as exc:  # noqa: BLE001 — endpoint не должен дать 500
        return _envelope("error", session_id=session_id, pair_id=pair_id,
                         artifacts_dir=art_dir,
                         warnings=warnings + [f"{type(exc).__name__}: {exc}"],
                         message="Pipeline V2 artifacts exist but could not "
                                 "be converted to UI payload.")
    if payload is not None:
        diff_missing = not (art_dir / ENTITY_DIFF_FILENAME).is_file()
        de_missing = not (art_dir / DELTA_EXPLANATION_FILENAME).is_file()
        payload = _sanitize_payload(payload, warnings)
        status = "partial" if (diff_missing or de_missing or warnings) else "ok"
        return _envelope(status, session_id=session_id, pair_id=pair_id,
                         source="built_from_artifacts", payload=payload,
                         artifacts_dir=art_dir, warnings=warnings,
                         message=("Payload собран из артефактов dry-run."
                                  if status == "ok" else
                                  "Payload собран частично: не все отчёты "
                                  "dry-run доступны."))

    # 3) артефакты есть, но непригодны (битый/не-объектный summary и т.п.)
    if warnings:
        return _envelope("error", session_id=session_id, pair_id=pair_id,
                         artifacts_dir=art_dir, warnings=warnings,
                         message="Pipeline V2 artifacts exist but are not "
                                 "readable.")

    # 4) артефактов нет совсем
    return _envelope("not_found", session_id=session_id, pair_id=pair_id,
                     message=NOT_FOUND_MESSAGE,
                     available_pairs=list_pairs_with_artifacts(session_id))


# ─── block link preview: read-only discovery ────────────────────────────────


def _list_pairs_with_block_link_artifacts(session_id: str) -> list[str]:
    """pair_id пар, у которых есть артефакты для block link preview."""
    pairs_dir = resolve_session_dir(session_id) / "pairs"
    if not pairs_dir.is_dir():
        return []
    out: list[str] = []
    try:
        children = sorted(pairs_dir.iterdir())
    except OSError:
        return []
    for child in children:
        art = child / PIPELINE_V2_DIRNAME
        if not art.is_dir():
            continue
        # предикат СОВПАДАЕТ с условием сборки в discover: либо готовый
        # отчёт, либо полный набор (models + block_matching) — иначе
        # advertised-пара сама отвечала бы not_found
        if ((art / BLOCK_LINK_PREVIEW_FILENAME).is_file()
                or ((art / LEFT_MODEL_FILENAME).is_file()
                    and (art / RIGHT_MODEL_FILENAME).is_file()
                    and (art / BLOCK_MATCHING_FILENAME).is_file())):
            out.append(child.name)
    return out


def discover_block_link_preview(session_id: str,
                                pair_id: Optional[str] = None) -> dict:
    """Найти/собрать block link preview для сессии (read-only, fail-soft).

    Статусы ответа (контракт тот же, что у discover_pipeline_v2_payload):

    * ``ok``        — готовый отчёт с диска или собран on-the-fly;
    * ``not_found`` — нет ни готового отчёта, ни (models + block_matching);
    * ``error``     — артефакты есть, но непригодны (битый JSON и т.п.).

    НИЧЕГО не пишет (отчёт on-the-fly не кешируется на диск), ничего не
    запускает. ``ValueError`` — только на невалидный session_id/pair_id.

    pair_id проверяется строго: значение, которое ``_safe_id`` пришлось бы
    переписать, отклоняется (HTTP 400), а не молча резолвится в чужую пару.
    """
    if pair_id and _safe_id(pair_id) != pair_id:
        raise ValueError(f"invalid pair_id: {pair_id!r}")
    art_dir = pipeline_v2_artifacts_dir(session_id, pair_id)
    try:
        return _discover_block_link_preview(art_dir, session_id, pair_id)
    except Exception as exc:  # noqa: BLE001 — endpoint не должен дать 500
        return _envelope("error", session_id=session_id, pair_id=pair_id,
                         warnings=[f"{type(exc).__name__}: {exc}"],
                         message="Pipeline V2 block link preview artifacts "
                                 "could not be read.")


def _discover_block_link_preview(art_dir: Path, session_id: str,
                                 pair_id: Optional[str]) -> dict:
    warnings: list[str] = []

    # 1) готовый отчёт — отдать как есть
    ready_path = art_dir / BLOCK_LINK_PREVIEW_FILENAME
    ready, err = _read_json(ready_path)
    if isinstance(ready, dict) and ready.get("kind") == BLOCK_LINK_PREVIEW_KIND:
        return _envelope("ok", session_id=session_id, pair_id=pair_id,
                         source="ready_report",
                         payload=_sanitize_payload(ready, warnings),
                         artifacts_dir=art_dir, warnings=warnings,
                         message="Готовый block_link_preview_report.json.")
    if err:
        warnings.append(err)
    elif isinstance(ready, dict):
        warnings.append(f"{BLOCK_LINK_PREVIEW_FILENAME}: unexpected kind "
                        f"{ready.get('kind')!r} — rebuilt from artifacts")
    elif ready is not None:
        warnings.append(f"{BLOCK_LINK_PREVIEW_FILENAME}: expected JSON object, "
                        f"got {type(ready).__name__} — rebuilt from artifacts")

    # 2) собрать on-the-fly из артефактов dry-run (только чтение)
    required = {}
    for fname in (LEFT_MODEL_FILENAME, RIGHT_MODEL_FILENAME,
                  BLOCK_MATCHING_FILENAME):
        value, err = _read_json(art_dir / fname)
        if err:
            warnings.append(err)
        elif value is not None and not isinstance(value, dict):
            # файл есть, но это не объект — это error, а не not_found
            warnings.append(f"{fname}: expected JSON object, "
                            f"got {type(value).__name__}")
            value = None
        required[fname] = value
    left_model = required[LEFT_MODEL_FILENAME]
    right_model = required[RIGHT_MODEL_FILENAME]
    block_matching = required[BLOCK_MATCHING_FILENAME]

    # неполный набор при частично существующих артефактах — сигнал
    # оборванного dry-run, а не «Pipeline V2 не запускался»
    present = [f for f, v in required.items() if v is not None]
    missing = [f for f, v in required.items() if v is None]
    if present and missing:
        warnings.append("block link preview inputs incomplete: missing "
                        + ", ".join(missing))

    if (isinstance(left_model, dict) and isinstance(right_model, dict)
            and isinstance(block_matching, dict)):
        left_g, err = _read_json(art_dir / LEFT_GRAPHIC_FILENAME)
        if err:
            warnings.append(err)
        right_g, err = _read_json(art_dir / RIGHT_GRAPHIC_FILENAME)
        if err:
            warnings.append(err)
        visual_gate, err = _read_json(art_dir / VISUAL_GATE_FILENAME)
        if err:
            warnings.append(err)
        try:
            report = build_block_link_preview(
                left_model, right_model, block_matching,
                left_graphic_report=left_g if isinstance(left_g, dict) else None,
                right_graphic_report=right_g if isinstance(right_g, dict) else None,
                visual_gate_report=visual_gate if isinstance(visual_gate, dict) else None)
        except Exception as exc:  # noqa: BLE001 — endpoint не должен дать 500
            return _envelope("error", session_id=session_id, pair_id=pair_id,
                             artifacts_dir=art_dir,
                             warnings=warnings + [f"{type(exc).__name__}: {exc}"],
                             message="Pipeline V2 artifacts exist but block "
                                     "link preview could not be built.")
        return _envelope("ok", session_id=session_id, pair_id=pair_id,
                         source="built_from_artifacts",
                         payload=_sanitize_payload(report, warnings),
                         artifacts_dir=art_dir, warnings=warnings,
                         message="Preview собран on-the-fly из артефактов "
                                 "dry-run (не записан на диск).")

    # 3) что-то прочиталось с ошибками → error, иначе not_found
    if warnings:
        return _envelope("error", session_id=session_id, pair_id=pair_id,
                         artifacts_dir=art_dir, warnings=warnings,
                         message="Pipeline V2 artifacts exist but are not "
                                 "readable for block link preview.")
    return _envelope("not_found", session_id=session_id, pair_id=pair_id,
                     message=BLP_NOT_FOUND_MESSAGE,
                     available_pairs=_list_pairs_with_block_link_artifacts(
                         session_id))


GROUNDING_REPORT_FILENAME = "graphic_vision_grounding_report.json"


def _grounding_page_map(art_dir: Path, warnings: list[str]) -> dict:
    """{block_id: page_number} из visual_equivalence_gate (для page в карточках).

    Read-only, fail-soft: нет gate / битый gate → пустая карта (page=None).
    """
    gate, err = _read_json(art_dir / VISUAL_GATE_FILENAME)
    if err:
        warnings.append(err)
    page_map: dict[str, Any] = {}
    if isinstance(gate, dict):
        for bp in gate.get("block_pairs") or []:
            if not isinstance(bp, dict):
                continue
            lb, rb = bp.get("left_block_id"), bp.get("right_block_id")
            if lb and bp.get("left_page_number") is not None:
                page_map[lb] = bp.get("left_page_number")
            if rb and bp.get("right_page_number") is not None:
                page_map[rb] = bp.get("right_page_number")
    return page_map


def discover_graphic_vision_grounding_detail(
        session_id: str, pair_id: Optional[str] = None, *, kind: str = "all",
        status: str = "all", item_id: Optional[str] = None,
        limit: int = 100, offset: int = 0) -> dict:
    """Read-only детализация graphic_vision_grounding_report.json.

    Контракт ответа: собственный формат detail (см.
    :mod:`pipeline_v2_grounding_detail`), НЕ стандартный ``_envelope``:

    * ``ok``        — отчёт прочитан, карточки построены;
    * ``not_found`` — отчёта нет (``available=false``);
    * ``error``     — отчёт битый/непригоден (``available=false``), не 500.

    НИЧЕГО не пишет, не запускает, не вызывает модели. ``ValueError`` — только
    на невалидный session_id/pair_id (path traversal).
    """
    if pair_id and _safe_id(pair_id) != pair_id:
        raise ValueError(f"invalid pair_id: {pair_id!r}")
    art_dir = pipeline_v2_artifacts_dir(session_id, pair_id)
    try:
        report, err = _read_json(art_dir / GROUNDING_REPORT_FILENAME)
        if err:
            return {"version": _grounding_detail_mod.DETAIL_VERSION,
                    "kind": _grounding_detail_mod.DETAIL_KIND, "status": "error",
                    "available": False, "session_id": session_id,
                    "pair_id": pair_id,
                    "message": "Graphic vision grounding report could not be read.",
                    "warnings": [err]}
        if report is None:
            return {"version": _grounding_detail_mod.DETAIL_VERSION,
                    "kind": _grounding_detail_mod.DETAIL_KIND,
                    "status": "not_found", "available": False,
                    "session_id": session_id, "pair_id": pair_id,
                    "message": "Graphic vision grounding report not found for "
                               "this pair.", "warnings": []}
        if not isinstance(report, dict):
            return {"version": _grounding_detail_mod.DETAIL_VERSION,
                    "kind": _grounding_detail_mod.DETAIL_KIND, "status": "error",
                    "available": False, "session_id": session_id,
                    "pair_id": pair_id,
                    "message": "Graphic vision grounding report is not a JSON "
                               "object.", "warnings": []}
        warnings: list[str] = []
        page_map = _grounding_page_map(art_dir, warnings)
        detail = _grounding_detail_mod.build_grounding_detail(
            report, session_id=session_id, pair_id=pair_id, page_map=page_map,
            kind=kind, status=status, item_id=item_id, limit=limit, offset=offset)
        if warnings:
            detail["warnings"] = list(detail.get("warnings") or []) + warnings
        # санитайз NaN/Inf + глубины (как у остальных endpoint'ов)
        san_warn: list[str] = []
        detail = _sanitize_payload(detail, san_warn) or detail
        if san_warn:
            detail["warnings"] = list(detail.get("warnings") or []) + san_warn
        return detail
    except Exception as exc:  # noqa: BLE001 — endpoint не должен дать 500
        return {"version": _grounding_detail_mod.DETAIL_VERSION,
                "kind": _grounding_detail_mod.DETAIL_KIND, "status": "error",
                "available": False, "session_id": session_id, "pair_id": pair_id,
                "message": "Graphic vision grounding detail could not be built.",
                "warnings": [f"{type(exc).__name__}: {exc}"]}


def _eap_detail_envelope(status: str, *, session_id: str,
                         pair_id: Optional[str], message: str,
                         warnings: Optional[list[str]] = None) -> dict:
    """not_found/error ответ entity-alignment endpoint'а (detail-формат)."""
    return {
        "version": 1, "kind": ENTITY_ALIGNMENT_KIND, "status": status,
        "available": False, "session_id": session_id, "pair_id": pair_id,
        "source": None, "summary": {}, "pairs": [],
        "unpaired_entities": {"left": [], "right": []},
        "message": message, "warnings": warnings or [],
    }


def discover_entity_alignment_preview(
        session_id: str, pair_id: Optional[str] = None, *,
        classification: str = "all", limit: int = 100, offset: int = 0) -> dict:
    """Найти/собрать entity alignment preview для пары (read-only, fail-soft).

    Статусы ответа (detail-формат, см. pipeline_v2_entity_alignment_detail):

    * ``ok``        — готовый отчёт с диска или собран on-the-fly из артефактов;
    * ``not_found`` — нет ни готового отчёта, ни visual gate с block_pairs;
    * ``error``     — артефакты есть, но непригодны (битый JSON и т.п.), не 500.

    Фильтр ``classification`` и пагинация (``limit``/``offset``, clamp ≤500)
    применяются к ``pairs``; summary и unpaired_entities отдаются целиком.
    НИЧЕГО не пишет (отчёт on-the-fly не кешируется), не запускает, не вызывает
    модели. ``ValueError`` — только на невалидный session_id/pair_id.
    """
    if pair_id and _safe_id(pair_id) != pair_id:
        raise ValueError(f"invalid pair_id: {pair_id!r}")
    art_dir = pipeline_v2_artifacts_dir(session_id, pair_id)
    try:
        return _discover_entity_alignment_preview(
            art_dir, session_id, pair_id,
            classification=classification, limit=limit, offset=offset)
    except Exception as exc:  # noqa: BLE001 — endpoint не должен дать 500
        return _eap_detail_envelope(
            "error", session_id=session_id, pair_id=pair_id,
            message="Entity alignment preview report could not be read.",
            warnings=[f"{type(exc).__name__}: {exc}"])


def _read_entity_mapping_overrides(art_dir: Path, warnings: list[str]) -> Optional[dict]:
    """Прочитать entity_mapping_overrides.json пары (read-only, без mkdir).

    Нет файла → None (manual_mapping не добавляется). Битый → warning + None.
    Читается напрямую из art_dir, чтобы read-only discover не материализовал
    дерево пары.
    """
    ov, err = _read_json(art_dir / ENTITY_MAPPING_OVERRIDES_FILENAME)
    if err:
        warnings.append(err)
        return None
    return ov if isinstance(ov, dict) else None


def _discover_entity_alignment_preview(
        art_dir: Path, session_id: str, pair_id: Optional[str], *,
        classification: str, limit: int, offset: int) -> dict:
    warnings: list[str] = []
    overrides = _read_entity_mapping_overrides(art_dir, warnings)

    # 1) готовый отчёт — отдать как есть (с фильтром/пагинацией)
    ready, err = _read_json(art_dir / ENTITY_ALIGNMENT_PREVIEW_FILENAME)
    if isinstance(ready, dict) and ready.get("kind") == ENTITY_ALIGNMENT_KIND:
        detail = build_entity_alignment_detail(
            ready, session_id=session_id, pair_id=pair_id,
            classification=classification, limit=limit, offset=offset,
            source="ready_report", overrides=overrides, extra_warnings=warnings)
        return _sanitize_payload(detail, detail.setdefault("warnings", [])) or detail
    if err:
        warnings.append(err)
    elif isinstance(ready, dict):
        warnings.append(f"{ENTITY_ALIGNMENT_PREVIEW_FILENAME}: unexpected kind "
                        f"{ready.get('kind')!r} — rebuilt from artifacts")
    elif ready is not None:
        warnings.append(f"{ENTITY_ALIGNMENT_PREVIEW_FILENAME}: expected JSON "
                        f"object, got {type(ready).__name__} — rebuilt")

    # 2) собрать on-the-fly из артефактов dry-run (только чтение). Минимально
    #    нужен visual gate с block_pairs — без него отчёт пуст по построению.
    gate, gerr = _read_json(art_dir / VISUAL_GATE_FILENAME)
    if gerr:
        warnings.append(gerr)
    if not (isinstance(gate, dict) and isinstance(gate.get("block_pairs"), list)):
        if warnings:
            return _eap_detail_envelope(
                "error", session_id=session_id, pair_id=pair_id,
                message="Pipeline V2 artifacts exist but entity alignment "
                        "preview could not be read.", warnings=warnings)
        return _eap_detail_envelope(
            "not_found", session_id=session_id, pair_id=pair_id,
            message=EAP_NOT_FOUND_MESSAGE,
            warnings=warnings)

    def _opt(fname: str) -> Optional[dict]:
        value, e = _read_json(art_dir / fname)
        if e:
            warnings.append(e)
            return None
        return value if isinstance(value, dict) else None

    left_model = _opt(LEFT_MODEL_FILENAME)
    right_model = _opt(RIGHT_MODEL_FILENAME)
    left_graphic = _opt(LEFT_GRAPHIC_FILENAME)
    right_graphic = _opt(RIGHT_GRAPHIC_FILENAME)
    block_matching = _opt(BLOCK_MATCHING_FILENAME)
    block_link_preview = _opt(BLOCK_LINK_PREVIEW_FILENAME)
    graphic_matched = _opt(GRAPHIC_MATCHED_FILENAME)
    grounding = _opt(GROUNDING_REPORT_FILENAME)

    try:
        report = build_entity_alignment_preview_report(
            left_model, right_model, gate,
            block_matching_report=block_matching,
            block_link_preview_report=block_link_preview,
            left_graphic_report=left_graphic, right_graphic_report=right_graphic,
            graphic_matched_report=graphic_matched, grounding_report=grounding)
    except Exception as exc:  # noqa: BLE001 — endpoint не должен дать 500
        return _eap_detail_envelope(
            "error", session_id=session_id, pair_id=pair_id,
            message="Pipeline V2 artifacts exist but entity alignment preview "
                    "could not be built.",
            warnings=warnings + [f"{type(exc).__name__}: {exc}"])

    detail = build_entity_alignment_detail(
        report, session_id=session_id, pair_id=pair_id,
        classification=classification, limit=limit, offset=offset,
        source="built_from_artifacts", overrides=overrides, extra_warnings=warnings)
    return _sanitize_payload(detail, detail.setdefault("warnings", [])) or detail


# ─── link validation (read-only, mark-only) ──────────────────────────────────

LV_NOT_FOUND_MESSAGE = "Link validation report not found for this pair."


def _lv_detail_envelope(status: str, *, session_id: str, pair_id: Optional[str],
                        message: str, warnings: Optional[list[str]] = None) -> dict:
    """not_found/error ответ link-validation endpoint'а (detail-формат)."""
    return {
        "version": 1, "kind": LINK_VALIDATION_KIND, "status": status,
        "available": False, "session_id": session_id, "pair_id": pair_id,
        "source": None, "summary": {}, "items": [],
        "message": message, "warnings": warnings or [],
    }


def discover_link_validation(
        session_id: str, pair_id: Optional[str] = None, *,
        decision: str = "all", agreement: str = "all",
        limit: int = 100, offset: int = 0) -> dict:
    """Найти link validation report для пары (read-only, fail-soft).

    Статусы: ``ok`` (готовый отчёт), ``not_found`` (отчёта нет — runner НЕ
    запускается), ``error`` (битый JSON), не 404/500. Фильтры ``decision`` /
    ``agreement`` и пагинация (``limit`` clamp ≤500) применяются к ``items``;
    summary отдаётся целиком. НИЧЕГО не пишет, не запускает, не вызывает модели.
    Raw prompt/image не отдаются. ``ValueError`` — только на невалидный id.
    """
    if pair_id and _safe_id(pair_id) != pair_id:
        raise ValueError(f"invalid pair_id: {pair_id!r}")
    art_dir = pipeline_v2_artifacts_dir(session_id, pair_id)
    try:
        ready, err = _read_json(art_dir / LINK_VALIDATION_FILENAME)
        if err:
            return _lv_detail_envelope(
                "error", session_id=session_id, pair_id=pair_id,
                message="Link validation report could not be read.",
                warnings=[err])
        if ready is None:
            return _lv_detail_envelope(
                "not_found", session_id=session_id, pair_id=pair_id,
                message=LV_NOT_FOUND_MESSAGE)
        if not isinstance(ready, dict) or ready.get("kind") != LINK_VALIDATION_KIND:
            return _lv_detail_envelope(
                "error", session_id=session_id, pair_id=pair_id,
                message="Link validation report is not a valid report.",
                warnings=[f"{LINK_VALIDATION_FILENAME}: unexpected kind "
                          f"{ready.get('kind') if isinstance(ready, dict) else type(ready).__name__!r}"])
        detail = build_link_validation_detail(
            ready, session_id=session_id, pair_id=pair_id,
            decision=decision, agreement=agreement, limit=limit, offset=offset,
            source="ready_report")
        return _sanitize_payload(detail, detail.setdefault("warnings", [])) or detail
    except Exception as exc:  # noqa: BLE001 — endpoint не должен дать 500
        return _lv_detail_envelope(
            "error", session_id=session_id, pair_id=pair_id,
            message="Link validation report could not be read.",
            warnings=[f"{type(exc).__name__}: {exc}"])


# ─── exclusion preview v2 (read-only, mark-only) ─────────────────────────────

XP_NOT_FOUND_MESSAGE = "Exclusion Preview v2 report not found for this pair."

_XP_CLASSIFICATIONS = {"candidate_exclude", "review_only", "keep",
                       "link_validation_required"}
_XP_SEVERITIES = {"high", "medium", "low"}

# поля, которые никогда не отдаются клиенту (сырые трассировки, debug-дампы)
_XP_RAW_STRIP_KEYS = frozenset({
    "raw_qwen_description", "raw_qwen_text", "reasoning_trace",
    "debug_dump", "raw_full_response", "raw_image_b64",
    "raw_prompt", "raw_response", "trace",
})


def _xp_detail_envelope(status: str, *, session_id: str,
                        pair_id: Optional[str], message: str,
                        warnings: Optional[list[str]] = None) -> dict:
    """not_found/error ответ exclusion-preview endpoint'а (detail-формат)."""
    return {
        "version": 1, "kind": EXCLUSION_PREVIEW_KIND, "status": status,
        "available": False, "session_id": session_id, "pair_id": pair_id,
        "source": None, "summary": {}, "items": [],
        "total_count": 0, "filtered_count": 0,
        "message": message, "warnings": warnings or [],
    }


def discover_exclusion_preview(
        session_id: str, pair_id: Optional[str] = None, *,
        classification: str = "all", severity: str = "all",
        limit: int = 100, offset: int = 0) -> dict:
    """Найти Exclusion Preview v2 report для пары (read-only, fail-soft).

    Статусы: ``ok`` (готовый отчёт), ``not_found`` (отчёта нет — runner НЕ
    запускается), ``error`` (битый JSON), не 404/500. Фильтры
    ``classification`` / ``severity`` и пагинация (``limit`` clamp ≤500)
    применяются к ``items``; summary отдаётся целиком. НИЧЕГО не пишет, не
    запускает, не вызывает модели. Mark-only инварианты
    (auto_apply/enforce_allowed/use_as_grounded_fact=false) гарантированы на
    всех item'ах ответа. ``ValueError`` — только на невалидный id.
    """
    if pair_id and _safe_id(pair_id) != pair_id:
        raise ValueError(f"invalid pair_id: {pair_id!r}")
    art_dir = pipeline_v2_artifacts_dir(session_id, pair_id)
    try:
        ready, err = _read_json(art_dir / EXCLUSION_PREVIEW_FILENAME)
        if err:
            return _xp_detail_envelope(
                "error", session_id=session_id, pair_id=pair_id,
                message="Exclusion Preview v2 report could not be read.",
                warnings=[err])
        if ready is None:
            return _xp_detail_envelope(
                "not_found", session_id=session_id, pair_id=pair_id,
                message=XP_NOT_FOUND_MESSAGE)
        if (not isinstance(ready, dict)
                or ready.get("kind") != EXCLUSION_PREVIEW_KIND):
            return _xp_detail_envelope(
                "error", session_id=session_id, pair_id=pair_id,
                message="Exclusion Preview v2 report is not a valid report.",
                warnings=[f"{EXCLUSION_PREVIEW_FILENAME}: unexpected kind "
                          f"{ready.get('kind') if isinstance(ready, dict) else type(ready).__name__!r}"])

        warnings: list[str] = []
        items = list(ready.get("items") or [])
        total_count = len(items)

        # filter by classification
        if classification != "all":
            if classification not in _XP_CLASSIFICATIONS:
                return _xp_detail_envelope(
                    "error", session_id=session_id, pair_id=pair_id,
                    message=f"Invalid classification filter: {classification!r}")
            items = [it for it in items if it.get("classification") == classification]

        # filter by severity
        if severity != "all":
            if severity not in _XP_SEVERITIES:
                return _xp_detail_envelope(
                    "error", session_id=session_id, pair_id=pair_id,
                    message=f"Invalid severity filter: {severity!r}")
            items = [it for it in items if it.get("severity") == severity]

        filtered_count = len(items)

        # clamp limit
        limit = min(max(0, limit), 500)

        # paginate
        items = items[offset:offset + limit]

        # force mark-only on every returned item + strip raw/debug fields
        for it in items:
            it["use_as_grounded_fact"] = False
            it["auto_apply"] = False
            it["enforce_allowed"] = False
            for key in _XP_RAW_STRIP_KEYS:
                it.pop(key, None)

        # load operator review overrides (fail-soft) and wire into items
        operator_summary: dict = {
            "total": 0,
            "approve_exclude": 0,
            "reject_exclude": 0,
            "needs_review": 0,
            "keep": 0,
            "run_link_validation": 0,
        }
        try:
            if session_id and pair_id:
                ov_data = _excl_review_mod.read_exclusion_review_overrides(
                    session_id, pair_id)
                if ov_data.get("status") == "ok":
                    for it in items:
                        rev = _excl_review_mod.operator_review_for_item(ov_data, it)
                        it["operator_review"] = rev
                    op_sum = _excl_review_mod.summarize_decisions(ov_data)
                    operator_summary = op_sum
        except Exception:  # noqa: BLE001 — operator_review is fail-soft
            pass

        base_summary = dict(ready.get("summary") or {})
        base_summary["operator_review"] = operator_summary

        detail: dict[str, Any] = {
            "version": 1, "kind": EXCLUSION_PREVIEW_KIND, "status": "ok",
            "available": True, "session_id": session_id, "pair_id": pair_id,
            "source": "ready_report",
            "summary": base_summary,
            "items": items,
            "total_count": total_count,
            "filtered_count": filtered_count,
            "limit": limit, "offset": offset,
            "warnings": warnings,
        }
        san_warns: list[str] = []
        detail = _sanitize_payload(detail, san_warns) or detail
        if san_warns:
            detail["warnings"] = list(detail.get("warnings") or []) + san_warns
        return detail
    except Exception as exc:  # noqa: BLE001 — endpoint не должен дать 500
        return _xp_detail_envelope(
            "error", session_id=session_id, pair_id=pair_id,
            message="Exclusion Preview v2 report could not be read.",
            warnings=[f"{type(exc).__name__}: {exc}"])


SR_NOT_FOUND_MESSAGE = "Skip readiness report not found for this pair."
_SR_READINESS_VALUES = frozenset({"ready_to_skip", "blocked", "needs_review", "keep"})
_SR_REPORT_KIND = "skip_readiness_report_v1"


def _sr_detail_envelope(status: str, *, session_id: str,
                        pair_id: Optional[str], message: str,
                        warnings: Optional[list[str]] = None) -> dict:
    """not_found/error ответ skip-readiness endpoint'а (detail-формат)."""
    return {
        "version": 1, "kind": _SR_REPORT_KIND, "status": status,
        "available": False, "session_id": session_id, "pair_id": pair_id,
        "source": None, "summary": {}, "items": [],
        "total_count": 0, "filtered_count": 0,
        "auto_enforce_enabled": False, "enforce_allowed": False,
        "message": message, "warnings": warnings or [],
    }


def discover_skip_readiness(
        session_id: str, pair_id: Optional[str] = None, *,
        readiness: str = "all",
        limit: int = 100, offset: int = 0) -> dict:
    """Найти skip_readiness_report для пары (read-only, fail-soft).

    Статусы: ``ok`` (готовый отчёт), ``not_found`` (отчёта нет — runner НЕ
    запускается), ``error`` (битый JSON). Фильтр ``readiness`` и пагинация
    (``limit`` clamp ≤500) применяются к ``items``; summary отдаётся целиком.
    НИЧЕГО не пишет, не запускает, не вызывает модели. Mark-only инварианты
    (auto_apply/enforce_allowed/requires_explicit_operator_approval) форсируются
    на всех item'ах ответа. ``ValueError`` — только на невалидный id.
    """
    if pair_id and _safe_id(pair_id) != pair_id:
        raise ValueError(f"invalid pair_id: {pair_id!r}")
    art_dir = pipeline_v2_artifacts_dir(session_id, pair_id)
    try:
        ready, err = _read_json(art_dir / SKIP_READINESS_FILENAME)
        if err:
            return _sr_detail_envelope(
                "error", session_id=session_id, pair_id=pair_id,
                message="Skip readiness report could not be read.",
                warnings=[err])
        if ready is None:
            return _sr_detail_envelope(
                "not_found", session_id=session_id, pair_id=pair_id,
                message=SR_NOT_FOUND_MESSAGE)
        if (not isinstance(ready, dict)
                or ready.get("kind") != _SR_REPORT_KIND):
            return _sr_detail_envelope(
                "error", session_id=session_id, pair_id=pair_id,
                message="Skip readiness report is not a valid report.",
                warnings=[f"{SKIP_READINESS_FILENAME}: unexpected kind "
                          f"{ready.get('kind') if isinstance(ready, dict) else type(ready).__name__!r}"])

        warnings: list[str] = []
        items = list(ready.get("items") or [])
        total_count = len(items)

        # filter by readiness_status
        if readiness != "all":
            if readiness not in _SR_READINESS_VALUES:
                return _sr_detail_envelope(
                    "error", session_id=session_id, pair_id=pair_id,
                    message=f"Invalid readiness filter: {readiness!r}")
            items = [it for it in items if it.get("readiness_status") == readiness]

        filtered_count = len(items)

        # clamp limit
        limit = min(max(0, limit), 500)

        # paginate
        items = items[offset:offset + limit]

        # force mark-only invariants + strip raw/debug fields
        for it in items:
            it["auto_apply"] = False
            it["enforce_allowed"] = False
            it["requires_explicit_operator_approval"] = True
            for key in _XP_RAW_STRIP_KEYS:
                it.pop(key, None)

        detail: dict[str, Any] = {
            "version": 1, "kind": _SR_REPORT_KIND, "status": "ok",
            "available": True, "session_id": session_id, "pair_id": pair_id,
            "source": "ready_report",
            "auto_enforce_enabled": False,
            "enforce_allowed": False,
            "summary": dict(ready.get("summary") or {}),
            "items": items,
            "total_count": total_count,
            "filtered_count": filtered_count,
            "limit": limit, "offset": offset,
            "warnings": warnings,
        }
        san_warns: list[str] = []
        detail = _sanitize_payload(detail, san_warns) or detail
        if san_warns:
            detail["warnings"] = list(detail.get("warnings") or []) + san_warns
        return detail
    except Exception as exc:  # noqa: BLE001 — endpoint не должен дать 500
        return _sr_detail_envelope(
            "error", session_id=session_id, pair_id=pair_id,
            message="Skip readiness report could not be read.",
            warnings=[f"{type(exc).__name__}: {exc}"])


__all__ = [
    "PIPELINE_V2_DIRNAME",
    "UI_PAYLOAD_FILENAME",
    "SUMMARY_FILENAME",
    "ENTITY_DIFF_FILENAME",
    "DELTA_EXPLANATION_FILENAME",
    "BLOCK_LINK_PREVIEW_FILENAME",
    "BLOCK_MATCHING_FILENAME",
    "LEFT_MODEL_FILENAME",
    "RIGHT_MODEL_FILENAME",
    "VISUAL_GATE_FILENAME",
    "GROUNDING_REPORT_FILENAME",
    "ENTITY_ALIGNMENT_PREVIEW_FILENAME",
    "LINK_VALIDATION_FILENAME",
    "EXCLUSION_PREVIEW_FILENAME",
    "EXCLUSION_REVIEW_OVERRIDES_FILENAME",
    "SKIP_READINESS_FILENAME",
    "discover_pipeline_v2_payload",
    "discover_block_link_preview",
    "discover_graphic_vision_grounding_detail",
    "discover_entity_alignment_preview",
    "discover_link_validation",
    "discover_exclusion_preview",
    "discover_skip_readiness",
    "pipeline_v2_artifacts_dir",
    "list_pairs_with_artifacts",
    "resolve_session_dir",
]
