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

PIPELINE_V2_DIRNAME = "pipeline_v2"
UI_PAYLOAD_FILENAME = "pipeline_v2_ui_payload.json"
SUMMARY_FILENAME = "pipeline_v2_summary.json"
ENTITY_DIFF_FILENAME = "entity_diff_report.json"
DELTA_EXPLANATION_FILENAME = "delta_explanation_report.json"
LEFT_GRAPHIC_FILENAME = "left_graphic_descriptor_report.json"
RIGHT_GRAPHIC_FILENAME = "right_graphic_descriptor_report.json"

NOT_FOUND_MESSAGE = "Pipeline V2 artifacts not found for this session."


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


def _json_safe(value: Any, hits: list[int]) -> Any:
    """Заменить не-финитные float'ы (NaN/Inf из artifact-JSON) на None.

    json.loads принимает NaN/Infinity, а сериализация ответа на них падает —
    payload должен оставаться строгим JSON.
    """
    if isinstance(value, float) and not math.isfinite(value):
        hits.append(1)
        return None
    if isinstance(value, dict):
        return {k: _json_safe(v, hits) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v, hits) for v in value]
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

    gdr = None
    if isinstance(left_g, dict) or isinstance(right_g, dict):
        gdr = {"left": left_g if isinstance(left_g, dict) else None,
               "right": right_g if isinstance(right_g, dict) else None}
    return build_pipeline_v2_ui_payload(
        summary,
        diff if isinstance(diff, dict) else None,
        de if isinstance(de, dict) else None,
        graphic_descriptor_reports=gdr,
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


__all__ = [
    "PIPELINE_V2_DIRNAME",
    "UI_PAYLOAD_FILENAME",
    "SUMMARY_FILENAME",
    "ENTITY_DIFF_FILENAME",
    "DELTA_EXPLANATION_FILENAME",
    "discover_pipeline_v2_payload",
    "pipeline_v2_artifacts_dir",
    "list_pairs_with_artifacts",
    "resolve_session_dir",
]
