"""Каноничная конфигурация Stage Comparison.

Пользовательская модель: у объекта одна актуальная «рабочая» конфигурация —
какие PDF сопоставлены, в каком режиме анализируются, как упорядочены,
и какая сессия является каноничной. Эта конфигурация перезаписывается
кнопкой «Сохранить как каноничную»; история сессий обычному пользователю
не показывается.

Файл живёт по пути ``backend/app/data/stage_comparison_saved_config.json``,
формат (config_version = 2):

    {
      "config_version": 2,
      "stage_a_path": "<абсолютный путь к stage_1>",
      "stage_b_path": "<абсолютный путь к stage_2>",
      "object_label": "<человекочитаемое название объекта>",
      "stage_a_label": "stage_1",
      "stage_b_label": "stage_2",
      "canonical_session_id": "<sid от save-canonical>",
      "config_hash": "<sha256(pairs_summary)>",
      "pairs": [
        {
          "pair_id": "...",
          "left_filename": "...",
          "right_filename": "...",
          "disabled": false,
          "manual_links_count": 3,
          "order": 1
        },
        ...
      ],
      "saved_at": "<ISO timestamp UTC>",
      "updated_at": "<ISO timestamp UTC>",
      "updated_by": "<optional>",
      "note": "<свободная пометка>"
    }

Backward compatibility: load_saved_config возвращает legacy формат
(stage_a_path/stage_b_path) для существующих UI-точек, плюс новые поля.
Минимально валидный конфиг — непустые stage_a_path/stage_b_path.

Никаких внешних API. Никаких секретов. Чистый локальный JSON.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Optional

logger = logging.getLogger(__name__)


CONFIG_VERSION = 2

# Путь к файлу с saved config. Лежит рядом с usage_data.json/batch_queue.json
# в backend/app/data/. Можно переопределить через env для тестов.
_DEFAULT_SAVED_CONFIG_PATH = Path(__file__).resolve().parents[2] / "data" / "stage_comparison_saved_config.json"


def _config_path() -> Path:
    override = os.environ.get("STAGE_COMPARISON_SAVED_CONFIG_PATH", "").strip()
    if override:
        return Path(override)
    return _DEFAULT_SAVED_CONFIG_PATH


def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalize_pair_summary(pair: dict, *, order: int) -> dict:
    """Превратить pair dict (как из store._aggregate_session) в компактный summary.

    Берём только поля конфигурации, не результатов анализа.
    """
    left = pair.get("left") or {}
    right = pair.get("right") or {}
    links = pair.get("links") or []
    return {
        "pair_id": str(pair.get("id") or "").strip(),
        "left_filename": (left.get("filename") or "").strip() or None,
        "right_filename": (right.get("filename") or "").strip() or None,
        "left_pdf_path": (left.get("pdf_path") or "").strip() or None,
        "right_pdf_path": (right.get("pdf_path") or "").strip() or None,
        "disabled": str(pair.get("status") or "").strip() == "disabled",
        "status": (pair.get("status") or "").strip() or None,
        "manual_links_count": len([l for l in links if isinstance(l, dict)]),
        "order": int(order),
    }


def _compute_config_hash(pairs_summary: list[dict]) -> str:
    """sha256 от стабильной (отсортированной по order) сериализации pairs.

    Используется для invalidation analysis artifacts: если pairs изменились,
    config_hash меняется, UI может показать «результаты могут быть устаревшими».
    """
    canon = sorted(pairs_summary, key=lambda p: p.get("order") or 0)
    blob = json.dumps(canon, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def load_saved_config() -> Optional[dict[str, Any]]:
    """Прочитать saved config. None если файла нет или он битый.

    Не падает на любых I/O ошибках — UI должен корректно обработать
    отсутствие config'а (показать disabled кнопку).
    """
    p = _config_path()
    if not p.exists():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("saved_config: read failed for %s: %s", p, exc)
        return None
    if not isinstance(data, dict):
        return None
    # Минимально валидный config — stage_a_path + stage_b_path должны быть
    # непустыми строками. Остальные поля опциональные.
    sa = (data.get("stage_a_path") or "").strip()
    sb = (data.get("stage_b_path") or "").strip()
    if not sa or not sb:
        return None
    # Подстелим soft-defaults для UI: legacy v1 файлы без config_version /
    # canonical_session_id / pairs не валятся.
    data.setdefault("config_version", 1)
    data.setdefault("canonical_session_id", None)
    data.setdefault("config_hash", None)
    data.setdefault("pairs", [])
    data.setdefault("updated_at", data.get("saved_at"))
    return data


def save_saved_config(
    *,
    stage_a_path: str,
    stage_b_path: str,
    object_label: Optional[str] = None,
    stage_a_label: Optional[str] = None,
    stage_b_label: Optional[str] = None,
    note: Optional[str] = None,
    canonical_session_id: Optional[str] = None,
    pairs: Optional[Iterable[dict]] = None,
    updated_by: Optional[str] = None,
) -> dict[str, Any]:
    """Записать saved config. Возвращает сохранённый dict.

    Делает atomic write через temp+rename, чтобы при сбое не оставить
    битый файл.

    Если ``canonical_session_id`` + ``pairs`` переданы — это полная
    каноничная конфигурация (config_version = 2). Иначе — legacy режим:
    только пути (для обратной совместимости со старой кнопкой).

    Raises:
        ValueError: если stage_a_path/stage_b_path пустые.
        OSError: при сбое записи на диск.
    """
    sa = (stage_a_path or "").strip()
    sb = (stage_b_path or "").strip()
    if not sa or not sb:
        raise ValueError("stage_a_path и stage_b_path обязательны")

    now = _utc_now()
    pairs_list: list[dict] = []
    if pairs:
        for idx, raw in enumerate(pairs):
            if not isinstance(raw, dict):
                continue
            normalized = _normalize_pair_summary(raw, order=idx + 1)
            if not normalized["pair_id"]:
                continue
            pairs_list.append(normalized)
    has_session_info = bool((canonical_session_id or "").strip()) and bool(pairs_list)

    payload: dict[str, Any] = {
        "config_version": CONFIG_VERSION if has_session_info else 1,
        "stage_a_path": sa,
        "stage_b_path": sb,
        "object_label": (object_label or "").strip() or None,
        "stage_a_label": (stage_a_label or "").strip() or None,
        "stage_b_label": (stage_b_label or "").strip() or None,
        "canonical_session_id": (canonical_session_id or "").strip() or None,
        "pairs": pairs_list,
        "config_hash": _compute_config_hash(pairs_list) if pairs_list else None,
        "saved_at": now,
        "updated_at": now,
        "updated_by": (updated_by or "").strip() or None,
        "note": (note or "").strip() or None,
    }

    p = _config_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)
    return payload


def clear_saved_config() -> bool:
    """Удалить файл saved config. Возвращает True если что-то удалили."""
    p = _config_path()
    if not p.exists():
        return False
    try:
        p.unlink()
        return True
    except OSError as exc:
        logger.warning("saved_config: delete failed for %s: %s", p, exc)
        return False


__all__ = [
    "CONFIG_VERSION",
    "load_saved_config",
    "save_saved_config",
    "clear_saved_config",
    "_compute_config_hash",
]
