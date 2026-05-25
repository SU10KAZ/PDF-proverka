"""Снимки конфигурации пары PDF (links + page_alignment) для повторного применения.

Контекст: при каждом `POST /sessions` создаётся новая сессия с уникальным
`session_id`. Если пользователь уже настроил связи блоков и карту страниц для
пары PDF (А ↔ B), эти настройки лежат в `comparison/sessions/<old_sid>/pairs/<pid>/`
и для новой сессии бесполезны. Этот модуль позволяет сохранить «шаблон» пары
по детерминированному ключу (от полных путей PDF-файлов) и автоматически
применять его при следующем создании сессии с теми же файлами.

Ключ идентификации: SHA-1(left_pdf_path | right_pdf_path) — пользователь
выбрал «по полным путям». Если PDF переместили в другую папку — шаблон
не подтянется (это намеренное поведение, см. AskUserQuestion 2026-05-23).

Хранилище: `comparison/templates/<key>.json`. Без TTL, без эвикции —
ручное удаление пользователем (через POST .../clear-template).
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from . import paths as paths_mod

logger = logging.getLogger(__name__)

TEMPLATE_VERSION = 1


def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def template_key(left_pdf_path: str | None, right_pdf_path: str | None) -> Optional[str]:
    """Стабильный SHA-1 от пары полных путей. Если любого нет — None."""
    if not left_pdf_path or not right_pdf_path:
        return None
    # Нормализуем (resolve символические ссылки + лишние слеши), чтобы один и
    # тот же файл, переданный как абсолютный/относительный/со //, давал тот же
    # ключ. Без resolve() — пути могут не совпасть после смены cwd.
    try:
        lp = str(Path(left_pdf_path).resolve())
        rp = str(Path(right_pdf_path).resolve())
    except OSError:
        lp = str(left_pdf_path)
        rp = str(right_pdf_path)
    blob = lp + "|" + rp
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()


def _read_template_file(key: str) -> Optional[dict]:
    try:
        p = paths_mod.pair_template_path(key)
    except ValueError:
        return None
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("pair_template: cannot read %s: %s", p, exc)
        return None


def _write_template_file(key: str, payload: dict) -> Path:
    p = paths_mod.pair_template_path(key)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(p)
    return p


def find_template(left_pdf_path: str | None, right_pdf_path: str | None) -> Optional[dict]:
    """Найти существующий шаблон по identity пути или вернуть None."""
    key = template_key(left_pdf_path, right_pdf_path)
    if key is None:
        return None
    return _read_template_file(key)


# ─── Save / apply ────────────────────────────────────────────────────────


def save_template(session_id: str, pair_id: str) -> dict:
    """Снять снимок links + page_alignment для пары и записать в шаблон.

    Возвращает payload шаблона (с ключом и метаданными). Импортируем store
    лениво — store импортирует нас же в create_session.
    """
    from . import store as store_mod  # noqa: WPS433 (lazy: cyclic)

    session = store_mod.get_session(session_id)
    if session is None:
        raise KeyError("session_not_found")
    pair = next((p for p in (session.get("pairs") or []) if p.get("id") == pair_id), None)
    if pair is None:
        raise KeyError("pair_not_found")

    left = pair.get("left") or {}
    right = pair.get("right") or {}
    left_pdf_path = left.get("pdf_path")
    right_pdf_path = right.get("pdf_path")
    key = template_key(left_pdf_path, right_pdf_path)
    if key is None:
        raise ValueError("missing_pdf_paths")

    # Снимок: текущие links + alignment пары.
    links_p = paths_mod.links_path(session_id, pair_id)
    alignment_p = paths_mod.page_alignment_path(session_id, pair_id)
    links_data: list = []
    if links_p.exists():
        try:
            raw = json.loads(links_p.read_text(encoding="utf-8"))
            if isinstance(raw, list):
                links_data = raw
        except json.JSONDecodeError:
            pass
    alignment_data: dict = {}
    if alignment_p.exists():
        try:
            raw = json.loads(alignment_p.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                alignment_data = raw
        except json.JSONDecodeError:
            pass

    payload = {
        "version": TEMPLATE_VERSION,
        "key": key,
        "left_pdf_path": left_pdf_path,
        "right_pdf_path": right_pdf_path,
        "left_pdf_name": left.get("filename"),
        "right_pdf_name": right.get("filename"),
        "saved_at": _utc_now(),
        "source_session_id": session_id,
        "source_pair_id": pair_id,
        "links": links_data,
        "links_count": len(links_data),
        "page_alignment": alignment_data,
    }
    p = _write_template_file(key, payload)
    logger.info("pair_template: saved key=%s links=%d to %s", key, len(links_data), p)
    return payload


def apply_template(session_id: str, pair_id: str, template: Optional[dict] = None) -> dict:
    """Применить шаблон к существующей паре: перезаписать links + alignment.

    Возвращает {"applied": bool, "links_applied": n, "alignment_applied": bool,
                "template_key": key, "saved_at": "...", "source": "..."}.
    """
    from . import store as store_mod  # noqa: WPS433

    session = store_mod.get_session(session_id)
    if session is None:
        raise KeyError("session_not_found")
    pair = next((p for p in (session.get("pairs") or []) if p.get("id") == pair_id), None)
    if pair is None:
        raise KeyError("pair_not_found")

    if template is None:
        left = pair.get("left") or {}
        right = pair.get("right") or {}
        template = find_template(left.get("pdf_path"), right.get("pdf_path"))
    if template is None:
        return {"applied": False, "reason": "no_template"}

    links = template.get("links") or []
    alignment = template.get("page_alignment") or {}

    links_p = paths_mod.links_path(session_id, pair_id)
    links_p.parent.mkdir(parents=True, exist_ok=True)
    tmp = links_p.with_suffix(links_p.suffix + ".tmp")
    tmp.write_text(json.dumps(links, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(links_p)

    alignment_applied = False
    if alignment and isinstance(alignment, dict):
        alignment_p = paths_mod.page_alignment_path(session_id, pair_id)
        # Сохраняем left_page_count/right_page_count актуальной пары, остальное берём из шаблона.
        try:
            current = json.loads(alignment_p.read_text(encoding="utf-8")) if alignment_p.exists() else {}
        except (OSError, json.JSONDecodeError):
            current = {}
        merged = dict(alignment)
        for k in ("left_page_count", "right_page_count"):
            if k in current:
                merged[k] = current[k]
        merged["updated_at"] = _utc_now()
        atmp = alignment_p.with_suffix(alignment_p.suffix + ".tmp")
        atmp.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
        atmp.replace(alignment_p)
        alignment_applied = True

    # Помечаем пару, что шаблон применён.
    pair_meta = pair  # store.get_session возвращает агрегированный snapshot;
    # запишем флаг в pair.json (через store helper).
    try:
        pj_path = paths_mod.pair_json_path(session_id, pair_id)
        if pj_path.exists():
            try:
                pair_on_disk = json.loads(pj_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                pair_on_disk = {}
            if isinstance(pair_on_disk, dict):
                pair_on_disk["template_applied"] = True
                pair_on_disk["template_applied_at"] = _utc_now()
                pair_on_disk["template_key"] = template.get("key")
                pair_on_disk["template_saved_at"] = template.get("saved_at")
                pair_on_disk["template_source_session_id"] = template.get("source_session_id")
                pjtmp = pj_path.with_suffix(pj_path.suffix + ".tmp")
                pjtmp.write_text(json.dumps(pair_on_disk, ensure_ascii=False, indent=2), encoding="utf-8")
                pjtmp.replace(pj_path)
    except OSError as exc:
        logger.warning("pair_template: cannot mark pair.json applied: %s", exc)

    return {
        "applied": True,
        "links_applied": len(links),
        "alignment_applied": alignment_applied,
        "template_key": template.get("key"),
        "saved_at": template.get("saved_at"),
        "source_session_id": template.get("source_session_id"),
        "source_pair_id": template.get("source_pair_id"),
    }


def clear_applied_template(session_id: str, pair_id: str) -> dict:
    """Снять пометку template_applied с пары (links и alignment не трогаем —
    пользователь может захотеть оставить их или отредактировать вручную).
    Это просто реакция на «не показывать баннер 'применён шаблон'».
    """
    pj_path = paths_mod.pair_json_path(session_id, pair_id)
    if not pj_path.exists():
        raise KeyError("pair_not_found")
    try:
        pair_on_disk = json.loads(pj_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        pair_on_disk = {}
    if not isinstance(pair_on_disk, dict):
        return {"ok": False}
    for k in (
        "template_applied", "template_applied_at",
        "template_key", "template_saved_at", "template_source_session_id",
    ):
        pair_on_disk.pop(k, None)
    pjtmp = pj_path.with_suffix(pj_path.suffix + ".tmp")
    pjtmp.write_text(json.dumps(pair_on_disk, ensure_ascii=False, indent=2), encoding="utf-8")
    pjtmp.replace(pj_path)
    return {"ok": True}


def template_status(session_id: str, pair_id: str) -> dict:
    """Состояние шаблона для пары:
      • has_template — найден ли файл шаблона по identity путей пары;
      • applied      — есть ли пометка в pair.json;
      • template     — лёгкая метаданная (без полного links-payload).
    """
    from . import store as store_mod  # noqa: WPS433

    session = store_mod.get_session(session_id)
    if session is None:
        raise KeyError("session_not_found")
    pair = next((p for p in (session.get("pairs") or []) if p.get("id") == pair_id), None)
    if pair is None:
        raise KeyError("pair_not_found")

    left = pair.get("left") or {}
    right = pair.get("right") or {}
    key = template_key(left.get("pdf_path"), right.get("pdf_path"))
    tpl = _read_template_file(key) if key else None
    return {
        "ok": True,
        "key": key,
        "has_template": bool(tpl),
        "applied": bool(pair.get("template_applied")),
        "applied_at": pair.get("template_applied_at"),
        "template": (
            {
                "key": tpl.get("key"),
                "saved_at": tpl.get("saved_at"),
                "links_count": tpl.get("links_count") or len(tpl.get("links") or []),
                "left_pdf_name": tpl.get("left_pdf_name"),
                "right_pdf_name": tpl.get("right_pdf_name"),
                "source_session_id": tpl.get("source_session_id"),
            } if tpl else None
        ),
    }


__all__ = [
    "TEMPLATE_VERSION",
    "template_key",
    "find_template",
    "save_template",
    "apply_template",
    "clear_applied_template",
    "template_status",
]
