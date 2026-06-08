"""Хранение сессий сравнения стадий + бизнес-методы (создать, обновить, render).

Новое (v2) расположение runtime-данных:
  comparison/                          (см. paths.py; COMPARISON_ROOT env override)
    index.json
    sessions/
      <session_id>/
        session.json
        pairs/
          <pair_id>/
            pair.json
            page_alignment.json
            links.json
            graphic_diffs.json
            text_diff.json            (опционально)
            pages/left|right/page_NNNN.png
            crops/left|right/<block>.png

Для обратной совместимости остаётся legacy-fallback на старое расположение
`backend/app/data/stage_comparison_sessions/`: read-only, новые сессии туда
НЕ создаются.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from . import alignment as alignment_mod
from . import blocks as blocks_mod
from . import paths as paths_mod
from . import scanner as scanner_mod

logger = logging.getLogger(__name__)

_lock = threading.RLock()


# ─── Helpers ─────────────────────────────────────────────────────────────

def _utc_now() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_id(prefix: str = "", n: int = 16) -> str:
    return f"{prefix}{uuid.uuid4().hex[:n]}"


def _atomic_write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _read_json(path: Path) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


def _safe_id(value: str) -> str:
    """Sanitize id, like paths._safe_id but without raising for our internal usage."""
    safe = "".join(c for c in (value or "") if c.isalnum() or c in "-_")
    return safe


# Сохраняемое имя для обратной совместимости со старым кодом.
# Указывает на legacy directory — он же fallback для чтения.
SESSIONS_DIR = paths_mod.LEGACY_SESSIONS_DIR


# ─── PDF page count helper ───────────────────────────────────────────────

def _import_fitz():
    try:
        import fitz  # PyMuPDF
        return fitz
    except ImportError as exc:
        raise RuntimeError("PyMuPDF not installed: pip install PyMuPDF") from exc


def _pdf_page_count(pdf_path: str | None) -> int:
    if not pdf_path:
        return 0
    p = Path(pdf_path)
    if not p.exists() or not p.is_file():
        return 0
    try:
        fitz = _import_fitz()
        doc = fitz.open(str(p))
        try:
            return int(doc.page_count or 0)
        finally:
            doc.close()
    except Exception:
        return 0


# ─── Index ───────────────────────────────────────────────────────────────

def _read_index() -> dict:
    raw = _read_json(paths_mod.index_json_path())
    if isinstance(raw, dict) and isinstance(raw.get("sessions"), list):
        return raw
    return {"sessions": []}


def _update_index(session_meta: dict) -> None:
    idx = _read_index()
    sid = session_meta.get("id")
    sessions = [s for s in idx.get("sessions", []) if s.get("id") != sid]
    sessions.insert(0, session_meta)
    idx["sessions"] = sessions
    _atomic_write_json(paths_mod.index_json_path(), idx)


def _remove_from_index(session_id: str) -> None:
    idx = _read_index()
    sessions = [s for s in idx.get("sessions", []) if s.get("id") != session_id]
    idx["sessions"] = sessions
    _atomic_write_json(paths_mod.index_json_path(), idx)


# ─── Session persistence (new layout) ────────────────────────────────────

def _save_session_meta(session_id: str, payload: dict) -> None:
    _atomic_write_json(paths_mod.session_json_path(session_id), payload)


def _load_session_meta(session_id: str) -> dict | None:
    data = _read_json(paths_mod.session_json_path(session_id))
    if isinstance(data, dict) and data.get("id"):
        return data
    return None


_PAIR_PASSTHROUGH_FIELDS = (
    "template_applied", "template_applied_at",
    "template_key", "template_saved_at", "template_source_session_id",
    # Unified pipeline: режим анализа пары.
    #   "block_links"            — обычный режим, ожидаются связи блоков (default)
    #   "concept_no_block_links" — концептуальный анализ enriched MD целиком,
    #                              отсутствие связей блоков не считается проблемой.
    "analysis_mode",
    "analysis_mode_updated_at",
)


# Допустимые значения analysis_mode (см. unified pipeline).
ALLOWED_ANALYSIS_MODES = ("block_links", "concept_no_block_links")
DEFAULT_ANALYSIS_MODE = "block_links"


def _save_pair(session_id: str, pair: dict) -> None:
    pair_id = pair["id"]
    payload = {
        "id": pair_id,
        "status": pair.get("status"),
        "match_score": pair.get("match_score"),
        "left": pair.get("left"),
        "right": pair.get("right"),
    }
    for k in _PAIR_PASSTHROUGH_FIELDS:
        if k in pair:
            payload[k] = pair[k]
    _atomic_write_json(paths_mod.pair_json_path(session_id, pair_id), payload)


def _load_pair_meta(session_id: str, pair_id: str) -> dict | None:
    data = _read_json(paths_mod.pair_json_path(session_id, pair_id))
    if isinstance(data, dict) and data.get("id"):
        return data
    return None


# ── Analysis mode helpers ───────────────────────────────────────────────


def get_pair_analysis_mode(session_id: str, pair_id: str) -> str:
    """Текущий analysis_mode пары. По умолчанию `block_links`.

    Не бросает исключений, если pair.json не найден — возвращает default,
    чтобы старые пары без поля работали как и раньше.
    """
    meta = _load_pair_meta(session_id, pair_id) or {}
    mode = str(meta.get("analysis_mode") or DEFAULT_ANALYSIS_MODE).strip()
    if mode not in ALLOWED_ANALYSIS_MODES:
        return DEFAULT_ANALYSIS_MODE
    return mode


def set_pair_analysis_mode(session_id: str, pair_id: str, mode: str) -> dict:
    """Сохранить новый analysis_mode в pair.json.

    Бросает KeyError если pair.json не найден, ValueError если mode невалиден.
    Возвращает обновлённый pair-meta (dict).
    """
    if mode not in ALLOWED_ANALYSIS_MODES:
        raise ValueError(
            f"invalid_analysis_mode: {mode!r}; allowed {ALLOWED_ANALYSIS_MODES}"
        )
    meta = _load_pair_meta(session_id, pair_id)
    if meta is None:
        raise KeyError("pair_not_found")
    meta["analysis_mode"] = mode
    meta["analysis_mode_updated_at"] = _utc_now()
    _save_pair(session_id, meta)
    return meta


def _save_links(session_id: str, pair_id: str, links: list[dict]) -> None:
    _atomic_write_json(paths_mod.links_path(session_id, pair_id), links)


def _load_links(session_id: str, pair_id: str) -> list[dict]:
    data = _read_json(paths_mod.links_path(session_id, pair_id))
    return data if isinstance(data, list) else []


def _save_graphic_diffs(session_id: str, pair_id: str, diffs: list[dict]) -> None:
    _atomic_write_json(paths_mod.graphic_diffs_path(session_id, pair_id), diffs)


def _load_graphic_diffs(session_id: str, pair_id: str) -> list[dict]:
    data = _read_json(paths_mod.graphic_diffs_path(session_id, pair_id))
    return data if isinstance(data, list) else []


def _save_alignment(session_id: str, pair_id: str, alignment: dict) -> None:
    _atomic_write_json(paths_mod.page_alignment_path(session_id, pair_id), alignment)


def _load_alignment_raw(session_id: str, pair_id: str) -> dict | None:
    data = _read_json(paths_mod.page_alignment_path(session_id, pair_id))
    if isinstance(data, dict) and isinstance(data.get("items"), list):
        return data
    return None


# ─── Legacy fallback ──────────────────────────────────────────────────────

def _load_legacy_session(session_id: str) -> dict | None:
    path = paths_mod.legacy_session_json_path(session_id)
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return None


# ─── Aggregated session loader ────────────────────────────────────────────

def _list_pair_ids(session_id: str) -> list[str]:
    root = paths_mod.pairs_root(session_id)
    return sorted([d.name for d in root.iterdir() if d.is_dir()])


def _aggregate_session(session_id: str) -> dict | None:
    """Собрать сессию из новой папки comparison/ или fallback на legacy."""
    # Lazy import: pair_template lazy-imports store, чтобы избежать цикла на load.
    from . import pair_template as pair_template_mod  # noqa: WPS433

    meta = _load_session_meta(session_id)
    if meta is not None:
        pairs: list[dict] = []
        for pid in _list_pair_ids(session_id):
            pair_meta = _load_pair_meta(session_id, pid)
            if pair_meta is None:
                continue
            pair_meta["links"] = _load_links(session_id, pid)
            pair_meta["graphic_diffs"] = _load_graphic_diffs(session_id, pid)
            # Зелёная галочка в «Загрузке документации»: есть ли сохранённый
            # шаблон для этой пары PDF (identity ключ по полным путям).
            left = pair_meta.get("left") or {}
            right = pair_meta.get("right") or {}
            tpl_key = pair_template_mod.template_key(
                left.get("pdf_path"), right.get("pdf_path"),
            )
            if tpl_key:
                try:
                    pair_meta["has_template"] = paths_mod.pair_template_path(tpl_key).exists()
                except (ValueError, OSError):
                    pair_meta["has_template"] = False
            else:
                pair_meta["has_template"] = False
            pairs.append(pair_meta)
        # Восстановить порядок из session.json (pair_order), если есть
        order = meta.get("pair_order") or []
        if order:
            id_to_pair = {p["id"]: p for p in pairs}
            ordered: list[dict] = []
            for pid in order:
                if pid in id_to_pair:
                    ordered.append(id_to_pair.pop(pid))
            ordered.extend(id_to_pair.values())
            pairs = ordered
        return {
            "id": meta.get("id"),
            "created_at": meta.get("created_at"),
            "stage_a_path": meta.get("stage_a_path"),
            "stage_b_path": meta.get("stage_b_path"),
            "warnings": meta.get("warnings") or [],
            "pairs": pairs,
            "storage": "comparison",
        }
    # Legacy fallback
    legacy = _load_legacy_session(session_id)
    if legacy is not None:
        legacy["storage"] = "legacy"
        return legacy
    return None


def get_session(session_id: str) -> Optional[dict]:
    with _lock:
        return _aggregate_session(session_id)


def list_sessions() -> list[dict]:
    """Все сессии: сначала из comparison/index.json, затем legacy.

    Ghost-сессии (запись в index есть, а session.json на диске нет)
    отфильтровываются: они появляются, если кто-то удалил папку из FS,
    но забыл почистить index, и засоряют UI как «0/0 пар» сессии. Здесь
    мы их просто пропускаем — index чистится отдельно при следующей
    записи.
    """
    items: list[dict] = []
    seen: set[str] = set()

    idx = _read_index()
    for s in idx.get("sessions", []):
        sid = s.get("id")
        if not sid or sid in seen:
            continue
        # Перечитаем актуальные размеры
        try:
            meta = _load_session_meta(sid)
            if meta is None:
                # Ghost: запись в index есть, но session.json на диске нет.
                # Не показываем в UI; в логе — debug, чтобы оператор мог
                # вычистить index при необходимости.
                logger.debug("list_sessions: skipping ghost session %s (no session.json)", sid)
                continue
            pair_ids = _list_pair_ids(sid)
            matched = 0
            for pid in pair_ids:
                pm = _load_pair_meta(sid, pid)
                if pm and pm.get("status") == "matched":
                    matched += 1
            items.append({
                "id": sid,
                "created_at": meta.get("created_at"),
                "stage_a_path": meta.get("stage_a_path"),
                "stage_b_path": meta.get("stage_b_path"),
                "pairs_total": len(pair_ids),
                "pairs_matched": matched,
                "storage": "comparison",
            })
            seen.add(sid)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to read session %s: %s", sid, exc)

    # Legacy сессии (если папка существует)
    if paths_mod.LEGACY_SESSIONS_DIR.exists():
        try:
            for f in sorted(paths_mod.LEGACY_SESSIONS_DIR.glob("*.json")):
                try:
                    with open(f, "r", encoding="utf-8") as fh:
                        data = json.load(fh)
                except (OSError, json.JSONDecodeError):
                    continue
                sid = data.get("id") if isinstance(data, dict) else None
                if not sid or sid in seen:
                    continue
                items.append({
                    "id": sid,
                    "created_at": data.get("created_at"),
                    "stage_a_path": data.get("stage_a_path"),
                    "stage_b_path": data.get("stage_b_path"),
                    "pairs_total": len(data.get("pairs") or []),
                    "pairs_matched": sum(
                        1 for p in (data.get("pairs") or []) if p.get("status") == "matched"
                    ),
                    "storage": "legacy",
                })
                seen.add(sid)
        except OSError:
            pass

    items.sort(key=lambda x: x.get("created_at") or "", reverse=True)
    return items


# ─── Session creation ────────────────────────────────────────────────────

def create_session(stage_a_path: str, stage_b_path: str) -> tuple[dict, list[str]]:
    """Создать новую сессию: сканировать обе папки, сопоставить PDF, создать
    дефолтный page_alignment для каждой пары.

    Все артефакты создаются строго в comparison/.
    """
    warnings: list[str] = []

    left_entries, w1 = scanner_mod.scan_stage_folder(stage_a_path)
    warnings.extend(w1)
    right_entries, w2 = scanner_mod.scan_stage_folder(stage_b_path)
    warnings.extend(w2)

    raw_pairs = scanner_mod.match_pdfs(left_entries, right_entries)
    session_id = _new_id(n=16)
    pair_order: list[str] = []
    pair_summaries: list[dict] = []

    # Lazy import: pair_template импортирует store, цикл разрываем здесь.
    from . import pair_template as pair_template_mod  # noqa: WPS433

    templates_applied = 0
    with _lock:
        for rp in raw_pairs:
            pid = _new_id(prefix="p", n=8)
            d = rp.to_dict()
            d["id"] = pid

            left_pdf_path = (d.get("left") or {}).get("pdf_path")
            right_pdf_path = (d.get("right") or {}).get("pdf_path")
            template = pair_template_mod.find_template(left_pdf_path, right_pdf_path)

            _save_pair(session_id, d)
            _save_graphic_diffs(session_id, pid, [])

            if template is not None:
                # Шаблон найден — пишем links/alignment из шаблона, отмечаем
                # template_applied. Дефолтный alignment всё равно
                # пересчитываем — page_count для свежей пары может отличаться.
                tpl_links = template.get("links") or []
                _save_links(session_id, pid, tpl_links if isinstance(tpl_links, list) else [])

                left_count = _pdf_page_count(left_pdf_path)
                right_count = _pdf_page_count(right_pdf_path)
                base_alignment = alignment_mod.build_default(left_count, right_count)
                base_alignment.update({
                    "left_page_count": left_count,
                    "right_page_count": right_count,
                    "updated_at": _utc_now(),
                })
                tpl_alignment = template.get("page_alignment") or {}
                if isinstance(tpl_alignment, dict) and tpl_alignment:
                    # Из шаблона берём items/слоты, page_count'ы — актуальные.
                    merged = dict(tpl_alignment)
                    merged["left_page_count"] = left_count
                    merged["right_page_count"] = right_count
                    merged["updated_at"] = _utc_now()
                    _save_alignment(session_id, pid, merged)
                else:
                    _save_alignment(session_id, pid, base_alignment)

                # Помечаем pair.json.
                d["template_applied"] = True
                d["template_applied_at"] = _utc_now()
                d["template_key"] = template.get("key")
                d["template_saved_at"] = template.get("saved_at")
                d["template_source_session_id"] = template.get("source_session_id")
                _save_pair(session_id, d)
                templates_applied += 1
            else:
                _save_links(session_id, pid, [])
                left_count = _pdf_page_count(left_pdf_path)
                right_count = _pdf_page_count(right_pdf_path)
                alignment = alignment_mod.build_default(left_count, right_count)
                alignment.update({
                    "left_page_count": left_count,
                    "right_page_count": right_count,
                    "updated_at": _utc_now(),
                })
                _save_alignment(session_id, pid, alignment)

            pair_order.append(pid)
            pair_summaries.append({
                "id": pid,
                "status": d.get("status"),
                "match_score": d.get("match_score"),
            })

        meta = {
            "id": session_id,
            "created_at": _utc_now(),
            "stage_a_path": str(stage_a_path),
            "stage_b_path": str(stage_b_path),
            "warnings": warnings,
            "pair_order": pair_order,
        }
        _save_session_meta(session_id, meta)
        _update_index({
            "id": session_id,
            "created_at": meta["created_at"],
            "stage_a_path": meta["stage_a_path"],
            "stage_b_path": meta["stage_b_path"],
            "pairs_total": len(pair_order),
        })

    return _aggregate_session(session_id) or meta, warnings


# ─── Pair helpers ────────────────────────────────────────────────────────

def _find_pair_meta(session_id: str, pair_id: str) -> dict | None:
    # Сначала новая структура
    pm = _load_pair_meta(session_id, pair_id)
    if pm is not None:
        return pm
    # Legacy: ищем внутри session.json
    legacy = _load_legacy_session(session_id)
    if legacy is None:
        return None
    for p in legacy.get("pairs") or []:
        if p.get("id") == pair_id:
            return p
    return None


def _legacy_pair_in_session(session_id: str, pair_id: str) -> dict | None:
    legacy = _load_legacy_session(session_id)
    if legacy is None:
        return None
    for p in legacy.get("pairs") or []:
        if p.get("id") == pair_id:
            return p
    return None


def _pair_links(session_id: str, pair_id: str) -> list[dict]:
    """Получить links: сначала новый файл, затем legacy."""
    if paths_mod.links_path(session_id, pair_id).exists():
        return _load_links(session_id, pair_id)
    legacy_pair = _legacy_pair_in_session(session_id, pair_id)
    return list(legacy_pair.get("links") or []) if legacy_pair else []


def _pair_graphic_diffs(session_id: str, pair_id: str) -> list[dict]:
    if paths_mod.graphic_diffs_path(session_id, pair_id).exists():
        return _load_graphic_diffs(session_id, pair_id)
    legacy_pair = _legacy_pair_in_session(session_id, pair_id)
    return list(legacy_pair.get("graphic_diffs") or []) if legacy_pair else []


def _load_pair_blocks(pair: dict) -> tuple[list[dict], list[dict], dict, dict]:
    """Загрузить нормализованные блоки + метаданные страниц."""
    left = pair.get("left") or {}
    right = pair.get("right") or {}
    if left.get("result_json_path"):
        lb, lm = blocks_mod.normalize_blocks_from_result_json(left["result_json_path"])
    else:
        lb, lm = [], {"pages_total": 0, "pages": []}
    if right.get("result_json_path"):
        rb, rm = blocks_mod.normalize_blocks_from_result_json(right["result_json_path"])
    else:
        rb, rm = [], {"pages_total": 0, "pages": []}
    return lb, rb, lm, rm


# ─── Alignment ───────────────────────────────────────────────────────────

def _ensure_alignment(session_id: str, pair_id: str, *, persist: bool = True) -> dict:
    """Загрузить alignment, либо собрать дефолтный из метаданных PDF."""
    raw = _load_alignment_raw(session_id, pair_id)
    if raw is not None:
        return raw

    pair = _find_pair_meta(session_id, pair_id)
    if pair is None:
        return {"version": alignment_mod.ALIGNMENT_VERSION, "items": [],
                "left_page_count": 0, "right_page_count": 0}

    left_count = _pdf_page_count((pair.get("left") or {}).get("pdf_path"))
    right_count = _pdf_page_count((pair.get("right") or {}).get("pdf_path"))
    alignment = alignment_mod.build_default(left_count, right_count)
    alignment.update({
        "left_page_count": left_count,
        "right_page_count": right_count,
        "updated_at": _utc_now(),
    })
    if persist and _load_session_meta(session_id) is not None:
        _save_alignment(session_id, pair_id, alignment)
    return alignment


def get_alignment(session_id: str, pair_id: str) -> dict:
    pair = _find_pair_meta(session_id, pair_id)
    if pair is None:
        raise KeyError("pair_not_found")
    alignment = _ensure_alignment(session_id, pair_id)
    return {
        "pair_id": pair_id,
        "left_page_count": alignment.get("left_page_count", 0),
        "right_page_count": alignment.get("right_page_count", 0),
        "alignment": {
            "version": alignment.get("version", alignment_mod.ALIGNMENT_VERSION),
            "items": alignment.get("items") or [],
        },
        "updated_at": alignment.get("updated_at"),
    }


def save_alignment(
    session_id: str,
    pair_id: str,
    items: list[dict],
    *,
    force: bool = False,
) -> dict:
    """Валидировать + сохранить карту страниц.

    Возвращает payload:
      {ok: bool, saved_with_warnings: bool, items, left/right_page_count,
       updated_at, validation_errors, version}

    Если errors найдены И force=False — карта НЕ сохраняется (ok=false).
    Если force=True — карта сохраняется даже с warnings (ok=true,
    saved_with_warnings=true).

    Кроме того, после успешного сохранения вызывает _resync_links_after_alignment
    для пересчёта stale/cross-page-меток существующих связей блоков.
    """
    with _lock:
        pair = _find_pair_meta(session_id, pair_id)
        if pair is None:
            raise KeyError("pair_not_found")
        current = _ensure_alignment(session_id, pair_id, persist=False)
        left_count = current.get("left_page_count", 0)
        right_count = current.get("right_page_count", 0)
        normalized, errors = alignment_mod.validate(items or [], left_count, right_count)

        if errors and not force:
            return {
                "ok": False,
                "saved": False,
                "validation_errors": errors,
                "items_preview": normalized,
                "left_page_count": left_count,
                "right_page_count": right_count,
                "version": alignment_mod.ALIGNMENT_VERSION,
            }

        payload = {
            "ok": True,
            "saved": True,
            "saved_with_warnings": bool(errors),
            "version": alignment_mod.ALIGNMENT_VERSION,
            "items": normalized,
            "left_page_count": left_count,
            "right_page_count": right_count,
            "updated_at": _utc_now(),
            "validation_errors": errors,
        }
        _save_alignment(session_id, pair_id, payload)
        # Пересчитать stale/cross-page для существующих связей блоков
        resync_summary = _resync_links_after_alignment(session_id, pair_id, normalized)
        payload["links_resync"] = resync_summary
        return payload


def alignment_insert_blank(session_id: str, pair_id: str, slot: int, side: str) -> dict:
    if side not in ("left", "right"):
        raise ValueError("side_must_be_left_or_right")
    with _lock:
        pair = _find_pair_meta(session_id, pair_id)
        if pair is None:
            raise KeyError("pair_not_found")
        current = _ensure_alignment(session_id, pair_id, persist=False)
        new_items = alignment_mod.insert_blank(current.get("items") or [], slot, side)
        # insert_blank сам себя не валидирует жёстко, но кладёт null/null строку — это валидно
        return save_alignment(session_id, pair_id, new_items, force=True)


def alignment_move(session_id: str, pair_id: str, slot: int, direction: str) -> dict:
    if direction not in ("up", "down"):
        raise ValueError("direction_must_be_up_or_down")
    with _lock:
        pair = _find_pair_meta(session_id, pair_id)
        if pair is None:
            raise KeyError("pair_not_found")
        current = _ensure_alignment(session_id, pair_id, persist=False)
        new_items = alignment_mod.move(current.get("items") or [], slot, direction)
        return save_alignment(session_id, pair_id, new_items, force=True)


def alignment_insert_blank_side(session_id: str, pair_id: str, slot: int, side: str) -> dict:
    """Вставить пустой лист только на одной стороне (см. alignment.insert_blank_side)."""
    if side not in ("left", "right"):
        raise ValueError("side_must_be_left_or_right")
    with _lock:
        pair = _find_pair_meta(session_id, pair_id)
        if pair is None:
            raise KeyError("pair_not_found")
        current = _ensure_alignment(session_id, pair_id, persist=False)
        new_items = alignment_mod.insert_blank_side(current.get("items") or [], slot, side)
        return save_alignment(session_id, pair_id, new_items, force=True)


def alignment_delete_page_side(
    session_id: str, pair_id: str, slot: int, side: str
) -> dict:
    """Удалить страницу одной стороны в slot'е (см. alignment.delete_page_side)."""
    if side not in ("left", "right"):
        raise ValueError("side_must_be_left_or_right")
    with _lock:
        pair = _find_pair_meta(session_id, pair_id)
        if pair is None:
            raise KeyError("pair_not_found")
        current = _ensure_alignment(session_id, pair_id, persist=False)
        new_items = alignment_mod.delete_page_side(
            current.get("items") or [], slot, side
        )
        return save_alignment(session_id, pair_id, new_items, force=True)


def alignment_move_page_side(
    session_id: str, pair_id: str, slot: int, side: str, direction: str
) -> dict:
    """Передвинуть страницу одной стороны вверх/вниз (см. alignment.move_page_side)."""
    if side not in ("left", "right"):
        raise ValueError("side_must_be_left_or_right")
    if direction not in ("up", "down"):
        raise ValueError("direction_must_be_up_or_down")
    with _lock:
        pair = _find_pair_meta(session_id, pair_id)
        if pair is None:
            raise KeyError("pair_not_found")
        current = _ensure_alignment(session_id, pair_id, persist=False)
        new_items = alignment_mod.move_page_side(
            current.get("items") or [], slot, side, direction
        )
        return save_alignment(session_id, pair_id, new_items, force=True)


def alignment_reset(session_id: str, pair_id: str) -> dict:
    with _lock:
        pair = _find_pair_meta(session_id, pair_id)
        if pair is None:
            raise KeyError("pair_not_found")
        left_count = _pdf_page_count((pair.get("left") or {}).get("pdf_path"))
        right_count = _pdf_page_count((pair.get("right") or {}).get("pdf_path"))
        payload = alignment_mod.build_default(left_count, right_count)
        payload.update({
            "left_page_count": left_count,
            "right_page_count": right_count,
            "updated_at": _utc_now(),
        })
        _save_alignment(session_id, pair_id, payload)
        # Сбросить stale-метки у существующих связей
        _resync_links_after_alignment(session_id, pair_id, payload.get("items") or [])
        return payload


def _resync_links_after_alignment(
    session_id: str,
    pair_id: str,
    alignment_items: list[dict],
) -> dict:
    """Пересчитать stale/cross-page состояние существующих связей.

    Логика:
      • Для каждой связи смотрим, осталась ли страница левого блока в карте.
      • Auto-link, чья (left_page → right_page) пара по карте больше не соответствует
        фактическим (left_page, right_page) блоков, помечаем method="auto_stale".
      • Manual-link, чьи стороны не соответствуют карте, помечаем "manual_cross_page".
      • Если страница вообще выпала из карты (left_page == None в карте) — auto становится
        auto_stale.

    Не удаляет связи; UI решает, как их показать.
    """
    pair = _find_pair_meta(session_id, pair_id)
    if pair is None:
        return {"updated": 0, "stale_auto": 0, "cross_page_manual": 0}

    left_blocks, right_blocks, _, _ = _load_pair_blocks(pair)
    left_by_id = {b["id"]: b for b in left_blocks}
    right_by_id = {b["id"]: b for b in right_blocks}

    # Построим маппинг left_page → (right_page, slot)
    left_map: dict[int, tuple[int | None, int]] = {}
    for it in alignment_items:
        lp = it.get("left_page")
        if lp is None:
            continue
        left_map[int(lp)] = (
            int(it["right_page"]) if it.get("right_page") is not None else None,
            int(it.get("slot") or 0),
        )

    links = _pair_links(session_id, pair_id)
    updated = 0
    stale_auto = 0
    cross_page_manual = 0
    for link in links:
        old_method = link.get("method", "")
        lid = link.get("left_block_id")
        rid = link.get("right_block_id")
        lb = left_by_id.get(lid)
        rb = right_by_id.get(rid)
        actual_lp = lb.get("page") if lb else None
        actual_rp = rb.get("page") if rb else None
        link["left_page"] = actual_lp
        link["right_page"] = actual_rp
        link["page"] = actual_lp  # обратная совместимость

        mapped = left_map.get(actual_lp) if actual_lp is not None else None
        mapped_right = mapped[0] if mapped else None
        link["alignment_slot"] = mapped[1] if mapped else None

        if not lb or not rb:
            # блок исчез из result.json (например, после смены PDF) — stale
            base = "auto" if old_method.startswith("auto") else "manual"
            link["method"] = f"{base}_stale"
            link["stale_reason"] = "block_missing"
            updated += 1
            if old_method.startswith("auto"):
                stale_auto += 1
            else:
                cross_page_manual += 1
            continue

        link.pop("stale_reason", None)
        if old_method.startswith("auto"):
            # Auto: должен быть строго на mapping
            if mapped is None or mapped_right is None or actual_rp != mapped_right:
                if link.get("method") != "auto_stale":
                    updated += 1
                    stale_auto += 1
                link["method"] = "auto_stale"
                link["stale_reason"] = "alignment_changed"
            else:
                if link.get("method") != "auto":
                    updated += 1
                link["method"] = "auto"
        else:
            # Manual: если страницы не соответствуют карте — cross_page
            if mapped is not None and mapped_right is not None and actual_rp != mapped_right:
                if link.get("method") != "manual_cross_page":
                    updated += 1
                    cross_page_manual += 1
                link["method"] = "manual_cross_page"
            elif mapped is None or mapped_right is None:
                if link.get("method") != "manual_stale":
                    updated += 1
                    cross_page_manual += 1
                link["method"] = "manual_stale"
                link["stale_reason"] = "left_page_not_in_alignment"
            else:
                if link.get("method") != "manual":
                    updated += 1
                link["method"] = "manual"

    if updated:
        _save_links(session_id, pair_id, links)
    return {"updated": updated, "stale_auto": stale_auto, "cross_page_manual": cross_page_manual}


# ─── Pair view ───────────────────────────────────────────────────────────

def get_pair_view(session_id: str, pair_id: str) -> Optional[dict]:
    """Полная карточка пары: PDF, MD, result.json, блоки, связи, alignment."""
    pair = _find_pair_meta(session_id, pair_id)
    if pair is None:
        return None
    left_blocks, right_blocks, left_meta, right_meta = _load_pair_blocks(pair)
    alignment = _ensure_alignment(session_id, pair_id)
    links = _pair_links(session_id, pair_id)
    diffs = _pair_graphic_diffs(session_id, pair_id)
    return {
        "session_id": session_id,
        "pair": {
            "id": pair["id"],
            "status": pair.get("status"),
            "match_score": pair.get("match_score"),
            "left": pair.get("left"),
            "right": pair.get("right"),
        },
        "left_blocks": left_blocks,
        "right_blocks": right_blocks,
        "left_pages": left_meta,
        "right_pages": right_meta,
        "left_page_count": alignment.get("left_page_count", 0),
        "right_page_count": alignment.get("right_page_count", 0),
        "alignment": {
            "version": alignment.get("version", alignment_mod.ALIGNMENT_VERSION),
            "items": alignment.get("items") or [],
        },
        "links": links,
        "graphic_diffs": diffs,
    }


# ─── Links ───────────────────────────────────────────────────────────────

def _enrich_link_with_pages(link: dict, left_blocks: list[dict], right_blocks: list[dict],
                            alignment_items: list[dict]) -> dict:
    """Дополнить связь полями left_page/right_page/alignment_slot/cross_page."""
    lid = link.get("left_block_id")
    rid = link.get("right_block_id")
    lp = next((b.get("page") for b in left_blocks if b.get("id") == lid), None)
    rp = next((b.get("page") for b in right_blocks if b.get("id") == rid), None)
    slot = None
    mapped_right = None
    for it in alignment_items:
        if it.get("left_page") == lp:
            slot = it.get("slot")
            mapped_right = it.get("right_page")
            break
    out = dict(link)
    out["left_page"] = lp
    out["right_page"] = rp
    out["alignment_slot"] = slot
    out["page"] = lp  # обратная совместимость
    if (lp is not None and rp is not None and
            mapped_right is not None and rp != mapped_right and
            out.get("method") == "manual"):
        out["method"] = "manual_cross_page"
    return out


def add_manual_link(session_id: str, pair_id: str, left_block_id: str, right_block_id: str) -> dict:
    with _lock:
        pair = _find_pair_meta(session_id, pair_id)
        if pair is None:
            raise KeyError("pair_not_found")
        left_blocks, right_blocks, _, _ = _load_pair_blocks(pair)
        left_ids = {b["id"] for b in left_blocks}
        right_ids = {b["id"] for b in right_blocks}
        if left_block_id not in left_ids:
            raise ValueError(f"left_block_id_not_found:{left_block_id}")
        if right_block_id not in right_ids:
            raise ValueError(f"right_block_id_not_found:{right_block_id}")
        alignment = _ensure_alignment(session_id, pair_id)
        items = alignment.get("items") or []
        link = {
            "left_block_id": left_block_id,
            "right_block_id": right_block_id,
            "method": "manual",
            "score": 1.0,
            "created_at": _utc_now(),
        }
        link = _enrich_link_with_pages(link, left_blocks, right_blocks, items)
        links = _pair_links(session_id, pair_id)
        # Удалить старую связь с этой парой id'шников
        links = [l for l in links if not (
            l.get("left_block_id") == left_block_id and l.get("right_block_id") == right_block_id
        )]
        links.append(link)
        _save_links(session_id, pair_id, links)
        return link


def delete_link(session_id: str, pair_id: str, left_block_id: str, right_block_id: str) -> bool:
    with _lock:
        pair = _find_pair_meta(session_id, pair_id)
        if pair is None:
            raise KeyError("pair_not_found")
        links = _pair_links(session_id, pair_id)
        new_links = [l for l in links if not (
            l.get("left_block_id") == left_block_id and l.get("right_block_id") == right_block_id
        )]
        changed = len(new_links) != len(links)
        if changed:
            _save_links(session_id, pair_id, new_links)
        return changed


def run_auto_link(session_id: str, pair_id: str, *, iou_threshold: float = 0.5) -> dict:
    """Прогнать IoU-сопоставление с учётом alignment, не перезаписывая manual."""
    with _lock:
        pair = _find_pair_meta(session_id, pair_id)
        if pair is None:
            raise KeyError("pair_not_found")
        left_blocks, right_blocks, _, _ = _load_pair_blocks(pair)
        alignment = _ensure_alignment(session_id, pair_id)
        items = alignment.get("items") or []
        existing = _pair_links(session_id, pair_id)
        existing_pairs = {(l.get("left_block_id"), l.get("right_block_id")) for l in existing}
        manual_left = {l.get("left_block_id") for l in existing if str(l.get("method", "")).startswith("manual")}
        manual_right = {l.get("right_block_id") for l in existing if str(l.get("method", "")).startswith("manual")}
        # Очистим старые auto-link'и — пересоберём с учётом alignment
        kept = [l for l in existing if not str(l.get("method", "")).startswith("auto")]

        new_auto = blocks_mod.auto_link_blocks(
            left_blocks, right_blocks,
            iou_threshold=iou_threshold,
            alignment_items=items,
        )
        added = 0
        for link in new_auto:
            if (link["left_block_id"] in manual_left or
                link["right_block_id"] in manual_right):
                continue
            if (link["left_block_id"], link["right_block_id"]) in existing_pairs:
                continue
            link["created_at"] = _utc_now()
            kept.append(link)
            added += 1
        _save_links(session_id, pair_id, kept)
        return {"added": added, "links_total": len(kept)}


# ─── Graphic summary ─────────────────────────────────────────────────────

def compute_graphic_summary(session_id: str, pair_id: str) -> Optional[dict]:
    """Сводка по графическому diff'у с учётом alignment."""
    pair = _find_pair_meta(session_id, pair_id)
    if pair is None:
        return None
    left_blocks, right_blocks, _, _ = _load_pair_blocks(pair)
    alignment = _ensure_alignment(session_id, pair_id)
    items = alignment.get("items") or []
    page_stats = alignment_mod.compute_page_stats(items)

    # Какие страницы вообще участвуют в маппинге
    mapped_left_pages: set[int] = set()
    mapped_right_pages: set[int] = set()
    new_right_pages: list[dict] = []
    removed_left_pages: list[dict] = []
    reordered_pages: list[dict] = []
    for it in items:
        lp = it.get("left_page")
        rp = it.get("right_page")
        slot = it.get("slot")
        if lp is not None:
            mapped_left_pages.add(int(lp))
        if rp is not None:
            mapped_right_pages.add(int(rp))
        if lp is None and rp is not None:
            new_right_pages.append({"slot": slot, "right_page": rp})
        elif lp is not None and rp is None:
            removed_left_pages.append({"slot": slot, "left_page": lp})
        elif lp is not None and rp is not None and lp != rp:
            reordered_pages.append({"slot": slot, "left_page": lp, "right_page": rp})

    links = _pair_links(session_id, pair_id)
    # Stale-связи не считаем «активными» — блоки слева/справа в них могут
    # снова стать left_only/right_only.
    def _is_stale(m: str) -> bool:
        return m.endswith("_stale")
    def _is_active_link(l: dict) -> bool:
        return not _is_stale(str(l.get("method", "")))

    active_links = [l for l in links if _is_active_link(l)]
    stale_links  = [l for l in links if not _is_active_link(l)]
    linked_left  = {l.get("left_block_id") for l in active_links}
    linked_right = {l.get("right_block_id") for l in active_links}

    auto_links   = [l for l in active_links if str(l.get("method", "")).startswith("auto")]
    manual_links = [l for l in active_links if str(l.get("method", "")).startswith("manual")]

    left_only  = [b for b in left_blocks if b["id"] not in linked_left]
    right_only = [b for b in right_blocks if b["id"] not in linked_right]

    return {
        "left_blocks_total": len(left_blocks),
        "right_blocks_total": len(right_blocks),
        "auto_links": auto_links,
        "manual_links": manual_links,
        "stale_links": stale_links,
        "left_only": left_only,
        "right_only": right_only,
        "compared": _pair_graphic_diffs(session_id, pair_id),
        # Page-level статистика
        "page_stats": page_stats,
        "new_right_pages": new_right_pages,
        "removed_left_pages": removed_left_pages,
        "reordered_pages": reordered_pages,
    }


# ─── Page render / block crop ────────────────────────────────────────────

def _resolve_pdf_path(pair: dict, side: str) -> Path:
    side_data = pair.get(side) or {}
    pdf_path = side_data.get("pdf_path")
    if not pdf_path:
        raise FileNotFoundError(f"no_pdf_on_side:{side}")
    p = Path(pdf_path)
    if not p.exists():
        raise FileNotFoundError(f"pdf_not_found:{pdf_path}")
    return p


def render_pdf_page(
    session_id: str,
    pair_id: str,
    side: str,
    page: int,
    *,
    target_long_side: int = 1400,
) -> Path:
    """Рендерит страницу PDF в PNG и кеширует в comparison/.../pages/<side>/."""
    if side not in ("left", "right"):
        raise ValueError("side must be 'left' or 'right'")
    if page < 1:
        raise ValueError("page must be >= 1")

    pair = _find_pair_meta(session_id, pair_id)
    if pair is None:
        raise KeyError("pair_not_found")
    pdf_p = _resolve_pdf_path(pair, side)

    # Новое расположение
    target_dir = paths_mod.pages_dir(session_id, pair_id, side)
    out = target_dir / f"page_{page:04d}_{target_long_side}.png"
    if out.exists():
        return out

    # Legacy кеш — пробуем им воспользоваться (read-only)
    legacy_cache = paths_mod.legacy_cache_dir(session_id)
    legacy_file = legacy_cache / f"{pair_id}_{side}_p{page}_{target_long_side}.png"
    if legacy_file.exists():
        return legacy_file

    fitz = _import_fitz()
    doc = fitz.open(str(pdf_p))
    try:
        if page > doc.page_count:
            raise ValueError(f"page_out_of_range:{page}>doc:{doc.page_count}")
        p = doc[page - 1]
        long_side_pt = max(p.rect.width, p.rect.height)
        if long_side_pt < 1:
            raise ValueError("zero_page_size")
        scale = target_long_side / long_side_pt
        scale = max(0.5, min(6.0, scale))
        mat = fitz.Matrix(scale, scale)
        pix = p.get_pixmap(matrix=mat, alpha=False)
        pix.save(str(out))
    finally:
        doc.close()
    return out


def render_block_crop(
    session_id: str,
    pair_id: str,
    side: str,
    block_id: str,
    *,
    target_long_side: int = 1200,
) -> Path:
    if side not in ("left", "right"):
        raise ValueError("side must be 'left' or 'right'")
    pair = _find_pair_meta(session_id, pair_id)
    if pair is None:
        raise KeyError("pair_not_found")
    pdf_p = _resolve_pdf_path(pair, side)

    side_blocks_all = (_load_pair_blocks(pair)[0] if side == "left" else _load_pair_blocks(pair)[1])
    block = next((b for b in side_blocks_all if b["id"] == block_id), None)
    if block is None:
        raise KeyError(f"block_not_found:{block_id}")

    target_dir = paths_mod.crops_dir(session_id, pair_id, side)
    safe_bid = "".join(c if c.isalnum() else "_" for c in block_id)
    out = target_dir / f"{safe_bid}_{target_long_side}.png"
    if out.exists():
        return out

    fitz = _import_fitz()
    doc = fitz.open(str(pdf_p))
    try:
        page_num = block.get("page") or 1
        if page_num > doc.page_count:
            raise ValueError(f"page_out_of_range:{page_num}")
        p = doc[page_num - 1]
        bbox = block.get("bbox")
        bbox_norm = block.get("bbox_norm")
        pw = block.get("page_width", 0)
        ph = block.get("page_height", 0)
        if bbox and pw and ph:
            sx = p.rect.width / pw
            sy = p.rect.height / ph
            clip = fitz.Rect(bbox[0] * sx, bbox[1] * sy, bbox[2] * sx, bbox[3] * sy)
        elif bbox_norm:
            clip = fitz.Rect(
                bbox_norm[0] * p.rect.width, bbox_norm[1] * p.rect.height,
                bbox_norm[2] * p.rect.width, bbox_norm[3] * p.rect.height,
            )
        elif bbox:
            clip = fitz.Rect(*bbox)
        else:
            raise ValueError("no_bbox_for_block")

        clip = clip & p.rect
        if clip.is_empty or clip.width < 1 or clip.height < 1:
            raise ValueError("empty_clip")
        long_side = max(clip.width, clip.height)
        scale = target_long_side / long_side
        scale = max(0.5, min(6.0, scale))
        mat = fitz.Matrix(scale, scale)
        pix = p.get_pixmap(matrix=mat, clip=clip, alpha=False)
        pix.save(str(out))
    finally:
        doc.close()
    return out


def add_graphic_diff_result(
    session_id: str,
    pair_id: str,
    left_block_id: str,
    right_block_id: str,
    *,
    status: str,
    summary: str = "",
    raw_response: Optional[str] = None,
    model: Optional[str] = None,
    cost_usd: Optional[float] = None,
    error: Optional[str] = None,
    extra: Optional[dict] = None,
) -> dict:
    """Сохранить результат графического сравнения.

    ``extra`` — необязательный dict с provider-specific полями
    (``provider``, ``model_used``, ``fallback_used``,
    ``has_significant_difference``, ``differences``, ``confidence``,
    ``duration_sec``, ``raw_response_excerpt``). Они мерджатся в entry и
    сохраняются как есть. Ключи ``left_block_id``/``right_block_id``/
    ``status``/``summary``/``raw_response``/``model``/``cost_usd``/``error``/
    ``created_at`` всегда задаются основными аргументами и не перетираются.
    """
    with _lock:
        pair = _find_pair_meta(session_id, pair_id)
        if pair is None:
            raise KeyError("pair_not_found")
        diffs = _pair_graphic_diffs(session_id, pair_id)
        diffs = [d for d in diffs if not (
            d.get("left_block_id") == left_block_id and d.get("right_block_id") == right_block_id
        )]
        entry: dict = {}
        if isinstance(extra, dict):
            for k, v in extra.items():
                if k in {"left_block_id", "right_block_id", "status", "summary",
                          "raw_response", "model", "cost_usd", "error", "created_at"}:
                    # эти поля задаются жёстко основными аргументами
                    continue
                entry[k] = v
        entry.update({
            "left_block_id": left_block_id,
            "right_block_id": right_block_id,
            "status": status,
            "summary": summary,
            "raw_response": raw_response,
            "model": model,
            "cost_usd": cost_usd,
            "error": error,
            "created_at": _utc_now(),
        })
        diffs.append(entry)
        _save_graphic_diffs(session_id, pair_id, diffs)
        return entry


# ─── Manual PDF pair management ──────────────────────────────────────────

def _build_pair_entry_dict(pdf_path: Path | str, md_path: str | None, result_json_path: str | None,
                            base_root: Path | str | None = None) -> dict:
    pdf_p = Path(pdf_path)
    rel = pdf_p.name
    if base_root:
        try:
            rel = str(pdf_p.relative_to(Path(base_root)))
        except ValueError:
            pass
    return {
        "pdf_path": str(pdf_path),
        "md_path": (str(md_path) if md_path else None),
        "result_json_path": (str(result_json_path) if result_json_path else None),
        "relative": rel,
        "filename": pdf_p.name,
        "stem": pdf_p.stem,
        "has_md": bool(md_path),
        "has_result_json": bool(result_json_path),
    }


def _find_neighbours_for_pdf(pdf_path: str) -> tuple[str | None, str | None]:
    """Подсосать MD и result.json рядом с PDF — переиспользуем scanner-helpers."""
    p = Path(pdf_path)
    md = scanner_mod._find_md_near(p) if hasattr(scanner_mod, "_find_md_near") else None
    rj = scanner_mod._find_result_json_near(p) if hasattr(scanner_mod, "_find_result_json_near") else None
    return (str(md) if md else None), (str(rj) if rj else None)


def list_unmatched(session_id: str) -> dict:
    """Вернуть PDF, ещё не задействованные ни в одной паре."""
    session = _aggregate_session(session_id)
    if session is None:
        raise KeyError("session_not_found")

    used_left: set[str] = set()
    used_right: set[str] = set()
    for p in session.get("pairs") or []:
        if p.get("status") == "disabled":
            continue
        lp = (p.get("left") or {}).get("pdf_path")
        rp = (p.get("right") or {}).get("pdf_path")
        if lp:
            used_left.add(lp)
        if rp:
            used_right.add(rp)

    left_entries, _ = scanner_mod.scan_stage_folder(session.get("stage_a_path") or "")
    right_entries, _ = scanner_mod.scan_stage_folder(session.get("stage_b_path") or "")
    left_un = [e.to_dict() for e in left_entries if str(e.pdf_path) not in used_left]
    right_un = [e.to_dict() for e in right_entries if str(e.pdf_path) not in used_right]
    # Также вернём список всех PDF для удобства frontend (для select'ов)
    return {
        "left_unmatched": left_un,
        "right_unmatched": right_un,
        "left_all": [e.to_dict() for e in left_entries],
        "right_all": [e.to_dict() for e in right_entries],
    }


def _invalidate_pair_cache(session_id: str, pair_id: str) -> None:
    """Удалить рендеренные страницы и crop'ы для пары (PDF сменился)."""
    base = paths_mod.pair_dir(session_id, pair_id)
    for sub in ("pages", "crops"):
        d = base / sub
        if d.exists():
            try:
                import shutil
                shutil.rmtree(d, ignore_errors=True)
            except Exception:
                pass


def _save_pair_meta_only(session_id: str, pair: dict) -> None:
    _save_pair(session_id, pair)


def confirm_maybe_pairs(session_id: str) -> dict:
    """Подтвердить все пары со статусом ``maybe`` → ``matched``.

    Эквивалент массового «Я согласен со всеми автосопоставлениями».
    Не трогаем ``left/right`` PDF, alignment, links, graphic_diffs — только
    перезаписываем статус, чтобы пары перестали быть «возможными» и попали в
    счётчик ``pairs_matched``.
    """
    with _lock:
        if _load_session_meta(session_id) is None:
            raise KeyError("session_not_found")
        confirmed = 0
        affected_ids: list[str] = []
        for pid in _list_pair_ids(session_id):
            pm = _load_pair_meta(session_id, pid)
            if not pm:
                continue
            if pm.get("status") == "maybe":
                pm["status"] = "matched"
                _save_pair(session_id, pm)
                confirmed += 1
                affected_ids.append(pid)
        return {
            "confirmed": confirmed,
            "pair_ids": affected_ids,
        }


def update_pair_match(
    session_id: str,
    pair_id: str,
    *,
    right_pdf: str | None,
    right_md: str | None = None,
    right_result_json: str | None = None,
    status: str | None = "manual",
) -> dict:
    """Заменить правый PDF в существующей паре. Пересоздаёт alignment, чистит кеш."""
    with _lock:
        session = _aggregate_session(session_id)
        if session is None:
            raise KeyError("session_not_found")
        pair = _find_pair_meta(session_id, pair_id)
        if pair is None:
            raise KeyError("pair_not_found")

        if right_pdf is None:
            # Снять правый PDF
            pair["right"] = None
        else:
            # Если md/result_json не переданы — попробуем автоматом
            if right_md is None and right_result_json is None:
                right_md, right_result_json = _find_neighbours_for_pdf(right_pdf)
            pair["right"] = _build_pair_entry_dict(
                right_pdf, right_md, right_result_json,
                base_root=session.get("stage_b_path"),
            )
        if status:
            pair["status"] = status
        # match_score уже не от автомата — обнуляем
        pair["match_score"] = None
        _save_pair(session_id, pair)

        # Сбросить кеш и alignment
        _invalidate_pair_cache(session_id, pair_id)
        # Пересоздать alignment
        left_count = _pdf_page_count((pair.get("left") or {}).get("pdf_path"))
        right_count = _pdf_page_count((pair.get("right") or {}).get("pdf_path") if pair.get("right") else None)
        new_alignment = alignment_mod.build_default(left_count, right_count)
        new_alignment.update({
            "left_page_count": left_count,
            "right_page_count": right_count,
            "updated_at": _utc_now(),
        })
        _save_alignment(session_id, pair_id, new_alignment)
        # Сбросить auto-links и переотметить manual как stale (если блоки исчезли)
        existing_links = _pair_links(session_id, pair_id)
        kept = [l for l in existing_links if not str(l.get("method", "")).startswith("auto")]
        _save_links(session_id, pair_id, kept)
        _resync_links_after_alignment(session_id, pair_id, new_alignment.get("items") or [])
        # Сбросить graphic-diffs — старые crop'ы могли быть от другого PDF
        _save_graphic_diffs(session_id, pair_id, [])
        return _find_pair_meta(session_id, pair_id) or pair


def create_manual_pair(
    session_id: str,
    *,
    left_pdf: str | None,
    right_pdf: str | None,
    left_md: str | None = None,
    left_result_json: str | None = None,
    right_md: str | None = None,
    right_result_json: str | None = None,
) -> dict:
    """Создать новую пару вручную."""
    with _lock:
        session = _aggregate_session(session_id)
        if session is None:
            raise KeyError("session_not_found")
        if not left_pdf and not right_pdf:
            raise ValueError("at_least_one_pdf_required")

        meta = _load_session_meta(session_id)
        if meta is None:
            raise KeyError("session_meta_not_found")

        new_pid = _new_id(prefix="p", n=8)
        left_obj = None
        right_obj = None
        if left_pdf:
            if left_md is None and left_result_json is None:
                left_md, left_result_json = _find_neighbours_for_pdf(left_pdf)
            left_obj = _build_pair_entry_dict(left_pdf, left_md, left_result_json,
                                              base_root=session.get("stage_a_path"))
        if right_pdf:
            if right_md is None and right_result_json is None:
                right_md, right_result_json = _find_neighbours_for_pdf(right_pdf)
            right_obj = _build_pair_entry_dict(right_pdf, right_md, right_result_json,
                                               base_root=session.get("stage_b_path"))
        pair = {
            "id": new_pid,
            "status": "manual",
            "match_score": None,
            "left": left_obj,
            "right": right_obj,
        }
        _save_pair(session_id, pair)
        _save_links(session_id, new_pid, [])
        _save_graphic_diffs(session_id, new_pid, [])

        left_count = _pdf_page_count(left_pdf) if left_pdf else 0
        right_count = _pdf_page_count(right_pdf) if right_pdf else 0
        alignment = alignment_mod.build_default(left_count, right_count)
        alignment.update({
            "left_page_count": left_count,
            "right_page_count": right_count,
            "updated_at": _utc_now(),
        })
        _save_alignment(session_id, new_pid, alignment)

        # Обновить pair_order в session.json
        order = list(meta.get("pair_order") or [])
        order.append(new_pid)
        meta["pair_order"] = order
        _save_session_meta(session_id, meta)
        return pair


def set_pair_order(session_id: str, ordered_pair_ids: list[str]) -> list[str]:
    """Установить новый порядок пар в сессии (drag-and-drop reorder в UI).

    Принимает СПИСОК pair_id'ов в желаемом порядке. Сохраняет в
    session.json → `pair_order`. Пары, которых нет в новом списке, но
    есть в текущем порядке, добавляются в конец (защита от потери).
    Пары, которых нет в текущем списке, игнорируются (защита от
    «фантомных» id из stale UI cache).

    Возвращает фактический порядок после нормализации.

    Raises:
        KeyError: если session_id не существует.
    """
    with _lock:
        meta = _load_session_meta(session_id)
        if meta is None:
            raise KeyError("session_not_found")
        current = list(meta.get("pair_order") or [])
        current_set = set(current)
        # Берём из нового списка только те id, что реально существуют в сессии.
        new_order: list[str] = []
        seen: set[str] = set()
        for pid in ordered_pair_ids or []:
            if not isinstance(pid, str) or not pid:
                continue
            if pid not in current_set or pid in seen:
                continue
            new_order.append(pid)
            seen.add(pid)
        # Защита от потери: добавляем хвост из пар, которые не упомянули в
        # новом списке (UI не передал — возможно был отфильтрован).
        for pid in current:
            if pid not in seen:
                new_order.append(pid)
                seen.add(pid)
        if new_order == current:
            return current  # ничего не изменилось — не пишем
        meta["pair_order"] = new_order
        _save_session_meta(session_id, meta)
        return new_order


def delete_pair(session_id: str, pair_id: str, *, hard: bool = False) -> bool:
    """Удалить пару (hard=True) или просто пометить как disabled.

    soft-delete сохраняет файлы pair.json/alignment/links, но статус становится
    "disabled" — UI может скрывать.
    """
    with _lock:
        meta = _load_session_meta(session_id)
        if meta is None:
            raise KeyError("session_not_found")
        pair = _find_pair_meta(session_id, pair_id)
        if pair is None:
            raise KeyError("pair_not_found")
        if hard:
            # Физическое удаление: удаляем папку pair, обновляем pair_order
            pair_dir = paths_mod.pair_dir(session_id, pair_id)
            try:
                import shutil
                shutil.rmtree(pair_dir, ignore_errors=True)
            except Exception:
                pass
            order = [pid for pid in (meta.get("pair_order") or []) if pid != pair_id]
            meta["pair_order"] = order
            _save_session_meta(session_id, meta)
            return True
        # Soft
        pair["status"] = "disabled"
        _save_pair(session_id, pair)
        return True


# ─── Path allowlist (Задача 8) ───────────────────────────────────────────

def _parse_allowlist() -> list[Path]:
    raw = os.environ.get("AUDIT_STAGE_COMPARISON_ROOTS", "").strip()
    if not raw:
        return []
    # Поддержка ; (Windows) и : (Unix) — но осторожно: на Linux в путях иногда
    # бывают двоеточия в username. Поэтому делим по ; всегда, и дополнительно
    # пытаемся разделить по os.pathsep если ; не дал ничего.
    parts = [p.strip() for p in raw.split(";") if p.strip()]
    if len(parts) == 1 and os.pathsep != ";":
        parts = [p.strip() for p in raw.split(os.pathsep) if p.strip()]
    out: list[Path] = []
    for p in parts:
        try:
            out.append(Path(p).expanduser().resolve())
        except Exception:
            continue
    return out


def assert_path_in_allowlist(path: str) -> None:
    """Если AUDIT_STAGE_COMPARISON_ROOTS задан, путь должен быть внутри одного из root."""
    allow = _parse_allowlist()
    if not allow:
        return
    try:
        candidate = Path(path).expanduser().resolve()
    except Exception:
        raise PermissionError(f"path_resolution_failed:{path}")
    for root in allow:
        try:
            candidate.relative_to(root)
            return
        except ValueError:
            continue
    raise PermissionError(
        f"path_outside_allowlist:{candidate} (allowed roots: {[str(r) for r in allow]})"
    )


# ─── Alignment suggestion (Задача 6) ─────────────────────────────────────

def _page_fingerprint(pdf_path: str, page_idx_zero_based: int, blocks_on_page: list[dict]) -> dict:
    """Простой fingerprint страницы: размер, число блоков, объединённый bbox, текст-превью."""
    try:
        fitz = _import_fitz()
        doc = fitz.open(str(pdf_path))
        try:
            page = doc[page_idx_zero_based]
            width = float(page.rect.width)
            height = float(page.rect.height)
            text = page.get_text("text") or ""
        finally:
            doc.close()
    except Exception:
        width = 0.0; height = 0.0; text = ""

    block_count = len(blocks_on_page or [])
    union = None
    types: dict[str, int] = {}
    for b in blocks_on_page or []:
        types[b.get("type", "unknown")] = types.get(b.get("type", "unknown"), 0) + 1
        bn = b.get("bbox_norm")
        if bn and len(bn) == 4:
            if union is None:
                union = [bn[0], bn[1], bn[2], bn[3]]
            else:
                union[0] = min(union[0], bn[0])
                union[1] = min(union[1], bn[1])
                union[2] = max(union[2], bn[2])
                union[3] = max(union[3], bn[3])
    # Текст: первые 300 символов, нормализовано
    import re as _re
    txt = _re.sub(r"\s+", " ", text)[:300].strip().lower()
    return {
        "width": round(width, 2),
        "height": round(height, 2),
        "aspect": round((width / height) if height else 0, 3),
        "block_count": block_count,
        "block_types": types,
        "union_bbox_norm": union,
        "text_preview": txt,
    }


def _fingerprint_similarity(a: dict, b: dict) -> float:
    """Похожесть двух fingerprint'ов в [0, 1]."""
    if not a or not b:
        return 0.0
    sims: list[float] = []
    weights: list[float] = []

    # Aspect ratio (PDF orientation/size)
    if a.get("aspect") and b.get("aspect"):
        diff = abs(a["aspect"] - b["aspect"]) / max(a["aspect"], b["aspect"])
        sims.append(max(0.0, 1.0 - diff)); weights.append(1.0)

    # Block count (нормализованная разница)
    bc_a = a.get("block_count", 0); bc_b = b.get("block_count", 0)
    if max(bc_a, bc_b) > 0:
        sims.append(1.0 - abs(bc_a - bc_b) / max(bc_a, bc_b))
    else:
        sims.append(1.0)
    weights.append(0.8)

    # Block types
    types_a = a.get("block_types", {}); types_b = b.get("block_types", {})
    all_types = set(types_a) | set(types_b)
    if all_types:
        common = sum(min(types_a.get(t, 0), types_b.get(t, 0)) for t in all_types)
        total = sum(max(types_a.get(t, 0), types_b.get(t, 0)) for t in all_types)
        sims.append(common / total if total else 0.0); weights.append(0.6)

    # Text similarity (быстро)
    txt_a = a.get("text_preview") or ""
    txt_b = b.get("text_preview") or ""
    if txt_a and txt_b:
        from difflib import SequenceMatcher
        sims.append(SequenceMatcher(None, txt_a, txt_b).ratio()); weights.append(2.0)
    elif not txt_a and not txt_b:
        sims.append(0.5); weights.append(0.3)
    else:
        sims.append(0.0); weights.append(0.3)

    # Union bbox area diff
    ub_a = a.get("union_bbox_norm"); ub_b = b.get("union_bbox_norm")
    if ub_a and ub_b:
        area_a = max(0.0, (ub_a[2] - ub_a[0]) * (ub_a[3] - ub_a[1]))
        area_b = max(0.0, (ub_b[2] - ub_b[0]) * (ub_b[3] - ub_b[1]))
        if max(area_a, area_b) > 0:
            sims.append(1.0 - abs(area_a - area_b) / max(area_a, area_b))
            weights.append(0.4)

    total_w = sum(weights) or 1.0
    return sum(s * w for s, w in zip(sims, weights)) / total_w


def suggest_alignment(session_id: str, pair_id: str) -> dict:
    """Предложить новую карту страниц на основании fingerprint'ов."""
    pair = _find_pair_meta(session_id, pair_id)
    if pair is None:
        raise KeyError("pair_not_found")
    left_pdf = (pair.get("left") or {}).get("pdf_path")
    right_pdf = (pair.get("right") or {}).get("pdf_path")
    if not left_pdf or not right_pdf:
        return {
            "suggested_items": [],
            "confidence": 0.0,
            "warnings": ["one_side_has_no_pdf"],
        }

    left_blocks, right_blocks, _, _ = _load_pair_blocks(pair)
    left_blocks_by_page: dict[int, list[dict]] = {}
    for b in left_blocks:
        left_blocks_by_page.setdefault(b.get("page") or 1, []).append(b)
    right_blocks_by_page: dict[int, list[dict]] = {}
    for b in right_blocks:
        right_blocks_by_page.setdefault(b.get("page") or 1, []).append(b)

    left_count = _pdf_page_count(left_pdf)
    right_count = _pdf_page_count(right_pdf)

    warnings: list[str] = []
    if left_count == 0 or right_count == 0:
        return {"suggested_items": [], "confidence": 0.0,
                "warnings": ["empty_pdf"]}

    # Fingerprint'ы (потенциально медленно — но это операция по требованию)
    fp_left = [_page_fingerprint(left_pdf, i, left_blocks_by_page.get(i+1, []))
               for i in range(left_count)]
    fp_right = [_page_fingerprint(right_pdf, i, right_blocks_by_page.get(i+1, []))
                for i in range(right_count)]

    # Жадное сопоставление: для каждой левой страницы ищем лучшую правую,
    # не нарушая порядок (двигаем «указатель» справа только вперёд).
    suggested: list[dict] = []
    r_cursor = 0
    confidences: list[float] = []
    slot = 0
    while r_cursor < right_count:
        # пока left ещё есть, ищем для текущей левой страницы (lp_index = len(suggested where left_page is set))
        # Алгоритм проще: жадно итерируем по левым страницам, выбираем лучшую правую
        # из окна [r_cursor .. r_cursor+lookahead]
        break

    # Дополнительный, более понятный проход:
    suggested = []
    slot = 0
    r_pos = 0
    lookahead = 4
    for lp in range(1, left_count + 1):
        # Кандидаты справа: r_pos..min(right_count, r_pos+lookahead)
        best_rp = None; best_sc = -1.0
        for rp in range(r_pos, min(right_count, r_pos + lookahead + 1)):
            sc = _fingerprint_similarity(fp_left[lp - 1], fp_right[rp])
            if sc > best_sc:
                best_sc = sc; best_rp = rp + 1   # 1-based
        # Решение: соединить или пропустить
        if best_rp is None or best_sc < 0.45:
            # Не нашли — оставляем left без пары
            slot += 1
            suggested.append({
                "slot": slot, "left_page": lp, "right_page": None,
                "mode": "manual", "note": "no_match_found",
            })
            confidences.append(0.0)
            continue
        # Сначала добавим правые страницы, которые мы «пропускаем», как новые right-only
        for rp_skip in range(r_pos, best_rp - 1):
            slot += 1
            suggested.append({
                "slot": slot, "left_page": None, "right_page": rp_skip + 1,
                "mode": "manual", "note": "new_in_right",
            })
        slot += 1
        suggested.append({
            "slot": slot, "left_page": lp, "right_page": best_rp,
            "mode": "manual", "note": f"fp_score={best_sc:.2f}",
        })
        confidences.append(best_sc)
        r_pos = best_rp

    # Хвост справа
    for rp in range(r_pos, right_count):
        slot += 1
        suggested.append({
            "slot": slot, "left_page": None, "right_page": rp + 1,
            "mode": "manual", "note": "tail_right",
        })

    avg_conf = sum(confidences) / len(confidences) if confidences else 0.0
    return {
        "suggested_items": suggested,
        "confidence": round(avg_conf, 3),
        "warnings": warnings,
    }


def _read_text_file(path: str | None) -> str:
    """Прочитать текстовый файл (MD) безопасно, errors='replace'."""
    if not path:
        return ""
    try:
        return Path(path).read_text(encoding="utf-8", errors="replace")
    except (OSError, ValueError):
        return ""


def _page_text_index_from_result_json(path: str | None) -> dict[int, str]:
    """page_number → объединённый текст-слой блоков (pdfplumber_text / ocr_text).

    Офлайн-фолбэк для страниц без `**Наименование листа:**`: используем текст,
    который уже извлёк OCR/текст-слой. Сети нет, result.json читается с диска.
    Никогда не падает — на ошибке возвращает {}.
    """
    if not path:
        return {}
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    pages = data.get("pages") if isinstance(data, dict) else data
    if not isinstance(pages, list):
        return {}
    out: dict[int, str] = {}
    for p in pages:
        if not isinstance(p, dict):
            continue
        page_no = p.get("page_number") or p.get("page") or p.get("page_index")
        try:
            page_no = int(page_no)
        except (TypeError, ValueError):
            continue
        parts: list[str] = []
        for b in (p.get("blocks") or []):
            if not isinstance(b, dict):
                continue
            for key in ("pdfplumber_text", "ocr_text", "text"):
                val = b.get(key)
                if isinstance(val, str) and val.strip():
                    parts.append(val.strip())
                    break
        if parts:
            out[page_no] = "\n".join(parts)[:2000]
    return out


def suggest_alignment_by_stamp(session_id: str, pair_id: str,
                               *, use_llm: bool = False) -> dict:
    """Предложить карту страниц по ИМЕНИ листа из штампа (MD), с офлайн-фолбэком
    на текст-слой блоков (result.json) для безымянных страниц.

    В отличие от `suggest_alignment` (fingerprint, локальное окно) — матч
    глобальный по имени, поэтому находит листы, уехавшие далеко между стадиями
    (схема ГРЩ стр.21 ↔ стр.56).

    use_llm: если True И включён kill-switch STAGE_COMPARISON_STAMP_LLM_ENABLED
    И доступен Claude Code CLI — после детерминированного матчинга остаток
    НЕсматченных листов отдаётся Haiku, который доматчивает семантически
    эквивалентные имена («Однолинейная расчетная схема ГРЩ» == «Однолинейная
    схема ГРЩ»). Fail-soft: любая проблема → обычный детерминированный результат.
    """
    from . import stamp_matching as sm  # lazy import (избегаем циклов)

    pair = _find_pair_meta(session_id, pair_id)
    if pair is None:
        raise KeyError("pair_not_found")

    left = pair.get("left") or {}
    right = pair.get("right") or {}
    md_left = _read_text_file(left.get("md_path"))
    md_right = _read_text_file(right.get("md_path"))

    warnings: list[str] = []
    if not md_left:
        warnings.append("left_md_missing")
    if not md_right:
        warnings.append("right_md_missing")
    if not md_left or not md_right:
        return {
            "method": "stamp",
            "suggested_items": [],
            "confidence": 0.0,
            "warnings": warnings or ["md_missing"],
            "matched_count": 0,
            "left_only_count": 0,
            "right_only_count": 0,
        }

    extra_left = _page_text_index_from_result_json(left.get("result_json_path"))
    extra_right = _page_text_index_from_result_json(right.get("result_json_path"))

    left_idx = sm.build_sheet_index(md_left, extra_text_by_page=extra_left or None)
    right_idx = sm.build_sheet_index(md_right, extra_text_by_page=extra_right or None)

    # Опциональный LLM-слой доматчинга остатка (Haiku через Claude Code).
    llm_match_fn = None
    llm_diag: dict = {}
    if use_llm:
        try:
            from . import stamp_llm_match as slm
            from .text_llm_provider import ClaudeCodeProvider
            if slm.stamp_llm_enabled():
                provider = ClaudeCodeProvider()
                ok, reason = provider.check_availability()
                if ok:
                    llm_match_fn = slm.make_llm_match_fn(provider, diagnostics=llm_diag)
                else:
                    llm_diag = {"status": "provider_not_available", "error": reason,
                                "pairs_added": 0}
            else:
                llm_diag = {"status": "disabled_by_flag", "pairs_added": 0}
        except Exception as exc:  # fail-soft — LLM-слой не должен валить эндпоинт
            llm_diag = {"status": "setup_exception", "error": str(exc),
                        "pairs_added": 0}

    result = sm.match_sheet_indexes(left_idx, right_idx, llm_match_fn=llm_match_fn)
    result["warnings"] = list(dict.fromkeys([*warnings, *result.get("warnings", [])]))
    result["llm_requested"] = bool(use_llm)
    if use_llm:
        result["llm"] = llm_diag or {"status": "no_unmatched", "pairs_added": 0}

    # Сверка с реальным числом страниц PDF (MD-разметка может быть неполной).
    left_pdf_pages = _pdf_page_count(left.get("pdf_path"))
    right_pdf_pages = _pdf_page_count(right.get("pdf_path"))
    if left_pdf_pages and result.get("left_page_count") != left_pdf_pages:
        result["warnings"].append("left_md_page_count_mismatch")
    if right_pdf_pages and result.get("right_page_count") != right_pdf_pages:
        result["warnings"].append("right_md_page_count_mismatch")
    result["left_pdf_page_count"] = left_pdf_pages
    result["right_pdf_page_count"] = right_pdf_pages
    return result


def has_manual_alignment(session_id: str, pair_id: str) -> bool:
    """Есть ли у пары РУЧНОЕ/применённое выравнивание (не авто-дефолт).

    `get_alignment`/`_ensure_alignment` авто-создаёт дефолт со `mode='auto'`
    при первом открытии пары — это НЕ ручная работа. Ручным/применённым
    считаем alignment, где хоть один item `mode` ∈ {manual, blank}.
    """
    raw = _load_alignment_raw(session_id, pair_id)
    if not raw:
        return False
    for it in (raw.get("items") or []):
        if str(it.get("mode") or "") in ("manual", "blank"):
            return True
    return False


def apply_safe_stamp_alignment_for_pair(
    session_id: str, pair_id: str, *,
    use_llm: bool = False,
    overwrite_existing: bool = False,
) -> dict:
    """Пакетное безопасное авто-применение штамп-сопоставления для ОДНОЙ пары.

    Переиспользует `suggest_alignment_by_stamp` (тот же алгоритм, что и ручной
    `suggest-by-stamp`), фильтрует безопасные пары через
    `stamp_auto_apply.should_auto_apply_stamp_match` и сохраняет через тот же
    `save_alignment`, что и ручной `PUT page-alignment`. Display-поля в
    сохранённый alignment НЕ копируются.

    Не перезаписывает ручное выравнивание, если overwrite_existing=False.
    Fail-soft на уровне вызывающего (job); здесь бросаем только KeyError для
    отсутствующей пары.
    """
    from . import stamp_auto_apply as auto_mod  # lazy import (избегаем циклов)

    pair = _find_pair_meta(session_id, pair_id)
    if pair is None:
        raise KeyError("pair_not_found")

    summary = {
        "pair_id": pair_id, "status": "done", "applied": 0, "review": 0,
        "skipped_reason": None, "confidence": 0.0, "matched_count": 0,
        "multipart_match_count": 0,
        "split_prevented": 0, "true_left_only": 0, "true_right_only": 0,
        "positional_alignment": 0,
        "review_items": [], "errors": [],
    }

    if not overwrite_existing and has_manual_alignment(session_id, pair_id):
        summary["status"] = "skipped_existing_alignment"
        summary["skipped_reason"] = "existing_alignment"
        return summary

    sugg = suggest_alignment_by_stamp(session_id, pair_id, use_llm=use_llm)
    summary["confidence"] = sugg.get("confidence", 0.0)
    summary["matched_count"] = sugg.get("matched_count", 0)
    summary["multipart_match_count"] = sugg.get("multipart_match_count", 0)

    built = auto_mod.build_auto_apply_items(sugg.get("suggested_items") or [])
    summary["applied"] = built["applied"]
    summary["review"] = built["review"]
    summary["split_prevented"] = built.get("split_prevented", 0)
    summary["true_left_only"] = built.get("true_left_only", 0)
    summary["true_right_only"] = built.get("true_right_only", 0)
    summary["positional_alignment"] = built.get("positional_alignment", 0)
    summary["review_items"] = built.get("review_items", [])
    summary["reasons"] = built.get("reasons", {})

    if built["applied"] == 0 and built.get("positional_alignment", 0) == 0:
        # Нечего безопасно применить и нет позиционного выравнивания — НЕ трогаем
        # alignment (Вариант Б: лучше ничего, чем испортить карту). Если matcher
        # что-то нашёл, но оно ушло в review — помечаем пару needs_review.
        summary["status"] = "needs_review" if built["review"] > 0 else "no_safe_matches"
        summary["skipped_reason"] = (
            "unsafe_matches_not_applied" if built["review"] > 0 else "no_safe_matches")
        return summary

    save_res = save_alignment(session_id, pair_id, built["items"], force=True)
    if not save_res.get("ok"):
        summary["status"] = "error"
        summary["errors"].append("save_failed")
    return summary


def _backup_page_alignment(session_id: str, pair_id: str) -> Optional[str]:
    """Бэкап существующего page_alignment.json перед перезаписью (one-click).

    Возвращает путь бэкапа или None (файла ещё нет). Не бросает наружу."""
    try:
        import shutil
        src = paths_mod.page_alignment_path(session_id, pair_id)
        if not src.exists():
            return None
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        dst = src.with_name(f"{src.name}.bak_onclick_{ts}")
        shutil.copy2(src, dst)
        return str(dst)
    except Exception as exc:  # noqa: BLE001 — бэкап best-effort
        logger.warning("page_alignment backup failed %s/%s: %s", session_id, pair_id, exc)
        return None


def auto_match_apply_pair(
    session_id: str, pair_id: str, *,
    use_llm: bool = False,
    overwrite_existing: bool = False,
    dry_run: bool = False,
) -> dict:
    """One-click авто-сопоставление листов ОДНОЙ пары: suggest → классификация →
    (опц.) безопасное применение → подробный отчёт.

    Детерминированно и быстро (без Qwen/Opus/pipeline). ``use_llm`` по умолчанию
    False — тяжёлый LLM-доматчинг не запускается без явного флага. ``dry_run``
    True → ничего не сохраняет (preview). Перед реальной перезаписью делает
    бэкап ``page_alignment.json``. Связи блоков НЕ удаляются — ``save_alignment``
    лишь помечает их stale/cross-page через ``_resync_links_after_alignment``.

    Fail-soft: бросает только KeyError при отсутствии пары; прочее ловит
    вызывающий (endpoint).
    """
    from . import stamp_auto_apply as auto_mod  # lazy import (избегаем циклов)

    pair = _find_pair_meta(session_id, pair_id)
    if pair is None:
        raise KeyError("pair_not_found")

    report = {
        "session_id": session_id, "pair_id": pair_id,
        "status": "completed", "dry_run": bool(dry_run), "applied_to_disk": False,
        "use_llm": bool(use_llm),
        "summary": {
            "old_pages_total": 0, "new_pages_total": 0,
            "auto_applied": 0, "needs_review": 0,
            "unmatched_old": 0, "unmatched_new": 0,
            "replaced_existing": 0, "stale_block_links_marked": 0,
            "positional_alignment": 0,
        },
        "applied": [], "needs_review": [], "unmatched_old": [], "unmatched_new": [],
        "warnings": [], "backup_path": None,
    }

    # Сколько существующих ручных/применённых пар (с обеими страницами) будет
    # перезаписано — для отчёта (replaced_existing).
    existing = _load_alignment_raw(session_id, pair_id) or {}
    replaced = sum(
        1 for it in (existing.get("items") or [])
        if it.get("left_page") is not None and it.get("right_page") is not None
        and str(it.get("mode") or "") in ("manual", "blank")
    )
    report["summary"]["replaced_existing"] = replaced

    skip = (not overwrite_existing) and has_manual_alignment(session_id, pair_id)

    sugg = suggest_alignment_by_stamp(session_id, pair_id, use_llm=use_llm)
    report["warnings"] = list(sugg.get("warnings") or [])
    s = report["summary"]
    s["old_pages_total"] = sugg.get("left_page_count") or sugg.get("left_pdf_page_count") or 0
    s["new_pages_total"] = sugg.get("right_page_count") or sugg.get("right_pdf_page_count") or 0

    suggested_items = sugg.get("suggested_items") or []
    cls = auto_mod.classify_for_one_click(suggested_items)
    report["applied"] = cls["applied"]
    report["needs_review"] = cls["needs_review"]
    report["unmatched_old"] = cls["unmatched_old"]
    report["unmatched_new"] = cls["unmatched_new"]
    s["auto_applied"] = len(cls["applied"])
    s["needs_review"] = len(cls["needs_review"])
    s["unmatched_old"] = len(cls["unmatched_old"])
    s["unmatched_new"] = len(cls["unmatched_new"])
    s["positional_alignment"] = cls["positional_alignment"]

    if skip:
        report["status"] = "skipped_existing_alignment"
        report["warnings"].append("manual_alignment_exists_not_overwritten")
        return report

    built = auto_mod.build_auto_apply_items(suggested_items)

    if dry_run:
        report["status"] = "dry_run"
        return report

    if built["applied"] == 0 and built.get("positional_alignment", 0) == 0:
        # Нечего безопасно применить — НЕ трогаем карту (лучше ничего).
        report["status"] = "needs_review" if built["review"] > 0 else "no_safe_matches"
        return report

    report["backup_path"] = _backup_page_alignment(session_id, pair_id)
    save_res = save_alignment(session_id, pair_id, built["items"], force=True)
    if not save_res.get("ok"):
        report["status"] = "error"
        report["warnings"].append("save_failed")
        return report

    report["applied_to_disk"] = True
    rs = save_res.get("links_resync") or {}
    s["stale_block_links_marked"] = int(rs.get("stale_auto", 0)) + int(rs.get("cross_page_manual", 0))
    return report


__all__ = [
    "SESSIONS_DIR",
    "create_session",
    "get_session",
    "list_sessions",
    "get_pair_view",
    "add_manual_link",
    "delete_link",
    "run_auto_link",
    "compute_graphic_summary",
    "render_pdf_page",
    "render_block_crop",
    "add_graphic_diff_result",
    # Page alignment
    "get_alignment",
    "save_alignment",
    "alignment_insert_blank",
    "alignment_move",
    "alignment_reset",
    "suggest_alignment",
    "suggest_alignment_by_stamp",
    "has_manual_alignment",
    "apply_safe_stamp_alignment_for_pair",
    "auto_match_apply_pair",
    # Manual PDF pair management
    "list_unmatched",
    "update_pair_match",
    "create_manual_pair",
    "delete_pair",
    # Security
    "assert_path_in_allowlist",
]
