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

import gzip
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
from . import prepared_document as prepared_document_mod
from . import sheet_matcher as sheet_matcher_mod
from . import sheet_identity as sheet_identity_mod
from . import sheet_alignment as sheet_alignment_mod
from . import change_regions as change_regions_mod
from . import change_groups as change_groups_mod
from . import change_detection as change_detection_mod
from . import semantic_diff as semantic_diff_mod
from . import semantic_diff_v6a1 as semantic_diff_v6a1_mod
from . import semantic_diff_v6a2 as semantic_diff_v6a2_mod
from . import diagnostic_new_pipeline as diagnostic_new_pipeline_mod

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


_PAIR_PASSTHROUGH_FIELDS: tuple[str, ...] = ()


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
    meta = _load_session_meta(session_id)
    if meta is not None:
        pairs: list[dict] = []
        for pid in _list_pair_ids(session_id):
            pair_meta = _load_pair_meta(session_id, pid)
            if pair_meta is None:
                continue
            pair_meta["links"] = _load_links(session_id, pid)
            pair_meta["graphic_diffs"] = _load_graphic_diffs(session_id, pid)
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

    with _lock:
        for rp in raw_pairs:
            pid = _new_id(prefix="p", n=8)
            d = rp.to_dict()
            d["id"] = pid

            left_pdf_path = (d.get("left") or {}).get("pdf_path")
            right_pdf_path = (d.get("right") or {}).get("pdf_path")
            _save_pair(session_id, d)
            _save_graphic_diffs(session_id, pid, [])
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


# ─── Deterministic sheet matching ────────────────────────────────────────

def _prepared_document_for_comparison_pdf(pdf_path: str | None) -> tuple[dict, Path, Path]:
    """Найти PreparedDocument по document.pdf versioned-stage загрузки.

    Если его ещё нет, собрать из уже подготовленных артефактов. Исходные PDF,
    MD и blocks.json при этом не меняются.
    """
    pdf = Path(str(pdf_path or ""))
    if not pdf.is_file() or pdf.name != "document.pdf" or pdf.parent.name != "02_work":
        raise ValueError("pair_pdf_is_not_a_versioned_comparison_document")
    version_dir = pdf.parent.parent
    stage_dir = next((parent for parent in version_dir.parents if parent.name in {"stage_1", "stage_2"}), None)
    if stage_dir is None or stage_dir.parent.name != "comparison":
        raise ValueError("pair_pdf_is_outside_object_comparison_storage")
    prepared_path = prepared_document_mod.prepared_document_path(version_dir)
    if prepared_path.is_file():
        data = _read_json(prepared_path)
        if isinstance(data, dict) and data.get("kind") == prepared_document_mod.MODEL_KIND:
            return data, version_dir, stage_dir.parent
    data, prepared_path = prepared_document_mod.build_and_write_prepared_document(
        version_dir, stage_name=stage_dir.name,
        object_metadata=_read_json(stage_dir.parent / "object.json") or {},
    )
    return data, version_dir, stage_dir.parent


def run_sheet_matching(session_id: str, pair_id: str) -> dict:
    """Построить и, только при отсутствии ручной карты, применить карту листов.

    Ручной alignment никогда не перезаписывается: диагностический JSON всё
    равно сохраняется, но response сообщает, что применение пропущено.
    """
    with _lock:
        pair = _find_pair_meta(session_id, pair_id)
        if pair is None:
            raise KeyError("pair_not_found")
        left, _, comparison_left = _prepared_document_for_comparison_pdf((pair.get("left") or {}).get("pdf_path"))
        right, _, comparison_right = _prepared_document_for_comparison_pdf((pair.get("right") or {}).get("pdf_path"))
        if comparison_left != comparison_right:
            raise ValueError("pair_documents_belong_to_different_comparison_objects")
        result = sheet_matcher_mod.match_prepared_documents(
            left, right,
            left_pdf=(pair.get("left") or {}).get("pdf_path"),
            right_pdf=(pair.get("right") or {}).get("pdf_path"),
        )
        result_path = comparison_left / "diagnostics" / "sheet_matching.json"
        sheet_matcher_mod.write_sheet_matching_result(result_path, result)
        report_path = sheet_matcher_mod.write_sheet_matching_report(
            comparison_left / "diagnostics" / "sheet_matching.md", result,
        )

        current = _ensure_alignment(session_id, pair_id, persist=False)
        if sheet_matcher_mod.alignment_has_manual_items(current.get("items") or []):
            return {
                "ok": True, "applied": False, "reason": "manual_alignment_preserved",
                "sheet_matching": result, "result_path": str(result_path), "report_path": str(report_path),
                "alignment": get_alignment(session_id, pair_id),
            }
        alignment = save_alignment(
            session_id, pair_id, sheet_matcher_mod.alignment_items_from_result(result), force=False,
        )
        return {
            "ok": True, "applied": True, "reason": "automatic_alignment_applied",
            "sheet_matching": result, "result_path": str(result_path), "report_path": str(report_path), "alignment": alignment,
        }


def run_change_regions_cleanup_pilot(session_id: str, pair_id: str) -> dict:
    """Этап 5Б.1: canonical vector evidence для тех же трёх пилотных пар."""
    with _lock:
        pair = _find_pair_meta(session_id, pair_id)
        if pair is None: raise KeyError("pair_not_found")
        left, _, comparison = _prepared_document_for_comparison_pdf((pair.get("left") or {}).get("pdf_path"))
        right, _, comparison_right = _prepared_document_for_comparison_pdf((pair.get("right") or {}).get("pdf_path"))
        if comparison != comparison_right: raise ValueError("pair_documents_belong_to_different_comparison_objects")
        alignment = _read_json(comparison / "diagnostics" / "sheet_alignment.json")
        if not isinstance(alignment, dict): raise ValueError("sheet_alignment_result_missing_run_sheet_alignment_first")
        before = _read_json(comparison / "change_regions" / "change_regions.json") or {}
        destination = comparison / "change_regions_v5b1"
        report = change_regions_mod.run_pilot((pair.get("left") or {}).get("pdf_path"),(pair.get("right") or {}).get("pdf_path"),left,right,alignment,destination,canonical_vectors=True)
        report["kind"]="stage_comparison_change_regions_vector_cleanup_pilot"
        report["before_reference"]={"path":str(comparison / "change_regions" / "change_regions.json"),"summary":before.get("summary")}
        json_path, md_path = change_regions_mod.write_report(destination, report)
        return {"ok":True,"change_regions":report,"result_path":str(json_path),"report_path":str(md_path)}


def run_change_regions_rebuild_pilot(session_id: str, pair_id: str) -> dict:
    with _lock:
        pair=_find_pair_meta(session_id,pair_id)
        if pair is None: raise KeyError("pair_not_found")
        left,_,comparison=_prepared_document_for_comparison_pdf((pair.get("left") or {}).get("pdf_path")); right,_,_= _prepared_document_for_comparison_pdf((pair.get("right") or {}).get("pdf_path"))
        prior=_read_json(comparison/"change_regions_v5b1"/"change_regions.json")
        if not isinstance(prior,dict): raise ValueError("change_regions_v5b1_missing_run_cleanup_first")
        report=change_regions_mod.rebuild_regions_after_canonical(prior,left,right)
        destination=comparison/"change_regions_v5b2"; json_path=destination/"change_regions.json"; md_path=destination/"change_regions.md"
        change_regions_mod._write(json_path,json.dumps(report,ensure_ascii=False,indent=2,sort_keys=True)+"\n")
        lines=["# 5Б.2 — Regions после canonical diff",""]
        for item in report["items"]:
            lines += [f"## V2 {item['left_page']} ↔ V3 {item['right_page']}","",f"Regions: {item['summary']['regions_before']} → {item['summary']['regions_after']}; supporting long vectors: {item['summary']['supporting_long_vectors']}.","", "| Region | bbox V2 | Page area | role |", "| --- | --- | ---: | --- |", *[f"| {r['region_id']} | {r['bbox']} | {r['page_area_ratio']:.2%} | {r['region_role']} |" for r in item['regions']],""]
        change_regions_mod._write(md_path,"\n".join(lines)); return {"ok":True,"change_regions":report,"result_path":str(json_path),"report_path":str(md_path)}


def run_change_groups_pilot(session_id: str, pair_id: str) -> dict:
    """Этап 5Б.3: неизменяемые atomic regions 5Б.2 → change groups."""
    with _lock:
        pair = _find_pair_meta(session_id, pair_id)
        if pair is None:
            raise KeyError("pair_not_found")
        left, _, comparison = _prepared_document_for_comparison_pdf((pair.get("left") or {}).get("pdf_path"))
        right, _, comparison_right = _prepared_document_for_comparison_pdf((pair.get("right") or {}).get("pdf_path"))
        if comparison != comparison_right:
            raise ValueError("pair_documents_belong_to_different_comparison_objects")
        atomic = _read_json(comparison / "change_regions_v5b2" / "change_regions.json")
        if not isinstance(atomic, dict) or atomic.get("kind") != "stage_comparison_change_regions_rebuilt_after_canonical":
            raise ValueError("change_regions_v5b2_missing_run_rebuild_first")
        report = change_groups_mod.evaluate_change_groups(atomic, left, right)
        destination = comparison / "change_groups_v5b3"
        change_groups_mod.write_diagnostics(destination / "diagnostics", report, (pair.get("left") or {}).get("pdf_path"))
        json_path, md_path = change_groups_mod.write_report(destination, report)
        return {"ok": True, "change_groups": report, "result_path": str(json_path), "report_path": str(md_path)}


def run_semantic_diff_pilot(session_id: str, pair_id: str) -> dict:
    """Этап 6А: смысловой пилот поверх неизменяемого результата 5Б.4."""
    with _lock:
        pair = _find_pair_meta(session_id, pair_id)
        if pair is None:
            raise KeyError("pair_not_found")
        left_pdf = (pair.get("left") or {}).get("pdf_path")
        right_pdf = (pair.get("right") or {}).get("pdf_path")
        left, _, comparison = _prepared_document_for_comparison_pdf(left_pdf)
        right, _, comparison_right = _prepared_document_for_comparison_pdf(right_pdf)
        if comparison != comparison_right:
            raise ValueError("pair_documents_belong_to_different_comparison_objects")
        detection_path = comparison / "change_detection" / "change_detection.json"
        detection = _read_json(detection_path)
        if not isinstance(detection, dict) or detection.get("kind") != "stage_comparison_change_detection_v5b4":
            raise ValueError("change_detection_result_missing_run_stage_5b4_first")
        destination = comparison / "semantic_diff_v6a"
        llm_runner, llm_provider = semantic_diff_mod.resolve_provider_runner(destination / "llm_work")
        report = semantic_diff_mod.run_pilot(
            left_pdf, right_pdf, left, right, detection, destination, llm_runner=llm_runner,
        )
        report["llm_provider"] = llm_provider
        json_path, md_path = semantic_diff_mod.write_report(destination, report)
        return {"ok": True, "semantic_diff": report, "result_path": str(json_path), "report_path": str(md_path)}


def run_semantic_diff_v6a1_pilot(session_id: str, pair_id: str) -> dict:
    """Этап 6А.1: только deterministic evidence-first поверх тех же 12 групп."""
    with _lock:
        pair = _find_pair_meta(session_id, pair_id)
        if pair is None:
            raise KeyError("pair_not_found")
        left_pdf = (pair.get("left") or {}).get("pdf_path")
        right_pdf = (pair.get("right") or {}).get("pdf_path")
        left, _, comparison = _prepared_document_for_comparison_pdf(left_pdf)
        right, _, comparison_right = _prepared_document_for_comparison_pdf(right_pdf)
        if comparison != comparison_right:
            raise ValueError("pair_documents_belong_to_different_comparison_objects")
        detection = _read_json(comparison / "change_detection" / "change_detection.json")
        old_semantic = _read_json(comparison / "semantic_diff_v6a" / "semantic_diff.json")
        if not isinstance(detection, dict) or detection.get("kind") != "stage_comparison_change_detection_v5b4":
            raise ValueError("change_detection_result_missing_run_stage_5b4_first")
        if not isinstance(old_semantic, dict) or old_semantic.get("kind") != "stage_comparison_semantic_diff_v6a_pilot":
            raise ValueError("semantic_diff_v6a_missing_run_stage_6a_first")
        destination = comparison / "semantic_diff_v6a1"
        report = semantic_diff_v6a1_mod.run_pilot(
            left_pdf, right_pdf, left, right, detection, old_semantic, destination,
        )
        json_path, md_path = semantic_diff_v6a1_mod.write_report(destination, report)
        return {"ok": True, "semantic_diff": report, "result_path": str(json_path), "report_path": str(md_path)}


def run_semantic_diff_v6a2_mass(session_id: str, pair_id: str) -> dict:
    """Этап 6А.2: все группы 5Б.4, строго неизменённой логикой 6А.1."""
    with _lock:
        pair = _find_pair_meta(session_id, pair_id)
        if pair is None:
            raise KeyError("pair_not_found")
        left_pdf = (pair.get("left") or {}).get("pdf_path")
        right_pdf = (pair.get("right") or {}).get("pdf_path")
        left, _, comparison = _prepared_document_for_comparison_pdf(left_pdf)
        right, _, comparison_right = _prepared_document_for_comparison_pdf(right_pdf)
        if comparison != comparison_right:
            raise ValueError("pair_documents_belong_to_different_comparison_objects")
        detection = _read_json(comparison / "change_detection" / "change_detection.json")
        pilot = _read_json(comparison / "semantic_diff_v6a1" / "semantic_diff.json")
        if not isinstance(detection, dict) or detection.get("kind") != "stage_comparison_change_detection_v5b4":
            raise ValueError("change_detection_result_missing_run_stage_5b4_first")
        if not isinstance(pilot, dict) or pilot.get("kind") != "stage_comparison_semantic_diff_v6a1_pilot":
            raise ValueError("semantic_diff_v6a1_missing_run_stage_6a1_first")
        destination = comparison / "semantic_diff_v6a2"
        report = semantic_diff_v6a2_mod.run_mass(
            left_pdf, right_pdf, left, right, detection, destination, pilot_v6a1=pilot,
        )
        json_path, md_path = semantic_diff_v6a2_mod.write_report(destination, report)
        return {"ok": True, "semantic_diff": report, "result_path": str(json_path), "report_path": str(md_path)}


# ─── Диагностическая витрина новой цепочки (read-only) ───────────────────────
#
# Отдаёт уже посчитанные change groups 5Б.4 + смысл 6А.2 во вкладку
# «Расхождения» отдельным режимом. Ничего не считает, не пишет и не трогает
# старый Opus-путь: comparison_result.json, findings и экспертные решения
# остаются как были.


def _diagnostic_new_pipeline_context(session_id: str, pair_id: str) -> tuple[dict, Path, str, str]:
    pair = _find_pair_meta(session_id, pair_id)
    if pair is None:
        raise KeyError("pair_not_found")
    left_pdf = (pair.get("left") or {}).get("pdf_path")
    right_pdf = (pair.get("right") or {}).get("pdf_path")
    left, _, comparison = _prepared_document_for_comparison_pdf(left_pdf)
    right, _, comparison_right = _prepared_document_for_comparison_pdf(right_pdf)
    if comparison != comparison_right:
        raise ValueError("pair_documents_belong_to_different_comparison_objects")
    try:
        alignment_items = (get_alignment(session_id, pair_id).get("alignment") or {}).get("items") or []
    except Exception:  # noqa: BLE001 — карта страниц не обязательна для просмотра
        alignment_items = []
    payload = diagnostic_new_pipeline_mod.build_payload(comparison, left, right, alignment_items)
    payload["session_id"], payload["pair_id"] = session_id, pair_id
    return payload, comparison, str(left_pdf), str(right_pdf)


def get_new_pipeline_diagnostic(session_id: str, pair_id: str) -> dict:
    """Все change groups новой цепочки для диагностического режима вкладки."""
    payload, _, _, _ = _diagnostic_new_pipeline_context(session_id, pair_id)
    return payload


def render_new_pipeline_crop(
    session_id: str,
    pair_id: str,
    *,
    left_page: int,
    right_page: int,
    group_id: str,
    side: str,
    target_long_side: int = 1100,
    padding_pt: float = 18.0,
) -> tuple[bytes | Path, str]:
    """Кроп change group: готовый пилотный PNG, иначе рендер из PDF в память.

    Возвращает ``(Path | bytes, source)``. Ничего на диск не пишет: пилотные
    картинки 6А.1 переиспользуются как есть, остальные рендерятся на лету.
    """
    if side not in ("v2", "v3", "overlay"):
        raise ValueError("side must be 'v2', 'v3' or 'overlay'")
    payload, comparison, left_pdf, right_pdf = _diagnostic_new_pipeline_context(session_id, pair_id)
    if not payload.get("available"):
        raise FileNotFoundError(payload.get("reason") or "new_pipeline_diagnostic_unavailable")

    existing = diagnostic_new_pipeline_mod.pilot_crop_path(comparison, left_page, right_page, group_id, side)
    if existing.is_file():
        return existing, "pilot_file"
    if side == "overlay":
        # Overlay строит только этап 6А.1 и только для пилотных групп —
        # заново его не считаем, чтобы не изобретать вторую логику совмещения.
        raise FileNotFoundError("overlay_not_generated_for_this_group")

    group = diagnostic_new_pipeline_mod.find_group(payload, left_page, right_page, group_id)
    if group is None:
        raise KeyError("change_group_not_found")
    if side == "v2":
        pdf_path, page_number, bbox = left_pdf, left_page, group.get("bbox")
    else:
        pdf_path, page_number, bbox = right_pdf, right_page, group.get("bbox_right") or group.get("bbox")
    png = diagnostic_new_pipeline_mod.render_crop_bytes(
        pdf_path, page_number, bbox, padding_pt=padding_pt, target_long_side=target_long_side,
    )
    return png, "on_demand_render"


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


def render_pdf_page_svg(session_id: str, pair_id: str, side: str, page: int) -> Path:
    """Отдать страницу PDF как SVG и закэшировать рядом с PNG-страницами.

    Зачем вектор: листы формата 2384x3370 pt отдавались растром 1400 px, то
    есть 0.42 пикселя на пункт — на зуме 400% чертёж превращался в кашу.
    SVG рисуется браузером с нужным разрешением на любом масштабе.

    Важные свойства (проверены на боевых листах):
      * ``viewBox`` совпадает с ``page.rect``, поэтому системы координат
        block-оверлеев и change groups остаются прежними;
      * ``text_as_path=True`` превращает шрифты в контуры — CAD-гарнитуры
        (ISOCPEUR/GOST) больше не зависят от наличия шрифта у клиента,
        внешних ссылок в файле нет;
      * встроенные растры (скан-листы, QR, логотипы) переносятся как есть,
        причём в ИСХОДНОМ разрешении — это лучше нынешней копии 1400 px.

    Страницы без вектор-слоя тут не отсекаются: даже для скана SVG отдаёт
    оригинальный растр целиком. Решение «вектор или PNG» принимает вызывающий.
    """
    if side not in ("left", "right"):
        raise ValueError("side must be 'left' or 'right'")
    if page < 1:
        raise ValueError("page must be >= 1")

    pair = _find_pair_meta(session_id, pair_id)
    if pair is None:
        raise KeyError("pair_not_found")
    pdf_p = _resolve_pdf_path(pair, side)

    # Кэш хранится СЖАТЫМ: несжатый SVG плотного листа — 3.6 МБ, на пару
    # выходило 53 МБ. Заодно снимается повторное сжатие на каждый запрос:
    # готовые байты уходят клиенту как есть.
    out = paths_mod.pages_dir(session_id, pair_id, side) / f"page_{page:04d}.svg.gz"
    if out.exists():
        return out

    fitz = _import_fitz()
    doc = fitz.open(str(pdf_p))
    try:
        if page > doc.page_count:
            raise ValueError(f"page_out_of_range:{page}>doc:{doc.page_count}")
        svg = doc[page - 1].get_svg_image(text_as_path=True)
    finally:
        doc.close()

    # Атомарная запись: недописанный файл не должен попасть в кэш и уехать
    # клиенту как «готовая» страница.
    tmp = out.with_name(out.name + ".tmp")
    with gzip.open(tmp, "wb", compresslevel=6) as handle:
        handle.write(svg.encode("utf-8"))
    tmp.replace(out)
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
    """Разрешить новые object-local comparison-пути и legacy allowlist.

    Основной layout ограничен только веткой
    ``projects_v2/objects/<object>/comparison/**``. Настраиваемый legacy
    allowlist остаётся читаемым для уже созданных исторических сессий.
    """
    allow = _parse_allowlist()
    try:
        candidate = Path(path).expanduser().resolve()
    except Exception:
        raise PermissionError(f"path_resolution_failed:{path}")

    try:
        from backend.app.core.config import DATA_DIR
        v2_root = Path(
            os.environ.get("AUDIT_PROJECTS_V2_DIR") or (Path(DATA_DIR) / "projects_v2")
        ).expanduser().resolve()
        relative = candidate.relative_to((v2_root / "objects").resolve())
        parts = relative.parts
        if len(parts) >= 3 and parts[1] == "comparison" and parts[2] in {"stage_1", "stage_2"}:
            return
    except ValueError:
        pass

    # До введения object-local layout пустой allowlist означал unrestricted.
    # Сохраняем этот контракт для внешних тестовых/операторских путей.
    if not allow:
        return
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

__all__ = [
    "SESSIONS_DIR",
    "create_session",
    "get_session",
    "list_sessions",
    "get_pair_view",
    "delete_link",
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
    # Manual PDF pair management
    "list_unmatched",
    "update_pair_match",
    "create_manual_pair",
    "delete_pair",
    # Security
    "assert_path_in_allowlist",
]
