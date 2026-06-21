# -*- coding: utf-8 -*-
"""Pre-Qwen block equivalence gate (Stage 1 — observe only).

Идея: ДО отправки image/imagine/text-блоков в Qwen определить, какие блоки
между OLD (left/старая стадия) и NEW (right/новая стадия) версиями листа
идентичны. В будущем это позволит безопасно ПРОПУСКАТЬ Qwen для неизменённых
блоков. На первом этапе модуль работает в режиме ``observe`` — только строит
отчёт и diagnostics, НИЧЕГО не пропускает.

Конвейер прекчека (НЕ заменяет Qwen, дополняет):

    result.json OLD/NEW
      → extract_blocks_for_equivalence()       # нормализованные блоки + текст
      → pair_blocks_by_iou()                    # сопоставление по coords_norm/IoU
          ├─ detect_split_merge_candidates()    # один↔много → uncertain
          ├─ one-to-one уверенный → paired
          ├─ unmatched old → deleted_candidate
          └─ unmatched new → added_candidate
      → per paired block:
          compare_text_blocks()                 # canonical text equality
          compare_visual_blocks()               # ECC align (cv2) + diff ratios
      → decide_block_pair()                     # decision + qwen_action
      → build_block_equivalence_report()        # отчёт + summary

Принципы безопасности (см. задачу):
  * Stage 1 НИКОГДА не пропускает Qwen (observe).
  * При любом сомнении (render/align fail, split/merge, нет текста и нет
    визуала) → ``qwen_required``.
  * Не скрывать изменения. ``qwen_skip_candidate`` ставится только при
    уверенной идентичности (text-equal для текстовых блоков ИЛИ visual-identical
    для графики).
  * cv2 — опциональная зависимость: если недоступна, визуальное сравнение
    деградирует до ``qwen_required`` (а не до ложного skip).
  * Модуль fail-soft: ошибка одного блока/рендера не валит отчёт.

Координаты: используем ``coords_norm`` (0..1). Это устойчиво к разным DPI
рендера OLD/NEW. Рендер для визуального сравнения — из ИСХОДНОГО PDF по
``coords_norm`` (``image_file`` в result.json обычно /tmp-PDF от OCR-джобы и
уже недоступен).
"""
from __future__ import annotations

import difflib
import logging
import os
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Config / env
# ═══════════════════════════════════════════════════════════════════════════


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() not in ("0", "false", "no", "off")


def _env_float(name: str, default: float) -> float:
    try:
        raw = os.environ.get(name)
        if raw is None or raw.strip() == "":
            return default
        return float(raw)
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    try:
        raw = os.environ.get(name)
        if raw is None or raw.strip() == "":
            return default
        return int(float(raw))
    except (TypeError, ValueError):
        return default


@dataclass
class BlockEquivalenceConfig:
    """Параметры прекчека (читаются из env, безопасные дефолты)."""

    enabled: bool = False
    mode: str = "observe"           # "observe" — единственный режим Stage 1
    skip_qwen: bool = False         # Stage 1: всегда False (защита от случайного skip)

    iou_threshold: float = 0.5      # порог уверенного one-to-one pairing
    overlap_threshold: float = 0.2  # порог «перекрытия» для split/merge детекции

    render_long_side: int = 1000    # длинная сторона рендера блока для визуала
    visual_diff_pixel_threshold: int = 30   # |gray_old-gray_new| > => «изменён пиксель»
    visual_identical_max_ratio: float = 0.02  # total_diff_ratio <= => identical
    colored_diff_sat_threshold: int = 40     # |S_old - S_new| > => цветное изменение
    colored_identical_max_ratio: float = 0.01  # colored_overlay_diff_ratio <= => identical
    ecc_min_score: float = 0.55     # ниже корреляции ECC => alignment_failed

    max_visual_compares: int = 600  # верхняя граница визуальных сравнений на пару
    text_min_chars: int = 1         # минимум значащих символов чтобы считать «текст есть»

    @classmethod
    def from_env(cls) -> "BlockEquivalenceConfig":
        enabled = _env_flag("STAGE_COMPARISON_BLOCK_EQUIVALENCE_PRECHECK_ENABLED", False)
        mode = (os.environ.get("STAGE_COMPARISON_BLOCK_EQUIVALENCE_PRECHECK_MODE", "observe")
                .strip().lower() or "observe")
        # Stage 1 hard guard: skip разрешается ТОЛЬКО при mode != observe.
        skip = _env_flag("STAGE_COMPARISON_BLOCK_EQUIVALENCE_SKIP_QWEN", False)
        if mode == "observe":
            skip = False
        return cls(
            enabled=enabled,
            mode=mode,
            skip_qwen=skip,
            iou_threshold=_env_float("STAGE_COMPARISON_BLOCK_EQUIVALENCE_IOU_THRESHOLD", 0.5),
            overlap_threshold=_env_float("STAGE_COMPARISON_BLOCK_EQUIVALENCE_OVERLAP_THRESHOLD", 0.2),
            render_long_side=_env_int("STAGE_COMPARISON_BLOCK_EQUIVALENCE_RENDER_LONG_SIDE", 1000),
            visual_diff_pixel_threshold=_env_int(
                "STAGE_COMPARISON_BLOCK_EQUIVALENCE_VISUAL_DIFF_PIXEL_THRESHOLD", 30),
            visual_identical_max_ratio=_env_float(
                "STAGE_COMPARISON_BLOCK_EQUIVALENCE_VISUAL_IDENTICAL_MAX_RATIO", 0.02),
            colored_diff_sat_threshold=_env_int(
                "STAGE_COMPARISON_BLOCK_EQUIVALENCE_COLORED_DIFF_SAT_THRESHOLD", 40),
            colored_identical_max_ratio=_env_float(
                "STAGE_COMPARISON_BLOCK_EQUIVALENCE_COLORED_IDENTICAL_MAX_RATIO", 0.01),
            ecc_min_score=_env_float("STAGE_COMPARISON_BLOCK_EQUIVALENCE_ECC_MIN_SCORE", 0.55),
            max_visual_compares=_env_int("STAGE_COMPARISON_BLOCK_EQUIVALENCE_MAX_VISUAL_COMPARES", 600),
        )


# Decision / qwen_action enums (строки, для JSON-совместимости) -------------

# Категории решения по паре/блоку
DECISION_IDENTICAL_TEXT = "identical_text"
DECISION_IDENTICAL_VISUAL = "identical_visual"
DECISION_CHANGED_TEXT = "changed_text"
DECISION_CHANGED_VISUAL = "changed_visual"
DECISION_ADDED = "added_candidate"
DECISION_DELETED = "deleted_candidate"
DECISION_SPLIT_MERGE = "split_merge_uncertain"
DECISION_RENDER_FAILED = "render_failed"
DECISION_ALIGNMENT_FAILED = "alignment_failed"
DECISION_UNCERTAIN = "uncertain"

# qwen action
QWEN_REQUIRED = "qwen_required"
QWEN_SKIP_CANDIDATE = "qwen_skip_candidate"

# Решения, которые можно (в будущем) пропускать мимо Qwen.
_SKIP_DECISIONS = {DECISION_IDENTICAL_TEXT, DECISION_IDENTICAL_VISUAL}


# ═══════════════════════════════════════════════════════════════════════════
# Dataclasses
# ═══════════════════════════════════════════════════════════════════════════


@dataclass
class EqBlock:
    """Нормализованный блок для прекчека эквивалентности."""

    block_id: str
    page: int
    block_type: str
    coords_norm: Optional[list[float]] = None   # [x0,y0,x1,y1] in 0..1
    coords_px: Optional[list[float]] = None      # [x0,y0,x1,y1] in pixels
    page_width: int = 0
    page_height: int = 0
    text: str = ""
    image_file: Optional[str] = None
    crop_url: Optional[str] = None
    raw: dict = field(default_factory=dict)

    @property
    def is_text_like(self) -> bool:
        return self.block_type in ("text", "table")

    @property
    def is_image_like(self) -> bool:
        return not self.is_text_like


# ═══════════════════════════════════════════════════════════════════════════
# 1. extract_blocks_for_equivalence
# ═══════════════════════════════════════════════════════════════════════════


def _to_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        return int(float(v))
    except (TypeError, ValueError):
        return default


def _coerce_bbox(value: Any) -> Optional[list[float]]:
    """[x0,y0,x1,y1] из flat-bbox / polygon / dict (см. blocks._coerce_bbox)."""
    if value is None:
        return None
    if isinstance(value, dict):
        if "width" in value and "height" in value and ("x" in value or "left" in value):
            x = float(value.get("x", value.get("left", 0)) or 0)
            y = float(value.get("y", value.get("top", 0)) or 0)
            w = float(value.get("width", 0) or 0)
            h = float(value.get("height", 0) or 0)
            return [x, y, x + w, y + h]
        if all(k in value for k in ("x0", "y0", "x1", "y1")):
            return [float(value["x0"]), float(value["y0"]), float(value["x1"]), float(value["y1"])]
        if all(k in value for k in ("left", "top", "right", "bottom")):
            return [float(value["left"]), float(value["top"]), float(value["right"]), float(value["bottom"])]
        return None
    if not isinstance(value, (list, tuple)) or not value:
        return None
    if len(value) == 4 and all(isinstance(c, (int, float)) for c in value):
        return [float(value[0]), float(value[1]), float(value[2]), float(value[3])]
    try:
        xs, ys = [], []
        for pt in value:
            if isinstance(pt, (list, tuple)) and len(pt) >= 2:
                xs.append(float(pt[0]))
                ys.append(float(pt[1]))
        if xs and ys:
            return [min(xs), min(ys), max(xs), max(ys)]
    except (TypeError, ValueError):
        pass
    return None


def _block_type_norm(raw: dict) -> str:
    t = str(raw.get("block_type") or raw.get("type") or raw.get("label") or raw.get("kind") or "").lower().strip()
    if not t:
        return "unknown"
    if any(k in t for k in ("table", "таблиц")):
        return "table"
    if any(k in t for k in ("image", "picture", "figure", "карт", "схем")):
        return "image"
    if "text" in t or "para" in t or "header" in t or "title" in t:
        return "text"
    return t or "unknown"


def _block_page(raw: dict, fallback: int) -> int:
    for key in ("page_number", "page", "page_index", "page_idx"):
        if key in raw and raw[key] is not None:
            val = _to_int(raw[key], 0)
            if key in ("page_index", "page_idx") and val < 1:
                return val + 1
            if val >= 1:
                return val
    return fallback or 1


def _block_text(raw: dict) -> str:
    for k in ("ocr_text", "text", "pdfplumber_text"):
        v = raw.get(k)
        if isinstance(v, str) and v.strip():
            return v
    return ""


def _load_result_obj(result_json: Any) -> Optional[dict]:
    """Принять путь / dict / уже-загруженный объект."""
    if isinstance(result_json, dict):
        return result_json
    if isinstance(result_json, (str, Path)):
        try:
            import json
            p = Path(result_json)
            if not p.exists():
                return None
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            return None
    return None


def extract_blocks_for_equivalence(result_json: Any) -> list[EqBlock]:
    """Прочитать result.json (path/dict) и вернуть блоки для прекчека.

    Поддерживает оба формата result.json:
      A: ``data["pages"] = [{page_number,width,height,blocks:[...]}]``
      B: ``data["blocks"] = [...]`` (page_width/height на блоке).
    """
    data = _load_result_obj(result_json)
    if not isinstance(data, dict):
        return []

    out: list[EqBlock] = []
    seq = 0

    pages = data.get("pages") if isinstance(data, dict) else None
    if isinstance(pages, list):
        for p_idx, page in enumerate(pages):
            if not isinstance(page, dict):
                continue
            page_num = _to_int(page.get("page_number") or page.get("page") or (p_idx + 1), p_idx + 1)
            pw = _to_int(page.get("width", 0))
            ph = _to_int(page.get("height", 0))
            for raw in (page.get("blocks") or []):
                if not isinstance(raw, dict):
                    continue
                seq += 1
                out.append(_mk_eqblock(raw, seq, page_num, pw, ph))

    if not out:
        flat = data.get("blocks") if isinstance(data, dict) else None
        if isinstance(flat, list):
            for raw in flat:
                if not isinstance(raw, dict):
                    continue
                seq += 1
                pw = _to_int(raw.get("page_width", 0))
                ph = _to_int(raw.get("page_height", 0))
                out.append(_mk_eqblock(raw, seq, _block_page(raw, 1), pw, ph))

    return out


def _mk_eqblock(raw: dict, seq: int, page_fallback: int, pw: int, ph: int) -> EqBlock:
    coords_px = _coerce_bbox(raw.get("coords_px") or raw.get("bbox") or raw.get("coords") or raw.get("polygon"))
    coords_norm = _coerce_bbox(raw.get("coords_norm") or raw.get("bbox_norm")
                               or raw.get("polygon_points_norm"))
    if coords_norm is None and coords_px is not None and pw and ph:
        coords_norm = [coords_px[0] / pw, coords_px[1] / ph, coords_px[2] / pw, coords_px[3] / ph]
    bid = str(raw.get("id") or raw.get("block_id") or f"blk{seq:04d}").strip() or f"blk{seq:04d}"
    return EqBlock(
        block_id=bid,
        page=_block_page(raw, page_fallback),
        block_type=_block_type_norm(raw),
        coords_norm=coords_norm,
        coords_px=coords_px,
        page_width=pw,
        page_height=ph,
        text=_block_text(raw),
        image_file=(raw.get("image_file") or None),
        crop_url=(raw.get("crop_url") or None),
        raw={"shape_type": raw.get("shape_type")},
    )


# ═══════════════════════════════════════════════════════════════════════════
# 2. IoU + pairing
# ═══════════════════════════════════════════════════════════════════════════


def bbox_iou_norm(a: Optional[list[float]], b: Optional[list[float]]) -> float:
    """IoU двух bbox в одной системе координат (обычно нормализованной 0..1)."""
    if not a or not b or len(a) != 4 or len(b) != 4:
        return 0.0
    ax0, ay0, ax1, ay1 = min(a[0], a[2]), min(a[1], a[3]), max(a[0], a[2]), max(a[1], a[3])
    bx0, by0, bx1, by1 = min(b[0], b[2]), min(b[1], b[3]), max(b[0], b[2]), max(b[1], b[3])
    if ax1 <= ax0 or ay1 <= ay0 or bx1 <= bx0 or by1 <= by0:
        return 0.0
    ix0, iy0 = max(ax0, bx0), max(ay0, by0)
    ix1, iy1 = min(ax1, bx1), min(ay1, by1)
    if ix1 <= ix0 or iy1 <= iy0:
        return 0.0
    inter = (ix1 - ix0) * (iy1 - iy0)
    union = (ax1 - ax0) * (ay1 - ay0) + (bx1 - bx0) * (by1 - by0) - inter
    return inter / union if union > 0 else 0.0


def _block_box(b: EqBlock) -> Optional[list[float]]:
    """Нормализованный bbox блока (предпочитаем coords_norm)."""
    if b.coords_norm:
        return b.coords_norm
    if b.coords_px and b.page_width and b.page_height:
        return [b.coords_px[0] / b.page_width, b.coords_px[1] / b.page_height,
                b.coords_px[2] / b.page_width, b.coords_px[3] / b.page_height]
    return None


def detect_split_merge_candidates(
    old_blocks: list[EqBlock],
    new_blocks: list[EqBlock],
    *,
    overlap_threshold: float = 0.2,
) -> tuple[list[dict], set[str], set[str]]:
    """Найти split/merge группы (один блок ↔ несколько на другой стороне).

    Возвращает (groups, consumed_old_ids, consumed_new_ids). Группа:
      ``{"old_ids": [...], "new_ids": [...], "reason": "split"|"merge"|"tangled"}``

    Блок считается участником split/merge, если он перекрывает (IoU ≥
    overlap_threshold) ≥2 блока противоположной стороны. Такие блоки
    исключаются из one-to-one matching → ``split_merge_uncertain`` (qwen_required).
    """
    # adjacency по overlap_threshold
    old_to_new: dict[str, list[str]] = {ob.block_id: [] for ob in old_blocks}
    new_to_old: dict[str, list[str]] = {nb.block_id: [] for nb in new_blocks}
    for ob in old_blocks:
        oa = _block_box(ob)
        if not oa:
            continue
        for nb in new_blocks:
            nb_box = _block_box(nb)
            if not nb_box:
                continue
            if bbox_iou_norm(oa, nb_box) >= overlap_threshold:
                old_to_new[ob.block_id].append(nb.block_id)
                new_to_old[nb.block_id].append(ob.block_id)

    # участники: old с ≥2 new ИЛИ new с ≥2 old
    seed_old = {oid for oid, lst in old_to_new.items() if len(lst) >= 2}
    seed_new = {nid for nid, lst in new_to_old.items() if len(lst) >= 2}
    if not seed_old and not seed_new:
        return [], set(), set()

    # Расширить группы по связности (connected components) среди участников
    consumed_old: set[str] = set()
    consumed_new: set[str] = set()
    groups: list[dict] = []
    visited_old: set[str] = set()

    def _grow(seed_oid: str) -> tuple[set[str], set[str]]:
        g_old, g_new = set(), set()
        stack_old = [seed_oid]
        while stack_old:
            oid = stack_old.pop()
            if oid in g_old:
                continue
            g_old.add(oid)
            for nid in old_to_new.get(oid, []):
                if nid not in g_new:
                    g_new.add(nid)
                    for back_oid in new_to_old.get(nid, []):
                        if back_oid not in g_old:
                            stack_old.append(back_oid)
        return g_old, g_new

    # стартуем компоненты от любого участника
    all_seeds_old = set(seed_old)
    for nid in seed_new:
        all_seeds_old.update(new_to_old.get(nid, []))
    for oid in old_blocks_ids(old_blocks):
        if oid not in all_seeds_old or oid in visited_old:
            continue
        g_old, g_new = _grow(oid)
        visited_old |= g_old
        # группа считается split/merge только если действительно >1↔>=1 или 1↔>1
        if len(g_old) >= 2 or len(g_new) >= 2:
            reason = "tangled"
            if len(g_old) == 1 and len(g_new) >= 2:
                reason = "split"          # один OLD → много NEW
            elif len(g_new) == 1 and len(g_old) >= 2:
                reason = "merge"          # много OLD → один NEW
            groups.append({
                "old_ids": sorted(g_old),
                "new_ids": sorted(g_new),
                "reason": reason,
            })
            consumed_old |= g_old
            consumed_new |= g_new

    return groups, consumed_old, consumed_new


def old_blocks_ids(blocks: list[EqBlock]) -> list[str]:
    return [b.block_id for b in blocks]


@dataclass
class PairingResult:
    paired: list[dict] = field(default_factory=list)        # {old_id,new_id,iou,old_page,new_page}
    split_merge: list[dict] = field(default_factory=list)   # groups
    deleted: list[str] = field(default_factory=list)         # unmatched old ids
    added: list[str] = field(default_factory=list)           # unmatched new ids


def pair_blocks_by_iou(
    old_blocks: list[EqBlock],
    new_blocks: list[EqBlock],
    *,
    iou_threshold: float = 0.5,
    overlap_threshold: float = 0.2,
    page_pairs: Optional[list[tuple[int, int]]] = None,
) -> PairingResult:
    """Сопоставить блоки OLD↔NEW по IoU (нормализованные координаты).

    page_pairs: список ``(old_page, new_page)``. Если None — пары страниц
    с ОДИНАКОВЫМ номером (identity), по объединению присутствующих страниц.

    Логика:
      * split/merge детектируются первыми и исключаются из one-to-one;
      * остаток — жадный one-to-one по IoU ≥ iou_threshold;
      * непарные OLD → deleted, непарные NEW → added.

    Сопоставление идёт ВНУТРИ пары страниц (блок OLD-страницы матчится только с
    блоками соответствующей NEW-страницы).

    **One-sided страницы.** Страница, которой нет в ``page_pairs`` (например в
    разреженном/одностороннем ``page_alignment``: лист есть только в OLD или
    только в NEW), НЕ отбрасывается. Все её блоки попадают в deleted (OLD-only)
    или added (NEW-only) — то есть в ``qwen_required``. Иначе на парах с сильно
    «уехавшими» листами охват прекчека был бы неполным.
    """
    res = PairingResult()

    old_by_page: dict[int, list[EqBlock]] = {}
    for ob in old_blocks:
        old_by_page.setdefault(ob.page, []).append(ob)
    new_by_page: dict[int, list[EqBlock]] = {}
    for nb in new_blocks:
        new_by_page.setdefault(nb.page, []).append(nb)

    if page_pairs is None:
        pages = sorted(set(old_by_page) | set(new_by_page))
        page_pairs = [(p, p) for p in pages]

    matched_old: set[str] = set()
    matched_new: set[str] = set()

    for op, np_ in page_pairs:
        old_on = old_by_page.get(op, [])
        new_on = new_by_page.get(np_, [])
        if not old_on and not new_on:
            continue

        # split/merge сперва
        groups, consumed_old, consumed_new = detect_split_merge_candidates(
            old_on, new_on, overlap_threshold=overlap_threshold)
        for g in groups:
            g = dict(g)
            g["old_page"] = op
            g["new_page"] = np_
            res.split_merge.append(g)
        matched_old |= consumed_old
        matched_new |= consumed_new

        # one-to-one по IoU среди оставшихся
        candidates: list[tuple[float, str, str]] = []
        for ob in old_on:
            if ob.block_id in consumed_old:
                continue
            oa = _block_box(ob)
            if not oa:
                continue
            for nb in new_on:
                if nb.block_id in consumed_new:
                    continue
                nb_box = _block_box(nb)
                if not nb_box:
                    continue
                sc = bbox_iou_norm(oa, nb_box)
                if sc >= iou_threshold:
                    candidates.append((sc, ob.block_id, nb.block_id))
        candidates.sort(key=lambda x: -x[0])
        used_l: set[str] = set()
        used_r: set[str] = set()
        for sc, lid, rid in candidates:
            if lid in used_l or rid in used_r:
                continue
            used_l.add(lid)
            used_r.add(rid)
            matched_old.add(lid)
            matched_new.add(rid)
            res.paired.append({
                "old_id": lid, "new_id": rid,
                "iou": round(sc, 4),
                "old_page": op, "new_page": np_,
            })

    # Все непарные блоки → deleted/added, ВКЛЮЧАЯ блоки на one-sided страницах
    # (страницы вне page_pairs). Это гарантирует полный охват: ни один блок не
    # выпадает из отчёта на парах с разреженным/односторонним page_alignment.
    for ob in old_blocks:
        if ob.block_id not in matched_old:
            res.deleted.append(ob.block_id)
    for nb in new_blocks:
        if nb.block_id not in matched_new:
            res.added.append(nb.block_id)

    return res


# ═══════════════════════════════════════════════════════════════════════════
# 3. Text compare
# ═══════════════════════════════════════════════════════════════════════════

_WS_RUN = re.compile(r"[ \t\f\v]+")


def canonicalize_text(text: Optional[str]) -> str:
    """Канонизация текста для строгого сравнения: NFKC, trim, нормализация
    пробелов и переводов строк. Регистр и содержимое СОХРАНЯЮТСЯ (изменение
    регистра/символов может быть значимым)."""
    if not text:
        return ""
    t = unicodedata.normalize("NFKC", str(text))
    t = t.replace("\r\n", "\n").replace("\r", "\n")
    lines = [_WS_RUN.sub(" ", ln).strip() for ln in t.split("\n")]
    lines = [ln for ln in lines if ln]
    return "\n".join(lines).strip()


def compare_text_blocks(old_block: EqBlock, new_block: EqBlock) -> dict:
    """Сравнить текст двух блоков. fuzzy-skip НЕ включаем — similarity только
    логируется. ``text_equal`` = строгое совпадение canonical (оба непустые)."""
    # Общий нормализатор text_block_equivalence (снимает HTML/debug-префикс,
    # ё→е, lower): иначе HTML-обёрнутый ocr_text давал ПРОТИВОПОЛОЖНЫЕ вердикты
    # между этим слоем и text_block_equivalence (reserc.md #60/#13).
    # Импорт локальный: text_block_equivalence импортирует EqBlock отсюда →
    # модульный импорт создал бы циклическую зависимость.
    from backend.app.services.stage_comparison.text_block_equivalence import (
        normalize_block_text,
    )

    co = normalize_block_text(old_block.text)
    cn = normalize_block_text(new_block.text)
    has_old = len(co) >= 1
    has_new = len(cn) >= 1
    text_equal = bool(has_old and has_new and co == cn)
    if has_old or has_new:
        similarity = round(difflib.SequenceMatcher(None, co, cn).ratio(), 4)
    else:
        similarity = 0.0
    return {
        "has_text_old": has_old,
        "has_text_new": has_new,
        "text_equal": text_equal,
        "similarity": similarity,
        "chars_old": len(co),
        "chars_new": len(cn),
    }


# ═══════════════════════════════════════════════════════════════════════════
# 4. Visual compare (cv2 ECC, optional dependency)
# ═══════════════════════════════════════════════════════════════════════════


def _cv2():
    try:
        import cv2  # type: ignore
        return cv2
    except Exception:  # noqa: BLE001 — optional dep
        return None


def cv2_available() -> bool:
    return _cv2() is not None


def _is_raster_path(p: str) -> bool:
    return Path(p).suffix.lower() in (".png", ".jpg", ".jpeg", ".bmp", ".tif", ".tiff", ".webp")


def load_or_render_block_image(
    block: EqBlock,
    *,
    source_pdf_path: Optional[str | Path],
    render_long_side: int = 1000,
):
    """Получить изображение блока (BGR ndarray) для визуального сравнения.

    Приоритет:
      1. ``image_file`` — если это локальный РАСТР (png/jpg/…) и файл существует
         (НЕ /tmp-PDF от OCR-джобы);
      2. рендер из ИСХОДНОГО PDF по ``coords_norm`` (или coords_px+page_size).

    Возвращает ``(img|None, meta)``. ``meta.status`` ∈
    {``image_file``, ``rendered``, ``render_failed``}.
    """
    import numpy as np

    # 1. image_file как локальный растр
    if block.image_file:
        try:
            p = Path(str(block.image_file))
            if p.exists() and p.is_file() and _is_raster_path(str(p)):
                from PIL import Image
                with Image.open(str(p)) as im:
                    arr = np.array(im.convert("RGB"))[:, :, ::-1].copy()  # RGB→BGR
                if arr.size:
                    return arr, {"status": "image_file", "source": str(p)}
        except Exception as exc:  # noqa: BLE001 — fall through to render
            logger.debug("load_or_render_block_image: image_file failed: %s", exc)

    # 2. рендер из исходного PDF по нормализованным координатам
    box = _block_box(block)
    if not source_pdf_path or not box:
        return None, {"status": "render_failed", "error": "no_source_or_box"}
    try:
        import fitz  # PyMuPDF
    except ImportError:
        return None, {"status": "render_failed", "error": "no_pymupdf"}
    sp = Path(str(source_pdf_path))
    if not sp.exists():
        return None, {"status": "render_failed", "error": "pdf_not_found"}
    try:
        doc = fitz.open(str(sp))
        try:
            pno = int(block.page) - 1
            if pno < 0 or pno >= doc.page_count:
                return None, {"status": "render_failed", "error": f"page_oob:{block.page}"}
            page = doc[pno]
            rw, rh = float(page.rect.width), float(page.rect.height)
            x0 = min(box[0], box[2]) * rw
            y0 = min(box[1], box[3]) * rh
            x1 = max(box[0], box[2]) * rw
            y1 = max(box[1], box[3]) * rh
            clip = fitz.Rect(x0, y0, x1, y1) & page.rect
            if clip.is_empty or clip.width < 2 or clip.height < 2:
                return None, {"status": "render_failed", "error": "empty_clip"}
            long_side = max(clip.width, clip.height)
            scale = max(0.5, min(8.0, render_long_side / long_side))
            mat = fitz.Matrix(scale, scale)
            pix = page.get_pixmap(matrix=mat, clip=clip, alpha=False)
            # pixmap → ndarray BGR
            buf = np.frombuffer(pix.samples, dtype=np.uint8)
            arr = buf.reshape(pix.height, pix.width, pix.n)
            if pix.n == 4:
                arr = arr[:, :, :3]
            arr = arr[:, :, ::-1].copy()  # RGB→BGR
            return arr, {"status": "rendered", "width": pix.width, "height": pix.height}
        finally:
            doc.close()
    except Exception as exc:  # noqa: BLE001 — fail-soft
        return None, {"status": "render_failed", "error": f"{type(exc).__name__}"}


def compare_visual_blocks(old_img, new_img, *, cfg: Optional[BlockEquivalenceConfig] = None,
                          debug_path: Optional[str | Path] = None) -> dict:
    """Сравнить два изображения блоков с ECC-выравниванием (cv2).

    Возвращает dict:
      ``status`` ∈ {identical_visual, changed_visual, alignment_failed,
                    render_failed, visual_unavailable}
      ``total_diff_ratio``, ``colored_overlay_diff_ratio``, ``diff_bbox``
      (нормализованный [x0,y0,x1,y1] или None), ``alignment_score``.

    При недоступности cv2 → ``visual_unavailable`` (downstream → qwen_required).
    При несошедшемся ECC → ``alignment_failed`` (visual_uncertain → qwen_required).
    Никогда не бросает наружу (fail-soft).
    """
    cfg = cfg or BlockEquivalenceConfig()
    out = {
        "status": DECISION_RENDER_FAILED,
        "total_diff_ratio": None,
        "colored_overlay_diff_ratio": None,
        "diff_bbox": None,
        "alignment_score": None,
    }
    if old_img is None or new_img is None:
        out["status"] = DECISION_RENDER_FAILED
        return out

    cv2 = _cv2()
    if cv2 is None:
        out["status"] = "visual_unavailable"
        return out

    try:
        import numpy as np

        # Привести к общему размеру (по размеру OLD)
        h, w = old_img.shape[:2]
        if h < 4 or w < 4:
            out["status"] = DECISION_RENDER_FAILED
            return out
        new_resized = cv2.resize(new_img, (w, h), interpolation=cv2.INTER_AREA)

        old_gray = cv2.cvtColor(old_img, cv2.COLOR_BGR2GRAY)
        new_gray = cv2.cvtColor(new_resized, cv2.COLOR_BGR2GRAY)

        # ECC alignment (MOTION_EUCLIDEAN)
        warp = np.eye(2, 3, dtype=np.float32)
        criteria = (cv2.TERM_CRITERIA_EPS | cv2.TERM_CRITERIA_COUNT, 200, 1e-5)
        try:
            cc, warp = cv2.findTransformECC(
                old_gray.astype(np.float32), new_gray.astype(np.float32),
                warp, cv2.MOTION_EUCLIDEAN, criteria, None, 5)
        except cv2.error:
            out["status"] = DECISION_ALIGNMENT_FAILED
            return out
        out["alignment_score"] = round(float(cc), 4)
        if not np.isfinite(cc) or cc < cfg.ecc_min_score:
            out["status"] = DECISION_ALIGNMENT_FAILED
            return out

        aligned_gray = cv2.warpAffine(
            new_gray, warp, (w, h),
            flags=cv2.INTER_LINEAR + cv2.WARP_INVERSE_MAP,
            borderMode=cv2.BORDER_REPLICATE)
        aligned_color = cv2.warpAffine(
            new_resized, warp, (w, h),
            flags=cv2.INTER_LINEAR + cv2.WARP_INVERSE_MAP,
            borderMode=cv2.BORDER_REPLICATE)

        # Маска изменённых пикселей (по серому). Лёгкий blur гасит суб-пиксельный
        # antialiasing (OLD/NEW рендерятся из разных PDF и могут чуть отличаться
        # масштабом/DPI — кромки чёрных линий «дрожат» на пиксель). Без него
        # тонкие линии давали бы ложный changed_visual.
        og = cv2.GaussianBlur(old_gray, (3, 3), 0)
        ag = cv2.GaussianBlur(aligned_gray, (3, 3), 0)
        # Игнорируем тонкую кромку из borderReplicate после варпа.
        diff = cv2.absdiff(og, ag)
        margin = max(1, int(round(min(h, w) * 0.01)))
        if 2 * margin < min(h, w):
            inner = np.zeros_like(diff)
            inner[margin:h - margin, margin:w - margin] = 1
            diff = diff * inner
        mask = (diff > cfg.visual_diff_pixel_threshold).astype(np.uint8)
        total = float(h * w)
        changed = float(int(mask.sum()))
        total_diff_ratio = round(changed / total, 5) if total > 0 else 1.0
        out["total_diff_ratio"] = total_diff_ratio

        # Цветной overlay diff через HSV saturation
        old_s = cv2.cvtColor(old_img, cv2.COLOR_BGR2HSV)[:, :, 1].astype(np.int16)
        new_s = cv2.cvtColor(aligned_color, cv2.COLOR_BGR2HSV)[:, :, 1].astype(np.int16)
        s_diff = np.abs(old_s - new_s)
        colored_mask = (s_diff > cfg.colored_diff_sat_threshold).astype(np.uint8)
        colored_ratio = round(float(int(colored_mask.sum())) / total, 5) if total > 0 else 0.0
        out["colored_overlay_diff_ratio"] = colored_ratio

        # diff bbox (нормализованный)
        ys, xs = np.where(mask > 0)
        if xs.size and ys.size:
            x0, x1 = int(xs.min()), int(xs.max())
            y0, y1 = int(ys.min()), int(ys.max())
            out["diff_bbox"] = [round(x0 / w, 4), round(y0 / h, 4),
                                round((x1 + 1) / w, 4), round((y1 + 1) / h, 4)]

        is_identical = (total_diff_ratio <= cfg.visual_identical_max_ratio
                        and colored_ratio <= cfg.colored_identical_max_ratio)
        out["status"] = DECISION_IDENTICAL_VISUAL if is_identical else DECISION_CHANGED_VISUAL

        if (not is_identical) and debug_path is not None:
            try:
                _write_visual_debug(cv2, np, old_img, aligned_color, mask, debug_path)
            except Exception as exc:  # noqa: BLE001 — debug never fails compare
                logger.debug("compare_visual_blocks: debug write failed: %s", exc)

        return out
    except Exception as exc:  # noqa: BLE001 — fail-soft
        logger.debug("compare_visual_blocks failed: %s", exc)
        out["status"] = DECISION_ALIGNMENT_FAILED
        return out


def _write_visual_debug(cv2, np, old_img, aligned_color, mask, debug_path) -> None:
    """OLD | NEW(aligned) | diff-overlay — горизонтальная склейка для отладки."""
    h, w = old_img.shape[:2]
    overlay = old_img.copy()
    red = np.zeros_like(overlay)
    red[:, :, 2] = 255
    m3 = (mask > 0)[:, :, None]
    overlay = np.where(m3, (0.4 * overlay + 0.6 * red).astype(overlay.dtype), overlay)
    sep = np.full((h, 4, 3), 255, dtype=old_img.dtype)
    combo = np.hstack([old_img, sep, aligned_color, sep, overlay])
    p = Path(debug_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    cv2.imwrite(str(p), combo)


# ═══════════════════════════════════════════════════════════════════════════
# 5. Decision
# ═══════════════════════════════════════════════════════════════════════════


def decide_block_pair(
    old_block: Optional[EqBlock],
    new_block: Optional[EqBlock],
    *,
    text_cmp: Optional[dict] = None,
    visual_cmp: Optional[dict] = None,
    kind: str = "paired",
) -> dict:
    """Принять решение по блоку/паре.

    kind: "paired" | "added" | "deleted" | "split_merge".
    Возвращает ``{"decision", "qwen_action", "reason"}``.

    Безопасность: ``qwen_skip_candidate`` только при уверенной идентичности.
    Любое сомнение → ``qwen_required``.
    """
    if kind == "added":
        return {"decision": DECISION_ADDED, "qwen_action": QWEN_REQUIRED,
                "reason": "block present only in NEW"}
    if kind == "deleted":
        return {"decision": DECISION_DELETED, "qwen_action": QWEN_REQUIRED,
                "reason": "block present only in OLD"}
    if kind == "split_merge":
        return {"decision": DECISION_SPLIT_MERGE, "qwen_action": QWEN_REQUIRED,
                "reason": "one-to-many block overlap (split/merge)"}

    # paired
    text_cmp = text_cmp or {}
    visual_cmp = visual_cmp or {}
    is_text_pair = bool(old_block and new_block
                        and old_block.is_text_like and new_block.is_text_like)

    # Тип блока изменился (text↔image) — это изменение.
    if old_block and new_block and (old_block.is_text_like != new_block.is_text_like):
        return {"decision": DECISION_CHANGED_TEXT if (text_cmp.get("has_text_old") or text_cmp.get("has_text_new"))
                else DECISION_CHANGED_VISUAL,
                "qwen_action": QWEN_REQUIRED, "reason": "block type changed"}

    if is_text_pair:
        # Текстовые блоки полностью покрываются OCR-текстом — он авторитетен.
        if text_cmp.get("text_equal"):
            return {"decision": DECISION_IDENTICAL_TEXT, "qwen_action": QWEN_SKIP_CANDIDATE,
                    "reason": "canonical text identical"}
        if text_cmp.get("has_text_old") and text_cmp.get("has_text_new"):
            return {"decision": DECISION_CHANGED_TEXT, "qwen_action": QWEN_REQUIRED,
                    "reason": f"text differs (sim={text_cmp.get('similarity')})"}
        # текста почти нет — решаем по визуалу
        return _decide_by_visual(visual_cmp, text_cmp)

    # image-like пара: ВИЗУАЛ авторитетен. OCR-текст на чертеже шумный и НЕ
    # должен сам по себе давать changed_text (иначе любой OCR-дребезг ложно
    # помечает неизменённый чертёж). OCR используется только как fallback,
    # когда визуал не дал решения.
    return _decide_by_visual(visual_cmp, text_cmp)


def _decide_by_visual(visual_cmp: dict, text_cmp: dict) -> dict:
    status = visual_cmp.get("status")
    if status == DECISION_IDENTICAL_VISUAL:
        return {"decision": DECISION_IDENTICAL_VISUAL, "qwen_action": QWEN_SKIP_CANDIDATE,
                "reason": f"visual identical (diff={visual_cmp.get('total_diff_ratio')})"}
    if status == DECISION_CHANGED_VISUAL:
        return {"decision": DECISION_CHANGED_VISUAL, "qwen_action": QWEN_REQUIRED,
                "reason": f"visual changed (diff={visual_cmp.get('total_diff_ratio')})"}
    # Визуал не дал решения (alignment/render failed, cv2 нет, не запускался) →
    # fallback на OCR-текст: если он есть с обеих сторон и различается — это
    # сигнал change (changed_text), иначе — честный uncertain → qwen_required.
    ocr_differs = bool(text_cmp.get("has_text_old") and text_cmp.get("has_text_new")
                       and not text_cmp.get("text_equal"))
    if status == DECISION_ALIGNMENT_FAILED:
        if ocr_differs:
            return {"decision": DECISION_CHANGED_TEXT, "qwen_action": QWEN_REQUIRED,
                    "reason": "visual_uncertain (ECC failed); OCR text differs"}
        return {"decision": DECISION_ALIGNMENT_FAILED, "qwen_action": QWEN_REQUIRED,
                "reason": "ECC alignment failed (visual_uncertain)"}
    if status == DECISION_RENDER_FAILED:
        if ocr_differs:
            return {"decision": DECISION_CHANGED_TEXT, "qwen_action": QWEN_REQUIRED,
                    "reason": "render failed; OCR text differs"}
        return {"decision": DECISION_RENDER_FAILED, "qwen_action": QWEN_REQUIRED,
                "reason": "block render failed"}
    if status == "visual_unavailable":
        if ocr_differs:
            return {"decision": DECISION_CHANGED_TEXT, "qwen_action": QWEN_REQUIRED,
                    "reason": "cv2 unavailable; OCR text differs"}
        return {"decision": DECISION_UNCERTAIN, "qwen_action": QWEN_REQUIRED,
                "reason": "cv2 unavailable — no visual compare"}
    if ocr_differs:
        return {"decision": DECISION_CHANGED_TEXT, "qwen_action": QWEN_REQUIRED,
                "reason": "no visual; OCR text differs"}
    return {"decision": DECISION_UNCERTAIN, "qwen_action": QWEN_REQUIRED,
            "reason": "no decisive text or visual signal"}


# ═══════════════════════════════════════════════════════════════════════════
# 6. build_block_equivalence_report
# ═══════════════════════════════════════════════════════════════════════════


def _empty_summary() -> dict:
    return {
        "total_old_blocks": 0,
        "total_new_blocks": 0,
        "paired": 0,
        "identical_text": 0,
        "identical_visual": 0,
        "changed_text": 0,
        "changed_visual": 0,
        "added_candidates": 0,
        "deleted_candidates": 0,
        "uncertain": 0,
        "split_merge": 0,
        "render_failed": 0,
        "alignment_failed": 0,
        "potential_qwen_saved": 0,
        # #63: разбивка причин сбоев визуала для наблюдаемости.
        "render_failed_reasons": {},
        "visual_unavailable": 0,
        "alignment_method_distribution": {},
    }


def build_block_equivalence_report(
    old_result: Any,
    new_result: Any,
    *,
    cfg: Optional[BlockEquivalenceConfig] = None,
    old_pdf_path: Optional[str | Path] = None,
    new_pdf_path: Optional[str | Path] = None,
    page_pairs: Optional[list[tuple[int, int]]] = None,
    debug_dir: Optional[str | Path] = None,
    session_id: Optional[str] = None,
    pair_id: Optional[str] = None,
    generated_at: Optional[str] = None,
) -> dict:
    """Собрать полный отчёт об эквивалентности блоков OLD↔NEW.

    old_result/new_result — путь к result.json, dict или список EqBlock.
    Визуальное сравнение выполняется только если переданы pdf-пути (или
    у блока есть локальный растровый image_file). Без них paired image-блоки
    остаются ``qwen_required`` (uncertain) — это safe-default observe-режима.
    """
    cfg = cfg or BlockEquivalenceConfig.from_env()

    old_blocks = old_result if _is_block_list(old_result) else extract_blocks_for_equivalence(old_result)
    new_blocks = new_result if _is_block_list(new_result) else extract_blocks_for_equivalence(new_result)
    old_by_id = {b.block_id: b for b in old_blocks}
    new_by_id = {b.block_id: b for b in new_blocks}

    pairing = pair_blocks_by_iou(
        old_blocks, new_blocks,
        iou_threshold=cfg.iou_threshold,
        overlap_threshold=cfg.overlap_threshold,
        page_pairs=page_pairs,
    )

    summary = _empty_summary()
    summary["total_old_blocks"] = len(old_blocks)
    summary["total_new_blocks"] = len(new_blocks)
    summary["paired"] = len(pairing.paired)

    pairs_out: list[dict] = []
    warnings: list[str] = []
    visual_compares_done = 0

    # ── paired ────────────────────────────────────────────────────────────
    for pr in pairing.paired:
        ob = old_by_id.get(pr["old_id"])
        nb = new_by_id.get(pr["new_id"])
        if ob is None or nb is None:
            continue
        text_cmp = compare_text_blocks(ob, nb)

        visual_cmp = None
        need_visual = _pair_needs_visual(ob, nb, text_cmp)
        if need_visual:
            if visual_compares_done >= cfg.max_visual_compares:
                warnings.append("max_visual_compares_reached")
                visual_cmp = {"status": DECISION_UNCERTAIN}
            else:
                visual_cmp = _run_visual_for_pair(
                    ob, nb, cfg=cfg,
                    old_pdf_path=old_pdf_path, new_pdf_path=new_pdf_path,
                    debug_dir=debug_dir)
                visual_compares_done += 1

        decision = decide_block_pair(ob, nb, text_cmp=text_cmp, visual_cmp=visual_cmp, kind="paired")
        rec = {
            "old_id": ob.block_id, "new_id": nb.block_id,
            "old_page": pr["old_page"], "new_page": pr["new_page"],
            "old_type": ob.block_type, "new_type": nb.block_type,
            "iou": pr["iou"],
            "decision": decision["decision"],
            "qwen_action": decision["qwen_action"],
            "reason": decision["reason"],
            "text": text_cmp,
        }
        if visual_cmp is not None:
            rec["visual"] = visual_cmp
        pairs_out.append(rec)
        _tally(summary, decision, visual_cmp)  # #63: пробросить visual для разбивки сбоев

    # ── added / deleted ─────────────────────────────────────────────────────
    added_out: list[dict] = []
    for nid in pairing.added:
        nb = new_by_id.get(nid)
        if nb is None:
            continue
        decision = decide_block_pair(None, nb, kind="added")
        added_out.append({"new_id": nid, "page": nb.page, "type": nb.block_type,
                          "decision": decision["decision"], "qwen_action": decision["qwen_action"]})
        _tally(summary, decision)

    deleted_out: list[dict] = []
    for oid in pairing.deleted:
        ob = old_by_id.get(oid)
        if ob is None:
            continue
        decision = decide_block_pair(ob, None, kind="deleted")
        deleted_out.append({"old_id": oid, "page": ob.page, "type": ob.block_type,
                            "decision": decision["decision"], "qwen_action": decision["qwen_action"]})
        _tally(summary, decision)

    # ── split/merge ──────────────────────────────────────────────────────────
    split_out: list[dict] = []
    for g in pairing.split_merge:
        decision = decide_block_pair(None, None, kind="split_merge")
        split_out.append({**g, "decision": decision["decision"], "qwen_action": decision["qwen_action"]})
        _tally(summary, decision)

    if not cv2_available():
        warnings.append("cv2_unavailable_visual_compare_degraded")

    report = {
        "schema_version": 1,
        "session_id": session_id,
        "pair_id": pair_id,
        "generated_at": generated_at,
        "enabled": cfg.enabled,
        "mode": cfg.mode,
        "skip_qwen": cfg.skip_qwen,
        "cv2_available": cv2_available(),
        "thresholds": {
            "iou_threshold": cfg.iou_threshold,
            "overlap_threshold": cfg.overlap_threshold,
            "render_long_side": cfg.render_long_side,
            "visual_diff_pixel_threshold": cfg.visual_diff_pixel_threshold,
            "visual_identical_max_ratio": cfg.visual_identical_max_ratio,
            "colored_diff_sat_threshold": cfg.colored_diff_sat_threshold,
            "colored_identical_max_ratio": cfg.colored_identical_max_ratio,
            "ecc_min_score": cfg.ecc_min_score,
        },
        "page_pairs": [[op, np_] for op, np_ in (page_pairs or [])] or None,
        "summary": summary,
        "pairs": pairs_out,
        "added": added_out,
        "deleted": deleted_out,
        "split_merge": split_out,
        "warnings": warnings,
        "visual_compares_done": visual_compares_done,
    }
    return report


def _is_block_list(obj: Any) -> bool:
    return isinstance(obj, list) and (not obj or isinstance(obj[0], EqBlock))


def _pair_needs_visual(ob: EqBlock, nb: EqBlock, text_cmp: dict) -> bool:
    """Нужно ли визуальное сравнение для пары.

    Текстовая пара с РЕШАЮЩИМ текстовым сигналом (равны или явно различны при
    наличии текста с обеих сторон) визуал не требует. Иначе — требует
    (графика / нет текста / изменился тип)."""
    type_changed = (ob.is_text_like != nb.is_text_like)
    if type_changed:
        return False  # тип изменился → и так change, визуал не нужен
    if ob.is_text_like and nb.is_text_like:
        if text_cmp.get("text_equal"):
            return False
        if text_cmp.get("has_text_old") and text_cmp.get("has_text_new"):
            return False  # текст явно различается — OCR авторитетен для текста
        return True  # текста нет → пробуем визуал
    # image-like пара: визуал авторитетен для чертежей — пробуем всегда.
    return True


def _run_visual_for_pair(
    ob: EqBlock, nb: EqBlock, *,
    cfg: BlockEquivalenceConfig,
    old_pdf_path: Optional[str | Path],
    new_pdf_path: Optional[str | Path],
    debug_dir: Optional[str | Path],
) -> dict:
    old_img, old_meta = load_or_render_block_image(
        ob, source_pdf_path=old_pdf_path, render_long_side=cfg.render_long_side)
    new_img, new_meta = load_or_render_block_image(
        nb, source_pdf_path=new_pdf_path, render_long_side=cfg.render_long_side)
    if old_img is None or new_img is None:
        return {"status": DECISION_RENDER_FAILED,
                "old_render": old_meta.get("status"), "new_render": new_meta.get("status"),
                # #63: суб-причина (no_source/pdf_not_found/empty_clip/page_oob) для summary.
                "render_error": old_meta.get("error") or new_meta.get("error") or "unknown",
                "total_diff_ratio": None, "colored_overlay_diff_ratio": None,
                "diff_bbox": None, "alignment_score": None}
    debug_path = None
    if debug_dir is not None:
        safe = "".join(c if c.isalnum() else "_" for c in ob.block_id)[:48]
        debug_path = Path(debug_dir) / f"{safe}_diff.png"
    res = compare_visual_blocks(old_img, new_img, cfg=cfg, debug_path=debug_path)
    res["old_render"] = old_meta.get("status")
    res["new_render"] = new_meta.get("status")
    return res


def _tally(summary: dict, decision: dict, visual_cmp: Optional[dict] = None) -> None:
    # #63: разбивка причин визуальных сбоев (по сырому visual_cmp, даже если OCR
    # потом перевёл decision в changed_text).
    if visual_cmp:
        vstatus = visual_cmp.get("status")
        if vstatus == DECISION_RENDER_FAILED:
            err = str(visual_cmp.get("render_error") or "unknown")
            rf = summary.setdefault("render_failed_reasons", {})
            rf[err] = rf.get(err, 0) + 1
        elif vstatus == "visual_unavailable":
            summary["visual_unavailable"] = summary.get("visual_unavailable", 0) + 1
        if visual_cmp.get("alignment_score") is not None:
            amd = summary.setdefault("alignment_method_distribution", {})
            amd["euclidean"] = amd.get("euclidean", 0) + 1
    d = decision["decision"]
    mapping = {
        DECISION_IDENTICAL_TEXT: "identical_text",
        DECISION_IDENTICAL_VISUAL: "identical_visual",
        DECISION_CHANGED_TEXT: "changed_text",
        DECISION_CHANGED_VISUAL: "changed_visual",
        DECISION_ADDED: "added_candidates",
        DECISION_DELETED: "deleted_candidates",
        DECISION_SPLIT_MERGE: "split_merge",
        DECISION_RENDER_FAILED: "render_failed",
        DECISION_ALIGNMENT_FAILED: "alignment_failed",
        DECISION_UNCERTAIN: "uncertain",
    }
    key = mapping.get(d)
    if key:
        summary[key] = summary.get(key, 0) + 1
    if decision.get("qwen_action") == QWEN_SKIP_CANDIDATE:
        summary["potential_qwen_saved"] = summary.get("potential_qwen_saved", 0) + 1


# ═══════════════════════════════════════════════════════════════════════════
# 7. Pair-level orchestration (resolve session/pair → report artifact)
# ═══════════════════════════════════════════════════════════════════════════


def _utc_now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _page_pairs_from_alignment(session_id: str, pair_id: str) -> Optional[list[tuple[int, int]]]:
    """Построить (old_page, new_page) из page_alignment.json (left=old, right=new).

    Возвращает None при отсутствии/ошибке alignment → pairing использует
    identity по номерам страниц.
    """
    try:
        from . import store as store_mod
        alignment = store_mod.get_alignment(session_id, pair_id)
    except Exception:  # noqa: BLE001 — fail-soft → identity page mapping
        return None
    # store.get_alignment вкладывает карту в ключ "alignment" (внутри — items);
    # поддерживаем и плоский вид {items:[...]} (например, прямой read файла).
    alignment = alignment or {}
    inner = alignment.get("alignment") if isinstance(alignment.get("alignment"), dict) else alignment
    items = (inner or {}).get("items") or []
    pairs: list[tuple[int, int]] = []
    for it in items:
        lp = it.get("left_page")
        rp = it.get("right_page")
        if lp is None or rp is None:
            continue
        try:
            pairs.append((int(lp), int(rp)))
        except (TypeError, ValueError):
            continue
    return pairs or None


def build_pair_diagnostics(report: dict) -> dict:
    """Компактная диагностика для pipeline/job status (см. задачу, п.9)."""
    s = (report or {}).get("summary") or {}
    return {
        "enabled": report.get("enabled"),
        "mode": report.get("mode"),
        "skip_qwen": report.get("skip_qwen"),
        "cv2_available": report.get("cv2_available"),
        "total_old_blocks": s.get("total_old_blocks", 0),
        "total_new_blocks": s.get("total_new_blocks", 0),
        "paired": s.get("paired", 0),
        "identical_text": s.get("identical_text", 0),
        "identical_visual": s.get("identical_visual", 0),
        "changed_visual": s.get("changed_visual", 0),
        "changed_text": s.get("changed_text", 0),
        "added_candidates": s.get("added_candidates", 0),
        "deleted_candidates": s.get("deleted_candidates", 0),
        "split_merge": s.get("split_merge", 0),
        "uncertain": s.get("uncertain", 0)
        + s.get("render_failed", 0) + s.get("alignment_failed", 0),
        "potential_qwen_saved": s.get("potential_qwen_saved", 0),
        # #63: разбивка причин визуальных сбоев.
        "render_failed_reasons": s.get("render_failed_reasons", {}),
        "visual_unavailable": s.get("visual_unavailable", 0),
        "alignment_method_distribution": s.get("alignment_method_distribution", {}),
        "report_path_rel": "block_equivalence/block_equivalence_report.json",
    }


def run_pair_precheck(
    session_id: str,
    pair_id: str,
    *,
    cfg: Optional[BlockEquivalenceConfig] = None,
    write_artifact: bool = True,
    write_debug: bool = True,
) -> Optional[dict]:
    """Прогнать прекчек эквивалентности для одной пары и (опц.) записать
    артефакт. left=OLD (старая стадия), right=NEW (новая стадия).

    Возвращает компактную диагностику (``build_pair_diagnostics``) или None,
    если прекчек не применим (нет result.json и т.п.). НИЧЕГО не пропускает —
    Stage 1 observe. Полностью fail-soft: любая ошибка → None + warning в лог.
    """
    cfg = cfg or BlockEquivalenceConfig.from_env()
    try:
        from . import store as store_mod
        from . import paths as paths_mod

        pair = store_mod._find_pair_meta(session_id, pair_id)
        if not pair:
            logger.debug("block_equivalence: pair_not_found %s/%s", session_id, pair_id)
            return None
        left = pair.get("left") or {}
        right = pair.get("right") or {}
        old_rjp = left.get("result_json_path")
        new_rjp = right.get("result_json_path")
        if not old_rjp or not new_rjp:
            logger.debug("block_equivalence: missing result_json for %s/%s", session_id, pair_id)
            return None

        page_pairs = _page_pairs_from_alignment(session_id, pair_id)
        debug_dir = None
        if write_debug:
            try:
                debug_dir = paths_mod.block_equivalence_debug_dir(session_id, pair_id)
            except Exception:  # noqa: BLE001
                debug_dir = None

        report = build_block_equivalence_report(
            old_rjp, new_rjp,
            cfg=cfg,
            old_pdf_path=left.get("pdf_path"),
            new_pdf_path=right.get("pdf_path"),
            page_pairs=page_pairs,
            debug_dir=debug_dir,
            session_id=session_id,
            pair_id=pair_id,
            generated_at=_utc_now_iso(),
        )

        if write_artifact:
            try:
                import json
                out = paths_mod.block_equivalence_report_path(session_id, pair_id)
                tmp = out.with_suffix(".json.tmp")
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(report, f, ensure_ascii=False, indent=2)
                tmp.replace(out)
            except Exception as exc:  # noqa: BLE001 — artifact write failure is non-fatal
                logger.warning("block_equivalence: report write failed %s/%s: %s",
                               session_id, pair_id, exc)

        return build_pair_diagnostics(report)
    except Exception as exc:  # noqa: BLE001 — never fail the caller
        logger.warning("block_equivalence: run_pair_precheck failed %s/%s: %s",
                       session_id, pair_id, exc)
        return None


def read_pair_report(session_id: str, pair_id: str) -> Optional[dict]:
    """Прочитать сохранённый отчёт пары (если есть). Используется orchestrator'ом
    для surface'инга diagnostics без повторного расчёта."""
    try:
        import json
        from . import paths as paths_mod
        p = paths_mod.block_equivalence_report_path(session_id, pair_id)
        if not p.exists():
            return None
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:  # noqa: BLE001
        return None


__all__ = [
    "BlockEquivalenceConfig",
    "EqBlock",
    "extract_blocks_for_equivalence",
    "bbox_iou_norm",
    "pair_blocks_by_iou",
    "detect_split_merge_candidates",
    "PairingResult",
    "canonicalize_text",
    "compare_text_blocks",
    "load_or_render_block_image",
    "compare_visual_blocks",
    "cv2_available",
    "decide_block_pair",
    "build_block_equivalence_report",
    "build_pair_diagnostics",
    "run_pair_precheck",
    "read_pair_report",
]
