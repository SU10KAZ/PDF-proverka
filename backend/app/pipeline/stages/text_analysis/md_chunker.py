"""
text_analysis/md_chunker.py
---------------------------
Нарезка MD текст-анализа на чанки, когда полный промпт превышает жёсткий лимит
Codex CLI на один ход (1 048 576 символов входа → turn/start: input_too_large,
падение за ~1 сек после thread.started).

Стратегия (двухступенчатая, решение зафиксировано с Андреем Ивановичем):
  Tier 1 — вызывающий сначала убирает inline-базу норм из system (её всё равно
           перепроверяет этап 04). Часто этого хватает и нарезка не нужна.
  Tier 2 — если и без норм-базы промпт > лимита, режем MD ПО ЛИСТАМ и в КАЖДЫЙ
           чанк добавляем общий «скелет» — сводные текстовые листы (ПЗ, таблицы
           нагрузок, спецификация). Скелет сохраняет перекрёстную сверку
           (ПЗ↔таблицы↔спецификация), которая иначе умерла бы между чанками.

Разбор MD на листы переиспользует `build_fact_index` из evidence_first_fallback:
текстовые сводные листы (без image-блоков) классифицируются как section_class
== "pz" — это и есть кандидаты в скелет.

Склейка частичных ответов — `merge_text_analysis_parts`: конкатенация
text_findings с дедупом по (source+суть+норма) и сквозной перенумерацией
T-001…, объединение normative_refs_found, shallow-merge project_params,
ремап finding_id в items_verified_from_blocks.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

# Бюджет с запасом под служебную обёртку (Required output field: …, разделители).
# Ниже жёсткого лимита Codex 1 048 576.
CODEX_TEXT_INPUT_BUDGET = 1_000_000

# Максимум символов «скелета» (сводных листов), дублируемого в каждый чанк.
# Если ПЗ крупнее — усекаем (перекрёстная сверка становится частичной, логируем).
DEFAULT_SKELETON_CAP = 200_000

# Оверхед промпт-обёртки (маркеры скелета/таргета, префикс, Required output field).
_WRAPPER_OVERHEAD = 6_000

# Минимальный полезный размер чанка: если после скелета остаётся меньше — режем
# сам скелет, иначе получим слишком много микро-чанков.
_MIN_PER_CHUNK = 60_000


@dataclass
class ChunkPlan:
    """План нарезки текст-анализа.

    skeleton — общий блок сводных листов (reference-only) в каждый чанк.
    chunks   — список тел таргет-листов на каждый проход.
    """
    skeleton: str
    chunks: list[str]
    skeleton_truncated: bool = False
    total_pages: int = 0
    skeleton_pages: int = 0


def _cap_join(bodies: list[str], cap: int) -> tuple[str, bool]:
    """Склеить тела листов, не превышая cap символов. Вернуть (текст, усечён?)."""
    out: list[str] = []
    used = 0
    truncated = False
    for b in bodies:
        b = b or ""
        piece = (b + "\n\n")
        if used + len(piece) > cap:
            remaining = cap - used
            if remaining > 500:
                out.append(b[:remaining])
            truncated = True
            break
        out.append(b)
        used += len(piece)
    return ("\n\n".join(out).strip(), truncated)


def _group_pages(bodies: list[str], per_chunk: int) -> list[str]:
    """Сгруппировать тела листов в чанки, не превышая per_chunk символов.

    Один лист крупнее per_chunk становится отдельным чанком как есть (внутри
    листа не режем — это увело бы за пределы атомарности замечаний).
    """
    chunks: list[str] = []
    cur: list[str] = []
    cur_len = 0
    for b in bodies:
        b = b or ""
        blen = len(b) + 2
        if cur and cur_len + blen > per_chunk:
            chunks.append("\n\n".join(cur))
            cur, cur_len = [], 0
        cur.append(b)
        cur_len += blen
    if cur:
        chunks.append("\n\n".join(cur))
    return chunks or [""]


def plan_text_analysis_chunks(
    md: str,
    *,
    total_budget: int = CODEX_TEXT_INPUT_BUDGET,
    system_len: int,
    skeleton_cap: int = DEFAULT_SKELETON_CAP,
    wrapper_overhead: int = _WRAPPER_OVERHEAD,
) -> ChunkPlan | None:
    """Построить план нарезки или вернуть None, если весь MD влезает одним ходом.

    system_len — длина system-промпта (УЖЕ без норм-базы: Tier 1 применён
    вызывающим). Если system + md + обёртка помещаются в бюджет — None
    (single-pass, поведение без изменений).
    """
    md = md or ""
    if system_len + len(md) + wrapper_overhead <= total_budget:
        return None

    # Разбор MD на листы (переиспользуем детерминированный парсер evidence_first).
    from backend.app.services.stage_comparison.evidence_first_fallback import (
        build_fact_index,
    )

    fi = build_fact_index("text", md)
    pages = fi.pages
    bodies = [p.body for p in pages]

    # Скелет = сводные текстовые листы (ПЗ/содержание/ведомости/таблицы/спец).
    skeleton_bodies = [p.body for p in pages if p.section_class == "pz"]
    skeleton, sk_trunc = _cap_join(skeleton_bodies, skeleton_cap)

    per_chunk = total_budget - system_len - len(skeleton) - wrapper_overhead
    if per_chunk < _MIN_PER_CHUNK:
        # Скелет съел бюджет — ужимаем его до трети бюджета (крайний случай:
        # огромная ПЗ). Перекрёстная сверка станет частичной.
        hard_cap = max(0, total_budget // 3)
        skeleton, sk_trunc2 = _cap_join(skeleton_bodies, hard_cap)
        sk_trunc = sk_trunc or sk_trunc2
        per_chunk = total_budget - system_len - len(skeleton) - wrapper_overhead
        per_chunk = max(per_chunk, _MIN_PER_CHUNK)

    chunks = _group_pages(bodies, per_chunk)
    return ChunkPlan(
        skeleton=skeleton,
        chunks=chunks,
        skeleton_truncated=sk_trunc,
        total_pages=len(pages),
        skeleton_pages=len(skeleton_bodies),
    )


# ─── Склейка частичных ответов ──────────────────────────────────────────────


def _norm_key(s: str) -> str:
    """Грубая нормализация текста для дедупа: только буквы/цифры, нижний регистр."""
    s = (s or "").lower()
    s = re.sub(r"[^0-9a-zа-яё]+", " ", s).strip()
    return s[:160]


def _finding_dedup_key(f: dict) -> tuple:
    return (
        _norm_key(str(f.get("source", ""))),
        _norm_key(str(f.get("finding", ""))),
        _norm_key(str(f.get("norm", ""))),
    )


def merge_text_analysis_parts(parts: list[dict]) -> dict:
    """Слить частичные 02_text_analysis.json (по одному на чанк) в один документ.

    - text_findings: конкат → дедуп по (source+суть+норма) → перенумерация T-001…
    - normative_refs_found: объединение по ref
    - project_params: shallow-merge непустых значений
    - items_verified_from_blocks: ремап finding_id на новые T-NNN
    - остальные скалярные поля (stage/project_id/text_source/…): из первого части
    """
    parts = [p for p in parts if isinstance(p, dict)]
    if not parts:
        return {"text_findings": [], "normative_refs_found": []}

    merged: dict = dict(parts[0])

    # ── text_findings: дедуп + перенумерация, с картой (part_idx, old_id)→new_id ──
    kept: list[dict] = []
    seen: dict[tuple, str] = {}          # dedup_key → new_id
    id_map: dict[tuple, str] = {}        # (part_idx, old_id) → new_id (для ремапа)
    for pi, part in enumerate(parts):
        for f in (part.get("text_findings") or []):
            if not isinstance(f, dict):
                continue
            old_id = str(f.get("id", ""))
            key = _finding_dedup_key(f)
            if key in seen:
                # дубль: ссылки этой части ведут на уже сохранённое замечание
                id_map[(pi, old_id)] = seen[key]
                continue
            new_id = f"T-{len(kept) + 1:03d}"
            nf = dict(f)
            nf["id"] = new_id
            kept.append(nf)
            seen[key] = new_id
            if old_id:
                id_map[(pi, old_id)] = new_id
    merged["text_findings"] = kept

    # ── normative_refs_found: объединение по ref ──
    refs: list[dict] = []
    seen_refs: set[str] = set()
    for part in parts:
        for r in (part.get("normative_refs_found") or []):
            if not isinstance(r, dict):
                continue
            rk = _norm_key(str(r.get("ref", "")))
            if rk and rk in seen_refs:
                continue
            if rk:
                seen_refs.add(rk)
            refs.append(r)
    merged["normative_refs_found"] = refs

    # ── project_params: shallow-merge непустых ──
    params: dict = {}
    for part in parts:
        pp = part.get("project_params")
        if isinstance(pp, dict):
            for k, v in pp.items():
                if k not in params or params[k] in (None, "", [], {}, 0):
                    params[k] = v
    if params:
        merged["project_params"] = params

    # ── items_verified_from_blocks: ремап finding_id ──
    items: list[dict] = []
    for pi, part in enumerate(parts):
        for it in (part.get("items_verified_from_blocks") or []):
            if not isinstance(it, dict):
                continue
            old_fid = str(it.get("finding_id", ""))
            new_fid = id_map.get((pi, old_fid))
            if not new_fid:
                continue  # ссылка на выброшенное/неизвестное замечание — отбрасываем
            nit = dict(it)
            nit["finding_id"] = new_fid
            items.append(nit)
    if items or "items_verified_from_blocks" in merged:
        merged["items_verified_from_blocks"] = items

    return merged
