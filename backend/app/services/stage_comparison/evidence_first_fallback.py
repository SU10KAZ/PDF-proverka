"""evidence_first_s2_fallback — fallback-стратегия для больших enriched MD пар.

Когда суммарный объём `left_enriched.md + right_enriched.md` превышает
`STAGE_COMPARISON_ENRICHED_COMPARE_MAX_CHARS`, обычный путь
`run_enriched_comparison` отдаёт `status=too_large` и `changes=[]` — пара
выпадает из сравнения целиком.

Эта стратегия (выбрана по research-отчёту от 2026-05-29, проект КР2) НЕ
сравнивает огромные MD напрямую и НЕ делает наивный compact. Pipeline:

    raw enriched MD
      → canonical fact index (left/right)      # детерминированный парсинг
      → scope map                              # какие разделы есть с каждой стороны
      → deterministic fact diff                # штамп / scope-only разделы / листы — без LLM
      → scope-aware section split              # выровненные чанки ≤ chunk budget
      → shared global header                   # штамп + материальная сводка в каждый чанк
      → per-chunk LLM compare/review           # Opus по каждому чанку
      → original MD evidence verification      # каждый quote сверяется с raw MD
      → merge + dedup                          # детерминированные + LLM, дедуп по сигнатуре
      → final comparison_result.json

Почему так (см. research): на КР2 (865K) naive-full дал 7 confirmed_unique,
compact — 8, а scope-aware section split + evidence verification — 13. raw count
section-split (38) обманчив: ~половина — description-variance из low-confidence
Qwen-блоков, её снимает evidence verification + дедуп.

Контракт безопасности:
  * Стратегия вызывается ТОЛЬКО из too_large-ветки и ТОЛЬКО когда включён флаг
    `STAGE_COMPARISON_EVIDENCE_FIRST_FALLBACK_ENABLED` (default false → shadow).
  * Каждый per-chunk вызов укладывается в chunk budget — общий лимит 600K не
    отменяется, safety-guard сохраняется на уровне чанка.
  * Любая ошибка внутри стратегии не валит пару: возвращается осмысленный
    payload (в худшем случае — с детерминированными изменениями и warning).
"""
from __future__ import annotations

import json
import logging
import os
import re
import time
import unicodedata
import uuid
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)

STRATEGY = "evidence_first_s2_fallback"
STRATEGY_VERSION = 1


# ─── Config ──────────────────────────────────────────────────────────────


@dataclass
class FallbackConfig:
    enabled: bool = False
    chunk_max_chars: int = 200_000      # бюджет на (left+right) одного чанка
    max_chunks: int = 16                # верхняя граница числа LLM-вызовов
    header_max_chars: int = 12_000      # cap shared global header
    min_quote_len: int = 8              # минимальная длина quote для верификации
    fuzzy_threshold: float = 0.6        # token-overlap для «нашлось»
    drop_ungrounded: bool = True        # выкидывать LLM-changes без evidence в raw MD


def _env_bool(name: str, default: bool = False) -> bool:
    raw = (os.environ.get(name) or "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def load_fallback_config() -> FallbackConfig:
    return FallbackConfig(
        enabled=_env_bool("STAGE_COMPARISON_EVIDENCE_FIRST_FALLBACK_ENABLED", False),
        chunk_max_chars=_env_int("STAGE_COMPARISON_EVIDENCE_FIRST_CHUNK_MAX_CHARS", 200_000),
        max_chunks=_env_int("STAGE_COMPARISON_EVIDENCE_FIRST_MAX_CHUNKS", 16),
        header_max_chars=_env_int("STAGE_COMPARISON_EVIDENCE_FIRST_HEADER_MAX_CHARS", 12_000),
        min_quote_len=_env_int("STAGE_COMPARISON_EVIDENCE_FIRST_MIN_QUOTE_LEN", 8),
        fuzzy_threshold=_env_float("STAGE_COMPARISON_EVIDENCE_FIRST_FUZZY_THRESHOLD", 0.6),
        drop_ungrounded=_env_bool("STAGE_COMPARISON_EVIDENCE_FIRST_DROP_UNGROUNDED", True),
    )


# ─── Stage 1: canonical fact index ─────────────────────────────────────────


_PAGE_HEADING_RE = re.compile(
    r"^\s{0,3}#{1,6}\s+(?:СТРАНИЦА|Страница|PAGE|Page)\s*[:№#]?\s*(\d+)\s*$",
    re.MULTILINE,
)
_SHEET_NO_RE = re.compile(r"\*\*Лист:\*\*\s*(.+)")
_SHEET_NAME_RE = re.compile(r"\*\*Наименование листа:\*\*\s*(.+)")
_STAMP_RE = re.compile(r"\*\*Штамп:\*\*\s*(.+)")
_QWEN_BLOCK_ID_RE = re.compile(r"block_id:\s*(\S+)")

# Маркеры классификации раздела (section_class).
_SECTION_MARKERS = {
    "pz": (
        "пояснительная записка", "содержание тома", "содержание раздела",
        "общие данные", "общие указания", "ведомость",
    ),
    "architectural": (
        "архитектурные решения", "фасад", "план этажа",
    ),
    "sections_details": (
        "разрез", "сечени", "узел", "узлы", "детали армировани",
        "детали ", "профили ферм",
    ),
    "structural": (
        "схема расположения", "монолитных конструкци", "плиты перекрыти",
        "плиты покрыти", "плита перекрыти", "плита покрыти",
        "фундамент", "армировани", "конструктивные",
    ),
}

# Материальные/спецификационные маркеры для shared header.
_MATERIAL_MARKERS = (
    "класс бетона", "бетон класса", "арматура", "арматурн",
    "w6", "w8", "w10", "f100", "f150", "f200", "а240", "а400",
    "а500", "в25", "в30", "в35", "морозостойкост", "водонепроницаем",
)


@dataclass
class PageRec:
    page: int
    sheet_no: str
    sheet_name: str
    body: str
    section_class: str
    building_part: str
    image_block_ids: list[str] = field(default_factory=list)

    @property
    def scope_key(self) -> str:
        return f"{self.section_class}|{self.building_part}"


@dataclass
class FactIndex:
    side: str
    pages: list[PageRec]
    stamp: str
    total_chars: int

    def scope_keys(self) -> set[str]:
        return {p.scope_key for p in self.pages}

    def pages_for(self, scope_key: str) -> list[PageRec]:
        return [p for p in self.pages if p.scope_key == scope_key]


def _norm_building_part(sheet_name: str) -> str:
    """Извлечь нормализованную привязку к корпусу из имени листа."""
    s = sheet_name or ""
    m = re.search(r"Корпус[аы]?\s*([0-9][0-9.,\s]*)", s, re.IGNORECASE)
    if not m:
        return "общий"
    raw = m.group(1)
    nums = re.findall(r"\d+(?:\.\d+)?", raw)
    if not nums:
        return "общий"
    return ",".join(sorted(set(nums), key=lambda x: (float(x))))


def _classify_section(sheet_name: str, body: str, n_images: int) -> str:
    """Классификация раздела листа.

    Ключевой сигнал — наличие image-блоков: текстовые/ПЗ-листы их не имеют
    (`img=0`), чертёжные — имеют (`img>=1`). Это устойчивее, чем ловить
    маркеры в прозе ПЗ (которая полна слов «конструктивные», «монолитных»).
    """
    name = (sheet_name or "").lower()
    # Текстовая страница (нет чертежей) → ПЗ; очень короткая → служебная.
    if n_images == 0:
        if len((body or "").strip()) < 300:
            return "other"
        return "pz"
    # Чертёжный лист. Классифицируем по имени листа (приоритет), затем телу.
    hay = name if name else (body or "")[:300].lower()
    if "архитектурн" in hay or "фасад" in hay:
        return "architectural"
    # «Схема расположения …» — устойчивый маркер КР; проверяем раньше, чем
    # «план», чтобы конструктивные схемы не утекали в АР.
    if "схема расположения" in hay or "монолитных конструкци" in hay \
            or "плиты перекрыти" in hay or "плиты покрыти" in hay \
            or "плита перекрыти" in hay or "плита покрыти" in hay:
        return "structural"
    if any(m in hay for m in _SECTION_MARKERS["sections_details"]):
        return "sections_details"
    # «План N этажа / план кровли» без «схемы расположения» — это АР.
    if re.search(r"план\s+(?:\d+|перв|втор|трет|кровл|техническ|подзем)", hay):
        return "architectural"
    if any(m in hay for m in _SECTION_MARKERS["structural"]):
        return "structural"
    return "structural"  # чертёж без явных маркеров → структурный по умолчанию


def _extract_stamp(md: str) -> str:
    """Достать первую штамп-строку (Шифр|Стадия|Объект|Организация)."""
    m = _STAMP_RE.search(md or "")
    if m:
        return m.group(1).strip()[:600]
    return ""


def build_fact_index(side: str, md: str) -> FactIndex:
    """Stage 1: распарсить enriched MD в детерминированный fact index."""
    md = md or ""
    pages: list[PageRec] = []
    matches = list(_PAGE_HEADING_RE.finditer(md))
    if matches:
        for i, m in enumerate(matches):
            num = int(m.group(1))
            start = m.end()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(md)
            body = md[start:end]
            sno = _SHEET_NO_RE.search(body)
            snm = _SHEET_NAME_RE.search(body)
            sheet_no = (sno.group(1).strip() if sno else "").strip("- ")
            sheet_name = (snm.group(1).strip() if snm else "").strip("- ")
            block_ids = _QWEN_BLOCK_ID_RE.findall(body)
            sect = _classify_section(sheet_name, body, len(block_ids))
            bpart = _norm_building_part(sheet_name) if sect in ("structural", "sections_details") else "общий"
            pages.append(PageRec(
                page=num, sheet_no=sheet_no, sheet_name=sheet_name,
                body=body, section_class=sect, building_part=bpart,
                image_block_ids=block_ids,
            ))
    else:
        # Нет page-разметки — единый псевдо-лист (стратегия всё равно
        # отработает через section split одного чанка).
        pages.append(PageRec(
            page=1, sheet_no="", sheet_name="", body=md,
            section_class="other", building_part="общий",
            image_block_ids=_QWEN_BLOCK_ID_RE.findall(md),
        ))
    return FactIndex(side=side, pages=pages, stamp=_extract_stamp(md), total_chars=len(md))


# ─── Stage 2: scope map ────────────────────────────────────────────────────


@dataclass
class ScopeMap:
    left_only: list[str]            # scope_keys только слева
    right_only: list[str]           # scope_keys только справа
    common: list[str]               # scope_keys с обеих сторон
    left_index: FactIndex
    right_index: FactIndex

    def as_dict(self) -> dict:
        def summarize(idx: FactIndex, keys: list[str]) -> list[dict]:
            out = []
            for k in keys:
                ps = idx.pages_for(k)
                out.append({
                    "scope_key": k,
                    "pages": [p.page for p in ps],
                    "chars": sum(len(p.body) for p in ps),
                    "sheet_names": sorted({p.sheet_name for p in ps if p.sheet_name})[:8],
                })
            return out
        return {
            "left_only": summarize(self.left_index, self.left_only),
            "right_only": summarize(self.right_index, self.right_only),
            "common": [
                {
                    "scope_key": k,
                    "left_pages": [p.page for p in self.left_index.pages_for(k)],
                    "right_pages": [p.page for p in self.right_index.pages_for(k)],
                    "left_chars": sum(len(p.body) for p in self.left_index.pages_for(k)),
                    "right_chars": sum(len(p.body) for p in self.right_index.pages_for(k)),
                }
                for k in self.common
            ],
        }


def build_scope_map(left_index: FactIndex, right_index: FactIndex) -> ScopeMap:
    """Stage 2: какие scope_keys есть слева/справа/с обеих сторон."""
    lk = left_index.scope_keys()
    rk = right_index.scope_keys()
    # Сохраняем порядок появления для детерминированности.
    def ordered(idx: FactIndex, keys: set[str]) -> list[str]:
        seen: list[str] = []
        for p in idx.pages:
            if p.scope_key in keys and p.scope_key not in seen:
                seen.append(p.scope_key)
        return seen
    return ScopeMap(
        left_only=ordered(left_index, lk - rk),
        right_only=ordered(right_index, rk - lk),
        common=ordered(left_index, lk & rk),
        left_index=left_index,
        right_index=right_index,
    )


# ─── Stage 3: deterministic fact diff (без LLM) ────────────────────────────


def _mk_change(
    *, type_: str, source: str, severity: str, title: str, summary: str,
    old_value: str = "", new_value: str = "", category: str = "general",
    construction_impact: str = "", cost_impact: str = "unknown",
    requires_human_review: bool = True, confidence: float = 0.7,
    evidence_left: Optional[dict] = None, evidence_right: Optional[dict] = None,
    provenance: str = "deterministic",
) -> dict:
    return {
        "id": f"chg_{uuid.uuid4().hex[:10]}",
        "source": source,
        "type": type_,
        "category": category,
        "severity": severity,
        "title": title[:240],
        "summary": summary[:1200],
        "old_value": old_value[:800],
        "new_value": new_value[:800],
        "construction_impact": construction_impact[:600],
        "cost_impact": cost_impact,
        "requires_human_review": bool(requires_human_review),
        "confidence": round(max(0.0, min(1.0, confidence)), 3),
        "evidence_left": evidence_left or {"quote": "", "section": "", "approx_location": ""},
        "evidence_right": evidence_right or {"quote": "", "section": "", "approx_location": ""},
        "provenance": provenance,
    }


_SECTION_HUMAN = {
    "pz": "Пояснительная записка / текстовая часть",
    "architectural": "Архитектурные листы (АР)",
    "sections_details": "Разрезы / детали армирования / узлы",
    "structural": "Конструктивные схемы (КР)",
    "other": "Прочие листы",
}


def _scope_human(scope_key: str) -> str:
    sect, _, part = scope_key.partition("|")
    base = _SECTION_HUMAN.get(sect, sect)
    if part and part != "общий":
        return f"{base} (Корпус {part})"
    return base


def deterministic_fact_diff(scope_map: ScopeMap) -> list[dict]:
    """Stage 3: детерминированные изменения без LLM.

    Покрывает то, что мис-фреймит LLM при section-split:
      * смена штампа (шифр/стадия/объект);
      * разделы, существующие только с одной стороны → 1 grouped change;
      * изменение состава листов внутри общего scope (added/removed sheets).
    """
    changes: list[dict] = []
    li, ri = scope_map.left_index, scope_map.right_index

    # 3.1 — штамп
    if li.stamp and ri.stamp and _norm_text(li.stamp) != _norm_text(ri.stamp):
        changes.append(_mk_change(
            type_="stamp_changed", source="stamp", severity="high",
            title="Изменён штамп комплекта (шифр / стадия / объект / организация)",
            summary="Штамп левой и правой стадий различается.",
            old_value=li.stamp, new_value=ri.stamp,
            category="general", confidence=0.95, requires_human_review=False,
            evidence_left={"quote": li.stamp[:240], "section": "Штамп", "approx_location": "штамп"},
            evidence_right={"quote": ri.stamp[:240], "section": "Штамп", "approx_location": "штамп"},
        ))

    # 3.2 — scope-only разделы (1 grouped change на раздел, не на лист)
    for key in scope_map.left_only:
        ps = li.pages_for(key)
        names = sorted({p.sheet_name for p in ps if p.sheet_name})
        sample = ps[0]
        changes.append(_mk_change(
            type_="section_changed", source="text", severity="medium",
            title=f"Раздел изъят из новой стадии: {_scope_human(key)}",
            summary=(f"В старой стадии присутствует раздел «{_scope_human(key)}» "
                     f"({len(ps)} листов), в новой стадии он отсутствует. "
                     "Изменение состава выпуска, а не отдельных листов."),
            old_value="; ".join(names[:10]) or f"{len(ps)} листов",
            new_value="(раздел отсутствует)",
            category="general", confidence=0.8,
            evidence_left={
                "quote": (sample.sheet_name or sample.body[:200])[:240],
                "section": _scope_human(key),
                "approx_location": f"стр. {ps[0].page}-{ps[-1].page}",
            },
        ))
    for key in scope_map.right_only:
        ps = ri.pages_for(key)
        names = sorted({p.sheet_name for p in ps if p.sheet_name})
        sample = ps[0]
        changes.append(_mk_change(
            type_="section_changed", source="text", severity="medium",
            title=f"Раздел добавлен в новой стадии: {_scope_human(key)}",
            summary=(f"В новой стадии появился раздел «{_scope_human(key)}» "
                     f"({len(ps)} листов), которого не было в старой стадии."),
            old_value="(раздел отсутствует)",
            new_value="; ".join(names[:10]) or f"{len(ps)} листов",
            category="general", confidence=0.8,
            evidence_right={
                "quote": (sample.sheet_name or sample.body[:200])[:240],
                "section": _scope_human(key),
                "approx_location": f"стр. {ps[0].page}-{ps[-1].page}",
            },
        ))

    # 3.3 — изменение состава листов внутри общего scope
    for key in scope_map.common:
        lnames = {_norm_text(p.sheet_name) for p in li.pages_for(key) if p.sheet_name}
        rnames = {_norm_text(p.sheet_name) for p in ri.pages_for(key) if p.sheet_name}
        removed = lnames - rnames
        added = rnames - lnames
        # Только если разница существенная (>=1 лист) и не «весь scope».
        if removed and len(removed) < len(lnames or {1}):
            sample = next((p for p in li.pages_for(key) if _norm_text(p.sheet_name) in removed), None)
            changes.append(_mk_change(
                type_="section_changed", source="text", severity="medium",
                title=f"Изъяты листы из раздела {_scope_human(key)}",
                summary=f"В разделе «{_scope_human(key)}» в новой стадии отсутствуют {len(removed)} листов, бывших в старой.",
                old_value="; ".join(sorted({p.sheet_name for p in li.pages_for(key) if _norm_text(p.sheet_name) in removed}))[:800],
                new_value="(листы отсутствуют)",
                confidence=0.6,
                evidence_left={
                    "quote": (sample.sheet_name if sample else "")[:240],
                    "section": _scope_human(key),
                    "approx_location": f"стр. {sample.page}" if sample else "",
                },
            ))
        if added and len(added) < len(rnames or {1}):
            sample = next((p for p in ri.pages_for(key) if _norm_text(p.sheet_name) in added), None)
            changes.append(_mk_change(
                type_="section_changed", source="text", severity="medium",
                title=f"Добавлены листы в раздел {_scope_human(key)}",
                summary=f"В разделе «{_scope_human(key)}» в новой стадии появились {len(added)} листов, которых не было в старой.",
                old_value="(листы отсутствуют)",
                new_value="; ".join(sorted({p.sheet_name for p in ri.pages_for(key) if _norm_text(p.sheet_name) in added}))[:800],
                confidence=0.6,
                evidence_right={
                    "quote": (sample.sheet_name if sample else "")[:240],
                    "section": _scope_human(key),
                    "approx_location": f"стр. {sample.page}" if sample else "",
                },
            ))
    return changes


# ─── Stage 4: scope-aware section split ─────────────────────────────────────


@dataclass
class Chunk:
    chunk_id: str
    scope_key: str
    title: str
    left_pages: list[int]
    right_pages: list[int]
    left_md: str
    right_md: str

    @property
    def total_chars(self) -> int:
        return len(self.left_md) + len(self.right_md)


def _pages_md(pages: list[PageRec]) -> str:
    return "".join(f"## СТРАНИЦА {p.page}\n{p.body}" for p in pages)


def _split_pages_by_budget(pages: list[PageRec], n_parts: int) -> list[list[PageRec]]:
    """Поделить список страниц на n_parts примерно равных по объёму частей."""
    if n_parts <= 1 or len(pages) <= 1:
        return [pages]
    total = sum(len(p.body) for p in pages) or 1
    target = total / n_parts
    parts: list[list[PageRec]] = []
    cur: list[PageRec] = []
    acc = 0
    for p in pages:
        cur.append(p)
        acc += len(p.body)
        if acc >= target and len(parts) < n_parts - 1:
            parts.append(cur)
            cur = []
            acc = 0
    if cur:
        parts.append(cur)
    return parts


def scope_aware_section_split(scope_map: ScopeMap, cfg: FallbackConfig) -> list[Chunk]:
    """Stage 4: выровненные чанки по общим scope_keys, каждый ≤ chunk budget."""
    chunks: list[Chunk] = []
    for key in scope_map.common:
        lpages = scope_map.left_index.pages_for(key)
        rpages = scope_map.right_index.pages_for(key)
        total = sum(len(p.body) for p in lpages) + sum(len(p.body) for p in rpages)
        if total <= cfg.chunk_max_chars:
            n_parts = 1
        else:
            n_parts = max(1, -(-total // cfg.chunk_max_chars))  # ceil
        lparts = _split_pages_by_budget(lpages, n_parts)
        rparts = _split_pages_by_budget(rpages, n_parts)
        # Выравниваем число частей (берём max, недостающие — пустые).
        m = max(len(lparts), len(rparts))
        while len(lparts) < m:
            lparts.append([])
        while len(rparts) < m:
            rparts.append([])
        for i in range(m):
            lp, rp = lparts[i], rparts[i]
            if not lp and not rp:
                continue
            suffix = f" [{i + 1}/{m}]" if m > 1 else ""
            chunks.append(Chunk(
                chunk_id=f"{key}#{i}",
                scope_key=key,
                title=_scope_human(key) + suffix,
                left_pages=[p.page for p in lp],
                right_pages=[p.page for p in rp],
                left_md=_pages_md(lp),
                right_md=_pages_md(rp),
            ))
    # Жёсткий cap на число LLM-вызовов.
    if len(chunks) > cfg.max_chunks:
        logger.warning("evidence_first_fallback: %d chunks > cap %d, truncating",
                       len(chunks), cfg.max_chunks)
        chunks = chunks[:cfg.max_chunks]
    return chunks


# ─── Stage 5: shared global header ─────────────────────────────────────────


def _collect_material_lines(idx: FactIndex, limit: int) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for p in idx.pages:
        for line in p.body.splitlines():
            low = line.lower()
            if any(mk in low for mk in _MATERIAL_MARKERS):
                clean = re.sub(r"\s+", " ", line).strip(" -*•\t")
                if 6 <= len(clean) <= 240 and clean.lower() not in seen:
                    seen.add(clean.lower())
                    out.append(clean)
            if len(out) >= limit:
                return out
    return out


def build_shared_header(scope_map: ScopeMap, cfg: FallbackConfig) -> str:
    """Stage 5: компактный global header — штамп + материальная сводка.

    Этот заголовок прокидывается в КАЖДЫЙ чанк, чтобы cross-section факты
    (класс бетона/арматуры, лежащие в ПЗ или легенде) были доступны при
    сравнении любой части — иначе section-split их теряет.
    """
    li, ri = scope_map.left_index, scope_map.right_index
    parts: list[str] = ["<SHARED_GLOBAL_HEADER>"]
    if li.stamp or ri.stamp:
        parts.append("Штамп OLD: " + (li.stamp or "—")[:300])
        parts.append("Штамп NEW: " + (ri.stamp or "—")[:300])
    lmat = _collect_material_lines(li, 60)
    rmat = _collect_material_lines(ri, 60)
    if lmat:
        parts.append("\nМатериалы/спецификация OLD (сводка):")
        parts.extend("  - " + m for m in lmat)
    if rmat:
        parts.append("\nМатериалы/спецификация NEW (сводка):")
        parts.extend("  - " + m for m in rmat)
    parts.append("</SHARED_GLOBAL_HEADER>")
    blob = "\n".join(parts)
    if len(blob) > cfg.header_max_chars:
        blob = blob[:cfg.header_max_chars] + "\n…(header truncated)…\n</SHARED_GLOBAL_HEADER>"
    return blob


# ─── Stage 6: per-chunk LLM compare ────────────────────────────────────────


def build_chunk_user_prompt(chunk: Chunk, shared_header: str) -> str:
    """User-prompt для одного чанка section-split."""
    return (
        "Сравни ОДИН ВЫРОВНЕННЫЙ РАЗДЕЛ двух стадий документа и верни JSON по "
        "схеме из системного промпта. Это ЧАСТЬ большого документа — сравнивай "
        "только в пределах этого раздела, но используй SHARED_GLOBAL_HEADER как "
        "общий контекст (штамп, классы бетона/арматуры из ПЗ/легенды).\n\n"
        f"Раздел: {chunk.title}\n"
        f"OLD страницы: {chunk.left_pages}\nNEW страницы: {chunk.right_pages}\n\n"
        + shared_header
        + "\n\n<OLD_ENRICHED_MD>\n" + (chunk.left_md or "(нет страниц)")
        + "\n</OLD_ENRICHED_MD>\n\n<NEW_ENRICHED_MD>\n" + (chunk.right_md or "(нет страниц)")
        + "\n</NEW_ENRICHED_MD>\n"
    )


def compare_chunk(
    chunk: Chunk,
    shared_header: str,
    *,
    provider: Any,
    system_prompt: str,
    model: str,
    timeout_sec: int,
    work_dir: Optional[Any] = None,
    parse_fn: Any,
    normalize_fn: Any,
) -> dict:
    """Stage 6: вызвать provider на одном чанке. Возвращает diagnostics-словарь.

    parse_fn / normalize_fn инжектятся из enriched_comparison, чтобы не
    дублировать парсинг JSON-ответа и нормализацию changes.
    """
    user_prompt = build_chunk_user_prompt(chunk, shared_header)
    t0 = time.monotonic()
    out: dict[str, Any] = {
        "chunk_id": chunk.chunk_id, "title": chunk.title,
        "left_pages": chunk.left_pages, "right_pages": chunk.right_pages,
        "total_chars": chunk.total_chars, "status": "error",
        "changes": [], "error": None, "duration_sec": 0.0,
    }
    try:
        result = provider.invoke(
            system_prompt=system_prompt, user_prompt=user_prompt,
            model=model, timeout_sec=timeout_sec, work_dir=work_dir,
        )
    except Exception as exc:  # noqa: BLE001
        out["error"] = f"provider_exception:{type(exc).__name__}:{exc}"
        out["duration_sec"] = round(time.monotonic() - t0, 3)
        return out
    out["duration_sec"] = round(result.duration_sec or (time.monotonic() - t0), 3)
    if result.status != "done":
        out["status"] = result.status
        out["error"] = result.error
        return out
    model_text, extract_err = parse_fn["extract"](result.raw_response)
    parsed, parse_err = parse_fn["parse"](model_text)
    if parsed is None:
        out["status"] = "invalid_json"
        out["error"] = parse_err or extract_err or "invalid_json"
        return out
    raw_changes = parsed.get("changes") if isinstance(parsed.get("changes"), list) else []
    norm: list[dict] = []
    for rc in raw_changes:
        n = normalize_fn(rc)
        if n:
            n["provenance"] = "llm_chunk"
            n["chunk_id"] = chunk.chunk_id
            norm.append(n)
    out["status"] = "done"
    out["changes"] = norm
    out["summary"] = str(parsed.get("summary") or "").strip()[:600]
    return out


# ─── Stage 7: original MD evidence verification ────────────────────────────


def _norm_text(s: str) -> str:
    s = unicodedata.normalize("NFKC", s or "")
    s = s.replace("ё", "е").replace("Ё", "Е")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s


def _quote_grounded(quote: str, haystack_norm: str, cfg: FallbackConfig) -> tuple[bool, float]:
    """Проверить, что quote реально есть в raw MD стороны.

    Возвращает (grounded, score). Сначала exact-substring, затем token-overlap.
    """
    q = _norm_text(quote)
    if len(q) < cfg.min_quote_len:
        return False, 0.0
    if q in haystack_norm:
        return True, 1.0
    toks = [t for t in re.split(r"[\s,;:]+", q) if len(t) >= 4]
    if not toks:
        return False, 0.0
    hit = sum(1 for t in toks if t in haystack_norm)
    score = hit / len(toks)
    return (score >= cfg.fuzzy_threshold), round(score, 2)


def verify_change_evidence(
    change: dict, left_norm: str, right_norm: str, cfg: FallbackConfig,
) -> dict:
    """Stage 7: пометить change флагами grounding по raw MD.

    Добавляет `evidence_verified` (bool) и `evidence_scores`. Детерминированные
    changes (provenance=deterministic) считаются grounded по построению.
    """
    if change.get("provenance") == "deterministic":
        change["evidence_verified"] = True
        change["evidence_scores"] = {"left": 1.0, "right": 1.0}
        return change
    el = (change.get("evidence_left") or {}).get("quote") or ""
    er = (change.get("evidence_right") or {}).get("quote") or ""
    # evidence[] массив тоже учитываем.
    arr = change.get("evidence") or []
    gl, sl = _quote_grounded(el, left_norm, cfg) if el else (False, 0.0)
    gr, sr = _quote_grounded(er, right_norm, cfg) if er else (False, 0.0)
    arr_grounded = False
    arr_score = 0.0
    for e in arr:
        side = e.get("side")
        hay = left_norm if side == "left" else right_norm if side == "right" else (left_norm + " " + right_norm)
        g, sc = _quote_grounded(e.get("quote") or "", hay, cfg)
        arr_grounded = arr_grounded or g
        arr_score = max(arr_score, sc)
    # Изменение grounded, если хотя бы одна сторона/якорь подтверждены.
    # Для added/removed допустима только одна сторона.
    grounded = gl or gr or arr_grounded
    change["evidence_verified"] = bool(grounded)
    change["evidence_scores"] = {"left": sl, "right": sr, "evidence_array": arr_score}
    return change


# ─── Stage 8: merge + dedup ────────────────────────────────────────────────


def _change_signature(c: dict) -> str:
    """Сигнатура для дедупа: тип + ключевые токены title + локация."""
    title_toks = [t for t in re.split(r"[\s/]+", _norm_text(c.get("title") or "")) if len(t) >= 4]
    title_key = " ".join(sorted(set(title_toks))[:6])
    loc = _norm_text(
        (c.get("evidence_left") or {}).get("approx_location", "")
        + (c.get("evidence_right") or {}).get("approx_location", "")
    )
    sheet = re.findall(r"\d+", loc)
    return f"{c.get('type')}|{title_key}|{','.join(sheet[:3])}"


def merge_and_dedup(deterministic: list[dict], llm_changes: list[dict]) -> tuple[list[dict], int]:
    """Stage 8: объединить и дедупнуть. Детерминированные имеют приоритет."""
    out: list[dict] = []
    seen: dict[str, int] = {}
    duplicates = 0
    for c in deterministic + llm_changes:
        sig = _change_signature(c)
        if sig in seen:
            duplicates += 1
            # Если новый — детерминированный, а старый — нет, заменим.
            idx = seen[sig]
            if c.get("provenance") == "deterministic" and out[idx].get("provenance") != "deterministic":
                out[idx] = c
            continue
        seen[sig] = len(out)
        out.append(c)
    return out, duplicates


# ─── Orchestrator ──────────────────────────────────────────────────────────


def run_evidence_first_fallback(
    *,
    left_md: str,
    right_md: str,
    provider: Any,
    system_prompt: str,
    model: str,
    timeout_sec: int,
    parse_extract_fn: Any,
    parse_json_fn: Any,
    normalize_change_fn: Any,
    config: Optional[FallbackConfig] = None,
    work_dir: Optional[Any] = None,
    base_input_stats: Optional[dict] = None,
) -> dict:
    """Полный pipeline evidence_first_s2_fallback. Возвращает comparison_result payload.

    Никогда не бросает наружу: ошибки оборачиваются в warnings/status.
    """
    cfg = config or load_fallback_config()
    t0 = time.monotonic()
    diagnostics: dict[str, Any] = {"strategy": STRATEGY, "version": STRATEGY_VERSION}
    warnings_list: list[str] = []

    try:
        left_idx = build_fact_index("left", left_md)
        right_idx = build_fact_index("right", right_md)
        scope_map = build_scope_map(left_idx, right_idx)
        diagnostics["scope_map"] = scope_map.as_dict()

        det_changes = deterministic_fact_diff(scope_map)
        diagnostics["deterministic_changes"] = len(det_changes)

        chunks = scope_aware_section_split(scope_map, cfg)
        shared_header = build_shared_header(scope_map, cfg)
        diagnostics["shared_header_chars"] = len(shared_header)
        diagnostics["chunks"] = [
            {"chunk_id": c.chunk_id, "title": c.title, "total_chars": c.total_chars,
             "left_pages": c.left_pages, "right_pages": c.right_pages}
            for c in chunks
        ]

        parse_fn = {"extract": parse_extract_fn, "parse": parse_json_fn}
        chunk_results: list[dict] = []
        llm_changes: list[dict] = []
        oversize = 0
        for ch in chunks:
            if ch.total_chars > cfg.chunk_max_chars * 1.5:
                oversize += 1
            cr = compare_chunk(
                ch, shared_header, provider=provider, system_prompt=system_prompt,
                model=model, timeout_sec=timeout_sec, work_dir=work_dir,
                parse_fn=parse_fn, normalize_fn=normalize_change_fn,
            )
            chunk_results.append({k: v for k, v in cr.items() if k != "changes"} | {"changes_count": len(cr.get("changes") or [])})
            if cr.get("status") == "done":
                llm_changes.extend(cr.get("changes") or [])
            else:
                warnings_list.append(f"Чанк {ch.chunk_id}: status={cr.get('status')} ({cr.get('error') or '—'})")
        diagnostics["chunk_results"] = chunk_results
        if oversize:
            warnings_list.append(f"{oversize} чанков превышают chunk budget в 1.5×.")

        # Stage 7 — evidence verification (детерминированные проходят по построению)
        left_norm = _norm_text(left_md)
        right_norm = _norm_text(right_md)
        verified_llm: list[dict] = []
        dropped_ungrounded = 0
        for c in llm_changes:
            verify_change_evidence(c, left_norm, right_norm, cfg)
            if not c.get("evidence_verified"):
                if cfg.drop_ungrounded:
                    dropped_ungrounded += 1
                    continue
                c["requires_human_review"] = True
            verified_llm.append(c)
        for c in det_changes:
            verify_change_evidence(c, left_norm, right_norm, cfg)
        diagnostics["llm_changes_raw"] = len(llm_changes)
        diagnostics["llm_changes_dropped_ungrounded"] = dropped_ungrounded

        # Stage 8 — merge + dedup
        merged, dup = merge_and_dedup(det_changes, verified_llm)
        diagnostics["duplicates_removed"] = dup
        diagnostics["final_changes"] = len(merged)

        # снимаем внутренние поля, мешающие schema unified_findings? — нет,
        # они дополнительные и безопасные (provenance / evidence_verified).
        duration = round(time.monotonic() - t0, 3)
        summary = (
            f"[{STRATEGY}] Сравнение больших enriched MD ({left_idx.total_chars}+"
            f"{right_idx.total_chars} симв.) выполнено через scope-aware section split: "
            f"{len(chunks)} чанков, {len(det_changes)} детерминированных + "
            f"{len(verified_llm)} проверенных LLM-изменений, дедуп −{dup}, "
            f"итого {len(merged)}."
        )
        input_stats = dict(base_input_stats or {})
        input_stats["fallback_strategy"] = STRATEGY
        input_stats["chunks"] = len(chunks)
        return {
            "status": "done",
            "strategy": STRATEGY,
            "fallback": True,
            "summary": summary,
            "changes": merged,
            "warnings": warnings_list,
            "diagnostics": diagnostics,
            "input_stats": input_stats,
            "duration_sec": duration,
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001
        logger.exception("evidence_first_fallback: pipeline failed")
        return {
            "status": "error",
            "strategy": STRATEGY,
            "fallback": True,
            "summary": "",
            "changes": [],
            "warnings": warnings_list + [f"fallback_pipeline_exception:{type(exc).__name__}:{exc}"],
            "diagnostics": diagnostics,
            "input_stats": dict(base_input_stats or {}),
            "duration_sec": round(time.monotonic() - t0, 3),
            "error": f"fallback_pipeline_exception:{type(exc).__name__}",
        }


__all__ = [
    "STRATEGY",
    "STRATEGY_VERSION",
    "FallbackConfig",
    "load_fallback_config",
    "build_fact_index",
    "build_scope_map",
    "deterministic_fact_diff",
    "scope_aware_section_split",
    "build_shared_header",
    "build_chunk_user_prompt",
    "compare_chunk",
    "verify_change_evidence",
    "merge_and_dedup",
    "run_evidence_first_fallback",
]
