"""EV2 кросс-блок сигнал — ищет, не снято ли замечание информацией в другом месте.

Закрывает категорию экспертных отклонений «инфо-в-другом-месте» (~5/24):
«марки есть в ведомости на том же листе», «см. п.2 Общих указаний», «см. раздел …».

Офлайн (без нейросети). Переиспользует готовые ретриверы critic_v2:
  get_neighbor_blocks(finding, graph)   — соседние блоки (по индексу/координатам/странице)
  get_cross_references(finding, graph)  — разрешённые ссылки «см. лист N / узел / спец.»

БЕЗОПАСНАЯ эвристика (под инвариант «не удалять реальное»):
  xref_refutes выдаётся ТОЛЬКО когда замечание — про ОТСУТСТВИЕ/НЕПОЛНОТУ
  («отсутствует / не указан / неполн / нет»), а искомые марки/значения РЕАЛЬНО
  найдены в тексте соседнего блока/ссылки. Это прямой случай «оно есть в другом месте».
  Во всех прочих случаях — xref_context_only + список блоков-кандидатов для зрения (Фаза 4).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

# замечание про отсутствие/неполноту — кандидат на «оно есть в другом месте»
_ABSENCE_RE = re.compile(
    r"отсутств|не\s+указан|не\s+заполнен|неполн|нет\s+в\b|нет\s+на\b|пропущен|"
    r"не\s+приведен|не\s+показан|не\s+обознач|не\s+хватает|missing",
    re.IGNORECASE,
)
# «салиентные» токены: марки/числа/обозначения, которые замечание считает отсутствующими
_MARK_RE = re.compile(r"[A-ZА-Я]{1,4}[-.]?\d{1,4}|\b\d{2,5}\b|⌀\s*\d+", re.IGNORECASE)


@dataclass
class CrossBlockSignal:
    kind: str = "none"            # xref_refutes|xref_supports|xref_context_only|none
    decision_hint: str = "neutral"  # reject_candidate|accept_candidate|neutral
    confidence: float = 0.0
    candidate_block_ids: list = field(default_factory=list)  # для Фазы 4 (multi-image)
    evidence: list = field(default_factory=list)             # [{block_id, page, snippet}]
    reason: str = ""


def _tokens(s: str) -> set:
    return {w for w in re.sub(r"[^0-9a-zа-яё]+", " ", (s or "").lower()).split() if len(w) > 3}


def _salient(text: str) -> list:
    return [m.group(0).strip() for m in _MARK_RE.finditer(text or "")]


def _block_text(b) -> str:
    parts = [getattr(b, "label", "") or "", getattr(b, "text", "") or ""]
    return " ".join(p for p in parts if p)


def run_cross_block(finding: dict, document_graph: dict) -> CrossBlockSignal:
    """Офлайн кросс-блок-сигнал. Никогда не reject'ит сам — максимум reject_candidate."""
    if not document_graph or not document_graph.get("pages"):
        return CrossBlockSignal(reason="Граф документа недоступен.")

    try:
        from backend.app.pipeline.stages.findings_review.critic_v2.context.neighbor_blocks import (
            get_neighbor_blocks,
        )
        from backend.app.pipeline.stages.findings_review.critic_v2.context.cross_references import (
            get_cross_references,
        )
    except Exception:
        return CrossBlockSignal(reason="Кросс-блок-ретриверы недоступны.")

    problem = f"{finding.get('problem') or ''} {finding.get('description') or ''}"
    is_absence = bool(_ABSENCE_RE.search(problem))
    needle = _tokens(problem)
    salient = _salient(finding.get("problem") or "")

    try:
        neighbors = get_neighbor_blocks(finding, document_graph) or []
    except Exception:
        neighbors = []
    try:
        xrefs = get_cross_references(finding, document_graph) or []
    except Exception:
        xrefs = []

    candidates, evidence = [], []
    refutes_hit = False

    for b in neighbors:
        txt = _block_text(b)
        if not txt:
            continue
        topic_overlap = len(needle & _tokens(txt))
        # релевантный сосед → кандидат для зрения
        if topic_overlap >= 2:
            bid = getattr(b, "block_id", None)
            if bid and bid not in candidates:
                candidates.append(bid)
                evidence.append({"block_id": bid, "page": getattr(b, "page", None),
                                 "snippet": txt[:160]})
        # безопасный refute: замечание про отсутствие, а салиентная марка есть у соседа
        if is_absence and salient:
            found_marks = [m for m in salient if m in txt]
            if len(found_marks) >= max(1, len(salient) // 2):
                refutes_hit = True

    # ссылки «см. лист/узел/спец» с непустым разрешённым текстом → тоже кандидаты
    for r in xrefs:
        resolved = getattr(r, "resolved_text", "") or ""
        if resolved:
            evidence.append({"ref": getattr(r, "ref_text", ""),
                             "snippet": resolved[:160]})
            if is_absence and salient and any(m in resolved for m in salient):
                refutes_hit = True

    if refutes_hit:
        return CrossBlockSignal(
            kind="xref_refutes", decision_hint="reject_candidate", confidence=0.7,
            candidate_block_ids=candidates[:3], evidence=evidence[:5],
            reason="Искомое (отсутствующее по замечанию) найдено в другом блоке/ссылке.",
        )
    if candidates:
        return CrossBlockSignal(
            kind="xref_context_only", decision_hint="neutral", confidence=0.3,
            candidate_block_ids=candidates[:3], evidence=evidence[:5],
            reason="Найдены релевантные соседние блоки (кандидаты для зрения).",
        )
    return CrossBlockSignal(reason="Релевантных соседних блоков не найдено.")
