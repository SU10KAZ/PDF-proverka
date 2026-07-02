"""EV2 слияние источников — детерминированная политика (norm + visual + cross_block).

Правила F1–F9 применяются по приоритету сверху вниз; первое сработавшее — финал.
Аудируемо и тестируется офлайн.

ГЛАВНЫЙ ИНВАРИАНТ: `reject` достижим ТОЛЬКО из
  F1 — сильный визуал (decision==reject; в EV2 это уже ≥2 «yes», а guard в extract.py
       понижает «yes» без цитаты до cannot_tell, т.е. reject визуала уже «с якорем»);
  F2 — cross_block xref_refutes И визуал НЕ против (reject/borderline).
norm_signal НЕ МОЖЕТ породить reject ни при какой комбинации (проверяется property-тестом).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class FusedVerdict:
    finding_id: str
    decision: str                # accept | reject | borderline | needs_human
    confidence: float
    source: str                  # visual_strong|cross_block_strong|norm_flag|conflict|...
    taxonomy: str
    reason: str
    sources_used: list = field(default_factory=list)
    norm_flags: list = field(default_factory=list)
    norm_suggestions: dict = field(default_factory=dict)
    visual_votes: dict = field(default_factory=dict)
    candidate_block_ids: list = field(default_factory=list)
    requires_human_review: bool = False
    evidence_quote: str = ""


def _is_strong_visual_reject(visual) -> bool:
    """visual.decision==reject уже сильный по конструкции _aggregate+guard."""
    return visual is not None and getattr(visual, "decision", None) == "reject"


def _vis_decision(visual) -> Optional[str]:
    return getattr(visual, "decision", None) if visual is not None else None


def fuse(visual, norm_signal, cross_block, *, finding_id: str) -> FusedVerdict:
    """Чистая детерминированная функция. norm_signal не может дать reject."""
    vis = _vis_decision(visual)
    vis_conf = getattr(visual, "confidence", 0.0) if visual is not None else 0.0
    vis_votes = getattr(visual, "votes", {}) if visual is not None else {}
    vis_quote = ""
    for p in getattr(visual, "perceptions", []) or []:
        if getattr(p, "evidence_quote", ""):
            vis_quote = p.evidence_quote
            break

    norm_hint = getattr(norm_signal, "decision_hint", "none") if norm_signal else "none"
    norm_flags = list(getattr(norm_signal, "flags", []) or []) if norm_signal else []
    norm_sugg = dict(getattr(norm_signal, "suggestions", {}) or {}) if norm_signal else {}
    norm_req_human = bool(norm_sugg.get("requires_human_review"))

    xb_kind = getattr(cross_block, "kind", "none") if cross_block else "none"
    xb_cands = list(getattr(cross_block, "candidate_block_ids", []) or []) if cross_block else []

    sources = []
    if visual is not None:
        sources.append("visual")
    if norm_signal and norm_signal.kind != "none":
        sources.append("norm")
    if cross_block and xb_kind != "none":
        sources.append("cross_block")

    def _mk(decision, source, taxonomy, conf, reason, req_human=False):
        return FusedVerdict(
            finding_id=finding_id, decision=decision, confidence=round(conf, 2),
            source=source, taxonomy=taxonomy, reason=reason,
            sources_used=sources, norm_flags=norm_flags, norm_suggestions=norm_sugg,
            visual_votes=vis_votes, candidate_block_ids=xb_cands,
            requires_human_review=req_human or norm_req_human,
            evidence_quote=vis_quote,
        )

    # F1 — сильный визуальный reject
    if _is_strong_visual_reject(visual):
        return _mk("reject", "visual_strong", "visual_misread", vis_conf,
                   "Чертёж опровергает замечание (визуал с цитатой).")

    # F2 — кросс-блок прямое опровержение + визуал не против
    if xb_kind == "xref_refutes" and vis in ("reject", "borderline"):
        return _mk("reject", "cross_block_strong", "info_elsewhere_refutes",
                   min(0.7, vis_conf or 0.7),
                   "Искомое найдено в другом блоке (визуал не противоречит).")

    # F6 — конфликт: визуал подтверждает, кросс-блок опровергает → эксперт (выше F3,
    # чтобы конфликт не «проглатывался» как тихий accept)
    if vis == "accept" and xb_kind == "xref_refutes":
        return _mk("needs_human", "conflict", "source_conflict", 0.4,
                   "Источники конфликтуют (кросс-блок опровергает, визуал подтверждает).")

    # F3 — подтверждение
    if vis == "accept" or xb_kind == "xref_supports":
        src = "visual_confirm" if vis == "accept" else "cross_block_supports"
        return _mk("accept", src, "confirmed", max(vis_conf, 0.6),
                   "Чертёж/смежный блок подтверждает проблему.")

    # F4 — норма заменена/отменена/устаревшая → accept-с-пометкой (НЕ reject)
    if norm_hint == "accept_with_flag":
        return _mk("accept", "norm_flag", "norm_superseded",
                   getattr(norm_signal, "confidence", 0.7),
                   getattr(norm_signal, "reason", "Норма заменена/устарела; замечание валидно."),
                   req_human=norm_req_human)

    # F5 — слабый визуальный reject/borderline без поддержки
    if vis == "borderline":
        return _mk("borderline", "weak_visual", "visual_unanchored", 0.5,
                   "Визуальный сигнал слабый, без согласованного подтверждения.")

    # F7 — визуал воздержался
    if vis == "needs_human":
        return _mk("needs_human", "visual_abstain", "unreadable", vis_conf or 0.3,
                   "По блоку нельзя проверить визуально.")

    # F8 — норма не в индексе и остальные неуверенны
    if norm_hint == "soft_human":
        return _mk("needs_human", "norm_not_indexed", "norm_coverage_gap",
                   getattr(norm_signal, "confidence", 0.3),
                   getattr(norm_signal, "reason", "Норма не в индексе — нужен эксперт."))

    # F9 — нет уверенного источника
    return _mk("needs_human", "no_signal", "insufficient_evidence", 0.0,
               "Недостаточно данных для автоматической проверки.")
