"""EV precedent-путь — память экспертных решений как офлайн-сигнал (без нейросети).

Идея: «лучшее из KB-агента» — поиск похожих ОТКЛОНЁННЫХ экспертом решений в
knowledge_base/decisions_log.json — встроено в EV четвёртым источником слияния
(рядом с visual / norm / cross_block). Закрывает зону, где зрение слепо: дубли,
неприменимая норма, формальные расхождения — их нельзя «увидеть» на чертеже, но по
ним у эксперта уже есть прецеденты.

ИНВАРИАНТ БЕЗОПАСНОСТИ (как у norm_check): PrecedentSignal.hint НЕ имеет значения,
ведущего к reject. Максимум влияния — suspect_flag → в fusion поднимает «unsure»
замечание до borderline + requires_human_review. Удалить реальное замечание
прецедент не может ни при какой комбинации (проверяется property-тестом).

Ретривер здесь СВОЙ (копия алгоритма прежнего KB-агента), чтобы EV не зависел от
пакета critic_v2/ (он подлежит удалению). Алгоритм похожести не меняется:
  section +0.40 | category +0.30 | severity +0.10 | текст (Jaccard) до +0.20.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from backend.app.core.config import ROOT_DIR as _ROOT

_DECISIONS_LOG = _ROOT / "knowledge_base" / "decisions_log.json"

# hint ⊂ {none, neutral, suspect_flag} — reject недостижим по типу
HINT_NONE = "none"
HINT_NEUTRAL = "neutral"
HINT_SUSPECT = "suspect_flag"
_SAFE_HINTS = {HINT_NONE, HINT_NEUTRAL, HINT_SUSPECT}


def _as_text(value) -> str:
    return "" if value is None else str(value)


def _tokenize(text: str) -> set:
    return set(re.findall(r"[\w]{3,}", _as_text(text).lower(), re.UNICODE))


def _jaccard(q: set, d: set) -> float:
    if not q or not d:
        return 0.0
    inter = len(q & d)
    union = len(q | d)
    return inter / union if union else 0.0


@dataclass
class SimilarDecision:
    decision_id: str
    source_project: str
    section: str
    severity: str
    category: str
    summary: str
    expert_reason: str
    similarity_score: float
    text_sim: float = 0.0   # чистое текстовое сходство (Jaccard), БЕЗ метаданных


@dataclass
class PrecedentSignal:
    kind: str = "none"                 # precedent_reject | none
    hint: str = HINT_NONE              # none | neutral | suspect_flag
    enforce: bool = False              # влияет ли на decision (иначе — только запись)
    confidence: float = 0.0
    taxonomy: str = ""
    reason: str = ""
    flags: list = field(default_factory=list)      # попадут в финальный finding
    examples: list = field(default_factory=list)   # [{decision_id, score, expert_reason, source_project}]
    top_score: float = 0.0
    n_matches: int = 0

    def __post_init__(self):
        # жёсткая гарантия инварианта на уровне типа
        if self.hint not in _SAFE_HINTS:
            self.hint = HINT_NEUTRAL


class PrecedentRetriever:
    """Поиск похожих ОТКЛОНЁННЫХ экспертных решений в decisions_log.json."""

    def __init__(self, decisions: list) -> None:
        self._entries = decisions
        self._entry_tokens = [
            _tokenize(_as_text(e.get("summary")) + " " + _as_text(e.get("expert_reason")))
            for e in decisions
        ]

    @classmethod
    def from_path(cls, path: Path) -> "PrecedentRetriever":
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        entries = data.get("entries", data) if isinstance(data, dict) else data
        return cls(entries)

    def find_similar(self, finding: dict, *, top_k: int = 5,
                     min_score: float = 0.30) -> list:
        q_section = _as_text(finding.get("section"))
        q_category = _as_text(finding.get("category"))
        q_severity = _as_text(finding.get("severity"))
        # схема findings v2: текст замечания = title + description (поля problem/
        # summary — из старых форматов, оставлены для совместимости)
        q_tokens = _tokenize(
            _as_text(finding.get("title")) + " "
            + _as_text(finding.get("problem")) + " "
            + _as_text(finding.get("description")) + " "
            + _as_text(finding.get("summary"))
        )

        scored = []
        for i, e in enumerate(self._entries):
            if e.get("expert_decision") != "rejected":
                continue
            score = 0.0
            if q_section and _as_text(e.get("section")) == q_section:
                score += 0.40
            if q_category and _as_text(e.get("category")) == q_category:
                score += 0.30
            if q_severity and _as_text(e.get("severity")) == q_severity:
                score += 0.10
            text_sim = _jaccard(q_tokens, self._entry_tokens[i])
            score += text_sim * 0.20
            if score >= min_score:
                scored.append((score, text_sim, i))

        scored.sort(key=lambda x: -x[0])
        out = []
        for score, text_sim, i in scored[:top_k]:
            e = self._entries[i]
            out.append(SimilarDecision(
                decision_id=_as_text(e.get("id")) or "?",
                source_project=_as_text(e.get("source_project")),
                section=_as_text(e.get("section")),
                severity=_as_text(e.get("severity")),
                category=_as_text(e.get("category")),
                summary=_as_text(e.get("summary")),
                expert_reason=_as_text(e.get("expert_reason")),
                similarity_score=round(score, 3),
                text_sim=round(text_sim, 3),
            ))
        return out


_RETRIEVER: Optional[PrecedentRetriever] = None
_RETRIEVER_TRIED = False


def get_default_retriever() -> Optional[PrecedentRetriever]:
    """Ленивый синглтон: decisions_log.json загружается и токенизируется ОДИН раз
    на процесс (≈6300 записей). Fail-soft: при любой ошибке — None (прецедент
    просто не участвует, EV работает как раньше)."""
    global _RETRIEVER, _RETRIEVER_TRIED
    if _RETRIEVER is not None:
        return _RETRIEVER
    if _RETRIEVER_TRIED:
        return None
    _RETRIEVER_TRIED = True
    try:
        _RETRIEVER = PrecedentRetriever.from_path(_DECISIONS_LOG)
    except Exception:
        _RETRIEVER = None
    return _RETRIEVER


def run_precedent_check(
    finding: dict,
    *,
    section: str = "",
    enforce: bool = False,
    retriever: Optional[PrecedentRetriever] = None,
    min_score: float = 0.45,
    text_min: float = 0.12,
    top_k: int = 5,
) -> PrecedentSignal:
    """Офлайн-проверка: похоже ли замечание на ранее ОТКЛОНЁННЫЕ экспертом.

    Никогда не возвращает reject-хинт. `enforce` управляет только тем, влияет ли
    сигнал на decision в fusion (shadow vs live) — сам поиск идёт всегда.

    «Сильным» (suspect_flag) прецедент считается ТОЛЬКО при реальном СОДЕРЖАТЕЛЬНОМ
    совпадении (text_sim >= text_min). Совпадение по одним метаданным (раздел+
    критичность даёт 0.50 у любого замечания секции) НЕ считается — иначе сигнал
    срабатывал бы на всём подряд (проверено shadow-прогоном).
    """
    retriever = retriever if retriever is not None else get_default_retriever()
    if retriever is None:
        return PrecedentSignal(kind="none", hint=HINT_NONE, enforce=enforce)

    q = dict(finding)
    if not q.get("section"):
        q["section"] = section
    try:
        examples = retriever.find_similar(q, top_k=top_k, min_score=min(0.30, min_score))
    except Exception:
        return PrecedentSignal(kind="none", hint=HINT_NONE, enforce=enforce)

    if not examples:
        return PrecedentSignal(kind="none", hint=HINT_NONE, enforce=enforce,
                               reason="Похожих отклонённых решений не найдено.")

    # «сильные» = содержательно похожие (text_sim >= text_min) И общий score >= порога
    strong_examples = [ex for ex in examples
                       if ex.text_sim >= text_min and ex.similarity_score >= min_score]
    strong = bool(strong_examples)
    hint = HINT_SUSPECT if strong else HINT_NEUTRAL
    lead = strong_examples[0] if strong else examples[0]

    ex_out = [{
        "decision_id": ex.decision_id,
        "score": ex.similarity_score,
        "text_sim": ex.text_sim,
        "expert_reason": ex.expert_reason[:200],
        "source_project": ex.source_project,
    } for ex in (strong_examples or examples)[:3]]
    flags = [f"precedent_reject:{ex.decision_id}" for ex in strong_examples[:3]]
    reason = (
        f"{len(strong_examples)} содержательно похожих отклонённых экспертом "
        f"(топ {lead.decision_id}, score {lead.similarity_score}, "
        f"text_sim {lead.text_sim}): {lead.expert_reason[:150]}"
    ) if strong else (
        f"Есть {len(examples)} совпадений только по метаданным (без сходства текста)."
    )
    return PrecedentSignal(
        kind="precedent_reject" if strong else "none",
        hint=hint,
        enforce=enforce,
        confidence=lead.similarity_score if strong else 0.0,
        taxonomy="matches_rejected_precedent",
        reason=reason,
        flags=flags,
        examples=ex_out,
        top_score=lead.similarity_score,
        n_matches=len(strong_examples),
    )
