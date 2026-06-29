"""
kb_retriever.py
---------------
Knowledge-base retriever: finds similar expert decisions from decisions_log.json
for a given finding.

Usage:
    retriever = KBRetriever.from_default()
    examples = retriever.find_similar(finding, top_k=5)
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

_DEFAULT_KB_PATH = Path(__file__).parent.parent.parent.parent.parent.parent.parent / "knowledge_base" / "decisions_log.json"


def _as_text(value) -> str:
    if value is None:
        return ""
    return str(value)


def _tokenize(text: str) -> set[str]:
    text = _as_text(text)
    tokens = re.findall(r"[\w]{3,}", text.lower(), re.UNICODE)
    return set(tokens)


def _jaccard(q: set[str], d: set[str]) -> float:
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
    expert_decision: str
    expert_reason: str
    similarity_score: float
    match_reasons: list


class KBRetriever:
    """
    Retrieves similar expert decisions from decisions_log.json.

    Scoring:
        section match:   +0.40
        category match:  +0.30
        severity match:  +0.10
        text similarity: up to +0.20
    """

    def __init__(self, decisions: list) -> None:
        self._entries = decisions
        self._entry_tokens = [
            _tokenize(_as_text(e.get("summary", "")) + " " + _as_text(e.get("expert_reason", "")))
            for e in decisions
        ]

    @classmethod
    def from_path(cls, path: Path) -> "KBRetriever":
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        entries = data.get("entries", data) if isinstance(data, dict) else data
        return cls(entries)

    @classmethod
    def from_default(cls) -> "KBRetriever":
        return cls.from_path(_DEFAULT_KB_PATH)

    def find_similar(
        self,
        finding: dict,
        top_k: int = 5,
        only_rejected: bool = True,
        min_score: float = 0.15,
    ) -> list:
        q_section = _as_text(finding.get("section", ""))
        q_category = _as_text(finding.get("category", ""))
        q_severity = _as_text(finding.get("severity", ""))
        q_tokens = _tokenize(
            _as_text(finding.get("problem", ""))
            + " " + _as_text(finding.get("description", ""))
            + " " + _as_text(finding.get("summary", ""))
        )

        scored = []
        for i, entry in enumerate(self._entries):
            if only_rejected and entry.get("expert_decision") != "rejected":
                continue

            score = 0.0
            reasons = []

            if q_section and _as_text(entry.get("section")) == q_section:
                score += 0.40
                reasons.append("section=" + q_section)

            if q_category and _as_text(entry.get("category")) == q_category:
                score += 0.30
                reasons.append("category=" + q_category)

            if q_severity and _as_text(entry.get("severity")) == q_severity:
                score += 0.10
                reasons.append("severity=" + q_severity)

            sim = _jaccard(q_tokens, self._entry_tokens[i])
            score += sim * 0.20
            if sim > 0.05:
                reasons.append("text_sim=" + str(round(sim, 2)))

            if score >= min_score:
                scored.append((score, i, reasons))

        scored.sort(key=lambda x: -x[0])

        results = []
        for score, i, matched_reasons in scored[:top_k]:
            e = self._entries[i]
            results.append(SimilarDecision(
                decision_id=_as_text(e.get("id", "?")) or "?",
                source_project=_as_text(e.get("source_project", "")),
                section=_as_text(e.get("section", "")),
                severity=_as_text(e.get("severity", "")),
                category=_as_text(e.get("category", "")),
                summary=_as_text(e.get("summary", "")),
                expert_decision=_as_text(e.get("expert_decision", "")),
                expert_reason=_as_text(e.get("expert_reason", "")),
                similarity_score=round(score, 3),
                match_reasons=matched_reasons,
            ))

        return results

    def stats(self) -> dict:
        total = len(self._entries)
        rejected = sum(1 for e in self._entries if e.get("expert_decision") == "rejected")
        accepted = total - rejected
        sections: dict = {}
        for e in self._entries:
            s = e.get("section", "?")
            sections[s] = sections.get(s, 0) + 1
        return {"total": total, "rejected": rejected, "accepted": accepted,
                "sections": dict(sorted(sections.items(), key=lambda x: -x[1]))}
