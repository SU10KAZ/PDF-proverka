"""Conditional cross_discipline router.

Heuristic: scan the MD for cross-discipline trigger markers. If any are
present, the cross_discipline lens runs. If none are present AND the
discipline is on the "intra-only" list, skip the lens.

Pure Python, no LLM. Cheap (regex match on the MD).
"""
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass, field
from pathlib import Path

TRIGGER_TERMS = [
    # explicit cross-discipline language
    "смеж",
    "по заданию",
    "от смежник",
    "задание на",
    "согласован",
    "получ. задание",
    "передан в",
    # disciplines mentioned in the MD
    r"\bОВ\b",
    r"\bЭОМ\b",
    r"\bВК\b",
    r"\bСС\b",
    r"\bАПС\b",
    r"\bТХ\b",
    r"\bКЖ\b",
    r"\bАР\b",
    r"\bКМ\b",
    r"\bГАЗ\b",
    # coordination domain terms
    "пусковой ток",
    "тепловая нагрузка",
    "электропитани",
    "отверсти",
    "проходк",
    "закладн",
    "автоматик",
    "вентоборудовани",
    "огнезащитн",
    # contractual / TZ
    "ТЗ",
    "ТЗ vs",
    "по ТЗ",
    "задание заказчик",
]

# Disciplines that are inherently cross-discipline-heavy (always trigger).
ALWAYS_TRIGGER_DISCIPLINES = {"MULTI", "TZ_RD", "CROSS"}

# Disciplines that are inherently intra-only (less likely to need XD).
INTRA_DOMINANT_DISCIPLINES = {"KJ", "KM", "GEO", "GP", "POS"}


@dataclass
class RouterDecision:
    cross_discipline_triggered: bool
    triggers_hit: list[str] = field(default_factory=list)
    discipline: str = ""
    reason: str = ""

    def to_dict(self) -> dict:
        return {
            "cross_discipline_triggered": self.cross_discipline_triggered,
            "triggers_hit": self.triggers_hit,
            "discipline": self.discipline,
            "reason": self.reason,
        }


def should_run_cross_discipline(md_content: str, discipline: str) -> RouterDecision:
    """Decide whether to run the cross_discipline lens for this MD.

    Rules (first match wins):
      1. Discipline in ALWAYS_TRIGGER_DISCIPLINES → always trigger.
      2. Discipline in INTRA_DOMINANT_DISCIPLINES → trigger only if at
         least 2 trigger terms appear in MD.
      3. Otherwise → trigger if at least 1 trigger term appears in MD.
    """
    discipline_norm = (discipline or "").upper()
    md_text = md_content or ""

    triggers_hit: list[str] = []
    for term in TRIGGER_TERMS:
        if term.startswith("\\b") or term.endswith("\\b"):
            pattern = re.compile(term)
        else:
            pattern = re.compile(re.escape(term), re.IGNORECASE)
        if pattern.search(md_text):
            triggers_hit.append(term)
    triggers_hit = sorted(set(triggers_hit))

    if discipline_norm in ALWAYS_TRIGGER_DISCIPLINES:
        return RouterDecision(
            cross_discipline_triggered=True,
            triggers_hit=triggers_hit,
            discipline=discipline_norm,
            reason=f"discipline {discipline_norm} in ALWAYS_TRIGGER set",
        )

    if discipline_norm in INTRA_DOMINANT_DISCIPLINES:
        if len(triggers_hit) >= 2:
            return RouterDecision(
                cross_discipline_triggered=True,
                triggers_hit=triggers_hit,
                discipline=discipline_norm,
                reason=f"discipline {discipline_norm} is intra-dominant but {len(triggers_hit)} triggers ≥ 2 found",
            )
        return RouterDecision(
            cross_discipline_triggered=False,
            triggers_hit=triggers_hit,
            discipline=discipline_norm,
            reason=f"discipline {discipline_norm} is intra-dominant; {len(triggers_hit)} triggers (< 2)",
        )

    if triggers_hit:
        return RouterDecision(
            cross_discipline_triggered=True,
            triggers_hit=triggers_hit,
            discipline=discipline_norm,
            reason=f"{len(triggers_hit)} cross-discipline triggers found",
        )

    return RouterDecision(
        cross_discipline_triggered=False,
        triggers_hit=[],
        discipline=discipline_norm,
        reason="no cross-discipline triggers found in MD",
    )


def reviewer_trigger(post_critic_count: int,
                      missed_warnings: list,
                      discipline: str) -> dict:
    """Decide whether to run the optional reviewer (A4 only).

    The reviewer fires when:
      - critic reported ≥ 2 substantive missed_findings_warning items, AND
      - post-critic count < 12, AND
      - case discipline is not in {AR, KJ} (low marginal value per parent
        stand data).
    """
    discipline_norm = (discipline or "").upper()
    if discipline_norm in {"AR", "KJ"}:
        return {
            "reviewer_triggered": False,
            "reason": f"discipline {discipline_norm} excluded from reviewer fan-out",
        }
    if not missed_warnings or len(missed_warnings) < 2:
        return {
            "reviewer_triggered": False,
            "reason": f"only {len(missed_warnings or [])} missed-finding warnings (< 2)",
        }
    if post_critic_count >= 12:
        return {
            "reviewer_triggered": False,
            "reason": f"post-critic count {post_critic_count} >= 12 — no room",
        }
    return {
        "reviewer_triggered": True,
        "reason": f"{len(missed_warnings)} missed warnings, post-critic count {post_critic_count} < 12",
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("md_file", help="Path to MD file")
    ap.add_argument("--discipline", default="MULTI")
    args = ap.parse_args()

    md = Path(args.md_file).read_text(encoding="utf-8")
    decision = should_run_cross_discipline(md, args.discipline)
    print(json.dumps(decision.to_dict(), ensure_ascii=False, indent=2))
