#!/usr/bin/env python3
"""
simulate_critic_v2_assisted_round2.py
=====================================

Offline simulation of assisted_round2_candidate rules on round2 engineer
feedback.

Inputs:
- critic v2 test/assisted_round1_review/analysis/critic_v2_assisted_round1_feedback_enriched.csv
- critic v2 test/assisted_round1_review/critic_v2_triage_ui_assisted_round1.json

Outputs (default /tmp):
- critic_v2_assisted_round2_simulation.md
- critic_v2_assisted_round2_simulation.json
- critic_v2_assisted_round2_rule_hits.csv
- critic_v2_assisted_round2_risky_impact.csv

Strictly read-only:
- No LLM calls
- No production writes
- No network
- Rules cannot read human_decision / preferred_tab / reviewer_note /
  human_reason / priority / triage_correct (enforced by tests via SpyDict).
"""
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ANALYSIS_DIR = REPO_ROOT / "critic v2 test" / "assisted_round1_review" / "analysis"
DEFAULT_PKG_DIR = REPO_ROOT / "critic v2 test" / "assisted_round1_review"
DEFAULT_OUT_DIR = Path("/tmp")

# Fields that rules MUST NOT read (label-leakage). Enforced by tests.
LABEL_ONLY_FIELDS = frozenset({
    "human_decision", "preferred_tab", "reviewer_note",
    "human_reason", "priority", "triage_correct",
})

# Whitelisted runtime features rules may read.
RUNTIME_FEATURES = frozenset({
    "finding_id", "project_name", "section", "title",
    "current_tab", "current_queue", "reason", "taxonomy_reason",
    "evidence_quality", "score", "source_dependency",
    "round1_rule", "bucket",
    # Stage-02-style finding text fields supported via the v2 marker funcs
    "description", "recommendation", "sub_problem", "explanation",
})


# ──────────────────────────────────────────────────────────────────────────────
# Markers
# ──────────────────────────────────────────────────────────────────────────────

# OCR signals: explicit OCR words only — broken-token regex is intentionally
# excluded because round1 showed it is far too aggressive on technical labels.
_A1V2_OCR_TEXT_MARKERS = (
    "ocr", "распозна", "артефакт распозн",
    "битый", "битая", "нечитаемо", "нечитаем",
    "неразборчив", "ошибка распозна",
    "ocr-ошибк", "ocr ошибк",
)
_A1V2_OCR_TAXONOMIES = frozenset({
    "visual_or_ocr_misread",
    "false_positive_due_to_missing_context",
})
# Additional optional signal: explicit OCR-style typo hint pattern in title
_A1V2_OCR_TYPO_HINT_RE = re.compile(
    r"опечатк|опечатка|перепутан[ао]|перестановк[аи] букв", re.IGNORECASE
)
# Source-dependency signal that suggests low confidence input
_A1V2_OCR_SOURCE_SIGNALS = frozenset({"needs_more_context"})

# RD vs PZ markers (KJ/EOM only) — at least 2 needed.
_CV2_RD_PZ_MARKERS = (
    "пз ", "пояснительн", "расчёт", "расчет",
    "rei ", " rei", "огнестойк",
    "сп 468", "сп 385", "сп 484", "сп 485", "сп 486",
    "экспертиз", "нормативная баз",
    "расчётное обоснован", "расчетное обоснован",
    "не чертёж рд", "не требуется в рд",
    "расчётный параметр", "расчетный параметр",
    "класс бетона", "коэффициент",
)
_CV2_SECTIONS = frozenset({"KJ", "EOM"})

# Already covered: requires BOTH a strong text marker AND one of:
# - safe taxonomy_reason in known set, OR
# - explicit pointer to location of coverage (лист/таблица/спецификация/общие
#   указания).
_DV2_STRONG_TEXT_MARKERS = (
    "уже указ", "уже учт",
    "присутствует в спецификац", "присутствует в раздел",
    "есть в смежном раздел",
    "определяется по таблиц",
    "указано в специф", "указано в ведомост",
    "указано в общих указани", "в общих указани",
    "перечислено в специф",
    "продублирован", "дублирован",
    "указан на сторон",
)
_DV2_SAFE_TAXONOMIES = frozenset({
    "duplicate_or_already_covered",
    "already_resolved_by_project_note",
})
_DV2_COVERAGE_LOCATION_MARKERS = (
    "по таблиц", "в таблиц",
    "в спецификац", "по спецификац",
    "в общих указани", "общими указани",
    "на стороннем листе", "лист ",
    "ведомост",
)


# ──────────────────────────────────────────────────────────────────────────────
# Rule predicates (pure, label-free).
# ──────────────────────────────────────────────────────────────────────────────


def _runtime_text_blob(item: dict) -> str:
    parts = []
    for key in ("title", "description", "recommendation",
                "sub_problem", "explanation"):
        v = item.get(key)
        if v:
            parts.append(str(v))
    return " ".join(parts).lower()


def a1_v2_signals(item: dict) -> dict[str, bool]:
    blob = _runtime_text_blob(item)
    tax = (item.get("taxonomy_reason") or "").strip()
    ev = (item.get("evidence_quality") or "").strip()
    src = (item.get("source_dependency") or "").strip()
    return {
        "ocr_text": any(m in blob for m in _A1V2_OCR_TEXT_MARKERS),
        "ocr_taxonomy": tax in _A1V2_OCR_TAXONOMIES,
        "ocr_typo_hint": bool(_A1V2_OCR_TYPO_HINT_RE.search(blob)),
        "low_evidence": ev in ("partial", "weak"),
        "low_source": src in _A1V2_OCR_SOURCE_SIGNALS,
    }


def rule_a1_v2_fires(item: dict, required: int = 2) -> bool:
    sigs = a1_v2_signals(item)
    return sum(1 for v in sigs.values() if v) >= required


def c_v2_signals(item: dict) -> dict[str, bool]:
    section = (item.get("section") or "").strip().upper()
    if section not in _CV2_SECTIONS:
        return {"section_gate": False}
    blob = _runtime_text_blob(item)
    hits = sum(1 for m in _CV2_RD_PZ_MARKERS if m in blob)
    return {
        "section_gate": True,
        "marker_count": hits,
        "has_two_markers": hits >= 2,
        "has_one_marker": hits >= 1,
    }


def c_v2_guard_violated(item: dict) -> bool:
    """C_v2 must NOT fire on strong economic/spec-mismatch findings without RD/PZ context."""
    severity = (item.get("severity") or item.get("category") or "").upper()
    if severity not in ("ЭКОНОМИЧЕСКОЕ", "КРИТИЧЕСКОЕ"):
        return False
    ev = (item.get("evidence_quality") or "").strip()
    try:
        score = float(item.get("score") or 0)
    except (TypeError, ValueError):
        score = 0.0
    if ev == "valid" and score >= 8:
        # Strong + economic + valid + score>=8 → require ≥2 markers explicitly.
        sigs = c_v2_signals(item)
        return not sigs.get("has_two_markers", False)
    return False


def rule_c_v2_fires(item: dict) -> bool:
    if c_v2_guard_violated(item):
        return False
    sigs = c_v2_signals(item)
    return bool(sigs.get("section_gate")) and bool(sigs.get("has_two_markers"))


def d_v2_signals(item: dict) -> dict[str, bool]:
    blob = _runtime_text_blob(item)
    tax = (item.get("taxonomy_reason") or "").strip()
    return {
        "strong_text": any(m in blob for m in _DV2_STRONG_TEXT_MARKERS),
        "safe_taxonomy": tax in _DV2_SAFE_TAXONOMIES,
        "coverage_location": any(m in blob for m in _DV2_COVERAGE_LOCATION_MARKERS),
    }


def d_v2_guard_violated(item: dict) -> bool:
    """D_v2 must NOT fire on strong critical/economic high-score valid findings
    without a concrete pointer to *where* it is already covered."""
    severity = (item.get("severity") or item.get("category") or "").upper()
    if severity not in ("ЭКОНОМИЧЕСКОЕ", "КРИТИЧЕСКОЕ"):
        return False
    ev = (item.get("evidence_quality") or "").strip()
    try:
        score = float(item.get("score") or 0)
    except (TypeError, ValueError):
        score = 0.0
    if ev == "valid" and score >= 8:
        sigs = d_v2_signals(item)
        return not sigs.get("coverage_location", False)
    return False


def rule_d_v2_fires(item: dict) -> bool:
    if d_v2_guard_violated(item):
        return False
    sigs = d_v2_signals(item)
    # Need strong_text AND at least one of (safe_taxonomy, coverage_location).
    if not sigs["strong_text"]:
        return False
    return sigs["safe_taxonomy"] or sigs["coverage_location"]


RULE_FUNCS = {
    "A1_v2": rule_a1_v2_fires,
    "C_v2": rule_c_v2_fires,
    "D_v2": rule_d_v2_fires,
}

COMBOS = {
    "only_C_v2": ("C_v2",),
    "A1_v2 + C_v2": ("A1_v2", "C_v2"),
    "C_v2 + D_v2": ("C_v2", "D_v2"),
    "A1_v2 + C_v2 + D_v2": ("A1_v2", "C_v2", "D_v2"),
    "conservative_plus_C_v2": ("C_v2",),  # alias name for clarity
    "strict_only_C_v2_D_v2": ("C_v2", "D_v2"),  # both must require 2 strong signals already
}


# ──────────────────────────────────────────────────────────────────────────────
# Loading
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class Item:
    raw: dict
    finding_id: str
    section: str
    project_name: str
    title: str
    current_tab: str
    round1_rule: str
    taxonomy_reason: str
    evidence_quality: str
    score: Any
    source_dependency: str
    bucket: str
    # Reviewer ground-truth (only used for metrics, NEVER for rules)
    triage_correct: str
    preferred_tab: str
    reviewer_note: str
    priority: str
    severity: str = ""

    def runtime_view(self) -> dict:
        """Return a dict containing only whitelisted runtime features.

        Tests verify that rules invoked on this view do not raise even when
        label-only fields are missing — i.e. the rules don't depend on them.
        """
        d = {
            "finding_id": self.finding_id,
            "section": self.section,
            "project_name": self.project_name,
            "title": self.title,
            "current_tab": self.current_tab,
            "round1_rule": self.round1_rule,
            "taxonomy_reason": self.taxonomy_reason,
            "evidence_quality": self.evidence_quality,
            "score": self.score,
            "source_dependency": self.source_dependency,
            "bucket": self.bucket,
            "severity": self.severity,
        }
        return d


def load_enriched(path: Path) -> list[Item]:
    items: list[Item] = []
    seen = set()
    with path.open(encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            fid = row.get("finding_id") or ""
            # Dedup: keep the LATEST row per fid (later in file = later feedback).
            seen.add(fid)
            items.append(Item(
                raw=row,
                finding_id=fid,
                section=(row.get("section") or "").upper(),
                project_name=row.get("project_name") or "",
                title=row.get("title") or "",
                current_tab=row.get("current_tab") or "",
                round1_rule=row.get("round1_rule") or "",
                taxonomy_reason=row.get("taxonomy_reason") or "",
                evidence_quality=row.get("evidence_quality") or "",
                score=row.get("score") or "",
                source_dependency=row.get("source_dependency") or "",
                bucket=row.get("bucket") or "",
                triage_correct=row.get("triage_correct") or "",
                preferred_tab=row.get("preferred_tab") or "",
                reviewer_note=row.get("reviewer_note") or "",
                priority=row.get("priority") or "",
            ))
    # Latest-wins dedup
    by_fid: dict[str, Item] = {}
    for it in items:
        by_fid[it.finding_id] = it
    return list(by_fid.values())


def load_ui_export(pkg_dir: Path) -> dict[str, dict]:
    p = pkg_dir / "critic_v2_triage_ui_assisted_round1.json"
    if not p.exists():
        return {}
    d = json.loads(p.read_text(encoding="utf-8"))
    return {it["finding_id"]: it for it in d.get("items", []) if it.get("finding_id")}


# ──────────────────────────────────────────────────────────────────────────────
# Metrics
# ──────────────────────────────────────────────────────────────────────────────


def reviewer_outcome(item: Item) -> str:
    """One of: confirmed_SR, returned_to_primary, moved_to_needs_context,
    moved_to_hidden, unknown."""
    pref = (item.preferred_tab or "").strip()
    if pref == "primary":
        return "returned_to_primary"
    if pref == "needs_context":
        return "moved_to_needs_context"
    if pref == "hidden_by_critic":
        return "moved_to_hidden"
    if pref == "suggested_reject":
        return "confirmed_SR"
    tc = (item.triage_correct or "").lower()
    if tc == "yes":
        return "confirmed_SR"
    if tc == "no":
        return "returned_to_primary"
    return "unknown"


def evaluate_rule(items: list[Item], rule_name: str) -> dict[str, Any]:
    fn = RULE_FUNCS[rule_name]
    matched = []
    by_section = Counter()
    by_reason = Counter()
    risky_hit = []
    for it in items:
        if fn(it.runtime_view()):
            matched.append(it)
            by_section[it.section or "(empty)"] += 1
            by_reason[it.round1_rule or "(empty)"] += 1
            if it.bucket == "risky_accepted_22":
                risky_hit.append(it)

    confirmed = sum(1 for it in matched if reviewer_outcome(it) == "confirmed_SR")
    returned = sum(1 for it in matched if reviewer_outcome(it) == "returned_to_primary")
    needs_ctx = sum(1 for it in matched if reviewer_outcome(it) == "moved_to_needs_context")
    hidden = sum(1 for it in matched if reviewer_outcome(it) == "moved_to_hidden")
    unknown = sum(1 for it in matched if reviewer_outcome(it) == "unknown")
    denom = confirmed + returned + needs_ctx + hidden
    precision = (confirmed / denom) if denom else None
    risky_returned = sum(1 for it in risky_hit if reviewer_outcome(it) == "returned_to_primary")
    return {
        "matched": len(matched),
        "confirmed_SR": confirmed,
        "returned_to_primary": returned,
        "moved_to_needs_context": needs_ctx,
        "moved_to_hidden": hidden,
        "unknown": unknown,
        "precision": precision,
        "risky_hits": len(risky_hit),
        "risky_returned_to_primary": risky_returned,
        "by_section": dict(by_section),
        "by_round1_reason": dict(by_reason),
        "matched_fids": [it.finding_id for it in matched],
    }


def evaluate_combo(items: list[Item], rules: tuple[str, ...]) -> dict[str, Any]:
    """An item is matched by combo if ANY of the rules fires."""
    matched: list[Item] = []
    for it in items:
        view = it.runtime_view()
        for r in rules:
            if RULE_FUNCS[r](view):
                matched.append(it)
                break
    confirmed = sum(1 for it in matched if reviewer_outcome(it) == "confirmed_SR")
    returned = sum(1 for it in matched if reviewer_outcome(it) == "returned_to_primary")
    needs_ctx = sum(1 for it in matched if reviewer_outcome(it) == "moved_to_needs_context")
    hidden = sum(1 for it in matched if reviewer_outcome(it) == "moved_to_hidden")
    denom = confirmed + returned + needs_ctx + hidden
    precision = (confirmed / denom) if denom else None
    risky_matched = [it for it in matched if it.bucket == "risky_accepted_22"]
    risky_returned = sum(1 for it in risky_matched
                         if reviewer_outcome(it) == "returned_to_primary")
    # Net workload: confirmed SR cards are wins; returned_to_primary are losses.
    net = confirmed - returned
    return {
        "rules": list(rules),
        "matched": len(matched),
        "confirmed_SR": confirmed,
        "returned_to_primary": returned,
        "moved_to_needs_context": needs_ctx,
        "moved_to_hidden": hidden,
        "precision": precision,
        "risky_hits": len(risky_matched),
        "risky_returned_to_primary": risky_returned,
        "net_workload": net,
    }


def risky_impact(items: list[Item], rule_names: list[str]) -> list[dict]:
    """For each rule_name + 'combo_all', report how many of the 14 risky-returned
    cards are still affected and how many of the 2 confirmed remain affected."""
    risky = [it for it in items if it.bucket == "risky_accepted_22"]
    risky_returned_set = {it.finding_id for it in risky
                          if reviewer_outcome(it) == "returned_to_primary"}
    risky_confirmed_set = {it.finding_id for it in risky
                           if reviewer_outcome(it) == "confirmed_SR"}
    out = []
    for rule_name in rule_names:
        fn = RULE_FUNCS[rule_name]
        hit_returned = sum(1 for it in risky if it.finding_id in risky_returned_set
                           and fn(it.runtime_view()))
        hit_confirmed = sum(1 for it in risky if it.finding_id in risky_confirmed_set
                            and fn(it.runtime_view()))
        out.append({
            "rule": rule_name,
            "risky_returned_total": len(risky_returned_set),
            "risky_returned_still_affected": hit_returned,
            "risky_returned_no_longer_affected": len(risky_returned_set) - hit_returned,
            "risky_confirmed_total": len(risky_confirmed_set),
            "risky_confirmed_still_affected": hit_confirmed,
        })
    return out


def best_combo(combo_results: dict[str, dict]) -> str:
    """Pick combo with the highest net_workload that has precision >= 0.6
    (a conservative bar above D_v1's 37.4%); fall back to highest net regardless."""
    qualified = [(name, r) for name, r in combo_results.items()
                 if r["precision"] is not None and r["precision"] >= 0.6]
    if qualified:
        return max(qualified, key=lambda kv: (kv[1]["net_workload"], kv[1]["confirmed_SR"]))[0]
    return max(combo_results.items(),
               key=lambda kv: (kv[1]["net_workload"], kv[1]["confirmed_SR"]))[0]


# ──────────────────────────────────────────────────────────────────────────────
# Writers
# ──────────────────────────────────────────────────────────────────────────────


def write_rule_hits_csv(path: Path, items: list[Item]) -> None:
    rows = []
    for it in items:
        view = it.runtime_view()
        rows.append({
            "finding_id": it.finding_id,
            "section": it.section,
            "round1_rule": it.round1_rule,
            "current_tab": it.current_tab,
            "bucket": it.bucket,
            "reviewer_outcome": reviewer_outcome(it),
            "a1_v2_fires": rule_a1_v2_fires(view),
            "c_v2_fires": rule_c_v2_fires(view),
            "d_v2_fires": rule_d_v2_fires(view),
        })
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(
            fh,
            fieldnames=["finding_id", "section", "round1_rule", "current_tab",
                        "bucket", "reviewer_outcome",
                        "a1_v2_fires", "c_v2_fires", "d_v2_fires"],
            quoting=csv.QUOTE_MINIMAL,
        )
        w.writeheader()
        for r in rows:
            w.writerow(r)


def write_risky_csv(path: Path, items: list[Item]) -> None:
    risky = [it for it in items if it.bucket == "risky_accepted_22"]
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(
            fh,
            fieldnames=["finding_id", "section", "project_name",
                        "reviewer_outcome",
                        "preferred_tab", "triage_correct",
                        "a1_v2_fires", "c_v2_fires", "d_v2_fires",
                        "still_affected_by_round2_candidate"],
            quoting=csv.QUOTE_MINIMAL,
        )
        w.writeheader()
        for it in risky:
            view = it.runtime_view()
            a1 = rule_a1_v2_fires(view)
            c = rule_c_v2_fires(view)
            d = rule_d_v2_fires(view)
            w.writerow({
                "finding_id": it.finding_id,
                "section": it.section,
                "project_name": it.project_name,
                "reviewer_outcome": reviewer_outcome(it),
                "preferred_tab": it.preferred_tab,
                "triage_correct": it.triage_correct,
                "a1_v2_fires": a1,
                "c_v2_fires": c,
                "d_v2_fires": d,
                "still_affected_by_round2_candidate": (a1 or c or d),
            })


def render_md(items: list[Item],
              per_rule: dict[str, dict],
              combo_results: dict[str, dict],
              risky_impact_rows: list[dict],
              recommendation: str) -> str:
    def pct(p): return "—" if p is None else f"{p:.1%}"
    out = []
    out.append("# Critic v2 — assisted_round2_candidate offline simulation")
    out.append("")
    out.append(
        "**Источник:** round2 engineer feedback (50 файлов, 250 уникальных "
        "finding_id, 97 SR-карточек после дедупликации).")
    out.append("**Production не тронут.** Симуляция выполнена в /tmp + analysis/, "
               "никаких изменений в triage.py / production critic не сделано.")
    out.append("")
    out.append("## A. Per-rule v2 metrics")
    out.append("")
    out.append("| rule | matched | confirmed_SR | back_to_primary | needs_ctx | hidden | unknown | precision | risky_hits | risky_returned |")
    out.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for name, m in per_rule.items():
        out.append(
            f"| `{name}` | {m['matched']} | {m['confirmed_SR']} | "
            f"{m['returned_to_primary']} | {m['moved_to_needs_context']} | "
            f"{m['moved_to_hidden']} | {m['unknown']} | {pct(m['precision'])} | "
            f"{m['risky_hits']} | {m['risky_returned_to_primary']} |"
        )
    out.append("")
    out.append("## B. Per-rule section breakdown")
    out.append("")
    out.append("| rule | AR | KJ | EOM | other |")
    out.append("|---|---:|---:|---:|---:|")
    for name, m in per_rule.items():
        sec = m["by_section"]
        out.append(
            f"| `{name}` | {sec.get('AR',0)} | {sec.get('KJ',0)} | "
            f"{sec.get('EOM',0)} | "
            f"{sum(n for k,n in sec.items() if k not in ('AR','KJ','EOM'))} |"
        )
    out.append("")
    out.append("## C. Combo metrics")
    out.append("")
    out.append("| combo | matched | confirmed_SR | back_to_primary | precision | net_workload | risky_hits | risky_returned |")
    out.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for name, m in combo_results.items():
        out.append(
            f"| `{name}` | {m['matched']} | {m['confirmed_SR']} | "
            f"{m['returned_to_primary']} | {pct(m['precision'])} | "
            f"{m['net_workload']} | {m['risky_hits']} | "
            f"{m['risky_returned_to_primary']} |"
        )
    out.append("")
    out.append("## D. Risky_accepted impact (14 returned + 2 confirmed)")
    out.append("")
    out.append("| rule | risky_returned_total | still_affected | no_longer_affected | risky_confirmed_total | confirmed_still_affected |")
    out.append("|---|---:|---:|---:|---:|---:|")
    for r in risky_impact_rows:
        out.append(
            f"| `{r['rule']}` | {r['risky_returned_total']} | "
            f"{r['risky_returned_still_affected']} | "
            f"{r['risky_returned_no_longer_affected']} | "
            f"{r['risky_confirmed_total']} | "
            f"{r['risky_confirmed_still_affected']} |"
        )
    out.append("")
    out.append("## E. Recommendation")
    out.append("")
    out.append(f"Лучший combo по net_workload (precision ≥ 60% prefered): **`{recommendation}`**")
    out.append("")
    out.append("### Безопасные шаги")
    out.append("")
    out.append("1. **Не внедрять** v2 правила в `triage.py` без отдельного подтверждения.")
    out.append("2. Сначала собрать round3 feedback по карточкам, которые v2-кандидат "
               "**всё ещё** отправляет в SR.")
    out.append("3. Если по round3 D_v2/C_v2 имеют precision ≥ 60% — можно "
               "обсуждать внедрение через отдельный profile name (assisted_round2).")
    out.append("4. A1_v2 в текущем виде почти не матчится (требует 2 сигнала, "
               "OCR-таксономии нет у feedback-карточек). Возможно, A1 надо "
               "отключить совсем, а OCR-сигналы оставить только для будущего LLM-tag.")
    out.append("")
    out.append("## F. Production safety check")
    out.append("")
    out.append("- `manager.py`, `findings_review/runner.py`, `rule_filter.py`, "
               "`scorer.py`, `llm_gate.py`, `triage.py` не изменены в этом проходе.")
    out.append("- LLM не вызывался.")
    out.append("- БД и `_output/<project>/` не тронуты.")
    out.append("- Все правила работают только над pre-review фичами "
               "(`title`, `taxonomy_reason`, `evidence_quality`, `score`, "
               "`source_dependency`, `section`, `severity`). "
               "`reviewer_note`, `preferred_tab`, `human_decision`, `human_reason`, "
               "`priority`, `triage_correct` — НЕ читаются.")
    return "\n".join(out)


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────


def parse_args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--analysis-dir", type=Path, default=DEFAULT_ANALYSIS_DIR)
    ap.add_argument("--pkg-dir", type=Path, default=DEFAULT_PKG_DIR)
    ap.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    return ap.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    args.out_dir.mkdir(parents=True, exist_ok=True)

    enriched_csv = args.analysis_dir / "critic_v2_assisted_round1_feedback_enriched.csv"
    items = load_enriched(enriched_csv)
    print(f"loaded items: {len(items)}")

    per_rule = {name: evaluate_rule(items, name) for name in RULE_FUNCS}
    combos = {name: evaluate_combo(items, rules) for name, rules in COMBOS.items()}
    risky_rows = risky_impact(items, list(RULE_FUNCS.keys()))
    rec = best_combo(combos)

    write_rule_hits_csv(args.out_dir / "critic_v2_assisted_round2_rule_hits.csv", items)
    write_risky_csv(args.out_dir / "critic_v2_assisted_round2_risky_impact.csv", items)

    md = render_md(items, per_rule, combos, risky_rows, rec)
    (args.out_dir / "critic_v2_assisted_round2_simulation.md").write_text(
        md, encoding="utf-8")

    summary = {
        "items_total": len(items),
        "per_rule": per_rule,
        "combos": combos,
        "risky_impact": risky_rows,
        "recommended_combo": rec,
        "production_safety": {
            "manager_modified": False,
            "runner_modified": False,
            "rule_filter_modified": False,
            "scorer_modified": False,
            "llm_gate_modified": False,
            "triage_modified_in_this_pass": False,
            "llm_called": False,
            "label_only_fields_used_by_rules": False,
        },
    }
    (args.out_dir / "critic_v2_assisted_round2_simulation.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    # Console summary
    print("\nper-rule precision:")
    for name, m in per_rule.items():
        p = m["precision"]
        ps = "—" if p is None else f"{p:.1%}"
        print(f"  {name}: matched={m['matched']} confirmed={m['confirmed_SR']} "
              f"returned={m['returned_to_primary']} precision={ps}")
    print(f"\nrecommended combo: {rec}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
