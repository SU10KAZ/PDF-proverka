"""
Offline simulation candidate tuning rules для Critic v2 на feedback round 1.

КЛЮЧЕВОЕ ПРАВИЛО:
  candidate rules используют ТОЛЬКО признаки, доступные до экспертной разметки:
    title, description, recommendation, section, reason, risk_level,
    taxonomy_reason, source_dependency, original_tab, evidence_quality, score
  human_decision / preferred_tab / triage_correct / reviewer_note —
  используются ИСКЛЮЧИТЕЛЬНО как labels для подсчёта метрик после применения
  правил.

Запрещено:
  - не вызывает LLM;
  - не пишет в _output проектов;
  - не меняет production pipeline;
  - не модифицирует critic_v2 logic.

Запуск:
  python backend/scripts/simulate_critic_v2_tuning_rules_round1.py \
    [--feedback-dir "critic v2 test"] \
    [--ui-export /tmp/.../critic_v2_triage_ui.json] \
    [--out-json /tmp/critic_v2_tuning_rules_round1_simulation.json] \
    [--out-md   /tmp/critic_v2_tuning_rules_round1_simulation.md] \
    [--out-csv  /tmp/critic_v2_tuning_rules_round1_rule_hits.csv]
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Callable

DEFAULT_FEEDBACK_DIR = Path("critic v2 test")
DEFAULT_UI_EXPORT = Path(
    "/tmp/critic_v2_ui_export_for_manual_review/llm_no_context/critic_v2_triage_ui.json"
)
DEFAULT_OUT_JSON = Path("/tmp/critic_v2_tuning_rules_round1_simulation.json")
DEFAULT_OUT_MD = Path("/tmp/critic_v2_tuning_rules_round1_simulation.md")
DEFAULT_OUT_CSV = Path("/tmp/critic_v2_tuning_rules_round1_rule_hits.csv")

# Fields rules MAY read at runtime — все остальные считать labels.
RUNTIME_FEATURES = frozenset({
    "title", "description", "recommendation", "section",
    "reason", "risk_level", "taxonomy_reason", "source_dependency",
    "original_tab", "evidence_quality", "score", "queue",
})

# Fields rules MUST NOT read at runtime (labels only).
LABEL_ONLY = frozenset({
    "human_decision", "human_reason", "preferred_tab", "triage_correct",
    "reviewer_note", "priority",
})


# ──────────────────────────────────────────────────────────────────────────────
# Data loading (deliberately small surface — shared with round1 analyzer)
# ──────────────────────────────────────────────────────────────────────────────

def load_feedback(feedback_dir: Path) -> list[dict[str, Any]]:
    rows = []
    if not feedback_dir.exists():
        return rows
    for f in sorted(feedback_dir.glob("*.json")):
        try:
            d = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        scope = d.get("scope") or {}
        for item in (d.get("feedback") or []):
            rows.append({
                "_source_file": f.name,
                "_scope_project": scope.get("project_name") or scope.get("project_id"),
                **item,
            })
    return rows


def load_ui_index(path: Path) -> dict[tuple[str, str], dict[str, Any]]:
    if not path.exists():
        return {}
    try:
        d = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return {
        (it.get("project_name") or "", it.get("finding_id") or ""): it
        for it in d.get("items") or []
    }


def join(feedback: list[dict[str, Any]],
         ui_idx: dict[tuple[str, str], dict[str, Any]]) -> list[dict[str, Any]]:
    enriched = []
    for r in feedback:
        pn = r.get("project_name") or r.get("_scope_project") or ""
        fid = r.get("finding_id") or ""
        ui = ui_idx.get((pn, fid))
        if not ui and ":" in fid:
            ui = ui_idx.get((fid.rsplit(":", 1)[0], fid))
            if ui:
                pn = fid.rsplit(":", 1)[0]
        ui = ui or {}
        out = dict(r)
        if not out.get("project_name"):
            out["project_name"] = pn
        if not out.get("section") and ui.get("section"):
            out["section"] = ui["section"]
        for f in ("queue", "reason", "taxonomy_reason", "evidence_quality",
                  "score", "source_dependency", "risk_level",
                  "human_decision", "human_reason",
                  "title", "description", "recommendation"):
            out.setdefault(f, ui.get(f))
        out["_in_ui"] = bool(ui)
        enriched.append(out)
    return enriched


# ──────────────────────────────────────────────────────────────────────────────
# Rule infrastructure
#
# Rules return a "decision" dict:
#   {"predicted_tab": "suggested_reject" | "hidden_by_critic" | "primary"
#                     | "needs_context" | None,
#    "matched": True/False}
# A rule MUST not see fields from LABEL_ONLY.
# ──────────────────────────────────────────────────────────────────────────────

def _text_blob(item: dict[str, Any]) -> str:
    """Concatenated text features available pre-review."""
    parts = [
        str(item.get("title") or ""),
        str(item.get("description") or ""),
        str(item.get("recommendation") or ""),
    ]
    return " ".join(parts).lower()


# Compiled keyword sets (lowercased).
OCR_PATTERNS = [
    r"\bocr\b", "мусор", "нераспозн", "неразборчив",
    "битый", "битое обозначен",
    "неверное определени", "артефакт распозн",
]
# Catch broken tokens like "А-А-А" or all-caps fragments under 4 chars.
# Text blob is lowercased via _text_blob → so we match both cases via IGNORECASE.
BROKEN_TOKEN_RE = re.compile(
    r"\b[а-яa-z]{1,3}(?:[-/][а-яa-z]{1,3}){2,}\b",
    re.IGNORECASE,
)
SHORT_CYR_FRAG_RE = re.compile(
    r"\b[а-я]\d{0,2}[-/][а-я]\d{0,2}\b",
    re.IGNORECASE,
)

AUX_SCHEME_PATTERNS = [
    "вспомогательн", "условное обозначен", "экспликац",
    "пояснительная схем",
]
# But NOT triggered if mainline construction risk words are present.
STRONG_IMPACT_PATTERNS = [
    "обрушен", "несущ", "пожарн", "эвакуац", "безопасност",
    "категория функциональной", "класс пожар", "пути эвакуац",
]

RD_PZ_PATTERNS = [
    "пз раздела", "пояснительная записк", "расчёт", "расчет",
    "расчётный параметр", "расчетный параметр",
    "огнестойк", "rei", "rei ", "сп 468", "сп 385",
    "экспертиз", "нормативная база", "расчётное обоснован",
    "расчетное обоснован",
]

ALREADY_COVERED_PATTERNS = [
    "смежный раздел", "смежн", "дублирован",
    "присутствует в раздел", "присутствует на стороне",
    "указано в спецификац", "указано в ведомост",
    "есть схема", "имеется схема",
    "уже указан", "уже учт",
    "определяется по таблиц",
    "в марке кабел",
]

# For Rule F (text-side hints to keep in primary)
KEEP_PRIMARY_PATTERNS = [
    "нужно смотреть", "требуется проверка", "влияет", "оставить",
    "критич", "необходимо проверить",
]

# Rule E taxonomies & semantic categories
RULE_E_TAXONOMY_OK = frozenset({
    "other", "requirement_not_mandatory",
    "acceptable_design_solution", "duplicate_or_already_covered",
})
# "category" is derived from text — we don't have explicit category field, so
# derive a lightweight category from the title/description.
CATEGORY_KEYWORDS: dict[str, list[str]] = {
    "documentation": ["документац", "пз", "пояснительная", "ссылка на нп",
                      "ссылк", "нормативн"],
    "normative_refs": ["сп ", "гост ", "пуэ", "фз-", "норматив"],
    "fire_safety": ["огнестойк", "пожар", "эвакуац", "rei",
                    "противопожар"],
    "spec_mismatch": ["спецификац", "ведомост", "позиц", "марк", "кабел"],
    "evacuation": ["эвакуац", "пути эвакуац", "выход", "лестничн"],
}


def _category_of(item: dict[str, Any]) -> set[str]:
    blob = _text_blob(item)
    return {cat for cat, kws in CATEGORY_KEYWORDS.items()
            if any(k in blob for k in kws)}


def _has_any(blob: str, patterns: list[str]) -> bool:
    return any(p in blob for p in patterns)


def _matches_any_regex(blob: str, regexes) -> bool:
    return any(rx.search(blob) for rx in regexes)


# ──────────────────────────────────────────────────────────────────────────────
# Candidate rules
# ──────────────────────────────────────────────────────────────────────────────

def rule_A1_ocr_to_reject(it: dict[str, Any]) -> dict[str, Any]:
    """A1 — OCR markers → suggested_reject."""
    blob = _text_blob(it)
    hit = (
        any(re.search(p, blob) for p in OCR_PATTERNS)
        or BROKEN_TOKEN_RE.search(blob) is not None
        or SHORT_CYR_FRAG_RE.search(blob) is not None
    )
    if hit and it.get("original_tab") != "suggested_reject":
        return {"matched": True, "predicted_tab": "suggested_reject"}
    return {"matched": False, "predicted_tab": None}


def rule_A2_ocr_to_hidden(it: dict[str, Any]) -> dict[str, Any]:
    """A2 — same OCR detector but routes to hidden_by_critic."""
    blob = _text_blob(it)
    hit = (
        any(re.search(p, blob) for p in OCR_PATTERNS)
        or BROKEN_TOKEN_RE.search(blob) is not None
        or SHORT_CYR_FRAG_RE.search(blob) is not None
    )
    if hit and it.get("original_tab") != "hidden_by_critic":
        return {"matched": True, "predicted_tab": "hidden_by_critic"}
    return {"matched": False, "predicted_tab": None}


def rule_B_ar_auxiliary(it: dict[str, Any]) -> dict[str, Any]:
    if it.get("section") != "AR":
        return {"matched": False, "predicted_tab": None}
    blob = _text_blob(it)
    if not _has_any(blob, AUX_SCHEME_PATTERNS):
        return {"matched": False, "predicted_tab": None}
    if _has_any(blob, STRONG_IMPACT_PATTERNS):
        # Strong construction/safety impact — don't downgrade.
        return {"matched": False, "predicted_tab": None}
    if it.get("original_tab") == "suggested_reject":
        return {"matched": False, "predicted_tab": None}
    return {"matched": True, "predicted_tab": "suggested_reject"}


def rule_C_rd_pz(it: dict[str, Any]) -> dict[str, Any]:
    if it.get("section") not in ("KJ", "EOM"):
        return {"matched": False, "predicted_tab": None}
    blob = _text_blob(it)
    if not _has_any(blob, RD_PZ_PATTERNS):
        return {"matched": False, "predicted_tab": None}
    if it.get("original_tab") == "suggested_reject":
        return {"matched": False, "predicted_tab": None}
    return {"matched": True, "predicted_tab": "suggested_reject"}


def rule_D_already_covered(it: dict[str, Any]) -> dict[str, Any]:
    blob = _text_blob(it)
    if not _has_any(blob, ALREADY_COVERED_PATTERNS):
        return {"matched": False, "predicted_tab": None}
    if it.get("original_tab") == "suggested_reject":
        return {"matched": False, "predicted_tab": None}
    return {"matched": True, "predicted_tab": "suggested_reject"}


def rule_E_secondary_gate(it: dict[str, Any]) -> dict[str, Any]:
    """E — high-score secondary gate.

    Triggers ONLY on metadata fields available at triage time:
      reason == deterministic_accept_high_score
      AND risk_level == low
      AND taxonomy_reason in RULE_E_TAXONOMY_OK
      AND category derived from text intersects RULE_E categories.
    """
    if it.get("reason") != "deterministic_accept_high_score":
        return {"matched": False, "predicted_tab": None}
    if it.get("risk_level") != "low":
        return {"matched": False, "predicted_tab": None}
    if it.get("taxonomy_reason") not in RULE_E_TAXONOMY_OK:
        return {"matched": False, "predicted_tab": None}
    cats = _category_of(it)
    if not (cats & {"documentation", "normative_refs", "fire_safety",
                    "spec_mismatch", "evacuation"}):
        return {"matched": False, "predicted_tab": None}
    if it.get("original_tab") == "suggested_reject":
        return {"matched": False, "predicted_tab": None}
    return {"matched": True, "predicted_tab": "suggested_reject"}


def rule_F_needs_context_recalibrate(it: dict[str, Any]) -> dict[str, Any]:
    if it.get("original_tab") != "needs_context":
        return {"matched": False, "predicted_tab": None}
    if it.get("source_dependency") != "cross_section_required":
        return {"matched": False, "predicted_tab": None}
    if it.get("taxonomy_reason") != "insufficient_source_context":
        return {"matched": False, "predicted_tab": None}
    blob = _text_blob(it)
    if _has_any(blob, KEEP_PRIMARY_PATTERNS):
        return {"matched": True, "predicted_tab": "primary"}
    if _has_any(blob, ALREADY_COVERED_PATTERNS):
        return {"matched": True, "predicted_tab": "suggested_reject"}
    return {"matched": False, "predicted_tab": None}


RULES: dict[str, Callable[[dict[str, Any]], dict[str, Any]]] = {
    "A1_ocr_to_reject": rule_A1_ocr_to_reject,
    "A2_ocr_to_hidden": rule_A2_ocr_to_hidden,
    "B_ar_auxiliary": rule_B_ar_auxiliary,
    "C_rd_pz": rule_C_rd_pz,
    "D_already_covered": rule_D_already_covered,
    "E_secondary_gate": rule_E_secondary_gate,
    "F_needs_context_recalibrate": rule_F_needs_context_recalibrate,
}


# ──────────────────────────────────────────────────────────────────────────────
# Per-rule metrics
# ──────────────────────────────────────────────────────────────────────────────

def evaluate_rule(items: list[dict[str, Any]], rule_name: str,
                  rule_fn: Callable) -> dict[str, Any]:
    matched = 0
    correct = 0           # predicted_tab matches reviewer's preferred_tab
    wrong = 0
    accepted_affected = 0
    rejected_affected = 0
    critical_affected = 0
    by_section = Counter()
    target_hits = Counter()   # direction-specific recall numerator
    risky_examples = []        # human_decision=accepted but rule downgrades

    # Reviewer-defined target directions for recall (label space).
    target_directions = [
        ("primary", "suggested_reject"),
        ("primary", "hidden_by_critic"),
        ("primary", "needs_context"),
        ("needs_context", "primary"),
        ("needs_context", "suggested_reject"),
        ("needs_context", "hidden_by_critic"),
    ]

    sample_hits = []

    for it in items:
        d = rule_fn(it)
        if not d["matched"]:
            continue
        matched += 1
        pred = d["predicted_tab"]
        pref = it.get("preferred_tab")
        if pred == pref:
            correct += 1
            ot = it.get("original_tab")
            target_hits[(ot, pred)] += 1
        elif pref:
            wrong += 1
        # Side metrics (based on labels — not used in rule conditions)
        if it.get("human_decision") == "accepted":
            accepted_affected += 1
            if pred in ("hidden_by_critic", "suggested_reject"):
                risky_examples.append({
                    "finding_id": it.get("finding_id"),
                    "project_name": it.get("project_name"),
                    "section": it.get("section"),
                    "original_tab": it.get("original_tab"),
                    "predicted_tab": pred,
                    "human_decision": "accepted",
                    "title": (it.get("title") or "")[:160],
                })
        elif it.get("human_decision") == "rejected":
            rejected_affected += 1
        if it.get("priority") == "critical":
            critical_affected += 1
        by_section[it.get("section") or "?"] += 1
        if len(sample_hits) < 20:
            sample_hits.append({
                "finding_id": it.get("finding_id"),
                "project_name": it.get("project_name"),
                "section": it.get("section"),
                "original_tab": it.get("original_tab"),
                "predicted_tab": pred,
                "preferred_tab": pref,
                "human_decision": it.get("human_decision"),
                "score": it.get("score"),
                "title": (it.get("title") or "")[:140],
            })

    # Recall denominator: how many feedback items moved in this direction.
    denom_by_direction = Counter()
    for it in items:
        ot = it.get("original_tab")
        pt = it.get("preferred_tab")
        if ot and pt and ot != pt:
            denom_by_direction[(ot, pt)] += 1

    recall_by_direction = {}
    for (ot, pt) in target_directions:
        denom = denom_by_direction.get((ot, pt), 0)
        num = target_hits.get((ot, pt), 0)
        recall_by_direction[f"{ot} → {pt}"] = {
            "hits": num, "total": denom,
            "recall": round(num / denom, 3) if denom else None,
        }

    return {
        "rule": rule_name,
        "matched_items": matched,
        "correct_direction_count": correct,
        "wrong_direction_count": wrong,
        "precision_against_preferred_tab": (
            round(correct / matched, 3) if matched else None
        ),
        "recall_by_direction": recall_by_direction,
        "accepted_items_affected": accepted_affected,
        "rejected_items_affected": rejected_affected,
        "critical_priority_affected": critical_affected,
        "by_section": dict(by_section),
        "risky_accepted_hits": risky_examples[:20],
        "sample_hits": sample_hits,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Combination simulation
# ──────────────────────────────────────────────────────────────────────────────

# Priority of predicted_tab — more restrictive wins.
TAB_PRIORITY = {
    "hidden_by_critic": 4, "suggested_reject": 3,
    "needs_context": 2, "primary": 1,
}


def combine(rules: list[tuple[str, Callable]], item: dict[str, Any]
            ) -> tuple[str | None, list[str]]:
    """Return (final_predicted_tab, list_of_triggered_rules)."""
    triggers = []
    final = None
    for name, fn in rules:
        d = fn(item)
        if d["matched"] and d["predicted_tab"]:
            triggers.append(name)
            if final is None or TAB_PRIORITY[d["predicted_tab"]] > TAB_PRIORITY[final]:
                final = d["predicted_tab"]
    return final, triggers


def section_aware(items: list[dict[str, Any]], rules_per_section
                  ) -> tuple[dict, list]:
    """Apply different rule sets depending on section. Returns combo metrics + per-item rows."""
    section_rules = {s: [(n, RULES[n]) for n in names]
                     for s, names in rules_per_section.items()}
    return evaluate_combination(
        items,
        combo_name="section_aware",
        rule_resolver=lambda it: section_rules.get(it.get("section"), []),
    )


def evaluate_combination(items: list[dict[str, Any]], combo_name: str,
                         rule_resolver: Callable | None = None,
                         rules_list: list[tuple[str, Callable]] | None = None
                         ) -> tuple[dict, list]:
    """
    Compute UI/business metrics for a combined rule set.

    Either pass rules_list (applied to every item), or rule_resolver(item) ->
    list of (name, fn) (for section-aware setups).
    """
    new_tab_counts = Counter()
    base_tab_counts = Counter()
    moved_primary_out = 0
    moved_to_collapsed = 0
    moved_to_hidden = 0
    human_acc_collapsed = 0
    human_acc_hidden = 0
    human_rej_out_of_primary = 0
    correct_moves = 0
    wrong_moves = 0
    no_move_with_preferred = 0
    per_item_rows = []

    for it in items:
        base_tab_counts[it.get("original_tab")] += 1
        rules = rule_resolver(it) if rule_resolver else (rules_list or [])
        pred, triggers = combine(rules, it)
        final_tab = pred or it.get("original_tab")
        new_tab_counts[final_tab] += 1

        if pred and pred != it.get("original_tab"):
            if it.get("original_tab") == "primary":
                moved_primary_out += 1
            if pred in ("suggested_reject", "needs_context", "hidden_by_critic"):
                moved_to_collapsed += 1
            if pred == "hidden_by_critic":
                moved_to_hidden += 1
            if it.get("human_decision") == "accepted":
                if pred in ("suggested_reject", "needs_context",
                            "hidden_by_critic"):
                    human_acc_collapsed += 1
                if pred == "hidden_by_critic":
                    human_acc_hidden += 1
            if it.get("human_decision") == "rejected" \
                    and it.get("original_tab") == "primary" \
                    and pred != "primary":
                human_rej_out_of_primary += 1
            pref = it.get("preferred_tab")
            if pref:
                if pred == pref:
                    correct_moves += 1
                else:
                    wrong_moves += 1
            per_item_rows.append({
                "combo": combo_name,
                "finding_id": it.get("finding_id"),
                "project_name": it.get("project_name"),
                "section": it.get("section"),
                "original_tab": it.get("original_tab"),
                "predicted_tab": pred,
                "preferred_tab": it.get("preferred_tab"),
                "human_decision": it.get("human_decision"),
                "triggers": ";".join(triggers),
            })
        elif it.get("preferred_tab") and it.get("preferred_tab") != it.get("original_tab"):
            no_move_with_preferred += 1

    base_primary = base_tab_counts.get("primary", 0)
    new_primary = new_tab_counts.get("primary", 0)
    total = sum(base_tab_counts.values()) or 1

    # Baseline recalls — what's already achieved by Critic v2.
    accepted_total = sum(1 for it in items if it.get("human_decision") == "accepted")
    accepted_in_primary = sum(
        1 for it in items
        if it.get("human_decision") == "accepted" and it.get("original_tab") == "primary"
    )
    accepted_not_in_hidden = sum(
        1 for it in items
        if it.get("human_decision") == "accepted" and it.get("original_tab") != "hidden_by_critic"
    )
    # New recalls after rules applied.
    def _final(it):
        rules = rule_resolver(it) if rule_resolver else (rules_list or [])
        pred, _ = combine(rules, it)
        return pred or it.get("original_tab")

    new_accepted_in_primary = sum(
        1 for it in items
        if it.get("human_decision") == "accepted" and _final(it) == "primary"
    )
    new_accepted_not_in_hidden = sum(
        1 for it in items
        if it.get("human_decision") == "accepted" and _final(it) != "hidden_by_critic"
    )

    metrics = {
        "combo": combo_name,
        "items_total": total,
        "base_primary_count": base_primary,
        "new_primary_count": new_primary,
        "primary_queue_reduction_delta": base_primary - new_primary,
        "suggested_reject_delta": (
            new_tab_counts.get("suggested_reject", 0)
            - base_tab_counts.get("suggested_reject", 0)
        ),
        "hidden_by_critic_delta": (
            new_tab_counts.get("hidden_by_critic", 0)
            - base_tab_counts.get("hidden_by_critic", 0)
        ),
        "needs_context_delta": (
            new_tab_counts.get("needs_context", 0)
            - base_tab_counts.get("needs_context", 0)
        ),
        "items_moved": moved_to_collapsed + (
            new_tab_counts.get("primary", 0) - base_tab_counts.get("primary", 0)
            if new_primary > base_primary else 0
        ),
        "moved_primary_out": moved_primary_out,
        "moved_to_collapsed": moved_to_collapsed,
        "moved_to_hidden": moved_to_hidden,
        "human_accepted_moved_to_collapsed": human_acc_collapsed,
        "human_accepted_moved_to_hidden": human_acc_hidden,
        "human_rejected_moved_out_of_primary": human_rej_out_of_primary,
        "correct_moves_vs_preferred": correct_moves,
        "wrong_moves_vs_preferred": wrong_moves,
        "no_move_when_preferred_set": no_move_with_preferred,
        "accepted_primary_visible_recall_base": (
            round(accepted_in_primary / accepted_total, 3) if accepted_total else None
        ),
        "accepted_primary_visible_recall_new": (
            round(new_accepted_in_primary / accepted_total, 3) if accepted_total else None
        ),
        "accepted_not_hidden_recall_base": (
            round(accepted_not_in_hidden / accepted_total, 3) if accepted_total else None
        ),
        "accepted_not_hidden_recall_new": (
            round(new_accepted_not_in_hidden / accepted_total, 3) if accepted_total else None
        ),
        "net_workload_improvement": human_rej_out_of_primary - human_acc_collapsed,
    }
    return metrics, per_item_rows


# ──────────────────────────────────────────────────────────────────────────────
# Writers
# ──────────────────────────────────────────────────────────────────────────────

def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    if not rows:
        path.write_text("combo,finding_id,project_name,section,original_tab,"
                        "predicted_tab,preferred_tab,human_decision,triggers\n",
                        encoding="utf-8")
        return
    fields = ["combo", "finding_id", "project_name", "section",
              "original_tab", "predicted_tab", "preferred_tab",
              "human_decision", "triggers"]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def write_markdown(report: dict[str, Any], path: Path) -> None:
    out = []
    out.append("# Critic v2 — Round 1 Tuning Rules Simulation")
    out.append("")
    out.append(f"- items processed: **{report['items_total']}**")
    out.append("")
    out.append("> Все candidate rules используют только pre-review признаки "
               "(title/description/recommendation, section, reason, "
               "risk_level, taxonomy_reason, source_dependency, "
               "original_tab, evidence_quality, score). `human_decision` и "
               "`preferred_tab` — только для подсчёта метрик.")
    out.append("")

    out.append("## Per-rule metrics")
    out.append("")
    out.append("| rule | matched | correct | wrong | precision | "
               "acc_aff | rej_aff | crit_aff |")
    out.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for rname, r in report["per_rule"].items():
        p = r["precision_against_preferred_tab"]
        out.append(
            f"| `{rname}` | {r['matched_items']} | "
            f"{r['correct_direction_count']} | "
            f"{r['wrong_direction_count']} | "
            f"{p if p is not None else '—'} | "
            f"{r['accepted_items_affected']} | "
            f"{r['rejected_items_affected']} | "
            f"{r['critical_priority_affected']} |"
        )
    out.append("")

    out.append("## Recall by direction (per rule)")
    out.append("")
    out.append("| rule | direction | hits | total | recall |")
    out.append("|---|---|---:|---:|---:|")
    for rname, r in report["per_rule"].items():
        for direction, info in r["recall_by_direction"].items():
            if info["total"] == 0:
                continue
            out.append(
                f"| `{rname}` | {direction} | {info['hits']} | "
                f"{info['total']} | {info['recall']} |"
            )
    out.append("")

    out.append("## Combinations")
    out.append("")
    out.append("| combo | matched | correct | wrong | prim_q_red | "
               "rej_out_prim | acc_collapsed | acc_hidden | acc_recall_∆ | "
               "not_hidden_recall_∆ | net_impr |")
    out.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for c in report["combos"]:
        m = c["metrics"]
        ar_base = m["accepted_primary_visible_recall_base"] or 0
        ar_new = m["accepted_primary_visible_recall_new"] or 0
        nh_base = m["accepted_not_hidden_recall_base"] or 0
        nh_new = m["accepted_not_hidden_recall_new"] or 0
        out.append(
            f"| `{c['name']}` | {m['moved_primary_out'] + m['moved_to_collapsed']} | "
            f"{m['correct_moves_vs_preferred']} | "
            f"{m['wrong_moves_vs_preferred']} | "
            f"{m['primary_queue_reduction_delta']} | "
            f"{m['human_rejected_moved_out_of_primary']} | "
            f"{m['human_accepted_moved_to_collapsed']} | "
            f"{m['human_accepted_moved_to_hidden']} | "
            f"{round(ar_new - ar_base, 3)} | "
            f"{round(nh_new - nh_base, 3)} | "
            f"{m['net_workload_improvement']} |"
        )
    out.append("")

    out.append("## Sample hits (top-20 per rule)")
    out.append("")
    for rname, r in report["per_rule"].items():
        if not r["sample_hits"]:
            continue
        out.append(f"### `{rname}` — first {len(r['sample_hits'])} hits")
        out.append("")
        out.append("| finding_id | section | orig→pred | preferred | human | title |")
        out.append("|---|---|---|---|---|---|")
        for h in r["sample_hits"]:
            out.append(
                f"| {h['finding_id']} | {h['section']} | "
                f"{h['original_tab']}→{h['predicted_tab']} | "
                f"{h['preferred_tab'] or '—'} | "
                f"{h['human_decision'] or '—'} | "
                f"{h['title'].replace('|', '/')[:90]} |"
            )
        out.append("")

    out.append("## Risky accepted hits")
    out.append("")
    out.append("Карточки, которые правило отправляет в collapsed/hidden, "
               "хотя эксперт их принял (`human_decision=accepted`).")
    out.append("")
    for rname, r in report["per_rule"].items():
        if not r["risky_accepted_hits"]:
            continue
        out.append(f"### `{rname}` — {len(r['risky_accepted_hits'])} risky")
        for ex in r["risky_accepted_hits"][:5]:
            out.append(
                f"- [{ex['section']}] {ex['finding_id']} "
                f"{ex['original_tab']}→{ex['predicted_tab']}: "
                f"{ex['title'].replace(chr(10),' ')[:140]}"
            )
        out.append("")

    out.append("## Рекомендации")
    out.append("")
    out.extend(report["recommendations"])
    path.write_text("\n".join(out), encoding="utf-8")


# ──────────────────────────────────────────────────────────────────────────────
# Top-level
# ──────────────────────────────────────────────────────────────────────────────

def derive_recommendations(per_rule: dict[str, Any],
                           combos: list[dict[str, Any]]) -> list[str]:
    out = []
    safe = []
    risky = []
    for rname, r in per_rule.items():
        p = r["precision_against_preferred_tab"]
        risk_ratio = (r["accepted_items_affected"]
                      / r["matched_items"]) if r["matched_items"] else 0
        if r["matched_items"] == 0:
            continue
        if (p is not None and p >= 0.7) and risk_ratio <= 0.05:
            safe.append((rname, r["matched_items"], p, risk_ratio))
        elif risk_ratio > 0.15:
            risky.append((rname, r["matched_items"], p, risk_ratio))
    out.append("**Safe-to-test rules (precision ≥ 0.7, risk ≤ 5%):**")
    if safe:
        for n, m, p, rr in safe:
            out.append(f"- `{n}`: matched={m}, precision={p}, risk={round(rr,3)}")
    else:
        out.append("- (нет правил с такими порогами — см. combo-результаты)")
    out.append("")
    out.append("**Risky rules (>15% попадает в принятые экспертом):**")
    if risky:
        for n, m, p, rr in risky:
            out.append(f"- `{n}`: matched={m}, precision={p}, risk={round(rr,3)}")
    else:
        out.append("- (нет таких)")
    out.append("")
    # Combo recs
    best = max(combos, key=lambda c: c["metrics"]["net_workload_improvement"]
               if c["metrics"]["net_workload_improvement"] is not None else -999)
    out.append(f"**Лучшая combo по net_workload_improvement:** "
               f"`{best['name']}` (net={best['metrics']['net_workload_improvement']})")
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--feedback-dir", default=str(DEFAULT_FEEDBACK_DIR))
    ap.add_argument("--ui-export", default=str(DEFAULT_UI_EXPORT))
    ap.add_argument("--out-json", default=str(DEFAULT_OUT_JSON))
    ap.add_argument("--out-md", default=str(DEFAULT_OUT_MD))
    ap.add_argument("--out-csv", default=str(DEFAULT_OUT_CSV))
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    feedback = load_feedback(Path(args.feedback_dir))
    ui_idx = load_ui_index(Path(args.ui_export))
    items = join(feedback, ui_idx)

    per_rule = {}
    for rname, rfn in RULES.items():
        per_rule[rname] = evaluate_rule(items, rname, rfn)

    combo_defs = [
        ("A1+B+C+D+E+F", ["A1_ocr_to_reject", "B_ar_auxiliary", "C_rd_pz",
                          "D_already_covered", "E_secondary_gate",
                          "F_needs_context_recalibrate"]),
        ("A2+B+C+D+E+F", ["A2_ocr_to_hidden", "B_ar_auxiliary", "C_rd_pz",
                          "D_already_covered", "E_secondary_gate",
                          "F_needs_context_recalibrate"]),
        ("conservative_A1_C_D", ["A1_ocr_to_reject", "C_rd_pz",
                                 "D_already_covered"]),
    ]
    combo_csv_rows = []
    combo_results = []
    for cname, names in combo_defs:
        rules_list = [(n, RULES[n]) for n in names]
        metrics, rows = evaluate_combination(items, combo_name=cname,
                                             rules_list=rules_list)
        combo_csv_rows.extend(rows)
        combo_results.append({"name": cname, "rules": names, "metrics": metrics})

    # section_aware: AR uses A1+B, KJ/EOM uses A1+C+D+E
    section_metrics, section_rows = section_aware(items, {
        "AR": ["A1_ocr_to_reject", "B_ar_auxiliary"],
        "KJ": ["A1_ocr_to_reject", "C_rd_pz", "D_already_covered", "E_secondary_gate"],
        "EOM": ["A1_ocr_to_reject", "C_rd_pz", "D_already_covered", "E_secondary_gate"],
    })
    section_metrics["combo"] = "section_aware_AR_A1B_KJ_EOM_A1CDE"
    combo_csv_rows.extend(section_rows)
    combo_results.append({"name": section_metrics["combo"],
                          "rules": ["section_aware"], "metrics": section_metrics})

    recs = derive_recommendations(per_rule, combo_results)

    report = {
        "items_total": len(items),
        "per_rule": per_rule,
        "combos": combo_results,
        "recommendations": recs,
    }
    Path(args.out_json).write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    write_markdown(report, Path(args.out_md))
    write_csv(combo_csv_rows, Path(args.out_csv))

    if not args.quiet:
        print(f"items: {len(items)}")
        for rname, r in per_rule.items():
            print(f"  {rname:35s} matched={r['matched_items']:3d} "
                  f"correct={r['correct_direction_count']:3d} "
                  f"precision={r['precision_against_preferred_tab']} "
                  f"acc_aff={r['accepted_items_affected']}")
        print()
        for c in combo_results:
            m = c["metrics"]
            print(f"  combo {c['name']:45s} prim_red={m['primary_queue_reduction_delta']:3d} "
                  f"rej_out={m['human_rejected_moved_out_of_primary']:3d} "
                  f"acc_collapsed={m['human_accepted_moved_to_collapsed']} "
                  f"net={m['net_workload_improvement']}")
        print(f"\nwrote: {args.out_json}")
        print(f"wrote: {args.out_md}")
        print(f"wrote: {args.out_csv}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
