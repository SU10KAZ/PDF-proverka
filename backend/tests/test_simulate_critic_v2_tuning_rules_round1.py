"""
Tests for backend/scripts/simulate_critic_v2_tuning_rules_round1.py.

Главное ограничение: candidate rules ОБЯЗАНЫ использовать только pre-review
признаки (title/description/recommendation, section, reason, risk_level,
taxonomy_reason, source_dependency, original_tab, evidence_quality, score).
human_decision и preferred_tab — только labels для подсчёта метрик.

Этот файл проверяет:
  - правила не читают LABEL_ONLY поля;
  - каждое правило срабатывает на ожидаемом тексте;
  - метрики считаются;
  - production не тронут.
"""
from __future__ import annotations

import importlib.util
import inspect
import json
import re
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


@pytest.fixture(scope="module")
def mod():
    p = _ROOT / "backend" / "scripts" / "simulate_critic_v2_tuning_rules_round1.py"
    spec = importlib.util.spec_from_file_location("sim_round1", p)
    assert spec and spec.loader
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


# ──────────────────────────────────────────────────────────────────────────────
# Black-box safety: rules must not read LABEL_ONLY fields.
# We assert via a "spying" item that raises if a forbidden key is read.
# ──────────────────────────────────────────────────────────────────────────────

class SpyItem(dict):
    """Dict that screams when any forbidden key is accessed."""

    FORBIDDEN = ("human_decision", "human_reason", "preferred_tab",
                 "triage_correct", "reviewer_note", "priority")

    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self.forbidden_reads = []

    def get(self, key, default=None):
        if key in self.FORBIDDEN:
            self.forbidden_reads.append(key)
        return super().get(key, default)

    def __getitem__(self, key):
        if key in self.FORBIDDEN:
            self.forbidden_reads.append(key)
        return super().__getitem__(key)


def _spy(**fields) -> SpyItem:
    base = SpyItem({
        "title": "", "description": "", "recommendation": "",
        "section": "AR", "reason": None, "risk_level": None,
        "taxonomy_reason": None, "source_dependency": None,
        "original_tab": "primary", "evidence_quality": "valid",
        "score": 8, "queue": "strong_keep",
        # Forbidden fields with non-trivial values — if a rule reads them, we'll know.
        "human_decision": "accepted",
        "human_reason": "DO_NOT_READ",
        "preferred_tab": "hidden_by_critic",
        "triage_correct": "no",
        "reviewer_note": "DO_NOT_READ",
        "priority": "critical",
    })
    base.update(fields)
    base.forbidden_reads = []
    return base


@pytest.mark.parametrize("rule_name", [
    "A1_ocr_to_reject", "A2_ocr_to_hidden", "B_ar_auxiliary",
    "C_rd_pz", "D_already_covered", "E_secondary_gate",
    "F_needs_context_recalibrate",
])
def test_rule_does_not_read_label_fields(mod, rule_name):
    """All candidate rules must avoid LABEL_ONLY fields at decision time."""
    rule = mod.RULES[rule_name]
    # Vary section so the section-aware branches trigger too.
    for section in ("AR", "KJ", "EOM"):
        for reason in (None, "deterministic_accept_high_score", "needs_context"):
            item = _spy(section=section, reason=reason,
                        risk_level="low", taxonomy_reason="other",
                        original_tab="primary",
                        title="ABC",
                        description="random text",
                        recommendation="do something")
            rule(item)
            assert item.forbidden_reads == [], (
                f"{rule_name} read forbidden fields: {item.forbidden_reads}"
            )


def test_runtime_features_whitelist_documented(mod):
    """Sanity: the script declares a runtime whitelist and a label-only set."""
    assert hasattr(mod, "RUNTIME_FEATURES")
    assert hasattr(mod, "LABEL_ONLY")
    # LABEL_ONLY must not leak into RUNTIME_FEATURES
    assert not (mod.RUNTIME_FEATURES & mod.LABEL_ONLY)


# ──────────────────────────────────────────────────────────────────────────────
# Rule behaviour
# ──────────────────────────────────────────────────────────────────────────────


def test_A1_ocr_matches_explicit_marker(mod):
    it = {"title": "Это OCR мусор и нераспознанные обозначения",
          "description": "", "recommendation": "",
          "section": "AR", "original_tab": "primary"}
    d = mod.RULES["A1_ocr_to_reject"](it)
    assert d["matched"] is True
    assert d["predicted_tab"] == "suggested_reject"


def test_A1_broken_token_regex(mod):
    it = {"title": "Маркировка А-А-А-А непонятна",
          "description": "", "recommendation": "",
          "section": "AR", "original_tab": "primary"}
    d = mod.RULES["A1_ocr_to_reject"](it)
    assert d["matched"] is True


def test_A1_does_not_trigger_for_clean_text(mod):
    it = {"title": "Не указан класс пожарной опасности",
          "description": "СП 1.13130 п. 4.3",
          "recommendation": "", "section": "AR",
          "original_tab": "primary"}
    d = mod.RULES["A1_ocr_to_reject"](it)
    assert d["matched"] is False


def test_B_ar_auxiliary_skipped_for_non_ar(mod):
    it = {"title": "вспомогательная схема", "description": "",
          "recommendation": "", "section": "KJ",
          "original_tab": "primary"}
    d = mod.RULES["B_ar_auxiliary"](it)
    assert d["matched"] is False


def test_B_ar_auxiliary_blocked_by_strong_impact(mod):
    it = {"title": "вспомогательная схема для путей эвакуации",
          "description": "", "recommendation": "", "section": "AR",
          "original_tab": "primary"}
    d = mod.RULES["B_ar_auxiliary"](it)
    assert d["matched"] is False


def test_B_ar_auxiliary_triggers(mod):
    it = {"title": "Вспомогательная схема расположения",
          "description": "", "recommendation": "", "section": "AR",
          "original_tab": "primary"}
    d = mod.RULES["B_ar_auxiliary"](it)
    assert d["matched"] is True
    assert d["predicted_tab"] == "suggested_reject"


def test_C_rd_pz_matches_in_KJ(mod):
    it = {"title": "REI 150 не указан в общих указаниях",
          "description": "", "recommendation": "", "section": "KJ",
          "original_tab": "primary"}
    d = mod.RULES["C_rd_pz"](it)
    assert d["matched"] is True
    assert d["predicted_tab"] == "suggested_reject"


def test_C_rd_pz_matches_in_EOM(mod):
    it = {"title": "Расчётное обоснование отсутствует",
          "description": "ПЗ раздела ЭОМ", "recommendation": "",
          "section": "EOM", "original_tab": "primary"}
    d = mod.RULES["C_rd_pz"](it)
    assert d["matched"] is True


def test_C_rd_pz_does_not_match_AR(mod):
    it = {"title": "REI 150", "description": "", "recommendation": "",
          "section": "AR", "original_tab": "primary"}
    d = mod.RULES["C_rd_pz"](it)
    assert d["matched"] is False


def test_D_already_covered(mod):
    it = {"title": "Параметры АВ присутствуют в смежном разделе — дублирование не требуется",
          "description": "", "recommendation": "", "section": "EOM",
          "original_tab": "primary"}
    d = mod.RULES["D_already_covered"](it)
    assert d["matched"] is True
    assert d["predicted_tab"] == "suggested_reject"


def test_E_secondary_gate_requires_all_metadata(mod):
    """E срабатывает только при выполнении всех 4 условий + текстовой категории."""
    base = {"title": "Документация: ссылка на СП 256",
            "description": "Замечание по документации",
            "recommendation": "", "section": "AR",
            "original_tab": "primary",
            "reason": "deterministic_accept_high_score",
            "risk_level": "low",
            "taxonomy_reason": "other"}
    d = mod.RULES["E_secondary_gate"](base)
    assert d["matched"] is True
    assert d["predicted_tab"] == "suggested_reject"

    # Wrong risk_level breaks it
    base2 = dict(base, risk_level="high")
    assert mod.RULES["E_secondary_gate"](base2)["matched"] is False

    # Wrong reason breaks it
    base3 = dict(base, reason="borderline")
    assert mod.RULES["E_secondary_gate"](base3)["matched"] is False

    # Wrong taxonomy breaks it
    base4 = dict(base, taxonomy_reason="insufficient_source_context")
    assert mod.RULES["E_secondary_gate"](base4)["matched"] is False


def test_E_does_not_use_human_decision(mod):
    """E работает без human_decision (даже если accepted/rejected)."""
    args = {"title": "СП 256: ссылка на нормативные документы",
            "description": "", "recommendation": "", "section": "AR",
            "original_tab": "primary",
            "reason": "deterministic_accept_high_score",
            "risk_level": "low", "taxonomy_reason": "other"}
    d_no_label = mod.RULES["E_secondary_gate"](args)
    assert d_no_label["matched"] is True
    d_with_label = mod.RULES["E_secondary_gate"](
        dict(args, human_decision="accepted")
    )
    assert d_with_label == d_no_label, "human_decision should not affect rule"


def test_F_needs_context_recalibrate_to_reject(mod):
    it = {"title": "Параметры в смежном разделе",
          "description": "уже указаны в разделе",
          "recommendation": "",
          "section": "AR", "original_tab": "needs_context",
          "source_dependency": "cross_section_required",
          "taxonomy_reason": "insufficient_source_context"}
    d = mod.RULES["F_needs_context_recalibrate"](it)
    assert d["matched"] is True
    assert d["predicted_tab"] == "suggested_reject"


def test_F_needs_context_recalibrate_to_primary(mod):
    it = {"title": "Влияет на безопасность",
          "description": "оставить в основной", "recommendation": "",
          "section": "AR", "original_tab": "needs_context",
          "source_dependency": "cross_section_required",
          "taxonomy_reason": "insufficient_source_context"}
    d = mod.RULES["F_needs_context_recalibrate"](it)
    assert d["matched"] is True
    assert d["predicted_tab"] == "primary"


def test_F_no_match_without_full_metadata(mod):
    it = {"title": "уже есть в смежном", "description": "",
          "recommendation": "", "section": "AR",
          "original_tab": "primary"}
    assert mod.RULES["F_needs_context_recalibrate"](it)["matched"] is False


# ──────────────────────────────────────────────────────────────────────────────
# Aggregate / combination metrics
# ──────────────────────────────────────────────────────────────────────────────


def _items_set():
    """Tiny but representative dataset for end-to-end metric checks."""
    return [
        # OCR garbage, reviewer agrees → suggested_reject
        {"finding_id": "P1:F1", "project_name": "P1", "section": "AR",
         "title": "ABC OCR мусор", "description": "", "recommendation": "",
         "original_tab": "primary", "reason": "deterministic_accept_high_score",
         "risk_level": "low", "taxonomy_reason": "other",
         "evidence_quality": "valid", "score": 9,
         "human_decision": "rejected", "preferred_tab": "suggested_reject",
         "triage_correct": "no", "priority": "normal"},
        # RD vs PZ in KJ, reviewer agrees
        {"finding_id": "P2:F1", "project_name": "P2", "section": "KJ",
         "title": "REI 150 не указан", "description": "ПЗ", "recommendation": "",
         "original_tab": "primary", "reason": "deterministic_accept_high_score",
         "risk_level": "low", "taxonomy_reason": "other",
         "evidence_quality": "valid", "score": 10,
         "human_decision": "rejected", "preferred_tab": "suggested_reject",
         "triage_correct": "no", "priority": "normal"},
        # Already covered note from EOM
        {"finding_id": "P3:F1", "project_name": "P3", "section": "EOM",
         "title": "присутствует в смежном разделе", "description": "",
         "recommendation": "", "original_tab": "primary",
         "reason": "deterministic_accept_high_score",
         "risk_level": "low", "taxonomy_reason": "duplicate_or_already_covered",
         "evidence_quality": "valid", "score": 10,
         "human_decision": "rejected", "preferred_tab": "suggested_reject",
         "triage_correct": "no", "priority": "normal"},
        # Clean primary, reviewer keeps as is — rules should not touch
        {"finding_id": "P4:F1", "project_name": "P4", "section": "AR",
         "title": "Не указан класс пожарной опасности по ФЗ-123",
         "description": "Влияет на эвакуацию", "recommendation": "",
         "original_tab": "primary", "reason": "deterministic_accept_high_score",
         "risk_level": "high", "taxonomy_reason": "other",
         "evidence_quality": "valid", "score": 11,
         "human_decision": "accepted", "preferred_tab": "primary",
         "triage_correct": "yes", "priority": "critical"},
    ]


def test_evaluate_rule_counts_matches(mod):
    items = _items_set()
    r = mod.evaluate_rule(items, "A1", mod.RULES["A1_ocr_to_reject"])
    assert r["matched_items"] == 1
    # Reviewer's preferred for P1:F1 is suggested_reject → correct.
    assert r["correct_direction_count"] == 1
    assert r["wrong_direction_count"] == 0
    assert r["accepted_items_affected"] == 0
    assert r["rejected_items_affected"] == 1


def test_evaluate_combination_metrics(mod):
    items = _items_set()
    rules = [("A1", mod.RULES["A1_ocr_to_reject"]),
             ("C", mod.RULES["C_rd_pz"]),
             ("D", mod.RULES["D_already_covered"])]
    metrics, rows = mod.evaluate_combination(items, "test_combo",
                                              rules_list=rules)
    # 3 items move out of primary, 1 stays.
    assert metrics["primary_queue_reduction_delta"] == 3
    assert metrics["human_rejected_moved_out_of_primary"] == 3
    assert metrics["human_accepted_moved_to_collapsed"] == 0
    # Correct moves: all 3 matched reviewers' preferred_tab.
    assert metrics["correct_moves_vs_preferred"] == 3
    assert metrics["wrong_moves_vs_preferred"] == 0
    # CSV rows recorded for every moved item.
    assert len(rows) == 3


def test_combine_uses_most_restrictive_tab(mod):
    """If two rules fire, the more restrictive predicted_tab wins."""
    it = {"title": "Это OCR мусор",  # triggers A1 (suggested_reject)
          "description": "", "recommendation": "",
          "section": "AR", "original_tab": "primary",
          "reason": None, "risk_level": None, "taxonomy_reason": None}
    # A2 would route to hidden_by_critic — more restrictive.
    rules = [("A1", mod.RULES["A1_ocr_to_reject"]),
             ("A2", mod.RULES["A2_ocr_to_hidden"])]
    final, trig = mod.combine(rules, it)
    assert final == "hidden_by_critic"
    assert set(trig) == {"A1", "A2"}


def test_section_aware_combo_uses_different_rules(mod):
    items = _items_set()
    metrics, rows = mod.section_aware(items, {
        "AR": ["A1_ocr_to_reject", "B_ar_auxiliary"],
        "KJ": ["C_rd_pz", "D_already_covered"],
        "EOM": ["C_rd_pz", "D_already_covered"],
    })
    # P1 (AR, OCR) → A1 fires.
    # P2 (KJ, RD) → C fires.
    # P3 (EOM, already covered) → D fires.
    # P4 (AR, clean fire safety) → nothing fires.
    assert metrics["moved_primary_out"] == 3
    assert metrics["human_accepted_moved_to_collapsed"] == 0


# ──────────────────────────────────────────────────────────────────────────────
# Artifact writers
# ──────────────────────────────────────────────────────────────────────────────


def test_csv_written(mod, tmp_path):
    items = _items_set()
    rules = [("A1", mod.RULES["A1_ocr_to_reject"])]
    _, rows = mod.evaluate_combination(items, "c", rules_list=rules)
    out = tmp_path / "hits.csv"
    mod.write_csv(rows, out)
    text = out.read_text(encoding="utf-8")
    assert "combo,finding_id" in text
    assert "P1:F1" in text


def test_markdown_written(mod, tmp_path):
    items = _items_set()
    per_rule = {}
    for n, fn in mod.RULES.items():
        per_rule[n] = mod.evaluate_rule(items, n, fn)
    rules = [("A1", mod.RULES["A1_ocr_to_reject"])]
    m, _ = mod.evaluate_combination(items, "c", rules_list=rules)
    rep = {"items_total": len(items), "per_rule": per_rule,
           "combos": [{"name": "c", "rules": ["A1"], "metrics": m}],
           "recommendations": ["test rec"]}
    out = tmp_path / "out.md"
    mod.write_markdown(rep, out)
    text = out.read_text(encoding="utf-8")
    assert "Tuning Rules Simulation" in text
    assert "Per-rule metrics" in text
    assert "Combinations" in text


# ──────────────────────────────────────────────────────────────────────────────
# Production safety
# ──────────────────────────────────────────────────────────────────────────────


def test_no_llm_or_network_imports():
    src = (_ROOT / "backend" / "scripts"
           / "simulate_critic_v2_tuning_rules_round1.py").read_text(encoding="utf-8")
    for forbidden in ("anthropic", "openai", "requests.", "httpx.",
                      "urllib.request", "lmstudio"):
        assert forbidden not in src, f"forbidden token: {forbidden}"


def test_no_pipeline_imports():
    src = (_ROOT / "backend" / "scripts"
           / "simulate_critic_v2_tuning_rules_round1.py").read_text(encoding="utf-8")
    for forbidden in ("from backend.app.pipeline",
                      "import backend.app.pipeline"):
        assert forbidden not in src, f"forbidden pipeline import: {forbidden}"


def test_write_targets_whitelisted():
    src = (_ROOT / "backend" / "scripts"
           / "simulate_critic_v2_tuning_rules_round1.py").read_text(encoding="utf-8")
    write_text_targets = set(re.findall(r"(\w+)\.write_text\(", src))
    assert write_text_targets.issubset({"path", "Path"}), (
        f"unexpected write_text targets: {write_text_targets}"
    )


def test_critic_v2_logic_not_modified_in_this_pass():
    """Belt-and-braces: simulation script must not contain edit calls
    against critic_v2 modules."""
    src = (_ROOT / "backend" / "scripts"
           / "simulate_critic_v2_tuning_rules_round1.py").read_text(encoding="utf-8")
    forbidden_paths = [
        "stages/findings_review",
        "pipeline/manager.py",
        "03_findings_review.json",
        "rule_filter.py", "scorer.py", "triage.py", "llm_gate.py",
    ]
    # The simulation must not touch these production paths even via strings.
    # (It may mention them in commentary, so we only forbid path-writes.)
    write_text_calls = re.findall(r"\.write_text\(([^)]*)\)", src)
    for call in write_text_calls:
        for token in forbidden_paths:
            assert token not in call, (
                f"forbidden production path appears in write_text: {call!r}"
            )
