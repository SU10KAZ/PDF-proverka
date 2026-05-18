"""
Tests for the experimental `assisted_round2_candidate` triage profile.

Контракт:
    * Профиль зарегистрирован: PROFILE_ASSISTED_ROUND2_CANDIDATE in VALID_PROFILES.
    * Применяет ТОЛЬКО C_v2 (RD vs PZ) и D_v2 (already_covered, strict).
    * НЕ применяет A1_v2 OCR, Rule E, Rule B, Rule F.
    * НИКОГДА не перемещает в hidden_by_critic.
    * Moved items: visible_by_default=False, collapsed_by_default=True,
      can_restore=True, queue=suggested_reject.
    * Правила НЕ читают human_decision/preferred_tab/reviewer_note/
      human_reason/triage_correct/priority.
    * C_v2: section ∈ {KJ, EOM}, >=2 independent RD/PZ signals.
    * C_v2: на AR не срабатывает; на ЭКОНОМИЧЕСКОЕ+valid+>=8 без RD-not-required
      маркера — guard блокирует.
    * D_v2: требует >=2 strong signals; одной фразы "смежный раздел" не хватает.
    * D_v2: на strong-severity+valid+>=8 без concrete place — guard блокирует.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


from backend.app.pipeline.stages.findings_review.critic_v2 import (  # noqa: E402
    PROFILE_ASSISTED_ROUND2_CANDIDATE,
    PROFILE_CONSERVATIVE,
    ROUND2_REASON_ALREADY_COVERED,
    ROUND2_REASON_RD_PZ,
    VALID_PROFILES,
    apply_round2_rules,
    build_triage_result,
    run_critic_v2_offline,
)
from backend.app.pipeline.stages.findings_review.critic_v2.triage import (  # noqa: E402
    EVIDENCE_VALID,
    EVIDENCE_WEAK,
    QUEUE_HIDDEN,
    QUEUE_MAIN_REVIEW,
    QUEUE_NEEDS_CONTEXT,
    QUEUE_STRONG_KEEP,
    QUEUE_SUGGESTED_REJECT,
    RISK_MEDIUM,
    REC_REJECT,
    TriageDecision,
    _round2_match_already_covered,
    _round2_match_rd_pz,
)


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _make_decision(
    finding_id: str = "F-001",
    queue: str = QUEUE_MAIN_REVIEW,
    evidence: str = EVIDENCE_VALID,
    score: int = 6,
    source_dependency: str = "",
    taxonomy_reason: str = None,
    risk_level: str = "low",
    explanation: str = "",
    reason: str = "",
) -> TriageDecision:
    return TriageDecision(
        finding_id=finding_id,
        human_queue=queue,
        critic_recommendation="review",
        visible_by_default=(queue not in (QUEUE_HIDDEN, QUEUE_SUGGESTED_REJECT,
                                          QUEUE_NEEDS_CONTEXT)),
        collapsed_by_default=False,
        can_restore=False,
        confidence=None,
        risk_level=risk_level,
        reason=reason,
        explanation=explanation,
        evidence_quality=evidence,
        usefulness_score=score,
        source_dependency=source_dependency,
        taxonomy_reason=taxonomy_reason,
        deterministic_decision="accept",
        llm_decision=None,
        final_decision="accept",
        was_guard_blocked=False,
        was_downgraded_reject=False,
        profile=PROFILE_ASSISTED_ROUND2_CANDIDATE,
        raw={},
    )


# ─── Profile registration ────────────────────────────────────────────────────


def test_profile_registered():
    assert PROFILE_ASSISTED_ROUND2_CANDIDATE in VALID_PROFILES
    assert PROFILE_ASSISTED_ROUND2_CANDIDATE == "assisted_round2_candidate"


def test_profile_config_exists():
    from backend.app.pipeline.stages.findings_review.critic_v2.triage import (
        get_profile_config,
    )
    cfg = get_profile_config(PROFILE_ASSISTED_ROUND2_CANDIDATE)
    assert cfg.name == PROFILE_ASSISTED_ROUND2_CANDIDATE
    assert cfg.non_production is True
    # Inherits conservative: no taxonomy-based suggested_reject expansion
    assert len(cfg.suggested_reject_taxonomies) == 0


# ─── C_v2 matcher ────────────────────────────────────────────────────────────


def test_c_v2_requires_2_signals():
    """Single RD/PZ marker is NOT enough."""
    assert _round2_match_rd_pz("EOM", "пояснительная записка содержит расчёт") is True
    # Один сигнал — false
    assert _round2_match_rd_pz("EOM", "только расчёт без других маркеров") is False
    # Совсем без сигналов — false
    assert _round2_match_rd_pz("EOM", "обычный текст") is False


def test_c_v2_section_gated_to_kj_eom():
    blob = "пояснительная записка раздела и расчётное обоснование REI 90"
    assert _round2_match_rd_pz("KJ", blob) is True
    assert _round2_match_rd_pz("EOM", blob) is True
    # AR — никогда
    assert _round2_match_rd_pz("AR", blob) is False
    # OV, VK, etc — никогда
    assert _round2_match_rd_pz("OV", blob) is False
    assert _round2_match_rd_pz(None, blob) is False


def test_c_v2_does_not_fire_on_ar_even_with_signals():
    """Spec: C_v2 не срабатывает на AR."""
    decision = _make_decision()
    finding = {
        "section": "AR",
        "description": "Пояснительная записка содержит расчёт огнестойкости REI 90 "
                       "по СП 468 — экспертиза подтвердила. Это не чертёж РД.",
    }
    result = apply_round2_rules(decision, finding)
    assert result is decision  # not moved
    assert result.human_queue == QUEUE_MAIN_REVIEW


def test_c_v2_fires_on_kj_with_2_signals():
    decision = _make_decision()
    finding = {
        "section": "KJ",
        "description": "Расчёт огнестойкости REI 90 проводится в пояснительной "
                       "записке по СП 468.",
    }
    result = apply_round2_rules(decision, finding)
    assert result.human_queue == QUEUE_SUGGESTED_REJECT
    assert result.reason == ROUND2_REASON_RD_PZ
    assert result.critic_recommendation == REC_REJECT
    assert result.visible_by_default is False
    assert result.collapsed_by_default is True
    assert result.can_restore is True
    assert result.profile == PROFILE_ASSISTED_ROUND2_CANDIDATE
    assert "C_v2_rd_pz" in result.raw["applied_round2_rules"]


def test_c_v2_guard_economic_high_score_blocks():
    """Spec §C-guard: ЭКОНОМИЧЕСКОЕ + valid + score>=8 без 'не требуется в РД'
    маркера — блокируем даже при 2 RD/PZ сигналах."""
    decision = _make_decision(evidence=EVIDENCE_VALID, score=9)
    finding = {
        "section": "EOM",
        "severity": "ЭКОНОМИЧЕСКОЕ",
        "description": "Расчёт огнестойкости REI 90 указан в пояснительной "
                       "записке по СП 468.",
    }
    result = apply_round2_rules(decision, finding)
    assert result is decision  # guard заблокировал


def test_c_v2_guard_economic_allowed_with_explicit_rd_not_required():
    """Если есть явный маркер 'не требуется в РД' — guard пропускает."""
    decision = _make_decision(evidence=EVIDENCE_VALID, score=9)
    finding = {
        "section": "EOM",
        "severity": "ЭКОНОМИЧЕСКОЕ",
        "description": "Расчёт огнестойкости REI 90 указан в пояснительной "
                       "записке — не требуется в РД, есть в экспертизе.",
    }
    result = apply_round2_rules(decision, finding)
    assert result.human_queue == QUEUE_SUGGESTED_REJECT


def test_c_v2_guard_spec_mismatch_with_volume_blocks():
    """spec_mismatch с объёмом/стоимостью — блокируем."""
    decision = _make_decision()
    finding = {
        "section": "EOM",
        "category": "spec_mismatch",
        "description": "Расчёт REI 90 — пояснительная записка показывает другой объём, "
                       "стоимость превышает заявленную.",
    }
    result = apply_round2_rules(decision, finding)
    assert result is decision


# ─── D_v2 matcher ────────────────────────────────────────────────────────────


def test_d_v2_requires_2_strong_signals():
    """Один сигнал — false."""
    # Только taxonomy — недостаточно
    assert _round2_match_already_covered(
        "обычный текст без явных маркеров",
        "duplicate_or_already_covered",
        None, ""
    ) is False
    # Только source_dependency — недостаточно
    assert _round2_match_already_covered(
        "обычный текст",
        None,
        "enough_source", ""
    ) is False
    # Одна explicit-фраза — недостаточно
    assert _round2_match_already_covered(
        "это уже указано в проекте",
        None, None, ""
    ) is False


def test_d_v2_fires_with_taxonomy_plus_phrase():
    """taxonomy + explicit phrase = 2 signals."""
    assert _round2_match_already_covered(
        "это уже указано в спецификации",
        "duplicate_or_already_covered",
        None, ""
    ) is True


def test_d_v2_fires_with_taxonomy_plus_concrete_place_in_explanation():
    """taxonomy + concrete-place в explanation + есть adjacency hint в blob."""
    # taxonomy + явная phrase + concrete place (S1+S3+S4) = 3 signals
    assert _round2_match_already_covered(
        "уже указано в таблице 5 спецификации",
        "duplicate_or_already_covered",
        None, "См. лист 12, таблица материалов"
    ) is True


def test_d_v2_does_not_fire_on_vague_adjacent_only():
    """Spec: 'смежный раздел' без конкретики не считается."""
    assert _round2_match_already_covered(
        "указано в смежном разделе",  # один vague hint
        None, None, ""
    ) is False


def test_d_v2_does_not_fire_with_just_source_dep_and_vague_phrase():
    """source_dependency + vague 'смежный' — недостаточно."""
    assert _round2_match_already_covered(
        "это в смежном разделе",
        None,
        "enough_source", ""
    ) is False  # 1 signal (enough_source) + vague adjacent (no count)


def test_d_v2_full_match_moves_to_suggested_reject():
    decision = _make_decision(source_dependency="enough_source")
    finding = {
        "section": "KJ",
        "taxonomy_reason": "duplicate_or_already_covered",
        "description": "Это уже указано в спецификации на листе 5.",
    }
    result = apply_round2_rules(decision, finding)
    assert result.human_queue == QUEUE_SUGGESTED_REJECT
    assert result.reason == ROUND2_REASON_ALREADY_COVERED
    assert result.can_restore is True
    assert "D_v2_already_covered" in result.raw["applied_round2_rules"]


def test_d_v2_guard_strong_severity_without_concrete_place_blocks():
    """Spec §D-guard: КРИТИЧЕСКОЕ+valid+>=8 без concrete place — guard."""
    decision = _make_decision(evidence=EVIDENCE_VALID, score=9,
                              source_dependency="enough_source")
    finding = {
        "section": "EOM",
        "severity": "КРИТИЧЕСКОЕ",
        "taxonomy_reason": "duplicate_or_already_covered",
        # есть taxonomy + enough_source — 2 signals, но без concrete place
        "description": "Это уже описано где-то.",
    }
    result = apply_round2_rules(decision, finding)
    assert result is decision


def test_d_v2_guard_strong_severity_passes_with_concrete_spec_marker():
    """Если есть упоминание spec/таблицы/общих указаний — guard пропускает."""
    decision = _make_decision(evidence=EVIDENCE_VALID, score=9,
                              source_dependency="enough_source")
    finding = {
        "section": "EOM",
        "severity": "КРИТИЧЕСКОЕ",
        "taxonomy_reason": "duplicate_or_already_covered",
        "description": "Это уже указано в спецификации на листе 5, "
                       "и в общих указаниях.",
    }
    result = apply_round2_rules(decision, finding)
    assert result.human_queue == QUEUE_SUGGESTED_REJECT


# ─── Never moves to hidden_by_critic ─────────────────────────────────────────


def test_round2_never_emits_hidden():
    """Spec §3: round2 никогда не перемещает в hidden_by_critic."""
    decision = _make_decision()
    finding = {
        "section": "EOM",
        "taxonomy_reason": "duplicate_or_already_covered",
        "description": "Расчёт REI указан в пояснительной записке. "
                       "Уже есть в спецификации.",
    }
    result = apply_round2_rules(decision, finding)
    assert result.human_queue != QUEUE_HIDDEN
    assert result.human_queue == QUEUE_SUGGESTED_REJECT


def test_round2_does_not_touch_hidden_items():
    """Items уже в hidden — не трогаем."""
    decision = _make_decision(queue=QUEUE_HIDDEN)
    finding = {"section": "KJ", "description": "Расчёт REI пояснительная записка"}
    result = apply_round2_rules(decision, finding)
    assert result is decision  # unchanged


def test_round2_does_not_touch_suggested_reject_items():
    """Already in suggested_reject — leave alone."""
    decision = _make_decision(queue=QUEUE_SUGGESTED_REJECT)
    finding = {"section": "KJ", "description": "Расчёт REI пояснительная записка"}
    result = apply_round2_rules(decision, finding)
    assert result is decision


def test_round2_does_not_touch_needs_context_items():
    decision = _make_decision(queue=QUEUE_NEEDS_CONTEXT)
    finding = {"section": "KJ", "description": "Расчёт REI пояснительная записка"}
    result = apply_round2_rules(decision, finding)
    assert result is decision


# ─── Strong-keep extra guard ─────────────────────────────────────────────────


def test_round2_strong_keep_guard_c_only_blocks():
    """C_v2-only hit on critical strong_keep with valid+>=8 → block."""
    decision = _make_decision(queue=QUEUE_STRONG_KEEP,
                              evidence=EVIDENCE_VALID, score=10)
    finding = {
        "section": "KJ",
        "severity": "КРИТИЧЕСКОЕ",
        "description": "Расчёт REI 90 в пояснительной записке — не требуется в РД",
    }
    result = apply_round2_rules(decision, finding)
    assert result is decision  # strong-keep guard


def test_round2_strong_keep_d_passes():
    """D_v2 hit on strong_keep — guard пропускает (D разрешён в guard)."""
    decision = _make_decision(queue=QUEUE_STRONG_KEEP,
                              evidence=EVIDENCE_VALID, score=10,
                              source_dependency="enough_source")
    finding = {
        "section": "KJ",
        "severity": "КРИТИЧЕСКОЕ",
        "taxonomy_reason": "duplicate_or_already_covered",
        "description": "Уже указано в спецификации на листе 5, общие указания.",
    }
    result = apply_round2_rules(decision, finding)
    assert result.human_queue == QUEUE_SUGGESTED_REJECT


# ─── Purity: no human_* fields read ──────────────────────────────────────────


class _SpyDict(dict):
    """Dict that records all .get() / [] reads — tests can assert forbidden
    fields are NOT accessed."""
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self._read_keys: set[str] = set()

    def __getitem__(self, k):
        self._read_keys.add(k)
        return super().__getitem__(k)

    def get(self, k, default=None):
        self._read_keys.add(k)
        return super().get(k, default)


def test_round2_does_not_read_human_decision_fields():
    """Spec: round2 НЕ читает human_decision/preferred_tab/reviewer_note/
    human_reason/triage_correct/priority."""
    decision = _make_decision()
    finding = _SpyDict({
        "section": "KJ",
        "description": "Расчёт REI указан в пояснительной записке СП 468.",
        # Эти поля — runtime/human, читать запрещено
        "human_decision": "accepted",
        "preferred_tab": "primary",
        "reviewer_note": "должен быть оставлен",
        "human_reason": "конкретное объяснение",
        "triage_correct": "no",
        "priority": "high",
    })
    apply_round2_rules(decision, finding)
    forbidden = {"human_decision", "preferred_tab", "reviewer_note",
                 "human_reason", "triage_correct", "priority"}
    leaked = finding._read_keys & forbidden
    assert not leaked, f"Round2 read forbidden runtime fields: {leaked}"


def test_round2_only_reads_pre_review_fields():
    """Affirmative: only the documented pre-review fields are read."""
    decision = _make_decision()
    finding = _SpyDict({
        "section": "KJ",
        "description": "Расчёт REI указан в пояснительной записке СП 468.",
    })
    apply_round2_rules(decision, finding)
    allowed = {"title", "description", "recommendation", "sub_problem",
               "explanation", "section", "discipline", "severity", "category",
               "taxonomy_reason"}
    out_of_contract = finding._read_keys - allowed
    assert not out_of_contract, (
        f"Round2 accessed undocumented fields: {out_of_contract}"
    )


# ─── Integration with build_triage_result ────────────────────────────────────


def test_build_triage_result_applies_round2_under_profile():
    """End-to-end: build_triage_result with round2 profile applies post-rules."""
    findings = [
        {
            "id": "F-001",
            "section": "EOM",
            "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
            "title": "Огнестойкость не указана",
            "description": "Расчёт REI 90 находится в пояснительной записке "
                           "по СП 468, не требуется в РД.",
            "recommendation": "Проверить через ПЗ.",
            "evidence": [{"type": "block_reference", "block_id": "B-1",
                          "page": 1, "text": "REI 90"}],
            "related_block_ids": ["B-1"],
            "impact_area": "safety",
            "has_evidence": True, "has_action": True, "has_impact": True,
        },
        {
            "id": "F-002",
            "section": "AR",  # AR — C_v2 не должен сработать
            "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
            "title": "Расчёт REI",
            "description": "Расчёт REI 90 в пояснительной записке по СП 468.",
            "recommendation": "Проверить.",
            "evidence": [{"type": "block_reference", "block_id": "B-2",
                          "page": 1, "text": "AR"}],
            "related_block_ids": ["B-2"],
            "impact_area": "construction",
            "has_evidence": True, "has_action": True, "has_impact": True,
        },
    ]
    result = run_critic_v2_offline(findings, blocks_index={"B-1", "B-2"})
    triage = build_triage_result(
        findings, result.decisions, llm_decisions=None,
        profile=PROFILE_ASSISTED_ROUND2_CANDIDATE,
    )
    by_id = {td.finding_id: td for td in triage}
    # F-001 (EOM, 2 signals) → suggested_reject
    assert by_id["F-001"].human_queue == QUEUE_SUGGESTED_REJECT
    assert by_id["F-001"].reason == ROUND2_REASON_RD_PZ
    # F-002 (AR) → НЕ suggested_reject, потому что AR гейтит C_v2
    assert by_id["F-002"].human_queue != QUEUE_SUGGESTED_REJECT


def test_build_triage_result_conservative_unaffected_by_round2_code():
    """Conservative профиль не должен затрагиваться round2 кодом."""
    findings = [{
        "id": "F-001",
        "section": "EOM",
        "severity": "РЕКОМЕНДАТЕЛЬНОЕ",
        "title": "X",
        "description": "Расчёт REI 90 в пояснительной записке по СП 468.",
        "recommendation": "Проверить.",
        "evidence": [{"type": "block_reference", "block_id": "B-1",
                      "page": 1, "text": "X"}],
        "related_block_ids": ["B-1"],
        "impact_area": "safety",
        "has_evidence": True, "has_action": True, "has_impact": True,
    }]
    result = run_critic_v2_offline(findings, blocks_index={"B-1"})
    triage_cons = build_triage_result(
        findings, result.decisions, llm_decisions=None,
        profile=PROFILE_CONSERVATIVE,
    )
    # Conservative не должен ставить в suggested_reject из-за RD/PZ маркеров
    assert triage_cons[0].human_queue != QUEUE_SUGGESTED_REJECT


# ─── Moved item flags ─────────────────────────────────────────────────────────


def test_moved_item_has_correct_visibility_flags():
    decision = _make_decision()
    finding = {
        "section": "KJ",
        "description": "Расчёт REI 90 в пояснительной записке СП 468.",
    }
    result = apply_round2_rules(decision, finding)
    assert result.human_queue == QUEUE_SUGGESTED_REJECT
    assert result.visible_by_default is False
    assert result.collapsed_by_default is True
    assert result.can_restore is True


def test_moved_item_risk_not_lowered():
    """Spec §4: risk_level не понижается автоматически."""
    decision = _make_decision(risk_level="high")
    finding = {
        "section": "KJ",
        "description": "Расчёт REI 90 в пояснительной записке СП 468.",
    }
    result = apply_round2_rules(decision, finding)
    assert result.risk_level == "high"  # preserved


def test_moved_item_low_risk_bumped_to_medium():
    """low → medium (rule downgrade carries minimum medium risk for reviewer)."""
    decision = _make_decision(risk_level="low")
    finding = {
        "section": "KJ",
        "description": "Расчёт REI 90 в пояснительной записке СП 468.",
    }
    result = apply_round2_rules(decision, finding)
    assert result.risk_level == RISK_MEDIUM


# ─── Raw debug payload ───────────────────────────────────────────────────────


def test_moved_item_records_raw_debug_payload():
    decision = _make_decision(reason="prev_reason", explanation="prev_explanation")
    finding = {
        "section": "KJ",
        "description": "Расчёт REI 90 в пояснительной записке СП 468.",
    }
    result = apply_round2_rules(decision, finding)
    assert result.raw.get("applied_round2_rules")
    assert result.raw.get("round2_rule_reason") == ROUND2_REASON_RD_PZ
    assert result.raw.get("round2_pre_queue") == QUEUE_MAIN_REVIEW
    assert result.raw.get("round2_pre_reason") == "prev_reason"
    assert "round2_rules=" in result.explanation
