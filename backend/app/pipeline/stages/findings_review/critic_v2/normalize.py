"""
critic_v2/normalize.py
-----------------------
Backward-compatible adapter that converts any finding dict
(from real 03_findings.json or fixtures) into a NormalizedFinding.

Handles all field variants seen in production findings and fixtures.
Never raises on missing fields.

evidence_quality assessment logic:
  none    — no refs, no quotes
  weak    — refs present but no block index to verify; or single-block absence claim;
            or name-heuristic signals semantic mismatch
  partial — some refs verified (in block index), some phantom; or unverified but ≥2 refs
  valid   — ≥1 ref confirmed in block index, OR ≥1 meaningful quote, OR ≥2 refs
            with a concrete fact claim (no index available)
"""
from __future__ import annotations

import re
from typing import Any, Optional

from .models import (
    EVIDENCE_NONE,
    EVIDENCE_PARTIAL,
    EVIDENCE_VALID,
    EVIDENCE_WEAK,
    NormalizedFinding,
)

# ─── Severity sets ────────────────────────────────────────────────────────────

_HIGH_SEVERITY = {"КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ", "ЭКСПЛУАТАЦИОННОЕ"}

# ─── Impact keywords ──────────────────────────────────────────────────────────

_IMPACT_KEYWORDS: dict[str, str] = {
    # Electrical / EOM
    "кабель": "construction",
    "cable": "construction",
    "монтаж": "construction",
    "закупка": "construction",
    "спецификация": "construction",
    "specification": "construction",
    "поставк": "construction",
    "комплект": "construction",
    "безопасност": "safety",
    "пожар": "safety",
    "заземлен": "safety",
    "уравнивани": "safety",
    "электробезопасн": "safety",
    "safety": "safety",
    "fire": "safety",
    "стоимост": "cost_schedule",
    "сроки": "cost_schedule",
    "переделк": "cost_schedule",
    "экономич": "cost_schedule",
    "cost": "cost_schedule",
    "schedule": "cost_schedule",
    "экспертиз": "acceptance",
    "приемк": "acceptance",
    "согласован": "acceptance",
    "стройнадзор": "acceptance",
    "предписани": "acceptance",
    "юридич": "legal",
    "эксплуатац": "operations",
    "обслуживан": "operations",
    "ремонт": "operations",
    "надежност": "reliability",
    "работоспособн": "reliability",
    "документац": "documentation",
    "норматив": "normative",
    # Structural / KJ / КЖ
    "арматур": "construction",
    "армирован": "construction",
    "защитный слой": "construction",
    "защитн.*слой": "construction",
    "защитн": "construction",         # защитный
    "бетон": "construction",
    "железобетон": "construction",
    "фундамент": "construction",
    "колонн": "construction",
    "перекрыти": "construction",
    "плит": "construction",
    "стен": "construction",
    "балк": "construction",
    "анкер": "construction",
    "сечен": "construction",          # сечение арматуры / сечение элемента
    "нагрузк": "construction",
    "ведомост": "construction",       # ведомость элементов / ведомость спецификации
    "расход": "construction",         # расход арматуры / материалов
    "позиц": "construction",          # позиция в спецификации
    "количеств": "construction",      # количество в спецификации
    "масс": "construction",           # масса элемента
    "марк": "construction",           # марка бетона / марка стали
    "class": "construction",
    "reinforcement": "construction",
    "concrete": "construction",
    "cover": "construction",
    "rebar": "construction",
}

# Category → impact axis fallback
_CAT_MAP: dict[str, str] = {
    # Electrical / EOM
    "grounding": "safety",
    "cable": "construction",
    "lighting": "operations",
    "fire_alarm": "safety",
    "specification": "construction",
    "ventilation": "operations",
    "current_transformer": "acceptance",
    "normative_refs": "acceptance",
    "documentation": "acceptance",
    # Structural / KJ / КЖ
    "cover_thickness": "construction",       # защитный слой бетона
    "spec_mismatch": "construction",         # расхождение в спецификации
    "reinforcement": "construction",         # армирование
    "concrete": "construction",              # бетон / марка бетона
    "rebar": "construction",                 # арматура
    "anchor": "construction",               # анкеровка
    "wall": "construction",
    "slab": "construction",
    "column": "construction",
    "beam": "construction",
    "foundation": "construction",
    "load": "construction",
    "section_mismatch": "construction",
    "drawing_mismatch": "construction",
    "quantity_error": "construction",       # нулевое количество / ошибка количества
    # Architecture / AR
    "opening": "construction",
    "door": "construction",
    "window": "construction",
    "facade": "construction",
    "staircase": "construction",
    "room": "construction",
    "dimension": "construction",
}

# ─── Heuristic: block name signals semantic mismatch ─────────────────────────
# If block_id name strongly suggests a different topic than the finding category
_TOPIC_MISMATCH_PAIRS: list[tuple[re.Pattern, set[str]]] = [
    # lighting table block used for grounding/safety finding
    (re.compile(r"light|свет|осв", re.IGNORECASE), {"grounding", "fire_alarm", "cable", "current_transformer"}),
    # HVAC block used for electrical finding
    (re.compile(r"hvac|вент|duct|воздух", re.IGNORECASE), {"grounding", "cable", "specification"}),
    # stamp block for technical finding
    (re.compile(r"stamp|штамп|title.?block", re.IGNORECASE), {"grounding", "cable", "fire_alarm"}),
]


def _block_name_suggests_mismatch(block_id: str, category: str) -> bool:
    """Return True if block_id name suggests it's from a different topic."""
    cat = category.lower()
    for pattern, bad_cats in _TOPIC_MISMATCH_PAIRS:
        if cat in bad_cats and pattern.search(block_id):
            return True
    return False


# ─── Evidence collection ──────────────────────────────────────────────────────

def _collect_evidence_refs(raw: dict) -> list[str]:
    """Collect block_ids from evidence[], related_block_ids, source_block_ids."""
    refs: list[str] = []
    seen: set[str] = set()

    def _add(bid: str) -> None:
        if bid and bid not in seen:
            refs.append(bid)
            seen.add(bid)

    for ev in raw.get("evidence", []):
        if isinstance(ev, dict):
            bid = ev.get("block_id") or ev.get("id")
            if bid:
                _add(str(bid))
        elif isinstance(ev, str):
            _add(ev)

    for bid in raw.get("related_block_ids", []):
        if isinstance(bid, str):
            _add(bid)

    for bid in raw.get("source_block_ids", []):
        if isinstance(bid, str):
            _add(bid)

    return refs


def _collect_evidence_quotes(raw: dict) -> list[str]:
    """Collect text snippets from evidence_text_refs and norm_quote."""
    quotes: list[str] = []
    for ref in raw.get("evidence_text_refs", []):
        if isinstance(ref, dict):
            for key in ("quote", "text", "summary"):
                v = ref.get(key)
                if v and isinstance(v, str) and len(v.strip()) > 10:
                    quotes.append(v.strip())
    nq = raw.get("norm_quote")
    if nq and isinstance(nq, str) and len(nq.strip()) > 10:
        quotes.append(nq.strip())
    return quotes


# ─── Evidence quality assessment ──────────────────────────────────────────────

def _assess_evidence_quality(
    raw: dict,
    evidence_refs: list[str],
    evidence_quotes: list[str],
    category: str,
    blocks_index: Optional[set[str]],
) -> tuple[str, list[str], list[str]]:
    """
    Determine evidence_quality, phantom_block_ids, verified_block_ids.

    Returns:
        (quality, phantom_ids, verified_ids)

    Logic:
      - no refs + no quotes → none
      - block index provided:
          * all refs phantom → none (if no quotes) or weak (if quotes)
          * some refs verified → partial (mix) or valid (all verified)
          * all refs verified → valid
      - no block index:
          * refs present but name suggests semantic mismatch → weak
          * single ref, absence claim in description → weak
          * ≥2 independent refs + concrete fact claim → valid
          * 1 ref + concrete fact + not absence → partial
          * anything else → weak
      - meaningful quotes always upgrade to at least weak
    """
    # Nothing at all
    if not evidence_refs and not evidence_quotes:
        return EVIDENCE_NONE, [], []

    phantom_ids: list[str] = []
    verified_ids: list[str] = []

    # ── With block index ──
    if blocks_index is not None:
        for ref in evidence_refs:
            if ref in blocks_index:
                verified_ids.append(ref)
            else:
                phantom_ids.append(ref)

        if not verified_ids and not evidence_quotes:
            return EVIDENCE_NONE, phantom_ids, verified_ids
        if not verified_ids and evidence_quotes:
            return EVIDENCE_WEAK, phantom_ids, verified_ids
        if phantom_ids:
            return EVIDENCE_PARTIAL, phantom_ids, verified_ids
        # All refs verified
        return EVIDENCE_VALID, phantom_ids, verified_ids

    # ── Without block index ──
    # Check for semantic mismatch by block name heuristic
    mismatch_count = sum(
        1 for ref in evidence_refs
        if _block_name_suggests_mismatch(ref, category)
    )
    if mismatch_count == len(evidence_refs) and evidence_refs and not evidence_quotes:
        return EVIDENCE_WEAK, [], []

    # Absence claim with single ref → weak (can't confirm absence from one block)
    is_absence_claim = bool(re.search(
        r"\b(отсутствует|не предусмотрен|не показан|не указан|не обозначен"
        r"|absent|missing|not found|not shown)\b",
        raw.get("description", "") + " " + raw.get("problem", ""),
        re.IGNORECASE,
    ))
    if is_absence_claim and len(evidence_refs) == 1 and not evidence_quotes:
        return EVIDENCE_WEAK, [], []

    # Has meaningful quotes → at least partial
    if evidence_quotes:
        if len(evidence_refs) >= 1:
            return EVIDENCE_VALID, [], []
        return EVIDENCE_PARTIAL, [], []

    # Multiple refs → more credible
    if len(evidence_refs) >= 3:
        return EVIDENCE_VALID, [], []
    if len(evidence_refs) >= 2:
        return EVIDENCE_PARTIAL, [], []

    # Single ref, no quotes, no absence claim → weak
    return EVIDENCE_WEAK, [], []


# ─── Other field extractors ───────────────────────────────────────────────────

def _detect_impact_area(raw: dict) -> str | None:
    category = str(raw.get("category", "")).lower()
    combined = " ".join([
        category,
        str(raw.get("risk", "")).lower(),
        str(raw.get("description", "")).lower(),
        str(raw.get("problem", "")).lower(),
    ])
    for keyword, axis in _IMPACT_KEYWORDS.items():
        if keyword in combined:
            return axis
    return _CAT_MAP.get(category)


def _get_action(raw: dict) -> str | None:
    for field in ("solution", "action_required", "action", "recommendation", "рекомендация"):
        v = raw.get(field)
        if v and isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _get_confidence(raw: dict) -> float | None:
    quality = raw.get("quality")
    if isinstance(quality, dict):
        c = quality.get("confidence")
        if c is not None:
            try:
                return float(c)
            except (TypeError, ValueError):
                pass
    norm_conf = raw.get("norm_confidence")
    if norm_conf is not None:
        try:
            return float(norm_conf)
        except (TypeError, ValueError):
            pass
    return None


# ─── Public API ───────────────────────────────────────────────────────────────

def normalize_finding(
    raw: dict[str, Any],
    blocks_index: Optional[set[str]] = None,
) -> NormalizedFinding:
    """
    Convert any finding dict to NormalizedFinding. Never raises.

    Args:
        raw: finding dict (from 03_findings.json or fixture)
        blocks_index: optional set of known block_ids from 02_blocks_analysis.json.
                      When provided, enables phantom-block detection.
    """
    fid = str(raw.get("id") or raw.get("finding_id") or "unknown")

    title = (
        raw.get("problem")
        or raw.get("title")
        or str(raw.get("description", ""))[:80]
    )
    title = str(title).strip() if title else ""

    description = str(raw.get("description") or raw.get("problem") or "").strip()
    severity = str(raw.get("severity") or "unknown").strip()
    category = str(raw.get("category") or "unknown").strip()

    evidence_refs = _collect_evidence_refs(raw)
    evidence_quotes = _collect_evidence_quotes(raw)
    impact_area = _detect_impact_area(raw)
    action_required = _get_action(raw)
    confidence = _get_confidence(raw)

    quality, phantom_ids, verified_ids = _assess_evidence_quality(
        raw, evidence_refs, evidence_quotes, category, blocks_index,
    )

    return NormalizedFinding(
        finding_id=fid,
        title=title,
        description=description,
        severity=severity,
        category=category,
        evidence_refs=evidence_refs,
        evidence_quotes=evidence_quotes,
        impact_area=impact_area,
        action_required=action_required,
        confidence=confidence,
        raw=raw,
        evidence_quality=quality,
        phantom_block_ids=phantom_ids,
        verified_block_ids=verified_ids,
    )


def normalize_findings(
    raws: list[dict[str, Any]],
    blocks_index: Optional[set[str]] = None,
) -> list[NormalizedFinding]:
    return [normalize_finding(r, blocks_index) for r in raws]
