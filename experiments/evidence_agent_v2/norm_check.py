"""EV2 норм-путь — офлайн-проверка нормы из замечания (без нейросети).

Переиспользует существующую норм-логику проекта (НЕ дублирует):
  norms.external_provider.resolve_norm_status  — статус по «грязному» коду
  norms._core.extract_norms_from_text          — извлечение кодов regex'ом
  norms.tools.norms_api.get_paragraph/semantic_search — цитата/подсказка (опц.)

ИНВАРИАНТ БЕЗОПАСНОСТИ: NormSignal.decision_hint НЕ имеет значения, ведущего к
reject. Максимум негативного влияния — soft_human. Норма «отменена/заменена» →
accept_with_flag (замечание валидно, меняется/флагуется норма), а не удаление.
"""
from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
_NORMS_TOOLS = _ROOT / "norms" / "tools"
if str(_NORMS_TOOLS) not in sys.path:
    sys.path.insert(0, str(_NORMS_TOOLS))

# decision_hint ⊂ {neutral, accept_with_flag, soft_human, none} — reject недостижим
HINT_NEUTRAL = "neutral"
HINT_ACCEPT_FLAG = "accept_with_flag"
HINT_SOFT_HUMAN = "soft_human"
HINT_NONE = "none"
_SAFE_HINTS = {HINT_NEUTRAL, HINT_ACCEPT_FLAG, HINT_SOFT_HUMAN, HINT_NONE}

# «п.7.1» / «п. 7» / «пункт 7.1», но НЕ «П» из «СП» (lookbehind на кириллицу)
_PARA_RE = re.compile(r"(?<![А-Яа-яЁё])п(?:\.|ункт)?\s*(\d+(?:\.\d+)*)", re.IGNORECASE)
# приоритет «худшего» статуса для агрегации по нескольким кодам
_STATUS_RANK = {"cancelled": 5, "replaced": 4, "outdated_edition": 3,
                "unknown": 2, "active": 1}


@dataclass
class NormSignal:
    kind: str = "none"            # norm_ok|norm_edition_flag|norm_replaced_flag|
                                  # norm_cancelled_flag|norm_not_indexed|norm_unsupported|
                                  # norm_inferred|none
    decision_hint: str = HINT_NONE
    confidence: float = 0.0
    matched_code: Optional[str] = None
    status: str = "unknown"
    replacement_doc: Optional[str] = None
    flags: list = field(default_factory=list)        # попадут в финальный finding
    suggestions: dict = field(default_factory=dict)  # для эксперта, НЕ для decision
    reason: str = ""
    codes_checked: list = field(default_factory=list)

    def __post_init__(self):
        # жёсткая гарантия инварианта на уровне типа
        if self.decision_hint not in _SAFE_HINTS:
            self.decision_hint = HINT_SOFT_HUMAN


def extract_norm_codes(finding: dict) -> tuple[list, Optional[str], bool]:
    """(codes, paragraph, inferred). Сначала из finding.norm; иначе из problem/desc."""
    from norms._core import extract_norms_from_text

    norm_str = str(finding.get("norm") or "")
    codes = extract_norms_from_text(norm_str)
    inferred = False
    if not codes:
        joined = f"{finding.get('problem') or ''} {finding.get('description') or ''}"
        codes = extract_norms_from_text(joined)
        inferred = bool(codes)
    m = _PARA_RE.search(norm_str)
    paragraph = m.group(1) if m else None
    return codes, paragraph, inferred


def _classify_status(resolved: dict, inferred: bool) -> tuple[str, str, float]:
    """(kind, decision_hint, confidence) — чистая функция (легко тестируется)."""
    found = bool(resolved.get("found"))
    status = str(resolved.get("status") or "unknown")
    reason = str(resolved.get("resolution_reason") or "")

    if found and status == "active":
        if inferred:
            return "norm_inferred", HINT_NEUTRAL, 0.2
        return "norm_ok", HINT_NEUTRAL, 0.8
    if status == "outdated_edition":
        return "norm_edition_flag", HINT_ACCEPT_FLAG, 0.7
    if status == "replaced":
        return "norm_replaced_flag", HINT_ACCEPT_FLAG, 0.7
    if status == "cancelled":
        return "norm_cancelled_flag", HINT_ACCEPT_FLAG, 0.6
    if not found and reason == "not_in_index":
        return "norm_not_indexed", HINT_SOFT_HUMAN, 0.3
    if not found and reason == "unsupported_family":
        return "norm_unsupported", HINT_NEUTRAL, 0.2
    return "none", HINT_NEUTRAL, 0.1


def run_norm_check(
    finding: dict, *, with_paragraph: bool = True, with_semantic: bool = False
) -> NormSignal:
    """Офлайн норм-проверка замечания. Никогда не возвращает reject-хинт."""
    from norms.external_provider import resolve_norm_status

    codes, paragraph, inferred = extract_norm_codes(finding)
    if not codes:
        return NormSignal(kind="none", decision_hint=HINT_NONE, confidence=0.0,
                          reason="Норма в замечании не распознана.")

    # проверяем все коды, агрегируем «худший» статус для флага
    per_code = []
    for code in codes[:5]:
        try:
            resolved = resolve_norm_status(code) or {}
        except Exception:
            resolved = {"found": False, "status": "unknown", "resolution_reason": "error"}
        kind, hint, conf = _classify_status(resolved, inferred)
        per_code.append((code, resolved, kind, hint, conf))

    # выбрать доминирующий: accept_with_flag > soft_human > neutral
    def _hint_rank(h):
        return {HINT_ACCEPT_FLAG: 3, HINT_SOFT_HUMAN: 2, HINT_NEUTRAL: 1, HINT_NONE: 0}[h]
    per_code.sort(key=lambda x: (_hint_rank(x[3]),
                                 _STATUS_RANK.get(x[1].get("status", "unknown"), 0),
                                 x[4]), reverse=True)
    code, resolved, kind, hint, conf = per_code[0]

    flags, suggestions = [], {}
    status = str(resolved.get("status") or "unknown")
    replacement = resolved.get("replacement_doc")
    if kind == "norm_edition_flag":
        flags.append("norm_outdated_edition")
        if resolved.get("current_version"):
            suggestions["current_version"] = resolved["current_version"]
    elif kind == "norm_replaced_flag":
        flags.append(f"norm_replaced:{replacement or '?'}")
        if replacement:
            suggestions["replacement_doc"] = replacement
    elif kind == "norm_cancelled_flag":
        flags.append("norm_cancelled")
    elif kind == "norm_not_indexed":
        flags.append("norm_not_in_index")

    requires_human = kind == "norm_cancelled_flag"

    # опц. цитата пункта — усиливает прозрачность (не меняет decision)
    if with_paragraph and paragraph and resolved.get("found"):
        try:
            from norms_api import get_paragraph
            para = get_paragraph(code, paragraph, max_lines=4)
            if para.get("found"):
                suggestions["paragraph_text"] = str(para.get("text", ""))[:400]
        except Exception:
            pass

    # опц. «правильная» норма по смыслу — ТОЛЬКО подсказка эксперту
    if with_semantic:
        try:
            from norms_api import semantic_search
            hits = semantic_search(str(finding.get("problem") or "")[:200], top=2)
            if hits and hits[0].get("code") and hits[0]["code"] != code:
                suggestions["suggested_norm"] = {"code": hits[0]["code"],
                                                 "score": hits[0].get("score")}
        except Exception:
            pass

    reason_map = {
        "norm_ok": f"Норма {code} действует.",
        "norm_inferred": f"Норма {code} выведена из текста (явно не указана).",
        "norm_edition_flag": f"Норма {code}: устаревшая редакция (замечание валидно).",
        "norm_replaced_flag": f"Норма {code} заменена на {replacement or '?'} (замечание валидно).",
        "norm_cancelled_flag": f"Норма {code} отменена — нужна проверка формулировки.",
        "norm_not_indexed": f"Норма {code} не в индексе — отдать эксперту.",
        "norm_unsupported": f"Семейство нормы {code} не поддерживается индексом.",
        "none": "Статус нормы не определён.",
    }
    sig = NormSignal(
        kind=kind, decision_hint=hint, confidence=conf,
        matched_code=resolved.get("matched_code") or code, status=status,
        replacement_doc=replacement, flags=flags, suggestions=suggestions,
        reason=reason_map.get(kind, ""), codes_checked=[c for c, *_ in per_code],
    )
    if requires_human:
        sig.suggestions["requires_human_review"] = True
    return sig
