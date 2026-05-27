"""Document-type detection for Stage 01 / completeness lens routing.

Detection priority (first wins):
  1. Explicit `project_info["document_type"]` — if value is one of the 4
     supported types, return it with confidence 1.0. If value is unknown,
     ignore and fall through.
  2. Section heuristic — `project_info["section"]` mapped to a likely
     document_type (e.g. section="ТЗ" → tz_vs_rd). Confidence 0.85.
  3. Filename / PDF basename regex on `project_info["pdf_file"]` or
     `project_info["name"]`. Confidence 0.80.
  4. MD-content regex on the supplied md_text. Confidence 0.75.
  5. Fallback: ("full_rd", 0.5).

The four supported types:
  - full_rd            — full Rabochaya Dokumentatsiya / РД
  - audit_comparison   — fragment-by-fragment comparison between two RD sections
  - tz_vs_rd           — ТЗ requirements juxtaposed against РД solutions
  - specification_only — spec / ведомость / single isolated calculation

Public API:
    detect_document_type(project_info: dict, md_text: str | None = None)
        -> tuple[str, float]

Pure stdlib. Python 3.11+. No LLM, no network.

This file mirrors the rules described in
  production_preparation/schemas/document_type_design.md
and the prompt rules in
  algorithm_research/prompt_optimization/optimized_prompts_v2/completeness.md
(lines 24–58).
"""
from __future__ import annotations

import re
from typing import Optional

# Set of valid document_type values.
ALLOWED: frozenset[str] = frozenset({
    "full_rd",
    "audit_comparison",
    "tz_vs_rd",
    "specification_only",
})

DEFAULT_TYPE: str = "full_rd"
DEFAULT_CONFIDENCE: float = 0.5

# Confidence rung per detection rule.
CONF_EXPLICIT: float = 1.0
CONF_SECTION: float = 0.85
CONF_FILENAME: float = 0.80
CONF_CONTENT: float = 0.75

# Minimum confidence required to ACCEPT a non-default detection.
# Below this, the caller should consider the answer ambiguous (the default
# rules already return >= 0.5 so the fallback is the only sub-threshold case).
ACCEPT_THRESHOLD: float = 0.7


# ---------------------------------------------------------------------------
# Section heuristic.
# ---------------------------------------------------------------------------

# Map from project_info["section"] (case-insensitive, partial-match) to
# document_type. First match wins.
SECTION_HINTS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bТЗ\s*vs\s*РД\b", re.IGNORECASE), "tz_vs_rd"),
    (re.compile(r"\bТЗ\b", re.IGNORECASE), "tz_vs_rd"),
    (re.compile(r"\b(сравнен|cross|кросс)", re.IGNORECASE), "audit_comparison"),
    (re.compile(r"\b(спецификац|specification|ведомост)", re.IGNORECASE),
     "specification_only"),
]


# ---------------------------------------------------------------------------
# Filename regex heuristic.
# ---------------------------------------------------------------------------

FILENAME_HINTS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"(?:^|[_\-\s])tz[_\-\s]*vs[_\-\s]*rd", re.IGNORECASE), "tz_vs_rd"),
    (re.compile(r"(?:^|[_\-\s])tz(?:[_\-\s]|\.)", re.IGNORECASE), "tz_vs_rd"),
    (re.compile(r"(?:^|[_\-\s])(?:cross|сравн|comparison)", re.IGNORECASE),
     "audit_comparison"),
    (re.compile(r"(?:^|[_\-\s])(?:spec|specification|ведомость|specifikatsiya)",
                re.IGNORECASE), "specification_only"),
]


# ---------------------------------------------------------------------------
# MD-content regex heuristic.
# ---------------------------------------------------------------------------

# Compiled once. Each pattern set scores a document_type by counting hits;
# the type with the highest total score (above a small floor) wins.
_CONTENT_PATTERNS: dict[str, list[re.Pattern[str]]] = {
    "tz_vs_rd": [
        re.compile(r"\bтехническ(?:ое|им)\s+задани[ея]\b", re.IGNORECASE),
        re.compile(r"\bпо\s+ТЗ\b", re.IGNORECASE),
        re.compile(r"\bтребован[ия]+\s+ТЗ\b", re.IGNORECASE),
        re.compile(r"\bТЗ\s*[/–-]\s*РД\b", re.IGNORECASE),
        re.compile(r"\bзадан[ие]+\s+заказчик", re.IGNORECASE),
    ],
    "audit_comparison": [
        re.compile(r"\bсравнен[ия]+\s+(?:раздел|сечен|фрагмент)", re.IGNORECASE),
        re.compile(r"\b(?:расхождени[ея]|несоответстви[ея])\s+между\b", re.IGNORECASE),
        re.compile(r"\bв\s+разделе\s+\S+.{0,80}\bв\s+разделе\s+\S+", re.IGNORECASE | re.DOTALL),
        re.compile(r"\b(?:ЭОМ|ОВ|ВК|СС|АПС|КЖ|АР|КМ)\b.*?\bvs\b.*?\b(?:ЭОМ|ОВ|ВК|СС|АПС|КЖ|АР|КМ)\b",
                   re.IGNORECASE | re.DOTALL),
    ],
    "specification_only": [
        re.compile(r"^\s*(?:#+\s*)?спецификаци[яиюей]\b", re.IGNORECASE | re.MULTILINE),
        re.compile(r"^\s*(?:#+\s*)?ведомость\b", re.IGNORECASE | re.MULTILINE),
        re.compile(r"\|[^|\n]*кабель[^|\n]*\|[^|\n]*сечени[ея][^|\n]*\|", re.IGNORECASE),
        re.compile(r"\bпоз\.\s*\d+\b", re.IGNORECASE),
    ],
    "full_rd": [
        # Strong full-RD markers — pояснительная записка, однолинейная,
        # таблица кабелей, etc. A document that contains many of these is
        # almost certainly a full РД, not a fragment.
        re.compile(r"\bпояснительн(?:ая|ой)\s+записк", re.IGNORECASE),
        re.compile(r"\bоднолинейн(?:ая|ой)\s+схем", re.IGNORECASE),
        re.compile(r"\bкабельн(?:ый|ого)\s+журнал", re.IGNORECASE),
        re.compile(r"\bтабл[ия]ц[ыа]?\s+нагрузок", re.IGNORECASE),
        re.compile(r"\bосновн(?:ые|ых)\s+технических?\s+решени", re.IGNORECASE),
    ],
}

# A document_type needs at least this many hits to win the content pass.
_CONTENT_MIN_HITS: int = 1
_CONTENT_MARGIN: int = 1  # winner must beat runner-up by at least this many hits.


# ---------------------------------------------------------------------------
# Public API.
# ---------------------------------------------------------------------------

def detect_document_type(
    project_info: dict,
    md_text: Optional[str] = None,
) -> tuple[str, float]:
    """Return (document_type, confidence). Defaults to ('full_rd', 0.5).

    Detection priority chain:
      1. project_info["document_type"] if in ALLOWED → conf 1.0
      2. project_info["section"] regex → conf 0.85
      3. project_info["pdf_file"] / project_info["name"] regex → conf 0.80
      4. md_text content regex (highest-scoring type if any) → conf 0.75
      5. ("full_rd", 0.5) fallback.
    """
    pi = project_info or {}

    # 1. Explicit.
    explicit = (pi.get("document_type") or "").strip()
    if explicit in ALLOWED:
        return (explicit, CONF_EXPLICIT)

    # 2. Section.
    section = str(pi.get("section") or "").strip()
    if section:
        for pattern, dtype in SECTION_HINTS:
            if pattern.search(section):
                return (dtype, CONF_SECTION)

    # 3. Filename.
    candidates_for_filename: list[str] = []
    for k in ("pdf_file", "name", "project_id"):
        v = pi.get(k)
        if isinstance(v, str) and v:
            candidates_for_filename.append(v)
    for cand in candidates_for_filename:
        for pattern, dtype in FILENAME_HINTS:
            if pattern.search(cand):
                return (dtype, CONF_FILENAME)

    # 4. Content.
    if md_text:
        scores: dict[str, int] = {}
        for dtype, patterns in _CONTENT_PATTERNS.items():
            count = 0
            for p in patterns:
                if p.search(md_text):
                    count += 1
            if count > 0:
                scores[dtype] = count
        # Pick the highest scorer that meets MIN_HITS and beats runner-up
        # by MARGIN. We never let `full_rd` win on content alone unless it
        # significantly beats the other types — otherwise the fallback is
        # already full_rd at lower confidence.
        if scores:
            ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
            top, top_hits = ranked[0]
            runner_hits = ranked[1][1] if len(ranked) > 1 else 0
            if top_hits >= _CONTENT_MIN_HITS and (top_hits - runner_hits) >= _CONTENT_MARGIN:
                if top == "full_rd":
                    # Require stronger evidence to commit to full_rd via
                    # content alone (otherwise the same call yields fallback).
                    if top_hits >= 2:
                        return (top, CONF_CONTENT)
                else:
                    return (top, CONF_CONTENT)

    # 5. Fallback.
    return (DEFAULT_TYPE, DEFAULT_CONFIDENCE)


# ---------------------------------------------------------------------------
# Self-tests.
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # 8 explicit cases, paralleling document_type_examples.md.

    # 1. Explicit full_rd
    t, c = detect_document_type({"document_type": "full_rd"})
    assert t == "full_rd" and c == 1.0, (t, c)

    # 2. Explicit audit_comparison
    t, c = detect_document_type({"document_type": "audit_comparison"})
    assert t == "audit_comparison" and c == 1.0, (t, c)

    # 3. Section hint = ТЗ
    t, c = detect_document_type({"section": "ТЗ", "name": "foo"})
    assert t == "tz_vs_rd" and abs(c - 0.85) < 1e-9, (t, c)

    # 4. Section hint = "Сравнение разделов"
    t, c = detect_document_type({"section": "Сравнение разделов ЭОМ/ОВ"})
    assert t == "audit_comparison" and abs(c - 0.85) < 1e-9, (t, c)

    # 5. Filename hint
    t, c = detect_document_type({"pdf_file": "13АВ_РД_ЭО_К3_spec_cables.pdf"})
    assert t == "specification_only" and abs(c - 0.80) < 1e-9, (t, c)

    # 6. Content hint — strong full_rd
    md = (
        "# Пояснительная записка\n\n"
        "## Кабельный журнал\n\n"
        "## Однолинейная схема\n"
    )
    t, c = detect_document_type({}, md_text=md)
    assert t == "full_rd" and abs(c - 0.75) < 1e-9, (t, c)

    # 7. Content hint — tz_vs_rd
    md2 = (
        "Требования ТЗ заказчика: установить ВРУ на 250 кВА.\n"
        "По ТЗ предусмотрено резервное питание от ДГУ.\n"
    )
    t, c = detect_document_type({}, md_text=md2)
    assert t == "tz_vs_rd" and abs(c - 0.75) < 1e-9, (t, c)

    # 8. Ambiguous → fallback
    t, c = detect_document_type({"name": "untitled_project"})
    assert t == "full_rd" and c == 0.5, (t, c)

    # Bonus: unknown explicit value falls through (NOT in ALLOWED).
    t, c = detect_document_type({"document_type": "ZZZ_UNKNOWN", "section": "ТЗ"})
    assert t == "tz_vs_rd" and abs(c - 0.85) < 1e-9, (t, c)

    # Bonus: explicit beats everything.
    t, c = detect_document_type(
        {"document_type": "specification_only", "section": "ТЗ"},
        md_text="Пояснительная записка ..."
    )
    assert t == "specification_only" and c == 1.0, (t, c)

    print("OK: 10 detect_document_type cases passed.")
