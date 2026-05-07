#!/usr/bin/env python3
"""
analyze_human_rejection_reasons.py
------------------------------------
Analyse false_accept rejection reasons from the human benchmark to prepare
a reject taxonomy for the LLM gate.

Reads (read-only):
  <benchmark-output-dir>/false_accepts.json
  <benchmark-output-dir>/human_benchmark_records.json   (optional, for context)
  <benchmark-output-dir>/human_benchmark_summary.json   (optional, for overview)

Writes:
  <output-dir>/human_rejection_reason_analysis.json
  <output-dir>/human_rejection_reason_analysis.md
  <output-dir>/false_accept_reason_samples.csv          (--export-csv)

NOT connected to production pipeline.
Does NOT read or modify any production artifacts.

Reject taxonomy:
  wrong_norm_context         — wrong norm / para cited, norm not applicable
  wrong_measurement_or_number — AI misread a dimension, quantity, or value
  visual_or_ocr_misread      — OCR/visual recognition error, not a real defect
  acceptable_design_solution  — the design choice is permitted by norms
  not_functionally_significant — minor/formal mismatch, no real construction impact
  duplicate_or_already_covered — duplicate finding or covered in another document
  insufficient_source_context — AI lacked context available in another section
  wrong_scope_or_section      — finding belongs to a different section/document
  calculation_not_supported   — AI derived a conclusion the numbers don't support
  human_business_decision     — rejected for business/management reasons
  other                       — none of the above

LLM gate fitness:
  llm_can_handle   — LLM reading the actual drawing would catch this
  needs_human      — requires domain context beyond what LLM sees
  borderline_llm   — LLM might help but not reliably

Usage:
    python backend/scripts/analyze_human_rejection_reasons.py \\
        --benchmark-output-dir /tmp/human_benchmark_kj_after \\
        --top-n 50 --export-csv

    # Combined KJ + AR benchmark dirs:
    python backend/scripts/analyze_human_rejection_reasons.py \\
        --benchmark-output-dir /tmp/benchmark_all \\
        --output-dir /tmp/taxonomy_analysis
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Optional

# ─── sys.path bootstrap ───────────────────────────────────────────────────────

_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ─── Taxonomy definitions ─────────────────────────────────────────────────────

TAXONOMY = {
    "wrong_norm_context": {
        "label": "Wrong norm/context",
        "description": "Wrong norm cited, inapplicable paragraph, norm used outside its scope",
        "llm_fitness": "llm_can_handle",
        "deterministic_signal": False,
        "llm_prompt_hint": "Check whether the cited norm paragraph actually applies to this element type and conditions.",
    },
    "wrong_measurement_or_number": {
        "label": "Wrong measurement/number",
        "description": "AI misread a dimension, reinforcement diameter, quantity, or numerical value",
        "llm_fitness": "llm_can_handle",
        "deterministic_signal": True,  # OCR/number errors often have digit patterns
        "llm_prompt_hint": "Re-read the exact numeric values from the drawing; verify the AI's reading against visible dimensions.",
    },
    "visual_or_ocr_misread": {
        "label": "Visual/OCR misread",
        "description": "OCR artefact or visual misread — the defect is not in the document",
        "llm_fitness": "llm_can_handle",
        "deterministic_signal": True,
        "llm_prompt_hint": "Check whether the described text or value is actually present in the drawing blocks.",
    },
    "acceptable_design_solution": {
        "label": "Acceptable design solution",
        "description": "The proposed design is normatively permitted; AI incorrectly flagged it as a violation",
        "llm_fitness": "borderline_llm",
        "deterministic_signal": False,
        "llm_prompt_hint": "Verify whether the chosen design variant is explicitly permitted by the applicable norm.",
    },
    "not_functionally_significant": {
        "label": "Not functionally significant",
        "description": "Formal/minor mismatch with no construction, safety, or financial impact",
        "llm_fitness": "borderline_llm",
        "deterministic_signal": False,
        "llm_prompt_hint": "Assess whether this finding would actually affect construction, safety, or cost.",
    },
    "duplicate_or_already_covered": {
        "label": "Duplicate / already covered",
        "description": "Finding duplicates another remark or is covered in a referenced document",
        "llm_fitness": "llm_can_handle",
        "deterministic_signal": True,
        "llm_prompt_hint": "Check if this information is already documented in the referenced section or common notes.",
    },
    "insufficient_source_context": {
        "label": "Insufficient source context",
        "description": "Required information exists in another section/document that AI did not see",
        "llm_fitness": "needs_human",
        "deterministic_signal": False,
        "llm_prompt_hint": "Note: requires cross-document context. Flag as needs_cross_section_review.",
    },
    "wrong_scope_or_section": {
        "label": "Wrong scope/section",
        "description": "Finding belongs to a different discipline section (e.g., PPR, PZ, another KJ volume)",
        "llm_fitness": "needs_human",
        "deterministic_signal": False,
        "llm_prompt_hint": "Identify which document/section owns this topic and whether it appears in design documentation.",
    },
    "calculation_not_supported": {
        "label": "Calculation not supported",
        "description": "AI derived a conclusion that the actual numbers or norm formula do not support",
        "llm_fitness": "borderline_llm",
        "deterministic_signal": False,
        "llm_prompt_hint": "Re-verify the calculation/formula the AI used; check whether the cited norm clause exists and applies.",
    },
    "human_business_decision": {
        "label": "Business/management decision",
        "description": "Rejected for project/business reasons outside technical scope",
        "llm_fitness": "needs_human",
        "deterministic_signal": False,
        "llm_prompt_hint": None,
    },
    "other": {
        "label": "Other",
        "description": "Could not be classified by keyword patterns",
        "llm_fitness": "needs_human",
        "deterministic_signal": False,
        "llm_prompt_hint": None,
    },
}

# ─── Classification patterns (ordered — first match wins) ────────────────────

_CLASSIFICATION_RULES: list[tuple[str, re.Pattern]] = [
    # Visual/OCR misread — highest priority
    ("visual_or_ocr_misread", re.compile(
        r"(ocr|ошибка распознов|ошибка чтения|некорректн.*распознов"
        r"|нечитаем|не распознаётся|артефакт.*ocr|ocr.*артефакт"
        r"|пайплайн распознал|ии.*принял.*за|ии.*распознал"
        r"|неверно.*прочитан|ошибочно.*прочитан|ошибочно принято"
        r"|сбой ocr|неверн.*текст|artifact|garbled"
        r"|замечание не валидн|проблемы не существует"
        r"|замечани.*невалидн|ии.*читал.*неполн"
        r"|некорректно.*принято|ошибочно.*принят|неверно.*прочитан"
        r"|артефакт распознован|артефакт.*пайплайн|такой проблемы нет"
        r"|пайплайн зафиксировал.*контекстн|некорретная опись"
        r"|ии-пайплайн зафиксировал|ии.*неверно.*принял)",
        re.IGNORECASE,
    )),
    # Wrong norm context — norm not applicable, wrong para, AI invented coefficient
    ("wrong_norm_context", re.compile(
        r"(норма не применяетс|пункт не применяетс|не распространяетс"
        r"|норматив.*не.*действу|снип.*заменён|несуществующ.*коэффициент"
        r"|неверн.*ссылк.*нормат|неверн.*пункт|ошибочн.*ссылк"
        r"|реальн.*норма|формул.*не.*применяетс|ии.*процитировал.*неверн"
        r"|ии обоснов.*несуществующ|ссылаетс.*на неверн|неверно.*процитир"
        r"|ии.*принял.*[0-9].*за.*[а-яА-Я]|из метки.*за|ии.*неверн.*расчет)",
        re.IGNORECASE,
    )),
    # Wrong measurement — AI got specific number wrong (after OCR check)
    ("wrong_measurement_or_number", re.compile(
        r"(неверн.*значен|неверн.*размер|неверн.*цифр|неверн.*числ"
        r"|ошибочн.*значен|ошибочн.*размер|перепутал.*разм|перепутал.*число"
        r"|суммой.*подтвержден|размерн.*цепочк"
        r"|фактическ.*наименьший.*размер|радиус.*за.*размер)",
        re.IGNORECASE,
    )),
    # Calculation not supported — AI's math/logic wrong
    ("calculation_not_supported", re.compile(
        r"(расчёт не подтвержд|вывод не обоснован"
        r"|при.*уже выполненн.*расчёте|ии обоснов.*некорр.*формул"
        r"|ии сам рассчитал.*подтвердил|ии.*рассчитал.*и подтвердил"
        r"|ии.*применил.*формулу.*неверн|уже подтвердил соответстви"
        r"|замечание.*при уже выполненн)",
        re.IGNORECASE,
    )),
    # Duplicate / already covered in the same document
    ("duplicate_or_already_covered", re.compile(
        r"(дублирует|уже учтено|уже указано|уже приведено|уже содержитс"
        r"|уже проставлен|дубл|duplicate|already covered"
        r"|приводитс.*в.*пз|приводитс.*в.*таблиц"
        r"|задан.*в.*общих указани|задан.*в.*пояснительн"
        r"|задан.*в.*лист.*[1-9]|задан.*в.*таблиц"
        r"|стандартно приводитс|стандартно указываетс"
        r"|на.*листе.*присутствует|в.*таблице.*на.*том же листе"
        r"|описан.*в.*ведомости|информация.*присутствует.*на"
        r"|все.*описан.*на.*лист|расшифровка.*выполнена.*через"
        r"|все.*данные.*в.*таблиц|все.*в.*таблиц|чётко.*видим"
        r"|явно проставлен|чёрным по белому написано"
        r"|подтверждено по document|приведена в.*таблице.*на"
        r"|отклонено так как.*лист.*[0-9]|отклонено так как.*таблиц"
        r"|отклонено так как.*ведомост|отклонено так как.*схем.*показ"
        r"|отклонено так как.*данные.*в|отклонено так как.*чётко.*видим"
        r"|отклонено так как.*присутствует|отклонено так как.*маркировк"
        r"|отклонено так как.*полн.*набор|присутствует.*маркировка"
        r"|четко.*видим.*маркировку|четко.*видим.*разрез"
        r"|четко.*видим.*выноск|полный набор данных.*в)",
        re.IGNORECASE,
    )),
    # Insufficient source context — info in another doc/section not seen by AI
    ("insufficient_source_context", re.compile(
        r"(в другом разделе|другом комплекте|другом выпуске"
        r"|в пояснительн.*записк|в расчётных документах"
        r"|комплект кж|книга [0-9]|кж\d|кж.*книга"
        r"|прошла государственн.*экспертиз|прошедш.*экспертиз"
        r"|экспертиз.*принят|объект прошёл.*экспертиз"
        r"|гэ.*проверила|гэ.*приняла"
        r"|ии анализировал отдельн.*лист.*не увидев"
        r"|в смежн.*раздел|разрабатываетс.*на стадии пд"
        r"|не дублируетс.*в.*каждом|не.*дублируетс.*на каждом"
        r"|в.*пз.*раздела|в.*пояснительн.*пз"
        r"|стандартно задаётся в пз|определяется.*в иг|в иг[эе]"
        r"|данные зафиксированы в разделе|зафиксированы в разделе [А-ЯA-Z]"
        r"|в разделе [а-яА-ЯA-Za-z]{2,5}|ссылка на.*комплект"
        r"|содержание пз кж|содержатся.*в пз|в пз кж"
        r"|данные.*в.*пз|в расчётных документах пз|относится.*к расчётн"
        r"|отклонено так как.*в пз|отклонено так как.*раздел [А-ЯA-Z]"
        r"|технологических параметры.*ппр|задаваемые в ппр)",
        re.IGNORECASE,
    )),
    # Acceptable design solution — design choice is normatively permitted
    ("acceptable_design_solution", re.compile(
        r"(допустимо по норм|соответствует норм|нормативно разрешен"
        r"|нормативно допустим|допустимое.*решен|конструктивное.*решение"
        r"|проектное решение|проектировщик.*допустим|вариант.*решен"
        r"|в пределах.*допуск|стандартн.*практик|стандартн.*нотац"
        r"|стандартн.*инструкц|стандартн.*производственн.*норм"
        r"|стандартн.*прием|принято.*экспертизой"
        r"|не является нарушен|нормой для|является нормой"
        r"|стандартн.*обозначен|стандартн.*запись|стандартн.*механизм"
        r"|принципиальн.*схем|паттерн.*установк|паттерн|это.*типовой"
        r"|допустимая запись|типов.*узл|типов.*схем|зона.*сейсмичност"
        r"|не сейсмический|сейсм.*не.*действу|соответствует.*условию.*п\."
        r"|конструктив.*допустим|минимальный.*шаг.*соответствует"
        r"|строитель.*однозначно.*понимает|прораб.*однозначно)",
        re.IGNORECASE,
    )),
    # Wrong scope or section — belongs to PPR, PD, another section
    ("wrong_scope_or_section", re.compile(
        r"(не относитс.*к.*разделу|не относитс.*к.*данному|относитс.*к ппр"
        r"|относитс.*к пд|относитс.*к.*подрядчик|регулируетс.*подрядчик"
        r"|технологи.*производства работ|в.*ппр.*а не.*чертеж"
        r"|не.*данного.*задания|вне.*данного.*задания"
        r"|данная документация.*не.*данного|в.*пд.*а не в.*рд"
        r"|вопрос не к этому разделу|не к.*разделу.*а к"
        r"|этот вопрос.*раздел|относится к разделу [А-ЯA-Z]"
        r"|конструктивное обрамление не требуетс|не требует.*обрамлени)",
        re.IGNORECASE,
    )),
    # Not functionally significant — minor/formal, no real impact
    ("not_functionally_significant", re.compile(
        r"(не влияет на|без.*влияни|не несёт.*последстви|не имеет.*значени"
        r"|формальн.*несоответстви|формальн.*редакционн|формальн.*недочёт"
        r"|не существенн|не влечёт.*последстви|практического.*влияни"
        r"|практической.*путаниц|финансов.*влияни.*нет|строитель.*влияни.*нет"
        r"|мелкий дефект|без.*строительных.*последстви|ниже.*порога.*значимости"
        r"|в пределах.*погрешности|ocr-стилистика|не существенен"
        r"|будут заполнены|рабочая маркировка"
        r"|опечатк|содержательно идентич|смысл.*однозначно.*понятен"
        r"|не.*влечёт.*строительных|не.*имеет.*финансовых"
        r"|без.*практического.*значени|строитель.*правильно.*закажет"
        r"|строитель.*поймёт|строитель.*однозначно"
        r"|мелкий оформительский|не несущ.*рисков|деффект не несу"
        r"|не стоит.*обращать внимание|не стоит.*обрашать"
        r"|отклонено так как.*оформительский|такой проблемы нет.*арт"
        r"|арифметическая ошибка отсутствует|ошибка отсутствует"
        r"|замечание ошибочн|ошибки не обнаружено)",
        re.IGNORECASE,
    )),
    # Human business decision
    ("human_business_decision", re.compile(
        r"(управленческ.*решени|бизнес.*решени|по.*договор|заказчик.*принял"
        r"|бюджет.*ограничен|нецелесообразн.*с.*коммерческ)",
        re.IGNORECASE,
    )),
]

def classify_reason(reason: str) -> str:
    """Classify a human rejection reason string into a taxonomy category."""
    if not reason or not reason.strip():
        return "other"
    for category, pattern in _CLASSIFICATION_RULES:
        if pattern.search(reason):
            return category
    return "other"


def classify_reason_multi(reason: str) -> list[str]:
    """Return all matching taxonomy categories (for analysis — some reasons match multiple)."""
    if not reason or not reason.strip():
        return ["other"]
    matched = [cat for cat, pattern in _CLASSIFICATION_RULES if pattern.search(reason)]
    return matched if matched else ["other"]


# ─── LLM fitness aggregator ──────────────────────────────────────────────────

def get_llm_fitness(category: str) -> str:
    return TAXONOMY.get(category, TAXONOMY["other"])["llm_fitness"]


# ─── Record loader ────────────────────────────────────────────────────────────

def load_false_accepts(benchmark_dir: Path) -> list[dict]:
    """Load false_accepts.json from a benchmark output directory."""
    path = benchmark_dir / "false_accepts.json"
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, OSError):
        return []


def load_benchmark_summary(benchmark_dir: Path) -> Optional[dict]:
    path = benchmark_dir / "human_benchmark_summary.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


# ─── Core analysis ───────────────────────────────────────────────────────────

def enrich_record(rec: dict) -> dict:
    """Add taxonomy classification fields to a false_accept record."""
    reason = rec.get("human_reason", "")
    primary = classify_reason(reason)
    multi = classify_reason_multi(reason)
    tax = TAXONOMY.get(primary, TAXONOMY["other"])
    return {
        **rec,
        "taxonomy_primary": primary,
        "taxonomy_all": multi,
        "taxonomy_label": tax["label"],
        "llm_fitness": tax["llm_fitness"],
        "llm_prompt_hint": tax.get("llm_prompt_hint"),
        "deterministic_signal": tax.get("deterministic_signal", False),
    }


def analyze_false_accepts(records: list[dict]) -> dict:
    """Build full analysis of false_accept records."""
    enriched = [enrich_record(r) for r in records]

    # Primary taxonomy distribution
    tax_counts = Counter(r["taxonomy_primary"] for r in enriched)
    llm_fitness_counts = Counter(r["llm_fitness"] for r in enriched)

    # By section
    by_section: dict[str, Counter] = defaultdict(Counter)
    for r in enriched:
        sec = r.get("section") or "unknown"
        by_section[sec][r["taxonomy_primary"]] += 1

    # By evidence quality
    by_ev = Counter(r.get("evidence_quality", "unknown") for r in enriched)

    # By severity
    by_sev = Counter(r.get("severity", "unknown") for r in enriched)

    # By category
    by_cat = Counter(r.get("category", "unknown") for r in enriched)

    # By impact_area
    by_impact = Counter(r.get("impact_area") or "none" for r in enriched)

    # Top human reasons (raw text, first 80 chars)
    top_reasons = Counter(
        (r.get("human_reason") or "")[:80].strip()
        for r in enriched
        if r.get("human_reason")
    ).most_common(20)

    # Deterministic-catchable fraction
    det_catchable = sum(1 for r in enriched if r.get("deterministic_signal"))

    # LLM fitness analysis
    llm_can_handle = [r for r in enriched if r["llm_fitness"] == "llm_can_handle"]
    borderline_llm = [r for r in enriched if r["llm_fitness"] == "borderline_llm"]
    needs_human = [r for r in enriched if r["llm_fitness"] == "needs_human"]

    return {
        "total_false_accept": len(enriched),
        "deterministic_catchable": det_catchable,
        "deterministic_catchable_rate": round(det_catchable / len(enriched), 3) if enriched else 0.0,
        "taxonomy_distribution": dict(tax_counts.most_common()),
        "llm_fitness_distribution": dict(llm_fitness_counts.most_common()),
        "by_section": {k: dict(v.most_common()) for k, v in sorted(by_section.items())},
        "by_evidence_quality": dict(by_ev.most_common()),
        "by_severity": dict(by_sev.most_common()),
        "by_category": dict(by_cat.most_common(15)),
        "by_impact_area": dict(by_impact.most_common(10)),
        "top_human_reasons": [{"reason": r, "count": c} for r, c in top_reasons],
        "llm_can_handle_count": len(llm_can_handle),
        "borderline_llm_count": len(borderline_llm),
        "needs_human_count": len(needs_human),
        "enriched_records": enriched,
    }


# ─── Markdown renderer ────────────────────────────────────────────────────────

def render_markdown(analysis: dict, top_n: int = 30) -> str:
    n = analysis["total_false_accept"]
    lines = [
        "# Human Rejection Reason Analysis — False Accepts",
        "",
        "## Overview",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Total false_accept analysed | **{n}** |",
        f"| Deterministic-catchable | {analysis['deterministic_catchable']} ({analysis['deterministic_catchable_rate']*100:.1f}%) |",
        f"| LLM can handle | **{analysis['llm_can_handle_count']}** ({analysis['llm_can_handle_count']*100//n if n else 0}%) |",
        f"| Borderline LLM | {analysis['borderline_llm_count']} ({analysis['borderline_llm_count']*100//n if n else 0}%) |",
        f"| Needs human | {analysis['needs_human_count']} ({analysis['needs_human_count']*100//n if n else 0}%) |",
        "",
        "## Taxonomy Distribution",
        "",
        "| Category | Count | Rate | LLM Fitness | Description |",
        "|----------|-------|------|-------------|-------------|",
    ]
    for cat, cnt in analysis["taxonomy_distribution"].items():
        tax = TAXONOMY.get(cat, TAXONOMY["other"])
        rate = f"{cnt*100//n}%" if n else "0%"
        lines.append(
            f"| {cat} | **{cnt}** | {rate} | {tax['llm_fitness']} | {tax['description'][:60]} |"
        )
    lines.append("")

    # By section
    by_sec = analysis["by_section"]
    if by_sec:
        lines += ["## By Section", ""]
        for sec, cats in sorted(by_sec.items()):
            total_sec = sum(cats.values())
            top_cat = max(cats, key=cats.get) if cats else "?"
            lines.append(f"**{sec}** ({total_sec} false_accepts) — top reason: `{top_cat}`")
        lines.append("")

    # By evidence quality
    lines += [
        "## By Evidence Quality",
        "",
        "| Evidence Quality | Count |",
        "|-----------------|-------|",
    ]
    for ev, cnt in analysis["by_evidence_quality"].items():
        lines.append(f"| {ev} | {cnt} |")
    lines.append("")

    # By severity
    lines += [
        "## By Severity",
        "",
        "| Severity | Count |",
        "|----------|-------|",
    ]
    for sev, cnt in analysis["by_severity"].items():
        lines.append(f"| {sev} | {cnt} |")
    lines.append("")

    # Recommendations
    llm_count = analysis["llm_can_handle_count"]
    border_count = analysis["borderline_llm_count"]
    human_count = analysis["needs_human_count"]
    det_count = analysis["deterministic_catchable"]

    det_cats = [cat for cat, info in TAXONOMY.items() if info.get("deterministic_signal")]
    llm_cats = [cat for cat, info in TAXONOMY.items() if info["llm_fitness"] == "llm_can_handle"]
    border_cats = [cat for cat, info in TAXONOMY.items() if info["llm_fitness"] == "borderline_llm"]
    human_cats = [cat for cat, info in TAXONOMY.items() if info["llm_fitness"] == "needs_human"]

    lines += [
        "## Recommendations",
        "",
        "### A) What Can Deterministic Filter Catch",
        "",
        f"- **{det_count}** false_accepts ({analysis['deterministic_catchable_rate']*100:.1f}%) have deterministic signals",
        f"  (categories: {', '.join(f'`{c}`' for c in det_cats)})",
        "- These are OCR/measurement errors and duplicates — detectable by pattern matching on evidence block content or title.",
        "- Approach: add OCR-error patterns to rule_filter or pre-filter step.",
        "",
        "### B) What LLM Gate Should Handle",
        "",
        f"- **{llm_count}** false_accepts can likely be caught by LLM reading the actual drawing",
        f"  (categories: {', '.join(f'`{c}`' for c in llm_cats)})",
        "- These require re-reading the document with focus on: actual numeric values, whether cited norm applies, whether info exists elsewhere.",
        "- LLM gate should be given the original finding + drawing blocks + norm references.",
        "",
        f"- **{border_count}** false_accepts are borderline for LLM",
        f"  (categories: {', '.join(f'`{c}`' for c in border_cats)})",
        "- LLM can help but needs explicit prompting about design variants and norm flexibility.",
        "",
        "### C) What Requires Human Review",
        "",
        f"- **{human_count}** false_accepts require human judgment",
        f"  (categories: {', '.join(f'`{c}`' for c in human_cats)})",
        "- These involve: cross-section context, project management decisions, scope boundaries.",
        "- Cannot be resolved by LLM without access to referenced documents.",
        "",
        "### D) LLM Gate Prompt Hints by Category",
        "",
    ]
    for cat, info in TAXONOMY.items():
        hint = info.get("llm_prompt_hint")
        cnt = analysis["taxonomy_distribution"].get(cat, 0)
        if hint and cnt > 0:
            lines.append(f"- **{cat}** ({cnt}): _{hint}_")
    lines.append("")

    # Top raw reasons
    top_reasons = analysis["top_human_reasons"]
    if top_reasons:
        lines += [
            "## Top Human Rejection Reasons (raw)",
            "",
            "| Count | Reason (first 80 chars) |",
            "|-------|------------------------|",
        ]
        for entry in top_reasons[:15]:
            lines.append(f"| {entry['count']} | {entry['reason']} |")
        lines.append("")

    # Samples
    enriched = analysis["enriched_records"]
    if enriched:
        lines += [
            f"## False Accept Samples (top {min(top_n, len(enriched))})",
            "",
            "| ID | Project | Sec | Sev | Tax | LLM | Score | Human Reason |",
            "|----|---------|-----|-----|-----|-----|-------|-------------|",
        ]
        # Sort by taxonomy then score desc
        tax_order = list(TAXONOMY.keys())
        sorted_recs = sorted(
            enriched,
            key=lambda r: (
                tax_order.index(r.get("taxonomy_primary", "other"))
                if r.get("taxonomy_primary", "other") in tax_order else 99,
                -(r.get("critic_score") or 0),
            ),
        )
        for rec in sorted_recs[:top_n]:
            fid = (rec.get("finding_id") or "?")[:16]
            proj = (rec.get("project_name") or rec.get("project") or "?")[:18]
            sec = (rec.get("section") or "?")[:4]
            sev = (rec.get("severity") or "?")[:8]
            tax = rec.get("taxonomy_primary", "other")[:22]
            llm = rec.get("llm_fitness", "?")[:14]
            score = rec.get("critic_score", 0)
            h_reason = (rec.get("human_reason") or "")[:60]
            lines.append(f"| {fid} | {proj} | {sec} | {sev} | {tax} | {llm} | {score} | {h_reason} |")
        lines.append("")

    lines += [
        "---",
        "_Generated by analyze_human_rejection_reasons.py. Production pipeline NOT modified._",
    ]
    return "\n".join(lines) + "\n"


# ─── CSV export ───────────────────────────────────────────────────────────────

def export_csv(enriched: list[dict], csv_path: Path) -> None:
    fields = [
        "finding_id", "project_name", "section", "severity", "category",
        "evidence_quality", "impact_area", "critic_score",
        "taxonomy_primary", "llm_fitness", "deterministic_signal",
        "human_reason", "title",
    ]
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for rec in enriched:
            row = {k: rec.get(k, "") for k in fields}
            row["project_name"] = rec.get("project_name") or rec.get("project") or ""
            writer.writerow(row)


# ─── Main runner ──────────────────────────────────────────────────────────────

def run_analysis(
    benchmark_dirs: list[Path],
    output_dir: Path,
    top_n: int = 50,
    export_csv_flag: bool = False,
    quiet: bool = False,
) -> dict:
    """
    Load false_accepts from one or more benchmark directories, analyse, write outputs.
    Returns analysis dict.
    """
    # Load records from all dirs
    all_records: list[dict] = []
    for bdir in benchmark_dirs:
        if not bdir.exists():
            raise FileNotFoundError(f"Benchmark dir not found: {bdir}")
        recs = load_false_accepts(bdir)
        all_records.extend(recs)
        if not quiet:
            print(f"  Loaded {len(recs)} false_accepts from: {bdir}")

    if not quiet:
        print(f"  Total false_accepts: {len(all_records)}")

    analysis = analyze_false_accepts(all_records)

    output_dir.mkdir(parents=True, exist_ok=True)

    # Write JSON
    json_out = analysis.copy()
    # Exclude full enriched_records from summary JSON to keep it readable
    json_out["enriched_records"] = [
        {k: v for k, v in r.items() if k not in ("description", "recommendation")}
        for r in analysis["enriched_records"]
    ]
    json_path = output_dir / "human_rejection_reason_analysis.json"
    json_path.write_text(json.dumps(json_out, ensure_ascii=False, indent=2), encoding="utf-8")
    if not quiet:
        print(f"  Written: {json_path}")

    # Write Markdown
    md = render_markdown(analysis, top_n=top_n)
    md_path = output_dir / "human_rejection_reason_analysis.md"
    md_path.write_text(md, encoding="utf-8")
    if not quiet:
        print(f"  Written: {md_path}")

    # Write CSV
    if export_csv_flag:
        csv_path = output_dir / "false_accept_reason_samples.csv"
        export_csv(analysis["enriched_records"], csv_path)
        if not quiet:
            print(f"  Written: {csv_path}")

    return analysis


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Analyse human rejection reasons for false_accept cases from benchmark.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python %(prog)s --benchmark-output-dir /tmp/human_benchmark_kj_after

  # Multiple dirs (KJ + AR combined)
  python %(prog)s \\
      --benchmark-output-dir /tmp/human_benchmark_kj_after \\
      --benchmark-output-dir /tmp/human_benchmark_ar_after \\
      --output-dir /tmp/taxonomy_combined

  # With CSV export
  python %(prog)s --benchmark-output-dir /tmp/human_benchmark_kj_after \\
      --top-n 50 --export-csv
""",
    )
    parser.add_argument(
        "--benchmark-output-dir", dest="benchmark_dirs",
        action="append", type=Path, required=True,
        help="Benchmark output dir (repeat for multiple).",
    )
    parser.add_argument(
        "--top-n", type=int, default=50,
        help="Number of sample records to show in report (default: 50).",
    )
    parser.add_argument(
        "--export-csv", action="store_true",
        help="Export false_accept_reason_samples.csv.",
    )
    parser.add_argument(
        "--output-dir", type=Path, default=None,
        help="Output directory (default: first --benchmark-output-dir).",
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Suppress console output.",
    )
    args = parser.parse_args()

    output_dir = args.output_dir or args.benchmark_dirs[0]

    try:
        analysis = run_analysis(
            benchmark_dirs=args.benchmark_dirs,
            output_dir=output_dir,
            top_n=args.top_n,
            export_csv_flag=args.export_csv,
            quiet=args.quiet,
        )
    except FileNotFoundError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1

    if not args.quiet:
        n = analysis["total_false_accept"]
        print()
        print("=" * 68)
        print("HUMAN REJECTION REASON ANALYSIS")
        print("=" * 68)
        print(f"  Total false_accepts  : {n}")
        print(f"  Deterministic signal : {analysis['deterministic_catchable']} ({analysis['deterministic_catchable_rate']*100:.1f}%)")
        print(f"  LLM can handle       : {analysis['llm_can_handle_count']} ({analysis['llm_can_handle_count']*100//n if n else 0}%)")
        print(f"  Borderline LLM       : {analysis['borderline_llm_count']}")
        print(f"  Needs human          : {analysis['needs_human_count']}")
        print()
        print(f"  ── Taxonomy ─────────────────────────────────────────────")
        for cat, cnt in analysis["taxonomy_distribution"].items():
            tax = TAXONOMY.get(cat, TAXONOMY["other"])
            bar = "█" * (cnt * 20 // n) if n else ""
            print(f"  {cat:<35} {cnt:>4}  {bar}")
        print()
        print(f"  ── LLM Fitness ──────────────────────────────────────────")
        for fitness, cnt in analysis["llm_fitness_distribution"].items():
            print(f"  {fitness:<20}: {cnt}")
        print()
        print(f"  Output: {output_dir}/human_rejection_reason_analysis.{{json,md}}")
        if args.export_csv:
            print(f"          {output_dir}/false_accept_reason_samples.csv")
        print("=" * 68)
        print("  NOTE: Production pipeline NOT modified.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
