#!/usr/bin/env python3
"""
analyze_llm_taxonomy_other.py
-------------------------------
Analyse LLM gate false_accept cases where human_taxonomy_reason = "other"
(or no LLM decision was issued) to identify new taxonomy categories.

Reads from a benchmark output directory produced by:
    benchmark_critic_v2_against_human.py --llm-gate ...

Input files:
    human_benchmark_records.json
    false_accepts.json
    critic_v2_llm_taxonomy_decisions.json  (optional; enriches with LLM explanations)
    human_benchmark_summary.json           (optional; for context)

Outputs:
    <output-dir>/llm_taxonomy_other_analysis.json
    <output-dir>/llm_taxonomy_other_analysis.md
    <output-dir>/llm_taxonomy_other_samples.csv   (with --export-csv)

NOT connected to production pipeline.
Does NOT modify any production artifacts.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Optional

# ─── sys.path bootstrap ───────────────────────────────────────────────────────

_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


# ─── Proposed taxonomy categories ─────────────────────────────────────────────

PROPOSED_CATEGORIES: dict[str, dict] = {
    "value_already_correct": {
        "description": "Значение фактически корректное; замечание построено на ложном расхождении.",
        "llm_suitability": "llm_can_handle",
        "llm_action": "reject",
        "automation_confidence": "high",
        "note": (
            "OCR/pipeline прочитал одно значение, реальное другое. "
            "LLM видит расхождение как несуществующее или уже объяснённое."
        ),
    },
    "requirement_not_mandatory": {
        "description": (
            "Требование не обязательно/рекомендательное или "
            "норма неприменима к данному разделу/условию."
        ),
        "llm_suitability": "llm_can_handle",
        "llm_action": "reject_or_borderline",
        "automation_confidence": "medium",
        "note": (
            "Включает: сейсмические нормы для несейсмических зон, "
            "строительные технологии (ППР, а не РД), необязательные ГОСТ-ссылки."
        ),
    },
    "design_stage_limitation": {
        "description": (
            "Для текущей стадии/раздела детализация не требуется; "
            "информация в другом разделе/стадии."
        ),
        "llm_suitability": "borderline_llm",
        "llm_action": "needs_human",
        "automation_confidence": "medium",
        "note": (
            "Включает: ПЗ vs РД, ПД vs РД, ОДИ vs АР, ПЗ КЖ vs графика KJ. "
            "Требует знания о составе комплекта."
        ),
    },
    "outside_audit_scope": {
        "description": (
            "Замечание вне цели проверки: другой раздел, другой комплект, "
            "ППР, ТПР, решение подрядчика."
        ),
        "llm_suitability": "borderline_llm",
        "llm_action": "needs_human",
        "automation_confidence": "medium",
        "note": (
            "Включает: замечания по смежным разделам (АР→КЖ, ЭОМ→КЖ), "
            "технологию производства работ (ППР)."
        ),
    },
    "already_resolved_by_project_note": {
        "description": (
            "Закрыто общим примечанием, ссылкой в РД, таблицей или "
            "другим листом того же комплекта."
        ),
        "llm_suitability": "llm_can_handle",
        "llm_action": "reject",
        "automation_confidence": "medium",
        "note": (
            "Включает: данные в общих указаниях, в ведомости деталей, "
            "в табл. на листе 1, в перекрёстной ссылке."
        ),
    },
    "human_marked_minor": {
        "description": (
            "Человек отклонил как формальную мелочь без последствий для строительства "
            "или безопасности."
        ),
        "llm_suitability": "borderline_llm",
        "llm_action": "borderline",
        "automation_confidence": "low",
        "note": (
            "Включает: опечатки в заголовках, двойные ссылки на норму, "
            "малое расхождение в массе/цене, редакционные погрешности. "
            "LLM часто не может оценить «незначительность» без контекста."
        ),
    },
    "false_positive_due_to_missing_context": {
        "description": (
            "Модель сделала вывод без достаточного контекста: "
            "OCR-артефакт, обрезка блока, чтение частичного текста."
        ),
        "llm_suitability": "llm_can_handle",
        "llm_action": "reject",
        "automation_confidence": "high",
        "note": (
            "Включает: OCR-артефакты page-nesting (соседние блоки попали в кадр), "
            "обрывы текста из-за усечения OCR-блока, неправильное чтение меток. "
            "Это подгруппа visual_or_ocr_misread с акцентом на контекстный сбой."
        ),
    },
    "wrong_element_or_location": {
        "description": (
            "Замечание относится не к тому элементу, листу или позиции "
            "из-за ошибки маппинга пайплайна."
        ),
        "llm_suitability": "llm_can_handle",
        "llm_action": "reject",
        "automation_confidence": "medium",
        "note": (
            "Включает: перепутан лист/марка/позиция, сравниваются разные конструктивные "
            "элементы, неправильный sheet/page. "
            "Схоже с wrong_norm_context, но для геометрии/маркировки."
        ),
    },
    "other_unclassified": {
        "description": "Не классифицировано по другим категориям.",
        "llm_suitability": "needs_human",
        "llm_action": "needs_human",
        "automation_confidence": "none",
        "note": "Требует ручного разбора.",
    },
}

# ─── Classification rules ─────────────────────────────────────────────────────
#
# Each rule: (category, [patterns_in_human_reason], [patterns_in_llm_explanation])
# Applied in order; first match wins.
# Patterns are case-insensitive regex.

_RULES: list[tuple[str, list[str], list[str]]] = [
    # OCR / pipeline misread / artifact
    (
        "false_positive_due_to_missing_context",
        [
            r"артефакт распознов",
            r"артефакт.*ocr",
            r"ocr.*артефакт",
            r"некорр?ектн.*распознов",
            r"ошибка распознов",
            r"артефакт.*нарезк",
            r"page.nesting",
            r"усечен.*ocr",
            r"обрыв.*ocr",
            r"обрыв.*блок",
            r"ocr.блок",
            r"вырвано из контекст",
            r"ошибоч.*счит",
            r"некорректн.*счит",
            r"некорр?ект.*прочит",
            r"ошибка.*прочит",
            r"неверн.*прочит",
            r"aiпайплайн.*зафиксировал",
            r"нейросет.*некорр",
            r"нейросет.*неверн",
            r"пайплайн.*зафиксировал",
            r"попал.*в кадр",
            r"нарезк.*страниц",
        ],
        [r"ocr", r"artifact", r"misread", r"page.*nest"],
    ),
    # Value already correct — value confirmed elsewhere in document
    (
        "value_already_correct",
        [
            r"проблем.*не существует",
            r"такой проблемы нет",
            r"ошибки не обнаружено",
            r"корректн.*задан.*общ.*указани",
            r"верн.*указан",
            r"подтвержден",
            r"присутствует.*маркировк",
            r"чётко.*указан",
            r"однозначно.*задан",
            r"содержатся.*на.*лист",
            r"содержатся.*в.*ведомост",
            r"содержат.*на тех же",
            r"полная.*информация",
            r"полные.*данные",
            r"полный.*набор",
            r"задан.*в.*ведомост",
            r"задан.*в спецификац",
            r"задан.*в общ",
            r"задан.*на листе",
            r"расшифровк.*марк.*выполнен",
            r"привязк.*к.*координационным",
            r"привязк.*к.*граням",
            r"стандартн.*нотаци",
            r"стандартн.*запись",
            r"стандартн.*обозначени",
            r"стандартн.*практик",
            r"стандартн.*инструкц",
            r"стандартн.*нормативн.*конструкц",
            r"принципиальн.*схем",
            r"условно.*армирование.*показано",
            r"«армирование показано условно»",
            r"разрезн.*нотаци",
        ],
        [],
    ),
    # Outside audit scope — other section / PPR / subcontractor
    (
        "outside_audit_scope",
        [
            r"это.*вопрос.*не к этому разделу",
            r"вне данного задани",
            r"самостоятельн.*комплект",
            r"разработан.*вне данного",
            r"относится к технологи.*производства работ",
            r"регулируется ппр",
            r"относятся к.*ппр",
            r"задаваемые в ппр",
            r"относится.*к смежному разделу",
            r"не к этому разделу",
            r"к разделу ар",
            r"эти данные зафиксированн.*в разделе",
            r"принадлежат комплекту кж",
            r"ссылка прямая и однозначная",
            r"прямой адресной ссылк",
            r"узлы примыкания.*принадлежат",
        ],
        [],
    ),
    # Already resolved by note / cross-reference in same document
    (
        "already_resolved_by_project_note",
        [
            r"задан.*в общ.*указани",
            r"содержится.*в общ.*указани",
            r"описан.*в.*ведомост",
            r"присутствует.*в.*спецификац",
            r"в пункте.*общих указаний.*черным по белому",
            r"в п\.\s*\d+.*общих указаний",
            r"указан.*на.*листе\s*\d",
            r"см\. лист",
            r"ссылка.*на комплект",
            r"содержится.*на листах",
            r"указан.*в таблиц",
            r"в таблице.*на листе",
            r"таблица.*тех же листах",
            r"определяется по таблице",
            r"задан.*в спецификации",
            r"данные зафиксированны",
            r"задаётся в.*пз.*кж",
            r"задается в.*пз",
            r"указывается в пз",
            r"задаётся в пз",
            r"рассчит.*по таблице",
            r"параметр.*«a».*определяется по п\.",
            r"углы.*определяются по п\.",
            r"проектная.*документация.*прошла.*экспертиз",
        ],
        [],
    ),
    # Design stage limitation — detail not required at this stage/section
    (
        "design_stage_limitation",
        [
            r"относится к.*пз",
            r"задаётся в.*пз",
            r"содержится в пз",
            r"указывается в пз",
            r"рабочие чертежи",
            r"для.*стадии.*пд",
            r"для.*стадии рд",
            r"стадия рд",
            r"для.*стадии",
            r"не требуется для.*данной стадии",
            r"не требуется на стадии рд",
            r"разрабатывается.*стадии пд",
            r"разрабатывается на стадии",
            r"расчеты.*разрабатываются.*стадии пд",
            r"относится к стадии",
            r"не относится к рабочей документации",
            r"спецификация.*на стадии",
            r"стандартно указывается в пз",
            r"стандартно задаётся в пз",
        ],
        [],
    ),
    # Requirement not mandatory — norm optional / not applicable to region/condition
    (
        "requirement_not_mandatory",
        [
            r"зона сейсмичности.*5 баллов",
            r"применяется.*с 6 баллов",
            r"не применяется",
            r"неприменим",
            r"применяется добровольно",
            r"не влечёт.*строительных.*последствий",
            r"не влечёт.*финансовых.*последствий",
            r"не влечёт.*последствий",
            r"не несёт.*финансовых",
            r"не несёт.*рисков",
            r"не несёт.*последствий",
            r"не влияет на строительство",
            r"не влияет на строительный процесс",
            r"не влияет на конструктивные решения",
            r"не влияет на.*строй",
            r"формальн.*придирка",
            r"формальн.*замечание",
            r"формальн.*несоответствие",
            r"формальн.*погрешность",
            r"формальн.*недоработк",
            r"формальн.*ошибк",
            r"редакционн.*погрешность",
            r"не несет рисков",
            r"не несет последствий",
            r"не стоит на",
            r"мелкий оформительский де[фф]ект",
            r"малозначимое",
            r"незначительное",
            r"опечатка в",
            r"опечатка",
            r"устаревшая ссылка.*не влияет",
            r"устаревш.*не влечёт",
            r"устаревш.*не влияет",
            r"экспертиза пройдена",
            r"экспертиза принята",
            r"гэ.*проверила",
            r"проверила и приняла",
            r"прошедшей экспертиз",
            r"прошла.*экспертиз",
            r"рекомендательного характера",
            r"справочный характер",
            r"носит справочный",
            r"ссылка на продуктовый стандарт",
            r"сортаментный стандарт",
            r"ссылки на гост.*стального проката",
            r"физические характеристики.*не изменились",
            r"сортаментные характеристики",
        ],
        [],
    ),
    # Wrong element/location
    (
        "wrong_element_or_location",
        [
            r"сравниваются разные конструктивные элементы",
            r"относится не к тому элементу",
            r"относится не к тому листу",
            r"не к той позиции",
            r"перепутан.*лист",
            r"перепутан.*марк",
            r"перепутан.*позиц",
            r"ошибочно.*принята",
            r"ошибочно.*идентифицирован",
            r"t=200.*в легенде.*плита",
            r"плита.*первого этажа.*на отм",
            r"разные.*горизонтах",
            r"расположены на разных уровнях",
            r"разные горизонты",
            r"разные.*этажах",
            r"листы 3 и 4.*описывает.*разных слоях",
        ],
        [],
    ),
    # human_marked_minor — human explicitly calls it minor/formal
    (
        "human_marked_minor",
        [
            r"расхождение.*760 руб",
            r"расхождение.*[0-9]+.*кг.*760",
            r"8,66 кг",
            r"не несёт финансовых или строительных последствий",
            r"мелкая.*опечатка",
            r"опечатка.*в.*заголовк",
            r"опечатка.*в.*перечне",
            r"опечатка.*в.*ссылочных",
            r"опечатк.*нивелируется",
            r"два диапазона.*форматирование",
            r"форматирование заголовка",
            r"редакционн.*погрешность.*формления",
            r"дублирование.*текста.*редакционн",
            r"одинаковая геометрия.*стандартная практик",
            r"повторяющийся маркер",
        ],
        [],
    ),
]


def classify_case(human_reason: str, llm_explanation: str) -> str:
    """Classify an other-taxonomy false_accept into a proposed category."""
    text = (human_reason or "").lower()
    expl = (llm_explanation or "").lower()

    for category, human_patterns, llm_patterns in _RULES:
        for pat in human_patterns:
            if re.search(pat, text, re.IGNORECASE):
                return category
        for pat in llm_patterns:
            if re.search(pat, expl, re.IGNORECASE):
                return category

    return "other_unclassified"


# ─── Data loaders ─────────────────────────────────────────────────────────────

def load_benchmark_data(benchmark_dir: Path) -> dict:
    """Load all relevant benchmark output files from one directory."""
    result = {
        "records": [],
        "false_accepts": [],
        "llm_taxonomy_decisions": {},
        "summary": {},
        "dir": str(benchmark_dir),
    }

    records_path = benchmark_dir / "human_benchmark_records.json"
    if records_path.exists():
        result["records"] = json.loads(records_path.read_text(encoding="utf-8"))

    fa_path = benchmark_dir / "false_accepts.json"
    if fa_path.exists():
        result["false_accepts"] = json.loads(fa_path.read_text(encoding="utf-8"))

    tax_path = benchmark_dir / "critic_v2_llm_taxonomy_decisions.json"
    if tax_path.exists():
        decisions = json.loads(tax_path.read_text(encoding="utf-8"))
        result["llm_taxonomy_decisions"] = {d["finding_id"]: d for d in decisions}

    summary_path = benchmark_dir / "human_benchmark_summary.json"
    if summary_path.exists():
        result["summary"] = json.loads(summary_path.read_text(encoding="utf-8"))

    return result


# ─── Case selection ───────────────────────────────────────────────────────────

def select_other_cases(
    false_accepts: list[dict],
    llm_taxonomy_decisions: dict[str, dict],
) -> list[dict]:
    """
    Select false_accept cases where:
    - human_decision == "rejected"
    - critic_decision after LLM == "accept" or "borderline"
    - human_taxonomy_reason == "other" OR no LLM decision available

    Returns enriched case dicts.
    """
    cases = []
    for fa in false_accepts:
        fid = fa.get("finding_id", "")
        td = llm_taxonomy_decisions.get(fid, {})
        taxonomy_reason = td.get("human_taxonomy_reason", "no_llm_decision")

        # Only include "other" or cases with no LLM decision
        if taxonomy_reason not in ("other", "no_llm_decision", None, ""):
            continue

        cases.append({
            "finding_id": fid,
            "project_name": fa.get("project_name", ""),
            "section": fa.get("section", ""),
            "title": fa.get("title", ""),
            "description": fa.get("description", ""),
            "recommendation": fa.get("recommendation", ""),
            "human_reason": fa.get("human_reason", ""),
            "human_decision": fa.get("human_decision", "rejected"),
            "critic_decision": fa.get("critic_decision", ""),
            "critic_decision_before_llm": fa.get("critic_decision_before_llm", ""),
            "critic_score": fa.get("critic_score", 0),
            "evidence_quality": fa.get("evidence_quality", ""),
            "severity": fa.get("severity", ""),
            "category": fa.get("category", ""),
            "sheet": fa.get("sheet", ""),
            "page": fa.get("page"),
            "match_confidence": fa.get("match_confidence", ""),
            # LLM decision fields
            "llm_decision": td.get("llm_decision", ""),
            "llm_taxonomy_reason": taxonomy_reason,
            "llm_explanation": td.get("explanation", ""),
            "llm_confidence": td.get("confidence"),
            "llm_source_dependency": td.get("source_dependency", ""),
        })

    return cases


# ─── Keyword clustering ───────────────────────────────────────────────────────

def cluster_by_keywords(cases: list[dict], top_n: int = 20) -> dict:
    """
    Simple keyword frequency analysis of human_reason texts.

    Returns:
        {
            "top_human_reason_phrases": [(phrase, count), ...],
            "top_human_reason_starts": [(first_50_chars, count), ...],
        }
    """
    reason_starts: Counter = Counter()
    full_reasons: Counter = Counter()
    keyword_patterns = [
        (r"артефакт\s+распознов\w+", "ocr_artifact"),
        (r"ошибка\s+распознов\w+", "ocr_error"),
        (r"некорр?ект\w*\s+распознов\w+", "ocr_misread"),
        (r"стандарт\w+\s+практик\w+", "standard_practice"),
        (r"стандарт\w+\s+обозначени\w+", "standard_notation"),
        (r"задан\s+в\s+общ\w+\s+указани\w+", "in_general_notes"),
        (r"формальн\w+", "formal_issue"),
        (r"не\s+влечёт", "no_consequence"),
        (r"не\s+влияет", "no_effect"),
        (r"экспертиза\s+пройдена", "expertise_passed"),
        (r"экспертиза\s+принята", "expertise_accepted"),
        (r"ппр", "ppr_scope"),
        (r"технологи\w+\s+производства\s+работ", "construction_technology"),
        (r"другой\s+раздел|иной\s+раздел|к\s+разделу\s+\w+", "wrong_section"),
        (r"пз\s+кж|пз\s+раздела|пояснительная\s+записка", "pz_reference"),
        (r"стадии?\s+пд|стадии?\s+рд", "wrong_stage"),
        (r"опечатк\w+", "typo"),
        (r"мелк\w+|незначительн\w+|малозначим\w+", "minor_issue"),
        (r"рекомендательн\w+|необязательн\w+", "not_mandatory"),
    ]

    keyword_counts: Counter = Counter()
    for case in cases:
        reason = case.get("human_reason", "")
        if not reason:
            continue
        # Count start-of-reason phrases
        start = reason[:60].strip()
        reason_starts[start] += 1
        # Count keyword patterns
        for pattern, label in keyword_patterns:
            if re.search(pattern, reason, re.IGNORECASE):
                keyword_counts[label] += 1

    return {
        "top_keyword_labels": keyword_counts.most_common(top_n),
        "top_reason_starts": reason_starts.most_common(top_n),
    }


# ─── Main analysis ────────────────────────────────────────────────────────────

def analyze_other_cases(
    benchmark_dirs: list[Path],
    top_n: int = 100,
) -> dict:
    """
    Load data from one or more benchmark directories, classify other-taxonomy
    false_accept cases, and return analysis result.
    """
    all_cases: list[dict] = []
    all_summaries: list[dict] = []
    source_dirs: list[str] = []

    for bdir in benchmark_dirs:
        data = load_benchmark_data(bdir)
        cases = select_other_cases(
            data["false_accepts"],
            data["llm_taxonomy_decisions"],
        )
        for c in cases:
            c["_source_dir"] = str(bdir)
        all_cases.extend(cases)
        if data["summary"]:
            all_summaries.append(data["summary"])
        source_dirs.append(str(bdir))

    # Classify each case
    for case in all_cases:
        case["proposed_category"] = classify_case(
            case.get("human_reason", ""),
            case.get("llm_explanation", ""),
        )

    # Aggregate stats
    category_counter: Counter = Counter(c["proposed_category"] for c in all_cases)
    section_counter: Counter = Counter(c.get("section", "?") for c in all_cases)
    evidence_counter: Counter = Counter(c.get("evidence_quality", "?") for c in all_cases)
    severity_counter: Counter = Counter(c.get("severity", "?") for c in all_cases)
    category_counter_str: Counter = Counter(c.get("category", "?") for c in all_cases)

    # Human reason clusters
    keyword_clusters = cluster_by_keywords(all_cases, top_n=min(top_n, 30))

    # Category metadata with counts
    category_breakdown = {}
    for cat_name, cat_meta in PROPOSED_CATEGORIES.items():
        count = category_counter.get(cat_name, 0)
        examples = [
            {
                "finding_id": c["finding_id"],
                "section": c["section"],
                "project_name": c["project_name"],
                "title": c["title"][:100],
                "human_reason": c["human_reason"][:200],
                "llm_explanation": c["llm_explanation"][:150],
                "evidence_quality": c["evidence_quality"],
                "critic_score": c["critic_score"],
            }
            for c in all_cases if c["proposed_category"] == cat_name
        ][:5]
        category_breakdown[cat_name] = {
            **cat_meta,
            "count": count,
            "pct": round(count / len(all_cases) * 100, 1) if all_cases else 0.0,
            "examples": examples,
        }

    # LLM suitability summary
    suitability: Counter = Counter()
    for cat_name, cat_meta in PROPOSED_CATEGORIES.items():
        cnt = category_counter.get(cat_name, 0)
        suitability[cat_meta["llm_suitability"]] += cnt

    return {
        "meta": {
            "source_dirs": source_dirs,
            "total_other_cases": len(all_cases),
            "sections": dict(section_counter),
            "evidence_quality_dist": dict(evidence_counter),
            "severity_dist": dict(severity_counter),
            "finding_category_dist": dict(category_counter_str.most_common(20)),
        },
        "category_breakdown": category_breakdown,
        "suitability_summary": {
            "llm_can_handle": suitability["llm_can_handle"],
            "borderline_llm": suitability["borderline_llm"],
            "needs_human": suitability["needs_human"],
        },
        "keyword_clusters": keyword_clusters,
        "cases": all_cases[:top_n],
    }


# ─── Markdown renderer ────────────────────────────────────────────────────────

def render_markdown(analysis: dict, top_n: int = 50) -> str:
    meta = analysis["meta"]
    cat_breakdown = analysis["category_breakdown"]
    suitability = analysis["suitability_summary"]
    keyword_cl = analysis["keyword_clusters"]
    total = meta["total_other_cases"]

    lines = [
        "# LLM Taxonomy `other` — False Accept Analysis",
        "",
        "## Overview",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Total `other`/unmapped false_accept cases | **{total}** |",
        f"| Source directories | {len(meta['source_dirs'])} |",
        "| Sections | " + ", ".join(f"{k}={v}" for k, v in meta["sections"].items()) + " |",
        "",
        "## Proposed Category Breakdown",
        "",
        "| Category | Count | % | LLM Suitability | LLM Action |",
        "|----------|-------|---|-----------------|------------|",
    ]
    for cat_name, cat_data in sorted(
        cat_breakdown.items(), key=lambda x: -x[1]["count"]
    ):
        if cat_data["count"] == 0:
            continue
        lines.append(
            f"| {cat_name} | {cat_data['count']} | {cat_data['pct']}% "
            f"| {cat_data['llm_suitability']} | {cat_data['llm_action']} |"
        )
    lines.append("")

    # Automation suitability summary
    lines += [
        "## Automation Suitability Summary",
        "",
        f"| Decision | Count |",
        f"|---------|-------|",
        f"| `llm_can_handle` (→ reject) | {suitability['llm_can_handle']} |",
        f"| `borderline_llm` (→ needs_human or borderline) | {suitability['borderline_llm']} |",
        f"| `needs_human` (→ cannot automate) | {suitability['needs_human']} |",
        "",
    ]

    # Recommendations
    llm_handle_cats = [
        name for name, d in cat_breakdown.items()
        if d.get("llm_suitability") == "llm_can_handle" and d["count"] > 0
    ]
    borderline_cats = [
        name for name, d in cat_breakdown.items()
        if d.get("llm_suitability") == "borderline_llm" and d["count"] > 0
    ]
    needs_human_cats = [
        name for name, d in cat_breakdown.items()
        if d.get("llm_suitability") == "needs_human" and d["count"] > 0
    ]

    lines += [
        "## Recommendations",
        "",
        "### A. Categories safe for LLM to reject (add to prompt → reject)",
        "",
    ]
    for cat in llm_handle_cats:
        d = cat_breakdown[cat]
        lines.append(f"- **`{cat}`** ({d['count']} cases): {d['description']}")
    if not llm_handle_cats:
        lines.append("_None clearly safe for automated rejection._")
    lines.append("")

    lines += ["### B. Categories for LLM borderline / needs_human (add to prompt → borderline)", ""]
    for cat in borderline_cats:
        d = cat_breakdown[cat]
        lines.append(f"- **`{cat}`** ({d['count']} cases): {d['description']}")
    if not borderline_cats:
        lines.append("_None._")
    lines.append("")

    lines += ["### C. Categories that cannot be automated (require human context)", ""]
    for cat in needs_human_cats:
        d = cat_breakdown[cat]
        lines.append(f"- **`{cat}`** ({d['count']} cases): {d['description']}")
    if not needs_human_cats:
        lines.append("_None._")
    lines.append("")

    # Category notes
    lines += ["## Category Details", ""]
    for cat_name, cat_data in sorted(cat_breakdown.items(), key=lambda x: -x[1]["count"]):
        if cat_data["count"] == 0:
            continue
        lines += [
            f"### `{cat_name}` — {cat_data['count']} cases ({cat_data['pct']}%)",
            "",
            f"_{cat_data['description']}_",
            "",
            f"**LLM suitability:** `{cat_data['llm_suitability']}` / "
            f"**Recommended action:** `{cat_data['llm_action']}`",
            "",
            f"**Note:** {cat_data['note']}",
            "",
        ]
        if cat_data.get("examples"):
            lines.append("**Examples:**")
            for ex in cat_data["examples"][:3]:
                lines += [
                    f"- `{ex['finding_id']}` [{ex['section']}] {ex['title'][:80]}",
                    f"  - Human: _{ex['human_reason'][:120]}_",
                    f"  - LLM: _{ex['llm_explanation'][:100]}_",
                ]
            lines.append("")

    # Top keyword clusters
    lines += ["## Top Keyword Labels in `other` Reasons", ""]
    top_kw = keyword_cl.get("top_keyword_labels", [])
    if top_kw:
        lines += ["| Keyword Label | Count |", "|--------------|-------|"]
        for label, cnt in top_kw[:15]:
            lines.append(f"| `{label}` | {cnt} |")
        lines.append("")

    # Sample cases
    cases = analysis.get("cases", [])
    if cases:
        show = cases[:top_n]
        lines += [
            f"## Sample Cases ({len(show)} of {total})",
            "",
            "| # | ID | Sec | ProposedCat | Score | EV | Human Reason (first 80 chars) |",
            "|---|-----|-----|-------------|-------|----|-------------------------------|",
        ]
        for i, c in enumerate(show, 1):
            reason = (c.get("human_reason") or "")[:80].replace("|", "∣")
            lines.append(
                f"| {i} | {c['finding_id'][:16]} | {c['section']} "
                f"| {c['proposed_category']} | {c['critic_score']} "
                f"| {c['evidence_quality']} | {reason} |"
            )
        lines.append("")

    lines += [
        "---",
        "_Generated by analyze_llm_taxonomy_other.py. Production pipeline NOT modified._",
    ]
    return "\n".join(lines) + "\n"


# ─── CSV export ───────────────────────────────────────────────────────────────

def export_csv(cases: list[dict], csv_path: Path) -> None:
    """Write cases to CSV for spreadsheet analysis."""
    if not cases:
        return
    fields = [
        "finding_id", "project_name", "section", "proposed_category",
        "human_reason", "llm_explanation", "llm_taxonomy_reason",
        "llm_decision", "critic_decision", "critic_score", "evidence_quality",
        "severity", "category", "sheet", "title", "description",
    ]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(cases)


# ─── Output writer ────────────────────────────────────────────────────────────

def write_outputs(
    output_dir: Path,
    analysis: dict,
    export_csv_flag: bool = False,
    top_n: int = 50,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    # JSON
    (output_dir / "llm_taxonomy_other_analysis.json").write_text(
        json.dumps(analysis, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # Markdown
    md = render_markdown(analysis, top_n=top_n)
    (output_dir / "llm_taxonomy_other_analysis.md").write_text(md, encoding="utf-8")

    # CSV
    if export_csv_flag:
        export_csv(
            analysis.get("cases", []),
            output_dir / "llm_taxonomy_other_samples.csv",
        )


# ─── Console print ────────────────────────────────────────────────────────────

def print_analysis(analysis: dict) -> None:
    meta = analysis["meta"]
    cat_breakdown = analysis["category_breakdown"]
    suitability = analysis["suitability_summary"]
    total = meta["total_other_cases"]

    print()
    print("=" * 72)
    print("LLM TAXONOMY `other` — FALSE ACCEPT ANALYSIS")
    print("=" * 72)
    print(f"  Total other/unmapped cases : {total}")
    print(f"  Sections                   : {meta['sections']}")
    print()
    print("  ── Proposed Category Breakdown ──────────────────────────────────")
    for cat_name, cat_data in sorted(cat_breakdown.items(), key=lambda x: -x[1]["count"]):
        if cat_data["count"] == 0:
            continue
        bar = "█" * min(cat_data["count"] // 2 + 1, 20)
        print(
            f"  {cat_name:<42} {cat_data['count']:>4}  {cat_data['pct']:>5.1f}%  "
            f"[{cat_data['llm_suitability']}]  {bar}"
        )
    print()
    print("  ── Automation Suitability ──────────────────────────────────────")
    print(f"  llm_can_handle   (→ reject)              : {suitability['llm_can_handle']}")
    print(f"  borderline_llm   (→ needs_human/border)  : {suitability['borderline_llm']}")
    print(f"  needs_human      (cannot automate)       : {suitability['needs_human']}")
    print()

    # Safe-to-add categories
    safe = [
        n for n, d in cat_breakdown.items()
        if d.get("llm_suitability") == "llm_can_handle" and d["count"] > 0
    ]
    if safe:
        print("  ── Categories to Add to LLM Prompt (→ reject) ──────────────────")
        for n in safe:
            print(f"  ✓ {n:<40} {cat_breakdown[n]['count']:>4} cases")
        print()

    borderline = [
        n for n, d in cat_breakdown.items()
        if d.get("llm_suitability") == "borderline_llm" and d["count"] > 0
    ]
    if borderline:
        print("  ── Categories to Add to LLM Prompt (→ needs_human) ─────────────")
        for n in borderline:
            print(f"  ~ {n:<40} {cat_breakdown[n]['count']:>4} cases")
        print()

    print("=" * 72)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Analyse LLM taxonomy=other false_accept cases from benchmark output. "
            "Proposes new taxonomy categories for LLM gate prompt improvement."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single benchmark dir
  python %(prog)s \\
      --benchmark-output-dir /tmp/human_benchmark_kj_llm_real \\
      --output-dir /tmp/taxonomy_other_analysis

  # Multiple dirs
  python %(prog)s \\
      --benchmark-output-dir /tmp/human_benchmark_kj_llm_real \\
      --benchmark-output-dir /tmp/human_benchmark_ar_llm_real \\
      --export-csv \\
      --output-dir /tmp/taxonomy_other_combined
""",
    )
    parser.add_argument(
        "--benchmark-output-dir",
        dest="benchmark_dirs",
        action="append",
        type=Path,
        required=True,
        metavar="PATH",
        help="Benchmark output directory (can repeat for multiple sections).",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=100,
        help="Max cases to include in JSON/markdown samples (default: 100).",
    )
    parser.add_argument(
        "--export-csv",
        action="store_true",
        help="Also write llm_taxonomy_other_samples.csv.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Where to write outputs (default: first --benchmark-output-dir).",
    )
    args = parser.parse_args()

    output_dir = args.output_dir or args.benchmark_dirs[0]

    # Validate dirs exist
    missing = [str(d) for d in args.benchmark_dirs if not d.exists()]
    if missing:
        print(f"ERROR: Benchmark dirs not found: {missing}", file=sys.stderr)
        return 1

    print(f"\nAnalysing taxonomy=other false_accept cases...")
    for d in args.benchmark_dirs:
        print(f"  Input: {d}")

    analysis = analyze_other_cases(
        benchmark_dirs=args.benchmark_dirs,
        top_n=args.top_n,
    )

    write_outputs(
        output_dir=output_dir,
        analysis=analysis,
        export_csv_flag=args.export_csv,
        top_n=args.top_n,
    )

    print_analysis(analysis)

    print(f"\n  Output files:")
    print(f"    {output_dir}/llm_taxonomy_other_analysis.json")
    print(f"    {output_dir}/llm_taxonomy_other_analysis.md")
    if args.export_csv:
        print(f"    {output_dir}/llm_taxonomy_other_samples.csv")
    print()
    print("  NOTE: Production pipeline NOT modified.")
    print("        No production artifacts changed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
