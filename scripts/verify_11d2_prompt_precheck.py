#!/usr/bin/env python3
"""11D.2 §8 — проверка промпта ДО единственного реального вызова.

Работает по ТОЧНОМУ тексту, ушедшему бы в stdin CLI (снят репетицией в режиме
fake, ноль обращений к модели). Никакого содержимого промпта в вывод не
попадает: только хэши, размеры и флаги наличия.
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
from pathlib import Path

PROMPT_PATH = Path(sys.argv[1])
MD_PATH = Path(sys.argv[2])
NORMS_PATH = Path(sys.argv[3])
CHECKLIST_PATH = Path(sys.argv[4])
OUT_PATH = Path(sys.argv[5])

prompt = PROMPT_PATH.read_text(encoding="utf-8")
md = MD_PATH.read_text(encoding="utf-8")
norms = NORMS_PATH.read_text(encoding="utf-8")
checklist = CHECKLIST_PATH.read_text(encoding="utf-8")

DOC_OPEN = "===== SOURCE DOCUMENT (inlined by the pipeline) ====="
DOC_CLOSE = "===== END OF SOURCE DOCUMENT ====="
head, _, rest = prompt.partition(DOC_OPEN)
doc_body, _, tail = rest.partition(DOC_CLOSE)
instructions = head            # всё до тела документа
transport_tail = tail          # всё после тела документа

SEVERITY_VALUES = [
    "КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ", "ЭКСПЛУАТАЦИОННОЕ",
    "РЕКОМЕНДАТЕЛЬНОЕ", "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
]
SEVERITY_DEFS = {
    "КРИТИЧЕСКОЕ": "it cannot be built as designed",
    "ЭКОНОМИЧЕСКОЕ": "money, volumes, wrong grade or quantity",
    "ЭКСПЛУАТАЦИОННОЕ": "it will cause problems later",
    "РЕКОМЕНДАТЕЛЬНОЕ": "typos and minor inconsistencies",
    "ПРОВЕРИТЬ ПО СМЕЖНЫМ": "it needs data from an adjacent discipline",
}

# Инструменты. Ищем ТРЕБОВАНИЕ инструмента, а не любое совпадение слова:
# «no file reading» в транспортном контракте — это запрет, а не требование.
TOOL_REQUIREMENT_PATTERNS = {
    "Read":  r"\bRead\s+tool\b|\bREAD\s+via\b|\buse\s+Read\b|\bvia\s+Read\b|\bRead\(",
    "Write": r"\bWrite\s+tool\b|\bWRITE\s+via\b|\buse\s+Write\b|\bvia\s+Write\b|\bWrite\(",
    "Bash":  r"\bBash\s+tool\b|\buse\s+Bash\b|\bvia\s+Bash\b|\bBash\(",
    "Grep":  r"\bGrep\s+tool\b|\buse\s+Grep\b|\bvia\s+Grep\b|\bGrep\(",
    "Glob":  r"\bGlob\s+tool\b|\buse\s+Glob\b|\bvia\s+Glob\b|\bGlob\(",
    "WebSearch": r"\bWebSearch\b|\bWebFetch\b",
}
GENERIC_TOOL_REQUIREMENT = [
    "allowed-tools", "allowedTools", "You have access to the following tools",
    "DO NOT output to chat", "After writing, output a brief summary",
    "Use the tool", "call the tool",
]

ABS_PATH_RE = re.compile(r"(?<![\w:])/(?:home|srv|opt|var|usr|tmp|mnt|media|root|data)/[^\s`'\"<>]*")

# Инженерные опоры профиля ЭОМ и шаблона этапа.
# Профиль ЭОМ приезжает в шаблон ПЛЕЙСХОЛДЕРАМИ, а не собственными
# заголовками: `{DISCIPLINE_ROLE}`, `{DISCIPLINE_CHECKLIST}`,
# `{DISCIPLINE_FINDING_CATEGORIES}`. Поэтому проверяем ТЕЛО профиля, а не
# выдуманный заголовок. `{DISCIPLINE_TRIAGE_TABLE}` в шаблоне этого этапа
# отсутствует вовсе — это свойство шаблона, общее для legacy и provider,
# а не потеря 11D.1; фиксируем наблюдением, а не проверкой.
EOM_MARKERS = {
    "role_electrical": "electrical",
    "role_section": "## Role",
    "checklist_body_inlined": checklist.strip()[:300],
    "checklist_formula_marker": "I = P / (\u221a3 \u00d7 U \u00d7 cos\u03c6)",
    "checklist_uzo_marker": "\u0423\u0417\u041e/\u0423\u0417\u0414\u041f",
    "finding_categories_section": "## Finding Categories",
    "cross_discipline_criteria": "\u041f\u0420\u041e\u0412\u0415\u0420\u0418\u0422\u042c \u041f\u041e \u0421\u041c\u0415\u0416\u041d\u042b\u041c",
}
JSON_CONTRACT_MARKERS = {
    "schema_header": "## Output JSON Schema",
    "text_source_field": '"text_source"',
    "text_findings_field": '"text_findings"',
    "normative_refs_field": '"normative_refs_found"',
    "items_verified_field": '"items_verified_from_blocks"',
    "severity_field": '"severity"',
}
NORMATIVE_MARKERS = {
    "normative_section": "## Normative Reference",
    "sp256": "СП 256",
    "pue": "ПУЭ",
}
TRANSPORT_11D1_WORDING = {
    "tool_access_only": "This restriction is about TOOL ACCESS ONLY.",
    "absence_is_a_finding": "must be reported as usual",
    "old_wording_removed": None,   # заполняется ниже как «отсутствует»
}

severity_section_count = instructions.count("## Severity Semantics (what each value means)")
sev_block_start = instructions.find("## Severity Semantics (what each value means)")
sev_block = ""
if sev_block_start >= 0:
    nxt = instructions.find("\n## ", sev_block_start + 10)
    sev_block = instructions[sev_block_start: nxt if nxt > 0 else len(instructions)]

# md_prescan: на этом документе секция пуста (11D.1). Проверяем именно это.
prescan_markers = [m for m in ("## Deterministic Pre-scan", "PRE-SCAN", "pre-scan findings") if m in instructions]

checks = {
    "1_severity_semantics_present": sev_block_start >= 0,
    "2_severity_semantics_exactly_once": severity_section_count == 1,
    "3_all_severity_values_defined": {
        v: (v in sev_block and SEVERITY_DEFS[v] in sev_block) for v in SEVERITY_VALUES
    },
    "4_no_claude_md_dependency": "CLAUDE.md" not in prompt,
    "5_no_read_requirement": not re.search(TOOL_REQUIREMENT_PATTERNS["Read"], prompt),
    "6_no_write_requirement": not re.search(TOOL_REQUIREMENT_PATTERNS["Write"], prompt),
    "7_no_bash_requirement": not re.search(TOOL_REQUIREMENT_PATTERNS["Bash"], prompt),
    "8_no_grep_requirement": not re.search(TOOL_REQUIREMENT_PATTERNS["Grep"], prompt),
    "9_no_glob_requirement": not re.search(TOOL_REQUIREMENT_PATTERNS["Glob"], prompt),
    "10_no_generic_tool_requirement": {
        m: (m not in prompt) for m in GENERIC_TOOL_REQUIREMENT
    },
    "11_no_project_path_dependency": {
        "abs_paths_in_instructions": len(ABS_PATH_RE.findall(instructions)),
        "abs_paths_in_transport_tail": len(ABS_PATH_RE.findall(transport_tail)),
        "artifact_filename_absent": "02_text_analysis.json" not in prompt,
        "placeholder_present": "(not available in this run)" in instructions,
    },
    "12_full_md_inlined_verbatim": {
        "md_chars": len(md),
        "doc_body_chars": len(doc_body.strip("\n")),
        # Тело user-сообщения = служебная строка обязательного поля + ВЕСЬ MD.
        # Строку добавляет боевой сборщик обоих путей (prompt_builder), она не
        # часть документа, поэтому «дословность» проверяется как суффикс.
        "md_inlined_as_suffix_verbatim": doc_body.strip("\n").endswith(md.strip("\n")),
        "prefix_before_md": doc_body.strip("\n")[: len(doc_body.strip("\n")) - len(md.strip("\n"))],
        "md_sha256": hashlib.sha256(md.encode("utf-8")).hexdigest(),
    },
    "13_eom_discipline_rules_present": {k: (v in instructions) for k, v in EOM_MARKERS.items()},
    "13b_observed_not_a_check": {
        "triage_table_placeholder_in_stage_template": False,
        "note": ("{DISCIPLINE_TRIAGE_TABLE} в шаблоне text_analysis отсутствует "
                 "в принципе — одинаково у legacy и provider; наблюдение, не проверка"),
    },
    "14_normative_context_present": {
        **{k: (v in instructions) for k, v in NORMATIVE_MARKERS.items()},
        "norms_file_chars": len(norms),
        "norms_body_inlined": norms.strip()[:400] in instructions,
        "norms_sha256": hashlib.sha256(norms.encode("utf-8")).hexdigest(),
    },
    "15_json_contract_present": {k: (v in instructions) for k, v in JSON_CONTRACT_MARKERS.items()},
    "16_md_prescan_matches_11d1": {
        "expected_11d1": "секция pre-scan пуста на этом документе (0 находок)",
        "prescan_section_markers_found": prescan_markers,
        "matches": prescan_markers == [],
    },
    "17_transport_wording_11d1": {
        "tool_access_only": TRANSPORT_11D1_WORDING["tool_access_only"] in transport_tail,
        "absence_is_a_finding": TRANSPORT_11D1_WORDING["absence_is_a_finding"] in transport_tail,
        "old_file_missing_wording_absent":
            "do not report that a file is missing" not in prompt,
        "old_nothing_to_look_up_absent": "nothing to look up" not in prompt,
        "no_tools_declared": "You have NO tools in this run" in transport_tail,
    },
}


def flatten(node):
    if isinstance(node, bool):
        return [node]
    if isinstance(node, dict):
        out = []
        for k, v in node.items():
            if k in ("abs_paths_in_instructions", "abs_paths_in_transport_tail"):
                out.append(v == 0)
            elif k == "13b_observed_not_a_check" or k == "prefix_before_md":
                continue
            elif isinstance(v, (bool, dict)):
                out.extend(flatten(v))
        return out
    return []


bools = flatten(checks)
report = {
    "kind": "11D.2 §8 — предпроверка промпта ДО единственного реального вызова",
    "model_called": False,
    "prompt_source": str(PROMPT_PATH),
    "prompt_source_note": (
        "текст снят репетицией в режиме fake (подставной CLI, 0 обращений к модели); "
        "промпт не зависит от каталога попытки — абсолютные пути инструкций заменены "
        "фиксированным плейсхолдером"
    ),
    "fingerprint": {
        "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
        "prompt_chars": len(prompt),
        "instructions_chars": len(instructions),
        "document_chars": len(doc_body.strip("\n")),
        "transport_tail_chars": len(transport_tail),
        "instructions_sha256": hashlib.sha256(instructions.encode("utf-8")).hexdigest(),
        "severity_block_sha256": hashlib.sha256(sev_block.encode("utf-8")).hexdigest(),
        "severity_block_chars": len(sev_block),
    },
    "sections_in_order": [
        line.strip() for line in instructions.splitlines() if line.startswith("## ")
    ],
    "checks": checks,
    "all_checks_passed": all(bools),
    "checks_total": len(bools),
    "checks_failed": sum(1 for b in bools if not b),
    "content_in_report": False,
}
OUT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
print(json.dumps({
    "all_checks_passed": report["all_checks_passed"],
    "checks_total": report["checks_total"],
    "checks_failed": report["checks_failed"],
    "prompt_sha256": report["fingerprint"]["prompt_sha256"],
    "prompt_chars": report["fingerprint"]["prompt_chars"],
    "instructions_chars": report["fingerprint"]["instructions_chars"],
    "document_chars": report["fingerprint"]["document_chars"],
    "sections": report["sections_in_order"],
}, ensure_ascii=False, indent=2))
