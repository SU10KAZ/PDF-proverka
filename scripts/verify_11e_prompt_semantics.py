#!/usr/bin/env python3
"""11E §13 — построчная сверка смысла двух промптов свода. НОЛЬ вызовов модели.

Что сравнивается и почему именно это.

  A. LEGACY ENGINEERING INPUT — `prepare_findings_merge_task`, то есть ветка,
     которая сегодня работает на центре через `claude -p` с файловыми
     инструментами. Именно её заменяет 11E, поэтому база сравнения — она, а не
     API-промпт ветки OpenRouter. Урок 11D.1 дословно: сверка с API-промптом
     слепа к тому, что снял `_clean_template_for_api`, и не видит разницы,
     которая как раз и интересна.

  B. PROVIDER ENGINEERING INPUT — `provider_transport.build_provider_prompt`
     поверх боевого сборщика `build_findings_merge_messages`.

Каждая строка инструкций A классифицируется:

    PRESERVED       строка дословно есть в инструкциях B
    FORMAT_ONLY     есть после схлопывания пробелов
    ORDER_ONLY      есть, но в другом месте промпта
    TRANSPORT_ONLY  снята намеренно: это инструкция инструменту, которого нет
    CONTENT_REMOVED инженерная строка пропала — НАРУШЕНИЕ
    UNKNOWN         не удалось отнести ни к чему

Строки, которых в A не было, помечаются CONTENT_ADDED с указанием источника
(справка о входе, контракт транспорта, смысл severity, полезная нагрузка).

Отдельно проверяется HIDDEN_CONTEXT: где на платформе записаны определения
значений `severity`. Если единственная копия живёт в корневом `CLAUDE.md`,
который `ProviderAdapter` намеренно подавляет, — это скрытая зависимость, и её
надо переносить в промпт явно (как это сделано на 11D.1).

В отчёт НЕ попадает ни один фрагмент документа заказчика: сравниваются
ИНСТРУКЦИИ, а полезная нагрузка учитывается только счётчиками.

Прогон:
    python scripts/verify_11e_prompt_semantics.py \
        --inputs-dir <каталог с 02_/01_ артефактами> --section EOM \
        --out docs/distributed_audit_workers/11e/11E_PROMPT_SEMANTIC_DIFF.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("AUDIT_DISABLE_DOTENV", "1")
os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
sys.dont_write_bytecode = True

#: Строки, снятие которых является ТРАНСПОРТНЫМ, а не смысловым. Список закрыт
#: намеренно: всё, что под него не подошло и пропало, — CONTENT_REMOVED.
_TRANSPORT_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("read_tool", re.compile(r"Read tool", re.IGNORECASE)),
    ("write_tool", re.compile(r"Write tool", re.IGNORECASE)),
    ("read_via", re.compile(r"READ via")),
    ("write_via", re.compile(r"WRITE via")),
    ("chat_output", re.compile(r"output to chat", re.IGNORECASE)),
    ("brief_summary", re.compile(r"After writing, output a brief summary",
                                 re.IGNORECASE)),
    ("output_path", re.compile(r"\{OUTPUT_PATH\}")),
    ("md_file_path", re.compile(r"\{MD_FILE_PATH\}")),
    ("absolute_path", re.compile(r"(?<![\w:])/(?:home|srv|opt|var|usr|tmp|mnt|"
                                 r"media|root|data)/")),
)

#: Источники строк, добавленных provider-режимом.
_ADDED_SOURCES: tuple[tuple[str, str], ...] = (
    ("input_data_note", "## Input Data (this run)"),
    ("transport_contract", "## OUTPUT TRANSPORT"),
    ("severity_semantics", "## Severity Semantics (what each value means)"),
    ("payload_frame", "===== STAGE OUTPUTS TO CONSOLIDATE"),
)

#: Строки, чьё снятие ФОРМАЛЬНО транспортное (инструкция инструменту), но по
#: СУЩЕСТВУ меняет состав доступных модели данных. Такие обязаны быть названы
#: отдельно, а не растворяться в общей корзине TRANSPORT_ONLY: «сняли
#: инструкцию» и «отняли источник» — разные утверждения, и второе должно быть
#: видно в отчёте, даже когда оно принято сознательно.
_DATA_AVAILABILITY_MARKERS: tuple[tuple[str, str, str], ...] = (
    (
        "md_file_context",
        "**MD file** (for context)",
        "Ветка Claude CLI давала модели путь к Markdown и рассчитывала на Read. "
        "Ни ветка API (OpenRouter), ни provider-маршрут его не вкладывают: "
        "полный документ рядом с двумя артефактами этапов удвоил бы промпт, а "
        "продовый безинструментный путь свода этого источника не имеет. "
        "Разница унаследована от решения о ветке API и в 11E не вводится; "
        "в промпте про неё сказано прямо (INPUT_DATA_NOTE, пункт 3), чтобы "
        "модель не ссылалась на то, чего не видела.",
    ),
)


def _norm(line: str) -> str:
    return re.sub(r"\s+", " ", line).strip()


def _classify_missing(line: str, provider_head: str, provider_all: str,
                      provider_norm: set[str]) -> tuple[str, str]:
    for name, pattern in _TRANSPORT_PATTERNS:
        if pattern.search(line):
            return "TRANSPORT_ONLY", name
    if _norm(line) in provider_norm:
        return "FORMAT_ONLY", "whitespace"
    if _norm(line) and _norm(line) in _norm(provider_all):
        return "ORDER_ONLY", "moved"
    return "CONTENT_REMOVED", ""


def _severity_definition_sources() -> dict[str, Any]:
    """Где на платформе записаны определения значений severity.

    Ищутся не сами слова (они есть везде как перечень), а РАСШИФРОВКА хотя бы
    одного значения — то, без чего шкала не имеет смысла.
    """
    needles = ("нельзя строить", "cannot be built")
    hits: dict[str, list[str]] = {}
    roots = {
        "prompts": REPO_ROOT / "prompts",
        "disciplines": REPO_ROOT / "disciplines",
        "root_claude_md": REPO_ROOT / "CLAUDE.md",
        "provider_transport": REPO_ROOT / "backend" / "app" / "pipeline" / "stages",
    }
    for name, root in roots.items():
        found: list[str] = []
        paths = [root] if root.is_file() else sorted(root.rglob("*"))
        for path in paths:
            if not path.is_file() or path.suffix not in (".md", ".py"):
                continue
            try:
                text = path.read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue
            if any(needle in text for needle in needles):
                found.append(str(path.relative_to(REPO_ROOT)))
        hits[name] = found
    return hits


def main() -> int:
    parser = argparse.ArgumentParser(description="11E: сверка смысла промптов свода")
    parser.add_argument("--inputs-dir", required=True,
                        help="каталог с 02_text_analysis.json и 01_blocks_analysis.json")
    parser.add_argument("--section", required=True)
    parser.add_argument("--md-name", default=None)
    parser.add_argument("--out", required=True)
    parser.add_argument("--prompts-dir", default=str(REPO_ROOT / "prompts"))
    args = parser.parse_args()

    inputs_dir = Path(args.inputs_dir).resolve()
    os.environ["AUDIT_PROMPTS_DIR"] = str(Path(args.prompts_dir).resolve())
    os.environ["AUDIT_DISCIPLINE_PROFILE_STRICT"] = "1"

    workspace = Path(tempfile.mkdtemp(prefix="11e-semantic-"))
    try:
        vdir = workspace / "project" / "doc" / "v1"
        out = vdir / "_output"
        out.mkdir(parents=True)
        for name in ("02_text_analysis.json", "01_blocks_analysis.json"):
            source = inputs_dir / name
            if not source.is_file():
                raise SystemExit(f"нет обязательного входа: {source}")
            shutil.copy2(source, out / name)
        md_files = sorted(p for p in inputs_dir.glob("*.md") if p.is_file())
        if args.md_name:
            md_files = [p for p in md_files if p.name == args.md_name]
        md_name = ""
        if md_files:
            shutil.copy2(md_files[0], vdir / md_files[0].name)
            md_name = md_files[0].name

        project_info = {
            "project_id": f"{args.section}/11e",
            "name": "11e",
            "section": args.section,
            "md_file": md_name,
        }
        (vdir / "project_info.json").write_text(
            json.dumps(project_info, ensure_ascii=False, indent=2), encoding="utf-8",
        )

        from backend.app.services.common import audit_scope
        import backend.app.pipeline.stages.prepare.prompt_builder as prompt_builder
        from backend.app.pipeline.stages.prepare.task_builder import (
            prepare_findings_merge_task,
        )
        from backend.app.pipeline.stages.findings_merge import provider_transport

        with audit_scope.bind_audit_scope(
            output_dir=out, version_dir=vdir,
            project_id=project_info["project_id"], version_id="v1",
        ):
            legacy = prepare_findings_merge_task(
                project_info, project_info["project_id"]
            )
            messages = prompt_builder.build_findings_merge_messages(
                project_info, project_info["project_id"]
            )
            inputs = provider_transport.resolve_merge_inputs(out)

        built = provider_transport.build_provider_prompt(messages)
        provider_all = built["prompt"]
        provider_head = provider_all.split("===== STAGE OUTPUTS TO CONSOLIDATE", 1)[0]
        provider_norm = {_norm(line) for line in provider_head.splitlines() if _norm(line)}

        # ── Классификация строк legacy ───────────────────────────────────────
        buckets: dict[str, list[dict[str, Any]]] = {}
        counts: dict[str, int] = {}
        for number, line in enumerate(legacy.splitlines(), start=1):
            if not line.strip():
                continue
            if line in provider_head:
                verdict, reason = "PRESERVED", ""
            else:
                verdict, reason = _classify_missing(
                    line, provider_head, provider_all, provider_norm
                )
            counts[verdict] = counts.get(verdict, 0) + 1
            if verdict != "PRESERVED":
                buckets.setdefault(verdict, []).append({
                    "line_no": number,
                    "reason": reason,
                    # Строка инструкций — не данные заказчика, её можно
                    # показывать; обрезка только чтобы отчёт читался.
                    "text": line.strip()[:200],
                })

        # ── Строки, добавленные provider-режимом ─────────────────────────────
        legacy_lines = {line for line in legacy.splitlines() if line.strip()}
        legacy_norm = {_norm(x) for x in legacy_lines}
        added: dict[str, int] = {}
        added_unclassified: list[str] = []
        current_source = "instructions_head"
        # Смотрятся ОБЕ инструкционные части: голова (справка о входе, смысл
        # severity) и хвост после payload (контракт транспорта). Обход только
        # головы давал бы «transport_contract: 0» — то есть отчёт молчал бы про
        # блок, который в промпте стоит последним и потому весит больше всего.
        tail = provider_all.split("===== END OF STAGE OUTPUTS =====", 1)
        instruction_lines = provider_head.splitlines()
        if len(tail) > 1:
            instruction_lines += tail[1].splitlines()
        for line in instruction_lines:
            for name, marker in _ADDED_SOURCES:
                if line.startswith(marker):
                    current_source = name
            if not line.strip() or line in legacy_lines:
                continue
            if _norm(line) in legacy_norm:
                continue
            added[current_source] = added.get(current_source, 0) + 1
            if current_source == "instructions_head":
                added_unclassified.append(line.strip()[:200])

        marker_report = provider_transport.semantic_preservation_report(
            api_prompt="\n\n".join(m["content"] for m in messages),
            provider_prompt=provider_all,
        )
        input_facts = inputs.as_facts()
        coverage = provider_transport.input_coverage_report(
            provider_all, input_facts["expected_input_finding_ids"],
        )

        report = {
            "kind": "11E §13 — построчная сверка смысла промптов свода",
            "model_calls": 0,
            "base_A": {
                "what": "prepare_findings_merge_task (ветка Claude CLI, файловые инструменты)",
                "why": (
                    "именно её заменяет 11E на воркере; сверка с API-промптом "
                    "слепа к тому, что уже снял _clean_template_for_api (урок 11D.1)"
                ),
                "chars": len(legacy),
                "sha256": hashlib.sha256(legacy.encode("utf-8")).hexdigest(),
                "non_empty_lines": sum(1 for l in legacy.splitlines() if l.strip()),
            },
            "base_B": {
                "what": "provider_transport.build_provider_prompt поверх build_findings_merge_messages",
                "instructions_chars": built["system_chars"],
                "payload_chars": built["payload_chars"],
                "prompt_chars": built["prompt_chars"],
                "prompt_sha256": hashlib.sha256(
                    provider_all.encode("utf-8")
                ).hexdigest(),
                "absolute_paths_in_instructions": built[
                    "absolute_paths_remaining_in_instructions"
                ],
                "severity_semantics_anchor": built["map"]["severity_semantics_anchor"],
            },
            "line_classification": {
                "counts": counts,
                "details": buckets,
            },
            "content_added": {
                "by_source": added,
                "unclassified_lines": added_unclassified,
                "payload_note": (
                    "полезная нагрузка (два артефакта этапов) в classification не "
                    "участвует: в legacy её не было по построению — модель должна "
                    "была прочитать файлы сама. Её объём — в base_B.payload_chars"
                ),
            },
            "data_availability_delta": [
                {
                    "id": name,
                    "legacy_line_present": marker in legacy,
                    "available_in_provider_run": False,
                    "declared_in_prompt": (
                        "NOT available in this run" in provider_head
                    ),
                    "rationale": rationale,
                }
                for name, marker, rationale in _DATA_AVAILABILITY_MARKERS
            ],
            "marker_report_vs_api_prompt": marker_report,
            "input_contract": {
                key: value for key, value in input_facts.items()
                if key not in ("text_finding_ids", "block_finding_ids",
                               "expected_input_finding_ids")
            },
            "input_coverage": {
                key: value for key, value in coverage.items()
                if key not in ("expected_ids", "encoded_ids")
            },
            "hidden_context": {
                "question": (
                    "где на платформе записаны ОПРЕДЕЛЕНИЯ значений severity "
                    "(а не их перечень)"
                ),
                "search_needles": ["нельзя строить", "cannot be built"],
                "sources": _severity_definition_sources(),
            },
            "verdict": {
                "content_removed": counts.get("CONTENT_REMOVED", 0),
                "unknown": counts.get("UNKNOWN", 0),
                "engineering_lost_vs_api": marker_report["engineering_lost"],
                "transport_leaked": marker_report["transport_markers_leaked"],
                "input_missing_before_inference": coverage["missing_before_inference"],
                "passed": (
                    counts.get("CONTENT_REMOVED", 0) == 0
                    and counts.get("UNKNOWN", 0) == 0
                    and marker_report["passed"]
                    and coverage["passed"]
                ),
            },
        }

        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8",
        )
        print(json.dumps(report["verdict"], ensure_ascii=False, indent=2))
        print(f"отчёт: {out_path}")
        return 0 if report["verdict"]["passed"] else 1
    finally:
        shutil.rmtree(workspace, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
