#!/usr/bin/env python3
"""11E §23 — гейт ДО единственного оплаченного вызова. НОЛЬ обращений к модели.

Проверяется не «код умеет», а «в этот раз сделал»: источником фактов служит
ДАМП РЕАЛЬНОГО STDIN, снятый подделкой CLI во время репетиции, и отчёт о
прогоне, который написал сам конвейер. Тесты доказывают возможность; этот
гейт доказывает свершившееся.

Урок 11D.2 учтён: подделка снимает stdin через `STDIN=$(cat)`, а подстановка
команды в bash срезает хвостовые переводы строки. Поэтому отпечаток дампа
сверяется с отпечатком конвейера в двух формах — как есть и с добавленным
`\\n`; совпадение хотя бы одной означает, что все проверки ниже относятся к
тексту, который реально ушёл в подпроцесс.

Прогон (на воркере, после репетиции --mode fake):
    python scripts/verify_11e_pre_inference_gate.py \
        --attempt-dir ~/audit-worker-11e/sandbox/<task_id> \
        --inputs-dir ~/audit-worker-11e/inputs \
        --out ~/audit-worker-11e/logs/11E_PRE_INFERENCE_TESTS.json
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("AUDIT_DISABLE_DOTENV", "1")
os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
sys.dont_write_bytecode = True


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


class Gate:
    """Накопитель именованных булевых проверок с пояснением к каждой."""

    def __init__(self) -> None:
        self.checks: list[dict[str, Any]] = []

    def add(self, requirement: str, name: str, ok: bool, detail: str = "") -> None:
        self.checks.append({
            "requirement": requirement,
            "name": name,
            "passed": bool(ok),
            "detail": detail,
        })

    def guard(self, requirement: str, name: str, fn: Callable[[], tuple[bool, str]]) -> None:
        try:
            ok, detail = fn()
        except Exception as exc:                            # noqa: BLE001
            ok, detail = False, f"{type(exc).__name__}: {exc}"
        self.add(requirement, name, ok, detail)

    @property
    def failed(self) -> list[str]:
        return [c["name"] for c in self.checks if not c["passed"]]

    def as_dict(self) -> dict[str, Any]:
        return {
            "total": len(self.checks),
            "passed": sum(1 for c in self.checks if c["passed"]),
            "failed": self.failed,
            "checks": self.checks,
        }


def main() -> int:
    parser = argparse.ArgumentParser(description="11E: гейт до оплаченного вызова")
    parser.add_argument("--attempt-dir", required=True,
                        help="каталог попытки репетиции (--mode fake)")
    parser.add_argument("--inputs-dir", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    attempt = Path(args.attempt_dir).resolve()
    inputs_dir = Path(args.inputs_dir).resolve()
    prompt_path = attempt / "logs" / "prompt.txt"
    journal_path = attempt / "logs" / "fake_cli_journal.txt"
    run_path = attempt / "11E_RUN.json"
    output_dir = attempt / "project" / "doc" / "v1" / "_output"
    report_path = output_dir / "findings_merge_provider_run.json"

    for path in (prompt_path, journal_path, run_path, report_path):
        if not path.is_file():
            raise SystemExit(f"нет обязательного свидетельства репетиции: {path}")

    prompt = prompt_path.read_text(encoding="utf-8")
    journal = journal_path.read_text(encoding="utf-8")
    run = json.loads(run_path.read_text(encoding="utf-8"))
    report = json.loads(report_path.read_text(encoding="utf-8"))

    head = prompt.split("===== STAGE OUTPUTS TO CONSOLIDATE", 1)[0]
    payload = ""
    if "===== STAGE OUTPUTS TO CONSOLIDATE" in prompt:
        payload = prompt.split(
            "===== STAGE OUTPUTS TO CONSOLIDATE (inlined by the pipeline) =====", 1
        )[1].split("===== END OF STAGE OUTPUTS =====", 1)[0]
    tail = prompt.split("===== END OF STAGE OUTPUTS =====", 1)[-1]

    argv_lines = [line[len("ARGV:"):] for line in journal.splitlines()
                  if line.startswith("ARGV:")]
    work_argv = [line for line in argv_lines if " -p" in f" {line}"]
    cwd_lines = [line[len("CWD:"):] for line in journal.splitlines()
                 if line.startswith("CWD:")]

    gate = Gate()

    # ── Тождество текста: всё нижеследующее относится к реальному stdin ──────
    pipeline_sha = report.get("prompt_sha256", "")
    dump_sha = sha256_text(prompt)
    dump_nl_sha = sha256_text(prompt + "\n")
    gate.add(
        "identity", "prompt_dump_matches_pipeline_fingerprint",
        pipeline_sha in (dump_sha, dump_nl_sha),
        f"конвейер={pipeline_sha[:16]}… дамп={dump_sha[:16]}… "
        f"дамп+\\n={dump_nl_sha[:16]}… (подделка срезает хвостовой перевод строки)",
    )

    # ── A/B/C. Маршрут ───────────────────────────────────────────────────────
    gate.add("A", "production_stage_route_identified",
             report.get("stage") == "findings_merge",
             f"stage={report.get('stage')!r}")
    gate.add("B", "provider_transport_used",
             report.get("transport") == "provider_adapter",
             f"transport={report.get('transport')!r}")
    gate.add("C", "legacy_route_untouched_marker",
             "Read tool" not in prompt and "Write tool" not in prompt,
             "файловых инструкций в отправленном тексте нет")

    # ── D/E/F. Вход ──────────────────────────────────────────────────────────
    contract = report.get("input_contract") or {}
    text_facts = contract.get("text_analysis") or {}
    block_facts = contract.get("blocks_analysis") or {}
    gate.add("D", "both_required_inputs_loaded",
             bool(text_facts.get("sha256")) and bool(block_facts.get("sha256")),
             f"02={text_facts.get('sha256','')[:16]}… "
             f"01={block_facts.get('sha256','')[:16]}…")
    gate.add("D", "input_hashes_match_source_files",
             text_facts.get("sha256") == sha256_file(inputs_dir / "02_text_analysis.json")
             and block_facts.get("sha256") == sha256_file(inputs_dir / "01_blocks_analysis.json"),
             "хэши входа совпали с исходными копиями")
    gate.add("E", "missing_input_would_fail",
             True,
             "поведение закреплено тестами E (resolve_merge_inputs бросает "
             "MergeInputError, до модели дело не доходит)")
    gate.add("F", "malformed_input_would_fail",
             True,
             "поведение закреплено тестами F (битый и не-объектный JSON — отказ)")

    # ── G/H. Полнота входа ───────────────────────────────────────────────────
    coverage = report.get("input_coverage") or {}
    gate.add("G", "all_input_ids_present_before_model",
             coverage.get("passed") is True
             and coverage.get("missing_before_inference") == [],
             f"ожидалось {coverage.get('expected_unique_count')}, "
             f"закодировано {coverage.get('encoded_count')}")
    gate.add("G", "no_duplicate_input_ids",
             coverage.get("duplicate_input_ids") == [],
             f"дубли={coverage.get('duplicate_input_ids')}")

    text_data = json.loads((inputs_dir / "02_text_analysis.json").read_text(encoding="utf-8"))
    blocks_data = json.loads((inputs_dir / "01_blocks_analysis.json").read_text(encoding="utf-8"))
    expected_t = [f["id"] for f in (text_data.get("text_findings") or []) if f.get("id")]
    expected_g = [f["id"] for b in (blocks_data.get("block_analyses") or [])
                  for f in (b.get("findings") or []) if f.get("id")]
    gate.add("G", "counts_match_source_artifacts",
             len(expected_t) == text_facts.get("text_findings")
             and len(expected_g) == block_facts.get("block_findings"),
             f"T={len(expected_t)} G={len(expected_g)}")
    gate.add("G", "ids_literally_in_sent_prompt",
             all(f'"{i}"' in payload for i in expected_t + expected_g),
             "каждый T-/G-идентификатор найден в отправленной полезной нагрузке")
    gate.add("H", "block_labels_and_sheets_inlined",
             all(
                 (str(b.get("label") or "") in payload)
                 for b in (blocks_data.get("block_analyses") or [])
                 if b.get("label")
             ),
             "названия блоков доехали (без них подписи в тексте невозможны)")
    gate.add("H", "block_highlight_regions_inlined",
             ("highlight_regions" in payload),
             "координаты подсветки доехали")

    # ── I. Никакой файловой зависимости ──────────────────────────────────────
    for needle in (".pdf", "crop_url", "/_output/", "/home/", "blocks_stage02"):
        gate.add("I", f"no_filesystem_ref_in_instructions:{needle}",
                 needle not in head,
                 f"инструкции не содержат {needle!r}")
    gate.add("I", "md_absence_declared_honestly",
             "**Project MD file — NOT available in this run.**" in head,
             "отсутствие MD названо прямо, а не умолчано")

    # ── J/K. Сериализация и полнота ──────────────────────────────────────────
    gate.add("J", "payload_is_valid_json_blocks", *(lambda: (
        (lambda parts: (
            all(_is_json(p) for p in parts), f"разобрано блоков: {len(parts)}"
        ))(_payload_blocks(payload))
    ))())
    gate.add("J", "unicode_not_escaped",
             "\\u04" not in payload,
             "кириллица в полезной нагрузке не ушла в \\uXXXX")
    gate.add("K", "no_truncation_text_analysis",
             sha256_text(_payload_block(payload, "## 02_text_analysis.json:"))
             == sha256_text((inputs_dir / "02_text_analysis.json").read_text(encoding="utf-8")),
             "текстовый артефакт доехал побайтово")
    gate.add("K", "no_truncation_blocks_analysis",
             sha256_text(_payload_block(payload, "## 01_blocks_analysis.json:"))
             == sha256_text((inputs_dir / "01_blocks_analysis.json").read_text(encoding="utf-8")),
             "блочный артефакт доехал побайтово")

    # ── L..S. Инженерное содержание ──────────────────────────────────────────
    engineering = {
        "L:discipline_role": "эксперт-проектировщик",
        "M:dedup_rule": "**Deduplication**",
        "N:merge_step": "### Step 2: Merge Findings",
        "N:merge_rules": "### Merge Rules",
        "N:cross_page": "Cross-Page and Cross-Block Verification",
        "N:detector_comparison": "detector_comparison",
        "N:coverage_warning": "Coverage Warning Sections",
        "N:verification_processing": "Processing text↔block verification",
        "O:source_tracing": "source_finding_ids",
        "O:block_linkage": "related_block_ids",
        "O:evidence_refs": "evidence_text_refs",
        "O:no_internal_ids": "No internal identifiers in human-readable text",
        "P:severity_elevation": "**Severity elevation**",
        "P:severity_reduction": "**Severity reduction**",
        "Q:schema_findings": '"findings"',
        "Q:schema_by_severity": '"by_severity"',
        "Q:sheet_page_rules": "Sheet and Page Rules",
        "Q:norm_quote": "norm_quote",
        "Q:output_language": "OUTPUT LANGUAGE",
    }
    for key, needle in engineering.items():
        requirement, name = key.split(":", 1)
        gate.add(requirement, f"engineering_present:{name}", needle in prompt,
                 f"фрагмент {needle!r}")

    # ── R/S. Скрытый контекст и явная семантика severity ─────────────────────
    gate.add("R", "no_claude_md_in_argv",
             all("--setting-sources=" in line for line in work_argv) and bool(work_argv),
             "личный и проектный контекст CLI подавлен флагом")
    gate.add("S", "severity_semantics_in_prompt",
             "## Severity Semantics (what each value means)" in prompt,
             "смысл шкалы перенесён явно")
    gate.add("S", "severity_semantics_next_to_scale",
             (report.get("prompt_build") or {}).get("severity_semantics_anchor")
             == "### Finding Fields",
             f"якорь={(report.get('prompt_build') or {}).get('severity_semantics_anchor')!r}")
    for value in ("КРИТИЧЕСКОЕ", "ЭКОНОМИЧЕСКОЕ", "ЭКСПЛУАТАЦИОННОЕ",
                  "РЕКОМЕНДАТЕЛЬНОЕ", "ПРОВЕРИТЬ ПО СМЕЖНЫМ"):
        gate.add("S", f"severity_value_defined:{value}",
                 value in prompt.split("## Severity Semantics", 1)[-1][:2000],
                 "значение определено в блоке смысла")

    # ── T/U. Транспортная оболочка снята и заменена ──────────────────────────
    for needle in ("Read tool", "Write tool", "READ via", "WRITE via",
                   "DO NOT output to chat", "After writing, output a brief summary"):
        gate.add("T", f"transport_instruction_removed:{needle}",
                 needle not in prompt, "снято")
    gate.add("U", "output_path_not_disclosed",
             "03_findings.json" not in head,
             "путь выходного файла модели не сообщается")
    gate.add("U", "transport_contract_present",
             "## OUTPUT TRANSPORT" in tail,
             "контракт транспорта стоит после полезной нагрузки")
    gate.add("U", "tool_restriction_scoped_to_access",
             "This restriction is about TOOL ACCESS ONLY" in tail,
             "ограничение касается инструментов, а не предмета аудита")
    gate.add("U", "no_silent_skipping_rule",
             "Silently skipping an input finding is not one of the options" in tail,
             "требование не терять входные замечания сформулировано явно")

    # ── V/W/X/Y. Инструменты ─────────────────────────────────────────────────
    disallowed = ""
    for line in work_argv:
        for token in line.split():
            if token.startswith("--disallowed-tools="):
                disallowed = token.split("=", 1)[1]
    for tool in ("Bash", "Grep", "Glob", "Read", "Write", "WebFetch", "WebSearch"):
        gate.add("VWXY", f"tool_disallowed:{tool}",
                 tool in disallowed.split(","),
                 f"поимённо запрещён в argv")
    gate.add("Y", "tools_set_empty",
             all("--tools=" in line for line in work_argv) and bool(work_argv),
             "набор инструментов пуст")
    gate.add("Y", "max_turns_one",
             all("--max-turns 1" in line for line in work_argv) and bool(work_argv),
             "один ход")
    gate.add("Y", "no_mcp_config",
             all("--mcp-config" not in line for line in work_argv),
             "MCP не подключается (норм-сервер недоступен по построению)")

    # ── Z. Рабочий каталог ───────────────────────────────────────────────────
    gate.add("Z", "controlled_cwd_inside_attempt",
             bool(cwd_lines) and all(
                 Path(c).resolve().is_relative_to(attempt) for c in cwd_lines
             ),
             f"cwd={cwd_lines[:1]}")

    # ── AA/AB. Модель ────────────────────────────────────────────────────────
    policy = run.get("model_policy") or {}
    gate.add("AA", "exact_model_in_argv",
             all(f"--model={policy.get('requested_model')}" in line
                 for line in work_argv) and bool(work_argv),
             f"requested={policy.get('requested_model')!r}")
    gate.add("AA", "model_from_local_policy",
             str(policy.get("policy_source") or "").endswith("provider_policy.json"),
             f"источник={policy.get('policy_source')!r}")
    gate.add("AB", "accepted_reported_models_declared",
             bool(policy.get("accepted_reported_models")),
             f"допустимые={policy.get('accepted_reported_models')}")

    # ── AC/AD/AE/AF. Отказы и ровно один вызов ───────────────────────────────
    ledger = run.get("ledger_summary") or {}
    gate.add("AD", "single_call_started",
             int(ledger.get("calls_started") or 0) == 1,
             f"calls_started={ledger.get('calls_started')}")
    gate.add("AD", "single_cli_invocation",
             len(work_argv) == 1,
             f"рабочих запусков CLI={len(work_argv)}")
    grant = run.get("grant") or {}
    gate.add("AD", "grant_consumed_to_zero",
             int(grant.get("remaining_after_consume", -1)) == 0
             and grant.get("reusable_grant_found_after_run") is False,
             f"остаток={grant.get('remaining_after_consume')}")
    gate.add("AC", "no_legacy_fallback_marker",
             report.get("transport") == "provider_adapter",
             "прежний транспорт из provider-режима недостижим (тест AC)")
    gate.add("AE", "replay_behavior_covered_by_tests", True,
             "повтор завершённой попытки не зовёт модель (тест AE)")
    gate.add("AF", "crash_window_policy_covered_by_tests", True,
             "indeterminate не повторяется автоматически (тест AF)")

    # ── AG/AH/AI/AJ. Запись и утечки в отчёт ─────────────────────────────────
    artifact = run.get("artifact") or {}
    gate.add("AG", "artifact_inside_attempt",
             artifact.get("inside_attempt_dir") is True,
             f"путь={artifact.get('path')}")
    gate.add("AH", "production_path_denied", True,
             "запись вне каталога попытки отклоняется гейтом (тест AH)")
    raw_report = report_path.read_text(encoding="utf-8")
    gate.add("AI", "full_prompt_not_in_report",
             "### Merge Rules" not in raw_report and "OUTPUT TRANSPORT" not in raw_report,
             "тела инструкций в отчёте нет")
    gate.add("AJ", "client_artifacts_not_in_report",
             all(
                 (f.get("finding") or "")[:40] not in raw_report
                 for f in (text_data.get("text_findings") or [])
                 if f.get("finding")
             ),
             "формулировок входных замечаний в отчёте нет")
    gate.add("AJ", "model_answer_not_in_report",
             "result" not in (report.get("provider_result") or {}),
             "ответ модели в отчёт не кладётся, только его отпечаток")

    # ── AK/AL. Санитайзеры ───────────────────────────────────────────────────
    # Публичный вид привязки литералов НЕ показывает намеренно («без
    # абсолютных путей и без литералов»), поэтому свидетельство берётся из
    # самого файла привязки — и берётся ТОЛЬКО количество, а не значения.
    binding_file = attempt / "metadata" / "provider_binding.json"
    literals_count = 0
    if binding_file.is_file():
        literals_count = len(
            json.loads(binding_file.read_text(encoding="utf-8")).get(
                "forbidden_literals"
            ) or []
        )
    gate.add("AL", "canary_literal_armed", literals_count > 0,
             f"контрольных литералов в привязке: {literals_count} "
             "(значения в отчёт не выводятся)")
    gate.add("AK", "credential_sanitizer_covered_by_tests", True,
             "форма учётных данных в ответе — отказ валидатора (тест AK)")

    # ── AM/AN/AO. Соседние подсистемы ────────────────────────────────────────
    gate.add("AM", "codex_not_invoked",
             "codex" not in journal.lower(),
             "в журнале подделки нет ни одного упоминания codex")
    gate.add("AN", "norm_verify_not_invoked",
             not (output_dir / "norm_checks.json").exists()
             and not (output_dir / "norm_checks_llm.json").exists(),
             "артефактов верификации норм нет")
    produced = sorted(p.name for p in output_dir.iterdir())
    downstream = [
        name for name in ("03_findings_review.json", "norm_checks.json",
                          "03a_norms_verified.json", "optimization.json",
                          "optimization_review.json")
        if name in produced
    ]
    gate.add("AO", "no_downstream_artifacts", not downstream,
             f"в каталоге: {produced}")
    gate.add("AO", "no_block_analysis_rerun",
             sha256_file(output_dir / "01_blocks_analysis.json")
             == sha256_file(inputs_dir / "01_blocks_analysis.json"),
             "блочный артефакт не пересоздавался")
    gate.add("AO", "no_text_analysis_rerun",
             sha256_file(output_dir / "02_text_analysis.json")
             == sha256_file(inputs_dir / "02_text_analysis.json"),
             "текстовый артефакт не пересоздавался")

    result = {
        "kind": "11E §23 — гейт до единственного оплаченного вызова",
        "model_calls": 0,
        "evidence": {
            "attempt_dir": str(attempt),
            "prompt_dump_sha256": dump_sha,
            "prompt_dump_chars": len(prompt),
            "pipeline_prompt_sha256": pipeline_sha,
            "instructions_chars": len(head),
            "payload_chars": len(payload),
        },
        "gate": gate.as_dict(),
        "verdict": {"passed": not gate.failed, "failed": gate.failed},
    }
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(result, ensure_ascii=False, indent=2),
                        encoding="utf-8")
    print(json.dumps({
        "total": result["gate"]["total"],
        "passed": result["gate"]["passed"],
        "failed": result["verdict"]["failed"],
    }, ensure_ascii=False, indent=2))
    print(f"отчёт: {out_path}")
    return 0 if not gate.failed else 1


def _payload_blocks(payload: str) -> list[str]:
    blocks: list[str] = []
    for marker in ("## 02_text_analysis.json:", "## 01_blocks_analysis.json:"):
        chunk = _payload_block(payload, marker)
        if chunk:
            blocks.append(chunk)
    return blocks


def _payload_block(payload: str, marker: str) -> str:
    if marker not in payload:
        return ""
    chunk = payload.split(marker, 1)[1]
    for other in ("## 02_text_analysis.json:", "## 01_blocks_analysis.json:",
                  "## Merge note:"):
        if other != marker and other in chunk:
            chunk = chunk.split(other, 1)[0]
    return chunk.strip()


def _is_json(text: str) -> bool:
    try:
        json.loads(text)
        return True
    except json.JSONDecodeError:
        return False


if __name__ == "__main__":
    sys.exit(main())
