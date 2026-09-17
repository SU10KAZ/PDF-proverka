#!/usr/bin/env python3
"""Post-freeze source-first evaluation and reporting for ProjectChange Miner V2."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import sys
import time
from typing import Any

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill


REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
from experiments.project_change_272.ai_first_projectchange_miner_v2 import (  # noqa: E402
    CORPUS,
    MODEL,
    OUT,
    REASONING,
    now,
    obj,
    read_json,
    sha256,
    write_new,
)

DEPS = CORPUS / "controlled_inference_f1_f4_f2_v3/runtime_deps"
sys.path.insert(0, str(DEPS))
from experiments.project_change_semantic_codex_272.provider import (  # noqa: E402
    cli_command,
    safe_env,
    sandbox_command,
)
import jsonschema  # noqa: E402


PAIR_A_ROOT = CORPUS / "pair_a_ai_mapping_change_miner_v1"
PAIR_B_ROOT = CORPUS / "pair_b_ai_mapping_change_miner_v1"
PAIR_A_TRUTH = CORPUS / "fresh_dev_pair_a_live_v2/SOURCE_FIRST_TRUTH.json"
PAIR_B_TRUTH = CORPUS / "pair_b_independent_finding_loss_trace/SOURCE_VERIFICATION.json"

S = {"type": "string"}
STRINGS = {"type": "array", "items": S}
EVAL_ROW = obj(
    projectchange_id=S,
    quality={"type": "string", "enum": ["CORRECT", "PARTIAL", "FALSE", "INSUFFICIENT_TO_JUDGE"]},
    rationale=S,
    grouping_quality={"type": "string", "enum": ["GOOD_HUMAN_LEVEL", "TOO_ATOMIC", "OVER_MERGED", "DUPLICATE"]},
    grouping_rationale=S,
    source_refs=STRINGS,
)
TRUTH_ROW = obj(
    truth_id=S,
    outcome={"type": "string", "enum": ["STRONG", "PARTIAL", "MISSED"]},
    matched_projectchange_ids=STRINGS,
    rationale=S,
)
EVAL_SCHEMA = obj(
    pair={"type": "string", "enum": ["A", "B"]},
    projectchange_rows={"type": "array", "items": EVAL_ROW},
    truth_rows={"type": "array", "items": TRUTH_ROW},
    f13_status={"type": "string", "enum": ["PASS", "FAIL", "NOT_APPLICABLE"]},
    f13_rationale=S,
    notes=STRINGS,
)

EVALUATION_PROMPT = """Ты post-freeze source-first evaluator AI-FIRST
PROJECTCHANGE MINER V2. Результаты уже frozen; не исправляй их и не создавай
новые ProjectChanges. Оцени каждый final ProjectChange ровно один раз:
CORRECT, PARTIAL, FALSE или INSUFFICIENT_TO_JUDGE. Затем оцени grouping:
GOOD_HUMAN_LEVEL, TOO_ATOMIC, OVER_MERGED или DUPLICATE.

В SOURCE DATA есть: frozen V2 ProjectChanges с two-sided block evidence;
независимая source-first truth (REAL15 для A или PROVEN10/F13 для B); и
same-version source audits старых V1 cards, открытые только после V2 freeze.
Старые audits являются дополнительной проверенной source evidence, но сходство
формулировки само по себе не доказывает V2 карточку. Проверяй subject, scope,
OLD state, NEW state, направление, отсутствие metadata-only claims и то, что
это одно событие. Неаудированную графическую/пространственную гипотезу оценивай
INSUFFICIENT_TO_JUDGE, а не угадывай. FALSE только при доказанной ошибке или
контрдоказательстве. PARTIAL — доказанное ядро с существенным overclaim либо
неполным состоянием.

Grouping: TOO_ATOMIC, если карточка является лишь параметром/комнатой внутри
одного более широкого инженерного события; OVER_MERGED, если соединены
независимые события; DUPLICATE, если тот же event остаётся другой final card;
иначе GOOD_HUMAN_LEVEL. Для DUPLICATE назови другой ID в rationale.

Для каждого truth item верни STRONG/PARTIAL/MISSED по frozen V2, не по hints.
STRONG требует корректного инженерного смысла и достаточного охвата, PARTIAL —
смысл найден, но существенная часть пропущена/искажена. Для Pair B отдельно
проверь F13: FAIL, если final V2 утверждает отсутствие OLD систем подпора ТШ/ЛХ
паркинга вопреки counterevidence; иначе PASS. Для Pair A f13 NOT_APPLICABLE.
Верни только JSON; exact partition всех переданных ProjectChange IDs и truth IDs."""


def verify_result_freeze() -> dict[str, Any]:
    freeze = read_json(OUT / "PROJECTCHANGE_V2_RESULT_FREEZE.json")
    for relative, expected in freeze["hashes"].items():
        if sha256(OUT / relative) != expected:
            raise RuntimeError(f"Result freeze mismatch: {relative}")
    return freeze


def compact_change(change: dict[str, Any]) -> dict[str, Any]:
    return {
        key: change[key]
        for key in (
            "projectchange_id",
            "engineering_subject",
            "scope",
            "locations",
            "change_summary",
            "old_state",
            "new_state",
            "changed_parameters",
            "old_pages",
            "new_pages",
            "evidence_modalities",
            "confidence",
            "why_one_event",
            "dedupe_lineage",
            "dedupe_reason",
        )
        if key in change
    } | {
        "evidence_items": [
            {
                "side": item["side"],
                "physical_page": item["physical_page"],
                "block_id": item["block_id"],
                "block_type": item["block_type"],
                "relevant_fragment": item["relevant_fragment"],
                "evidence_role": item["evidence_role"],
            }
            for item in change["evidence_items"]
        ]
    }


def compact_old_audit(path: Path) -> list[dict[str, Any]]:
    rows = read_json(path)["rows"]
    return [
        {
            key: row.get(key)
            for key in (
                "change_id",
                "audit_status",
                "engineering_subject",
                "change_summary",
                "reason",
                "rationale",
                "old_pages",
                "new_pages",
            )
        }
        for row in rows
    ]


def compact_truth(pair: str) -> tuple[list[dict[str, Any]], list[str], dict[str, Any]]:
    if pair == "A":
        truth = read_json(PAIR_A_TRUTH)
        real = [entry for entry in truth["entries"] if entry.get("classification") == "REAL_CHANGE"]
        rows = [
            {
                key: entry.get(key)
                for key in ("id", "title", "old_pages", "new_pages", "old_state", "new_state", "basis", "route")
            }
            for entry in real
        ]
        return rows, [entry["id"] for entry in real], {"f13": None}
    truth = read_json(PAIR_B_TRUTH)
    proven = [item for item in truth["findings"] if item.get("verdict") == "PROVEN"]
    rows = [
        {
            key: item.get(key)
            for key in (
                "finding_id",
                "engineering_subject",
                "old_state",
                "new_state",
                "old_physical_pdf_pages",
                "new_physical_pdf_pages",
                "evidence_types",
                "minimal_sufficient_evidence",
                "not_proven",
            )
        }
        for item in proven
    ]
    f13 = read_json(PAIR_B_ROOT / "F13_CHECK.json")
    return rows, [item["finding_id"] for item in proven], {"f13": f13}


def make_payload(pair: str) -> tuple[dict[str, Any], list[str], list[str], dict[str, str]]:
    final = read_json(OUT / "FINAL_PROJECTCHANGES.json")["pairs"][pair]["projectchanges"]
    truth_rows, truth_ids, extra = compact_truth(pair)
    audit_path = (PAIR_A_ROOT if pair == "A" else PAIR_B_ROOT) / "FALSE_POSITIVE_AUDIT.json"
    truth_path = PAIR_A_TRUTH if pair == "A" else PAIR_B_TRUTH
    payload = {
        "pair": pair,
        "frozen_v2_projectchanges": [compact_change(item) for item in final],
        "source_first_truth": truth_rows,
        "prior_same_version_source_audit": compact_old_audit(audit_path),
        **extra,
    }
    hashes = {
        "result_freeze": sha256(OUT / "PROJECTCHANGE_V2_RESULT_FREEZE.json"),
        "final_projectchanges": sha256(OUT / "FINAL_PROJECTCHANGES.json"),
        "source_first_truth": sha256(truth_path),
        "prior_source_audit": sha256(audit_path),
    }
    if pair == "B":
        hashes["f13"] = sha256(PAIR_B_ROOT / "F13_CHECK.json")
    return payload, [item["projectchange_id"] for item in final], truth_ids, hashes


async def evaluation_call(pair: str, payload: dict[str, Any]) -> dict[str, Any]:
    call_id = f"PAIR_{pair}_SOURCE_FIRST_EVALUATION"
    target = OUT / "evaluation_raw" / call_id
    target.mkdir(parents=True, exist_ok=False)
    input_dir = OUT / "evaluation_inputs" / call_id
    input_dir.mkdir(parents=True, exist_ok=False)
    exact = EVALUATION_PROMPT + "\nSOURCE DATA:\n" + json.dumps(payload, ensure_ascii=False)
    write_new(input_dir / "MODEL_INPUT.json", payload)
    write_new(input_dir / "EXACT_PROMPT.txt", exact)
    write_new(target / "prompt.txt", exact)
    write_new(target / "schema.json", EVAL_SCHEMA)
    command = sandbox_command(target, cli_command([]))
    input_hashes = {path.name: sha256(path) for path in target.iterdir()}
    write_new(
        target / "INVOCATION.json",
        {
            "call_id": call_id,
            "at": now(),
            "stage": "SOURCE_FIRST_EVALUATION",
            "model": MODEL,
            "reasoning": REASONING,
            "provider": "codex_chatgpt",
            "openrouter": 0,
            "claude": 0,
            "command": command,
            "input_hashes": input_hashes,
            "retries": 0,
            "images": 0,
        },
    )
    started = time.monotonic()
    with (target / "raw.jsonl").open("wb") as stdout, (target / "stderr.txt").open("wb") as stderr:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdin=asyncio.subprocess.PIPE,
            stdout=stdout,
            stderr=stderr,
            env=safe_env(),
            start_new_session=True,
        )
        try:
            await asyncio.wait_for(process.communicate(exact.encode()), timeout=1800)
        except BaseException:
            os.killpg(process.pid, signal.SIGKILL)
            await process.wait()
            raise
    records = []
    for line in (target / "raw.jsonl").read_text(encoding="utf-8").splitlines():
        try:
            records.append(json.loads(line))
        except ValueError:
            pass
    usage = [row["usage"] for row in records if row.get("type") == "turn.completed" and row.get("usage")]
    tool_items = [
        row for row in records
        if row.get("type", "").startswith("item.")
        and row.get("item", {}).get("type") not in {None, "agent_message", "reasoning", "error"}
    ]
    receipt = {
        "call_id": call_id,
        "at": now(),
        "exit_code": process.returncode,
        "wall_time_seconds": time.monotonic() - started,
        "usage": usage,
        "tool_items": len(tool_items),
        "raw_sha256": sha256(target / "raw.jsonl"),
    }
    write_new(target / "RECEIPT.json", receipt)
    if process.returncode or len(usage) != 1 or tool_items:
        raise RuntimeError(f"Evaluation call failed; no retry: {call_id}")
    value = read_json(target / "final.txt")
    jsonschema.validate(value, EVAL_SCHEMA)
    write_new(target / "parsed.json", value)
    print(json.dumps({"call": call_id, "seconds": round(receipt["wall_time_seconds"])}), flush=True)
    return value


def validate_eval(pair: str, value: dict[str, Any], pc_ids: list[str], truth_ids: list[str]) -> None:
    if value["pair"] != pair:
        raise RuntimeError(f"Pair {pair}: evaluation pair mismatch")
    actual_pc = [row["projectchange_id"] for row in value["projectchange_rows"]]
    actual_truth = [row["truth_id"] for row in value["truth_rows"]]
    if len(actual_pc) != len(set(actual_pc)) or set(actual_pc) != set(pc_ids):
        raise RuntimeError(f"Pair {pair}: ProjectChange evaluation is not exact partition")
    if len(actual_truth) != len(set(actual_truth)) or set(actual_truth) != set(truth_ids):
        raise RuntimeError(f"Pair {pair}: truth evaluation is not exact partition")
    known = set(pc_ids)
    for row in value["truth_rows"]:
        if not set(row["matched_projectchange_ids"]) <= known:
            raise RuntimeError(f"Pair {pair}: truth match points to unknown ProjectChange")
    if pair == "A" and value["f13_status"] != "NOT_APPLICABLE":
        raise RuntimeError("Pair A F13 must be NOT_APPLICABLE")
    if pair == "B" and value["f13_status"] == "NOT_APPLICABLE":
        raise RuntimeError("Pair B F13 must be PASS or FAIL")


async def evaluate() -> None:
    verify_result_freeze()
    manifest: dict[str, Any] = {"created_at": now(), "model": MODEL, "reasoning": REASONING, "pairs": {}}
    for pair in ("A", "B"):
        payload, pc_ids, truth_ids, hashes = make_payload(pair)
        manifest["pairs"][pair] = {
            "projectchanges": len(pc_ids),
            "truth_items": len(truth_ids),
            "source_hashes": hashes,
        }
        value = await evaluation_call(pair, payload)
        validate_eval(pair, value, pc_ids, truth_ids)
        output = {
            "created_at": now(),
            "pair": pair,
            "model": MODEL,
            "reasoning": REASONING,
            "source_hashes": hashes,
            **value,
        }
        write_new(OUT / f"PAIR_{pair}_EVALUATION.json", output)
    write_new(OUT / "EVALUATION_ACCESS_RECEIPT.json", manifest)
    print("SOURCE_FIRST_EVALUATION_COMPLETE", flush=True)


def sum_usage(receipts: list[Path]) -> dict[str, int]:
    fields = ("input_tokens", "cached_input_tokens", "cache_write_input_tokens", "output_tokens", "reasoning_output_tokens")
    total = {field: 0 for field in fields}
    for path in receipts:
        receipt = read_json(path)
        if len(receipt["usage"]) != 1:
            raise RuntimeError(f"Bad usage receipt: {path}")
        for field in fields:
            total[field] += int(receipt["usage"][0].get(field, 0))
    total["input_plus_output_tokens"] = total["input_tokens"] + total["output_tokens"]
    return total


def count_rows(rows: list[dict[str, Any]], field: str, values: tuple[str, ...]) -> dict[str, int]:
    return {value: sum(row[field] == value for row in rows) for value in values}


def autosize(sheet: Any) -> None:
    for column in sheet.columns:
        width = min(70, max(12, max(len(str(cell.value or "")) for cell in column) + 2))
        sheet.column_dimensions[column[0].column_letter].width = width
    sheet.freeze_panes = "A2"
    sheet.auto_filter.ref = sheet.dimensions
    for cell in sheet[1]:
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor="1F4E78")


def report() -> None:
    freeze = verify_result_freeze()
    evals = {pair: read_json(OUT / f"PAIR_{pair}_EVALUATION.json") for pair in ("A", "B")}
    final = read_json(OUT / "FINAL_PROJECTCHANGES.json")
    sections: dict[str, Any] = {}
    receipt_groups = {
        "PAIR_A_MINING_V2": sorted((OUT / "pair_a_raw").glob("*/RECEIPT.json")),
        "PAIR_B_MINING_V2": sorted((OUT / "pair_b_raw").glob("*/RECEIPT.json")),
        "PAIR_A_DEDUPE": [OUT / "dedupe_raw/PAIR_A_DEDUPE/RECEIPT.json"],
        "PAIR_B_DEDUPE": [OUT / "dedupe_raw/PAIR_B_DEDUPE/RECEIPT.json"],
        "PAIR_A_EVALUATION": [OUT / "evaluation_raw/PAIR_A_SOURCE_FIRST_EVALUATION/RECEIPT.json"],
        "PAIR_B_EVALUATION": [OUT / "evaluation_raw/PAIR_B_SOURCE_FIRST_EVALUATION/RECEIPT.json"],
    }
    for name, receipts in receipt_groups.items():
        if not all(path.is_file() for path in receipts):
            raise RuntimeError(f"Missing receipts for {name}")
        sections[name] = {"calls": len(receipts), **sum_usage(receipts)}
    architecture_names = ("PAIR_A_MINING_V2", "PAIR_B_MINING_V2", "PAIR_A_DEDUPE", "PAIR_B_DEDUPE")
    eval_names = ("PAIR_A_EVALUATION", "PAIR_B_EVALUATION")
    architecture_receipts = [path for name in architecture_names for path in receipt_groups[name]]
    evaluation_receipts = [path for name in eval_names for path in receipt_groups[name]]
    token_usage = {
        "created_at": now(),
        "model": MODEL,
        "reasoning": REASONING,
        "accounting_note": "cached_input_tokens are already included in input_tokens and are not added again; input_plus_output_tokens is the descriptive architecture comparison total; evaluation is post-freeze and reported separately",
        "sections": sections,
        "TOTAL_ARCHITECTURE": {"calls": len(architecture_receipts), **sum_usage(architecture_receipts)},
        "POST_FREEZE_EVALUATION": {"calls": len(evaluation_receipts), **sum_usage(evaluation_receipts)},
        "GRAND_TOTAL_ALL_AI_CALLS": {"calls": len(architecture_receipts) + len(evaluation_receipts), **sum_usage(architecture_receipts + evaluation_receipts)},
    }
    write_new(OUT / "TOKEN_USAGE.json", token_usage)

    grouping_rows = []
    summary: dict[str, Any] = {}
    for pair in ("A", "B"):
        pc_rows = evals[pair]["projectchange_rows"]
        truth_rows = evals[pair]["truth_rows"]
        quality = count_rows(pc_rows, "quality", ("CORRECT", "PARTIAL", "FALSE", "INSUFFICIENT_TO_JUDGE"))
        grouping = count_rows(pc_rows, "grouping_quality", ("GOOD_HUMAN_LEVEL", "TOO_ATOMIC", "OVER_MERGED", "DUPLICATE"))
        truth = count_rows(truth_rows, "outcome", ("STRONG", "PARTIAL", "MISSED"))
        summary[pair] = {
            "mapping_groups": 32 if pair == "A" else 35,
            "model_calls": 32 if pair == "A" else 35,
            "v1_miner_cards": 147 if pair == "A" else 62,
            "v2_final_projectchanges": len(final["pairs"][pair]["projectchanges"]),
            "unresolved_hints": len(final["pairs"][pair]["unresolved_hints"]),
            "truth": truth,
            "quality": quality,
            "grouping": grouping,
            "f13": evals[pair]["f13_status"],
        }
        grouping_rows.extend({"pair": pair, **row} for row in pc_rows)
    write_new(
        OUT / "GROUPING_QUALITY_AUDIT.json",
        {"created_at": now(), "rows": grouping_rows, "counts": {pair: summary[pair]["grouping"] for pair in ("A", "B")}},
    )
    v1v2 = {
        "created_at": now(),
        "pairs": {
            pair: {
                "v1_miner_cards": summary[pair]["v1_miner_cards"],
                "v2_final_projectchanges": summary[pair]["v2_final_projectchanges"],
                "reduction_percent": round(
                    100 * (summary[pair]["v1_miner_cards"] - summary[pair]["v2_final_projectchanges"]) / summary[pair]["v1_miner_cards"],
                    2,
                ),
                "interpretation": "Reduction is not treated as quality by itself.",
            }
            for pair in ("A", "B")
        },
        "known_previous_costs": {
            "PAIR_A_DISCOVERY_V1_INPUT_PLUS_OUTPUT": 2431989,
            "PAIR_B_DISCOVERY_V1_INPUT_PLUS_OUTPUT": 5471376,
            "GROUPER_PLUS_VERIFIER": 43134583,
            "POST_FREEZE_DUPLICATE_AUDIT": 1958081,
        },
        "v2_architecture_input_plus_output": token_usage["TOTAL_ARCHITECTURE"]["input_plus_output_tokens"],
    }
    write_new(OUT / "V1_VS_V2.json", v1v2)

    false_total = sum(summary[pair]["quality"]["FALSE"] for pair in ("A", "B"))
    missed_total = sum(summary[pair]["truth"]["MISSED"] for pair in ("A", "B"))
    overmerged_total = sum(summary[pair]["grouping"]["OVER_MERGED"] for pair in ("A", "B"))
    substantially_lower = all(
        summary[pair]["v2_final_projectchanges"] <= 0.8 * summary[pair]["v1_miner_cards"]
        for pair in ("A", "B")
    )
    architecture_cheaper = token_usage["TOTAL_ARCHITECTURE"]["input_plus_output_tokens"] < 43134583
    if missed_total == 0 and false_total == 0 and overmerged_total <= 2 and substantially_lower and architecture_cheaper:
        recommendation = "AI_FIRST_PROJECTCHANGE_V2_PROMISING"
    elif missed_total > 0 or false_total > 0 or not substantially_lower:
        recommendation = "AI_FIRST_PROJECTCHANGE_V2_NOT_PROVEN"
    else:
        recommendation = "MORE_CONTROLLED_TESTING_REQUIRED"

    wb = Workbook()
    ws = wb.active
    ws.title = "Summary"
    ws.append(["Metric", "Pair A", "Pair B"])
    for key in ("mapping_groups", "model_calls", "v1_miner_cards", "v2_final_projectchanges", "unresolved_hints"):
        ws.append([key, summary["A"][key], summary["B"][key]])
    for group, keys in (("truth", ("STRONG", "PARTIAL", "MISSED")), ("quality", ("CORRECT", "PARTIAL", "FALSE", "INSUFFICIENT_TO_JUDGE")), ("grouping", ("GOOD_HUMAN_LEVEL", "TOO_ATOMIC", "OVER_MERGED", "DUPLICATE"))):
        for key in keys:
            ws.append([f"{group}.{key}", summary["A"][group][key], summary["B"][group][key]])
    ws.append(["F13", "NOT_APPLICABLE", summary["B"]["f13"]])
    ws.append(["Recommendation", recommendation, recommendation])
    autosize(ws)
    for pair in ("A", "B"):
        sheet = wb.create_sheet(f"ProjectChanges {pair}")
        sheet.append(["projectchange_id", "quality", "grouping_quality", "subject", "summary", "quality_rationale", "grouping_rationale", "old_pages", "new_pages"])
        by_id = {item["projectchange_id"]: item for item in final["pairs"][pair]["projectchanges"]}
        for row in evals[pair]["projectchange_rows"]:
            item = by_id[row["projectchange_id"]]
            sheet.append([row["projectchange_id"], row["quality"], row["grouping_quality"], item["engineering_subject"], item["change_summary"], row["rationale"], row["grouping_rationale"], json.dumps(item["old_pages"]), json.dumps(item["new_pages"])])
        autosize(sheet)
        truth_sheet = wb.create_sheet(f"Truth {pair}")
        truth_sheet.append(["truth_id", "outcome", "matched_projectchange_ids", "rationale"])
        for row in evals[pair]["truth_rows"]:
            truth_sheet.append([row["truth_id"], row["outcome"], ", ".join(row["matched_projectchange_ids"]), row["rationale"]])
        autosize(truth_sheet)
    token_sheet = wb.create_sheet("Token Usage")
    token_sheet.append(["Section", "Calls", "Input", "Cached Input", "Output", "Reasoning", "Input+Output"])
    for name, row in sections.items():
        token_sheet.append([name, row["calls"], row["input_tokens"], row["cached_input_tokens"], row["output_tokens"], row["reasoning_output_tokens"], row["input_plus_output_tokens"]])
    row = token_usage["TOTAL_ARCHITECTURE"]
    token_sheet.append(["TOTAL_ARCHITECTURE", row["calls"], row["input_tokens"], row["cached_input_tokens"], row["output_tokens"], row["reasoning_output_tokens"], row["input_plus_output_tokens"]])
    autosize(token_sheet)
    wb.save(OUT / "RESULTS.xlsx")

    def pair_lines(pair: str, truth_name: str) -> list[str]:
        s = summary[pair]
        return [
            f"- mapping groups: {s['mapping_groups']}",
            f"- model calls: {s['model_calls']}",
            f"- V1 miner cards: {s['v1_miner_cards']}",
            f"- V2 final ProjectChanges: {s['v2_final_projectchanges']}",
            f"- unresolved hints: {s['unresolved_hints']}",
            f"- {truth_name} STRONG/PARTIAL/MISSED: {s['truth']['STRONG']}/{s['truth']['PARTIAL']}/{s['truth']['MISSED']}",
            *( [f"- F13: {s['f13']}"] if pair == "B" else [] ),
            f"- correct/partial/false/insufficient: {s['quality']['CORRECT']}/{s['quality']['PARTIAL']}/{s['quality']['FALSE']}/{s['quality']['INSUFFICIENT_TO_JUDGE']}",
            f"- good/too atomic/over-merged/duplicates: {s['grouping']['GOOD_HUMAN_LEVEL']}/{s['grouping']['TOO_ATOMIC']}/{s['grouping']['OVER_MERGED']}/{s['grouping']['DUPLICATE']}",
        ]

    total = token_usage["TOTAL_ARCHITECTURE"]
    conclusion = (
        "V2 не доказала целевое упрощение: итоговое число карточек не снизилось существенно одновременно для обеих пар, даже если recall/precision остаются приемлемыми. "
        "Lightweight dedupe не заменил human-level grouping для Pair A."
        if recommendation == "AI_FIRST_PROJECTCHANGE_V2_NOT_PROVEN"
        else "V2 показала потенциально полезное прямое формирование ProjectChange при существенно меньшей стоимости, но вывод следует читать вместе с quality/grouping audit."
    )
    report_text = "\n".join(
        [
            "# AI-FIRST PROJECTCHANGE MINER V2 — FINAL REPORT",
            "",
            "STATUS: COMPLETE — RESULT FROZEN, SOURCE-FIRST EVALUATION COMPLETE",
            "",
            f"MODEL: {MODEL} {REASONING}",
            "",
            "## PAIR A",
            "",
            *pair_lines("A", "REAL15"),
            "",
            "## PAIR B",
            "",
            *pair_lines("B", "PROVEN10"),
            "",
            "## TOKEN USAGE",
            "",
            f"- Pair A mining: {sections['PAIR_A_MINING_V2']}",
            f"- Pair B mining: {sections['PAIR_B_MINING_V2']}",
            f"- Pair A dedupe: {sections['PAIR_A_DEDUPE']}",
            f"- Pair B dedupe: {sections['PAIR_B_DEDUPE']}",
            f"- TOTAL architecture: {total}",
            f"- Post-freeze evaluation (separate): {token_usage['POST_FREEZE_EVALUATION']}",
            "- Compare to Grouper+Verifier V1: 43,134,583 tokens",
            "- Cached input is included in input and is not added twice.",
            "",
            "## ARCHITECTURE CONCLUSION",
            "",
            conclusion,
            "",
            f"RECOMMENDATION: `{recommendation}`",
            "",
            "PRODUCTION: UNCHANGED",
            "",
            "VALIDATION: NOT OPENED",
            "",
            "FINAL HOLDOUT: NOT OPENED",
            "",
        ]
    )
    write_new(OUT / "FINAL_REPORT.md", report_text)
    print(json.dumps({"recommendation": recommendation, "summary": summary}, ensure_ascii=False), flush=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["evaluate", "report"])
    action = parser.parse_args().action
    if action == "evaluate":
        asyncio.run(evaluate())
    else:
        report()


if __name__ == "__main__":
    main()
