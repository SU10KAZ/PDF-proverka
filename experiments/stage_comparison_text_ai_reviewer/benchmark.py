#!/usr/bin/env python3
"""Reproducible semantic AI-reviewer benchmark for Stage 4.

The dataset combines traceable real fragments from the current Balchug Estate
comparison with explicitly labelled adversarial controls required to measure
semantic safety.  Ground truth is hand-authored and never used by production.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.app.services.stage_comparison import text_ai_reviewer as reviewer  # noqa: E402
from experiments.stage_comparison_text_differences import benchmark as stage3_benchmark  # noqa: E402


ARTIFACT_ROOT = Path(__file__).resolve().parent / "artifacts"
DATASET_PATH = ARTIFACT_ROOT / "benchmark_dataset.json"
GROUND_TRUTH_PATH = ARTIFACT_ROOT / "benchmark_ground_truth.json"
RUNS_ROOT = ARTIFACT_ROOT / "runs"
SUMMARY_PATH = ARTIFACT_ROOT / "benchmark_summary.json"
REPORT_PATH = ARTIFACT_ROOT / "BENCHMARK_REPORT.md"
STAGE3_DATASET_PATH = (
    REPO_ROOT / "experiments/stage_comparison_text_differences/artifacts/benchmark_dataset.json"
)
SESSION_ID = "121d764109184c13"
PAIR_ROOT = REPO_ROOT / "comparison" / "sessions" / SESSION_ID / "pairs"
MODELS = {
    "codex": ("gpt-5.6-luna", "gpt-5.6-terra", "gpt-5.6-sol"),
    "claude": ("claude-fable-5", "claude-sonnet-5", "claude-opus-5"),
}
EFFORT = "medium"
BATCH_SIZE = 9


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def sha256_json(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def source_fragment(
    fragment_id: str, text: str, *, page: int, sheet: str,
    source_kind: str, bboxes: list[dict[str, Any]] | None = None,
    local_context: str = "",
) -> dict[str, Any]:
    return {
        "fragment_id": fragment_id,
        "page": page,
        "sheet": sheet,
        "text": text,
        "bboxes": list(bboxes or []),
        "source_kind": source_kind,
        "table_key": "",
        "local_context": local_context,
    }


def expected(status: str, left: list[str], right: list[str]) -> dict[str, Any]:
    return {"final_status": status, "left_fragment_ids": left, "right_fragment_ids": right}


def preliminary(
    status: str, left: list[str], right: list[str], *, actual_pages: list[int] | None = None,
) -> dict[str, Any]:
    return {
        "status": status,
        "left_fragment_ids": left,
        "right_fragment_ids": right,
        "actual_right_pages": list(actual_pages or []),
    }


def make_group(
    group_id: str, category: str, left: list[dict[str, Any]], right: list[dict[str, Any]],
    prelim: list[dict[str, Any]], truth: list[dict[str, Any]], *, source_type: str,
    left_pages: list[int] | None = None, right_pages: list[int] | None = None,
    source_ref: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "group_id": group_id,
        "category": category,
        "source_type": source_type,
        "source_ref": source_ref or {},
        "left_pages": left_pages or sorted({item["page"] for item in left}),
        "right_pages": right_pages or sorted({item["page"] for item in right}),
        "source_left": left,
        "source_right": right,
        "required_fragment_ids": {
            "left": [item["fragment_id"] for item in left],
            "right": [item["fragment_id"] for item in right],
        },
        "preliminary": prelim,
        "ground_truth": truth,
    }


def _old_source(item: dict[str, Any], side: str, case: dict[str, Any]) -> dict[str, Any]:
    return source_fragment(
        item["id"], item["text"], page=int(item["page"]),
        sheet=f"{side.upper()} page {item['page']}", source_kind="real_project",
    )


def _stage3_preliminary(case: dict[str, Any]) -> list[dict[str, Any]]:
    if case["id"] == "formatting_only":
        return [preliminary(
            "SAME", [case["left_fragments"][0]["id"]], [case["right_fragments"][0]["id"]]
        )]
    raw = stage3_benchmark.deterministic_baseline(case)
    output = []
    for item in raw["changed"]:
        output.append(preliminary("CHANGED", item["left_ids"], item["right_ids"]))
    for item in raw["removed"]:
        output.append(preliminary("REMOVED", item["left_ids"], []))
    for item in raw["added"]:
        output.append(preliminary("ADDED", [], item["right_ids"]))
    return output


def _stage3_truth(case: dict[str, Any]) -> list[dict[str, Any]]:
    truth = case["ground_truth"]
    output = [expected("CHANGED", x["left_ids"], x["right_ids"]) for x in truth["changed"]]
    output.extend(expected("REMOVED", x["left_ids"], []) for x in truth["removed"])
    output.extend(expected("ADDED", [], x["right_ids"]) for x in truth["added"])
    ambiguous = truth.get("ambiguous")
    if ambiguous:
        output.append(expected(
            "UNCERTAIN", list(ambiguous["left_ids"]), list(ambiguous["right_ids"])
        ))
    if case["id"] == "formatting_only":
        output.append(expected(
            "SAME", [case["left_fragments"][0]["id"]], [case["right_fragments"][0]["id"]]
        ))
    return output


def _real_stage2_groups() -> list[dict[str, Any]]:
    pair_id = "p570d156f57"
    comparison = load_json(PAIR_ROOT / pair_id / "text_comparison.json")
    fragments = {
        side: {str(item["id"]): item for item in comparison["fragments"][side]}
        for side in ("left", "right")
    }
    same = next(
        item for item in comparison["matches"]
        if item["status"] == "same_on_linked_sheet" and len(item["canonical_text"]) > 35
    )
    moved = next(
        item for item in comparison["matches"]
        if item["status"] == "found_on_other_sheet"
    )

    def converted(side: str, fragment_id: str) -> dict[str, Any]:
        item = fragments[side][fragment_id]
        return source_fragment(
            fragment_id, item["text"], page=int(item["pdf_page"]),
            sheet=str(item.get("sheet_number") or f"page {item['pdf_page']}"),
            source_kind="real_project", bboxes=item.get("bboxes") or [],
        )

    same_left, same_right = same["left_fragment_id"], same["right_fragment_id"]
    moved_left, moved_right = moved["left_fragment_id"], moved["right_fragment_id"]
    return [
        make_group(
            "real_exact_same", "same_same_words",
            [converted("left", same_left)], [converted("right", same_right)],
            [preliminary("SAME", [same_left], [same_right])],
            [expected("SAME", [same_left], [same_right])],
            source_type="real_project", source_ref={"session_id": SESSION_ID, "pair_id": pair_id},
        ),
        make_group(
            "real_moved_other_sheet", "moved_other_sheet",
            [converted("left", moved_left)], [converted("right", moved_right)],
            [preliminary("MOVED", [moved_left], [moved_right], actual_pages=[moved["right_page"]])],
            [expected("MOVED", [moved_left], [moved_right])],
            source_type="real_project", left_pages=list(moved["expected_left_pages"]),
            right_pages=list(moved["expected_right_pages"]),
            source_ref={"session_id": SESSION_ID, "pair_id": pair_id, "actual_right_page": moved["right_page"]},
        ),
    ]


CONTROL_SPECS: tuple[dict[str, Any], ...] = (
    {
        "id": "semantic_paraphrase", "category": "same_strong_paraphrase", "status": "SAME",
        "left": ["Для помещения предусматривается вытяжная вентиляция."],
        "right": ["Удаление воздуха из помещения осуществляется системой вытяжной вентиляции."],
        "preliminary": [("REMOVED", [0], []), ("ADDED", [], [0])],
    },
    {
        "id": "operating_mode", "category": "changed_semantic_word", "status": "CHANGED",
        "left": ["Система работает постоянно."], "right": ["Система работает периодически."],
        "preliminary": [("CHANGED", [0], [0])],
    },
    {
        "id": "negation", "category": "changed_negation", "status": "CHANGED",
        "left": ["Для кладовых предусматривается механическая вытяжная вентиляция."],
        "right": ["Для кладовых механическая вытяжная вентиляция не предусматривается."],
        "preliminary": [("CHANGED", [0], [0])],
    },
    {
        "id": "material_change", "category": "changed_material", "status": "CHANGED",
        "left": ["Воздуховоды выполняются из оцинкованной стали."],
        "right": ["Воздуховоды выполняются из нержавеющей стали."],
        "preliminary": [("CHANGED", [0], [0])],
    },
    {
        "id": "quantity_words", "category": "same_quantity_words", "status": "SAME",
        "left": ["Количество лифтов — 2."], "right": ["Предусмотрено два лифта."],
        "preliminary": [("REMOVED", [0], []), ("ADDED", [], [0])],
    },
    {
        "id": "unit_magnitude", "category": "changed_unit_and_value", "status": "CHANGED",
        "left": ["Расход воздуха — 1200 м³/ч."],
        "right": ["Расчётный расход системы составляет 1,6 тыс. м³/ч."],
        "preliminary": [("CHANGED", [0], [0])],
    },
    {
        "id": "formula_equivalent", "category": "same_formula", "status": "SAME",
        "left": ["Расход определяется по формуле Q = L × n."],
        "right": ["Расчёт выполняется по выражению Q=n·L."],
        "preliminary": [("REMOVED", [0], []), ("ADDED", [], [0])],
    },
    {
        "id": "formula_changed", "category": "changed_formula", "status": "CHANGED",
        "left": ["Расход определяется по формуле Q = V × n."],
        "right": ["Расход определяется по формуле Q = G / (Cп - Cн)."],
        "preliminary": [("CHANGED", [0], [0])],
    },
    {
        "id": "calculation_input", "category": "changed_calculation_input", "status": "CHANGED",
        "left": ["Расчётная кратность воздухообмена n = 3."],
        "right": ["Расчётная кратность воздухообмена n = 5."],
        "preliminary": [("CHANGED", [0], [0])],
    },
    {
        "id": "calculation_method", "category": "changed_calculation_method", "status": "CHANGED",
        "left": ["Расход определяется исходя из 3-кратного воздухообмена."],
        "right": ["Расход определяется по количеству выделяемых вредностей."],
        "preliminary": [("REMOVED", [0], []), ("ADDED", [], [0])],
    },
    {
        "id": "one_to_many", "category": "same_one_to_many", "status": "SAME",
        "left": ["Приток воздуха подаётся в верхнюю зону. Удаление воздуха выполняется из верхней и нижней зон."],
        "right": ["Приточный воздух поступает в верхнюю зону.", "Вытяжка производится из верхней и нижней зон."],
        "preliminary": [("REMOVED", [0], []), ("ADDED", [], [0]), ("ADDED", [], [1])],
    },
    {
        "id": "many_to_one", "category": "changed_many_to_one", "status": "CHANGED",
        "left": ["Вентилятор работает постоянно.", "Резервный вентилятор включается автоматически."],
        "right": ["Основной вентилятор работает периодически, резервный включается автоматически."],
        "preliminary": [("REMOVED", [0], []), ("REMOVED", [1], []), ("ADDED", [], [0])],
    },
    {
        "id": "false_moved", "category": "similar_but_not_moved", "status": "UNCERTAIN",
        "left": ["Удаление воздуха из санузлов выполняется вытяжной системой."],
        "right": ["Удаление дыма из коридора выполняется системой противодымной вентиляции."],
        "preliminary": [("MOVED", [0], [0])], "right_page": 18, "expected_right_page": 7,
    },
    {
        "id": "same_words_different_context", "category": "same_text_different_context", "status": "UNCERTAIN",
        "left": ["Расчётная температура воздуха — 20 °C."],
        "right": ["Расчётная температура воздуха — 20 °C."],
        "left_context": "Жилая комната квартиры.", "right_context": "Помещение серверной.",
        "preliminary": [("SAME", [0], [0])],
    },
)


def _control_group(spec: dict[str, Any]) -> dict[str, Any]:
    left_ids = [f"ctl_{spec['id']}_L{i + 1}" for i in range(len(spec["left"]))]
    right_ids = [f"ctl_{spec['id']}_R{i + 1}" for i in range(len(spec["right"]))]
    right_page = int(spec.get("right_page") or 2)
    left = [
        source_fragment(
            fragment_id, text, page=1, sheet="П-1", source_kind="controlled_adversarial",
            local_context=str(spec.get("left_context") or ""),
        )
        for fragment_id, text in zip(left_ids, spec["left"])
    ]
    right = [
        source_fragment(
            fragment_id, text, page=right_page, sheet=f"РД-{right_page}",
            source_kind="controlled_adversarial",
            local_context=str(spec.get("right_context") or ""),
        )
        for fragment_id, text in zip(right_ids, spec["right"])
    ]
    prelim = []
    for status, left_indexes, right_indexes in spec["preliminary"]:
        prelim.append(preliminary(
            status,
            [left_ids[index] for index in left_indexes],
            [right_ids[index] for index in right_indexes],
            actual_pages=[right_page] if status == "MOVED" else [],
        ))
    truth = [expected(spec["status"], left_ids, right_ids)]
    return make_group(
        spec["id"], spec["category"], left, right, prelim, truth,
        source_type="controlled_adversarial",
        left_pages=[1], right_pages=[int(spec.get("expected_right_page") or right_page)],
        source_ref={"basis": "explicit_stage4_acceptance_case"},
    )


def build_dataset() -> dict[str, Any]:
    old = load_json(STAGE3_DATASET_PATH)
    groups = []
    for case in old["cases"]:
        left = [_old_source(item, "left", case) for item in case["left_fragments"]]
        right = [_old_source(item, "right", case) for item in case["right_fragments"]]
        groups.append(make_group(
            f"real_{case['id']}", case["category"], left, right,
            _stage3_preliminary(case), _stage3_truth(case), source_type="real_project",
            left_pages=case["left_pages"], right_pages=case["right_pages"],
            source_ref={
                "session_id": SESSION_ID, "pair_id": case["pair_id"],
                "link_id": case["link_id"], "stage3_dataset_sha256": old["dataset_sha256"],
            },
        ))
    groups.extend(_real_stage2_groups())
    groups.extend(_control_group(spec) for spec in CONTROL_SPECS)
    if len(groups) != 27:
        raise RuntimeError(f"expected 27 benchmark groups, got {len(groups)}")
    payload = {
        "version": 1,
        "prompt_version": reviewer.PROMPT_VERSION,
        "response_schema_sha256": sha256_json(reviewer.RESPONSE_SCHEMA),
        "source": {
            "real_project": "272_Sadovnicheskaya_76_Balchug_Esteyt",
            "session_id": SESSION_ID,
            "ground_truth_author": "manual_source_review",
            "production_dependency": False,
            "real_groups": sum(group["source_type"] == "real_project" for group in groups),
            "controlled_groups": sum(group["source_type"] == "controlled_adversarial" for group in groups),
        },
        "groups": groups,
    }
    payload["dataset_sha256"] = sha256_json(payload)
    write_json(DATASET_PATH, payload)
    ground_truth = {
        "version": 1,
        "dataset_sha256": payload["dataset_sha256"],
        "author": payload["source"]["ground_truth_author"],
        "production_dependency": False,
        "groups": [
            {
                "group_id": group["group_id"], "category": group["category"],
                "expected_decisions": group["ground_truth"],
            }
            for group in groups
        ],
    }
    ground_truth["ground_truth_sha256"] = sha256_json(ground_truth)
    write_json(GROUND_TRUTH_PATH, ground_truth)
    return payload


def _baseline_response(groups: list[dict[str, Any]]) -> dict[str, Any]:
    output = []
    for group in groups:
        left = {x["fragment_id"]: x for x in group["source_left"]}
        right = {x["fragment_id"]: x for x in group["source_right"]}
        decisions = []
        for item in group["preliminary"]:
            status = item["status"]
            if status == "AMBIGUOUS":
                status = "UNCERTAIN"
            left_ids, right_ids = item["left_fragment_ids"], item["right_fragment_ids"]
            texts = [left[x]["text"] for x in left_ids] + [right[x]["text"] for x in right_ids]
            decisions.append({
                "left_fragment_ids": left_ids,
                "right_fragment_ids": right_ids,
                "final_status": status,
                "confidence": "high" if status != "UNCERTAIN" else "low",
                "summary": " / ".join(texts)[:500] or "Нет текста",
                "reason": "Предварительная deterministic-классификация.",
                "actual_right_pages": item.get("actual_right_pages") or [],
            })
        output.append({"group_id": group["group_id"], "decisions": decisions})
    return {"groups": output}


def _codex_result(stdout: str) -> tuple[str, dict[str, int], str | None]:
    answer = ""
    usage: dict[str, int] = {}
    for line in stdout.splitlines():
        try:
            event = json.loads(line)
        except (json.JSONDecodeError, ValueError):
            continue
        if event.get("type") == "item.completed":
            item = event.get("item") or {}
            if item.get("type") == "agent_message":
                answer = str(item.get("text") or "")
        elif event.get("type") == "turn.completed":
            usage = {
                key: int(value) for key, value in (event.get("usage") or {}).items()
                if isinstance(value, int)
            }
    return answer, usage, None


def _claude_result(stdout: str, requested_model: str) -> tuple[str, dict[str, int], str | None]:
    payload = json.loads(stdout)
    usage = {
        key: int(value) for key, value in (payload.get("usage") or {}).items()
        if isinstance(value, int)
    }
    model_usage = payload.get("modelUsage") or {}
    candidates = [str(name) for name in model_usage if not str(name).startswith("claude-haiku-")]
    reported = requested_model if requested_model in model_usage else (
        candidates[0] if len(candidates) == 1 else None
    )
    structured = payload.get("structured_output")
    answer = (
        json.dumps(structured, ensure_ascii=False)
        if isinstance(structured, (dict, list)) else str(payload.get("result") or "")
    )
    return answer, usage, reported


def invoke(provider: str, model: str, prompt: str, timeout: float) -> dict[str, Any]:
    schema_path: Path | None = None
    if provider == "codex":
        handle = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", encoding="utf-8", delete=False,
        )
        with handle:
            json.dump(reviewer.RESPONSE_SCHEMA, handle, ensure_ascii=False)
        schema_path = Path(handle.name)
        argv = [
            "codex", "exec", "--json", "--sandbox", "read-only",
            "--skip-git-repo-check", "--ephemeral", "--ignore-rules",
            "--ignore-user-config", f"--model={model}", "-c",
            f'model_reasoning_effort="{EFFORT}"', "--output-schema", str(schema_path), "-",
        ]
    elif provider == "claude":
        argv = [
            "claude", "--safe-mode", "--strict-mcp-config", "--disable-slash-commands",
            "--no-session-persistence", "--setting-sources=", f"--model={model}",
            f"--effort={EFFORT}", "--tools=", "--permission-mode", "dontAsk",
            "--max-turns", "2", "--output-format", "json",
            "--json-schema", json.dumps(reviewer.RESPONSE_SCHEMA, ensure_ascii=False), "-p",
        ]
    else:
        raise ValueError(provider)
    started = time.monotonic()
    try:
        try:
            completed = subprocess.run(
                argv, input=prompt, text=True, capture_output=True,
                cwd=REPO_ROOT, timeout=timeout, check=False,
            )
        except subprocess.TimeoutExpired as exc:
            return {
                "ok": False, "elapsed_sec": round(time.monotonic() - started, 3),
                "exit_code": None, "reported_model": None, "usage": {}, "answer": "",
                "error": f"timeout:{exc.timeout}",
            }
    finally:
        if schema_path is not None:
            schema_path.unlink(missing_ok=True)
    try:
        if provider == "codex":
            answer, usage, reported = _codex_result(completed.stdout)
        else:
            answer, usage, reported = _claude_result(completed.stdout, model)
        parse_error = ""
    except (json.JSONDecodeError, ValueError, TypeError) as exc:
        answer, usage, reported = "", {}, None
        parse_error = f"provider_envelope:{exc}"
    return {
        "ok": completed.returncode == 0 and bool(answer),
        "elapsed_sec": round(time.monotonic() - started, 3),
        "exit_code": completed.returncode, "reported_model": reported,
        "usage": usage, "answer": answer,
        "error": parse_error or (completed.stderr.strip() if completed.returncode else ""),
        "provider_stdout": completed.stdout,
        "provider_stderr": completed.stderr,
    }


def run_model(
    dataset: dict[str, Any], provider: str, model: str, mode: str,
    *, timeout: float, resume: bool,
) -> dict[str, Any]:
    safe_model = re.sub(r"[^a-zA-Z0-9_.-]+", "_", model)
    path = RUNS_ROOT / f"{provider}__{safe_model}__{mode}__{EFFORT}.json"
    batches = []
    if resume and path.is_file():
        existing = load_json(path)
        if (
            existing.get("dataset_sha256") == dataset["dataset_sha256"]
            and existing.get("native_json_schema_enforced") is True
            and (provider != "claude" or existing.get("structured_output_max_turns") == 2)
        ):
            batches = list(existing.get("batches") or [])
            completed_ids = {
                group_id for batch in batches for group_id in batch.get("group_ids") or []
            }
            if completed_ids == {group["group_id"] for group in dataset["groups"]}:
                return existing
    groups = dataset["groups"]
    for start in range(0, len(groups), BATCH_SIZE):
        batch = groups[start:start + BATCH_SIZE]
        batch_ids = [x["group_id"] for x in batch]
        if any(set(item.get("group_ids") or []) == set(batch_ids) for item in batches):
            continue
        print(
            f"[{provider}/{model}/{mode}] groups {start + 1}-{start + len(batch)}/{len(groups)}",
            flush=True,
        )
        call = invoke(
            provider, model,
            reviewer.prompt_for_groups(batch, include_hint=mode == "with_hint"),
            timeout,
        )
        batches.append({"group_ids": batch_ids, **call})
        write_json(path, {
            "version": 1, "provider": provider, "requested_model": model,
            "mode": mode, "reasoning_effort": EFFORT,
            "native_json_schema_enforced": True,
            "structured_output_max_turns": 2 if provider == "claude" else None,
            "dataset_sha256": dataset["dataset_sha256"], "batches": batches,
        })
    return load_json(path)


def _actual_label(expected_item: dict[str, Any], decisions: list[dict[str, Any]]) -> str:
    left = set(expected_item["left_fragment_ids"])
    right = set(expected_item["right_fragment_ids"])
    overlaps = [
        item for item in decisions
        if set(item["left_fragment_ids"]) & left or set(item["right_fragment_ids"]) & right
    ]
    if len(overlaps) == 1 and set(overlaps[0]["left_fragment_ids"]) == left \
            and set(overlaps[0]["right_fragment_ids"]) == right:
        return str(overlaps[0]["final_status"])
    statuses = {str(item["final_status"]) for item in overlaps}
    if statuses == {"REMOVED", "ADDED"}:
        return "REMOVED_ADDED"
    return "MISSING" if not statuses else "MIXED"


def _evaluate_group(group: dict[str, Any], decisions: list[dict[str, Any]] | None) -> dict[str, Any]:
    truth = group["ground_truth"]
    baseline = reviewer.validate_response(
        _baseline_response([group]), [group], safe_same_moved=False
    )[0]["decisions"]
    result = decisions or []
    matrix: dict[str, dict[str, int]] = {}
    correct = opportunities = harmful = 0
    accuracy = 0
    for item in truth:
        expected_status = item["final_status"]
        actual = _actual_label(item, result)
        base = _actual_label(item, baseline)
        matrix.setdefault(expected_status, {})[actual] = matrix.setdefault(expected_status, {}).get(actual, 0) + 1
        accuracy += int(actual == expected_status)
        if base != expected_status:
            opportunities += 1
            correct += int(actual == expected_status)
        elif actual != expected_status:
            harmful += 1
    false_same = false_moved = 0
    truth_exact = {
        (frozenset(x["left_fragment_ids"]), frozenset(x["right_fragment_ids"]), x["final_status"])
        for x in truth
    }
    for item in result:
        key = (
            frozenset(item["left_fragment_ids"]),
            frozenset(item["right_fragment_ids"]), item["final_status"],
        )
        if item["final_status"] == "SAME" and key not in truth_exact:
            false_same += 1
        if item["final_status"] == "MOVED" and key not in truth_exact:
            false_moved += 1
    return {
        "expected": len(truth), "correct": accuracy,
        "false_same": false_same, "false_moved": false_moved,
        "reclassification_opportunities": opportunities,
        "correct_reclassification": correct, "harmful_reclassification": harmful,
        "confusion_matrix": matrix,
    }


def _merge_matrix(target: dict[str, Any], source: dict[str, Any]) -> None:
    for expected_status, row in source.items():
        for actual, count in row.items():
            target.setdefault(expected_status, {})[actual] = (
                target.setdefault(expected_status, {}).get(actual, 0) + count
            )


def aggregate_run(dataset: dict[str, Any], run: dict[str, Any]) -> dict[str, Any]:
    groups = {group["group_id"]: group for group in dataset["groups"]}
    parsed: dict[str, dict[str, Any]] = {}
    group_errors: dict[str, list[str]] = {group_id: [] for group_id in groups}
    elapsed = []
    json_failures = provenance_failures = value_failures = hallucinations = 0
    input_tokens = output_tokens = 0
    usage_available = True
    reported_models = set()
    for call in run["batches"]:
        elapsed.append(float(call.get("elapsed_sec") or 0))
        reported = call.get("reported_model")
        if reported:
            reported_models.add(str(reported))
        usage = call.get("usage") or {}
        if "input_tokens" not in usage or "output_tokens" not in usage:
            usage_available = False
        else:
            call_input = int(usage["input_tokens"])
            if run["provider"] == "claude":
                call_input += int(usage.get("cache_creation_input_tokens") or 0)
                call_input += int(usage.get("cache_read_input_tokens") or 0)
            input_tokens += call_input
            output_tokens += int(usage["output_tokens"])
        if not call.get("ok"):
            json_failures += 1
            for group_id in call["group_ids"]:
                group_errors[group_id].append(call.get("error") or "provider_failure")
            continue
        try:
            payload = json.loads(call.get("answer") or "")
        except (json.JSONDecodeError, ValueError) as exc:
            json_failures += 1
            for group_id in call["group_ids"]:
                group_errors[group_id].append(f"json_parse:{exc}")
            continue
        raw_groups = payload.get("groups") if isinstance(payload, dict) else None
        if not isinstance(raw_groups, list):
            json_failures += 1
            for group_id in call["group_ids"]:
                group_errors[group_id].append("root_schema")
            continue
        response_by_id = {
            str(item.get("group_id") or ""): item for item in raw_groups if isinstance(item, dict)
        }
        for group_id in call["group_ids"]:
            try:
                normalized = reviewer.validate_group_response(
                    response_by_id.get(group_id), groups[group_id], safe_same_moved=True
                )
            except reviewer.ReviewValidationError as exc:
                error = str(exc)
                group_errors[group_id].append(error)
                provenance_failures += int(
                    "fragment" in error or "coverage" in error or "duplicate" in error
                )
                value_failures += int("unsupported" in error)
                hallucinations += int("hallucinated" in error or "unsupported" in error)
            else:
                parsed[group_id] = normalized
                policies = [
                    str(item.get("policy_reason") or "")
                    for item in normalized["decisions"]
                    if str(item.get("policy_reason") or "").startswith("unsupported_model_")
                ]
                if policies:
                    group_errors[group_id].extend(f"policy:{policy}" for policy in policies)
                    value_failures += len(policies)
                    hallucinations += len(policies)

    totals = {
        "expected": 0, "correct": 0, "false_same": 0, "false_moved": 0,
        "reclassification_opportunities": 0, "correct_reclassification": 0,
        "harmful_reclassification": 0,
    }
    raw_totals = {"correct": 0, "false_same": 0, "false_moved": 0, "harmful_reclassification": 0}
    matrix: dict[str, Any] = {}
    case_results = []
    for group_id, group in groups.items():
        normalized = parsed.get(group_id)
        metrics = _evaluate_group(
            group, normalized["decisions"] if normalized else None
        )
        raw_decisions = None
        if normalized:
            raw_decisions = [
                {**item, "final_status": item.get("model_final_status", item["final_status"])}
                for item in normalized["decisions"]
            ]
        raw_metrics = _evaluate_group(group, raw_decisions)
        for key in totals:
            totals[key] += metrics[key]
        _merge_matrix(matrix, metrics["confusion_matrix"])
        for key in raw_totals:
            raw_totals[key] += raw_metrics[key]
        case_results.append({
            "group_id": group_id, "category": group["category"],
            "validation_errors": group_errors[group_id], "metrics": metrics,
            "decisions": normalized["decisions"] if normalized else None,
            "raw_model_metrics": raw_metrics,
        })
    return {
        "provider": run["provider"], "requested_model": run["requested_model"],
        "reported_models": sorted(reported_models), "mode": run["mode"],
        "reasoning_effort": run["reasoning_effort"], "groups": len(groups),
        "final_classification_accuracy": round(totals["correct"] / totals["expected"], 4),
        "raw_model_classification_accuracy": round(raw_totals["correct"] / totals["expected"], 4),
        "raw_model_false_same": raw_totals["false_same"],
        "raw_model_false_moved": raw_totals["false_moved"],
        "raw_model_harmful_reclassification": raw_totals["harmful_reclassification"],
        **totals, "confusion_matrix": matrix,
        "provenance_failures": provenance_failures, "value_failures": value_failures,
        "hallucinations": hallucinations, "json_failures": json_failures,
        "avg_batch_time_sec": round(statistics.mean(elapsed), 3) if elapsed else None,
        "avg_group_time_sec": round(sum(elapsed) / len(groups), 3) if elapsed else None,
        "p50_batch_time_sec": round(statistics.median(elapsed), 3) if elapsed else None,
        "p95_batch_time_sec": percentile(elapsed, .95),
        "input_tokens": input_tokens if usage_available else None,
        "output_tokens": output_tokens if usage_available else None,
        "case_results": case_results,
    }


def baseline_run(dataset: dict[str, Any]) -> dict[str, Any]:
    calls = []
    for start in range(0, len(dataset["groups"]), BATCH_SIZE):
        groups = dataset["groups"][start:start + BATCH_SIZE]
        begun = time.monotonic()
        answer = _baseline_response(groups)
        calls.append({
            "group_ids": [x["group_id"] for x in groups], "ok": True,
            "elapsed_sec": round(time.monotonic() - begun, 6), "exit_code": 0,
            "reported_model": None, "usage": {},
            "answer": json.dumps(answer, ensure_ascii=False), "error": "",
        })
    return {
        "version": 1, "provider": "deterministic",
        "requested_model": "DETERMINISTIC ONLY", "mode": "baseline",
        "reasoning_effort": "none", "dataset_sha256": dataset["dataset_sha256"],
        "batches": calls,
    }


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[max(0, math.ceil(fraction * len(ordered)) - 1)], 3)


def fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{value:.3f}" if isinstance(value, float) else str(value)


def select_production_candidate(summaries: list[dict[str, Any]]) -> dict[str, Any] | None:
    baseline = next((row for row in summaries if row["provider"] == "deterministic"), None)
    candidates = [
        row for row in summaries
        if row["provider"] != "deterministic" and row["mode"] == "with_hint"
        and row["json_failures"] == 0
        and (baseline is None or row["final_classification_accuracy"] > baseline["final_classification_accuracy"])
        and row["correct_reclassification"] > row["harmful_reclassification"]
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda row: (
        row["false_same"] + row["false_moved"],
        -row["final_classification_accuracy"], row["harmful_reclassification"],
        row["value_failures"], row["hallucinations"], row["json_failures"],
        row["raw_model_false_same"] + row["raw_model_false_moved"],
        row["avg_group_time_sec"] or math.inf,
        row["input_tokens"] or math.inf, row["output_tokens"] or math.inf,
    ))


def build_report(dataset: dict[str, Any], summaries: list[dict[str, Any]]) -> str:
    winner = select_production_candidate(summaries)
    ground_truth = load_json(GROUND_TRUTH_PATH)
    lines = [
        "# AI Reviewer benchmark П ↔ РД", "",
        f"Dataset `{dataset['dataset_sha256']}`: **{len(dataset['groups'])}** groups, "
        f"{dataset['source']['real_groups']} real-project and "
        f"{dataset['source']['controlled_groups']} controlled adversarial.", "",
        f"Independent ground truth `{ground_truth['ground_truth_sha256']}`; it is not used "
        "by production.", "",
        f"Schema `{dataset['response_schema_sha256']}`; validator "
        f"`{reviewer.VALIDATOR_VERSION}`.", "",
        "All model runs used identical fragments, ordering, schema, batches and `medium` effort. "
        "The same native structured-output schema was enforced by both CLIs. "
        "`with_hint` includes deterministic preliminary decisions; `without_hint` hides them. "
        "`final/raw` means the accepted validator result versus the model proposal before the "
        "SAME/MOVED safety gate.", "",
        "| Provider | Model | Mode | Accuracy final/raw | False SAME final/raw | False MOVED final/raw | Corrected | Harmful final/raw | Prov. fail | Value fail | Halluc. | JSON fail | Avg group, s | Input/output |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summaries:
        lines.append(
            "| {provider} | {model} | {mode} | {accuracy}/{raw_accuracy} | {fs}/{raw_fs} | {fm}/{raw_fm} | "
            "{correct}/{opportunities} | {harmful}/{raw_harmful} | {prov} | {value} | {hall} | {jsonf} | "
            "{avg} | {inp}/{out} |".format(
                provider=row["provider"], model=row["requested_model"], mode=row["mode"],
                accuracy=fmt(row["final_classification_accuracy"]), fs=row["false_same"],
                raw_accuracy=fmt(row["raw_model_classification_accuracy"]),
                raw_fs=row["raw_model_false_same"], fm=row["false_moved"],
                raw_fm=row["raw_model_false_moved"], correct=row["correct_reclassification"],
                opportunities=row["reclassification_opportunities"],
                harmful=row["harmful_reclassification"],
                raw_harmful=row["raw_model_harmful_reclassification"],
                prov=row["provenance_failures"],
                value=row["value_failures"], hall=row["hallucinations"],
                jsonf=row["json_failures"], avg=fmt(row["avg_group_time_sec"]),
                inp=fmt(row["input_tokens"]), out=fmt(row["output_tokens"]),
            )
        )
    lines.extend(["", "## Production selection", ""])
    if winner is None:
        lines.extend([
            "No model passed the minimum utility gate (better than deterministic baseline, "
            "zero JSON failures, and more corrected than harmful reclassifications).", "",
        ])
    else:
        lines.extend([
            f"Selected: **{winner['provider']} / {winner['requested_model']} / "
            f"{winner['mode']} / {winner['reasoning_effort']}**.", "",
            "Selection first minimizes accepted false SAME/MOVED, then maximizes factual "
            "accuracy and minimizes harmful reclassification, value/hallucination and JSON "
            "failures. Raw unsafe proposals remain a reported tie-breaker before latency and "
            "tokens. A candidate must first improve on deterministic accuracy, have "
            "zero JSON failures, and correct more preliminary errors than it harms.", "",
        ])
        winner_cases = {item["group_id"]: item for item in winner["case_results"]}
        dataset_groups = {item["group_id"]: item for item in dataset["groups"]}
        examples = [
            ("semantic_paraphrase", "SAME paraphrase"),
            ("calculation_method", "Semantic CHANGED"),
            ("calculation_input", "Numerical CHANGED"),
            ("real_moved_other_sheet", "MOVED"),
            ("false_moved", "False MOVED guard"),
            ("same_words_different_context", "UNCERTAIN context"),
            ("formula_equivalent", "Equivalent formula"),
            ("formula_changed", "Changed formula"),
        ]
        lines.extend(["### Representative cases", ""])
        for group_id, label in examples:
            case = winner_cases[group_id]
            source_group = dataset_groups[group_id]
            baseline = reviewer.validate_response(
                _baseline_response([source_group]), [source_group], safe_same_moved=False
            )[0]["decisions"]
            expected_labels = ", ".join(item["final_status"] for item in source_group["ground_truth"])
            baseline_labels = ", ".join(
                _actual_label(item, baseline) for item in source_group["ground_truth"]
            )
            final_labels = ", ".join(
                _actual_label(item, case["decisions"] or []) for item in source_group["ground_truth"]
            )
            lines.append(
                f"- **{label}** (`{group_id}`): deterministic `{baseline_labels}` → "
                f"final `{final_labels}`; ground truth `{expected_labels}`; "
                f"validation `{case['validation_errors'] or 'ok'}`."
            )
        lines.append("")
    lines.extend(["", "## Confusion matrices and errors", ""])
    for row in summaries:
        lines.extend([
            f"### {row['provider']} / {row['requested_model']} / {row['mode']}", "",
            "```json", json.dumps(row["confusion_matrix"], ensure_ascii=False, indent=2), "```", "",
        ])
        errors = [x for x in row["case_results"] if x["validation_errors"] or x["metrics"]["correct"] != x["metrics"]["expected"]]
        if not errors:
            lines.extend(["No case errors.", ""])
        else:
            for item in errors:
                lines.append(
                    f"- `{item['group_id']}`: {item['metrics']['correct']}/{item['metrics']['expected']}; "
                    f"false_same={item['metrics']['false_same']}; false_moved={item['metrics']['false_moved']}; "
                    f"harmful={item['metrics']['harmful_reclassification']}; "
                    f"validation={item['validation_errors'] or 'ok'}."
                )
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def summarize(dataset: dict[str, Any]) -> dict[str, Any]:
    runs = [baseline_run(dataset)]
    for path in sorted(RUNS_ROOT.glob("*.json")):
        run = load_json(path)
        if run.get("dataset_sha256") == dataset["dataset_sha256"]:
            runs.append(run)
    summaries = [aggregate_run(dataset, run) for run in runs]
    ground_truth = load_json(GROUND_TRUTH_PATH)
    payload = {
        "version": 1, "dataset_sha256": dataset["dataset_sha256"],
        "ground_truth_sha256": ground_truth["ground_truth_sha256"],
        "response_schema_sha256": dataset["response_schema_sha256"],
        "validator_version": reviewer.VALIDATOR_VERSION,
        "group_count": len(dataset["groups"]), "runs": summaries,
        "production_candidate": (
            {
                key: winner[key] for key in (
                    "provider", "requested_model", "mode", "reasoning_effort",
                    "final_classification_accuracy", "false_same", "false_moved",
                    "raw_model_false_same", "raw_model_false_moved",
                    "correct_reclassification", "harmful_reclassification",
                    "value_failures", "hallucinations", "json_failures",
                    "avg_group_time_sec", "input_tokens", "output_tokens",
                )
            }
            if (winner := select_production_candidate(summaries)) else None
        ),
    }
    write_json(SUMMARY_PATH, payload)
    REPORT_PATH.write_text(build_report(dataset, summaries), encoding="utf-8")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("build", "run", "summarize", "all"))
    parser.add_argument("--provider", choices=tuple(MODELS))
    parser.add_argument("--model", action="append", default=[])
    parser.add_argument("--mode", choices=("with_hint", "without_hint"))
    parser.add_argument("--timeout", type=float, default=240.0)
    parser.add_argument("--no-resume", action="store_true")
    args = parser.parse_args()
    dataset = build_dataset() if args.command in {"build", "all"} else load_json(DATASET_PATH)
    if args.command in {"run", "all"}:
        providers = (args.provider,) if args.provider else tuple(MODELS)
        modes = (args.mode,) if args.mode else ("with_hint", "without_hint")
        for provider in providers:
            models = tuple(args.model) if args.model else MODELS[provider]
            for model in models:
                if model not in MODELS[provider]:
                    raise SystemExit(f"model not in verified runtime set: {provider}/{model}")
                for mode in modes:
                    run_model(
                        dataset, provider, model, mode,
                        timeout=args.timeout, resume=not args.no_resume,
                    )
    if args.command in {"summarize", "all"}:
        result = summarize(dataset)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"dataset={DATASET_PATH} sha256={dataset['dataset_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
