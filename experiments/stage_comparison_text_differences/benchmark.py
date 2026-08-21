#!/usr/bin/env python3
"""Reproducible Stage 3 text-difference benchmark on real comparison data.

The immutable dataset contains only fragments already marked
``remaining_for_comparison`` by Stage 2.  Ground truth is a hand-authored map
of source fragment ids; no model is used as a judge.
"""
from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import math
import re
import statistics
import subprocess
import time
import unicodedata
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[2]
ARTIFACT_ROOT = Path(__file__).resolve().parent / "artifacts"
DATASET_PATH = ARTIFACT_ROOT / "benchmark_dataset.json"
RUNS_ROOT = ARTIFACT_ROOT / "runs"
SUMMARY_PATH = ARTIFACT_ROOT / "benchmark_summary.json"
REPORT_PATH = ARTIFACT_ROOT / "BENCHMARK_REPORT.md"
SESSION_ID = "121d764109184c13"
SOURCE_ROOT = REPO_ROOT / "comparison" / "sessions" / SESSION_ID / "pairs"

MODELS = {
    "codex": (
        "gpt-5.6-luna",
        "gpt-5.6-terra",
        "gpt-5.6-sol",
    ),
    "claude": (
        "claude-fable-5",
        "claude-sonnet-5",
        "claude-opus-5",
    ),
}
REASONING_EFFORT = "medium"
PROMPT_VERSION = "stage3_text_delta_v1"


def changed(
    left_ids: list[str], right_ids: list[str], summary: str,
) -> dict[str, Any]:
    return {"left_ids": left_ids, "right_ids": right_ids, "summary": summary}


def removed(left_ids: list[str], summary: str) -> dict[str, Any]:
    return {"left_ids": left_ids, "summary": summary}


def added(right_ids: list[str], summary: str) -> dict[str, Any]:
    return {"right_ids": right_ids, "summary": summary}


# These ids were independently reviewed against the real Stage 2 artifact.
# Every id is revalidated by build_dataset() before the snapshot is written.
CASE_SPECS: tuple[dict[str, Any], ...] = (
    {
        "id": "area_and_added_room",
        "category": "table_numeric_and_added",
        "pair_id": "p570d156f57",
        "link_id": "link_5bac0a5098c7",
        "left_ids": ["txt_e80e92b24f192d93", "txt_aa1c03c75c340d10"],
        "right_ids": [
            "txt_fe97492628af5ca5", "txt_3298fa2e583b7208",
            "txt_953d02f2bdb15653",
        ],
        "ground_truth": {
            "changed": [
                changed(
                    ["txt_e80e92b24f192d93"], ["txt_fe97492628af5ca5"],
                    "Площадь автостоянки С.А.2: 4394,20 → 4369,80 м².",
                ),
                changed(
                    ["txt_aa1c03c75c340d10"], ["txt_3298fa2e583b7208"],
                    "Площадь электрощитовой 1.Т.5: 65,50 → 59,70 м².",
                ),
            ],
            "removed": [],
            "added": [added(
                ["txt_953d02f2bdb15653"],
                "С.А.1 Автостоянка — 10830,50 м², категория B2.",
            )],
        },
    },
    {
        "id": "equipment_marks",
        "category": "stable_key_mark_change",
        "pair_id": "p570d156f57",
        "link_id": "link_5bac0a5098c7",
        "left_ids": [
            "txt_08cf6ef3e5fd2670", "txt_8fe1c0ead799ea2e",
            "txt_0a9d101db83a7af5",
        ],
        "right_ids": [
            "txt_670a57c3be6569f4", "txt_3d6a5299ee1b8411",
            "txt_a9752d4dad25a29f",
        ],
        "ground_truth": {
            "changed": [
                changed(
                    ["txt_08cf6ef3e5fd2670"], ["txt_670a57c3be6569f4"],
                    "Наименование 2.К.1: «Проеход блока кладовых» → «Проезд блока кладовых».",
                ),
                changed(
                    ["txt_8fe1c0ead799ea2e"], ["txt_3d6a5299ee1b8411"],
                    "Марка электрощитовой 2.Т.1: ВРЧ-К2 → ВРУ-К2.",
                ),
                changed(
                    ["txt_0a9d101db83a7af5"], ["txt_a9752d4dad25a29f"],
                    "Марка электрощитовой 2.Т.2: ВРЧ1 → ВРУ1.",
                ),
            ],
            "removed": [],
            "added": [],
        },
    },
    {
        "id": "formatting_only",
        "category": "almost_unchanged",
        "pair_id": "p570d156f57",
        "link_id": "link_5bac0a5098c7",
        "left_ids": ["txt_20398df4864d255a"],
        "right_ids": ["txt_6c64ed13c41fcd36"],
        "ground_truth": {"changed": [], "removed": [], "added": []},
    },
    {
        "id": "wastewater_values",
        "category": "multi_value_table_row",
        "pair_id": "p3f0a37b288",
        "link_id": "link_f74183e1c225",
        "left_ids": ["txt_8987880b31131ac4"],
        "right_ids": ["txt_4a8eaf20a282f69d"],
        "ground_truth": {
            "changed": [changed(
                ["txt_8987880b31131ac4"], ["txt_4a8eaf20a282f69d"],
                "Общий расход стоков на дом: 114,08 / 10,58 / 4,65 → 121,33 / 12,24 / 6,41.",
            )],
            "removed": [],
            "added": [],
        },
    },
    {
        "id": "unrelated_corrections",
        "category": "false_changed_trap",
        "pair_id": "p597ae0bb5d",
        "link_id": "link_9ac9ff39564b",
        "left_ids": ["txt_ab103faee9411ce2", "txt_e6de1f25c20f1626"],
        "right_ids": ["txt_c1e76795f7202656", "txt_ecef6c97de398bd0"],
        "ground_truth": {
            "changed": [],
            "removed": [
                removed(
                    ["txt_ab103faee9411ce2"],
                    "Изменение типа, планировки и количества квартир.",
                ),
                removed(
                    ["txt_e6de1f25c20f1626"],
                    "Увеличение этажности корпуса 4: 10 → 16 этажей.",
                ),
            ],
            "added": [
                added(
                    ["txt_c1e76795f7202656"],
                    "Увеличение высоты этажей 2–16 корпуса 4.",
                ),
                added(
                    ["txt_ecef6c97de398bd0"],
                    "Увеличение высоты оконных проёмов: 2,85 → 3,10 м.",
                ),
            ],
        },
    },
    {
        "id": "long_note_reference",
        "category": "long_rewritten_note",
        "pair_id": "p597ae0bb5d",
        "link_id": "link_0e6fcde0c4a4",
        "left_ids": ["txt_8512d2770cd66577"],
        "right_ids": ["txt_4e6a709a0da14f97"],
        "ground_truth": {
            "changed": [changed(
                ["txt_8512d2770cd66577"], ["txt_4e6a709a0da14f97"],
                "Источник расчётов инсоляции: Приложение 1 → графическая часть; ссылка на Приложение 1 удалена.",
            )],
            "removed": [],
            "added": [],
        },
    },
    {
        "id": "title_metadata",
        "category": "document_metadata",
        "pair_id": "pbb3aa1717f",
        "link_id": "link_ffeee0e233a5",
        "left_ids": [
            "txt_33b07d3d9025e9e3", "txt_e661954ac70568a1",
            "txt_21a8e970728a46fa",
        ],
        "right_ids": [
            "txt_25d2578d58c845a0", "txt_094d1a5fdcaba7bb",
            "txt_325122c0e854be55",
        ],
        "ground_truth": {
            "changed": [
                changed(
                    ["txt_33b07d3d9025e9e3"], ["txt_25d2578d58c845a0"],
                    "Шифр документа: АА/БЭ-03-ДСЗ-АР2 → АА/БЭ-03-ДСЗ-АР2-КОРР.",
                ),
                changed(
                    ["txt_e661954ac70568a1"], ["txt_094d1a5fdcaba7bb"],
                    "Номер тома: 3.2 → 3.1.",
                ),
                changed(
                    ["txt_21a8e970728a46fa"], ["txt_325122c0e854be55"],
                    "Год: 2024 → 2025.",
                ),
            ],
            "removed": [],
            "added": [],
        },
    },
    {
        "id": "customer_change",
        "category": "name_change",
        "pair_id": "pfc7429293b",
        "link_id": "link_9b280d5e5b14",
        "left_ids": ["txt_51393a3e9e16c794"],
        "right_ids": ["txt_a17aec0369c61110"],
        "ground_truth": {
            "changed": [changed(
                ["txt_51393a3e9e16c794"], ["txt_a17aec0369c61110"],
                "Заказчик: ООО «Артел Архитектс» → ООО «МБ-Проект Бюро».",
            )],
            "removed": [],
            "added": [],
        },
    },
    {
        "id": "removed_note",
        "category": "removed_long_text",
        "pair_id": "p597ae0bb5d",
        "link_id": "link_d237431408ab",
        "left_ids": ["txt_29a6d53cd0aa76f4", "txt_1f0213a8730759bb"],
        "right_ids": [],
        "ground_truth": {
            "changed": [],
            "removed": [removed(
                ["txt_29a6d53cd0aa76f4", "txt_1f0213a8730759bb"],
                "Примечание о выполнении условий инсоляции на 3-м и вышерасположенных этажах.",
            )],
            "added": [],
        },
    },
    {
        "id": "added_drawing_rows",
        "category": "added_table_rows",
        "pair_id": "p597ae0bb5d",
        "link_id": "link_06f9e80ad3c2",
        "left_ids": [],
        "right_ids": [
            "txt_feb948ca4f6c3dec", "txt_5d3d23a169ee61f5",
            "txt_f5de3bfb35145d1b",
        ],
        "ground_truth": {
            "changed": [],
            "removed": [],
            "added": [
                added(["txt_feb948ca4f6c3dec"], "Ситуационный план, масштаб 1:2000."),
                added(["txt_5d3d23a169ee61f5"], "Схема инсоляции корпуса 4, масштаб 1:500."),
                added(["txt_f5de3bfb35145d1b"], "Расчёт КЕО, точка 1, спальня корпуса 4 на 2-м этаже, масштаб 1:50."),
            ],
        },
    },
    {
        "id": "contents_with_ambiguity",
        "category": "ambiguous_pairing",
        "pair_id": "p26c08b83a6",
        "link_id": "link_4f7238b8c9ed",
        "left_ids": [
            "txt_4aa446ba4f5d1127", "txt_79ec0d971f7d0b3f",
            "txt_ffdd4acd472e7149",
        ],
        "right_ids": [
            "txt_4547ab37fae19405", "txt_68ba5628a0f374ce",
            "txt_fe1382206759393b",
        ],
        "ground_truth": {
            "changed": [changed(
                ["txt_4aa446ba4f5d1127"], ["txt_4547ab37fae19405"],
                "Система уравнивания потенциалов: дополнительная → основная.",
            )],
            "removed": [],
            "added": [],
            "ambiguous": {
                "left_ids": ["txt_79ec0d971f7d0b3f", "txt_ffdd4acd472e7149"],
                "right_ids": ["txt_68ba5628a0f374ce", "txt_fe1382206759393b"],
                "note": "Перенумерация/перестройка перечня ВРУ не даёт надёжной пары строк.",
            },
        },
    },
)


RESULT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["changed", "removed", "added"],
    "properties": {
        "changed": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["left_ids", "right_ids", "summary", "before", "after"],
                "properties": {
                    "left_ids": {"type": "array", "items": {"type": "string"}},
                    "right_ids": {"type": "array", "items": {"type": "string"}},
                    "summary": {"type": "string"},
                    "before": {"type": "string"},
                    "after": {"type": "string"},
                },
            },
        },
        "removed": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["left_ids", "summary", "before"],
                "properties": {
                    "left_ids": {"type": "array", "items": {"type": "string"}},
                    "summary": {"type": "string"},
                    "before": {"type": "string"},
                },
            },
        },
        "added": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["right_ids", "summary", "after"],
                "properties": {
                    "right_ids": {"type": "array", "items": {"type": "string"}},
                    "summary": {"type": "string"},
                    "after": {"type": "string"},
                },
            },
        },
    },
}


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def sha256_json(payload: Any) -> str:
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def build_dataset() -> dict[str, Any]:
    cases: list[dict[str, Any]] = []
    source_signatures: dict[str, str] = {}
    for spec in CASE_SPECS:
        pair_root = SOURCE_ROOT / spec["pair_id"]
        comparison = load_json(pair_root / "text_comparison.json")
        exclusions = load_json(pair_root / "text_exclusions.json")
        if exclusions.get("source_signature") != comparison.get("source_signature"):
            raise RuntimeError(f"stale exclusions for {spec['pair_id']}")
        source_signatures[spec["pair_id"]] = str(comparison["source_signature"])
        links = {
            str(item["id"]): item
            for item in load_json(pair_root / "sheet_links.json")["links"]
        }
        link = links[spec["link_id"]]
        remaining = {
            side: set(comparison["remaining"][side]) for side in ("left", "right")
        }
        fragments = {
            side: {str(item["id"]): item for item in comparison["fragments"][side]}
            for side in ("left", "right")
        }
        resolved: dict[str, list[dict[str, Any]]] = {}
        for side in ("left", "right"):
            selected = []
            for fragment_id in spec[f"{side}_ids"]:
                if fragment_id not in remaining[side]:
                    raise RuntimeError(
                        f"{spec['id']}: {fragment_id} is not remaining_for_comparison"
                    )
                source = fragments[side].get(fragment_id)
                if not source:
                    raise RuntimeError(f"{spec['id']}: missing {fragment_id}")
                allowed_pages = set(link[f"{side}_pages"])
                if int(source["pdf_page"]) not in allowed_pages:
                    raise RuntimeError(f"{spec['id']}: {fragment_id} outside accepted link")
                selected.append({
                    "id": fragment_id,
                    "page": int(source["pdf_page"]),
                    "text": str(source["text"]),
                    "canonical_text": str(source["canonical_text"]),
                })
            resolved[side] = selected
        case = {
            "id": spec["id"],
            "category": spec["category"],
            "pair_id": spec["pair_id"],
            "link_id": spec["link_id"],
            "left_pages": list(link["left_pages"]),
            "right_pages": list(link["right_pages"]),
            "left_fragments": resolved["left"],
            "right_fragments": resolved["right"],
            "ground_truth": spec["ground_truth"],
        }
        cases.append(case)
    payload = {
        "version": 1,
        "prompt_version": PROMPT_VERSION,
        "source": {
            "session_id": SESSION_ID,
            "pair_source_signatures": source_signatures,
            "only_remaining_for_comparison": True,
            "ground_truth_author": "manual_id_mapping",
            "production_dependency": False,
        },
        "cases": cases,
    }
    payload["dataset_sha256"] = sha256_json(payload)
    write_json(DATASET_PATH, payload)
    return payload


def prompt_for(case: dict[str, Any]) -> str:
    model_case = {
        "case_id": case["id"],
        "left_pages": case["left_pages"],
        "right_pages": case["right_pages"],
        "left_fragments": [
            {"id": item["id"], "text": item["text"]}
            for item in case["left_fragments"]
        ],
        "right_fragments": [
            {"id": item["id"], "text": item["text"]}
            for item in case["right_fragments"]
        ],
    }
    return """Ты сравниваешь только оставшиеся текстовые фрагменты П (LEFT) и РД (RIGHT).

Определи factual delta: changed, removed, added.
- changed допустим только для фрагментов об одной и той же сущности/решении;
- если соответствие ненадёжно, выбирай removed + added;
- не делай инженерных выводов, не оценивай значимость, не исправляй исходные данные;
- не придумывай fragment ids и не используй id вне входа;
- before/after верни дословно из соответствующих фрагментов; несколько текстов соединяй переводом строки;
- summary формулируй кратко, сохраняя числа, марки, площади, размеры и обозначения;
- чистое различие Markdown/LaTeX/пробелов без фактического изменения не выводи;
- каждый входной id можно использовать не более одного раза;
- верни только один JSON-объект, без Markdown.

JSON Schema:
""" + json.dumps(RESULT_SCHEMA, ensure_ascii=False, separators=(",", ":")) + "\n\nINPUT:\n" + json.dumps(
        model_case, ensure_ascii=False, separators=(",", ":")
    )


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
            raw_usage = event.get("usage") or {}
            usage = {
                key: int(value) for key, value in raw_usage.items()
                if isinstance(value, int)
            }
    return answer, usage, None


def _claude_result(stdout: str, requested_model: str) -> tuple[str, dict[str, int], str | None]:
    payload = json.loads(stdout)
    raw_usage = payload.get("usage") or {}
    usage = {
        key: int(value) for key, value in raw_usage.items()
        if isinstance(value, int)
    }
    reported = None
    model_usage = payload.get("modelUsage") or {}
    if requested_model in model_usage:
        reported = requested_model
    else:
        candidates = [
            str(name) for name in model_usage
            if not str(name).startswith("claude-haiku-")
        ]
        if len(candidates) == 1:
            reported = candidates[0]
    return str(payload.get("result") or ""), usage, reported


def invoke(provider: str, model: str, prompt: str, timeout: float) -> dict[str, Any]:
    if provider == "codex":
        argv = [
            "codex", "exec", "--json", "--sandbox", "read-only",
            "--skip-git-repo-check", "--ephemeral", "--ignore-rules",
            "--ignore-user-config", f"--model={model}", "-c",
            f'model_reasoning_effort="{REASONING_EFFORT}"', "-",
        ]
    elif provider == "claude":
        argv = [
            "claude", "--safe-mode", "--strict-mcp-config",
            "--disable-slash-commands", "--no-session-persistence",
            "--setting-sources=", f"--model={model}",
            f"--effort={REASONING_EFFORT}", "--tools=",
            "--permission-mode", "dontAsk", "--max-turns", "1",
            "--output-format", "json", "-p",
        ]
    else:
        raise ValueError(provider)
    started = time.monotonic()
    try:
        completed = subprocess.run(
            argv, input=prompt, text=True, capture_output=True,
            cwd=REPO_ROOT, timeout=timeout, check=False,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "ok": False,
            "elapsed_sec": round(time.monotonic() - started, 3),
            "exit_code": None,
            "error": "timeout",
            "stdout": str(exc.stdout or ""),
            "stderr": str(exc.stderr or ""),
        }
    elapsed = round(time.monotonic() - started, 3)
    try:
        if provider == "codex":
            answer, usage, reported = _codex_result(completed.stdout)
        else:
            answer, usage, reported = _claude_result(completed.stdout, model)
    except (json.JSONDecodeError, ValueError, TypeError) as exc:
        answer, usage, reported = "", {}, None
        parse_error = f"provider_envelope: {exc}"
    else:
        parse_error = ""
    return {
        "ok": completed.returncode == 0 and bool(answer),
        "elapsed_sec": elapsed,
        "exit_code": completed.returncode,
        "reported_model": reported,
        "usage": usage,
        "answer": answer,
        "error": parse_error or (completed.stderr.strip() if completed.returncode else ""),
    }


def run_model(
    dataset: dict[str, Any], provider: str, model: str, *, timeout: float,
    resume: bool,
) -> dict[str, Any]:
    safe_model = re.sub(r"[^a-zA-Z0-9_.-]+", "_", model)
    out_path = RUNS_ROOT / f"{provider}__{safe_model}__{REASONING_EFFORT}.json"
    if resume and out_path.is_file():
        existing = load_json(out_path)
        if existing.get("dataset_sha256") == dataset["dataset_sha256"]:
            return existing
    results = []
    for index, case in enumerate(dataset["cases"], 1):
        print(f"[{provider}/{model}] {index}/{len(dataset['cases'])} {case['id']}", flush=True)
        call = invoke(provider, model, prompt_for(case), timeout)
        results.append({"case_id": case["id"], **call})
        payload = {
            "version": 1,
            "provider": provider,
            "requested_model": model,
            "reasoning_effort": REASONING_EFFORT,
            "dataset_sha256": dataset["dataset_sha256"],
            "prompt_version": PROMPT_VERSION,
            "cases": results,
        }
        write_json(out_path, payload)
    return load_json(out_path)


def canonicalize(value: str) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).lower().replace("ё", "е")
    text = re.sub(r"\\(?:geq|ge)", "≥", text)
    text = re.sub(r"\\text\s*\{([^{}]*)\}", r"\1", text)
    text = text.replace("м^3", "м3").replace("м³", "м3")
    text = text.replace("–", "-").replace("—", "-").replace("−", "-")
    text = re.sub(r"(?<=\d),(?=\d)", ".", text)
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"(?<=[≥≤])\s+", "", text)
    text = re.sub(r"(?<=\d)\s+(?=[a-zа-я])", "", text)
    return re.sub(r"\s*([|:;,.()\-/])\s*", r"\1", text).strip(" .")


def stable_key(text: str) -> str:
    canonical = canonicalize(text)
    field = re.match(r"^(заказчик|проектировщик)\s*[:\-]", canonical)
    if field:
        return field.group(1)
    match = re.match(
        r"^([a-zа-я0-9]+(?:[.\-][a-zа-я0-9]+)+)(?=\s|:|$)", canonical
    )
    return match.group(1) if match else ""


def similarity(left: str, right: str) -> float:
    return difflib.SequenceMatcher(
        None, canonicalize(left), canonicalize(right), autojunk=False
    ).ratio()


def deterministic_baseline(case: dict[str, Any]) -> dict[str, Any]:
    left = {item["id"]: item for item in case["left_fragments"]}
    right = {item["id"]: item for item in case["right_fragments"]}
    unused_left = set(left)
    unused_right = set(right)
    output = {"changed": [], "removed": [], "added": []}

    def consume(left_id: str, right_id: str, *, is_same: bool = False) -> None:
        unused_left.discard(left_id)
        unused_right.discard(right_id)
        if is_same:
            return
        before, after = left[left_id]["text"], right[right_id]["text"]
        output["changed"].append({
            "left_ids": [left_id], "right_ids": [right_id],
            "summary": f"{before} → {after}", "before": before, "after": after,
        })

    exact_left: dict[str, list[str]] = {}
    exact_right: dict[str, list[str]] = {}
    for fragment_id, item in left.items():
        exact_left.setdefault(canonicalize(item["text"]), []).append(fragment_id)
    for fragment_id, item in right.items():
        exact_right.setdefault(canonicalize(item["text"]), []).append(fragment_id)
    for canonical in sorted(set(exact_left) & set(exact_right)):
        if len(exact_left[canonical]) == len(exact_right[canonical]) == 1:
            consume(exact_left[canonical][0], exact_right[canonical][0], is_same=True)

    left_keys: dict[str, list[str]] = {}
    right_keys: dict[str, list[str]] = {}
    for fragment_id in unused_left:
        key = stable_key(left[fragment_id]["text"])
        if key:
            left_keys.setdefault(key, []).append(fragment_id)
    for fragment_id in unused_right:
        key = stable_key(right[fragment_id]["text"])
        if key:
            right_keys.setdefault(key, []).append(fragment_id)
    for key in sorted(set(left_keys) & set(right_keys)):
        if len(left_keys[key]) == len(right_keys[key]) == 1:
            consume(left_keys[key][0], right_keys[key][0])

    candidates = []
    for left_id in unused_left:
        for right_id in unused_right:
            score = similarity(left[left_id]["text"], right[right_id]["text"])
            if score >= 0.76:
                candidates.append((score, left_id, right_id))
    for _, left_id, right_id in sorted(candidates, reverse=True):
        if left_id in unused_left and right_id in unused_right:
            consume(left_id, right_id)

    for fragment_id in sorted(unused_left):
        text = left[fragment_id]["text"]
        output["removed"].append({
            "left_ids": [fragment_id], "summary": text, "before": text,
        })
    for fragment_id in sorted(unused_right):
        text = right[fragment_id]["text"]
        output["added"].append({
            "right_ids": [fragment_id], "summary": text, "after": text,
        })
    return output


def validate_result(value: Any, case: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
    errors: list[str] = []
    if not isinstance(value, dict) or set(value) != {"changed", "removed", "added"}:
        return None, ["root_schema"]
    definitions = {
        "changed": ({"left_ids", "right_ids", "summary", "before", "after"}, ("left_ids", "right_ids")),
        "removed": ({"left_ids", "summary", "before"}, ("left_ids",)),
        "added": ({"right_ids", "summary", "after"}, ("right_ids",)),
    }
    available = {
        "left_ids": {item["id"] for item in case["left_fragments"]},
        "right_ids": {item["id"] for item in case["right_fragments"]},
    }
    used = {"left_ids": set(), "right_ids": set()}
    for bucket, (keys, id_fields) in definitions.items():
        items = value.get(bucket)
        if not isinstance(items, list):
            errors.append(f"{bucket}_not_list")
            continue
        for index, item in enumerate(items):
            prefix = f"{bucket}[{index}]"
            if not isinstance(item, dict) or set(item) != keys:
                errors.append(f"{prefix}_schema")
                continue
            for key in keys - set(id_fields):
                if not isinstance(item.get(key), str):
                    errors.append(f"{prefix}_{key}_not_string")
            for field in id_fields:
                ids = item.get(field)
                if not isinstance(ids, list) or not ids or not all(isinstance(x, str) for x in ids):
                    errors.append(f"{prefix}_{field}_invalid")
                    continue
                if len(ids) != len(set(ids)):
                    errors.append(f"{prefix}_{field}_duplicates")
                for fragment_id in ids:
                    if fragment_id not in available[field]:
                        errors.append(f"{prefix}_{field}_hallucinated:{fragment_id}")
                    if fragment_id in used[field]:
                        errors.append(f"{prefix}_{field}_reused:{fragment_id}")
                    used[field].add(fragment_id)
    return (value if not errors else None), errors


def _id_set(item: dict[str, Any], key: str) -> frozenset[str]:
    return frozenset(str(value) for value in item.get(key) or [])


def _numbers(text: str) -> set[str]:
    return set(re.findall(r"(?<![a-zа-я0-9])\d+(?:[.,]\d+)*(?!\d)", text.lower()))


def evaluate_case(case: dict[str, Any], result: dict[str, Any] | None) -> dict[str, Any]:
    truth = case["ground_truth"]
    ambiguous = truth.get("ambiguous") or {}
    ambiguous_left = set(ambiguous.get("left_ids") or [])
    ambiguous_right = set(ambiguous.get("right_ids") or [])
    expected_changed = {
        (_id_set(item, "left_ids"), _id_set(item, "right_ids"))
        for item in truth["changed"]
    }
    expected_removed = {
        fragment_id for item in truth["removed"] for fragment_id in item["left_ids"]
    }
    expected_added = {
        fragment_id for item in truth["added"] for fragment_id in item["right_ids"]
    }
    metrics = {
        "expected_changed": len(expected_changed), "changed_tp": 0,
        "changed_fn": len(expected_changed), "false_changed": 0,
        "expected_removed": len(expected_removed), "removed_tp": 0,
        "removed_fp": 0, "removed_fn": len(expected_removed),
        "expected_added": len(expected_added), "added_tp": 0,
        "added_fp": 0, "added_fn": len(expected_added),
        "value_errors": 0, "hallucinations": 0,
        "summary_good": 0, "summary_total": 0,
    }
    if result is None:
        return metrics
    actual_changed = set()
    for item in result["changed"]:
        left_ids = _id_set(item, "left_ids")
        right_ids = _id_set(item, "right_ids")
        if left_ids & ambiguous_left or right_ids & ambiguous_right:
            continue
        pair = (left_ids, right_ids)
        actual_changed.add(pair)
        if pair not in expected_changed:
            metrics["false_changed"] += 1
    metrics["changed_tp"] = len(expected_changed & actual_changed)
    metrics["changed_fn"] = len(expected_changed - actual_changed)

    actual_removed = {
        fragment_id for item in result["removed"]
        for fragment_id in item["left_ids"] if fragment_id not in ambiguous_left
    }
    actual_added = {
        fragment_id for item in result["added"]
        for fragment_id in item["right_ids"] if fragment_id not in ambiguous_right
    }
    metrics["removed_tp"] = len(expected_removed & actual_removed)
    metrics["removed_fp"] = len(actual_removed - expected_removed)
    metrics["removed_fn"] = len(expected_removed - actual_removed)
    metrics["added_tp"] = len(expected_added & actual_added)
    metrics["added_fp"] = len(actual_added - expected_added)
    metrics["added_fn"] = len(expected_added - actual_added)

    lookup = {
        "left": {item["id"]: item["text"] for item in case["left_fragments"]},
        "right": {item["id"]: item["text"] for item in case["right_fragments"]},
    }
    forbidden = re.compile(r"критич|нарушен|ошибк|необходимо|стоимост|ухудшен|улучшен", re.I)
    for bucket in ("changed", "removed", "added"):
        for item in result[bucket]:
            left_text = "\n".join(lookup["left"].get(x, "") for x in item.get("left_ids") or [])
            right_text = "\n".join(lookup["right"].get(x, "") for x in item.get("right_ids") or [])
            if "before" in item and item["before"] != left_text:
                metrics["value_errors"] += 1
            if "after" in item and item["after"] != right_text:
                metrics["value_errors"] += 1
            source_numbers = _numbers(left_text + " " + right_text)
            unsupported = _numbers(item.get("summary") or "") - source_numbers
            if unsupported:
                metrics["hallucinations"] += 1
            summary = str(item.get("summary") or "").strip()
            metrics["summary_total"] += 1
            full_length = max(1, len(left_text) + len(right_text))
            if (
                summary and len(summary) <= 200 and not forbidden.search(summary)
                and (len(summary) <= full_length or full_length < 50)
            ):
                metrics["summary_good"] += 1
    return metrics


def safe_ratio(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 4) if denominator else None


def percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(fraction * len(ordered)) - 1)
    return round(ordered[index], 3)


def aggregate_run(dataset: dict[str, Any], run: dict[str, Any]) -> dict[str, Any]:
    cases = {case["id"]: case for case in dataset["cases"]}
    totals: dict[str, int] = {}
    elapsed: list[float] = []
    json_failures = 0
    case_results = []
    reported_models = set()
    input_tokens = output_tokens = 0
    usage_available = True
    for call in run["cases"]:
        case = cases[call["case_id"]]
        parsed = None
        validation_errors: list[str] = []
        if not call.get("ok"):
            validation_errors.append(call.get("error") or "provider_failure")
        else:
            try:
                raw = json.loads(call.get("answer") or "")
            except (json.JSONDecodeError, ValueError) as exc:
                validation_errors.append(f"json_parse:{exc}")
            else:
                parsed, validation_errors = validate_result(raw, case)
        if parsed is None:
            json_failures += 1
        metrics = evaluate_case(case, parsed)
        for key, value in metrics.items():
            totals[key] = totals.get(key, 0) + int(value)
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
        case_results.append({
            "case_id": case["id"], "validation_errors": validation_errors,
            "metrics": metrics, "result": parsed,
        })
    removed_precision = safe_ratio(
        totals.get("removed_tp", 0), totals.get("removed_tp", 0) + totals.get("removed_fp", 0)
    )
    removed_recall = safe_ratio(totals.get("removed_tp", 0), totals.get("expected_removed", 0))
    added_precision = safe_ratio(
        totals.get("added_tp", 0), totals.get("added_tp", 0) + totals.get("added_fp", 0)
    )
    added_recall = safe_ratio(totals.get("added_tp", 0), totals.get("expected_added", 0))
    return {
        "provider": run["provider"],
        "requested_model": run["requested_model"],
        "reported_models": sorted(reported_models),
        "reasoning_effort": run["reasoning_effort"],
        "cases": len(run["cases"]),
        "changed_pairing_accuracy": safe_ratio(
            totals.get("changed_tp", 0), totals.get("expected_changed", 0)
        ),
        "changed_tp": totals.get("changed_tp", 0),
        "changed_expected": totals.get("expected_changed", 0),
        "false_changed": totals.get("false_changed", 0),
        "removed_precision": removed_precision,
        "removed_recall": removed_recall,
        "added_precision": added_precision,
        "added_recall": added_recall,
        "value_errors": totals.get("value_errors", 0),
        "hallucinations": totals.get("hallucinations", 0),
        "json_failures": json_failures,
        "summary_quality": safe_ratio(
            totals.get("summary_good", 0), totals.get("summary_total", 0)
        ),
        "avg_time_sec": round(statistics.mean(elapsed), 3) if elapsed else None,
        "p50_time_sec": round(statistics.median(elapsed), 3) if elapsed else None,
        "p95_time_sec": percentile(elapsed, 0.95),
        "input_tokens": input_tokens if usage_available else None,
        "output_tokens": output_tokens if usage_available else None,
        "case_results": case_results,
    }


def baseline_run(dataset: dict[str, Any]) -> dict[str, Any]:
    calls = []
    for case in dataset["cases"]:
        started = time.monotonic()
        answer = deterministic_baseline(case)
        calls.append({
            "case_id": case["id"], "ok": True,
            "elapsed_sec": round(time.monotonic() - started, 6),
            "reported_model": None, "usage": {},
            "answer": json.dumps(answer, ensure_ascii=False), "error": "",
        })
    return {
        "version": 1, "provider": "deterministic",
        "requested_model": "DETERMINISTIC BASELINE",
        "reasoning_effort": "none", "dataset_sha256": dataset["dataset_sha256"],
        "prompt_version": PROMPT_VERSION, "cases": calls,
    }


def fmt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def _compact(value: str, limit: int = 320) -> str:
    text = re.sub(r"\s+", " ", str(value or "")).strip().replace("`", "'")
    return text if len(text) <= limit else text[:limit - 1].rstrip() + "…"


def _case_input(case: dict[str, Any], side: str) -> str:
    return _compact(" ⏐ ".join(item["text"] for item in case[f"{side}_fragments"]))


def _truth_digest(case: dict[str, Any]) -> str:
    truth = case["ground_truth"]
    pieces = []
    for bucket in ("changed", "removed", "added"):
        summaries = [_compact(item["summary"], 180) for item in truth[bucket]]
        pieces.append(
            f"{bucket}={len(summaries)}"
            + (f" ({'; '.join(summaries)})" if summaries else "")
        )
    if truth.get("ambiguous"):
        pieces.append("неоднозначная пара исключена из exact metric")
    return "; ".join(pieces)


def _result_digest(case_result: dict[str, Any]) -> str:
    if case_result["validation_errors"]:
        return "не принят: " + ", ".join(case_result["validation_errors"])
    result = case_result["result"] or {"changed": [], "removed": [], "added": []}
    counts = "/".join(
        str(len(result[bucket])) for bucket in ("changed", "removed", "added")
    )
    summaries = [
        _compact(item.get("summary") or "", 140)
        for bucket in ("changed", "removed", "added")
        for item in result[bucket]
    ]
    return f"changed/removed/added={counts}" + (
        f" — {'; '.join(summaries)}" if summaries else ""
    )


def build_report(dataset: dict[str, Any], summaries: list[dict[str, Any]]) -> str:
    lines = [
        "# Benchmark текстовых расхождений П ↔ РД", "",
        f"Dataset: `{dataset['dataset_sha256']}`; кейсов: **{len(dataset['cases'])}**; "
        f"листовых групп: **{len({(c['pair_id'], c['link_id']) for c in dataset['cases']})}**.",
        "",
        "Все model-calls получили один prompt, один JSON Schema, один порядок фрагментов и effort `medium`. "
        "Temperature ни один CLI не экспонирует. Неоднозначные ID кейса `contents_with_ambiguity` исключены из точной метрики.",
        "Codex-модели взяты из фактического `codex debug models`, Claude-модели подтверждены успешными "
        "вызовами текущей авторизованной сессии. CLI: Codex 0.149.0-alpha.4, Claude Code 2.1.233.",
        "Для Claude input включает обычные, cache creation и cache read input tokens; для Codex — "
        "сообщённый CLI `input_tokens`. Стоимость не вычислялась.",
        "",
        "| Контур | Модель | Changed | False changed | Removed P/R | Added P/R | Halluc. | Value err. | JSON fail | Summary | Avg / p50 / p95, s | Input / output |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in summaries:
        lines.append(
            "| {provider} | {model} | {tp}/{expected} ({accuracy}) | {false} | "
            "{rp}/{rr} | {ap}/{ar} | {hall} | {value} | {jsonf} | {summary} | "
            "{avg} / {p50} / {p95} | {inp} / {out} |".format(
                provider=row["provider"], model=row["requested_model"],
                tp=row["changed_tp"], expected=row["changed_expected"],
                accuracy=fmt(row["changed_pairing_accuracy"]), false=row["false_changed"],
                rp=fmt(row["removed_precision"]), rr=fmt(row["removed_recall"]),
                ap=fmt(row["added_precision"]), ar=fmt(row["added_recall"]),
                hall=row["hallucinations"], value=row["value_errors"],
                jsonf=row["json_failures"], summary=fmt(row["summary_quality"]),
                avg=fmt(row["avg_time_sec"]), p50=fmt(row["p50_time_sec"]),
                p95=fmt(row["p95_time_sec"]), inp=fmt(row["input_tokens"]),
                out=fmt(row["output_tokens"]),
            )
        )
    lines.extend([
        "", "## Выбор архитектуры", "",
        "Выбран **вариант A: полностью deterministic**. Baseline разделил факты не хуже пяти из шести "
        "модельных контуров: 11/12 changed, 0 false changed, 0 hallucinations и 0 value errors. "
        "Единственный сложный кейс он консервативно оставил как removed + added. Модели эту неоднозначность "
        "не разрешили стабильно: четыре повторили ошибку baseline, Luna перенесла pairing-ошибку в другой "
        "кейс, Sonnet вернул невалидный пустой ответ. "
        "Terra дала лучший summary среди фактически надёжных model-runs, но не улучшила factual metrics, "
        "добавила примерно 11 секунд на вызов и большой контекст. Поэтому production не вызывает модель; "
        "строгий JSON/provenance validator сохранён как проверяемый контракт для будущего gated-варианта.",
        "", "## Показательные реальные кейсы", "",
        "Ниже семь выборочных сравнений, а не полный dump. Формат счётчика: `changed/removed/added`.", "",
    ])
    cases = {case["id"]: case for case in dataset["cases"]}
    by_model = {
        (row["provider"], row["requested_model"]): {
            item["case_id"]: item for item in row["case_results"]
        }
        for row in summaries
    }
    examples = (
        ("area_and_added_room", (
            ("deterministic", "DETERMINISTIC BASELINE"),
            ("codex", "gpt-5.6-terra"), ("claude", "claude-opus-5"),
        ), "Все три пути сохранили числа и корректно отделили новую строку таблицы."),
        ("equipment_marks", (
            ("claude", "claude-sonnet-5"), ("codex", "gpt-5.6-terra"),
        ), "Terra сохранила все три пары; Sonnet пропустил одну изменённую марку."),
        ("formatting_only", (
            ("deterministic", "DETERMINISTIC BASELINE"), ("codex", "gpt-5.6-sol"),
        ), "Оба пути правильно не создали расхождение из безопасного форматирования."),
        ("unrelated_corrections", (
            ("claude", "claude-sonnet-5"), ("claude", "claude-fable-5"),
            ("codex", "gpt-5.6-terra"),
        ), "Fable и Terra не создали ложный changed; Sonnet не прошёл JSON-контракт."),
        ("long_note_reference", (
            ("deterministic", "DETERMINISTIC BASELINE"),
            ("codex", "gpt-5.6-terra"), ("claude", "claude-fable-5"),
        ), "Фактическая пара найдена всеми; модель полезнее только по краткости формулировки."),
        ("customer_change", (
            ("codex", "gpt-5.6-luna"), ("codex", "gpt-5.6-terra"),
            ("claude", "claude-opus-5"),
        ), "Luna разложила одно изменение имени на removed + added; Terra и Opus связали его как changed."),
        ("contents_with_ambiguity", (
            ("deterministic", "DETERMINISTIC BASELINE"),
            ("claude", "claude-opus-5"), ("claude", "claude-sonnet-5"),
        ), "Baseline оставил всё как removed + added; Opus тоже не связал системы потенциалов, а его "
           "changed относится к неоднозначным строкам ВРУ; Sonnet вернул невалидный пустой ответ. "
           "Кейс подтверждает приоритет precision; неоднозначные ВРУ-ID исключены из exact metric."),
    )
    for case_id, model_keys, analysis in examples:
        case = cases[case_id]
        lines.extend([
            f"### `{case_id}`", "",
            f"- П: `{_case_input(case, 'left')}`", "",
            f"- РД: `{_case_input(case, 'right')}`", "",
            f"- Эталон: {_truth_digest(case)}", "",
        ])
        for model_key in model_keys:
            result = by_model[model_key][case_id]
            lines.extend([
                f"- {model_key[0]} / `{model_key[1]}`: {_result_digest(result)}", "",
            ])
        lines.extend([f"Вывод: {analysis}", ""])
    lines.extend(["", "## Ошибки по кейсам", ""])
    for row in summaries:
        failures = []
        for case in row["case_results"]:
            metric = case["metrics"]
            if case["validation_errors"] or any(metric[key] for key in (
                "changed_fn", "false_changed", "removed_fp", "removed_fn",
                "added_fp", "added_fn", "value_errors", "hallucinations",
            )):
                failures.append(case)
        lines.append(f"### {row['provider']} / {row['requested_model']}")
        lines.append("")
        if not failures:
            lines.append("Ошибок по точной метрике нет.")
        else:
            for case in failures:
                metric = case["metrics"]
                lines.append(
                    f"- `{case['case_id']}`: changed_fn={metric['changed_fn']}, "
                    f"false_changed={metric['false_changed']}, removed_fp/fn="
                    f"{metric['removed_fp']}/{metric['removed_fn']}, added_fp/fn="
                    f"{metric['added_fp']}/{metric['added_fn']}, value_errors="
                    f"{metric['value_errors']}, hallucinations={metric['hallucinations']}, "
                    f"validation={case['validation_errors'] or 'ok'}."
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
    payload = {
        "version": 1, "dataset_sha256": dataset["dataset_sha256"],
        "case_count": len(dataset["cases"]), "models": summaries,
    }
    write_json(SUMMARY_PATH, payload)
    REPORT_PATH.write_text(build_report(dataset, summaries), encoding="utf-8")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("build", "run", "summarize", "all"))
    parser.add_argument("--provider", choices=tuple(MODELS))
    parser.add_argument("--model", action="append", default=[])
    parser.add_argument("--timeout", type=float, default=180.0)
    parser.add_argument("--no-resume", action="store_true")
    args = parser.parse_args()

    dataset = build_dataset() if args.command in {"build", "all"} else load_json(DATASET_PATH)
    if args.command in {"run", "all"}:
        providers = (args.provider,) if args.provider else tuple(MODELS)
        for provider in providers:
            models = tuple(args.model) if args.model else MODELS[provider]
            for model in models:
                if model not in MODELS[provider]:
                    raise SystemExit(f"model not in verified catalog: {provider}/{model}")
                run_model(
                    dataset, provider, model, timeout=args.timeout,
                    resume=not args.no_resume,
                )
    if args.command in {"summarize", "all"}:
        payload = summarize(dataset)
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    else:
        print(f"dataset={DATASET_PATH} sha256={dataset['dataset_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
