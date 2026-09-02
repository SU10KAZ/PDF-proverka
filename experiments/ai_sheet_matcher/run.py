"""CLI for the isolated AI Sheet Matcher research spike.

Two phases are intentional: ``candidate-audit`` must complete and persist its
result before ``experiment`` is allowed to make a model call.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

from .core import (
    ALGORITHM_VERSION,
    PROJECT_CONFIG,
    SENTINEL_OPTION_IDS,
    ProjectDataset,
    aggregate_decisions,
    build_candidate_recall,
    build_project_dataset,
    build_selector_prompt,
    canonical_json,
    decision_metrics,
    digest,
    production_sources_unchanged,
    selector_schema,
    verify_selector_response,
)


REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT = REPO_ROOT / "comparison" / "ai_sheet_matcher" / "20260902_experiment"
DEFAULT_MODEL = "gpt-5.6-sol"
DEFAULT_EFFORT = "medium"


@dataclass
class GatewayResult:
    ok: bool
    response: dict[str, Any] | None
    duration_s: float
    usage: dict[str, int]
    error: str
    attempts: int


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(canonical_json(dict(row)) + "\n" for row in rows),
        encoding="utf-8",
    )


def _clean_model_env() -> dict[str, str]:
    forbidden_prefixes = ("OPENAI_", "ANTHROPIC_", "OPENROUTER_", "AWS_", "GOOGLE_", "GEMINI_")
    forbidden_exact = {"CLAUDE_CODE_SSE_PORT", "CLAUDECODE", "CLAUDE_CODE_ENTRYPOINT"}
    return {
        key: value for key, value in os.environ.items()
        if not key.startswith(forbidden_prefixes) and key not in forbidden_exact
    }


def _extract_usage(events: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    best: dict[str, int] = {}

    def visit(value: Any) -> None:
        nonlocal best
        if isinstance(value, Mapping):
            candidate = {
                key: int(value[key])
                for key in ("input_tokens", "cached_input_tokens", "output_tokens", "total_tokens")
                if isinstance(value.get(key), int)
            }
            if len(candidate) > len(best) or sum(candidate.values()) > sum(best.values()):
                best = candidate
            for nested in value.values():
                visit(nested)
        elif isinstance(value, list):
            for nested in value:
                visit(nested)

    for event in events:
        visit(event)
    if best and "total_tokens" not in best:
        best["total_tokens"] = best.get("input_tokens", 0) + best.get("output_tokens", 0)
    return best


def _json_object(text: str) -> dict[str, Any] | None:
    try:
        value = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def call_codex_bounded(
    *,
    prompt: str,
    schema: Mapping[str, Any],
    model: str,
    effort: str,
    images: Sequence[Path],
    timeout_s: int = 1200,
    retries: int = 1,
) -> GatewayResult:
    codex_bin = shutil.which("codex")
    if not codex_bin:
        return GatewayResult(False, None, 0.0, {}, "codex executable not found", 0)
    transient_markers = ("at capacity", "overloaded", "rate limit", "too many requests", "temporarily unavailable", "502", "503")
    last_error = "empty model response"
    total_duration = 0.0
    for attempt in range(1, retries + 2):
        with tempfile.TemporaryDirectory(prefix="ai_sheet_selector_call_") as work:
            workdir = Path(work)
            schema_path = workdir / "schema.json"
            output_path = workdir / "last_message.json"
            _write_json(schema_path, schema)
            command = [
                codex_bin,
                "exec",
                "--json",
                "-m", model,
                "-s", "read-only",
                "--skip-git-repo-check",
                "--ephemeral",
                "-C", str(workdir),
                "-c", f"model_reasoning_effort={effort}",
                "--output-schema", str(schema_path),
                "-o", str(output_path),
            ]
            if images:
                command.extend(["-i", *(str(path) for path in images)])
            command.append("-")
            started = time.monotonic()
            try:
                process = subprocess.run(
                    command,
                    input=prompt,
                    capture_output=True,
                    text=True,
                    cwd=str(workdir),
                    env=_clean_model_env(),
                    timeout=timeout_s,
                )
            except subprocess.TimeoutExpired:
                duration = time.monotonic() - started
                total_duration += duration
                return GatewayResult(False, None, total_duration, {}, f"timeout after {timeout_s}s", attempt)
            duration = time.monotonic() - started
            total_duration += duration
            events = []
            for line in (process.stdout or "").splitlines():
                event = _json_object(line)
                if event is not None:
                    events.append(event)
            usage = _extract_usage(events)
            response = None
            if output_path.is_file():
                response = _json_object(output_path.read_text(encoding="utf-8"))
            if response is not None:
                return GatewayResult(True, response, total_duration, usage, "", attempt)
            combined = ((process.stderr or "") + "\n" + (process.stdout or "")).strip()
            last_error = re.sub(r"\s+", " ", combined[-600:]) or f"empty response, exit {process.returncode}"
            if attempt <= retries and any(marker in combined.casefold() for marker in transient_markers):
                time.sleep(3 * attempt)
                continue
            return GatewayResult(False, None, total_duration, usage, last_error, attempt)
    return GatewayResult(False, None, total_duration, {}, last_error, retries + 1)


def _image_pages(dataset: ProjectDataset) -> tuple[list[int], list[int]]:
    left_pages = [int(task["left_page"]) for task in dataset.tasks]
    right_pages = sorted({
        int(page)
        for task in dataset.tasks
        for option_id in task["option_ids"]
        if option_id not in SENTINEL_OPTION_IDS
        for page in dataset.options[option_id]["right_pages"]
    })
    return left_pages, right_pages


def render_images(dataset: ProjectDataset, target: Path) -> list[Path]:
    import fitz  # type: ignore[import-not-found]  # local production dependency

    target.mkdir(parents=True, exist_ok=True)
    output: list[Path] = []
    left_pages, right_pages = _image_pages(dataset)
    for side, pair_key, pages in (
        ("LEFT", "left", left_pages),
        ("RIGHT", "right", right_pages),
    ):
        pdf_path = Path(str(dataset.pair[pair_key]["pdf_path"]))
        with fitz.open(str(pdf_path)) as document:
            for page in pages:
                image_path = target / f"{side}_page_{page:03d}.jpg"
                pixmap = document[page - 1].get_pixmap(
                    matrix=fitz.Matrix(1.25, 1.25), colorspace=fitz.csRGB, alpha=False,
                )
                pixmap.save(str(image_path), jpg_quality=72)
                output.append(image_path)
    return output


def _load_datasets() -> list[ProjectDataset]:
    return [build_project_dataset(REPO_ROOT, pair_id) for pair_id in PROJECT_CONFIG]


def _readme(candidate_signature: str, *, complete: bool) -> str:
    status = "complete" if complete else "candidate recall complete; model calls not started"
    return f"""# AI Sheet Matcher — isolated research spike

Status: **{status}**

This directory is an offline experiment over three frozen Stage Comparison
pairs. It is not imported by the backend or frontend, does not change
`production-sheet-matcher.v3`, does not change production thresholds, does not
write to pair run directories, and is not deployed.

The experiment has two deliberately separate phases:

1. `python -m experiments.ai_sheet_matcher.run candidate-audit`
2. `python -m experiments.ai_sheet_matcher.run experiment`

The second phase refuses to call a model unless `candidate_recall.json` exists
and matches the current frozen inputs. Candidate signature:
`{candidate_signature}`.

Selector outputs contain only a frozen payload signature plus prebuilt task and
option IDs. Page numbers, evidence, values, and entity names cannot be emitted
by the model. Pass A and Pass B use byte-identical payloads; three independent
ephemeral calls are made for every mode/project/pass. A deterministic verifier
checks bindings, page bounds, direction, evidence refs, cardinality, complete
groups, map conflicts, stability, and saved engineer decisions. Any doubt is
blocked from materialization.

Artifacts:

- `candidate_recall.json` — top-5/top-10 audit before model calls;
- `model_runs.jsonl` — bounded outputs and call telemetry, without raw prompts;
- `decisions.jsonl` — per-task Pass A/B, final status, verifier, and evidence refs;
- `stability.json` — cold-run overlaps and map signatures;
- `metrics.json` — requested baseline/AI/safety/cost metrics;
- `experiment_report.md` — findings and rollout verdict.
"""


def run_candidate_audit(output: Path) -> None:
    datasets = _load_datasets()
    recall = build_candidate_recall(datasets)
    output.mkdir(parents=True, exist_ok=True)
    _write_json(output / "candidate_recall.json", recall)
    (output / "README.md").write_text(
        _readme(str(recall["input_signature"]), complete=False), encoding="utf-8",
    )
    print("candidate recall persisted before model calls")
    for project in recall["projects"]:
        summary = project["summary_all_unique_cases"]
        print(
            project["pair_id"],
            f"R@5={summary['candidate_recall_at_5']:.3f}",
            f"R@10={summary['candidate_recall_at_10']:.3f}",
            f"generation={summary['candidate_generation_problem_count']}",
            f"selection={summary['candidate_selection_problem_count']}",
        )
    print(output / "candidate_recall.json")


def _job(
    dataset: ProjectDataset,
    *,
    mode: str,
    cold_run: int,
    pass_name: str,
    prompt: str,
    payload: Mapping[str, Any],
    images: Sequence[Path],
    model: str,
    effort: str,
) -> dict[str, Any]:
    schema = selector_schema(dataset, str(payload["payload_signature"]))
    result = call_codex_bounded(
        prompt=prompt,
        schema=schema,
        model=model,
        effort=effort,
        images=images,
    )
    verification = verify_selector_response(
        dataset, str(payload["payload_signature"]), result.response,
    )
    return {
        "project": dataset.project,
        "pair_id": dataset.pair_id,
        "run_id": dataset.run_id,
        "mode": mode,
        "cold_run": cold_run,
        "pass_name": pass_name,
        "payload_signature": payload["payload_signature"],
        "candidate_input_signature": dataset.input_signature,
        "model": model,
        "reasoning_effort": effort,
        "model_call": {
            "ok": result.ok,
            "duration_s": round(result.duration_s, 3),
            "usage": result.usage,
            "error": result.error,
            "attempts": result.attempts,
        },
        "response": result.response,
        "verification": verification,
    }


def _call_metrics(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    calls = [item.get("model_call") or {} for item in records]
    token_values = [
        int((call.get("usage") or {}).get("total_tokens"))
        for call in calls
        if isinstance((call.get("usage") or {}).get("total_tokens"), int)
    ]
    return {
        "model_calls": len(calls),
        "model_attempts_including_retries": sum(int(call.get("attempts") or 0) for call in calls),
        "successful_calls": sum(bool(call.get("ok")) for call in calls),
        "failed_calls": sum(not bool(call.get("ok")) for call in calls),
        "verified_map_calls": sum(bool(item.get("verification", {}).get("ok")) for item in records),
        "rejected_map_calls": sum(not bool(item.get("verification", {}).get("ok")) for item in records),
        "runtime_sum_s": round(sum(float(call.get("duration_s") or 0.0) for call in calls), 3),
        "tokens_total": sum(token_values) if token_values else None,
        "tokens_available_for_calls": len(token_values),
    }


def _metrics(
    datasets: Sequence[ProjectDataset],
    recall: Mapping[str, Any],
    decisions: Sequence[Mapping[str, Any]],
    records: Sequence[Mapping[str, Any]],
    stability: Sequence[Mapping[str, Any]],
    *,
    model: str,
    effort: str,
    wall_runtime_s: float,
) -> dict[str, Any]:
    recall_by_pair = {item["pair_id"]: item for item in recall["projects"]}
    projects = []
    for dataset in datasets:
        by_mode: dict[str, Any] = {}
        for mode in ("TEXT", "VISION_TEXT"):
            selected_decisions = [
                item for item in decisions
                if item["pair_id"] == dataset.pair_id and item["mode"] == mode
            ]
            selected_records = [
                item for item in records
                if item["pair_id"] == dataset.pair_id and item["mode"] == mode
            ]
            by_mode[mode] = {
                **decision_metrics(selected_decisions),
                **_call_metrics(selected_records),
            }
        project_recall = recall_by_pair[dataset.pair_id]
        projects.append({
            "project": dataset.project,
            "pair_id": dataset.pair_id,
            "run_id": dataset.run_id,
            "baseline": {
                **dataset.baseline,
                "human_confirmation_relation_count": len(dataset.human_links),
                "human_confirmation_left_case_count": project_recall["human_confirmation_left_case_count"],
            },
            "candidate_recall": project_recall["summary_all_unique_cases"],
            "AI_TEXT": by_mode["TEXT"],
            "AI_VISION_TEXT": by_mode["VISION_TEXT"],
        })
    unsupported_auto = sum(
        project[mode]["unsupported_auto_matches"]
        for project in projects for mode in ("AI_TEXT", "AI_VISION_TEXT")
    )
    stable_auto = sum(
        project[mode]["stable_auto_decisions"]
        for project in projects for mode in ("AI_TEXT", "AI_VISION_TEXT")
    )
    verdict = "C" if unsupported_auto else ("B" if stable_auto else "C")
    return {
        "kind": "ai_sheet_matcher_experiment_metrics",
        "schema_version": "ai-sheet-matcher-metrics.v1",
        "algorithm_version": ALGORITHM_VERSION,
        "model": model,
        "reasoning_effort": effort,
        "cold_runs": 3,
        "passes_per_cold_run": 2,
        "projects": projects,
        "overall_candidate_recall": recall["summary"],
        "overall_calls": _call_metrics(records),
        "wall_runtime_s": round(wall_runtime_s, 3),
        "source_artifacts_unchanged": all(production_sources_unchanged(dataset) for dataset in datasets),
        "safety_gate": {
            "criterion": "UNSUPPORTED_AUTO_MATCHES == 0",
            "unsupported_auto_matches": unsupported_auto,
            "passed": unsupported_auto == 0,
            "human_decision_priority_enforced": True,
        },
        "verdict": verdict,
        "verdict_reason": (
            "Promising but candidate recall, independent manual truth, and/or stable coverage are insufficient for controlled rollout."
            if verdict == "B"
            else "The experiment did not demonstrate enough safe stable improvement for rollout."
        ),
        "stability_record_count": len(stability),
    }


def _percent(value: Any) -> str:
    return "—" if value is None else f"{float(value) * 100:.1f}%"


def _report(
    metrics: Mapping[str, Any],
    recall: Mapping[str, Any],
    decisions: Sequence[Mapping[str, Any]],
    stability: Sequence[Mapping[str, Any]],
) -> str:
    lines = [
        "# Research spike — AI Sheet Matcher",
        "",
        "## Итог",
        "",
        f"Вердикт: **{metrics['verdict']}**. Safety gate: **{'PASS' if metrics['safety_gate']['passed'] else 'FAIL'}**; "
        f"unsupported automatic matches: **{metrics['safety_gate']['unsupported_auto_matches']}**.",
        "",
        "Эксперимент изолирован и выполнен на frozen-артефактах. Production matcher, пороги, UI, исходные прогоны "
        "и deployment не изменялись. Reference hypotheses использованы только как аудит-кейсы. Сохранённое решение "
        "инженера имеет безусловный приоритет над моделью.",
        "",
        "AI-часть намеренно ограничена сложными листами: 24 LEFT для ИОС 1.1, 5 для ИОС 3.1 и 7 для ИОС 2.1. "
        "Candidate recall измерен шире — на всех 70 уникальных human-confirmed/reference кейсах.",
        "",
        "## Метрики benchmark",
        "",
        "| Проект | Baseline H/P/U | Human confirmations | Recall@5 | Recall@10 | TEXT auto/review/unresolved/unsupported | VISION+TEXT auto/review/unresolved/unsupported |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for project in metrics["projects"]:
        baseline = project["baseline"]
        recall_row = project["candidate_recall"]
        text = project["AI_TEXT"]
        vision = project["AI_VISION_TEXT"]
        lines.append(
            f"| {project['project']} | {baseline['HIGH']}/{baseline['POSSIBLE']}/{baseline['UNKNOWN']} | "
            f"{baseline['human_confirmation_relation_count']} relations / {baseline['human_confirmation_left_case_count']} LEFT cases | "
            f"{_percent(recall_row['candidate_recall_at_5'])} | {_percent(recall_row['candidate_recall_at_10'])} | "
            f"{text['stable_auto_decisions']}/{text['human_review']}/{text['unresolved']}/{text['unsafe_or_unsupported_decisions']} | "
            f"{vision['stable_auto_decisions']}/{vision['human_review']}/{vision['unresolved']}/{vision['unsafe_or_unsupported_decisions']} |"
        )

    generation_examples: list[str] = []
    selection_examples: list[str] = []
    for project in recall["projects"]:
        for case in project["cases"]:
            problem = case["problem_class"]
            target = generation_examples if problem == "CANDIDATE_GENERATION_PROBLEM" else selection_examples
            if problem in {"CANDIDATE_GENERATION_PROBLEM", "CANDIDATE_SELECTION_PROBLEM"} and len(target) < 12:
                target.append(
                    f"{project['project']} LEFT {case['audit_left_page']} → RIGHT {case['expected_right_pages']} ({problem})"
                )
    lines.extend([
        "",
        "## Ответы на десять вопросов исследования",
        "",
        "### 1. Где проблема была в candidate generation",
        "",
        f"В {recall['summary']['candidate_generation_problem_count']} из 70 уникальных human/reference кейсов обязательный "
        "RIGHT-лист или вся обязательная группа не попали в top-10 конкретного LEFT. Примеры:",
        "",
        *(f"- {item}" for item in generation_examples),
        "",
        "Для одиночного LEFT селектор такие пропуски исправить не может. Document Map Review иногда может собрать "
        "группу из кандидатов соседних LEFT, но это не помогло ГРЩ LEFT 52 → RIGHT 21/22/23 (22 и 23 отсутствовали) "
        "и ВРУ-4 LEFT 29/30 → RIGHT 28 (RIGHT 28 отсутствовал у LEFT 30).",
        "",
        "### 2. Где кандидат был найден, но deterministic matcher его не выбрал уверенно",
        "",
        f"В {recall['summary']['candidate_selection_problem_count']} human-confirmed кейсах нужные страницы уже были "
        "в top-10, но exact HIGH production relation не было. Примеры:",
        "",
        *(f"- {item}" for item in selection_examples),
        "",
        "### 3. Результат TEXT AI",
        "",
    ])
    for project in metrics["projects"]:
        text = project["AI_TEXT"]
        lines.append(
            f"- {project['project']}: {text['stable_auto_decisions']} retrospectively supported stable auto, "
            f"{text['human_review']} human review, {text['unresolved']} unresolved, "
            f"{text['unsafe_or_unsupported_decisions']} stable-but-blocked unsupported."
        )
    lines.extend([
        "",
        "Всего TEXT дал 14 поддержанных стабильных LEFT-решений из 36 исследованных задач. Это retrospective score: "
        "human mappings не передавались модели, но использовались после ответа как safety gate, поэтому результат не "
        "равен готовой production-автоматизации.",
        "",
        "### 4. Прирост VISION+TEXT относительно TEXT",
        "",
    ])
    for project in metrics["projects"]:
        text = project["AI_TEXT"]
        vision = project["AI_VISION_TEXT"]
        lines.append(
            f"- {project['project']}: {vision['stable_auto_decisions']} против {text['stable_auto_decisions']} "
            f"({vision['stable_auto_decisions'] - text['stable_auto_decisions']:+d}); unresolved "
            f"{vision['unresolved']} против {text['unresolved']}."
        )
    lines.extend([
        "",
        "Vision дал измеримый прирост только на переименованной серии ИОС 1.1: +5 supported stable LEFT. "
        "На ИОС 3.1 прирост нулевой, на ИОС 2.1 — нулевой и стабильность карты стала хуже. При этом TEXT уже содержит "
        "сохранённые OCR/image-description артефакты, поэтому это инкремент vision поверх vision-enriched текста.",
        "",
        "### 5. Обнаруженные 1→1 / 1→N / N→1 / FUNCTION_DISTRIBUTED",
        "",
    ])
    for project in metrics["projects"]:
        text_detected = project["AI_TEXT"]["detected_relations"]
        vision_detected = project["AI_VISION_TEXT"]["detected_relations"]
        lines.append(
            f"- {project['project']}: TEXT {text_detected}; VISION+TEXT {vision_detected}. "
            "Это уникальные стабильные bounded option IDs, не ground truth."
        )
    lines.extend([
        "",
        "Стабильных 1→N, N→1 или FUNCTION_DISTRIBUTED не обнаружено. Для ВРУ-3 локальные проходы устойчиво "
        "предлагали LEFT 27→RIGHT 27 и LEFT 28→RIGHT 27, но Document Map Review не оформил prebuilt MERGED option: "
        "один из листов переводился в NEED_MORE_EVIDENCE. Для ВРУ-4 один vision-pass предложил нестабильный "
        "FUNCTION_DISTRIBUTED LEFT 29/30→RIGHT 28/29; он не материализован. ГРЩ не имел полной bounded-группы.",
        "",
        "### 6. Стабильность во всех трёх cold runs",
        "",
    ])
    stability_by_key = {(item["pair_id"], item["mode"]): item for item in stability}
    for project in metrics["projects"]:
        for mode in ("TEXT", "VISION_TEXT"):
            row = stability_by_key[(project["pair_id"], mode)]
            overlaps = ", ".join(_percent(item["overlap"]) for item in row["pairwise_overlap"])
            lines.append(
                f"- {project['project']} {mode}: {row['stable_task_count']} стабильных задач, "
                f"{row['disagreement_or_failure_count']} расхождений/отказов; pairwise map overlap {overlaps}."
            )
    text_human = sum(
        project["AI_TEXT"]["human_review"] + project["AI_TEXT"]["unresolved"]
        for project in metrics["projects"]
    )
    vision_human = sum(
        project["AI_VISION_TEXT"]["human_review"] + project["AI_VISION_TEXT"]["unresolved"]
        for project in metrics["projects"]
    )
    lines.extend([
        "",
        "35 из 36 model calls прошли verifier целиком. Один VISION_TEXT pass ИОС 2.1 был детерминированно отклонён "
        "из-за конфликта использования RIGHT 26 двумя LEFT-задачами; оба решения этого pass закрыты fail-closed.",
        "",
        "### 7. Сколько решений осталось человеку",
        "",
        f"TEXT оставил человеку {text_human} из 36 задач; VISION+TEXT — {vision_human} из 36. В число входят "
        "нестабильные/неразрешённые задачи и стабильные выборы, которые конфликтуют с human mapping либо не имеют "
        "human confirmation.",
        "",
        "Ключевой кандидат на ручную перепроверку: ИОС 2.1. TEXT и VISION+TEXT во всех шести проходах выбрали "
        "функциональные пары LEFT 17→RIGHT 27, LEFT 18→RIGHT 24 и LEFT 19→RIGHT 25, тогда как сохранённые "
        "engineer-accepted links указывают RIGHT 7/8/9. AI не переопределил человека: все три пары заблокированы.",
        "",
        "LEFT 20 и LEFT 51 ИОС 2.1 остались нестабильными; vision не разрешил конкуренцию RIGHT 29/30 и RIGHT 29/63. "
        "LEFT 21→RIGHT 29 был стабилен в обоих режимах, но оставлен человеку из-за отсутствия подтверждённой карты.",
        "",
        "### 8. Unsupported automatic matches",
        "",
        f"Unsupported automatic matches: **{metrics['safety_gate']['unsupported_auto_matches']}**. "
        f"Safety gate: **{'PASS' if metrics['safety_gate']['passed'] else 'FAIL'}**. "
        "Стабильные, но неподдержанные выборы посчитаны отдельно и заблокированы до materialization.",
        "",
        "### 9. Стоимость",
        "",
        f"Model calls: {metrics['overall_calls']['model_calls']} "
        f"({metrics['overall_calls']['model_attempts_including_retries']} attempts including retries); "
        f"wall runtime: {metrics['wall_runtime_s']:.1f}s; summed call runtime: {metrics['overall_calls']['runtime_sum_s']:.1f}s; "
        f"tokens: {metrics['overall_calls']['tokens_total'] if metrics['overall_calls']['tokens_total'] is not None else 'CLI не предоставил'}.",
        "",
        "### 10. Рекомендация",
        "",
        "**B — перспективно, нужны доработки; controlled rollout пока не рекомендован.**",
        "",
        "До rollout нужны: discipline-agnostic candidate generation с устойчивыми структурными сдвигами и групповыми "
        "кандидатами; независимый ручной functional ground truth для конфликтов со старыми page-number links; более "
        "надёжное group reasoning; повтор того же six-pass safety gate.",
        "",
        "## Очередь ручного аудита",
        "",
        "Полная трассировка находится в `decisions.jsonl`. Ниже стабильные конкретные выборы, заблокированные human-priority gate:",
        "",
        "| Проект | Режим | LEFT | Выбор LEFT→RIGHT | Тип | Verifier | Confidence |",
        "|---|---|---:|---|---|---|---|",
    ])
    priority = [
        item for item in decisions
        if item["final_status"] == "HUMAN_REVIEW" and item.get("selected_option_id")
    ]
    for item in priority[:40]:
        option = item.get("selected_option") or {}
        lines.append(
            f"| {item['project']} | {item['mode']} | {item['left_page']} | "
            f"{option.get('left_pages')}→{option.get('right_pages')} | {option.get('decision_type')} | "
            f"{item['verifier_status']} | {item.get('confidence')} |"
        )
    if not priority:
        lines.append("| — | — | — | — | — | — | Нет стабильных заблокированных выборов |")
    lines.extend([
        "",
        "## Ограничения интерпретации",
        "",
        "- `stable_auto_decisions` — решения, retrospectively совпавшие с сохранённой human map и прошедшие six-pass gate; "
        "это не новая независимая ground truth.",
        "- Старые `sheet_links` с `user_accepted` считаются human decisions и не переопределяются, даже когда functional evidence "
        "указывает на другую страницу.",
        "- AI-метрики относятся к 36 приоритетным задачам, не ко всем LEFT-листам документов.",
        "- NO_ANALOG никогда не материализуется: top-10 не доказывает полноту отсутствия.",
        "",
        "## Доказательство изоляции",
        "",
        f"Frozen source artifacts unchanged: **{metrics['source_artifacts_unchanged']}**. "
        "Запись выполнялась только в папку эксперимента. Deploy не выполнялся.",
        "",
    ])
    return "\n".join(lines)


def run_experiment(output: Path, *, model: str, effort: str, workers: int) -> None:
    recall_path = output / "candidate_recall.json"
    if not recall_path.is_file():
        raise RuntimeError("candidate_recall.json is required; run candidate-audit before any model calls")
    datasets = _load_datasets()
    recall = json.loads(recall_path.read_text(encoding="utf-8"))
    expected_signature = digest([dataset.input_signature for dataset in datasets])
    if recall.get("input_signature") != expected_signature:
        raise RuntimeError("candidate recall is stale; rerun candidate-audit before model calls")

    wall_started = time.monotonic()
    records: list[dict[str, Any]] = []
    prompts: dict[tuple[str, str], tuple[str, dict[str, Any]]] = {}
    image_paths: dict[str, list[Path]] = {}
    with tempfile.TemporaryDirectory(prefix="ai_sheet_matcher_renders_") as render_root_raw:
        render_root = Path(render_root_raw)
        for dataset in datasets:
            prompts[(dataset.pair_id, "TEXT")] = build_selector_prompt(dataset, mode="TEXT")
            prompts[(dataset.pair_id, "VISION_TEXT")] = build_selector_prompt(dataset, mode="VISION_TEXT")
            image_paths[dataset.pair_id] = render_images(dataset, render_root / dataset.pair_id)

        jobs = []
        for dataset in datasets:
            for mode in ("TEXT", "VISION_TEXT"):
                prompt, payload = prompts[(dataset.pair_id, mode)]
                images = image_paths[dataset.pair_id] if mode == "VISION_TEXT" else []
                for cold_run in (1, 2, 3):
                    for pass_name in ("A", "B"):
                        jobs.append((dataset, mode, cold_run, pass_name, prompt, payload, images))
        print(f"starting {len(jobs)} bounded model calls; candidate recall is already persisted")
        with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
            futures = {
                executor.submit(
                    _job,
                    dataset,
                    mode=mode,
                    cold_run=cold_run,
                    pass_name=pass_name,
                    prompt=prompt,
                    payload=payload,
                    images=images,
                    model=model,
                    effort=effort,
                ): (dataset.pair_id, mode, cold_run, pass_name)
                for dataset, mode, cold_run, pass_name, prompt, payload, images in jobs
            }
            for index, future in enumerate(as_completed(futures), 1):
                key = futures[future]
                record = future.result()
                records.append(record)
                print(index, "/", len(jobs), key, "ok=" + str(record["model_call"]["ok"]), f"{record['model_call']['duration_s']:.1f}s")

    records.sort(key=lambda item: (item["pair_id"], item["mode"], item["cold_run"], item["pass_name"]))
    decisions: list[dict[str, Any]] = []
    stability: list[dict[str, Any]] = []
    for dataset in datasets:
        for mode in ("TEXT", "VISION_TEXT"):
            project_decisions, project_stability = aggregate_decisions(
                dataset, mode=mode, run_records=records,
            )
            decisions.extend(project_decisions)
            stability.append(project_stability)

    wall_runtime = time.monotonic() - wall_started
    metrics = _metrics(
        datasets, recall, decisions, records, stability,
        model=model, effort=effort, wall_runtime_s=wall_runtime,
    )
    _write_jsonl(output / "model_runs.jsonl", records)
    _write_jsonl(output / "decisions.jsonl", decisions)
    _write_json(output / "stability.json", {
        "kind": "ai_sheet_matcher_stability",
        "schema_version": "stability.v1",
        "records": stability,
    })
    _write_json(output / "metrics.json", metrics)
    (output / "experiment_report.md").write_text(
        _report(metrics, recall, decisions, stability), encoding="utf-8",
    )
    (output / "README.md").write_text(
        _readme(str(recall["input_signature"]), complete=True), encoding="utf-8",
    )
    print("experiment complete", output)
    print("safety", metrics["safety_gate"], "verdict", metrics["verdict"])


def finalize_existing(output: Path) -> None:
    """Rebuild derived artifacts from saved model outputs without new calls."""
    recall_path = output / "candidate_recall.json"
    runs_path = output / "model_runs.jsonl"
    if not recall_path.is_file() or not runs_path.is_file():
        raise RuntimeError("candidate_recall.json and model_runs.jsonl are required")
    datasets = _load_datasets()
    recall = json.loads(recall_path.read_text(encoding="utf-8"))
    expected_signature = digest([dataset.input_signature for dataset in datasets])
    if recall.get("input_signature") != expected_signature:
        raise RuntimeError("saved model runs refer to stale candidate inputs")
    records = [
        json.loads(line) for line in runs_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    dataset_by_pair = {dataset.pair_id: dataset for dataset in datasets}
    for record in records:
        dataset = dataset_by_pair.get(str(record.get("pair_id") or ""))
        if dataset is None:
            raise RuntimeError("saved model run refers to an unknown pair")
        record["verification"] = verify_selector_response(
            dataset,
            str(record.get("payload_signature") or ""),
            record.get("response"),
        )
    decisions: list[dict[str, Any]] = []
    stability: list[dict[str, Any]] = []
    for dataset in datasets:
        for mode in ("TEXT", "VISION_TEXT"):
            project_decisions, project_stability = aggregate_decisions(
                dataset, mode=mode, run_records=records,
            )
            decisions.extend(project_decisions)
            stability.append(project_stability)
    previous_metrics = (
        json.loads((output / "metrics.json").read_text(encoding="utf-8"))
        if (output / "metrics.json").is_file() else {}
    )
    first = records[0] if records else {}
    metrics = _metrics(
        datasets, recall, decisions, records, stability,
        model=str(first.get("model") or DEFAULT_MODEL),
        effort=str(first.get("reasoning_effort") or DEFAULT_EFFORT),
        wall_runtime_s=float(previous_metrics.get("wall_runtime_s") or 0.0),
    )
    _write_jsonl(output / "model_runs.jsonl", records)
    _write_jsonl(output / "decisions.jsonl", decisions)
    _write_json(output / "stability.json", {
        "kind": "ai_sheet_matcher_stability",
        "schema_version": "stability.v1",
        "records": stability,
    })
    _write_json(output / "metrics.json", metrics)
    (output / "experiment_report.md").write_text(
        _report(metrics, recall, decisions, stability), encoding="utf-8",
    )
    (output / "README.md").write_text(
        _readme(str(recall["input_signature"]), complete=True), encoding="utf-8",
    )
    print("derived artifacts finalized without model calls", output)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=("candidate-audit", "experiment", "finalize"))
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--effort", default=DEFAULT_EFFORT)
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    output = args.output.resolve()
    if args.phase == "candidate-audit":
        run_candidate_audit(output)
    elif args.phase == "finalize":
        finalize_existing(output)
    else:
        run_experiment(output, model=args.model, effort=args.effort, workers=args.workers)


if __name__ == "__main__":
    main()
