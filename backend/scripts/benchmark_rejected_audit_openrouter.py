#!/usr/bin/env python3
"""Benchmark frozen rejected-finding cases through OpenRouter.

This is an isolated, paid, text-only comparison harness.  It reuses the
production audit prompt, JSON schema, and deterministic normalization guards,
but never mutates the canonical audit results or project source artifacts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.findings.rejected_audit_service import (  # noqa: E402
    build_messages,
    load_latest_results,
    load_manifest,
    normalize_batch_output,
    output_schema,
    utc_now_iso,
)


DEFAULT_AUDIT_DIR = (
    ROOT
    / "comparison/rejected_findings_audit/2026-07-kuldyaev-hybrid-v1/sol-high/auto-retry-v3"
)
DEFAULT_OUTPUT_DIR = DEFAULT_AUDIT_DIR / "model-benchmarks/deepseek-v4-flash-0731-high-5"
DEFAULT_MODEL = "deepseek/deepseek-v4-flash-0731"
PILOT_CASE_IDS = (
    "RF-20260713-5cae97bb0e504cff",  # Codex: insufficient_evidence
    "RF-20260709-9db7f511e94e6a2b",  # Codex: expert_correct
    "RF-20260714-fec37756416a24ac",  # Codex: expert_correct
    "RF-20260713-153f98f86214915d",  # Codex: expert_may_be_wrong
    "RF-20260715-bba67595cbaf6f76",  # Codex: expert_may_be_wrong
)
STRATIFIED_QUOTAS = {
    "insufficient_evidence": 25,
    "expert_correct": 16,
    "expert_may_be_wrong": 9,
}
ENDPOINT = "https://openrouter.ai/api/v1/chat/completions"
PRICE_INPUT_PER_M = 0.09
PRICE_OUTPUT_PER_M = 0.18
DEFAULT_MAX_TOKENS = 16000


def _json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _atomic_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _load_dotenv_key() -> str:
    value = os.environ.get("OPENROUTER_API_KEY", "").strip()
    if value:
        return value
    env_path = ROOT / ".env"
    try:
        text = env_path.read_text(encoding="utf-8")
    except OSError:
        return ""
    for line in text.splitlines():
        match = re.match(r"^\s*OPENROUTER_API_KEY\s*=\s*(.*?)\s*$", line)
        if match:
            return match.group(1).strip().strip("\"'")
    return ""


def _selected_cases(audit_dir: Path, case_ids: tuple[str, ...]) -> tuple[list[dict], dict[str, dict]]:
    manifest = load_manifest(audit_dir / "manifest.jsonl")
    by_id = {str(case.get("case_id")): case for case in manifest}
    latest, malformed = load_latest_results(audit_dir / "results.jsonl")
    if malformed:
        raise ValueError(f"В эталонном results.jsonl повреждённых строк: {malformed}")
    cases: list[dict] = []
    references: dict[str, dict] = {}
    for case_id in case_ids:
        case = by_id.get(case_id)
        reference = latest.get(case_id)
        if not case or not reference or reference.get("status") != "success":
            raise ValueError(f"Нет замороженного кейса или успешного эталона Codex: {case_id}")
        images = (case.get("context") or {}).get("images") or []
        if images:
            raise ValueError(f"Пилот DeepSeek должен быть text-only, но у {case_id} есть изображения")
        cases.append(case)
        references[case_id] = reference
    return cases, references


def _eligible_text_cases(audit_dir: Path) -> tuple[dict[str, dict], dict[str, dict]]:
    manifest = load_manifest(audit_dir / "manifest.jsonl")
    latest, malformed = load_latest_results(audit_dir / "results.jsonl")
    if malformed:
        raise ValueError(f"В эталонном results.jsonl повреждённых строк: {malformed}")
    cases: dict[str, dict] = {}
    references: dict[str, dict] = {}
    for case in manifest:
        case_id = str(case.get("case_id") or "")
        reference = latest.get(case_id)
        if not case_id or not reference or reference.get("status") != "success":
            continue
        if ((case.get("context") or {}).get("images") or []):
            continue
        cases[case_id] = case
        references[case_id] = reference
    return cases, references


def _stable_rank(case_id: str) -> str:
    return hashlib.sha256(f"deepseek-v4-flash-0731-hybrid-50:{case_id}".encode()).hexdigest()


def _stratified_case_ids(
    audit_dir: Path,
    quotas: dict[str, int],
    excluded: set[str],
) -> tuple[list[str], dict[str, int]]:
    cases, references = _eligible_text_cases(audit_dir)
    population: dict[str, int] = {}
    selected: list[str] = []
    for verdict, quota in quotas.items():
        available = [
            case_id
            for case_id in cases
            if references[case_id].get("verdict") == verdict
        ]
        population[verdict] = len(available)
        candidates = [case_id for case_id in available if case_id not in excluded]
        candidates.sort(key=_stable_rank)
        if len(candidates) < quota:
            raise ValueError(
                f"Для страты {verdict} доступно {len(candidates)}, требуется {quota}"
            )
        selected.extend(candidates[:quota])
    return selected, population


def _response_format(case_id: str) -> dict:
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "rejected_finding_expert_audit",
            "strict": True,
            "schema": output_schema([case_id]),
        },
    }


def _request_body(
    case: dict,
    model: str,
    reasoning_effort: str,
    max_tokens: int,
) -> dict:
    case_id = str(case["case_id"])
    return {
        "model": model,
        "messages": build_messages([case], image_alignment={case_id: []}),
        "reasoning": {"effort": reasoning_effort, "exclude": True},
        "response_format": _response_format(case_id),
        "provider": {
            "sort": "price",
            "require_parameters": True,
            "data_collection": "deny",
        },
        "temperature": 0,
        "max_tokens": max_tokens,
        "stream": False,
    }


def _disclosure(
    audit_dir: Path,
    cases: list[dict],
    references: dict[str, dict],
    model: str,
    reasoning_effort: str,
    max_tokens: int,
    population: dict[str, int] | None = None,
) -> dict:
    requests = [
        _request_body(case, model, reasoning_effort, max_tokens) for case in cases
    ]
    request_sizes = [len(_json_bytes(body)) for body in requests]
    case_rows = []
    for case, request_size in zip(cases, request_sizes):
        case_id = str(case["case_id"])
        context = case.get("context") or {}
        case_rows.append({
            "case_id": case_id,
            "codex_reference_verdict": references[case_id].get("verdict"),
            "discipline": case.get("discipline"),
            "request_utf8_bytes": request_size,
            "context_utf8_bytes": len(_json_bytes(context)),
            "image_count": 0,
        })
    return {
        "schema_version": 2,
        "generated_at": utc_now_iso(),
        "purpose": (
            f"{len(cases)}-case text-only quality, cost, and hybrid-routing benchmark "
            "against prior frozen Codex results"
        ),
        "source_audit_dir": str(audit_dir.resolve()),
        "source_manifest_sha256": _sha256_file(audit_dir / "manifest.jsonl"),
        "source_results_sha256": _sha256_file(audit_dir / "results.jsonl"),
        "external_processor": "OpenRouter and its selected downstream DeepSeek provider",
        "endpoint": ENDPOINT,
        "model": model,
        "reasoning_effort": reasoning_effort,
        "max_tokens_per_request": max_tokens,
        "provider_policy": {
            "sort": "price",
            "require_parameters": True,
            "data_collection": "deny",
            "fallbacks": "OpenRouter default among eligible endpoints",
        },
        "transmitted_data": (
            "finding, expert rejection reason, OCR/text blocks, document-text excerpts, "
            "norm context, retrieval metadata, and document identifiers; no images"
        ),
        "case_count": len(cases),
        "image_count": 0,
        "reference_population_by_verdict": population or {},
        "sample_by_reference_verdict": {
            verdict: sum(
                references[str(case["case_id"])].get("verdict") == verdict
                for case in cases
            )
            for verdict in STRATIFIED_QUOTAS
        },
        "total_request_utf8_bytes": sum(request_sizes),
        "request_sha256": [_sha256_bytes(_json_bytes(body)) for body in requests],
        "case_rows": case_rows,
        "price_snapshot_usd_per_million_tokens": {
            "input": PRICE_INPUT_PER_M,
            "output_including_reasoning": PRICE_OUTPUT_PER_M,
        },
    }


def _parse_content(response: dict) -> dict:
    choices = response.get("choices") or []
    if not choices:
        raise ValueError("OpenRouter response has no choices")
    content = ((choices[0] or {}).get("message") or {}).get("content")
    if isinstance(content, list):
        content = "".join(
            str(part.get("text") or "") if isinstance(part, dict) else str(part)
            for part in content
        )
    text = str(content or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
        text = re.sub(r"\s*```$", "", text)
    payload = json.loads(text)
    if not isinstance(payload, dict):
        raise ValueError("OpenRouter response content is not a JSON object")
    return payload


def _call_openrouter(body: dict, api_key: str, timeout: int) -> tuple[dict, int]:
    request = urllib.request.Request(
        ENDPOINT,
        data=_json_bytes(body),
        method="POST",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "http://localhost:8081",
            "X-Title": "PDF-proverka rejected-finding benchmark",
            "X-OpenRouter-Metadata": "enabled",
        },
    )
    started = time.monotonic()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read()
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:4000]
        raise RuntimeError(f"OpenRouter HTTP {exc.code}: {detail}") from exc
    duration_ms = round((time.monotonic() - started) * 1000)
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("OpenRouter returned a non-object response")
    return payload, duration_ms


def _usage(response: dict) -> dict:
    usage = response.get("usage") or {}
    prompt = int(usage.get("prompt_tokens") or 0)
    completion = int(usage.get("completion_tokens") or 0)
    cost = usage.get("cost")
    source = "openrouter_usage"
    if cost in (None, ""):
        cost = prompt * PRICE_INPUT_PER_M / 1_000_000 + completion * PRICE_OUTPUT_PER_M / 1_000_000
        source = "catalog_price_fallback"
    completion_details = usage.get("completion_tokens_details") or {}
    prompt_details = usage.get("prompt_tokens_details") or {}
    return {
        "prompt_tokens": prompt,
        "completion_tokens": completion,
        "total_tokens": int(usage.get("total_tokens") or prompt + completion),
        "reasoning_tokens": int(completion_details.get("reasoning_tokens") or 0),
        "cached_tokens": int(prompt_details.get("cached_tokens") or 0),
        "cost_usd": float(cost),
        "cost_source": source,
    }


def _agreement(rows: list[dict], field: str) -> dict:
    comparable = [row for row in rows if row.get("status") == "success"]
    matches = sum(row["deepseek"].get(field) == row["codex"].get(field) for row in comparable)
    return {
        "field": field,
        "matches": matches,
        "total": len(comparable),
        "rate": round(matches / len(comparable), 4) if comparable else 0.0,
    }


def _strict_auto_accept(row: dict) -> bool:
    if row.get("status") != "success":
        return False
    result = row.get("deepseek") or {}
    return bool(
        result.get("verdict") == "insufficient_evidence"
        and result.get("recommended_action") == "collect_context"
        and result.get("binding_status") == "exact"
        and result.get("missing_context")
        and not result.get("guard_adjustments")
    )


def _hybrid_metrics(rows: list[dict], population: dict[str, int]) -> dict:
    accepted = [row for row in rows if _strict_auto_accept(row)]
    unsafe = [
        row for row in accepted
        if (row.get("codex") or {}).get("verdict") != "insufficient_evidence"
    ]
    critical = [
        row for row in rows
        if (row.get("codex") or {}).get("verdict") == "expert_may_be_wrong"
    ]
    detected_critical = [
        row for row in critical
        if row.get("status") == "success"
        and (row.get("deepseek") or {}).get("verdict") == "expert_may_be_wrong"
    ]
    by_reference: dict[str, dict] = {}
    weighted_matches = 0.0
    population_total = 0
    for verdict, population_count in population.items():
        stratum = [
            row for row in rows
            if (row.get("codex") or {}).get("verdict") == verdict
        ]
        matches = sum(
            row.get("status") == "success"
            and (row.get("deepseek") or {}).get("verdict") == verdict
            for row in stratum
        )
        rate = matches / len(stratum) if stratum else 0.0
        by_reference[verdict] = {
            "matches": matches,
            "sample_count": len(stratum),
            "end_to_end_rate": round(rate, 4),
            "population_count": population_count,
        }
        weighted_matches += rate * population_count
        population_total += population_count
    return {
        "policy": (
            "Auto-accept only guard-clean insufficient_evidence with exact binding and "
            "explicit missing_context; route every other result or error to Codex"
        ),
        "auto_accepted_without_codex": len(accepted),
        "routed_to_codex": len(rows) - len(accepted),
        "estimated_codex_saving_rate": round(len(accepted) / len(rows), 4) if rows else 0.0,
        "unsafe_auto_accepts_against_codex": len(unsafe),
        "critical_cases": len(critical),
        "critical_detected_as_may_be_wrong": len(detected_critical),
        "critical_recall": round(len(detected_critical) / len(critical), 4) if critical else 0.0,
        "critical_auto_accepted_as_insufficient": sum(row in accepted for row in critical),
        "end_to_end_agreement_by_reference": by_reference,
        "prevalence_weighted_verdict_agreement": (
            round(weighted_matches / population_total, 4) if population_total else 0.0
        ),
    }


def _write_report(summary: dict, path: Path) -> None:
    lines = [
        f"# DeepSeek V4 Flash — тест на {summary['case_count']} замечаниях",
        "",
        f"- Модель: `{summary['model']}`; reasoning: `{summary['reasoning_effort']}`",
        f"- Успешно: **{summary['successful_cases']} / {summary['case_count']}**",
        f"- Совпадение итогового вердикта: **{summary['verdict_agreement']['matches']} / {summary['verdict_agreement']['total']}**",
        f"- Совпадение рекомендуемого действия: **{summary['action_agreement']['matches']} / {summary['action_agreement']['total']}**",
        f"- Стоимость: **${summary['total_cost_usd']:.6f}** (в среднем ${summary['average_cost_usd']:.6f} на кейс)",
        f"- Время: **{summary['total_duration_seconds']:.1f} с**",
        f"- Строгий гибрид отправил бы в Codex: **{summary['hybrid']['routed_to_codex']} / {summary['case_count']}**",
        f"- Небезопасных авто-принятий против эталона Codex: **{summary['hybrid']['unsafe_auto_accepts_against_codex']}**",
        f"- Полнота выявления `expert_may_be_wrong`: **{summary['hybrid']['critical_detected_as_may_be_wrong']} / {summary['hybrid']['critical_cases']}**",
        "",
        "| Кейс | Codex | DeepSeek | Действие совпало | Стоимость, $ | Время, с |",
        "|---|---|---|---:|---:|---:|",
    ]
    for row in summary["cases"]:
        deepseek = row.get("deepseek") or {}
        codex = row.get("codex") or {}
        lines.append(
            f"| `{row['case_id']}` | {codex.get('verdict', '—')} | "
            f"{deepseek.get('verdict', row.get('status', '—'))} | "
            f"{'да' if deepseek.get('recommended_action') == codex.get('recommended_action') else 'нет'} | "
            f"{row.get('usage', {}).get('cost_usd', 0):.6f} | {row.get('duration_ms', 0) / 1000:.1f} |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def prepare(args: argparse.Namespace) -> int:
    population: dict[str, int] = {}
    if args.case_id:
        case_ids = list(args.case_id)
    elif args.sample_size == 50:
        case_ids, population = _stratified_case_ids(
            args.audit_dir,
            STRATIFIED_QUOTAS,
            set(PILOT_CASE_IDS),
        )
    else:
        case_ids = list(PILOT_CASE_IDS)
    if len(case_ids) != args.sample_size or len(set(case_ids)) != len(case_ids):
        raise ValueError(
            f"Ожидалось {args.sample_size} уникальных кейсов, получено {len(case_ids)}"
        )
    cases, references = _selected_cases(args.audit_dir, tuple(case_ids))
    if not population:
        _, all_references = _eligible_text_cases(args.audit_dir)
        population = {
            verdict: sum(ref.get("verdict") == verdict for ref in all_references.values())
            for verdict in STRATIFIED_QUOTAS
        }
    disclosure = _disclosure(
        args.audit_dir,
        cases,
        references,
        args.model,
        args.reasoning_effort,
        args.max_tokens,
        population,
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    path = args.output_dir / "external_openrouter_disclosure.json"
    _atomic_json(path, disclosure)
    print(json.dumps({
        "disclosure": str(path.resolve()),
        "disclosure_sha256": _sha256_file(path),
        "model": args.model,
        "reasoning_effort": args.reasoning_effort,
        "case_count": len(cases),
        "image_count": 0,
        "total_request_utf8_bytes": disclosure["total_request_utf8_bytes"],
        "case_rows": disclosure["case_rows"],
        "estimated_upper_bound_usd": round(
            disclosure["total_request_utf8_bytes"] / 4 * PRICE_INPUT_PER_M / 1_000_000
            + len(cases) * args.max_tokens * PRICE_OUTPUT_PER_M / 1_000_000,
            6,
        ),
    }, ensure_ascii=False, indent=2))
    return 0


def run(args: argparse.Namespace) -> int:
    disclosure_path = args.output_dir / "external_openrouter_disclosure.json"
    if not disclosure_path.is_file():
        raise ValueError("Сначала выполните prepare")
    actual_disclosure_sha = _sha256_file(disclosure_path)
    if not args.confirm_paid_openrouter or args.confirm_disclosure_sha256 != actual_disclosure_sha:
        raise ValueError(
            "Нужно явно подтвердить платный OpenRouter и точный SHA disclosure: "
            f"{actual_disclosure_sha}"
        )
    frozen_disclosure = json.loads(disclosure_path.read_text(encoding="utf-8"))
    frozen_case_ids = tuple(
        str(row.get("case_id") or "")
        for row in (frozen_disclosure.get("case_rows") or [])
    )
    if not frozen_case_ids or len(set(frozen_case_ids)) != len(frozen_case_ids):
        raise ValueError("Disclosure не содержит уникальный замороженный набор кейсов")
    cases, references = _selected_cases(args.audit_dir, frozen_case_ids)
    expected_disclosure = _disclosure(
        args.audit_dir,
        cases,
        references,
        args.model,
        args.reasoning_effort,
        args.max_tokens,
        frozen_disclosure.get("reference_population_by_verdict") or {},
    )
    for key in (
        "source_manifest_sha256",
        "source_results_sha256",
        "model",
        "reasoning_effort",
        "max_tokens_per_request",
        "request_sha256",
        "case_rows",
    ):
        if frozen_disclosure.get(key) != expected_disclosure.get(key):
            raise ValueError(f"Disclosure устарел или вход изменился: {key}")

    api_key = _load_dotenv_key()
    if not api_key:
        raise ValueError("OPENROUTER_API_KEY не настроен")

    rows: list[dict] = []
    for index, case in enumerate(cases, start=1):
        case_id = str(case["case_id"])
        print(f"OpenRouter {index}/{len(cases)}: {case_id}", flush=True)
        body = _request_body(case, args.model, args.reasoning_effort, args.max_tokens)
        response: dict | None = None
        duration_ms = 0
        usage: dict = {}
        try:
            response, duration_ms = _call_openrouter(body, api_key, args.timeout)
            usage = _usage(response)
            choices = response.get("choices") or []
            first_choice = choices[0] if choices else {}
            sanitized_response = {
                "id": response.get("id"),
                "model": response.get("model"),
                "provider": response.get("provider"),
                "usage": response.get("usage"),
                "openrouter_metadata": response.get("openrouter_metadata"),
                "finish_reason": first_choice.get("finish_reason"),
                "message": first_choice.get("message"),
            }
            _atomic_json(args.output_dir / f"response_{case_id}.json", sanitized_response)
            raw_payload = _parse_content(response)
            normalized, errors = normalize_batch_output(
                [case], raw_payload, image_alignment={case_id: []}
            )
            if errors or len(normalized) != 1:
                raise ValueError("; ".join(errors) or "Нормализованный результат отсутствует")
            result = normalized[0]
            result["model"] = args.model
            result["reasoning_effort"] = args.reasoning_effort
            result["duration_ms"] = duration_ms
            result.update(usage)
            rows.append({
                "case_id": case_id,
                "status": "success",
                "codex": references[case_id],
                "deepseek": result,
                "usage": usage,
                "duration_ms": duration_ms,
                "provider": response.get("provider"),
                "response_model": response.get("model"),
            })
        except Exception as exc:  # one paid attempt per selected case; no retries
            rows.append({
                "case_id": case_id,
                "status": "error",
                "error": str(exc)[:4000],
                "codex": references[case_id],
                "deepseek": {},
                "usage": usage,
                "duration_ms": duration_ms,
                "provider": response.get("provider") if response else None,
                "response_model": response.get("model") if response else None,
            })

    successful = [row for row in rows if row["status"] == "success"]
    total_cost = sum(float(row["usage"].get("cost_usd") or 0) for row in rows)
    total_duration_ms = sum(int(row.get("duration_ms") or 0) for row in rows)
    population = frozen_disclosure.get("reference_population_by_verdict") or {}
    summary = {
        "generated_at": utc_now_iso(),
        "model": args.model,
        "reasoning_effort": args.reasoning_effort,
        "case_count": len(rows),
        "successful_cases": len(successful),
        "failed_cases": len(rows) - len(successful),
        "verdict_agreement": _agreement(rows, "verdict"),
        "action_agreement": _agreement(rows, "recommended_action"),
        "axis_agreement": [
            _agreement(rows, field)
            for field in (
                "binding_status",
                "factual_verdict",
                "report_value",
                "reason_quality",
                "confidence",
            )
        ],
        "hybrid": _hybrid_metrics(rows, population),
        "total_cost_usd": round(total_cost, 9),
        "average_cost_usd": round(total_cost / len(rows), 9) if rows else 0.0,
        "total_duration_seconds": round(total_duration_ms / 1000, 3),
        "total_prompt_tokens": sum(int(row["usage"].get("prompt_tokens") or 0) for row in rows),
        "total_completion_tokens": sum(int(row["usage"].get("completion_tokens") or 0) for row in rows),
        "total_reasoning_tokens": sum(int(row["usage"].get("reasoning_tokens") or 0) for row in rows),
        "cases": rows,
    }
    _atomic_json(args.output_dir / "comparison.json", rows)
    _atomic_json(args.output_dir / "summary.json", summary)
    _write_report(summary, args.output_dir / "report.md")
    print(json.dumps({key: value for key, value in summary.items() if key != "cases"}, ensure_ascii=False, indent=2))
    return 0 if len(successful) == len(rows) else 5


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "run"))
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--reasoning-effort", choices=("high", "xhigh"), default="high")
    parser.add_argument("--case-id", action="append", default=[])
    parser.add_argument("--sample-size", type=int, choices=(5, 50), default=5)
    parser.add_argument("--max-tokens", type=int, default=DEFAULT_MAX_TOKENS)
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--confirm-paid-openrouter", action="store_true")
    parser.add_argument("--confirm-disclosure-sha256", default="")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    args.audit_dir = args.audit_dir.resolve()
    args.output_dir = args.output_dir.resolve()
    if args.command == "prepare":
        return prepare(args)
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
