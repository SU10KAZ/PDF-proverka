"""Parallel Claude + Codex execution and deterministic OPT merge."""
from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import unicodedata
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable

from backend.app.core.config import (
    OPTIMIZATION_ENSEMBLE_CODEX_MODEL,
    OPTIMIZATION_ENSEMBLE_CODEX_REASONING_EFFORT,
    OPTIMIZATION_ENSEMBLE_CLAUDE_MODEL,
)
from backend.app.services.common.cli_utils import is_cancelled, is_rate_limited
from backend.app.services.common.codex_stream_filter import wrap_codex_on_output
import backend.app.services.llm.claude_runner as claude_runner
from backend.app.services.storage.stage_artifacts import (
    BLOCKS_ANALYSIS_FILENAME,
    TEXT_ANALYSIS_FILENAME,
    resolve_existing,
)


INPUT_ARTIFACTS = (
    TEXT_ANALYSIS_FILENAME,
    BLOCKS_ANALYSIS_FILENAME,
    "03_findings.json",
    "document_graph.json",
)
OPT_TYPES = ("cheaper_analog", "faster_install", "simpler_design", "lifecycle")
STOP_WORDS = {
    "для", "или", "при", "что", "это", "как", "без", "под", "над", "вместо",
    "текущий", "текущее", "решение", "предложить", "предлагается", "использовать",
    "заменить", "замена", "проект", "система", "оборудование", "материал", "позиция",
    "the", "and", "for", "with", "from", "use", "using", "replace", "current",
}
ACTION_PATTERNS: dict[str, tuple[str, ...]] = {
    "analog": ("аналог", "замен", "бренд", "производител", "дешев", "стоимост", "закуп"),
    "unify": ("унифиц", "типоразмер", "номенклатур", "стандартиз", "серийн"),
    "install": ("монтаж", "сборн", "заводск", "модул", "свар", "креп", "трудозатрат"),
    "layout": ("компонов", "трасс", "расклад", "геометр", "сечен", "диаметр"),
    "lifecycle": ("эксплуатац", "энерго", "сервис", "ремонт", "жизненн", "частот"),
}


@dataclass
class ProviderRun:
    provider: str
    model: str
    output_dir: Path
    exit_code: int = 1
    output: str = ""
    cli_result: Any = None
    data: dict[str, Any] | None = None
    error: str | None = None
    cancelled: bool = False
    rate_limited: bool = False

    @property
    def success(self) -> bool:
        return self.data is not None


@dataclass
class EnsembleRunResult:
    success: bool
    run_id: str
    providers: list[ProviderRun] = field(default_factory=list)
    status: str = "failed"
    error: str | None = None
    cancelled: bool = False
    rate_limited: bool = False


def _atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _read_document(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if isinstance(data, list):
        data = {"meta": {}, "items": data}
    if not isinstance(data, dict):
        return None
    items = data.get("items")
    if not isinstance(items, list) or any(not isinstance(item, dict) for item in items):
        return None
    return data


def _normalize(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).lower().replace("ё", "е")
    return " ".join(re.findall(r"[a-zа-я0-9]+", text))


def _tokens(value: Any) -> set[str]:
    return {
        token for token in _normalize(value).split()
        if len(token) > 2 and token not in STOP_WORDS
    }


def _list_text(value: Any) -> str:
    if isinstance(value, list):
        return " ".join(str(item) for item in value)
    return str(value or "")


def _similarity(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    intersection = len(left & right)
    jaccard = intersection / len(left | right)
    containment = intersection / min(len(left), len(right))
    return max(jaccard, containment * 0.85)


def _actions(item: dict[str, Any]) -> set[str]:
    text = _normalize(f"{item.get('current', '')} {item.get('proposed', '')}")
    return {
        family
        for family, patterns in ACTION_PATTERNS.items()
        if any(pattern in text for pattern in patterns)
    }


def _pages(item: dict[str, Any]) -> set[str]:
    value = item.get("page")
    values = value if isinstance(value, list) else [value]
    return {str(part) for part in values if part not in (None, "")}


def compare_items(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    """Return conservative duplicate evidence for two optimization items."""
    current = _similarity(_tokens(left.get("current")), _tokens(right.get("current")))
    proposed = _similarity(_tokens(left.get("proposed")), _tokens(right.get("proposed")))
    specs = _similarity(
        _tokens(_list_text(left.get("spec_items"))),
        _tokens(_list_text(right.get("spec_items"))),
    )
    same_type = bool(left.get("type") and left.get("type") == right.get("type"))
    page_overlap = bool(_pages(left) & _pages(right))
    left_actions = _actions(left)
    right_actions = _actions(right)
    action_compatible = bool(left_actions & right_actions) or (
        not left_actions and not right_actions and proposed >= 0.58
    )
    exact = bool(
        _normalize(left.get("current"))
        and _normalize(left.get("current")) == _normalize(right.get("current"))
        and _normalize(left.get("proposed")) == _normalize(right.get("proposed"))
    )
    duplicate = exact or (
        same_type and action_compatible and (
            (specs >= 0.64 and current >= 0.50 and proposed >= 0.42)
            or (current >= 0.78 and proposed >= 0.62)
            or (page_overlap and current >= 0.72 and proposed >= 0.52)
        )
    )
    score = round(
        0.30 * current + 0.35 * proposed + 0.25 * specs
        + (0.05 if same_type else 0.0) + (0.05 if page_overlap else 0.0),
        3,
    )
    return {
        "duplicate": duplicate,
        "score": score,
        "current_similarity": round(current, 3),
        "proposed_similarity": round(proposed, 3),
        "spec_similarity": round(specs, 3),
        "same_type": same_type,
        "page_overlap": page_overlap,
        "action_compatible": action_compatible,
    }


def _provider_detection(provider: str, model: str, raw_id: str, run_id: str) -> dict[str, str]:
    return {
        "provider": provider,
        "model": model,
        "raw_optimization_id": raw_id,
        "run_id": run_id,
    }


def _provider_item(
    item: dict[str, Any], *, provider: str, model: str, run_id: str,
) -> dict[str, Any]:
    clean = dict(item)
    raw_id = str(clean.get("id") or "")
    clean["provenance"] = {
        "found_by": [provider],
        "detector_summary": provider,
        "detections": [_provider_detection(provider, model, raw_id, run_id)],
    }
    clean["detector_summary"] = provider
    return clean


def _richness(item: dict[str, Any]) -> float:
    spec_count = len(item.get("spec_items") or [])
    trace = 1 if item.get("page") not in (None, "", []) else 0
    text_len = len(str(item.get("current") or "")) + len(str(item.get("proposed") or ""))
    return spec_count * 4 + trace * 5 + min(text_len, 800) / 100


def _merge_duplicate(claude_item: dict[str, Any], codex_item: dict[str, Any]) -> dict[str, Any]:
    # Prefer the better grounded formulation; close scores keep Claude wording.
    c_score = _richness(claude_item)
    x_score = _richness(codex_item)
    primary, secondary = (
        (codex_item, claude_item) if x_score > c_score + 2 else (claude_item, codex_item)
    )
    merged = dict(primary)
    for key in (
        "section", "page", "sheet", "spec_items", "current", "proposed",
        "type", "savings_pct", "savings_basis", "timeline_impact", "risks", "status", "norm",
    ):
        if merged.get(key) in (None, "", [], {}) and secondary.get(key) not in (None, "", [], {}):
            merged[key] = secondary[key]

    detections: list[dict[str, Any]] = []
    for item in (claude_item, codex_item):
        for detection in (item.get("provenance") or {}).get("detections") or []:
            if isinstance(detection, dict) and detection not in detections:
                detections.append(dict(detection))
    merged["provenance"] = {
        "found_by": ["claude", "codex"],
        "detector_summary": "claude_codex",
        "detections": detections,
    }
    merged["detector_summary"] = "claude_codex"
    return merged


def _meta_for_items(
    base_meta: dict[str, Any], items: list[dict[str, Any]], *, run_id: str,
    status: str, source_counts: dict[str, int], duplicate_count: int,
) -> dict[str, Any]:
    meta = dict(base_meta or {})
    by_type = {key: 0 for key in OPT_TYPES}
    savings: list[float] = []
    for item in items:
        item_type = item.get("type")
        if item_type in by_type:
            by_type[item_type] += 1
        try:
            savings.append(max(0.0, float(item.get("savings_pct") or 0)))
        except (TypeError, ValueError):
            pass
    top_items = sorted(
        items,
        key=lambda item: float(item.get("savings_pct") or 0)
        if str(item.get("savings_pct") or "").replace(".", "", 1).isdigit() else 0,
        reverse=True,
    )[:3]
    meta.update({
        "analysis_date": datetime.now(timezone.utc).date().isoformat(),
        "total_items": len(items),
        "by_type": by_type,
        "estimated_savings_pct": round(sum(savings) / len(savings), 1) if savings else 0,
        "top3_summary": "; ".join(str(item.get("proposed") or "")[:160] for item in top_items),
        "ensemble": {
            "mode": "claude_codex",
            "status": status,
            "run_id": run_id,
            "source_counts": source_counts,
            "duplicates_merged": duplicate_count,
        },
    })
    meta.setdefault("project_id", "")
    meta.setdefault("project_name", "")
    return meta


def merge_optimization_documents(
    claude_doc: dict[str, Any] | None,
    codex_doc: dict[str, Any] | None,
    *,
    run_id: str,
    claude_model: str = OPTIMIZATION_ENSEMBLE_CLAUDE_MODEL,
    codex_model: str = OPTIMIZATION_ENSEMBLE_CODEX_MODEL,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Merge independent provider outputs, collapsing only strong duplicates."""
    claude_items = [
        _provider_item(item, provider="claude", model=claude_model, run_id=run_id)
        for item in (claude_doc or {}).get("items", [])
    ]
    codex_items = [
        _provider_item(item, provider="codex", model=codex_model, run_id=run_id)
        for item in (codex_doc or {}).get("items", [])
    ]
    merged_items = list(claude_items)
    unmatched_codex = set(range(len(codex_items)))
    matches: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []

    for claude_index, claude_item in enumerate(claude_items):
        comparisons = []
        for codex_index in sorted(unmatched_codex):
            evidence = compare_items(claude_item, codex_items[codex_index])
            comparisons.append((evidence["score"], codex_index, evidence))
            if not evidence["duplicate"] and evidence["score"] >= 0.50:
                candidates.append({
                    "claude_id": claude_item.get("id"),
                    "codex_id": codex_items[codex_index].get("id"),
                    **evidence,
                })
        duplicates = [row for row in comparisons if row[2]["duplicate"]]
        if not duplicates:
            continue
        _, codex_index, evidence = max(duplicates, key=lambda row: row[0])
        merged_items[claude_index] = _merge_duplicate(claude_item, codex_items[codex_index])
        unmatched_codex.remove(codex_index)
        matches.append({
            "claude_id": claude_item.get("id"),
            "codex_id": codex_items[codex_index].get("id"),
            **evidence,
        })

    merged_items.extend(codex_items[index] for index in sorted(unmatched_codex))
    for index, item in enumerate(merged_items, start=1):
        item["id"] = f"OPT-{index:03d}"

    status = "ok" if claude_doc is not None and codex_doc is not None else "degraded"
    source_counts = {
        "claude_only": sum(item.get("detector_summary") == "claude" for item in merged_items),
        "codex_only": sum(item.get("detector_summary") == "codex" for item in merged_items),
        "claude_codex": sum(item.get("detector_summary") == "claude_codex" for item in merged_items),
    }
    base_meta = (claude_doc or codex_doc or {}).get("meta") or {}
    merged = {
        "meta": _meta_for_items(
            base_meta, merged_items, run_id=run_id, status=status,
            source_counts=source_counts, duplicate_count=len(matches),
        ),
        "items": merged_items,
    }
    report = {
        "run_id": run_id,
        "status": status,
        "inputs": {"claude": len(claude_items), "codex": len(codex_items)},
        "output_items": len(merged_items),
        "source_counts": source_counts,
        "duplicates_merged": len(matches),
        "matches": matches,
        "possible_duplicates_kept": candidates,
        "policy": "conservative deterministic match; ambiguous pairs are kept",
    }
    return merged, report


def restore_ensemble_provenance(output_dir: Path) -> int:
    """Restore ensemble attribution after an agentic Corrector rewrites items.

    Corrector keeps item IDs by contract. Raw provider artifacts remain untouched;
    this helper only reattaches attribution fields that an older prompt may omit.
    """
    output_dir = Path(output_dir)
    if not (output_dir / "optimization_merge_report.json").is_file():
        return 0
    current_path = output_dir / "optimization.json"
    source_path = output_dir / "optimization_pre_review.json"
    if not source_path.is_file():
        source_path = current_path
    current = _read_document(current_path)
    source = _read_document(source_path)
    if current is None or source is None:
        return 0
    source_by_id = {
        str(item.get("id") or ""): item
        for item in source.get("items", [])
        if item.get("id")
    }
    restored = 0
    for item in current.get("items", []):
        old = source_by_id.get(str(item.get("id") or ""))
        if not old:
            continue
        if not isinstance(item.get("provenance"), dict) and isinstance(old.get("provenance"), dict):
            item["provenance"] = old["provenance"]
            restored += 1
        if not item.get("detector_summary") and old.get("detector_summary"):
            item["detector_summary"] = old["detector_summary"]
    old_ensemble = (source.get("meta") or {}).get("ensemble")
    if old_ensemble and not (current.get("meta") or {}).get("ensemble"):
        current.setdefault("meta", {})["ensemble"] = old_ensemble
    if restored:
        _atomic_write_json(current_path, current)
    return restored


def _snapshot_inputs(source_dir: Path, provider_dir: Path) -> None:
    provider_dir.mkdir(parents=True, exist_ok=True)
    for name in INPUT_ARTIFACTS:
        source = resolve_existing(source_dir, name)
        if source.is_file():
            # копируем под КАНОНИЧЕСКИМ именем, даже если источник был legacy
            shutil.copy2(source, provider_dir / name)


async def _provider_log(
    callback: Callable[..., Awaitable[None]], provider: str, message: str,
) -> None:
    await callback(f"[OPT {provider}] {message}")


def _make_provider_on_output(
    log: Callable[..., Awaitable[None]], provider: str,
) -> Callable[[str], Awaitable[None]]:
    """on_output для ноги ансамбля.

    Claude стримит stream-json — его разбирает manager._log (result →
    cli_summary, прочее подавляется). Codex exec в plain-режиме стримит весь
    транскрипт (эхо промпта, вывод команд, дампы файлов) — без фильтра это
    ~2500 строк мусора на прогон, поэтому пропускаем через белый список
    CodexExecStreamFilter.
    """
    async def _prefixed(message: str) -> None:
        await _provider_log(log, provider, message)

    if provider != "codex":
        return _prefixed
    return wrap_codex_on_output(_prefixed)


async def _run_provider(
    *, provider: str, model: str, project_info: dict[str, Any], project_id: str,
    provider_dir: Path, source_output_dir: Path, version_dir: Path,
    version_id: str | None, log: Callable[..., Awaitable[None]],
    reasoning_effort: str | None = None,
) -> ProviderRun:
    result = ProviderRun(provider=provider, model=model, output_dir=provider_dir)
    try:
        exit_code, output, cli_result = await claude_runner.run_optimization(
            project_info,
            project_id,
            on_output=_make_provider_on_output(log, provider),
            output_dir=provider_dir,
            version_dir=version_dir,
            version_id=version_id,
            model_override=model,
            visual_output_dir=source_output_dir,
            reasoning_effort_override=reasoning_effort,
        )
        result.exit_code = exit_code
        result.output = output or ""
        result.cli_result = cli_result
        result.cancelled = is_cancelled(exit_code)
        result.rate_limited = is_rate_limited(exit_code, result.output, "")
        result.data = _read_document(provider_dir / "optimization.json")
        if result.data is None:
            result.error = f"exit {exit_code}; optimization.json missing or invalid"
        elif exit_code != 0:
            result.error = f"CLI exit {exit_code}, valid optimization.json preserved"
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        result.error = str(exc)
    return result


async def run_optimization_ensemble(
    *, project_info: dict[str, Any], project_id: str, output_dir: Path,
    version_dir: Path, version_id: str | None,
    log: Callable[..., Awaitable[None]],
) -> EnsembleRunResult:
    """Run Claude and Codex concurrently against the same frozen input artifacts."""
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:8]
    run_root = output_dir / "_optimization_ensemble" / run_id
    claude_dir = run_root / "claude"
    codex_dir = run_root / "codex"
    _snapshot_inputs(output_dir, claude_dir)
    _snapshot_inputs(output_dir, codex_dir)
    for name in (
        "optimization_claude.json",
        "optimization_codex.json",
        "optimization_merge_report.json",
    ):
        stale = output_dir / name
        if stale.is_file():
            shutil.copy2(stale, run_root / f"previous_{name}")
            stale.unlink()
    previous = output_dir / "optimization.json"
    if previous.is_file():
        shutil.copy2(previous, run_root / "previous_optimization.json")

    await log("OPT ensemble: Claude и Codex запущены параллельно на одном снимке входных данных")
    providers = await asyncio.gather(
        _run_provider(
            provider="claude", model=OPTIMIZATION_ENSEMBLE_CLAUDE_MODEL,
            project_info=project_info, project_id=project_id, provider_dir=claude_dir,
            source_output_dir=output_dir, version_dir=version_dir,
            version_id=version_id, log=log,
        ),
        _run_provider(
            provider="codex", model=OPTIMIZATION_ENSEMBLE_CODEX_MODEL,
            project_info=project_info, project_id=project_id, provider_dir=codex_dir,
            source_output_dir=output_dir, version_dir=version_dir,
            version_id=version_id, log=log,
            reasoning_effort=OPTIMIZATION_ENSEMBLE_CODEX_REASONING_EFFORT,
        ),
    )

    successful = [provider for provider in providers if provider.success]
    provider_status = {
        provider.provider: {
            "model": provider.model,
            "success": provider.success,
            "exit_code": provider.exit_code,
            "error": provider.error,
            "output_dir": str(provider.output_dir),
        }
        for provider in providers
    }
    if not successful:
        report = {"run_id": run_id, "status": "failed", "providers": provider_status}
        _atomic_write_json(output_dir / "optimization_merge_report.json", report)
        error = "; ".join(
            f"{provider.provider}: {provider.error or provider.output[-160:]}" for provider in providers
        )
        return EnsembleRunResult(
            success=False, run_id=run_id, providers=providers, error=error,
            cancelled=any(provider.cancelled for provider in providers),
            rate_limited=any(provider.rate_limited for provider in providers),
        )

    docs = {provider.provider: provider.data for provider in providers}
    for provider in successful:
        raw_target = output_dir / f"optimization_{provider.provider}.json"
        _atomic_write_json(raw_target, provider.data or {"meta": {}, "items": []})

    merged, report = merge_optimization_documents(
        docs.get("claude"), docs.get("codex"), run_id=run_id,
    )
    report["providers"] = provider_status
    _atomic_write_json(output_dir / "optimization.json", merged)
    _atomic_write_json(output_dir / "optimization_merge_report.json", report)
    status = str(report["status"])
    await log(
        f"OPT ensemble: {report['inputs']['claude']} Claude + {report['inputs']['codex']} Codex "
        f"-> {report['output_items']} итоговых; дублей объединено {report['duplicates_merged']}"
    )
    return EnsembleRunResult(
        success=True, run_id=run_id, providers=providers, status=status,
        error="; ".join(
            f"{provider.provider}: {provider.error}" for provider in providers
            if not provider.success and provider.error
        ) or None,
    )
