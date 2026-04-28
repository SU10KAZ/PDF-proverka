"""
Experiment runner: OpenRouter path for stage 02 block_batch.

Phase A — model quality comparison (single-block subset, flash vs pro)
Phase B — batch profile economics on full document (chosen mainline)
Phase C — parallelism economics (top-2 profiles × parallelism 2/3/4)
Fallback sample — optional 20-block Pro escalation check

Все пути через OpenRouter: google/gemini-2.5-flash и google/gemini-3.1-pro-preview.
Прямой Gemini Developer API в этом скрипте не используется (гео-блокирован).

Usage:
    python scripts/run_gemini_openrouter_stage02_experiment.py \\
        --pdf "13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf" --dry-run

    python scripts/run_gemini_openrouter_stage02_experiment.py \\
        --pdf "13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf" --phase a --parallelism 3

Production defaults (block_batch stage model в stage_models.json) не переключаются автоматически.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import os
import random
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# ── Project root on Python path ───────────────────────────────────────────────
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
    stream=sys.stdout,
)
logger = logging.getLogger("openrouter_exp")

# ── Batch profiles ────────────────────────────────────────────────────────────
BATCH_PROFILES: dict[str, dict[str, dict[str, int]]] = {
    "b6": {
        "heavy":  {"target": 4, "max": 6},
        "normal": {"target": 6, "max": 6},
        "light":  {"target": 6, "max": 6},
    },
    "b8": {
        "heavy":  {"target": 4, "max": 6},
        "normal": {"target": 8, "max": 8},
        "light":  {"target": 8, "max": 8},
    },
    "b10": {
        "heavy":  {"target": 5, "max": 6},
        "normal": {"target": 10, "max": 10},
        "light":  {"target": 10, "max": 10},
    },
    "b12": {
        "heavy":  {"target": 5, "max": 6},
        "normal": {"target": 12, "max": 12},
        "light":  {"target": 12, "max": 12},
    },
}

SUBSET_SIZE = 60
SUBSET_SEED = 42
FALLBACK_SAMPLE_SIZE = 20
HARD_CAP = 12
SOLO_THRESHOLD_KB = 3000

MODEL_FLASH = "google/gemini-2.5-flash"
MODEL_PRO = "google/gemini-3.1-pro-preview"


# ─── Per-run metrics ──────────────────────────────────────────────────────────

@dataclass
class RunMetrics:
    run_id: str = ""
    model_id: str = ""
    batch_profile: str = "single"
    parallelism: int = 1
    mode: str = "single_block"

    total_input_blocks: int = 0
    total_batches: int = 0
    completed_batches: int = 0
    failed_batches: int = 0

    risk_heavy: int = 0
    risk_normal: int = 0
    risk_light: int = 0

    coverage_pct: float = 0.0
    missing_count: int = 0
    duplicate_count: int = 0
    extra_count: int = 0
    inferred_block_id_count: int = 0

    unreadable_count: int = 0
    empty_summary_count: int = 0
    empty_key_values_count: int = 0
    total_key_values: int = 0
    median_key_values: float = 0.0
    blocks_with_findings: int = 0
    total_findings: int = 0
    findings_per_100_blocks: float = 0.0

    elapsed_s: float = 0.0
    avg_batch_duration_s: float = 0.0
    median_batch_duration_s: float = 0.0
    p95_batch_duration_s: float = 0.0

    total_prompt_tokens: int = 0
    total_output_tokens: int = 0
    total_reasoning_tokens: int = 0
    total_cached_tokens: int = 0
    avg_prompt_tokens: float = 0.0
    median_prompt_tokens: float = 0.0

    avg_batch_size: float = 0.0
    median_batch_size: float = 0.0
    max_batch_size: int = 0
    avg_batch_kb: float = 0.0
    median_batch_kb: float = 0.0

    total_cost_usd: float = 0.0
    cost_per_valid_block: float = 0.0
    cost_per_finding: float = 0.0
    cost_sources_actual: int = 0
    cost_sources_estimated: int = 0

    retry_count: int = 0
    provider_errors: int = 0
    schema_errors: int = 0
    timeout_errors: int = 0

    strict_schema_enabled: bool = True
    response_healing_enabled: bool = True
    require_parameters_enabled: bool = True
    dry_run: bool = False


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    return (s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2)


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = int(len(s) * pct / 100)
    return s[min(idx, len(s) - 1)]


def _save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _save_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


# ─── Block loading & subset ───────────────────────────────────────────────────

def load_blocks_index(project_dir: Path) -> list[dict]:
    idx_path = project_dir / "_output" / "blocks" / "index.json"
    if not idx_path.exists():
        raise FileNotFoundError(f"blocks/index.json not found: {idx_path}")
    idx = json.loads(idx_path.read_text(encoding="utf-8"))
    blocks = idx.get("blocks", [])
    if not blocks:
        raise ValueError(f"No blocks in index.json: {idx_path}")
    return blocks


def create_or_load_subset(
    blocks: list[dict],
    subset_size: int,
    seed: int,
    subset_file: Path | None,
    exp_dir: Path,
) -> tuple[list[str], list[dict]]:
    """Return (block_ids, block_dicts) for the fixed subset (reuse or create)."""
    if subset_file and subset_file.exists():
        ids = json.loads(subset_file.read_text(encoding="utf-8"))
        id_set = set(ids)
        subset_blocks = [b for b in blocks if b["block_id"] in id_set]
        if len(subset_blocks) == len(ids):
            logger.info("Reusing existing subset (%d blocks) from %s", len(ids), subset_file)
            return ids, subset_blocks
        logger.warning(
            "Subset file has %d IDs but only %d found in index — regenerating.",
            len(ids), len(subset_blocks),
        )

    pages: dict[int, list[dict]] = {}
    for b in blocks:
        pages.setdefault(b.get("page", 0), []).append(b)
    page_lists = list(pages.values())

    rng = random.Random(seed)
    for lst in page_lists:
        rng.shuffle(lst)

    chosen: list[dict] = []
    page_iters = [iter(lst) for lst in page_lists]
    rng.shuffle(page_iters)

    while len(chosen) < subset_size and any(True for _ in page_iters if page_iters):
        for it in list(page_iters):
            if len(chosen) >= subset_size:
                break
            try:
                chosen.append(next(it))
            except StopIteration:
                page_iters.remove(it)

    ids = [b["block_id"] for b in chosen]
    _save_json(exp_dir / "fixed_subset_block_ids.json", ids)
    _save_json(exp_dir / "fixed_subset_manifest.json", chosen)
    logger.info("Created fixed subset: %d blocks", len(ids))
    return ids, chosen


# ─── Risk classification + batch packing ─────────────────────────────────────

def classify_risk(block: dict) -> str:
    """heavy/normal/light (mirrors blocks.py _classify_block_risk)."""
    if block.get("is_full_page"):
        return "heavy"
    if block.get("quadrant"):
        return "heavy"
    if block.get("merged_block_ids"):
        return "heavy"

    size_kb = float(block.get("size_kb", 0) or 0)
    ocr_len = int(block.get("ocr_text_len", 0) or 0)
    render = block.get("render_size") or []
    render_long = max((float(x) for x in render), default=0.0) if render else 0.0
    crop = block.get("crop_px") or []
    crop_long = 0.0
    if isinstance(crop, (list, tuple)) and len(crop) == 4:
        crop_long = max(float(crop[2]) - float(crop[0]), float(crop[3]) - float(crop[1]))

    if size_kb >= 2000 or render_long >= 2500 or ocr_len >= 4000 or crop_long >= 3000:
        return "heavy"
    if size_kb >= 500 or render_long >= 1500 or ocr_len >= 1000 or crop_long >= 1500:
        return "normal"
    return "light"


def pack_blocks(
    blocks: list[dict],
    risk_targets: dict[str, dict[str, int]],
    hard_cap: int = HARD_CAP,
    solo_kb: int = SOLO_THRESHOLD_KB,
) -> list[list[dict]]:
    """Pack blocks into batches (mirrors blocks.py _pack_blocks_claude_risk_aware)."""
    hard_cap = min(hard_cap, HARD_CAP)

    solo = [b for b in blocks if float(b.get("size_kb", 0)) >= solo_kb]
    normal_list = [b for b in blocks if float(b.get("size_kb", 0)) < solo_kb]

    batches: list[list[dict]] = [[b] for b in solo]
    current: list[dict] = []
    current_cap = hard_cap
    heavy_count = 0
    heavy_max = min(risk_targets.get("heavy", {"max": 6})["max"], hard_cap)

    for b in normal_list:
        risk = classify_risk(b)
        b_cap = min(risk_targets.get(risk, risk_targets.get("normal", {"max": 8}))["max"], hard_cap)
        proposed_cap = min(current_cap, b_cap)

        needs_new = bool(current) and (
            len(current) >= proposed_cap
            or (risk == "heavy" and heavy_count >= heavy_max)
        )

        if needs_new:
            if current:
                batches.append(current)
            current = []
            current_cap = hard_cap
            heavy_count = 0
            heavy_max = min(risk_targets.get("heavy", {"max": 6})["max"], hard_cap)

        if not current:
            current_cap = b_cap
        else:
            current_cap = min(current_cap, b_cap)
        if risk == "heavy":
            heavy_count += 1
        current.append(b)

    if current:
        batches.append(current)

    # Final hard-cap split
    final: list[list[dict]] = []
    for group in batches:
        if len(group) <= hard_cap:
            final.append(group)
        else:
            for i in range(0, len(group), hard_cap):
                final.append(group[i:i + hard_cap])
    return final


def apply_byte_cap_split(batches: list[list[dict]], byte_cap_kb: int) -> list[list[dict]]:
    """Apply deterministic byte-cap splitter on top of pre-packed batches."""
    from webapp.services.openrouter_block_batch import split_batch_by_byte_cap
    result: list[list[dict]] = []
    for group in batches:
        total_kb = sum(float(b.get("size_kb", 0) or 0) for b in group)
        if total_kb <= byte_cap_kb and len(group) <= HARD_CAP:
            result.append(group)
        else:
            sub = split_batch_by_byte_cap(group, byte_cap_kb=byte_cap_kb, hard_cap=HARD_CAP)
            result.extend(sub)
    return result


# ─── Message building ────────────────────────────────────────────────────────

def build_system_prompt(project_info: dict, total_blocks: int) -> str:
    section = (project_info or {}).get("section", "KJ")
    return f"""You are an expert auditor of residential building project documentation (section: {section}).

## Your task
Analyze each provided image block (cropped drawing fragment) and return a JSON object with block_analyses array.

## CRITICAL RULES
1. Return EXACTLY one analysis object per block in the batch.
2. Use EXACTLY the block_id value provided in the input — do NOT modify it.
3. Extract ALL readable text: cable specs, breaker ratings, dimensions, pipe sizes, equipment tags, material marks.
4. Identify audit findings with appropriate severity levels.
5. Set unreadable_text=true and describe unreadable_details if text is illegible.

## Output schema (strict JSON, no markdown)
Return a JSON object with this structure:
{{
  "batch_id": <int>,
  "project_id": "<string>",
  "timestamp": "<ISO8601 string>",
  "block_analyses": [
    {{
      "block_id": "<exact id from input>",
      "page": <int>,
      "sheet": "<sheet number or null>",
      "label": "<short drawing title>",
      "sheet_type": "<schema|plan|section|detail|table|other>",
      "unreadable_text": false,
      "unreadable_details": null,
      "summary": "<concise description>",
      "key_values_read": ["<all extracted values>"],
      "findings": [
        {{
          "id": "G-001",
          "severity": "<КРИТИЧЕСКОЕ|ЭКОНОМИЧЕСКОЕ|ЭКСПЛУАТАЦИОННОЕ|РЕКОМЕНДАТЕЛЬНОЕ|ПРОВЕРИТЬ ПО СМЕЖНЫМ>",
          "category": "<category>",
          "finding": "<detailed description>",
          "norm": "<normative reference or empty>",
          "norm_quote": null,
          "block_evidence": "<what supports this>",
          "value_found": "<specific problematic value or empty>"
        }}
      ]
    }}
  ]
}}

## Severity
- КРИТИЧЕСКОЕ: Violates mandatory codes.
- ЭКОНОМИЧЕСКОЕ: Extra costs / waste.
- ЭКСПЛУАТАЦИОННОЕ: Operational problems.
- РЕКОМЕНДАТЕЛЬНОЕ: Minor / typo.
- ПРОВЕРИТЬ ПО СМЕЖНЫМ: Needs cross-section check.

Return ONLY the JSON object. No markdown fences, no explanation text.
Total blocks in this experiment run: {total_blocks}
"""


def build_page_contexts(project_dir: Path) -> dict[int, str]:
    dg_path = project_dir / "_output" / "document_graph.json"
    if not dg_path.exists():
        return {}
    try:
        dg = json.loads(dg_path.read_text(encoding="utf-8"))
        contexts: dict[int, str] = {}
        for page_data in dg.get("pages", []):
            page_num = page_data.get("page", 0)
            lines = []
            sheet_no = page_data.get("sheet_no", "")
            sheet_name = page_data.get("sheet_name", "")
            if sheet_no:
                lines.append(f"**Лист:** {sheet_no}" + (f" — {sheet_name}" if sheet_name else ""))
            for tb in page_data.get("text_blocks", []):
                txt = str(tb.get("text", "")).strip()
                if txt:
                    lines.append(txt[:500])
            contexts[page_num] = "\n".join(lines)
        return contexts
    except Exception as e:
        logger.warning("Could not load document_graph.json: %s", e)
        return {}


def build_messages(
    blocks: list[dict],
    project_dir: Path,
    project_info: dict,
    batch_id: int,
    total_batches: int,
    system_prompt: str,
    page_contexts: dict[int, str],
) -> list[dict]:
    import base64
    blocks_dir = project_dir / "_output" / "blocks"

    block_labels = [
        f"block_id={b['block_id']}, page={b.get('page', '?')}, OCR_label={b.get('ocr_label', '')}"
        for b in blocks
    ]
    user_parts: list[dict] = [
        {
            "type": "text",
            "text": (
                f"## Batch {batch_id:03d}/{total_batches} — {len(blocks)} blocks\n\n"
                f"Analyze EACH block below. Return block_id EXACTLY as given.\n\n"
                f"Block list:\n" + "\n".join(f"- {l}" for l in block_labels)
            ),
        }
    ]

    current_page: int | None = None
    for b in blocks:
        page = b.get("page", 0)
        if page != current_page:
            current_page = page
            ctx = page_contexts.get(page, f"PDF Page {page}")
            user_parts.append({
                "type": "text",
                "text": f"\n=== PDF Page {page} ===\n{ctx}\n",
            })

        img_file = blocks_dir / b["file"]
        if img_file.exists():
            b64 = base64.b64encode(img_file.read_bytes()).decode()
            user_parts.append({
                "type": "image_url",
                "image_url": {"url": f"data:image/png;base64,{b64}"},
            })
        else:
            user_parts.append({
                "type": "text",
                "text": f"[IMAGE NOT FOUND: {b['file']}]",
            })

        user_parts.append({
            "type": "text",
            "text": f"[block_id: {b['block_id']}]",
        })

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user",   "content": user_parts},
    ]


# ─── Metrics ──────────────────────────────────────────────────────────────────

@dataclass
class BatchResultEnvelope:
    """Lightweight envelope for run_batches_async output."""
    batch_id: int
    model_id: str
    input_block_ids: list[str]
    is_error: bool = False
    error_type: str = ""          # provider | schema | timeout | completeness
    error_message: str = ""
    duration_ms: int = 0
    parsed_data: dict | None = None
    prompt_tokens: int = 0
    output_tokens: int = 0
    reasoning_tokens: int = 0
    cached_tokens: int = 0
    cost_usd: float = 0.0
    cost_source: str = "estimated"
    retry_count: int = 0
    missing: list[str] = field(default_factory=list)
    duplicates: list[str] = field(default_factory=list)
    extra: list[str] = field(default_factory=list)
    inferred_block_id_count: int = 0
    response_id: str = ""
    finish_reason: str = ""


def compute_metrics(
    results: list[BatchResultEnvelope],
    all_input_blocks: list[dict],
    run_id: str,
    model_id: str,
    batch_profile: str,
    parallelism: int,
    mode: str,
    elapsed_s: float,
    *,
    strict_schema_enabled: bool,
    response_healing_enabled: bool,
    require_parameters_enabled: bool,
    dry_run: bool,
) -> RunMetrics:
    m = RunMetrics(
        run_id=run_id,
        model_id=model_id,
        batch_profile=batch_profile,
        parallelism=parallelism,
        mode=mode,
        elapsed_s=elapsed_s,
        strict_schema_enabled=strict_schema_enabled,
        response_healing_enabled=response_healing_enabled,
        require_parameters_enabled=require_parameters_enabled,
        dry_run=dry_run,
        total_input_blocks=len(all_input_blocks),
        total_batches=len(results),
    )

    for b in all_input_blocks:
        r = classify_risk(b)
        if r == "heavy":
            m.risk_heavy += 1
        elif r == "normal":
            m.risk_normal += 1
        else:
            m.risk_light += 1

    all_returned_ids: set[str] = set()
    durations: list[float] = []
    prompt_tok_list: list[float] = []
    batch_sizes: list[int] = []
    batch_kbs: list[float] = []

    block_by_id = {b["block_id"]: b for b in all_input_blocks}
    duplicate_total = 0
    extra_total = 0

    for res in results:
        if res.is_error:
            m.failed_batches += 1
            if res.error_type == "provider":
                m.provider_errors += 1
            elif res.error_type == "schema":
                m.schema_errors += 1
            elif res.error_type == "timeout":
                m.timeout_errors += 1
        else:
            m.completed_batches += 1

        m.retry_count += res.retry_count
        durations.append(res.duration_ms / 1000.0)
        m.total_prompt_tokens += res.prompt_tokens
        m.total_output_tokens += res.output_tokens
        m.total_reasoning_tokens += res.reasoning_tokens
        m.total_cached_tokens += res.cached_tokens
        m.total_cost_usd += res.cost_usd
        m.inferred_block_id_count += res.inferred_block_id_count
        duplicate_total += len(res.duplicates)
        extra_total += len(res.extra)

        if res.cost_source == "actual":
            m.cost_sources_actual += 1
        else:
            m.cost_sources_estimated += 1

        if res.prompt_tokens:
            prompt_tok_list.append(float(res.prompt_tokens))

        batch_sizes.append(len(res.input_block_ids))
        kb = sum(float(block_by_id[b].get("size_kb", 0)) for b in res.input_block_ids if b in block_by_id)
        batch_kbs.append(kb)

        if not res.is_error and res.parsed_data:
            for analysis in res.parsed_data.get("block_analyses", []):
                if not isinstance(analysis, dict):
                    continue
                bid = analysis.get("block_id", "")
                if bid:
                    all_returned_ids.add(bid)
                if analysis.get("unreadable_text"):
                    m.unreadable_count += 1
                s = str(analysis.get("summary", "")).strip()
                if not s:
                    m.empty_summary_count += 1
                kv = analysis.get("key_values_read", [])
                if not kv:
                    m.empty_key_values_count += 1
                else:
                    m.total_key_values += len(kv)
                findings = analysis.get("findings", [])
                if findings:
                    m.blocks_with_findings += 1
                    m.total_findings += len(findings)

    # Coverage
    input_ids = {b["block_id"] for b in all_input_blocks}
    missing_ids = input_ids - all_returned_ids
    m.missing_count = len(missing_ids)
    m.duplicate_count = duplicate_total
    m.extra_count = extra_total
    if m.total_input_blocks:
        m.coverage_pct = round(
            (m.total_input_blocks - m.missing_count) / m.total_input_blocks * 100, 2
        )

    # KV median
    per_block_kv: list[float] = []
    for res in results:
        if not res.is_error and res.parsed_data:
            for a in res.parsed_data.get("block_analyses", []):
                if isinstance(a, dict):
                    per_block_kv.append(float(len(a.get("key_values_read", []))))
    m.median_key_values = round(_median(per_block_kv), 2)

    if m.total_input_blocks:
        m.findings_per_100_blocks = round(m.total_findings / m.total_input_blocks * 100, 2)

    if durations:
        m.avg_batch_duration_s = round(sum(durations) / len(durations), 2)
        m.median_batch_duration_s = round(_median(durations), 2)
        m.p95_batch_duration_s = round(_percentile(durations, 95), 2)

    if prompt_tok_list:
        m.avg_prompt_tokens = round(sum(prompt_tok_list) / len(prompt_tok_list), 1)
        m.median_prompt_tokens = round(_median(prompt_tok_list), 1)

    if batch_sizes:
        m.avg_batch_size = round(sum(batch_sizes) / len(batch_sizes), 2)
        m.median_batch_size = round(_median([float(s) for s in batch_sizes]), 2)
        m.max_batch_size = max(batch_sizes)
    if batch_kbs:
        m.avg_batch_kb = round(sum(batch_kbs) / len(batch_kbs), 2)
        m.median_batch_kb = round(_median(batch_kbs), 2)

    valid_blocks = m.total_input_blocks - m.missing_count
    if valid_blocks > 0:
        m.cost_per_valid_block = round(m.total_cost_usd / valid_blocks, 6)
    if m.total_findings > 0:
        m.cost_per_finding = round(m.total_cost_usd / m.total_findings, 6)

    return m


# ─── Winner rules ─────────────────────────────────────────────────────────────

def apply_phase_a_gate(flash_m: RunMetrics, pro_m: RunMetrics) -> tuple[str, str, str]:
    """Return (mainline, fallback, gate_summary_md)."""
    lines = ["## Phase A Quality Gate: google/gemini-2.5-flash vs google/gemini-3.1-pro-preview\n"]
    failures: list[str] = []

    def check(cond: bool, msg: str) -> bool:
        if not cond:
            failures.append(msg)
        lines.append(f"- {'PASS' if cond else 'FAIL'} {msg}")
        return cond

    check(flash_m.coverage_pct == 100.0,
          f"coverage=100% (flash={flash_m.coverage_pct}%)")
    check(flash_m.missing_count == 0,
          f"missing=0 (flash={flash_m.missing_count})")
    check(flash_m.duplicate_count == 0,
          f"duplicate=0 (flash={flash_m.duplicate_count})")
    check(flash_m.extra_count == 0,
          f"extra=0 (flash={flash_m.extra_count})")
    check(flash_m.unreadable_count <= pro_m.unreadable_count,
          f"unreadable flash<=pro ({flash_m.unreadable_count}<={pro_m.unreadable_count})")

    bwf_ok = True
    f_ratio_ok = True
    kv_ok = True

    if pro_m.blocks_with_findings > 0:
        bwf_ratio = flash_m.blocks_with_findings / pro_m.blocks_with_findings
        bwf_ok = check(bwf_ratio >= 0.95,
                       f"blocks_with_findings flash>=95% of pro ({bwf_ratio:.1%})")
    if pro_m.total_findings > 0:
        f_ratio = flash_m.total_findings / pro_m.total_findings
        f_ratio_ok = check(f_ratio >= 0.95,
                           f"total_findings flash>=95% of pro ({f_ratio:.1%})")
    if pro_m.median_key_values > 0:
        kv_ratio = flash_m.median_key_values / pro_m.median_key_values
        kv_ok = check(kv_ratio >= 0.90,
                      f"median_kv flash>=90% of pro ({kv_ratio:.1%})")

    cost_ok = (
        flash_m.cost_per_valid_block < pro_m.cost_per_valid_block * 0.7
        if pro_m.cost_per_valid_block else True
    )
    check(cost_ok,
          f"cost/valid_block flash substantially < pro "
          f"(flash=${flash_m.cost_per_valid_block:.5f} pro=${pro_m.cost_per_valid_block:.5f})")

    lines.append("")
    if failures:
        lines.append(f"**GATE RESULT: Flash FAILED** ({len(failures)} checks failed)")
        lines.append(f"-> Mainline candidate: **{MODEL_PRO}**")
        lines.append(f"-> Fallback: N/A (pro is mainline)")
        mainline = MODEL_PRO
        fallback = MODEL_PRO
    else:
        lines.append("**GATE RESULT: Flash PASSED** all checks")
        lines.append(f"-> Mainline candidate: **{MODEL_FLASH}**")
        lines.append(f"-> Fallback/escalation: **{MODEL_PRO}**")
        mainline = MODEL_FLASH
        fallback = MODEL_PRO

    return mainline, fallback, "\n".join(lines)


def select_batch_profile_winner(metrics_list: list[RunMetrics]) -> tuple[str, str]:
    """Priority: coverage -> stability -> cost/valid_block -> elapsed."""
    eligible = [m for m in metrics_list if m.coverage_pct == 100.0 and m.missing_count == 0 and m.duplicate_count == 0 and m.extra_count == 0]
    if not eligible:
        eligible = sorted(metrics_list, key=lambda m: -m.coverage_pct)[:2]

    eligible.sort(key=lambda m: (
        -(m.coverage_pct),
        m.failed_batches,
        m.cost_per_valid_block if m.cost_per_valid_block else 9999,
        m.elapsed_s,
    ))
    winner = eligible[0].batch_profile if eligible else "b10"
    runner_up = eligible[1].batch_profile if len(eligible) > 1 else winner
    return winner, runner_up


def select_parallelism_winner(metrics_list: list[RunMetrics]) -> tuple[int, str]:
    """Priority: coverage/stability -> elapsed -> cost/valid_block -> retry rate.

    Returns (parallelism, profile).
    """
    eligible = [m for m in metrics_list if m.coverage_pct == 100.0 and m.missing_count == 0]
    if not eligible:
        eligible = sorted(metrics_list, key=lambda m: -m.coverage_pct)
    eligible.sort(key=lambda m: (
        -(m.coverage_pct),
        m.failed_batches,
        m.elapsed_s,
        m.cost_per_valid_block if m.cost_per_valid_block else 9999,
        (m.provider_errors + m.timeout_errors + m.schema_errors),
    ))
    if not eligible:
        return 3, ""
    return eligible[0].parallelism, eligible[0].batch_profile


# ─── Runner ───────────────────────────────────────────────────────────────────

async def run_batches_async(
    batch_list: list[list[dict]],
    project_dir: Path,
    project_info: dict,
    system_prompt: str,
    page_contexts: dict[int, str],
    all_input_blocks: list[dict],
    model_id: str,
    parallelism: int,
    run_id: str,
    *,
    strict_schema: bool,
    response_healing: bool,
    require_parameters: bool,
    provider_data_collection: str | None,
    dry_run: bool,
) -> tuple[list[BatchResultEnvelope], float]:
    from webapp.services.openrouter_block_batch import run_openrouter_block_batch

    total = len(batch_list)
    semaphore = asyncio.Semaphore(parallelism)
    results: list[BatchResultEnvelope | None] = [None] * total

    async def _run_one(idx: int, batch_blocks: list[dict]) -> None:
        async with semaphore:
            bid = idx + 1
            input_ids = [b["block_id"] for b in batch_blocks]

            if dry_run:
                # Simulate perfect response (validates logic without API calls)
                results[idx] = BatchResultEnvelope(
                    batch_id=bid,
                    model_id=model_id,
                    input_block_ids=input_ids,
                    duration_ms=0,
                    parsed_data={
                        "batch_id": bid,
                        "project_id": "dry_run",
                        "timestamp": "",
                        "block_analyses": [
                            {
                                "block_id": bk["block_id"],
                                "page": bk.get("page", 0),
                                "sheet": None,
                                "label": bk.get("ocr_label", ""),
                                "sheet_type": "other",
                                "unreadable_text": False,
                                "unreadable_details": None,
                                "summary": "[dry-run placeholder]",
                                "key_values_read": [],
                                "findings": [],
                            }
                            for bk in batch_blocks
                        ],
                    },
                )
                return

            messages = build_messages(
                batch_blocks, project_dir, project_info,
                bid, total, system_prompt, page_contexts,
            )

            result = await run_openrouter_block_batch(
                messages,
                input_ids,
                model=model_id,
                batch_id=bid,
                strict_schema=strict_schema,
                response_healing=response_healing,
                require_parameters=require_parameters,
                provider_data_collection=provider_data_collection,
                temperature=0.2,
                single_block_inference=(len(input_ids) == 1),
            )

            err_type = ""
            err_msg = ""
            if result.is_error:
                emsg = (result.llm.error_message or "").lower()
                if "timeout" in emsg:
                    err_type = "timeout"
                elif "schema" in emsg or "invalid json" in emsg:
                    err_type = "schema"
                else:
                    err_type = "provider"
                err_msg = result.llm.error_message

            env = BatchResultEnvelope(
                batch_id=bid,
                model_id=model_id,
                input_block_ids=input_ids,
                is_error=result.is_error,
                error_type=err_type,
                error_message=err_msg,
                duration_ms=result.llm.duration_ms,
                parsed_data=result.llm.json_data if isinstance(result.llm.json_data, dict) else None,
                prompt_tokens=result.llm.input_tokens,
                output_tokens=result.llm.output_tokens,
                reasoning_tokens=result.llm.reasoning_tokens,
                cached_tokens=result.llm.cached_tokens,
                cost_usd=result.llm.cost_usd,
                cost_source=result.llm.cost_source,
                missing=result.missing,
                duplicates=result.duplicates,
                extra=result.extra,
                inferred_block_id_count=result.inferred_block_id_count,
                response_id=result.llm.response_id,
                finish_reason=result.llm.finish_reason,
            )
            results[idx] = env

            status = "OK" if not env.is_error else f"ERROR({env.error_type})"
            coverage_tag = ""
            if not env.is_error:
                if env.missing or env.duplicates or env.extra:
                    coverage_tag = f" miss={len(env.missing)} dup={len(env.duplicates)} extra={len(env.extra)}"
            logger.info(
                "[%s] batch %03d/%d: %s blocks=%d tokens(p/o/r/c)=%d/%d/%d/%d cost=$%.4f(%s)%s",
                run_id, bid, total, status, len(input_ids),
                env.prompt_tokens, env.output_tokens, env.reasoning_tokens, env.cached_tokens,
                env.cost_usd, env.cost_source, coverage_tag,
            )

    start = time.monotonic()
    tasks = [asyncio.create_task(_run_one(i, b)) for i, b in enumerate(batch_list)]
    await asyncio.gather(*tasks)
    elapsed = time.monotonic() - start

    final: list[BatchResultEnvelope] = []
    for i, r in enumerate(results):
        if r is None:
            r = BatchResultEnvelope(
                batch_id=i + 1, model_id=model_id,
                input_block_ids=[b["block_id"] for b in batch_list[i]],
                is_error=True, error_type="provider",
                error_message="Task did not complete",
            )
        final.append(r)

    logger.info("[%s] All %d batches done in %.1fs", run_id, total, elapsed)
    return final, elapsed


# ─── Phase A ──────────────────────────────────────────────────────────────────

async def run_phase_a(
    subset_blocks: list[dict],
    project_dir: Path,
    project_info: dict,
    exp_dir: Path,
    parallelism: int,
    *,
    strict_schema: bool,
    response_healing: bool,
    require_parameters: bool,
    provider_data_collection: str | None,
    dry_run: bool,
    flash_only: str | None = None,
) -> tuple[RunMetrics, RunMetrics, str, str, list[BatchResultEnvelope]]:
    """Returns (flash_m, pro_m, mainline, fallback, flash_results)."""
    system_prompt = build_system_prompt(project_info, len(subset_blocks))
    page_contexts = build_page_contexts(project_dir)
    single_batches = [[b] for b in subset_blocks]
    total = len(single_batches)

    flash_results: list[BatchResultEnvelope] = []
    flash_m: RunMetrics | None = None
    pro_m: RunMetrics | None = None

    models_to_run = [MODEL_FLASH, MODEL_PRO] if not flash_only else [flash_only]

    for model_id in models_to_run:
        run_id = f"phA_{model_id.split('/')[-1].split('-')[1]}"
        logger.info("=== Phase A: %s (%d single-block requests) ===", model_id, total)

        results, elapsed = await run_batches_async(
            single_batches,
            project_dir, project_info,
            system_prompt, page_contexts, subset_blocks,
            model_id, parallelism, run_id,
            strict_schema=strict_schema,
            response_healing=response_healing,
            require_parameters=require_parameters,
            provider_data_collection=provider_data_collection,
            dry_run=dry_run,
        )

        m = compute_metrics(
            results, subset_blocks,
            run_id=run_id, model_id=model_id,
            batch_profile="single", parallelism=parallelism,
            mode="single_block", elapsed_s=elapsed,
            strict_schema_enabled=strict_schema,
            response_healing_enabled=response_healing,
            require_parameters_enabled=require_parameters,
            dry_run=dry_run,
        )

        _save_json(exp_dir / f"phase_a_{model_id.replace('/', '_')}_metrics.json", asdict(m))
        logger.info(
            "[%s] coverage=%.1f%% findings=%d kv_median=%.1f cost=$%.4f elapsed=%.1fs",
            model_id, m.coverage_pct, m.total_findings, m.median_key_values,
            m.total_cost_usd, m.elapsed_s,
        )

        if model_id == MODEL_FLASH:
            flash_m = m
            flash_results = results
        elif model_id == MODEL_PRO:
            pro_m = m

    # If flash_only, pad pro_m with empty metrics so comparator doesn't blow up
    if flash_m is None:
        flash_m = RunMetrics(model_id=MODEL_FLASH)
    if pro_m is None:
        pro_m = RunMetrics(model_id=MODEL_PRO)

    mainline, fallback, gate_md = apply_phase_a_gate(flash_m, pro_m)

    _save_json(exp_dir / "model_quality_subset_summary.json", {
        "flash": asdict(flash_m),
        "pro": asdict(pro_m),
    })
    _save_csv(exp_dir / "model_quality_subset_summary.csv", [asdict(flash_m), asdict(pro_m)])
    _save_json(exp_dir / "subset_side_by_side_models.json", {
        "flash": asdict(flash_m),
        "pro": asdict(pro_m),
        "gate_result": {"mainline": mainline, "fallback": fallback},
    })
    (exp_dir / "subset_side_by_side_models.md").write_text(
        _build_phase_a_sbs_md(flash_m, pro_m), encoding="utf-8"
    )
    (exp_dir / "model_quality_winner.md").write_text(
        f"# Phase A Quality Gate Result\n\n{gate_md}\n\n## Recommendation\n"
        f"- **Mainline**: {mainline}\n"
        f"- **Fallback/Escalation**: {fallback}\n",
        encoding="utf-8",
    )

    # Subset-level side-by-side with per-block divergence (for report 5)
    (exp_dir / "model_quality_subset_summary.md").write_text(
        _build_model_summary_md(flash_m, pro_m, mainline), encoding="utf-8"
    )

    logger.info("Phase A complete. Mainline: %s | Fallback: %s", mainline, fallback)
    return flash_m, pro_m, mainline, fallback, flash_results


def _build_model_summary_md(flash_m: RunMetrics, pro_m: RunMetrics, mainline: str) -> str:
    lines = [
        "# Phase A — Model Quality Summary (OpenRouter single-block subset)\n",
        f"Chosen mainline: **{mainline}**\n",
        "| Metric | google/gemini-2.5-flash | google/gemini-3.1-pro-preview |",
        "|--------|-------------------------|-------------------------------|",
        f"| Coverage | {flash_m.coverage_pct}% | {pro_m.coverage_pct}% |",
        f"| Missing | {flash_m.missing_count} | {pro_m.missing_count} |",
        f"| Duplicates | {flash_m.duplicate_count} | {pro_m.duplicate_count} |",
        f"| Extra | {flash_m.extra_count} | {pro_m.extra_count} |",
        f"| Inferred block_id | {flash_m.inferred_block_id_count} | {pro_m.inferred_block_id_count} |",
        f"| Unreadable | {flash_m.unreadable_count} | {pro_m.unreadable_count} |",
        f"| Blocks with findings | {flash_m.blocks_with_findings} | {pro_m.blocks_with_findings} |",
        f"| Total findings | {flash_m.total_findings} | {pro_m.total_findings} |",
        f"| Findings/100 | {flash_m.findings_per_100_blocks} | {pro_m.findings_per_100_blocks} |",
        f"| Median KV | {flash_m.median_key_values} | {pro_m.median_key_values} |",
        f"| Total KV | {flash_m.total_key_values} | {pro_m.total_key_values} |",
        f"| Prompt tokens | {flash_m.total_prompt_tokens} | {pro_m.total_prompt_tokens} |",
        f"| Output tokens | {flash_m.total_output_tokens} | {pro_m.total_output_tokens} |",
        f"| Reasoning tokens | {flash_m.total_reasoning_tokens} | {pro_m.total_reasoning_tokens} |",
        f"| Cached tokens | {flash_m.total_cached_tokens} | {pro_m.total_cached_tokens} |",
        f"| Total cost USD | ${flash_m.total_cost_usd:.4f} | ${pro_m.total_cost_usd:.4f} |",
        f"| Cost/valid block | ${flash_m.cost_per_valid_block:.5f} | ${pro_m.cost_per_valid_block:.5f} |",
        f"| Cost/finding | ${flash_m.cost_per_finding:.5f} | ${pro_m.cost_per_finding:.5f} |",
        f"| Elapsed (s) | {flash_m.elapsed_s:.1f} | {pro_m.elapsed_s:.1f} |",
        f"| Avg batch dur (s) | {flash_m.avg_batch_duration_s} | {pro_m.avg_batch_duration_s} |",
        f"| P95 batch dur (s) | {flash_m.p95_batch_duration_s} | {pro_m.p95_batch_duration_s} |",
        f"| Retry count | {flash_m.retry_count} | {pro_m.retry_count} |",
        f"| Provider errors | {flash_m.provider_errors} | {pro_m.provider_errors} |",
        f"| Cost source actual | {flash_m.cost_sources_actual}/{flash_m.total_batches} | {pro_m.cost_sources_actual}/{pro_m.total_batches} |",
    ]
    return "\n".join(lines) + "\n"


def _build_phase_a_sbs_md(flash_m: RunMetrics, pro_m: RunMetrics) -> str:
    rows = [
        ("Model", flash_m.model_id, pro_m.model_id),
        ("Coverage %", f"{flash_m.coverage_pct:.1f}%", f"{pro_m.coverage_pct:.1f}%"),
        ("Missing blocks", str(flash_m.missing_count), str(pro_m.missing_count)),
        ("Duplicate blocks", str(flash_m.duplicate_count), str(pro_m.duplicate_count)),
        ("Extra blocks", str(flash_m.extra_count), str(pro_m.extra_count)),
        ("Inferred block_ids", str(flash_m.inferred_block_id_count), str(pro_m.inferred_block_id_count)),
        ("Unreadable blocks", str(flash_m.unreadable_count), str(pro_m.unreadable_count)),
        ("Empty summary", str(flash_m.empty_summary_count), str(pro_m.empty_summary_count)),
        ("Blocks w/ findings", str(flash_m.blocks_with_findings), str(pro_m.blocks_with_findings)),
        ("Total findings", str(flash_m.total_findings), str(pro_m.total_findings)),
        ("Findings/100 blocks", f"{flash_m.findings_per_100_blocks:.1f}", f"{pro_m.findings_per_100_blocks:.1f}"),
        ("Median KV count", f"{flash_m.median_key_values:.1f}", f"{pro_m.median_key_values:.1f}"),
        ("Total KV count", str(flash_m.total_key_values), str(pro_m.total_key_values)),
        ("Prompt tokens", str(flash_m.total_prompt_tokens), str(pro_m.total_prompt_tokens)),
        ("Output tokens", str(flash_m.total_output_tokens), str(pro_m.total_output_tokens)),
        ("Reasoning tokens", str(flash_m.total_reasoning_tokens), str(pro_m.total_reasoning_tokens)),
        ("Cached tokens", str(flash_m.total_cached_tokens), str(pro_m.total_cached_tokens)),
        ("Total cost USD", f"${flash_m.total_cost_usd:.4f}", f"${pro_m.total_cost_usd:.4f}"),
        ("Cost/valid block", f"${flash_m.cost_per_valid_block:.5f}", f"${pro_m.cost_per_valid_block:.5f}"),
        ("Cost/finding", f"${flash_m.cost_per_finding:.5f}", f"${pro_m.cost_per_finding:.5f}"),
        ("Elapsed (s)", f"{flash_m.elapsed_s:.1f}", f"{pro_m.elapsed_s:.1f}"),
        ("Avg batch dur (s)", f"{flash_m.avg_batch_duration_s:.2f}", f"{pro_m.avg_batch_duration_s:.2f}"),
        ("Retry count", str(flash_m.retry_count), str(pro_m.retry_count)),
    ]
    lines = ["# Phase A — Model Quality Side-by-Side (OpenRouter)\n",
             "| Metric | 2.5 Flash | 3.1 Pro |",
             "|--------|-----------|---------|"]
    for name, flash_val, pro_val in rows:
        lines.append(f"| {name} | {flash_val} | {pro_val} |")
    return "\n".join(lines) + "\n"


# ─── Phase B ──────────────────────────────────────────────────────────────────

async def run_phase_b(
    all_blocks: list[dict],
    project_dir: Path,
    project_info: dict,
    exp_dir: Path,
    mainline_model: str,
    parallelism: int,
    *,
    strict_schema: bool,
    response_healing: bool,
    require_parameters: bool,
    provider_data_collection: str | None,
    byte_cap_kb: int,
    dry_run: bool,
    only_profile: str | None = None,
) -> tuple[str, str, list[RunMetrics]]:
    system_prompt = build_system_prompt(project_info, len(all_blocks))
    page_contexts = build_page_contexts(project_dir)

    metrics_list: list[RunMetrics] = []
    profiles = [only_profile] if only_profile else list(BATCH_PROFILES.keys())

    for profile_name in profiles:
        risk_targets = BATCH_PROFILES[profile_name]
        batches = pack_blocks(all_blocks, risk_targets)
        batches = apply_byte_cap_split(batches, byte_cap_kb=byte_cap_kb)
        run_id = f"phB_{profile_name}"
        logger.info("=== Phase B: profile=%s batches=%d ===", profile_name, len(batches))

        results, elapsed = await run_batches_async(
            batches,
            project_dir, project_info,
            system_prompt, page_contexts, all_blocks,
            mainline_model, parallelism, run_id,
            strict_schema=strict_schema,
            response_healing=response_healing,
            require_parameters=require_parameters,
            provider_data_collection=provider_data_collection,
            dry_run=dry_run,
        )
        m = compute_metrics(
            results, all_blocks,
            run_id=run_id, model_id=mainline_model,
            batch_profile=profile_name, parallelism=parallelism,
            mode="batch", elapsed_s=elapsed,
            strict_schema_enabled=strict_schema,
            response_healing_enabled=response_healing,
            require_parameters_enabled=require_parameters,
            dry_run=dry_run,
        )
        metrics_list.append(m)
        _save_json(exp_dir / f"phase_b_{profile_name}_metrics.json", asdict(m))
        logger.info(
            "[B/%s] coverage=%.1f%% findings=%d cost=$%.4f elapsed=%.1fs batches=%d",
            profile_name, m.coverage_pct, m.total_findings,
            m.total_cost_usd, m.elapsed_s, m.total_batches,
        )

    winner, runner_up = select_batch_profile_winner(metrics_list) if metrics_list else (profiles[0], profiles[0])
    logger.info("Phase B winner: %s | runner-up: %s", winner, runner_up)

    _save_json(exp_dir / "batch_profile_summary.json", [asdict(m) for m in metrics_list])
    _save_csv(exp_dir / "batch_profile_summary.csv", [asdict(m) for m in metrics_list])
    (exp_dir / "batch_profile_summary.md").write_text(
        _build_batch_profile_md(metrics_list, winner, runner_up),
        encoding="utf-8",
    )

    return winner, runner_up, metrics_list


def _build_batch_profile_md(metrics: list[RunMetrics], winner: str, runner_up: str) -> str:
    lines = [
        "# Phase B — Batch Profile Comparison (OpenRouter)\n",
        f"Winner: **{winner}** | Runner-up: **{runner_up}**\n",
        "| Profile | Coverage | Batches | Avg Size | Max Size | Avg KB | Findings | Cost USD | Cost/block | Elapsed (s) |",
        "|---------|----------|---------|----------|----------|--------|----------|----------|------------|-------------|",
    ]
    for m in metrics:
        marker = " ★" if m.batch_profile == winner else (" ●" if m.batch_profile == runner_up else "")
        lines.append(
            f"| {m.batch_profile}{marker} | {m.coverage_pct:.1f}% | {m.total_batches} "
            f"| {m.avg_batch_size:.1f} | {m.max_batch_size} | {m.avg_batch_kb:.0f} "
            f"| {m.total_findings} | ${m.total_cost_usd:.4f} | ${m.cost_per_valid_block:.5f} "
            f"| {m.elapsed_s:.1f} |"
        )
    return "\n".join(lines) + "\n"


# ─── Phase C (parallelism) ────────────────────────────────────────────────────

async def run_phase_c(
    all_blocks: list[dict],
    project_dir: Path,
    project_info: dict,
    exp_dir: Path,
    mainline_model: str,
    top_profiles: list[str],
    *,
    strict_schema: bool,
    response_healing: bool,
    require_parameters: bool,
    provider_data_collection: str | None,
    byte_cap_kb: int,
    dry_run: bool,
    parallelism_options: tuple[int, ...] = (2, 3, 4),
) -> tuple[int, str, list[RunMetrics]]:
    """Returns (winner_parallelism, winner_profile, metrics_list)."""
    system_prompt = build_system_prompt(project_info, len(all_blocks))
    page_contexts = build_page_contexts(project_dir)

    metrics_list: list[RunMetrics] = []
    unique_profiles = list(dict.fromkeys(top_profiles))  # dedup, preserve order

    for profile_name in unique_profiles:
        risk_targets = BATCH_PROFILES[profile_name]
        batches = pack_blocks(all_blocks, risk_targets)
        batches = apply_byte_cap_split(batches, byte_cap_kb=byte_cap_kb)

        for para in parallelism_options:
            run_id = f"phC_{profile_name}_p{para}"
            logger.info("=== Phase C: profile=%s parallelism=%d ===", profile_name, para)

            results, elapsed = await run_batches_async(
                batches,
                project_dir, project_info,
                system_prompt, page_contexts, all_blocks,
                mainline_model, para, run_id,
                strict_schema=strict_schema,
                response_healing=response_healing,
                require_parameters=require_parameters,
                provider_data_collection=provider_data_collection,
                dry_run=dry_run,
            )
            m = compute_metrics(
                results, all_blocks,
                run_id=run_id, model_id=mainline_model,
                batch_profile=profile_name, parallelism=para,
                mode="batch", elapsed_s=elapsed,
                strict_schema_enabled=strict_schema,
                response_healing_enabled=response_healing,
                require_parameters_enabled=require_parameters,
                dry_run=dry_run,
            )
            metrics_list.append(m)
            _save_json(exp_dir / f"phase_c_{profile_name}_p{para}_metrics.json", asdict(m))
            logger.info(
                "[C/%s/p%d] coverage=%.1f%% elapsed=%.1fs retries=%d",
                profile_name, para, m.coverage_pct, m.elapsed_s, m.retry_count,
            )

    winner_para, winner_prof = select_parallelism_winner(metrics_list) if metrics_list else (3, unique_profiles[0])

    _save_json(exp_dir / "parallelism_summary.json", [asdict(m) for m in metrics_list])
    _save_csv(exp_dir / "parallelism_summary.csv", [asdict(m) for m in metrics_list])
    (exp_dir / "parallelism_summary.md").write_text(
        _build_parallelism_md(metrics_list, winner_para, winner_prof),
        encoding="utf-8",
    )

    return winner_para, winner_prof, metrics_list


def _build_parallelism_md(metrics: list[RunMetrics], winner_para: int, winner_prof: str) -> str:
    lines = [
        "# Phase C — Parallelism Comparison (OpenRouter)\n",
        f"Winner: **profile={winner_prof}, parallelism={winner_para}**\n",
        "| Profile | Parallelism | Coverage | Failed | Provider err | Retries | Elapsed (s) | Cost USD | Cost/block |",
        "|---------|-------------|----------|--------|--------------|---------|-------------|----------|------------|",
    ]
    for m in metrics:
        marker = " ★" if (m.parallelism == winner_para and m.batch_profile == winner_prof) else ""
        lines.append(
            f"| {m.batch_profile} | {m.parallelism}{marker} | {m.coverage_pct:.1f}% "
            f"| {m.failed_batches} | {m.provider_errors} | {m.retry_count} "
            f"| {m.elapsed_s:.1f} | ${m.total_cost_usd:.4f} | ${m.cost_per_valid_block:.5f} |"
        )
    return "\n".join(lines) + "\n"


# ─── Fallback sample ─────────────────────────────────────────────────────────

async def run_fallback_sample(
    all_blocks: list[dict],
    project_dir: Path,
    project_info: dict,
    exp_dir: Path,
    flash_results: list[BatchResultEnvelope],
    *,
    strict_schema: bool,
    response_healing: bool,
    require_parameters: bool,
    provider_data_collection: str | None,
    parallelism: int,
    dry_run: bool,
) -> RunMetrics | None:
    # Select weakest: unreadable + empty KV + no findings, + error blocks
    weak_ids: list[str] = []
    for res in flash_results:
        if res.is_error:
            weak_ids.extend(res.input_block_ids)
            continue
        if not res.parsed_data:
            weak_ids.extend(res.input_block_ids)
            continue
        for a in res.parsed_data.get("block_analyses", []):
            if not isinstance(a, dict):
                continue
            score = (
                int(bool(a.get("unreadable_text"))) * 3
                + int(not a.get("key_values_read")) * 2
                + int(not a.get("findings"))
            )
            if score >= 2:
                weak_ids.append(a.get("block_id", ""))

    seen: set[str] = set()
    unique_weak = []
    for bid in weak_ids:
        if bid and bid not in seen:
            seen.add(bid)
            unique_weak.append(bid)
    sample_ids = unique_weak[:FALLBACK_SAMPLE_SIZE]

    if len(sample_ids) < FALLBACK_SAMPLE_SIZE:
        heavy_blocks = sorted(
            [b for b in all_blocks if b["block_id"] not in set(sample_ids)],
            key=lambda b: -float(b.get("size_kb", 0)),
        )
        for b in heavy_blocks:
            if len(sample_ids) >= FALLBACK_SAMPLE_SIZE:
                break
            sample_ids.append(b["block_id"])

    id_set = set(sample_ids)
    sample_blocks = [b for b in all_blocks if b["block_id"] in id_set]
    if not sample_blocks:
        logger.info("Fallback sample: no blocks selected (skipping)")
        return None

    logger.info("Fallback sample: %d blocks on %s", len(sample_blocks), MODEL_PRO)

    system_prompt = build_system_prompt(project_info, len(sample_blocks))
    page_contexts = build_page_contexts(project_dir)
    single_batches = [[b] for b in sample_blocks]

    results, elapsed = await run_batches_async(
        single_batches,
        project_dir, project_info,
        system_prompt, page_contexts, sample_blocks,
        MODEL_PRO, parallelism, "fallback_sample",
        strict_schema=strict_schema,
        response_healing=response_healing,
        require_parameters=require_parameters,
        provider_data_collection=provider_data_collection,
        dry_run=dry_run,
    )

    m = compute_metrics(
        results, sample_blocks,
        run_id="fallback_sample", model_id=MODEL_PRO,
        batch_profile="single", parallelism=parallelism,
        mode="single_block", elapsed_s=elapsed,
        strict_schema_enabled=strict_schema,
        response_healing_enabled=response_healing,
        require_parameters_enabled=require_parameters,
        dry_run=dry_run,
    )
    _save_json(exp_dir / "fallback_sample_summary.json", asdict(m))
    (exp_dir / "fallback_sample_summary.md").write_text(
        f"# Fallback Sample — Pro Escalation Check\n\n"
        f"Blocks tested: {len(sample_blocks)} (weakest Flash results)\n\n"
        f"| Metric | Value |\n|--------|-------|\n"
        f"| Coverage | {m.coverage_pct:.1f}% |\n"
        f"| Total findings | {m.total_findings} |\n"
        f"| Unreadable | {m.unreadable_count} |\n"
        f"| Empty KV | {m.empty_key_values_count} |\n"
        f"| Total cost USD | ${m.total_cost_usd:.4f} |\n"
        f"| Cost/valid block | ${m.cost_per_valid_block:.5f} |\n"
        f"| Elapsed (s) | {m.elapsed_s:.1f} |\n",
        encoding="utf-8",
    )
    return m


# ─── Final recommendation ─────────────────────────────────────────────────────

def build_winner_recommendation(
    *,
    mainline: str,
    fallback: str,
    winner_profile: str,
    winner_parallelism: int,
    flash_m: RunMetrics,
    pro_m: RunMetrics,
    batch_metrics: list[RunMetrics],
    para_metrics: list[RunMetrics],
    fallback_m: RunMetrics | None,
    dry_run: bool,
) -> str:
    note = "\n> **Note: dry-run mode — no actual API calls were made.**\n" if dry_run else ""

    flash_cheaper = ""
    if pro_m.cost_per_valid_block:
        pct = (1 - flash_m.cost_per_valid_block / pro_m.cost_per_valid_block) * 100
        flash_cheaper = f"~{round(pct):.0f}% cheaper per valid block vs Pro"
    else:
        flash_cheaper = "cheaper"

    escalation_rec = "Not applicable (Pro is mainline)"
    if mainline == MODEL_FLASH and fallback_m is not None:
        if fallback_m.coverage_pct == 100.0 and fallback_m.total_findings > 0:
            escalation_rec = (
                f"RECOMMENDED for weakest blocks. Pro recovered {fallback_m.total_findings} findings "
                f"on {fallback_m.total_input_blocks} weak blocks for ${fallback_m.total_cost_usd:.4f} "
                f"(${fallback_m.cost_per_valid_block:.5f}/block)."
            )
        else:
            escalation_rec = (
                f"NOT justified (stats: coverage={fallback_m.coverage_pct}%, "
                f"findings={fallback_m.total_findings})"
            )

    return f"""# Winner Recommendation (OpenRouter stage 02)
{note}
## Final Answers

| Question | Answer |
|----------|--------|
| Mainline model | **{mainline}** |
| Fallback/Escalation model | **{fallback}** |
| Batch profile | **{winner_profile}** |
| Parallelism | **{winner_parallelism}** |
| Selective escalation on Pro | {escalation_rec} |

## Phase A Summary

| Model | Coverage | Findings | KV median | Cost/block |
|-------|----------|----------|-----------|------------|
| {flash_m.model_id} | {flash_m.coverage_pct:.1f}% | {flash_m.total_findings} | {flash_m.median_key_values:.1f} | ${flash_m.cost_per_valid_block:.5f} |
| {pro_m.model_id}   | {pro_m.coverage_pct:.1f}%   | {pro_m.total_findings}   | {pro_m.median_key_values:.1f}   | ${pro_m.cost_per_valid_block:.5f}   |

- **Flash** is {flash_cheaper}
- {'Flash quality within acceptable range of Pro.' if mainline == MODEL_FLASH else 'Flash did NOT meet quality gate — Pro recommended as mainline.'}

## Batch Profile Winner: {winner_profile}
{_batch_winner_justification(batch_metrics, winner_profile) if batch_metrics else 'Not tested'}

## Parallelism Winner: {winner_parallelism}
{_para_winner_justification(para_metrics, winner_parallelism) if para_metrics else 'Not tested'}

## Constraints / notes
- Production defaults (stage_models.json block_batch) **UNCHANGED**.
- Claude CLI path не затронут.
- OpenRouter strict schema, response-healing, provider.require_parameters=true всегда включены в exp runner.
- Direct Gemini API путь в этом эксперименте не использовался (гео-блокировка).
- Actual OpenRouter usage.cost приоритетнее локальной оценки; если usage.cost не пришёл — fallback на _MODEL_PRICES.
"""


def _batch_winner_justification(metrics: list[RunMetrics], winner: str) -> str:
    m = next((m for m in metrics if m.batch_profile == winner), None)
    if not m:
        return ""
    return (
        f"Profile **{winner}** wins: coverage={m.coverage_pct:.1f}%, batches={m.total_batches}, "
        f"cost/block=${m.cost_per_valid_block:.5f}, elapsed={m.elapsed_s:.1f}s"
    )


def _para_winner_justification(metrics: list[RunMetrics], winner_para: int) -> str:
    m = next((m for m in metrics if m.parallelism == winner_para), None)
    if not m:
        return ""
    return (
        f"Parallelism **{winner_para}** wins: coverage={m.coverage_pct:.1f}%, "
        f"elapsed={m.elapsed_s:.1f}s, retries={m.retry_count}"
    )


# ─── CLI ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="OpenRouter stage 02 experiment runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--pdf", required=True, help="Exact PDF filename (pilot project)")
    p.add_argument("--project-dir", help="Override: explicit project directory")
    p.add_argument("--dry-run", action="store_true", help="Validate config, no API calls")
    p.add_argument("--model", default=None,
                   help="Override mainline model for B/C phases (bypass Phase A gate)")
    p.add_argument("--subset-file", help="Existing fixed_subset_block_ids.json to reuse")
    p.add_argument("--single-block-subset", action="store_true",
                   help="Run Phase A only (1 block/request, both models)")
    p.add_argument("--batch-profile", choices=list(BATCH_PROFILES), default=None,
                   help="Run only this batch profile in Phase B")
    p.add_argument("--parallelism", type=int, default=3, choices=[1, 2, 3, 4, 5],
                   help="Default parallelism for Phase A and B")
    p.add_argument("--no-healing", action="store_true",
                   help="Disable OpenRouter response-healing plugin")
    p.add_argument("--no-schema", action="store_true",
                   help="Disable strict JSON schema (fall back to json_object)")
    p.add_argument("--provider-data-collection",
                   choices=["allow", "deny"], default=None,
                   help="Set provider.data_collection (opt-in; not set silently)")
    p.add_argument("--byte-cap-kb", type=int, default=None,
                   help="Raw PNG byte cap per batch (KB). Default from config.")
    p.add_argument("--full-run", action="store_true", help="Run all phases")
    p.add_argument("--phase", choices=["a", "b", "c", "fallback", "all"], default="all")
    p.add_argument("--limit-blocks", type=int, default=None,
                   help="DEBUG ONLY — do not use for final decisions")
    p.add_argument("--no-fallback", action="store_true", help="Skip fallback Pro sample")
    return p.parse_args()


async def main_async(args: argparse.Namespace) -> None:
    from webapp.config import (
        OPENROUTER_API_KEY,
        OPENROUTER_STAGE02_RAW_BYTE_CAP_KB,
    )
    from webapp.services.project_service import resolve_project_by_pdf, ProjectByPdfError

    # ── Resolve project ───────────────────────────────────────────────────
    if args.project_dir:
        project_dir = Path(args.project_dir)
        project_id = project_dir.name
    else:
        try:
            project_id, project_dir = resolve_project_by_pdf(args.pdf)
        except ProjectByPdfError as e:
            logger.error("Cannot resolve project: %s", e)
            sys.exit(1)

    logger.info("Project dir: %s", project_dir)

    project_info_path = project_dir / "project_info.json"
    if not project_info_path.exists():
        logger.error("project_info.json not found: %s", project_info_path)
        sys.exit(1)
    project_info = json.loads(project_info_path.read_text(encoding="utf-8"))

    # ── Resolve API key ───────────────────────────────────────────────────
    if not OPENROUTER_API_KEY and not args.dry_run:
        logger.error("OPENROUTER_API_KEY not set — set in .env or use --dry-run")
        sys.exit(1)
    if not OPENROUTER_API_KEY:
        logger.warning("OPENROUTER_API_KEY not set — running in dry-run mode only")

    strict_schema = not args.no_schema
    response_healing = not args.no_healing
    require_parameters = True
    byte_cap_kb = args.byte_cap_kb or OPENROUTER_STAGE02_RAW_BYTE_CAP_KB
    phase = args.phase
    if args.single_block_subset:
        phase = "a"
    if args.full_run:
        phase = "all"

    # ── Load blocks ───────────────────────────────────────────────────────
    try:
        all_blocks = load_blocks_index(project_dir)
    except (FileNotFoundError, ValueError) as e:
        logger.error("Cannot load blocks: %s", e)
        sys.exit(1)

    if args.limit_blocks:
        logger.warning(
            "--limit-blocks=%d — DEBUG ONLY, do not use for final decisions",
            args.limit_blocks,
        )
        all_blocks = all_blocks[:args.limit_blocks]

    # ── Experiment directory ──────────────────────────────────────────────
    ts = _ts()
    exp_dir = project_dir / "_experiments" / "gemini_openrouter_stage02" / ts
    exp_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Experiment dir: %s", exp_dir)

    # ── Subset ────────────────────────────────────────────────────────────
    subset_file = Path(args.subset_file) if args.subset_file else None
    if subset_file is None:
        prev_dir = project_dir / "_experiments"
        if prev_dir.exists():
            candidates = sorted(
                prev_dir.rglob("fixed_subset_block_ids.json"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            if candidates:
                subset_file = candidates[0]
                logger.info("Auto-detected previous subset: %s", subset_file)

    subset_ids, subset_blocks = create_or_load_subset(
        all_blocks, SUBSET_SIZE, SUBSET_SEED, subset_file, exp_dir
    )

    # ── Manifest ──────────────────────────────────────────────────────────
    manifest = {
        "timestamp": ts,
        "pdf": args.pdf,
        "project_id": project_id,
        "project_dir": str(project_dir),
        "total_blocks": len(all_blocks),
        "subset_size": len(subset_ids),
        "subset_source": str(subset_file) if subset_file else "created",
        "dry_run": args.dry_run,
        "phase": phase,
        "api_key_present": bool(OPENROUTER_API_KEY),
        "parallelism": args.parallelism,
        "strict_schema": strict_schema,
        "response_healing": response_healing,
        "require_parameters": require_parameters,
        "provider_data_collection": args.provider_data_collection,
        "byte_cap_kb": byte_cap_kb,
        "models": [MODEL_FLASH, MODEL_PRO],
    }
    _save_json(exp_dir / "manifest.json", manifest)

    logger.info(
        "Setup: total_blocks=%d subset=%d dry_run=%s strict=%s healing=%s byte_cap=%dKB",
        len(all_blocks), len(subset_blocks), args.dry_run, strict_schema, response_healing, byte_cap_kb,
    )

    if args.dry_run:
        _print_dry_run_summary(manifest, all_blocks, subset_blocks, BATCH_PROFILES)
        if not OPENROUTER_API_KEY:
            logger.info("Stopping after dry-run validation (no OPENROUTER_API_KEY).")
            return

    # ── Phase A ───────────────────────────────────────────────────────────
    flash_m = RunMetrics(model_id=MODEL_FLASH)
    pro_m = RunMetrics(model_id=MODEL_PRO)
    mainline = args.model or MODEL_FLASH
    fallback = MODEL_PRO
    flash_results: list[BatchResultEnvelope] = []

    if phase in ("a", "all"):
        flash_m, pro_m, mainline, fallback, flash_results = await run_phase_a(
            subset_blocks, project_dir, project_info, exp_dir,
            parallelism=args.parallelism,
            strict_schema=strict_schema,
            response_healing=response_healing,
            require_parameters=require_parameters,
            provider_data_collection=args.provider_data_collection,
            dry_run=args.dry_run,
        )

    if args.model:
        mainline = args.model
        logger.info("Model override applied: %s", mainline)

    # ── Phase B ───────────────────────────────────────────────────────────
    winner_profile = "b10"
    runner_up_profile = "b10"
    batch_metrics: list[RunMetrics] = []
    if phase in ("b", "all"):
        winner_profile, runner_up_profile, batch_metrics = await run_phase_b(
            all_blocks, project_dir, project_info, exp_dir,
            mainline, args.parallelism,
            strict_schema=strict_schema,
            response_healing=response_healing,
            require_parameters=require_parameters,
            provider_data_collection=args.provider_data_collection,
            byte_cap_kb=byte_cap_kb,
            dry_run=args.dry_run,
            only_profile=args.batch_profile,
        )
    elif args.batch_profile:
        winner_profile = args.batch_profile
        runner_up_profile = args.batch_profile

    # ── Phase C ───────────────────────────────────────────────────────────
    winner_parallelism = args.parallelism
    para_metrics: list[RunMetrics] = []
    if phase in ("c", "all"):
        top_profiles = [winner_profile, runner_up_profile]
        winner_parallelism, winner_profile_c, para_metrics = await run_phase_c(
            all_blocks, project_dir, project_info, exp_dir,
            mainline, top_profiles,
            strict_schema=strict_schema,
            response_healing=response_healing,
            require_parameters=require_parameters,
            provider_data_collection=args.provider_data_collection,
            byte_cap_kb=byte_cap_kb,
            dry_run=args.dry_run,
        )
        if winner_profile_c:
            winner_profile = winner_profile_c

    # ── Fallback sample ───────────────────────────────────────────────────
    fallback_m: RunMetrics | None = None
    if phase in ("fallback", "all") and not args.no_fallback and mainline == MODEL_FLASH and flash_results:
        fallback_m = await run_fallback_sample(
            all_blocks, project_dir, project_info, exp_dir,
            flash_results,
            strict_schema=strict_schema,
            response_healing=response_healing,
            require_parameters=require_parameters,
            provider_data_collection=args.provider_data_collection,
            parallelism=args.parallelism,
            dry_run=args.dry_run,
        )

    # ── Winner recommendation ─────────────────────────────────────────────
    rec_md = build_winner_recommendation(
        mainline=mainline,
        fallback=fallback,
        winner_profile=winner_profile,
        winner_parallelism=winner_parallelism,
        flash_m=flash_m,
        pro_m=pro_m,
        batch_metrics=batch_metrics,
        para_metrics=para_metrics,
        fallback_m=fallback_m,
        dry_run=args.dry_run,
    )
    (exp_dir / "winner_recommendation.md").write_text(rec_md, encoding="utf-8")
    logger.info("Saved winner_recommendation.md")

    print("\n" + "=" * 70)
    print("EXPERIMENT COMPLETE")
    print(f"Artifacts: {exp_dir}")
    print("=" * 70)
    print(rec_md[:1400])
    print("=" * 70)


def _print_dry_run_summary(
    manifest: dict,
    all_blocks: list[dict],
    subset_blocks: list[dict],
    profiles: dict,
) -> None:
    risk_counts: dict[str, int] = {"heavy": 0, "normal": 0, "light": 0}
    for b in all_blocks:
        risk_counts[classify_risk(b)] += 1

    print("\n" + "=" * 70)
    print("DRY-RUN SUMMARY — Configuration Validated")
    print("=" * 70)
    print(f"  Project: {manifest['project_dir']}")
    print(f"  PDF: {manifest['pdf']}")
    print(f"  OpenRouter key: {'PRESENT' if manifest['api_key_present'] else 'MISSING'}")
    print(f"  Strict schema: {manifest['strict_schema']}")
    print(f"  Response healing: {manifest['response_healing']}")
    print(f"  require_parameters: {manifest['require_parameters']}")
    print(f"  byte_cap_kb: {manifest['byte_cap_kb']}")
    print(f"  Total blocks: {manifest['total_blocks']}")
    print(f"    heavy={risk_counts['heavy']} normal={risk_counts['normal']} light={risk_counts['light']}")
    print(f"  Subset: {manifest['subset_size']} blocks (from {manifest['subset_source']})")
    print()
    for profile_name, targets in profiles.items():
        batches = pack_blocks(all_blocks, targets)
        batches = apply_byte_cap_split(batches, byte_cap_kb=manifest["byte_cap_kb"])
        sizes = [len(b) for b in batches]
        kbs = [sum(float(x.get("size_kb", 0)) for x in b) for b in batches]
        print(
            f"  {profile_name}: {len(batches)} batches | "
            f"sizes avg={sum(sizes)/len(sizes):.1f} min={min(sizes)} max={max(sizes)} | "
            f"kb avg={sum(kbs)/len(kbs):.0f} max={max(kbs):.0f}"
        )
    print("=" * 70 + "\n")


def main() -> None:
    args = parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
