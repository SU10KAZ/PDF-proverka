"""
Experiment runner: Direct Gemini Developer API for stage 02 block_batch.

Phase A — model quality comparison (single-block subset, flash vs pro)
Phase B1 — batch profile economics on full document (chosen mainline)
Phase B2 — parallelism economics (top-2 profiles)
Phase C — optional Flex smoke test
Fallback sample — optional 20-block Pro escalation check

Usage:
    python scripts/run_gemini_direct_stage02_experiment.py \\
        --pdf "13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf" \\
        --dry-run

    python scripts/run_gemini_direct_stage02_experiment.py \\
        --pdf "13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf" \\
        --phase a

    python scripts/run_gemini_direct_stage02_experiment.py \\
        --pdf "13АВ-РД-КЖ5.17-23.1-К2 (1) (1).pdf" \\
        --phase all --model gemini-2.5-flash --parallelism 3

See --help for full list of options.
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
)
logger = logging.getLogger("gemini_exp")

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


# ─── Per-run metrics ──────────────────────────────────────────────────────────

@dataclass
class RunMetrics:
    run_id: str = ""
    model_id: str = ""
    tier: str = "standard"
    batch_profile: str = "single"
    parallelism: int = 1
    mode: str = "single_block"          # single_block | batch

    total_input_blocks: int = 0
    total_batches: int = 0
    completed_batches: int = 0
    failed_batches: int = 0

    # Risk class distribution
    risk_heavy: int = 0
    risk_normal: int = 0
    risk_light: int = 0

    # Coverage
    coverage_pct: float = 0.0
    missing_count: int = 0
    duplicate_count: int = 0
    extra_count: int = 0
    inferred_block_id_count: int = 0

    # Quality
    unreadable_count: int = 0
    empty_summary_count: int = 0
    empty_key_values_count: int = 0
    total_key_values: int = 0
    median_key_values: float = 0.0
    blocks_with_findings: int = 0
    total_findings: int = 0
    findings_per_100_blocks: float = 0.0

    # Timing
    elapsed_s: float = 0.0
    avg_batch_duration_s: float = 0.0
    median_batch_duration_s: float = 0.0
    p95_batch_duration_s: float = 0.0

    # Token telemetry
    total_prompt_tokens: int = 0
    total_output_tokens: int = 0
    total_thought_tokens: int = 0
    total_cached_tokens: int = 0
    avg_prompt_tokens: float = 0.0
    median_prompt_tokens: float = 0.0
    avg_predicted_tokens: float = 0.0

    # Batching stats (Phase B only)
    avg_batch_size: float = 0.0
    median_batch_size: float = 0.0
    max_batch_size: int = 0
    avg_batch_kb: float = 0.0

    # Cost
    total_cost_usd: float = 0.0
    cost_per_valid_block: float = 0.0
    cost_per_finding: float = 0.0

    # Errors
    retry_count: int = 0
    provider_errors: int = 0
    schema_errors: int = 0
    timeout_errors: int = 0
    cache_hits: int = 0

    # Flags
    cache_enabled: bool = True
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
    """Load full blocks list from _output/blocks/index.json."""
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
    """Return (block_ids, block_dicts) for the fixed subset.

    If subset_file is provided and valid, reuse it.
    Otherwise create a stratified random subset and save it.
    """
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

    # Stratified by page, round-robin
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

    out_ids = exp_dir / "fixed_subset_block_ids.json"
    out_manifest = exp_dir / "fixed_subset_manifest.json"
    _save_json(out_ids, ids)
    _save_json(out_manifest, chosen)
    logger.info("Created fixed subset: %d blocks → %s", len(ids), out_ids)
    return ids, chosen


# ─── Risk classification + batch packing ─────────────────────────────────────

def classify_risk(block: dict) -> str:
    """Classify block as heavy/normal/light (mirrors blocks.py _classify_block_risk)."""
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


HARD_CAP = 12
SOLO_THRESHOLD_KB = 3000


def pack_blocks(
    blocks: list[dict],
    risk_targets: dict[str, dict[str, int]],
    hard_cap: int = HARD_CAP,
    solo_kb: int = SOLO_THRESHOLD_KB,
) -> list[list[dict]]:
    """Pack blocks into batches using risk-aware strategy (mirrors blocks.py logic)."""
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


# ─── Message building (simplified for experiment caching) ─────────────────────

def build_experiment_messages(
    blocks: list[dict],
    project_dir: Path,
    project_info: dict,
    project_id: str,
    batch_id: int,
    total_batches: int,
    base_system_prompt: str,
    page_contexts: dict[int, str],
) -> list[dict]:
    """Build OpenRouter-format messages for a batch of blocks.

    system: pre-built base system prompt (cacheable, same for all batches)
    user:   per-batch context (page context + interleaved PNG + block labels)
    """
    import base64

    blocks_dir = project_dir / "_output" / "blocks"

    block_labels = []
    for b in blocks:
        block_labels.append(
            f"block_id={b['block_id']}, page={b.get('page','?')}, "
            f"OCR_label={b.get('ocr_label','')}"
        )

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
        {"role": "system", "content": base_system_prompt},
        {"role": "user",   "content": user_parts},
    ]


def build_base_system_prompt(project_info: dict, total_blocks: int) -> str:
    """Build a cacheable base system prompt for all batches in an experiment run."""
    section = (project_info or {}).get("section", "KJ")
    return f"""You are an expert auditor of residential building project documentation (section: {section}).

## Your task
Analyze each provided image block (cropped drawing fragment) and return a JSON object with block_analyses array.

## CRITICAL RULES
1. Return EXACTLY one analysis object per block in the batch
2. Use EXACTLY the block_id value provided in the input — do NOT modify it
3. Extract ALL readable text: cable specs, breaker ratings, dimensions, pipe sizes, equipment tags
4. Identify audit findings with appropriate severity levels
5. Set unreadable_text=true and describe unreadable_details if any text is illegible

## Output schema
Return a JSON object with this structure:
{{
  "block_analyses": [
    {{
      "block_id": "<exact id from input>",
      "page": <integer PDF page number>,
      "sheet": "<sheet number from drawing stamp, or null>",
      "label": "<short drawing title>",
      "sheet_type": "<schema|plan|section|detail|table|other>",
      "unreadable_text": false,
      "unreadable_details": null,
      "summary": "<concise description of what this drawing shows>",
      "key_values_read": ["<all extracted values: cable specs, ratings, dimensions, etc.>"],
      "findings": [
        {{
          "id": "G-001",
          "severity": "<КРИТИЧЕСКОЕ|ЭКОНОМИЧЕСКОЕ|ЭКСПЛУАТАЦИОННОЕ|РЕКОМЕНДАТЕЛЬНОЕ|ПРОВЕРИТЬ ПО СМЕЖНЫМ>",
          "category": "<category>",
          "finding": "<detailed description>",
          "norm": "<normative reference>",
          "norm_quote": null,
          "block_evidence": "<what in the drawing supports this>",
          "value_found": "<specific problematic value>"
        }}
      ]
    }}
  ]
}}

## Severity levels
- КРИТИЧЕСКОЕ: Violates mandatory building codes (fire safety, structural, electrical)
- ЭКОНОМИЧЕСКОЕ: Causes extra costs or waste of materials
- ЭКСПЛУАТАЦИОННОЕ: Will cause maintenance problems during operation
- РЕКОМЕНДАТЕЛЬНОЕ: Minor non-conformity or typo
- ПРОВЕРИТЬ ПО СМЕЖНЫМ: Requires verification with another section

## Drawing types to focus on
- Single-line electrical diagrams: read ALL cable labels, breaker ratings, transformer ratings
- Floor plans: read equipment positioning, clearances, cable routing
- Installation details: read fixing methods, materials, dimensions
- Specifications/tables: extract all numeric values
- Section views: describe construction details

Return ONLY the JSON object. No markdown, no explanation.
Total blocks in this experiment: {total_blocks}
"""


def build_page_contexts(project_dir: Path) -> dict[int, str]:
    """Load page contexts from document_graph.json."""
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


# ─── Metrics computation ──────────────────────────────────────────────────────

def compute_metrics_from_results(
    results: list,           # list of GeminiBlockBatchResult
    all_input_blocks: list[dict],
    run_id: str,
    model_id: str,
    tier: str,
    batch_profile: str,
    parallelism: int,
    mode: str,
    elapsed_s: float,
    cache_enabled: bool,
    dry_run: bool = False,
) -> RunMetrics:
    from webapp.services.gemini_direct_runner import GeminiBlockBatchResult

    m = RunMetrics(
        run_id=run_id,
        model_id=model_id,
        tier=tier,
        batch_profile=batch_profile,
        parallelism=parallelism,
        mode=mode,
        elapsed_s=elapsed_s,
        cache_enabled=cache_enabled,
        dry_run=dry_run,
        total_input_blocks=len(all_input_blocks),
        total_batches=len(results),
    )

    # Risk distribution
    for b in all_input_blocks:
        r = classify_risk(b)
        if r == "heavy":    m.risk_heavy += 1
        elif r == "normal": m.risk_normal += 1
        else:               m.risk_light += 1

    # Per-batch aggregation
    all_returned_ids: set[str] = set()
    durations: list[float] = []
    prompt_tok_list: list[float] = []
    predicted_tok_list: list[float] = []
    batch_sizes: list[int] = []
    batch_kbs: list[float] = []

    for res in results:
        if res.is_error:
            m.failed_batches += 1
            if res.error_type == "provider":   m.provider_errors += 1
            elif res.error_type == "schema":   m.schema_errors += 1
            elif res.error_type == "timeout":  m.timeout_errors += 1
        else:
            m.completed_batches += 1

        m.retry_count += res.retry_count
        if res.cache_hit:
            m.cache_hits += 1

        durations.append(res.duration_ms / 1000.0)
        m.total_prompt_tokens  += res.prompt_tokens
        m.total_output_tokens  += res.output_tokens
        m.total_thought_tokens += res.thought_tokens
        m.total_cached_tokens  += res.cached_tokens
        m.total_cost_usd       += res.cost_usd
        m.inferred_block_id_count += res.inferred_block_id_count

        if res.predicted_prompt_tokens:
            predicted_tok_list.append(float(res.predicted_prompt_tokens))
        if res.prompt_tokens:
            prompt_tok_list.append(float(res.prompt_tokens))

        batch_sizes.append(len(res.input_block_ids))
        batch_kbs.append(
            sum(float(b.get("size_kb", 0)) for b in all_input_blocks
                if b["block_id"] in res.input_block_ids)
        )

        if not res.is_error and res.parsed_data:
            for analysis in res.parsed_data.get("block_analyses", []):
                if not isinstance(analysis, dict):
                    continue
                bid = analysis.get("block_id", "")
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
    if m.total_input_blocks:
        m.coverage_pct = round(
            (m.total_input_blocks - m.missing_count) / m.total_input_blocks * 100, 2
        )

    # KV median
    per_block_kv = []
    for res in results:
        if not res.is_error and res.parsed_data:
            for a in res.parsed_data.get("block_analyses", []):
                if isinstance(a, dict):
                    per_block_kv.append(len(a.get("key_values_read", [])))
    m.median_key_values = round(_median([float(x) for x in per_block_kv]), 2)

    if m.total_input_blocks:
        m.findings_per_100_blocks = round(m.total_findings / m.total_input_blocks * 100, 2)

    # Timing
    if durations:
        m.avg_batch_duration_s    = round(sum(durations) / len(durations), 2)
        m.median_batch_duration_s = round(_median(durations), 2)
        m.p95_batch_duration_s    = round(_percentile(durations, 95), 2)

    if prompt_tok_list:
        m.avg_prompt_tokens    = round(sum(prompt_tok_list) / len(prompt_tok_list), 1)
        m.median_prompt_tokens = round(_median(prompt_tok_list), 1)
    if predicted_tok_list:
        m.avg_predicted_tokens = round(sum(predicted_tok_list) / len(predicted_tok_list), 1)

    if batch_sizes:
        m.avg_batch_size    = round(sum(batch_sizes) / len(batch_sizes), 2)
        m.median_batch_size = round(_median([float(s) for s in batch_sizes]), 2)
        m.max_batch_size    = max(batch_sizes)
    if batch_kbs:
        m.avg_batch_kb = round(sum(batch_kbs) / len(batch_kbs), 2)

    valid_blocks = m.total_input_blocks - m.missing_count
    if valid_blocks > 0:
        m.cost_per_valid_block = round(m.total_cost_usd / valid_blocks, 6)
    if m.total_findings > 0:
        m.cost_per_finding = round(m.total_cost_usd / m.total_findings, 6)

    return m


# ─── Phase A quality gate ─────────────────────────────────────────────────────

def apply_phase_a_quality_gate(
    flash_m: RunMetrics,
    pro_m: RunMetrics,
) -> tuple[str, str, str]:
    """Apply quality gate: flash vs pro.

    Returns (mainline, fallback, gate_summary_md).
    """
    lines = ["## Phase A Quality Gate: Gemini 2.5 Flash vs 3.1 Pro\n"]
    failures: list[str] = []

    def check(cond: bool, msg: str) -> bool:
        if not cond:
            failures.append(msg)
        lines.append(f"- {'✓' if cond else '✗'} {msg}")
        return cond

    check(flash_m.coverage_pct == 100.0 and flash_m.missing_count == 0,
          f"coverage=100% (flash={flash_m.coverage_pct}%)")
    check(flash_m.missing_count == 0,
          f"missing=0 (flash={flash_m.missing_count})")
    check(flash_m.duplicate_count == 0,
          f"duplicate=0 (flash={flash_m.duplicate_count})")
    check(flash_m.extra_count == 0,
          f"extra=0 (flash={flash_m.extra_count})")
    check(flash_m.unreadable_count <= pro_m.unreadable_count,
          f"unreadable flash≤pro ({flash_m.unreadable_count}≤{pro_m.unreadable_count})")

    if pro_m.blocks_with_findings > 0:
        bwf_ratio = flash_m.blocks_with_findings / pro_m.blocks_with_findings
        check(bwf_ratio >= 0.95,
              f"blocks_with_findings flash≥95% of pro ({bwf_ratio:.1%})")
    if pro_m.total_findings > 0:
        f_ratio = flash_m.total_findings / pro_m.total_findings
        check(f_ratio >= 0.95,
              f"total_findings flash≥95% of pro ({f_ratio:.1%})")
    if pro_m.median_key_values > 0:
        kv_ratio = flash_m.median_key_values / pro_m.median_key_values
        check(kv_ratio >= 0.90,
              f"median_kv flash≥90% of pro ({kv_ratio:.1%})")

    cost_ok = flash_m.cost_per_valid_block < pro_m.cost_per_valid_block * 0.7 if pro_m.cost_per_valid_block else True
    check(cost_ok,
          f"cost/valid_block flash substantially < pro (flash=${flash_m.cost_per_valid_block:.4f} pro=${pro_m.cost_per_valid_block:.4f})")

    lines.append("")
    if failures:
        lines.append(f"**GATE RESULT: Flash FAILED** ({len(failures)} checks failed)")
        lines.append("→ Mainline candidate: **gemini-3.1-pro-preview**")
        lines.append("→ Fallback: N/A (pro is mainline)")
        mainline = "gemini-3.1-pro-preview"
        fallback = "gemini-3.1-pro-preview"
    else:
        lines.append("**GATE RESULT: Flash PASSED** all checks")
        lines.append("→ Mainline candidate: **gemini-2.5-flash**")
        lines.append("→ Fallback/escalation: **gemini-3.1-pro-preview**")
        mainline = "gemini-2.5-flash"
        fallback = "gemini-3.1-pro-preview"

    return mainline, fallback, "\n".join(lines)


# ─── Phase B winner ───────────────────────────────────────────────────────────

def select_batch_profile_winner(metrics_list: list[RunMetrics]) -> tuple[str, str]:
    """Select top-2 batch profiles from Phase B1.

    Priority: coverage=100 → stability → cost_per_valid_block → elapsed_s.
    Returns (winner, runner_up).
    """
    eligible = [m for m in metrics_list if m.coverage_pct == 100.0 and m.missing_count == 0]
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


def select_parallelism_winner(metrics_list: list[RunMetrics]) -> int:
    """Select winning parallelism from Phase B2.

    Priority: coverage=100 → min failed → min elapsed → min cost.
    """
    eligible = [m for m in metrics_list if m.coverage_pct == 100.0 and m.missing_count == 0]
    if not eligible:
        eligible = sorted(metrics_list, key=lambda m: -m.coverage_pct)
    eligible.sort(key=lambda m: (
        -(m.coverage_pct),
        m.failed_batches,
        m.elapsed_s,
        m.total_cost_usd,
    ))
    return eligible[0].parallelism if eligible else 3


# ─── Run helpers ──────────────────────────────────────────────────────────────

async def run_batches_async(
    batch_list: list[list[dict]],
    project_dir: Path,
    project_info: dict,
    base_system_prompt: str,
    page_contexts: dict[int, str],
    all_blocks: list[dict],
    model_id: str,
    tier: str,
    parallelism: int,
    api_key: str,
    use_cache: bool,
    dry_run: bool,
    exp_dir: Path,
    run_id: str,
) -> list:
    """Run all batches with parallelism control and return list of GeminiBlockBatchResult."""
    from webapp.services.gemini_direct_runner import (
        GeminiBlockBatchResult,
        GeminiCacheManager,
        run_gemini_direct_block_batch,
    )

    cache_manager = GeminiCacheManager(api_key) if (use_cache and api_key) else None

    if cache_manager:
        # Pre-warm: create cache for the base system prompt
        logger.info("[%s] Pre-warming context cache (model=%s)…", run_id, model_id)
        await cache_manager.get_or_create(model_id, base_system_prompt)

    total = len(batch_list)
    semaphore = asyncio.Semaphore(parallelism)
    results: list[GeminiBlockBatchResult | None] = [None] * total

    async def _run_one(idx: int, batch_blocks: list[dict]) -> None:
        async with semaphore:
            bid = idx + 1
            input_ids = [b["block_id"] for b in batch_blocks]

            if dry_run:
                # Simulate a result without API call
                results[idx] = GeminiBlockBatchResult(
                    batch_id=bid,
                    model_id=model_id,
                    tier=tier,
                    is_error=False,
                    duration_ms=0,
                    input_block_ids=input_ids,
                    returned_block_ids=input_ids,
                    predicted_prompt_tokens=len(batch_blocks) * 500,  # rough estimate
                    parsed_data={
                        "batch_id": bid,
                        "project_id": "dry_run",
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
                logger.debug("[dry-run] batch %03d/%d: %d blocks", bid, total, len(batch_blocks))
                return

            messages = build_experiment_messages(
                batch_blocks, project_dir, project_info,
                str(project_dir), bid, total,
                base_system_prompt, page_contexts,
            )
            result = await run_gemini_direct_block_batch(
                messages,
                input_ids,
                batch_id=bid,
                model_id=model_id,
                tier=tier,
                api_key=api_key,
                cache_manager=cache_manager,
            )
            results[idx] = result
            status = "OK" if not result.is_error else f"ERROR({result.error_type})"
            logger.info(
                "[%s] batch %03d/%d: %s | tokens=%d cost=$%.4f",
                run_id, bid, total, status, result.total_tokens, result.cost_usd,
            )

    start = time.monotonic()
    tasks = [asyncio.create_task(_run_one(i, b)) for i, b in enumerate(batch_list)]
    await asyncio.gather(*tasks)
    elapsed = time.monotonic() - start

    if cache_manager:
        await cache_manager.cleanup()

    # Replace None with error placeholder
    final = []
    for i, r in enumerate(results):
        if r is None:
            from webapp.services.gemini_direct_runner import GeminiBlockBatchResult
            r = GeminiBlockBatchResult(
                batch_id=i + 1, model_id=model_id, tier=tier,
                is_error=True, error_type="provider",
                error_message="Task did not complete",
                input_block_ids=[b["block_id"] for b in batch_list[i]],
            )
        final.append(r)

    logger.info("[%s] All %d batches done in %.1fs", run_id, total, elapsed)
    return final, elapsed


# ─── Phase A ──────────────────────────────────────────────────────────────────

async def run_phase_a(
    blocks: list[dict],
    subset_ids: list[str],
    subset_blocks: list[dict],
    project_dir: Path,
    project_info: dict,
    exp_dir: Path,
    api_key: str,
    parallelism: int,
    use_cache: bool,
    dry_run: bool,
) -> tuple[RunMetrics, RunMetrics, str, str]:
    """Phase A: model quality comparison on single-block subset.

    Returns (flash_metrics, pro_metrics, mainline, fallback).
    """
    base_prompt = build_base_system_prompt(project_info, len(subset_blocks))
    page_contexts = build_page_contexts(project_dir)

    # Single-block batches (1 block per request)
    single_batches = [[b] for b in subset_blocks]
    total = len(single_batches)

    all_model_metrics: list[RunMetrics] = []
    results_by_model: dict[str, list] = {}

    for model_id in ["gemini-2.5-flash", "gemini-3.1-pro-preview"]:
        run_id = f"phA_{model_id.split('.')[-1].split('-')[0]}"
        logger.info("=== Phase A: %s (%d single-block requests) ===", model_id, total)

        results, elapsed = await run_batches_async(
            single_batches,
            project_dir, project_info,
            base_prompt, page_contexts, subset_blocks,
            model_id, "standard", parallelism, api_key,
            use_cache, dry_run, exp_dir, run_id,
        )
        results_by_model[model_id] = results

        m = compute_metrics_from_results(
            results, subset_blocks,
            run_id=run_id, model_id=model_id, tier="standard",
            batch_profile="single", parallelism=parallelism,
            mode="single_block", elapsed_s=elapsed,
            cache_enabled=use_cache, dry_run=dry_run,
        )
        all_model_metrics.append(m)

        # Save per-run metrics
        _save_json(exp_dir / f"phase_a_{model_id}_metrics.json", asdict(m))
        logger.info(
            "[%s] coverage=%.1f%% findings=%d kv_median=%.1f cost=$%.4f elapsed=%.1fs",
            model_id, m.coverage_pct, m.total_findings, m.median_key_values,
            m.total_cost_usd, m.elapsed_s,
        )

    flash_m = next((m for m in all_model_metrics if "flash" in m.model_id), all_model_metrics[0])
    pro_m   = next((m for m in all_model_metrics if "pro"   in m.model_id), all_model_metrics[-1])

    # Quality gate
    mainline, fallback, gate_md = apply_phase_a_quality_gate(flash_m, pro_m)

    # Side-by-side summary
    sbs = {
        "flash": asdict(flash_m),
        "pro":   asdict(pro_m),
        "gate_result": {"mainline": mainline, "fallback": fallback},
    }
    _save_json(exp_dir / "model_quality_subset_summary.json", {
        "flash": asdict(flash_m),
        "pro":   asdict(pro_m),
    })
    _save_json(exp_dir / "subset_side_by_side_models.json", sbs)
    _save_csv(exp_dir / "model_quality_subset_summary.csv", [asdict(flash_m), asdict(pro_m)])

    sbs_md = _build_phase_a_sbs_md(flash_m, pro_m)
    (exp_dir / "subset_side_by_side_models.md").write_text(sbs_md, encoding="utf-8")

    (exp_dir / "model_quality_winner.md").write_text(
        f"# Phase A Quality Gate Result\n\n{gate_md}\n\n## Recommendation\n"
        f"- **Mainline**: {mainline}\n"
        f"- **Fallback/Escalation**: {fallback}\n",
        encoding="utf-8",
    )

    logger.info("Phase A complete. Mainline: %s | Fallback: %s", mainline, fallback)
    return flash_m, pro_m, mainline, fallback


def _build_phase_a_sbs_md(flash_m: RunMetrics, pro_m: RunMetrics) -> str:
    rows = [
        ("Model",              flash_m.model_id,                        pro_m.model_id),
        ("Coverage %",         f"{flash_m.coverage_pct:.1f}%",          f"{pro_m.coverage_pct:.1f}%"),
        ("Missing blocks",     str(flash_m.missing_count),              str(pro_m.missing_count)),
        ("Duplicate blocks",   str(flash_m.duplicate_count),            str(pro_m.duplicate_count)),
        ("Extra blocks",       str(flash_m.extra_count),                str(pro_m.extra_count)),
        ("Inferred block_ids", str(flash_m.inferred_block_id_count),    str(pro_m.inferred_block_id_count)),
        ("Unreadable blocks",  str(flash_m.unreadable_count),           str(pro_m.unreadable_count)),
        ("Empty summary",      str(flash_m.empty_summary_count),        str(pro_m.empty_summary_count)),
        ("Blocks w/ findings", str(flash_m.blocks_with_findings),       str(pro_m.blocks_with_findings)),
        ("Total findings",     str(flash_m.total_findings),             str(pro_m.total_findings)),
        ("Findings/100 blocks",f"{flash_m.findings_per_100_blocks:.1f}",f"{pro_m.findings_per_100_blocks:.1f}"),
        ("Median KV count",    f"{flash_m.median_key_values:.1f}",      f"{pro_m.median_key_values:.1f}"),
        ("Total KV count",     str(flash_m.total_key_values),           str(pro_m.total_key_values)),
        ("Prompt tokens",      str(flash_m.total_prompt_tokens),        str(pro_m.total_prompt_tokens)),
        ("Output tokens",      str(flash_m.total_output_tokens),        str(pro_m.total_output_tokens)),
        ("Thought tokens",     str(flash_m.total_thought_tokens),       str(pro_m.total_thought_tokens)),
        ("Cached tokens",      str(flash_m.total_cached_tokens),        str(pro_m.total_cached_tokens)),
        ("Total cost USD",     f"${flash_m.total_cost_usd:.4f}",        f"${pro_m.total_cost_usd:.4f}"),
        ("Cost/valid block",   f"${flash_m.cost_per_valid_block:.5f}",  f"${pro_m.cost_per_valid_block:.5f}"),
        ("Cost/finding",       f"${flash_m.cost_per_finding:.5f}",      f"${pro_m.cost_per_finding:.5f}"),
        ("Elapsed (s)",        f"{flash_m.elapsed_s:.1f}",              f"{pro_m.elapsed_s:.1f}"),
        ("Avg batch dur (s)",  f"{flash_m.avg_batch_duration_s:.2f}",   f"{pro_m.avg_batch_duration_s:.2f}"),
        ("Cache hits",         str(flash_m.cache_hits),                 str(pro_m.cache_hits)),
        ("Retry count",        str(flash_m.retry_count),                str(pro_m.retry_count)),
    ]
    lines = ["# Phase A — Model Quality Side-by-Side\n",
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
    api_key: str,
    use_cache: bool,
    dry_run: bool,
) -> tuple[str, str, int]:
    """Phase B: batch profile + parallelism economics.

    Returns (winner_profile, winner_parallelism, ...).
    """
    base_prompt = build_base_system_prompt(project_info, len(all_blocks))
    page_contexts = build_page_contexts(project_dir)

    b1_metrics: list[RunMetrics] = []

    # ── B1: batch profiles, fixed parallelism=3 ──
    for profile_name, risk_targets in BATCH_PROFILES.items():
        batches = pack_blocks(all_blocks, risk_targets)
        run_id = f"phB1_{profile_name}"
        logger.info("=== Phase B1: profile=%s batches=%d ===", profile_name, len(batches))

        results, elapsed = await run_batches_async(
            batches,
            project_dir, project_info,
            base_prompt, page_contexts, all_blocks,
            mainline_model, "standard", 3, api_key,
            use_cache, dry_run, exp_dir, run_id,
        )
        m = compute_metrics_from_results(
            results, all_blocks,
            run_id=run_id, model_id=mainline_model, tier="standard",
            batch_profile=profile_name, parallelism=3,
            mode="batch", elapsed_s=elapsed,
            cache_enabled=use_cache, dry_run=dry_run,
        )
        b1_metrics.append(m)
        _save_json(exp_dir / f"phase_b1_{profile_name}_metrics.json", asdict(m))
        logger.info(
            "[B1/%s] coverage=%.1f%% findings=%d cost=$%.4f elapsed=%.1fs batches=%d",
            profile_name, m.coverage_pct, m.total_findings,
            m.total_cost_usd, m.elapsed_s, m.total_batches,
        )

    winner_profile, runner_up_profile = select_batch_profile_winner(b1_metrics)
    logger.info("B1 winner: %s | runner-up: %s", winner_profile, runner_up_profile)

    _save_json(exp_dir / "batch_profile_summary.json", [asdict(m) for m in b1_metrics])
    _save_csv(exp_dir / "batch_profile_summary.csv", [asdict(m) for m in b1_metrics])
    (exp_dir / "batch_profile_summary.md").write_text(
        _build_batch_profile_md(b1_metrics, winner_profile, runner_up_profile),
        encoding="utf-8",
    )

    # ── B2: parallelism, top-2 profiles ──
    b2_metrics: list[RunMetrics] = []
    for profile_name in [winner_profile, runner_up_profile]:
        if profile_name == runner_up_profile and profile_name == winner_profile:
            continue
        risk_targets = BATCH_PROFILES[profile_name]
        batches = pack_blocks(all_blocks, risk_targets)

        for para in [2, 3, 4]:
            run_id = f"phB2_{profile_name}_p{para}"
            logger.info("=== Phase B2: profile=%s parallelism=%d ===", profile_name, para)

            results, elapsed = await run_batches_async(
                batches,
                project_dir, project_info,
                base_prompt, page_contexts, all_blocks,
                mainline_model, "standard", para, api_key,
                use_cache, dry_run, exp_dir, run_id,
            )
            m = compute_metrics_from_results(
                results, all_blocks,
                run_id=run_id, model_id=mainline_model, tier="standard",
                batch_profile=profile_name, parallelism=para,
                mode="batch", elapsed_s=elapsed,
                cache_enabled=use_cache, dry_run=dry_run,
            )
            b2_metrics.append(m)
            _save_json(exp_dir / f"phase_b2_{profile_name}_p{para}_metrics.json", asdict(m))
            logger.info(
                "[B2/%s/p%d] coverage=%.1f%% elapsed=%.1fs",
                profile_name, para, m.coverage_pct, m.elapsed_s,
            )

    winner_parallelism = select_parallelism_winner(b2_metrics) if b2_metrics else 3

    _save_json(exp_dir / "parallelism_summary.json", [asdict(m) for m in b2_metrics])
    _save_csv(exp_dir / "parallelism_summary.csv", [asdict(m) for m in b2_metrics])
    (exp_dir / "parallelism_summary.md").write_text(
        _build_parallelism_md(b2_metrics, winner_parallelism),
        encoding="utf-8",
    )

    logger.info("Phase B complete. Profile=%s Parallelism=%d", winner_profile, winner_parallelism)
    return winner_profile, winner_parallelism, b1_metrics, b2_metrics


def _build_batch_profile_md(
    metrics: list[RunMetrics], winner: str, runner_up: str
) -> str:
    lines = [
        "# Phase B1 — Batch Profile Comparison\n",
        f"Winner: **{winner}** | Runner-up: **{runner_up}**\n",
        "| Profile | Coverage | Batches | Avg Size | Total Findings | Cost USD | Elapsed (s) |",
        "|---------|----------|---------|----------|----------------|----------|-------------|",
    ]
    for m in metrics:
        marker = " ★" if m.batch_profile == winner else (" ●" if m.batch_profile == runner_up else "")
        lines.append(
            f"| {m.batch_profile}{marker} | {m.coverage_pct:.1f}% | {m.total_batches} "
            f"| {m.avg_batch_size:.1f} | {m.total_findings} "
            f"| ${m.total_cost_usd:.4f} | {m.elapsed_s:.1f} |"
        )
    return "\n".join(lines) + "\n"


def _build_parallelism_md(metrics: list[RunMetrics], winner: int) -> str:
    lines = [
        "# Phase B2 — Parallelism Comparison\n",
        f"Winner parallelism: **{winner}**\n",
        "| Profile | Para | Coverage | Failed | Elapsed (s) | Cost USD |",
        "|---------|------|----------|--------|-------------|----------|",
    ]
    for m in metrics:
        marker = " ★" if m.parallelism == winner else ""
        lines.append(
            f"| {m.batch_profile} | {m.parallelism}{marker} | {m.coverage_pct:.1f}% "
            f"| {m.failed_batches} | {m.elapsed_s:.1f} | ${m.total_cost_usd:.4f} |"
        )
    return "\n".join(lines) + "\n"


# ─── Phase C (Flex) ───────────────────────────────────────────────────────────

async def run_phase_c_flex(
    all_blocks: list[dict],
    project_dir: Path,
    project_info: dict,
    exp_dir: Path,
    mainline_model: str,
    winner_profile: str,
    winner_parallelism: int,
    api_key: str,
    use_cache: bool,
    dry_run: bool,
) -> RunMetrics | None:
    """Phase C: Flex tier smoke test."""
    base_prompt = build_base_system_prompt(project_info, len(all_blocks))
    page_contexts = build_page_contexts(project_dir)
    risk_targets = BATCH_PROFILES.get(winner_profile, BATCH_PROFILES["b10"])
    batches = pack_blocks(all_blocks, risk_targets)

    run_id = f"phC_flex_{mainline_model.split('.')[0]}"
    logger.info("=== Phase C Flex: model=%s profile=%s para=%d ===",
                mainline_model, winner_profile, winner_parallelism)

    results, elapsed = await run_batches_async(
        batches,
        project_dir, project_info,
        base_prompt, page_contexts, all_blocks,
        mainline_model, "flex", winner_parallelism, api_key,
        use_cache, dry_run, exp_dir, run_id,
    )

    m = compute_metrics_from_results(
        results, all_blocks,
        run_id=run_id, model_id=mainline_model, tier="flex",
        batch_profile=winner_profile, parallelism=winner_parallelism,
        mode="batch", elapsed_s=elapsed,
        cache_enabled=use_cache, dry_run=dry_run,
    )
    _save_json(exp_dir / "flex_smoke_summary.json", asdict(m))
    flex_md = (
        f"# Phase C — Flex Smoke Test\n\n"
        f"Model: {mainline_model} | Profile: {winner_profile} | Para: {winner_parallelism}\n\n"
        f"| Metric | Flex |\n|--------|------|\n"
        f"| Coverage | {m.coverage_pct:.1f}% |\n"
        f"| Failed batches | {m.failed_batches} |\n"
        f"| Total findings | {m.total_findings} |\n"
        f"| Total cost USD | ${m.total_cost_usd:.4f} |\n"
        f"| Elapsed (s) | {m.elapsed_s:.1f} |\n"
        f"| Retry count | {m.retry_count} |\n"
    )
    (exp_dir / "flex_smoke_summary.md").write_text(flex_md, encoding="utf-8")
    logger.info("[PhaseC/flex] coverage=%.1f%% cost=$%.4f elapsed=%.1fs",
                m.coverage_pct, m.total_cost_usd, m.elapsed_s)
    return m


# ─── Fallback sample (Pro escalation) ─────────────────────────────────────────

async def run_fallback_sample(
    all_blocks: list[dict],
    project_dir: Path,
    project_info: dict,
    exp_dir: Path,
    flash_results: list,        # list of GeminiBlockBatchResult from Phase A flash run
    api_key: str,
    use_cache: bool,
    dry_run: bool,
) -> RunMetrics | None:
    """Fallback sample: run 20 weakest Flash blocks on Pro to measure escalation value."""
    from webapp.services.gemini_direct_runner import GeminiBlockBatchResult

    # Select weakest blocks: unreadable + empty KV + no findings, sorted by page
    weak_ids: list[str] = []
    for res in flash_results:
        if res.is_error:
            weak_ids.extend(res.input_block_ids)
            continue
        for a in (res.parsed_data or {}).get("block_analyses", []):
            if not isinstance(a, dict):
                continue
            score = (
                int(bool(a.get("unreadable_text"))) * 3
                + int(not a.get("key_values_read")) * 2
                + int(not a.get("findings"))
            )
            if score >= 2:
                weak_ids.append(a.get("block_id", ""))

    # Deduplicate + take up to FALLBACK_SAMPLE_SIZE
    seen: set[str] = set()
    unique_weak = [bid for bid in weak_ids if bid and bid not in seen and not seen.add(bid)]
    sample_ids = unique_weak[:FALLBACK_SAMPLE_SIZE]

    if len(sample_ids) < FALLBACK_SAMPLE_SIZE:
        # Fill from heaviest blocks
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
    logger.info("Fallback sample: %d blocks on gemini-3.1-pro-preview", len(sample_blocks))

    base_prompt = build_base_system_prompt(project_info, len(sample_blocks))
    page_contexts = build_page_contexts(project_dir)
    single_batches = [[b] for b in sample_blocks]

    results, elapsed = await run_batches_async(
        single_batches,
        project_dir, project_info,
        base_prompt, page_contexts, sample_blocks,
        "gemini-3.1-pro-preview", "standard", 3, api_key,
        use_cache, dry_run, exp_dir, "fallback_sample",
    )

    m = compute_metrics_from_results(
        results, sample_blocks,
        run_id="fallback_sample",
        model_id="gemini-3.1-pro-preview", tier="standard",
        batch_profile="single", parallelism=3,
        mode="single_block", elapsed_s=elapsed,
        cache_enabled=use_cache, dry_run=dry_run,
    )
    _save_json(exp_dir / "fallback_sample_summary.json", asdict(m))
    fb_md = (
        f"# Fallback Sample — Pro Escalation Check\n\n"
        f"Blocks tested: {len(sample_blocks)} (weakest Flash results)\n\n"
        f"| Metric | Pro Fallback |\n|--------|--------------|\n"
        f"| Coverage | {m.coverage_pct:.1f}% |\n"
        f"| Total findings | {m.total_findings} |\n"
        f"| Unreadable | {m.unreadable_count} |\n"
        f"| Total cost USD | ${m.total_cost_usd:.4f} |\n"
        f"| Cost/valid block | ${m.cost_per_valid_block:.5f} |\n"
        f"| Elapsed (s) | {m.elapsed_s:.1f} |\n"
    )
    (exp_dir / "fallback_sample_summary.md").write_text(fb_md, encoding="utf-8")
    return m


# ─── Final recommendation ─────────────────────────────────────────────────────

def build_winner_recommendation(
    mainline: str,
    fallback: str,
    winner_profile: str,
    winner_parallelism: int,
    flash_m: RunMetrics,
    pro_m: RunMetrics,
    flex_m: RunMetrics | None,
    batch_m_list: list[RunMetrics],
    para_m_list: list[RunMetrics],
    dry_run: bool,
) -> str:
    flex_rec = "Not tested"
    if flex_m:
        if flex_m.coverage_pct == 100.0 and flex_m.failed_batches == 0:
            flex_rec = (
                f"Recommended for bulk/offline mode. "
                f"Cost: ${flex_m.total_cost_usd:.4f} vs standard. "
                f"Elapsed: {flex_m.elapsed_s:.1f}s"
            )
        else:
            flex_rec = (
                f"NOT recommended — unstable. "
                f"Coverage: {flex_m.coverage_pct:.1f}%, Failed: {flex_m.failed_batches}"
            )

    note = "\n> **Note: dry-run mode — no actual API calls were made.**\n" if dry_run else ""

    return f"""# Winner Recommendation
{note}
## Final Answers

| Question | Answer |
|----------|--------|
| Mainline model | **{mainline}** |
| Fallback/Escalation model | **{fallback}** |
| Batch profile | **{winner_profile}** |
| Parallelism | **{winner_parallelism}** |
| Flex recommendation | {flex_rec} |

## Phase A Quality Summary

| Model | Coverage | Findings | KV median | Cost/block |
|-------|----------|----------|-----------|------------|
| {flash_m.model_id} | {flash_m.coverage_pct:.1f}% | {flash_m.total_findings} | {flash_m.median_key_values:.1f} | ${flash_m.cost_per_valid_block:.5f} |
| {pro_m.model_id}   | {pro_m.coverage_pct:.1f}%   | {pro_m.total_findings}   | {pro_m.median_key_values:.1f}   | ${pro_m.cost_per_valid_block:.5f}   |

## Cost/Quality Trade-off

- **Flash** is {
    f"~{round((1 - flash_m.cost_per_valid_block / pro_m.cost_per_valid_block) * 100):.0f}% cheaper per valid block"
    if pro_m.cost_per_valid_block else "cheaper"
} than **Pro**
- {'Flash quality is within acceptable range of Pro' if mainline == 'gemini-2.5-flash' else 'Flash did not meet quality gate — Pro recommended'}
- Standard tier winner for normal pipeline; Flex as optional bulk mode

## Batch Profile Winner: {winner_profile}
{_batch_winner_justification(batch_m_list, winner_profile) if batch_m_list else 'Not tested'}

## Parallelism Winner: {winner_parallelism}
{_para_winner_justification(para_m_list, winner_parallelism) if para_m_list else 'Not tested'}

## What did NOT pass
- Production defaults remain UNCHANGED — no global switch made
- Direct Gemini path requires explicit GEMINI_DIRECT_API_KEY
- No Flex as default — standard tier remains mainline for latency-sensitive runs
"""


def _batch_winner_justification(metrics: list[RunMetrics], winner: str) -> str:
    m = next((m for m in metrics if m.batch_profile == winner), None)
    if not m:
        return ""
    return (
        f"Profile **{winner}** wins: coverage={m.coverage_pct:.1f}%, "
        f"cost/block=${m.cost_per_valid_block:.5f}, elapsed={m.elapsed_s:.1f}s"
    )


def _para_winner_justification(metrics: list[RunMetrics], winner: int) -> str:
    m = next((m for m in metrics if m.parallelism == winner), None)
    if not m:
        return ""
    return (
        f"Parallelism **{winner}** wins: coverage={m.coverage_pct:.1f}%, "
        f"elapsed={m.elapsed_s:.1f}s"
    )


# ─── CLI ──────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Gemini Direct API stage 02 experiment runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--pdf", required=True, help="Exact PDF filename to use as pilot project")
    p.add_argument("--project-dir", help="Override: explicit project directory path")
    p.add_argument("--dry-run", action="store_true", help="Validate config, no API calls")
    p.add_argument("--provider", default="gemini_direct", choices=["gemini_direct"],
                   help="Provider (currently only gemini_direct)")
    p.add_argument("--model", default=None,
                   help="Override mainline model for B/C phases (default: determined by Phase A gate)")
    p.add_argument("--tier", default="standard", choices=["standard", "flex"])
    p.add_argument("--subset-file", help="Path to existing fixed_subset_block_ids.json to reuse")
    p.add_argument("--single-block-subset", action="store_true",
                   help="Run Phase A only (1 block/request, both models)")
    p.add_argument("--batch-profile", choices=list(BATCH_PROFILES), default=None,
                   help="Run only this batch profile in Phase B")
    p.add_argument("--parallelism", type=int, default=3, choices=[1, 2, 3, 4, 5],
                   help="Default parallelism for Phase A and B1")
    p.add_argument("--no-cache", action="store_true", help="Disable context caching")
    p.add_argument("--full-run", action="store_true", help="Run all phases")
    p.add_argument("--phase", choices=["a", "b", "c", "fallback", "all"], default="all",
                   help="Which phases to run (default: all)")
    p.add_argument("--limit-blocks", type=int, default=None,
                   help="Limit number of blocks for debugging only (do NOT use for final decisions)")
    p.add_argument("--no-flex", action="store_true", help="Skip Phase C Flex smoke test")
    p.add_argument("--no-fallback", action="store_true", help="Skip fallback Pro sample")
    return p.parse_args()


async def main_async(args: argparse.Namespace) -> None:
    from webapp.services.project_service import resolve_project_by_pdf, ProjectByPdfError

    # ── Resolve project ────────────────────────────────────────────────────
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

    # ── Resolve API key ────────────────────────────────────────────────────
    api_key = os.environ.get("GEMINI_DIRECT_API_KEY", "") or os.environ.get("GOOGLE_API_KEY", "")
    if not api_key and not args.dry_run:
        logger.error(
            "GEMINI_DIRECT_API_KEY (or GOOGLE_API_KEY) not set. "
            "Set the environment variable or use --dry-run for config validation."
        )
        sys.exit(1)
    if not api_key:
        logger.warning("No Gemini API key found — running in dry-run mode only")

    use_cache = not args.no_cache
    phase = args.phase
    if args.single_block_subset:
        phase = "a"
    if args.full_run:
        phase = "all"

    # ── Load blocks ────────────────────────────────────────────────────────
    try:
        all_blocks = load_blocks_index(project_dir)
    except (FileNotFoundError, ValueError) as e:
        logger.error("Cannot load blocks: %s", e)
        sys.exit(1)

    if args.limit_blocks:
        logger.warning(
            "--limit-blocks=%d set — FOR DEBUGGING ONLY, do not use for final decisions",
            args.limit_blocks,
        )
        all_blocks = all_blocks[:args.limit_blocks]

    # ── Experiment directory ───────────────────────────────────────────────
    ts = _ts()
    exp_dir = project_dir / "_experiments" / "gemini_direct_stage02" / ts
    exp_dir.mkdir(parents=True, exist_ok=True)
    logger.info("Experiment dir: %s", exp_dir)

    # ── Subset ────────────────────────────────────────────────────────────
    subset_file = Path(args.subset_file) if args.subset_file else None
    # Try to reuse latest subset from previous experiments
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
        "use_cache": use_cache,
        "phase": phase,
        "api_key_present": bool(api_key),
        "parallelism": args.parallelism,
    }
    _save_json(exp_dir / "manifest.json", manifest)

    logger.info(
        "Setup: total_blocks=%d subset=%d dry_run=%s cache=%s",
        len(all_blocks), len(subset_blocks), args.dry_run, use_cache,
    )

    if args.dry_run:
        logger.info("DRY-RUN: Config validated. No API calls will be made.")
        _print_dry_run_summary(manifest, all_blocks, subset_blocks, BATCH_PROFILES)
        # Still run dry-run phases to validate logic
        if not api_key:
            logger.info("Stopping after dry-run validation (no GEMINI_DIRECT_API_KEY).")
            return

    # ── Phase A ────────────────────────────────────────────────────────────
    flash_m = pro_m = None
    mainline = args.model or "gemini-2.5-flash"
    fallback = "gemini-3.1-pro-preview"
    flash_results_for_fallback = []

    if phase in ("a", "all"):
        flash_m, pro_m, mainline, fallback = await run_phase_a(
            all_blocks, subset_ids, subset_blocks,
            project_dir, project_info, exp_dir,
            api_key, args.parallelism, use_cache, args.dry_run,
        )

    if args.model:
        mainline = args.model
        logger.info("Model override: %s", mainline)

    # ── Phase B ────────────────────────────────────────────────────────────
    winner_profile = "b10"
    winner_parallelism = 3
    b1_metrics: list[RunMetrics] = []
    b2_metrics: list[RunMetrics] = []

    if phase in ("b", "all") and (phase == "all" or flash_m is not None):
        winner_profile, winner_parallelism, b1_metrics, b2_metrics = await run_phase_b(
            all_blocks, project_dir, project_info, exp_dir,
            mainline, api_key, use_cache, args.dry_run,
        )
    elif args.batch_profile:
        winner_profile = args.batch_profile

    # ── Phase C ────────────────────────────────────────────────────────────
    flex_m: RunMetrics | None = None
    if phase in ("c", "all") and not args.no_flex:
        flex_m = await run_phase_c_flex(
            all_blocks, project_dir, project_info, exp_dir,
            mainline, winner_profile, winner_parallelism,
            api_key, use_cache, args.dry_run,
        )

    # ── Fallback sample ────────────────────────────────────────────────────
    if phase in ("fallback", "all") and not args.no_fallback and mainline == "gemini-2.5-flash":
        if flash_results_for_fallback:
            await run_fallback_sample(
                all_blocks, project_dir, project_info, exp_dir,
                flash_results_for_fallback, api_key, use_cache, args.dry_run,
            )
        else:
            logger.info("Skipping fallback sample: no flash results available from Phase A")

    # ── Final recommendation ───────────────────────────────────────────────
    _fm = flash_m or RunMetrics(model_id="gemini-2.5-flash")
    _pm = pro_m or RunMetrics(model_id="gemini-3.1-pro-preview")

    rec_md = build_winner_recommendation(
        mainline, fallback, winner_profile, winner_parallelism,
        _fm, _pm, flex_m, b1_metrics, b2_metrics,
        dry_run=args.dry_run,
    )
    (exp_dir / "winner_recommendation.md").write_text(rec_md, encoding="utf-8")
    logger.info("Saved winner_recommendation.md")

    # ── Print summary ──────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("EXPERIMENT COMPLETE")
    print(f"Artifacts: {exp_dir}")
    print("=" * 70)
    print(rec_md[:1200])
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
    print(f"  PDF:     {manifest['pdf']}")
    print(f"  API key: {'PRESENT' if manifest['api_key_present'] else 'MISSING'}")
    print(f"  Cache:   {'enabled' if manifest['use_cache'] else 'disabled'}")
    print(f"  Total blocks: {manifest['total_blocks']}")
    print(f"    heavy={risk_counts['heavy']} normal={risk_counts['normal']} light={risk_counts['light']}")
    print(f"  Subset: {manifest['subset_size']} blocks (from {manifest['subset_source']})")
    print()
    for profile_name, targets in profiles.items():
        batches = pack_blocks(all_blocks, targets)
        sizes = [len(b) for b in batches]
        print(
            f"  {profile_name}: {len(batches)} batches | "
            f"sizes: avg={sum(sizes)/len(sizes):.1f} min={min(sizes)} max={max(sizes)}"
        )
    print("=" * 70 + "\n")


def main() -> None:
    args = parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
