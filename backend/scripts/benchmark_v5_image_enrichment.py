#!/usr/bin/env python3
"""Benchmark v5 image enrichment on a small set of blocks without modifying
production session files.

Usage:

    # Single block by id (right side, force re-call):
    python backend/scripts/benchmark_v5_image_enrichment.py \
        --session ba413a93c5754f6c \
        --pair pf06effb7 \
        --side right \
        --blocks 47FF-P4TD-MWA \
        --force

    # Several blocks at once (left + right):
    python backend/scripts/benchmark_v5_image_enrichment.py \
        --session ba413a93c5754f6c --pair pf06effb7 \
        --side both --orders 1,3 --limit 3

    # Allow partial (salvaged) results without nonzero exit:
    python backend/scripts/benchmark_v5_image_enrichment.py \
        --session ... --pair ... --side right --blocks 47FF-P4TD-MWA \
        --allow-partial

    # Write a temp enriched MD copy for inspection (production data not touched):
    python backend/scripts/benchmark_v5_image_enrichment.py \
        --session ... --pair ... --side right --blocks 47FF-P4TD-MWA \
        --write-patch /tmp/v5-benchmark-patch.md

By default the script does NOT touch:
  * <pair>/text_enrichment/<side>_image_descriptions.json
  * <pair>/text_enrichment/<side>_enriched.md
  * <pair>/text_enrichment/cache/

Exit codes:
  0  — все блоки прошли OK
  1  — хотя бы один блок вернул invalid_json или finish_reason=length
       (если --allow-partial не задано)
  2  — argument/setup error

Output is line-by-line per block, suitable for piping to less / grep.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from dataclasses import replace as _dc_replace
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from backend.app.services.stage_comparison import md_image_enrichment as m  # noqa: E402
from backend.app.services.stage_comparison import graphic_llm_local as g  # noqa: E402
from backend.app.services.stage_comparison import store as store_mod  # noqa: E402


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Benchmark v5 image enrichment on selected blocks.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--session", required=True, help="session_id")
    p.add_argument("--pair", required=True, help="pair_id")
    p.add_argument(
        "--side", choices=("left", "right", "both"), default="right",
        help="which side to run on",
    )
    p.add_argument(
        "--blocks", default="",
        help="comma-separated block_ids (matched against md_block_id / side_block_id)",
    )
    p.add_argument(
        "--orders", default="",
        help="comma-separated MD orders (1-based) to run on",
    )
    p.add_argument(
        "--pages", default="",
        help="comma-separated pages to run on",
    )
    p.add_argument(
        "--limit", type=int, default=0,
        help="cap total blocks across the run (0 = no cap)",
    )
    p.add_argument(
        "--force", action="store_true",
        help="ignore existing per-block cache (force re-call Qwen)",
    )
    p.add_argument(
        "--allow-partial", action="store_true",
        help="exit 0 even if some blocks come back salvaged/truncated",
    )
    p.add_argument(
        "--write-patch", default="",
        help="path to write a JSON dump of all v5 results (production untouched)",
    )
    return p.parse_args()


def _csv_set(value: str) -> set[str]:
    return {x.strip() for x in (value or "").split(",") if x.strip()}


def _csv_ints(value: str) -> set[int]:
    out: set[int] = set()
    for x in (value or "").split(","):
        x = x.strip()
        if not x:
            continue
        try:
            out.add(int(x))
        except ValueError:
            print(f"WARN: invalid int in --orders/--pages: {x!r}", file=sys.stderr)
    return out


def _resolve_md_path(session_id: str, pair_id: str, side: str) -> Path | None:
    """Look up MD path from session.json's pair entry."""
    from backend.app.services.stage_comparison import store as _store
    sess = _store.get_session(session_id)
    if sess is None:
        return None
    pair = next((p for p in (sess.get("pairs") or []) if p.get("id") == pair_id), None)
    if pair is None:
        return None
    side_obj = pair.get(side) or {}
    md_path = side_obj.get("md_path")
    if not md_path:
        return None
    return Path(md_path)


def _resolve_target_blocks(
    session_id: str, pair_id: str, side: str,
    block_filter: set[str], order_filter: set[int], page_filter: set[int],
    limit: int,
) -> list[tuple[m.MdBlock, str]]:
    """Return list of (md_block, side_block_id) tuples to benchmark."""
    md_path = _resolve_md_path(session_id, pair_id, side)
    if md_path is None or not md_path.exists():
        print(f"ERROR: cannot resolve MD path for {session_id}/{pair_id}/{side}", file=sys.stderr)
        return []
    md_text = md_path.read_text(encoding="utf-8")
    blocks = m.parse_md_blocks(md_text)
    image_blocks = [b for b in blocks if b.is_image]

    # Read existing image_descriptions.json to learn side_block_id mappings
    from backend.app.services.stage_comparison import paths as _paths
    desc_path = _paths.text_enrichment_descriptions_path(session_id, pair_id, side)
    side_block_by_order: dict[int, str] = {}
    if desc_path.exists():
        try:
            data = json.loads(desc_path.read_text(encoding="utf-8"))
            for it in data.get("items") or []:
                o = it.get("order")
                sbid = it.get("side_block_id") or it.get("md_block_id")
                if isinstance(o, int) and sbid:
                    side_block_by_order[o] = sbid
        except (OSError, json.JSONDecodeError):
            pass

    selected: list[tuple[m.MdBlock, str]] = []
    for mb in image_blocks:
        sbid = side_block_by_order.get(mb.order) or mb.block_id or ""
        if block_filter and sbid not in block_filter and (mb.block_id or "") not in block_filter:
            continue
        if order_filter and mb.order not in order_filter:
            continue
        if page_filter and (mb.page or -1) not in page_filter:
            continue
        if not sbid:
            continue
        selected.append((mb, sbid))
    if limit > 0:
        selected = selected[:limit]
    return selected


async def _bench_one(
    session_id: str, pair_id: str, side: str,
    mb: m.MdBlock, side_block_id: str,
    cfg: g.LocalGraphicLLMConfig, force: bool,
) -> dict:
    """Run v5 enrichment on one block. Returns result dict (does NOT write to disk)."""
    block_type = m.classify_image_block(mb)
    type_cfg = m.get_block_type_config(block_type)
    prompt_text, prompt_version = m.get_prompt_for_block_type(block_type)
    render_long = int(type_cfg["render_target_long_side"])
    image_long = int(type_cfg["image_input_long_side"])
    max_tokens = type_cfg.get("max_tokens") or cfg.max_tokens
    max_cont = type_cfg.get("max_continuations")
    if max_cont is None:
        max_cont = cfg.max_continuations

    # Render crop (this DOES create a PNG, but only into the existing
    # crops dir which is normally not in git; this is the same behaviour
    # as a regular enrichment run).
    crop_path = store_mod.render_block_crop(
        session_id, pair_id, side, side_block_id, target_long_side=render_long,
    )
    if not crop_path or not Path(crop_path).exists():
        return {
            "block_id": side_block_id, "order": mb.order, "page": mb.page,
            "block_type": block_type, "prompt_version": prompt_version,
            "status": "no_image", "error": "render_failed_or_missing",
        }

    cfg_override = _dc_replace(
        cfg,
        image_long_side=image_long,
        max_tokens=int(max_tokens),
        max_continuations=int(max_cont),
    )

    # Cache: respect `force` flag.
    if not force:
        try:
            img_bytes = Path(crop_path).read_bytes()
            key = m.compute_image_cache_key(img_bytes, cfg.model, prompt_version)
            cached = m.read_cache(session_id, pair_id, key)
            if cached:
                return {
                    "block_id": side_block_id, "order": mb.order, "page": mb.page,
                    "block_type": block_type, "prompt_version": prompt_version,
                    "from_cache": True,
                    "status": cached.get("status") or "done",
                    "duration_sec": 0.0,
                    "labels_count": len(((cached.get("description") or {}).get("diff_anchors") or {}).get("labels") or []),
                    "ratings_count": len(((cached.get("description") or {}).get("diff_anchors") or {}).get("ratings") or []),
                    "connections_count": len(((cached.get("description") or {}).get("diff_anchors") or {}).get("connections") or []),
                }
        except OSError:
            pass

    t0 = time.monotonic()
    try:
        res = await g.describe_image_local(Path(crop_path), prompt_text, cfg=cfg_override)
    except Exception as exc:  # noqa: BLE001
        return {
            "block_id": side_block_id, "order": mb.order, "page": mb.page,
            "block_type": block_type, "prompt_version": prompt_version,
            "status": "exception", "error": f"{type(exc).__name__}:{exc}",
            "duration_sec": round(time.monotonic() - t0, 1),
        }
    elapsed = time.monotonic() - t0

    parsed = res.parsed or {}
    da = parsed.get("diff_anchors") or {}
    labels = da.get("labels") or []
    ratings = da.get("ratings") or []
    connections = da.get("connections") or []

    # Run quality analysis the same way enrich_side would
    item_context = {
        "block_type": block_type,
        "salvaged": (res.status == "partial"),
        "parse_error_detail": res.parse_error_detail,
        "continuation_warnings": (parsed.get("continuation_warnings") or []),
    }
    quality = m.analyze_qwen_description_quality(parsed if isinstance(parsed, dict) else {}, item_context)

    return {
        "block_id": side_block_id, "order": mb.order, "page": mb.page,
        "block_type": block_type,
        "prompt_version": prompt_version,
        "render_target_long_side": render_long,
        "image_input_long_side": image_long,
        "max_tokens": int(max_tokens),
        "max_continuations": int(max_cont),
        "duration_sec": round(elapsed, 1),
        "status": res.status,
        "finish_reason": res.finish_reason,
        "parse_error_detail": res.parse_error_detail,
        "response_char_count": int(res.response_char_count or 0),
        "usage": res.usage or {},
        "labels_count": len(labels),
        "ratings_count": len(ratings),
        "connections_count": len(connections),
        "sample_labels": [
            (lab.get("raw_text") if isinstance(lab, dict) else str(lab))
            for lab in labels[:5]
        ],
        "sample_ratings": [
            (r.get("raw_text") if isinstance(r, dict) else str(r))
            for r in ratings[:5]
        ],
        "sample_connections": [
            (f"{c.get('from_raw')} -> {c.get('to_raw')}" if isinstance(c, dict) else str(c))
            for c in connections[:5]
        ],
        "usable_for_diff": quality["usable_for_diff"],
        "warnings": quality["warnings"],
        "adjusted_confidence": quality["adjusted_confidence"],
    }


def _print_result(res: dict, idx: int, total: int) -> None:
    print(f"\n=== [{idx}/{total}] block={res['block_id']} (page={res.get('page')} order={res.get('order')}) ===")
    print(f"  block_type:        {res.get('block_type')}")
    print(f"  prompt_version:    {res.get('prompt_version')}")
    print(f"  render_long_side:  {res.get('render_target_long_side')}")
    print(f"  image_long_side:   {res.get('image_input_long_side')}")
    print(f"  max_tokens:        {res.get('max_tokens')}")
    print(f"  max_continuations: {res.get('max_continuations')}")
    print(f"  duration_sec:      {res.get('duration_sec')}")
    print(f"  status:            {res.get('status')}")
    print(f"  finish_reason:     {res.get('finish_reason')}")
    print(f"  parse_error:       {res.get('parse_error_detail')}")
    print(f"  response_chars:    {res.get('response_char_count')}")
    usage = res.get("usage") or {}
    print(f"  usage:             prompt={usage.get('prompt_tokens')} completion={usage.get('completion_tokens')} total={usage.get('total_tokens')}")
    print(f"  anchors:           labels={res.get('labels_count')} ratings={res.get('ratings_count')} connections={res.get('connections_count')}")
    print(f"  sample_labels:     {res.get('sample_labels')}")
    print(f"  sample_ratings:    {res.get('sample_ratings')}")
    print(f"  sample_connections:{res.get('sample_connections')}")
    print(f"  usable_for_diff:   {res.get('usable_for_diff')}")
    print(f"  warnings:          {res.get('warnings')}")
    if res.get("adjusted_confidence") is not None:
        print(f"  adjusted_confidence: {res.get('adjusted_confidence')}")


def _is_failure_for_strict_mode(res: dict) -> bool:
    """Strict exit: nonzero if invalid_json or finish_reason=length."""
    if res.get("status") not in ("done", "partial"):
        return True
    if (res.get("finish_reason") or "") == "length":
        return True
    if res.get("parse_error_detail") in ("truncated_json", "json_parse_failed", "no_opening_brace",
                                         "empty_content", "markdown_reasoning"):
        return True
    return False


async def main() -> int:
    args = _parse_args()
    sides = ("left", "right") if args.side == "both" else (args.side,)
    block_filter = _csv_set(args.blocks)
    order_filter = _csv_ints(args.orders)
    page_filter = _csv_ints(args.pages)

    if not (block_filter or order_filter or page_filter) and args.limit == 0:
        print("ERROR: specify at least --blocks/--orders/--pages or --limit N", file=sys.stderr)
        return 2

    cfg = g.load_local_graphic_llm_config()
    ok, reason = g.check_local_graphic_llm_available(cfg)
    if not ok:
        print(f"ERROR: graphic LLM not available: {reason}", file=sys.stderr)
        return 2

    print(f"benchmark v5: session={args.session} pair={args.pair} sides={sides}")
    print(f"  cfg.model={cfg.model}  base_url={cfg.base_url[:50]}...")
    print(f"  force={args.force}  allow_partial={args.allow_partial}")

    results: list[dict] = []
    failures: list[dict] = []

    for side in sides:
        selected = _resolve_target_blocks(
            args.session, args.pair, side,
            block_filter, order_filter, page_filter, args.limit,
        )
        if not selected:
            print(f"\n[side={side}] no blocks matched filters; skipping")
            continue
        print(f"\n[side={side}] will run on {len(selected)} block(s)")
        for i, (mb, sbid) in enumerate(selected, start=1):
            res = await _bench_one(
                args.session, args.pair, side, mb, sbid, cfg, args.force,
            )
            res["side"] = side
            results.append(res)
            _print_result(res, i, len(selected))
            if not args.allow_partial and _is_failure_for_strict_mode(res):
                failures.append(res)

    if args.write_patch:
        out = Path(args.write_patch)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n[+] wrote results dump: {out}  ({len(results)} blocks)")

    print(f"\n=== SUMMARY ===")
    print(f"  total blocks:   {len(results)}")
    print(f"  hit cache:      {sum(1 for r in results if r.get('from_cache'))}")
    print(f"  status=done:    {sum(1 for r in results if r.get('status') == 'done')}")
    print(f"  status=partial: {sum(1 for r in results if r.get('status') == 'partial')}")
    print(f"  status=other:   {sum(1 for r in results if r.get('status') not in ('done', 'partial'))}")
    print(f"  finish=length:  {sum(1 for r in results if r.get('finish_reason') == 'length')}")
    print(f"  total duration: {sum(r.get('duration_sec') or 0 for r in results):.1f}s")
    print(f"  failures (strict): {len(failures)}")

    if failures and not args.allow_partial:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
