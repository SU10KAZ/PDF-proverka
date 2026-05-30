"""Automatic problem-block tiled high-res retry for Qwen image enrichment.

Background (from `experiments/qwen_problem_block_recognition/`):
  * provider = `qwen/qwen3.6-35b-a3b` via LM Studio + ngrok, OpenAI-compatible,
    image-base64 only (PDF URL / PDF base64 are rejected with HTTP 400);
  * dense graphic blocks frequently fail not because Qwen can't read them but
    because one big crop makes the model generate so long that the call exceeds
    the ngrok read timeout (~300s) → ReadError / http_error / truncated JSON;
  * splitting the block into overlapping tiles (each a fast, small call) and
    merging the per-tile facts recovered blocks where the baseline produced
    timeout / http_error / invalid_json / near-empty output. Tiled won 7/8 live
    problem blocks and recovered 100% of baseline failures.

This module adds that recovery to the production pipeline **behind a feature
flag (default OFF)**. It is intentionally self-contained and dependency-light:

  * tiling is pure PIL (no PyMuPDF here — the high-res image is produced by the
    existing ``render_crop`` callback, the same one ``enrich_side`` already uses);
  * Qwen is reached through a caller-supplied async ``describe_fn`` (the same
    ``_call_describe`` ``enrich_side`` uses), so this module never imports the
    HTTP client and is trivially unit-testable with fakes;
  * it never raises into the pipeline — any failure returns the baseline result
    plus diagnostics.

Integration: ``enrich_side`` calls :func:`maybe_run_problem_block_retry` after the
baseline per-block result is finalized. PDF-URL / PDF-Base64 are deliberately NOT
implemented as a production path (provider does not support them).
"""
from __future__ import annotations

import hashlib
import io
import logging
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger(__name__)

# Generic problem-flagged token markers in model prose (lowercased contains-match).
_BAD_OUTPUT_MARKERS = (
    "текст неразборчив", "неразборчив", "не удалось распознать",
    "невозможно определить", "слишком низкое качество", "низкое качество",
    "unreadable", "cannot read", "too small", "не читается", "нечитаем",
    "невозможно прочитать", "не могу распознать",
)

# Fact list keys that count as "real" extraction (v4 + v5 flat).
_FACT_LIST_KEYS = (
    "visible_text", "labels", "materials", "numeric_parameters",
    "elevations", "dimensions", "equipment", "connections", "tables",
)


# ─── Config ────────────────────────────────────────────────────────────────


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_str(name: str, default: str) -> str:
    raw = os.environ.get(name, "").strip()
    return raw or default


@dataclass
class ProblemBlockRetryConfig:
    enabled: bool = False
    mode: str = "tiled"
    after_main: bool = True
    max_attempts: int = 1
    min_confidence: float = 0.45

    retry_on_timeout: bool = True
    retry_on_http_error: bool = True
    retry_on_invalid_json: bool = True
    retry_on_not_usable: bool = True

    render_dpi: int = 600                 # informational; render is long-side driven
    render_long_side: int = 4000          # high-res render target (px) before tiling
    tile_width: int = 1600
    tile_height: int = 1600
    tile_overlap: int = 200
    max_tiles: int = 24
    tile_timeout_sec: int = 300
    max_total_sec_per_block: int = 1200
    cache_enabled: bool = True

    # Per-block min long side below which tiling is pointless (single tile).
    min_long_side_for_tiling: int = 1400

    @classmethod
    def from_env(cls) -> "ProblemBlockRetryConfig":
        return cls(
            enabled=_env_bool("STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_RETRY_ENABLED", False),
            mode=_env_str("STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_RETRY_MODE", "tiled"),
            after_main=_env_bool("STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_RETRY_AFTER_MAIN", True),
            max_attempts=_env_int("STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_MAX_ATTEMPTS", 1),
            min_confidence=_env_float("STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_MIN_CONFIDENCE", 0.45),
            retry_on_timeout=_env_bool("STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_RETRY_ON_TIMEOUT", True),
            retry_on_http_error=_env_bool("STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_RETRY_ON_HTTP_ERROR", True),
            retry_on_invalid_json=_env_bool("STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_RETRY_ON_INVALID_JSON", True),
            retry_on_not_usable=_env_bool("STAGE_COMPARISON_QWEN_PROBLEM_BLOCK_RETRY_ON_NOT_USABLE", True),
            render_dpi=_env_int("STAGE_COMPARISON_QWEN_TILE_RENDER_DPI", 600),
            render_long_side=_env_int("STAGE_COMPARISON_QWEN_TILE_RENDER_LONG_SIDE", 4000),
            tile_width=_env_int("STAGE_COMPARISON_QWEN_TILE_WIDTH", 1600),
            tile_height=_env_int("STAGE_COMPARISON_QWEN_TILE_HEIGHT", 1600),
            tile_overlap=_env_int("STAGE_COMPARISON_QWEN_TILE_OVERLAP", 200),
            max_tiles=_env_int("STAGE_COMPARISON_QWEN_TILE_MAX_TILES", 24),
            tile_timeout_sec=_env_int("STAGE_COMPARISON_QWEN_TILE_TIMEOUT_SEC", 300),
            max_total_sec_per_block=_env_int("STAGE_COMPARISON_QWEN_TILE_MAX_TOTAL_SEC_PER_BLOCK", 1200),
            cache_enabled=_env_bool("STAGE_COMPARISON_QWEN_TILE_CACHE_ENABLED", True),
            min_long_side_for_tiling=_env_int("STAGE_COMPARISON_QWEN_TILE_MIN_LONG_SIDE", 1400),
        )


# ─── Tile prompt ─────────────────────────────────────────────────────────────

TILE_PROMPT = """Ты видишь ФРАГМЕНT графического блока проектной/рабочей документации (один tile из большой схемы/чертежа/таблицы).

Извлеки ТОЛЬКО проверяемые факты, которые реально видны именно в этом фрагменте.
Не делай выводов про весь лист, если видишь только часть. Не выдумывай значения.
Если надпись не читается — НЕ угадывай, помести её в visible_text с низким confidence.

Верни СТРОГО один JSON-объект:
{
  "tile_id": "",
  "visible_text": [{"text":"","confidence":0.0,"evidence_snippet":""}],
  "labels": [{"raw_text":"","kind":"panel|switchgear|breaker|line|cable|room|other","confidence":0.0}],
  "materials": [{"name":"","spec":"","evidence_snippet":""}],
  "numeric_parameters": [{"name":"","value":"","unit":"","evidence_snippet":""}],
  "elevations": [{"value":"","unit":"","ref":""}],
  "dimensions": [{"value":"","unit":"","of":""}],
  "equipment": [{"name":"","mark":"","qty":""}],
  "connections": [{"from":"","to":"","relation":""}],
  "tables": [{"title":"","rows":[],"notes":""}],
  "warnings": [],
  "confidence": 0.0,
  "usable_for_diff": true
}
Только JSON, без markdown."""


# ─── Detection: is this a problem block? ─────────────────────────────────────


def _facts_total(payload: Any) -> int:
    if not isinstance(payload, dict):
        return 0
    n = 0
    for k in _FACT_LIST_KEYS:
        v = payload.get(k)
        if isinstance(v, list):
            n += len(v)
    sa = payload.get("scheme_analysis")
    if isinstance(sa, dict):
        n += len(sa.get("nodes") or []) + len(sa.get("connections") or [])
    da = payload.get("diff_anchors")
    if isinstance(da, dict):
        n += len(da.get("labels") or []) + len(da.get("ratings") or []) + len(da.get("connections") or [])
    return n


def _has_generic_only_text(payload: dict) -> bool:
    """visible_text present but every entry is a generic phrase like 'чертёж'."""
    vt = payload.get("visible_text") if isinstance(payload, dict) else None
    if not isinstance(vt, list) or not vt:
        return False
    generic = ("чертеж", "чертёж", "схема", "таблица", "изображение", "drawing", "scheme")
    saw_any = False
    for it in vt:
        t = (it.get("text") if isinstance(it, dict) else str(it)) or ""
        t = t.strip().lower()
        if not t:
            continue
        saw_any = True
        # any concrete (non-generic-phrase) text means it's NOT generic-only,
        # regardless of length — a short label like "ЩР" is a real fact.
        if not any(g in t for g in generic):
            return False
    return saw_any


def should_retry_problem_block(
    result: dict,
    error: Optional[BaseException],
    block_meta: dict,
    cfg: ProblemBlockRetryConfig,
) -> tuple[bool, str]:
    """Decide whether a baseline result warrants a tiled retry.

    ``result`` is the finalized per-block item (from ``enrich_side``), including
    ``status``, ``description``, ``usable_for_diff``, ``finish_reason``,
    ``confidence_adjusted``, ``warnings``. Returns ``(retry_required, reason)``.
    Only fires for graphic blocks (a resolved image existed)."""
    if not cfg.enabled:
        return False, "disabled"
    if cfg.mode != "tiled":
        return False, f"mode_not_supported:{cfg.mode}"

    status = (result or {}).get("status")
    desc = (result or {}).get("description") if isinstance(result, dict) else None
    warnings = [str(w).lower() for w in (result or {}).get("warnings", []) or []]
    err_text = ""
    if error is not None:
        err_text = f"{type(error).__name__}:{error}".lower()
    if isinstance(desc, dict) and desc.get("error"):
        err_text = (err_text + " " + str(desc.get("error"))).lower()

    # hard errors
    if cfg.retry_on_timeout and ("timeout" in err_text or "timed_out" in err_text or status == "timeout"):
        return True, "timeout"
    if cfg.retry_on_http_error and ("readerror" in err_text or "http_" in err_text
                                    or "http_error" in err_text or status == "http_error"):
        return True, "http_error"
    if cfg.retry_on_invalid_json and (
        status in ("error",) and ("json" in err_text or "parse" in err_text or "no_parsed_json" in err_text)
        or "invalid_json" in err_text
        or any("json" in w for w in warnings)
    ):
        return True, "invalid_json"
    if status in ("error", "no_image_found"):
        # generic error not otherwise matched
        if status == "error":
            return True, "baseline_error"

    # truncated output
    if (result or {}).get("finish_reason") == "length":
        return True, "truncated_output"

    # not usable
    if cfg.retry_on_not_usable and result.get("usable_for_diff") is False:
        return True, "not_usable"

    # empty / generic facts despite being a graphic block
    facts = _facts_total(desc)
    if facts == 0:
        return True, "empty_facts"
    if facts <= 2:
        return True, "near_empty_facts"
    if isinstance(desc, dict) and _has_generic_only_text(desc):
        return True, "generic_output"

    # low confidence
    conf = result.get("confidence_adjusted")
    if conf is None and isinstance(desc, dict):
        conf = desc.get("confidence")
    try:
        if conf is not None and float(conf) < cfg.min_confidence:
            return True, "low_confidence"
    except (TypeError, ValueError):
        pass

    # bad-output prose markers
    blob = ""
    if isinstance(desc, dict):
        blob = (str(desc.get("summary") or "") + " " + str(desc.get("error") or "")).lower()
    if any(m in blob for m in _BAD_OUTPUT_MARKERS):
        return True, "bad_output_marker"

    return False, "ok"


# ─── Tiling (pure PIL) ───────────────────────────────────────────────────────


def split_image_into_tiles(
    png_bytes: bytes,
    *,
    tile_width: int,
    tile_height: int,
    overlap: int,
    max_tiles: int,
) -> tuple[list[bytes], dict]:
    """Split a PNG into overlapping tiles. If the natural grid exceeds
    ``max_tiles``, the image is downscaled so the whole block fits within the
    tile budget (preserves coverage, bounds the number of Qwen calls).

    Returns (list[png_bytes], meta)."""
    from PIL import Image

    with Image.open(io.BytesIO(png_bytes)) as im:
        im.load()
        if im.mode not in ("RGB", "L"):
            im = im.convert("RGB")
        w, h = im.size

        step_x = max(1, tile_width - overlap)
        step_y = max(1, tile_height - overlap)
        ncols = max(1, -(-max(0, w - overlap) // step_x)) if w > tile_width else 1
        nrows = max(1, -(-max(0, h - overlap) // step_y)) if h > tile_height else 1

        # budget: downscale if too many tiles
        downscaled = False
        if ncols * nrows > max_tiles:
            import math
            shrink = math.sqrt(max_tiles / float(ncols * nrows))
            nw, nh = max(tile_width, int(w * shrink)), max(tile_height, int(h * shrink))
            im = im.resize((nw, nh), Image.LANCZOS)
            w, h = im.size
            downscaled = True
            ncols = max(1, -(-max(0, w - overlap) // step_x)) if w > tile_width else 1
            nrows = max(1, -(-max(0, h - overlap) // step_y)) if h > tile_height else 1

        tiles: list[bytes] = []
        positions: list[dict] = []
        for r in range(nrows):
            for c in range(ncols):
                x0 = c * step_x
                y0 = r * step_y
                x1 = min(w, x0 + tile_width)
                y1 = min(h, y0 + tile_height)
                if x1 <= x0 or y1 <= y0:
                    continue
                crop = im.crop((x0, y0, x1, y1))
                buf = io.BytesIO()
                crop.save(buf, format="PNG", optimize=True)
                tiles.append(buf.getvalue())
                positions.append({"row": r, "col": c, "box": [x0, y0, x1, y1]})
                if len(tiles) >= max_tiles:
                    break
            if len(tiles) >= max_tiles:
                break

    meta = {
        "image_size": [w, h],
        "n_tiles": len(tiles),
        "grid": [nrows, ncols],
        "downscaled": downscaled,
        "tile_size": [tile_width, tile_height],
        "overlap": overlap,
        "positions": positions,
    }
    return tiles, meta


# ─── Merge ───────────────────────────────────────────────────────────────────


def _item_signature(it: Any) -> str:
    if isinstance(it, dict):
        for k in ("raw_text", "text", "name", "value", "from", "title"):
            v = it.get(k)
            if v:
                return str(v).strip().lower()
        try:
            import json as _json
            return _json.dumps(it, ensure_ascii=False, sort_keys=True).lower()
        except Exception:  # noqa: BLE001
            return str(it).lower()
    return str(it).strip().lower()


def merge_tiled_qwen_results(
    tile_payloads: list[dict],
    *,
    baseline: Optional[dict] = None,
) -> dict:
    """Union tile fact lists with dedup + provenance. Confidence rises when the
    same fact appears in multiple tiles. Uncertain facts stay uncertain."""
    merged: dict[str, Any] = {"status": "done"}
    for key in _FACT_LIST_KEYS:
        merged[key] = []
    seen: dict[str, dict[str, int]] = {k: {} for k in _FACT_LIST_KEYS}
    warnings: list[str] = []
    confidences: list[float] = []

    for ti, payload in enumerate(tile_payloads):
        if not isinstance(payload, dict):
            continue
        tile_id = payload.get("tile_id") or f"tile_{ti}"
        for key in _FACT_LIST_KEYS:
            items = payload.get(key)
            if not isinstance(items, list):
                continue
            for it in items:
                sig = _item_signature(it)
                if not sig:
                    continue
                if sig in seen[key]:
                    # confirmed by another tile → bump provenance count
                    idx = seen[key][sig]
                    tgt = merged[key][idx]
                    if isinstance(tgt, dict):
                        tgt.setdefault("_tiles", [])
                        if tile_id not in tgt["_tiles"]:
                            tgt["_tiles"].append(tile_id)
                            tgt["_confirmations"] = len(tgt["_tiles"])
                    continue
                if isinstance(it, dict):
                    it = dict(it)
                    it["_tiles"] = [tile_id]
                    it["_confirmations"] = 1
                merged[key].append(it)
                seen[key][sig] = len(merged[key]) - 1
        w = payload.get("warnings")
        if isinstance(w, list):
            warnings.extend(str(x) for x in w)
        c = payload.get("confidence")
        try:
            if c is not None:
                confidences.append(float(c))
        except (TypeError, ValueError):
            pass

    # confidence: mean of tile confidences, lightly boosted by multi-tile confirmations
    base_conf = (sum(confidences) / len(confidences)) if confidences else 0.0
    multi = sum(
        1 for key in _FACT_LIST_KEYS for it in merged[key]
        if isinstance(it, dict) and it.get("_confirmations", 1) >= 2
    )
    boost = min(0.15, 0.02 * multi)
    merged["confidence"] = round(min(1.0, base_conf + boost), 3)
    merged["warnings"] = list(dict.fromkeys(warnings))
    merged["usable_for_diff"] = _facts_total(merged) > 0
    merged["_merged_from_tiles"] = len(tile_payloads)
    return merged


# ─── Cache ───────────────────────────────────────────────────────────────────


def compute_retry_cache_key(
    *,
    session_id: str,
    pair_id: str,
    side: str,
    block_id: str,
    image_bytes: bytes,
    cfg: ProblemBlockRetryConfig,
    model: str,
    prompt_version: str = "tile_v1",
) -> str:
    h = hashlib.sha256()
    h.update(image_bytes)
    parts = [
        session_id, pair_id, side, block_id, "tiled_retry",
        str(cfg.render_dpi), str(cfg.render_long_side),
        f"{cfg.tile_width}x{cfg.tile_height}", str(cfg.tile_overlap),
        str(cfg.max_tiles), prompt_version, model,
    ]
    h.update(("|".join(parts)).encode("utf-8"))
    return "tiledretry_" + h.hexdigest()[:32]


# ─── Orchestration ───────────────────────────────────────────────────────────


@dataclass
class RetryOutcome:
    status: str = "skipped"            # done | failed | skipped
    reason: str = ""                   # retry_reason or skip reason
    method: str = "tiled"
    description: Optional[dict] = None  # merged result (if done)
    diagnostics: dict = field(default_factory=dict)
    cache_hit: bool = False


async def run_problem_block_tiled_retry(
    *,
    block_id: str,
    side_block: Optional[dict],
    baseline_item: dict,
    retry_reason: str,
    render_crop: Optional[Callable[..., Optional[Path]]],
    describe_fn: Callable[..., Awaitable[Any]],
    cfg: ProblemBlockRetryConfig,
    session_id: str = "",
    pair_id: str = "",
    side: str = "",
    model: str = "",
    cache_read: Optional[Callable[[str], Optional[dict]]] = None,
    cache_write: Optional[Callable[[str, dict], None]] = None,
) -> RetryOutcome:
    """Render the block high-res, tile it, run each tile through Qwen, merge.

    ``render_crop(block_id, target_long_side=...) -> Path`` and
    ``describe_fn(image_path, prompt) -> DescribeResult`` are the same callbacks
    ``enrich_side`` already holds, so this never imports the HTTP client. Never
    raises: failures return ``status='failed'`` with diagnostics."""
    t0 = time.monotonic()
    diag: dict[str, Any] = {
        "block_id": block_id, "retry_reason": retry_reason, "retry_method": "tiled",
        "tiles_count": 0, "tiles_done": 0, "tiles_failed": 0, "errors": [],
    }

    # 1) high-res image source
    if render_crop is None:
        diag["errors"].append("no_render_crop")
        return RetryOutcome(status="skipped", reason="no_input_source", diagnostics=diag)
    try:
        hi_path = _invoke_render(render_crop, block_id, cfg.render_long_side)
    except Exception as exc:  # noqa: BLE001
        diag["errors"].append(f"render_failed:{type(exc).__name__}:{exc}")
        return RetryOutcome(status="failed", reason="render_failed", diagnostics=diag)
    if hi_path is None or not Path(hi_path).exists():
        diag["errors"].append("highres_render_missing")
        return RetryOutcome(status="skipped", reason="no_input_source", diagnostics=diag)

    try:
        image_bytes = Path(hi_path).read_bytes()
    except OSError as exc:
        diag["errors"].append(f"read_failed:{exc}")
        return RetryOutcome(status="failed", reason="read_failed", diagnostics=diag)

    # 2) cache
    cache_key = ""
    if cfg.cache_enabled and cache_read is not None:
        cache_key = compute_retry_cache_key(
            session_id=session_id, pair_id=pair_id, side=side, block_id=block_id,
            image_bytes=image_bytes, cfg=cfg, model=model,
        )
        diag["cache_key"] = cache_key
        cached = None
        try:
            cached = cache_read(cache_key)
        except Exception:  # noqa: BLE001
            cached = None
        if cached and isinstance(cached.get("description"), dict):
            d = cached["diagnostics"] if isinstance(cached.get("diagnostics"), dict) else {}
            d["problem_block_retry_cache_hit"] = True
            return RetryOutcome(
                status="done", reason=retry_reason, method="tiled",
                description=cached["description"], diagnostics={**diag, **d},
                cache_hit=True,
            )

    # 3) skip tiling if the image is small (single fast call would have worked)
    try:
        from PIL import Image
        with Image.open(io.BytesIO(image_bytes)) as _im:
            iw, ih = _im.size
    except Exception:  # noqa: BLE001
        iw = ih = 0
    if max(iw, ih) < cfg.min_long_side_for_tiling:
        diag["errors"].append(f"too_small_for_tiling:{iw}x{ih}")
        return RetryOutcome(status="skipped", reason="too_small_for_tiling", diagnostics=diag)

    # 4) tile
    try:
        tiles, tmeta = split_image_into_tiles(
            image_bytes, tile_width=cfg.tile_width, tile_height=cfg.tile_height,
            overlap=cfg.tile_overlap, max_tiles=cfg.max_tiles,
        )
    except Exception as exc:  # noqa: BLE001
        diag["errors"].append(f"tile_split_failed:{type(exc).__name__}:{exc}")
        return RetryOutcome(status="failed", reason="tile_split_failed", diagnostics=diag)
    diag["tile_meta"] = tmeta
    diag["tiles_count"] = len(tiles)
    diag["tile_size"] = [cfg.tile_width, cfg.tile_height]
    diag["tile_overlap"] = cfg.tile_overlap
    diag["render_dpi"] = cfg.render_dpi
    if not tiles:
        diag["errors"].append("no_tiles")
        return RetryOutcome(status="failed", reason="no_tiles", diagnostics=diag)

    # 5) run each tile (fail-soft per tile; honor total-time budget)
    import tempfile
    tile_payloads: list[dict] = []
    tmpdir = Path(tempfile.mkdtemp(prefix="qwen_tile_"))
    try:
        for i, tb in enumerate(tiles):
            if time.monotonic() - t0 > cfg.max_total_sec_per_block:
                diag["errors"].append("total_time_budget_exceeded")
                break
            tile_path = tmpdir / f"tile_{i:02d}.png"
            try:
                tile_path.write_bytes(tb)
            except OSError as exc:
                diag["tiles_failed"] += 1
                diag["errors"].append(f"tile_write_{i}:{exc}")
                continue
            try:
                res = await describe_fn(tile_path, TILE_PROMPT)
            except Exception as exc:  # noqa: BLE001
                diag["tiles_failed"] += 1
                diag["errors"].append(f"tile_{i}:{type(exc).__name__}")
                continue
            parsed = getattr(res, "parsed", None)
            status = getattr(res, "status", None)
            if parsed and isinstance(parsed, dict):
                parsed = dict(parsed)
                parsed.setdefault("tile_id", f"tile_{i}")
                tile_payloads.append(parsed)
                diag["tiles_done"] += 1
            else:
                diag["tiles_failed"] += 1
                if getattr(res, "error", None):
                    diag["errors"].append(f"tile_{i}:{str(res.error)[:60]}")
    finally:
        try:
            for p in tmpdir.glob("*"):
                p.unlink()
            tmpdir.rmdir()
        except OSError:
            pass

    diag["retry_latency_sec"] = round(time.monotonic() - t0, 3)

    if diag["tiles_done"] == 0:
        diag["errors"].append("all_tiles_failed")
        return RetryOutcome(status="failed", reason=retry_reason, diagnostics=diag)

    # 6) merge
    merged = merge_tiled_qwen_results(tile_payloads, baseline=baseline_item.get("description"))
    merged["method_used"] = "tiled_retry"
    merged["status"] = "done"

    # 7) cache write
    if cfg.cache_enabled and cache_write is not None and cache_key:
        try:
            cache_write(cache_key, {"description": merged, "diagnostics": diag})
        except Exception:  # noqa: BLE001
            logger.debug("tiled retry cache_write failed", exc_info=True)

    return RetryOutcome(status="done", reason=retry_reason, method="tiled",
                        description=merged, diagnostics=diag, cache_hit=False)


def _invoke_render(render_crop: Callable[..., Optional[Path]], block_id: str,
                   target_long_side: int) -> Optional[Path]:
    """Call render_crop with the new (block_id, target_long_side=) contract,
    falling back to the legacy (block_id) one for test fakes."""
    try:
        return render_crop(block_id, target_long_side=int(target_long_side))
    except TypeError:
        return render_crop(block_id)


def _improved(baseline_item: dict, merged: Optional[dict]) -> bool:
    """Did the retry beat the baseline? More facts, or baseline was unusable."""
    if not isinstance(merged, dict):
        return False
    base_desc = baseline_item.get("description") if isinstance(baseline_item, dict) else None
    base_status = baseline_item.get("status")
    base_usable = baseline_item.get("usable_for_diff")
    merged_facts = _facts_total(merged)
    if merged_facts == 0:
        return False
    if base_status in ("error", "no_image_found", "timeout", "http_error"):
        return True
    if base_usable is False and merged.get("usable_for_diff"):
        return True
    return merged_facts > _facts_total(base_desc)


async def maybe_run_problem_block_retry(
    *,
    item: dict,
    side_block: Optional[dict],
    error: Optional[BaseException],
    render_crop: Optional[Callable[..., Optional[Path]]],
    describe_fn: Callable[..., Awaitable[Any]],
    cfg: Optional[ProblemBlockRetryConfig] = None,
    session_id: str = "",
    pair_id: str = "",
    side: str = "",
    model: str = "",
    cache_read: Optional[Callable[[str], Optional[dict]]] = None,
    cache_write: Optional[Callable[[str, dict], None]] = None,
) -> dict:
    """High-level entry used by ``enrich_side``. Mutates and returns ``item``.

    Contract:
      * if disabled or the block is fine → returns item unchanged (plus a small
        ``problem_block_retry`` diag stub when disabled-but-flagged is irrelevant);
      * if a problem is detected → preserves baseline under ``baseline_result``,
        runs tiled retry, and on improvement swaps in the merged description with
        ``method_used='tiled_retry'``; on failure keeps baseline + diagnostics;
      * never raises."""
    cfg = cfg or ProblemBlockRetryConfig.from_env()

    retry_required, reason = should_retry_problem_block(item, error, side_block or {}, cfg)
    if not retry_required:
        return item

    side_block_id = item.get("side_block_id") or item.get("md_block_id") or ""
    # All diagnostic fields are always present (defaults below) so downstream
    # consumers never have to guard against missing keys.
    diag_block = {
        "block_id": side_block_id,
        "retry_enabled": True,
        "retry_attempted": False,
        "retry_reason": reason,
        "retry_method": cfg.mode,
        "baseline_status": item.get("status"),
        "baseline_confidence": item.get("confidence_adjusted"),
        "baseline_usable_for_diff": item.get("usable_for_diff"),
        "tiles_count": 0,
        "tiles_done": 0,
        "tiles_failed": 0,
        "retry_status": "skipped",
        "retry_improved": False,
        "final_method_used": "baseline",
        "final_confidence": item.get("confidence_adjusted"),
        "final_usable_for_diff": item.get("usable_for_diff"),
        "cache_hit": False,
        "errors": [],
    }

    # preserve baseline raw output (never lose it)
    if "baseline_result" not in item:
        item["baseline_result"] = {
            "status": item.get("status"),
            "description": item.get("description"),
            "usable_for_diff": item.get("usable_for_diff"),
            "finish_reason": item.get("finish_reason"),
            "warnings": list(item.get("warnings") or []),
        }

    if not side_block_id or render_crop is None:
        diag_block["errors"].append("no_input_source")
        diag_block["retry_status"] = "skipped"
        item["problem_block_retry"] = diag_block
        return item

    diag_block["retry_attempted"] = True
    try:
        outcome = await run_problem_block_tiled_retry(
            block_id=side_block_id, side_block=side_block, baseline_item=item,
            retry_reason=reason, render_crop=render_crop, describe_fn=describe_fn,
            cfg=cfg, session_id=session_id, pair_id=pair_id, side=side, model=model,
            cache_read=cache_read, cache_write=cache_write,
        )
    except Exception as exc:  # noqa: BLE001 — never break the pipeline
        logger.warning("problem_block_retry raised (ignored): %s", exc, exc_info=True)
        diag_block["retry_status"] = "failed"
        diag_block["errors"].append(f"unexpected:{type(exc).__name__}")
        item["problem_block_retry"] = diag_block
        return item

    diag_block["retry_status"] = outcome.status
    diag_block["cache_hit"] = outcome.cache_hit
    diag_block.update({k: v for k, v in outcome.diagnostics.items()
                       if k in ("tiles_count", "tiles_done", "tiles_failed",
                                "retry_latency_sec", "errors", "cache_key")})

    if outcome.status == "done" and _improved(item, outcome.description):
        # swap in the improved description
        item["description"] = outcome.description
        item["status"] = "done"
        item["usable_for_diff"] = bool(outcome.description.get("usable_for_diff", True))
        item["method_used"] = "tiled_retry"
        item["baseline_method"] = "image_crop"
        item.setdefault("warnings", [])
        item["warnings"] = list(dict.fromkeys(item["warnings"] + ["recovered_by_tiled_retry"]))
        diag_block["retry_improved"] = True
        diag_block["final_method_used"] = "tiled_retry"
        diag_block["final_confidence"] = outcome.description.get("confidence")
        diag_block["final_usable_for_diff"] = item["usable_for_diff"]
        diag_block["tiles_count"] = outcome.diagnostics.get("tiles_count", 0)
    else:
        # keep baseline; record that retry didn't help
        item["method_used"] = "baseline"
        diag_block["retry_improved"] = False
        diag_block["final_method_used"] = "baseline"

    item["problem_block_retry"] = diag_block
    return item


def summarize_problem_block_retry(descriptions: list[dict], cfg: ProblemBlockRetryConfig) -> dict:
    """Pair/session-level summary from per-item ``problem_block_retry`` diags."""
    s = {
        "enabled": cfg.enabled,
        "blocks_checked": len(descriptions),
        "retry_attempted": 0,
        "retry_done": 0,
        "retry_failed": 0,
        "retry_skipped": 0,
        "improved": 0,
        "cache_hits": 0,
    }
    for d in descriptions:
        diag = d.get("problem_block_retry") if isinstance(d, dict) else None
        if not isinstance(diag, dict):
            continue
        if diag.get("retry_attempted"):
            s["retry_attempted"] += 1
        rs = diag.get("retry_status")
        if rs == "done":
            s["retry_done"] += 1
        elif rs == "failed":
            s["retry_failed"] += 1
        elif rs == "skipped":
            s["retry_skipped"] += 1
        if diag.get("retry_improved"):
            s["improved"] += 1
        if diag.get("cache_hit"):
            s["cache_hits"] += 1
    return s


__all__ = [
    "ProblemBlockRetryConfig",
    "should_retry_problem_block",
    "split_image_into_tiles",
    "merge_tiled_qwen_results",
    "compute_retry_cache_key",
    "run_problem_block_tiled_retry",
    "maybe_run_problem_block_retry",
    "summarize_problem_block_retry",
    "RetryOutcome",
    "TILE_PROMPT",
]
