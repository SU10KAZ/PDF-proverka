#!/usr/bin/env python3
"""Evaluate Chandra OpenAI-compatible vision models on existing project blocks.

The runner is intentionally experiment-only:
- reads existing blocks/index.json and optional reused audit_set_block_ids.json;
- does not modify _output/blocks or production pipeline defaults;
- can render temporary native-resolution crops from the original PDF into the
  experiment directory, preserving the original block source untouched;
- sends one block per request to avoid batch-quality confounds.
"""

from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import re
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import fitz
from dotenv import load_dotenv
from openai import OpenAI


DEFAULT_MODELS = [
    "qwen/qwen3.6-35b-a3b",
    "qwen/qwen3.6-27b",
    "google/gemma-4-31b",
]

PROMPT = """Ты эксперт по КЖ-чертежам. Проанализируй один графический блок чертежа.

Нужно проверить не только OCR, но и визуальный смысл схемы: элементы, связи, размеры,
армирование, отметки, таблицы, противоречия и потенциальные замечания.

Верни строго JSON:
{
  "summary": "конкретное описание блока, не общие слова",
  "drawing_type": "plan|section|detail|schedule|table|notes|unknown",
  "key_values_read": ["размеры, марки, отметки, шаги, классы, позиции"],
  "relationships": ["как элементы связаны друг с другом"],
  "possible_issues": [
    {
      "severity": "critical|check|recommendation",
      "issue": "конкретное потенциальное замечание",
      "evidence": "что именно видно в блоке",
      "confidence": "high|medium|low"
    }
  ],
  "unreadable_parts": ["что не удалось прочитать"],
  "quality_notes": "оценка: хватает ли качества изображения для вывода"
}

Не выдумывай. Если не видно, так и напиши. Не объявляй ошибку только потому, что
не хватает контекста соседних листов; помечай такие случаи как check."""


@dataclass
class BlockImage:
    block_id: str
    page: int
    label: str
    source_file: Path
    image_file: Path
    width: int
    height: int
    size_kb: float
    native_crop: bool


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _safe_model_dir(model: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", model).strip("_")


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _find_project_file(project_dir: Path, pattern: str) -> Path:
    matches = sorted(project_dir.glob(pattern))
    if not matches:
        raise FileNotFoundError(f"No {pattern} found in {project_dir}")
    return matches[0]


def _load_blocks_index(project_dir: Path) -> list[dict[str, Any]]:
    index_path = project_dir / "_output" / "blocks" / "index.json"
    if not index_path.exists():
        raise FileNotFoundError(f"Missing blocks index: {index_path}")
    data = _load_json(index_path)
    blocks = data.get("blocks") if isinstance(data, dict) else data
    if not isinstance(blocks, list):
        raise ValueError(f"Unsupported index format: {index_path}")
    return blocks


def _find_reused_audit_set(project_dir: Path) -> Path | None:
    candidates = sorted(project_dir.glob("_experiments/**/audit_set_block_ids.json"))
    return candidates[-1] if candidates else None


def _load_block_ids(project_dir: Path, explicit_ids: list[str] | None, max_blocks: int) -> tuple[list[str], str]:
    if explicit_ids:
        return explicit_ids[:max_blocks], "explicit_cli"

    reused = _find_reused_audit_set(project_dir)
    if reused:
        payload = _load_json(reused)
        if isinstance(payload, list):
            ids = payload
        elif isinstance(payload, dict):
            ids = payload.get("block_ids") or payload.get("audit_set_block_ids") or payload.get("ids") or []
        else:
            ids = []
        if ids:
            return [str(x) for x in ids[:max_blocks]], str(reused)

    blocks = _load_blocks_index(project_dir)
    ranked = sorted(
        blocks,
        key=lambda b: (
            float(b.get("size_kb", 0) or 0) * 2
            + int(b.get("ocr_text_len", 0) or 0) / 100
            + max(b.get("render_size") or [0, 0]) / 50
        ),
        reverse=True,
    )
    return [b["block_id"] for b in ranked[:max_blocks]], "heuristic_top_complexity"


def _page_dimensions(result_json: Path) -> dict[int, tuple[int, int]]:
    data = _load_json(result_json)
    pages = data.get("pages", []) if isinstance(data, dict) else []
    dims: dict[int, tuple[int, int]] = {}
    for page in pages:
        try:
            dims[int(page["page_number"])] = (int(page["width"]), int(page["height"]))
        except Exception:
            continue
    return dims


def _source_blocks_by_id(result_json: Path) -> dict[str, dict[str, Any]]:
    data = _load_json(result_json)
    pages = data.get("pages", []) if isinstance(data, dict) else []
    out: dict[str, dict[str, Any]] = {}
    for page in pages:
        for block in page.get("blocks", []) or []:
            bid = block.get("id")
            if bid:
                out[str(bid)] = block
    return out


def _render_crop_pdf_bytes(
    *,
    pdf_bytes: bytes,
    block: dict[str, Any],
    out_path: Path,
    max_long_side: int,
) -> tuple[int, int]:
    coords = block.get("crop_px") or block.get("coords_px") or []
    if len(coords) == 4:
        x1, y1, x2, y2 = [float(v) for v in coords]
        target_long = max(max(1.0, x2 - x1), max(1.0, y2 - y1))
    else:
        target_long = 0.0
    if max_long_side > 0 and target_long > 0:
        target_long = min(target_long, float(max_long_side))

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    try:
        page = doc[0]
        long_side_pt = max(page.rect.width, page.rect.height)
        if long_side_pt < 1:
            raise ValueError(f"Zero-size crop PDF for {block.get('id') or block.get('block_id')}")
        render_scale = (target_long / long_side_pt) if target_long > 0 else 300 / 72
        render_scale = max(0.05, min(12.0, render_scale))
        pix = page.get_pixmap(matrix=fitz.Matrix(render_scale, render_scale), alpha=False)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pix.save(str(out_path))
        return pix.width, pix.height
    finally:
        doc.close()


def _download_and_render_crop_url(
    *,
    crop_url: str,
    block: dict[str, Any],
    pdf_out_path: Path,
    png_out_path: Path,
    max_long_side: int,
) -> tuple[int, int]:
    req = urllib.request.Request(crop_url, headers={"User-Agent": "chandra_vision_eval/1.0"})
    with urllib.request.urlopen(req, timeout=45) as resp:
        pdf_bytes = resp.read()
    pdf_out_path.parent.mkdir(parents=True, exist_ok=True)
    pdf_out_path.write_bytes(pdf_bytes)
    return _render_crop_pdf_bytes(
        pdf_bytes=pdf_bytes,
        block=block,
        out_path=png_out_path,
        max_long_side=max_long_side,
    )


def _render_native_crop(
    *,
    pdf_path: Path,
    page_dims: dict[int, tuple[int, int]],
    block: dict[str, Any],
    out_path: Path,
    max_long_side: int,
) -> tuple[int, int]:
    page_num = int(block["page"])
    coords = block.get("crop_px")
    if not coords or len(coords) != 4:
        raise ValueError(f"Block {block['block_id']} has no crop_px")
    if page_num not in page_dims:
        raise ValueError(f"No page dimensions for page {page_num}")

    x1, y1, x2, y2 = [float(v) for v in coords]
    crop_w_px = max(1.0, x2 - x1)
    crop_h_px = max(1.0, y2 - y1)
    target_long = max(crop_w_px, crop_h_px)
    if max_long_side > 0:
        target_long = min(target_long, float(max_long_side))

    doc = fitz.open(str(pdf_path))
    try:
        page = doc[page_num - 1]
        page_w_px, page_h_px = page_dims[page_num]
        scale_x = page.rect.width / page_w_px
        scale_y = page.rect.height / page_h_px
        clip = fitz.Rect(x1 * scale_x, y1 * scale_y, x2 * scale_x, y2 * scale_y)
        long_side_pt = max(clip.width, clip.height)
        if long_side_pt < 1:
            raise ValueError(f"Zero-size crop for {block['block_id']}")
        render_scale = target_long / long_side_pt
        render_scale = max(0.05, min(12.0, render_scale))
        pix = page.get_pixmap(matrix=fitz.Matrix(render_scale, render_scale), clip=clip, alpha=False)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pix.save(str(out_path))
        return pix.width, pix.height
    finally:
        doc.close()


def _prepare_images(
    project_dir: Path,
    exp_dir: Path,
    blocks: list[dict[str, Any]],
    block_ids: list[str],
    native_crops: bool,
    native_max_long_side: int,
) -> list[BlockImage]:
    by_id = {b["block_id"]: b for b in blocks}
    missing = [bid for bid in block_ids if bid not in by_id]
    if missing:
        raise ValueError(f"Block ids not found in index: {missing}")

    pdf_path = _find_project_file(project_dir, "*.pdf")
    result_json = _find_project_file(project_dir, "*_result.json")
    dims = _page_dimensions(result_json)
    source_blocks = _source_blocks_by_id(result_json)
    blocks_dir = project_dir / "_output" / "blocks"
    native_dir = exp_dir / "native_crops"
    crop_pdf_dir = exp_dir / "downloaded_crop_pdfs"

    prepared: list[BlockImage] = []
    for bid in block_ids:
        block = by_id[bid]
        src = blocks_dir / block["file"]
        image_path = src
        width, height = [int(x) for x in block.get("render_size") or [0, 0]]
        native = False
        if native_crops:
            image_path = native_dir / f"block_{bid}_native.png"
            source_block = source_blocks.get(bid, {})
            crop_url = source_block.get("crop_url")
            try:
                if crop_url:
                    width, height = _download_and_render_crop_url(
                        crop_url=str(crop_url),
                        block=source_block | {"block_id": bid},
                        pdf_out_path=crop_pdf_dir / f"{bid}.pdf",
                        png_out_path=image_path,
                        max_long_side=native_max_long_side,
                    )
                else:
                    raise ValueError("crop_url missing")
            except Exception as exc:
                print(f"[WARN] {bid}: crop_url download/render failed ({exc}); falling back to local PDF crop")
                width, height = _render_native_crop(
                    pdf_path=pdf_path,
                    page_dims=dims,
                    block=block,
                    out_path=image_path,
                    max_long_side=native_max_long_side,
                )
            native = True
        elif not image_path.exists():
            raise FileNotFoundError(f"Missing block image: {image_path}")

        prepared.append(BlockImage(
            block_id=bid,
            page=int(block.get("page", 0) or 0),
            label=str(block.get("ocr_label") or block.get("label") or ""),
            source_file=src,
            image_file=image_path,
            width=width,
            height=height,
            size_kb=round(image_path.stat().st_size / 1024, 1),
            native_crop=native,
        ))
    return prepared


def _build_client(request_timeout: float) -> OpenAI:
    load_dotenv()
    base = os.environ.get("CHANDRA_BASE_URL", "").rstrip("/")
    user = os.environ.get("NGROK_AUTH_USER", "")
    password = os.environ.get("NGROK_AUTH_PASS", "")
    missing = [k for k, v in {
        "CHANDRA_BASE_URL": base,
        "NGROK_AUTH_USER": user,
        "NGROK_AUTH_PASS": password,
    }.items() if not v]
    if missing:
        raise RuntimeError(f"Missing env vars: {', '.join(missing)}")
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return OpenAI(
        base_url=f"{base}/v1",
        api_key="lm-studio",
        default_headers={
            "Authorization": f"Basic {token}",
            "ngrok-skip-browser-warning": "true",
        },
        timeout=request_timeout,
    )


def _image_data_url(path: Path) -> str:
    raw = base64.b64encode(path.read_bytes()).decode()
    return f"data:image/png;base64,{raw}"


def _parse_jsonish(text: str) -> tuple[dict[str, Any] | None, str | None]:
    s = (text or "").strip()
    if not s:
        return None, "empty"
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s)
        s = re.sub(r"\s*```$", "", s)
    try:
        value = json.loads(s)
        return value if isinstance(value, dict) else {"value": value}, None
    except Exception:
        pass
    m = re.search(r"\{.*\}", s, re.S)
    if m:
        try:
            value = json.loads(m.group(0))
            return value if isinstance(value, dict) else {"value": value}, None
        except Exception as exc:
            return None, f"json_parse_error: {exc}"
    return None, "json_not_found"


def _call_model(client: OpenAI, model: str, block_image: BlockImage, max_tokens: int, request_timeout: float) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "text", "text": PROMPT},
                    {"type": "image_url", "image_url": {"url": _image_data_url(block_image.image_file)}},
                ],
            }],
            temperature=0,
            max_tokens=max_tokens,
            timeout=request_timeout,
        )
        elapsed = time.perf_counter() - started
        text = response.choices[0].message.content or ""
        parsed, parse_error = _parse_jsonish(text)
        usage = getattr(response, "usage", None)
        return {
            "ok": True,
            "model": model,
            "block_id": block_image.block_id,
            "elapsed_s": round(elapsed, 3),
            "raw_text": text,
            "parsed": parsed,
            "parse_error": parse_error,
            "usage": usage.model_dump() if usage is not None else None,
        }
    except Exception as exc:
        elapsed = time.perf_counter() - started
        return {
            "ok": False,
            "model": model,
            "block_id": block_image.block_id,
            "elapsed_s": round(elapsed, 3),
            "error_type": type(exc).__name__,
            "error": str(exc)[:2000],
        }


def _count_list(parsed: dict[str, Any] | None, key: str) -> int:
    if not isinstance(parsed, dict):
        return 0
    value = parsed.get(key)
    return len(value) if isinstance(value, list) else 0


def _write_outputs(
    exp_dir: Path,
    project_dir: Path,
    models: list[str],
    images: list[BlockImage],
    results: list[dict[str, Any]],
    block_id_source: str,
    native_max_long_side: int,
) -> None:
    by_result = {(r["model"], r["block_id"]): r for r in results}
    rows = []
    for image in images:
        for model in models:
            result = by_result.get((model, image.block_id), {})
            parsed = result.get("parsed") if result.get("ok") else None
            rows.append({
                "model": model,
                "block_id": image.block_id,
                "page": image.page,
                "ok": result.get("ok", False),
                "elapsed_s": result.get("elapsed_s", 0),
                "image_width": image.width,
                "image_height": image.height,
                "image_kb": image.size_kb,
                "native_crop": image.native_crop,
                "json_ok": bool(parsed),
                "key_values_count": _count_list(parsed, "key_values_read"),
                "relationships_count": _count_list(parsed, "relationships"),
                "possible_issues_count": _count_list(parsed, "possible_issues"),
                "unreadable_count": _count_list(parsed, "unreadable_parts"),
                "parse_error": result.get("parse_error") or "",
                "error": result.get("error", ""),
            })

    _save_json(exp_dir / "manifest.json", {
        "project_dir": str(project_dir),
        "models": models,
        "block_count": len(images),
        "request_count": len(images) * len(models),
        "block_id_source": block_id_source,
        "native_crops": any(i.native_crop for i in images),
        "native_max_long_side": native_max_long_side,
        "prompt": PROMPT,
        "created_at": datetime.now().isoformat(),
    })
    _save_json(exp_dir / "test_set_block_ids.json", [i.block_id for i in images])
    _save_json(exp_dir / "image_manifest.json", [i.__dict__ | {
        "source_file": str(i.source_file),
        "image_file": str(i.image_file),
    } for i in images])
    _save_json(exp_dir / "run_summary.json", rows)

    with (exp_dir / "run_summary.csv").open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [])
        if rows:
            writer.writeheader()
            writer.writerows(rows)

    lines = [
        "# Chandra Vision Model Eval",
        "",
        f"- Project: `{project_dir}`",
        f"- Blocks: {len(images)}",
        f"- Models: {', '.join(f'`{m}`' for m in models)}",
        f"- Block id source: `{block_id_source}`",
        f"- Native crops: `{any(i.native_crop for i in images)}`",
        "",
        "## Per Model Summary",
        "",
        "| Model | OK | JSON OK | Avg KV | Avg relationships | Avg issues | Total elapsed s |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for model in models:
        mr = [r for r in rows if r["model"] == model]
        ok = sum(1 for r in mr if r["ok"])
        jok = sum(1 for r in mr if r["json_ok"])
        avg_kv = sum(r["key_values_count"] for r in mr) / len(mr) if mr else 0
        avg_rel = sum(r["relationships_count"] for r in mr) / len(mr) if mr else 0
        avg_issues = sum(r["possible_issues_count"] for r in mr) / len(mr) if mr else 0
        elapsed = sum(float(r["elapsed_s"] or 0) for r in mr)
        lines.append(f"| `{model}` | {ok}/{len(mr)} | {jok}/{len(mr)} | {avg_kv:.1f} | {avg_rel:.1f} | {avg_issues:.1f} | {elapsed:.1f} |")
    (exp_dir / "run_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")

    side = ["# Side By Side", ""]
    for image in images:
        side.extend([
            f"## {image.block_id} — page {image.page}",
            "",
            f"- Label: {image.label}",
            f"- Image: `{image.image_file}`",
            f"- Size: {image.width}x{image.height}, {image.size_kb} KB",
            "",
        ])
        for model in models:
            result = by_result.get((model, image.block_id), {})
            parsed = result.get("parsed") if result.get("ok") else None
            side.append(f"### `{model}`")
            if not result.get("ok"):
                side.append(f"ERROR: {result.get('error_type')} — {result.get('error')}")
            elif parsed:
                side.append(f"- Summary: {parsed.get('summary', '')}")
                side.append(f"- KV: {_count_list(parsed, 'key_values_read')}")
                side.append(f"- Relationships: {_count_list(parsed, 'relationships')}")
                side.append(f"- Issues: {_count_list(parsed, 'possible_issues')}")
                if parsed.get("possible_issues"):
                    for issue in parsed.get("possible_issues", [])[:5]:
                        side.append(f"  - {issue}")
            else:
                side.append(f"Parse issue: {result.get('parse_error')}")
                side.append((result.get("raw_text") or "")[:1200])
            side.append("")
    (exp_dir / "side_by_side.md").write_text("\n".join(side), encoding="utf-8")

    rec = [
        "# Winner Recommendation",
        "",
        "This file is a first-pass machine summary. Do not choose a winner by counts only.",
        "Review `side_by_side.md` for semantic quality: concrete dimensions, correct element relationships, low hallucination, and useful issue evidence.",
        "",
    ]
    successful_models = []
    for model in models:
        mr = [r for r in rows if r["model"] == model]
        ok = sum(1 for r in mr if r["ok"])
        jok = sum(1 for r in mr if r["json_ok"])
        successful_models.append((jok, ok, model))
    successful_models.sort(reverse=True)
    if successful_models:
        rec.append(f"Preliminary operational candidate by response validity: `{successful_models[0][2]}`.")
    (exp_dir / "winner_recommendation.md").write_text("\n".join(rec) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--project-dir", required=True)
    ap.add_argument("--models", nargs="+", default=DEFAULT_MODELS)
    ap.add_argument("--block-id", action="append", dest="block_ids")
    ap.add_argument("--max-blocks", type=int, default=12)
    ap.add_argument("--native-crops", action="store_true", help="Render temporary crop PNGs at crop_px/native size from PDF.")
    ap.add_argument("--native-max-long-side", type=int, default=0, help="Optional safety cap; 0 means no downscale cap.")
    ap.add_argument("--max-tokens", type=int, default=1800)
    ap.add_argument("--request-timeout", type=float, default=180)
    ap.add_argument("--prepare-only", action="store_true")
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    project_dir = Path(args.project_dir).resolve()
    exp_dir = project_dir / "_experiments" / "chandra_vision_model_eval" / _ts()
    exp_dir.mkdir(parents=True, exist_ok=True)

    blocks = _load_blocks_index(project_dir)
    block_ids, block_id_source = _load_block_ids(project_dir, args.block_ids, args.max_blocks)
    images = _prepare_images(
        project_dir=project_dir,
        exp_dir=exp_dir,
        blocks=blocks,
        block_ids=block_ids,
        native_crops=args.native_crops,
        native_max_long_side=args.native_max_long_side,
    )

    if args.prepare_only:
        _write_outputs(exp_dir, project_dir, args.models, images, [], block_id_source, args.native_max_long_side)
        print(f"Prepared images only: {exp_dir}")
        return 0

    client = _build_client(args.request_timeout)
    results: list[dict[str, Any]] = []
    for model in args.models:
        model_dir = exp_dir / "model_outputs" / _safe_model_dir(model)
        model_dir.mkdir(parents=True, exist_ok=True)
        for image in images:
            out_path = model_dir / f"{image.block_id}.json"
            if args.resume and out_path.exists():
                result = _load_json(out_path)
            else:
                print(f"[{model}] {image.block_id} {image.width}x{image.height} {image.size_kb}KB")
                result = _call_model(client, model, image, args.max_tokens, args.request_timeout)
                _save_json(out_path, result)
            results.append(result)

    _write_outputs(exp_dir, project_dir, args.models, images, results, block_id_source, args.native_max_long_side)
    print(f"Artifacts: {exp_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
