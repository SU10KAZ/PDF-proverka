#!/usr/bin/env python3
"""Probe HYBRID-5: what a Vision crop actually costs, measured not assumed.

(a) renders real tight crops from the benchmark PDFs at several zoom levels;
(b) calls gpt-5.6-sol through the same codex CLI Track A used, once with no image
    and once per image, with an identical one-word prompt and reasoning=low, and
    reads `usage.input_tokens` from the `--json` turn.completed event;
(c) image cost = input_tokens(with image) - input_tokens(baseline).

    python -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p5_vision_crop_cost --render
    python -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p5_vision_crop_cost --measure
"""
from __future__ import annotations

import argparse
import json
import math
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TA = ROOT / "experiments" / "stage_comparison_vector_blocks" / "artifacts"
OUT = Path(__file__).resolve().parents[1] / "artifacts"
CROPS = OUT / "hybrid_crops"
CODEX = Path(
    "/home/coder/.vscode-server/extensions/openai.chatgpt-26.818.41705-linux-x64/bin/linux-x86_64/codex"
)
MODEL = "gpt-5.6-sol"
PROMPT = "Ответь одним словом: ок"

# Tight crops grounded in the two hybrid cases of the brief.
# (pair, side, sub-rectangle inside the block bbox_norm, label)
TIGHT = [
    ("vk_nodes", "right", (0.55, 0.62, 1.00, 1.00), "vk_nodes_notes_block"),
    ("ss_table_graphic", "right", (0.00, 0.55, 1.00, 1.00), "ss_table_extra_contour"),
    ("eom_singleline_changed", "right", (0.00, 0.00, 0.55, 0.45), "eom_branch_group"),
]
ZOOMS = (2.0, 4.0)


def render() -> list[dict]:
    import fitz

    CROPS.mkdir(parents=True, exist_ok=True)
    pairs = {p["pair_id"]: p for p in json.loads((TA / "block_pairs.json").read_text("utf-8"))["pairs"]}
    rows = []
    for pair_id, side, sub, label in TIGHT:
        spec = pairs[pair_id][side]
        doc = fitz.open(ROOT / spec["pdf"])
        page = doc[spec["page_index"]]
        pw, ph = page.rect.width, page.rect.height
        bx0, by0, bx1, by1 = spec["bbox_norm"]
        X0, Y0 = bx0 * pw, by0 * ph
        W, H = (bx1 - bx0) * pw, (by1 - by0) * ph
        rect = fitz.Rect(X0 + sub[0] * W, Y0 + sub[1] * H, X0 + sub[2] * W, Y0 + sub[3] * H)
        for zoom in ZOOMS:
            pix = page.get_pixmap(matrix=fitz.Matrix(zoom, zoom), clip=rect)
            path = CROPS / f"{label}_z{zoom:g}.png"
            pix.save(path)
            rows.append(
                {
                    "label": label,
                    "zoom": zoom,
                    "path": str(path),
                    "width": pix.width,
                    "height": pix.height,
                    "bytes": path.stat().st_size,
                }
            )
            print(rows[-1])
        doc.close()
    (OUT / "hybrid_crop_inventory.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2) + "\n", "utf-8")
    return rows


def patch_tokens(w: int, h: int, cap: int = 1536) -> int:
    """OpenAI patch model as commonly documented for the GPT-5 family:
    32x32 patches, image shrunk so the patch count fits `cap`. UNVERIFIED for
    gpt-5.6-sol; this probe measures the truth and compares."""
    n = math.ceil(w / 32) * math.ceil(h / 32)
    if n <= cap:
        return n
    shrink = math.sqrt(cap * 32 * 32 / (w * h))
    w2, h2 = w * shrink, h * shrink
    scale = min(math.floor(w2 / 32) * 32 / w2 if w2 else 1, math.floor(h2 / 32) * 32 / h2 if h2 else 1)
    w3, h3 = w2 * scale, h2 * scale
    return math.ceil(w3 / 32) * math.ceil(h3 / 32)


def call(images: list[Path]) -> dict:
    cmd = [
        str(CODEX), "exec", "--ephemeral", "--skip-git-repo-check", "--ignore-rules",
        "--sandbox", "read-only", "--model", MODEL,
        "-c", 'model_reasoning_effort="low"', "-C", "/tmp", "--json",
    ]
    for p in images:
        cmd += [f"--image={p}"]
    cmd.append(PROMPT)
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    usage = None
    for line in r.stdout.splitlines():
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        if ev.get("type") == "turn.completed":
            usage = ev["usage"]
    if usage is None:
        raise RuntimeError(f"no usage; rc={r.returncode} stderr={r.stderr[-800:]}")
    return usage


def measure() -> None:
    from PIL import Image

    inventory = json.loads((OUT / "hybrid_crop_inventory.json").read_text("utf-8"))
    targets: list[tuple[str, Path]] = []
    for name in ("ss_simple_node/left.png", "ss_table_graphic/left.png",
                 "eom_singleline_changed/left.png", "ar_plan/left.png", "ss_plan_dense/left.png"):
        targets.append((f"benchmark:{name}", TA / "diagnostics" / name))
    for row in inventory:
        targets.append((f"tight:{row['label']}_z{row['zoom']:g}", Path(row["path"])))

    baselines = [call([])["input_tokens"] for _ in range(3)]
    base = min(baselines)
    rows = []
    for label, path in targets:
        w, h = Image.open(path).size
        u = call([path])
        cost = u["input_tokens"] - base
        rows.append(
            {
                "label": label,
                "path": str(path),
                "width": w,
                "height": h,
                "png_bytes": path.stat().st_size,
                "input_tokens_total": u["input_tokens"],
                "measured_image_tokens": cost,
                "patch_model_prediction": patch_tokens(w, h),
                "raw_patches_uncapped": math.ceil(w / 32) * math.ceil(h / 32),
            }
        )
        print(rows[-1])
    res = {
        "codex_version": subprocess.run([str(CODEX), "--version"], capture_output=True, text=True).stdout.strip(),
        "model": MODEL,
        "reasoning_effort": "low",
        "baseline_input_tokens_runs": baselines,
        "baseline_used": base,
        "rows": rows,
    }
    (OUT / "hybrid_vision_crop_cost.json").write_text(json.dumps(res, ensure_ascii=False, indent=2) + "\n", "utf-8")
    print(json.dumps({k: v for k, v in res.items() if k != "rows"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--render", action="store_true")
    ap.add_argument("--measure", action="store_true")
    a = ap.parse_args()
    if a.render or not a.measure:
        render()
    if a.measure:
        measure()
