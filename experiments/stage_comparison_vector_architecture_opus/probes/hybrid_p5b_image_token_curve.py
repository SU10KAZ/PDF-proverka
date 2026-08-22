#!/usr/bin/env python3
"""Probe HYBRID-5b: image-token cost curve on synthetic squares of known size.

Isolates the size->token relation from image content. Same codex CLI, same model,
reasoning=low, identical one-word prompt; cost = input_tokens - baseline.

    <venv>/bin/python -m experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p5b_image_token_curve
"""
from __future__ import annotations

import json
import math
from pathlib import Path

from PIL import Image, ImageDraw

from experiments.stage_comparison_vector_architecture_opus.probes.hybrid_p5_vision_crop_cost import (
    call,
    CODEX,
    MODEL,
)

OUT = Path(__file__).resolve().parents[1] / "artifacts"
TMP = OUT / "hybrid_crops"
SIZES = [(320, 320), (640, 640), (1024, 1024), (1280, 1280), (2048, 2048), (2560, 2560)]


def make(w: int, h: int) -> Path:
    TMP.mkdir(parents=True, exist_ok=True)
    path = TMP / f"synthetic_{w}x{h}.png"
    im = Image.new("RGB", (w, h), "white")
    d = ImageDraw.Draw(im)
    for i in range(0, w, 64):
        d.line([(i, 0), (i, h)], fill="black")
    for j in range(0, h, 64):
        d.line([(0, j), (w, j)], fill="black")
    im.save(path)
    return path


def main() -> None:
    base = min(call([])["input_tokens"] for _ in range(2))
    rows = []
    for w, h in SIZES:
        p = make(w, h)
        u = call([p])
        raw = math.ceil(w / 32) * math.ceil(h / 32)
        rows.append(
            {
                "w": w, "h": h, "raw_patches": raw,
                "measured_image_tokens": u["input_tokens"] - base,
                "tokens_per_raw_patch": round((u["input_tokens"] - base) / raw, 4),
                "png_bytes": p.stat().st_size,
            }
        )
        print(rows[-1])
    res = {"model": MODEL, "reasoning_effort": "low", "baseline": base, "rows": rows}
    (OUT / "hybrid_image_token_curve.json").write_text(json.dumps(res, ensure_ascii=False, indent=2) + "\n", "utf-8")


if __name__ == "__main__":
    main()
