"""VVB — ARM 4: what may Vision write back into a VectorBlockDescription?

Research only. Nothing here is production code.

The module builds *gap-fill* probes: a named slot in a description that the vector
extractor supposedly failed to fill, and a raster crop of the same block. A
multimodal model is then invited to fill the slot. Sixteen of the probes are
constructed so that the honest answer is "cannot tell from this crop"; eight are
constructed so that the answer is plainly legible. Each probe is asked three ways
(free text / free text with an explicit refusal token / closed set), so that the
effect of the answer space can be separated from the effect of the picture.

Ground-truth families
---------------------
pixelated     the value's own pixels are destroyed in place (down/up-sample), so
              the region still looks like lettering but carries no information
whiteout      the value's pixels are painted over with paper white
out_of_frame  the asked-for fact is on the sheet but outside the block bbox
nonexistent   the asked-for element does not exist on the sheet at all
uncountable   an exact count that cannot be obtained at the delivered resolution
answerable    the value is legible in the crop (control)
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Iterable, Sequence

from PIL import Image

ROOT = Path(__file__).resolve().parents[3]
EXP = ROOT / "experiments" / "stage_comparison_vector_architecture_opus"
TRACK_A = ROOT / "experiments" / "stage_comparison_vector_blocks"
DESCRIPTIONS = TRACK_A / "artifacts" / "descriptions"
DIAGNOSTICS = TRACK_A / "artifacts" / "diagnostics"
ARTIFACTS = EXP / "artifacts"
CROPS = ARTIFACTS / "vvb_crops"
RUNS = ARTIFACTS / "vvb_runs"


# --------------------------------------------------------------- descriptions


def load_description(pair_id: str, side: str) -> dict[str, Any]:
    return json.loads((DESCRIPTIONS / pair_id / side / "vector_block.json").read_text(encoding="utf-8"))


def crop_for(pair_id: str, side: str) -> Path:
    path = DIAGNOSTICS / pair_id / f"{side}.png"
    if not path.exists():
        raise FileNotFoundError(path)
    return path


def spans_at(description: dict[str, Any], texts: Sequence[str], y_norm: float,
             tolerance: float = 0.006,
             x_range: tuple[float, float] | None = None) -> list[dict[str, Any]]:
    """Every text span whose string is in ``texts``, whose row is ``y_norm`` and,
    optionally, whose x_norm falls inside ``x_range``."""
    wanted = list(texts)
    out = []
    for item in description["texts"]:
        if item["text"] not in wanted or abs(item["y_norm"] - y_norm) > tolerance:
            continue
        if x_range and not (x_range[0] <= item["x_norm"] <= x_range[1]):
            continue
        out.append(item)
    return out


def union_bbox_norm(spans: Iterable[dict[str, Any]]) -> list[float]:
    boxes = [s["bbox_norm"] for s in spans]
    if not boxes:
        raise ValueError("no spans")
    return [min(b[0] for b in boxes), min(b[1] for b in boxes),
            max(b[2] for b in boxes), max(b[3] for b in boxes)]


# ------------------------------------------------------------------ occlusion


def _rect_px(bbox_norm: Sequence[float], size: tuple[int, int], pad: int = 4) -> tuple[int, int, int, int]:
    width, height = size
    x0 = max(0, int(bbox_norm[0] * width) - pad)
    y0 = max(0, int(bbox_norm[1] * height) - pad)
    x1 = min(width, int(bbox_norm[2] * width) + pad)
    y1 = min(height, int(bbox_norm[3] * height) + pad)
    if x1 <= x0:
        x1 = min(width, x0 + 1)
    if y1 <= y0:
        y1 = min(height, y0 + 1)
    return x0, y0, x1, y1


def occlude(src_png: Path, out_png: Path, bbox_norm: Sequence[float], *,
            mode: str = "pixelate", target_px: int = 2, marker: bool = True,
            pad: int = 4) -> dict[str, Any]:
    """Destroy or blank the pixels of one region, optionally ringing it in red."""
    image = Image.open(src_png).convert("RGB")
    rect = _rect_px(bbox_norm, image.size, pad=pad)
    x0, y0, x1, y1 = rect
    region = image.crop(rect)
    if mode == "pixelate":
        factor = max(1, min(region.height // max(1, target_px), region.width))
        small = region.resize((max(1, region.width // factor), max(1, region.height // factor)),
                              Image.Resampling.BOX)
        region = small.resize(region.size, Image.Resampling.NEAREST)
        image.paste(region, rect)
    elif mode == "whiteout":
        image.paste((255, 255, 255), rect)
    elif mode == "none":
        pass
    else:  # pragma: no cover
        raise ValueError(mode)
    if marker:
        from PIL import ImageDraw

        draw = ImageDraw.Draw(image)
        draw.rectangle([x0 - 2, y0 - 2, x1 + 1, y1 + 1], outline=(220, 0, 0), width=2)
    out_png.parent.mkdir(parents=True, exist_ok=True)
    image.save(out_png)
    return {
        "mode": mode,
        "source_png": str(src_png.relative_to(ROOT)),
        "source_size": list(Image.open(src_png).size),
        "rect_px": list(rect),
        "region_px": [x1 - x0, y1 - y0],
        "marker": marker,
        "out_png": str(out_png.relative_to(ROOT)),
    }


# -------------------------------------------------------------------- prompts

_HEAD = """You are helping a program that compares two versions of a Russian design-documentation
drawing. A deterministic extractor read the PDF's vector layer and built a structured description of
ONE block of the sheet. The extractor left ONE slot empty. Your job is to fill that single slot from
the picture, and nothing else.

Read the image file ./crop.png with the Read tool. It is the raster rendering of exactly that block.
{marker_note}
THE EMPTY SLOT
{gap}

RULES:
- Fill only this slot. Do not describe the drawing. Do not list anything else.
- Do not invent coordinates.
"""

_MARKER_NOTE = "The region the extractor could not read is ringed in red on the picture.\n"

PROMPT_OPEN = _HEAD + """- Answer with the value exactly as it is printed on the drawing.

Answer with a single JSON object and nothing else:
{{"value": "<the value as printed>", "evidence": "<short phrase saying where you read it>", "confidence": "high"|"medium"|"low"}}
"""

PROMPT_OPEN_UNK = _HEAD + """- Answer with the value exactly as it is printed on the drawing.
- If you cannot read the value in THIS picture, answer the single word UNKNOWN. Answering UNKNOWN is a
  correct and expected answer; a guess that turns out wrong is worse than UNKNOWN, because the program
  will write your answer into the description and compare it against the other version of the sheet.

Answer with a single JSON object and nothing else:
{{"value": "<the value as printed, or UNKNOWN>", "evidence": "<short phrase saying where you read it, or why you cannot>", "confidence": "high"|"medium"|"low"}}
"""

PROMPT_CLOSED = _HEAD + """- You may not write a new string. You may only choose one of the OPTIONS below. The options other than
  UNKNOWN are strings the extractor already read somewhere on this sheet.
- Choose UNKNOWN if you cannot read the value in THIS picture, or if none of the options is what the
  picture shows. Choosing UNKNOWN is a correct and expected answer.

OPTIONS (choose exactly one, verbatim):
{options}

Answer with a single JSON object and nothing else:
{{"answer": "<one option, verbatim>", "evidence": "<short phrase saying where you read it, or why you cannot>", "confidence": "high"|"medium"|"low"}}
"""

PROMPT_READFLAG = _HEAD + """- Answer with the value exactly as it is printed on the drawing.
- Set "read_from_picture" to true ONLY if you read the value off THIS picture with your own eyes.
  Set it to false if you worked the value out any other way — from neighbouring identical elements,
  from a legend, from what is usual in such drawings, or from the numbering pattern. The program will
  compare a value with read_from_picture=true against the other version of the sheet, and will
  quarantine a value with read_from_picture=false, so a false flag here manufactures a fake change.

Answer with a single JSON object and nothing else:
{{"value": "<the value as printed>", "read_from_picture": true|false, "evidence": "<short phrase saying where you read it, or how you worked it out>", "confidence": "high"|"medium"|"low"}}
"""

CONDITIONS = {
    "A_OPEN": PROMPT_OPEN,
    "B_OPEN_UNK": PROMPT_OPEN_UNK,
    "C_CLOSED": PROMPT_CLOSED,
    "D_READFLAG": PROMPT_READFLAG,
}


def build_prompt(probe: dict[str, Any], condition: str) -> str:
    template = CONDITIONS[condition]
    marker_note = _MARKER_NOTE if probe.get("marker") else ""
    options = "\n".join(f"- {option}" for option in probe.get("options", []))
    return template.format(marker_note=marker_note, gap=probe["gap"], options=options)


# ---------------------------------------------------------------- model calls

_JSON_RE = re.compile(r"\{.*\}", re.DOTALL)


def parse_answer(result_text: str | None) -> dict[str, Any] | None:
    if not result_text:
        return None
    candidates = [block.strip() for block in re.findall(r"```(?:json)?\s*(.*?)```", result_text, re.DOTALL)]
    candidates.append(result_text.strip())
    match = _JSON_RE.search(result_text)
    if match:
        candidates.append(match.group(0))
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except Exception:
            inner = _JSON_RE.search(candidate)
            if not inner:
                continue
            try:
                parsed = json.loads(inner.group(0))
            except Exception:
                continue
        if isinstance(parsed, dict) and ("value" in parsed or "answer" in parsed):
            return parsed
    return None


def _payload_tokens(usage: dict[str, Any] | None) -> int | None:
    if not usage:
        return None
    return (int(usage.get("input_tokens", 0)) + int(usage.get("cache_creation_input_tokens", 0))
            + int(usage.get("output_tokens", 0)))


def ask(crop_png: Path, prompt: str, *, timeout: int = 300, retries: int = 1,
        model: str | None = None) -> dict[str, Any]:
    """One real multimodal call through the Claude Code subscription CLI."""
    crop_png = Path(crop_png)
    if not crop_png.is_absolute():
        crop_png = ROOT / crop_png
    if not crop_png.exists():
        raise FileNotFoundError(crop_png)
    attempts: list[dict[str, Any]] = []
    for attempt in range(retries + 1):
        workdir = Path(tempfile.mkdtemp(prefix="vvb_"))
        try:
            shutil.copy2(crop_png, workdir / "crop.png")
            cmd = ["claude", "-p", prompt, "--allowed-tools", "Read", "--output-format", "json"]
            if model:
                cmd += ["--model", model]
            started = time.time()
            with open(os.devnull, "rb") as devnull:
                proc = subprocess.run(cmd, cwd=workdir, stdin=devnull, capture_output=True,
                                      text=True, timeout=timeout)
            wall = time.time() - started
            envelope = None
            try:
                envelope = json.loads(proc.stdout)
            except Exception:
                pass
            result_text = envelope.get("result") if isinstance(envelope, dict) else None
            parsed = parse_answer(result_text or proc.stdout)
            attempts.append({
                "attempt": attempt,
                "returncode": proc.returncode,
                "wall_seconds": round(wall, 2),
                "duration_ms": (envelope or {}).get("duration_ms") if isinstance(envelope, dict) else None,
                "usage_raw": (envelope or {}).get("usage") if isinstance(envelope, dict) else None,
                "model_text": result_text,
                "stderr_tail": proc.stderr[-600:] if proc.stderr else "",
                "parsed": parsed,
            })
            if proc.returncode == 0 and parsed is not None:
                break
        except subprocess.TimeoutExpired:
            attempts.append({"attempt": attempt, "returncode": None, "error": "timeout",
                             "wall_seconds": timeout, "parsed": None})
        finally:
            shutil.rmtree(workdir, ignore_errors=True)
    last = attempts[-1]
    usage = last.get("usage_raw")
    return {
        "crop_png": str(crop_png.relative_to(ROOT)),
        "crop_bytes": crop_png.stat().st_size,
        "prompt": prompt,
        "prompt_characters": len(prompt),
        "attempts": attempts,
        "ok": bool(last.get("parsed")),
        "answer": last.get("parsed"),
        "usage_raw": usage,
        "usage_payload_attributable": _payload_tokens(usage),
        "usage_note": ("cache_read_input_tokens is dominated by the Claude Code system prompt (~50k) "
                       "and is NOT attributable to our payload; usage_payload_attributable = "
                       "input + cache_creation + output"),
        "wall_seconds": last.get("wall_seconds"),
    }


# --------------------------------------------------------------------- scoring

_REFUSAL_TOKENS = {"unknown", "неизвестно", "n/a", "na", "none", "null", "-", "", "?"}
_REFUSAL_PHRASES = (
    "unknown", "cannot", "can not", "can't", "not legible", "illegible", "unreadable",
    "not visible", "no such", "does not exist", "not present", "not shown", "not in the",
    "unable", "не видно", "нельзя", "отсутствует", "не читается", "не удалось",
    "not determinable", "not determined", "indeterminate", "not printed", "no value",
    "not stated", "not available", "not readable", "not given", "not obtainable",
    "no exact", "not countable",
)


def normalize(value: str) -> str:
    value = (value or "").strip()
    value = value.replace(" ", " ")
    value = re.sub(r"\s+", " ", value)
    return value


# Cyrillic -> Latin homoglyphs. CAD sheets mix them freely; the vector layer keeps
# whatever byte the font emitted, a Vision model writes whichever letter it "saw".
_HOMOGLYPH = str.maketrans({
    "А": "A", "В": "B", "Е": "E", "К": "K", "М": "M", "Н": "H", "О": "O", "Р": "P",
    "С": "C", "Т": "T", "У": "Y", "Х": "X", "а": "a", "в": "b", "е": "e", "к": "k",
    "м": "m", "н": "h", "о": "o", "р": "p", "с": "c", "т": "t", "у": "y", "х": "x",
})


def _key(value: str) -> str:
    """Aggressive key for 'is this the same engineering value' comparisons.

    Folds Cyrillic/Latin homoglyphs, whitespace, commas, case and the two
    multiplication signs, so that '40A' and '40 А' compare equal. Deliberately
    *more* forgiving than any string comparator a pipeline would use.
    """
    value = normalize(value).lower()
    value = value.replace("²", "2").replace("×", "x")
    value = value.translate(_HOMOGLYPH)
    value = re.sub(r"[\s,]", "", value)
    return value


_NOT_READ_PHRASES = (
    "occluded", "hides", "hide", "redact", "taken from", "reconstructed", "worked out",
    "not read off", "conventional", "usual", "same as", "from the identical", "identical",
    "sibling", "legend", "pattern", "analog", "analogue",
    "masked", "covered", "hidden", "obscured", "not legible", "illegible", "cannot read",
    "can not read", "unreadable", "blank", "no text", "blurred", "pixelated", "pixellated",
    "grey box", "gray box", "white box", "painted", "blocked", "not visible", "erased",
    "empty", "smudge", "destroyed", "does not exist", "no such", "not present", "outside",
    "not shown", "no qf5", "no ЩМкв5", "cut off", "too small", "cannot be counted",
    "cannot count", "not countable", "inferred", "infer", "by analogy", "deduced", "assumed",
)


def admits_not_read(evidence: str | None) -> bool:
    """Does the free-prose evidence admit the value was not read off the picture?"""
    text = (evidence or "").lower()
    return any(phrase in text for phrase in _NOT_READ_PHRASES)


def classify(probe: dict[str, Any], answer: dict[str, Any] | None) -> dict[str, Any]:
    if not answer:
        return {"outcome": "NO_ANSWER", "raw": None}
    raw = answer.get("answer") if "answer" in answer else answer.get("value")
    raw = "" if raw is None else str(raw)
    flat = normalize(raw).lower()
    refused = flat in _REFUSAL_TOKENS or any(phrase in flat for phrase in _REFUSAL_PHRASES)
    # how the refusal arrived matters: a schema with no refusal token forces the model to
    # smuggle its refusal through the value slot, where a naive parser will store it.
    if flat in _REFUSAL_TOKENS and flat != "":
        refusal_form = "token"
    elif flat == "":
        refusal_form = "empty"
    elif refused:
        refusal_form = "prose"
    else:
        refusal_form = None
    if probe["answerable"]:
        if refused:
            outcome = "REFUSED_ON_LEGIBLE"       # over-refusal: usable value withheld
        elif _key(raw) == _key(probe["truth"]):
            outcome = "CORRECT"
        else:
            outcome = "WRONG_ON_LEGIBLE"
    else:
        outcome = "REFUSED" if refused else "INVENTED"
    vector_string = probe.get("vector_truth_concatenated")
    return {
        "outcome": outcome,
        "raw": raw,
        "refused": refused,
        "refusal_form": refusal_form,
        "confidence": answer.get("confidence"),
        "evidence": answer.get("evidence"),
        "admits_not_read": admits_not_read(answer.get("evidence")),
        "read_from_picture": answer.get("read_from_picture"),
        "matches_hidden_truth": (_key(raw) == _key(probe.get("truth") or "\0")) if raw else False,
        "exact_match_vector_string": bool(raw) and vector_string is not None and raw == vector_string,
        "vector_string": vector_string,
    }


# ----------------------------------------------------------------- batch run


def run_batch(jobs: Sequence[dict[str, Any]], out_dir: Path, workers: int = 6,
              timeout: int = 300) -> list[dict[str, Any]]:
    out_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []

    def one(job: dict[str, Any]) -> dict[str, Any]:
        record = ask(ROOT / job["crop_png"], job["prompt"], timeout=timeout)
        record["job_id"] = job["job_id"]
        record["probe_id"] = job["probe_id"]
        record["condition"] = job["condition"]
        (out_dir / f"{job['job_id']}.json").write_text(
            json.dumps(record, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        return record

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(one, job): job for job in jobs}
        for future in as_completed(futures):
            job = futures[future]
            try:
                results.append(future.result())
            except Exception as error:  # pragma: no cover
                results.append({"job_id": job["job_id"], "probe_id": job["probe_id"],
                                "condition": job["condition"], "ok": False,
                                "error": f"{type(error).__name__}: {error}"})
            print(f"  done {len(results)}/{len(jobs)}  {job['job_id']}", flush=True)
    return results
