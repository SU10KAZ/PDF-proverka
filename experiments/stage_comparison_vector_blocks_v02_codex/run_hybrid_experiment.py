#!/usr/bin/env python3
"""Compare Vector-only, Vision-only and targeted Hybrid on ten hard pairs."""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

import fitz

from .benchmark_data import REPOSITORY_ROOT, benchmark_manifest
from .l3_change_only import payload_metrics


EXPERIMENT_DIR = Path(__file__).resolve().parent
ARTIFACTS = EXPERIMENT_DIR / "artifacts"
SCHEMA = EXPERIMENT_DIR / "ai_output_schema.json"
MODEL = "gpt-5.6-sol"
PAIR_IDS = (
    "ss_scheme_text_changed",
    "ss_plan_dense",
    "ss_crop_mismatch_page07",
    "ar_plan",
    "ar_plan_page08",
    "vk_plan",
    "vk_nodes",
    "vk_axono_page17",
    "eom_singleline_changed",
    "ov_plan_floor07",
)


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _base_prompt(arm: str) -> str:
    return f"""Ты сравниваешь 10 пар инженерных графических блоков, LEFT — старая версия, RIGHT — новая.
Режим входа: {arm}. Для каждой пары верни ровно один classification, только фактические изменения,
короткое evidence и uncertainties. Не считай разную границу crop реальным изменением конструкции.
Не выдумывай неразборчивые подписи. Не открывай файлы и не вызывай инструменты: используй только
текст этого prompt и приложенные изображения. Верни только JSON по заданной схеме.
"""


def _render_images(directory: Path, pairs: dict[str, dict[str, Any]]) -> dict[str, list[Path]]:
    result = {}
    for pair_id in PAIR_IDS:
        pair = pairs[pair_id]; paths = []
        for side_name in ("left", "right"):
            side = pair[side_name]
            pdf = Path(side["pdf"]); pdf = pdf if pdf.is_absolute() else REPOSITORY_ROOT / pdf
            document = fitz.open(pdf); page = document[side["page_index"]]; rect = page.rect
            bbox = side["bbox_norm"]
            clip = fitz.Rect(bbox[0] * rect.width, bbox[1] * rect.height, bbox[2] * rect.width, bbox[3] * rect.height)
            pixmap = page.get_pixmap(matrix=fitz.Matrix(1.25, 1.25), clip=clip, alpha=False)
            path = directory / f"{pair_id}-{side_name}.png"; pixmap.save(path); document.close(); paths.append(path)
        result[pair_id] = paths
    return result


def _question(payload: dict[str, Any]) -> str:
    routing = payload["quality_route"]
    failed = set(routing["failed_gates"])
    if "crop_ok" in failed:
        return "Есть crop mismatch. Проверь, unmatched contour — реальное изменение или различие границы crop; затем проверь только крупные изменения внутри общей области."
    if "text_ok" in failed:
        return "Geometry evidence доступно, но vector text сломан/неполон. Проверь только подписи, числовые значения и видимые добавления/удаления геометрии."
    if "caps_ok" in failed or "topology_ok" in failed:
        return "Vector geometry/topology capped. Проверь только возможные пропущенные ветви, линии и крупные структурные изменения."
    return "Проверь только uncertainties из vector diff и не пересказывай совпадающие элементы."


def _prompts(routing: dict[str, Any]) -> dict[str, str]:
    rows = {row["pair_id"]: row for row in routing["pairs"]}
    vector = [_base_prompt("VECTOR_ONLY: только L3_CHANGE_ONLY; raster отсутствует.")]
    vision = [_base_prompt("VISION_ONLY: только raster crops; vector evidence отсутствует."), "Изображения приложены в порядке:"]
    hybrid = [_base_prompt("HYBRID: raster crops + короткий L3_CHANGE_ONLY + узкий вопрос.")]
    ordinal = 1
    for pair_id in PAIR_IDS:
        payload = rows[pair_id]["l3_change_only"]
        vector.append(json.dumps({"pair_id": pair_id, "l3_change_only": payload}, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        vision.append(f"{ordinal}. {pair_id} LEFT; {ordinal + 1}. {pair_id} RIGHT")
        hybrid.append(json.dumps({"pair_id": pair_id, "vector_diff": payload, "target_question": _question(payload)}, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        ordinal += 2
    return {"vector": "\n\n".join(vector) + "\n", "vision": "\n".join(vision) + "\n", "hybrid": "\n\n".join(hybrid) + "\n"}


def _usage(events: list[dict[str, Any]], stderr: str) -> dict[str, int | None]:
    found: dict[str, int] = {}
    def visit(value: Any) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                normalized = key.lower()
                if normalized in {"input_tokens", "output_tokens", "cached_input_tokens", "total_tokens"} and isinstance(item, int):
                    found[normalized] = max(found.get(normalized, 0), item)
                visit(item)
        elif isinstance(value, list):
            for item in value: visit(item)
    visit(events)
    match = re.search(r"tokens used\s+([\d\s\u00a0]+)", stderr)
    total_cli = int(re.sub(r"\D", "", match.group(1))) if match else None
    return {
        "input_tokens": found.get("input_tokens"), "cached_input_tokens": found.get("cached_input_tokens"),
        "output_tokens": found.get("output_tokens"), "total_tokens": found.get("total_tokens") or total_cli,
    }


def _invoke(prompt: str, images: list[Path], work: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    output = work / "last-message.json"
    command = [
        "codex", "exec", "--ephemeral", "--skip-git-repo-check", "--ignore-rules", "--sandbox", "read-only",
        "--model", MODEL, "--output-schema", str(SCHEMA), "--json", "--output-last-message", str(output), "-C", str(work),
    ]
    for image in images:
        command.extend(("--image", str(image)))
    command.append("-")
    started = time.perf_counter()
    completed = subprocess.run(command, input=prompt, text=True, capture_output=True, timeout=1200, check=False)
    latency = time.perf_counter() - started
    if completed.returncode:
        raise RuntimeError(f"codex exec failed: {completed.stderr[-6000:]}")
    events = []
    for line in completed.stdout.splitlines():
        try: events.append(json.loads(line))
        except json.JSONDecodeError: pass
    result = json.loads(output.read_text(encoding="utf-8"))
    metadata = {
        "model": MODEL, "latency_seconds": round(latency, 6), "image_count": len(images),
        "prompt": payload_metrics(prompt), "usage": _usage(events, completed.stderr),
        "returncode": completed.returncode, "stderr_tail": completed.stderr[-2000:],
    }
    return result, metadata


def run(arms: tuple[str, ...]) -> dict[str, Any]:
    manifest = benchmark_manifest(); pairs = {pair["pair_id"]: pair for pair in manifest["pairs"]}
    routing = _load(ARTIFACTS / "routing_results.json"); prompts = _prompts(routing)
    existing = _load(ARTIFACTS / "hybrid_results.json") if (ARTIFACTS / "hybrid_results.json").is_file() else {
        "schema_version": "vector-hybrid-experiment-v0.2-codex", "model": MODEL,
        "pair_ids": list(PAIR_IDS), "arms": {}, "human_evaluation": None,
    }
    with tempfile.TemporaryDirectory(prefix="vector-hybrid-") as directory:
        work = Path(directory); images_by_pair = _render_images(work, pairs)
        all_images = [path for pair_id in PAIR_IDS for path in images_by_pair[pair_id]]
        for arm in arms:
            prompt = prompts[arm]
            images = [] if arm == "vector" else all_images
            result, metadata = _invoke(prompt, images, work)
            existing["arms"][arm] = {"metadata": metadata, "output": result}
            print(arm, metadata["latency_seconds"], metadata["usage"], flush=True)
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    (ARTIFACTS / "hybrid_results.json").write_text(json.dumps(existing, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return existing


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument("--arm", choices=("vector", "vision", "hybrid", "all"), default="all")
    args = parser.parse_args(); arms = ("vector", "vision", "hybrid") if args.arm == "all" else (args.arm,)
    run(arms)
