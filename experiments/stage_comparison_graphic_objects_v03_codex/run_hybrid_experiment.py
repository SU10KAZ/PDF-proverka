#!/usr/bin/env python3
"""One fused graphic-only Hybrid call for ten routed hard pairs."""
from __future__ import annotations

import json
import re
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

import fitz

from backend.app.services.common.pdf_crop import extract_block_crop, open_pdf

from .benchmark_data import benchmark_manifest, ground_truth_artifact
from .input_contract import REPOSITORY_ROOT, resolve_prepared_block


EXPERIMENT_DIR = Path(__file__).resolve().parent
ARTIFACTS = EXPERIMENT_DIR / "artifacts"
SCHEMA = EXPERIMENT_DIR / "hybrid_output.schema.json"
MODEL = "gpt-5.6-sol"
PAIR_IDS = (
    "ss_scheme_text_changed", "ss_plan_dense", "ss_simple_node",
    "ss_crop_mismatch_page07", "ar_plan", "ar_plan_page08", "vk_plan",
    "vk_nodes", "vk_axono_page17", "ov_plan_floor07",
)


def _render(work: Path, pairs: dict[str, dict[str, Any]]) -> list[Path]:
    images = []
    for pair_id in PAIR_IDS:
        pair = pairs[pair_id]
        for side_name, scope_key in (("left", "left_blocks"), ("right", "right_blocks")):
            block = resolve_prepared_block(pair["scope"][scope_key][0])
            crop_pdf = work / f"{pair_id}-{side_name}.pdf"; image = work / f"{pair_id}-{side_name}.png"
            document = open_pdf(block["source_pdf_path"])
            try:
                extract_block_crop(document, block["page_index"], block["coords_norm"], crop_pdf)
            finally:
                document.close()
            cropped = fitz.open(crop_pdf)
            try:
                page = cropped[0]; pixmap = page.get_pixmap(matrix=fitz.Matrix(1.25, 1.25), alpha=False)
                if pixmap.width > 2600 or pixmap.height > 2600:
                    scale = min(2600 / pixmap.width, 2600 / pixmap.height)
                    pixmap = page.get_pixmap(matrix=fitz.Matrix(1.25 * scale, 1.25 * scale), alpha=False)
                pixmap.save(image)
            finally:
                cropped.close()
            images.append(image)
    return images


def _prompt(routing: dict[str, Any]) -> str:
    routed = {row["pair_id"]: row for row in routing["pairs"]}
    lines = [
        "Ты выполняешь ОДИН fused Hybrid comparison для 10 пар уже подготовленных графических блоков.",
        "Картинки приложены строго в порядке pair_id LEFT, затем RIGHT, указанном ниже.",
        "Сравнивай ТОЛЬКО графические объекты: линии, контуры, символы, ветви, соединения, положение и стиль.",
        "Игнорируй изменения текста, чисел, подписей и содержимого таблиц: для них есть отдельные pipelines.",
        "Разную границу crop, padding и CAD primitive packaging не считай проектным изменением.",
        "Vector candidates шумные и являются только адресной подсказкой, не ground truth. Верни JSON по schema.",
    ]
    for ordinal, pair_id in enumerate(PAIR_IDS, 1):
        row = routed[pair_id]; counts = row["change_status_counts"]
        samples = []
        for candidate in (row.get("hybrid_packet") or {}).get("vector_found", [])[:3]:
            samples.append({key: candidate.get(key) for key in ("type", "left_object", "right_object", "confidence")})
        lines.append(json.dumps({"ordinal": ordinal, "pair_id": pair_id, "images": [f"{2*ordinal-1}:LEFT", f"{2*ordinal}:RIGHT"], "vector_candidate_counts": counts, "candidate_samples": samples, "specific_uncertainty": row["route_reasons"]}, ensure_ascii=False, separators=(",", ":")))
    return "\n".join(lines) + "\n"


def _usage(events: list[dict[str, Any]], stderr: str) -> dict[str, int | None]:
    found: dict[str, int] = {}
    def visit(value: Any) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                if key.lower() in {"input_tokens", "output_tokens", "cached_input_tokens", "total_tokens"} and isinstance(item, int):
                    found[key.lower()] = max(found.get(key.lower(), 0), item)
                visit(item)
        elif isinstance(value, list):
            for item in value: visit(item)
    visit(events); match = re.search(r"tokens used\s+([\d\s\u00a0]+)", stderr)
    cli_total = int(re.sub(r"\D", "", match.group(1))) if match else None
    calculated = (found.get("input_tokens") or 0) + (found.get("output_tokens") or 0)
    return {"input_tokens": found.get("input_tokens"), "cached_input_tokens": found.get("cached_input_tokens"), "output_tokens": found.get("output_tokens"), "total_tokens": found.get("total_tokens") or cli_total or calculated or None}


def _evaluate(output: dict[str, Any]) -> dict[str, Any]:
    truth = {row["pair_id"]: row["expected_graphic_verdict"] for row in ground_truth_artifact()["pairs"]}
    rows = []
    for result in output["pairs"]:
        expected = truth[result["pair_id"]]; scored = expected != "UNSURE"
        rows.append({"pair_id": result["pair_id"], "expected": expected, "actual": result["classification"], "scored": scored, "correct": scored and expected == result["classification"]})
    scored_rows = [row for row in rows if row["scored"]]
    return {"rows": rows, "correct": sum(row["correct"] for row in scored_rows), "scored": len(scored_rows), "accuracy": round(sum(row["correct"] for row in scored_rows) / max(len(scored_rows), 1), 6), "ground_truth_policy": "manual graphic-only labels; UNSURE excluded"}


def run() -> dict[str, Any]:
    manifest = benchmark_manifest(); pairs = {row["pair_id"]: row for row in manifest["pairs"]}
    routing = json.loads((ARTIFACTS / "routing_results.json").read_text(encoding="utf-8")); prompt = _prompt(routing)
    with tempfile.TemporaryDirectory(prefix="graphic-v03-hybrid-") as directory:
        work = Path(directory); images = _render(work, pairs); output_path = work / "last-message.json"
        command = ["codex", "exec", "--ephemeral", "--skip-git-repo-check", "--ignore-rules", "--sandbox", "read-only", "--model", MODEL, "--output-schema", str(SCHEMA), "--json", "--output-last-message", str(output_path), "-C", str(work)]
        for image in images: command.extend(("--image", str(image)))
        command.append("-"); started = time.perf_counter()
        completed = subprocess.run(command, input=prompt, text=True, capture_output=True, timeout=1200, check=False)
        latency = time.perf_counter() - started
        if completed.returncode:
            raise RuntimeError(completed.stderr[-8000:])
        events = []
        for line in completed.stdout.splitlines():
            try: events.append(json.loads(line))
            except json.JSONDecodeError: pass
        output = json.loads(output_path.read_text(encoding="utf-8"))
    result = {"schema_version": "graphic-hybrid-results-v0.3-codex", "research_only": True, "architecture": "one fused call; no mandatory verifier and no old L3", "model": MODEL, "pair_ids": list(PAIR_IDS), "metadata": {"latency_seconds": round(latency, 6), "image_count": len(images), "prompt_chars": len(prompt), "estimated_prompt_tokens_chars_over_4": round(len(prompt) / 4), "usage": _usage(events, completed.stderr), "stderr_tail": completed.stderr[-2000:]}, "output": output, "human_evaluation": _evaluate(output)}
    (ARTIFACTS / "hybrid_results.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


if __name__ == "__main__":
    value = run(); print(json.dumps(value["metadata"], ensure_ascii=False)); print(json.dumps(value["human_evaluation"], ensure_ascii=False))
