#!/usr/bin/env python3
"""Vision verification of VectorBlockDescription before pair comparison."""
from __future__ import annotations

import copy
import json
import re
import statistics
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

import fitz

from .benchmark_data import REPOSITORY_ROOT, benchmark_manifest
from .extractor import PageCache, extract_block
from .l3_change_only import payload_metrics


EXPERIMENT_DIR = Path(__file__).resolve().parent
ARTIFACT = EXPERIMENT_DIR / "artifacts/verification_results.json"
ROUTING_ARTIFACT = EXPERIMENT_DIR / "artifacts/routing_results.json"
SCHEMA = EXPERIMENT_DIR / "verification_output_schema.json"
MODEL = "gpt-5.6-sol"
REAL_PAIR_IDS = (
    "ss_detail_page17", "vk_plan", "ar_plan", "eom_singleline_changed",
    "ss_crop_mismatch_page07", "ov_equipment_table",
)
NEW_BLOCKS = (
    ("new_gp_road_detail", "experiments/блоки разных дисциплин/ГП/ГП — 023 конструкция дорожной одежды — послойная конструкция покрытия — GP-ROAD-08.pdf"),
    ("new_kj_embedded_detail", "experiments/блоки разных дисциплин/КЖ/КЖ — 005 закладные детали — узел закладной детали — 7XYM-GXMD-RDW.pdf"),
    ("new_km_connection", "experiments/блоки разных дисциплин/КМ/КМ — 008 узел соединения — соединение стальных элементов — PTKX-YJL3-UFH.pdf"),
)

# Fixed after manual raster/description inspection, independently of the model
# verdict.  The EOM crop leaks non-visible title-block spans into the vector
# description; the GP sheet contains visible outlined/rasterized labels that
# the PDF text layer does not expose.
HUMAN_STATUS_OVERRIDES = {
    "real_eom_singleline_changed_left": (
        "PARTIAL",
        "description contains non-visible title-block/signature spans at the crop boundary",
    ),
    "new_gp_road_detail": (
        "PARTIAL",
        "visible title and upper layer labels are absent from the PDF text extraction",
    ),
}


def _compact(description: dict[str, Any]) -> dict[str, Any]:
    topology_keys = ("segments_total", "node_count", "edge_count", "connected_components", "endpoints", "branch_points", "t_junctions", "x_crossings_unconnected", "closed_contours")
    texts = [item["text"] for item in description["texts"]]
    return {
        "vector_quality": description["vector_quality"],
        "text_quality": description["text_quality"],
        "cap_flags": description["cap_flags"],
        "primitive_summary": description["primitive_summary"],
        "content_extent": description["content_extent"],
        "labels": texts[:100], "labels_truncated": len(texts) > 100,
        "engineering_values": [item["text"] for item in description["dimensions"][:60]],
        "topology": {key: description["topology"][key] for key in topology_keys},
        "repeated_patterns": [{"count": item["count"], "primitive_type": item["primitive_type"], "segment_count": item["segment_count"]} for item in description["repeated_elements"][:30]],
        "declared_facts": {
            "text_span_count": len(texts),
            "repeated_group_count": len(description["repeated_elements"]),
            "main_connected_components": description["topology"]["connected_components"],
        },
        "provenance_rule": "Vision may verify/reject/fill gaps, but must not invent coordinates or unsupported exact geometry.",
    }


def _render(pdf: Path, page_index: int, bbox: list[float], output: Path) -> None:
    document = fitz.open(pdf); page = document[page_index]; rect = page.rect
    clip = fitz.Rect(bbox[0] * rect.width, bbox[1] * rect.height, bbox[2] * rect.width, bbox[3] * rect.height)
    pixmap = page.get_pixmap(matrix=fitz.Matrix(1.25, 1.25), clip=clip, alpha=False)
    pixmap.save(output); document.close()


def _fixture(path: Path, *, other: bool = False) -> None:
    document = fitz.open(); page = document.new_page(width=420, height=300)
    shape = page.new_shape()
    if other:
        shape.draw_circle(fitz.Point(210, 150), 90); shape.finish(color=(0, 0, 0), width=2); shape.commit()
        page.insert_text(fitz.Point(170, 155), "OTHER CROP", fontsize=14)
    else:
        shape.draw_line(fitz.Point(40, 45), fitz.Point(380, 45))
        for index in range(14):
            x = 55 + index * 24
            shape.draw_line(fitz.Point(x, 45), fitz.Point(x, 220))
            shape.draw_circle(fitz.Point(x, 130), 5)
        shape.finish(color=(0, 0, 0), width=0.8); shape.commit()
        for index in range(14):
            page.insert_text(fitz.Point(44 + index * 24, 245), f"QF{index + 1}", fontsize=7)
        page.insert_text(fitz.Point(35, 25), "2 inputs · 14 branches", fontsize=10)
    document.save(path); document.close()


def _expected(description: dict[str, Any]) -> str:
    if description["vector_quality"] == "VECTOR_DATA_INSUFFICIENT":
        return "FAILED"
    if description["text_quality"]["status"] != "TEXT_GOOD" or any(description["cap_flags"].values()):
        return "PARTIAL"
    return "VERIFIED"


def _samples(work: Path) -> list[dict[str, Any]]:
    manifest = benchmark_manifest(); pairs = {pair["pair_id"]: pair for pair in manifest["pairs"]}
    cache = PageCache(EXPERIMENT_DIR / ".page_cache")
    samples = []
    for pair_id in REAL_PAIR_IDS:
        pair = pairs[pair_id]
        for side_name in ("left", "right"):
            side = pair[side_name]; pdf = Path(side["pdf"]); pdf = pdf if pdf.is_absolute() else REPOSITORY_ROOT / pdf
            description = extract_block(pdf, page_index=side["page_index"], bbox_norm=side["bbox_norm"], block_id=side["block_id"], page_cache=cache)
            image = work / f"{pair_id}-{side_name}.png"; _render(pdf, side["page_index"], side["bbox_norm"], image)
            sample_id = f"real_{pair_id}_{side_name}"
            default_status = _expected(description)
            expected_status, reason = HUMAN_STATUS_OVERRIDES.get(
                sample_id, (default_status, "deterministic quality flags")
            )
            samples.append({"sample_id": sample_id, "group": "existing_real", "expected_status": expected_status, "expected_reason": reason, "description": _compact(description), "image": image})
    for sample_id, relative in NEW_BLOCKS:
        pdf = REPOSITORY_ROOT / relative
        description = extract_block(pdf, page_index=0, bbox_norm=(0, 0, 1, 1), block_id=sample_id, page_cache=cache)
        image = work / f"{sample_id}.png"; _render(pdf, 0, [0, 0, 1, 1], image)
        default_status = _expected(description)
        expected_status, reason = HUMAN_STATUS_OVERRIDES.get(
            sample_id, (default_status, "deterministic quality flags")
        )
        samples.append({"sample_id": sample_id, "group": "new_real", "expected_status": expected_status, "expected_reason": reason, "description": _compact(description), "image": image})

    fixture_pdf, other_pdf = work / "fixture.pdf", work / "other.pdf"; _fixture(fixture_pdf); _fixture(other_pdf, other=True)
    fixture_description = extract_block(fixture_pdf, page_index=0, bbox_norm=(0, 0, 1, 1), block_id="controlled-fixture", page_cache=PageCache(work / "fixture-cache"))
    compact = _compact(fixture_description); compact["declared_facts"].update({"principal_branches": 14, "input_count": 2, "expected_labels": [f"QF{i}" for i in range(1, 15)]})
    fixture_image, other_image = work / "fixture.png", work / "other.png"; _render(fixture_pdf, 0, [0, 0, 1, 1], fixture_image); _render(other_pdf, 0, [0, 0, 1, 1], other_image)
    variants: list[tuple[str, str, dict[str, Any], Path]] = [("controlled_correct", "VERIFIED", copy.deepcopy(compact), fixture_image)]
    removed = copy.deepcopy(compact); removed["declared_facts"]["principal_branches"] = 13; removed["declared_facts"]["expected_labels"].remove("QF14"); variants.append(("controlled_removed_element", "PARTIAL", removed, fixture_image))
    wrong_count = copy.deepcopy(compact); wrong_count["declared_facts"]["principal_branches"] = 18; variants.append(("controlled_wrong_count", "PARTIAL", wrong_count, fixture_image))
    missing_label = copy.deepcopy(compact); missing_label["labels"] = [x for x in missing_label["labels"] if x != "QF7"]; missing_label["declared_facts"]["expected_labels"].remove("QF7"); variants.append(("controlled_missing_label", "PARTIAL", missing_label, fixture_image))
    wrong_topology = copy.deepcopy(compact); wrong_topology["topology"]["branch_points"] += 10; variants.append(("controlled_wrong_topology", "PARTIAL", wrong_topology, fixture_image))
    broken = copy.deepcopy(compact); broken["labels"] = ["���", "���"]; broken["text_quality"]["status"] = "TEXT_BROKEN"; variants.append(("controlled_broken_text", "PARTIAL", broken, fixture_image))
    capped = copy.deepcopy(compact); capped["cap_flags"]["segments_capped"] = True; capped["cap_flags"]["topology_capped"] = True; capped["labels"] = capped["labels"][:4]; variants.append(("controlled_capped_geometry", "PARTIAL", capped, fixture_image))
    variants.append(("controlled_wrong_crop", "FAILED", copy.deepcopy(compact), other_image))
    samples.extend({"sample_id": sample_id, "group": "controlled", "expected_status": expected, "expected_reason": "controlled corruption ground truth", "description": description, "image": image} for sample_id, expected, description, image in variants)
    return samples


def _prompt(samples: list[dict[str, Any]]) -> str:
    rows = ["""Ты — Vision verifier структурированного VectorBlockDescription. Для каждого sample одновременно даны raster crop и compact description.
Не описывай чертёж заново. Проверь, соответствует ли description важной видимой структуре. VERIFIED: достаточно полно/корректно.
PARTIAL: основа верна, но есть отсутствующая/ненадёжная часть. FAILED: существенное несоответствие или другой crop.
Vision может подтверждать, отвергать и называть ограниченные missing facts, но не создавать координаты или точные численные geometry facts без provenance.
Не вызывай инструменты и не открывай файлы. Images приложены в том же порядке, что samples. Верни только JSON по схеме."""]
    for index, sample in enumerate(samples, 1):
        rows.append(json.dumps({"image_ordinal": index, "sample_id": sample["sample_id"], "vector_description": sample["description"]}, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
    return "\n\n".join(rows) + "\n"


def _usage(events: list[dict[str, Any]], stderr: str) -> dict[str, Any]:
    found: dict[str, int] = {}
    def visit(value: Any) -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                if key.lower() in {"input_tokens", "output_tokens", "cached_input_tokens", "total_tokens"} and isinstance(item, int): found[key.lower()] = max(found.get(key.lower(), 0), item)
                visit(item)
        elif isinstance(value, list):
            for item in value: visit(item)
    visit(events); match = re.search(r"tokens used\s+([\d\s\u00a0]+)", stderr)
    return {"input_tokens": found.get("input_tokens"), "cached_input_tokens": found.get("cached_input_tokens"), "output_tokens": found.get("output_tokens"), "total_tokens": found.get("total_tokens") or (int(re.sub(r"\D", "", match.group(1))) if match else None)}


def _invoke(samples: list[dict[str, Any]], work: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    prompt = _prompt(samples); output = work / "verification-output.json"
    command = ["codex", "exec", "--ephemeral", "--skip-git-repo-check", "--ignore-rules", "--sandbox", "read-only", "--model", MODEL, "--output-schema", str(SCHEMA), "--json", "--output-last-message", str(output), "-C", str(work)]
    for sample in samples: command.extend(("--image", str(sample["image"])))
    command.append("-"); started = time.perf_counter(); completed = subprocess.run(command, input=prompt, text=True, capture_output=True, timeout=1200, check=False); latency = time.perf_counter() - started
    if completed.returncode: raise RuntimeError(completed.stderr[-6000:])
    events=[]
    for line in completed.stdout.splitlines():
        try: events.append(json.loads(line))
        except json.JSONDecodeError: pass
    return json.loads(output.read_text(encoding="utf-8")), {"latency_seconds": round(latency,6), "prompt": payload_metrics(prompt), "usage": _usage(events,completed.stderr), "image_count":len(samples), "model":MODEL}


def _pipeline_analysis(samples: list[dict[str, Any]], outputs: list[dict[str, Any]], metadata: list[dict[str, Any]]) -> dict[str, Any]:
    output_map = {row["sample_id"]: row for batch in outputs for row in batch["samples"]}
    rows=[]
    for sample in samples:
        actual=output_map[sample["sample_id"]]["status"]; expected=sample["expected_status"]
        rows.append({"sample_id":sample["sample_id"],"group":sample["group"],"expected_status":expected,"expected_reason":sample["expected_reason"],"actual_status":actual,"correct":expected==actual,**{key:output_map[sample["sample_id"]][key] for key in ("verified_facts","missing_facts","suspicious_facts","evidence","limitations")}})
    routing=json.loads(ROUTING_ARTIFACT.read_text(encoding="utf-8")); block_qualities=[(row["quality"][side+"_vector"],row["quality"][side+"_text"]["status"],row["quality"][side+"_caps"]) for row in routing["pairs"] for side in ("left","right")]
    risky=sum(vector!="GOOD" or text!="TEXT_GOOD" or any(caps.values()) for vector,text,caps in block_qualities); total=len(block_qualities)
    real=[row for row in rows if row["group"]!="controlled"]; controlled=[row for row in rows if row["group"]=="controlled"]
    input_total=sum((item["usage"].get("input_tokens") or item["prompt"]["estimated_tokens"]) for item in metadata); output_total=sum(item["usage"].get("output_tokens") or 0 for item in metadata); latency_total=sum(item["latency_seconds"] for item in metadata)
    per_sample_input=input_total/len(samples); per_sample_output=output_total/len(samples); per_sample_latency=latency_total/len(samples)
    diff_calls=sum(row["actual_route"]!="VECTOR_OK" for row in routing["pairs"])
    return {
        "status_counts": dict(sorted(__import__("collections").Counter(row["actual_status"] for row in real).items())),
        "real_accuracy": round(sum(row["correct"] for row in real)/len(real),6),
        "controlled_exact_status_accuracy": round(sum(row["correct"] for row in controlled)/len(controlled),6),
        "controlled_corruption_detection": round(sum(row["actual_status"]!="VERIFIED" for row in controlled if row["sample_id"]!="controlled_correct")/sum(row["sample_id"]!="controlled_correct" for row in controlled),6),
        "controlled_failed_detected": any(row["sample_id"]=="controlled_wrong_crop" and row["actual_status"]=="FAILED" for row in controlled),
        "benchmark_blocks": total, "risky_blocks": risky, "high_confidence_blocks": total-risky,
        "pipelines": {
            "A_diff_vision_only": {"verification_calls":0,"diff_vision_calls":diff_calls,"total_vision_calls":diff_calls,"estimated_verification_input_tokens":0,"estimated_verification_latency_seconds":0},
            "B_verify_all_then_diff": {"verification_calls":total,"diff_vision_calls":diff_calls,"total_vision_calls":total+diff_calls,"estimated_verification_input_tokens":round(per_sample_input*total),"estimated_verification_output_tokens":round(per_sample_output*total),"estimated_verification_latency_seconds":round(per_sample_latency*total,3)},
            "C_verify_risky_then_diff": {"verification_calls":risky,"diff_vision_calls":diff_calls,"total_vision_calls":risky+diff_calls,"estimated_verification_input_tokens":round(per_sample_input*risky),"estimated_verification_output_tokens":round(per_sample_output*risky),"estimated_verification_latency_seconds":round(per_sample_latency*risky,3)},
        },
        "interpretation": "Verify-all spends Vision on high-confidence blocks. Risk-only covers every block already rejected by deterministic standalone gates; measured verifier benefit is error localization/completion before comparison, not permission to overwrite exact vector geometry.",
    }, rows


def run() -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="vector-verify-") as directory:
        work=Path(directory); samples=_samples(work); groups=([row for row in samples if row["group"]=="existing_real"],[row for row in samples if row["group"]!="existing_real"])
        outputs=[];metadata=[]
        for group in groups:
            output,meta=_invoke(group,work);outputs.append(output);metadata.append(meta);print(group[0]["group"],len(group),meta,flush=True)
        analysis,rows=_pipeline_analysis(samples,outputs,metadata)
    result={"schema_version":"vector-description-verification-v0.2-codex","research_only":True,"model":MODEL,"metadata":metadata,"samples":rows,"analysis":analysis}
    ARTIFACT.parent.mkdir(parents=True,exist_ok=True);ARTIFACT.write_text(json.dumps(result,ensure_ascii=False,indent=2,sort_keys=True)+"\n",encoding="utf-8");return result


if __name__=="__main__":
    result=run();print(json.dumps(result["analysis"],ensure_ascii=False,indent=2))
