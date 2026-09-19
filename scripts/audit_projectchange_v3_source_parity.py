#!/usr/bin/env python3
"""Field-level parity: production ProjectChange V3 source packaging vs frozen A/B.

Zero model calls.  Reads only the frozen V3 experiment inputs (source admission,
page records, structure, exact prompts/model inputs, semantic maps, miner and
dedupe outputs).  Never opens validation, final holdout or truth/evaluation
files.  Production packaging is rebuilt into a scratch work directory; the
frozen experiment is read-only.

Usage:
    python scripts/audit_projectchange_v3_source_parity.py --work <scratch> --out <json>
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from backend.app.services.project_change_v3 import contracts, dedupe, provider, source_prep  # noqa: E402

CORPUS = Path("/home/coder/auditmanager/corpus-audits/20260914_project_change_272")
FROZEN = CORPUS / "ai_first_semantic_mapping_projectchange_v3"
# Files of the frozen experiment this audit is allowed to read.  Evaluation,
# grouping-quality, truth, validation and holdout artifacts are never opened.
FORBIDDEN_MARKERS = ("EVALUATION", "GROUPING_QUALITY", "validation", "holdout", "SPLIT.json", "truth")
SRC = "<SOURCE>"


def _sha(path: Path | str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _read(path: Path) -> Any:
    text = str(path)
    if any(marker in text for marker in FORBIDDEN_MARKERS):
        raise RuntimeError(f"forbidden artifact: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _norm(value: Any, prefixes: list[str]) -> Any:
    """Replace location-dependent source roots with one token."""
    text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    for prefix in prefixes:
        text = text.replace(prefix, SRC)
    return json.loads(text)


def _norm_text(text: str, prefixes: list[str]) -> str:
    for prefix in prefixes:
        text = text.replace(prefix, SRC)
    return text


def _png_pixels_equal(a: str, b: str) -> bool:
    import fitz

    pa, pb = fitz.Pixmap(a), fitz.Pixmap(b)
    return (pa.width, pa.height, pa.n) == (pb.width, pb.height, pb.n) and pa.samples == pb.samples


def build_payload(prompt: str, data: Any, images: list[dict[str, Any]]) -> tuple[str, list[str], list[dict[str, Any]]]:
    """Production payload builder; replica of the 58f63ad7 adapter if absent."""
    builder = getattr(provider, "build_codex_payload", None)
    if builder is not None:
        return builder(prompt, data, images)
    labels, paths, seen = [], [], set()
    for row in images:  # 58f63ad7 CodexProvider.complete: dedupe by PATH
        path = str(row.get("path") or "")
        if not path or path in seen:
            continue
        seen.add(path)
        paths.append(path)
        label = dict(row.get("label") or {})
        label["image"] = len(paths)
        labels.append(label)
    payload = (prompt + "\nIMAGES:\n" + json.dumps(labels, ensure_ascii=False)
               + "\nSOURCE DATA:\n" + json.dumps(data, ensure_ascii=False))
    return payload, paths, labels


def compare_pages(pair: str, prod_source: Path, frozen_source: Path, prefixes: list[str]) -> dict[str, Any]:
    fields = [
        "side", "physical_page", "source_pdf", "source_pdf_sha256", "native_page_text",
    ]
    block_fields = [
        "block_id", "modality", "source_block_type", "bbox", "structured_md", "tables",
        "existing_description",
    ]
    counts = {f: 0 for f in fields + block_fields + ["graphic_crop_ref", "graphic_crop_bytes",
                                                    "graphic_crop_pixels", "full_page_bytes",
                                                    "full_page_pixels", "block_order"]}
    mismatches: list[dict[str, Any]] = []
    pages = blocks = crops = 0
    modality_totals: dict[str, int] = {}
    for frozen_page in sorted(frozen_source.glob("*/p*/page.json")):
        side_dir, page_dir = frozen_page.parent.parent.name, frozen_page.parent.name
        prod_page = prod_source / side_dir / page_dir / "page.json"
        f, p = _read(frozen_page), json.loads(prod_page.read_text(encoding="utf-8"))
        pages += 1
        for name in fields:
            if f.get(name) != p.get(name):
                counts[name] += 1
                mismatches.append({"page": f"{side_dir}/{page_dir}", "field": name})
        if [b["block_id"] for b in f["blocks"]] != [b["block_id"] for b in p["blocks"]]:
            counts["block_order"] += 1
            mismatches.append({"page": f"{side_dir}/{page_dir}", "field": "block_order"})
            continue
        if f["full_page_sha256"] != p["full_page_sha256"]:
            counts["full_page_bytes"] += 1
            if not _png_pixels_equal(f["full_page_ref"], p["full_page_ref"]):
                counts["full_page_pixels"] += 1
                mismatches.append({"page": f"{side_dir}/{page_dir}", "field": "full_page_pixels"})
        for fb, pb in zip(f["blocks"], p["blocks"]):
            blocks += 1
            modality_totals[pb["modality"]] = modality_totals.get(pb["modality"], 0) + 1
            for name in block_fields:
                if fb.get(name) != pb.get(name):
                    counts[name] += 1
                    mismatches.append({"page": f"{side_dir}/{page_dir}", "block": fb["block_id"], "field": name,
                                       "frozen": fb.get(name) if name in ("modality", "source_block_type", "bbox") else None,
                                       "production": pb.get(name) if name in ("modality", "source_block_type", "bbox") else None})
            if _norm(fb.get("graphic_crop_ref"), prefixes) != _norm(pb.get("graphic_crop_ref"), prefixes):
                counts["graphic_crop_ref"] += 1
                mismatches.append({"page": f"{side_dir}/{page_dir}", "block": fb["block_id"], "field": "graphic_crop_ref"})
            if fb.get("graphic_crop_ref"):
                crops += 1
                if fb["graphic_crop_sha256"] != pb.get("graphic_crop_sha256"):
                    counts["graphic_crop_bytes"] += 1
                    if not pb.get("graphic_crop_ref") or not _png_pixels_equal(fb["graphic_crop_ref"], pb["graphic_crop_ref"]):
                        counts["graphic_crop_pixels"] += 1
                        mismatches.append({"page": f"{side_dir}/{page_dir}", "block": fb["block_id"], "field": "graphic_crop_pixels"})
    return {
        "pages_compared": pages,
        "blocks_compared": blocks,
        "graphic_crops_compared": crops,
        "production_modalities": modality_totals,
        "field_mismatch_counts": counts,
        "mismatches": mismatches[:200],
        "mismatch_total": len(mismatches),
    }


def audit_pair(pair: str, work: Path) -> dict[str, Any]:
    freeze = _read(FROZEN / "EXPERIMENT_FREEZE.json")["pairs"][pair]
    admission = _read(Path(freeze["source_admission"]))
    artifacts = {side: admission[side]["artifacts"] for side in ("old", "new")}
    out: dict[str, Any] = {"pair_key": freeze["pair_key"]}

    # 1. Source references: frozen admission vs the production resolution rule.
    from backend.app.services.stage_comparison.production_orchestrator import _resolved_document_paths

    resolution = {}
    for side in ("old", "new"):
        art = artifacts[side]
        resolved = _resolved_document_paths({"pdf_path": art["pdf"]["path"]})
        resolution[side] = {
            "pdf_sha_match": _sha(resolved["pdf"]) == art["pdf"]["sha256"],
            "blocks_path_equal": str(resolved["blocks"]) == art["blocks"]["path"],
            "blocks_sha_match": _sha(resolved["blocks"]) == art["blocks"]["sha256"],
            "markdown_path_equal": str(resolved["markdown"]) == art["work_md"]["path"],
            "markdown_sha_match": _sha(resolved["markdown"]) == art["work_md"]["sha256"],
        }
    out["source_resolution"] = resolution

    # 2. Rebuild production packaging from the same admitted files.
    pair_work = work / f"pair_{pair.lower()}"
    prepared = source_prep.prepare_comparison_sources(
        pair_id=pair,
        old_paths={"pdf": Path(artifacts["old"]["pdf"]["path"]), "blocks": Path(artifacts["old"]["blocks"]["path"]),
                   "markdown": Path(artifacts["old"]["work_md"]["path"])},
        new_paths={"pdf": Path(artifacts["new"]["pdf"]["path"]), "blocks": Path(artifacts["new"]["blocks"]["path"]),
                   "markdown": Path(artifacts["new"]["work_md"]["path"])},
        work_dir=pair_work,
        object_id="parity_audit",
    )
    prefixes = [str(FROZEN / "source" / f"pair_{pair.lower()}"), str(pair_work / "source")]
    out["pages"] = compare_pages(pair, pair_work / "source", FROZEN / "source" / f"pair_{pair.lower()}", prefixes)

    # 3. Mapper-visible structure and exact mapping payload.
    frozen_structure = _read(Path(freeze["structure"]))
    prod_structure = prepared["structure"]
    out["mapping_structure_equal"] = _norm(frozen_structure, prefixes) == _norm(prod_structure, prefixes)
    frozen_map_prompt = (FROZEN / "mapping_inputs" / f"PAIR_{pair}_SEMANTIC_MAPPING" / "EXACT_PROMPT.txt").read_text(encoding="utf-8")
    payload, paths, labels = build_payload(
        contracts.MAPPER_PROMPT, {"pair": pair, "pages": prod_structure}, source_prep.mapping_images(prod_structure)
    )
    frozen_images = _read(FROZEN / "mapping_raw" / f"PAIR_{pair}_SEMANTIC_MAPPING" / "INVOCATION.json").get("images")
    out["mapping_payload"] = {
        "exact_text_equal_after_path_normalization": _norm_text(payload, prefixes) == _norm_text(frozen_map_prompt, prefixes),
        "production_chars": len(payload),
        "frozen_chars": len(frozen_map_prompt),
        "production_images": len(paths),
        "frozen_images": frozen_images,
    }

    # 4. Miner-visible exact payload for every frozen semantic region.
    semantic_map = _read(FROZEN / f"PAIR_{pair}_SEMANTIC_MAP.json")
    region_rows = []
    for region in semantic_map["regions"]:
        call = f"PAIR_{pair}_{region['region_id']}"
        frozen_dir = FROZEN / "miner_inputs_optimized" / call
        data, images = source_prep.optimized_region_bundle(pair_id=pair, region=region, work_dir=pair_work)
        payload, paths, labels = build_payload(contracts.MINER_PROMPT, data, images)
        frozen_prompt = (frozen_dir / "EXACT_PROMPT.txt").read_text(encoding="utf-8")
        frozen_data = _read(frozen_dir / "MODEL_INPUT.json")
        frozen_labels = json.loads(frozen_prompt.split("\nIMAGES:\n", 1)[1].split("\nSOURCE DATA:\n", 1)[0])
        region_rows.append({
            "region_id": region["region_id"],
            "model_input_equal": _norm(frozen_data, prefixes) == _norm(data, prefixes),
            "exact_prompt_equal": _norm_text(payload, prefixes) == _norm_text(frozen_prompt, prefixes),
            "production_images": len(paths),
            "frozen_images": len(frozen_labels),
            "production_chars": len(payload),
            "over_1048576_chars": len(payload) > 1_048_576,
        })
    out["miner_payloads"] = {
        "regions": len(region_rows),
        "exact_prompt_equal": sum(r["exact_prompt_equal"] for r in region_rows),
        "model_input_equal": sum(r["model_input_equal"] for r in region_rows),
        "image_count_equal": sum(r["production_images"] == r["frozen_images"] for r in region_rows),
        "over_transport_limit": [r["region_id"] for r in region_rows if r["over_1048576_chars"]],
        "rows": region_rows,
    }

    # 5. Dedupe input and deterministic application.
    mined = _read(FROZEN / f"PAIR_{pair}_PROJECTCHANGE_MINER_RESULTS.json")
    changes = mined["projectchanges"] if "projectchanges" in mined else mined["pairs"][pair]["projectchanges"]
    frozen_dedupe_input = _read(FROZEN / "dedupe_inputs" / f"PAIR_{pair}_DEDUPE" / "MODEL_INPUT.json")
    prod_dedupe_input = {"pair": pair, "projectchanges": [dedupe.compact_change(c) for c in changes]}
    frozen_decisions = _read(FROZEN / f"PAIR_{pair}_DEDUPE.json")
    final_frozen = _read(FROZEN / f"PAIR_{pair}_FINAL_PROJECTCHANGES.json")
    final_frozen_changes = final_frozen["projectchanges"] if "projectchanges" in final_frozen else final_frozen["pairs"][pair]["projectchanges"]
    replayed = dedupe.apply_dedupe(pair, changes, frozen_decisions)
    out["dedupe"] = {
        "input_equal": frozen_dedupe_input == prod_dedupe_input,
        "replay_equal_frozen_final": replayed == final_frozen_changes,
        "mined": len(changes),
        "final": len(replayed),
    }
    return out


def missing_data_probes(work: Path) -> list[dict[str, Any]]:
    """Frozen contract (ai_first_semantic_mapping_projectchange_v3.py prepare_pair):
    block_type ∈ {text,image,stamp} else KeyError; TEXT with tables → TABLE
    (stamp maps to TEXT first); coords_norm and page_index required; blocks.json
    must be {"blocks": [...]}; work_md must exist and decode as UTF-8; a block
    absent from the Markdown gets an empty body; a zero-area crop fails."""
    import fitz

    def pdf(path: Path) -> Path:
        doc = fitz.open()
        doc.new_page(width=200, height=200).insert_text((20, 40), "X")
        doc.save(path)
        doc.close()
        return path

    def md(path: Path, body: str, block_id: str = "b1") -> Path:
        path.write_text(f"## Page 1\n\n### BLOCK #1 [TEXT]: {block_id}\n\n{body}\n", encoding="utf-8")
        return path

    base_block = {"block_id": "b1", "page_index": 0, "block_type": "text", "coords_norm": [0.1, 0.1, 0.5, 0.5]}
    cases = [
        ("stamp_with_table_is_TABLE", {**base_block, "block_type": "stamp"}, "| a | b |\n|---|---|\n| 1 | 2 |", "TABLE"),
        ("text_with_table_is_TABLE", base_block, "| a | b |\n|---|---|\n| 1 | 2 |", "TABLE"),
        ("stamp_without_table_is_TEXT", {**base_block, "block_type": "stamp"}, "Штамп", "TEXT"),
        ("image_is_GRAPHIC", {**base_block, "block_type": "image"}, "описание", "GRAPHIC"),
        ("unknown_block_type_fails", {**base_block, "block_type": "chart"}, "x", "ERROR"),
        ("missing_bbox_fails", {k: v for k, v in base_block.items() if k != "coords_norm"}, "x", "ERROR"),
        ("missing_page_index_fails", {k: v for k, v in base_block.items() if k != "page_index"}, "x", "ERROR"),
        ("zero_area_graphic_fails", {**base_block, "block_type": "image", "coords_norm": [0.5, 0.5, 0.5, 0.5]}, "x", "ERROR"),
        ("block_absent_from_md_is_empty", {**base_block, "block_id": "b2"}, "x", "EMPTY_BODY"),
    ]
    rows = []
    for name, block, body, expected in cases:
        root = Path(tempfile.mkdtemp(dir=work, prefix="probe_"))
        (root / "blocks.json").write_text(json.dumps({"blocks": [block]}), encoding="utf-8")
        try:
            structure = source_prep.prepare_side(side="OLD", pdf_path=pdf(root / "d.pdf"), blocks_path=root / "blocks.json",
                                                 md_path=md(root / "d.md", body), out_dir=root / "out")
            got_block = structure[0]["blocks"][0] if structure and structure[0]["blocks"] else None
            got = got_block["modality"] if got_block else "NO_BLOCK"
            if expected == "EMPTY_BODY":
                got = "EMPTY_BODY" if got_block and got_block["structured_md"] == "" else got
        except Exception as exc:  # noqa: BLE001 — the probe records the failure class
            got = "ERROR"
            detail = f"{type(exc).__name__}: {exc}"
        else:
            detail = ""
        rows.append({"case": name, "expected": expected, "production": got, "match": got == expected, "detail": detail})
    # Whole-file cases.
    for name, writer, expected in (
        ("missing_markdown_file_fails", None, "ERROR"),
        ("non_utf8_markdown_fails", b"\xff\xfe\xfa", "ERROR"),
        ("blocks_json_bare_list_fails", "list", "ERROR"),
    ):
        root = Path(tempfile.mkdtemp(dir=work, prefix="probe_"))
        blocks_payload: Any = {"blocks": [base_block]} if writer != "list" else [base_block]
        (root / "blocks.json").write_text(json.dumps(blocks_payload), encoding="utf-8")
        md_path = root / "d.md"
        if isinstance(writer, bytes):
            md_path.write_bytes(writer)
        elif writer == "list":
            md(md_path, "x")
        try:
            source_prep.prepare_side(side="OLD", pdf_path=pdf(root / "d.pdf"), blocks_path=root / "blocks.json",
                                     md_path=md_path, out_dir=root / "out")
            got, detail = "SILENT_OK", ""
        except Exception as exc:  # noqa: BLE001
            got, detail = "ERROR", f"{type(exc).__name__}: {exc}"
        rows.append({"case": name, "expected": expected, "production": got, "match": got == expected, "detail": detail})
    return rows


def image_identity_probe(work: Path) -> dict[str, Any]:
    """Two different paths with identical bytes must count once (frozen: SHA256)."""
    import fitz

    root = Path(tempfile.mkdtemp(dir=work, prefix="probe_img_"))
    pix = fitz.Pixmap(fitz.csRGB, fitz.IRect(0, 0, 8, 8), 0)
    pix.clear_with(200)
    a, b = root / "a.png", root / "b.png"
    pix.save(a)
    pix.save(b)
    images = [{"path": str(a), "label": {"kind": "GRAPHIC_CROP", "block_id": "a"}},
              {"path": str(b), "label": {"kind": "GRAPHIC_CROP", "block_id": "b"}}]
    _, paths, labels = build_payload("P", {}, images)
    return {"identical_bytes_two_paths": 2, "production_images": len(paths), "frozen_images": 1,
            "match": len(paths) == 1, "labels": labels}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--work", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--pairs", default="A,B")
    args = parser.parse_args()
    work = Path(args.work)
    work.mkdir(parents=True, exist_ok=True)
    result: dict[str, Any] = {
        "schema": "projectchange-v3-source-packaging-parity/1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "repo_head": __import__("subprocess").run(["git", "-C", str(REPO), "rev-parse", "HEAD"], capture_output=True, text=True).stdout.strip(),
        "production_files_sha256": {
            name: _sha(REPO / "backend/app/services/project_change_v3" / name)
            for name in ("source_prep.py", "provider.py", "dedupe.py", "contracts.py")
        },
        "prompt_sha256_equal_frozen": {
            name: _sha(FROZEN / name) == contracts.PROMPT_HASHES[name] for name in contracts.PROMPT_HASHES
        },
        "model_calls": 0,
        "validation_opened": False,
        "final_holdout_opened": False,
        "truth_opened": False,
        "pairs": {},
    }
    for pair in args.pairs.split(","):
        result["pairs"][pair] = audit_pair(pair, work)
    result["missing_data_probes"] = missing_data_probes(work)
    result["graphic_image_identity_probe"] = image_identity_probe(work)
    Path(args.out).write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    summary = {
        pair: {
            "pages_mismatch_total": r["pages"]["mismatch_total"],
            "mapping_structure_equal": r["mapping_structure_equal"],
            "mapping_payload_equal": r["mapping_payload"]["exact_text_equal_after_path_normalization"],
            "mapping_images": [r["mapping_payload"]["production_images"], r["mapping_payload"]["frozen_images"]],
            "miner_exact_prompt_equal": f"{r['miner_payloads']['exact_prompt_equal']}/{r['miner_payloads']['regions']}",
            "dedupe": r["dedupe"],
            "resolution": r["source_resolution"],
        }
        for pair, r in result["pairs"].items()
    }
    summary["probes"] = {p["case"]: p["match"] for p in result["missing_data_probes"]}
    summary["image_identity"] = result["graphic_image_identity_probe"]["match"]
    print(json.dumps(summary, ensure_ascii=False, indent=1))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
