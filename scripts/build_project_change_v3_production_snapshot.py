#!/usr/bin/env python3
"""Build production ProjectChange preview snapshot V3 (0 model calls).

Reads frozen PAIR_*_FINAL_PROJECTCHANGES.json and existing preview PDFs.
Writes backend/app/data/project_change_preview_snapshot_v3/.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
CORPUS = Path(
    "/home/coder/auditmanager/corpus-audits/20260914_project_change_272"
    "/ai_first_semantic_mapping_projectchange_v3"
)
OLD_SNAP = REPO / "backend/app/data/project_change_preview_snapshot"
OUT = REPO / "backend/app/data/project_change_preview_snapshot_v3"
OBJECT_API = "4f3e5916"
SNAPSHOT_OBJECT = "272_Sadovnicheskaya_76_Balchug_Esteyt"

PAIRS = {
    "A": {
        "key": "ad0a31a342a666082f2ef66a",
        "label": "??1",
        "cipher": "??1",
        "discipline": "????????????? ???????",
    },
    "B": {
        "key": "caea6d2810c334ec0368de8e",
        "label": "???4.2",
        "cipher": "???4.2",
        "discipline": "?????????, ?????????? ? ?????????????????",
    },
}

LEAK_TOKENS = ("REAL15", "PROVEN10", "F13", "CORRECT", "PARTIAL", "FALSE")


def sha_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sha_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def dump(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def bbox_to_region(bbox: list[float]) -> dict[str, Any]:
    x0, y0, x1, y1 = [float(x) for x in bbox]
    return {
        "x": x0,
        "y": y0,
        "width": max(0.0, x1 - x0),
        "height": max(0.0, y1 - y0),
        "units": "normalized",
    }


def bbox_to_page_box(bbox: list[float], page_width: float, page_height: float) -> list[float]:
    x0, y0, x1, y1 = [float(x) for x in bbox]
    return [x0 * page_width, y0 * page_height, x1 * page_width, y1 * page_height]


def evidence_id(pair_id: str, change_id: str, ev: dict[str, Any], index: int) -> str:
    raw = "|".join(
        [
            pair_id,
            change_id,
            ev.get("side", ""),
            str(ev.get("physical_page", "")),
            ev.get("block_id", ""),
            str(index),
        ]
    )
    return "pev_" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def decision_key(change_id: str) -> str:
    return "pcv3_" + hashlib.sha256(change_id.encode("utf-8")).hexdigest()


def lineage_key(change_id: str) -> str:
    return "pclineage_v3_" + hashlib.sha256(("lineage|" + change_id).encode("utf-8")).hexdigest()


def binding_signature(change: dict[str, Any]) -> str:
    payload = {
        "id": change.get("projectchange_id"),
        "subject": change.get("engineering_subject"),
        "old": change.get("old_state"),
        "new": change.get("new_state"),
        "summary": change.get("change_summary"),
    }
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def load_page_size(pdf_path: Path, page: int) -> tuple[float, float]:
    import fitz

    with fitz.open(pdf_path) as doc:
        p = doc[page - 1]
        return float(p.rect.width), float(p.rect.height)


def ensure_crop(
    crops_dir: Path,
    ev_id: str,
    ev: dict[str, Any],
    pdf_path: Path,
    page_box: list[float] | None,
) -> str | None:
    """Copy existing crop_ref or rasterize; return relative path or None."""
    dest = crops_dir / f"{ev_id}.png"
    if dest.exists():
        return f"evidence_crops/{ev_id}.png"
    crop_ref = ev.get("crop_ref") or ""
    if crop_ref and Path(crop_ref).is_file():
        shutil.copy2(crop_ref, dest)
        return f"evidence_crops/{ev_id}.png"
    if page_box is None:
        return None
    import fitz

    with fitz.open(pdf_path) as doc:
        page = doc[int(ev["physical_page"]) - 1]
        clip = fitz.Rect(page_box) & page.rect
        if clip.is_empty:
            return None
        scale = min(3.0, 1200 / max(clip.width, 1.0))
        page.get_pixmap(matrix=fitz.Matrix(scale, scale), clip=clip, alpha=False).save(dest)
    return f"evidence_crops/{ev_id}.png"


def map_change_to_item(
    pair_letter: str,
    pair_meta: dict[str, Any],
    change: dict[str, Any],
    pair_view: dict[str, Any],
    documents: dict[str, Any],
    evidence_index: dict[str, Any],
    crops_dir: Path,
    provenance_lines: list[str],
) -> dict[str, Any]:
    pair_id = pair_meta["key"]
    change_id = change["projectchange_id"]
    left = pair_view["pair"]["left"]
    right = pair_view["pair"]["right"]
    details = []
    for param in change.get("changed_parameters") or []:
        details.append(
            {
                "name": param.get("name", ""),
                "old_value": param.get("old_value", ""),
                "new_value": param.get("new_value", ""),
                "unit": param.get("unit", ""),
                "location": param.get("location", ""),
            }
        )

    presentation_evidence = []
    for index, ev in enumerate(change.get("evidence_items") or []):
        side = str(ev.get("side", "")).upper()
        doc_side = "old" if side == "OLD" else "new"
        doc_meta = documents[f"{pair_id}:{doc_side}"]
        pdf_path = OUT / doc_meta["file"]
        page = int(ev["physical_page"])
        bbox = list(ev.get("bbox") or [0, 0, 1, 1])
        region = bbox_to_region(bbox)
        try:
            pw, ph = load_page_size(pdf_path, page)
            page_box = bbox_to_page_box(bbox, pw, ph)
        except Exception:
            page_box = None
        ev_id = evidence_id(pair_id, change_id, ev, index)
        crop_rel = ensure_crop(crops_dir, ev_id, ev, pdf_path, page_box)
        doc_info = left if doc_side == "old" else right
        presentation_evidence.append(
            {
                "crop_precision": "EXACT_REGION" if page_box else "PAGE_LEVEL",
                "document": {
                    "id": doc_info.get("document_code") or doc_info.get("filename"),
                    "label": doc_info.get("document_code") or doc_info.get("filename"),
                    "pdf_path": f"PROJECT_CHANGE_PREVIEW/{doc_meta['file']}",
                    "version": doc_info.get("version_id") or "v002",
                },
                "id": ev_id,
                "image_url": (
                    f"/api/project-change-preview/objects/{OBJECT_API}/evidence/{ev_id}/crop"
                ),
                "page": page,
                "pair_id": pair_id,
                "quote": (ev.get("relevant_fragment") or "")[:2000],
                "region": region,
                "short_explanation_ru": (ev.get("evidence_role") or "?????????????? ?????????."),
                "side": side,
                "source_evidence_id": ev.get("block_id") or "",
                "source_type": ev.get("block_type") or "TEXT",
                "crop_file": crop_rel,
            }
        )
        evidence_index[ev_id] = {
            "box": page_box,
            "page": page,
            "pair_id": pair_id,
            "side": doc_side,
            "crop_file": crop_rel,
            "region": region,
        }

    # Strip diagnostic / evaluation labels from user-facing fields.
    item = {
        "binding_signature": binding_signature(change),
        "candidate_version": "projectchange_v3",
        "change_type": "OTHER",
        "cipher": pair_meta["cipher"],
        "conflicts": [],
        "decision_key": decision_key(change_id),
        "decision_state": "NONE",
        "details": details,
        "discipline": pair_meta["discipline"],
        "effective_decision": None,
        "engineering_subject": change.get("engineering_subject") or "",
        "engineering_system": "",
        "evidence": presentation_evidence,
        "id": change_id,
        "identity_reusable": True,
        "importance": "NORMAL",
        "lineage_key": lineage_key(change_id),
        "new_state": change.get("new_state") or "",
        "old_state": change.get("old_state") or "",
        "research_status": "REVIEW",
        "review_explanation_ru": "????????? ???????? ????????? (V3 production preview).",
        "review_question": "?????????????? ?? ??? ????????? ?? ???????? ???????????",
        "source_run_id": "projectchange_v3_production_snapshot",
        "status": "REVIEW",
        "summary_ru": change.get("change_summary") or "",
        "technical_provenance": list(provenance_lines),
        "scope": change.get("scope") or "",
        "locations": list(change.get("locations") or []),
        "old_pages": list(change.get("old_pages") or []),
        "new_pages": list(change.get("new_pages") or []),
        "modalities": list(change.get("modalities") or []),
    }
    return item


def leakage_scan(root: Path) -> dict[str, Any]:
    hits: list[dict[str, Any]] = []
    word_re = {
        tok: re.compile(rf"(?<![A-Za-z0-9_]){re.escape(tok)}(?![A-Za-z0-9_])")
        for tok in LEAK_TOKENS
    }
    for path in sorted(root.rglob("*.json")):
        text = path.read_text(encoding="utf-8")
        for tok, pattern in word_re.items():
            for m in pattern.finditer(text):
                hits.append(
                    {
                        "token": tok,
                        "file": str(path.relative_to(root)),
                        "offset": m.start(),
                    }
                )
    return {
        "schema": "source-truth-leakage/1",
        "tokens": list(LEAK_TOKENS),
        "hit_count": len(hits),
        "hits": hits,
        "pass": len(hits) == 0,
        "model_calls": 0,
    }


def build() -> Path:
    if OUT.exists():
        raise FileExistsError(f"Snapshot already exists (refusing overwrite): {OUT}")
    if not OLD_SNAP.is_dir():
        raise FileNotFoundError(f"Missing prior snapshot for PDF copy: {OLD_SNAP}")

    from backend.app.services.project_change_v3.provenance import (
        build_provenance,
        provenance_lines,
    )

    OUT.mkdir(parents=True)
    (OUT / "documents").mkdir()
    crops_dir = OUT / "evidence_crops"
    crops_dir.mkdir()

    old_pres = json.loads((OLD_SNAP / "presentation.json").read_text(encoding="utf-8"))
    provenance = build_provenance(source_prep_version="v002_admitted_pdf_md_blocks")
    prov_lines = provenance_lines(provenance)

    pairs_out: dict[str, Any] = {}
    documents_out: dict[str, Any] = {}
    evidence_index: dict[str, Any] = {}
    items: list[dict[str, Any]] = []

    for letter, meta in PAIRS.items():
        pair_id = meta["key"]
        # Copy PDFs + pair/doc metadata from existing preview snapshot (read).
        for side in ("old", "new"):
            src_name = f"documents/{pair_id}-{side}.pdf"
            src = OLD_SNAP / src_name
            if not src.is_file():
                raise FileNotFoundError(src)
            dst = OUT / src_name
            shutil.copy2(src, dst)
            documents_out[f"{pair_id}:{side}"] = dict(old_pres["documents"][f"{pair_id}:{side}"])
        pairs_out[pair_id] = dict(old_pres["pairs"][pair_id])

        final = json.loads((CORPUS / f"PAIR_{letter}_FINAL_PROJECTCHANGES.json").read_text(encoding="utf-8"))
        # READ ONLY ? never mutate corpus.
        for change in final["projectchanges"]:
            items.append(
                map_change_to_item(
                    letter,
                    meta,
                    change,
                    pairs_out[pair_id],
                    documents_out,
                    evidence_index,
                    crops_dir,
                    prov_lines,
                )
            )

    envelope = {
        "schema_version": "project-change-view/1",
        "object_id": SNAPSHOT_OBJECT,
        "origin": "PRODUCTION",
        "mode": "PREVIEW",
        "decision_mode": "READ_ONLY",
        "decision_namespace": "PROJECT_CHANGE_PREVIEW",
        "decision_revision": 0,
        "revision": 1,
        "candidate_status": "PRODUCTION_V3_FIXTURE",
        "candidate_version": "projectchange_v3",
        "source_run_id": "projectchange_v3_production_snapshot",
        "viewer_session": None,
        "summary": {
            "project_changes": len(items),
            "pairs": sorted(pairs_out.keys()),
            "review": len(items),
            "confirmed": 0,
        },
        "capabilities": {"decisions": False, "history": False},
        "items": items,
    }

    presentation = {
        "documents": documents_out,
        "envelope": envelope,
        "evidence": evidence_index,
        "pairs": pairs_out,
    }
    dump(OUT / "presentation.json", presentation)

    # Lightweight source receipts (hashes of copied PDFs + finals read).
    receipts = {
        "schema": "project-change-v3-source-receipts/1",
        "model_calls": 0,
        "finals": {},
        "documents": {},
    }
    for letter, meta in PAIRS.items():
        fp = CORPUS / f"PAIR_{letter}_FINAL_PROJECTCHANGES.json"
        receipts["finals"][letter] = {
            "path": str(fp),
            "sha256": sha_file(fp),
            "pair_key": meta["key"],
            "count": len(json.loads(fp.read_text(encoding="utf-8"))["projectchanges"]),
        }
        for side in ("old", "new"):
            rel = f"documents/{meta['key']}-{side}.pdf"
            receipts["documents"][rel] = sha_file(OUT / rel)
    dump(OUT / "source-receipts.json", receipts)

    source_manifest = {
        "schema_version": "project-change-v3-source-manifest/1",
        "object_id": SNAPSHOT_OBJECT,
        "origin": "PRODUCTION",
        "pairs": {
            letter: {"pair_key": meta["key"], "label": meta["label"]}
            for letter, meta in PAIRS.items()
        },
        "engine": provenance,
        "note": "Fixture snapshot built from frozen V3 finals; no inference.",
    }
    dump(OUT / "source-manifest.json", source_manifest)

    files: dict[str, str] = {}
    for path in sorted(OUT.rglob("*")):
        if path.is_file() and path.name != "MANIFEST.json":
            rel = str(path.relative_to(OUT)).replace("\\", "/")
            files[rel] = sha_file(path)

    manifest = {
        "schema": "project-change-production-snapshot/1",
        "object_id": SNAPSHOT_OBJECT,
        "decision_mode": "READ_ONLY",
        "decision_namespace": "PROJECT_CHANGE_PREVIEW",
        "auto_refresh": False,
        "candidate_version": "projectchange_v3",
        "snapshot_revision": 1,
        "source_run_id": "projectchange_v3_production_snapshot",
        "pdf_sanitization": "Copied admitted preview PDFs for ??1/???4.2 only.",
        "statistics": {
            "project_changes": len(items),
            "evidence_items": sum(len(i["evidence"]) for i in items),
            "unique_evidence": len(evidence_index),
            "pairs": list(pairs_out.keys()),
            "research_statuses": {"REVIEW": len(items)},
        },
        "files": files,
    }
    dump(OUT / "MANIFEST.json", manifest)
    # Re-hash MANIFEST into a companion is not required by PreviewService;
    # receipts include MANIFEST via service.sha at load.

    leakage = leakage_scan(OUT)
    dump(OUT / "SOURCE_TRUTH_LEAKAGE.json", leakage)
    # Append leakage file to manifest files map (update)
    files["SOURCE_TRUTH_LEAKAGE.json"] = sha_file(OUT / "SOURCE_TRUTH_LEAKAGE.json")
    manifest["files"] = files
    dump(OUT / "MANIFEST.json", manifest)

    print("BUILT", OUT)
    print("items", len(items), "evidence", len(evidence_index), "leak_hits", leakage["hit_count"])
    return OUT


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["build"])
    args = parser.parse_args()
    if args.command == "build":
        build()


if __name__ == "__main__":
    main()
