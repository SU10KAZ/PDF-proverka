#!/usr/bin/env python3
"""Запечатать идентичность источников фикстур Human Mapping: sha256 OLD/NEW PDF.

Фикстура HM (`human_mapping_fixtures/UI_DATA_PAIR_*.json`) хранит только sha
своей семантической карты. Чтобы реальная пара сравнения могла открыть её как
начальные данные, нужно знать, от КАКИХ исходных PDF эта карта построена.
Цепочка берётся из исследования и проверяется целиком, ничего не угадывается:

    UI_SNAPSHOT_MANIFEST.files[ui_data]      == sha256(ui_data)
    ui_data.source_sha256                     == sha256(<pair>_SEMANTIC_MAP.json)
    EXPERIMENT_FREEZE.pairs[..].pair_key      == ui_data.pair_key
    EXPERIMENT_FREEZE.source_admission_sha256 == sha256(SOURCE_ADMISSION.json)
    SOURCE_ADMISSION.old/new.artifacts.pdf.sha256  → старый/новый PDF

и сверяется с независимым запечатанным снимком ProjectChange
(`project_change_preview_snapshot_v3/presentation.json → documents`).
Результат — `backend/app/data/human_mapping_fixture_sources.json`; рантайм
читает только его (исследовательские пути ему не нужны). Моделей не вызывает.

    python3 scripts/build_hm_fixture_source_identity.py [--check]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DATA = REPO / "backend" / "app" / "data"
FIXTURES = DATA / "human_mapping_fixtures"
SNAPSHOT = DATA / "project_change_preview_snapshot_v3" / "presentation.json"
OUT = DATA / "human_mapping_fixture_sources.json"
RESEARCH = Path("/home/coder/auditmanager/corpus-audits/20260914_project_change_272/"
                "ai_first_semantic_mapping_projectchange_v3")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build() -> dict:
    manifest_path = FIXTURES / "UI_SNAPSHOT_MANIFEST.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    freeze_path = RESEARCH / "EXPERIMENT_FREEZE.json"
    freeze = json.loads(freeze_path.read_text(encoding="utf-8"))
    snapshot_docs = json.loads(SNAPSHOT.read_text(encoding="utf-8"))["documents"]
    fixtures = []
    for alias, entry in sorted(manifest["pairs"].items()):
        ui_path = FIXTURES / entry["data"]
        ui = json.loads(ui_path.read_text(encoding="utf-8"))
        assert manifest["files"][entry["data"]] == sha(ui_path), f"{alias}: ui_data differs from its manifest"
        semantic_map = RESEARCH / ui["source_semantic_map"]
        assert ui["source_sha256"] == entry["source_sha256"] == sha(semantic_map), f"{alias}: semantic map"
        research = next(p for p in freeze["pairs"].values() if p["pair_key"] == ui["pair_key"])
        admission_path = Path(research["source_admission"])
        assert research["source_admission_sha256"] == sha(admission_path), f"{alias}: source admission"
        admission = json.loads(admission_path.read_text(encoding="utf-8"))
        assert admission["pair_key"] == ui["pair_key"]
        sides = {}
        for side in ("old", "new"):
            pdf = admission[side]["artifacts"]["pdf"]["sha256"]
            sealed = snapshot_docs[f"{ui['pair_key']}:{side}"]["source_sha256"]
            assert pdf == sealed, f"{alias}/{side}: research PDF sha differs from the sealed snapshot"
            sides[side] = {
                "pdf_sha256": pdf,
                "document_code": admission[side]["document_code"],
                "version_id": admission[side]["version_id"],
            }
        fixtures.append({
            "fixture_alias": alias,
            "pair_key": ui["pair_key"],
            "label": ui["label"],
            "ui_data": entry["data"],
            "ui_data_sha256": sha(ui_path),
            "regions": len(ui["regions"]),
            "old": sides["old"],
            "new": sides["new"],
            "provenance": {
                "semantic_map": ui["source_semantic_map"],
                "semantic_map_sha256": ui["source_sha256"],
                "source_admission_sha256": research["source_admission_sha256"],
                "experiment_freeze_sha256": sha(freeze_path),
                "sealed_snapshot_documents_agree": True,
            },
        })
    return {
        "schema": "human-mapping-fixture-source-identity/1",
        "purpose": ("Approved sealed Human Mapping fixtures and the exact source PDFs they were built from. "
                    "A real comparison pair whose OLD and NEW PDFs are byte-identical (sha256) to one "
                    "fixture may use that fixture's frozen semantic mapping as INITIAL read-only HM data; "
                    "human decisions stay in the real pair's own scope."),
        "fixtures_dir": FIXTURES.name,
        "fixtures_manifest_sha256": sha(manifest_path),
        "fixtures": fixtures,
        "model_calls": 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="сверить закоммиченный файл, ничего не писать")
    args = parser.parse_args()
    text = json.dumps(build(), ensure_ascii=False, indent=2) + "\n"
    if args.check:
        same = OUT.is_file() and OUT.read_text(encoding="utf-8") == text
        print("MATCH" if same else "DIFFERS")
        return 0 if same else 1
    OUT.write_text(text, encoding="utf-8")
    print(OUT, hashlib.sha256(text.encode("utf-8")).hexdigest())
    return 0


if __name__ == "__main__":
    sys.exit(main())
