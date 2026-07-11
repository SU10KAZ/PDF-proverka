"""Шаг 7/10 — reusable v2-only storage harness для тестов.

Строит временный projects_v2 store с полным документом (input/metadata/
findings/optimization/pipeline_log) и ОТСУТСТВУЮЩИМ (или пустым) legacy
`projects/`, чтобы проверять, что read/export работают из v2 без legacy.

Ничего не пишет вне переданного tmp_path. Production не затрагивается.

Пример:
    store = build_v2_only_store(tmp_path)
    doc = add_v2_document(store, document_code="DOC-1", findings_n=5)
    adapter = store.adapter()
    assert adapter.findings_count(doc.doc_dir, doc.version_id) == 5
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


@dataclass
class V2OnlyDoc:
    object_folder: str
    discipline: str
    document_code: str
    object_id: str
    version_id: str
    doc_dir: Path
    version_dir: Path


@dataclass
class V2OnlyStore:
    root: Path            # tmp_path
    legacy_root: Path     # projects/ — отсутствует или пуст
    v2_root: Path         # projects_v2/
    docs: list = field(default_factory=list)

    def adapter(self):
        from backend.app.services.storage.projects_v2_adapter import ProjectsV2Adapter
        return ProjectsV2Adapter(self.v2_root)


def build_v2_only_store(tmp_path: Path, *, create_empty_legacy: bool = False) -> V2OnlyStore:
    """Создать пустой v2-store (objects/) и ОТСУТСТВУЮЩИЙ/пустой legacy."""
    v2 = Path(tmp_path) / "projects_v2"
    (v2 / "objects").mkdir(parents=True, exist_ok=True)
    (v2 / "_system").mkdir(parents=True, exist_ok=True)
    legacy = Path(tmp_path) / "projects"
    if create_empty_legacy:
        legacy.mkdir(exist_ok=True)
    return V2OnlyStore(root=Path(tmp_path), legacy_root=legacy, v2_root=v2)


def add_v2_document(
    store: V2OnlyStore,
    *,
    object_folder: str = "OBJ_HARNESS",
    discipline: str = "KJ",
    document_code: str = "DOC-1",
    object_id: str = "objharness",
    version_id: str = "v001",
    findings_n: int = 3,
    findings: Optional[list] = None,
    with_optimization: bool = True,
    with_pipeline_log: bool = True,
    with_blocks: bool = True,
    with_pdf: bool = True,
    extra_versions: tuple = (),
) -> V2OnlyDoc:
    """Добавить полный v2-документ в store. Возвращает V2OnlyDoc активной версии."""
    doc = (store.v2_root / "objects" / object_folder / "disciplines"
           / discipline / "documents" / document_code)
    doc.mkdir(parents=True, exist_ok=True)
    all_versions = [version_id, *extra_versions]
    (doc / "document.json").write_text(json.dumps({
        "schema_version": 1, "document_code": document_code, "object_id": object_id,
        "versions": [{"version_id": v, "version_no": i + 1}
                     for i, v in enumerate(all_versions)],
    }), encoding="utf-8")
    (doc / "current_version.txt").write_text(version_id, encoding="utf-8")

    vdir = doc / "versions" / version_id
    (vdir / "01_input").mkdir(parents=True, exist_ok=True)
    if with_pdf:
        (vdir / "01_input" / f"{document_code}.pdf").write_bytes(b"%PDF-1.4 fake")
        (vdir / "01_input" / f"{document_code}_document.md").write_bytes(b"# md")
    # пустые scaffold-папки прочих версий (если заданы)
    for ev in extra_versions:
        (doc / "versions" / ev / "01_input").mkdir(parents=True, exist_ok=True)

    latest = vdir / "03_analysis" / "latest"
    latest.mkdir(parents=True, exist_ok=True)
    if findings is None:
        findings = [{"id": f"F-{i}", "category": "Критическое"} for i in range(findings_n)]
    (latest / "03_findings.json").write_text(
        json.dumps({"findings": findings}), encoding="utf-8")
    if with_blocks:
        (latest / "01_blocks_analysis.json").write_text(
            json.dumps({"blocks": [{"block_id": "b1"}, {"block_id": "b2"}]}), encoding="utf-8")
        (latest / "02_text_analysis.json").write_text(
            json.dumps({"normative_refs_found": []}), encoding="utf-8")
    if with_optimization:
        (latest / "optimization.json").write_text(
            json.dumps({"items": [{"id": "o1", "savings_pct": 5}]}), encoding="utf-8")
    if with_pipeline_log:
        runs = vdir / "03_analysis" / "runs" / "r1"
        runs.mkdir(parents=True, exist_ok=True)
        (runs / "pipeline_log.json").write_text(
            json.dumps({"stages": ["prepare", "findings_merge"]}), encoding="utf-8")

    rec = V2OnlyDoc(object_folder, discipline, document_code, object_id, version_id, doc, vdir)
    store.docs.append(rec)
    return rec
