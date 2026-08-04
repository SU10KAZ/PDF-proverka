from __future__ import annotations

import inspect

from backend.app.pipeline.stages.block_context.reference_catalog import loader
from backend.app.pipeline.stages.block_grounding import block_profile_registry


def test_embedded_catalog_is_complete_and_self_contained():
    manifest = loader.load_catalog_manifest()
    records = loader.load_reference_records()
    rules = loader.load_reference_rules()

    assert manifest["runtime_dependency_on_experiments"] is False
    assert manifest["records_total"] == 1133 == len(records)
    # 106 = 105 дисциплинарных профилей + надведомственный «legend»
    # («Условные обозначения»): легенда встречается в любом разделе.
    assert manifest["profiles_total"] == 106
    assert sum(row["profile_id"] == "legend" for row in records) == 14
    assert set(manifest["disciplines"]) == {
        "АР", "ВК", "ГП", "КЖ", "КМ", "ОВ", "СС", "ТХ", "ЭОМ",
    }
    assert sum(bool(row.get("structure_signature")) for row in records) == 1132
    assert all(row.get("block_id") and row.get("profile_id") for row in records)
    assert rules["text_scope"]["outside_margin"] == 0.0
    assert rules["text_scope"]["invalid_region"] == "empty"


def test_runtime_registry_does_not_read_experiments():
    source = inspect.getsource(block_profile_registry)

    assert "блоки разных дисциплин" not in source
    assert ' / "experiments"' not in source


def test_reference_selection_uses_embedded_catalog_from_any_working_directory(
    tmp_path, monkeypatch,
):
    monkeypatch.chdir(tmp_path)
    reference = block_profile_registry.select_reference(
        "electrical_installation_detail",
        "ЭОМ",
        source_kind="structured_electrical",
        graph={"validation": {"subtype": "grounding_welds"}},
        classification={"description": "Узел сварного соединения полосы заземления"},
    )

    assert reference["block_id"] == "9HTP-UNPC-EQV"
    assert reference["coverage_file"].startswith(
        "pipeline:block_context/reference_catalog/"
    )


def test_profile_documents_are_part_of_stage_package():
    manifest = loader.load_catalog_manifest()
    for meta in manifest["disciplines"].values():
        path = loader.CATALOG_DIR / meta["profile_document"]
        assert path.is_file()
        assert "Профили дисциплины" in path.read_text(encoding="utf-8")
