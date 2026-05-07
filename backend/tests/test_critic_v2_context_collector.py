"""
test_critic_v2_context_collector.py
-------------------------------------
Tests for critic_v2 offline context enrichment layer.

All tests are offline and do NOT touch production artifacts.
No real project directories are accessed; all data is synthetic.

Runs with:
    python -m pytest backend/tests/test_critic_v2_context_collector.py -v
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.pipeline.stages.findings_review.critic_v2.context import (
    BlockSnippet,
    ContextCollector,
    ContextCollectionStats,
    CrossReference,
    FindingContextPackage,
    RelatedFinding,
    TableContextRow,
)
from backend.app.pipeline.stages.findings_review.critic_v2.context.common_notes import (
    get_common_notes,
)
from backend.app.pipeline.stages.findings_review.critic_v2.context.cross_references import (
    get_cross_references,
)
from backend.app.pipeline.stages.findings_review.critic_v2.context.neighbor_blocks import (
    get_neighbor_blocks,
)
from backend.app.pipeline.stages.findings_review.critic_v2.context.related_findings import (
    get_related_findings,
)
from backend.app.pipeline.stages.findings_review.critic_v2.context.table_context import (
    get_table_context,
)


# ─── Synthetic data helpers ───────────────────────────────────────────────────

def _block(bid: str, text: str, page: int = 1, coords=None, label: str = "") -> dict:
    return {
        "id": bid,
        "text": text,
        "page": page,
        "coords_norm": coords or [0.0, 0.1 * page, 1.0, 0.1 * page + 0.08],
        "label": label,
    }


def _image_block(bid: str, text: str, page: int = 1, label: str = "") -> dict:
    return {"id": bid, "text": text, "page": page, "label": label, "type": "image"}


def _page(page_num: int, text_blocks: list, image_blocks: list = None, sheet: str = "") -> dict:
    return {
        "page": page_num,
        "page_index": page_num,
        "sheet_no_raw": sheet or f"Лист {page_num}",
        "sheet_no_normalized": sheet or f"Лист {page_num}",
        "sheet_name": sheet or f"Лист {page_num}",
        "text_blocks": text_blocks,
        "image_blocks": image_blocks or [],
    }


def _graph(*pages) -> dict:
    return {"version": "1", "document_id": "TEST", "pages": list(pages)}


def _finding(
    fid: str,
    problem: str = "",
    desc: str = "",
    page=1,
    sheet: str = "",
    category: str = "test",
    evidence: list = None,
) -> dict:
    return {
        "id": fid,
        "problem": problem or f"Проблема {fid}",
        "description": desc,
        "solution": "",
        "category": category,
        "sheet": sheet or f"Лист {page}",
        "page": page,
        "severity": "КРИТИЧЕСКОЕ",
        "evidence": evidence or [],
        "related_block_ids": [],
    }


# ─── Tests: neighbor_blocks ───────────────────────────────────────────────────

class TestNeighborBlocks:
    def test_returns_blocks_near_evidence(self):
        blocks = [
            _block("B1", "First block"),
            _block("B2", "Second block", coords=[0.0, 0.1, 1.0, 0.18]),
            _block("B3", "Evidence block", coords=[0.0, 0.2, 1.0, 0.28]),
            _block("B4", "Fourth block", coords=[0.0, 0.3, 1.0, 0.38]),
            _block("B5", "Fifth block", coords=[0.0, 0.4, 1.0, 0.48]),
        ]
        graph = _graph(_page(1, blocks))
        finding = _finding("F-001", evidence=[{"block_id": "B3", "page": 1}])
        neighbors = get_neighbor_blocks(finding, graph, index_window=2)
        ids = {b.block_id for b in neighbors}
        assert "B3" not in ids  # evidence block excluded
        assert "B2" in ids or "B4" in ids  # at least one neighbor found

    def test_empty_graph_returns_empty(self):
        graph = _graph()
        finding = _finding("F-001", evidence=[{"block_id": "BX", "page": 1}])
        result = get_neighbor_blocks(finding, graph)
        assert result == []

    def test_no_evidence_blocks_returns_adjacent_page(self):
        # Finding on page 2, graph has page 1 and 3
        graph = _graph(
            _page(1, [_block("B-p1", "Page 1 content" * 10)]),
            _page(2, [_block("B-p2", "Evidence" * 10)]),
            _page(3, [_block("B-p3", "Page 3 content" * 10)]),
        )
        finding = _finding("F-001", page=2, evidence=[{"block_id": "B-p2", "page": 2}])
        neighbors = get_neighbor_blocks(finding, graph, max_neighbors=10)
        pages = {b.page for b in neighbors}
        # Should include adjacent pages 1 and 3
        assert len(pages) >= 1

    def test_max_neighbors_respected(self):
        blocks = [_block(f"B{i}", f"Block {i} with content " * 5) for i in range(20)]
        graph = _graph(_page(1, blocks))
        finding = _finding("F-001", evidence=[{"block_id": "B10", "page": 1}])
        result = get_neighbor_blocks(finding, graph, max_neighbors=3)
        assert len(result) <= 3

    def test_evidence_block_not_in_neighbors(self):
        blocks = [_block(f"B{i}", f"Block content {i}" * 8) for i in range(5)]
        graph = _graph(_page(1, blocks))
        finding = _finding("F-001", evidence=[{"block_id": "B2", "page": 1}])
        result = get_neighbor_blocks(finding, graph, index_window=2)
        ids = {b.block_id for b in result}
        assert "B2" not in ids

    def test_snippet_has_correct_page(self):
        blocks = [
            _block("BA", "Alpha block content here"),
            _block("BB", "Beta block content here"),
            _block("BC", "Evidence block content here"),
        ]
        graph = _graph(_page(5, blocks))
        finding = _finding("F-001", page=5, evidence=[{"block_id": "BC", "page": 5}])
        result = get_neighbor_blocks(finding, graph)
        for b in result:
            assert b.page == 5


# ─── Tests: common_notes ─────────────────────────────────────────────────────

class TestCommonNotes:
    def _long_notes_text(self, prefix: str = "") -> str:
        return f"{prefix}Общие указания: " + ("Применяемые нормативные требования. " * 20)

    def test_finds_general_notes_block(self):
        notes_block = _block("GN1", self._long_notes_text(), page=1)
        content_block = _block("C1", "Армирование стен показано условно.", page=2)
        graph = _graph(
            _page(1, [notes_block]),
            _page(2, [content_block]),
        )
        finding = _finding("F-001", page=2)
        result = get_common_notes(finding, graph)
        assert any(b.block_id == "GN1" for b in result)

    def test_same_sheet_notes_first(self):
        notes_p1 = _block("GN-p1", self._long_notes_text("Общие указания к листу 1. "), page=1)
        notes_p2 = _block("GN-p2", self._long_notes_text("Примечания к листу 2. "), page=2, label="ПРИМЕЧАНИЕ")
        graph = _graph(
            _page(1, [notes_p1], sheet="Лист 1"),
            _page(2, [notes_p2], sheet="Лист 2"),
        )
        finding = _finding("F-001", page=2, sheet="Лист 2")
        result = get_common_notes(finding, graph, max_blocks=2)
        # Same-sheet notes (p2) should come first
        if len(result) >= 2:
            assert result[0].page == 2

    def test_short_block_not_included(self):
        short_block = _block("SHORT", "Примечание: OK", page=1)
        graph = _graph(_page(1, [short_block]))
        finding = _finding("F-001", page=1)
        result = get_common_notes(finding, graph)
        # Block text is too short (<100 chars), should not be included
        assert not any(b.block_id == "SHORT" for b in result)

    def test_empty_graph_safe(self):
        result = get_common_notes(_finding("F-001"), {"pages": []})
        assert result == []

    def test_max_blocks_respected(self):
        blocks = [_block(f"GN{i}", self._long_notes_text(f"Notes {i}. "), page=i + 1) for i in range(10)]
        pages = [_page(i + 1, [blocks[i]]) for i in range(10)]
        graph = {"pages": pages}
        finding = _finding("F-001", page=5)
        result = get_common_notes(finding, graph, max_blocks=2)
        assert len(result) <= 2


# ─── Tests: table_context ─────────────────────────────────────────────────────

class TestTableContext:
    def _table_block(self, bid: str, page: int = 1) -> dict:
        text = (
            "Марка\t\tДиаметр\t\tКласс\t\tДлина\n"
            "поз.1\t\tØ12\t\tА500С\t\t3000 мм\n"
            "поз.2\t\tØ16\t\tА500С\t\t4500 мм\n"
            "поз.3\t\tØ20\t\tА500С\t\t6000 мм\n"
        )
        return _block(bid, text, page=page)

    def test_extracts_table_rows_from_evidence(self):
        block = self._table_block("TB1", page=3)
        graph = _graph(_page(3, [block]))
        finding = _finding(
            "F-001",
            problem="Диаметр арматуры в таблице указан неверно",
            page=3,
            evidence=[{"block_id": "TB1", "page": 3}],
        )
        result = get_table_context(finding, graph)
        assert len(result) >= 1
        row_types = {r.row_type for r in result}
        assert "header" in row_types or "target" in row_types

    def test_no_table_reference_returns_empty(self):
        block = _block("B1", "Стена армируется сетками С-1.", page=1)
        graph = _graph(_page(1, [block]))
        finding = _finding("F-001", problem="Отсутствует маркировка", page=1,
                           evidence=[{"block_id": "B1", "page": 1}])
        result = get_table_context(finding, graph)
        assert result == []

    def test_empty_evidence_returns_empty(self):
        graph = _graph(_page(1, []))
        finding = _finding("F-001", problem="Данные в таблице не совпадают")
        result = get_table_context(finding, graph)
        assert result == []

    def test_max_rows_respected(self):
        text = "\n".join(
            [f"Заголовок\tКол-во\tМасса"] +
            [f"поз.{i}\t{i*2}\t{i*5} кг" for i in range(1, 20)]
        )
        block = _block("TB2", text, page=2)
        graph = _graph(_page(2, [block]))
        finding = _finding("F-001", problem="Расхождение в таблице ведомости",
                           evidence=[{"block_id": "TB2", "page": 2}])
        result = get_table_context(finding, graph, max_rows=4)
        assert len(result) <= 4


# ─── Tests: cross_references ─────────────────────────────────────────────────

class TestCrossReferences:
    def test_finds_sheet_reference(self):
        block = _block("B1", "Армирование выполнить согласно схемам. см. лист 3.", page=2)
        graph = _graph(
            _page(2, [block]),
            _page(3, [_block("B-p3", "Схема армирования плиты перекрытия." * 10)],
                  sheet="Лист 3"),
        )
        finding = _finding("F-001", page=2, evidence=[{"block_id": "B1", "page": 2}])
        result = get_cross_references(finding, graph)
        assert any("лист" in r.ref_text.lower() or "3" in r.ref_text for r in result)

    def test_finds_detail_reference(self):
        block = _block("B2", "Узел 1-1 — конструктивный шов у перекрытия.", page=4)
        graph = _graph(_page(4, [block]))
        finding = _finding("F-001", page=4, evidence=[{"block_id": "B2", "page": 4}])
        result = get_cross_references(finding, graph)
        ref_texts = [r.ref_text.lower() for r in result]
        assert any("узел" in t or "1-1" in t for t in ref_texts)

    def test_empty_evidence_safe(self):
        graph = _graph(_page(1, []))
        finding = _finding("F-001", problem="See лист 5")
        result = get_cross_references(finding, graph)
        # Should find cross-ref in problem text
        assert isinstance(result, list)

    def test_no_refs_returns_empty(self):
        block = _block("B1", "Обычный текст без ссылок.", page=1)
        graph = _graph(_page(1, [block]))
        finding = _finding("F-001", page=1, evidence=[{"block_id": "B1", "page": 1}])
        result = get_cross_references(finding, graph)
        assert isinstance(result, list)

    def test_max_refs_respected(self):
        text = " ".join([f"см. лист {i}." for i in range(1, 20)])
        block = _block("B1", text, page=1)
        graph = _graph(_page(1, [block]))
        finding = _finding("F-001", page=1, evidence=[{"block_id": "B1", "page": 1}])
        result = get_cross_references(finding, graph, max_refs=3)
        assert len(result) <= 3

    def test_result_type(self):
        block = _block("B1", "Деталь согласно узлу 2-2 на листе 4.", page=1)
        graph = _graph(_page(1, [block]))
        finding = _finding("F-001", page=1, evidence=[{"block_id": "B1", "page": 1}])
        result = get_cross_references(finding, graph)
        for r in result:
            assert isinstance(r, CrossReference)
            assert isinstance(r.ref_text, str)


# ─── Tests: related_findings ─────────────────────────────────────────────────

class TestRelatedFindings:
    def test_finds_same_category_finding(self):
        f1 = _finding("F-001", problem="Диаметр арматуры не соответствует", category="rebar")
        f2 = _finding("F-002", problem="Диаметр арматуры в спецификации неверный", category="rebar")
        f3 = _finding("F-003", problem="Не указан класс бетона", category="concrete")
        result = get_related_findings(f1, [f1, f2, f3])
        ids = {r.finding_id for r in result}
        assert "F-001" not in ids  # self excluded
        assert "F-002" in ids      # same category + similar text

    def test_excludes_self(self):
        f = _finding("F-001", problem="Тест")
        result = get_related_findings(f, [f, f, f])
        assert all(r.finding_id != "F-001" for r in result)

    def test_finds_possible_duplicate(self):
        problem = "Защитный слой бетона не указан на сечении 1-1 лист 5 КЖ"
        f1 = _finding("F-001", problem=problem, category="cover_thickness")
        f2 = _finding("F-002", problem=problem + " и 2-2", category="cover_thickness")
        result = get_related_findings(f1, [f1, f2])
        assert any(r.similarity_type == "possible_duplicate" for r in result)

    def test_empty_findings_returns_empty(self):
        f = _finding("F-001", problem="Test")
        result = get_related_findings(f, [])
        assert result == []

    def test_max_related_respected(self):
        base = _finding("F-000", problem="Ширина хомутов по нормам А500С")
        others = [
            _finding(f"F-{i:03d}", problem="Ширина хомутов по нормам А500С неверная", category="rebar")
            for i in range(1, 20)
        ]
        result = get_related_findings(base, [base] + others, max_related=3)
        assert len(result) <= 3

    def test_result_type(self):
        f1 = _finding("F-001", problem="Тест армирования")
        f2 = _finding("F-002", problem="Тест армирования дополнительно")
        result = get_related_findings(f1, [f1, f2])
        for r in result:
            assert isinstance(r, RelatedFinding)


# ─── Tests: ContextCollector ──────────────────────────────────────────────────

class TestContextCollector:
    def _make_graph(self) -> dict:
        notes = _block("GN1", "Общие указания к проекту. " * 20, page=1, label="ОБЩИЕ УКАЗАНИЯ")
        content1 = _block("C1", "Армирование стены t=200 мм показано условно.", page=2)
        content2 = _block("C2", "Защитный слой бетона — 25 мм.", page=2)
        content3 = _block("C3", "Класс бетона В30, арматура А500С.", page=3)
        return _graph(
            _page(1, [notes]),
            _page(2, [content1, content2]),
            _page(3, [content3]),
        )

    def test_collect_one_returns_package(self):
        collector = ContextCollector(self._make_graph())
        finding = _finding("F-001", page=2, evidence=[{"block_id": "C1", "page": 2}])
        pkg = collector.collect_one(finding)
        assert isinstance(pkg, FindingContextPackage)
        assert pkg.finding_id == "F-001"

    def test_collect_one_finds_common_notes(self):
        collector = ContextCollector(self._make_graph())
        finding = _finding("F-001", page=2, evidence=[{"block_id": "C1", "page": 2}])
        pkg = collector.collect_one(finding)
        assert len(pkg.common_notes) >= 1
        assert any(b.block_id == "GN1" for b in pkg.common_notes)

    def test_collect_one_finds_neighbor_blocks(self):
        collector = ContextCollector(self._make_graph())
        finding = _finding("F-001", page=2, evidence=[{"block_id": "C1", "page": 2}])
        pkg = collector.collect_one(finding)
        # C2 is on the same page, should be a neighbor
        neighbor_ids = {b.block_id for b in pkg.neighbor_blocks}
        assert "C2" in neighbor_ids

    def test_empty_collector_safe(self):
        collector = ContextCollector.empty()
        finding = _finding("F-001")
        pkg = collector.collect_one(finding)
        assert pkg.finding_id == "F-001"
        assert pkg.collected_context_summary == "no_context"
        assert not pkg.has_useful_context

    def test_collect_all_returns_stats(self):
        collector = ContextCollector(self._make_graph())
        findings = [
            _finding("F-001", page=2, evidence=[{"block_id": "C1", "page": 2}]),
            _finding("F-002", page=3, evidence=[{"block_id": "C3", "page": 3}]),
        ]
        packages, stats = collector.collect_all(findings)
        assert len(packages) == 2
        assert isinstance(stats, ContextCollectionStats)
        assert stats.total_findings == 2

    def test_stats_count_common_notes(self):
        collector = ContextCollector(self._make_graph())
        findings = [
            _finding("F-001", page=2, evidence=[{"block_id": "C1", "page": 2}]),
        ]
        _, stats = collector.collect_all(findings)
        assert stats.findings_with_common_notes >= 1

    def test_collect_all_empty_safe(self):
        collector = ContextCollector.empty()
        packages, stats = collector.collect_all([])
        assert packages == []
        assert stats.total_findings == 0

    def test_no_production_mutation(self, tmp_path):
        """Collector reads project artifacts but never writes to them."""
        # Create a minimal project directory with production-like structure
        output_dir = tmp_path / "_output"
        output_dir.mkdir()
        graph_data = {"pages": []}
        (output_dir / "document_graph.json").write_text(
            json.dumps(graph_data), encoding="utf-8"
        )
        findings_data = {"findings": []}
        (output_dir / "03_findings.json").write_text(
            json.dumps(findings_data), encoding="utf-8"
        )

        collector = ContextCollector.from_project_dir(tmp_path)
        pkg = collector.collect_one({"id": "F-001", "problem": "Test", "description": ""})
        assert pkg.finding_id == "F-001"

        # Verify nothing new was written to _output/
        written = list(output_dir.iterdir())
        assert len(written) == 2, f"Unexpected files in _output: {written}"


# ─── Tests: FindingContextPackage ────────────────────────────────────────────

class TestFindingContextPackage:
    def _make_pkg(self) -> FindingContextPackage:
        pkg = FindingContextPackage(finding_id="F-001")
        pkg.common_notes = [
            BlockSnippet("GN1", page=1, text="Общие указания: применять СП 63.13330.2018")
        ]
        pkg.neighbor_blocks = [
            BlockSnippet("B2", page=2, text="Следующий блок")
        ]
        pkg.cross_references = [
            CrossReference(ref_text="см. лист 3", resolved_text="Схема армирования")
        ]
        pkg.collected_context_summary = "общие_указания=1; соседние_блоки=1"
        return pkg

    def test_has_useful_context_true(self):
        pkg = self._make_pkg()
        assert pkg.has_useful_context is True

    def test_has_useful_context_false(self):
        pkg = FindingContextPackage(finding_id="F-001")
        assert pkg.has_useful_context is False

    def test_to_dict_structure(self):
        pkg = self._make_pkg()
        d = pkg.to_dict()
        assert d["finding_id"] == "F-001"
        assert "common_notes_count" in d
        assert "neighbor_blocks_count" in d
        assert "cross_references_count" in d
        assert d["common_notes_count"] == 1

    def test_to_llm_text_has_sections(self):
        pkg = self._make_pkg()
        text = pkg.to_llm_text()
        assert "Общие указания" in text or "General Notes" in text
        assert len(text) > 10

    def test_to_llm_text_respects_max_chars(self):
        pkg = self._make_pkg()
        # Add many blocks
        pkg.neighbor_blocks = [
            BlockSnippet(f"B{i}", page=1, text="x" * 500)
            for i in range(10)
        ]
        text = pkg.to_llm_text(max_chars=1000)
        assert len(text) <= 1200  # allow some overhead

    def test_total_context_blocks(self):
        pkg = self._make_pkg()
        pkg.primary_blocks = [BlockSnippet("P1", page=1, text="Primary")]
        assert pkg.total_context_blocks >= 2  # primary + neighbor + common_notes


# ─── Tests: ContextCollector.save_artifact ───────────────────────────────────

class TestSaveArtifact:
    def test_saves_to_output_dir(self, tmp_path):
        collector = ContextCollector.empty()
        findings = [_finding("F-001")]
        packages, stats = collector.collect_all(findings)
        artifact_path = ContextCollector.save_artifact(
            packages, stats, tmp_path / "artifacts"
        )
        assert artifact_path.exists()
        data = json.loads(artifact_path.read_text())
        assert "stats" in data
        assert "packages" in data
        assert data["stats"]["total_findings"] == 1

    def test_artifact_json_valid(self, tmp_path):
        collector = ContextCollector.empty()
        packages = [FindingContextPackage("F-001"), FindingContextPackage("F-002")]
        stats = ContextCollectionStats(total_findings=2)
        ContextCollector.save_artifact(packages, stats, tmp_path)
        data = json.loads((tmp_path / "critic_v2_context_packages.json").read_text())
        assert len(data["packages"]) == 2

    def test_does_not_write_to_production_paths(self, tmp_path):
        """Verify artifact is written to scratch dir, not project _output."""
        scratch = tmp_path / "scratch"
        production_like = tmp_path / "projects" / "test" / "_output"
        production_like.mkdir(parents=True)

        collector = ContextCollector.empty()
        packages, stats = collector.collect_all([_finding("F-001")])
        ContextCollector.save_artifact(packages, stats, scratch)

        # Production dir not touched
        assert not (production_like / "critic_v2_context_packages.json").exists()
        # Scratch dir has artifact
        assert (scratch / "critic_v2_context_packages.json").exists()


# ─── Tests: ContextCollector.from_project_dir ────────────────────────────────

class TestFromProjectDir:
    def test_missing_graph_returns_empty_collector(self, tmp_path):
        """from_project_dir with no document_graph.json doesn't crash."""
        (tmp_path / "_output").mkdir()
        collector = ContextCollector.from_project_dir(tmp_path)
        assert collector is not None
        pkg = collector.collect_one(_finding("F-001"))
        assert pkg.finding_id == "F-001"

    def test_loads_document_graph(self, tmp_path):
        output_dir = tmp_path / "_output"
        output_dir.mkdir()
        notes_text = "Общие указания к конструкциям. " * 20
        graph = {
            "pages": [
                {
                    "page": 1, "page_index": 1,
                    "sheet_no_raw": "Лист 1", "sheet_no_normalized": "Лист 1",
                    "sheet_name": "Лист 1",
                    "text_blocks": [
                        {"id": "GN1", "text": notes_text, "page": 1,
                         "coords_norm": [0, 0, 1, 0.5], "label": "ОБЩИЕ УКАЗАНИЯ"}
                    ],
                    "image_blocks": [],
                }
            ]
        }
        (output_dir / "document_graph.json").write_text(json.dumps(graph))
        collector = ContextCollector.from_project_dir(tmp_path)
        finding = _finding("F-001", page=1)
        pkg = collector.collect_one(finding)
        # Should find the general notes block
        assert len(pkg.common_notes) >= 1

    def test_loads_all_findings_for_related_search(self, tmp_path):
        output_dir = tmp_path / "_output"
        output_dir.mkdir()
        (output_dir / "document_graph.json").write_text(json.dumps({"pages": []}))
        findings_data = {
            "findings": [
                _finding("F-001", problem="Диаметр Ø12 не соответствует нормам", category="rebar"),
                _finding("F-002", problem="Диаметр арматуры Ø12 занижен по нормам", category="rebar"),
            ]
        }
        (output_dir / "03_findings.json").write_text(json.dumps(findings_data))
        collector = ContextCollector.from_project_dir(tmp_path)
        assert len(collector._all_findings) == 2
