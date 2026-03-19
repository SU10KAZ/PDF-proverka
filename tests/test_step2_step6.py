"""Тесты Step 2 (Compact/Merge) и Step 6 (Grounding)."""
import json
import pytest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from webapp.services.grounding_service import (
    classify_grounding_level,
    _finding_is_well_grounded,
    compute_grounding_candidates,
)


# ─── Grounding levels ─────────────────────────────────────────────────────

class TestClassifyGroundingLevel:
    def test_strong_with_source_and_evidence(self):
        """source_block_ids + согласованный evidence → strong."""
        f = {
            "source_block_ids": ["IMG-001"],
            "evidence": [{"type": "image", "block_id": "IMG-001", "page": 5}],
            "related_block_ids": ["IMG-001"],
        }
        assert classify_grounding_level(f) == "grounded_strong"

    def test_strong_with_source_and_text_refs(self):
        """source_block_ids + selected_text → strong."""
        f = {
            "source_block_ids": ["IMG-001"],
            "selected_text_block_ids": ["TB-001"],
            "evidence": [],
            "related_block_ids": [],
        }
        assert classify_grounding_level(f) == "grounded_strong"

    def test_strong_with_real_evidence_and_related(self):
        """Real image evidence (non-grounding-service) + related → strong."""
        f = {
            "evidence": [{"type": "image", "block_id": "IMG-001", "page": 5}],
            "related_block_ids": ["IMG-001"],
        }
        assert classify_grounding_level(f) == "grounded_strong"

    def test_weak_only_related(self):
        """Только related_block_ids без source/evidence → weak."""
        f = {
            "related_block_ids": ["IMG-001"],
            "evidence": [],
        }
        assert classify_grounding_level(f) == "grounded_weak"

    def test_weak_only_grounding_service_evidence(self):
        """Evidence только от grounding_service → weak."""
        f = {
            "evidence": [{"type": "image", "block_id": "X", "source": "grounding_service"}],
            "related_block_ids": [],
        }
        assert classify_grounding_level(f) == "grounded_weak"

    def test_weak_only_candidates(self):
        """Только grounding_candidates → weak."""
        f = {
            "grounding_candidates": [{"block_id": "X", "score": 0.3}],
            "evidence": [],
            "related_block_ids": [],
        }
        assert classify_grounding_level(f) == "grounded_weak"

    def test_ungrounded_empty(self):
        """Пустой finding → ungrounded."""
        f = {}
        assert classify_grounding_level(f) == "ungrounded"

    def test_ungrounded_no_source_no_evidence(self):
        """Нет source, evidence, related → ungrounded."""
        f = {
            "evidence": [],
            "related_block_ids": [],
            "source_block_ids": [],
        }
        assert classify_grounding_level(f) == "ungrounded"

    def test_well_grounded_requires_strong(self):
        """_finding_is_well_grounded требует strong level."""
        weak = {"related_block_ids": ["X"], "evidence": []}
        assert _finding_is_well_grounded(weak) is False

        strong = {
            "source_block_ids": ["X"],
            "evidence": [{"type": "image", "block_id": "X"}],
            "related_block_ids": ["X"],
        }
        assert _finding_is_well_grounded(strong) is True


class TestComputeGroundingCandidates:
    def test_source_block_bonus(self):
        """source_block_ids получает bonus при ранжировании."""
        findings = [{
            "id": "F-001",
            "problem": "Кабель не соответствует спецификации",
            "description": "Сечение кабеля ВВГнг 5x10 вместо 5x16",
            "source_block_ids": ["BLOCK_A"],
            "evidence": [],
            "related_block_ids": [],
        }]
        blocks = [
            {"block_id": "BLOCK_A", "page": 5, "summary": "Кабель ВВГнг",
             "key_values_read": ["ВВГнг 5x10"], "findings": []},
            {"block_id": "BLOCK_B", "page": 6, "summary": "Кабель ВВГнг спецификация",
             "key_values_read": ["ВВГнг 5x16"], "findings": []},
        ]
        result = compute_grounding_candidates(findings, blocks)
        candidates = result[0].get("grounding_candidates", [])
        assert len(candidates) > 0
        # BLOCK_A должен быть первым (source bonus)
        assert candidates[0]["block_id"] == "BLOCK_A"

    def test_grounding_level_assigned(self):
        """Каждый finding получает grounding_level."""
        findings = [
            {"id": "F-001", "problem": "Test", "description": "Test desc",
             "evidence": [], "related_block_ids": []},
        ]
        blocks = [
            {"block_id": "B1", "page": 1, "summary": "Test summary",
             "findings": [], "key_values_read": []},
        ]
        result = compute_grounding_candidates(findings, blocks)
        assert "grounding_level" in result[0]

    def test_strong_grounded_skipped(self):
        """Strong-grounded finding не трогается."""
        findings = [{
            "id": "F-001",
            "problem": "Test",
            "description": "Test desc",
            "source_block_ids": ["B1"],
            "evidence": [{"type": "image", "block_id": "B1"}],
            "related_block_ids": ["B1"],
        }]
        blocks = [
            {"block_id": "B1", "page": 1, "summary": "Test", "findings": [],
             "key_values_read": []},
        ]
        result = compute_grounding_candidates(findings, blocks)
        assert result[0]["grounding_level"] == "grounded_strong"
        assert "grounding_candidates" not in result[0]


# ─── Compact fallback ─────────────────────────────────────────────────────

class TestCompactPageSheetMapFallback:
    def test_v1_fallback_from_md(self, tmp_path):
        """v1 compact строит page_sheet_map из MD если graph пуст."""
        # Создаём минимальный MD
        md_content = """## СТРАНИЦА 3
**Лист:** 1
**Наименование листа:** Общие данные

### BLOCK [TEXT]: T1
Текст

## СТРАНИЦА 5
**Лист:** 2 (из 10)
**Наименование листа:** План 1-го этажа

### BLOCK [IMAGE]: I1
Чертёж
"""
        md_path = tmp_path / "test_document.md"
        md_path.write_text(md_content, encoding="utf-8")

        from webapp.services.task_builder import _extract_page_to_sheet_map
        mapping = _extract_page_to_sheet_map(str(md_path))

        assert len(mapping) >= 2
        assert mapping.get(3) == "1"
        assert mapping.get(5) == "2 (из 10)"


# ─── Legacy project checker ──────────────────────────────────────────────

class TestLegacyChecker:
    def test_detects_legacy_v1_graph(self, tmp_path):
        """Checker выявляет v1 graph."""
        from tools.check_project_artifacts import check_project

        # Создаём минимальную структуру проекта
        (tmp_path / "project_info.json").write_text(
            json.dumps({"project_id": "TEST/001"}), encoding="utf-8"
        )
        output_dir = tmp_path / "_output"
        output_dir.mkdir()
        (output_dir / "document_graph.json").write_text(
            json.dumps({"version": 1, "pages": []}), encoding="utf-8"
        )

        result = check_project(tmp_path)
        assert result["is_legacy"] is True
        assert result["graph_version"] == 1
        assert any("v1" in w for w in result["warnings"])

    def test_detects_filename_block_evidence(self, tmp_path):
        """Checker выявляет filename-form block_evidence."""
        from tools.check_project_artifacts import check_project

        (tmp_path / "project_info.json").write_text(
            json.dumps({"project_id": "TEST/002"}), encoding="utf-8"
        )
        output_dir = tmp_path / "_output"
        output_dir.mkdir()
        (output_dir / "document_graph.json").write_text(
            json.dumps({"version": 2, "pages": [
                {"page": 1, "sheet_no_raw": "1", "text_blocks": [], "image_blocks": []}
            ]}), encoding="utf-8"
        )
        (output_dir / "_findings_compact.json").write_text(
            json.dumps({
                "page_sheet_map": {"1": "1"},
                "blocks_compact": [],
                "preliminary_findings": [
                    {"id": "G-001", "block_evidence": "block_IMG.png"},
                ],
            }), encoding="utf-8"
        )

        result = check_project(tmp_path)
        assert result["findings_block_evidence_filename"] == 1

    def test_ok_project_not_legacy(self, tmp_path):
        """Проект с v2 graph и полными данными → не legacy."""
        from tools.check_project_artifacts import check_project

        (tmp_path / "project_info.json").write_text(
            json.dumps({"project_id": "TEST/003"}), encoding="utf-8"
        )
        output_dir = tmp_path / "_output"
        output_dir.mkdir()
        (output_dir / "document_graph.json").write_text(
            json.dumps({"version": 2, "pages": [
                {"page": 1, "sheet_no_raw": "1", "text_blocks": [], "image_blocks": []}
            ]}), encoding="utf-8"
        )
        (output_dir / "_findings_compact.json").write_text(
            json.dumps({
                "page_sheet_map": {"1": "1"},
                "blocks_compact": [
                    {"block_id": "I1", "selected_text_block_ids": ["T1"]},
                ],
                "preliminary_findings": [
                    {"id": "G-001", "block_evidence": "I1"},
                ],
            }), encoding="utf-8"
        )

        result = check_project(tmp_path)
        assert result["is_legacy"] is False


# ─── Merge output contract ────────────────────────────────────────────────

class TestMergeContract:
    def test_merge_source_g_ids_in_schema(self):
        """merge_source_g_ids должен быть в merge prompt schema."""
        prompt_path = Path(__file__).parent.parent / ".claude" / "findings_merge_task.md"
        content = prompt_path.read_text(encoding="utf-8")
        assert "merge_source_g_ids" in content

    def test_source_block_ids_in_schema(self):
        """source_block_ids должен быть в merge prompt schema."""
        prompt_path = Path(__file__).parent.parent / ".claude" / "findings_merge_task.md"
        content = prompt_path.read_text(encoding="utf-8")
        assert "source_block_ids" in content

    def test_selected_text_block_ids_in_merge_schema(self):
        """selected_text_block_ids должен быть в merge prompt."""
        prompt_path = Path(__file__).parent.parent / ".claude" / "findings_merge_task.md"
        content = prompt_path.read_text(encoding="utf-8")
        assert "selected_text_block_ids" in content

    def test_evidence_text_refs_in_merge_schema(self):
        """evidence_text_refs должен быть в merge prompt."""
        prompt_path = Path(__file__).parent.parent / ".claude" / "findings_merge_task.md"
        content = prompt_path.read_text(encoding="utf-8")
        assert "evidence_text_refs" in content

    def test_source_block_ids_distinct_from_related(self):
        """source_block_ids и related_block_ids — разные поля в schema."""
        prompt_path = Path(__file__).parent.parent / ".claude" / "findings_merge_task.md"
        content = prompt_path.read_text(encoding="utf-8")
        # Оба поля присутствуют как отдельные описания
        assert "source_block_ids" in content
        assert "related_block_ids" in content
        # Описание source != related
        assert "где замечание реально ОБНАРУЖЕНО" in content.lower() or \
               "ГДЕ замечание реально ОБНАРУЖЕНО" in content
