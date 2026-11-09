import copy

from backend.app.pipeline.stages.findings_merge.source_lineage import text_candidate_lineage


def test_recovers_origin_without_promoting_to_evidence():
    finding = {"id": "F-1", "source_finding_ids": ["T-001", "T-001", "G-1"],
               "severity": "ПРОВЕРИТЬ ПО СМЕЖНЫМ", "related_block_ids": [], "evidence": []}
    original = copy.deepcopy(finding)
    source = {"text_findings": [{"id": "T-001", "finding": "Original bounded claim",
                               "source": "sheet 2", "related_block_ids": ["a", "a", "missing"]}]}
    assert text_candidate_lineage([finding], source, {"a"}) == 1
    receipt = finding.pop("source_candidate_lineage")
    assert finding == original
    assert receipt[0]["existing_block_ids"] == ["a"]
    assert receipt[0]["unresolved_block_ids"] == ["missing"]
    assert receipt[0]["status"] == "requires_semantic_review"
    finding["source_candidate_lineage"] = receipt
    assert text_candidate_lineage([finding], source, {"a"}) == 0


def test_missing_and_ambiguous_ids_never_borrow_other_candidate_evidence():
    finding = {"source_finding_ids": ["T-absent", "T-duplicate"]}
    source = {"text_findings": [{"id": "T-duplicate", "related_block_ids": ["a"]}] * 2}
    text_candidate_lineage([finding], source, {"a"})
    assert finding["source_candidate_lineage"] == [
        {"source_finding_id": "T-absent", "status": "missing_candidate"},
        {"source_finding_id": "T-duplicate", "status": "ambiguous_candidate_id"},
    ]
    finding["source_finding_ids"] = []
    assert text_candidate_lineage([finding], source, {"a"}) == 1
    assert "source_candidate_lineage" not in finding


def test_invalid_candidate_refs_do_not_become_character_ids():
    finding = {"source_finding_ids": ["T-001"]}
    text_candidate_lineage([finding], {"text_findings": [{"id": "T-001", "related_block_ids": "abc"}]}, {"a"})
    assert finding["source_candidate_lineage"][0]["existing_block_ids"] == []


def test_merge_backfill_reads_text_candidates_from_explicit_run(tmp_path):
    import json
    from backend.app.pipeline.stages.findings_merge.runner import backfill_text_evidence_in_findings

    finding = {"id": "F-001", "sheet": "Лист 2", "source_finding_ids": ["T-001"],
               "evidence": [], "related_block_ids": []}
    for name, data in {
        "03_findings.json": {"findings": [finding]},
        "02_text_analysis.json": {"text_findings": [{"id": "T-001", "finding": "claim", "related_block_ids": ["a"]}]},
        "01_blocks_analysis.json": {"block_analyses": [{"block_id": "a"}]},
    }.items():
        (tmp_path / name).write_text(json.dumps(data))
    backfill_text_evidence_in_findings("unused", output_dir=tmp_path)
    result = json.loads((tmp_path / "03_findings.json").read_text())["findings"][0]
    assert result["source_candidate_lineage"][0]["existing_block_ids"] == ["a"]
    assert result["evidence"] == [] and result["related_block_ids"] == []
