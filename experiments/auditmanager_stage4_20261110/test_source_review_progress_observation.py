import json
from experiments.auditmanager_stage4_20261110.observe_current_method_collection import source_review_progress


def setup_files(tmp_path, *, budget=2, reports=None):
    output = tmp_path / 'out'
    output.mkdir()
    (tmp_path/'input_manifest.json').write_text(json.dumps([{'output_dir': str(output)}]))
    (tmp_path/'protocol.json').write_text(json.dumps({'method_config': {'source_review_max_findings': budget}}))
    (output/'03_source_review_queue.json').write_text(json.dumps({'pending_count': 3, 'packets': [{'status': 'pending', 'finding_id': str(i), 'finding_snapshot': 'SECRET_PROJECT_CLAIM'} for i in range(3)]}))
    (output/'03_source_review_candidates.json').write_text(json.dumps({'status': 'running', 'reports': reports or []}))
    return output


def test_counts_are_not_acceptance_and_do_not_expose_claims(tmp_path):
    setup_files(tmp_path, reports=[{'finding_id': '0', 'status': 'not_completed', 'response': 'SECRET_MODEL_VERDICT'}])
    result = source_review_progress(tmp_path)
    assert result['planned_workflows'] == 2
    assert result['outside_budget'] == 1
    assert result['remaining_without_report'] == 1
    assert result['report_status_counts'] == {'not_completed': 1}
    assert 'SECRET' not in json.dumps(result)
    assert result['semantic_results_read'] is False


def test_inconsistent_queue_count_is_not_progress(tmp_path):
    output = setup_files(tmp_path)
    (output/'03_source_review_queue.json').write_text(json.dumps({'pending_count': 4, 'packets': []}))
    assert source_review_progress(tmp_path)['status'] == 'queue_count_inconsistent'


def test_duplicate_reports_are_not_double_counted(tmp_path):
    setup_files(tmp_path, reports=[{'finding_id': '0'}, {'finding_id': '0'}])
    assert source_review_progress(tmp_path)['status'] == 'report_count_inconsistent'


def test_missing_queue_is_not_failure(tmp_path):
    output = setup_files(tmp_path)
    (output/'03_source_review_queue.json').unlink()
    assert source_review_progress(tmp_path)['status'] == 'not_planned'
