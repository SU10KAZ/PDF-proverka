import json
from backend.app.pipeline.stages.findings_merge.candidate_coverage import (
    build_candidate_coverage, write_candidate_coverage,
)


def test_links_do_not_certify_meaning_and_omissions_remain_visible():
    text = {'text_findings': [{'id': 'T-1', 'finding': 'two distinct locations'}, {'id': 'T-2'}]}
    blocks = {'block_analyses': [{'block_id': 'b', 'findings': [{'id': 'G-1'}],
                                'deferred_findings': [{'finding': 'needs upper plan'}]}]}
    final = {'findings': [{'id': 'F-1', 'source_finding_ids': ['T-1', 'T-1', 'unknown']} ]}
    report = build_candidate_coverage(text, blocks, final)
    assert report['candidate_count'] == 4
    assert report['counts']['unlinked_requires_trace'] == 2
    assert report['candidates'][0]['linked_findings'] == [{'id': 'F-1', 'pointer': '/findings/0'}]
    assert report['candidates'][3]['candidate']['finding'] == 'needs upper plan'
    assert report['unresolved_final_source_ids'] == ['unknown']
    assert not any(r['semantic_preservation_verified'] for r in report['candidates'])


def test_duplicate_ids_cannot_certify_linkage():
    report = build_candidate_coverage({'text_findings': [{'id': 'T-1'}, {'id': 'T-1'}]}, {},
                                      {'findings': [{'id': 'F-1', 'source_finding_ids': ['T-1']}]})
    assert report['counts'] == {'ambiguous_source_identity': 2}


def test_file_receipt_is_repeatable_and_does_not_modify_sources(tmp_path):
    inputs = {'02_text_analysis.json': {'text_findings': [{'id': 'T-1'}]},
              '01_blocks_analysis.json': {'block_analyses': []},
              '03_findings.json': {'findings': []}}
    for name, data in inputs.items():
        (tmp_path / name).write_text(json.dumps(data))
    before = {name: (tmp_path / name).read_bytes() for name in inputs}
    result = write_candidate_coverage(tmp_path)
    assert result['input_inventory_complete']
    assert result == write_candidate_coverage(tmp_path)
    assert before == {name: (tmp_path / name).read_bytes() for name in inputs}
    (tmp_path / '01_blocks_analysis.json').unlink()
    assert not write_candidate_coverage(tmp_path)['input_inventory_complete']
