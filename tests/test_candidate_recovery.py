import json
from backend.app.pipeline.stages.prepare.candidate_recovery import build_recovery_passes, validate_recovery_payload


def test_recovery_selects_unlinked_and_requires_graphic_evidence(tmp_path, monkeypatch):
    import backend.app.pipeline.stages.prepare.candidate_recovery as recovery
    blocks = tmp_path / 'crops'
    blocks.mkdir()
    monkeypatch.setattr(recovery, 'resolve_blocks_dir', lambda _: blocks)
    data = {'02_text_analysis.json': {'text_findings': [{'id': 'T-1'}]},
            '01_blocks_analysis.json': {'block_analyses': [{'block_id': 'g', 'findings': [{'id': 'G-1'}, {'id': 'G-2'}]}]},
            '03_findings.json': {'findings': [{'id': 'F-1', 'source_finding_ids': ['T-1']}]},
            'document_graph.json': {'pages': [{'page': 2, 'image_blocks': [{'id': 'g'}]}]}}
    for name, payload in data.items():
        (tmp_path / name).write_text(json.dumps(payload))
    md = tmp_path / 'document.md'
    md.write_text('## Page 1\nunrelated\n## Page 2 — Sheet 53 — Cabinet\n### BLOCK [GRAPHIC]: g\nmodel summary\n')
    passes = build_recovery_passes(tmp_path, md)
    assert len(passes) == 1
    packet = json.loads(passes[0].messages[-1]['content'])
    assert 'unrelated' not in packet['authoritative_md']
    assert packet['selected_md_pages'] == [2]
    assert [c['source_id'] for c in packet['candidate_group']] == ['G-1', 'G-2']
    proposed = {'source_finding_ids': ['G-1'], 'evidence': [{'block_id': 'g'}]}
    result = validate_recovery_payload(passes[0], {'findings': [proposed]})
    assert result['findings'] == []
    assert result['recovery_pending'][0]['reason'] == 'graphic_source_unavailable'
    (blocks / 'index.json').write_text(json.dumps({'blocks': [{'block_id': 'g', 'file': 'g.png'}]}))
    (blocks / 'g.png').write_bytes(b'local test crop; inference stubbed')
    passes = build_recovery_passes(tmp_path, md)
    assert passes[0].image_paths == ((blocks / 'g.png').resolve(),)
    result = validate_recovery_payload(passes[0], {'findings': [proposed]})
    assert len(result['findings']) == 1
    assert result['findings'][0]['recovery_semantic_verified'] is False
    proposed['evidence'] = [{'block_id': 'invented'}]
    assert not validate_recovery_payload(passes[0], {'findings': [proposed]})['findings']


def test_deferred_graphic_request_is_scheduled_without_becoming_a_finding(tmp_path, monkeypatch):
    import backend.app.pipeline.stages.prepare.candidate_recovery as recovery
    monkeypatch.setattr(recovery, 'resolve_blocks_dir', lambda _:tmp_path/'missing_crops')
    for name, data in {
        '02_text_analysis.json': {'text_findings':[]},
        '01_blocks_analysis.json': {'block_analyses':[{'block_id':'g', 'findings':[],
            'deferred_findings':[{'finding':'aerator count','required_context':['upper roof']}]}]},
        '03_findings.json': {'findings':[]},
    }.items():
        (tmp_path/name).write_text(json.dumps(data))
    md = tmp_path/'document.md';md.write_text('## Page 2\n### BLOCK #1 [GRAPHIC]: g\nLower roof\n')
    requests = build_recovery_passes(tmp_path,md)
    assert len(requests) == 1
    packet = json.loads(requests[0].messages[-1]['content'])
    assert packet['candidate']['source_id'].startswith('D-')
    assert packet['candidate']['candidate']['required_context'] == ['upper roof']
    result = validate_recovery_payload(requests[0],{'findings': [{
        'source_finding_ids':[packet['candidate']['source_id']], 'evidence':[{'block_id':'g'}]}]})
    assert result['findings'] == []
