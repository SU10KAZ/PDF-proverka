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


def test_prescan_trigger_reaches_review_and_preserves_final_trace(tmp_path, monkeypatch):
    from backend.app.pipeline.stages.findings_merge.candidate_coverage import build_candidate_coverage, write_candidate_coverage
    from backend.app.pipeline.stages.findings_merge.candidate_decisions import reconcile_candidate_decisions
    from backend.app.pipeline.stages.prepare.codex_targeted_findings import combine_findings_with_targeted
    import backend.app.pipeline.stages.prepare.candidate_recovery as recovery
    monkeypatch.setattr(recovery, 'resolve_blocks_dir', lambda _: tmp_path / 'missing_crops')
    trigger = {'rule_key': 'air_curtain_u_designation', 'status': 'not_checked',
               'claim': 'Check equipment identity', 'source_block_hints': ['t2'],
               'origin': 'deterministic/md_prescan', 'semantic_verified': False}
    text = {'text_findings': [], 'prescan_review_requests': [trigger]}
    base = {'findings': [{'id': 'F-001', 'problem': 'Existing useful finding'}]}
    for name, data in {'02_text_analysis.json': text,
                       '01_blocks_analysis.json': {}, '03_findings.json': base}.items():
        (tmp_path / name).write_text(json.dumps(data))
    md = tmp_path / 'document.md'
    md.write_text('## Page 1\n### BLOCK [TEXT]: t1\nunrelated source\n'
                  '## Page 2\n### BLOCK [TEXT]: t2\nEquipment schedule\n')
    request, = build_recovery_passes(tmp_path, md)
    packet = json.loads(request.messages[-1]['content'])
    candidate = packet['candidate']
    sid = candidate['source_id']
    assert sid.startswith('P-')
    assert candidate['candidate'] == trigger
    assert packet['selected_md_pages'] == [2]
    assert packet['allowed_source_refs'] == ['t2']
    assert 'unrelated source' not in packet['authoritative_md']
    assert json.loads((tmp_path / '03_findings.json').read_text()) == base
    assert build_candidate_coverage(text, {}, base)['candidates'][0]['source_id'] == sid
    for action in ('refuted', 'retained'):
        decision = {'origin': 'prescan', 'pointer': candidate['pointer'],
                    'decision': action, 'reason': 'Source-based decision', 'source_refs': ['t2']}
        payload = {'candidate_decisions': [decision], 'findings': []}
        if action == 'retained':
            payload['findings'] = [{'problem': 'Supported identity conflict',
                'source_finding_ids': [sid], 'evidence': [{'block_id': 't2'}]}]
        validated = validate_recovery_payload(request, payload)
        combined = combine_findings_with_targeted(base, [(request.stage, validated)])
        assert combined['findings'][0] == base['findings'][0]
        assert len(combined['findings']) == (2 if action == 'retained' else 1)
        coverage = build_candidate_coverage(text, {}, combined)
        (tmp_path / '03_findings.json').write_text(json.dumps(combined))
        (tmp_path / 'document_graph.json').write_text(json.dumps({
            'pages': [{'page': 2, 'text_blocks': [{'id': 't2'}]}]}))
        receipt = write_candidate_coverage(tmp_path)['decision_coverage']
        assert receipt['not_checked'] == 0
        assert receipt['semantic_verified'] is False
    assert base == {'findings': [{'id': 'F-001', 'problem': 'Existing useful finding'}]}


def test_source_refs_adapter_preserves_addresses_without_inventing_quotes():
    from backend.app.pipeline.stages.prepare.codex_targeted_findings import CodexTargetedPass
    packet = {'candidate': {'source_id': 'D-source', 'origin': 'graphic', 'pointer': '/block_analyses/0/deferred_findings/0'},
              'attached_image_block_ids_in_order': ['g'], 'allowed_source_refs': ['g', 't'],
              'existing_findings': []}
    request = CodexTargetedPass('candidate_recovery_test', 'test.json',
                               [{'role': 'user', 'content': json.dumps(packet)}], ())
    finding = {'source_finding_ids': ['D-source'], 'source_refs': ['g', 't'],
               'report_section': 'questions', 'finding': 'Locate the third item'}
    result = validate_recovery_payload(request, {'findings': [finding]})
    adapted, = result['findings']
    assert [e['block_id'] for e in adapted['evidence']] == ['g', 't']
    assert all('quote' not in e for e in adapted['evidence'])
    assert adapted['recovery_semantic_verified'] is False
    assert adapted['report_section'] == 'questions'
    assert 'evidence' not in finding
    rejected = validate_recovery_payload(request, {'findings': [{**finding, 'source_refs': ['g', 'unknown']}]})
    assert rejected['findings'] == []
    assert rejected['recovery_pending'][0]['reason'] == 'missing_inspected_source_reference'


def test_text_evidence_with_explicit_refs_is_preserved_without_quote_invention():
    from backend.app.pipeline.stages.prepare.codex_targeted_findings import CodexTargetedPass
    packet = {'candidate': {'source_id': 'G-084', 'origin': 'graphic', 'pointer': '/findings/0'},
              'attached_image_block_ids_in_order': ['g'],
              'allowed_source_refs': ['g', 't'], 'existing_findings': []}
    request = CodexTargetedPass('candidate_recovery_test', 'test.json',
                               [{'role': 'user', 'content': json.dumps(packet)}], ())
    finding = {'source_finding_ids': ['G-084'], 'source_refs': ['g', 't'],
               'evidence': 'Six lengths sum to 730; schedule says 700.'}
    accepted, = validate_recovery_payload(request, {'findings': [finding]})['findings']
    assert accepted['recovery_original_evidence_text'] == finding['evidence']
    assert [e['block_id'] for e in accepted['evidence']] == ['g', 't']
    assert all('quote' not in e for e in accepted['evidence'])
    assert accepted['recovery_semantic_verified'] is False
    assert isinstance(finding['evidence'], str)
    for refs in ([], ['unknown'], ['g', 'unknown']):
        assert not validate_recovery_payload(request, {'findings': [
            {**finding, 'source_refs': refs}]})['findings']
    # Do not replace a structured evidence array that cites a wrong source.
    assert not validate_recovery_payload(request, {'findings': [
        {**finding, 'evidence': [{'block_id': 'unknown'}]}]})['findings']
    packet['attached_image_block_ids_in_order'] = []
    request = CodexTargetedPass('candidate_recovery_test', 'test.json',
                               [{'role': 'user', 'content': json.dumps(packet)}], ())
    assert not validate_recovery_payload(request, {'findings': [finding]})['findings']


def test_mixed_metadata_candidate_survives_gate_to_source_recovery(tmp_path, monkeypatch):
    from backend.app.pipeline.stages.block_analysis.finding_evidence_gate import gate_findings
    import backend.app.pipeline.stages.prepare.candidate_recovery as recovery

    monkeypatch.setattr(recovery, 'resolve_blocks_dir', lambda _: tmp_path / 'missing_crops')
    original = {
        'finding': 'Номер 0.0.2.2 присвоен венткамере и помещению РУ.',
        'recommendation': 'Исправить тип фрагмента в разметке.',
        'claim_type': 'contradiction', 'affected_entity': '0.0.2.2',
        'evidence_quote': '0.0.2.2 — Венткамера; 0.0.2.2 — РУ.',
        'evidence_kind': 'text', 'context_status': 'sufficient',
        'confidence': 0.95, 'counterevidence_checked': True,
        'source_block_ids': ['t1'],
    }
    published, deferred, _ = gate_findings([original])
    assert published == []
    assert deferred[0]['_evidence_gate']['reasons'] == ['internal_metadata_comparison']
    base = {'findings': [{'id': 'F-existing', 'problem': 'Existing useful result'}]}
    for name, data in {
        '02_text_analysis.json': {'text_findings': []},
        '01_blocks_analysis.json': {'block_analyses': [{
            'block_id': 't1', 'findings': published, 'deferred_findings': deferred}]},
        '03_findings.json': base,
    }.items():
        (tmp_path / name).write_text(json.dumps(data, ensure_ascii=False))
    md = tmp_path / 'document.md'
    md.write_text('## Page 1\n### BLOCK [TEXT]: t1\n' + original['evidence_quote'])
    request, = build_recovery_passes(tmp_path, md)
    packet = json.loads(request.messages[-1]['content'])
    candidate = packet['candidate']
    assert candidate['deferred'] is True
    assert candidate['source_id'].startswith('D-')
    assert candidate['candidate'] == deferred[0]
    assert original['evidence_quote'] in packet['authoritative_md']
    assert 't1' in packet['allowed_source_refs']
    assert packet['existing_findings'] == base['findings']
    assert json.loads((tmp_path / '03_findings.json').read_text()) == base
    assert '_evidence_gate' not in original
