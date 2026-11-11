import hashlib
import json
from backend.app.pipeline.stages.prepare.recovery_context import search_md_context, build_context_followup
from backend.app.pipeline.stages.prepare.codex_targeted_findings import CodexTargetedPass

MD = '## Page 1 — Sheet A\n### BLOCK #1 [TEXT]: b1\nMark U1.1\n## Page 2\n### BLOCK #2 [GRAPHIC]: g1\nSummary U1.1\n'


def test_search_keeps_source_routes_and_reports_budget_and_zero_matches():
    result = search_md_context(MD, ['U1.1', 'missing', '.*'], max_blocks=1)
    assert result['hits'][0]['block_id'] == 'b1'
    assert result['hits'][0]['page'] == 1
    assert result['backlog'][0]['block_id'] == 'g1'
    assert result['query_hit_counts']['missing'] == 0
    assert result['query_hit_counts']['.*'] == 0
    assert not result['absence_proven']


def test_followup_hash_binds_md_and_does_not_promote_graphic_summary(tmp_path):
    path = tmp_path/'document.md';path.write_text(MD)
    packet = {'md_source': {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()},
              'authoritative_md': '', 'allowed_source_refs': [],
              'candidate': {'origin':'text','pointer':'/text_findings/0'}}
    request = CodexTargetedPass('candidate_recovery_x', 'x.json',
                               [{'role':'system','content':'review'}, {'role':'user','content':json.dumps(packet)}])
    payload = {'candidate_decisions':[{'origin':'text','pointer':'/text_findings/0','decision':'needs_data','context_queries':['U1.1']}]}
    followup, receipt = build_context_followup(request, payload)
    assert receipt['status'] == 'scheduled'
    updated = json.loads(followup.messages[-1]['content'])
    assert updated['allowed_source_refs'] == ['b1']
    assert len(updated['retrieved_context']) == 2
    assert build_context_followup(followup, payload)[0] is None
    path.write_text(MD+'changed')
    assert build_context_followup(request, payload)[1]['status'] == 'md_version_changed'


async def _exercise_pipeline_followup(monkeypatch, tmp_path):
    from backend.app.services.llm import claude_runner
    from backend.app.models.usage import LLMResult
    from backend.app.pipeline.stages.prepare import codex_targeted_findings as targeted
    path = tmp_path/'document.md'; path.write_text(MD)
    candidate = {'origin':'text','pointer':'/text_findings/0','source_id':'T-1'}
    packet = {'candidate':candidate, 'existing_findings':[], 'allowed_source_refs':[], 'authoritative_md':'',
              'attached_image_block_ids_in_order':[],
              'md_source':{'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()}}
    request = CodexTargetedPass('candidate_recovery_test','test.json',
                               [{'role':'system','content':'review'},{'role':'user','content':json.dumps(packet)}])
    calls = []
    async def fake_stage(**kwargs):
        calls.append(kwargs['stage'])
        payload = {'findings':[], 'candidate_decisions':[{
            'origin':'text','pointer':'/text_findings/0','decision':'needs_data',
            'reason':'needs more context','source_refs':[], 'context_queries':['U1.1']}]}
        return 0, 'ok', LLMResult(json_data=payload, model='codex/gpt-6-astra')
    monkeypatch.setattr(targeted,'build_targeted_findings_passes',lambda *a:[request])
    monkeypatch.setattr(claude_runner,'_codex_targeted_findings_enabled',lambda:True)
    monkeypatch.setattr(claude_runner,'_run_codex_json_stage',fake_stage)
    monkeypatch.setattr(claude_runner,'_save_audit_trail',lambda *a,**k:None)
    result = await claude_runner._run_codex_targeted_findings_merge(
        project_info={}, project_id='test',model='codex/gpt-6-astra',
        base_result=LLMResult(json_data={'findings':[]}), base_text='base',on_output=None,
        output_dir=tmp_path,version_dir=tmp_path,version_id='v001')
    assert result[0] == 0
    assert calls == ['candidate_recovery_test','candidate_recovery_test_context']
    reports = result[2].json_data['candidate_recovery_reports']
    assert reports[0]['context_retrieval']['status'] == 'scheduled'
    assert reports[1]['context_retrieval']['status'] == 'followup_limit'


def test_pipeline_schedules_exactly_one_context_followup(monkeypatch, tmp_path):
    import asyncio
    asyncio.run(_exercise_pipeline_followup(monkeypatch,tmp_path))


def test_common_phrase_does_not_starve_late_exact_mark_or_repeat_context():
    md = ''.join(f'## Page {i}\n### BLOCK #{i} [TEXT]: b{i}\ncommon phrase\n' for i in range(1, 8))
    md += '## Page 8\n### BLOCK #8 [TEXT]: exact\nUnit U4.3\n'
    result = search_md_context(md, ['common phrase', 'U4.3'], max_blocks=2, exclude_block_ids={'b1'})
    assert [h['block_id'] for h in result['hits']] == ['b2', 'exact']
    assert result['already_supplied'][0]['block_id'] == 'b1'
    assert result['backlog']


def test_graphic_summary_requires_indexed_image_even_if_summary_already_shown(tmp_path):
    path = tmp_path/'document.md'; path.write_text(MD)
    crop_dir = tmp_path/'crops'; crop_dir.mkdir()
    index = crop_dir/'index.json'; index.write_text(json.dumps({'blocks':[{'block_id':'g1','file':'g.png'}]}))
    (crop_dir/'g.png').write_bytes(b'graphic transport fixture')
    candidate = {'origin':'graphic','pointer':'/block_analyses/0/deferred_findings/0'}
    packet = {'candidate':candidate,'authoritative_md':MD,'allowed_source_refs':['b1'],
              'md_source':{'path':str(path),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()},
              'crop_source':{'directory':str(crop_dir),'index_sha256':hashlib.sha256(index.read_bytes()).hexdigest()}}
    request = CodexTargetedPass('candidate_recovery_test','x.json',[{'role':'system','content':'review'},
        {'role':'user','content':json.dumps(packet)}])
    payload = {'candidate_decisions':[{**candidate,'decision':'needs_data','context_queries':['U1.1']}]}
    followup, receipt = build_context_followup(request,payload)
    assert followup.image_paths == ((crop_dir/'g.png').resolve(),)
    assert receipt['images']['attached'][0]['block_id'] == 'g1'
    assert 'g1' in json.loads(followup.messages[-1]['content'])['allowed_source_refs']
    index.write_text('{}')
    followup, receipt = build_context_followup(request,payload)
    assert followup.image_paths == ()
    assert receipt['images']['unavailable'] == ['g1']
    assert 'g1' not in json.loads(followup.messages[-1]['content'])['allowed_source_refs']


def test_resume_backlog_order_without_injecting_unmatched_sources():
    from backend.app.pipeline.stages.prepare.recovery_context import search_md_context
    md = ('### BLOCK #1 [TEXT]: a\nalpha\n'
          '### BLOCK #2 [TEXT]: b\nbeta\n'
          '### BLOCK #3 [TEXT]: c\nalpha beta\n'
          '### BLOCK #4 [TEXT]: unrelated\nno match\n')
    first = search_md_context(md, ['alpha','beta'], max_blocks=1)
    prior = [h['block_id'] for h in first['backlog']]
    resumed = search_md_context(md, ['beta','alpha'], max_blocks=1,
        exclude_block_ids=['a'], prior_backlog_ids=['unrelated', *prior])
    assert resumed['hits'][0]['block_id'] == prior[0]
    assert all(h['block_id'] != 'unrelated' for h in resumed['hits'])
    assert resumed['absence_proven'] is False


def test_sheet_lookup_uses_bound_sheet_numbers_not_pdf_pages():
    import hashlib
    import pytest
    from backend.app.pipeline.stages.prepare.recovery_context import search_md_context
    md='## Page 9\n### BLOCK #1 [TEXT]: a\nMember schedule\n## Page 10\n### BLOCK #2 [TEXT]: b\nDimensions\n'
    graph={'source_md_sha256':hashlib.sha256(md.encode()).hexdigest(),
        'pages':[{'page':9,'sheet_no':'7'},{'page':10,'sheet_no':'8'}]}
    result=search_md_context(md,['Листы 7–8.'],source_graph=graph)
    assert [h['block_id'] for h in result['hits']]==['a','b']
    assert [h['page'] for h in result['hits']]==[9,10]
    assert not search_md_context(md,['Лист 9'],source_graph=graph)['hits']
    assert not search_md_context(md,['Листы 7–8.'])['hits']
    assert not result['absence_proven']
    with pytest.raises(ValueError,match='graph_version_changed'):
        search_md_context(md+'changed',['Лист 7'],source_graph=graph)


def test_node_token_precedes_address_substrings_even_in_resumed_backlog():
    md=('### BLOCK #1 [TEXT]: title\n> **Stamp:** по адресу Москва\nАдрес объекта\n'
        '### BLOCK #2 [TEXT]: other\n> **Stamp:** по адресу Москва\nОГМ50\n'
        '### BLOCK #3 [GRAPHIC]: node\nУзел по А; ОГМ5\n')
    result=search_md_context(md,['по А'],max_blocks=1,prior_backlog_ids=['title','other'])
    assert [h['block_id'] for h in result['hits']]==['node']
    assert {h['block_id'] for h in result['backlog']}=={'title','other'}
    assert result['query_hit_counts']['по А']==3
    assert result['hits'][0]['query_match_kinds']['по А']=='body_token'
    assert all(h['query_match_kinds']['по А']=='substring' for h in result['backlog'])
    mark=search_md_context(md,['ОГМ5'],max_blocks=1)
    assert mark['hits'][0]['block_id']=='node'
    assert mark['backlog'][0]['block_id']=='other'
    assert not result['absence_proven']


def test_metadata_and_morphological_matches_are_preserved_with_original_text():
    md=('### BLOCK #1 [TEXT]: stamp\n> **Stamp:** Code: КМ8\nTitle\n'
        '### BLOCK #2 [TEXT]: stem\nХарактеристики бетона\n'
        '### BLOCK #3 [TEXT]: body\nКМ8: бетон\n')
    result=search_md_context(md,['КМ8','бетон'],max_blocks=1)
    assert result['hits'][0]['block_id']=='body'
    assert {r['block_id'] for r in result['backlog']}=={'stamp','stem'}
    all_rows=search_md_context(md,['КМ8','бетон'],max_blocks=10)['hits']
    for row in all_rows:assert row['content'] in md
    assert all_rows[1]['query_match_kinds']=={'КМ8':'metadata_token'}


def test_compact_node_reference_finds_target_not_only_referring_views():
    md = ('## Page 11\n### BLOCK #1 [GRAPHIC]: view\n'
          '> **Stamp:** Code: КМ | Stage: Р | Sheet: 9 | Revisions: 2\nОГМ11 по Б(4)\n'
          '## Page 6\n### BLOCK #2 [GRAPHIC]: node\n'
          '> **Stamp:** Code: КМ | Stage: П | Sheet: 4 | Revisions: 1\nУзлы Б, В, Г, Д\n'
          '## Page 4\n### BLOCK #3 [GRAPHIC]: wrong_sheet\n'
          '> **Stamp:** Code: КМ | Stage: Р | Sheet: 2\nУзел Б\n')
    report = search_md_context(md, ['ОГМ11', 'Б(4)', 'Д(4)'], max_blocks=1)
    hit, = report['hits']
    assert hit['block_id'] == 'node'
    assert hit['content'] in md
    assert hit['source_provenance'] == {'code':'КМ', 'stage':'П', 'sheet':'4', 'revisions':'1'}
    assert hit['query_match_kinds'] == {'Б(4)':'node_sheet_navigation', 'Д(4)':'node_sheet_navigation'}
    assert hit['reference_applicability_verified'] is False
    assert [r['block_id'] for r in report['backlog']] == ['view']
    assert not report['absence_proven']


def test_node_reference_does_not_infer_node_from_metadata_or_other_sheet():
    md = ('## Page 4\n### BLOCK #1 [GRAPHIC]: wrong\n'
          '> **Stamp:** Code: Б | Stage: Р | Sheet: 4\nУзел В\n')
    assert not search_md_context(md, ['Б(4)'])['hits']
    assert not search_md_context(md, ['В(9)'])['hits']


def test_dimension_typography_finds_source_without_equating_execution():
    from backend.app.pipeline.stages.prepare.recovery_context import search_md_context
    md = ('### BLOCK #1 [TEXT]: cable\n'
          '| Кабель | ППГнг(A)-HF |\n| Сечение | 5x35 |\n'
          '### BLOCK #2 [TEXT]: unrelated\nX1 is a terminal; 5x350 is another section\n')
    for query in ('5×35', '5х35', '5 х 35'):
        report = search_md_context(md, [query])
        assert report['hits'][0]['block_id'] == 'cable'
        assert report['hits'][0]['query_match_kinds'][query] == 'dimension_format_navigation'
        assert '5x35' in report['hits'][0]['content']
        assert '5×35' not in report['hits'][0]['content']
        assert report['hits'][0]['reference_applicability_verified'] is False
    assert not search_md_context(md, ['ППГнг(A)-FRHF'])['hits']
    assert not search_md_context(md, ['Х1'])['hits']


def test_explicit_document_sheet_precedes_generic_marks_without_dropping_hits():
    md = '''## Page 1
### BLOCK #1 [IMAGE]: other_sheet
> **Stamp:** Code: DOC-КМ1 | Stage: П | Sheet: 3
ОГМ 4
## Page 2
### BLOCK #2 [IMAGE]: other_document
> **Stamp:** Code: OTHER-КМ1 | Stage: П | Sheet: 6
ОГМ 4
## Page 3
### BLOCK #3 [IMAGE]: detail
> **Stamp:** Code: DOC-КМ1 | Stage: П | Sheet: 6
ОГМ 4
'''
    result = search_md_context(md, ['DOC-КМ1 лист 6', 'ОГМ 4'], max_blocks=1)
    assert [h['block_id'] for h in result['hits']] == ['detail']
    assert {h['block_id'] for h in result['backlog']} == {'other_sheet', 'other_document'}
    assert result['query_hit_counts']['DOC-КМ1 лист 6'] == 1
    assert not result['hits'][0]['reference_applicability_verified']
    assert not result['absence_proven']


def test_document_sheet_navigation_preserves_stage_ambiguity_and_literal_codes():
    md = '''## Page 1
### BLOCK #1 [IMAGE]: stage_p
> **Stamp:** Code: DOC-КМ1 | Stage: П | Sheet: 6
Detail
### BLOCK #2 [IMAGE]: stage_r
> **Stamp:** Code: DOC-КМ1 | Stage: Р | Sheet: 6
Detail
### BLOCK #3 [IMAGE]: different_execution
> **Stamp:** Code: DOC-КМ1.2 | Stage: П | Sheet: 6
Detail
'''
    result = search_md_context(md, ['DOC-КМ1 лист 6'])
    assert {h['block_id'] for h in result['hits']} == {'stage_p', 'stage_r'}
    assert {h['source_provenance']['stage'] for h in result['hits']} == {'П', 'Р'}
    assert all(not h['reference_applicability_verified'] for h in result['hits'])
    # Latin KM and Cyrillic КМ remain different; no transcription repair.
    assert search_md_context(md, ['DOC-KM1 лист 6'])['hits'] == []
    assert search_md_context(md, ['DOC-КМ1 лист 60'])['hits'] == []
