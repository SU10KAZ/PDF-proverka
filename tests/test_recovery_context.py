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
