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
