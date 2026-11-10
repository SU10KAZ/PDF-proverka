"""Drain bounded source-search pages locally, without simulated model responses."""
import json
from pathlib import Path
from backend.app.pipeline.stages.findings_verify.source_review_context import expand_source_review_context, MAX_CONTEXT_REVISIONS
from experiments.auditmanager_stage4_20261110.run_itp_graphic_review_pilot import MD

HERE=Path(__file__).parent

def main():
    initial=Path('/tmp/auditmanager_itp_initial_context_astra_20261110')
    queue=json.loads((initial/'followup.json').read_text())['expanded_queue']
    target='blk_7e913f6c32cb4db7aa9d9b8688b6e979'
    pages=[]
    while queue['packets'][0]['context_revision'] < MAX_CONTEXT_REVISIONS:
        queue=expand_source_review_context(queue,'F-042',MD.read_text(),[],continue_backlog=True)
        p=queue['packets'][0];r=p['context_search_receipt']
        pages.append({'revision':p['context_revision'],'added_block_ids':r['added_block_ids'],
            'remaining_hits':len(r['backlog']),'deferred_queries':r['deferred_queries'],
            'text_chars':sum(len(s['authoritative_text']) for s in p['sources']),
            'target_retrieved':target in {s['block_id'] for s in p['sources']}})
    p=queue['packets'][0]
    result={'known_case_local_search_replay':True,'model_calls':0,'new_model_responses_simulated':False,
        'queries_origin':'Recorded original Astra queries; no target ID or control answer given to search',
        'pages':pages,'target_retrieved':target in {s['block_id'] for s in p['sources']},
        'absence_proven':False,'whole_project_search_complete':False,
        'limitation':'Bounded literal search still has backlog. Expanded sources not yet evaluated by a model; image cap and semantic sufficiency remain separate.'}
    (HERE/'ITP_context_backlog_replay.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    out=Path('/tmp/auditmanager_itp_context_backlog_20261110');out.mkdir(exist_ok=True)
    (out/'expanded_queue.json').write_text(json.dumps(queue,ensure_ascii=False,indent=2))
    print(json.dumps(result,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
