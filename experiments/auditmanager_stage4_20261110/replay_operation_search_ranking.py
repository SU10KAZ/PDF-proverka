"""Compare source-search ordering on sealed OGM5 queries; no provider calls."""
import hashlib
import json
import re
from pathlib import Path
import subprocess

from backend.app.pipeline.stages.prepare.recovery_context import search_md_context
from .prepare_ogm5_continuation import MD, DEST


def main():
    old_code=subprocess.check_output(['git','show','63fcd145:backend/app/pipeline/stages/prepare/recovery_context.py'],text=True)
    namespace={'__name__':'frozen_recovery_context','__package__':'backend.app.pipeline.stages.prepare'}
    exec(compile(old_code,'63fcd145/recovery_context.py','exec'),namespace)
    old_search=namespace['search_md_context']
    md=MD.read_text()
    graph=json.loads(Path('/tmp/auditmanager_six_candidate_inputs_20261110/KM/03_analysis/runs/candidate/document_graph.json').read_text())
    request=json.loads((DEST/'request.json').read_text());packet=json.loads(request['messages'][-1]['content'])
    prepared=json.loads(Path('/tmp/auditmanager_km_ogm5_second_continuation_20261110/request.json').read_text())
    queries_by_operation=json.loads(prepared['messages'][-1]['content'])['context_searches']
    rows=[]
    for operation,search in queries_by_operation.items():
        queries=list(search['query_hit_counts'])
        kwargs={'exclude_block_ids':packet['allowed_source_refs'],'source_graph':graph,
            'prior_backlog_ids':[b['block_id'] for b in packet['omitted']]}
        old=old_search(md,queries,**kwargs);new=search_md_context(md,queries,**kwargs)
        ids=lambda r:{b['block_id'] for b in r['hits']+r['backlog']+r['already_supplied']}
        assert ids(old)==ids(new)
        assert old['query_hit_counts']==new['query_hit_counts']
        assert all(b['content'] in md for b in new['hits'])
        rows.append({'operation':operation,'queries':queries,'query_hit_counts':new['query_hit_counts'],
            'candidate_source_set_unchanged':True,'candidate_source_count':len(ids(new)),
            'before_selected':[b['block_id'] for b in old['hits']],
            'after_selected':[{'block_id':b['block_id'],'kind':b['kind'],'query_match_kinds':b['query_match_kinds']} for b in new['hits']],
            'backlog_count':len(new['backlog']),
            'selected_substring_only_before':sum(all(not re.search(r'(?<!\w)'+re.escape(q.casefold())+r'(?!\w)',b['content'].casefold()) for q in b['queries']) for b in old['hits']),
            'selected_substring_only_after':sum(all(k=='substring' for k in b['query_match_kinds'].values()) for b in new['hits'])})
    report={'source_md_sha256':hashlib.sha256(MD.read_bytes()).hexdigest(),
        'baseline_commit':'63fcd145','rows':rows,'new_model_calls':0,
        'old_response_reused_for_changed_prompt':False,'semantic_quality_improvement_proven':False,
        'limits':['Ordering comparison on known OGM5 context requests, not a blind audit.',
            'All literal candidates remain available; ranking does not establish relevance or object identity.',
            'GRAPHIC hit requires image attachment and reading; its summary alone is not evidence.',
            'Changed prepared requests require a new response, never a cached answer to the old prompt.']}
    Path(__file__).with_name('KM_operation_search_ranking_replay.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps([{k:r[k] for k in ('operation','candidate_source_count','selected_substring_only_before','selected_substring_only_after')} for r in rows],ensure_ascii=False))


if __name__=='__main__':main()
