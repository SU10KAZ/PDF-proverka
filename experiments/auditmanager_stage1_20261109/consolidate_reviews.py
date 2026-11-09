"""Versioned consolidation: validated repair supersedes model output, not human evidence."""
import collections, hashlib, html, json
from pathlib import Path
HERE=Path(__file__).resolve().parent
BASE=HERE/'adjudication'
REPAIR=Path('/home/coder/auditmanager/corpus-audits/20261109_stage1_revision_repair')
OUT=HERE/'consolidated'
def read(p):return json.loads(p.read_text())
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
    rows=read(BASE/'queue.json'); by={r['candidate_id']:r for r in rows}; changes=[];inputs=[]
    for job in read(REPAIR/'review/jobs.json'):
        p=REPAIR/'review/jobs'/job['id']/'status.json';s=read(p)
        assert s['status']=='reviewed_provisional' and not s.get('pending')
        assert {a['candidate_id'] for a in s['assessments']}=={c['id'] for c in job['candidates']}
        e=p.parent/f"{s['steps']-1:02d}_evidence.json"
        assert not [x for x in read(e) if 'assessment' in x and 'error' in x]
        inputs.extend({'path':str(x),'sha256':sha(x)} for x in (p,e))
        for a in s['assessments']:
            cid=a['candidate_id'];r=by[cid];original=next(c for c in job['candidates'] if c['id']==cid)
            assert original==r['original_candidate']
            old=r['saved_provisional_assessment']
            r['assessment_history']=[{'origin':'initial','assessment':old,'validation_problem':r['unresolved_validation']}, {'origin':'repair','assessment':a,'source':str(p)}]
            r['effective_provisional_assessment']=a;r['unresolved_validation']=None
            changes.append({'candidate_id':cid,'before':old,'after':a,'semantic_comparison':'pending; atom wording/count changes are not accuracy metrics'})
    assert len(changes)==25
    for r in rows:
        r.setdefault('effective_provisional_assessment',r['saved_provisional_assessment'])
        atoms=r['effective_provisional_assessment']['atoms'];flags=[]
        if any(a['verdict']=='needs_data' for a in atoms):flags.append('unresolved_evidence')
        if any(a.get('recommended_output')=='finding' and a.get('practical_impact')=='documentation' for a in atoms):flags.append('check_question_profile_and_utility')
        if any(a['verdict']=='refuted' for a in atoms):flags.append('verify_refutation_scope')
        r['current_flags']=flags
        r['accepted_defect']=None;r['accepted_question']=None
        r['note']='Model repair is not engineering acceptance. Primary checks have separate authority.'
    assert len(rows)==291 and len(by)==291
    summary={'records':291,'historical':47,'classic':244,'repaired_records':25,'unresolved_validation_revisions':sum(bool(r['unresolved_validation']) for r in rows),
             'model_atom_counts_not_defects':dict(collections.Counter(a['verdict'] for r in rows for a in r['effective_provisional_assessment']['atoms'])),
             'records_with_unresolved_evidence':sum('unresolved_evidence' in r['current_flags'] for r in rows),'stage_complete':False,'production_promotion_allowed':False}
    OUT.mkdir(exist_ok=True)
    for name,value in [('records.json',rows),('repair_comparison.json',changes),('summary.json',summary),('repair_input_hashes.json',inputs)]:
        (OUT/name).write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')
    esc=lambda x:html.escape(str(x))
    body=['<!doctype html><html lang="ru"><meta charset="utf-8"><title>Свод двух прогонов</title><style>body{font:16px sans-serif;max-width:1150px;margin:30px auto}pre{white-space:pre-wrap}details{padding:10px;border-bottom:1px solid #ddd}</style><h1>Свод основного прогона и повтора</h1><p>291 запись сохранена; 25 предварительных оценок обновлены после повтора. Это не эталон и не доказательство улучшения качества.</p><pre>'+esc(json.dumps(summary,ensure_ascii=False,indent=2))+'</pre>']
    for r in rows:body.append('<details><summary>'+esc(r['candidate_id'])+' — '+esc(', '.join(r['current_flags']) or 'проверить смысл и полезность')+'</summary><pre>'+esc(json.dumps(r,ensure_ascii=False,indent=2))+'</pre></details>')
    (OUT/'Свод_двух_прогонов.html').write_text('\n'.join(body)+'</html>')
    print(json.dumps(summary,ensure_ascii=False,indent=2))
if __name__=='__main__':main()
