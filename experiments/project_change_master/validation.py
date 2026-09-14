"""Frozen source-selected validation; results never modify a candidate."""
import argparse
from collections import Counter
import hashlib
import resource
import time
from pathlib import Path

from experiments.evidence_scope_binding_v1.source import table_nodes
from experiments.evidence_scope_binding_v1.core import bind
from experiments.evidence_scope_binding_v1.run import enrich
from experiments.engineering_subject_resolver_v1 import source as subjects
from experiments.engineering_subject_resolver_v1.core import rank, deterministic
from experiments.project_change_text_v1.narrative import build as narrative
from experiments.project_change_text_v1.engine import compare as text_compare
from experiments.table_materialization_v3.run import materialize_document
from .groups import compare_groups
from .state import apply
from .run import ROOT,REPO,read,write,sha,immutable,now

BASE=ROOT/'03_VALIDATIONS/recovered_documents'
QUERIES_PER_DOCUMENT=12


def freeze():
    p=ROOT/'02_FROZEN_CANDIDATES/end_to_end_v1/MANIFEST.json'
    if p.exists():verify();print('Frozen candidate verified');return
    packages=['project_change_master','evidence_scope_binding_v1','engineering_subject_resolver_v1',
              'project_change_text_v1','text_comparison_v1','text_alignment_v2','text_safe_coverage',
              'semantic_foundation_v3','table_materialization_v3','table_project_change_v1','text_old_scope_recovery_v1']
    files=[p for name in packages for p in (REPO/'experiments'/name).glob('*.py')]
    files.append(REPO/'experiments/table_materialization_v3/policy.json')
    code={str(f):sha(f) for f in files}
    immutable(p,dict(frozen_at=now(),code=code,config=dict(queries_per_document=QUERIES_PER_DOCUMENT,
        subject_retrieval_k=6,scope='certified_hierarchy',state='typed_event',groups='closed_interval',
        TEXT='frozen_project_change_text_v1',GRAPHIC='NOT_ACTIVATED',AI_calls=0),prompts=[],
        selection_policy='Metadata, source filenames and hashes only; one document per discipline; seed in source manifest'))
    for f in files:
        out=p.parent/'code'/f.relative_to(REPO);out.parent.mkdir(parents=True,exist_ok=True);out.write_bytes(f.read_bytes())
    print('Frozen end-to-end candidate')


def verify():
    for p,h in read(ROOT/'02_FROZEN_CANDIDATES/end_to_end_v1/MANIFEST.json')['code'].items():
        if sha(p)!=h:raise ValueError('Candidate drift: '+p)


def prepare():
    verify();started=time.perf_counter();queries=[];inventory=[];bindings={}
    subjects.V3=BASE/'v3'  # Explicit offline input adapter configuration only.
    for pair in read(BASE/'PAIRS.json'):
        pools={};texts={}
        for side in ['old','new']:
            doc=pair[side];version=doc['document_version'];directory=BASE/'v3'/version
            if not directory.exists():
                result=materialize_document(doc)
                for name in ['ledger','tables','semantics','decisions']:immutable(directory/(name+'.json'),result[name])
            pool,coverage=subjects.table_pool(doc,pair['pair_key'],side)
            pools[side]=pool
            immutable(BASE/'pools'/(version+'.json'),pool)
            texts[side]=narrative(doc)
            immutable(BASE/'narrative'/(version+'.json'),texts[side])
            inventory.append(dict(project=pair['project'],document=doc['document_code'],side=side,
                TABLE_subjects=len(pool),TABLE_coverage=coverage,TEXT_units=len(texts[side]['units']),TEXT_quality=texts[side]['quality']))
        selected=sorted(pools['new'],key=lambda s:hashlib.sha256(('master-query-v1'+s['subject_id']).encode()).hexdigest())[:QUERIES_PER_DOCUMENT]
        # Source case IDs are locked before any identity/state inference.
        immutable(BASE/'selections'/(pair['pair_key'].split('/')[-1]+'.json'),
                  dict(new_subject_ids=[s['subject_id'] for s in selected],pool_size=len(pools['new']),predictions_seen=False))
        for q in selected:
            unscoped=rank(q,pools['old']);old=[]
            for s in [q]+[r['subject'] for r in unscoped]:
                if s['subject_id'] not in bindings:
                    f,n=table_nodes(s);b=bind(f,n);bindings[s['subject_id']]={'binding':b,'subject':enrich(s,b)}
                if s['subject_id']!=q['subject_id']:old.append(bindings[s['subject_id']]['subject'])
            sq=bindings[q['subject_id']]['subject'];cid='v_'+hashlib.sha256(q['subject_id'].encode()).hexdigest()[:20]
            packet=subjects.packet(cid,sq,rank(sq,old))
            immutable(BASE/'packets'/(cid+'.json'),packet)
            queries.append(dict(case_id=cid,project=pair['project'],document=pair['new']['document_code'],
                                packet_path=str(BASE/'packets'/(cid+'.json')),packet_sha256=sha(BASE/'packets'/(cid+'.json'))))
        print('Prepared',pair['new']['document_code'],'TABLE',len(pools['old']),len(pools['new']),'selected',len(selected),flush=True)
    immutable(BASE/'CASE_MANIFEST.json',queries)
    immutable(BASE/'SCOPE_BINDINGS.json',bindings)
    immutable(BASE/'SOURCE_INVENTORY.json',inventory)
    write(BASE/'PREPARE_PERFORMANCE.json',dict(seconds=time.perf_counter()-started,peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,ai_calls=0))
    print('Locked',len(queries),'TABLE cases')


def blind():
    """Source-only artifacts: no confidence, selected relation, predicted event."""
    verify();cases=[]
    def view(s):
        return dict(document=s['evidence'][0]['document_code'],version=s['document_version'],
                    source=s['text'],column_headers=s['headers'],
                    page=s['page'],evidence=s['evidence'],
                    source_context=[{'text':c['text'],'source_evidence':c.get('scope_evidence',[])} for c in s['context']])
    for case in read(BASE/'CASE_MANIFEST.json'):
        p=read(case['packet_path'])
        cases.append(dict(case_id=case['case_id'],NEW=view(p['new']),
            OLD=[dict(label='OLD_'+str(i+1),**view(r['subject'])) for i,r in enumerate(p['old_candidates'])]))
    immutable(BASE/'blind/CASES.json',cases)
    (BASE/'blind/README.md').write_text('''# Независимая проверка локальных источников

В CASES.json нет ответов сравнивающей системы. Сначала определите, относятся ли
OLD и NEW к одному инженерному объекту: ДА / НЕТ / НЕЯСНО / ИСТОЧНИК ПОВРЕЖДЁН.
Затем укажите, есть ли реальное изменение и какое; перечислите OLD/NEW свидетелей.
Для группы проверьте все строки между её явными границами. Отсутствие найденного
соответствия не означает удаление. Сверяйте модели, единицы и числа с PDF.

Это новые документы внутри ранее использованного проекта, а не новые проекты.
Не открывайте RESULTS.json до завершения ответов. Ответы сохраняйте отдельно.
''')
    print('Blind source packets prepared',len(cases))


def predict():
    verify();started=time.perf_counter();rows=[];changes=[];textrows=[]
    for case in read(BASE/'CASE_MANIFEST.json'):
        if sha(case['packet_path'])!=case['packet_sha256']:raise ValueError('Packet drift')
        p=read(case['packet_path']);relation=deterministic(p)
        state=apply(p,relation);groups=compare_groups(p,relation)
        cs=state['project_changes']+groups['project_changes'];changes+=cs
        rows.append(dict(**case,relation=relation,state=state,groups=groups))
    for pair in read(BASE/'PAIRS.json'):
        a=read(BASE/'narrative'/(pair['old']['document_version']+'.json'))
        b=read(BASE/'narrative'/(pair['new']['document_version']+'.json'))
        result=text_compare(pair['pair_key'],a['units'],b['units'])
        textrows.append(dict(project=pair['project'],document=pair['new']['document_code'],result=result))
        changes+=result['changes']
    immutable(BASE/'RESULTS.json',dict(TABLE=rows,TEXT=textrows,project_changes=changes))
    score={}
    for route in ['TEXT','TABLE']:
        cs=[c for c in changes if route in c['routes']]
        score[route]=dict(candidates=len(cs),accepted=sum(c['status']=='PROVEN' for c in cs),
                         review=sum(c['status']=='REVIEW' for c in cs),correct=None,false=None,
                         precision=None,adjudication='UNADJUDICATED',
                         query_count=len(rows) if route=='TABLE' else None)
    score.update(independent_projects=0,previously_unseen_document_pairs=len(read(BASE/'PAIRS.json')),
                 generalization='INSUFFICIENT_EVIDENCE',GRAPHIC='NOT_YET_VALIDATED')
    write(BASE/'SCORECARD.json',score)
    write(BASE/'PREDICT_PERFORMANCE.json',dict(seconds=time.perf_counter()-started,peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        artifact_bytes=sum(p.stat().st_size for p in BASE.rglob('*') if p.is_file()),ai_calls=0,input_tokens=0,output_tokens=0,cost_usd=0))
    print(score)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['freeze','prepare','blind','predict','verify'])
    globals()[p.parse_args().action]()
