"""Offline accounting of provisional reviews; never promotes model judgments."""
import collections
import hashlib
import html
import json
from pathlib import Path

HERE=Path(__file__).resolve().parent
WORK=Path('/home/coder/auditmanager/corpus-audits/20261109_stage1_source_review')
OUT=HERE/'adjudication'

def read(p): return json.loads(p.read_text())
def digest(p): return hashlib.sha256(p.read_bytes()).hexdigest()

def main():
    OUT.mkdir(exist_ok=True)
    jobs=read(WORK/'review/jobs.json');rows=[];integrity=[];problems=[]
    for j in jobs:
        base=WORK/'review/jobs'/j['id'];p=base/'status.json';s=read(p)
        integrity.append({'path':str(p),'sha256':digest(p)})
        rejected={}
        # Replay only acceptance/rejection bookkeeping, not any model call.
        for response in sorted(base.glob('*_response.json')):
            data=read(response).get('json') or {}
            ep=base/(response.name.replace('_response','_evidence'))
            feedback=read(ep)
            errors=[e for e in feedback if 'assessment' in e and 'error' in e]
            bad={e['assessment'].get('candidate_id'):e['error'] for e in errors}
            for a in data.get('assessments',[]):
                cid=a.get('candidate_id')
                if cid in bad: rejected[cid]={'error':bad[cid],'response':str(response),'feedback':str(ep)}
                else: rejected.pop(cid,None)
            integrity.extend({'path':str(x),'sha256':digest(x)} for x in [response,ep])
        by={a['candidate_id']:a for a in s['assessments']}
        assert set(by)=={c['id'] for c in j['candidates']}
        for c in j['candidates']:
            a=by[c['id']];atoms=a['atoms'];flags=[]
            if c['id'] in rejected: flags.append('rejected_revision_stale_saved_assessment')
            if any(x['verdict']=='needs_data' for x in atoms):flags.append('unresolved_evidence')
            if any(x.get('recommended_output')=='finding' and x.get('practical_impact')=='documentation' for x in atoms): flags.append('check_question_profile_and_utility')
            if any(x['verdict']=='refuted' for x in atoms):flags.append('verify_refutation_scope')
            historical=j.get('tracking_control_id')
            followups=[]
            if historical:
                for fp in sorted((HERE/'followup').glob('*primary*.json')):
                    if read(fp).get('control_id')==historical:followups.append(str(fp))
            row={'candidate_id':c['id'],'job':j['id'],'discipline':j['discipline'],'phase':j['phase'],
                 'original_candidate':c,'saved_provisional_assessment':a,'flags':flags,
                 'unresolved_validation':rejected.get(c['id']),'existing_primary_followups':followups,
                 'adjudication_status':'pending','accepted_defect':None,'accepted_question':None,
                 'note':'Followup evidence is separate; no automatic verdict merge.'}
            rows.append(row)
            if c['id'] in rejected:problems.append({'candidate_id':c['id'],'job':j['id'],**rejected[c['id']]})
    assert len(rows)==291 and len({r['candidate_id'] for r in rows})==291
    counts=collections.Counter(x['verdict'] for r in rows for x in r['saved_provisional_assessment']['atoms'])
    summary={'jobs':len(jobs),'candidates':len(rows),'historical_candidates':47,'classic_candidates':244,
             'model_atom_counts_not_defects':dict(counts),'jobs_with_rejected_revision':len({r['job'] for r in problems}),
             'candidates_with_rejected_revision':len(problems),'candidate_flag_counts':dict(collections.Counter(f for r in rows for f in r['flags'])),
             'stage_complete':False,'production_promotion_allowed':False,'all_model_verdicts_provisional':True}
    for name,data in [('queue.json',rows),('summary.json',summary),('validation_failures.json',problems),('input_hashes.json',integrity)]:
        (OUT/name).write_text(json.dumps(data,ensure_ascii=False,indent=2)+'\n')
    esc=lambda x:html.escape(str(x))
    body=['<!doctype html><html lang="ru"><meta charset="utf-8"><title>Разбор прогона Astra</title><style>body{font:16px sans-serif;max-width:1200px;margin:32px auto}details{padding:12px;border-bottom:1px solid #ccc}pre{white-space:pre-wrap}strong{color:#a33}</style><h1>Разбор прогона Astra</h1><p>291 исходная запись, 112 заданий. Оценки модели предварительные. Число тезисов не равно числу дефектов. Этап 1 не закрыт.</p>',f'<p><strong>Отклонённая новая оценка при сохранённой старой: {len(problems)} записей в {summary["jobs_with_rejected_revision"]} заданиях.</strong> Требуется отдельная проверка, исходные результаты сохранены.</p>']
    for r in sorted(rows,key=lambda r:(not bool(r['unresolved_validation']),r['candidate_id'])):
        body.append(f'<details><summary>{esc(r["candidate_id"])} — {esc(", ".join(r["flags"]) or "сверка смысла и пользы")}</summary><pre>{esc(json.dumps(r,ensure_ascii=False,indent=2))}</pre></details>')
    body.append('</html>');(OUT/'Разбор_прогона.html').write_text('\n'.join(body))
    print(json.dumps(summary,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
