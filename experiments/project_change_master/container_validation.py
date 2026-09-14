"""Fresh source-selected document test of the frozen equipment-form consumer."""
import argparse
import hashlib
from pathlib import Path
import resource
import time

import fitz

from .run import ROOT,REPO,read,write,sha,immutable,now
from .recovery_v2 import recover_pairs
from .containers import extract,compare

BASE=ROOT/'03_VALIDATIONS/equipment_forms_v1'


def prepare():
    frozen=ROOT/'02_FROZEN_CANDIDATES/equipment_forms_v1/MANIFEST.json'
    files=[Path(__file__),REPO/'experiments/project_change_master/containers.py',REPO/'experiments/project_change_master/test_containers.py']
    if not frozen.exists():
        code={**read(ROOT/'02_FROZEN_CANDIDATES/end_to_end_v2/MANIFEST.json')['code'],**{str(p):sha(p) for p in files}}
        immutable(frozen,dict(frozen_at=now(),code=code,config={'route':'TABLE','absence_proof':False,'unverified_model_changes':'REVIEW'},prompts=[]))
        for p in files:
            dest=frozen.parent/'code'/p.name;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(p.read_bytes())
    for p,h in read(frozen)['code'].items():
        if sha(p)!=h:raise ValueError('Frozen code drift')
    source=read(ROOT/'03_VALIDATIONS/recovered_documents/MANIFEST.json')
    spent={p['document'] for m in [source,read(ROOT/'03_VALIDATIONS/recovered_v2/MANIFEST.json')] for p in m['selected']}
    target=BASE/'MANIFEST.json';seed='master-equipment-forms-fresh-v1'
    if not target.exists():
        eligible=[p for p in source['eligible'] if p['document'] not in spent and p['discipline']=='OV']
        selected=[]
        for p in sorted(eligible,key=lambda p:hashlib.sha256((seed+p['document']).encode()).hexdigest())[:2]:
            p=dict(p);p['sources']={side:{name:dict(path=str(Path(p[side])/'02_work'/name),sha256=sha(Path(p[side])/'02_work'/name)) for name in ['document.pdf','document.md','result.json']} for side in ['old','new']};selected.append(p)
        immutable(target,dict(locked_at=now(),seed=seed,selected=selected,excluded_spent_documents=sorted(spent),
                             candidate_sha256=sha(frozen),independent_projects=0,predictions_seen=False))
    recover_pairs(read(target),BASE)
    packets=[]
    for pair in read(BASE/'PAIRS.json'):
        pools={side:extract(pair[side]) for side in ['old','new']}
        immutable(BASE/'pools'/(pair['pair_key'].split('/')[-1]+'.json'),pools)
        for new in sorted(pools['new']['records'],key=lambda r:hashlib.sha256((seed+r['record_id']).encode()).hexdigest())[:4]:
            cid='blind_'+hashlib.sha256(new['record_id'].encode()).hexdigest()[:16]
            old=[r for r in pools['old']['records'] if r['subject']['stable_id']==new['subject']['stable_id']]
            packet=dict(case_id=cid,source_document=pair['new']['document_code'],NEW=[],OLD=[])
            for side,records in [('new',[new]),('old',old)]:
                pdf=fitz.open(pair[side]['artifacts']['pdf']['path'])
                for i,r in enumerate(records):
                    path=BASE/'blind'/f'{cid}_{side}_{i}.png';path.parent.mkdir(parents=True,exist_ok=True)
                    pdf[r['page']-1].get_pixmap(matrix=fitz.Matrix(1.4,1.4)).save(path)
                    packet[side.upper()].append(dict(image=path.name,page=r['page'],pdf=pair[side]['artifacts']['pdf'],
                        version=pair[side]['document_version']))
            packets.append(packet)
    immutable(BASE/'blind/CASES.json',packets)
    (BASE/'blind/README.md').write_text('''# Слепая проверка источников

Откройте пары PNG из CASES.json. Есть ли один и тот же инженерный объект?
Есть ли реальное изменение, и какое? Ответ: ДА / НЕТ / НЕЯСНО / ПОВРЕЖДЁН.
Запишите, что было и стало, с номерами страниц. Если OLD отсутствует в пакете,
отметьте НЕЯСНО; это не доказательство добавления. Модельный ответ, уверенность
и тип предсказанного события в пакет не включены. Ответы сохраняйте отдельно.

Документы отобраны до предсказания, но принадлежат ранее использованному проекту.
Результаты этой проверки не доказывают переносимость на независимые проекты.
''')
    print('Source-only blind packets',len(packets),'documents',[p['new']['document_code'] for p in read(BASE/'PAIRS.json')])


def predict():
    for p,h in read(ROOT/'02_FROZEN_CANDIDATES/equipment_forms_v1/MANIFEST.json')['code'].items():
        if sha(p)!=h:raise ValueError('Frozen code drift')
    start=time.perf_counter();out=[]
    for p in read(BASE/'PAIRS.json'):
        out.append(dict(document=p['new']['document_code'],**compare(p['pair_key'],p['old'],p['new'])))
    immutable(BASE/'RESULTS.json',out)
    cs=[c for r in out for c in r['result']['project_changes']]
    score=dict(documents=len(out),candidate_events=len(cs),accepted=sum(c['status']=='PROVEN' for c in cs),review=sum(c['status']=='REVIEW' for c in cs),
               old_forms=sum(len(r['old']['records']) for r in out),new_forms=sum(len(r['new']['records']) for r in out),
               correct=None,false=None,precision=None,independent_projects=0,adjudication='PENDING_INDEPENDENT_SOURCE_REVIEW')
    write(BASE/'SCORECARD.json',score)
    write(BASE/'PERFORMANCE.json',dict(seconds=time.perf_counter()-start,peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,ai_calls=0,input_tokens=0,output_tokens=0,cost_usd=0))
    print(score)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('action',choices=['prepare','predict']);globals()[p.parse_args().action]()
