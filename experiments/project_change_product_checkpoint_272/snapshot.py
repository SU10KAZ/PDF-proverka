"""Read-only presentation of frozen outputs. No truth/audit inputs or inference."""
import hashlib
import json
import shutil
from collections import Counter
from pathlib import Path

ROOT = Path('/home/coder/auditmanager/corpus-audits/20260914_project_change_272')
OUT = ROOT / 'projectchange_product_checkpoint_pair_a_b'
BASE = ROOT / 'fresh_dev_sample_f5_pipeline_v6'
LIVE_A = ROOT / 'fresh_dev_pair_a_live_v2'
V4 = ROOT / 'fresh_dev_pair_a_post_inference_repair_v4'
REPO = Path(__file__).resolve().parents[2]
WEB = OUT / 'release/web'


def read(p): return json.loads(Path(p).read_text())
def sha(p): return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def save(p, data):
    p = Path(p); p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2) + '\n')


def asset(path, digest, suffix):
    path = Path(path)
    target = WEB / 'assets' / (digest + suffix)
    if not target.exists():
        assert sha(path) == digest, path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)
    assert sha(target) == digest
    return 'assets/' + target.name


def build(label, rows, groups, plan, live):
    """Groups are authoritative; review/negative entries are never regrouped."""
    bykey = {r['package_id']: r for r in rows}
    package_paths = {r['key']: r['package'] for r in plan}
    diagnostics = {}; presented = {}; receipts = []
    for key, row in bykey.items():
        p = read(BASE / package_paths[key]); raw = row['raw']; n = row['normalized']
        request_path = live / 'calls' / key / 'REQUEST.json'
        images = read(request_path)['images'] if request_path.exists() else []
        delivered = {e['evidence_id']: e for e in images}
        roles = {q['evidence_id']: q for q in n.get('claim_applicability', {}).get('evidence_role_requirements', [])}
        sides = {}
        for side in ('old', 'new'):
            bound = {b['evidence_id'] for b in n.get(side+'_state', {}).get('evidence_bindings', [])}
            cited = set(raw.get(side+'_state', {}).get('evidence_ids', []))
            evidence = []
            for e in p['evidence_packet']['evidence'][side]:
                assert e['side'] == side
                pdf = e['source_receipt']; picture = None
                if e['evidence_id'] in delivered:
                    img = delivered[e['evidence_id']]
                    assert img['side'] == side and img['page'] == e['page']
                    assert img['sha256'] == e['raster']['sha256']
                    attempt=live/'calls'/key/'attempt_001'
                    image_name=f'image_{images.index(img):02d}.png'
                    invocation=read(attempt/'INVOCATION.json')
                    assert invocation['input_hashes'][image_name]==img['sha256']
                    assert read(live/'calls'/key/'SUCCESS.json')['status']=='SUCCESS'
                    assert sha(attempt/image_name)==img['sha256']
                    picture = asset(attempt/image_name, img['sha256'], '.png')
                ev = dict(page=e['page'], source_type=e['route'], source_kind=e['source_kind'],
                          quote=e.get('quote', ''), image=picture,
                          pdf=asset(pdf['path'], pdf['sha256'], '.pdf') + '#page=' + str(e['page']),
                          use='Подтверждённая ссылка' if e['evidence_id'] in bound else
                              'Ссылка требует проверки' if e['evidence_id'] in cited else 'Контекст',
                          cited=e['evidence_id'] in bound or e['evidence_id'] in cited,
                          image_note='Изображение, переданное модели' if picture else
                                     'Изображение модели не передавалось',
                          evidence_id=e['evidence_id'])
                evidence.append(ev)
                receipts.append(dict(candidate=key, side=side, evidence_id=e['evidence_id'], page=e['page'],
                                     pdf_sha256=pdf['sha256'], raster_sha256=delivered.get(e['evidence_id'], {}).get('sha256'),
                                     document_version=e['document_version']))
            evidence.sort(key=lambda e: (not e['cited'], e['page']))
            sides[side] = dict(value=raw.get(side+'_state', {}).get('value') or 'Состояние не определено', evidence=evidence)
        presented[key] = dict(title=raw.get('project_change_summary') or p['candidate_subject']['subject'],
                              status=row['status'], **sides)
        diagnostics[key] = dict(candidate_id=key, raw_verdict=raw.get('verdict', 'NO_CALL_MISSING'),
            final_verdict=row['status'], completeness=row['completeness'], binding=row.get('binding_status'),
            blockers=n.get('issues', []), reason=n.get('reason', raw.get('reasoning_ru', '')),
            sufficiency=n.get('sufficiency'), evidence_roles=roles,
            source_modality={e['evidence_id']: e['route'] for es in p['evidence_packet']['evidence'].values() for e in es})
    display = []
    for i, group in enumerate(groups, 1):
        members = group['member_ids']; assert all(bykey[k]['status']=='ACCEPT' for k in members)
        item = dict(presented[members[0]], id=f'PC_{i:03d}', diagnostic_keys=members)
        if len(members)>1:
            item['title']='; '.join(presented[k]['title'] for k in members)
            for side in ('old','new'):
                item[side]=dict(value='; '.join(presented[k][side]['value'] for k in members),
                               evidence=[e for k in members for e in presented[k][side]['evidence']])
        display.append(item)
    assert {k for g in groups for k in g['member_ids']} == {k for k,r in bykey.items() if r['status']=='ACCEPT'}
    for status in ('REVIEW','NOT_CHANGE'):
        for i,(key,row) in enumerate(((k,r) for k,r in bykey.items() if r['status']==status),1):
            display.append(dict(presented[key], id=('R' if status=='REVIEW' else 'N')+f'_{i:03d}', diagnostic_keys=[key]))
    data=dict(pair=label, discipline='АР1' if label=='A' else 'ИОС4.2', counts=dict(Counter(r['status'] for r in rows)),
              project_changes=len(groups), rows=display, ready=True)
    save(OUT/f'PAIR_{label}_UI_DATA.json',data)
    save(WEB/f'data/{label}.json',data)
    save(WEB/f'data/{label}_diagnostic.json',diagnostics)
    save(OUT/f'PAIR_{label}_UI_PROVENANCE.json',receipts)
    return data


def pair_a():
    from openpyxl import load_workbook
    rows=read(V4/'PAIR_A_REPAIRED_V4_RESULTS.json')
    book=load_workbook(V4/'PAIR_A_REPAIRED_V4_SYSTEM_OUTPUT.xlsx',read_only=True)
    xrows=list(book['System output'].values);book.close()
    assert {r[0]:r[6] for r in xrows[1:]}=={r['package_id']:r['status'] for r in rows}
    data=build('A',rows,read(V4/'PROJECT_CHANGES_REPLAY.json')['groups'],read(LIVE_A/'CALL_PLAN.json')['packages'],LIVE_A)
    assert data['counts']=={'ACCEPT':11,'REVIEW':45,'NOT_CHANGE':3}
    WEB.mkdir(parents=True,exist_ok=True)
    for name in ('index.html','app.js','style.css'):
        shutil.copyfile(Path(__file__).parent/name,WEB/name)
    if not (WEB/'data/B.json').exists():
        save(WEB/'data/B.json',dict(pair='B',discipline='ИОС4.2',ready=False,message='Live run ещё не выполнен.'))
    save(OUT/'PAIR_A_INPUT_RECEIPT.json',dict(files={str(V4/n):sha(V4/n) for n in
        ('PAIR_A_REPAIRED_V4_RESULTS.json','PROJECT_CHANGES_REPLAY.json','PAIR_A_REPAIRED_V4_SYSTEM_OUTPUT.xlsx','FINAL_REPORT_VERIFIED.md')},
        truth_used=False,model_calls=0))
    print(data['counts'])

if __name__=='__main__': pair_a()
