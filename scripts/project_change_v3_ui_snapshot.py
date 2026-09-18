#!/usr/bin/env python3
"""Build and serve the immutable ProjectChange V3 research UI snapshot.

This is deliberately a presentation-only tool.  It reads the two sealed V3
result files and their already materialised evidence; it never imports or runs
the mapper, miner, dedupe, evaluation, or any model client.

    python scripts/project_change_v3_ui_snapshot.py build
    python scripts/project_change_v3_ui_snapshot.py serve --port 8774
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import shutil
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, quote, unquote, urlsplit


REPO = Path(__file__).resolve().parents[1]
SOURCE = Path('/home/coder/auditmanager/corpus-audits/20260914_project_change_272/ai_first_semantic_mapping_projectchange_v3')
SNAPSHOT = SOURCE.parent / 'projectchange_v3_ai_first_ui_snapshot'
PAIRS = {
    'A': {'key': 'ad0a31a342a666082f2ef66a', 'label': 'АР1'},
    'B': {'key': 'caea6d2810c334ec0368de8e', 'label': 'ИОС4.2'},
}


def sha(path: Path) -> str:
    return hashlib.file_digest(path.open('rb'), 'sha256').hexdigest()


def dump(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + '\n', encoding='utf-8')


def page_block(pair: str, evidence: dict) -> dict:
    page = SOURCE / 'source' / f'pair_{pair.lower()}' / evidence['side'].lower() / f"p{evidence['physical_page']:03d}" / 'page.json'
    data = json.loads(page.read_text(encoding='utf-8'))
    block = next((b for b in data['blocks'] if b['block_id'] == evidence['block_id']), None)
    if not block:
        raise ValueError(f"Missing evidence block {evidence['block_id']} in {page}")
    return block


def evidence_view(pair: str, evidence: dict, crops: Path) -> dict:
    block = page_block(pair, evidence)
    crop = Path(evidence['crop_ref']) if evidence.get('crop_ref') else None
    crop_name = ''
    if crop and crop.is_file():
        crop_name = f"{pair}_{evidence['side'].lower()}_{evidence['physical_page']:03d}_{evidence['block_id']}.png"
        destination = crops / crop_name
        if not destination.exists():
            shutil.copy2(crop, destination)
    # For TEXT/TABLE this is the source's preserved structured rendition, not
    # a newly inferred formatting or extraction.
    return {
        'id': evidence['block_id'], 'side': evidence['side'], 'type': evidence['block_type'],
        'page': evidence['physical_page'], 'bbox': evidence['bbox'], 'source_pdf': evidence['source_pdf'],
        'source_pdf_sha256': sha(Path(evidence['source_pdf'])), 'role': evidence['evidence_role'],
        'fragment': evidence['relevant_fragment'], 'structured_md': block.get('structured_md', ''),
        'tables': block.get('tables', []), 'crop': 'crops/' + crop_name if crop_name else '',
    }


def build_pair(pair: str, crops: Path) -> tuple[dict, dict]:
    final_path = SOURCE / f'PAIR_{pair}_FINAL_PROJECTCHANGES.json'
    freeze_path = SOURCE / f'PAIR_{pair}_AI_FIRST_PROJECTCHANGE_V3_RESULT_FREEZE.json'
    final = json.loads(final_path.read_text(encoding='utf-8'))
    freeze = json.loads(freeze_path.read_text(encoding='utf-8'))
    items = []
    for change in final['projectchanges']:
        items.append({
            'id': change['projectchange_id'], 'subject': change['engineering_subject'], 'scope': change['scope'],
            'locations': change['locations'], 'change': change['change_summary'], 'old': change['old_state'],
            'new': change['new_state'], 'old_pages': change['old_pages'], 'new_pages': change['new_pages'],
            # The final V3 list has no per-event rejection/review state.  Its
            # members are therefore shown as the frozen system output, without
            # importing source-first evaluation labels.
            'status': 'ACCEPT', 'parameters': change['changed_parameters'],
            'evidence': [evidence_view(pair, e, crops) for e in change['evidence_items']],
            'modalities': change['modalities'], 'diagnostic': {
                'confidence': change['confidence'], 'why_one_event': change['why_one_event'],
                'dedupe_lineage': change['dedupe_lineage'], 'dedupe_reason': change['dedupe_reason'],
            },
        })
    data = {
        'schema': 'projectchange-v3-ui-data/1', 'pair': pair, 'pair_key': PAIRS[pair]['key'],
        'label': PAIRS[pair]['label'], 'source_freeze_sha256': sha(freeze_path),
        'source_final_sha256': sha(final_path), 'frozen_at': freeze['frozen_at'],
        'system_output_counts': {'projectchanges': len(items), 'confirmed': len(items), 'review': 0, 'not_change': 0},
        'items': items,
    }
    audit = {'pair': pair, 'pair_key': PAIRS[pair]['key'], 'projectchange_ids': [i['id'] for i in items],
             'evidence_count': sum(len(i['evidence']) for i in items),
             'modalities': {m: sum(m in i['modalities'] for i in items) for m in ['TEXT', 'TABLE', 'GRAPHIC']}}
    return data, audit


def build() -> Path:
    if SNAPSHOT.exists():
        raise FileExistsError(f'Snapshot already exists and is immutable: {SNAPSHOT}')
    SNAPSHOT.mkdir(mode=0o755)
    crops = SNAPSHOT / 'crops'; crops.mkdir()
    audits, files = {}, {}
    for pair in PAIRS:
        data, audit = build_pair(pair, crops)
        name = f'PAIR_{pair}_UI_DATA.json'; dump(SNAPSHOT / name, data); audits[pair] = audit
        files[name] = sha(SNAPSHOT / name)
    dump(SNAPSHOT / 'UI_DATA_AUDIT.json', audits)
    files['UI_DATA_AUDIT.json'] = sha(SNAPSHOT / 'UI_DATA_AUDIT.json')
    # The smoke receipt is filled only after a browser pass.  It is deliberately
    # not part of the sealed UI-data checksum: it records verification of the
    # already immutable data, never modifies it.
    dump(SNAPSHOT / 'BROWSER_SMOKE_TEST.json', {'status': 'NOT_RUN', 'model_calls': 0})
    files['BROWSER_SMOKE_TEST.json'] = sha(SNAPSHOT / 'BROWSER_SMOKE_TEST.json')
    commit = __import__('subprocess').check_output(['git', '-C', str(REPO), 'rev-parse', 'HEAD'], text=True).strip()
    manifest = {'schema': 'projectchange-v3-ui-snapshot/1', 'immutable': True, 'model_calls': 0,
                'ui_build_commit': commit, 'source': str(SOURCE), 'pairs': {}, 'files': files}
    for pair in PAIRS:
        freeze = SOURCE / f'PAIR_{pair}_AI_FIRST_PROJECTCHANGE_V3_RESULT_FREEZE.json'
        manifest['pairs'][pair] = {'pair_key': PAIRS[pair]['key'], 'source_freeze': freeze.name,
            'source_freeze_sha256': sha(freeze), 'ui_data': f'PAIR_{pair}_UI_DATA.json',
            'ui_data_sha256': files[f'PAIR_{pair}_UI_DATA.json']}
    # A self-hash is mathematically impossible.  This checksum is the canonical
    # sealed manifest payload before its displayed checksum field is added.
    manifest['snapshot_sha256'] = hashlib.sha256(json.dumps(manifest, ensure_ascii=False, sort_keys=True,
        separators=(',', ':')).encode()).hexdigest()
    dump(SNAPSHOT / 'V3_UI_SNAPSHOT_MANIFEST.json', manifest)
    dump(SNAPSHOT / 'RESEARCH_UI_RECEIPT.json', {'status': 'READY_FOR_BROWSER_SMOKE', 'model_calls': 0,
         'snapshot_sha256': sha(SNAPSHOT / 'V3_UI_SNAPSHOT_MANIFEST.json'), 'ui_build_commit': commit})
    (SNAPSHOT / 'FINAL_REPORT.md').write_text('# ProjectChange V3 UI snapshot\n\nPending browser smoke test.\n', encoding='utf-8')
    return SNAPSHOT


def seal_smoke(result: Path) -> None:
    """Attach a browser receipt without ever changing frozen UI data/crops."""
    if not result.is_file() or not (SNAPSHOT / 'V3_UI_SNAPSHOT_MANIFEST.json').is_file():
        raise FileNotFoundError('Snapshot or smoke result is missing')
    value = json.loads(result.read_text(encoding='utf-8'))
    if value.get('model_calls') != 0 or value.get('status') != 'PASS':
        raise ValueError('Only a completed zero-model browser smoke result may be sealed')
    dump(SNAPSHOT / 'BROWSER_SMOKE_TEST.json', value)
    receipt = json.loads((SNAPSHOT / 'RESEARCH_UI_RECEIPT.json').read_text(encoding='utf-8'))
    receipt.update({'status': 'READY_FOR_USER_V3_UI_REVIEW', 'browser_smoke_sha256': sha(SNAPSHOT / 'BROWSER_SMOKE_TEST.json')})
    dump(SNAPSHOT / 'RESEARCH_UI_RECEIPT.json', receipt)
    (SNAPSHOT / 'FINAL_REPORT.md').write_text('# ProjectChange V3 UI snapshot\n\nBrowser smoke: PASS.\n', encoding='utf-8')


HTML = r'''<!doctype html><meta charset="utf-8"><title>ProjectChange V3 · research preview</title>
<style>body{margin:0;background:#f4f6f8;color:#162431;font:14px system-ui,sans-serif}header{padding:18px 5%;background:#fff;border-bottom:1px solid #d8dde3}h1{font-size:21px;margin:0 0 8px}.note{color:#596575}.bar,main{max-width:1400px;margin:auto}.bar{padding:14px 5%;display:flex;gap:9px;align-items:center}.bar button{padding:7px 12px;border:1px solid #b9c6d0;border-radius:5px;background:#fff;cursor:pointer}.bar button.active{background:#d8f1ee;border-color:#13857b}.counts{margin-left:auto;color:#596575}main{padding:0 5% 30px}.row{background:#fff;border:1px solid #d8dde3;border-radius:7px;margin:9px 0}.row summary{padding:12px;cursor:pointer;display:grid;grid-template-columns:120px 1fr 120px;gap:12px}.badge{font-size:12px;color:#08695f}.detail{padding:0 14px 16px}.states,.ev{display:grid;grid-template-columns:1fr 1fr;gap:14px}.state,.evidence{background:#f7f9fa;padding:10px;border-radius:5px;min-width:0}.evidence img{width:100%;max-height:280px;object-fit:contain;background:#fff;border:1px solid #d8dde3}.evidence pre{white-space:pre-wrap;max-height:280px;overflow:auto;background:#fff;padding:8px}.evidence a{color:#08776e}.params{width:100%;border-collapse:collapse}.params td,.params th{padding:6px;border-bottom:1px solid #d8dde3;text-align:left}.diag{margin-top:10px;color:#596575}dialog{max-width:95vw;width:1200px;border:0;border-radius:7px;padding:14px}dialog img{width:100%;max-height:85vh;object-fit:contain}@media(max-width:800px){.states,.ev{grid-template-columns:1fr}.row summary{grid-template-columns:1fr}.counts{display:none}}</style>
<header><h1>3. Изменения проекта · V3 research snapshot</h1><div class="note">Только frozen system output. Source-first evaluation и expected truth не загружаются.</div></header><div class="bar"><button id="A">АР1</button><button id="B">ИОС4.2</button><button id="accepted">Подтверждено системой</button><button id="review">Требуют проверки</button><button id="diag">Диагностика: выкл.</button><span class="counts" id="counts"></span></div><main id="app"></main><dialog id="zoom"><button onclick="zoom.close()">Закрыть</button><img></dialog>
<script>let pair='A',tab='accepted',diagnostic=false,data={};const E=s=>document.querySelector(s),esc=s=>String(s??'').replace(/[&<>"']/g,x=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[x]));async function load(p){data=await fetch('/data/PAIR_'+p+'_UI_DATA.json').then(x=>x.json());render()}function evidence(e){let body=e.type==='GRAPHIC'&&e.crop?'<img src="/'+e.crop+'" onclick="zoom.querySelector(\'img\').src=this.src;zoom.showModal()">':e.type==='TABLE'&&e.structured_md?'<pre>'+esc(e.structured_md)+'</pre>':'<pre>'+esc(e.structured_md||e.fragment)+'</pre>';return '<div class=evidence><b>'+e.side+' · '+e.type+' · стр. '+e.page+'</b><p>'+esc(e.role)+'</p>'+body+'<p><a target=_blank href="/pdf?pair='+pair+'&id='+encodeURIComponent(e.id)+'&side='+e.side+'&page='+e.page+'">Открыть PDF page</a> · bbox '+e.bbox.map(x=>x.toFixed(3)).join(', ')+'</p></div>'}function render(){for(const p of ['A','B'])E('#'+p).classList.toggle('active',p===pair);E('#diag').textContent='Диагностика: '+(diagnostic?'вкл.':'выкл.');const list=data.items.filter(x=>tab==='accepted'?x.status==='ACCEPT':x.status==='REVIEW');E('#counts').textContent='Всего system output: '+data.system_output_counts.projectchanges+' · показано: '+list.length;E('#app').innerHTML=list.length?list.map(x=>'<details class=row><summary><b>'+esc(x.id)+'</b><span>'+esc(x.change)+'</span><span class=badge>Подтверждено системой</span></summary><div class=detail><p><b>Объект:</b> '+esc(x.subject)+'</p><div class=states><div class=state><b>OLD</b><p>'+esc(x.old)+'</p><small>Страницы: '+x.old_pages.join(', ')+'</small></div><div class=state><b>NEW</b><p>'+esc(x.new)+'</p><small>Страницы: '+x.new_pages.join(', ')+'</small></div></div><h3>Изменившиеся характеристики</h3><table class=params><tr><th>Характеристика</th><th>OLD</th><th>NEW</th></tr>'+x.parameters.map(q=>'<tr><td>'+esc(q.name)+'</td><td>'+esc(q.old_value)+'</td><td>'+esc(q.new_value)+'</td></tr>').join('')+'</table><h3>Evidence</h3><div class=ev>'+x.evidence.map(evidence).join('')+'</div>'+(diagnostic?'<details class=diag open><summary>Диагностика</summary><pre>'+esc(JSON.stringify(x.diagnostic,null,2))+'</pre></details>':'')+'</div></details>').join(''):'<p>В этой вкладке нет элементов.</p>'}for(const p of ['A','B'])E('#'+p).onclick=()=>{pair=p;load(p)};E('#accepted').onclick=()=>{tab='accepted';render()};E('#review').onclick=()=>{tab='review';render()};E('#diag').onclick=()=>{diagnostic=!diagnostic;render()};load(pair)</script>'''


def serve(port: int) -> None:
    if not (SNAPSHOT / 'V3_UI_SNAPSHOT_MANIFEST.json').is_file():
        raise FileNotFoundError('Build the snapshot first')
    datasets = {p: json.loads((SNAPSHOT / f'PAIR_{p}_UI_DATA.json').read_text()) for p in PAIRS}
    evidence = {p: {e['id']: e for item in d['items'] for e in item['evidence']} for p, d in datasets.items()}
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_): pass
        def send(self, raw, content_type='text/html; charset=utf-8', status=200):
            self.send_response(status); self.send_header('Content-Type',content_type); self.send_header('Content-Length',str(len(raw))); self.end_headers(); self.wfile.write(raw)
        def do_GET(self):
            u=urlsplit(self.path); path=unquote(u.path); q=parse_qs(u.query)
            if path=='/': return self.send(HTML.encode())
            if path.startswith('/data/') or path.startswith('/crops/'):
                file=(SNAPSHOT/path.lstrip('/')).resolve()
                if file.is_file() and file.is_relative_to(SNAPSHOT): return self.send(file.read_bytes(),'application/json' if file.suffix=='.json' else 'image/png')
            if path=='/pdf':
                p=q.get('pair',[''])[0]; e=evidence.get(p,{}).get(q.get('id',[''])[0]); page=q.get('page',[''])[0]
                if e and page==str(e['page']) and Path(e['source_pdf']).is_file() and sha(Path(e['source_pdf']))==e['source_pdf_sha256']:
                    return self.send(Path(e['source_pdf']).read_bytes(),'application/pdf')
            self.send(b'Not found','text/plain',404)
    print(f'RESEARCH UI: http://127.0.0.1:{port}/', flush=True)
    ThreadingHTTPServer(('127.0.0.1',port),Handler).serve_forever()


if __name__ == '__main__':
    parser=argparse.ArgumentParser(); sub=parser.add_subparsers(dest='command',required=True); sub.add_parser('build'); s=sub.add_parser('serve'); s.add_argument('--port',type=int,default=8774); seal=sub.add_parser('seal-smoke'); seal.add_argument('result',type=Path); a=parser.parse_args()
    if a.command=='build': print(build())
    elif a.command=='serve': serve(a.port)
    else: seal_smoke(a.result)
