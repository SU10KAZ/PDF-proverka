#!/usr/bin/env python3
"""Build and serve the isolated, zero-model Human Mapping Verification UI.

It only reads the sealed V3 semantic maps and already materialised page/block
assets. Reviews are append-only JSONL records outside the frozen V3 directory.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlsplit

REPO = Path(__file__).resolve().parents[1]
SOURCE = Path('/home/coder/auditmanager/corpus-audits/20260914_project_change_272/ai_first_semantic_mapping_projectchange_v3')
OUT = SOURCE.parent / 'human_mapping_verification_ui_v1'
PAIRS = {'A': {'label': 'АР1', 'key': 'ad0a31a342a666082f2ef66a'}, 'B': {'label': 'ИОС4.2', 'key': 'caea6d2810c334ec0368de8e'}}


def digest(path: Path) -> str:
    return hashlib.file_digest(path.open('rb'), 'sha256').hexdigest()


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + '\n', encoding='utf-8')


def block_view(pair: str, ref: dict, assets: Path) -> dict:
    page_dir = SOURCE / 'source' / f'pair_{pair.lower()}' / ref['side'].lower() / f"p{ref['physical_page']:03d}"
    page = json.loads((page_dir / 'page.json').read_text(encoding='utf-8'))
    block = next(b for b in page['blocks'] if b['block_id'] == ref['block_id'])
    crop_ref = block.get('graphic_crop_ref') or ''
    crop = Path(crop_ref) if crop_ref else page_dir / f"{block['block_id']}.png"
    copied_crop = ''
    if crop.is_file():
        name = f"{pair}_{ref['side'].lower()}_{ref['physical_page']:03d}_{block['block_id']}.png"
        shutil.copy2(crop, assets / name)
        copied_crop = f'assets/{name}'
    return {'id': block['block_id'], 'side': ref['side'], 'type': block['modality'], 'page': ref['physical_page'],
            'bbox': block['bbox'], 'structured_md': block.get('structured_md', ''), 'tables': block.get('tables', []),
            'crop': copied_crop, 'relevance': ref.get('relevance', '')}


def pages_for_region(pair: str, region: dict, assets: Path) -> dict:
    chosen = region['important_text_blocks'] + region['important_table_blocks'] + region['important_graphic_blocks']
    result = {'OLD': [], 'NEW': []}
    for side in ('OLD', 'NEW'):
        pages = sorted({r['physical_page'] for r in chosen if r['side'] == side})
        for number in pages:
            page_dir = SOURCE / 'source' / f'pair_{pair.lower()}' / side.lower() / f'p{number:03d}'
            meta = json.loads((page_dir / 'page.json').read_text(encoding='utf-8'))
            image = page_dir / 'full_page.png'
            name = f'{pair}_{side.lower()}_{number:03d}.png'
            shutil.copy2(image, assets / name)
            blocks = []
            for b in meta['blocks']:
                crop_ref = b.get('graphic_crop_ref') or ''
                crop = Path(crop_ref) if crop_ref else page_dir / f"{b['block_id']}.png"
                copied_crop = ''
                if b['modality'] == 'GRAPHIC' and crop.is_file():
                    crop_name = f"{pair}_{side.lower()}_{number:03d}_{b['block_id']}.png"
                    shutil.copy2(crop, assets / crop_name)
                    copied_crop = f'assets/{crop_name}'
                blocks.append({'id': b['block_id'], 'type': b['modality'], 'page': number, 'bbox': b['bbox'],
                    'structured_md': b.get('structured_md', ''), 'tables': b.get('tables', []), 'crop': copied_crop})
            result[side].append({'page': number, 'image': f'assets/{name}', 'blocks': blocks})
    return result


def build() -> Path:
    if OUT.exists():
        raise FileExistsError(f'Output exists (deliberately not overwritten): {OUT}')
    (OUT / 'assets').mkdir(parents=True)
    (OUT / 'human_mapping_reviews').mkdir()
    manifest = {'schema': 'human-mapping-verification-ui/1', 'model_calls': 0, 'source': str(SOURCE), 'pairs': {}, 'files': {}}
    for pair, info in PAIRS.items():
        mapping_path = SOURCE / f'PAIR_{pair}_SEMANTIC_MAP.json'
        mapping = json.loads(mapping_path.read_text(encoding='utf-8'))
        regions = []
        for raw in mapping['regions']:
            refs = raw['important_text_blocks'] + raw['important_table_blocks'] + raw['important_graphic_blocks']
            regions.append({'id': raw['region_id'], 'domain': raw['engineering_domain'], 'scope': raw['scope'],
                'reason': raw['reason_for_correspondence'], 'confidence': raw['confidence'],
                'old_blocks': [block_view(pair, x, OUT / 'assets') for x in refs if x['side'] == 'OLD'],
                'new_blocks': [block_view(pair, x, OUT / 'assets') for x in refs if x['side'] == 'NEW'],
                'pages': pages_for_region(pair, raw, OUT / 'assets')})
        data = {'schema': 'human-mapping-ui-data/1', 'pair': pair, 'pair_key': info['key'], 'label': info['label'],
                'source_semantic_map': mapping_path.name, 'source_sha256': digest(mapping_path), 'regions': regions}
        name = f'UI_DATA_PAIR_{pair}.json'; write_json(OUT / name, data)
        manifest['pairs'][pair] = {'label': info['label'], 'regions': len(regions), 'source_sha256': digest(mapping_path), 'data': name}
        manifest['files'][name] = digest(OUT / name)
    (OUT / 'HUMAN_MAPPING_CONTRACT.md').write_text(CONTRACT, encoding='utf-8')
    manifest['files']['HUMAN_MAPPING_CONTRACT.md'] = digest(OUT / 'HUMAN_MAPPING_CONTRACT.md')
    write_json(OUT / 'UI_SNAPSHOT_MANIFEST.json', manifest)
    write_json(OUT / 'BROWSER_SMOKE_TEST.json', {'status': 'NOT_RUN', 'model_calls': 0})
    (OUT / 'FINAL_REPORT.md').write_text('# Human Mapping Verification UI V1\n\nPending browser smoke test.\n', encoding='utf-8')
    return OUT


CONTRACT = '''# Human Mapping Contract\n\nThis UI is research-only. It does not modify Semantic Mapping V3, ProjectChanges, evidence, prompts, mining, dedupe, source files, production, validation, or final holdout.\n\n- `HUMAN_CONFIRMED`: in a future pipeline, exactly these OLD and NEW blocks are a hard-linked direct-comparison area.\n- `HUMAN_REJECTED`: only this exact OLD↔NEW set is prohibited; every member block remains available for other semantic matches.\n- `HUMAN_UNCERTAIN` and `UNREVIEWED`: no restriction; ordinary AI semantic mapping applies.\n\nEvery review is append-only in `human_mapping_reviews/reviews.jsonl`. A later decision carries `supersedes_review_id`; it never overwrites prior history. The current UI does not consume reviews into any pipeline.\n'''


HTML = r'''<!doctype html><meta charset="utf-8"><title>Human Mapping Verification · research</title><style>
*{box-sizing:border-box}body{margin:0;background:#f4f6f8;color:#162431;font:14px system-ui,sans-serif}header,.bar,.main{max-width:1800px;margin:auto;padding:14px 2%}header{background:white;max-width:none;padding-left:calc((100% - 1800px)/2 + 2%);border-bottom:1px solid #d5dce2}h1{font-size:20px;margin:0 0 4px}.note{color:#596575}.bar,.controls,.decision{display:flex;gap:7px;align-items:center;flex-wrap:wrap}.bar button,.decision button,.controls button{padding:7px 10px;border:1px solid #aebbc6;background:white;border-radius:5px;cursor:pointer}.bar button.active,.controls button.active{background:#d7f0ed;border-color:#087c72}.progress{margin-left:auto;color:#52616d}.card{background:white;border:1px solid #d5dce2;border-radius:8px;padding:13px}.title{display:flex;justify-content:space-between;gap:12px}.controls{padding:9px;background:#f7f9fa;border-radius:6px;margin-top:10px}.controls label{white-space:nowrap}.pages{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px}.side h2{font-size:15px;margin:0 0 7px}.viewport{height:70vh;min-height:500px;overflow:auto;position:relative;background:#dce4e9;border:1px solid #aebbc6;touch-action:none}.viewport.pan{cursor:grab}.viewport.pan.dragging{cursor:grabbing}.sheet{width:100%;transform-origin:top left;will-change:transform;padding:0 0 20px}.page{position:relative;background:#e5eaee;margin-bottom:10px;overflow:hidden;box-shadow:0 1px 3px #788896}.page img{display:block;width:100%;height:auto}.block{position:absolute;border:2px solid #1677c8;background:#1677c822;cursor:pointer}.block.TABLE{border-color:#e07b00;background:#e07b0022}.block.GRAPHIC{border-color:#8d43b6;background:#8d43b622}.block.selected{outline:3px solid #e62d86;background:#e62d8640}.link{padding:8px;background:#edf7f6;border-left:4px solid #087c72;margin:8px 0}.detail{margin-top:10px;background:#f7f9fa;padding:10px}.detail pre{white-space:pre-wrap;max-height:240px;overflow:auto}.detail table{border-collapse:collapse;width:100%}.detail td,.detail th{border:1px solid #ccd5dc;padding:4px}.crop{max-width:100%;max-height:300px;cursor:zoom-in}.manual{background:#fff7e7;border-left:4px solid #e07b00;padding:8px;margin-top:10px}.link-layer{position:fixed;inset:0;width:100vw;height:100vh;pointer-events:none;z-index:20}.link-line{stroke:#087c72;stroke-width:3;stroke-dasharray:8 6;fill:none;opacity:.9}.link-line.all{stroke:#75439c;stroke-width:2;opacity:.65}@media(max-width:900px){.pages{grid-template-columns:1fr}.progress{margin-left:0}.viewport{height:58vh}}</style>
<header><h1>Проверка OLD ↔ NEW связей</h1><div class=note>Research UI · frozen Semantic Mapping V3 · никаких model calls</div></header><div class=bar><button id=A>АР1</button><button id=B>ИОС4.2</button><button data-q=UNREVIEWED>Не проверено</button><button data-q=HUMAN_CONFIRMED>Подтверждено</button><button data-q=HUMAN_REJECTED>Отклонено</button><button data-q=HUMAN_UNCERTAIN>Не уверен</button><span class=progress id=progress></span></div><main class=main><div id=app></div></main><svg class=link-layer id=links aria-hidden=true></svg><dialog id=zoom><button onclick="zoom.close()">Закрыть</button><img></dialog><script>
let pair='A',queue='UNREVIEWED',data,history=[],selected={OLD:[],NEW:[]},panMode=null,showAll=false,view={OLD:{x:0,y:0,z:1,locked:false},NEW:{x:0,y:0,z:1,locked:false}};const $=s=>document.querySelector(s),esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
async function load(){data=await fetch('/data/UI_DATA_PAIR_'+pair+'.json').then(r=>r.json());history=await fetch('/reviews?pair='+pair).then(r=>r.json());selected={OLD:[],NEW:[]};render()}
function latest(r){let x=history.filter(h=>h.region_id===r.id);return x.length?x[x.length-1]:null}function stat(r){return latest(r)?.status||'UNREVIEWED'}function linked(r){let l=latest(r);return {OLD:selected.OLD.length?selected.OLD:(l?.old_block_ids||r.old_blocks.map(b=>b.id)),NEW:selected.NEW.length?selected.NEW:(l?.new_block_ids||r.new_blocks.map(b=>b.id))}}
function overlay(p,b,r){let [x,y,w,h]=b.bbox,link=linked(r);let active=selected[p.side].includes(b.id)||link[p.side].includes(b.id);return `<button class="block ${b.type} ${active?'selected':''}" title="${esc(b.id)} · ${b.type}" data-side="${p.side}" data-id="${b.id}" style="left:${x*100}%;top:${y*100}%;width:${(w-x)*100}%;height:${(h-y)*100}%"></button>`}
function page(p,r){return `<div class=page><img src="/${p.image}">${p.blocks.map(b=>overlay(p,b,r)).join('')}</div>`}
function showDetail(side,id,r){let b=[...r.old_blocks,...r.new_blocks].find(x=>x.id===id)||r.pages[side].flatMap(p=>p.blocks).find(x=>x.id===id);let table=b.tables?.length?'<table>'+b.tables[0].map(row=>'<tr>'+row.map(cell=>'<td>'+esc(cell)+'</td>').join('')+'</tr>').join('')+'</table>':'';return `<div class=detail><b>${esc(b.id)}</b> · ${b.type} · стр. ${b.page??'—'} · bbox ${b.bbox.map(x=>x.toFixed(3)).join(', ')}${b.type==='GRAPHIC'&&b.crop?`<br><img class=crop src="/${b.crop}" onclick="zoom.querySelector('img').src=this.src;zoom.showModal()">`:''}${b.type==='TABLE'?table:`<pre>${esc(b.structured_md||'Структурированное содержимое отсутствует.')}</pre>`}</div>`}
function transform(side){let v=view[side],s=$('#sheet-'+side);if(s)s.style.transform=`translate(${v.x}px,${v.y}px) scale(${v.z})`;requestAnimationFrame(drawLines)}
function controls(side){let v=view[side];return `<b>${side}</b><button data-pan="${side}" class="${panMode===side?'active':''}">${panMode===side?'Перемещение включено':'Переместить лист'}</button><button data-reset="${side}">Сбросить положение</button><button data-lock="${side}" class="${v.locked?'active':''}">${v.locked?'Положение зафиксировано':'Зафиксировать положение'}</button><label>Масштаб <input data-zoom="${side}" type=range min=.6 max=1.8 step=.1 value=${v.z}></label>`}
function render(){for(let x of ['A','B'])$('#'+x).classList.toggle('active',pair===x);let reviewed=data.regions.filter(r=>stat(r)!=='UNREVIEWED').length;$('#progress').textContent=`Проверено ${reviewed} / ${data.regions.length}`;let r=data.regions.find(x=>stat(x)===queue)||data.regions[0];if(!r){$('#app').innerHTML='<p>В этой очереди нет связей.</p>';return}let l=latest(r);$('#app').innerHTML=`<section class=card><div class=title><div><b>${r.id}</b> · ${esc(r.domain)}<p>${esc(r.scope)}</p></div><small>AI-proposed link · confidence ${r.confidence}</small></div><div class=link>OLD blocks (${r.old_blocks.map(x=>esc(x.id)).join(', ')}) ↔ NEW blocks (${r.new_blocks.map(x=>esc(x.id)).join(', ')})</div><div class=controls>${controls('OLD')} ${controls('NEW')}<button id=reset-both>Сбросить оба</button><button id=all-links class="${showAll?'active':''}">Показать все связи: ${showAll?'вкл.':'выкл.'}</button></div><div class=pages><div class=side><h2>OLD</h2><div class="viewport ${panMode==='OLD'?'pan':''}" id=view-OLD><div class=sheet id=sheet-OLD>${r.pages.OLD.map(p=>page({...p,side:'OLD'},r)).join('')}</div></div></div><div class=side><h2>NEW</h2><div class="viewport ${panMode==='NEW'?'pan':''}" id=view-NEW><div class=sheet id=sheet-NEW>${r.pages.NEW.map(p=>page({...p,side:'NEW'},r)).join('')}</div></div></div></div><div class=manual>Выберите один или несколько блоков слева и справа, чтобы создать ручную связь. Выделены: OLD ${selected.OLD.length}, NEW ${selected.NEW.length}.</div><div class=decision><textarea id=comment placeholder="Комментарий (необязательно)"></textarea><button data-status=HUMAN_CONFIRMED>ПОДТВЕРДИТЬ</button><button data-status=HUMAN_REJECTED>ОТКЛОНИТЬ</button><button data-status=HUMAN_UNCERTAIN>НЕ УВЕРЕН</button></div>${l?`<p>Последнее решение: <b>${l.status}</b> · ${esc(l.timestamp)}</p>`:''}<div id=details></div></section>`;for(let side of ['OLD','NEW']){transform(side);bindPan(side);$('#view-'+side).addEventListener('scroll',()=>requestAnimationFrame(drawLines));}document.querySelectorAll('.block').forEach(b=>b.onclick=e=>{if(panMode)return;let s=b.dataset.side,id=b.dataset.id;selected[s]=selected[s].includes(id)?selected[s].filter(x=>x!==id):[...selected[s],id];render();$('#details').innerHTML=showDetail(s,id,r)});document.querySelectorAll('[data-status]').forEach(b=>b.onclick=()=>save(r,b.dataset.status));document.querySelectorAll('[data-pan]').forEach(b=>b.onclick=()=>{panMode=panMode===b.dataset.pan?null:b.dataset.pan;render()});document.querySelectorAll('[data-reset]').forEach(b=>{b.onclick=()=>{let v=view[b.dataset.reset];v.x=v.y=0;v.z=1;render()}});document.querySelectorAll('[data-lock]').forEach(b=>b.onclick=()=>{view[b.dataset.lock].locked=!view[b.dataset.lock].locked;render()});document.querySelectorAll('[data-zoom]').forEach(i=>i.oninput=()=>{view[i.dataset.zoom].z=+i.value;transform(i.dataset.zoom)});$('#reset-both').onclick=()=>{for(let s of ['OLD','NEW'])Object.assign(view[s],{x:0,y:0,z:1});render()};$('#all-links').onclick=()=>{showAll=!showAll;render()};requestAnimationFrame(drawLines)}
function bindPan(side){let box=$('#view-'+side),drag;box.onpointerdown=e=>{if(panMode!==side||view[side].locked||e.target.closest('.block'))return;drag={x:e.clientX,y:e.clientY,ox:view[side].x,oy:view[side].y};box.setPointerCapture(e.pointerId);box.classList.add('dragging')};box.onpointermove=e=>{if(!drag)return;view[side].x=drag.ox+e.clientX-drag.x;view[side].y=drag.oy+e.clientY-drag.y;transform(side)};box.onpointerup=box.onpointercancel=()=>{drag=null;box.classList.remove('dragging')}}
function center(side,id){let e=document.querySelector(`.block[data-side="${side}"][data-id="${id}"]`);if(!e)return null;let r=e.getBoundingClientRect();return {x:r.left+r.width/2,y:r.top+r.height/2}}function path(a,b,cls=''){return `<path class="link-line ${cls}" d="M ${a.x} ${a.y} L ${b.x} ${b.y}"/>`}function drawLines(){let r=data?.regions.find(x=>stat(x)===queue)||data?.regions[0];if(!r)return;let l=linked(r),old=l.OLD.map(x=>center('OLD',x)).filter(Boolean),nw=l.NEW.map(x=>center('NEW',x)).filter(Boolean),svg=$('#links');if(!old.length||!nw.length||!svg)return;let paths='';if(showAll){for(let a of old)for(let b of nw)paths+=path(a,b,'all')}else{let avg=a=>({x:a.reduce((n,v)=>n+v.x,0)/a.length,y:a.reduce((n,v)=>n+v.y,0)/a.length});paths=path(avg(old),avg(nw))}svg.innerHTML=paths}
async function save(r,status){let old=selected.OLD.length?selected.OLD:r.old_blocks.map(b=>b.id),nw=selected.NEW.length?selected.NEW:r.new_blocks.map(b=>b.id);if(!old.length||!nw.length)return alert('Выберите блоки с обеих сторон.');let prior=latest(r);let response=await fetch('/reviews',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({pair_key:data.pair_key,region_id:r.id,old_block_ids:old,new_block_ids:nw,status,comment:$('#comment').value,previous_review_id:prior?.review_id||null})});if(!response.ok)return alert('Не удалось сохранить решение.');history.push(await response.json());selected={OLD:[],NEW:[]};render()}
$('#A').onclick=()=>{pair='A';load()};$('#B').onclick=()=>{pair='B';load()};document.querySelectorAll('[data-q]').forEach(b=>b.onclick=()=>{queue=b.dataset.q;render()});window.addEventListener('resize',()=>requestAnimationFrame(drawLines));load()</script>'''


def serve(port: int) -> None:
    if not (OUT / 'UI_SNAPSHOT_MANIFEST.json').is_file():
        raise FileNotFoundError('Run build first')
    reviews = OUT / 'human_mapping_reviews' / 'reviews.jsonl'
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_): pass
        def reply(self, raw: bytes, typ='application/json', status=200):
            self.send_response(status); self.send_header('Content-Type', typ); self.send_header('Content-Length', str(len(raw))); self.end_headers(); self.wfile.write(raw)
        def do_GET(self):
            url = urlsplit(self.path); path = unquote(url.path)
            if path == '/': return self.reply(HTML.encode(), 'text/html; charset=utf-8')
            if path == '/reviews':
                pair = url.query.split('pair=', 1)[-1] if 'pair=' in url.query else ''
                rows = [json.loads(x) for x in reviews.read_text(encoding='utf-8').splitlines()] if reviews.exists() else []
                return self.reply(json.dumps([x for x in rows if x['pair'] == pair], ensure_ascii=False).encode())
            if path.startswith('/data/') or path.startswith('/assets/'):
                file = (OUT / path.lstrip('/').removeprefix('data/')).resolve()
                if file.is_file() and file.is_relative_to(OUT): return self.reply(file.read_bytes(), 'image/png' if file.suffix=='.png' else 'application/json')
            self.reply(b'Not found', 'text/plain', 404)
        def do_POST(self):
            if urlsplit(self.path).path != '/reviews': return self.reply(b'Not found', 'text/plain', 404)
            try:
                raw = json.loads(self.rfile.read(int(self.headers.get('Content-Length', '0'))))
                status = raw['status']; assert status in {'HUMAN_CONFIRMED','HUMAN_REJECTED','HUMAN_UNCERTAIN'}
                assert raw['pair_key'] in {x['key'] for x in PAIRS.values()} and raw['old_block_ids'] and raw['new_block_ids']
            except (KeyError, ValueError, AssertionError): return self.reply(b'Bad review', 'text/plain', 400)
            pair = next(k for k,v in PAIRS.items() if v['key'] == raw['pair_key'])
            row = {'review_id': str(uuid.uuid4()), 'pair': pair, 'pair_key': raw['pair_key'], 'region_id': raw['region_id'],
                   'old_block_ids': raw['old_block_ids'], 'new_block_ids': raw['new_block_ids'], 'status': status,
                   'timestamp': datetime.now(timezone.utc).isoformat(), 'comment': raw.get('comment',''), 'reviewer_source':'HUMAN',
                   'supersedes_review_id': raw.get('previous_review_id')}
            with reviews.open('a', encoding='utf-8') as f: f.write(json.dumps(row, ensure_ascii=False, sort_keys=True)+'\n')
            self.reply(json.dumps(row, ensure_ascii=False).encode())
    print(f'RESEARCH UI: http://127.0.0.1:{port}/', flush=True); ThreadingHTTPServer(('127.0.0.1', port), Handler).serve_forever()


if __name__ == '__main__':
    p=argparse.ArgumentParser(); s=p.add_subparsers(dest='command',required=True); s.add_parser('build'); x=s.add_parser('serve'); x.add_argument('--port',type=int,default=8777); a=p.parse_args()
    print(build()) if a.command=='build' else serve(a.port)
