#!/usr/bin/env python3
"""Build and serve the isolated, zero-model Human Mapping Verification UI.

V1.2 adds BlockLink editing, clickable viewer-clipped dotted lines, and append-only block-link history.

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


CONTRACT = '''# Human Mapping Contract\n\nThis UI is research-only. It does not modify Semantic Mapping V3, ProjectChanges, evidence, prompts, mining, dedupe, source files, production, validation, or final holdout.\n\n- `HUMAN_CONFIRMED`: in a future pipeline, exactly these OLD and NEW blocks are a hard-linked direct-comparison area.\n- `HUMAN_REJECTED`: only this exact OLD↔NEW set is prohibited; every member block remains available for other semantic matches.\n- `HUMAN_UNCERTAIN` and `UNREVIEWED`: no restriction; ordinary AI semantic mapping applies.\n\nEvery review is append-only in `human_mapping_reviews/reviews.jsonl`. A later decision carries `supersedes_review_id`; it never overwrites prior history. The current UI does not consume reviews into any pipeline.\n\n## BlockLink layer (V1.2)\n\nA BlockLink is one OLD block id ↔ one NEW block id. Frozen V3 maps provide group membership only.\n\n- 1→1: one AI_PROPOSED BlockLink\n- 1→N / N→1: INHERITED_GROUP spokes (visual membership, not engineered pair truth)\n- N↔N: no Cartesian product invented; define BlockLinks manually\n\nHuman edits append to `human_mapping_reviews/human_block_link_edits.jsonl (live; smoke archived separately)` (`ADD_BLOCK_LINK`, `DELETE_BLOCK_LINK`, `REASSIGN_BLOCK_LINK`).\n\nDotted lines clip to `#mapping-workspace` (`overflow: hidden`).\n'''


HTML = r'''<!doctype html><meta charset="utf-8"><title>Human Mapping Verification · V1.2</title><style>
*{box-sizing:border-box}body{margin:0;background:#f4f6f8;color:#162431;font:14px system-ui,sans-serif}header,.bar,.main{max-width:1800px;margin:auto;padding:14px 2%}header{background:white;max-width:none;padding-left:calc((100% - 1800px)/2 + 2%);border-bottom:1px solid #d5dce2}h1{font-size:20px;margin:0 0 4px}.note{color:#596575}.bar,.controls,.decision{display:flex;gap:7px;align-items:center;flex-wrap:wrap}.bar button,.decision button,.controls button{padding:7px 10px;border:1px solid #aebbc6;background:white;border-radius:5px;cursor:pointer}.bar button.active,.controls button.active{background:#d7f0ed;border-color:#087c72}.progress{margin-left:auto;color:#52616d}.card{background:white;border:1px solid #d5dce2;border-radius:8px;padding:13px}.title{display:flex;justify-content:space-between;gap:12px}.controls{padding:9px;background:#f7f9fa;border-radius:6px;margin-top:10px}.controls label{white-space:nowrap}.pages{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px}.side h2{font-size:15px;margin:0 0 7px}.viewport{height:70vh;min-height:500px;overflow:auto;position:relative;background:#dce4e9;border:1px solid #aebbc6;touch-action:none}.viewport.pan{cursor:grab}.viewport.pan.dragging{cursor:grabbing}.sheet{width:100%;transform-origin:top left;will-change:transform;padding:0 0 20px}.page{position:relative;background:#e5eaee;margin-bottom:10px;overflow:hidden;box-shadow:0 1px 3px #788896}.page img{display:block;width:100%;height:auto}.block{position:absolute;border:2px solid #1677c8;background:#1677c822;cursor:pointer}.block.TABLE{border-color:#e07b00;background:#e07b0022}.block.GRAPHIC{border-color:#8d43b6;background:#8d43b622}.block.selected{outline:3px solid #e62d86;background:#e62d8640}.link{padding:8px;background:#edf7f6;border-left:4px solid #087c72;margin:8px 0}.detail{margin-top:10px;background:#f7f9fa;padding:10px}.detail pre{white-space:pre-wrap;max-height:240px;overflow:auto}.detail table{border-collapse:collapse;width:100%}.detail td,.detail th{border:1px solid #ccd5dc;padding:4px}.crop{max-width:100%;max-height:300px;cursor:zoom-in}.manual{background:#fff7e7;border-left:4px solid #e07b00;padding:8px;margin-top:10px}.mapping-workspace{display:grid;grid-template-columns:1fr 1fr;gap:12px;position:relative;overflow:hidden;isolation:isolate}.link-layer{position:absolute;inset:0;width:100%;height:100%;pointer-events:none;z-index:5;overflow:hidden}.link-hit{stroke:transparent;stroke-width:16;fill:none;pointer-events:stroke;cursor:pointer}.link-line.inherited{stroke:#7a8a96;stroke-width:2;opacity:.55}.link-line.human{stroke:#0b6e4f}.link-line.selected{stroke:#e62d86;stroke-width:4;opacity:1}.block.link-end{outline:3px solid #087c72;background:#087c7240}.block.member{box-shadow:inset 0 0 0 2px #7a8a96}.banner{padding:8px;background:#eef2f6;border-left:4px solid #7a8a96;margin:8px 0;color:#44515c}.link-editor{display:none;background:#f3eef8;border-left:4px solid #75439c;padding:10px;margin-top:10px}.link-editor.open{display:block}.side-labels{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px}.side-labels h2{font-size:15px;margin:0 0 7px}.link-line{stroke:#087c72;stroke-width:3;stroke-dasharray:8 6;fill:none;opacity:.9}.link-line.all{stroke:#75439c;stroke-width:2;opacity:.65}@media(max-width:900px){.pages{grid-template-columns:1fr}.progress{margin-left:0}.viewport{height:58vh}}</style>
<header><h1>Проверка OLD ↔ NEW связей · V1.2</h1><div class=note>Research UI · BlockLink editing · frozen Semantic Mapping V3 · никаких model calls</div></header><div class=bar><button id=A>АР1</button><button id=B>ИОС4.2</button><button data-q=UNREVIEWED>Не проверено</button><button data-q=HUMAN_CONFIRMED>Подтверждено</button><button data-q=HUMAN_REJECTED>Отклонено</button><button data-q=HUMAN_UNCERTAIN>Не уверен</button><span class=progress id=progress></span></div><main class=main><div id=app></div></main><dialog id=zoom><button onclick="zoom.close()">Закрыть</button><img></dialog><script>
let pair='A',queue='UNREVIEWED',data,history=[],linkEvents=[],selected={OLD:[],NEW:[]},panMode=null,showAll=false,editMode=false,selectedLinkId=null,reassignSide=null,view={OLD:{x:0,y:0,z:1,locked:false},NEW:{x:0,y:0,z:1,locked:false}};const $=s=>document.querySelector(s),esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
async function load(){data=await fetch('/data/UI_DATA_PAIR_'+pair+'.json').then(r=>r.json());history=await fetch('/reviews?pair='+pair).then(r=>r.json());linkEvents=await fetch('/block_links?pair='+pair).then(r=>r.json());selected={OLD:[],NEW:[]};selectedLinkId=null;reassignSide=null;render()}
function latest(r){let x=history.filter(h=>h.region_id===r.id);return x.length?x[x.length-1]:null}function stat(r){return latest(r)?.status||'UNREVIEWED'}function linked(r){let l=latest(r);return {OLD:selected.OLD.length?selected.OLD:(l?.old_block_ids||r.old_blocks.map(b=>b.id)),NEW:selected.NEW.length?selected.NEW:(l?.new_block_ids||r.new_blocks.map(b=>b.id))}}
function memberIds(r){return {OLD:r.old_blocks.map(b=>b.id),NEW:r.new_blocks.map(b=>b.id)}}
function findBlock(r,side,id){return [...r.old_blocks,...r.new_blocks].find(x=>x.id===id)||r.pages[side].flatMap(p=>p.blocks).find(x=>x.id===id)}
function baseProposedLinks(r){const olds=r.old_blocks.map(b=>b.id),news=r.new_blocks.map(b=>b.id);if(olds.length===1&&news.length===1)return[{link_id:`ai:${r.id}:${olds[0]}:${news[0]}`,old_block_id:olds[0],new_block_id:news[0],source:'AI_PROPOSED',membership_kind:'EXPLICIT_1_1'}];if(olds.length===1)return news.map(n=>({link_id:`ai:${r.id}:${olds[0]}:${n}`,old_block_id:olds[0],new_block_id:n,source:'AI_PROPOSED',membership_kind:'INHERITED_GROUP_1_N'}));if(news.length===1)return olds.map(o=>({link_id:`ai:${r.id}:${o}:${news[0]}`,old_block_id:o,new_block_id:news[0],source:'AI_PROPOSED',membership_kind:'INHERITED_GROUP_N_1'}));return[]}
function effectiveLinks(r){let links=baseProposedLinks(r).map(l=>({...l}));for(const e of linkEvents.filter(x=>x.region_id===r.id)){if(e.event_type==='ADD_BLOCK_LINK')links.push({link_id:e.link_id,old_block_id:e.old_block_id,new_block_id:e.new_block_id,source:'HUMAN_MANUAL',membership_kind:'HUMAN_DEFINED'});else if(e.event_type==='DELETE_BLOCK_LINK')links=links.filter(l=>l.link_id!==e.link_id&&!(l.old_block_id===e.old_block_id&&l.new_block_id===e.new_block_id));else if(e.event_type==='REASSIGN_BLOCK_LINK')links=links.map(l=>(l.link_id===e.link_id||(l.old_block_id===e.previous_old_block_id&&l.new_block_id===e.previous_new_block_id))?{link_id:e.link_id,old_block_id:e.old_block_id,new_block_id:e.new_block_id,source:'HUMAN_MANUAL',membership_kind:'HUMAN_DEFINED'}:l)}const seen=new Set();return links.filter(l=>{const k=l.old_block_id+'|'+l.new_block_id;if(seen.has(k))return false;seen.add(k);return true})}
function visibleLinks(r){const all=effectiveLinks(r);if(showAll)return all;const sel=[...selected.OLD,...selected.NEW];if(!sel.length&&selectedLinkId)return all.filter(l=>l.link_id===selectedLinkId);if(!sel.length)return all;return all.filter(l=>selected.OLD.includes(l.old_block_id)||selected.NEW.includes(l.new_block_id)||l.link_id===selectedLinkId)}
function overlay(p,b,r){const mem=memberIds(r),selLink=effectiveLinks(r).find(l=>l.link_id===selectedLinkId);const isMember=mem[p.side].includes(b.id);const isSelected=selected[p.side].includes(b.id);const isEnd=!!selLink&&((p.side==='OLD'&&selLink.old_block_id===b.id)||(p.side==='NEW'&&selLink.new_block_id===b.id));let [x,y,w,h]=b.bbox;return `<button class="block ${b.type} ${isSelected?'selected':''} ${isEnd?'link-end':''} ${isMember&&!isSelected&&!isEnd?'member':''}" title="${esc(b.id)} · ${b.type}" data-side="${p.side}" data-id="${b.id}" style="left:${x*100}%;top:${y*100}%;width:${(w-x)*100}%;height:${(h-y)*100}%"></button>`}
function page(p,r){return `<div class=page><img src="/${p.image}">${p.blocks.map(b=>overlay(p,b,r)).join('')}</div>`}
function showDetail(side,id,r){let b=findBlock(r,side,id);let table=b.tables?.length?'<table>'+b.tables[0].map(row=>'<tr>'+row.map(cell=>'<td>'+esc(cell)+'</td>').join('')+'</tr>').join('')+'</table>':'';return `<div class=detail><b>${esc(b.id)}</b> · ${b.type} · стр. ${b.page??'—'} · bbox ${b.bbox.map(x=>x.toFixed(3)).join(', ')}${b.type==='GRAPHIC'&&b.crop?`<br><img class=crop src="/${b.crop}" onclick="zoom.querySelector('img').src=this.src;zoom.showModal()">`:''}${b.type==='TABLE'?table:`<pre>${esc(b.structured_md||'Структурированное содержимое отсутствует.')}</pre>`}</div>`}
function transform(side){let v=view[side],s=$('#sheet-'+side);if(s)s.style.transform=`translate(${v.x}px,${v.y}px) scale(${v.z})`;requestAnimationFrame(drawLines)}
function controls(side){let v=view[side];return `<b>${side}</b><button data-pan="${side}" class="${panMode===side?'active':''}">${panMode===side?'Перемещение включено':'Переместить лист'}</button><button data-reset="${side}">Сбросить положение</button><button data-lock="${side}" class="${v.locked?'active':''}">${v.locked?'Положение зафиксировано':'Зафиксировать положение'}</button><label>Масштаб <input data-zoom="${side}" type=range min=.6 max=1.8 step=.1 value=${v.z}></label>`}
function render(){for(let x of ['A','B'])$('#'+x).classList.toggle('active',pair===x);let reviewed=data.regions.filter(r=>stat(r)!=='UNREVIEWED').length;$('#progress').textContent=`Проверено ${reviewed} / ${data.regions.length}`;let r=data.regions.find(x=>stat(x)===queue)||data.regions[0];if(!r){$('#app').innerHTML='<p>В этой очереди нет связей.</p>';return}let l=latest(r),links=effectiveLinks(r);$('#app').innerHTML=`<section class=card><div class=title><div><b>${r.id}</b> · ${esc(r.domain)}<p>${esc(r.scope)}</p></div><small>AI-proposed group · confidence ${r.confidence} · BlockLinks: ${links.length}</small></div><div class=link>Group membership · OLD (${r.old_blocks.map(x=>esc(x.id)).join(', ')}) ↔ NEW (${r.new_blocks.map(x=>esc(x.id)).join(', ')})</div><div class=controls>${controls('OLD')} ${controls('NEW')}<button id=reset-both>Сбросить оба</button><button id=all-links class="${showAll?'active':''}">Показать все связи: ${showAll?'вкл.':'выкл.'}</button><button id=edit-links class="${editMode?'active':''}">Редактировать связи: ${editMode?'вкл.':'выкл.'}</button></div><div class=side-labels><h2>OLD</h2><h2>NEW</h2></div><div class=mapping-workspace id=mapping-workspace><div class="viewport ${panMode==='OLD'?'pan':''}" id=view-OLD><div class=sheet id=sheet-OLD>${r.pages.OLD.map(p=>page({...p,side:'OLD'},r)).join('')}</div></div><div class="viewport ${panMode==='NEW'?'pan':''}" id=view-NEW><div class=sheet id=sheet-NEW>${r.pages.NEW.map(p=>page({...p,side:'NEW'},r)).join('')}</div></div><svg class=link-layer id=links aria-hidden=true></svg></div><div class=manual>Режим: ${editMode?'редактирование BlockLink':'просмотр'}. Выделены: OLD ${selected.OLD.length}, NEW ${selected.NEW.length}. <button id=add-spoke>Добавить BlockLink(и) 1→N / N→1</button> <button id=add-one>Добавить одну пару 1→1</button> <button id=add-cartesian>Создать все пары (N×N, явно)</button></div><div class="link-editor ${editMode||selectedLinkId?'open':''}" id=link-editor></div><div class=decision><textarea id=comment placeholder="Комментарий (необязательно)"></textarea><button data-status=HUMAN_CONFIRMED>ПОДТВЕРДИТЬ</button><button data-status=HUMAN_REJECTED>ОТКЛОНИТЬ</button><button data-status=HUMAN_UNCERTAIN>НЕ УВЕРЕН</button></div>${l?`<p>Последнее решение: <b>${l.status}</b> · ${esc(l.timestamp)}</p>`:''}<div id=details></div></section>`;for(let side of ['OLD','NEW']){transform(side);bindPan(side);$('#view-'+side).addEventListener('scroll',()=>requestAnimationFrame(drawLines));}document.querySelectorAll('.block').forEach(b=>b.onclick=async e=>{if(panMode)return;let s=b.dataset.side,id=b.dataset.id;if(editMode&&reassignSide&&selectedLinkId&&s===reassignSide){await reassignLink(r,s,id);return}selected[s]=selected[s].includes(id)?selected[s].filter(x=>x!==id):[...selected[s],id];render();$('#details').innerHTML=showDetail(s,id,r)});document.querySelectorAll('[data-status]').forEach(b=>b.onclick=()=>save(r,b.dataset.status));document.querySelectorAll('[data-pan]').forEach(b=>b.onclick=()=>{panMode=panMode===b.dataset.pan?null:b.dataset.pan;render()});document.querySelectorAll('[data-reset]').forEach(b=>{b.onclick=()=>{let v=view[b.dataset.reset];v.x=v.y=0;v.z=1;render()}});document.querySelectorAll('[data-lock]').forEach(b=>b.onclick=()=>{view[b.dataset.lock].locked=!view[b.dataset.lock].locked;render()});document.querySelectorAll('[data-zoom]').forEach(i=>i.oninput=()=>{view[i.dataset.zoom].z=+i.value;transform(i.dataset.zoom)});$('#reset-both').onclick=()=>{for(let s of ['OLD','NEW'])Object.assign(view[s],{x:0,y:0,z:1});render()};$('#all-links').onclick=()=>{showAll=!showAll;render()};$('#edit-links').onclick=()=>{editMode=!editMode;if(!editMode)reassignSide=null;panMode=null;render()};
const _as=$('#add-spoke'),_ao=$('#add-one'),_ac=$('#add-cartesian');
if(_as)_as.onclick=()=>addLinks(r,'spoke');
if(_ao)_ao.onclick=()=>addLinks(r,'one');
if(_ac)_ac.onclick=()=>addLinks(r,'cartesian');
(function fillLinkEditor(r){const box=$('#link-editor');if(!box)return;const sel=effectiveLinks(r).find(l=>l.link_id===selectedLinkId);if(!sel){box.innerHTML='<b>Редактор BlockLink</b><p>Кликните пунктирную линию или создайте связь из выделения.</p>';return}const ob=findBlock(r,'OLD',sel.old_block_id),nb=findBlock(r,'NEW',sel.new_block_id);box.innerHTML='<b>Выбранный BlockLink</b><div>OLD: <code>'+esc(sel.old_block_id)+'</code> · '+esc(ob&&ob.type||'?')+' · стр. '+(ob&&ob.page!=null?ob.page:'—')+'<br>NEW: <code>'+esc(sel.new_block_id)+'</code> · '+esc(nb&&nb.type||'?')+' · стр. '+(nb&&nb.page!=null?nb.page:'—')+'<br>source: '+esc(sel.source)+' · '+esc(sel.membership_kind||'')+'</div><div class=link-editor-actions><button id=reassign-old class="'+(reassignSide==='OLD'?'active':'')+'">Переназначить OLD</button><button id=reassign-new class="'+(reassignSide==='NEW'?'active':'')+'">Переназначить NEW</button><button id=delete-link>Удалить связь</button><button id=clear-link-sel>Снять выбор линии</button></div>'+(reassignSide?('<p>Кликните блок на стороне '+reassignSide+'.</p>'):'');const ro=$('#reassign-old'),rn=$('#reassign-new'),dl=$('#delete-link'),cl=$('#clear-link-sel');if(ro)ro.onclick=()=>{reassignSide=reassignSide==='OLD'?null:'OLD';render()};if(rn)rn.onclick=()=>{reassignSide=reassignSide==='NEW'?null:'NEW';render()};if(dl)dl.onclick=()=>deleteLink(r);if(cl)cl.onclick=()=>{selectedLinkId=null;reassignSide=null;render()}})(r);
requestAnimationFrame(drawLines)}
function bindPan(side){let box=$('#view-'+side),drag;box.onpointerdown=e=>{if(panMode!==side||view[side].locked||e.target.closest('.block'))return;drag={x:e.clientX,y:e.clientY,ox:view[side].x,oy:view[side].y};box.setPointerCapture(e.pointerId);box.classList.add('dragging')};box.onpointermove=e=>{if(!drag)return;view[side].x=drag.ox+e.clientX-drag.x;view[side].y=drag.oy+e.clientY-drag.y;transform(side)};box.onpointerup=box.onpointercancel=()=>{drag=null;box.classList.remove('dragging')}}
function workspaceOrigin(){const w=$('#mapping-workspace');if(!w)return null;const r=w.getBoundingClientRect();return {left:r.left,top:r.top,width:r.width,height:r.height}}
function center(side,id){const e=document.querySelector(`.block[data-side="${side}"][data-id="${id}"]`);const origin=workspaceOrigin();if(!e||!origin)return null;const r=e.getBoundingClientRect();return {x:r.left+r.width/2-origin.left,y:r.top+r.height/2-origin.top}}
function pathPair(a,b,link,selected){const cls=['link-line',link.source==='HUMAN_MANUAL'?'human':'',String(link.membership_kind||'').startsWith('INHERITED_GROUP')?'inherited':'',selected?'selected':''].filter(Boolean).join(' ');const d=`M ${a.x} ${a.y} L ${b.x} ${b.y}`;return `<path class="link-hit" data-link-id="${esc(link.link_id)}" d="${d}"/><path class="${cls}" data-link-id="${esc(link.link_id)}" d="${d}"/>`}
function drawLines(){const r=data?.regions.find(x=>stat(x)===queue)||data?.regions[0];const svg=$('#links');const origin=workspaceOrigin();if(!r||!svg||!origin)return;svg.setAttribute('viewBox',`0 0 ${origin.width} ${origin.height}`);svg.setAttribute('width',String(origin.width));svg.setAttribute('height',String(origin.height));let html='';for(const link of visibleLinks(r)){const a=center('OLD',link.old_block_id),b=center('NEW',link.new_block_id);if(!a||!b)continue;html+=pathPair(a,b,link,link.link_id===selectedLinkId)}svg.innerHTML=html;svg.querySelectorAll('.link-hit').forEach(p=>{p.addEventListener('click',ev=>{ev.stopPropagation();selectedLinkId=p.getAttribute('data-link-id');editMode=true;reassignSide=null;render()})})}
async function saveLinkEvent(payload){const response=await fetch('/block_links',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)});let body=null;try{body=await response.json()}catch(e){body=null}if(!response.ok){const code=body&&body.error||'ERROR';if(code==='BLOCK_LINK_ALREADY_EXISTS')alert('Такая связь уже существует');else if(code==='NO_CHANGE'){}else if(code==='BLOCK_LINK_NOT_FOUND')alert('Исходная связь не найдена');else alert('Не удалось сохранить BlockLink событие.');return {ok:false,error:code,body}}if(body&&body.error==='NO_CHANGE')return {ok:true,noop:true,body};if(body&&body.event_type){linkEvents.push(body);return {ok:true,row:body}}return {ok:false,error:'BAD_RESPONSE',body}}
async function addLinks(r,mode){if(!editMode)return alert('Включите «Редактировать связи».');const olds=selected.OLD,news=selected.NEW;let pairs=[];if(mode==='one'){if(olds.length!==1||news.length!==1)return alert('Для 1→1 выберите ровно один OLD и один NEW.');pairs=[[olds[0],news[0]]]}else if(mode==='spoke'){if(olds.length===1&&news.length>=1)pairs=news.map(n=>[olds[0],n]);else if(news.length===1&&olds.length>=1)pairs=olds.map(o=>[o,news[0]]);else return alert('Для 1→N / N→1 одна сторона должна содержать ровно 1 блок.')}else if(mode==='cartesian'){if(!olds.length||!news.length)return;if(!confirm('Явно создать декартово произведение BlockLink?'))return;for(const o of olds)for(const n of news)pairs.push([o,n])}const existing=new Set(effectiveLinks(r).map(l=>l.old_block_id+'|'+l.new_block_id));let rejectedDup=false;for(const [o,n] of pairs){if(existing.has(o+'|'+n)){rejectedDup=true;continue}const link_id='human:'+crypto.randomUUID();const res=await saveLinkEvent({event_type:'ADD_BLOCK_LINK',pair_key:data.pair_key,region_id:r.id,link_id,old_block_id:o,new_block_id:n,previous_old_block_id:null,previous_new_block_id:null,comment:($('#comment')&&$('#comment').value)||''});if(res&&res.ok&&res.row)existing.add(o+'|'+n);if(res&&res.error==='BLOCK_LINK_ALREADY_EXISTS')rejectedDup=true}if(rejectedDup)alert('Такая связь уже существует');selected={OLD:[],NEW:[]};render()}
async function deleteLink(r){const link=effectiveLinks(r).find(l=>l.link_id===selectedLinkId);if(!link)return;await saveLinkEvent({event_type:'DELETE_BLOCK_LINK',pair_key:data.pair_key,region_id:r.id,link_id:link.link_id,old_block_id:link.old_block_id,new_block_id:link.new_block_id,previous_old_block_id:link.old_block_id,previous_new_block_id:link.new_block_id,comment:($('#comment')&&$('#comment').value)||''});selectedLinkId=null;reassignSide=null;render()}
async function reassignLink(r,side,newId){const link=effectiveLinks(r).find(l=>l.link_id===selectedLinkId);if(!link)return;const prevO=link.old_block_id,prevN=link.new_block_id;const nextO=side==='OLD'?newId:prevO,nextN=side==='NEW'?newId:prevN;if(nextO===prevO&&nextN===prevN){reassignSide=null;render();return}const clash=effectiveLinks(r).some(l=>l.old_block_id===nextO&&l.new_block_id===nextN&&l.link_id!==link.link_id);if(clash){alert('Такая связь уже существует');reassignSide=null;render();return}const new_link_id='human:'+crypto.randomUUID();const res=await saveLinkEvent({event_type:'REASSIGN_BLOCK_LINK',pair_key:data.pair_key,region_id:r.id,link_id:new_link_id,old_block_id:nextO,new_block_id:nextN,previous_old_block_id:prevO,previous_new_block_id:prevN,previous_link_id:link.link_id,comment:($('#comment')&&$('#comment').value)||''});if(res&&res.ok&&res.row)selectedLinkId=new_link_id;reassignSide=null;render()}
async function save(r,status){let old=selected.OLD.length?selected.OLD:r.old_blocks.map(b=>b.id),nw=selected.NEW.length?selected.NEW:r.new_blocks.map(b=>b.id);if(!old.length||!nw.length)return alert('Выберите блоки с обеих сторон.');let prior=latest(r);let response=await fetch('/reviews',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({pair_key:data.pair_key,region_id:r.id,old_block_ids:old,new_block_ids:nw,status,comment:$('#comment').value,previous_review_id:prior?.review_id||null})});if(!response.ok)return alert('Не удалось сохранить решение.');history.push(await response.json());selected={OLD:[],NEW:[]};render()}
$('#A').onclick=()=>{pair='A';load()};$('#B').onclick=()=>{pair='B';load()};document.querySelectorAll('[data-q]').forEach(b=>b.onclick=()=>{queue=b.dataset.q;selectedLinkId=null;render()});window.addEventListener('resize',()=>requestAnimationFrame(drawLines));load()</script>'''


def _pair_data(pair: str) -> dict:
    return json.loads((OUT / f'UI_DATA_PAIR_{pair}.json').read_text(encoding='utf-8'))


def _region(pair: str, region_id: str):
    data = _pair_data(pair)
    for r in data['regions']:
        if r['id'] == region_id:
            return data, r
    return data, None


def _base_proposed_links(region: dict) -> list[dict]:
    olds = [b['id'] for b in region['old_blocks']]
    news = [b['id'] for b in region['new_blocks']]
    if len(olds) == 1 and len(news) == 1:
        return [{'link_id': f"ai:{region['id']}:{olds[0]}:{news[0]}", 'old_block_id': olds[0], 'new_block_id': news[0]}]
    if len(olds) == 1:
        return [{'link_id': f"ai:{region['id']}:{olds[0]}:{n}", 'old_block_id': olds[0], 'new_block_id': n} for n in news]
    if len(news) == 1:
        return [{'link_id': f"ai:{region['id']}:{o}:{news[0]}", 'old_block_id': o, 'new_block_id': news[0]} for o in olds]
    return []


def _effective_links(region: dict, events: list[dict]) -> list[dict]:
    links = [dict(x) for x in _base_proposed_links(region)]
    for e in events:
        if e.get('region_id') != region['id']:
            continue
        et = e.get('event_type')
        if et == 'ADD_BLOCK_LINK':
            links.append({'link_id': e['link_id'], 'old_block_id': e['old_block_id'], 'new_block_id': e['new_block_id']})
        elif et == 'DELETE_BLOCK_LINK':
            links = [l for l in links if l['link_id'] != e['link_id'] and not (l['old_block_id'] == e['old_block_id'] and l['new_block_id'] == e['new_block_id'])]
        elif et == 'REASSIGN_BLOCK_LINK':
            links = [
                {'link_id': e['link_id'], 'old_block_id': e['old_block_id'], 'new_block_id': e['new_block_id']}
                if (l['link_id'] == e.get('previous_link_id') or l['link_id'] == e['link_id'] or (l['old_block_id'] == e.get('previous_old_block_id') and l['new_block_id'] == e.get('previous_new_block_id')))
                else l
                for l in links
            ]
    seen = set()
    out = []
    for l in links:
        k = (l['old_block_id'], l['new_block_id'])
        if k in seen:
            continue
        seen.add(k)
        out.append(l)
    return out


def _read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(x) for x in path.read_text(encoding='utf-8').splitlines() if x.strip()]


def serve(port: int) -> None:
    if not (OUT / 'UI_SNAPSHOT_MANIFEST.json').is_file():
        raise FileNotFoundError('Run build first')
    reviews = OUT / 'human_mapping_reviews' / 'reviews.jsonl'
    # LIVE human BlockLink store (smoke archive is separate under smoke/)
    link_edits = OUT / 'human_mapping_reviews' / 'human_block_link_edits.jsonl'

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_):
            pass

        def reply(self, raw: bytes, typ='application/json', status=200):
            self.send_response(status)
            self.send_header('Content-Type', typ)
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def reply_err(self, code: str, status: int = 400):
            return self.reply(json.dumps({'error': code, 'ok': False}, ensure_ascii=False).encode(), status=status)

        def do_GET(self):
            url = urlsplit(self.path)
            path = unquote(url.path)
            if path == '/':
                return self.reply(HTML.encode(), 'text/html; charset=utf-8')
            if path == '/reviews':
                pair = url.query.split('pair=', 1)[-1] if 'pair=' in url.query else ''
                rows = _read_jsonl(reviews)
                return self.reply(json.dumps([x for x in rows if x.get('pair') == pair], ensure_ascii=False).encode())
            if path == '/block_links':
                pair = url.query.split('pair=', 1)[-1] if 'pair=' in url.query else ''
                rows = _read_jsonl(link_edits)
                return self.reply(json.dumps([x for x in rows if x.get('pair') == pair], ensure_ascii=False).encode())
            if path.startswith('/data/') or path.startswith('/assets/'):
                file = (OUT / path.lstrip('/').removeprefix('data/')).resolve()
                if file.is_file() and file.is_relative_to(OUT):
                    return self.reply(file.read_bytes(), 'image/png' if file.suffix == '.png' else 'application/json')
            self.reply(b'Not found', 'text/plain', 404)

        def do_POST(self):
            route = urlsplit(self.path).path
            try:
                raw = json.loads(self.rfile.read(int(self.headers.get('Content-Length', '0'))))
            except json.JSONDecodeError:
                return self.reply_err('BAD_JSON')

            if route == '/reviews':
                try:
                    status = raw['status']
                    assert status in {'HUMAN_CONFIRMED', 'HUMAN_REJECTED', 'HUMAN_UNCERTAIN'}
                    assert raw['pair_key'] in {x['key'] for x in PAIRS.values()} and raw['old_block_ids'] and raw['new_block_ids']
                except (KeyError, ValueError, AssertionError):
                    return self.reply(b'Bad review', 'text/plain', 400)
                pair = next(k for k, v in PAIRS.items() if v['key'] == raw['pair_key'])
                row = {
                    'review_id': str(uuid.uuid4()), 'pair': pair, 'pair_key': raw['pair_key'], 'region_id': raw['region_id'],
                    'old_block_ids': raw['old_block_ids'], 'new_block_ids': raw['new_block_ids'], 'status': status,
                    'timestamp': datetime.now(timezone.utc).isoformat(), 'comment': raw.get('comment', ''),
                    'reviewer_source': 'HUMAN', 'supersedes_review_id': raw.get('previous_review_id'),
                }
                with reviews.open('a', encoding='utf-8') as f:
                    f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + '\n')
                return self.reply(json.dumps(row, ensure_ascii=False).encode())

            if route == '/block_links':
                try:
                    event_type = raw['event_type']
                    assert event_type in {'ADD_BLOCK_LINK', 'DELETE_BLOCK_LINK', 'REASSIGN_BLOCK_LINK'}
                    pair_key = raw['pair_key']
                    region_id = raw['region_id']
                    link_id = raw['link_id']
                    old_block_id = raw['old_block_id']
                    new_block_id = raw['new_block_id']
                except (KeyError, ValueError, AssertionError):
                    return self.reply_err('BAD_BLOCK_LINK_EVENT')

                pair = next((k for k, v in PAIRS.items() if v['key'] == pair_key), None)
                if pair is None:
                    return self.reply_err('PAIR_NOT_FOUND')
                _data, region = _region(pair, region_id)
                if region is None:
                    return self.reply_err('REGION_NOT_FOUND')

                old_ids = {b['id'] for b in region['old_blocks']}
                new_ids = {b['id'] for b in region['new_blocks']}
                events = [e for e in _read_jsonl(link_edits) if e.get('pair') == pair and e.get('region_id') == region_id]
                effective = _effective_links(region, events)
                effective_pairs = {(l['old_block_id'], l['new_block_id']) for l in effective}

                if event_type in {'ADD_BLOCK_LINK', 'REASSIGN_BLOCK_LINK'}:
                    if old_block_id in new_ids and old_block_id not in old_ids:
                        return self.reply_err('WRONG_BLOCK_SIDE')
                    if new_block_id in old_ids and new_block_id not in new_ids:
                        return self.reply_err('WRONG_BLOCK_SIDE')
                    if old_block_id not in old_ids:
                        return self.reply_err('OLD_BLOCK_NOT_IN_REGION')
                    if new_block_id not in new_ids:
                        return self.reply_err('NEW_BLOCK_NOT_IN_REGION')

                if event_type == 'REASSIGN_BLOCK_LINK':
                    prev_link_id = raw.get('previous_link_id')
                    prev_o = raw.get('previous_old_block_id')
                    prev_n = raw.get('previous_new_block_id')
                    source_exists = any(
                        (prev_link_id and l['link_id'] == prev_link_id)
                        or (prev_o is not None and prev_n is not None and l['old_block_id'] == prev_o and l['new_block_id'] == prev_n)
                        for l in effective
                    )
                    if not source_exists:
                        return self.reply_err('BLOCK_LINK_NOT_FOUND')
                    if prev_o == old_block_id and prev_n == new_block_id:
                        return self.reply(json.dumps({'error': 'NO_CHANGE', 'ok': True}, ensure_ascii=False).encode())
                    if (old_block_id, new_block_id) in effective_pairs:
                        return self.reply_err('BLOCK_LINK_ALREADY_EXISTS')

                if event_type == 'ADD_BLOCK_LINK':
                    if (old_block_id, new_block_id) in effective_pairs:
                        return self.reply_err('BLOCK_LINK_ALREADY_EXISTS')

                if event_type == 'DELETE_BLOCK_LINK':
                    exists = any(
                        (l['link_id'] == link_id) or (l['old_block_id'] == old_block_id and l['new_block_id'] == new_block_id)
                        for l in effective
                    )
                    if not exists:
                        return self.reply_err('BLOCK_LINK_NOT_FOUND')

                row = {
                    'event_id': str(uuid.uuid4()), 'event_type': event_type, 'pair': pair, 'pair_key': pair_key,
                    'region_id': region_id, 'link_id': link_id, 'old_block_id': old_block_id, 'new_block_id': new_block_id,
                    'previous_old_block_id': raw.get('previous_old_block_id'),
                    'previous_new_block_id': raw.get('previous_new_block_id'),
                    'previous_link_id': raw.get('previous_link_id'),
                    'timestamp': datetime.now(timezone.utc).isoformat(), 'comment': raw.get('comment', ''),
                    'reviewer_source': 'HUMAN',
                }
                with link_edits.open('a', encoding='utf-8') as f:
                    f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + '\n')
                return self.reply(json.dumps(row, ensure_ascii=False).encode())

            self.reply(b'Not found', 'text/plain', 404)

    print(f'RESEARCH UI: http://127.0.0.1:{port}/', flush=True)
    ThreadingHTTPServer(('127.0.0.1', port), Handler).serve_forever()


if __name__ == '__main__':
    p=argparse.ArgumentParser(); s=p.add_subparsers(dest='command',required=True); s.add_parser('build'); x=s.add_parser('serve'); x.add_argument('--port',type=int,default=8777); a=p.parse_args()
    print(build()) if a.command=='build' else serve(a.port)
