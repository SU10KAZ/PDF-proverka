"""Production Human Mapping API + HTML (V1.2.4 REASSIGN contract).

Every route is scoped to one comparison: ``object_id`` + comparison/pair id.
Both are validated (strict safe-ID, HTTP 400) BEFORE any filesystem access.
The sealed АР1/ИОС4.2 fixtures stay reachable only as compatibility aliases
of the fixture object; they never control an arbitrary comparison.

A REAL pair without its own published HM data whose two source PDFs are
byte-identical to an approved sealed fixture (``fixture_binding``) opens that
fixture's frozen mapping as initial read-only data; its reviews and BlockLink
edits are stored under the real pair, never in the fixture stores.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse

from backend.app.services.human_mapping_production import fixture_binding, storage
from backend.app.services.human_mapping_production.validation import (
    BlockLinkValidationError,
    validate_block_link_event,
)

APP_ROOT = Path(__file__).resolve().parents[2]  # backend/app
FIXTURES = APP_ROOT / "data" / "human_mapping_fixtures"
FIXTURE_OBJECT = "4f3e5916"
OBJECT_DEFAULT = FIXTURE_OBJECT  # backward-compatible name
# Compatibility aliases for sealed AR1/IOS4.2 fixtures of FIXTURE_OBJECT only.
PAIR_KEYS = {
    "A": "ad0a31a342a666082f2ef66a",
    "B": "caea6d2810c334ec0368de8e",
}
PAIR_BY_KEY = {v: k for k, v in PAIR_KEYS.items()}
FIXTURE_LABELS = {"A": "АР1", "B": "ИОС4.2"}

router = APIRouter(tags=["Human Mapping"])
api_router = APIRouter(prefix="/api/human-mapping/objects/{object_id}", tags=["Human Mapping"])

HTML_PAGE = r'''<!doctype html><meta charset="utf-8"><title>Human Mapping Verification · V1.2</title><style>
*{box-sizing:border-box}body{margin:0;background:#f4f6f8;color:#162431;font:14px system-ui,sans-serif}header,.bar,.main{max-width:1800px;margin:auto;padding:14px 2%}header{background:white;max-width:none;padding-left:calc((100% - 1800px)/2 + 2%);border-bottom:1px solid #d5dce2}h1{font-size:20px;margin:0 0 4px}.note{color:#596575}.bar,.controls,.decision{display:flex;gap:7px;align-items:center;flex-wrap:wrap}.bar button,.decision button,.controls button{padding:7px 10px;border:1px solid #aebbc6;background:white;border-radius:5px;cursor:pointer}.bar button.active,.controls button.active{background:#d7f0ed;border-color:#087c72}.progress{margin-left:auto;color:#52616d}.card{background:white;border:1px solid #d5dce2;border-radius:8px;padding:13px}.title{display:flex;justify-content:space-between;gap:12px}.controls{padding:9px;background:#f7f9fa;border-radius:6px;margin-top:10px}.controls label{white-space:nowrap}.pages{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px}.side h2{font-size:15px;margin:0 0 7px}.viewport{height:70vh;min-height:500px;overflow:auto;position:relative;background:#dce4e9;border:1px solid #aebbc6;touch-action:none}.viewport.pan{cursor:grab}.viewport.pan.dragging{cursor:grabbing}.sheet{width:100%;transform-origin:top left;will-change:transform;padding:0 0 20px}.page{position:relative;background:#e5eaee;margin-bottom:10px;overflow:hidden;box-shadow:0 1px 3px #788896}.page img{display:block;width:100%;height:auto}.block{position:absolute;border:2px solid #1677c8;background:#1677c822;cursor:pointer}.block.TABLE{border-color:#e07b00;background:#e07b0022}.block.GRAPHIC{border-color:#8d43b6;background:#8d43b622}.block.selected{outline:3px solid #e62d86;background:#e62d8640}.link{padding:8px;background:#edf7f6;border-left:4px solid #087c72;margin:8px 0}.detail{margin-top:10px;background:#f7f9fa;padding:10px}.detail pre{white-space:pre-wrap;max-height:240px;overflow:auto}.detail table{border-collapse:collapse;width:100%}.detail td,.detail th{border:1px solid #ccd5dc;padding:4px}.crop{max-width:100%;max-height:300px;cursor:zoom-in}.manual{background:#fff7e7;border-left:4px solid #e07b00;padding:8px;margin-top:10px}.mapping-workspace{display:grid;grid-template-columns:1fr 1fr;gap:12px;position:relative;overflow:hidden;isolation:isolate}.link-layer{position:absolute;inset:0;width:100%;height:100%;pointer-events:none;z-index:5;overflow:hidden}.link-hit{stroke:transparent;stroke-width:16;fill:none;pointer-events:stroke;cursor:pointer}.link-line.inherited{stroke:#7a8a96;stroke-width:2;opacity:.55}.link-line.human{stroke:#0b6e4f}.link-line.selected{stroke:#e62d86;stroke-width:4;opacity:1}.block.link-end{outline:3px solid #087c72;background:#087c7240}.block.member{box-shadow:inset 0 0 0 2px #7a8a96}.banner{padding:8px;background:#eef2f6;border-left:4px solid #7a8a96;margin:8px 0;color:#44515c}.link-editor{display:none;background:#f3eef8;border-left:4px solid #75439c;padding:10px;margin-top:10px}.link-editor.open{display:block}.side-labels{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:12px}.side-labels h2{font-size:15px;margin:0 0 7px}.link-line{stroke:#087c72;stroke-width:3;stroke-dasharray:8 6;fill:none;opacity:.9}.link-line.all{stroke:#75439c;stroke-width:2;opacity:.65}@media(max-width:900px){.pages{grid-template-columns:1fr}.progress{margin-left:0}.viewport{height:58vh}}</style>
<header><h1>Проверка OLD ↔ NEW связей · V1.2</h1><div class=note>Production · BlockLink editing · Semantic Mapping V3 · никаких model calls</div></header><div class=bar><span id=hm-context class=progress style="margin-left:0"></span><span id=fixture-nav hidden><button id=A>АР1</button><button id=B>ИОС4.2</button></span><button data-q=UNREVIEWED>Не проверено</button><button data-q=HUMAN_CONFIRMED>Подтверждено</button><button data-q=HUMAN_REJECTED>Отклонено</button><button data-q=HUMAN_UNCERTAIN>Не уверен</button><span class=progress id=progress></span></div><main class=main><div id=app></div></main><dialog id=zoom><button onclick="zoom.close()">Закрыть</button><img></dialog>
<script>
(function(){
  /* Server-validated comparison context. The active comparison is exactly
     CTX.pair; there is no fallback to fixture A/B. */
  const CTX=__HM_CONTEXT_JSON__;
  window.__HM_CONTEXT__=CTX;
  const API='/api/human-mapping/objects/'+encodeURIComponent(CTX.object);
  const PAIR_URL=CTX.pair?API+'/comparisons/'+encodeURIComponent(CTX.pair):null;
  const _fetch=window.fetch.bind(window);
  window.fetch=function(url, opts){
    let u=String(url);
    if(u.startsWith('/data/UI_DATA_PAIR_')||u.startsWith('/reviews')||u.startsWith('/block_links')){
      if(!PAIR_URL) return Promise.reject(new Error('HM_NO_ACTIVE_COMPARISON'));
      if(u.startsWith('/data/UI_DATA_PAIR_')) u=PAIR_URL+'/ui-data';
      else if(u.startsWith('/reviews')) u=PAIR_URL+'/reviews';
      else u=PAIR_URL+'/block-links';
    } else if(u.startsWith('/assets/')){
      u=(PAIR_URL||API)+'/assets/'+u.slice('/assets/'.length);
    }
    return _fetch(u, opts);
  };
})();
</script>
<script>
let pair=(window.__HM_CONTEXT__.fixture_letter||window.__HM_CONTEXT__.pair||''),queue='UNREVIEWED',data,history=[],linkEvents=[],selected={OLD:[],NEW:[]},panMode=null,showAll=false,editMode=false,selectedLinkId=null,reassignSide=null,view={OLD:{x:0,y:0,z:1,locked:false},NEW:{x:0,y:0,z:1,locked:false}};const $=s=>document.querySelector(s),esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
async function load(){const ctx=window.__HM_CONTEXT__;if(!ctx.pair){data=null;$('#progress').textContent='';$('#app').innerHTML='<p>Откройте Human Mapping из сравнения проекта: нужен объект и пара сравнения.</p>'+(ctx.fixture_nav?'<p>Для объекта 272 доступны зафиксированные сравнения АР1 и ИОС4.2 — кнопки выше.</p>':'');return}const res=await fetch('/data/UI_DATA_PAIR_'+pair+'.json');if(!res.ok){data=null;$('#progress').textContent='';$('#app').innerHTML='<p>Для этого сравнения нет данных Human Mapping (анализ V3 ещё не опубликовал семантические регионы).</p>';return}data=await res.json();history=await fetch('/reviews').then(r=>r.json());linkEvents=await fetch('/block_links').then(r=>r.json());selected={OLD:[],NEW:[]};selectedLinkId=null;reassignSide=null;render()}
function assetUrl(u){u=String(u||'');return u.startsWith('/')?u:'/'+u}
function reviewBanner(r){const st=r.mapping_state;if(!st||st==='MAPPED')return '';const ms=r.membership_state||{};const empty=['OLD','NEW'].filter(s=>ms[s]==='EMPTY');const txt=st==='REVIEW_INVALID_MEMBERSHIP_REFS'?'Картограф сослался на блоки, которых нет на указанных страницах ('+(r.invalid_membership_refs||[]).length+'). Членство региона неполное — проверьте вручную.':'Картограф не выделил важных блоков для стороны '+empty.join(' и ')+'. Членство этой стороны пустое; ниже показаны сопоставленные страницы — создайте связи вручную.';return `<div class=banner role=status><b>Требует проверки:</b> ${esc(txt)}</div>`}
function latest(r){let x=history.filter(h=>h.region_id===r.id);return x.length?x[x.length-1]:null}function stat(r){return latest(r)?.status||'UNREVIEWED'}function linked(r){let l=latest(r);return {OLD:selected.OLD.length?selected.OLD:(l?.old_block_ids||r.old_blocks.map(b=>b.id)),NEW:selected.NEW.length?selected.NEW:(l?.new_block_ids||r.new_blocks.map(b=>b.id))}}
function memberIds(r){return {OLD:r.old_blocks.map(b=>b.id),NEW:r.new_blocks.map(b=>b.id)}}
function findBlock(r,side,id){return [...r.old_blocks,...r.new_blocks].find(x=>x.id===id)||r.pages[side].flatMap(p=>p.blocks).find(x=>x.id===id)}
function baseProposedLinks(r){const olds=r.old_blocks.map(b=>b.id),news=r.new_blocks.map(b=>b.id);if(olds.length===1&&news.length===1)return[{link_id:`ai:${r.id}:${olds[0]}:${news[0]}`,old_block_id:olds[0],new_block_id:news[0],source:'AI_PROPOSED',membership_kind:'EXPLICIT_1_1'}];if(olds.length===1)return news.map(n=>({link_id:`ai:${r.id}:${olds[0]}:${n}`,old_block_id:olds[0],new_block_id:n,source:'AI_PROPOSED',membership_kind:'INHERITED_GROUP_1_N'}));if(news.length===1)return olds.map(o=>({link_id:`ai:${r.id}:${o}:${news[0]}`,old_block_id:o,new_block_id:news[0],source:'AI_PROPOSED',membership_kind:'INHERITED_GROUP_N_1'}));return[]}
function effectiveLinks(r){let links=baseProposedLinks(r).map(l=>({...l}));for(const e of linkEvents.filter(x=>x.region_id===r.id)){if(e.event_type==='ADD_BLOCK_LINK')links.push({link_id:e.link_id,old_block_id:e.old_block_id,new_block_id:e.new_block_id,source:'HUMAN_MANUAL',membership_kind:'HUMAN_DEFINED'});else if(e.event_type==='DELETE_BLOCK_LINK')links=links.filter(l=>l.link_id!==e.link_id&&!(l.old_block_id===e.old_block_id&&l.new_block_id===e.new_block_id));else if(e.event_type==='REASSIGN_BLOCK_LINK')links=links.map(l=>(l.link_id===e.previous_link_id)?{link_id:e.link_id,old_block_id:e.old_block_id,new_block_id:e.new_block_id,source:'HUMAN_MANUAL',membership_kind:'HUMAN_DEFINED'}:l)}const seen=new Set();return links.filter(l=>{const k=l.old_block_id+'|'+l.new_block_id;if(seen.has(k))return false;seen.add(k);return true})}
function visibleLinks(r){const all=effectiveLinks(r);if(showAll)return all;const sel=[...selected.OLD,...selected.NEW];if(!sel.length&&selectedLinkId)return all.filter(l=>l.link_id===selectedLinkId);if(!sel.length)return all;return all.filter(l=>selected.OLD.includes(l.old_block_id)||selected.NEW.includes(l.new_block_id)||l.link_id===selectedLinkId)}
function overlay(p,b,r){const mem=memberIds(r),selLink=effectiveLinks(r).find(l=>l.link_id===selectedLinkId);const isMember=mem[p.side].includes(b.id);const isSelected=selected[p.side].includes(b.id);const isEnd=!!selLink&&((p.side==='OLD'&&selLink.old_block_id===b.id)||(p.side==='NEW'&&selLink.new_block_id===b.id));let [x,y,w,h]=b.bbox;return `<button class="block ${b.type} ${isSelected?'selected':''} ${isEnd?'link-end':''} ${isMember&&!isSelected&&!isEnd?'member':''}" title="${esc(b.id)} · ${b.type}" data-side="${p.side}" data-id="${b.id}" style="left:${x*100}%;top:${y*100}%;width:${(w-x)*100}%;height:${(h-y)*100}%"></button>`}
function page(p,r){return `<div class=page><img src="${assetUrl(p.image)}">${p.blocks.map(b=>overlay(p,b,r)).join('')}</div>`}
function tableHtml(t){const grid=rows=>'<table>'+rows.map(row=>'<tr>'+row.map(cell=>'<td>'+esc(cell)+'</td>').join('')+'</tr>').join('')+'</table>';if(Array.isArray(t))return grid(t);const rows=String(t||'').split('\n').map(l=>l.trim()).filter(l=>l.startsWith('|')&&!/^\|[\s:|-]+\|?$/.test(l)).map(l=>l.replace(/^\||\|$/g,'').split('|').map(c=>c.trim()));return rows.length?grid(rows):'<pre>'+esc(String(t||''))+'</pre>'}
function showDetail(side,id,r){let b=findBlock(r,side,id);let table=b.tables?.length?b.tables.map(tableHtml).join(''):'';return `<div class=detail><b>${esc(b.id)}</b> · ${b.type} · стр. ${b.page??'—'} · bbox ${b.bbox.map(x=>x.toFixed(3)).join(', ')}${b.type==='GRAPHIC'&&b.crop?`<br><img class=crop src="${assetUrl(b.crop)}" onclick="zoom.querySelector('img').src=this.src;zoom.showModal()">`:''}${b.type==='TABLE'?table:`<pre>${esc(b.structured_md||'Структурированное содержимое отсутствует.')}</pre>`}</div>`}
function transform(side){let v=view[side],s=$('#sheet-'+side);if(s)s.style.transform=`translate(${v.x}px,${v.y}px) scale(${v.z})`;requestAnimationFrame(drawLines)}
function controls(side){let v=view[side];return `<b>${side}</b><button data-pan="${side}" class="${panMode===side?'active':''}">${panMode===side?'Перемещение включено':'Переместить лист'}</button><button data-reset="${side}">Сбросить положение</button><button data-lock="${side}" class="${v.locked?'active':''}">${v.locked?'Положение зафиксировано':'Зафиксировать положение'}</button><label>Масштаб <input data-zoom="${side}" type=range min=.6 max=1.8 step=.1 value=${v.z}></label>`}
function render(){for(let x of ['A','B'])$('#'+x).classList.toggle('active',pair===x);let reviewed=data.regions.filter(r=>stat(r)!=='UNREVIEWED').length;$('#progress').textContent=`Проверено ${reviewed} / ${data.regions.length}`;let r=data.regions.find(x=>stat(x)===queue)||data.regions[0];if(!r){$('#app').innerHTML='<p>В этой очереди нет связей.</p>';return}let l=latest(r),links=effectiveLinks(r);$('#app').innerHTML=`<section class=card><div class=title><div><b>${r.id}</b> · ${esc(r.domain)}<p>${esc(r.scope)}</p></div><small>AI-proposed group · confidence ${r.confidence} · BlockLinks: ${links.length}</small></div>${reviewBanner(r)}<div class=link>Group membership · OLD (${r.old_blocks.map(x=>esc(x.id)).join(', ')}) ↔ NEW (${r.new_blocks.map(x=>esc(x.id)).join(', ')})</div><div class=controls>${controls('OLD')} ${controls('NEW')}<button id=reset-both>Сбросить оба</button><button id=all-links class="${showAll?'active':''}">Показать все связи: ${showAll?'вкл.':'выкл.'}</button><button id=edit-links class="${editMode?'active':''}">Редактировать связи: ${editMode?'вкл.':'выкл.'}</button></div><div class=side-labels><h2>OLD</h2><h2>NEW</h2></div><div class=mapping-workspace id=mapping-workspace><div class="viewport ${panMode==='OLD'?'pan':''}" id=view-OLD><div class=sheet id=sheet-OLD>${r.pages.OLD.map(p=>page({...p,side:'OLD'},r)).join('')}</div></div><div class="viewport ${panMode==='NEW'?'pan':''}" id=view-NEW><div class=sheet id=sheet-NEW>${r.pages.NEW.map(p=>page({...p,side:'NEW'},r)).join('')}</div></div><svg class=link-layer id=links aria-hidden=true></svg></div><div class=manual>Режим: ${editMode?'редактирование BlockLink':'просмотр'}. Выделены: OLD ${selected.OLD.length}, NEW ${selected.NEW.length}. <button id=add-spoke>Добавить BlockLink(и) 1→N / N→1</button> <button id=add-one>Добавить одну пару 1→1</button> <button id=add-cartesian>Создать все пары (N×N, явно)</button></div><div class="link-editor ${editMode||selectedLinkId?'open':''}" id=link-editor></div><div class=decision><textarea id=comment placeholder="Комментарий (необязательно)"></textarea><button data-status=HUMAN_CONFIRMED>ПОДТВЕРДИТЬ</button><button data-status=HUMAN_REJECTED>ОТКЛОНИТЬ</button><button data-status=HUMAN_UNCERTAIN>НЕ УВЕРЕН</button></div>${l?`<p>Последнее решение: <b>${l.status}</b> · ${esc(l.timestamp)}</p>`:''}<div id=details></div></section>`;for(let side of ['OLD','NEW']){transform(side);bindPan(side);$('#view-'+side).addEventListener('scroll',()=>requestAnimationFrame(drawLines));}document.querySelectorAll('.block').forEach(b=>b.onclick=async e=>{if(panMode)return;let s=b.dataset.side,id=b.dataset.id;if(editMode&&reassignSide&&selectedLinkId&&s===reassignSide){await reassignLink(r,s,id);return}selected[s]=selected[s].includes(id)?selected[s].filter(x=>x!==id):[...selected[s],id];render();$('#details').innerHTML=showDetail(s,id,r)});document.querySelectorAll('[data-status]').forEach(b=>b.onclick=()=>save(r,b.dataset.status));document.querySelectorAll('[data-pan]').forEach(b=>b.onclick=()=>{panMode=panMode===b.dataset.pan?null:b.dataset.pan;render()});document.querySelectorAll('[data-reset]').forEach(b=>{b.onclick=()=>{let v=view[b.dataset.reset];v.x=v.y=0;v.z=1;render()}});document.querySelectorAll('[data-lock]').forEach(b=>b.onclick=()=>{view[b.dataset.lock].locked=!view[b.dataset.lock].locked;render()});document.querySelectorAll('[data-zoom]').forEach(i=>i.oninput=()=>{view[i.dataset.zoom].z=+i.value;transform(i.dataset.zoom)});$('#reset-both').onclick=()=>{for(let s of ['OLD','NEW'])Object.assign(view[s],{x:0,y:0,z:1});render()};$('#all-links').onclick=()=>{showAll=!showAll;render()};$('#edit-links').onclick=()=>{editMode=!editMode;if(!editMode)reassignSide=null;panMode=null;render()};
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
(function(){const c=window.__HM_CONTEXT__;const go=l=>{location.search='?object='+encodeURIComponent(c.object)+'&pair='+l};$('#A').onclick=()=>go('A');$('#B').onclick=()=>go('B');$('#fixture-nav').hidden=!c.fixture_nav;$('#hm-context').textContent=c.pair?('Сравнение '+(c.label||c.pair)+' · объект '+c.object):('Объект '+c.object+' · сравнение не выбрано')})();document.querySelectorAll('[data-q]').forEach(b=>b.onclick=()=>{queue=b.dataset.q;selectedLinkId=null;render()});window.addEventListener('resize',()=>requestAnimationFrame(drawLines));load()</script>'''


def _scope(object_id: str, pair: str) -> tuple[str, str, str | None]:
    """Validate IDs, then resolve (object, storage pair id, fixture letter)."""
    try:
        storage.require_safe_id(object_id, "object")
        storage.require_safe_id(pair, "pair")
    except storage.InvalidScopeId as exc:
        raise HTTPException(400, detail={"error": "INVALID_SCOPE_ID", "kind": exc.kind, "ok": False}) from exc
    if object_id == FIXTURE_OBJECT:
        if pair in PAIR_KEYS:
            return object_id, PAIR_KEYS[pair], pair
        if pair in PAIR_BY_KEY:
            return object_id, pair, PAIR_BY_KEY[pair]
    return object_id, pair, None


def _data_source(object_id: str, pair_id: str, letter: str | None) -> tuple[str | None, dict | None]:
    """(kind, seed receipt): published V3 data > fixture alias > source-identical seed."""
    if (storage.pair_dir(object_id, pair_id, smoke=False) / "ui_data.json").is_file():
        return "PUBLISHED", None
    if letter:
        return ("FIXTURE_ALIAS", None) if (FIXTURES / f"UI_DATA_PAIR_{letter}.json").is_file() else (None, None)
    seed = fixture_binding.seed_for(object_id, pair_id)
    return ("SOURCE_IDENTICAL_FIXTURE_SEED", seed) if seed["match_status"] == "BOUND" else (None, None)


def _load_ui_data(object_id: str, pair_id: str, letter: str | None) -> dict:
    kind, seed = _data_source(object_id, pair_id, letter)
    if kind == "PUBLISHED":
        generic = storage.pair_dir(object_id, pair_id, smoke=False) / "ui_data.json"
        return json.loads(generic.read_text(encoding="utf-8"))
    if kind == "FIXTURE_ALIAS":
        return json.loads((FIXTURES / f"UI_DATA_PAIR_{letter}.json").read_text(encoding="utf-8"))
    if kind == "SOURCE_IDENTICAL_FIXTURE_SEED":
        return fixture_binding.seeded_ui_data(seed)
    raise HTTPException(404, "HUMAN_MAPPING_UI_DATA_NOT_FOUND")


def _region(object_id: str, pair_id: str, letter: str | None, region_id: str):
    data = _load_ui_data(object_id, pair_id, letter)
    for r in data.get("regions") or []:
        if r.get("id") == region_id:
            return data, r
    return data, None


def _context_json(context: dict) -> str:
    raw = json.dumps(context, ensure_ascii=False)
    return raw.replace("<", "\\u003c").replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")


@router.get("/human-mapping/", response_class=HTMLResponse)
@router.get("/human-mapping", response_class=HTMLResponse)
def human_mapping_page(
    object: str | None = Query(default=None),
    pair: str | None = Query(default=None),
    comparison: str | None = Query(default=None),
):
    requested_pair = comparison or pair
    if object is None and requested_pair is None:
        object_id, pair_id, letter = FIXTURE_OBJECT, None, None
    elif object is None:
        raise HTTPException(400, detail={"error": "OBJECT_REQUIRED", "ok": False})
    elif requested_pair is None:
        try:
            storage.require_safe_id(object, "object")
        except storage.InvalidScopeId as exc:
            raise HTTPException(400, detail={"error": "INVALID_SCOPE_ID", "kind": exc.kind, "ok": False}) from exc
        if object != FIXTURE_OBJECT:
            raise HTTPException(400, detail={"error": "COMPARISON_REQUIRED", "ok": False})
        object_id, pair_id, letter = object, None, None
    else:
        object_id, pair_id, letter = _scope(object, requested_pair)
    kind, seed = _data_source(object_id, pair_id, letter) if pair_id else (None, None)
    context = {
        "object": object_id,
        "pair": pair_id,
        "fixture_letter": letter,
        "fixture_nav": object_id == FIXTURE_OBJECT and (pair_id is None or letter is not None),
        "label": FIXTURE_LABELS.get(letter or "", (seed or {}).get("fixture_source", {}).get("label") or pair_id),
        "data_source": kind,
        "session_id": (seed or {}).get("session_id"),
        "seed": fixture_binding.public_receipt(seed) if seed else None,
    }
    return HTMLResponse(HTML_PAGE.replace("__HM_CONTEXT_JSON__", _context_json(context), 1))


@api_router.get("/pairs/{pair}/ui-data")
@api_router.get("/comparisons/{pair}/ui-data")
def ui_data(object_id: str, pair: str):
    object_id, pair_id, letter = _scope(object_id, pair)
    data = _load_ui_data(object_id, pair_id, letter)
    raw = json.dumps(data, ensure_ascii=False)
    raw = raw.replace(
        '"assets/',
        f'"/api/human-mapping/objects/{object_id}/comparisons/{pair}/assets/',
    )
    return JSONResponse(json.loads(raw))


def _serve_asset(bases: list[Path], asset_path: str) -> Response:
    for base in bases:
        file = (base / asset_path).resolve()
        if file.is_relative_to(base) and file.is_file():
            media = "image/png" if file.suffix.lower() == ".png" else "application/octet-stream"
            return Response(file.read_bytes(), media_type=media, headers={"Cache-Control": "no-store"})
    raise HTTPException(404, "Asset not found")


@api_router.get("/pairs/{pair}/assets/{asset_path:path}")
@api_router.get("/comparisons/{pair}/assets/{asset_path:path}")
def pair_asset(object_id: str, pair: str, asset_path: str):
    object_id, pair_id, letter = _scope(object_id, pair)
    bases = [(storage.pair_dir(object_id, pair_id, smoke=False) / "assets").resolve()]
    kind, seed = _data_source(object_id, pair_id, letter)
    if kind == "FIXTURE_ALIAS":
        bases.append((FIXTURES / "assets").resolve())
    elif kind == "SOURCE_IDENTICAL_FIXTURE_SEED":
        bases.append(Path(seed["_assets_dir"]).resolve())
    return _serve_asset(bases, asset_path)


@api_router.get("/assets/{asset_path:path}")
def fixture_asset(object_id: str, asset_path: str):
    try:
        storage.require_safe_id(object_id, "object")
    except storage.InvalidScopeId as exc:
        raise HTTPException(400, detail={"error": "INVALID_SCOPE_ID", "kind": exc.kind, "ok": False}) from exc
    if object_id != FIXTURE_OBJECT:
        raise HTTPException(404, "Asset not found")
    return _serve_asset([(FIXTURES / "assets").resolve()], asset_path)


@api_router.get("/pairs/{pair}/reviews")
@api_router.get("/comparisons/{pair}/reviews")
def get_reviews(object_id: str, pair: str, smoke: bool = Query(default=False)):
    object_id, pair_id, _letter = _scope(object_id, pair)
    return storage.list_reviews(object_id, pair_id, smoke=smoke)


@api_router.post("/pairs/{pair}/reviews")
@api_router.post("/comparisons/{pair}/reviews")
async def post_review(
    object_id: str, pair: str, request: Request, smoke: bool = Query(default=False)
):
    object_id, pair_id, letter = _scope(object_id, pair)
    try:
        raw = await request.json()
        status = raw["status"]
        assert status in {"HUMAN_CONFIRMED", "HUMAN_REJECTED", "HUMAN_UNCERTAIN"}
        assert raw.get("old_block_ids") and raw.get("new_block_ids")
        region_id = raw["region_id"]
    except Exception as exc:
        raise HTTPException(400, "Bad review") from exc
    _data, region = _region(object_id, pair_id, letter, region_id)
    if region is None:
        raise HTTPException(400, detail={"error": "REGION_NOT_FOUND", "ok": False})
    row = {
        "review_id": str(uuid.uuid4()),
        "pair": letter or pair_id,
        "pair_key": pair_id,
        "comparison_id": pair_id,
        "object_id": object_id,
        "region_id": region_id,
        "old_block_ids": raw["old_block_ids"],
        "new_block_ids": raw["new_block_ids"],
        "status": status,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "comment": raw.get("comment", ""),
        "reviewer_source": "HUMAN",
        "supersedes_review_id": raw.get("previous_review_id"),
    }
    storage.append_review(object_id, pair_id, row, smoke=smoke)
    return row


@api_router.get("/pairs/{pair}/block-links")
@api_router.get("/comparisons/{pair}/block-links")
def get_block_links(object_id: str, pair: str, smoke: bool = Query(default=False)):
    object_id, pair_id, _letter = _scope(object_id, pair)
    return storage.list_block_links(object_id, pair_id, smoke=smoke)


@api_router.post("/pairs/{pair}/block-links")
@api_router.post("/comparisons/{pair}/block-links")
async def post_block_link(
    object_id: str, pair: str, request: Request, smoke: bool = Query(default=False)
):
    object_id, pair_id, letter = _scope(object_id, pair)
    try:
        raw = await request.json()
        event_type = raw["event_type"]
        region_id = raw["region_id"]
        link_id = raw["link_id"]
        old_block_id = raw["old_block_id"]
        new_block_id = raw["new_block_id"]
    except Exception as exc:
        raise HTTPException(400, detail={"error": "BAD_BLOCK_LINK_EVENT", "ok": False}) from exc

    _data, region = _region(object_id, pair_id, letter, region_id)
    if region is None:
        raise HTTPException(400, detail={"error": "REGION_NOT_FOUND", "ok": False})

    events = storage.list_block_links(object_id, pair_id, smoke=smoke)
    try:
        resolved = validate_block_link_event(
            event_type=event_type,
            region=region,
            events=events,
            link_id=link_id,
            old_block_id=old_block_id,
            new_block_id=new_block_id,
            previous_link_id=raw.get("previous_link_id"),
        )
    except BlockLinkValidationError as exc:
        raise HTTPException(400, detail={"error": exc.code, "ok": False}) from exc

    if resolved.get("noop"):
        return {"error": "NO_CHANGE", "ok": True}

    row = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "pair": letter or pair_id,
        "pair_key": pair_id,
        "comparison_id": pair_id,
        "object_id": object_id,
        "region_id": region_id,
        "link_id": link_id,
        "old_block_id": old_block_id,
        "new_block_id": new_block_id,
        "previous_old_block_id": resolved.get("previous_old_block_id"),
        "previous_new_block_id": resolved.get("previous_new_block_id"),
        "previous_link_id": resolved.get("previous_link_id"),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "comment": raw.get("comment", ""),
        "reviewer_source": "HUMAN",
    }
    storage.append_block_link(object_id, pair_id, row, smoke=smoke)
    return row
