"use strict";
const $ = id => document.getElementById(id);
const labels = {YES:"Да",NO:"Нет",UNSURE:"Не могу определить",BROKEN:"Проблема с примером"};
let boot, state, current, chosen = null, pending = null, busy = false;
const storageKey = "human-dev-blind-qa-v1";
function status(text) { $("status").textContent = text; }
function persist() {
  try { localStorage.setItem(storageKey, JSON.stringify({packet:boot.packet_sha256,current,chosen,note:$("note").value,pending})); }
  catch { status("Черновик хранится только в этой вкладке. Не закрывайте её до подтверждения сохранения."); }
}
async function api(path, options = {}) {
  const response = await fetch(path, {...options, signal:AbortSignal.timeout(12000)});
  const data = await response.json();
  if (!response.ok) {const error=new Error(data.error || "Ошибка запроса");error.status=response.status;throw error;}
  return data;
}
function node(tag, text, cls) {
  const n = document.createElement(tag); if (text !== undefined) n.textContent = text; if (cls) n.className = cls; return n;
}
function lines(rows) {
  const box = node("div",undefined,"source");
  for (const row of rows) box.append(node("div",typeof row === "string" ? row : row.text,"line" + (row.focus ? " focus" : "")));
  return box;
}
function panel(p) {
  const box = node("article",undefined,"panel"); box.append(node("h3",p.title),node("p",`Физическая страница ${p.page} из ${p.page_count}`));
  if (p.preceding_heading) box.append(node("p","Предшествующий заголовок: " + p.preceding_heading));
  box.append(lines(p.context));
  const full = node("details"); full.append(node("summary","Весь текст исходной страницы"),lines(p.full_context)); box.append(full);
  const pdf = node("a","Открыть PDF"); pdf.href = p.pdf_url; pdf.target = "_blank"; pdf.rel = "noopener"; box.append(pdf);
  const details = node("details"); details.append(node("summary","PDF и соседние страницы"));
  const nav = node("div",undefined,"navigation"), prev = node("button","←"), next = node("button","→"), input = node("input"), go = node("button","Перейти");
  input.type = "number"; input.min = 1; input.max = p.page_count; input.value = p.page; input.setAttribute("aria-label","Номер физической страницы");
  nav.append(prev,node("span","Страница"),input,node("span",`/ ${p.page_count}`),next,go);
  const img = node("img"); img.alt = "Страница исходного PDF";
  const context = node("div"); let loaded = false, sequence = 0;
  async function loadPage(number) {
    const page = Math.max(1,Math.min(p.page_count,number || p.page)); input.value = page;
    const request = ++sequence;
    img.src = `/page/${p.source_id}/${page}.png`; pdf.href = `/pdf/${p.source_id}#page=${page}`;
    prev.disabled = page === 1; next.disabled = page === p.page_count;
    try { const data = await api(`/text/${p.source_id}/${page}`); if(request === sequence) context.replaceChildren(lines(data.lines)); }
    catch(error) { if(request === sequence) context.textContent = error.message; }
  }
  prev.onclick = () => loadPage(Number(input.value)-1); next.onclick = () => loadPage(Number(input.value)+1); go.onclick = () => loadPage(Number(input.value));
  details.addEventListener("toggle",() => {if(details.open && !loaded){loaded=true;loadPage(p.page);}});
  details.append(nav,img,context); box.append(details); return box;
}
function available() {
  if (state.phase === "blind") return boot.cases.filter(c => !state.records[c.case_id]);
  if (state.phase === "final") return [];
  return boot.cases.filter(c => state.disagreements.some(d => d.case_id === c.case_id));
}
function choose(answer) {
  chosen = answer;
  document.querySelectorAll("[data-answer]").forEach(b => b.setAttribute("aria-pressed",String(b.dataset.answer === chosen)));
  $("save").disabled = busy || !chosen; persist();
}
function render(preferred) {
  $("progress").textContent = `Ответов перепроверки: ${state.answered} / ${state.total}`;
  $("summary").hidden = state.phase === "blind";
  if(state.stats) {
    const s = state.stats;
    $("summary").textContent = `Выбрано: ${s.selected}. Совпадений: ${s.blind_matches}. Расхождений: ${s.blind_disagreements}. Не могу определить: ${s.unsure}. Проблема с примером: ${s.broken}.`;
  }
  const cases = available();
  const unresolved = cases.find(c => !["YES","NO"].includes(state.review_records[c.case_id]?.answer));
  current = cases.find(c => c.case_id === preferred)?.case_id || unresolved?.case_id || cases[0]?.case_id;
  $("task").hidden = !current;
  $("finish").hidden = state.phase === "blind";
  $("download-final").hidden = state.phase !== "final";
  $("finish-text").textContent = state.phase === "final" ? "Итоговый набор зафиксирован. Решения и история сохранены." :
    "Показаны только расхождения. Примите окончательное решение по каждому из них. После последнего решения итоговый набор будет зафиксирован. Неопределённые или проблемные примеры требуют уточнения.";
  $("save").textContent = state.phase === "review" ? "Сохранить окончательное решение" : "Сохранить ответ";
  if(!current) return;
  $("case-select").replaceChildren(...cases.map(c => {const o=node("option",`Пример ${boot.cases.indexOf(c)+1}${state.review_records[c.case_id] ? " · решение сохранено" : ""}`);o.value=c.case_id;return o;}));
  $("case-select").value=current;
  const c = boot.cases.find(c => c.case_id === current);
  $("question").textContent=c.question; $("document").textContent=c.document;
  $("panels").replaceChildren(...c.panels.map(panel));
  $("comparison").hidden = state.phase === "blind";
  if(state.phase === "review") {
    const d=state.disagreements.find(d=>d.case_id===current);
    $("comparison").textContent=`Первоначальный ответ: ${labels[d.original_answer]}. Слепая перепроверка: ${labels[d.blind_answer]}. Ваше окончательное решение:`;
  }
  $("note").value = state.phase === "review" ? state.review_records[current]?.note || "" : "";
  choose(state.phase === "review" ? state.review_records[current]?.answer || null : null);
}
async function submit() {
  if(busy || (!pending && !chosen)) return;
  busy=true; $("save").disabled=true;
  document.querySelectorAll("[data-answer]").forEach(b=>b.disabled=true); $("case-select").disabled=true; $("note").disabled=true;
  if(!pending) pending={stage:state.phase,case_id:current,answer:chosen,note:$("note").value,
    expected_revision:state.phase==="review"?(state.review_records[current]?.revision||0):0,
    submission_id:crypto.randomUUID(),namespace:boot.namespace,packet_sha256:boot.packet_sha256};
  persist(); status("Сохраняю…");
  try {
    const fresh=await api("/api/state"); boot.csrf=fresh.csrf;
    const result=await api("/api/answers",{method:"POST",headers:{"Content-Type":"application/json","X-Wave1-Token":boot.csrf},body:JSON.stringify(pending)});
    pending=null; chosen=null; state=result; render(); persist(); status("Ответ сохранён"); window.scrollTo({top:0,behavior:"smooth"});
  } catch(error) {
    if(error.status===409) {
      try {state=await api("/api/state");pending=null;render();persist();status(error.message + ". Показано текущее состояние; проверьте его перед следующим ответом.");}
      catch {status("Не удалось обновить состояние. Повторите сохранение.");}
    } else {status("Сохранение не подтверждено: " + error.message + ". Нажмите «Повторить сохранение» — повтор будет безопасным.");}
    if(pending) $("save").textContent="Повторить сохранение";
  }
  finally {
    busy=false; $("save").disabled=!pending&&!chosen;
    document.querySelectorAll("[data-answer]").forEach(b=>b.disabled=!!pending); $("case-select").disabled=!!pending; $("note").disabled=!!pending;
    if(!pending) $("save").textContent=state.phase==="review"?"Сохранить окончательное решение":"Сохранить ответ";
  }
}
document.querySelectorAll("[data-answer]").forEach(b=>b.onclick=()=>choose(b.dataset.answer));
$("note").oninput=persist; $("case-select").onchange=()=>render($("case-select").value);
$("save").onclick=()=>submit();
(async()=>{
  try {
    boot=await api("/api/bootstrap"); state=boot;
    let draft; try {draft=JSON.parse(localStorage.getItem(storageKey));}catch{}
    render(draft?.packet===boot.packet_sha256?draft.current:undefined);
    if(draft?.packet===boot.packet_sha256) {
      pending=draft.pending;
      if(pending){status("Проверяю сохранение предыдущего ответа…"); await submit();}
      else if(current===draft.current){$("note").value=draft.note||"";choose(draft.chosen);}
    }
  }catch(error){status(error.message);}
})();
