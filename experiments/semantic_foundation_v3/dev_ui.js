"use strict";
(async () => {
  const $ = id => document.getElementById(id);
  const data = await fetch("cases.json").then(r => { if (!r.ok) throw Error("Не удалось открыть вопросы"); return r.json(); });
  if (data.namespace !== "SEMANTIC_FOUNDATION_V3_DEV") throw Error("Неверный набор вопросов");
  const key = `${data.namespace}:${data.packet_sha256}`;
  let answers = {}, index = 0;
  const stored = localStorage.getItem(key);
  if (stored) {
    const parsed = JSON.parse(stored);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw Error("Не удалось проверить сохранённые ответы. Не очищайте данные браузера.");
    for (const [id, choice] of Object.entries(parsed)) {
      const c = data.cases.find(c => c.case_id === id);
      if (!c || !c.allowed_choices.includes(choice)) throw Error("Сохранённые ответы не соответствуют этому набору. Не очищайте данные браузера.");
    }
    answers = parsed;
  }
  const labels = {SAME:"Одна единица",NEW:"Разные единицы",UNSURE:"Не уверен",OWNER_PRECEDING_SECTION:"Предшествующий раздел",OWNER_FOLLOWING_HEADING:"Заголовок ниже"};
  function save(next) {
    try { localStorage.setItem(key, JSON.stringify(next)); answers = next; $("error").textContent = ""; render(); }
    catch (_) { $("error").textContent = "Ответ не сохранён: браузер не разрешил запись. Экспортируйте уже сохранённые ответы."; }
  }
  for (const [i, c] of data.cases.entries()) {
    const option = document.createElement("option"); option.value = i; option.textContent = `${i+1}. ${c.stratum} · ${c.document_code}`; $("caseSelect").append(option);
  }
  function render() {
    const c = data.cases[index];
    $("progress").textContent = `Ответов: ${Object.keys(answers).length} из ${data.cases.length}. Вопрос ${index+1}. 22 автоматических контроля не требуют разметки.`;
    $("caseSelect").value = index; $("document").textContent = c.document_code; $("question").textContent = c.question;
    $("answers").replaceChildren();
    for (const choice of c.allowed_choices) {
      const b = document.createElement("button"); b.textContent = labels[choice]; b.classList.toggle("selected", answers[c.case_id] === choice);
      b.setAttribute("aria-pressed", String(answers[c.case_id] === choice)); b.onclick = () => save({...answers,[c.case_id]:choice}); $("answers").append(b);
    }
    $("prev").disabled = index === 0; $("next").disabled = index === data.cases.length-1; $("clear").disabled = !Object.hasOwn(answers,c.case_id);
    $("panels").replaceChildren();
    for (const [i,p] of c.panels.entries()) {
      const box = document.createElement("section"); box.className = "panel";
      const title = document.createElement("p"); title.textContent = `${i ? "Правый" : "Левый"} якорь · страница ${p.page} · строка ${p.markdown_line}`;
      const link = document.createElement("a"); link.href = `${c.pdf}#page=${p.page}`; link.target = "_blank"; link.rel = "noopener"; link.textContent = "Открыть PDF отдельно";
      const pre = document.createElement("pre");
      for (const [j,line] of p.context.entries()) {
        const number = p.context_start_line+j, span = document.createElement(number === p.markdown_line ? "mark" : "span");
        span.textContent = `${number}: ${line}\n`; pre.append(span);
      }
      const frame = document.createElement("iframe"); frame.src = link.href; frame.title = `PDF, страница ${p.page}`;
      box.append(title,link,pre,frame); $("panels").append(box);
    }
  }
  $("prev").onclick = () => { if(index>0) {index--;render();} };
  $("next").onclick = () => { if(index<data.cases.length-1) {index++;render();} };
  $("caseSelect").onchange = e => {index=Number(e.target.value);render();};
  $("nextEmpty").onclick = () => {const offset=data.cases.findIndex((_,i)=>!Object.hasOwn(answers,data.cases[(index+i+1)%data.cases.length].case_id));if(offset>=0){index=(index+offset+1)%data.cases.length;render();}};
  $("clear").onclick = () => {const next={...answers};delete next[data.cases[index].case_id];save(next);};
  $("export").onclick = () => {
    const boundary={},ownership={};
    for(const c of data.cases) (c.kind === "OWNER" ? ownership : boundary)[c.case_id]=answers[c.case_id] ?? null;
    const blob=new Blob([JSON.stringify({namespace:data.namespace,packet_sha256:data.packet_sha256,answers:boundary,ownership_answers:ownership},null,2)+"\n"],{type:"application/json"});
    const url=URL.createObjectURL(blob),a=document.createElement("a");a.href=url;a.download="dev_answers.json";a.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
  };
  render();
})().catch(e => { document.getElementById("error").textContent = e.message; });
