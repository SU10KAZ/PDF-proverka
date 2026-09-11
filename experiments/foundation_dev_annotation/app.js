"use strict";
(() => {
  const $ = id => document.getElementById(id);
  const UNSURE = "Не могу определить";
  const FAILED = "Ответ пока не подтверждён. Мы сохранили его как черновик. Не отправляйте повторно.";
  let data, state, current, draft = null, busy = false, timer = null, recoveryAttempt = 0;
  let storageAvailable = true, started = false;
  let events = [];

  function trace(event, details = {}) {
    events.push({at: new Date().toISOString(), event, ...details});
    events = events.slice(-30);
    if (data) {
      try { localStorage.setItem(`${data.namespace}:${data.packet_sha256}:diagnostics`, JSON.stringify(events)); }
      catch (_) { /* Diagnostics must never prevent saving. */ }
    }
  }

  async function api(path, body) {
    const controller = new AbortController();
    let timeout;
    trace("request_started", {path});
    try {
      const operation = (async () => {
        const response = await fetch(path, {cache: "no-store", signal: controller.signal,
          ...(body ? {method: "POST", headers: {"Content-Type": "application/json", "X-Wave1-Token": data.csrf}, body: JSON.stringify(body)} : {})});
        trace("http_response", {path, status: response.status});
        const value = await response.json();
        if (!response.ok) { const error = new Error(value.error || "Не удалось связаться с сервером"); error.status = response.status; throw error; }
        return value;
      })();
      // Also bound response.json() and fetch wrappers which ignore AbortSignal.
      const deadline = new Promise((_, reject) => { timeout = setTimeout(() => {
        const error = new Error("Сервер не ответил за 8 секунд"); error.name = "TimeoutError";
        reject(error); controller.abort();
      }, 8000); });
      const value = await Promise.race([operation, deadline]);
      if (value.csrf && data) data.csrf = value.csrf;
      trace("request_complete", {path});
      return value;
    } catch (error) {
      trace("request_failed", {path, error: error.name, message: error.message});
      throw error;
    } finally { clearTimeout(timeout); }
  }
  function message(text, error = false) {
    $("status").textContent = text;
    $("status").classList.toggle("error", error);
    $("save-status").textContent = text;
    $("save-status").classList.toggle("error", error);
  }
  function key(id) { return `${data.namespace}:${data.packet_sha256}:draft:${id}`; }
  function readDraft(id) {
    try {
      const raw = localStorage.getItem(key(id));
      if (!raw) return null;
      const saved = JSON.parse(raw);
      if (saved.request.case_id !== id || saved.request.packet_sha256 !== data.packet_sha256 || saved.request.namespace !== data.namespace) return null;
      return saved;
    } catch (_) { storageAvailable = false; return null; }
  }
  function writeDraft() {
    try { localStorage.setItem(key(draft.request.case_id), JSON.stringify(draft)); storageAvailable = true; }
    catch (_) { storageAvailable = false; }
  }
  function removeDraft(id, submission) {
    try {
      const saved = readDraft(id);
      if (saved && saved.request.submission_id === submission) localStorage.removeItem(key(id));
    } catch (_) { storageAvailable = false; }
  }
  function requestFor(answer) {
    return {case_id: current.case_id, human_answer: answer, problem_reason: null, note: "",
      expected_revision: state.records[current.case_id]?.revision || 0, submission_id: crypto.randomUUID(),
      namespace: data.namespace, packet_sha256: data.packet_sha256};
  }
  function select(answer) {
    if (busy || draft?.pending || state.frozen) return;
    const prior = draft?.request;
    draft = {request: requestFor(answer), pending: false};
    if (answer === "BROKEN_CASE" && prior?.human_answer === answer) {
      draft.request.problem_reason = prior.problem_reason; draft.request.note = prior.note;
    }
    writeDraft(); message(""); renderDecision();
  }
  function renderText(container, rows) {
    let table = null;
    for (const row of rows) {
      if (/^\s*\|.*\|\s*$/.test(row.text)) {
        if (!table) { const wrap = document.createElement("div"); wrap.className = "table-wrap"; table = document.createElement("table"); wrap.append(table); container.append(wrap); }
        const cells = row.text.trim().slice(1, -1).split("|");
        // Separator rows carry no readable words; keep an anchored separator visible.
        if (!row.focus && cells.every(cell => /^\s*:?-+:?\s*$/.test(cell))) continue;
        const tr = document.createElement("tr");
        if (row.focus) tr.className = "focus";
        for (const text of cells) { const td = document.createElement("td"); td.textContent = text.replace(/\*\*/g, "").trim(); tr.append(td); }
        table.append(tr);
      } else {
        table = null;
        const line = document.createElement(row.focus ? "mark" : "p");
        if (/^#+\s/.test(row.text)) line.className = "source-heading";
        line.textContent = row.text.replace(/^#+\s*/, "").replace(/\*\*/g, "");
        container.append(line);
      }
    }
  }
  function renderPanels() {
    $("panels").replaceChildren();
    for (const panel of current.panels) {
      const box = document.createElement("section"); box.className = "panel";
      const title = document.createElement("h3"); title.textContent = panel.title;
      const top = document.createElement("div"); top.className = "panel-head";
      const page = document.createElement("span"); page.className = "page-label"; page.textContent = `Страница PDF: ${panel.page}`;
      const link = document.createElement("a"); link.href = panel.pdf_url; link.target = "_blank"; link.rel = "noopener"; link.textContent = "Открыть PDF";
      top.append(page, link); box.append(title, top);
      if (panel.preceding_heading) { const heading = document.createElement("p"); heading.className = "preceding"; heading.textContent = `Перед фрагментом: ${panel.preceding_heading}`; box.append(heading); }
      const focusPreview = document.createElement("div"); focusPreview.className = "anchor-preview";
      const focused = panel.context.find(row => row.focus);
      renderText(focusPreview, [focused]);
      if (!focused.text.replace(/[|\s:*_-]/g, "")) {
        const empty = document.createElement("p"); empty.textContent = "Выделенная строка таблицы не содержит текста."; focusPreview.append(empty);
      }
      box.append(focusPreview);
      const contextLabel = document.createElement("p"); contextLabel.className = "page-label"; contextLabel.textContent = "Текст вокруг фрагмента"; box.append(contextLabel);
      const context = document.createElement("div"); context.className = "context"; renderText(context, panel.context); box.append(context);
      const more = document.createElement("details"), moreTitle = document.createElement("summary"); moreTitle.textContent = "Больше текста на этой странице";
      const full = document.createElement("div"); full.className = "context"; renderText(full, panel.full_context); more.append(moreTitle, full); box.append(more);
      const imageDetails = document.createElement("details"), imageTitle = document.createElement("summary"); imageTitle.textContent = "Посмотреть страницу PDF"; imageDetails.append(imageTitle);
      imageDetails.addEventListener("toggle", () => {
        if (!imageDetails.open || imageDetails.querySelector("img")) return;
        const image = document.createElement("img"); image.src = panel.image_url; image.alt = `Страница PDF ${panel.page}`;
        image.onerror = () => { const warning = document.createElement("p"); warning.textContent = "Не удалось показать страницу. Используйте ссылку «Открыть PDF»."; image.replaceWith(warning); };
        imageDetails.append(image);
      });
      box.append(imageDetails); $("panels").append(box);
    }
  }
  function renderDecision() {
    const answer = draft?.request.human_answer;
    for (const button of $("answers").children) {
      button.setAttribute("aria-pressed", String(button.dataset.answer === answer));
      button.disabled = busy || !!draft?.pending;
    }
    $("problem").setAttribute("aria-pressed", String(answer === "BROKEN_CASE"));
    $("problem").disabled = busy || !!draft?.pending;
    $("problem-fields").hidden = answer !== "BROKEN_CASE";
    $("problem-reason").value = draft?.request.problem_reason || "";
    $("note").value = draft?.request.note || "";
    $("problem-reason").disabled = $("note").disabled = busy || !!draft?.pending;
    $("save").disabled = busy || !draft || draft.conflict || (answer === "BROKEN_CASE" && !draft.request.problem_reason);
    $("save").setAttribute("aria-busy", String(busy));
    $("case-select").disabled = busy || !!draft?.pending;
    $("diagnostic-text").textContent = JSON.stringify({namespace: data.namespace, packet_sha256: data.packet_sha256,
      schema_version: data.schema_version, annotator: data.annotator, case_id: current.case_id,
      ...current.diagnostics, saved: state.records[current.case_id] || null, events}, null, 2);
  }
  function render() {
    const p = state.progress;
    $("progress").replaceChildren(...[`Ответов: ${p.answered} / ${p.total}`, `Осталось: ${p.remaining}`, `Не уверен: ${p.unsure}`, `Проблема с примером: ${p.broken}`].map(text => { const span = document.createElement("span"); span.textContent = text; return span; }));
    $("complete").hidden = !state.frozen; $("task").hidden = state.frozen;
    if (state.frozen) { $("recover").hidden = true; return; }
    $("case-select").replaceChildren(...data.cases.map((c, i) => {
      const option = document.createElement("option"); option.value = c.case_id;
      option.textContent = `${i + 1}${state.records[c.case_id] ? " · сохранён" : ""}`; return option;
    }));
    $("case-select").value = current.case_id;
    $("document").textContent = current.document; $("question").textContent = current.question;
    $("diagnostics").open = false;
    $("answers").replaceChildren();
    for (const [value, label] of [["YES", current.yes], ["NO", current.no], ["UNSURE", UNSURE]]) {
      const button = document.createElement("button"); button.textContent = label; button.dataset.answer = value; button.onclick = () => select(value); $("answers").append(button);
    }
    renderPanels(); renderDecision();
  }
  function openCase(c) {
    current = c; draft = readDraft(c.case_id);
    const saved = state.records[c.case_id];
    if (draft && saved?.submission_id === draft.request.submission_id) { removeDraft(c.case_id, saved.submission_id); draft = null; }
    if (draft && !draft.pending && draft.request.expected_revision !== (saved?.revision || 0)) draft.conflict = true;
    render();
    if (draft?.pending) { message(FAILED, true); recover(); }
    else if (draft) message(draft.conflict ? "Ответ уже изменён в другом окне. Черновик сохранён; выберите ответ ещё раз для исправления." : "Восстановлен черновик ответа.");
    else if (saved) message("Этот ответ уже сохранён. Выберите ответ, если хотите его исправить.");
  }
  async function confirmed(record, snapshot) {
    // POST already contains authoritative state. A second request must not block
    // a successful save. Older receipt endpoints retain a bounded GET fallback.
    if (!snapshot?.records || !snapshot.progress) snapshot = await api("/api/state");
    if (!record || snapshot.records[record.case_id]?.revision < record.revision || !snapshot.records[record.case_id]) throw new Error("Состояние ответа ещё не подтверждено");
    const priorCase = current, priorDraft = draft;
    state = snapshot; draft = null; busy = false;
    current = data.cases.find(c => !state.records[c.case_id]) || data.cases[0];
    try { render(); }
    catch (error) { current = priorCase; draft = priorDraft; trace("render_failed", {message: error.message}); throw error; }
    // Preserve the draft until the next case was actually rendered successfully.
    removeDraft(record.case_id, record.submission_id);
    recoveryAttempt = 0; clearTimeout(timer); $("recover").hidden = true;
    trace("save_confirmed", {case_id: record.case_id, revision: record.revision, next_case_id: current.case_id});
    message("Ответ сохранён");
    window.scrollTo({top: 0, behavior: "instant"});
    if (state.frozen) {
      const link = document.createElement("a"); link.href = "/api/export"; link.download = "DEV_HUMAN_TRUTH_WAVE1.json"; link.click();
    } else {
      const nextDraft = readDraft(current.case_id);
      if (nextDraft) {
        const nextCase = current;
        setTimeout(() => { if (current === nextCase && !busy && !draft) openCase(nextCase); }, 0);
      }
    }
  }
  function unconfirmed(error) {
    if (error) trace("save_unconfirmed", {error: error.name, message: error.message});
    busy = false;
    message(storageAvailable ? FAILED : "Ответ пока не подтверждён. Выбранный ответ остаётся в этом окне. Хранилище черновиков недоступно; не закрывайте окно.", true);
    $("recover").hidden = false; $("recover").disabled = false; renderDecision();
    if (recoveryAttempt < 3) timer = setTimeout(recover, [1000, 3000, 8000][recoveryAttempt++]);
  }
  async function conflict(error) {
    state = await api("/api/state"); busy = false; draft.pending = false; draft.conflict = true; writeDraft();
    clearTimeout(timer); $("recover").hidden = true; render();
    message(error.message + ". Черновик оставлен здесь; выберите ответ ещё раз для исправления.", true);
  }
  async function recover() {
    if (!draft?.pending || busy) return;
    clearTimeout(timer); busy = true; $("recover").disabled = true;
    try {
      renderDecision();
      const receipt = await api(`/api/submissions/${draft.request.submission_id}`);
      if (receipt.record) return await confirmed(receipt.record, receipt);
      // Same id and same body: safe even if the original request is still in flight.
      const result = await api("/api/answers", draft.request);
      await confirmed(result.record, result);
    } catch (error) {
      if (error.status === 409) { try { await conflict(error); return; } catch (_) { /* keep pending */ } }
      unconfirmed(error);
    } finally {
      busy = false; $("recover").disabled = false; renderDecision();
    }
  }
  async function save() {
    if ($("save").disabled) return;
    if (draft?.pending) { recoveryAttempt = 0; await recover(); return; }
    draft.pending = true; writeDraft(); busy = true; recoveryAttempt = 0;
    try {
      renderDecision(); message("Сохраняем ответ…");
      const result = await api("/api/answers", draft.request); await confirmed(result.record, result);
    }
    catch (error) {
      if (error.status === 409) { try { await conflict(error); return; } catch (_) { /* keep pending */ } }
      unconfirmed(error);
    } finally {
      busy = false; $("recover").disabled = false; renderDecision();
    }
  }
  async function start() {
    try {
      data = await api("/api/bootstrap");
      if (data.namespace !== "FOUNDATION_V3_DEV_WAVE1") throw new Error("Открыт другой набор вопросов");
      state = data;
      try {
        const previous = JSON.parse(localStorage.getItem(`${data.namespace}:${data.packet_sha256}:diagnostics`) || "[]");
        if (Array.isArray(previous)) events = [...previous, ...events].slice(-30);
      } catch (_) { /* Draft and truth recovery do not depend on diagnostics. */ }
      for (const [value, label] of Object.entries(data.reasons)) { const option = document.createElement("option"); option.value = value; option.textContent = label; $("problem-reason").append(option); }
      $("save").onclick = save; $("problem").onclick = () => select("BROKEN_CASE");
      $("case-select").onchange = () => { message(""); openCase(data.cases.find(c => c.case_id === $("case-select").value)); };
      $("problem-reason").onchange = () => { draft.request.problem_reason = $("problem-reason").value || null; writeDraft(); renderDecision(); };
      $("note").oninput = () => { draft.request.note = $("note").value; writeDraft(); };
      $("recover").onclick = () => { recoveryAttempt = 0; recover(); };
      window.addEventListener("online", () => { recoveryAttempt = 0; recover(); });
      started = true;
      let resumedRecord = null;
      for (const c of data.cases) {
        const savedDraft = readDraft(c.case_id), record = state.records[c.case_id];
        if (savedDraft && record?.submission_id === savedDraft.request.submission_id) {
          removeDraft(c.case_id, record.submission_id); resumedRecord = record;
        }
      }
      const pending = data.cases.find(c => readDraft(c.case_id)?.pending);
      current = pending || data.cases.find(c => !state.records[c.case_id]) || data.cases[0];
      message(""); $("recover").hidden = true; $("reconnect").hidden = true; openCase(current);
      if (resumedRecord && !draft) { message("Ответ сохранён"); trace("saved_draft_reconciled", {case_id: resumedRecord.case_id, revision: resumedRecord.revision}); }
    } catch (_) {
      message("Не удалось подключиться к сохранённым ответам. Проверьте, что сервис запущен, и повторите подключение.", true);
      $("reconnect").hidden = false;
      $("reconnect").onclick = () => { if (!started) start(); };
    }
  }
  function handleUnexpected(error) {
    trace("frontend_error", {message: String(error?.message || error)});
    if (draft?.pending) unconfirmed(error);
  }
  window.addEventListener("error", event => handleUnexpected(event.error || event.message));
  window.addEventListener("unhandledrejection", event => handleUnexpected(event.reason));
  start();
})();
