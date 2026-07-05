(function () {
    const $ = (id) => document.getElementById(id);

    const elements = {
        refreshBtn: $("refreshBtn"),
        autoRefreshToggle: $("autoRefreshToggle"),
        autoRefreshSeconds: $("autoRefreshSeconds"),
        loadedModelsBody: $("loadedModelsBody"),
        loadedCountPill: $("loadedCountPill"),
        unloadAllBtn: $("unloadAllBtn"),
        modelsBody: $("modelsBody"),
        modelsCountPill: $("modelsCountPill"),
        serverEndpoint: $("serverEndpoint"),
        actionResult: $("actionResult"),
        serverCards: $("serverCards"),
        probeServersBtn: $("probeServersBtn"),
        // Модалка выбора объёма контекста при загрузке модели
        loadModal: $("loadModal"),
        loadModalModel: $("loadModalModel"),
        loadModalCtx: $("loadModalCtx"),
        loadModalPresets: $("loadModalPresets"),
        loadModalHint: $("loadModalHint"),
        loadModalCancel: $("loadModalCancel"),
        loadModalConfirm: $("loadModalConfirm"),
    };

    const state = { timer: null, lastStatus: null, profiles: null, probes: null, switching: false };

    function escapeHtml(value) {
        return String(value ?? "")
            .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
    }

    async function api(path) {
        const response = await fetch(path);
        const data = await response.json();
        if (!response.ok) throw new Error(data.detail || `HTTP ${response.status}`);
        return data;
    }

    const KIND_LABEL = {
        vlm: "VLM", ocr: "OCR", embedding: "эмбеддинг", asr: "ASR",
        reranker: "reranker", coder: "coder", llm: "LLM (текст)", unknown: "?",
    };

    function setBusy(button, busy, busyText) {
        if (!button) return;
        if (!button.dataset.idleText) button.dataset.idleText = button.textContent;
        button.disabled = busy;
        button.textContent = busy ? busyText : button.dataset.idleText;
    }

    function fmtCtx(n) {
        return n == null ? null : Number(n).toLocaleString("ru-RU");
    }

    function renderLoadedNow(status) {
        // Endpoint для примечания панели «Управление моделями».
        if (elements.serverEndpoint) elements.serverEndpoint.textContent = status.endpoint || "—";
        if (!elements.loadedModelsBody) return;
        const loaded = status.loaded_models
            || (status.models || []).filter((m) => m.loaded);
        if (elements.loadedCountPill) {
            elements.loadedCountPill.textContent = status.native_state_available
                ? `${loaded.length} загружено`
                : "нет данных";
        }
        if (elements.unloadAllBtn) elements.unloadAllBtn.disabled = !loaded.length;
        // Сервер не сообщил состояние загрузки (native /api/v0/models недоступен).
        if (!status.native_state_available) {
            elements.loadedModelsBody.innerHTML =
                `<tr><td colspan="6" class="empty-row">Сервер не сообщил состояние загрузки` +
                ` (native <span class="mono">/api/v0/models</span> недоступен).</td></tr>`;
            return;
        }
        if (!loaded.length) {
            elements.loadedModelsBody.innerHTML =
                `<tr><td colspan="6" class="empty-row">Сейчас в памяти нет загруженных моделей.</td></tr>`;
            return;
        }
        // Используемые пайплайном — выше, затем по имени.
        const sorted = [...loaded].sort((a, b) => {
            if (!!b.used_by - !!a.used_by) return !!b.used_by - !!a.used_by;
            return String(a.id).localeCompare(String(b.id), "ru");
        });
        elements.loadedModelsBody.innerHTML = sorted.map((m) => {
            const visionChip = m.vision
                ? `<span class="status-dot status-dot--ok">🖼 да</span>`
                : `<span class="muted">—</span>`;
            const kind = KIND_LABEL[m.kind] || m.kind || "?";
            const lc = fmtCtx(m.loaded_context);
            const mc = fmtCtx(m.max_context);
            const ctx = lc ? `${lc}${mc ? " / " + mc : ""}` : "—";
            const usedChip = m.used_by
                ? `<span class="status-dot status-dot--ok">${escapeHtml(m.used_by)}</span>`
                : `<span class="muted">—</span>`;
            return `
                <tr style="background:rgba(11,143,119,0.05)">
                    <td><span class="mono">${escapeHtml(m.id)}</span></td>
                    <td>${visionChip}</td>
                    <td>${escapeHtml(kind)}</td>
                    <td><span class="mono">${escapeHtml(ctx)}</span></td>
                    <td>${usedChip}</td>
                    <td><button class="btn btn--ghost btn--sm" data-unload="${escapeHtml(m.id)}">Выгрузить</button></td>
                </tr>
            `;
        }).join("");
        elements.loadedModelsBody.querySelectorAll("[data-unload]").forEach((el) => {
            el.addEventListener("click", () => doUnload(el.dataset.unload, el));
        });
    }

    function renderModels(status) {
        const models = status.models || [];
        elements.modelsCountPill.textContent = `${models.length} моделей`;
        if (!models.length) {
            elements.modelsBody.innerHTML =
                `<tr><td colspan="6" class="empty-row">${status.ok ? "Сервер вернул пустой список." : "Не удалось получить список (см. статус выше)."}</td></tr>`;
            return;
        }
        // Сортировка: используемые пайплайном → vision → по имени
        const sorted = [...models].sort((a, b) => {
            if (!!b.used_by - !!a.used_by) return !!b.used_by - !!a.used_by;
            if (b.vision - a.vision) return b.vision - a.vision;
            return String(a.id).localeCompare(String(b.id), "ru");
        });
        elements.modelsBody.innerHTML = sorted.map((m) => {
            const visionChip = m.vision
                ? `<span class="status-dot status-dot--ok">🖼 да</span>`
                : `<span class="muted">—</span>`;
            const reasoningChip = m.reasoning ? `<b>🧠 да</b>` : `<span class="muted">—</span>`;
            const usedChip = m.used_by
                ? `<span class="status-dot status-dot--ok">${escapeHtml(m.used_by)}</span>`
                : `<span class="muted">—</span>`;
            const kind = KIND_LABEL[m.kind] || m.kind || "?";
            const measuredMark = m.measured ? "" : ' <span class="muted" title="классифицировано эвристикой">≈</span>';
            const maxCtx = m.max_context || 262144;
            const loadedMark = m.loaded ? ' <span class="status-dot status-dot--ok" title="загружена сейчас">●</span>' : "";
            const loadBtn = `<button class="btn btn--primary btn--sm" data-load="${escapeHtml(m.id)}" data-max="${maxCtx}"${m.loaded ? "" : ""}>${m.loaded ? "Перезагрузить" : "Загрузить"}</button>`;
            return `
                <tr${m.used_by ? ' style="background:rgba(11,143,119,0.05)"' : ""}>
                    <td><span class="mono">${escapeHtml(m.id)}</span>${measuredMark}${loadedMark}</td>
                    <td>${visionChip}</td>
                    <td>${escapeHtml(kind)}</td>
                    <td>${reasoningChip}</td>
                    <td>${usedChip}</td>
                    <td>${loadBtn}</td>
                </tr>
            `;
        }).join("");
        elements.modelsBody.querySelectorAll("[data-load]").forEach((el) => {
            el.addEventListener("click", () => openLoadModal(el.dataset.load, Number(el.dataset.max) || 262144));
        });
    }

    async function apiPost(path, body) {
        const response = await fetch(path, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body || {}),
        });
        const data = await response.json();
        if (!response.ok) throw new Error(data.detail || `HTTP ${response.status}`);
        return data;
    }

    // ─── Загрузка / выгрузка моделей ───

    function errText(e) {
        if (!e) return "не удалось выполнить";
        if (typeof e === "string") return e;
        if (typeof e === "object") return e.message || e.type || JSON.stringify(e);
        return String(e);
    }

    function showActionResult(title, html, isError) {
        const box = elements.actionResult;
        if (!box) return;
        box.style.display = "";
        box.innerHTML = `
            <div class="result-box__title">${escapeHtml(title)}</div>
            <div${isError ? ' style="color:#ba3652"' : ""}>${html}</div>
        `;
    }

    const CTX_PRESETS = [4096, 8192, 16384, 32768, 65536, 131072];
    const CTX_HARD_MAX = 262144;  // потолок LoadModelRequest на бэкенде (le=262144)
    let loadModalTarget = null;

    function openLoadModal(model, maxCtx) {
        // Эффективный максимум: не больше жёсткого лимита бэкенда.
        const effMax = Math.min(maxCtx || CTX_HARD_MAX, CTX_HARD_MAX);
        loadModalTarget = { model, maxCtx: effMax };
        if (elements.loadModalModel) elements.loadModalModel.textContent = model;
        if (elements.loadModalHint) {
            const capped = (maxCtx && maxCtx > CTX_HARD_MAX)
                ? ` (у модели ${fmtCtx(maxCtx)}, но сервер грузит не более ${fmtCtx(CTX_HARD_MAX)})`
                : "";
            elements.loadModalHint.textContent = `Максимум: ${fmtCtx(effMax)} токенов${capped}.`;
        }
        // Пресеты ≤ эффективного максимума + сам максимум
        if (elements.loadModalPresets) {
            const presets = CTX_PRESETS.filter((v) => v < effMax).concat([effMax]);
            elements.loadModalPresets.innerHTML = presets.map((v) =>
                `<button type="button" class="btn btn--ghost btn--sm" data-preset="${v}">${v === effMax ? "Max" : fmtCtx(v)}</button>`
            ).join("");
            elements.loadModalPresets.querySelectorAll("[data-preset]").forEach((el) => {
                el.addEventListener("click", () => { elements.loadModalCtx.value = el.dataset.preset; });
            });
        }
        if (elements.loadModalCtx) {
            elements.loadModalCtx.max = String(effMax);
            // Разумный дефолт: 32768 или максимум, если он меньше
            elements.loadModalCtx.value = String(Math.min(32768, effMax));
        }
        if (elements.loadModal) elements.loadModal.style.display = "flex";
    }

    function closeLoadModal() {
        loadModalTarget = null;
        if (elements.loadModal) elements.loadModal.style.display = "none";
    }

    async function confirmLoad() {
        if (!loadModalTarget) return;
        const model = loadModalTarget.model;
        const maxCtx = loadModalTarget.maxCtx;
        let ctx = parseInt(elements.loadModalCtx && elements.loadModalCtx.value, 10);
        if (!Number.isFinite(ctx) || ctx < 512) ctx = 512;
        if (ctx > maxCtx) ctx = maxCtx;
        setBusy(elements.loadModalConfirm, true, "Загружаю...");
        try {
            const res = await apiPost("/api/model-control/load", { model, context_length: ctx });
            closeLoadModal();
            if (res && res.ok === false) {
                showActionResult(`Загрузка ${model}`, escapeHtml(errText(res.error)), true);
            } else {
                showActionResult(`Загрузка ${model}`, `Загружено с контекстом <b>${fmtCtx(ctx)}</b> токенов. Обновляю состояние...`, false);
            }
        } catch (error) {
            showActionResult(`Загрузка ${model}`, escapeHtml(String(error)), true);
        } finally {
            setBusy(elements.loadModalConfirm, false);
            refreshStatus({ silent: true });
        }
    }

    async function doUnload(instanceId, button) {
        if (!window.confirm(`Выгрузить модель «${instanceId}» из памяти сервера?`)) return;
        setBusy(button, true, "Выгружаю...");
        try {
            const res = await apiPost("/api/model-control/unload", { instance_id: instanceId });
            if (res && res.ok === false) {
                showActionResult(`Выгрузка ${instanceId}`, escapeHtml(errText(res.error)), true);
            } else {
                showActionResult(`Выгрузка ${instanceId}`, "Выгружено. Обновляю состояние...", false);
            }
        } catch (error) {
            showActionResult(`Выгрузка ${instanceId}`, escapeHtml(String(error)), true);
        } finally {
            refreshStatus({ silent: true });
        }
    }

    async function doUnloadAll() {
        if (!window.confirm("Выгрузить ВСЕ загруженные модели из памяти сервера?")) return;
        setBusy(elements.unloadAllBtn, true, "Выгружаю...");
        try {
            const res = await apiPost("/api/model-control/unload-all", {});
            showActionResult("Выгрузка всех", `Выгружено instance: <b>${res.count ?? 0}</b>. Обновляю состояние...`, res && res.ok === false);
        } catch (error) {
            showActionResult("Выгрузка всех", escapeHtml(String(error)), true);
        } finally {
            setBusy(elements.unloadAllBtn, false);
            refreshStatus({ silent: true });
        }
    }

    // ─── Переключатель серверов обработки ───

    function healthHtml(probe) {
        if (!probe) return `<span class="muted">проверка...</span>`;
        if (probe.alive) {
            const lat = probe.latency_ms != null ? ` • ${probe.latency_ms} мс` : "";
            return `<span class="status-dot status-dot--ok">жив</span> <span class="muted">${probe.model_count ?? "?"} моделей${lat}</span>`;
        }
        return `<span class="status-dot status-dot--bad">не отвечает</span> <span class="muted" title="${escapeHtml(probe.error || "")}">${escapeHtml((probe.error || "нет ответа").slice(0, 60))}</span>`;
    }

    function renderServerCards() {
        const data = state.profiles;
        if (!elements.serverCards) return;
        if (!data || !data.profiles) {
            elements.serverCards.innerHTML = `<div class="empty-row">Не удалось загрузить профили серверов.</div>`;
            return;
        }
        const probes = (state.probes && state.probes.probes) || {};
        elements.serverCards.innerHTML = data.profiles.map((p) => {
            const active = p.active;
            const probe = probes[p.id];
            const btn = active
                ? `<button class="btn btn--ghost" disabled>Активен сейчас</button>`
                : `<button class="btn btn--primary" data-switch="${escapeHtml(p.id)}" data-label="${escapeHtml(p.label)}">Переключиться</button>`;
            return `
                <div class="server-card${active ? " server-card--active" : ""}">
                    <div class="server-card__top">
                        <span class="server-card__title">${escapeHtml(p.label)}</span>
                        ${active ? `<span class="server-card__badge">активен</span>` : ""}
                    </div>
                    <div class="server-card__url">${escapeHtml(p.base_url)} <span class="muted">(${escapeHtml(p.auth_mode)})</span></div>
                    <div class="server-card__desc">${escapeHtml(p.description)}</div>
                    <div class="server-card__health">${healthHtml(probe)}</div>
                    <div class="server-card__foot">${btn}</div>
                </div>
            `;
        }).join("");
        elements.serverCards.querySelectorAll("[data-switch]").forEach((el) => {
            el.addEventListener("click", () => switchServer(el.dataset.switch, el.dataset.label, probes[el.dataset.switch]));
        });
    }

    async function loadServerProfiles() {
        if (!elements.serverCards) return;
        try {
            state.profiles = await api("/api/model-control/server-profiles");
            renderServerCards();
        } catch (error) {
            elements.serverCards.innerHTML = `<div class="empty-row" style="color:#ba3652">Профили серверов: ${escapeHtml(String(error))}</div>`;
            return;
        }
        probeServers({ silent: true });
    }

    async function probeServers({ silent = false } = {}) {
        if (!silent) setBusy(elements.probeServersBtn, true, "Проверяю...");
        try {
            state.probes = await api("/api/model-control/server-profiles/probe");
            renderServerCards();
        } catch (error) {
            /* проба не критична — карточки уже отрисованы */
        } finally {
            if (!silent) setBusy(elements.probeServersBtn, false);
        }
    }

    async function switchServer(profileId, label, probe) {
        if (state.switching) return;
        let warn = `Переключить весь пайплайн на «${label}»?\n\nЭто перепишет .env и перезапустит backend (~5–10 с).\nИдущие аудиты будут прерваны.`;
        if (probe && probe.alive === false) {
            warn += `\n\n⚠ Целевой сервер сейчас НЕ отвечает на пробу — переключение может не помочь.`;
        }
        if (!window.confirm(warn)) return;

        state.switching = true;
        stopAutoRefresh();
        elements.serverCards.innerHTML = `<div class="empty-row">Переключаю на «${escapeHtml(label)}» и перезапускаю backend...</div>`;
        try {
            await apiPost("/api/model-control/server-profiles/activate", { profile_id: profileId });
        } catch (error) {
            elements.serverCards.innerHTML = `<div class="empty-row" style="color:#ba3652">Ошибка переключения: ${escapeHtml(String(error))}</div>`;
            state.switching = false;
            return;
        }
        await waitForBackendAndReload(label);
    }

    async function waitForBackendAndReload(label) {
        const started = Date.now();
        const deadline = started + 60000;
        // Дадим backend реально упасть перед опросом (detached-рестарт ждёт 2 с).
        await new Promise((r) => setTimeout(r, 3500));
        while (Date.now() < deadline) {
            try {
                const resp = await fetch("/api/info", { cache: "no-store" });
                if (resp.ok) {
                    elements.serverCards.innerHTML = `<div class="empty-row">Backend поднялся на «${escapeHtml(label)}». Обновляю страницу...</div>`;
                    await new Promise((r) => setTimeout(r, 600));
                    window.location.reload();
                    return;
                }
            } catch (_) { /* ещё не поднялся */ }
            const secs = Math.round((Date.now() - started) / 1000);
            elements.serverCards.innerHTML = `<div class="empty-row">Ждём backend на «${escapeHtml(label)}»... (${secs} с)</div>`;
            await new Promise((r) => setTimeout(r, 1500));
        }
        elements.serverCards.innerHTML = `<div class="empty-row" style="color:#ba3652">Backend не поднялся за 60 с. Обновите страницу вручную (watchdog поднимет его в течение минуты).</div>`;
        state.switching = false;
    }

    async function refreshStatus({ silent = false } = {}) {
        if (!silent) setBusy(elements.refreshBtn, true, "Обновляю...");
        try {
            const status = await api("/api/model-control/remote-status");
            state.lastStatus = status;
            renderLoadedNow(status);
            renderModels(status);
        } catch (error) {
            const msg = `<tr><td colspan="5" class="empty-row" style="color:#ba3652">${escapeHtml(String(error))}</td></tr>`;
            if (elements.loadedModelsBody) elements.loadedModelsBody.innerHTML = msg;
            if (elements.modelsBody) elements.modelsBody.innerHTML = msg;
            if (elements.loadedCountPill) elements.loadedCountPill.textContent = "ошибка";
        } finally {
            if (!silent) setBusy(elements.refreshBtn, false);
        }
    }

    function startAutoRefresh() {
        stopAutoRefresh();
        if (!elements.autoRefreshToggle || !elements.autoRefreshToggle.checked) return;
        const seconds = Number(elements.autoRefreshSeconds.value || 30);
        state.timer = window.setInterval(() => refreshStatus({ silent: true }), seconds * 1000);
    }

    function stopAutoRefresh() {
        if (state.timer) { window.clearInterval(state.timer); state.timer = null; }
    }

    function bindEvents() {
        if (elements.refreshBtn) elements.refreshBtn.addEventListener("click", () => { refreshStatus(); loadServerProfiles(); });
        if (elements.autoRefreshToggle) elements.autoRefreshToggle.addEventListener("change", startAutoRefresh);
        if (elements.autoRefreshSeconds) elements.autoRefreshSeconds.addEventListener("change", startAutoRefresh);
        if (elements.probeServersBtn) elements.probeServersBtn.addEventListener("click", () => probeServers());
        if (elements.unloadAllBtn) elements.unloadAllBtn.addEventListener("click", doUnloadAll);
        if (elements.loadModalCancel) elements.loadModalCancel.addEventListener("click", closeLoadModal);
        if (elements.loadModalConfirm) elements.loadModalConfirm.addEventListener("click", confirmLoad);
        if (elements.loadModal) elements.loadModal.addEventListener("click", (e) => {
            if (e.target === elements.loadModal) closeLoadModal();  // клик по фону закрывает
        });
    }

    async function init() {
        bindEvents();
        await Promise.all([refreshStatus(), loadServerProfiles()]);
        startAutoRefresh();
    }

    window.addEventListener("beforeunload", stopAutoRefresh);
    document.addEventListener("DOMContentLoaded", init);
})();
