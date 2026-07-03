(function () {
    const $ = (id) => document.getElementById(id);

    const elements = {
        refreshBtn: $("refreshBtn"),
        autoRefreshToggle: $("autoRefreshToggle"),
        autoRefreshSeconds: $("autoRefreshSeconds"),
        connectionBadge: $("connectionBadge"),
        connectionMeta: $("connectionMeta"),
        hostScopeValue: $("hostScopeValue"),
        hostScopeMeta: $("hostScopeMeta"),
        estimateScopeValue: $("estimateScopeValue"),
        estimateScopeMeta: $("estimateScopeMeta"),
        metricCards: $("metricCards"),
        modelsBody: $("modelsBody"),
        modelsCountPill: $("modelsCountPill"),
        serverEndpoint: $("serverEndpoint"),
        actionResult: $("actionResult"),
        serverCards: $("serverCards"),
        probeServersBtn: $("probeServersBtn"),
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

    function formatGiB(value) {
        return value == null ? "—" : `${Number(value).toFixed(2)} GiB`;
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

    function renderConnection(status) {
        const ok = !!status.ok;
        const health = status.health || {};
        elements.connectionBadge.innerHTML =
            `<span class="status-dot ${ok ? "status-dot--ok" : "status-dot--bad"}">${ok ? "OK" : "Проблема"}</span>`;
        elements.connectionMeta.innerHTML = `
            <div><span class="mono">${escapeHtml(status.endpoint || "—")}</span></div>
            <div>auth: <b>${escapeHtml(status.auth_mode || "—")}</b>${status.error ? ` • <span style="color:#ba3652">${escapeHtml(String(status.error))}</span>` : ""}</div>
        `;
        elements.hostScopeValue.textContent = status.ok
            ? `${status.model_count} (🖼 ${status.vision_count} vision)`
            : "—";
        elements.hostScopeMeta.textContent = status.native_management_available
            ? "нативное управление доступно"
            : "нативное управление недоступно (сервер сам управляет)";
        elements.estimateScopeValue.textContent = health.alive
            ? `${health.latency_ms != null ? health.latency_ms + " мс" : "—"}`
            : "нет ответа";
        elements.estimateScopeMeta.textContent = health.alive
            ? "GET /v1/models"
            : escapeHtml(health.error || "endpoint недоступен");
        if (elements.serverEndpoint) elements.serverEndpoint.textContent = status.endpoint || "—";
    }

    function renderMetricCards(status) {
        const host = status.audit_host || {};
        const ram = host.ram || {};
        const swap = host.swap || {};
        const cpu = host.cpu || {};
        const cards = [
            {
                label: "RAM audit-сервера",
                value: `${formatGiB(ram.used_gib)} / ${formatGiB(ram.total_gib)}`,
                meta: `Свободно ${formatGiB(ram.available_gib)} • ${ram.percent ?? "—"}%`,
            },
            {
                label: "Swap audit-сервера",
                value: `${formatGiB(swap.used_gib)} / ${formatGiB(swap.total_gib)}`,
                meta: `Свободно ${formatGiB(swap.free_gib)} • ${swap.percent ?? "—"}%`,
            },
            {
                label: "CPU audit-сервера",
                value: `${cpu.percent ?? "—"}%`,
                meta: `${cpu.physical_cores ?? "—"} физ. / ${cpu.logical_cores ?? "—"} логических`,
            },
            {
                label: "GPU / VRAM",
                value: "на LLM-хосте",
                meta: escapeHtml(status.gpu_note || "GPU на удалённом LLM-хосте — локально недоступен"),
            },
        ];
        elements.metricCards.innerHTML = cards.map((card) => `
            <div class="metric-card">
                <div class="metric-card__label">${escapeHtml(card.label)}</div>
                <div class="metric-card__value">${escapeHtml(card.value)}</div>
                <div class="metric-card__meta">${escapeHtml(card.meta)}</div>
            </div>
        `).join("");
    }

    function renderModels(status) {
        const models = status.models || [];
        elements.modelsCountPill.textContent = `${models.length} моделей`;
        if (!models.length) {
            elements.modelsBody.innerHTML =
                `<tr><td colspan="5" class="empty-row">${status.ok ? "Сервер вернул пустой список." : "Не удалось получить список (см. статус выше)."}</td></tr>`;
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
            return `
                <tr${m.used_by ? ' style="background:rgba(11,143,119,0.05)"' : ""}>
                    <td><span class="mono">${escapeHtml(m.id)}</span>${measuredMark}</td>
                    <td>${visionChip}</td>
                    <td>${escapeHtml(kind)}</td>
                    <td>${reasoningChip}</td>
                    <td>${usedChip}</td>
                </tr>
            `;
        }).join("");
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
            renderConnection(status);
            renderMetricCards(status);
            renderModels(status);
        } catch (error) {
            elements.connectionBadge.innerHTML =
                `<span class="status-dot status-dot--bad">Проблема</span>`;
            elements.connectionMeta.innerHTML =
                `<div style="color:#ba3652">${escapeHtml(String(error))}</div>`;
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
    }

    async function init() {
        bindEvents();
        await Promise.all([refreshStatus(), loadServerProfiles()]);
        startAutoRefresh();
    }

    window.addEventListener("beforeunload", stopAutoRefresh);
    document.addEventListener("DOMContentLoaded", init);
})();
