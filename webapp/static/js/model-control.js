(function () {
    const $ = (id) => document.getElementById(id);

    const elements = {
        refreshBtn: $("refreshBtn"),
        autoRefreshToggle: $("autoRefreshToggle"),
        autoRefreshSeconds: $("autoRefreshSeconds"),
        selectedModel: $("selectedModel"),
        contextLength: $("contextLength"),
        flashAttention: $("flashAttention"),
        offloadKvCache: $("offloadKvCache"),
        evalBatchSize: $("evalBatchSize"),
        numExperts: $("numExperts"),
        estimateGpu: $("estimateGpu"),
        estimateBtn: $("estimateBtn"),
        loadBtn: $("loadBtn"),
        unloadAllBtn: $("unloadAllBtn"),
        quickContexts: $("quickContexts"),
        estimateBox: $("estimateBox"),
        actionResult: $("actionResult"),
        connectionBadge: $("connectionBadge"),
        connectionMeta: $("connectionMeta"),
        hostScopeValue: $("hostScopeValue"),
        hostScopeMeta: $("hostScopeMeta"),
        estimateScopeValue: $("estimateScopeValue"),
        estimateScopeMeta: $("estimateScopeMeta"),
        metricCards: $("metricCards"),
        loadedInstancesBody: $("loadedInstancesBody"),
        modelsBody: $("modelsBody"),
        processBody: $("processBody"),
        loadedCountPill: $("loadedCountPill"),
        modelsCountPill: $("modelsCountPill"),
        modelHint: $("modelHint"),
    };

    const state = {
        models: [],
        loadedInstances: [],
        timer: null,
        lastStatus: null,
    };

    function escapeHtml(value) {
        return String(value ?? "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;")
            .replace(/"/g, "&quot;")
            .replace(/'/g, "&#39;");
    }

    async function api(path, { method = "GET", body } = {}) {
        const response = await fetch(path, {
            method,
            headers: body ? { "Content-Type": "application/json" } : {},
            body: body ? JSON.stringify(body) : undefined,
        });
        const data = await response.json();
        if (!response.ok) {
            throw new Error(data.detail || `HTTP ${response.status}`);
        }
        return data;
    }

    function formatGiB(value) {
        return value == null ? "—" : `${Number(value).toFixed(2)} GiB`;
    }

    function formatMiB(value) {
        return value == null ? "—" : `${Number(value).toFixed(1)} MiB`;
    }

    function formatCount(value) {
        return value == null ? "—" : Number(value).toLocaleString("ru-RU");
    }

    function formatSeconds(value) {
        return value == null ? "—" : `${Number(value).toFixed(2)} c`;
    }

    function formatBytesToGiB(value) {
        if (value == null) return "—";
        return `${(Number(value) / (1024 ** 3)).toFixed(2)} GiB`;
    }

    function modelMetaByKey(key) {
        return state.models.find((item) => item.key === key) || null;
    }

    function persistForm() {
        const payload = {
            model: elements.selectedModel.value,
            contextLength: elements.contextLength.value,
            flashAttention: elements.flashAttention.checked,
            offloadKvCache: elements.offloadKvCache.checked,
            evalBatchSize: elements.evalBatchSize.value,
            numExperts: elements.numExperts.value,
            estimateGpu: elements.estimateGpu.value,
        };
        localStorage.setItem("model-control-form", JSON.stringify(payload));
    }

    function restoreForm() {
        try {
            const raw = localStorage.getItem("model-control-form");
            if (!raw) return;
            const saved = JSON.parse(raw);
            if (saved.contextLength) elements.contextLength.value = saved.contextLength;
            if (typeof saved.flashAttention === "boolean") elements.flashAttention.checked = saved.flashAttention;
            if (typeof saved.offloadKvCache === "boolean") elements.offloadKvCache.checked = saved.offloadKvCache;
            if (saved.evalBatchSize) elements.evalBatchSize.value = saved.evalBatchSize;
            if (saved.numExperts) elements.numExperts.value = saved.numExperts;
            if (saved.estimateGpu) elements.estimateGpu.value = saved.estimateGpu;
            if (saved.model) elements.selectedModel.dataset.pendingValue = saved.model;
        } catch (error) {
            console.warn("restoreForm failed", error);
        }
    }

    function collectFormPayload() {
        persistForm();
        const contextLength = Number(elements.contextLength.value || 0);
        const evalBatchSize = elements.evalBatchSize.value ? Number(elements.evalBatchSize.value) : null;
        const numExperts = elements.numExperts.value ? Number(elements.numExperts.value) : null;
        return {
            model: elements.selectedModel.value,
            context_length: contextLength,
            flash_attention: elements.flashAttention.checked,
            offload_kv_cache_to_gpu: elements.offloadKvCache.checked,
            eval_batch_size: evalBatchSize,
            num_experts: numExperts,
            gpu: elements.estimateGpu.value.trim() || null,
        };
    }

    function setActionResult(title, payload, ok = true) {
        const body = `
            <div class="result-box__title">${escapeHtml(title)}</div>
            <pre>${escapeHtml(JSON.stringify(payload, null, 2))}</pre>
        `;
        elements.actionResult.innerHTML = body;
        elements.actionResult.style.borderColor = ok ? "rgba(11, 143, 119, 0.2)" : "rgba(186, 54, 82, 0.2)";
    }

    function renderEstimate(payload) {
        if (!payload) {
            elements.estimateBox.innerHTML = `
                <div class="estimate-box__title">Оценка памяти</div>
                <div class="estimate-box__empty">Нажмите «Оценить память», чтобы прикинуть RAM/VRAM под текущий контекст.</div>
            `;
            return;
        }
        const verdict = payload.estimate_verdict || payload.error || (payload.ok ? "OK" : "Ошибка");
        elements.estimateBox.innerHTML = `
            <div class="estimate-box__title">Оценка памяти</div>
            <div class="estimate-grid">
                <div class="estimate-stat">
                    <div class="estimate-stat__label">GPU memory</div>
                    <div class="estimate-stat__value">${payload.estimated_gpu_memory_gb != null ? `${payload.estimated_gpu_memory_gb.toFixed(2)} GB` : "—"}</div>
                </div>
                <div class="estimate-stat">
                    <div class="estimate-stat__label">Total memory</div>
                    <div class="estimate-stat__value">${payload.estimated_total_memory_gb != null ? `${payload.estimated_total_memory_gb.toFixed(2)} GB` : "—"}</div>
                </div>
            </div>
            <div class="field__hint" style="margin-top:12px"><b>Вердикт:</b> ${escapeHtml(verdict)}</div>
            <pre style="margin-top:12px">${escapeHtml(payload.raw_output || "Нет вывода")}</pre>
        `;
    }

    function setBusy(button, busy, busyText, idleText) {
        if (!button) return;
        if (!button.dataset.idleText) {
            button.dataset.idleText = idleText || button.textContent;
        }
        button.disabled = busy;
        button.textContent = busy ? busyText : button.dataset.idleText;
    }

    function renderMetricCards(status) {
        const ram = status.system?.ram || {};
        const swap = status.system?.swap || {};
        const cpu = status.system?.cpu || {};
        const gpu = status.gpu || {};
        const firstGpu = (gpu.gpus || [])[0] || null;
        const cards = [
            {
                label: "RAM хоста",
                value: `${formatGiB(ram.used_gib)} / ${formatGiB(ram.total_gib)}`,
                meta: `Свободно ${formatGiB(ram.available_gib)} • ${ram.percent ?? "—"}%`,
            },
            {
                label: "Swap хоста",
                value: `${formatGiB(swap.used_gib)} / ${formatGiB(swap.total_gib)}`,
                meta: `Свободно ${formatGiB(swap.free_gib)} • ${swap.percent ?? "—"}%`,
            },
            {
                label: "CPU",
                value: `${cpu.percent ?? "—"}%`,
                meta: `${cpu.physical_cores ?? "—"} физ. / ${cpu.logical_cores ?? "—"} логических`,
            },
            {
                label: "GPU / VRAM",
                value: firstGpu ? `${firstGpu.memory_used_gib.toFixed(2)} / ${firstGpu.memory_total_gib.toFixed(2)} GiB` : "Недоступно",
                meta: firstGpu
                    ? `${escapeHtml(firstGpu.name)} • ${firstGpu.utilization_gpu_pct}% GPU • ${firstGpu.temperature_c}°C`
                    : escapeHtml(gpu.error || "На этом хосте нет nvidia-smi"),
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

    function renderConnection(status) {
        const chandra = status.chandra || {};
        const notes = status.notes || {};
        const ok = chandra.configured && chandra.reachable;
        elements.connectionBadge.innerHTML = `<span class="status-dot ${ok ? "status-dot--ok" : "status-dot--bad"}">${ok ? "Подключено" : "Проблема"}</span>`;
        elements.connectionMeta.innerHTML = `
            <div><span class="mono">${escapeHtml(chandra.base_url || "—")}</span></div>
            <div>Пинг: ${formatSeconds(chandra.elapsed_s)}${chandra.error ? ` • ${escapeHtml(String(chandra.error))}` : ""}</div>
        `;
        elements.hostScopeMeta.textContent = notes.host_metrics_scope || "current webapp host";
        elements.estimateScopeMeta.textContent = notes.estimate_scope || "current host lms CLI";
    }

    function syncModelSelect(models) {
        const currentValue = elements.selectedModel.value || elements.selectedModel.dataset.pendingValue || "";
        const options = models.map((model) => {
            const caps = model.capabilities || {};
            const tags = [
                caps.vision ? "vision" : "",
                caps.reasoning ? "reasoning" : "",
                (model.loaded_instances || []).length ? "loaded" : "",
            ].filter(Boolean).join(" • ");
            return `<option value="${escapeHtml(model.key)}">${escapeHtml(model.display_name || model.key)}${tags ? ` — ${escapeHtml(tags)}` : ""}</option>`;
        }).join("");
        elements.selectedModel.innerHTML = options || `<option value="">Нет моделей</option>`;
        if (currentValue && models.some((item) => item.key === currentValue)) {
            elements.selectedModel.value = currentValue;
        }
        delete elements.selectedModel.dataset.pendingValue;
        if (!elements.selectedModel.value && models[0]) {
            elements.selectedModel.value = models[0].key;
        }
        updateModelHint();
    }

    function updateModelHint() {
        const meta = modelMetaByKey(elements.selectedModel.value);
        if (!meta) {
            elements.modelHint.textContent = "Выберите модель, чтобы увидеть лимиты и capabilities.";
            return;
        }
        const caps = meta.capabilities || {};
        const reasoning = caps.reasoning?.allowed_options?.join(", ") || (caps.reasoning ? "yes" : "no");
        const loaded = (meta.loaded_instances || []).length;
        elements.modelHint.innerHTML = `
            max context: <span class="mono">${formatCount(meta.max_context_length)}</span> •
            variant: <span class="mono">${escapeHtml(meta.selected_variant || meta.path || "—")}</span> •
            vision: <b>${caps.vision ? "yes" : "no"}</b> •
            reasoning: <b>${escapeHtml(reasoning)}</b> •
            loaded instances: <b>${loaded}</b>
        `;
    }

    function renderLoadedInstances(instances) {
        elements.loadedCountPill.textContent = `${instances.length} instance`;
        if (!instances.length) {
            elements.loadedInstancesBody.innerHTML = `<tr><td colspan="5" class="empty-row">Сейчас ничего не загружено.</td></tr>`;
            return;
        }
        elements.loadedInstancesBody.innerHTML = instances.map((item) => {
            const config = item.config || {};
            const paramBits = [
                `FA: ${config.flash_attention ? "on" : "off"}`,
                `KV→GPU: ${config.offload_kv_cache_to_gpu ? "on" : "off"}`,
                config.eval_batch_size ? `eval_batch: ${config.eval_batch_size}` : "",
                config.num_experts ? `experts: ${config.num_experts}` : "",
            ].filter(Boolean);
            return `
                <tr>
                    <td>
                        <div><b>${escapeHtml(item.display_name || item.model_key)}</b></div>
                        <div class="muted mono">${escapeHtml(item.selected_variant || item.model_key)}</div>
                    </td>
                    <td class="mono">${escapeHtml(item.instance_id)}</td>
                    <td class="mono">${formatCount(config.context_length || item.max_context_length)}</td>
                    <td>${escapeHtml(paramBits.join(" • "))}</td>
                    <td>
                        <button class="tiny-action" data-unload-instance="${escapeHtml(item.instance_id)}">Выгрузить</button>
                    </td>
                </tr>
            `;
        }).join("");
    }

    function renderProcesses(rows, gpuRows) {
        const gpuByPid = new Map((gpuRows || []).map((row) => [row.pid, row]));
        if (!rows.length) {
            elements.processBody.innerHTML = `<tr><td colspan="6" class="empty-row">LM Studio процессы на этом хосте не найдены.</td></tr>`;
            return;
        }
        elements.processBody.innerHTML = rows.map((row) => {
            const gpu = gpuByPid.get(row.pid);
            return `
                <tr>
                    <td class="mono">${escapeHtml(row.pid)}</td>
                    <td>${escapeHtml(row.name || "—")}</td>
                    <td class="mono">${formatMiB(row.rss_mib)}</td>
                    <td class="mono">${gpu ? `${gpu.used_gpu_memory_gib.toFixed(2)} GiB` : "—"}</td>
                    <td>${escapeHtml(row.cpu_percent ?? 0)}%</td>
                    <td class="code-block">${escapeHtml(row.cmdline || "—")}</td>
                </tr>
            `;
        }).join("");
    }

    function renderModels(models) {
        elements.modelsCountPill.textContent = `${models.length} моделей`;
        if (!models.length) {
            elements.modelsBody.innerHTML = `<tr><td colspan="6" class="empty-row">LM Studio не вернул список моделей.</td></tr>`;
            return;
        }
        const sorted = [...models].sort((a, b) => {
            const loadedDiff = (b.loaded_instances || []).length - (a.loaded_instances || []).length;
            if (loadedDiff) return loadedDiff;
            return String(a.display_name || a.key).localeCompare(String(b.display_name || b.key), "ru");
        });
        elements.modelsBody.innerHTML = sorted.map((model) => {
            const caps = model.capabilities || {};
            const sizeBytes = model.size_bytes || 0;
            return `
                <tr>
                    <td>
                        <div><b>${escapeHtml(model.display_name || model.key)}</b></div>
                        <div class="muted mono">${escapeHtml(model.key)}</div>
                    </td>
                    <td>${caps.vision ? "yes" : "no"}</td>
                    <td>${caps.reasoning ? escapeHtml((caps.reasoning.allowed_options || []).join(", ") || "yes") : "no"}</td>
                    <td class="mono">${formatCount(model.max_context_length)}</td>
                    <td class="mono">${formatBytesToGiB(sizeBytes)}</td>
                    <td>${(model.loaded_instances || []).length}</td>
                </tr>
            `;
        }).join("");
    }

    async function refreshStatus({ silent = false } = {}) {
        if (!silent) setBusy(elements.refreshBtn, true, "Обновляю...");
        try {
            const status = await api("/api/model-control/status");
            state.lastStatus = status;
            state.models = status.models || [];
            state.loadedInstances = status.loaded_instances || [];
            renderConnection(status);
            renderMetricCards(status);
            syncModelSelect(state.models);
            renderLoadedInstances(state.loadedInstances);
            renderProcesses(status.processes?.lmstudio || [], status.gpu?.processes || []);
            renderModels(state.models);
        } catch (error) {
            setActionResult("Ошибка обновления статуса", { error: String(error) }, false);
        } finally {
            if (!silent) setBusy(elements.refreshBtn, false);
        }
    }

    function startAutoRefresh() {
        stopAutoRefresh();
        if (!elements.autoRefreshToggle.checked) return;
        const seconds = Number(elements.autoRefreshSeconds.value || 5);
        state.timer = window.setInterval(() => refreshStatus({ silent: true }), seconds * 1000);
    }

    function stopAutoRefresh() {
        if (state.timer) {
            window.clearInterval(state.timer);
            state.timer = null;
        }
    }

    async function handleEstimate() {
        const payload = collectFormPayload();
        setBusy(elements.estimateBtn, true, "Считаю...");
        try {
            const result = await api("/api/model-control/estimate", {
                method: "POST",
                body: {
                    model: payload.model,
                    context_length: payload.context_length,
                    gpu: payload.gpu,
                },
            });
            renderEstimate(result);
            setActionResult("Estimate завершён", result, !!result.ok);
        } catch (error) {
            const payloadError = { error: String(error) };
            renderEstimate(payloadError);
            setActionResult("Estimate завершён с ошибкой", payloadError, false);
        } finally {
            setBusy(elements.estimateBtn, false);
        }
    }

    async function handleLoad() {
        const payload = collectFormPayload();
        setBusy(elements.loadBtn, true, "Загружаю...");
        try {
            const result = await api("/api/model-control/load", {
                method: "POST",
                body: {
                    model: payload.model,
                    context_length: payload.context_length,
                    flash_attention: payload.flash_attention,
                    offload_kv_cache_to_gpu: payload.offload_kv_cache_to_gpu,
                    eval_batch_size: payload.eval_batch_size,
                    num_experts: payload.num_experts,
                },
            });
            setActionResult("Загрузка модели завершена", result, !!result.ok);
            await refreshStatus({ silent: true });
        } catch (error) {
            setActionResult("Загрузка модели завершилась ошибкой", { error: String(error) }, false);
        } finally {
            setBusy(elements.loadBtn, false);
        }
    }

    async function handleUnloadAll() {
        setBusy(elements.unloadAllBtn, true, "Выгружаю...");
        try {
            const result = await api("/api/model-control/unload-all", { method: "POST" });
            setActionResult("Все instance выгружены", result, !!result.ok);
            await refreshStatus({ silent: true });
        } catch (error) {
            setActionResult("Выгрузка завершилась ошибкой", { error: String(error) }, false);
        } finally {
            setBusy(elements.unloadAllBtn, false);
        }
    }

    async function handleUnloadInstance(instanceId) {
        try {
            const result = await api("/api/model-control/unload", {
                method: "POST",
                body: { instance_id: instanceId },
            });
            setActionResult(`Instance ${instanceId} выгружен`, result, !!result.ok);
            await refreshStatus({ silent: true });
        } catch (error) {
            setActionResult(`Не удалось выгрузить ${instanceId}`, { error: String(error) }, false);
        }
    }

    function bindEvents() {
        elements.refreshBtn.addEventListener("click", () => refreshStatus());
        elements.estimateBtn.addEventListener("click", handleEstimate);
        elements.loadBtn.addEventListener("click", handleLoad);
        elements.unloadAllBtn.addEventListener("click", handleUnloadAll);
        elements.selectedModel.addEventListener("change", () => {
            updateModelHint();
            persistForm();
        });
        elements.contextLength.addEventListener("change", persistForm);
        elements.flashAttention.addEventListener("change", persistForm);
        elements.offloadKvCache.addEventListener("change", persistForm);
        elements.evalBatchSize.addEventListener("change", persistForm);
        elements.numExperts.addEventListener("change", persistForm);
        elements.estimateGpu.addEventListener("change", persistForm);
        elements.autoRefreshToggle.addEventListener("change", startAutoRefresh);
        elements.autoRefreshSeconds.addEventListener("change", startAutoRefresh);
        elements.quickContexts.addEventListener("click", (event) => {
            const button = event.target.closest("[data-context]");
            if (!button) return;
            elements.contextLength.value = button.dataset.context;
            persistForm();
        });
        elements.loadedInstancesBody.addEventListener("click", (event) => {
            const button = event.target.closest("[data-unload-instance]");
            if (!button) return;
            handleUnloadInstance(button.dataset.unloadInstance);
        });
    }

    async function init() {
        restoreForm();
        bindEvents();
        renderEstimate(null);
        await refreshStatus();
        startAutoRefresh();
    }

    window.addEventListener("beforeunload", stopAutoRefresh);
    document.addEventListener("DOMContentLoaded", init);
})();
