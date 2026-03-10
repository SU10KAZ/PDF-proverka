/**
 * Audit Manager — SPA на Vue 3.
 * Маршрутизация, состояние, API-вызовы, live-статус.
 */
const { createApp, ref, computed, watch, onMounted, onUnmounted, nextTick } = Vue;

const app = createApp({
    setup() {
        // ─── State ───
        const currentView = ref('dashboard');
        const currentProjectId = ref(null);
        const currentProject = ref(null);
        const projects = ref([]);
        const loading = ref(false);

        // Findings
        const findingsData = ref(null);
        const filterSeverity = ref('');
        const filterSearch = ref('');
        const severityOptions = [
            'КРИТИЧЕСКОЕ', 'ЭКОНОМИЧЕСКОЕ', 'ЭКСПЛУАТАЦИОННОЕ',
            'РЕКОМЕНДАТЕЛЬНОЕ', 'ПРОВЕРИТЬ ПО СМЕЖНЫМ'
        ];

        // Tiles
        const tilesProjectId = ref('');
        const tilePages = ref([]);
        const selectedPage = ref(null);
        const selectedTile = ref(null);
        const tileAnalysis = ref({});
        const tileAnalysisLoading = ref(false);

        // Page analysis (page_summaries)
        const pageSummaries = ref({});       // {page_num: summary} — без full_text
        const pageAnalysis = ref(null);      // полный анализ выбранной страницы
        const pageAnalysisLoading = ref(false);
        const showPageAnalysis = ref(true);  // показать/скрыть панель

        // Blocks (OCR)
        const blocksProjectId = ref('');
        const blockPages = ref([]);
        const selectedBlockPage = ref(null);
        const selectedBlock = ref(null);
        const blockAnalysis = ref({});

        // Optimization
        const optimizationData = ref(null);
        const optimizationLoading = ref(false);
        const optimizationFilter = ref('');  // '' | 'cheaper_analog' | 'faster_install' | 'simpler_design' | 'lifecycle'

        // Document viewer (MD)
        const documentProjectId = ref('');
        const documentPages = ref([]);
        const documentCurrentPage = ref(null);
        const documentPageData = ref(null);
        const documentLoading = ref(false);

        // Log — отдельное хранилище для каждого проекта
        const logProjectId = ref('');
        const projectLogs = ref({});     // {projectId: [{time, level, message}]}
        const logAutoScroll = ref(true);
        const logContainer = ref(null);
        const logLoading = ref(false);

        // logEntries — computed, показывает логи текущего проекта
        const logEntries = computed(() => {
            const pid = logProjectId.value;
            return pid ? (projectLogs.value[pid] || []) : [];
        });

        // Prompts
        const promptsProjectId = ref('');
        const templates = ref([]);
        const promptsLoading = ref(false);
        const activePromptTab = ref(0);
        const promptsDiscipline = ref('');
        const disciplines = ref([]);
        const showDisciplineDropdown = ref(false);
        const currentDiscipline = computed(() => {
            return disciplines.value.find(d => d.code === promptsDiscipline.value) || {};
        });

        // WebSocket
        const wsConnected = ref(false);

        // ─── Live Status (polling) ───
        const liveStatus = ref({ running: {}, batches: {} });
        const elapsedTick = ref(0); // реактивный тик для обновления таймера
        let pollTimer = null;
        let tickTimer = null;

        // ─── Heartbeat ───
        const heartbeatData = ref({});       // {projectId: {stage, elapsed_sec, process_alive, eta_sec, ...}}
        const lastHeartbeatTime = ref({});   // {projectId: timestamp_ms последнего heartbeat}

        // ─── Global Usage (как на дашборде Anthropic) ───
        const globalUsage = ref({
            session_5h_output_tokens: 0, session_5h_input_tokens: 0,
            session_5h_cache_read_tokens: 0, session_5h_cache_create_tokens: 0,
            session_5h_total_tokens: 0, session_5h_messages: 0,
            session_5h_percent: 0, session_5h_limit: 12000000,
            session_5h_resets_in_sec: 0, session_5h_resets_in_text: '',
            weekly_all_output_tokens: 0, weekly_all_input_tokens: 0,
            weekly_all_total_tokens: 0, weekly_all_messages: 0,
            weekly_all_percent: 0, weekly_all_limit: 17000000,
            weekly_resets_at: '', weekly_resets_in_sec: 0,
            weekly_by_model: {},
            scanned_files: 0, scanned_messages: 0, scan_duration_ms: 0,
        });
        const showUsageDetails = ref(false);
        let usagePollTimer = null;

        const sonnetPercent = computed(() => {
            const m = globalUsage.value.weekly_by_model || {};
            return (m.sonnet && m.sonnet.percent) || 0;
        });

        // Старые usageCounters оставляем для совместимости с webapp-трекингом
        const usageCounters = ref({});

        // ─── Per-project usage (токены по проектам/этапам) ───
        const projectUsage = ref({});  // {project_id: {total_tokens, total_cost_usd, total_calls, stages_summary}}

        async function fetchAllProjectUsage() {
            try {
                const data = await api('/usage/projects-summary');
                projectUsage.value = data || {};
            } catch (e) {
                console.error('Failed to load projects usage:', e);
            }
        }

        async function fetchProjectUsage(projectId) {
            try {
                const data = await api(`/usage/project/${encodeURIComponent(projectId)}`);
                if (data && data.total_tokens > 0) {
                    projectUsage.value = { ...projectUsage.value, [projectId]: data };
                }
            } catch (e) {
                console.error('Failed to load project usage:', e);
            }
        }

        // Маппинг pipeline key → stage key в usage
        const _pipelineToStage = {
            'crop_blocks': 'crop_blocks',
            'text_analysis': 'text_analysis',
            'blocks_analysis': 'block_analysis',
            'findings': 'findings_merge',
            'norms_verified': 'norm_verify',
            'optimization': 'optimization',
        };

        function stageTokens(pipelineKey) {
            if (!currentProject.value) return null;
            const usage = projectUsage.value[currentProject.value.project_id];
            if (!usage || !usage.stages_summary) return null;
            const stageKey = _pipelineToStage[pipelineKey] || pipelineKey;
            return usage.stages_summary[stageKey] || null;
        }

        function stageDurationForProject(projectId, pipelineKey) {
            const usage = projectUsage.value[projectId];
            if (!usage || !usage.stages_summary) return null;
            const stageKey = _pipelineToStage[pipelineKey] || pipelineKey;
            const s = usage.stages_summary[stageKey];
            return (s && s.duration_ms > 0) ? s.duration_ms : null;
        }

        function formatDuration(ms) {
            if (!ms || ms <= 0) return '';
            const sec = Math.round(ms / 1000);
            if (sec < 60) return sec + 'с';
            const min = Math.floor(sec / 60);
            const remSec = sec % 60;
            if (min < 60) return min + 'м' + (remSec > 0 ? remSec + 'с' : '');
            const hr = Math.floor(min / 60);
            const remMin = min % 60;
            return hr + 'ч' + (remMin > 0 ? remMin + 'м' : '');
        }

        const currentProjectUsage = computed(() => {
            if (!currentProject.value) return null;
            const u = projectUsage.value[currentProject.value.project_id];
            return (u && u.total_tokens > 0) ? u : null;
        });

        async function pollLiveStatus() {
            try {
                const resp = await fetch('/api/audit/live-status');
                if (resp.ok) {
                    const data = await resp.json();
                    liveStatus.value = data;

                    // Обновляем auditRunning на основе live-данных
                    const hasAny = Object.keys(data.running).length > 0;
                    auditRunning.value = hasAny;
                    batchRunning.value = !!data.running['__BATCH__'];

                    // Backup heartbeat из polling (если WS не работает)
                    for (const [pid, info] of Object.entries(data.running || {})) {
                        if (info.last_heartbeat) {
                            const hbTime = new Date(info.last_heartbeat).getTime();
                            const current = lastHeartbeatTime.value[pid] || 0;
                            if (hbTime > current) {
                                lastHeartbeatTime.value = { ...lastHeartbeatTime.value, [pid]: hbTime };
                            }
                        }
                        if (info.eta_sec != null) {
                            heartbeatData.value = {
                                ...heartbeatData.value,
                                [pid]: { ...heartbeatData.value[pid], eta_sec: info.eta_sec },
                            };
                        }
                    }

                    // Очистка heartbeat для остановленных проектов
                    for (const pid of Object.keys(heartbeatData.value)) {
                        if (!data.running[pid]) {
                            const { [pid]: _, ...rest } = heartbeatData.value;
                            heartbeatData.value = rest;
                            const { [pid]: __, ...restTime } = lastHeartbeatTime.value;
                            lastHeartbeatTime.value = restTime;
                        }
                    }

                    // Обновляем batches в списке проектов (Dashboard)
                    if (currentView.value === 'dashboard' && projects.value.length > 0) {
                        for (const p of projects.value) {
                            if (data.batches[p.project_id]) {
                                p.completed_batches = data.batches[p.project_id].completed;
                                p.total_batches = data.batches[p.project_id].total;
                            }
                        }
                    }

                    // Обновляем текущий проект (Project Detail)
                    if (currentView.value === 'project' && currentProject.value) {
                        const pid = currentProject.value.project_id;
                        if (data.batches[pid]) {
                            currentProject.value.completed_batches = data.batches[pid].completed;
                            currentProject.value.total_batches = data.batches[pid].total;
                        }
                    }
                }
            } catch (e) {
                // Ignore polling errors
            }
        }

        function startPolling() {
            stopPolling();
            pollLiveStatus(); // сразу
            pollTimer = setInterval(pollLiveStatus, 5000);
            tickTimer = setInterval(() => { elapsedTick.value++; }, 1000);
        }

        function stopPolling() {
            if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
            if (tickTimer) { clearInterval(tickTimer); tickTimer = null; }
        }

        // ─── Helpers для live-статуса ───
        function isProjectRunning(projectId) {
            return !!(liveStatus.value.running && liveStatus.value.running[projectId]);
        }

        function getProjectLiveInfo(projectId) {
            const r = liveStatus.value.running ? liveStatus.value.running[projectId] : null;
            const b = liveStatus.value.batches ? liveStatus.value.batches[projectId] : null;
            if (!r && !b) return null;

            const info = { running: !!r };
            if (r) {
                info.stage = r.stage;
                info.status = r.status;
                info.progress_current = r.progress_current;
                info.progress_total = r.progress_total;
                info.started_at = r.started_at;
            }
            if (b) {
                info.batch_completed = b.completed;
                info.batch_total = b.total;
            }
            return info;
        }

        function stageLabel(stage) {
            const labels = {
                'crop_blocks': 'Кроп блоков',
                'text_analysis': 'Анализ текста',
                'block_analysis': 'Анализ блоков',
                'findings_merge': 'Свод замечаний',
                'norm_verify': 'Верификация норм',
                'norm_fix': 'Пересмотр замечаний',
                'excel': 'Excel-отчёт',
                'optimization': 'Оптимизация',
                'full': 'Полный конвейер',
                // Legacy aliases
                'prepare': 'Подготовка',
                'tile_batches': 'Генерация пакетов',
                'tile_audit': 'Анализ блоков',
                'main_audit': 'Свод замечаний',
                'merge': 'Слияние результатов',
            };
            return labels[stage] || stage || '';
        }

        function formatElapsed(startedAt) {
            if (!startedAt) return '';
            // elapsedTick обеспечивает реактивное обновление каждую секунду
            const _tick = elapsedTick.value;
            const start = new Date(startedAt);
            const now = new Date();
            const diff = Math.floor((now - start) / 1000);
            if (diff < 0) return '';
            const h = Math.floor(diff / 3600);
            const m = Math.floor((diff % 3600) / 60);
            const s = diff % 60;
            if (h > 0) {
                return `${h}:${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
            }
            return `${m.toString().padStart(2, '0')}:${s.toString().padStart(2, '0')}`;
        }

        function batchPercent(projectId) {
            const b = liveStatus.value.batches ? liveStatus.value.batches[projectId] : null;
            if (!b || !b.total) return 0;
            return Math.round(b.completed / b.total * 100);
        }

        function batchProgressText(projectId) {
            const r = liveStatus.value.running ? liveStatus.value.running[projectId] : null;
            const b = liveStatus.value.batches ? liveStatus.value.batches[projectId] : null;

            if (r) {
                const pct = r.progress_total > 0
                    ? Math.round(r.progress_current / r.progress_total * 100)
                    : 0;
                if ((r.stage === 'block_analysis' || r.stage === 'tile_audit') && b) {
                    return `${stageLabel(r.stage)}: пакет ${b.completed}/${b.total} (${Math.round(b.completed / b.total * 100)}%)`;
                }
                if (r.progress_total > 0) {
                    return `${stageLabel(r.stage)}: ${r.progress_current}/${r.progress_total} (${pct}%)`;
                }
                return `${stageLabel(r.stage)}...`;
            }
            return '';
        }

        // ─── Heartbeat helpers ───
        function secondsSinceHeartbeat(projectId) {
            const _tick = elapsedTick.value; // реактивность
            const lastTime = lastHeartbeatTime.value[projectId];
            if (!lastTime) return 999;
            return Math.floor((Date.now() - lastTime) / 1000);
        }

        function isHeartbeatStale(projectId) {
            return secondsSinceHeartbeat(projectId) > 60;
        }

        function getHeartbeatInfo(projectId) {
            return heartbeatData.value[projectId] || null;
        }

        function formatETA(etaSec) {
            if (etaSec == null || etaSec <= 0) return '';
            if (etaSec > 3600) {
                const h = Math.floor(etaSec / 3600);
                const m = Math.floor((etaSec % 3600) / 60);
                return `~${h}ч ${m}м`;
            }
            const m = Math.floor(etaSec / 60);
            if (m > 0) return `~${m} мин`;
            return `<1 мин`;
        }

        // ─── Usage Helpers ───
        function formatTokens(n) {
            if (n == null) return '0';
            if (n >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M';
            if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K';
            return String(n);
        }

        function formatCost(usd) {
            if (usd == null || usd === 0) return '$0.00';
            if (usd < 0.01) return '<$0.01';
            return '$' + usd.toFixed(2);
        }

        async function pollGlobalUsage() {
            try {
                const resp = await fetch('/api/usage/global');
                if (resp.ok) {
                    globalUsage.value = await resp.json();
                }
            } catch (e) {
                // Не критично — тихо пропускаем
            }
        }

        async function refreshGlobalUsage() {
            try {
                const resp = await fetch('/api/usage/global/refresh', { method: 'POST' });
                if (resp.ok) {
                    globalUsage.value = await resp.json();
                }
            } catch (e) {
                console.error('Failed to refresh global usage:', e);
            }
        }

        async function resetSessionCounter() {
            try {
                const resp = await fetch('/api/usage/reset-session', { method: 'POST' });
                if (resp.ok) {
                    await resp.json();
                }
            } catch (e) {
                console.error('Failed to reset session counter:', e);
            }
        }

        function heartbeatStatusText(projectId) {
            if (!isProjectRunning(projectId)) return '';
            const sec = secondsSinceHeartbeat(projectId);
            if (sec > 60) return `Claude думает... (нет вывода ${sec} сек)`;
            if (sec < 999) return `Процесс активен`;
            return '';
        }

        // ─── API helpers ───
        async function api(path) {
            const resp = await fetch(`/api${path}`);
            if (!resp.ok) throw new Error(`API error: ${resp.status}`);
            return resp.json();
        }

        // ─── Navigation ───
        function navigate(path) {
            window.location.hash = path;
        }

        function handleRoute() {
            const hash = window.location.hash.slice(1) || '/';
            selectedTile.value = null;

            if (hash === '/') {
                currentView.value = 'dashboard';
                connectGlobalWS();  // Вернуться на global WS
                refreshProjects();
            } else if (hash.match(/^\/project\/([^/]+)\/findings$/)) {
                const id = decodeURIComponent(hash.match(/^\/project\/([^/]+)\/findings$/)[1]);
                currentView.value = 'findings';
                connectGlobalWS();  // Не нужен project WS для findings
                loadFindings(id);
            } else if (hash.match(/^\/project\/([^/]+)\/blocks$/)) {
                const id = decodeURIComponent(hash.match(/^\/project\/([^/]+)\/blocks$/)[1]);
                currentView.value = 'blocks';
                connectGlobalWS();
                loadBlocks(id);
            } else if (hash.match(/^\/project\/([^/]+)\/tiles$/)) {
                const id = decodeURIComponent(hash.match(/^\/project\/([^/]+)\/tiles$/)[1]);
                currentView.value = 'tiles';
                connectGlobalWS();  // Не нужен project WS для tiles
                loadTiles(id);
            } else if (hash.match(/^\/project\/([^/]+)\/optimization$/)) {
                const id = decodeURIComponent(hash.match(/^\/project\/([^/]+)\/optimization$/)[1]);
                currentView.value = 'optimization';
                connectGlobalWS();
                loadOptimization(id);
            } else if (hash.match(/^\/project\/([^/]+)\/document$/)) {
                const id = decodeURIComponent(hash.match(/^\/project\/([^/]+)\/document$/)[1]);
                currentView.value = 'document';
                connectGlobalWS();
                loadDocument(id);
            } else if (hash.match(/^\/project\/([^/]+)\/prompts$/)) {
                const id = decodeURIComponent(hash.match(/^\/project\/([^/]+)\/prompts$/)[1]);
                currentView.value = 'prompts';
                promptsProjectId.value = id;
                activePromptTab.value = 0;
                connectGlobalWS();
                loadDisciplines().then(() => {
                    const proj = projects.value.find(p => p.name === id || p.project_id === id);
                    const section = proj?.section || 'EM';
                    promptsDiscipline.value = section;
                    loadTemplates(section);
                });
            } else if (hash.match(/^\/project\/([^/]+)\/log$/)) {
                const id = decodeURIComponent(hash.match(/^\/project\/([^/]+)\/log$/)[1]);
                currentView.value = 'log';
                logProjectId.value = id;
                loadProject(id);
                // Загружаем историю логов из файла (если ещё не загружена)
                if (!projectLogs.value[id] || projectLogs.value[id].length === 0) {
                    loadProjectLog(id);
                }
                connectProjectWS(id);  // Project WS только для лога
            } else if (hash.match(/^\/project\/([^/]+)$/)) {
                const id = decodeURIComponent(hash.match(/^\/project\/([^/]+)$/)[1]);
                currentView.value = 'project';
                connectGlobalWS();  // Не нужен project WS
                loadProject(id);
            }
        }

        // ─── Batch Selection (мультивыбор проектов) ───
        const selectedProjects = ref(new Set());
        const selectAllChecked = ref(false);
        const batchRunning = ref(false);
        const batchQueue = ref(null);
        const showBatchModal = ref(false);
        const batchMode = ref('standard');   // standard | pro
        const batchScope = ref('audit');     // audit | optimization | both
        const batchModalCount = ref(0);
        const batchAllMode = ref(false);  // true = запуск для ВСЕХ проектов

        function toggleProjectSelection(projectId) {
            const s = new Set(selectedProjects.value);
            if (s.has(projectId)) s.delete(projectId);
            else s.add(projectId);
            selectedProjects.value = s;
            selectAllChecked.value = s.size === projects.value.length && s.size > 0;
        }

        function toggleSelectAll() {
            if (selectAllChecked.value) {
                selectedProjects.value = new Set();
                selectAllChecked.value = false;
            } else {
                selectedProjects.value = new Set(projects.value.map(p => p.project_id));
                selectAllChecked.value = true;
            }
        }

        function isProjectSelected(projectId) {
            return selectedProjects.value.has(projectId);
        }

        const selectedCount = computed(() => selectedProjects.value.size);

        function openBatchModal() {
            batchModalCount.value = selectedProjects.value.size;
            batchMode.value = 'standard';
            batchScope.value = 'audit';
            batchAllMode.value = false;
            showBatchModal.value = true;
        }

        async function confirmBatchAction() {
            showBatchModal.value = false;
            // Формируем action: standard, pro, optimization, standard+optimization, pro+optimization
            let action = batchMode.value;  // standard | pro
            if (batchScope.value === 'optimization') {
                action = 'optimization';
            } else if (batchScope.value === 'both') {
                action = batchMode.value + '+optimization';
            }

            if (batchAllMode.value) {
                // Запуск для ВСЕХ проектов — выбираем все ID
                const allIds = projects.value.map(p => p.project_id);
                selectedProjects.value = new Set(allIds);
                batchAllMode.value = false;
            }
            await startBatchAction(action);
        }

        async function startBatchAction(action) {
            const ids = Array.from(selectedProjects.value);
            try {
                batchRunning.value = true;
                const resp = await fetch('/api/audit/batch', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ project_ids: ids, action: action }),
                });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || `API error: ${resp.status}`);
                }
                const data = await resp.json();
                batchQueue.value = data.queue;
                selectedProjects.value = new Set();
                selectAllChecked.value = false;
            } catch (e) {
                alert(e.message);
                batchRunning.value = false;
            }
        }

        function batchActionLabel(action) {
            const labels = {
                'resume': 'Продолжение прерванных',
                'standard': 'Стандартный аудит',
                'pro': 'PRO аудит',
                'optimization': 'Оптимизация',
                'standard+optimization': 'Стандартный + оптимизация',
                'pro+optimization': 'PRO + оптимизация',
            };
            return labels[action] || action;
        }

        async function cancelBatch() {
            if (!confirm('Отменить групповое действие?\n\nТекущий проект будет прерван.')) return;
            try {
                await fetch('/api/audit/batch/cancel', { method: 'DELETE' });
                batchRunning.value = false;
                batchQueue.value = null;
            } catch (e) { alert(e.message); }
        }

        // ─── Audit Actions ───
        const auditRunning = ref(false);

        async function apiPost(path) {
            const resp = await fetch(`/api${path}`, { method: 'POST' });
            if (!resp.ok) {
                const err = await resp.json().catch(() => ({}));
                throw new Error(err.detail || `API error: ${resp.status}`);
            }
            return resp.json();
        }

        function _afterAuditStart(projectId) {
            // Подключаем project WS для live-обновлений (прогресс, heartbeat, статус)
            connectProjectWS(projectId);
        }

        async function startPrepare(projectId) {
            try {
                auditRunning.value = true;
                await apiPost(`/audit/${projectId}/prepare`);
                _afterAuditStart(projectId);
            } catch (e) { alert(e.message); auditRunning.value = false; }
        }

        async function startTileAudit(projectId, startFrom = 1) {
            try {
                auditRunning.value = true;
                await apiPost(`/audit/${projectId}/tile-audit?start_from=${startFrom}`);
                _afterAuditStart(projectId);
            } catch (e) { alert(e.message); auditRunning.value = false; }
        }

        async function startMainAudit(projectId) {
            try {
                auditRunning.value = true;
                await apiPost(`/audit/${projectId}/main-audit`);
                _afterAuditStart(projectId);
            } catch (e) { alert(e.message); auditRunning.value = false; }
        }

        async function startSmartAudit(projectId) {
            try {
                auditRunning.value = true;
                await apiPost(`/audit/${projectId}/smart-audit`);
                _afterAuditStart(projectId);
            } catch (e) { alert(e.message); auditRunning.value = false; }
        }

        async function startStandardAudit(projectId) {
            try {
                auditRunning.value = true;
                await apiPost(`/audit/${projectId}/standard-audit`);
                _afterAuditStart(projectId);
            } catch (e) { alert(e.message); auditRunning.value = false; }
        }

        async function startProAudit(projectId) {
            try {
                auditRunning.value = true;
                await apiPost(`/audit/${projectId}/pro-audit`);
                _afterAuditStart(projectId);
            } catch (e) { alert(e.message); auditRunning.value = false; }
        }

        async function startNormVerify(projectId) {
            try {
                auditRunning.value = true;
                await apiPost(`/audit/${projectId}/verify-norms`);
                _afterAuditStart(projectId);
            } catch (e) { alert(e.message); auditRunning.value = false; }
        }

        async function resumePipeline(projectId) {
            try {
                auditRunning.value = true;
                await apiPost(`/audit/${projectId}/resume`);
                _afterAuditStart(projectId);
            } catch (e) { alert(e.message); auditRunning.value = false; }
        }

        const resumeInfo = ref(null);

        async function loadResumeInfo(projectId) {
            try {
                const resp = await fetch(`/api/audit/${projectId}/resume-info`);
                if (resp.ok) {
                    resumeInfo.value = await resp.json();
                }
            } catch (e) { resumeInfo.value = null; }
        }

        async function cancelAudit(projectId) {
            try {
                await fetch(`/api/audit/${projectId}/cancel`, { method: 'DELETE' });
                auditRunning.value = false;
            } catch (e) { alert(e.message); }
        }

        async function cleanProject(projectId) {
            const name = currentProject.value?.name || projectId;
            if (!confirm(`Очистить все результаты проекта "${name}"?\n\nБудут удалены:\n- Все блоки и нарезки\n- Все JSON-этапы (00-03)\n- Батчи и логи\n- Отчёты\n\nPDF и MD файлы сохраняются.`)) {
                return;
            }
            try {
                const resp = await fetch(`/api/projects/${encodeURIComponent(projectId)}/clean`, { method: 'DELETE' });
                const data = await resp.json();
                if (!resp.ok) {
                    alert(data.detail || 'Ошибка очистки');
                    return;
                }
                alert(`Очищено: ${data.deleted_files} файлов, ${data.freed_mb} MB освобождено`);
                // Обновляем данные проекта
                await refreshProjects();
                if (currentProject.value && currentProject.value.project_id === projectId) {
                    const updated = await apiGet(`/projects/${encodeURIComponent(projectId)}`);
                    if (updated) currentProject.value = updated;
                }
            } catch (e) { alert(e.message); }
        }

        async function retryStage(projectId, stage) {
            try {
                auditRunning.value = true;
                await apiPost(`/audit/${projectId}/retry/${stage}`);
                _afterAuditStart(projectId);
            } catch (e) { alert(e.message); auditRunning.value = false; }
        }

        async function skipStage(projectId, stage) {
            if (!confirm('Пропустить этап? Это может привести к неполному аудиту.')) return;
            try {
                await apiPost(`/audit/${projectId}/skip/${stage}`);
                await refreshProjects();
                if (currentProject.value && currentProject.value.project_id === projectId) {
                    const data = await apiGet(`/projects/${projectId}`);
                    if (data) currentProject.value = data;
                }
            } catch (e) { alert(e.message); }
        }

        // Запуск ВСЕХ проектов последовательно
        const allRunning = computed(() => {
            return liveStatus.value.running && '__ALL__' in liveStatus.value.running;
        });

        function startAllProjects() {
            // Открываем ту же модалку выбора режима, но для ВСЕХ проектов
            batchModalCount.value = projects.value.length;
            batchMode.value = 'standard';
            batchScope.value = 'audit';
            batchAllMode.value = true;
            showBatchModal.value = true;
        }

        async function generateExcel() {
            try {
                const data = await apiPost('/export/excel');
                if (data.file) {
                    window.open(`/api/export/download/${data.file}`, '_blank');
                }
            } catch (e) { alert(e.message); }
        }

        // Model Switcher удалён — модели per-stage настроены в config.py → _stage_models

        // ─── Disciplines ───
        const supportedDisciplines = ref([]);

        async function loadDisciplines() {
            try {
                const data = await api('/projects/disciplines');
                supportedDisciplines.value = data.disciplines;
            } catch (e) {
                console.error('Failed to load disciplines:', e);
                supportedDisciplines.value = [
                    { code: 'EM', name: 'Электроснабжение', short_name: 'ЭОМ/ЭС', color: '#f39c12' },
                ];
            }
        }

        function getDisciplineColor(code) {
            const d = supportedDisciplines.value.find(x => x.code === code);
            return d ? d.color : '#666';
        }

        function disciplineLabel(code) {
            const d = supportedDisciplines.value.find(x => x.code === code);
            return d ? d.short_name : code;
        }

        function disciplineBadgeStyle(code) {
            const color = getDisciplineColor(code);
            return {
                background: color + '22',
                color: color,
                borderColor: color,
                border: '1px solid ' + color,
            };
        }

        async function detectDiscipline(folderName) {
            try {
                const resp = await fetch('/api/projects/detect-discipline', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ folder_name: folderName }),
                });
                if (resp.ok) {
                    const data = await resp.json();
                    return data.code;
                }
            } catch (e) {
                console.error('Detect discipline error:', e);
            }
            return 'EM';
        }

        // ─── Add Project (scan & register) ───
        const showAddProject = ref(false);
        const unregisteredFolders = ref([]);
        const addProjectLoading = ref(false);

        async function scanFolders() {
            addProjectLoading.value = true;
            try {
                const data = await api('/projects/scan');
                // Для каждой папки автоматически определяем дисциплину
                const folders = data.folders;
                for (const f of folders) {
                    const detected = await detectDiscipline(f.folder);
                    f._detectedDiscipline = detected;
                    f._selectedDiscipline = detected;
                }
                unregisteredFolders.value = folders;
                showAddProject.value = true;
            } catch (e) {
                alert('Ошибка сканирования: ' + e.message);
            }
            addProjectLoading.value = false;
        }

        async function registerProject(folder) {
            const folderInfo = unregisteredFolders.value.find(f => f.folder === folder);
            if (!folderInfo) return;

            addProjectLoading.value = true;
            try {
                const body = {
                    folder: folder,
                    pdf_file: folderInfo.pdf_files[0],
                    md_file: folderInfo.md_files.length > 0 ? folderInfo.md_files[0] : null,
                    name: folder,
                    section: folderInfo._selectedDiscipline || 'EM',
                    description: '',
                };
                const resp = await fetch('/api/projects/register', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(body),
                });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || `Ошибка: ${resp.status}`);
                }
                // Убрать из списка незарегистрированных
                unregisteredFolders.value = unregisteredFolders.value.filter(f => f.folder !== folder);
                // Обновить список проектов
                await refreshProjects();
                if (unregisteredFolders.value.length === 0) {
                    showAddProject.value = false;
                }
            } catch (e) {
                alert('Ошибка регистрации: ' + e.message);
            }
            addProjectLoading.value = false;
        }

        function closeAddProject() {
            showAddProject.value = false;
        }

        // ─── Data Loading ───
        async function refreshProjects() {
            loading.value = true;
            try {
                const data = await api('/projects');
                projects.value = data.projects;
                fetchAllProjectUsage();  // загрузить usage для дашборда
            } catch (e) {
                console.error('Failed to load projects:', e);
            }
            loading.value = false;
        }

        async function loadProject(id) {
            currentProjectId.value = id;
            try {
                currentProject.value = await api(`/projects/${id}`);
                loadResumeInfo(id);
                fetchProjectUsage(id);  // загрузить детальный usage
            } catch (e) {
                console.error('Failed to load project:', e);
                currentProject.value = null;
            }
        }

        async function loadFindings(id) {
            findingsData.value = null;
            try {
                const params = new URLSearchParams();
                if (filterSeverity.value) params.set('severity', filterSeverity.value);
                if (filterSearch.value) params.set('search', filterSearch.value);
                const qs = params.toString();
                findingsData.value = await api(`/findings/${id}${qs ? '?' + qs : ''}`);
            } catch (e) {
                console.error('Failed to load findings:', e);
            }
        }

        async function loadTiles(id) {
            tilesProjectId.value = id;
            try {
                const [pagesData] = await Promise.all([
                    api(`/tiles/${id}/pages`),
                    loadTileAnalysis(id),
                    loadPageSummaries(id),
                ]);
                tilePages.value = pagesData.pages;
                if (pagesData.pages.length > 0 && !selectedPage.value) {
                    selectedPage.value = pagesData.pages[0].page_num;
                }
            } catch (e) {
                console.error('Failed to load tiles:', e);
            }
        }

        // ─── Blocks (OCR) ───

        async function loadBlocks(id) {
            blocksProjectId.value = id;
            selectedBlock.value = null;
            try {
                const [blocksData] = await Promise.all([
                    api(`/tiles/${id}/blocks`),
                    loadBlockAnalysis(id),
                ]);
                blockPages.value = blocksData.pages || [];
                if (blockPages.value.length > 0 && !selectedBlockPage.value) {
                    selectedBlockPage.value = blockPages.value[0].page_num;
                }
            } catch (e) {
                console.error('Failed to load blocks:', e);
                blockPages.value = [];
            }
        }

        async function loadBlockAnalysis(id) {
            try {
                const data = await api(`/tiles/${id}/blocks/analysis`);
                blockAnalysis.value = data.blocks || {};
            } catch (e) {
                blockAnalysis.value = {};
            }
        }

        const currentPageBlocks = computed(() => {
            if (!selectedBlockPage.value || !blockPages.value.length) return null;
            return blockPages.value.find(p => p.page_num === selectedBlockPage.value) || null;
        });

        function openBlock(block) {
            selectedBlock.value = block;
        }

        function blockHasAnalysis(blockId) {
            return !!blockAnalysis.value[blockId];
        }

        function blockFindingsCount(blockId) {
            const info = blockAnalysis.value[blockId];
            if (!info) return 0;
            return (info.findings || []).length;
        }

        function blockMaxSeverity(blockId) {
            const info = blockAnalysis.value[blockId];
            if (!info || !info.findings) return null;
            const order = ['КРИТИЧЕСКОЕ', 'ЭКОНОМИЧЕСКОЕ', 'ЭКСПЛУАТАЦИОННОЕ', 'РЕКОМЕНДАТЕЛЬНОЕ', 'ПРОВЕРИТЬ ПО СМЕЖНЫМ'];
            let best = 999;
            for (const f of info.findings) {
                const s = (f.severity || '').toUpperCase();
                for (let i = 0; i < order.length; i++) {
                    if (s.includes(order[i].substring(0, 6)) && i < best) {
                        best = i;
                    }
                }
            }
            return best < order.length ? order[best] : null;
        }

        const selectedBlockAnalysis = computed(() => {
            if (!selectedBlock.value) return null;
            return blockAnalysis.value[selectedBlock.value.block_id] || null;
        });

        // ─── Optimization ───
        // ─── Document Viewer (MD) ────────────────────────────
        function renderMarkdown(text) {
            if (!text) return '';
            if (typeof marked !== 'undefined') {
                try {
                    return marked.parse(text, { breaks: true, gfm: true });
                } catch (e) {
                    return text.replace(/</g, '&lt;').replace(/\n/g, '<br>');
                }
            }
            return text.replace(/</g, '&lt;').replace(/\n/g, '<br>');
        }

        async function loadDocument(id) {
            documentProjectId.value = id;
            documentLoading.value = true;
            documentPages.value = [];
            documentPageData.value = null;
            documentCurrentPage.value = null;
            try {
                currentProject.value = await api(`/projects/${id}`);
                const data = await api(`/document/${id}/pages`);
                documentPages.value = data.pages || [];
                if (data.pages && data.pages.length > 0) {
                    await loadDocumentPage(id, data.pages[0].page_num);
                }
            } catch (e) {
                console.error('Failed to load document:', e);
                documentPages.value = [];
            }
            documentLoading.value = false;
        }

        async function loadDocumentPage(id, pageNum) {
            documentCurrentPage.value = pageNum;
            try {
                const data = await api(`/document/${id}/page/${pageNum}`);
                documentPageData.value = data;
            } catch (e) {
                console.error('Failed to load page:', e);
                documentPageData.value = null;
            }
        }

        function docPrevPage() {
            const idx = documentPages.value.findIndex(p => p.page_num === documentCurrentPage.value);
            if (idx > 0) loadDocumentPage(documentProjectId.value, documentPages.value[idx - 1].page_num);
        }

        function docNextPage() {
            const idx = documentPages.value.findIndex(p => p.page_num === documentCurrentPage.value);
            if (idx < documentPages.value.length - 1) loadDocumentPage(documentProjectId.value, documentPages.value[idx + 1].page_num);
        }

        async function loadOptimization(id) {
            currentProjectId.value = id;
            optimizationLoading.value = true;
            optimizationData.value = null;
            try {
                currentProject.value = await api(`/projects/${id}`);
                const resp = await api(`/optimization/${id}`);
                if (resp.has_data) {
                    optimizationData.value = resp.data;
                }
            } catch (e) {
                console.error('Failed to load optimization:', e);
            }
            optimizationLoading.value = false;
        }

        async function startOptimization(id) {
            try {
                await apiPost(`/optimization/${id}/run`);
                if (currentView.value === 'project') loadProject(id);
            } catch (e) {
                alert('Ошибка запуска оптимизации: ' + (e.message || e));
            }
        }

        const filteredOptimization = computed(() => {
            if (!optimizationData.value) return [];
            const items = optimizationData.value.items || [];
            if (!optimizationFilter.value) return items;
            return items.filter(i => i.type === optimizationFilter.value);
        });

        const optimizationTypeLabels = {
            'cheaper_analog': 'Аналоги',
            'faster_install': 'Монтаж',
            'simpler_design': 'Конструктив',
            'lifecycle': 'Жизн. цикл',
        };

        const optimizationTypeColors = {
            'cheaper_analog': '#27ae60',
            'faster_install': '#2980b9',
            'simpler_design': '#e67e22',
            'lifecycle': '#8e44ad',
        };

        function optTypeLabel(type) {
            return optimizationTypeLabels[type] || type;
        }

        function optTypeColor(type) {
            return optimizationTypeColors[type] || '#999';
        }

        async function loadTileAnalysis(id) {
            tileAnalysisLoading.value = true;
            try {
                const data = await api(`/tiles/${id}/analysis`);
                tileAnalysis.value = data.tiles || {};
            } catch (e) {
                tileAnalysis.value = {};
            }
            tileAnalysisLoading.value = false;
        }

        const selectedTileAnalysis = computed(() => {
            if (!selectedTile.value) return null;
            return tileAnalysis.value[selectedTile.value] || null;
        });

        function tileHasAnalysis(tileName) {
            return !!tileAnalysis.value[tileName];
        }

        function tileFindingsCount(tileName) {
            const info = tileAnalysis.value[tileName];
            if (!info) return 0;
            return (info.findings || []).length;
        }

        function tileMaxSeverity(tileName) {
            const info = tileAnalysis.value[tileName];
            if (!info || !info.findings || info.findings.length === 0) return null;
            const order = ['КРИТИЧЕСКОЕ', 'ЭКОНОМИЧЕСКОЕ', 'ЭКСПЛУАТАЦИОННОЕ', 'РЕКОМЕНДАТЕЛЬНОЕ', 'ПРОВЕРИТЬ ПО СМЕЖНЫМ'];
            let best = 999;
            for (const f of info.findings) {
                const s = (f.severity || '').toUpperCase();
                for (let i = 0; i < order.length; i++) {
                    if (s.includes(order[i].substring(0, 6)) && i < best) {
                        best = i;
                    }
                }
            }
            return best < order.length ? order[best] : null;
        }

        // ─── Page Summaries ───
        async function loadPageSummaries(id) {
            try {
                const data = await api(`/tiles/${id}/page-summaries`);
                const map = {};
                for (const ps of (data.page_summaries || [])) {
                    map[ps.page] = ps;
                }
                pageSummaries.value = map;
            } catch (e) {
                pageSummaries.value = {};
            }
        }

        async function loadPageAnalysis(projectId, pageNum) {
            pageAnalysisLoading.value = true;
            try {
                const data = await api(`/tiles/${projectId}/page-analysis/${pageNum}`);
                pageAnalysis.value = data;
            } catch (e) {
                pageAnalysis.value = null;
            }
            pageAnalysisLoading.value = false;
        }

        function sheetTypeIcon(sheetType) {
            const icons = {
                'single_line_diagram': 'SLD',
                'panel_schedule': 'SCH',
                'floor_plan': 'PLAN',
                'parking_plan': 'PRK',
                'cable_routing': 'CBL',
                'grounding': 'GND',
                'entry_node': 'ENT',
                'specification': 'SPEC',
                'title_block': 'TTL',
                'general_notes': 'NOTE',
                'detail': 'DET',
                'other': '...',
            };
            return icons[sheetType] || '...';
        }

        function getPageSummary(pageNum) {
            return pageSummaries.value[pageNum] || null;
        }

        // ─── Computed ───
        const filteredFindings = computed(() => {
            if (!findingsData.value) return [];
            return findingsData.value.findings;
        });

        const currentPageTiles = computed(() => {
            if (!selectedPage.value || !tilePages.value.length) return null;
            return tilePages.value.find(p => p.page_num === selectedPage.value);
        });

        // Live-статус текущего проекта (для Project Detail)
        const currentProjectLive = computed(() => {
            if (!currentProject.value) return null;
            return getProjectLiveInfo(currentProject.value.project_id);
        });

        // ─── Helpers ───
        function stepClass(status) {
            if (status === 'done') return 'step-done';
            if (status === 'error') return 'step-error';
            if (status === 'partial') return 'step-partial';
            if (status === 'running') return 'step-running';
            if (status === 'skipped') return 'step-skipped';
            return '';
        }

        function sevClass(severity) {
            const s = (severity || '').toUpperCase();
            if (s.includes('КРИТИЧ')) return 'critical';
            if (s.includes('ЭКОНОМ')) return 'economic';
            if (s.includes('ЭКСПЛУАТ')) return 'operational';
            if (s.includes('РЕКОМЕНД')) return 'recommendation';
            if (s.includes('ПРОВЕР')) return 'check';
            return 'check';
        }

        function sevIcon(severity) {
            const s = (severity || '').toUpperCase();
            if (s.includes('КРИТИЧ')) return '\uD83D\uDD34';
            if (s.includes('ЭКОНОМ')) return '\uD83D\uDFE0';
            if (s.includes('ЭКСПЛУАТ')) return '\uD83D\uDFE1';
            if (s.includes('РЕКОМЕНД')) return '\uD83D\uDD35';
            return '\u26AA';
        }

        function tileImageUrl(projectId, pageNum, tileName) {
            // tileName = "page_07_r1c2.png"
            const match = tileName.match(/r(\d+)c(\d+)/);
            if (!match) return '';
            return `/api/tiles/${projectId}/image/${pageNum}/${match[1]}_${match[2]}`;
        }

        function openTile(tileName) {
            selectedTile.value = tileName;
        }

        let searchTimeout = null;
        function debounceSearch() {
            clearTimeout(searchTimeout);
            searchTimeout = setTimeout(() => {
                const hash = window.location.hash.slice(1);
                const match = hash.match(/^\/project\/([^/]+)\/findings$/);
                if (match) loadFindings(match[1]);
            }, 400);
        }

        // ─── Prompts ───
        async function loadDisciplines() {
            try {
                const resp = await fetch('/api/audit/disciplines');
                if (!resp.ok) return;
                const data = await resp.json();
                disciplines.value = data.disciplines || [];
            } catch (e) {
                console.error('loadDisciplines error:', e);
            }
        }

        async function loadTemplates(discipline) {
            promptsLoading.value = true;
            const qs = discipline ? `?discipline=${encodeURIComponent(discipline)}` : '';
            try {
                const resp = await fetch(`/api/audit/templates${qs}`);
                if (!resp.ok) throw new Error(`${resp.status}`);
                const data = await resp.json();
                templates.value = (data.templates || []).map(t => ({
                    ...t,
                    _editContent: t.content,
                    _dirty: false,
                }));
                if (activePromptTab.value >= templates.value.length) {
                    activePromptTab.value = 0;
                }
            } catch (e) {
                console.error('loadTemplates error:', e);
                templates.value = [];
            } finally {
                promptsLoading.value = false;
            }
        }

        async function switchDiscipline(code) {
            promptsDiscipline.value = code;
            showDisciplineDropdown.value = false;
            await loadTemplates(code);
        }

        const PROMPT_PLACEHOLDERS = /(\{(?:PROJECT_ID|OUTPUT_PATH|MD_FILE_PATH|DISCIPLINE_CHECKLIST|DISCIPLINE_NORMS_FILE|DISCIPLINE_ROLE|DISCIPLINE_FINDING_CATEGORIES|DISCIPLINE_DRAWING_TYPES|BLOCK_LIST|BATCH_ID|TOTAL_BATCHES|BLOCK_COUNT|BATCH_ID_PADDED)\})/g;

        function highlightPlaceholders(text) {
            // Escape HTML, then wrap placeholders in <mark>
            const escaped = text
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;');
            return escaped.replace(PROMPT_PLACEHOLDERS, '<mark class="ph-mark">$1</mark>') + '\n';
        }

        function syncScroll(event) {
            const textarea = event.target;
            const overlay = textarea.previousElementSibling;
            if (overlay) {
                overlay.scrollTop = textarea.scrollTop;
                overlay.scrollLeft = textarea.scrollLeft;
            }
        }

        async function saveTemplate(stage, content) {
            if (!confirm('Сохранить шаблон? Изменение применится для ВСЕХ проектов.')) return;
            try {
                const resp = await fetch(`/api/audit/templates/${stage}`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ content }),
                });
                if (!resp.ok) throw new Error(`${resp.status}`);
                await loadTemplates(promptsDiscipline.value);
            } catch (e) {
                alert('Ошибка сохранения шаблона: ' + e.message);
            }
        }

        function clearLog() {
            const pid = logProjectId.value;
            if (pid) {
                projectLogs.value[pid] = [];
                // Очищаем и на сервере
                fetch(`/api/audit/${encodeURIComponent(pid)}/log`, { method: 'DELETE' }).catch(() => {});
            }
        }

        async function loadProjectLog(projectId) {
            /**  Загрузить историю логов из файла проекта. */
            if (!projectId) return;
            logLoading.value = true;
            try {
                const resp = await fetch(`/api/audit/${encodeURIComponent(projectId)}/log?limit=500`);
                if (resp.ok) {
                    const data = await resp.json();
                    const entries = (data.entries || []).map(e => ({
                        time: e.timestamp ? new Date(e.timestamp).toLocaleTimeString() : '',
                        level: e.level || 'info',
                        message: e.message || '',
                    }));
                    // Инициализируем массив для проекта если нет, или заменяем
                    projectLogs.value[projectId] = entries;
                }
            } catch (e) {
                console.error('Failed to load project log:', e);
            } finally {
                logLoading.value = false;
            }
        }

        // ─── WebSocket ───
        // Два отдельных WS-соединения: project (лог конкретного проекта) и global (дашборд)
        let wsProject = null;       // /ws/audit/{projectId}
        let wsGlobal = null;        // /ws/global
        let wsProjectReconnects = 0;
        let wsCurrentProjectId = null;
        let wsMode = 'global';      // 'global' | 'project'

        function closeProjectWS() {
            wsCurrentProjectId = null;
            wsProjectReconnects = 0;
            if (wsProject) {
                wsProject.onclose = null;  // убрать reconnect-handler
                wsProject.close();
                wsProject = null;
            }
        }

        function closeGlobalWS() {
            if (wsGlobal) {
                wsGlobal.onclose = null;   // убрать reconnect-handler
                wsGlobal.close();
                wsGlobal = null;
            }
        }

        function connectProjectWS(projectId) {
            // Переключаемся в project-режим: закрываем global, открываем project
            wsMode = 'project';
            closeGlobalWS();
            closeProjectWS();
            wsCurrentProjectId = projectId;
            wsProjectReconnects = 0;
            const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
            wsProject = new WebSocket(`${proto}//${location.host}/ws/audit/${projectId}`);
            wsProject.onopen = () => {
                wsConnected.value = true;
                wsProjectReconnects = 0;
            };
            wsProject.onclose = () => {
                wsConnected.value = false;
                // Переподключение только если мы всё ещё в project-режиме для этого проекта
                if (wsMode === 'project' && wsCurrentProjectId === projectId && wsProjectReconnects < 5) {
                    wsProjectReconnects++;
                    const delay = Math.min(2000 * wsProjectReconnects, 10000);
                    console.log(`[WS] Project WS reconnecting in ${delay}ms (attempt ${wsProjectReconnects})`);
                    setTimeout(() => {
                        if (wsMode === 'project' && wsCurrentProjectId === projectId) {
                            connectProjectWS(projectId);
                        }
                    }, delay);
                }
            };
            wsProject.onmessage = (event) => {
                try {
                    const msg = JSON.parse(event.data);
                    handleWSMessage(msg);
                } catch (e) {
                    console.error('[WS] Project parse error:', e.message);
                }
            };
        }

        function connectGlobalWS() {
            // Переключаемся в global-режим: закрываем project, открываем global
            wsMode = 'global';
            closeProjectWS();
            if (wsGlobal && wsGlobal.readyState === WebSocket.OPEN) return;  // уже подключен
            closeGlobalWS();
            const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
            wsGlobal = new WebSocket(`${proto}//${location.host}/ws/global`);
            wsGlobal.onopen = () => { wsConnected.value = true; };
            wsGlobal.onclose = () => {
                wsConnected.value = false;
                // Переподключение только если мы в global-режиме
                if (wsMode === 'global') {
                    setTimeout(() => {
                        if (wsMode === 'global') connectGlobalWS();
                    }, 3000);
                }
            };
            wsGlobal.onmessage = (event) => {
                try {
                    const msg = JSON.parse(event.data);
                    handleWSMessage(msg);
                } catch (e) {
                    console.error('[WS] Global parse error:', e.message);
                }
            };
        }

        function pushToProjectLog(projectId, entry) {
            /** Добавить запись в лог конкретного проекта. */
            if (!projectId) return;
            if (!projectLogs.value[projectId]) {
                projectLogs.value[projectId] = [];
            }
            projectLogs.value[projectId].push(entry);
            // Авто-скролл если просматриваем этот проект
            if (logProjectId.value === projectId && logAutoScroll.value) {
                nextTick(() => {
                    const el = logContainer.value;
                    if (el) el.scrollTop = el.scrollHeight;
                });
            }
        }

        function handleWSMessage(msg) {
            const time = msg.timestamp ? new Date(msg.timestamp).toLocaleTimeString() : '';
            const pid = msg.project;

            if (msg.type === 'log') {
                pushToProjectLog(pid, {
                    time: time,
                    level: msg.data.level || 'info',
                    message: msg.data.message || '',
                });
            } else if (msg.type === 'progress') {
                // Update current project if viewing it
                if (currentProject.value && currentProject.value.project_id === pid) {
                    currentProject.value.completed_batches = msg.data.current;
                    currentProject.value.total_batches = msg.data.total;
                }
                // Авто-обновление анализа тайлов при завершении батча
                if (currentView.value === 'tiles' && tilesProjectId.value === pid) {
                    loadTileAnalysis(pid);
                    loadPageSummaries(pid);
                    if (selectedPage.value) {
                        loadPageAnalysis(pid, parseInt(selectedPage.value));
                    }
                }
            } else if (msg.type === 'heartbeat') {
                heartbeatData.value = {
                    ...heartbeatData.value,
                    [pid]: msg.data,
                };
                lastHeartbeatTime.value = {
                    ...lastHeartbeatTime.value,
                    [pid]: Date.now(),
                };
                // При heartbeat — обновляем глобальную статистику (если аудит идёт)
                if (msg.data.tokens) {
                    pollGlobalUsage();
                }
            } else if (msg.type === 'complete') {
                pushToProjectLog(pid, {
                    time: time,
                    level: 'success',
                    message: `Аудит завершён. Замечаний: ${msg.data.total_findings}. Время: ${msg.data.duration_minutes} мин.`,
                });
                auditRunning.value = false;
                // Обновляем данные при завершении
                pollLiveStatus();
                refreshProjects();
                // Обновить текущий проект если на его странице
                if (currentView.value === 'project' && currentProject.value && currentProject.value.project_id === pid) {
                    loadProject(pid);
                }
                // Обновить данные анализа тайлов если на странице тайлов
                if (currentView.value === 'tiles' && tilesProjectId.value === pid) {
                    loadTileAnalysis(pid);
                    loadPageSummaries(pid);
                    if (selectedPage.value) {
                        loadPageAnalysis(pid, parseInt(selectedPage.value));
                    }
                }
            } else if (msg.type === 'status') {
                // Реактивное обновление pipeline-индикаторов
                const pipeline = msg.data.pipeline;
                if (pipeline) {
                    if (currentProject.value && currentProject.value.project_id === pid) {
                        currentProject.value.pipeline = pipeline;
                    }
                    const proj = projects.value.find(p => p.project_id === pid);
                    if (proj) proj.pipeline = pipeline;
                }
            } else if (msg.type === 'error') {
                pushToProjectLog(pid, {
                    time: time,
                    level: 'error',
                    message: msg.data.message || 'Неизвестная ошибка',
                });
            } else if (msg.type === 'batch_progress') {
                batchQueue.value = msg.data;
                batchRunning.value = !msg.data.complete;
                if (msg.data.complete) {
                    refreshProjects();
                    selectedProjects.value = new Set();
                    selectAllChecked.value = false;
                }
            }
        }

        // Watch selectedPage → load page analysis
        watch(selectedPage, (newPage) => {
            if (newPage && tilesProjectId.value) {
                loadPageAnalysis(tilesProjectId.value, parseInt(newPage));
            } else {
                pageAnalysis.value = null;
            }
        });

        // Watch severity filter
        watch(filterSeverity, () => {
            const hash = window.location.hash.slice(1);
            const match = hash.match(/^\/project\/([^/]+)\/findings$/);
            if (match) loadFindings(match[1]);
        });

        // ─── Init ───
        onMounted(() => {
            window.addEventListener('hashchange', handleRoute);
            handleRoute();
            connectGlobalWS();
            startPolling();
            loadDisciplines();
            // Глобальная статистика — первый вызов + polling каждые 60с
            pollGlobalUsage();
            usagePollTimer = setInterval(pollGlobalUsage, 60000);
        });

        onUnmounted(() => {
            stopPolling();
            if (usagePollTimer) { clearInterval(usagePollTimer); usagePollTimer = null; }
        });

        return {
            // State
            currentView, currentProject, projects, loading,
            findingsData, filterSeverity, filterSearch, severityOptions,
            tilesProjectId, tilePages, selectedPage, selectedTile,
            tileAnalysis, tileAnalysisLoading, selectedTileAnalysis,
            tileHasAnalysis, tileFindingsCount, tileMaxSeverity,
            pageSummaries, pageAnalysis, pageAnalysisLoading, showPageAnalysis,
            sheetTypeIcon, getPageSummary,
            // Blocks (OCR)
            blocksProjectId, blockPages, selectedBlockPage, selectedBlock,
            blockAnalysis, selectedBlockAnalysis, currentPageBlocks,
            blockHasAnalysis, blockFindingsCount, blockMaxSeverity,
            openBlock, loadBlocks,
            logProjectId, logEntries, logAutoScroll, logContainer, logLoading,
            wsConnected,
            // Live status
            liveStatus,
            isProjectRunning, getProjectLiveInfo,
            stageLabel, formatElapsed, batchPercent, batchProgressText,
            currentProjectLive,
            // Heartbeat
            heartbeatData, lastHeartbeatTime,
            secondsSinceHeartbeat, isHeartbeatStale, getHeartbeatInfo,
            formatETA, heartbeatStatusText,
            // Methods
            navigate, refreshProjects, stepClass, sevClass, sevIcon,
            tileImageUrl, openTile, debounceSearch, clearLog,
            // Prompts
            promptsProjectId, templates, promptsLoading,
            activePromptTab, promptsDiscipline,
            disciplines, showDisciplineDropdown, currentDiscipline,
            loadTemplates, loadDisciplines,
            switchDiscipline, saveTemplate, highlightPlaceholders, syncScroll,
            // Audit actions
            auditRunning, allRunning,
            startPrepare, startTileAudit, startMainAudit,
            startSmartAudit, startStandardAudit, startProAudit,
            startNormVerify, startOptimization, cancelAudit, generateExcel,
            startAllProjects, resumePipeline, resumeInfo,
            retryStage, skipStage, cleanProject,
            // Batch selection
            selectedProjects, selectAllChecked, selectedCount,
            batchRunning, batchQueue,
            showBatchModal, batchMode, batchScope, batchModalCount, batchAllMode,
            toggleProjectSelection, toggleSelectAll, isProjectSelected,
            openBatchModal, confirmBatchAction, startBatchAction, cancelBatch,
            batchActionLabel,
            // Add project
            showAddProject, unregisteredFolders, addProjectLoading,
            scanFolders, registerProject, closeAddProject,
            // Disciplines
            supportedDisciplines, getDisciplineColor, disciplineLabel, disciplineBadgeStyle,
            // Model switcher
            // Usage (global dashboard)
            globalUsage, showUsageDetails, sonnetPercent,
            formatTokens, formatCost, refreshGlobalUsage, resetSessionCounter,
            usageCounters,
            // Usage (per-project)
            projectUsage, currentProjectUsage, stageTokens, stageDurationForProject, formatDuration,
            // Optimization
            optimizationData, optimizationLoading, optimizationFilter,
            filteredOptimization, optimizationTypeLabels, optimizationTypeColors,
            optTypeLabel, optTypeColor, loadOptimization,
            // Document viewer
            documentProjectId, documentPages, documentCurrentPage, documentPageData, documentLoading,
            loadDocument, loadDocumentPage, docPrevPage, docNextPage, renderMarkdown,
            // Computed
            filteredFindings, currentPageTiles,
        };
    }
});

app.mount('#app');
