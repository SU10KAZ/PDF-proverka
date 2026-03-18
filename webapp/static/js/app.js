/**
 * Audit Manager — SPA на Vue 3.
 * Маршрутизация, состояние, API-вызовы, live-статус.
 */
const { createApp, ref, computed, watch, onMounted, onUnmounted, nextTick } = Vue;

const app = createApp({
    setup() {
        // ─── State ───
        const currentView = ref('dashboard');
        const blockBackRoute = ref(null);  // куда вернуться из просмотра блока
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
        const blockCropErrors = ref(0);
        const blockTotalExpected = ref(0);
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

        // ─── Account info ───
        const accountInfo = ref({ email: '—', org: '—', plan: '—', loggedIn: false });
        const showAccountInfo = ref(false);

        const accountSwitching = ref(false);
        const accountAuthUrl = ref(null);
        let accountPollTimer = null;

        async function fetchAccountInfo() {
            try {
                const data = await api('/audit/account');
                accountInfo.value = data;
            } catch (e) {
                console.error('Failed to fetch account info:', e);
            }
        }

        async function switchAccount() {
            accountSwitching.value = true;
            accountAuthUrl.value = null;
            try {
                const resp = await fetch('/api/audit/account/switch', { method: 'POST' });
                const data = await resp.json();
                if (data.auth_url) {
                    accountAuthUrl.value = data.auth_url;
                }
                // Поллинг статуса каждые 2 секунды
                accountPollTimer = setInterval(async () => {
                    try {
                        const st = await api('/audit/account/switch/status');
                        if (st.auth_url && !accountAuthUrl.value) {
                            accountAuthUrl.value = st.auth_url;
                        }
                        if (st.status === 'done') {
                            clearInterval(accountPollTimer);
                            accountPollTimer = null;
                            accountSwitching.value = false;
                            accountAuthUrl.value = null;
                            await fetchAccountInfo();
                        }
                    } catch (e) {
                        console.error('Poll switch status error:', e);
                    }
                }, 2000);
            } catch (e) {
                console.error('Switch account error:', e);
                accountSwitching.value = false;
            }
        }

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

                    // Обновляем auditRunning — только прямые запуски (не batch/all)
                    const directRunning = Object.keys(data.running).filter(k => k !== '__BATCH__' && k !== '__ALL__');
                    auditRunning.value = directRunning.length > 0;
                    batchRunning.value = !!data.running['__BATCH__'];

                    // Pause status из live-status (piggyback)
                    if (data.paused !== undefined) {
                        isPaused.value = data.paused;
                        pauseMode.value = data.pause_mode || null;
                    }

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

        // Этапы, где работает Claude CLI (и есть heartbeat)
        // Остальные (crop_blocks, excel, merge, prepare) — Python-скрипты без Claude
        function isClaudeStage(stage) {
            const claudeStages = ['text_analysis', 'block_analysis', 'findings_merge', 'norm_verify', 'norm_fix', 'optimization', 'tile_audit', 'main_audit'];
            return claudeStages.includes(stage);
        }

        function getRunningStage(projectId) {
            const r = liveStatus.value.running ? liveStatus.value.running[projectId] : null;
            return r ? r.stage : null;
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

        function formatDurationSec(sec) {
            if (sec == null) return '';
            if (sec < 60) return sec + 'с';
            const m = Math.floor(sec / 60);
            const s = sec % 60;
            if (m < 60) return m + 'м' + (s > 0 ? ' ' + s + 'с' : '');
            const h = Math.floor(m / 60);
            const rm = m % 60;
            return h + 'ч' + (rm > 0 ? ' ' + rm + 'м' : '');
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
            const stage = getRunningStage(projectId);
            if (!isClaudeStage(stage)) return 'Выполняется...';
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

            if (hash === '/queue') {
                currentView.value = 'queue';
                connectGlobalWS();
                refreshBatchQueue();
                refreshProjects();  // для списка добавления
            } else if (hash === '/') {
                currentView.value = 'dashboard';
                connectGlobalWS();  // Вернуться на global WS
                refreshProjects();
            } else if (hash.match(/^\/project\/([^/]+)\/findings$/)) {
                const id = decodeURIComponent(hash.match(/^\/project\/([^/]+)\/findings$/)[1]);
                currentView.value = 'findings';
                currentProjectId.value = id;
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
                loadPromptDisciplines().then(() => {
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
        const batchMode = ref('audit');   // audit
        const batchScope = ref('audit');     // audit | optimization | both
        const batchModalCount = ref(0);
        const batchAllMode = ref(false);  // true = запуск для ВСЕХ проектов

        // ─── Pause / Resume ───
        const showPauseModal = ref(false);
        const isPaused = ref(false);
        const pauseMode = ref(null);

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

        function isSectionSelected(sectionCode) {
            const sectionPids = projects.value
                .filter(p => (p.section || 'OTHER') === sectionCode)
                .map(p => p.project_id);
            return sectionPids.length > 0 && sectionPids.every(id => selectedProjects.value.has(id));
        }

        function toggleSectionSelection(sectionCode) {
            const sectionPids = projects.value
                .filter(p => (p.section || 'OTHER') === sectionCode)
                .map(p => p.project_id);
            const s = new Set(selectedProjects.value);
            const allSelected = sectionPids.every(id => s.has(id));
            for (const id of sectionPids) {
                if (allSelected) s.delete(id); else s.add(id);
            }
            selectedProjects.value = s;
            selectAllChecked.value = s.size === projects.value.length && s.size > 0;
        }

        const selectedCount = computed(() => selectedProjects.value.size);

        function openBatchModal() {
            batchModalCount.value = selectedProjects.value.size;
            batchScope.value = 'audit';
            batchAllMode.value = false;
            showBatchModal.value = true;
        }

        async function confirmBatchAction() {
            showBatchModal.value = false;
            // Формируем action: audit, optimization, audit+optimization
            let action = 'audit';
            if (batchScope.value === 'optimization') {
                action = 'optimization';
            } else if (batchScope.value === 'both') {
                action = 'audit+optimization';
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
                'audit': 'Аудит',
                'optimization': 'Оптимизация',
                'audit+optimization': 'Аудит + оптимизация',
                // Legacy
                'standard': 'Аудит',
                'pro': 'Аудит',
                'standard+optimization': 'Аудит + оптимизация',
                'pro+optimization': 'Аудит + оптимизация',
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

        async function addToBatch() {
            const ids = Array.from(selectedProjects.value);
            if (!ids.length) return;
            try {
                const resp = await fetch('/api/audit/batch/add', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ project_ids: ids }),
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
            }
        }

        // ─── Queue Management ───
        const queueAddMode = ref(false);         // показывать ли панель добавления
        const queueAddAction = ref('audit');     // действие для добавляемых
        const queueAddSelected = ref(new Set()); // выбранные для добавления
        const queueDragIdx = ref(null);          // индекс перетаскиваемого элемента
        const queueDragOverIdx = ref(null);      // индекс над которым dragging

        async function refreshBatchQueue() {
            try {
                const resp = await fetch('/api/audit/batch/status');
                const data = await resp.json();
                batchRunning.value = data.active;
                batchQueue.value = data.active ? data.queue : null;
            } catch (e) { /* ignore */ }
        }

        async function removeFromQueue(projectId) {
            try {
                const resp = await fetch('/api/audit/batch/remove', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ project_id: projectId }),
                });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || `Ошибка: ${resp.status}`);
                }
                const data = await resp.json();
                batchQueue.value = data.queue;
            } catch (e) { alert(e.message); }
        }

        async function updateQueueItemAction(projectId, action) {
            try {
                const resp = await fetch('/api/audit/batch/update-action', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ project_id: projectId, action }),
                });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || `Ошибка: ${resp.status}`);
                }
                const data = await resp.json();
                batchQueue.value = data.queue;
            } catch (e) { alert(e.message); }
        }

        async function reorderQueue(newOrder) {
            try {
                const resp = await fetch('/api/audit/batch/reorder', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ order: newOrder }),
                });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || `Ошибка: ${resp.status}`);
                }
                const data = await resp.json();
                batchQueue.value = data.queue;
            } catch (e) { alert(e.message); }
        }

        // Drag and drop для queue items
        function onQueueDragStart(idx) { queueDragIdx.value = idx; }
        function onQueueDragOver(idx) { queueDragOverIdx.value = idx; }
        function onQueueDragEnd() {
            const from = queueDragIdx.value;
            const to = queueDragOverIdx.value;
            queueDragIdx.value = null;
            queueDragOverIdx.value = null;
            if (from === null || to === null || from === to) return;
            if (!batchQueue.value) return;

            // Собираем pending project_ids в новом порядке
            const items = batchQueue.value.items;
            const pendingItems = items.filter(i => i.status === 'pending');
            if (pendingItems.length < 2) return;

            // from/to — это индексы в полном списке, нужно перевести в pending
            const fromItem = items[from];
            const toItem = items[to];
            if (!fromItem || !toItem || fromItem.status !== 'pending') return;

            const pendingIds = pendingItems.map(i => i.project_id);
            const fromPendingIdx = pendingIds.indexOf(fromItem.project_id);
            const toPendingIdx = pendingIds.indexOf(toItem.project_id);
            if (fromPendingIdx < 0) return;

            // Переместить
            pendingIds.splice(fromPendingIdx, 1);
            const insertAt = toPendingIdx < 0 ? pendingIds.length : (fromPendingIdx < toPendingIdx ? toPendingIdx : toPendingIdx);
            pendingIds.splice(insertAt, 0, fromItem.project_id);
            reorderQueue(pendingIds);
        }

        function toggleQueueAddProject(projectId) {
            const s = new Set(queueAddSelected.value);
            if (s.has(projectId)) s.delete(projectId);
            else s.add(projectId);
            queueAddSelected.value = s;
        }

        async function confirmQueueAdd() {
            const ids = Array.from(queueAddSelected.value);
            if (!ids.length) return;
            try {
                const resp = await fetch('/api/audit/batch/add', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ project_ids: ids, action: queueAddAction.value }),
                });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || `Ошибка: ${resp.status}`);
                }
                const data = await resp.json();
                batchQueue.value = data.queue;
                queueAddSelected.value = new Set();
                queueAddMode.value = false;
            } catch (e) { alert(e.message); }
        }

        // Начать очередь из queue view (если очередь не запущена)
        async function startQueueFromView(action) {
            const ids = Array.from(queueAddSelected.value);
            if (!ids.length) return;
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
                queueAddSelected.value = new Set();
                queueAddMode.value = false;
            } catch (e) {
                alert(e.message);
                batchRunning.value = false;
            }
        }

        // Проекты доступные для добавления в очередь
        const queueAvailableProjects = computed(() => {
            if (!projects.value) return [];
            const inQueue = new Set();
            if (batchQueue.value) {
                for (const item of batchQueue.value.items) {
                    if (item.status !== 'completed' && item.status !== 'failed' && item.status !== 'cancelled') {
                        inQueue.add(item.project_id);
                    }
                }
            }
            return projects.value.filter(p => !inQueue.has(p.project_id));
        });

        // ─── Audit Actions ───
        const auditRunning = ref(false);
        // Диалог retry: запустить сейчас или добавить в очередь
        const retryDialog = ref({ show: false, projectId: '', stage: '', stageLabel: '' });

        async function apiPost(path, body) {
            const opts = { method: 'POST' };
            if (body !== undefined) {
                opts.headers = { 'Content-Type': 'application/json' };
                opts.body = JSON.stringify(body);
            }
            const resp = await fetch(`/api${path}`, opts);
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

        async function startAudit(projectId) {
            try {
                auditRunning.value = true;
                await apiPost(`/audit/${projectId}/full-audit`);
                _afterAuditStart(projectId);
            } catch (e) { alert(e.message); auditRunning.value = false; }
        }

        // Legacy aliases
        const startStandardAudit = startAudit;
        const startProAudit = startAudit;

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

        // ─── Pause / Resume (global) ───
        const anyRunning = computed(() => auditRunning.value || batchRunning.value);

        async function pausePipeline(mode) {
            showPauseModal.value = false;
            try {
                const resp = await apiPost('/audit/pause', { mode });
                isPaused.value = true;
                pauseMode.value = mode;
            } catch (e) { alert('Ошибка паузы: ' + e.message); }
        }

        async function resumePipelineGlobal() {
            try {
                await apiPost('/audit/resume');
                isPaused.value = false;
                pauseMode.value = null;
            } catch (e) { alert('Ошибка возобновления: ' + e.message); }
        }

        async function pollPauseStatus() {
            try {
                const resp = await fetch('/api/audit/pause/status');
                if (resp.ok) {
                    const data = await resp.json();
                    isPaused.value = data.paused;
                    pauseMode.value = data.mode || null;
                }
            } catch (_) {}
        }

        // Маппинг pipeline key → API stage name
        const pipelineToStage = {
            'crop_blocks': 'prepare',
            'text_analysis': 'text_analysis',
            'blocks_analysis': 'block_analysis',
            'findings': 'findings_merge',
            'norms_verified': 'norm_verify',
            'optimization': 'optimization',
        };

        const stageLabelMap = {
            'prepare': 'Кроп блоков',
            'text_analysis': 'Анализ текста',
            'block_analysis': 'Анализ блоков',
            'findings_merge': 'Свод замечаний',
            'findings_critic': 'Critic замечаний',
            'findings_review': 'Critic замечаний',
            'findings_corrector': 'Corrector замечаний',
            'norm_verify': 'Верификация норм',
            'optimization': 'Оптимизация',
            'optimization_critic': 'Critic оптимизации',
            'optimization_corrector': 'Corrector оптимизации',
        };

        function canStartFrom(pipelineKey) {
            if (!currentProject.value) return false;
            if (isProjectRunning(currentProject.value.project_id)) return false;
            const status = currentProject.value.pipeline?.[pipelineKey];
            return status === 'done' || status === 'error';
        }

        async function startFromStage(projectId, pipelineKey) {
            const stage = pipelineToStage[pipelineKey];
            if (!stage) return;
            const label = stageLabelMap[stage] || stage;
            // Используем retry-диалог для выбора "сейчас" / "в очередь"
            retryDialog.value = {
                show: true,
                projectId,
                stage,
                stageLabel: label,
            };
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

        function retryStage(projectId, stage) {
            const labels = {
                'crop_blocks': 'Кроп блоков', 'text_analysis': 'Анализ текста',
                'block_analysis': 'Анализ блоков', 'findings_merge': 'Свод замечаний',
                'findings_critic': 'Critic замечаний', 'findings_review': 'Critic замечаний',
                'norm_verify': 'Верификация норм', 'optimization': 'Оптимизация',
            };
            retryDialog.value = {
                show: true,
                projectId,
                stage,
                stageLabel: labels[stage] || stage,
            };
        }

        async function retryStageNow() {
            const { projectId, stage } = retryDialog.value;
            retryDialog.value.show = false;
            try {
                auditRunning.value = true;
                if (stage === 'optimization') {
                    await apiPost(`/optimization/${projectId}/run`);
                } else {
                    await apiPost(`/audit/${projectId}/retry/${stage}`);
                }
                _afterAuditStart(projectId);
            } catch (e) { alert(e.message); auditRunning.value = false; }
        }

        async function retryStageToQueue() {
            const { projectId, stage } = retryDialog.value;
            retryDialog.value.show = false;
            try {
                const resp = await fetch('/api/audit/batch/add-retry', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ project_id: projectId, stage: stage }),
                });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || `API error: ${resp.status}`);
                }
                const data = await resp.json();
                batchQueue.value = data.queue;
                batchRunning.value = true;
            } catch (e) { alert(e.message); }
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
            // Открываем модалку выбора объёма для ВСЕХ проектов
            batchModalCount.value = projects.value.length;
            batchScope.value = 'audit';
            batchAllMode.value = true;
            showBatchModal.value = true;
        }

        async function generateExcel(reportType = 'all') {
            try {
                const data = await apiPost(`/export/excel?report_type=${reportType}`);
                if (data.file) {
                    window.open(`/api/export/download/${data.file}`, '_blank');
                }
            } catch (e) { alert(e.message); }
        }

        // Model Switcher удалён — модели per-stage настроены в config.py → _stage_models

        // ─── Disciplines & Section Groups ───
        const objectName = ref('');
        const supportedDisciplines = ref([]);
        const collapsedSections = ref({});

        const projectsBySection = computed(() => {
            const groups = {};
            // Сначала создаём пустые группы для всех зарегистрированных дисциплин
            for (const d of supportedDisciplines.value) {
                groups[d.code] = [];
            }
            // Затем распределяем проекты по группам
            for (const p of projects.value) {
                const sec = p.section || 'OTHER';
                if (!groups[sec]) groups[sec] = [];
                groups[sec].push(p);
            }
            const order = supportedDisciplines.value.map(d => d.code);
            return Object.entries(groups).sort(([a], [b]) => {
                const ai = order.indexOf(a), bi = order.indexOf(b);
                return (ai === -1 ? 99 : ai) - (bi === -1 ? 99 : bi);
            });
        });

        function toggleSection(code) {
            collapsedSections.value[code] = !collapsedSections.value[code];
        }

        const allSectionsCollapsed = computed(() => {
            const sections = projectsBySection.value;
            if (!sections.length) return false;
            return sections.every(([code]) => collapsedSections.value[code]);
        });

        function toggleAllSections() {
            const collapse = !allSectionsCollapsed.value;
            for (const [code] of projectsBySection.value) {
                collapsedSections.value[code] = collapse;
            }
        }

        // ─── Edit Section ───
        const showEditSection = ref(false);
        const editSectionCode = ref('');
        const editSectionName = ref('');
        const editSectionColor = ref('#3498db');

        function openEditSection(code) {
            const d = supportedDisciplines.value.find(x => x.code === code);
            editSectionCode.value = code;
            editSectionName.value = d ? d.name : code;
            editSectionColor.value = d ? d.color : '#3498db';
            showEditSection.value = true;
        }

        async function saveEditSection() {
            const code = editSectionCode.value;
            const name = editSectionName.value.trim();
            if (!name) return;
            try {
                const resp = await fetch(`/api/projects/disciplines/${encodeURIComponent(code)}`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ name, color: editSectionColor.value }),
                });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || resp.statusText);
                }
                // Обновить локально
                const d = supportedDisciplines.value.find(x => x.code === code);
                if (d) {
                    d.name = name;
                    d.short_name = name;
                    d.color = editSectionColor.value;
                }
                showEditSection.value = false;
            } catch (e) {
                alert('Ошибка: ' + e.message);
            }
        }

        // ─── Excel по разделу ───
        const sectionExcelLoading = ref(null);

        async function exportSectionExcel(sectionCode, sectionProjects) {
            if (!sectionProjects.length) return;
            sectionExcelLoading.value = sectionCode;
            try {
                const resp = await fetch('/api/export/excel/section', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        section: sectionCode,
                        project_ids: sectionProjects.map(p => p.project_id),
                    }),
                });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || resp.statusText);
                }
                const data = await resp.json();
                // Скачать файл
                window.open('/api/export/download/' + encodeURIComponent(data.file), '_blank');
            } catch (e) {
                alert('Ошибка генерации Excel: ' + e.message);
            } finally {
                sectionExcelLoading.value = null;
            }
        }

        // ─── Drag & Drop разделов ───
        const dragSectionCode = ref(null);
        const dragOverCode = ref(null);

        function onSectionDragStart(e, code) {
            dragSectionCode.value = code;
            e.dataTransfer.effectAllowed = 'move';
            e.dataTransfer.setData('text/plain', code);
        }

        let lastDragSwap = 0;
        function onSectionDragOver(e, code) {
            if (dragSectionCode.value && dragSectionCode.value !== code) {
                dragOverCode.value = code;
                e.dataTransfer.dropEffect = 'move';
                // Debounce: не чаще раза в 100ms
                const now = Date.now();
                if (now - lastDragSwap < 100) return;
                lastDragSwap = now;
                // Переставить на лету
                const list = [...supportedDisciplines.value];
                const fromIdx = list.findIndex(d => d.code === dragSectionCode.value);
                const toIdx = list.findIndex(d => d.code === code);
                if (fromIdx !== -1 && toIdx !== -1 && fromIdx !== toIdx) {
                    const [moved] = list.splice(fromIdx, 1);
                    list.splice(toIdx, 0, moved);
                    supportedDisciplines.value = list;
                }
            }
        }

        function onSectionDragEnd() {
            if (dragSectionCode.value) {
                saveSectionOrder();
            }
            dragSectionCode.value = null;
            dragOverCode.value = null;
        }

        async function saveSectionOrder() {
            const codes = supportedDisciplines.value.map(d => d.code);
            try {
                await fetch('/api/projects/disciplines/reorder', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ codes }),
                });
            } catch (e) {
                console.error('Ошибка сохранения порядка:', e);
            }
        }

        async function deleteSection() {
            const code = editSectionCode.value;
            // Проверяем нет ли проектов в этом разделе
            const count = projects.value.filter(p => p.section === code).length;
            if (count > 0) {
                alert(`Нельзя удалить раздел "${code}" — в нём ${count} проект(ов). Сначала перенесите проекты.`);
                return;
            }
            if (!confirm(`Удалить раздел "${code}"?`)) return;
            try {
                const resp = await fetch(`/api/projects/disciplines/${encodeURIComponent(code)}`, {
                    method: 'DELETE',
                });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || resp.statusText);
                }
                supportedDisciplines.value = supportedDisciplines.value.filter(x => x.code !== code);
                showEditSection.value = false;
            } catch (e) {
                alert('Ошибка: ' + e.message);
            }
        }

        async function loadDisciplines() {
            try {
                const data = await api('/projects/disciplines');
                supportedDisciplines.value = data.disciplines;
            } catch (e) {
                console.error('Failed to load disciplines:', e);
                supportedDisciplines.value = [
                    { code: 'EM', name: 'Электроснабжение и электрооборудование', short_name: 'ЭОМ/ЭС', color: '#f39c12' },
                    { code: 'OV', name: 'Отопление, вентиляция и кондиционирование', short_name: 'ОВиК', color: '#3498db' },
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
        const addProjectStep = ref('choose'); // 'choose' | 'section' | 'project'
        const unregisteredFolders = ref([]);
        const addProjectLoading = ref(false);
        const newSectionName = ref('');
        const newSectionCode = ref('');
        const newSectionColor = ref('#3498db');
        const externalPath = ref('');
        const projectSource = ref('local'); // 'local' | 'external'

        function openAddModal() {
            addProjectStep.value = 'choose';
            showAddProject.value = true;
        }

        function goToAddSection() {
            addProjectStep.value = 'section';
            newSectionName.value = '';
            newSectionCode.value = '';
            newSectionColor.value = '#3498db';
        }

        async function goToAddProject() {
            addProjectStep.value = 'project';
            projectSource.value = 'local';
            externalPath.value = '';
            await scanFolders();
        }

        async function addSection() {
            const code = newSectionCode.value.trim().toUpperCase();
            const name = newSectionName.value.trim();
            if (!code || !name) { alert('Укажите код и название раздела'); return; }
            if (supportedDisciplines.value.find(d => d.code === code)) {
                alert('Раздел с таким кодом уже существует');
                return;
            }
            try {
                const resp = await fetch('/api/projects/disciplines', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ code, name, color: newSectionColor.value }),
                });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || `Ошибка: ${resp.status}`);
                }
                // Обновить список дисциплин с сервера
                supportedDisciplines.value.push({
                    code: code,
                    name: name,
                    short_name: name,
                    color: newSectionColor.value,
                    has_profile: false,
                });
                showAddProject.value = false;
            } catch (e) {
                alert('Ошибка: ' + e.message);
            }
        }

        async function scanFolders() {
            addProjectLoading.value = true;
            try {
                const data = await api('/projects/scan');
                const folders = data.folders;
                for (const f of folders) {
                    const detected = await detectDiscipline(f.folder);
                    f._detectedDiscipline = detected;
                    f._selectedDiscipline = detected;
                    f._isExternal = false;
                    f._selectedPdfs = [...f.pdf_files];   // все PDF выбраны по умолчанию
                    f._selectedMds = [...f.md_files];      // все MD выбраны по умолчанию
                }
                unregisteredFolders.value = folders;
            } catch (e) {
                alert('Ошибка сканирования: ' + e.message);
            }
            addProjectLoading.value = false;
        }

        async function scanExternalFolder() {
            const path = externalPath.value.trim();
            if (!path) { alert('Укажите путь к папке'); return; }
            addProjectLoading.value = true;
            try {
                const resp = await fetch('/api/projects/scan-external', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ path }),
                });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || resp.statusText);
                }
                const data = await resp.json();
                const folders = data.folders;
                for (const f of folders) {
                    const detected = await detectDiscipline(f.folder);
                    f._detectedDiscipline = detected;
                    f._selectedDiscipline = detected;
                    f._isExternal = true;
                    f._selectedPdfs = [...f.pdf_files];
                    f._selectedMds = [...f.md_files];
                }
                unregisteredFolders.value = folders;
            } catch (e) {
                alert('Ошибка сканирования: ' + e.message);
            }
            addProjectLoading.value = false;
        }

        async function registerProject(folder) {
            const folderInfo = unregisteredFolders.value.find(f => f.folder === folder);
            if (!folderInfo) return;

            addProjectLoading.value = true;
            const selPdfs = folderInfo._selectedPdfs && folderInfo._selectedPdfs.length > 0
                ? folderInfo._selectedPdfs : [folderInfo.pdf_files[0]];
            const selMds = folderInfo._selectedMds && folderInfo._selectedMds.length > 0
                ? folderInfo._selectedMds : (folderInfo.md_files.length > 0 ? [folderInfo.md_files[0]] : []);
            try {
                let resp;
                if (folderInfo._isExternal && folderInfo.full_path) {
                    resp = await fetch('/api/projects/register-external', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            source_path: folderInfo.full_path,
                            pdf_file: selPdfs[0],
                            pdf_files: selPdfs,
                            md_file: selMds.length > 0 ? selMds[0] : null,
                            md_files: selMds,
                            name: folder,
                            section: folderInfo._selectedDiscipline || 'EM',
                            description: '',
                        }),
                    });
                } else {
                    resp = await fetch('/api/projects/register', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            folder: folder,
                            pdf_file: selPdfs[0],
                            pdf_files: selPdfs,
                            md_file: selMds.length > 0 ? selMds[0] : null,
                            md_files: selMds,
                            name: folder,
                            section: folderInfo._selectedDiscipline || 'EM',
                            description: '',
                        }),
                    });
                }
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || `Ошибка: ${resp.status}`);
                }
                unregisteredFolders.value = unregisteredFolders.value.filter(f => f.folder !== folder);
                await refreshProjects();
                if (unregisteredFolders.value.length === 0) {
                    showAddProject.value = false;
                }
            } catch (e) {
                alert('Ошибка регистрации: ' + e.message);
            }
            addProjectLoading.value = false;
        }

        async function registerAllProjects() {
            const folders = [...unregisteredFolders.value];
            if (folders.length === 0) return;
            if (!confirm(`Добавить все ${folders.length} проект(ов)?`)) return;
            addProjectLoading.value = true;
            let errors = [];
            for (const folderInfo of folders) {
                const sPdfs = folderInfo._selectedPdfs && folderInfo._selectedPdfs.length > 0
                    ? folderInfo._selectedPdfs : [folderInfo.pdf_files[0]];
                const sMds = folderInfo._selectedMds && folderInfo._selectedMds.length > 0
                    ? folderInfo._selectedMds : (folderInfo.md_files.length > 0 ? [folderInfo.md_files[0]] : []);
                try {
                    let resp;
                    if (folderInfo._isExternal && folderInfo.full_path) {
                        resp = await fetch('/api/projects/register-external', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                                source_path: folderInfo.full_path,
                                pdf_file: sPdfs[0],
                                pdf_files: sPdfs,
                                md_file: sMds.length > 0 ? sMds[0] : null,
                                md_files: sMds,
                                name: folderInfo.folder,
                                section: folderInfo._selectedDiscipline || 'EM',
                                description: '',
                            }),
                        });
                    } else {
                        resp = await fetch('/api/projects/register', {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({
                                folder: folderInfo.folder,
                                pdf_file: sPdfs[0],
                                pdf_files: sPdfs,
                                md_file: sMds.length > 0 ? sMds[0] : null,
                                md_files: sMds,
                                name: folderInfo.folder,
                                section: folderInfo._selectedDiscipline || 'EM',
                                description: '',
                            }),
                        });
                    }
                    if (!resp.ok) {
                        const err = await resp.json().catch(() => ({}));
                        throw new Error(err.detail || `Ошибка: ${resp.status}`);
                    }
                    unregisteredFolders.value = unregisteredFolders.value.filter(f => f.folder !== folderInfo.folder);
                } catch (e) {
                    errors.push(`${folderInfo.folder}: ${e.message}`);
                }
            }
            await refreshProjects();
            addProjectLoading.value = false;
            if (errors.length > 0) {
                alert('Ошибки при добавлении:\n' + errors.join('\n'));
            }
            if (unregisteredFolders.value.length === 0) {
                showAddProject.value = false;
            }
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
                if (data.object_name) objectName.value = data.object_name;
                fetchAllProjectUsage();  // загрузить usage для дашборда
            } catch (e) {
                console.error('Failed to load projects:', e);
            }
            loading.value = false;
        }

        async function loadProject(id) {
            currentProjectId.value = id;
            try {
                currentProject.value = await api(`/projects/${encodeURIComponent(id)}`);
                loadResumeInfo(id);
                fetchProjectUsage(id);  // загрузить детальный usage
            } catch (e) {
                console.error('Failed to load project:', e);
                currentProject.value = null;
            }
        }

        // ─── Finding → Block map ───
        const findingBlockMap = ref({});   // {finding_id: [block_ids]}
        const findingBlockInfo = ref({});  // {block_id: {block_id, page, ocr_label}}
        const expandedFindingId = ref(null); // какой finding сейчас раскрыт

        async function loadFindingBlockMap(id) {
            try {
                const data = await api(`/findings/${id}/block-map`);
                findingBlockMap.value = data.block_map || {};
                findingBlockInfo.value = data.block_info || {};
            } catch (e) {
                findingBlockMap.value = {};
                findingBlockInfo.value = {};
            }
        }

        function toggleFindingBlocks(findingId) {
            expandedFindingId.value = expandedFindingId.value === findingId ? null : findingId;
        }

        function getFindingBlocks(findingId) {
            const blockIds = findingBlockMap.value[findingId] || [];
            return blockIds.map(bid => findingBlockInfo.value[bid] || { block_id: bid, page: null, ocr_label: '' });
        }

        function navigateToBlock(blockId, page) {
            const pid = currentProjectId.value;
            // Запомнить откуда пришли и какой элемент был раскрыт
            blockBackRoute.value = {
                hash: window.location.hash || `#/project/${encodeURIComponent(pid)}/findings`,
                expandedFinding: expandedFindingId.value,
                expandedOpt: expandedOptId.value,
            };
            // Переходим в blocks, выставляем нужную страницу и блок
            navigate(`/project/${encodeURIComponent(pid)}/blocks`);
            // После загрузки — выбрать страницу и блок
            nextTick(async () => {
                // Ждём загрузки блоков
                await new Promise(r => setTimeout(r, 300));
                if (page) selectedBlockPage.value = page;
                await nextTick();
                // Найти блок и открыть
                for (const pg of blockPages.value) {
                    const found = (pg.blocks || []).find(b => b.block_id === blockId);
                    if (found) {
                        selectedBlockPage.value = pg.page_num;
                        await nextTick();
                        openBlock(found);
                        // Скролл к блоку
                        const el = document.querySelector(`[data-block-id="${blockId}"]`);
                        if (el) el.scrollIntoView({ behavior: 'smooth', block: 'center' });
                        break;
                    }
                }
            });
        }

        function goBackFromBlock() {
            if (blockBackRoute.value) {
                const back = blockBackRoute.value;
                blockBackRoute.value = null;
                window.location.hash = back.hash;
                // Восстановить раскрытый элемент после навигации
                nextTick(() => {
                    setTimeout(() => {
                        if (back.expandedFinding) expandedFindingId.value = back.expandedFinding;
                        if (back.expandedOpt) expandedOptId.value = back.expandedOpt;
                    }, 200);
                });
            }
        }

        async function loadFindings(id) {
            findingsData.value = null;
            expandedFindingId.value = null;
            try {
                const params = new URLSearchParams();
                if (filterSeverity.value) params.set('severity', filterSeverity.value);
                if (filterSearch.value) params.set('search', filterSearch.value);
                const qs = params.toString();
                findingsData.value = await api(`/findings/${id}${qs ? '?' + qs : ''}`);
                // Загрузить маппинг блоков параллельно
                loadFindingBlockMap(id);
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
            blockCropErrors.value = 0;
            blockTotalExpected.value = 0;
            try {
                const [blocksData] = await Promise.all([
                    api(`/tiles/${id}/blocks`),
                    loadBlockAnalysis(id),
                    loadBlockToFindingsMap(id),
                ]);
                blockPages.value = blocksData.pages || [];
                blockCropErrors.value = blocksData.errors || 0;
                blockTotalExpected.value = blocksData.total_expected || 0;
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

        // ─── Block → Finding (обратная связь) ───
        // Маппинг block_id → [F-замечания] для показа в split-view блока
        const blockToFindings = ref({});  // {block_id: [{id, severity, problem, norm}]}

        async function loadBlockToFindingsMap(id) {
            try {
                // Загрузить block-map и findings параллельно
                const [mapData, findingsResp] = await Promise.all([
                    api(`/findings/${id}/block-map`),
                    api(`/findings/${id}`),
                ]);
                const bmap = mapData.block_map || {};
                const findings = findingsResp.findings || [];
                // Построить обратный маппинг
                const reverse = {};
                for (const f of findings) {
                    const blocks = bmap[f.id] || [];
                    for (const bid of blocks) {
                        if (!reverse[bid]) reverse[bid] = [];
                        reverse[bid].push({
                            id: f.id,
                            severity: f.severity,
                            problem: f.problem || f.finding || f.description || '',
                            norm: f.norm || '',
                            solution: f.solution || f.recommendation || '',
                        });
                    }
                }
                blockToFindings.value = reverse;
            } catch (e) {
                blockToFindings.value = {};
            }
        }

        function getBlockFindings(blockId) {
            return blockToFindings.value[blockId] || [];
        }

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

        // ─── Optimization → Block map ───
        const optBlockMap = ref({});       // {opt_id: [block_ids]}
        const optBlockInfo = ref({});      // {block_id: {block_id, page, ocr_label}}
        const expandedOptId = ref(null);

        async function loadOptBlockMap(id) {
            try {
                const data = await api(`/optimization/${id}/block-map`);
                optBlockMap.value = data.block_map || {};
                optBlockInfo.value = data.block_info || {};
            } catch (e) {
                optBlockMap.value = {};
                optBlockInfo.value = {};
            }
        }

        function toggleOptBlocks(optId) {
            expandedOptId.value = expandedOptId.value === optId ? null : optId;
        }

        function getOptBlocks(optId) {
            const blockIds = optBlockMap.value[optId] || [];
            return blockIds.map(bid => optBlockInfo.value[bid] || { block_id: bid, page: null, ocr_label: '' });
        }

        async function loadOptimization(id) {
            currentProjectId.value = id;
            optimizationLoading.value = true;
            optimizationData.value = null;
            expandedOptId.value = null;
            try {
                currentProject.value = await api(`/projects/${id}`);
                const resp = await api(`/optimization/${id}`);
                if (resp.has_data) {
                    optimizationData.value = resp.data;
                }
                loadOptBlockMap(id);
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

        const _optTypeOrder = { 'cheaper_analog': 0, 'faster_install': 1, 'simpler_design': 2, 'lifecycle': 3 };
        const filteredOptimization = computed(() => {
            if (!optimizationData.value) return [];
            const items = optimizationData.value.items || [];
            const filtered = optimizationFilter.value ? items.filter(i => i.type === optimizationFilter.value) : items;
            return [...filtered].sort((a, b) => (_optTypeOrder[a.type] ?? 9) - (_optTypeOrder[b.type] ?? 9));
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

        // Этапы которые не запускались (для pipeline summary)
        const _allPipelineStages = [
            {key: 'crop_blocks', label: 'Кроп блоков'},
            {key: 'text_analysis', label: 'Анализ текста'},
            {key: 'block_analysis', label: 'Анализ блоков'},
            {key: 'findings_merge', label: 'Свод замечаний'},
            {key: 'findings_critic', label: 'Critic замечаний'},
            {key: 'findings_corrector', label: 'Corrector замечаний'},
            {key: 'norm_verify', label: 'Верификация норм'},
            {key: 'optimization', label: 'Оптимизация'},
            {key: 'optimization_critic', label: 'Critic оптимизации'},
            {key: 'optimization_corrector', label: 'Corrector оптимизации'},
        ];
        const pipelineMissingStages = computed(() => {
            const p = currentProject.value;
            if (!p || !p.pipeline_summary || p.pipeline_summary.length === 0) return [];
            const present = new Set(p.pipeline_summary.map(s => s.key));
            // Показывать пропущенные только если конвейер вообще запускался
            return _allPipelineStages
                .filter(s => !present.has(s.key))
                .map(s => s.label);
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
            if (s.includes('РЕКОМЕНД')) return 'recommended';
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
        async function loadPromptDisciplines() {
            try {
                const resp = await fetch('/api/audit/disciplines');
                if (!resp.ok) return;
                const data = await resp.json();
                disciplines.value = data.disciplines || [];
            } catch (e) {
                console.error('loadPromptDisciplines error:', e);
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
                    message: `Аудит завершён. Замечаний: ${msg.data.total_findings}. Время: ${msg.data.duration_minutes} мин.` + (msg.data.pause_minutes > 1 ? ` (паузы: ${msg.data.pause_minutes} мин)` : ''),
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
            // Информация об аккаунте
            fetchAccountInfo();
        });

        onUnmounted(() => {
            stopPolling();
            if (usagePollTimer) { clearInterval(usagePollTimer); usagePollTimer = null; }
        });

        return {
            // State
            currentView, currentProject, currentProjectId, projects, loading,
            findingsData, filterSeverity, filterSearch, severityOptions,
            findingBlockMap, findingBlockInfo, expandedFindingId,
            toggleFindingBlocks, getFindingBlocks, navigateToBlock, blockBackRoute, goBackFromBlock,
            tilesProjectId, tilePages, selectedPage, selectedTile,
            tileAnalysis, tileAnalysisLoading, selectedTileAnalysis,
            tileHasAnalysis, tileFindingsCount, tileMaxSeverity,
            pageSummaries, pageAnalysis, pageAnalysisLoading, showPageAnalysis,
            sheetTypeIcon, getPageSummary,
            // Blocks (OCR)
            blocksProjectId, blockPages, blockCropErrors, blockTotalExpected,
            selectedBlockPage, selectedBlock,
            blockAnalysis, selectedBlockAnalysis, currentPageBlocks,
            blockHasAnalysis, blockFindingsCount, blockMaxSeverity,
            openBlock, loadBlocks, blockToFindings, getBlockFindings,
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
            formatETA, heartbeatStatusText, isClaudeStage, getRunningStage,
            // Methods
            navigate, refreshProjects, stepClass, sevClass, sevIcon,
            tileImageUrl, openTile, debounceSearch, clearLog,
            // Prompts
            promptsProjectId, templates, promptsLoading,
            activePromptTab, promptsDiscipline,
            disciplines, showDisciplineDropdown, currentDiscipline,
            loadTemplates, loadPromptDisciplines,
            switchDiscipline, saveTemplate, highlightPlaceholders, syncScroll,
            // Audit actions
            auditRunning, allRunning,
            startPrepare, startTileAudit, startMainAudit,
            startSmartAudit, startAudit, startStandardAudit, startProAudit,
            startNormVerify, startOptimization, cancelAudit, generateExcel,
            startAllProjects, resumePipeline, resumeInfo,
            startFromStage, canStartFrom, pipelineToStage,
            retryStage, retryDialog, retryStageNow, retryStageToQueue,
            skipStage, cleanProject,
            // Batch selection
            selectedProjects, selectAllChecked, selectedCount,
            batchRunning, batchQueue,
            showBatchModal, batchMode, batchScope, batchModalCount, batchAllMode,
            // Pause
            showPauseModal, isPaused, pauseMode, anyRunning,
            pausePipeline, resumePipelineGlobal,
            toggleProjectSelection, toggleSelectAll, isProjectSelected,
            isSectionSelected, toggleSectionSelection,
            sectionExcelLoading, exportSectionExcel,
            openBatchModal, confirmBatchAction, startBatchAction, cancelBatch, addToBatch,
            batchActionLabel,
            // Queue management
            queueAddMode, queueAddAction, queueAddSelected, queueDragIdx, queueDragOverIdx,
            refreshBatchQueue, removeFromQueue, updateQueueItemAction, reorderQueue,
            onQueueDragStart, onQueueDragOver, onQueueDragEnd,
            toggleQueueAddProject, confirmQueueAdd, startQueueFromView,
            queueAvailableProjects,
            // Add project
            showAddProject, addProjectStep, unregisteredFolders, addProjectLoading,
            openAddModal, goToAddSection, goToAddProject, addSection,
            newSectionName, newSectionCode, newSectionColor,
            scanFolders, scanExternalFolder, registerProject, registerAllProjects, closeAddProject,
            externalPath, projectSource,
            // Disciplines
            supportedDisciplines, getDisciplineColor, disciplineLabel, disciplineBadgeStyle,
            objectName, projectsBySection, collapsedSections, toggleSection,
            allSectionsCollapsed, toggleAllSections,
            showEditSection, editSectionCode, editSectionName, editSectionColor,
            openEditSection, saveEditSection, deleteSection,
            dragSectionCode, dragOverCode,
            onSectionDragStart, onSectionDragOver, onSectionDragEnd,
            // Model switcher
            // Usage (global dashboard)
            globalUsage, showUsageDetails, sonnetPercent,
            accountInfo, showAccountInfo, fetchAccountInfo,
            accountSwitching, accountAuthUrl, switchAccount,
            formatTokens, formatCost, formatDurationSec, refreshGlobalUsage, resetSessionCounter,
            usageCounters,
            // Usage (per-project)
            projectUsage, currentProjectUsage, stageTokens, stageDurationForProject, formatDuration,
            // Pipeline summary
            pipelineMissingStages,
            // Optimization
            optimizationData, optimizationLoading, optimizationFilter,
            optBlockMap, optBlockInfo, expandedOptId,
            toggleOptBlocks, getOptBlocks,
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
