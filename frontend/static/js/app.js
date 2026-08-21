/**
 * Audit Manager — SPA на Vue 3.
 * Маршрутизация, состояние, API-вызовы, live-статус.
 */
const { createApp, ref, reactive, computed, watch, onMounted, onUnmounted, nextTick } = Vue;

const app = createApp({
    setup() {
        // ─── State ───
        const theme = ref(localStorage.getItem('audit-theme') || 'dark');
        document.documentElement.setAttribute('data-theme', theme.value);

        const currentView = ref('dashboard');
        // Изолированный UI-контроллер распределённых вычислений. Production
        // читает только AuditManager API; mock включается лишь явным demo-флагом.
        const distributed = window.DistributedFeature.createManager();
        const blockBackRoute = ref(null);  // куда вернуться из просмотра блока

        // ─── Пользователи (сотрудники-эксперты) ────────────────────────────
        // Список сотрудников + текущий активный (от его имени сохраняются
        // решения эксперта — пишется в expert_reviewer). Активность каждого
        // агрегируется из knowledge_base/decisions_log.json.
        const usersList = ref([]);
        const usersCurrentId = ref(null);
        const usersLoading = ref(false);
        const usersAuthEnabled = ref(false);       // включена ли портальная авторизация
        const usersLoggedInUsername = ref(null);   // логин активной сессии
        const usersLoggedInMatched = ref(false);   // сопоставлен ли логин с сотрудником
        const currentProjectId = ref(null);
        const currentProject = ref(null);
        const visiblePipelineSummary = computed(() => {
            const rows = currentProject.value?.pipeline_summary;
            if (!Array.isArray(rows)) return [];
            const presentKeys = new Set(rows.map(row => String(row?.key || '')).filter(Boolean));
            return rows.filter(row => {
                const canonicalKey = String(row?.canonical_key || '');
                return !canonicalKey || !presentKeys.has(canonicalKey);
            });
        });
        const projectLoading = ref(false);   // идёт ли сейчас загрузка карточки проекта
        const projects = ref([]);
        const loading = ref(false);

        // ─── Версионность проекта ───────────────────────────────────────
        // activeVersionId — версия, в контексте которой сейчас работаем на
        // странице проекта. null = latest (для дашборда тоже latest). Все
        // load*/api*/start* функции автоматически подмешивают её в URL.
        const activeVersionId = ref(null);
        // versions_summary текущего проекта (массив записей из backend).
        const projectVersions = ref([]);
        const projectVersionsLoading = ref(false);
        // Список файлов активной версии (для upload).
        const versionFiles = ref([]);
        // Прогресс / последняя ошибка загрузки файлов в версию.
        const versionUploading = ref(false);
        const versionUploadError = ref('');
        // Выезжающая снизу панель с PDF активной версии (родной просмотрщик
        // браузера в <iframe>). false по умолчанию → до клика ничего не грузится.
        const showVersionPdf = ref(false);
        // ─── Переименование папки проекта (карандаш рядом с версией) ───
        const renameEditing = ref(false);
        const renameValue = ref('');
        const renameError = ref('');
        const renameBusy = ref(false);
        const renameInput = ref(null);

        // ─── Контроль ранее согласованных замечаний (migrated findings) ───
        // Отчёт пишется backend'ом в _versions/v{N}/_output/migrated_findings_report.json.
        // На фронте — только чтение/запуск, без редактирования содержимого.
        const migratedFindingsReport = ref(null);
        const migratedFindingsReportLoading = ref(false);
        const migratedFindingsError = ref('');

        // VersionAPI помещён в глобал через version_api.js (UMD). На случай
        // деплоя без CDN-фоллбека держим локальную stub-имплементацию.
        const VAPI = (typeof window !== 'undefined' && window.VersionAPI) ? window.VersionAPI : null;

        // Capabilities текущего сервера. backend.app.main:app поддерживает V2
        // audit и version-aware read-роутеры (cutover: 2026-05-14).
        // Если когда-нибудь снова откатимся на webapp.main:app — поменять на false.
        const serverCaps = {
            v2AuditSupported: true,
            runner: 'backend',
        };
        function _apiUrl(path, withVersion) {
            if (!VAPI) return '/api' + (path.startsWith('/') ? path : '/' + path);
            return VAPI.apiUrl(path, {
                versionId: activeVersionId.value,
                withVersion: withVersion !== false,
            });
        }

        // ─── PDF версии (drawer снизу) ──────────────────────────────────
        // URL стрим-эндпоинта PDF активной версии. Зависит от activeVersionId
        // через _apiUrl (тот подмешивает ?version_id=) → при смене версии
        // iframe сам перезагрузит нужный PDF. Отдаётся браузеру частями (Range).
        const versionPdfUrl = computed(() => {
            if (!currentProject.value) return '';
            return _apiUrl('/document/' +
                encodeURIComponent(currentProject.value.project_id) + '/pdf');
        });
        // Метка активной версии для заголовка панели (V1/V2/…).
        const activeVersionLabel = computed(() => {
            const vid = activeVersionId.value ||
                (currentProject.value && currentProject.value.latest_version_id);
            const v = (projectVersions.value || []).find(x => x.version_id === vid);
            return v ? v.label : '';
        });
        function toggleVersionPdf() { showVersionPdf.value = !showVersionPdf.value; }

        // ─── Data Cache ───
        const _cache = {
            project: new Map(),    // id → {data, ts}
            findings: new Map(),   // id → {data, ts}
            optimization: new Map(), // id → {data, ts}
            blocks: new Map(),     // id → {data, ts}
            TTL: 60000,            // 60 секунд — после этого перезапрос
        };
        function _cacheGet(type, id) {
            const entry = _cache[type].get(id);
            if (!entry) return null;
            if (Date.now() - entry.ts > _cache.TTL) { _cache[type].delete(id); return null; }
            return entry.data;
        }
        function _cacheSet(type, id, data) {
            _cache[type].set(id, { data, ts: Date.now() });
        }
        function _cacheInvalidate(type, id) {
            if (id) _cache[type].delete(id);
            else _cache[type].clear();
        }

        // Sidebar
        const sidebarSectionsOpen = ref(false);  // по умолчанию «Разделы» свёрнуты
        const sidebarFilterSection = ref(null);  // null = все разделы

        // Findings
        const findingsData = ref(null);
        const filterSeverity = ref('');
        const filterSearch = ref('');
        const severityOptions = [
            'КРИТИЧЕСКОЕ', 'ЭКОНОМИЧЕСКОЕ', 'ЭКСПЛУАТАЦИОННОЕ',
            'РЕКОМЕНДАТЕЛЬНОЕ', 'ПРОВЕРИТЬ ПО СМЕЖНЫМ'
        ];

        // ─── Pagination ───
        const PAGE_SIZE = 50;
        const findingsPage = ref(1);
        const optimizationPage = ref(1);
        const discussionPage = ref(1);

        // ─── Critic v2 UI Triage View (experimental, offline-only) ─────────
        // NOTE: Reads offline artifact critic_v2_triage_ui.json produced by
        // backend/scripts/replay_critic_v2_triage_policy.py --ui-export.
        // Does NOT touch production pipeline, legacy critic, or 03_findings_review.json.

        // Russian labels for engineer-facing display.
        // Backend tokens stay in english (used by replay/tuning/feedback JSON).
        // This dict only translates for the screen.
        const CV2_LABELS = {
            queue: {
                strong_keep: 'однозначно оставить',
                main_review: 'на проверку',
                borderline: 'спорное',
                needs_context: 'требует смежников',
                suggested_reject: 'к отклонению',
                hidden_by_critic: 'скрыть как мусор',
            },
            reason: {
                deterministic_accept_high_score: 'высокий score, evidence валидна',
                accepted_good_score_evidence: 'хороший score + evidence',
                borderline: 'на границе порогов',
                needs_context: 'нужен контекст из смежных разделов',
                suggested_reject_not_safe_to_hide: 'к отклонению (но не скрывать молча)',
                guard_blocked_llm_reject: 'LLM хотел отклонить — блокировано guard’ом',
                'det_reject:no_evidence': 'отклонено: нет evidence',
                'det_reject:ocr_artifact': 'отклонено: OCR-артефакт',
                'det_reject:low_business_value': 'отклонено: низкая практическая ценность',
                'llm_reject:already_resolved_by_project_note':
                    'отклонено LLM: уже решено в примечаниях проекта',
                round1_ocr_artifact_suggested_reject:
                    'OCR / ошибка распознавания',
                round1_rd_vs_pz_suggested_reject:
                    'расчётный параметр: ПЗ/расчёт, не чертёж РД',
                round1_already_covered_suggested_reject:
                    'уже есть в смежном разделе / спецификации',
                round2_rd_vs_pz_suggested_reject:
                    'расчётный параметр: ПЗ/расчёт, не чертёж РД',
                round2_already_covered_suggested_reject:
                    'уже есть в смежном разделе / спецификации',
            },
            evidence: {
                valid: 'валидна',
                partial: 'частичная',
                weak: 'слабая',
                none: 'нет',
            },
            source: {
                enough_source: 'источника достаточно',
                needs_more_context: 'нужно больше контекста',
                cross_section_required: 'нужны смежные разделы',
            },
            taxonomy: {
                other: 'другое',
                acceptable_design_solution: 'допустимое проектное решение',
                already_resolved_by_project_note: 'уже учтено в примечаниях',
                duplicate_or_already_covered: 'дубликат / уже покрыто',
                false_positive_due_to_missing_context:
                    'ложное срабатывание из-за нехватки контекста',
                insufficient_source_context: 'недостаточно исходного контекста',
                not_functionally_significant: 'не критично функционально',
                requirement_not_mandatory: 'требование добровольное',
            },
            risk: {
                low: 'низкий',
                medium: 'средний',
                high: 'высокий',
            },
            human: {
                accepted: 'принято',
                rejected: 'отклонено',
            },
            tab: {
                primary: 'Основная проверка',
                needs_context: 'Требует смежников',
                suggested_reject: 'К отклонению',
                hidden_by_critic: 'Скрыто критиком',
            },
            alignment: {
                aligned_visible:
                    'эксперт принял, critic оставил в основной',
                aligned_hidden:
                    'эксперт отклонил, critic свернул',
                accepted_collapsed:
                    'эксперт принял, critic свернул — проверить',
                accepted_needs_context:
                    'эксперт принял, critic отправил в контекст — проверить',
                rejected_visible:
                    'эксперт отклонил, critic оставил в основной',
                rejected_needs_context:
                    'эксперт отклонил, critic отправил в контекст',
                unknown:
                    'нет решения эксперта',
            },
            triage_correct: {
                yes: 'да, верно',
                no: 'нет, неверно',
                unsure: 'не уверен',
            },
            priority: {
                normal: 'обычный',
                important: 'важно',
                critical: 'критично',
            },
        };

        function cv2HumanizeExplanation(text) {
            // Translates short diagnostic strings like "score=10, ev=valid" or
            // "score=8, ev=partial; needs_context" into a Russian-friendly form.
            // Conservative: only known tokens are replaced; unknown text stays.
            if (!text) return '';
            let out = String(text);
            out = out.replace(/\bscore\s*=\s*(\d+)\b/gi, 'балл=$1');
            out = out.replace(/\bev\s*=\s*(valid|partial|weak|none)\b/gi,
                (_, v) => 'evidence=' + (CV2_LABELS.evidence[v.toLowerCase()] || v));
            return out;
        }

        // Classification of an item against expert_review (human_decision/tab).
        // The artifact already carries human_decision; we just compute the
        // alignment status here. UI-only — backend tokens are unchanged.
        // accepted_needs_context is kept separate from accepted_collapsed:
        // sending an accepted finding to "needs_context" is a softer mismatch
        // than burying it under suggested_reject/hidden_by_critic, and the
        // engineer review queue treats them differently.
        function cv2AlignmentOf(item) {
            if (!item) return 'unknown';
            const hd = item.human_decision;
            const tab = item.tab;
            if (!hd || hd === 'unknown') return 'unknown';
            if (hd === 'accepted') {
                if (tab === 'primary') return 'aligned_visible';
                if (tab === 'needs_context') return 'accepted_needs_context';
                if (tab === 'suggested_reject' || tab === 'hidden_by_critic') {
                    return 'accepted_collapsed';
                }
                return 'unknown';
            }
            if (hd === 'rejected') {
                if (tab === 'hidden_by_critic' || tab === 'suggested_reject') {
                    return 'aligned_hidden';
                }
                if (tab === 'needs_context') return 'rejected_needs_context';
                if (tab === 'primary') return 'rejected_visible';
            }
            return 'unknown';
        }

        // Disagreement = decision known and not aligned.
        // accepted_needs_context is treated as a disagreement: the spec wants
        // the reviewer to be able to surface it on the "Расхождения" view.
        function cv2IsDisagreement(alignment) {
            return alignment === 'accepted_collapsed'
                || alignment === 'accepted_needs_context'
                || alignment === 'rejected_visible'
                || alignment === 'rejected_needs_context';
        }

        function cv2Label(group, token) {
            // Returns Russian label for an english token. Falls back to the token
            // itself if no mapping is defined (so new vocabulary is still readable).
            if (token === null || token === undefined || token === '') return '';
            const dict = CV2_LABELS[group];
            if (!dict) return String(token);
            return dict[token] || String(token);
        }

        // ─── Critic v2 dev-flag: показывать ли отдельные debug-routes ─────────
        // Основной UX — колонка в обычной таблице "Замечания". Старый
        // experimental UI остаётся только для разработчика. Включается:
        //   localStorage.setItem('cv2_debug', '1')       — постоянно
        //   ?cv2debug=1 в URL                            — на текущую сессию
        //   window.cv2EnableDebug() / cv2DisableDebug()  — из консоли
        // Routes (#/critic-v2-ui, #/project/.../critic-v2*) остаются доступны
        // напрямую по URL даже без флага — флаг прячет только entry в навигации.
        function _readCv2DebugFlag() {
            try {
                if (typeof window === 'undefined') return false;
                const url = new URL(window.location.href);
                if (url.searchParams.get('cv2debug') === '1') return true;
                if (window.localStorage && window.localStorage.getItem('cv2_debug') === '1') return true;
            } catch (_) { /* SSR / sandboxed iframe */ }
            return false;
        }
        const cv2DebugVisible = ref(_readCv2DebugFlag());
        if (typeof window !== 'undefined') {
            window.cv2EnableDebug = function () {
                try { window.localStorage.setItem('cv2_debug', '1'); } catch (_) {}
                cv2DebugVisible.value = true;
                console.info('[cv2] debug nav enabled (localStorage.cv2_debug=1)');
            };
            window.cv2DisableDebug = function () {
                try { window.localStorage.removeItem('cv2_debug'); } catch (_) {}
                cv2DebugVisible.value = false;
                console.info('[cv2] debug nav disabled');
            };
        }

        // ─── Critic v2 → display score (0–100) for inline findings table ─────
        // Pure-функции. Дублируются в frontend/tests/cv2_findings_table.test.js
        // как mirror — если логика разойдётся, тест упадёт первым.
        // Backend поля не меняются: queue/score/confidence приходят как есть.

        // queue → диапазон [min, max] на 0–100
        const CV2_DISPLAY_QUEUE_RANGE = {
            strong_keep:      [90, 100],
            main_review:      [65,  85],
            borderline:       [50,  65],
            needs_context:    [40,  59],
            suggested_reject: [20,  39],
            hidden_by_critic: [ 0,  19],
        };

        // bucket → [lo, hi] на 0–100; используется и для label, и для фильтра
        const CV2_DISPLAY_BUCKETS = [
            { key: 'must_review',     label: 'важно проверить',       lo: 85, hi: 100 },
            { key: 'review',          label: 'на проверку',           lo: 60, hi:  84 },
            { key: 'needs_context',   label: 'нужен контекст',        lo: 40, hi:  59 },
            { key: 'likely_reject',   label: 'вероятно к отклонению', lo: 20, hi:  39 },
            { key: 'hidden',          label: 'скрыто Critic v2',      lo:  0, hi:  19 },
        ];

        function cv2DisplayScore(item) {
            // Маппит queue + (score 0–10, confidence 0–1) → display score 0–100.
            // Внутри диапазона очереди двигаем по нормализованной (score+confidence).
            if (!item) return null;
            const range = CV2_DISPLAY_QUEUE_RANGE[item.queue];
            if (!range) return null;
            const [lo, hi] = range;
            const span = hi - lo;
            // Нормализуем интенсивность: 70% от score (0–10) + 30% от confidence (0–1).
            const s = Number.isFinite(item.score) ? Math.max(0, Math.min(10, item.score)) / 10 : 0.5;
            const c = Number.isFinite(item.confidence) ? Math.max(0, Math.min(1, item.confidence)) : 0.5;
            const intensity = 0.7 * s + 0.3 * c;
            // Для suggested_reject/hidden высокая уверенность critic'а = НИЖНЯЯ оценка
            // (он уверен, что это не нужно), для остальных — наоборот.
            const inverted = item.queue === 'suggested_reject' || item.queue === 'hidden_by_critic';
            const t = inverted ? (1 - intensity) : intensity;
            return Math.round(lo + span * t);
        }

        function cv2DisplayBucket(score) {
            if (!Number.isFinite(score)) return null;
            for (const b of CV2_DISPLAY_BUCKETS) {
                if (score >= b.lo && score <= b.hi) return b;
            }
            return null;
        }

        function cv2DisplayLabel(score) {
            const b = cv2DisplayBucket(score);
            return b ? b.label : '';
        }

        // CSS-класс цвета бейджа (зелёный → красный по понижению score)
        function cv2DisplayClass(score) {
            const b = cv2DisplayBucket(score);
            return b ? ('cv2-disp-' + b.key) : 'cv2-disp-na';
        }

        // finding_id в triage-ui = "<project>:F-NNN"; в /api/findings = "F-NNN".
        // Извлекаем хвост после последнего ':'. Если ':' нет — возвращаем как есть.
        function cv2BareFindingId(rawId) {
            if (!rawId) return '';
            const s = String(rawId);
            const idx = s.lastIndexOf(':');
            return idx >= 0 ? s.slice(idx + 1) : s;
        }

        // Скрывать ли finding по умолчанию (tab=hidden_by_critic ИЛИ score≤19).
        // Используется в _applyFindingsFilter, когда cv2ShowHidden = false.
        function cv2IsHiddenByDefault(item) {
            if (!item) return false;
            if (item.tab === 'hidden_by_critic') return true;
            const score = cv2DisplayScore(item);
            return Number.isFinite(score) && score <= 19;
        }

        const cv2Export = ref(null);
        const cv2LoadError = ref('');
        const cv2ActiveTab = ref('primary');
        const cv2Filter = ref({
            section: '',
            queue: '',
            reason: '',
            evidence: '',
            scoreBucket: '',
            human: '',
            alignment: '',
        });

        function cv2ResetFilters() {
            cv2Filter.value = {
                section: '', queue: '', reason: '',
                evidence: '', scoreBucket: '', human: '',
                alignment: '',
            };
        }

        function cv2ParseExport(raw) {
            // Accepts a parsed JSON object. Validates shape: must have summary,
            // tabs (array of 4), items (array). Returns the same object on success
            // or throws an Error.
            if (!raw || typeof raw !== 'object') {
                throw new Error('JSON: ожидается объект.');
            }
            if (!raw.summary || typeof raw.summary !== 'object') {
                throw new Error('JSON: отсутствует "summary".');
            }
            if (!Array.isArray(raw.tabs) || raw.tabs.length !== 4) {
                throw new Error('JSON: ожидается ровно 4 вкладки в "tabs".');
            }
            if (!Array.isArray(raw.items)) {
                throw new Error('JSON: отсутствует массив "items".');
            }
            const expectedKeys = ['primary', 'needs_context',
                                  'suggested_reject', 'hidden_by_critic'];
            const actualKeys = raw.tabs.map(t => t.key);
            for (const k of expectedKeys) {
                if (!actualKeys.includes(k)) {
                    throw new Error(`JSON: вкладка "${k}" отсутствует.`);
                }
            }
            return raw;
        }

        // Project-scoped view state. Loader fetches read-only export from backend.
        const cv2ProjLoading = ref(false);
        const cv2ProjLoadError = ref('');
        const cv2ProjHint = ref('');
        // Disagreements mode is set when the user opens
        // #/project/<id>/critic-v2-disagreements. It pre-selects the
        // alignment=__disagreement__ filter and marks the feedback export
        // scope as "project_disagreements" so downstream tooling can tell
        // the two flows apart.
        const cv2ProjDisagreementsMode = ref(false);

        // Sub-mode внутри единой вкладки «Critic v2».
        // Значения: 'disagreements' | 'all' | 'assisted' | 'feedback'.
        // disagreements/all — режимы основного списка очередей (alignment-фильтр).
        // assisted — фокус на panel «Проверочные карточки assisted_round1».
        // feedback — фокус на panel «Импорт / экспорт feedback».
        // Sub-mode derived из cv2ProjDisagreementsMode (для backward compat
        // hash routes), но также может переключаться кликом sub-tab.
        const cv2ProjSubMode = ref('disagreements');

        // sync cv2ProjDisagreementsMode → cv2ProjSubMode когда меняется hash-route.
        // (Прямой watch не использую — Vue 3 в setup() уже реактивен, и
        // обновление cv2ProjDisagreementsMode из cv2LoadProject не должно
        // overwrite-ить пользовательский выбор sub-tab. См. _cv2DerivedSubMode.)
        function _cv2DerivedSubMode() {
            return cv2ProjDisagreementsMode.value ? 'disagreements' : 'all';
        }

        // Click handler для sub-tab. Обновляет state + hash (для shareable URL):
        // - disagreements/all → имеющиеся /critic-v2-disagreements и /critic-v2;
        // - assisted/feedback → /critic-v2 (sub-mode только во frontend state).
        function cv2SetProjSubMode(mode) {
            const allowed = ['disagreements', 'all', 'assisted', 'feedback'];
            if (!allowed.includes(mode)) return;
            cv2ProjSubMode.value = mode;
            // Auto-toggle cv2AssistedFilterOnly: в sub-mode 'assisted' включаем
            // (это main use-case инженеров), при выходе — отключаем.
            // cv2AssistedFilterOnly меняет ROUTING (assignment_tab vs effective_tab),
            // поэтому держать его включённым в disagreements/all/feedback нельзя —
            // там пользователь ожидает effective_tab.
            cv2AssistedFilterOnly.value = (mode === 'assisted');
            if (!currentProjectId.value) return;
            const id = currentProjectId.value;
            if (mode === 'disagreements') {
                cv2ProjDisagreementsMode.value = true;
                cv2Filter.value.alignment = '__disagreement__';
                if (!location.hash.endsWith('/critic-v2-disagreements')) {
                    navigate('/project/' + id + '/critic-v2-disagreements');
                }
            } else {
                // 'all' / 'assisted' / 'feedback' живут под общим hash /critic-v2.
                // Saved cv2ProjDisagreementsMode = false → корректный alignment.
                cv2ProjDisagreementsMode.value = false;
                if (mode === 'all') cv2Filter.value.alignment = '';
                if (!location.hash.endsWith('/critic-v2')) {
                    navigate('/project/' + id + '/critic-v2');
                }
            }
        }

        // Auto-load state: какой feedback-файл подтянут backend'ом для текущего
        // project view + список альтернативных matches (если их несколько).
        const cv2AutoLoadedFeedbackFile = ref('');
        const cv2AutoLoadedFeedbackMeta = ref(null);  // { entries, suggested_reject_count, match_quality }
        const cv2AvailableFeedbackMatches = ref([]);  // [{name, match_quality, entries, suggested_reject_count, scope_project_name}]
        const cv2AutoLoadStatus = ref('');            // '' | 'ok' | 'none' | 'error'
        const cv2AutoLoadMessage = ref('');

        function _cv2ClearProjectFeedback() {
            // Чистим cv2Feedback in-place, чтобы не утечь expert override между
            // проектами при навигации. cv2Feedback — reactive объект, нельзя
            // переприсвоить ссылку.
            for (const k of Object.keys(cv2Feedback)) {
                delete cv2Feedback[k];
            }
        }

        async function _cv2AutoLoadFeedbackForProject(projectId) {
            // Запрашивает /api/critic-v2/feedback-files?project_id=... и тянет
            // лучший match (если он есть). Backend возвращает sorted matches.
            cv2AutoLoadedFeedbackFile.value = '';
            cv2AutoLoadedFeedbackMeta.value = null;
            cv2AvailableFeedbackMatches.value = [];
            cv2AutoLoadStatus.value = '';
            cv2AutoLoadMessage.value = '';
            try {
                const url = '/api/critic-v2/feedback-files?project_id='
                    + encodeURIComponent(projectId);
                const resp = await fetch(url);
                if (!resp.ok) {
                    cv2AutoLoadStatus.value = 'error';
                    cv2AutoLoadMessage.value = 'Auto-load feedback: HTTP ' + resp.status;
                    return;
                }
                const data = await resp.json();
                const matches = Array.isArray(data.matches) ? data.matches : [];
                cv2AvailableFeedbackMatches.value = matches;
                if (matches.length === 0) {
                    cv2AutoLoadStatus.value = 'none';
                    cv2AutoLoadMessage.value =
                        'feedback-файл для этого проекта не найден. Можно импортировать вручную (см. блок «Импорт feedback»).';
                    return;
                }
                // Best match is matches[0]. Fetch its body and apply.
                const best = matches[0];
                const body = await fetch(
                    '/api/critic-v2/feedback-files/' + encodeURIComponent(best.name)
                );
                if (!body.ok) {
                    cv2AutoLoadStatus.value = 'error';
                    cv2AutoLoadMessage.value =
                        'Auto-load: HTTP ' + body.status + ' при чтении ' + best.name;
                    return;
                }
                const payload = await body.json();
                const res = _cv2MergeFeedbackEntries(payload.feedback || []);
                cv2AutoLoadedFeedbackFile.value = best.name;
                cv2AutoLoadedFeedbackMeta.value = {
                    entries: best.entries,
                    suggested_reject_count: best.suggested_reject_count,
                    match_quality: best.match_quality,
                    scope_project_name: best.scope_project_name,
                };
                cv2AutoLoadStatus.value = 'ok';
                cv2AutoLoadMessage.value =
                    'Auto-loaded ' + best.name + ' (' + res.merged + ' entries, '
                    + best.suggested_reject_count + ' preferred_tab=suggested_reject, '
                    + 'match=' + best.match_quality + ')';
            } catch (err) {
                cv2AutoLoadStatus.value = 'error';
                cv2AutoLoadMessage.value = 'Auto-load: ошибка сети: ' + (err && err.message || err);
            }
        }

        async function cv2SwitchFeedbackFile(name) {
            // Manual override: переключить feedback на конкретный файл из
            // dropdown. Сначала чистим, потом подтягиваем выбранный файл.
            if (!name) return;
            _cv2ClearProjectFeedback();
            cv2AutoLoadedFeedbackFile.value = '';
            cv2AutoLoadedFeedbackMeta.value = null;
            try {
                const body = await fetch(
                    '/api/critic-v2/feedback-files/' + encodeURIComponent(name)
                );
                if (!body.ok) {
                    cv2AutoLoadStatus.value = 'error';
                    cv2AutoLoadMessage.value = 'Switch: HTTP ' + body.status;
                    return;
                }
                const payload = await body.json();
                const res = _cv2MergeFeedbackEntries(payload.feedback || []);
                // Подсветим выбранный файл в metadata из cv2AvailableFeedbackMatches.
                const meta = cv2AvailableFeedbackMatches.value.find(m => m.name === name);
                cv2AutoLoadedFeedbackFile.value = name;
                cv2AutoLoadedFeedbackMeta.value = meta
                    ? {
                        entries: meta.entries,
                        suggested_reject_count: meta.suggested_reject_count,
                        match_quality: meta.match_quality,
                        scope_project_name: meta.scope_project_name,
                    }
                    : { entries: res.merged };
                cv2AutoLoadStatus.value = 'ok';
                cv2AutoLoadMessage.value =
                    'Загружен ' + name + ' (' + res.merged + ' entries)';
            } catch (err) {
                cv2AutoLoadStatus.value = 'error';
                cv2AutoLoadMessage.value = 'Switch: ' + (err && err.message || err);
            }
        }

        async function cv2LoadProject(projectId, opts) {
            // Read-only fetch. No LLM. No writes. No production pipeline mutation.
            const o = opts || {};
            const disagreementsMode = Boolean(o.disagreementsMode);
            cv2ProjLoading.value = true;
            cv2ProjLoadError.value = '';
            cv2ProjHint.value = '';
            cv2Export.value = null;
            cv2ProjDisagreementsMode.value = disagreementsMode;
            // sub-mode по умолчанию следует hash-route (для backward compat):
            // /critic-v2-disagreements → 'disagreements', /critic-v2 → 'all'.
            // Дальше пользователь может переключить на 'assisted'/'feedback'
            // через cv2SetProjSubMode.
            cv2ProjSubMode.value = disagreementsMode ? 'disagreements' : 'all';
            // Чистим feedback от прошлого проекта, чтобы preferred_tab не утёк
            // в чужой view (например, при навигации между проектами).
            _cv2ClearProjectFeedback();
            // Reset filters so two views don't bleed into each other, then
            // pre-apply the disagreement filter if we're in that mode.
            cv2ResetFilters();
            if (disagreementsMode) {
                cv2Filter.value.alignment = '__disagreement__';
            }
            try {
                const resp = await fetch(
                    '/api/critic-v2/projects/' + encodeURIComponent(projectId) + '/triage-ui'
                );
                if (!resp.ok) {
                    let detail = null;
                    try { detail = await resp.json(); } catch (_) {}
                    if (resp.status === 404 && detail && detail.detail) {
                        cv2ProjLoadError.value = detail.detail.message || 'Critic v2 artifact не найден.';
                        cv2ProjHint.value = detail.detail.hint_command || '';
                    } else {
                        cv2ProjLoadError.value = 'Ошибка загрузки: HTTP ' + resp.status;
                    }
                    return;
                }
                const raw = await resp.json();
                cv2Export.value = cv2ParseExport(raw);
                const def = cv2Export.value.tabs.find(t => t.default_open);
                cv2ActiveTab.value = def ? def.key : cv2Export.value.tabs[0].key;
                if (raw.warning) {
                    // показываем warning через сам export, но logger в консоль для трассировки
                    console.warn('[cv2] project warning:', raw.warning);
                }
                // Auto-load feedback: после успешной загрузки artifact ищем
                // подходящий *_feedback.json на backend и применяем его. Это
                // главное отличие от offline-view (которая ждёт file upload).
                await _cv2AutoLoadFeedbackForProject(projectId);
                // Auto-load assisted_round1 review-package для проекта. Это
                // независимо от feedback: review-package описывает, ЧТО надо
                // проверить, а feedback — РЕЗУЛЬТАТ ручной корректировки.
                await _cv2AutoLoadAssistedRound1ForProject(projectId);
            } catch (err) {
                cv2ProjLoadError.value = 'Ошибка сети: ' + (err && err.message || err);
            } finally {
                cv2ProjLoading.value = false;
            }
        }

        // ─── assisted_round1 review-package (read-only) ─────────────────────
        // Список карточек, которые инженер должен проверить вручную: 22
        // обязательных (risky_accepted_22) + 60 выборочных (sample_60). Источник
        // — CSV-файлы в critic v2 test/assisted_round1_review/. Frontend не
        // парсит их — только хранит то, что backend отдал по project_id.

        const cv2AssistedItems = ref([]);           // matched items для current project
        const cv2AssistedAllTotal = ref(0);         // 82 (22 + 60) на всех проектах
        const cv2AssistedMatchedTotal = ref(0);
        const cv2AssistedLoading = ref(false);
        const cv2AssistedError = ref('');
        // Filter toggle: только assisted_round1 карточки во вкладках.
        const cv2AssistedFilterOnly = ref(false);

        async function _cv2AutoLoadAssistedRound1ForProject(projectId) {
            cv2AssistedItems.value = [];
            cv2AssistedAllTotal.value = 0;
            cv2AssistedMatchedTotal.value = 0;
            cv2AssistedError.value = '';
            cv2AssistedFilterOnly.value = false;
            cv2AssistedLoading.value = true;
            try {
                const url = '/api/critic-v2/assisted-round1/items?project_id='
                    + encodeURIComponent(projectId);
                const resp = await fetch(url);
                if (!resp.ok) {
                    cv2AssistedError.value = 'assisted_round1: HTTP ' + resp.status;
                    return;
                }
                const data = await resp.json();
                cv2AssistedItems.value = Array.isArray(data.items) ? data.items : [];
                cv2AssistedAllTotal.value = data.all_items_total || 0;
                cv2AssistedMatchedTotal.value = data.matched_count || 0;
            } catch (err) {
                cv2AssistedError.value = 'assisted_round1: ' + (err && err.message || err);
            } finally {
                cv2AssistedLoading.value = false;
            }
        }

        // Карта finding_id → assisted item, для быстрого lookup'а в computed'ах.
        const cv2AssistedById = computed(() => {
            const out = {};
            for (const it of cv2AssistedItems.value) {
                if (it.finding_id) out[it.finding_id] = it;
            }
            return out;
        });

        // Русские ярлыки для статусов assisted_round1.
        // Используются и в per-item таблице, и в expert-correction badge на
        // карточке в assisted-mode.
        const CV2_ASSISTED_STATUS_LABEL = {
            still_candidate: 'ещё в к отклонению',
            expert_returned_primary: 'эксперт вернул в основную',
            expert_returned_context: 'эксперт отправил в контекст',
            expert_hidden: 'эксперт скрыл',
            missing: 'не найдено в artifact',
        };
        const CV2_TAB_LABEL_RU = {
            primary: 'Основная проверка',
            needs_context: 'Требует смежников',
            suggested_reject: 'Критик рекомендует отклонить',
            hidden_by_critic: 'Скрыто критиком',
        };

        // Определяем статус assisted item по семантике задания (assignment-based):
        // - 'still_candidate'          : effective_tab всё ещё = suggested_reject
        // - 'expert_returned_primary'  : expert вернул в primary
        // - 'expert_returned_context'  : expert отправил в needs_context
        // - 'expert_hidden'            : expert ушёл ещё дальше → hidden_by_critic
        // - 'missing'                  : finding_id не найден в artifact
        //
        // Важно: статус НЕ убирает карточку из задания — он только сообщает,
        // что с ней уже сделал эксперт. Инженер всё равно должен её увидеть.
        function cv2AssistedStatusOf(assistedItem) {
            if (!assistedItem || !cv2Export.value) return 'missing';
            const fid = assistedItem.finding_id;
            const found = cv2Export.value.items.find(i => i.finding_id === fid);
            if (!found) return 'missing';
            const eff = cv2EffectiveTab(found);
            const expected = assistedItem.expected_queue || 'suggested_reject';
            if (eff === expected) return 'still_candidate';
            if (eff === 'primary') return 'expert_returned_primary';
            if (eff === 'needs_context') return 'expert_returned_context';
            if (eff === 'hidden_by_critic') return 'expert_hidden';
            return 'still_candidate';  // fallback на безопасный статус
        }

        // Полная сводка для блока «Проверочные карточки» + debug.
        // Считается всегда от cv2AssistedItems (matched под текущий проект),
        // независимо от того, открыт ли filter-only.
        const cv2AssistedReport = computed(() => {
            const items = cv2AssistedItems.value;
            const report = {
                items_total_all_projects: cv2AssistedAllTotal.value,
                items_for_project: items.length,
                by_group: { risky_accepted_22: 0, sample_60: 0 },
                by_reason_group: {},
                found_in_artifact: 0,
                missing_in_artifact: 0,
                in_suggested_reject: 0,
                not_in_suggested_reject: 0,
                in_other_tab: { primary: 0, needs_context: 0, hidden_by_critic: 0 },
                per_item: [],
            };
            if (!cv2Export.value) {
                // Artifact ещё не загружен — статусы посчитать нельзя.
                for (const it of items) {
                    report.by_group[it.group] = (report.by_group[it.group] || 0) + 1;
                    const rg = it.reason_group || '—';
                    report.by_reason_group[rg] = (report.by_reason_group[rg] || 0) + 1;
                }
                return report;
            }
            const byArtifactId = {};
            for (const it of cv2Export.value.items) byArtifactId[it.finding_id] = it;
            for (const a of items) {
                const status = cv2AssistedStatusOf(a);
                const artifactItem = byArtifactId[a.finding_id] || null;
                const effective = artifactItem ? cv2EffectiveTab(artifactItem) : null;
                const fb = cv2Feedback[a.finding_id] || null;
                report.by_group[a.group] = (report.by_group[a.group] || 0) + 1;
                const rg = a.reason_group || '—';
                report.by_reason_group[rg] = (report.by_reason_group[rg] || 0) + 1;
                if (status === 'missing') {
                    report.missing_in_artifact += 1;
                } else {
                    report.found_in_artifact += 1;
                    if (effective === 'suggested_reject') {
                        report.in_suggested_reject += 1;
                    } else {
                        report.not_in_suggested_reject += 1;
                        if (effective in report.in_other_tab) {
                            report.in_other_tab[effective] += 1;
                        }
                    }
                }
                // expert_correction_label — что показать в badge на карточке
                // в assisted-mode. Null если correction нет (effective_tab
                // совпадает с expected_queue).
                let correctionLabel = null;
                if (status !== 'still_candidate' && status !== 'missing') {
                    correctionLabel = 'Эксперт ранее перенёс в: '
                        + (CV2_TAB_LABEL_RU[effective] || effective);
                }
                report.per_item.push({
                    finding_id: a.finding_id,
                    source_file: a.source_file,
                    group: a.group,
                    reason: a.reason,
                    reason_group: a.reason_group,
                    title: a.title,
                    assignment_tab: a.expected_queue || 'suggested_reject',
                    expected_queue: a.expected_queue,
                    critic_tab: artifactItem ? (artifactItem.tab || '') : null,
                    expert_preferred_tab: fb ? (fb.preferred_tab || '') : '',
                    effective_tab: effective,
                    status: status,
                    status_label: CV2_ASSISTED_STATUS_LABEL[status] || status,
                    expert_correction_label: correctionLabel,
                    reviewer_instruction: a.reviewer_instruction,
                });
            }
            return report;
        });

        // Per-finding-id lookup для DOM badge'а. cv2AssistedReport.per_item уже
        // содержит всю информацию, но v-for'у внутри cv2-item нужен быстрый
        // доступ. Возвращает { status, status_label, expert_correction_label,
        // assignment_tab } или null если карточка не в review-package.
        const cv2AssistedStatusByFid = computed(() => {
            const out = {};
            for (const row of cv2AssistedReport.value.per_item) {
                out[row.finding_id] = {
                    status: row.status,
                    status_label: row.status_label,
                    expert_correction_label: row.expert_correction_label,
                    assignment_tab: row.assignment_tab,
                    effective_tab: row.effective_tab,
                };
            }
            return out;
        });

        // Открыть карточку в текущем view: переключить на нужную вкладку
        // и проскроллить к данной строке. В assisted-mode используем
        // assignment_tab (где карточка фактически отрисована в этом режиме),
        // в обычном — effective_tab.
        function cv2AssistedFocusFinding(findingId) {
            if (!cv2Export.value) return;
            const item = cv2Export.value.items.find(i => i.finding_id === findingId);
            if (!item) return;
            const target = cv2RoutingTab(item) || cv2EffectiveTab(item);
            if (target && CV2_TABS.includes(target)) {
                cv2ActiveTab.value = target;
            }
            // Дать Vue отрисовать tab, потом проскроллить.
            setTimeout(() => {
                const el = document.getElementById('cv2-item-' + findingId);
                if (el && el.scrollIntoView) {
                    el.scrollIntoView({ behavior: 'smooth', block: 'center' });
                    el.classList.add('cv2-item--flash');
                    setTimeout(() => el.classList.remove('cv2-item--flash'), 1500);
                }
            }, 50);
        }

        function cv2OnFileSelected(event) {
            cv2LoadError.value = '';
            const file = event.target.files && event.target.files[0];
            if (!file) return;
            const reader = new FileReader();
            reader.onload = (e) => {
                try {
                    const raw = JSON.parse(e.target.result);
                    cv2Export.value = cv2ParseExport(raw);
                    // Open default tab (primary).
                    const def = cv2Export.value.tabs.find(t => t.default_open);
                    cv2ActiveTab.value = def ? def.key : cv2Export.value.tabs[0].key;
                } catch (err) {
                    cv2LoadError.value = 'Ошибка парсинга: ' + (err.message || err);
                    cv2Export.value = null;
                }
            };
            reader.onerror = () => {
                cv2LoadError.value = 'Не удалось прочитать файл.';
            };
            reader.readAsText(file);
        }

        function cv2ScoreBucket(score) {
            if (score === null || score === undefined) return 'none';
            if (score >= 10) return '10-11';
            if (score >= 8) return '8-9';
            if (score >= 6) return '6-7';
            if (score >= 4) return '4-5';
            return '0-3';
        }

        function cv2ItemMatchesFilter(it) {
            const f = cv2Filter.value;
            if (f.section && it.section !== f.section) return false;
            if (f.queue && it.queue !== f.queue) return false;
            if (f.reason && it.reason !== f.reason) return false;
            if (f.evidence && it.evidence_quality !== f.evidence) return false;
            if (f.scoreBucket && cv2ScoreBucket(it.score) !== f.scoreBucket) return false;
            if (f.human) {
                if (f.human === '__none__') {
                    if (it.human_decision) return false;
                } else if (it.human_decision !== f.human) {
                    return false;
                }
            }
            if (f.alignment) {
                const al = cv2AlignmentOf(it);
                if (f.alignment === '__disagreement__') {
                    if (!cv2IsDisagreement(al)) return false;
                } else if (f.alignment === '__none__alignment') {
                    if (al !== 'unknown') return false;
                } else if (al !== f.alignment) {
                    return false;
                }
            }
            // Assisted-round1 filter: показывать только items, finding_id которых
            // присутствует в review-package по текущему проекту. Этот фильтр НЕ
            // подменяет cv2EffectiveTab — он лишь сужает видимый набор. Карточка
            // остаётся в той вкладке, где её располагает effective_tab, поэтому
            // если карточка в primary вместо suggested_reject — инженер увидит её
            // в primary с включённым assisted-filter'ом.
            if (cv2AssistedFilterOnly.value) {
                if (!cv2AssistedById.value[it.finding_id]) return false;
            }
            return true;
        }

        const cv2HasHumanDecisions = computed(() => {
            if (!cv2Export.value) return false;
            return cv2Export.value.items.some(i => i.human_decision);
        });

        // Aggregated counts for the "Сверка с экспертом" panel.
        // Counts are computed from raw items (not filtered) so the summary stays
        // stable while the user changes the filter dropdown.
        const cv2AlignmentSummary = computed(() => {
            const out = {
                with_decision: 0,
                aligned: 0,
                disagreements: 0,
                aligned_visible: 0,
                aligned_hidden: 0,
                accepted_collapsed: 0,
                accepted_needs_context: 0,
                rejected_visible: 0,
                rejected_needs_context: 0,
                hidden_human_accepted: 0,
                suggested_reject_human_accepted: 0,
                without_decision: 0,
            };
            if (!cv2Export.value) return out;
            for (const it of cv2Export.value.items) {
                const hd = it.human_decision;
                const tab = it.tab;
                const al = cv2AlignmentOf(it);
                if (al === 'unknown') {
                    out.without_decision += 1;
                    continue;
                }
                out.with_decision += 1;
                if (al === 'aligned_visible') {
                    out.aligned += 1;
                    out.aligned_visible += 1;
                } else if (al === 'aligned_hidden') {
                    out.aligned += 1;
                    out.aligned_hidden += 1;
                } else if (al === 'accepted_collapsed') {
                    out.disagreements += 1;
                    out.accepted_collapsed += 1;
                } else if (al === 'accepted_needs_context') {
                    out.disagreements += 1;
                    out.accepted_needs_context += 1;
                } else if (al === 'rejected_visible') {
                    out.disagreements += 1;
                    out.rejected_visible += 1;
                } else if (al === 'rejected_needs_context') {
                    out.disagreements += 1;
                    out.rejected_needs_context += 1;
                }
                // High-impact specific buckets used in dashboards.
                if (hd === 'accepted' && tab === 'hidden_by_critic') {
                    out.hidden_human_accepted += 1;
                }
                if (hd === 'accepted' && tab === 'suggested_reject') {
                    out.suggested_reject_human_accepted += 1;
                }
            }
            return out;
        });

        const cv2FilterOptions = computed(() => {
            const empty = { sections: [], queues: [], reasons: [], evidences: [] };
            if (!cv2Export.value) return empty;
            const sec = new Set(), q = new Set(), r = new Set(), e = new Set();
            for (const it of cv2Export.value.items) {
                if (it.section) sec.add(it.section);
                if (it.queue) q.add(it.queue);
                if (it.reason) r.add(it.reason);
                if (it.evidence_quality) e.add(it.evidence_quality);
            }
            return {
                sections: [...sec].sort(),
                queues: [...q].sort(),
                reasons: [...r].sort(),
                evidences: [...e].sort(),
            };
        });

        // Effective tab for an item = expert override if set, else critic's tab.
        // Expert override comes from cv2Feedback[id].preferred_tab (set via
        // quick-route buttons or imported from *_feedback.json files).
        // This is what makes findings the expert moved to "suggested_reject"
        // actually appear in that queue instead of staying under critic's tab.
        function cv2EffectiveTab(item) {
            if (!item) return '';
            const fid = item.finding_id;
            const fb = fid ? cv2Feedback[fid] : null;
            const pref = fb && fb.preferred_tab;
            if (pref && CV2_TABS.includes(pref)) return pref;
            return item.tab || '';
        }

        // Assignment_tab — куда Critic v2 ИЗНАЧАЛЬНО назначил карточку.
        // Источник: assisted_round1 expected_queue (== suggested_reject для всех
        // current cards). Возвращает null если карточка не в review-package.
        // В assisted-mode маршрутизация идёт по assignment_tab, чтобы инженеры
        // видели ВСЕ кандидаты «к отклонению» — даже те, что эксперт ранее
        // вернул в primary через preferred_tab.
        function cv2AssignmentTab(item) {
            if (!item) return null;
            const a = cv2AssistedById.value[item.finding_id];
            if (!a) return null;
            const q = a.expected_queue;
            return (q && CV2_TABS.includes(q)) ? q : null;
        }

        // Routing tab: в assisted-mode для items из review-package используем
        // assignment_tab. Для не-review items и в обычном режиме — effective_tab.
        // assisted-mode = cv2AssistedFilterOnly=true. Это контракт: toggle на
        // панели становится семантическим переключателем view'а, а не просто
        // фильтром выборки.
        function cv2RoutingTab(item) {
            if (cv2AssistedFilterOnly.value) {
                const assignmentTab = cv2AssignmentTab(item);
                if (assignmentTab) return assignmentTab;
                // не-review карточка не маршрутизируется ни в одну вкладку
                // в assisted-mode (filter уже отсёк её через cv2ItemMatchesFilter).
                return '';
            }
            return cv2EffectiveTab(item);
        }

        const cv2ItemsByTab = computed(() => {
            const out = { primary: [], needs_context: [], suggested_reject: [], hidden_by_critic: [] };
            if (!cv2Export.value) return out;
            for (const it of cv2Export.value.items) {
                if (!cv2ItemMatchesFilter(it)) continue;
                const t = cv2RoutingTab(it);
                if (out[t]) out[t].push(it);
            }
            return out;
        });

        const cv2VisibleCountByTab = computed(() => {
            const m = cv2ItemsByTab.value;
            return {
                primary: m.primary.length,
                needs_context: m.needs_context.length,
                suggested_reject: m.suggested_reject.length,
                hidden_by_critic: m.hidden_by_critic.length,
            };
        });

        // Diagnostic counts: raw (critic's tab only) vs effective (after expert
        // overrides). Helpful when "badge says 1, MD has 12" surprises.
        const cv2DebugCounts = computed(() => {
            const out = {
                raw_total: 0,
                normalized_total: 0,
                by_critic_tab: { primary: 0, needs_context: 0, suggested_reject: 0, hidden_by_critic: 0 },
                by_effective_tab: { primary: 0, needs_context: 0, suggested_reject: 0, hidden_by_critic: 0 },
                by_expert_preferred: { primary: 0, needs_context: 0, suggested_reject: 0, hidden_by_critic: 0 },
                expert_overrides_total: 0,
                unmatched_critic_tab: 0,
                feedback_entries_loaded: Object.keys(cv2Feedback).length,
            };
            if (!cv2Export.value) return out;
            for (const it of cv2Export.value.items) {
                out.raw_total += 1;
                const ct = it.tab || '';
                if (ct in out.by_critic_tab) out.by_critic_tab[ct] += 1;
                else if (ct) out.unmatched_critic_tab += 1;

                const et = cv2EffectiveTab(it);
                if (et in out.by_effective_tab) {
                    out.by_effective_tab[et] += 1;
                    out.normalized_total += 1;
                }

                const fb = cv2Feedback[it.finding_id];
                const pref = fb && fb.preferred_tab;
                if (pref && pref in out.by_expert_preferred) {
                    out.by_expert_preferred[pref] += 1;
                    if (pref !== ct) out.expert_overrides_total += 1;
                }
            }
            return out;
        });

        // ─── Critic v2 UI: Feedback (frontend-only, never hits backend) ────
        // Reviewer marks per-finding triage quality. Stored in browser state
        // and exported as a JSON file. No DB write, no API call.
        const CV2_TABS = ['primary', 'needs_context',
                          'suggested_reject', 'hidden_by_critic'];
        const CV2_PRIORITIES = ['normal', 'important', 'critical'];
        const CV2_TRIAGE_VALUES = ['yes', 'no', 'unsure'];

        // Map: finding_id -> {triage_correct, preferred_tab, reviewer_note, priority}
        const cv2Feedback = reactive({});

        function cv2EnsureFeedback(findingId) {
            if (!cv2Feedback[findingId]) {
                cv2Feedback[findingId] = {
                    triage_correct: '',
                    preferred_tab: '',
                    reviewer_note: '',
                    priority: 'normal',
                };
            }
            return cv2Feedback[findingId];
        }

        function cv2SetTriageCorrect(findingId, value) {
            if (!CV2_TRIAGE_VALUES.includes(value)) return;
            cv2EnsureFeedback(findingId).triage_correct = value;
        }

        function cv2SetPreferredTab(findingId, tab) {
            if (!CV2_TABS.includes(tab)) return;
            const fb = cv2EnsureFeedback(findingId);
            fb.preferred_tab = tab;
            // If reviewer chose a different tab, mark triage as wrong by default.
            // Reviewer can still flip back to yes/unsure manually.
            const item = cv2Export.value
                ? cv2Export.value.items.find(i => i.finding_id === findingId)
                : null;
            if (item && item.tab !== tab && !fb.triage_correct) {
                fb.triage_correct = 'no';
            }
        }

        function cv2SetPriority(findingId, value) {
            if (!CV2_PRIORITIES.includes(value)) return;
            cv2EnsureFeedback(findingId).priority = value;
        }

        function cv2SetReviewerNote(findingId, text) {
            cv2EnsureFeedback(findingId).reviewer_note = text || '';
        }

        function cv2QuickRoute(findingId, tab) {
            // Quick-button shortcut: jump straight to a preferred_tab.
            cv2SetPreferredTab(findingId, tab);
        }

        function cv2QuickUnsure(findingId) {
            const fb = cv2EnsureFeedback(findingId);
            fb.triage_correct = 'unsure';
        }

        function cv2HasFeedback(findingId) {
            const fb = cv2Feedback[findingId];
            if (!fb) return false;
            return Boolean(
                fb.triage_correct || fb.preferred_tab
                || (fb.reviewer_note && fb.reviewer_note.trim())
                || (fb.priority && fb.priority !== 'normal')
            );
        }

        const cv2FeedbackSummary = computed(() => {
            const ids = Object.keys(cv2Feedback);
            let evaluated = 0, yes = 0, no = 0, unsure = 0;
            for (const id of ids) {
                const fb = cv2Feedback[id];
                if (!fb) continue;
                if (cv2HasFeedback(id)) evaluated += 1;
                if (fb.triage_correct === 'yes') yes += 1;
                else if (fb.triage_correct === 'no') no += 1;
                else if (fb.triage_correct === 'unsure') unsure += 1;
            }
            return { evaluated, yes, no, unsure };
        });

        function cv2BuildFeedbackExport() {
            // Pure function: builds the export payload from current state.
            // Does NOT touch any network / backend / disk.
            if (!cv2Export.value) return null;
            const itemsById = {};
            for (const it of cv2Export.value.items) {
                itemsById[it.finding_id] = it;
            }
            const sourceSummary = cv2Export.value.summary || {};
            const feedback = [];
            for (const fid of Object.keys(cv2Feedback)) {
                if (!cv2HasFeedback(fid)) continue;
                const fb = cv2Feedback[fid];
                const item = itemsById[fid] || {};
                feedback.push({
                    finding_id: fid,
                    project_name: item.project_name || '',
                    section: item.section || '',
                    original_tab: item.tab || '',
                    original_queue: item.queue || '',
                    triage_correct: fb.triage_correct || '',
                    preferred_tab: fb.preferred_tab || '',
                    priority: fb.priority || 'normal',
                    reviewer_note: (fb.reviewer_note || '').trim(),
                });
            }
            const scope = cv2Export.value.scope || null;
            // When the project view was opened via the "Расхождения" route, we
            // mark the export with mode=project_disagreements and capture the
            // active alignment filter so downstream tooling can tell that the
            // reviewer was looking specifically at disagreements.
            let scopeOut;
            if (scope) {
                const inDisagree = cv2ProjDisagreementsMode.value === true;
                scopeOut = {
                    mode: inDisagree ? 'project_disagreements' : (scope.mode || 'project'),
                    project_id: scope.project_id || null,
                    project_name: scope.project_name || null,
                    matched_by: scope.matched_by || null,
                };
                if (inDisagree) {
                    scopeOut.alignment_filter = '__disagreement__';
                }
            } else {
                scopeOut = { mode: 'global' };
            }
            return {
                export_type: 'critic_v2_triage_feedback',
                created_at: new Date().toISOString(),
                scope: scopeOut,
                source_file_summary: {
                    total: sourceSummary.total ?? null,
                    profile: sourceSummary.profile ?? null,
                    primary_queue_reduction_percent:
                        sourceSummary.primary_queue_reduction_percent ?? null,
                },
                feedback,
            };
        }

        function cv2ExportFeedback() {
            // User-triggered. Builds payload and triggers a browser download.
            // No backend call. Frontend-only.
            const payload = cv2BuildFeedbackExport();
            if (!payload) return;
            const blob = new Blob(
                [JSON.stringify(payload, null, 2)],
                { type: 'application/json' }
            );
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            const stamp = new Date().toISOString().replace(/[:.]/g, '-');
            a.download = `critic_v2_triage_feedback_${stamp}.json`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
        }

        // ─── Critic v2 UI: Feedback Import ────────────────────────────────
        // Reviewer feedback (preferred_tab, triage_correct, reviewer_note,
        // priority) lives in browser-state cv2Feedback. After reload it's
        // gone — so a finding the expert moved to "suggested_reject" stops
        // appearing there. Import re-hydrates state from a previously
        // downloaded *_feedback.json file or from the backend listing.

        const cv2ImportStatus = ref('');  // 'ok' | 'error' | ''
        const cv2ImportMessage = ref('');
        const cv2AvailableFeedbackFiles = ref([]);  // [{name, size, mtime, project_name?}]

        function _cv2MergeFeedbackEntries(entries) {
            let merged = 0;
            let skipped = 0;
            if (!Array.isArray(entries)) return { merged, skipped };
            for (const entry of entries) {
                const fid = entry && entry.finding_id;
                if (!fid) { skipped += 1; continue; }
                const fb = cv2EnsureFeedback(fid);
                if (entry.triage_correct) fb.triage_correct = entry.triage_correct;
                if (entry.preferred_tab) fb.preferred_tab = entry.preferred_tab;
                if (entry.priority) fb.priority = entry.priority;
                if (typeof entry.reviewer_note === 'string') {
                    fb.reviewer_note = entry.reviewer_note;
                }
                merged += 1;
            }
            return { merged, skipped };
        }

        function cv2ImportFeedbackFromObject(obj) {
            // Accepts a parsed JSON object (output of cv2ExportFeedback or
            // a *_feedback.json with the same shape). Merges in-place into
            // cv2Feedback. Does NOT clear existing feedback.
            cv2ImportStatus.value = '';
            cv2ImportMessage.value = '';
            if (!obj || typeof obj !== 'object') {
                cv2ImportStatus.value = 'error';
                cv2ImportMessage.value = 'Импорт: ожидается JSON-объект.';
                return { merged: 0, skipped: 0 };
            }
            const entries = obj.feedback;
            if (!Array.isArray(entries)) {
                cv2ImportStatus.value = 'error';
                cv2ImportMessage.value = 'Импорт: в JSON нет массива "feedback".';
                return { merged: 0, skipped: 0 };
            }
            const res = _cv2MergeFeedbackEntries(entries);
            cv2ImportStatus.value = 'ok';
            cv2ImportMessage.value =
                `Импортировано: ${res.merged} (пропущено без finding_id: ${res.skipped}).`;
            return res;
        }

        function cv2OnFeedbackFileSelected(event) {
            const file = event.target.files && event.target.files[0];
            if (!file) return;
            const reader = new FileReader();
            reader.onload = (e) => {
                try {
                    const obj = JSON.parse(e.target.result);
                    cv2ImportFeedbackFromObject(obj);
                } catch (err) {
                    cv2ImportStatus.value = 'error';
                    cv2ImportMessage.value = 'Импорт: ошибка парсинга JSON: ' + (err.message || err);
                }
            };
            reader.onerror = () => {
                cv2ImportStatus.value = 'error';
                cv2ImportMessage.value = 'Импорт: не удалось прочитать файл.';
            };
            reader.readAsText(file);
            event.target.value = '';  // allow re-selecting the same file
        }

        async function cv2RefreshFeedbackFiles() {
            // Read-only listing of *_feedback.json from the backend's
            // CRITIC_V2_FEEDBACK_DIR (default: "<repo>/critic v2 test/").
            try {
                const resp = await fetch('/api/critic-v2/feedback-files');
                if (!resp.ok) {
                    cv2AvailableFeedbackFiles.value = [];
                    return;
                }
                const data = await resp.json();
                cv2AvailableFeedbackFiles.value = Array.isArray(data.files) ? data.files : [];
            } catch (_) {
                cv2AvailableFeedbackFiles.value = [];
            }
        }

        async function cv2ImportFeedbackFromServer(name) {
            cv2ImportStatus.value = '';
            cv2ImportMessage.value = '';
            if (!name) return;
            try {
                const resp = await fetch(
                    '/api/critic-v2/feedback-files/' + encodeURIComponent(name)
                );
                if (!resp.ok) {
                    cv2ImportStatus.value = 'error';
                    cv2ImportMessage.value = 'Импорт: HTTP ' + resp.status;
                    return;
                }
                const obj = await resp.json();
                cv2ImportFeedbackFromObject(obj);
            } catch (err) {
                cv2ImportStatus.value = 'error';
                cv2ImportMessage.value = 'Импорт: ошибка сети: ' + (err && err.message || err);
            }
        }

        // Tiles

        // Page analysis (page_summaries)

        // Blocks (OCR)
        const blocksProjectId = ref('');
        const blockPages = ref([]);
        const blockCropErrors = ref(0);
        const blockTotalExpected = ref(0);
        const selectedBlockPage = ref(null);
        const selectedBlock = ref(null);
        const blockAnalysis = ref({});
        const blockImageContainer = ref(null);
        const blockZoom = ref(1);       // 1 = fit-to-container
        const blockPanX = ref(0);
        const blockPanY = ref(0);
        const blockPanning = ref(false);
        const blockPanStartX = ref(0);
        const blockPanStartY = ref(0);
        const blockNatW = ref(0);       // natural width of loaded image
        const blockNatH = ref(0);       // natural height of loaded image
        const blockBaseScale = ref(1);  // scale to fit image into container
        const textlayerHighlightsShadow = ref(null);      // observe-only артефакт, не 03_findings
        const showTextlayerHighlightsShadow = ref(false); // диагностический overlay

        // «txt»-режим: текст блока, реально уходящий в нейронку (Stage 01)
        const showBlockLlmText = ref(false);
        const blockLlmText = ref(null);
        const blockLlmTextLoading = ref(false);
        const blockLlmTextError = ref('');

        // Optimization
        const optimizationData = ref(null);
        const optimizationLoading = ref(false);
        const optimizationFilter = ref('');  // '' | 'cheaper_analog' | 'faster_install' | 'simpler_design' | 'lifecycle'
        const optimizationSearch = ref('');

        // Discussions (чат по замечаниям/оптимизациям)
        const discussionItems = ref([]);
        const discussionTab = ref('finding');  // 'finding' | 'optimization'
        const discussionModel = ref('');
        const discussionModels = ref([]);
        const activeDiscussion = ref(null);    // item_id открытого чата или null
        const activeDiscussionItem = ref(null); // полные данные текущего замечания/оптимизации (из findings API)
        const activeDiscussionBlocks = ref([]); // блоки привязанные к замечанию
        const showDiscussionBlocks = ref(false);
        const discussionMessages = ref([]);
        const discussionLoading = ref(false);
        const discussionSending = ref(false);
        const chatAttachedImage = ref(null); // base64 data URL
        const discussionCost = ref(0);
        const discussionContextTokens = ref(null); // {total_tokens, context_tokens, image_tokens, ...}
        const resolvedFindingsLoading = ref(false);
        const chatInput = ref('');
        const chatMessagesContainer = ref(null);
        // Редактирование сообщения
        const editingMessageIdx = ref(null);   // индекс редактируемого user-сообщения
        const editingMessageText = ref('');
        // Revision (кнопка "Изменить")
        const revisionData = ref(null);        // {original, revised, explanation}
        const revisionLoading = ref(false);
        // Скачать пакет аудита
        const auditPackageLoading = ref(false);
        const batchPackageLoading = ref(false);
        // Batch-кроп блоков (для проектов без аудита)
        const batchCropLoading = ref(false);
        const batchCropProgress = ref('');

        // Expert Review (экспертная оценка)
        const expertReviewMode = ref(false);
        const expertDecisions = ref({});  // { item_id: { decision: 'accepted'|'rejected'|null, rejection_reason: '' } }
        const expertReviewSaving = ref(false);

        // Knowledge Base (база знаний)
        const kbTab = ref('rejected');  // 'rejected' | 'accepted' | 'customer_confirmed' | 'missing_norms'
        const kbEntries = ref([]);
        const kbStats = ref({ rejected: 0, accepted: 0, customer_confirmed: 0, fixed_by_customer: 0, total: 0 });
        const kbLoading = ref(false);
        const kbSearch = ref('');
        const kbSectionFilter = ref('');
        const kbItemType = ref('finding');   // 'finding' | 'optimization' — фильтр колонки «Тип» (по умолчанию замечания)
        const kbTypeMenuOpen = ref(false);   // открыт ли дропдаун выбора типа в шапке таблицы
        const kbObjectFilter = ref('');   // id выбранного объекта (БЗ только по нему)
        const missingNorms = ref([]);
        const missingNormsStats = ref({ pending: 0, added: 0, dismissed: 0, total: 0 });
        const missingNormsFilter = ref('pending'); // 'pending' | 'added' | 'dismissed' | ''
        const kbUploadLoading = ref(false);

        // Document viewer (MD)
        const documentProjectId = ref('');
        const documentPages = ref([]);
        const documentCurrentPage = ref(null);
        const documentPageData = ref(null);
        const documentLoading = ref(false);

        // Log — отдельное хранилище для каждого проекта
        const logProjectId = ref('');
        // Каждая запись: либо log-строка {kind:'log', time, level, message},
        // либо finding-карточка {kind:'finding', time, finding_id, severity, category, problem, sheet, page, status, rejectReason}
        const projectLogs = ref({});
        const logAutoScroll = ref(true);
        const logContainer = ref(null);
        const logLoading = ref(false);
        // Файл лога длиннее окна выборки (limit в loadProjectLog) — текст
        // баннера «начало лога усечено» над секциями; '' = баннера нет.
        const logTruncatedNotice = ref('');

        // Текущая фаза «размышления модели»: merge | critic | corrector | done | ''
        const findingStage = ref({});     // {projectId: 'merge'|...}
        // Быстрый индекс finding_id → entry в projectLogs[pid] для обновления статуса
        const findingIndex = ref({});     // {projectId: {finding_id: entry}}

        // logEntries — computed, показывает логи текущего проекта
        const logEntries = computed(() => {
            const pid = logProjectId.value;
            return pid ? (projectLogs.value[pid] || []) : [];
        });

        // ─── Секции лога по этапам конвейера ───
        // Каждая запись лога несёт stage (job.stage бэкенда); секции идут в
        // каноническом порядке конвейера (блоки → текст → свод → параллельная
        // группа → долги → перенос → Excel). Неизвестные stage — в «Прочее».
        const LOG_STAGE_SECTIONS = [
            { key: 'prepare',            title: 'Подготовка',                stages: ['prepare'] },
            { key: 'crop_blocks',        title: 'Кроп блоков',               stages: ['crop_blocks'] },
            { key: 'block_context',      title: 'Обогащение блоков (Gemma)', stages: ['block_context', 'gemma_enrichment'] },
            { key: 'block_analysis',     title: 'Анализ блоков',             stages: ['block_analysis'] },
            { key: 'text_analysis',      title: 'Анализ текста',             stages: ['text_analysis'] },
            { key: 'findings_merge',     title: 'Свод замечаний',            stages: ['findings_merge', 'merge'] },
            { key: 'findings_review',    title: 'Верификатор',               stages: ['findings_review'] },
            { key: 'norm_verify',        title: 'Проверка норм',             stages: ['norm_verify', 'norm_fix'] },
            { key: 'optimization',       title: 'Оптимизация',               stages: ['optimization'] },
            { key: 'debt_control',       title: 'Контроль долгов',           stages: ['debt_control'] },
            { key: 'decision_carryover', title: 'Перенос вердиктов',         stages: ['decision_carryover'] },
            { key: 'excel',              title: 'Excel-отчёт',               stages: ['excel'] },
            { key: 'other',              title: 'Прочее',                    stages: [] },
        ];
        const LOG_STAGE_TO_SECTION = {};
        LOG_STAGE_SECTIONS.forEach(s => s.stages.forEach(st => { LOG_STAGE_TO_SECTION[st] = s.key; }));

        // Свёрнутость секций: {sectionKey: bool}; выбор пользователя переживает
        // приход новых записей. По умолчанию все секции раскрыты: этапы
        // конвейера работают и параллельно (верификатор ∥ нормы ∥ оптимизация),
        // поэтому «раскрывать только активную» прятало бы живой вывод.
        // Сбрасывается при смене проекта.
        const logSectionCollapsed = ref({});

        const logSections = computed(() => {
            const buckets = {};
            for (const e of logEntries.value) {
                const key = LOG_STAGE_TO_SECTION[e.stage || ''] || 'other';
                (buckets[key] = buckets[key] || []).push(e);
            }
            const sections = [];
            for (const s of LOG_STAGE_SECTIONS) {
                const list = buckets[s.key];
                if (list && list.length) {
                    sections.push({ key: s.key, title: s.title, entries: list });
                }
            }
            return sections;
        });

        function isLogSectionCollapsed(section) {
            return logSectionCollapsed.value[section.key] === true;
        }

        function toggleLogSection(section) {
            logSectionCollapsed.value = {
                ...logSectionCollapsed.value,
                [section.key]: !isLogSectionCollapsed(section),
            };
        }

        watch(logProjectId, () => { logSectionCollapsed.value = {}; });

        // Текущая фаза для отображаемого проекта
        const currentFindingStage = computed(() => {
            const pid = logProjectId.value;
            return pid ? (findingStage.value[pid] || '') : '';
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
        // Последний увиденный poll'ом этап по проекту: {pid: stage|null}.
        // По смене этапа pollLiveStatus тихо перезагружает карточку проекта.
        const _lastPolledStage = {};

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

        // ─── Paid API cost ───
        const paidCost = ref({
            display_usd: 0,
            total_lifetime_usd: 0,
            month_key: '',
            monthly_spent_usd: 0,
            monthly_adjustment_usd: 0,
            monthly_limit_usd: 250,
            monthly_remaining_usd: 250,
            monthly_percent: 0,
            monthly_over_limit_usd: 0,
            monthly_calibrated_to_usd: null,
            monthly_calibrated_at: null,
        });
        const showPaidCost = ref(false);
        // Paid API guard: kill-switch статус + последние paid/blocked события.
        const paidApiStatus = ref(null);
        const paidEvents = ref([]);
        const paidBlockedEvents = ref([]);

        // ─── Submit lock (защита от double-submit) ────────────────────
        // В инциденте 2026-05-16 на M31A было 3 retry за 35 секунд (14:29:41,
        // 14:30:07, 14:30:16) — каждый стоил $0.32. Похоже на double-click
        // или Enter, проскочивший защиту auditRunning.value.
        //
        // _withSubmitLock(key, fn) гарантирует: пока fn для данного key не
        // завершилась (resolve или reject), повторные клики/Enter с тем же
        // key игнорируются. Также игнорируются попытки в первые 800 мс
        // после release — защита от «отпустил мышь, тут же снова кликнул».
        const _submitLocks = new Map();   // key -> 'running' | 'cooldown'
        const _SUBMIT_COOLDOWN_MS = 800;

        async function _withSubmitLock(key, fn) {
            if (_submitLocks.has(key)) {
                console.warn('[submit-lock] ignored duplicate:', key);
                return null;
            }
            _submitLocks.set(key, 'running');
            try {
                return await fn();
            } finally {
                _submitLocks.set(key, 'cooldown');
                setTimeout(() => _submitLocks.delete(key), _SUBMIT_COOLDOWN_MS);
            }
        }

        function _isSubmitLocked(key) {
            return _submitLocks.has(key);
        }

        async function fetchPaidCost() {
            try {
                const data = await api('/usage/paid-cost');
                paidCost.value = data;
            } catch (e) {
                console.error('Failed to fetch paid cost:', e);
            }
        }

        async function fetchPaidApiStatus() {
            try {
                paidApiStatus.value = await api('/usage/paid-api/status');
            } catch (e) {
                // не критично — продолжаем работу
                console.warn('Failed to fetch paid-api/status:', e);
            }
        }

        async function fetchPaidEvents() {
            try {
                const data = await api('/usage/paid-cost/events?limit=20');
                paidEvents.value = data.events || [];
            } catch (e) {
                console.warn('Failed to fetch paid events:', e);
            }
        }

        async function fetchPaidBlockedEvents() {
            try {
                const data = await api('/usage/paid-cost/blocked-events?limit=20');
                paidBlockedEvents.value = data.events || [];
            } catch (e) {
                console.warn('Failed to fetch blocked events:', e);
            }
        }

        // ─── Paid cost — daily dashboard ───
        // Список дней с расходами + детализация выбранного дня.
        // По умолчанию: окно 7 дней, выбран самый свежий день с расходом.
        const paidDailyDays = ref([]);            // массив дней из endpoint'а
        const paidDailyTotals = ref({ period_total_usd: 0, period_calls: 0 });
        const paidDailyPeriod = ref(7);           // 7 / 30 / 90
        const paidDailySelectedDate = ref(null);  // строка "YYYY-MM-DD"
        const paidDailyExpanded = ref(false);     // collapsible

        async function fetchPaidCostDaily() {
            try {
                const data = await api(`/usage/paid-cost/daily?days=${paidDailyPeriod.value}`);
                paidDailyDays.value = data.days || [];
                paidDailyTotals.value = data.totals || { period_total_usd: 0, period_calls: 0 };
                // Авто-выбор: сохранить текущий, если он есть в новых данных;
                // иначе — первый день с n_calls > 0, иначе первый день, иначе null.
                const dates = paidDailyDays.value.map(d => d.date);
                if (paidDailySelectedDate.value && dates.includes(paidDailySelectedDate.value)) {
                    return;
                }
                const firstWithCost = paidDailyDays.value.find(d => (d.n_calls || 0) > 0);
                paidDailySelectedDate.value = firstWithCost
                    ? firstWithCost.date
                    : (paidDailyDays.value[0] ? paidDailyDays.value[0].date : null);
            } catch (e) {
                console.warn('Failed to fetch paid-cost/daily:', e);
                paidDailyDays.value = [];
                paidDailyTotals.value = { period_total_usd: 0, period_calls: 0 };
                paidDailySelectedDate.value = null;
            }
        }

        function setPaidDailyPeriod(days) {
            paidDailyPeriod.value = days;
            // Сбросить выбранную дату чтобы fetcher переподобрал свежую.
            paidDailySelectedDate.value = null;
            fetchPaidCostDaily();
        }

        function selectPaidDailyDate(date) {
            paidDailySelectedDate.value = date;
        }

        // Computed-helper для текущего выбранного дня (или null).
        const paidDailySelectedDay = computed(() => {
            if (!paidDailySelectedDate.value) return null;
            return paidDailyDays.value.find(d => d.date === paidDailySelectedDate.value) || null;
        });

        function formatCostFull(usd) {
            const v = Number(usd || 0);
            if (v === 0) return '$0.00';
            if (v < 0.01) return '$' + v.toFixed(4);
            return '$' + v.toFixed(2);
        }

        function entriesSortedDesc(obj) {
            // {key: usd} → [[key, usd], ...] отсортировано по сумме убыванию.
            if (!obj || typeof obj !== 'object') return [];
            return Object.entries(obj).sort((a, b) => Number(b[1]) - Number(a[1]));
        }

        async function resetPaidCost() {
            if (!confirm('Обнулить счётчик расходов? Общая сумма за всё время сохранится. Журналы paid_cost_events.jsonl и paid_api_blocked_events.jsonl НЕ очищаются.')) return;
            try {
                const resp = await fetch('/api/usage/paid-cost/reset', { method: 'POST' });
                if (resp.ok) paidCost.value = await resp.json();
            } catch (e) {
                console.error('Failed to reset paid cost:', e);
            }
        }

        function formatCostShort(usd) {
            if (!usd || usd === 0) return '$0';
            if (usd < 0.01) return '<$0.01';
            return '$' + usd.toFixed(2);
        }

        function formatSignedCost(usd) {
            const value = Number(usd || 0);
            return `${value >= 0 ? '+' : '−'}$${Math.abs(value).toFixed(2)}`;
        }

        function formatPaidMonth(monthKey) {
            const match = /^(\d{4})-(\d{2})$/.exec(String(monthKey || ''));
            if (!match) return 'текущий месяц';
            const names = [
                'январь', 'февраль', 'март', 'апрель', 'май', 'июнь',
                'июль', 'август', 'сентябрь', 'октябрь', 'ноябрь', 'декабрь',
            ];
            const monthIndex = Number(match[2]) - 1;
            if (monthIndex < 0 || monthIndex >= names.length) return 'текущий месяц';
            return `${names[monthIndex]} ${match[1]}`;
        }

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
            // Legacy: процент Sonnet из JSONL-сканера (Claude Code sessions)
            // При миграции на OpenRouter этот показатель уходит в 0 — это нормально
            const m = globalUsage.value.weekly_by_model || {};
            return (m.sonnet && m.sonnet.percent) || 0;
        });

        // Старые usageCounters оставляем для совместимости с webapp-трекингом
        const usageCounters = ref({});
        const BLOCK_CONTEXT_STAGE_UI_LABEL = 'Векторные графы блоков';

        // ─── Per-project usage (токены по проектам/этапам) ───
        const projectUsage = ref({});  // {project_id: {total_tokens, total_cost_usd, total_calls, stages_summary}}

        // Защита от регрессии: /usage/projects-summary раньше возвращал
        // неполный шейп (без input_tokens/output_tokens/model на этапах),
        // и затирал детальную usage, загруженную через /usage/project/{id}.
        // Теперь мы:
        //   1) мержим вместо replace;
        //   2) если по проекту уже есть более детальная запись
        //      (хотя бы одна stage имеет input_tokens или model),
        //      а пришедшая summary этих полей не содержит — оставляем старое.
        function _stageEntryHasDetail(stage) {
            if (!stage) return false;
            return (
                Object.prototype.hasOwnProperty.call(stage, 'input_tokens')
                || Object.prototype.hasOwnProperty.call(stage, 'output_tokens')
                || (typeof stage.model === 'string' && stage.model.length > 0)
            );
        }
        function _usageEntryHasDetail(entry) {
            if (!entry) return false;
            if (Object.prototype.hasOwnProperty.call(entry, 'total_input_tokens')) return true;
            const ss = entry.stages_summary || {};
            for (const k in ss) {
                if (_stageEntryHasDetail(ss[k])) return true;
            }
            return false;
        }
        async function fetchAllProjectUsage() {
            try {
                const data = await api('/usage/projects-summary');
                const incoming = data || {};
                const prev = projectUsage.value || {};
                const next = { ...prev };
                for (const pid in incoming) {
                    const oldEntry = prev[pid];
                    const newEntry = incoming[pid];
                    if (_usageEntryHasDetail(oldEntry) && !_usageEntryHasDetail(newEntry)) {
                        // Старое полнее — не теряем поля карточек этапов.
                        continue;
                    }
                    next[pid] = newEntry;
                }
                projectUsage.value = next;
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
            'gemma_enrichment': 'gemma_enrichment',
            'text_analysis': 'text_analysis',
            'blocks_analysis': 'block_analysis',
            'block_retry': 'block_retry',
            'findings': 'findings_merge',
            'findings_critic': 'findings_critic',
            'findings_corrector': 'findings_corrector',
            'norms_verified': 'norm_verify',
            'optimization': 'optimization',
            'optimization_critic': 'optimization_critic',
            'optimization_corrector': 'optimization_corrector',
            'excel': 'excel',
        };

        function stageTokens(pipelineKey) {
            if (!currentProject.value) return null;
            const usage = projectUsage.value[currentProject.value.project_id];
            if (!usage || !usage.stages_summary) return null;
            const stageKey = _pipelineToStage[pipelineKey] || pipelineKey;
            return usage.stages_summary[stageKey] || null;
        }

        function stageTokensFormatted(pipelineKey) {
            const s = stageTokens(pipelineKey);
            if (!s) return null;
            const inp = s.input_tokens || 0;
            const out = s.output_tokens || 0;
            if (inp === 0 && out === 0) return null;
            return { inp: formatTokens(inp), out: formatTokens(out) };
        }

        function stageModel(pipelineKey) {
            const s = stageTokens(pipelineKey);
            if (!s || !s.model) return '';
            // Краткое имя модели: google/gemini-3.1-pro-preview → Gemini, openai/gpt-5.4 → GPT
            const m = s.model;
            if (m.includes('ensemble/gpt-codex')) return 'GPT+Codex';
            if (m.includes('codex')) return 'Codex';
            if (m.includes('gemini')) return 'Gemini';
            if (m.includes('gpt')) return 'GPT';
            if (m.includes('opus')) return 'Opus';
            if (m.includes('sonnet')) return 'Sonnet';
            if (m.includes('claude')) return 'Claude';
            // Fallback: последняя часть после /
            const parts = m.split('/');
            return parts[parts.length - 1].substring(0, 10);
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

        // ETA в секундах → "15м 22с" или "1ч 5м"
        function formatEta(seconds) {
            if (seconds === null || seconds === undefined) return '';
            const sec = Math.max(0, Math.round(seconds));
            if (sec < 60) return sec + 'с';
            const min = Math.floor(sec / 60);
            const remSec = sec % 60;
            if (min < 60) return min + 'м' + (remSec > 0 ? ' ' + remSec + 'с' : '');
            const hr = Math.floor(min / 60);
            const remMin = min % 60;
            return hr + 'ч' + (remMin > 0 ? ' ' + remMin + 'м' : '');
        }

        // ─── Prepare-data queue (block context) ──────────────────────────
        async function fetchPrepareQueue() {
            try {
                const r = await fetch('/api/audit/prepare-data/queue');
                if (!r.ok) return;
                prepareQueue.value = await r.json();
            } catch (e) { /* ignore */ }
        }

        async function clearPrepareQueue() {
            try {
                const r = await fetch('/api/audit/prepare-data/queue/clear', {method: 'POST'});
                if (r.ok) {
                    await fetchPrepareQueue();
                }
            } catch (e) {
                console.error('clearPrepareQueue:', e);
            }
        }

        async function preparePause() {
            try {
                await fetch('/api/audit/prepare-data/queue/pause', {method: 'POST'});
                await fetchPrepareQueue();
            } catch (e) { console.error('preparePause:', e); }
        }

        async function prepareResume() {
            try {
                await fetch('/api/audit/prepare-data/queue/resume', {method: 'POST'});
                await fetchPrepareQueue();
            } catch (e) { console.error('prepareResume:', e); }
        }

        async function prepareCancel() {
            if (!confirm('Остановить подготовку данных?\n\n• Pending проекты пометятся как пропущенные.\n• Текущий блок дойдёт до конца, потом остановка.\n• Что уже обогащено — сохранится.')) return;
            try {
                await fetch('/api/audit/prepare-data/queue/cancel', {method: 'POST'});
                await fetchPrepareQueue();
            } catch (e) { console.error('prepareCancel:', e); }
        }

        const currentProjectUsage = computed(() => {
            if (!currentProject.value) return null;
            const u = projectUsage.value[currentProject.value.project_id];
            return (u && u.total_tokens > 0) ? u : null;
        });

        function usagePaidCost(usage) {
            return Number(usage?.paid_cost_usd ?? usage?.total_cost_usd ?? 0);
        }

        function usageFreeCost(usage) {
            return Number(usage?.free_cost_usd ?? usage?.notional_cost_usd ?? 0);
        }

        const pipelineTotalDuration = computed(() => {
            if (!currentProject.value) return null;
            const summary = currentProject.value.pipeline_summary || [];
            let totalSec = 0;
            for (const s of summary) {
                if (s.duration_sec && s.status === 'done') totalSec += s.duration_sec;
            }
            if (totalSec <= 0) return null;
            if (totalSec < 60) return `${totalSec} сек`;
            const min = Math.floor(totalSec / 60);
            const sec = totalSec % 60;
            return sec > 0 ? `${min} мин ${sec} сек` : `${min} мин`;
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
                        // Смена этапа (или завершение аудита) → тихо перезагрузить
                        // карточку. Страховка на случай потерянных WS-сообщений:
                        // без неё «Статус конвейера» замирает до конца аудита.
                        const liveStage = data.running[pid] ? data.running[pid].stage : null;
                        if (pid in _lastPolledStage && _lastPolledStage[pid] !== liveStage) {
                            refreshProjectCardSilently(pid);
                        }
                        _lastPolledStage[pid] = liveStage;
                    }
                }
            } catch (e) {
                // Ignore polling errors
            }
        }

        function startPolling() {
            stopPolling();
            pollLiveStatus(); // сразу
            pollTimer = setInterval(pollLiveStatus, 15000);
            tickTimer = setInterval(() => {
                // Обновлять tick только когда есть активные задачи
                if (liveStatus.value.running && Object.keys(liveStatus.value.running).length > 0) {
                    elapsedTick.value++;
                }
            }, 1000);
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
                'queued': 'В очереди',
                'crop_blocks': 'Кроп блоков',
                'gemma_enrichment': BLOCK_CONTEXT_STAGE_UI_LABEL,
                'text_analysis': 'Анализ текста',
                'block_analysis': 'Анализ блоков',
                'findings_merge': 'Свод замечаний',
                'norm_verify': 'Верификация норм',
                'norm_fix': 'Пересмотр замечаний',
                'debt_control': 'Контроль долгов',
                'decision_carryover': 'Перенос вердиктов',
                'evidence_verify': 'Проверка фактов (EV)',
                'excel': 'Excel-отчёт',
                'optimization': 'Оптимизация',
                'full': 'Полный конвейер',
                // Legacy aliases
                'prepare': 'Подготовка',
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
                // Queued — без многоточия и без спиннер-эффекта
                if (r.status === 'queued') {
                    return 'В очереди';
                }
                const pct = r.progress_total > 0
                    ? Math.round(r.progress_current / r.progress_total * 100)
                    : 0;
                if (r.stage === 'block_analysis' && b) {
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
            const claudeStages = ['text_analysis', 'block_analysis', 'findings_merge', 'norm_verify', 'norm_fix', 'optimization', 'main_audit'];
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

        async function clearUsageCounter() {
            if (!confirm('Обнулить отображаемые счётчики (Сессия / Все / Sonnet) и записи проектов?')) return;
            try {
                const resp = await fetch('/api/usage/clear-all', { method: 'POST' });
                if (resp.ok) {
                    await refreshGlobalUsage();
                }
            } catch (e) {
                console.error('Failed to clear usage:', e);
            }
        }

        async function editUsagePercent(scope, currentPct) {
            const labels = {
                session_5h: 'Сессия (5ч)',
                weekly_all: 'Все модели (неделя)',
                weekly_sonnet: 'Sonnet (неделя)',
            };
            const label = labels[scope] || scope;
            const cur = Math.round(Number(currentPct) || 0);
            const raw = window.prompt(
                `${label}: введите процент (0–100).\n` +
                `Сейчас: ${cur}%. Поправит счётчик под значение из аккаунта Anthropic.`,
                String(cur)
            );
            if (raw === null) return;
            const trimmed = String(raw).trim();
            if (!trimmed) return;
            const pct = Number(trimmed.replace(',', '.').replace('%', ''));
            if (!Number.isFinite(pct) || pct < 0 || pct > 100) {
                alert('Нужно число от 0 до 100');
                return;
            }
            try {
                const resp = await fetch('/api/usage/global/set-percent', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ scope, percent: pct }),
                });
                if (resp.ok) {
                    const data = await resp.json();
                    if (data && data.counters) {
                        globalUsage.value = data.counters;
                    } else {
                        await refreshGlobalUsage();
                    }
                } else {
                    const txt = await resp.text();
                    alert('Не удалось сохранить: ' + txt);
                }
            } catch (e) {
                console.error('Failed to set percent:', e);
                alert('Ошибка: ' + e.message);
            }
        }

        async function resetUsageOffsets() {
            if (!confirm('Показывать «как есть» (сбросить ручные правки процентов)?')) return;
            try {
                const resp = await fetch('/api/usage/global/reset-offsets', { method: 'POST' });
                if (resp.ok) {
                    const data = await resp.json();
                    if (data && data.counters) globalUsage.value = data.counters;
                    else await refreshGlobalUsage();
                }
            } catch (e) {
                console.error('Failed to reset offsets:', e);
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
        // path — путь без `/api`. По умолчанию version_id из activeVersionId
        // подмешивается автоматически (для read-эндпоинтов: projects,
        // findings, optimization, blocks/tiles, document).
        // Передай `opts.withVersion=false` для endpoint'ов, которые сами
        // управляют версией (например `/projects/.../versions/v2/files`).
        async function api(path, opts) {
            opts = opts || {};
            // V2-stub: если active V2 на legacy runner (serverCaps.v2AuditSupported=false),
            // ряд read-endpoints отдают V1-данные, потому что legacy webapp
            // игнорирует ?version_id=. Подменяем такой ответ на пустой stub,
            // чтобы UI V2 не показывал V1 содержимое (см. smoke 2026-05-14).
            // Логика вынесена в VAPI.v2EmptyStubFor для тестируемости.
            if (VAPI && VAPI.v2EmptyStubFor) {
                const stub = VAPI.v2EmptyStubFor(path, activeVersionId.value, serverCaps);
                if (stub !== null) return stub;
            }
            const url = _apiUrl(path, opts.withVersion);
            // Таймаут + ретрай: раньше `await fetch(url)` не имел таймаута — если
            // соединение зависало (нестабильная сеть/потеря пакетов), запрос ждал
            // вечно, и спиннер «Загрузка проекта…» висел бесконечно. Теперь
            // зависший запрос обрывается по таймауту и переповторяется. Ретраим
            // только сетевые/таймаут-ошибки (не HTTP 4xx/5xx). api() = только GET,
            // поэтому ретрай идемпотентен.
            const timeoutMs = opts.timeoutMs || 25000;
            const retries = opts.retries != null ? opts.retries : 1;
            let lastErr;
            for (let attempt = 0; attempt <= retries; attempt++) {
                const ctrl = new AbortController();
                const timer = setTimeout(() => ctrl.abort(), timeoutMs);
                let resp;
                try {
                    resp = await fetch(url, { signal: ctrl.signal });
                } catch (e) {
                    clearTimeout(timer);
                    lastErr = e;
                    if (attempt < retries) {
                        await new Promise(r => setTimeout(r, 600 * (attempt + 1)));
                        continue;  // сетевой сбой/таймаут — переповтор
                    }
                    throw e;
                }
                clearTimeout(timer);
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || `API error: ${resp.status}`);
                }
                return resp.json();
            }
            throw lastErr;
        }

        // ─── Theme ───
        function toggleTheme() {
            theme.value = theme.value === 'dark' ? 'light' : 'dark';
            document.documentElement.setAttribute('data-theme', theme.value);
            localStorage.setItem('audit-theme', theme.value);
        }

        // ─── Navigation ───
        function navigate(path) {
            window.location.hash = path;
        }

        function handleRoute() {
            const rawHash = window.location.hash.slice(1) || '/';
            // Отделяем query от path (хранится `?version_id=v2`).
            const qIdx = rawHash.indexOf('?');
            const hash = qIdx >= 0 ? rawHash.slice(0, qIdx) : rawHash;
            const routeQuery = new URLSearchParams(qIdx >= 0 ? rawHash.slice(qIdx + 1) : '');

            // Версия из URL — если она задана, она перебивает activeVersionId.
            // Если её нет — оставляем активной то, что уже выбрано пользователем,
            // либо latest (определится после loadProjectVersions).
            const urlVersion = (typeof window !== 'undefined' && window.VersionAPI)
                ? window.VersionAPI.parseVersionFromHash(rawHash)
                : null;
            if (urlVersion && urlVersion !== activeVersionId.value) {
                // Смена версии — чистим кэши проектных данных, чтобы не мигал V1
                // контент внутри V2 (см. ТЗ "При переключении V2 → V1 старые
                // данные должны очищаться до загрузки V1").
                _cacheInvalidate('project');
                _cacheInvalidate('findings');
                _cacheInvalidate('optimization');
                _cacheInvalidate('blocks');
                currentProject.value = null;
                findingsData.value = null;
                _migratedReset();
                activeVersionId.value = urlVersion;
            } else if (!urlVersion && qIdx < 0 && !hash.startsWith('/project')) {
                // На дашборде/прочих не-проектных страницах сбрасываем выбор
                // версии, чтобы не утаскивать его при возврате к другому проекту.
                activeVersionId.value = null;
                _migratedReset();
            }

            // При прямом открытии страницы проекта (refresh/bookmark) projects может быть пустым —
            // загружаем все проекты чтобы работала навигация по разделу и sidebar.
            if (projects.value.length === 0 && hash.startsWith('/project')) {
                refreshProjects();
                loadProjectGroups();
            }

            const distributedTab = window.DistributedFeature.routeToTab(hash);
            if (distributedTab) {
                currentView.value = 'distributed';
                distributed.setTab(distributedTab);
                distributed.load();
            } else if (hash === '/knowledge-base') {
                currentView.value = 'knowledge-base';
                connectGlobalWS();
                // БЗ фильтруется по глобально выбранному объекту (верхний селектор «Объект»).
                loadKnowledgeBase();
                loadKBStats();
            } else if (hash === '/queue') {
                currentView.value = 'queue';
                connectGlobalWS();
                refreshBatchQueue();
                fetchPrepareQueue();   // подгрузить prepare-data queue
                refreshProjects();  // для списка добавления
            } else if (hash === '/schedule') {
                currentView.value = 'schedule';
                connectGlobalWS();
                loadUsers();   // для admin-гейта кнопки «Изменить план»
                schedLoad();
            } else if (hash === '/critic-v2-ui') {
                // Experimental offline view. Does NOT touch production pipeline.
                currentView.value = 'critic-v2-ui';
                connectGlobalWS();
            } else if (hash === '/stage-comparison') {
                currentView.value = 'stage-comparison';
                connectGlobalWS();
                scLoadObjects();
            } else if (hash === '/') {
                currentView.value = 'dashboard';
                sidebarFilterSection.value = null;
                connectGlobalWS();  // Вернуться на global WS
                refreshProjects();
            } else if (hash.match(/^\/section\/([^/]+)\/optimization$/)) {
                const code = decodeURIComponent(hash.match(/^\/section\/([^/]+)\/optimization$/)[1]);
                const requestedTab = routeQuery.get('tab');
                const initialTab = ['specifications', 'accepted', 'signals'].includes(requestedTab)
                    ? requestedTab
                    : 'specifications';
                currentView.value = 'section-optimization';
                sidebarFilterSection.value = code;
                sidebarSectionsOpen.value = true;
                connectGlobalWS();
                refreshProjects();
                loadSectionOptimization(code, initialTab);
            } else if (hash.match(/^\/section\/(.+)$/)) {
                const code = decodeURIComponent(hash.match(/^\/section\/(.+)$/)[1]);
                currentView.value = 'dashboard';
                sidebarFilterSection.value = code;
                // Конкретный раздел — список раскрываем, чтобы было видно, где
                // мы находимся. «Все разделы» — состояние подменю НЕ трогаем:
                // этот маршрут открывает сама кнопка «Разделы», которая тем же
                // кликом сворачивает/разворачивает список (иначе роутер тут же
                // раскрывал бы его обратно).
                if (code !== '__all__') sidebarSectionsOpen.value = true;
                connectGlobalWS();
                refreshProjects();
            } else if (hash.match(/^\/project\/(.+)\/findings$/)) {
                const id = decodeURIComponent(hash.match(/^\/project\/(.+)\/findings$/)[1]);
                currentView.value = 'findings';
                currentProjectId.value = id;
                connectGlobalWS();
                loadProject(id);
                loadFindings(id);
                loadExpertDecisions();
            } else if (hash.match(/^\/project\/(.+)\/blocks$/)) {
                const id = decodeURIComponent(hash.match(/^\/project\/(.+)\/blocks$/)[1]);
                currentView.value = 'blocks';
                currentProjectId.value = id;
                connectGlobalWS();
                loadProject(id);
                loadBlocks(id);
            } else if (hash.match(/^\/project\/(.+)\/optimization$/)) {
                const id = decodeURIComponent(hash.match(/^\/project\/(.+)\/optimization$/)[1]);
                currentView.value = 'optimization';
                currentProjectId.value = id;
                connectGlobalWS();
                loadProject(id);
                loadOptimization(id);
                loadExpertDecisions();
            } else if (hash.match(/^\/project\/(.+)\/document$/)) {
                const id = decodeURIComponent(hash.match(/^\/project\/(.+)\/document$/)[1]);
                currentView.value = 'document';
                currentProjectId.value = id;
                connectGlobalWS();
                loadProject(id);
                loadDocument(id);
            } else if (hash.match(/^\/project\/(.+)\/critic-v2-disagreements$/)) {
                // Project-scoped Critic v2 — opens straight on the disagreements filter.
                // Same view, same endpoint; only the default filter and the
                // feedback-export scope change. Experimental, offline read-only.
                const id = decodeURIComponent(
                    hash.match(/^\/project\/(.+)\/critic-v2-disagreements$/)[1]
                );
                currentView.value = 'critic-v2-project';
                currentProjectId.value = id;
                connectGlobalWS();
                loadProject(id);
                cv2LoadProject(id, { disagreementsMode: true });
            } else if (hash.match(/^\/project\/(.+)\/critic-v2$/)) {
                // Project-scoped Critic v2 (experimental, offline read-only).
                const id = decodeURIComponent(hash.match(/^\/project\/(.+)\/critic-v2$/)[1]);
                currentView.value = 'critic-v2-project';
                currentProjectId.value = id;
                connectGlobalWS();
                loadProject(id);
                cv2LoadProject(id);
            } else if (hash.match(/^\/project\/(.+)\/log$/)) {
                const id = decodeURIComponent(hash.match(/^\/project\/(.+)\/log$/)[1]);
                currentView.value = 'log';
                currentProjectId.value = id;
                logProjectId.value = id;
                loadProject(id);
                // Историю логов перечитываем из файла ВСЕГДА: WS-события сброса
                // (log_reset/log_stage_reset) могли прийти, пока вкладка была
                // закрыта — память без ресинка показывала бы удалённые записи.
                loadProjectLog(id);
                connectProjectWS(id);  // Project WS только для лога
            } else if (hash.match(/^\/project\/(.+)$/)) {
                const id = decodeURIComponent(hash.match(/^\/project\/(.+)$/)[1]);
                currentView.value = 'project';
                currentProjectId.value = id;
                connectGlobalWS();  // Не нужен project WS
                loadProject(id);
            }
        }

        // ─── Batch Selection (мультивыбор проектов) ───
        const selectedProjects = ref(new Set());
        const selectAllChecked = ref(false);
        const batchRunning = ref(false);
        const batchQueue = ref(null);
        const prepareQueue = ref(null);  // block-context queue (см. prepare_service.py)
        const showBatchModal = ref(false);
        const batchMode = ref('audit');   // audit
        const batchScope = ref('audit');     // audit | optimization | both
        const batchModalCount = ref(0);
        const batchAllMode = ref(false);  // true = запуск для ВСЕХ проектов

        // ─── Edit Projects (смена раздела / скрытие из UI) ───
        const showEditProjectsModal = ref(false);
        const editProjectsNewSection = ref('');
        const editProjectsLoading = ref(false);
        // Map { source_project_id: target_project_id } — для пакетного merge.
        const editProjectsMergeMap = ref({});
        const editProjectsSelected = computed(() => {
            const ids = selectedProjects.value;
            return projects.value.filter(p => ids.has(p.project_id));
        });

        function _emptyLatestForTarget(targetId) {
            const t = (projects.value || []).find(p => p.project_id === targetId);
            if (!t || !Array.isArray(t.versions_summary)) return null;
            const latest = t.versions_summary.find(v => v.is_latest);
            if (!latest) return null;
            if (latest.version_id === 'v1') return null;
            if ((latest.pdf_count || 0) > 0) return null;
            return latest;
        }

        // Для конкретного source-проекта — список потенциальных target'ов того же
        // раздела (исключая сам source). Совпадения по normalizeProjectName
        // помечаются `_suggested` и поднимаются вверх.
        function mergeTargetsFor(source) {
            if (!source) return [];
            const sec = source.section;
            if (!sec) return [];
            const srcName = (typeof normalizeProjectName === 'function')
                ? normalizeProjectName(source.name || source.project_id) : '';
            // Исключаем сам source и любые другие source'ы из этой же batch-сессии,
            // чтобы пользователь случайно не привязал A→B и B→A.
            const selectedIds = new Set(editProjectsSelected.value.map(p => p.project_id));
            const out = (projects.value || [])
                .filter(p => p.section === sec
                    && p.project_id !== source.project_id
                    && !selectedIds.has(p.project_id))
                .map(p => {
                    const norm = (typeof normalizeProjectName === 'function')
                        ? normalizeProjectName(p.name || p.project_id) : '';
                    return Object.assign({}, p, { _suggested: !!srcName && norm === srcName });
                });
            out.sort((a, b) => {
                if (a._suggested && !b._suggested) return -1;
                if (!a._suggested && b._suggested) return 1;
                return String(a.name || a.project_id).localeCompare(String(b.name || b.project_id));
            });
            return out;
        }

        // Имя следующей версии у target (учитывает «пустую latest»).
        function mergeNextLabelFor(targetId) {
            if (!targetId) return 'V?';
            const t = (projects.value || []).find(p => p.project_id === targetId);
            if (!t) return 'V?';
            const empty = _emptyLatestForTarget(targetId);
            if (empty) return (empty.label || 'V' + empty.version_no) + ' (пустая)';
            return 'V' + ((t.version_count || 1) + 1);
        }

        function mergeTargetNameFor(targetId) {
            if (!targetId) return '';
            const t = (projects.value || []).find(p => p.project_id === targetId);
            return t ? (t.name || t.project_id) : targetId;
        }

        // Сколько строк имеют выбранный target — для кнопки «Привязать выбранные пары».
        const editProjectsMergeReadyCount = computed(() => {
            let n = 0;
            for (const src of editProjectsSelected.value) {
                if (editProjectsMergeMap.value[src.project_id]) n += 1;
            }
            return n;
        });

        function openEditProjectsModal() {
            if (selectedProjects.value.size === 0) return;
            editProjectsNewSection.value = '';
            // Пред-заполняем map авто-подсказками: для каждого выбранного
            // source ищем target с _suggested == true.
            const seeded = {};
            for (const src of editProjectsSelected.value) {
                const opts = mergeTargetsFor(src);
                const suggested = opts.find(o => o._suggested);
                if (suggested) seeded[src.project_id] = suggested.project_id;
            }
            editProjectsMergeMap.value = seeded;
            showEditProjectsModal.value = true;
        }

        // У source есть готовые findings/нормы/оптимизации? Используется для
        // pre-warn в модалке merge-as-version: backend всё равно отдаст 409
        // с кодом `source_output_not_empty`, но нагляднее предупредить заранее.
        function _projectHasAuditArtifacts(p) {
            if (!p) return false;
            if ((p.findings_count || 0) > 0) return true;
            if ((p.optimization_count || 0) > 0) return true;
            const pipeline = p.pipeline || {};
            const STAGES = ['text_analysis','blocks_analysis','findings','optimization','norms_verified'];
            return STAGES.some(s => pipeline[s] === 'done');
        }

        // Выполнить один merge с попыткой повторить запрос с discard_source_output
        // при 409 / source_output_not_empty. Возвращает { ok, version_label, error }.
        async function _mergeOnePair(source, targetId, { discardSourceOutput }) {
            const resp = await fetch('/api/projects/versions/from-project', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    target_project_id: targetId,
                    source_project_id: source.project_id,
                    comment: 'Привязано из окна Изменить выбранные проекты',
                    source: 'edit_projects_modal',
                    delete_source: true,
                    discard_source_output: !!discardSourceOutput,
                }),
            });
            if (resp.ok) {
                const data = await resp.json();
                return { ok: true, data };
            }
            const err = await resp.json().catch(() => ({}));
            return { ok: false, status: resp.status, detail: err.detail };
        }

        // Пакетное применение merge: каждая (source → target) пара
        // выполняется отдельным запросом, ошибки одной не останавливают другие.
        async function applyMergeAllAsVersion() {
            const pairs = [];
            for (const src of editProjectsSelected.value) {
                const tgt = editProjectsMergeMap.value[src.project_id];
                if (tgt) pairs.push({ source: src, targetId: tgt });
            }
            if (pairs.length === 0) return;

            // Одно подтверждение на всё действие. Второй вопрос («результаты
            // source будут потеряны») убран 2026-08-18: в projects_v2 артефакты
            // аудита переезжают в новую версию, а не отбрасываются, — спрашивать
            // о потере стало и лишним, и неверным. Флаг discard шлём всегда:
            // на legacy-пути он лишь снимает 409 source_output_not_empty,
            // из-за которого всплывал третий вопрос подряд.
            const withArtifacts = pairs.filter(p => _projectHasAuditArtifacts(p.source));
            const artifactsLine = withArtifacts.length
                ? `\nРезультаты аудита (${withArtifacts.length} шт.) переедут в новую версию.`
                : '';
            if (!confirm(
                `Привязать ${pairs.length} проект(ов) как версии существующих?\n` +
                `Исходные карточки будут удалены. V1 каждого target не изменится.` +
                artifactsLine
            )) return;

            editProjectsLoading.value = true;
            const errors = [];
            const okList = [];
            try {
                for (const { source, targetId } of pairs) {
                    try {
                        let res = await _mergeOnePair(source, targetId, { discardSourceOutput: true });
                        // Страховка для legacy-пути: если backend всё же ответил
                        // 409 source_output_not_empty — повторяем с флагом молча,
                        // согласие уже получено первым (и единственным) вопросом.
                        if (!res.ok && res.status === 409
                            && res.detail && typeof res.detail === 'object'
                            && res.detail.code === 'source_output_not_empty') {
                            res = await _mergeOnePair(source, targetId, { discardSourceOutput: true });
                        }
                        if (!res.ok) {
                            const msg = (res.detail && (res.detail.message || res.detail)) || `HTTP ${res.status}`;
                            throw new Error(typeof msg === 'string' ? msg : JSON.stringify(msg));
                        }
                        const verLabel = (res.data.version && res.data.version.label) || 'V?';
                        okList.push(`${source.name || source.project_id} → ${mergeTargetNameFor(targetId)} (${verLabel})`);
                    } catch (e) {
                        errors.push(`${source.name || source.project_id}: ${e.message}`);
                    }
                }
                const lines = [];
                if (okList.length) lines.push(`Готово (${okList.length}):\n` + okList.join('\n'));
                if (errors.length) lines.push(`Ошибки (${errors.length}):\n` + errors.join('\n'));
                if (lines.length) alert(lines.join('\n\n'));
                selectedProjects.value = new Set();
                selectAllChecked.value = false;
                showEditProjectsModal.value = false;
                await refreshProjects();
            } finally {
                editProjectsLoading.value = false;
            }
        }
        async function applyNewSectionToSelected() {
            const section = (editProjectsNewSection.value || '').trim();
            if (!section) return;
            const ids = Array.from(selectedProjects.value);
            if (ids.length === 0) return;
            editProjectsLoading.value = true;
            try {
                let failed = 0;
                for (const pid of ids) {
                    try {
                        const resp = await fetch(`/api/projects/${encodeURIComponent(pid)}/section`, {
                            method: 'PUT',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ section }),
                        });
                        if (!resp.ok) failed += 1;
                    } catch (e) {
                        failed += 1;
                    }
                }
                if (failed > 0) {
                    alert(`Не удалось обновить раздел у ${failed} из ${ids.length} проектов`);
                }
                selectedProjects.value = new Set();
                selectAllChecked.value = false;
                showEditProjectsModal.value = false;
                await refreshProjects();
            } finally {
                editProjectsLoading.value = false;
            }
        }
        async function deleteSelectedProjects() {
            const ids = Array.from(selectedProjects.value);
            if (ids.length === 0) return;
            const names = ids.slice(0, 8).join('\n  • ');
            const extra = ids.length > 8 ? `\n  …и ещё ${ids.length - 8}` : '';
            if (!confirm(
                `БЕЗВОЗВРАТНО удалить ${ids.length} проект(ов)?\n\n  • ${names}${extra}\n\n` +
                `Будут удалены папка проекта и его документ в projects_v2. Это действие нельзя отменить.`
            )) return;
            editProjectsLoading.value = true;
            try {
                let failed = 0;
                const failedIds = [];
                for (const pid of ids) {
                    try {
                        const resp = await fetch(`/api/projects/${encodeURIComponent(pid)}`, { method: 'DELETE' });
                        if (!resp.ok) { failed += 1; failedIds.push(pid); }
                    } catch (e) {
                        failed += 1; failedIds.push(pid);
                    }
                }
                if (failed > 0) {
                    alert(`Не удалось удалить ${failed} из ${ids.length} проектов:\n${failedIds.join('\n')}\n` +
                          `(возможно, идёт аудит — сначала отмените его)`);
                }
                selectedProjects.value = new Set();
                selectAllChecked.value = false;
                showEditProjectsModal.value = false;
                await refreshProjects();
            } finally {
                editProjectsLoading.value = false;
            }
        }

        async function hideSelectedFromUI() {
            const ids = Array.from(selectedProjects.value);
            if (ids.length === 0) return;
            if (!confirm(`Скрыть из UI ${ids.length} проект(ов)? Файлы на диске останутся.`)) return;
            editProjectsLoading.value = true;
            try {
                let failed = 0;
                for (const pid of ids) {
                    try {
                        const resp = await fetch(`/api/projects/${encodeURIComponent(pid)}/hide`, { method: 'POST' });
                        if (!resp.ok) failed += 1;
                    } catch (e) {
                        failed += 1;
                    }
                }
                if (failed > 0) {
                    alert(`Не удалось скрыть ${failed} из ${ids.length} проектов`);
                }
                selectedProjects.value = new Set();
                selectAllChecked.value = false;
                showEditProjectsModal.value = false;
                await refreshProjects();
            } finally {
                editProjectsLoading.value = false;
            }
        }

        // ─── Pause / Resume ───
        const showPauseModal = ref(false);
        const isPaused = ref(false);
        const pauseMode = ref(null);

        // ─── Model Config (per-stage) ───
        const showModelConfig = ref(false);
        const stageModelConfig = ref({});
        const availableModels = ref([]);
        const modelConfigPendingProjectId = ref(null);
        const stageModelSaveError = ref('');
        const stageLabels = {
            block_batch: "01 Блоки",
            text_analysis: "02 Текст",
            findings_merge: "03 Свод",
            findings_critic: "Верификатор",
            findings_corrector: "Верификатор (фикс)",
            norm_verify: "04 Нормы",
            norm_fix: "04b Пересмотр",
            optimization: "05 Оптимизация",
            optimization_critic: "C OPT Critic",
            optimization_corrector: "F OPT Fix",
        };

        const stageModelRestrictions = ref({});
        const stageModelHints = ref({});
        const stageEnsembleDetails = ref({});
        const CODEX_PRESET_MODEL = "__codex_exec__";
        const BLOCK_CODEX_ENSEMBLE_MODEL = "ensemble/gpt-codex";
        const OPT_CODEX_ENSEMBLE_MODEL = "ensemble/claude-codex-opt";
        const BASE_STAGE_MODEL_CONFIG = {
            text_analysis:          "claude-opus-5",
            block_batch:            "openai/gpt-5.4",
            findings_merge:         "claude-opus-5",
            findings_critic:        "claude-sonnet-5",
            findings_corrector:     "claude-sonnet-5",
            norm_verify:            "claude-opus-5",
            norm_fix:               "claude-opus-5",
            norm_requote:           "claude-opus-5",
            optimization:           "claude-opus-5",
            optimization_critic:    "claude-sonnet-5",
            optimization_corrector: "claude-sonnet-5",
        };
        const modelPresets = {
            claude_gpt_codex: {
                label: "Claude+GPT +Codex",
                hint: "01 Блоки: GPT+Codex, сравнение и gap-search · 05 Оптимизация: Claude+Codex.",
                config: {
                    ...BASE_STAGE_MODEL_CONFIG,
                    block_batch:            BLOCK_CODEX_ENSEMBLE_MODEL,
                    optimization:           OPT_CODEX_ENSEMBLE_MODEL,
                },
                batchModes: { block_batch: "findings_only_block_context" },
            },
            codex_exec: {
                label: "Full Codex",
                hint: "01 Блоки: GPT+Codex, сравнение и gap-search · 05 Оптимизация: Claude+Codex · остальные этапы выполняет Codex; перед запуском сохраняется snapshot.",
                config: {
                    text_analysis:          CODEX_PRESET_MODEL,
                    block_batch:            BLOCK_CODEX_ENSEMBLE_MODEL,
                    findings_merge:         CODEX_PRESET_MODEL,
                    findings_critic:        CODEX_PRESET_MODEL,
                    findings_corrector:     CODEX_PRESET_MODEL,
                    norm_verify:            CODEX_PRESET_MODEL,
                    norm_fix:               CODEX_PRESET_MODEL,
                    norm_requote:           CODEX_PRESET_MODEL,
                    optimization:           OPT_CODEX_ENSEMBLE_MODEL,
                    optimization_critic:    CODEX_PRESET_MODEL,
                    optimization_corrector: CODEX_PRESET_MODEL,
                },
                batchModes: { block_batch: "findings_only_block_context" },
            },
        };
        const activePreset = ref(null);
        // Сохранённый конфиг может не совпасть ни с одним пресетом: раскладку задали
        // вручную или пресет поменяли уже после сохранения (тогда конфиг «осиротел»).
        // Подсветки в этом случае нет — и окно открывалось пустым, без единого намёка,
        // на чём пойдёт запуск. Метку показываем только когда конфиг УЖЕ загружен,
        // иначе она мигала бы на старте, пока /audit/model/stages в пути.
        const isCustomStageConfig = computed(() => (
            Object.keys(stageModelConfig.value || {}).length > 0 && !activePreset.value
        ));
        const activePresetHint = computed(() => {
            const key = activePreset.value;
            if (key) return modelPresets[key]?.hint || '';
            return isCustomStageConfig.value
                ? 'Своя раскладка — не совпадает ни с одним пресетом. Модели заданы вручную в таблице ниже.'
                : '';
        });
        const stageBatchModes = ref({});  // { block_batch: "findings_only_block_context" }
        const stageBatchModeChoices = ref({});

        // Ensemble IDs are an execution detail. The table shows the base
        // model and one additive Codex flag instead of three internal columns.
        const visibleStageModels = computed(() => availableModels.value.filter(model => (
            model?.provider !== 'codex_cli'
            && model?.provider !== 'ensemble'
            && model?.provider !== 'optimization_ensemble'
        )));

        // Stage 01 supports one independent detector or the explicit dual ensemble.
        const findingsOnlyCompatibleBlockModels = [
            'openai/gpt-5.4',
            'ensemble/gpt-codex',
        ];

        function isFindingsOnlyMode() {
            return stageBatchModes.value?.block_batch === 'findings_only_block_context';
        }

        function normalizeAvailableModels(models) {
            const list = Array.isArray(models) ? [...models] : [];
            const hasCodex = list.some(m => m?.provider === 'codex_cli' || String(m?.id || '').startsWith('codex/'));
            if (!hasCodex) {
                const insertAt = Math.min(3, list.length);
                list.splice(insertAt, 0, { id: 'codex/gpt-5.4', label: 'Codex', provider: 'codex_cli', uiFallback: true });
            }
            return list;
        }

        function codexModelId() {
            return availableModels.value.find(m => m.provider === 'codex_cli')?.id || 'codex/gpt-5.4';
        }

        function resolvePresetModelId(modelId) {
            return modelId === CODEX_PRESET_MODEL ? codexModelId() : modelId;
        }

        function resolvePresetConfig(preset) {
            return Object.fromEntries(
                Object.entries(preset?.config || {}).map(([stageKey, modelId]) => [stageKey, resolvePresetModelId(modelId)])
            );
        }

        function getMatchingPresetKey(config, batchModes) {
            return Object.entries(modelPresets).find(([, preset]) => {
                const resolvedConfig = resolvePresetConfig(preset);
                const cfgMatch = Object.entries(resolvedConfig).every(([stageKey, modelId]) => config?.[stageKey] === modelId);
                if (!cfgMatch) return false;
                const presetModes = preset.batchModes || {};
                return Object.entries(presetModes).every(([stage, mode]) => (batchModes?.[stage] || 'findings_only_block_context') === mode);
            })?.[0] || null;
        }

        function applyPreset(presetKey) {
            const preset = modelPresets[presetKey];
            if (!preset) return;
            stageModelConfig.value = { ...stageModelConfig.value, ...resolvePresetConfig(preset) };
            stageBatchModes.value = { ...(preset.batchModes || { block_batch: 'findings_only_block_context' }) };
            activePreset.value = presetKey;
        }

        function isModelAllowed(stageKey, modelId) {
            const r = stageModelRestrictions.value[stageKey];
            if (r && !r.includes(modelId)) return false;
            // findings_only_block_context: production block_batch is GPT-5.4 only.
            if (stageKey === 'block_batch' && isFindingsOnlyMode()) {
                return findingsOnlyCompatibleBlockModels.includes(modelId) || String(modelId || '').startsWith('codex/');
            }
            return true;
        }

        function isBaseStageModelChecked(stageKey, modelId) {
            const effectiveModel = stageModelConfig.value[stageKey];
            if (effectiveModel === BLOCK_CODEX_ENSEMBLE_MODEL) {
                return stageKey === 'block_batch' && modelId === 'openai/gpt-5.4';
            }
            if (effectiveModel === OPT_CODEX_ENSEMBLE_MODEL) {
                return stageKey === 'optimization' && modelId === 'claude-opus-5';
            }
            return effectiveModel === modelId;
        }

        function isCodexStageChecked(stageKey) {
            const modelId = String(stageModelConfig.value[stageKey] || '');
            return modelId.startsWith('codex/')
                || modelId === BLOCK_CODEX_ENSEMBLE_MODEL
                || modelId === OPT_CODEX_ENSEMBLE_MODEL;
        }

        function isCodexStageAllowed(stageKey) {
            const allowed = stageModelRestrictions.value[stageKey];
            if (!allowed) return true;
            return allowed.includes(codexModelId())
                || (stageKey === 'block_batch' && allowed.includes(BLOCK_CODEX_ENSEMBLE_MODEL))
                || (stageKey === 'optimization' && allowed.includes(OPT_CODEX_ENSEMBLE_MODEL));
        }

        function selectBaseStageModel(stageKey, modelId) {
            const codexWasChecked = isCodexStageChecked(stageKey);
            if (codexWasChecked && stageKey === 'block_batch' && modelId === 'openai/gpt-5.4') {
                stageModelConfig.value[stageKey] = BLOCK_CODEX_ENSEMBLE_MODEL;
            } else if (codexWasChecked && stageKey === 'optimization' && modelId === 'claude-opus-5') {
                stageModelConfig.value[stageKey] = OPT_CODEX_ENSEMBLE_MODEL;
            } else {
                stageModelConfig.value[stageKey] = modelId;
            }
            activePreset.value = getMatchingPresetKey(stageModelConfig.value, stageBatchModes.value);
        }

        function toggleStageCodex(stageKey, event) {
            const enabled = Boolean(event?.target?.checked);
            const effectiveModel = stageModelConfig.value[stageKey];
            if (enabled) {
                if (stageKey === 'block_batch' && effectiveModel === 'openai/gpt-5.4') {
                    stageModelConfig.value[stageKey] = BLOCK_CODEX_ENSEMBLE_MODEL;
                } else if (stageKey === 'optimization' && effectiveModel === 'claude-opus-5') {
                    stageModelConfig.value[stageKey] = OPT_CODEX_ENSEMBLE_MODEL;
                } else {
                    stageModelConfig.value[stageKey] = codexModelId();
                }
            } else if (effectiveModel === BLOCK_CODEX_ENSEMBLE_MODEL) {
                stageModelConfig.value[stageKey] = 'openai/gpt-5.4';
            } else if (effectiveModel === OPT_CODEX_ENSEMBLE_MODEL) {
                stageModelConfig.value[stageKey] = 'claude-opus-5';
            } else if (String(effectiveModel || '').startsWith('codex/')) {
                stageModelConfig.value[stageKey] = BASE_STAGE_MODEL_CONFIG[stageKey]
                    || 'claude-sonnet-5';
            }
            activePreset.value = getMatchingPresetKey(stageModelConfig.value, stageBatchModes.value);
        }

        async function loadStageModels() {
            try {
                stageModelSaveError.value = '';
                const data = await api('/audit/model/stages');
                stageModelConfig.value = data.stages || {};
                availableModels.value = normalizeAvailableModels(data.available_models || []);
                stageModelRestrictions.value = data.restrictions || {};
                stageModelHints.value = data.hints || {};
                stageEnsembleDetails.value = data.ensemble_details || {};
                if (data.config_errors && Object.keys(data.config_errors).length > 0) {
                    stageModelSaveError.value = `Текущая конфигурация моделей невалидна: ${formatRejected(data.config_errors)}`;
                }
                // Параллельно подгружаем batch-modes (production block-context mode)
                try {
                    const bm = await api('/audit/model/batch-modes');
                    stageBatchModes.value = bm.modes || { block_batch: 'findings_only_block_context' };
                    stageBatchModeChoices.value = bm.choices || {};
                } catch (_) {
                    stageBatchModes.value = { block_batch: 'findings_only_block_context' };
                    stageBatchModeChoices.value = {};
                }
                activePreset.value = getMatchingPresetKey(stageModelConfig.value, stageBatchModes.value);
            } catch (e) {
                console.error('Failed to load stage models:', e);
            }
        }

        function formatRejected(rejected) {
            return Object.entries(rejected || {})
                .map(([stage, reason]) => `${stage}: ${reason}`)
                .join('; ');
        }

        async function saveStageModels() {
            stageModelSaveError.value = '';
            try {
                const modelResult = await apiPost('/audit/model/stages', stageModelConfig.value);
                const batchResult = await apiPost('/audit/model/batch-modes', stageBatchModes.value);
                const problems = [];
                if (modelResult?.rejected && Object.keys(modelResult.rejected).length > 0) {
                    problems.push(`Модели: ${formatRejected(modelResult.rejected)}`);
                }
                if (batchResult?.rejected && Object.keys(batchResult.rejected).length > 0) {
                    problems.push(`Режимы: ${formatRejected(batchResult.rejected)}`);
                }
                if (problems.length > 0) {
                    throw new Error(problems.join('\n'));
                }
                return { modelResult, batchResult };
            } catch (e) {
                console.error('Failed to save stage models:', e);
                stageModelSaveError.value = e?.message || 'Не удалось сохранить конфигурацию моделей';
                alert(stageModelSaveError.value);
                throw e;
            }
        }

        // pendingRetryStage: если задан — после сохранения моделей запустить retry этапа, а не полный аудит
        const pendingRetryStage = ref(null);
        // pendingActionFn: произвольный callback, выполняется после сохранения моделей (приоритет над retryStage/pid)
        const pendingActionFn = ref(null);
        function openModelConfig(projectId, retryStage = null, afterSaveFn = null, presetKey = null) {
            modelConfigPendingProjectId.value = projectId;
            pendingRetryStage.value = retryStage;
            pendingActionFn.value = afterSaveFn;

            loadStageModels().then(() => {
                if (presetKey) {
                    applyPreset(presetKey);
                }
                showModelConfig.value = true;
            });
        }

        async function saveAndStartAudit() {
            try {
                await saveStageModels();
            } catch (_) {
                return;
            }
            const pid = modelConfigPendingProjectId.value;
            showModelConfig.value = false;
            if (pendingActionFn.value) {
                const fn = pendingActionFn.value;
                pendingActionFn.value = null;
                await fn();
                return;
            }
            const retryStg = pendingRetryStage.value;
            pendingRetryStage.value = null;
            if (pid) {
                if (retryStg) {
                    _executeRetryStage(pid, retryStg);
                } else {
                    startAuditDirect(pid);
                }
            }
        }

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

        // Единый критерий «на проверку не запускали»: у карточки нет ни одного
        // результата аудита — ни замечаний, ни оптимизаций. Им пользуются и
        // кнопка «Выделить необработанные», и столбец «Не запускались на
        // проверку» на главной — чтобы цифра в столбце и число выделяемых по
        // клику проектов не разъезжались. Оптимизации учитываются наравне с
        // замечаниями: аудит, нашедший только оптимизации, — запускался.
        function isProjectUnanalyzed(p) {
            if (!p) return true;
            return !((p.findings_count > 0) || (p.optimization_count > 0));
        }

        // Проект отработан экспертом полностью — обе галочки на карточке.
        // expert_review_status оставлен как fallback: на части путей приходят
        // только раздельные findings_/optimization_review_status.
        function isProjectExpertChecked(p) {
            if (!p) return false;
            if (p.expert_review_status === 'complete') return true;
            return p.findings_review_status === 'complete'
                && p.optimization_review_status === 'complete';
        }

        // Счётчик в sidebar строго привязан к двум видимым галочкам карточки.
        // Общий expert_review_status здесь не fallback: проект перестаёт быть
        // «непроверенным» только когда complete стоит у обоих раздельных статусов.
        function hasBothExpertChecks(p) {
            return !!p
                && p.findings_review_status === 'complete'
                && p.optimization_review_status === 'complete';
        }

        // Ждёт ли проект проверки экспертом. Решает бэкенд (`review_pending`):
        // он смотрит на ПОСЛЕДНЮЮ версию, где есть результаты аудита, поэтому
        // свежая версия без аудита не превращает проверенный проект в
        // непроверенный и не прячет непроверенную предыдущую версию. Проект,
        // у которого проверять нечего (аудит не запускался ни разу), в счётчик
        // не попадает. Fallback на две галочки — для источников постарше,
        // которые поле ещё не отдают.
        function isProjectReviewPending(p) {
            if (!p) return false;
            if (typeof p.review_pending === 'boolean') return p.review_pending;
            return !hasBothExpertChecks(p);
        }

        // Ключ логического проекта: карточки «X V1» и «X_V1» — один проект.
        // Считает бэкенд (`base_project_key`), фронт только группирует; для
        // источников без поля ключом остаётся project_id (счёт как раньше).
        function projectBaseKey(p) {
            return (p && (p.base_project_key || p.project_id)) || '';
        }

        // Счётчики «всего проектов» и «не проверено» считают УНИКАЛЬНЫЕ проекты:
        // проект, загруженный несколькими карточками-версиями, — это один
        // проект, а не два-три (запрос Андрея Ивановича 2026-08-19).
        function uniqueProjectCount(items) {
            const keys = new Set();
            for (const p of items || []) keys.add(projectBaseKey(p));
            return keys.size;
        }

        function expertUncheckedCount(items) {
            const keys = new Set();
            for (const p of items || []) {
                if (isProjectReviewPending(p)) keys.add(projectBaseKey(p));
            }
            return keys.size;
        }

        // Номер версии из имени карточки («X V2», «X_V2») — зеркало
        // `base_project_key` на бэкенде, который этот же суффикс срезает.
        // Карточка без суффикса получает 0: в паре «X» и «X V2» последней
        // считается «X V2».
        const _VERSION_SUFFIX_RE = /[\s_\-]+[Vv]\s*(\d+)\s*$/;
        function cardVersionRank(p) {
            const m = _VERSION_SUFFIX_RE.exec(String((p && p.project_id) || ''));
            return m ? parseInt(m[1], 10) : 0;
        }

        // Последняя карточка каждого логического проекта: сводка разделов
        // считает ПРОЕКТЫ по их последней версии, а не карточки-версии.
        // Внутри одной карточки версия и так последняя — список проектов
        // отдаёт статус `current_version`, а он в v2 всегда старший.
        // Между карточками-версиями («X V1» и «X V2» — два документа)
        // старшинство решает суффикс имени, при равенстве — version_no.
        function latestProjectCards(items) {
            const byKey = new Map();
            for (const p of items || []) {
                const key = projectBaseKey(p);
                const prev = byKey.get(key);
                if (!prev) { byKey.set(key, p); continue; }
                const d = cardVersionRank(p) - cardVersionRank(prev);
                if (d > 0 || (d === 0 && (p.version_no || 1) > (prev.version_no || 1))) {
                    byKey.set(key, p);
                }
            }
            return Array.from(byKey.values());
        }

        // Эксперт полностью закрыл проект — СТРОГО по этой версии (решение
        // Андрея Ивановича 2026-08-19: «всегда ориентируемся на последний
        // проект»). Отличия от `isProjectReviewPending`, который спускается к
        // предыдущей версии с результатами: непроверенная свежая версия
        // обнуляет проверку проекта.
        // Пустая категория галочку не блокирует — зеркало `_review_incomplete`
        // на бэкенде: проект без оптимизаций иначе никогда не станет
        // проверенным (их статус остаётся пустым).
        function isProjectExpertResolved(p) {
            if (!p || isProjectUnanalyzed(p)) return false;   // проверять нечего
            const findingsOk = !(p.findings_count > 0)
                || p.findings_review_status === 'complete';
            const optOk = !(p.optimization_count > 0)
                || p.optimization_review_status === 'complete';
            if (findingsOk && optOk) return true;
            // fallback для источников, отдающих только сводный статус
            return p.expert_review_status === 'complete';
        }

        // Порядок карточек в разделе: 0 — проверенные экспертом, 1 — обработанные
        // (аудит прошёл, вердиктов ещё нет), 2 — те, на которых аудит не запускался.
        function projectOrderRank(p) {
            if (isProjectExpertChecked(p)) return 0;
            return isProjectUnanalyzed(p) ? 2 : 1;
        }

        // numeric: true — чтобы «АР1-2» шёл перед «АР1-10», а не после.
        const _projectNameCollator = new Intl.Collator('ru', { numeric: true, sensitivity: 'base' });

        // Сортировка проектов раздела: сначала по статусу, внутри статуса —
        // по имени по алфавиту.
        function sortSectionProjects(list) {
            return list.slice().sort((a, b) => {
                const byRank = projectOrderRank(a) - projectOrderRank(b);
                if (byRank !== 0) return byRank;
                return _projectNameCollator.compare(a.name || a.project_id || '', b.name || b.project_id || '');
            });
        }

        // Необработанные проекты раздела ('__all__' — по всем разделам сразу).
        function unanalyzedPids(sectionCode) {
            return projects.value
                .filter(p => (sectionCode === '__all__' || (p.section || 'OTHER') === sectionCode)
                             && isProjectUnanalyzed(p))
                .map(p => p.project_id);
        }

        // Все ли необработанные проекты уже выделены — состояние цифры-кнопки
        // «Необработаны» и кнопки «Выделить необработанные» (подсветка/подпись
        // + направление переключателя).
        function isUnanalyzedSelected(sectionCode) {
            const pids = unanalyzedPids(sectionCode);
            return pids.length > 0 && pids.every(id => selectedProjects.value.has(id));
        }

        // Переключатель выделения необработанных — цифра «Необработаны» на
        // главной и кнопка «Выделить необработанные» в разделе: первый клик
        // выделяет, повторный снимает выделение с тех же проектов.
        // sectionCode === '__all__' — строка «Итого» (все разделы сразу).
        function toggleUnanalyzedSelection(sectionCode) {
            const pids = unanalyzedPids(sectionCode);
            if (!pids.length) return;
            const s = new Set(selectedProjects.value);
            const allSelected = pids.every(id => s.has(id));
            for (const id of pids) {
                if (allSelected) s.delete(id); else s.add(id);
            }
            selectedProjects.value = s;
            selectAllChecked.value = s.size === projects.value.length && s.size > 0;
        }

        // Проект «не проверен» — тот же критерий, что и у счётчика в сайдбаре
        // (`isProjectReviewPending`): последняя версия С РЕЗУЛЬТАТАМИ без обеих
        // галочек. Раньше здесь была своя формула (`expert_review_status` по
        // текущей версии), из-за чего «Не проверено (N)» в шапке раздела и
        // бейдж в сайдбаре показывали разные числа. Проекты без аудита не
        // считаются — для них есть «Выделить необработанные».
        function isProjectUnreviewed(p) {
            return isProjectReviewPending(p);
        }

        function sectionUnreviewedPids(sectionCode) {
            return projects.value
                .filter(p => (p.section || 'OTHER') === sectionCode && isProjectUnreviewed(p))
                .map(p => p.project_id);
        }

        // Все ли «не проверенные» проекты раздела уже выделены (состояние флажка).
        function isSectionUnreviewedSelected(sectionCode) {
            const pids = sectionUnreviewedPids(sectionCode);
            return pids.length > 0 && pids.every(id => selectedProjects.value.has(id));
        }

        // Флажок «Не проверено»: выделить/снять все не проверенные проекты раздела.
        function toggleSectionUnreviewedSelection(sectionCode) {
            const pids = sectionUnreviewedPids(sectionCode);
            if (!pids.length) return;
            const s = new Set(selectedProjects.value);
            const allSelected = pids.every(id => s.has(id));
            for (const id of pids) {
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
            // Показываем выбор моделей перед запуском пакета
            openModelConfig(null, null, () => startBatchAction(action));
        }

        async function startBatchAction(action) {
            const ids = Array.from(selectedProjects.value);
            const lockKey = `batch:${action}:${ids.length}`;
            if (_isSubmitLocked(lockKey)) {
                console.warn('[submit-lock] batch duplicate ignored');
                return;
            }
            return _withSubmitLock(lockKey, async () => {
            try {
                batchRunning.value = true;
                const resp = await fetch('/api/audit/batch', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        project_ids: ids,
                        action: action,
                    }),
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
            }); // end _withSubmitLock
        }

        function batchActionLabel(action) {
            const labels = {
                'resume': 'Продолжение прерванных',
                'audit': 'Аудит',
                'optimization': 'Оптимизация',
                'audit+optimization': 'Аудит + оптимизация',
                'norm_verify': 'Верификация норм',
                // Legacy
                'standard': 'Аудит',
                'pro': 'Аудит',
                'standard+optimization': 'Аудит + оптимизация',
                'pro+optimization': 'Аудит + оптимизация',
            };
            return labels[action] || action;
        }

        // ЧЧ:ММ из epoch-секунд; если не сегодня — с датой "ДД.ММ ЧЧ:ММ"
        function formatQueueClock(ts) {
            if (!ts) return '';
            const d = new Date(ts * 1000);
            const pad = n => String(n).padStart(2, '0');
            const hhmm = pad(d.getHours()) + ':' + pad(d.getMinutes());
            const now = new Date();
            const sameDay = d.getFullYear() === now.getFullYear()
                && d.getMonth() === now.getMonth() && d.getDate() === now.getDate();
            return sameDay ? hhmm : pad(d.getDate()) + '.' + pad(d.getMonth() + 1) + ' ' + hhmm;
        }

        // Тайминги элемента очереди: "10:42 → 11:15 · 33м" (обновляется при polling'е)
        function queueItemTiming(item) {
            if (!item || !item.started_at) return '';
            const start = formatQueueClock(item.started_at);
            if (item.status === 'running') {
                const elapsed = Math.max(0, Date.now() / 1000 - item.started_at);
                return start + ' → … · ' + formatEta(elapsed);
            }
            if (!item.finished_at) return start;
            const end = formatQueueClock(item.finished_at);
            const dur = formatEta(Math.max(0, item.finished_at - item.started_at));
            return start + ' → ' + end + ' · ' + dur;
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
                // Показываем очередь даже когда не running (история, прерванная)
                batchQueue.value = data.queue || null;
            } catch (e) { /* ignore */ }
        }

        async function clearQueueHistory() {
            if (!confirm('Очистить историю очереди?')) return;
            try {
                const resp = await fetch('/api/audit/batch/history', { method: 'DELETE' });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || `Ошибка: ${resp.status}`);
                }
                batchQueue.value = null;
                batchRunning.value = false;
            } catch (e) { alert(e.message); }
        }

        async function resumeBatchQueue() {
            try {
                const resp = await fetch('/api/audit/batch/resume', { method: 'POST' });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || `Ошибка: ${resp.status}`);
                }
                const data = await resp.json();
                batchQueue.value = data.queue;
                batchRunning.value = true;
            } catch (e) { alert(e.message); }
        }

        async function removeFromQueue(projectId) {
            try {
                const resp = await fetch('/api/audit/batch/remove', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        project_id: projectId,
                        version_id: activeVersionId.value || null,
                    }),
                });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || `Ошибка: ${resp.status}`);
                }
                const data = await resp.json();
                batchQueue.value = data.queue;
            } catch (e) { alert(e.message); }
        }

        // Видимые элементы очереди: скрытые (hidden) не рисуем, но помним
        // исходный индекс — drag&drop/reorder работают по позиции в полном
        // списке, который на бэкенде не сокращается (worker идёт по индексу).
        const visibleQueueItems = computed(() => {
            const items = (batchQueue.value && batchQueue.value.items) || [];
            return items
                .map((it, i) => ({ ...it, _idx: i }))
                .filter(it => !it.hidden);
        });

        const finishedQueueCount = computed(() => {
            const done = ['completed', 'failed', 'skipped', 'cancelled'];
            return visibleQueueItems.value.filter(it => done.includes(it.status)).length;
        });

        async function hideFinishedQueueItems() {
            try {
                const resp = await fetch('/api/audit/batch/hide-finished', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({}),
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
                    const text = await resp.text();
                    let detail = `Ошибка: ${resp.status}`;
                    try { detail = JSON.parse(text).detail || detail; } catch {}
                    throw new Error(detail);
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
        const retryDialog = ref({ show: false, projectId: '', stage: '', stageLabel: '', mode: 'retry' });

        async function apiGet(path) {
            const resp = await fetch(`/api${path}`);
            if (!resp.ok) {
                const err = await resp.json().catch(() => ({}));
                throw new Error(err.detail || `API error: ${resp.status}`);
            }
            return resp.json();
        }

        async function apiPost(path, body, postOpts) {
            postOpts = postOpts || {};
            const opts = { method: 'POST' };
            if (body !== undefined) {
                opts.headers = { 'Content-Type': 'application/json' };
                opts.body = JSON.stringify(body);
            }
            const url = _apiUrl(path, postOpts.withVersion);
            const resp = await fetch(url, opts);
            if (!resp.ok) {
                const err = await resp.json().catch(() => ({}));
                throw new Error(err.detail || `API error: ${resp.status}`);
            }
            return resp.json();
        }

        async function apiPatch(path, body, patchOpts) {
            patchOpts = patchOpts || {};
            const opts = { method: 'PATCH' };
            if (body !== undefined) {
                opts.headers = { 'Content-Type': 'application/json' };
                opts.body = JSON.stringify(body);
            }
            const url = _apiUrl(path, patchOpts.withVersion);
            const resp = await fetch(url, opts);
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

        /**
         * Сделать сообщение об ошибке audit/optimization read-friendly.
         *
         * Если backend ответил 409 «Запуск аудита... legacy runner», вместо
         * сырого длинного текста показываем понятную фразу. Полный detail
         * пишем в console для отладки.
         *
         * @param {Error} e
         */
        function _friendlyAuditError(e) {
            const msg = e && e.message ? String(e.message) : 'Ошибка';
            // По тексту определяем, это ли наш safety-gate 409.
            if (/legacy runner/i.test(msg)) {
                console.warn('[audit] safety-gate 409:', msg);
                alert(
                    'Запуск аудита этой версии временно недоступен на legacy ' +
                    'runner. Версия и файлы сохранены, контроль ранее ' +
                    'согласованных замечаний доступен.'
                );
                return;
            }
            alert(msg);
        }

        async function startPrepare(projectId) {
            try {
                auditRunning.value = true;
                await apiPost(`/audit/${encodeURIComponent(projectId)}/prepare`);
                _afterAuditStart(projectId);
            } catch (e) { _friendlyAuditError(e); auditRunning.value = false; }
        }

        async function startMainAudit(projectId) {
            try {
                auditRunning.value = true;
                await apiPost(`/audit/${encodeURIComponent(projectId)}/main-audit`);
                _afterAuditStart(projectId);
            } catch (e) { _friendlyAuditError(e); auditRunning.value = false; }
        }

        async function startAudit(projectId) {
            // Показать модальник с выбором моделей перед запуском
            openModelConfig(projectId);
        }

        async function startAuditDirect(projectId) {
            return _withSubmitLock(`start:${projectId}`, async () => {
                try {
                    auditRunning.value = true;
                    await apiPost(`/audit/${encodeURIComponent(projectId)}/full-audit`);
                    _afterAuditStart(projectId);
                } catch (e) { _friendlyAuditError(e); auditRunning.value = false; }
            });
        }

        // Legacy aliases
        const startStandardAudit = startAudit;
        const startProAudit = startAudit;

        async function startNormVerify(projectId) {
            try {
                auditRunning.value = true;
                await apiPost(`/audit/${encodeURIComponent(projectId)}/verify-norms`);
                _afterAuditStart(projectId);
            } catch (e) { _friendlyAuditError(e); auditRunning.value = false; }
        }

        async function resumePipeline(projectId) {
            return _withSubmitLock(`resume:${projectId}`, async () => {
                try {
                    auditRunning.value = true;
                    await apiPost(`/audit/${encodeURIComponent(projectId)}/resume`);
                    _afterAuditStart(projectId);
                } catch (e) { _friendlyAuditError(e); auditRunning.value = false; }
            });
        }

        async function resumeToQueue(projectId) {
            try {
                const resp = await fetch('/api/audit/batch/add-resume', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        project_id: projectId,
                        version_id: activeVersionId.value || null,
                    }),
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
            'block_context': 'block_context',
            'gemma_enrichment': 'gemma_enrichment',
            'text_analysis': 'text_analysis',
            'blocks_analysis': 'block_analysis',
            'findings': 'findings_merge',
            'findings_critic': 'findings_critic',
            'findings_corrector': 'findings_corrector',
            'norms_verified': 'norm_verify',
            'optimization': 'optimization',
            'optimization_critic': 'optimization_critic',
            'optimization_corrector': 'optimization_corrector',
            'debt_control': 'debt_control',
            'decision_carryover': 'decision_carryover',
        };

        const stageLabelMap = {
            'prepare': 'Кроп блоков',
            'block_context': BLOCK_CONTEXT_STAGE_UI_LABEL,
            'gemma_enrichment': BLOCK_CONTEXT_STAGE_UI_LABEL,
            'text_analysis': 'Анализ текста',
            'block_analysis': 'Анализ блоков',
            'findings_merge': 'Свод замечаний',
            'findings_critic': 'Верификатор',
            'findings_review': 'Верификатор',
            'findings_corrector': 'Верификатор',
            'norm_verify': 'Верификация норм',
            'optimization': 'Оптимизация',
            'optimization_critic': 'Critic оптимизации',
            'optimization_corrector': 'Corrector оптимизации',
            'debt_control': 'Контроль долгов',
            'decision_carryover': 'Перенос вердиктов',
        };

        // Короткие памятки по фактическому алгоритму карточек пайплайна.
        const activeStageAlgorithmKey = ref(null);
        const STAGE_ALGORITHMS = Object.freeze({
            block_context: {
                title: 'Векторные графы блоков',
                subtitle: 'Подготавливает точный вход для Stage 01.',
                steps: [
                    { text: 'PDF-вектор · Vectograph · PNG' },
                    { text: 'Роутер выбирает лучший источник' },
                    { text: 'Собирает текст, связи и геометрию' },
                    { text: 'Единый контекст + метка источника', tone: 'result' },
                ],
            },
            text_analysis: {
                title: '02 Анализ текста',
                subtitle: 'Ищет ошибки в текстовой части проекта.',
                steps: [
                    { text: 'Markdown + векторный текст PDF' },
                    { text: 'Чек-лист для раздела проекта' },
                    { text: 'Выбранная модель ищет несоответствия' },
                    { text: 'Текстовые замечания', tone: 'result' },
                ],
            },
            findings: {
                title: '03 Свод замечаний',
                subtitle: 'Собирает общий список без потери авторства.',
                steps: [
                    { text: '01 Блоки + 02 Текст' },
                    { text: 'Смысловое сопоставление' },
                    { text: 'Похожие замечания объединяются' },
                    { text: 'Итоговый список + бейджи источников', tone: 'result' },
                ],
            },
            findings_critic: {
                title: 'Верификатор',
                subtitle: 'Отсеивает фантомы и правит слабые формулировки.',
                steps: [
                    { text: 'Итоговые замечания' },
                    { text: 'Проверка блоков, листов и доказательств' },
                    { text: 'Сомнительное уточняется или отклоняется' },
                    { text: 'Проверенные замечания', tone: 'result' },
                ],
            },
            norms_verified: {
                title: '04 Верификация норм',
                subtitle: 'Проверяет нормативные ссылки и цитаты.',
                steps: [
                    { text: 'Замечание + ссылка на норму' },
                    { text: 'Поиск пункта в базе норм' },
                    { text: 'Сверка смысла и точной цитаты' },
                    { text: 'Подтверждённая или исправленная ссылка', tone: 'result' },
                ],
            },
            optimization_critic: {
                title: 'Critic / Fix',
                subtitle: 'Проверяет качество идей оптимизации.',
                steps: [
                    { text: 'Предложения по оптимизации' },
                    { text: 'Critic ищет слабые и рискованные идеи' },
                    { text: 'Fix уточняет расчёт и формулировку' },
                    { text: 'Проверенные варианты', tone: 'result' },
                ],
            },
            debt_control: {
                title: 'Контроль долгов',
                subtitle: 'Не даёт потерять ранее согласованные замечания.',
                steps: [
                    { text: 'Согласованные замечания прошлой версии' },
                    { text: 'Сопоставление с новым аудитом' },
                    { text: 'Поиск пропавших позиций' },
                    { text: 'Список незакрытых долгов', tone: 'result' },
                ],
            },
            decision_carryover: {
                title: 'Перенос вердиктов',
                subtitle: 'Восстанавливает решения эксперта в новой версии.',
                steps: [
                    { text: 'Вердикты эксперта из прошлой версии' },
                    { text: 'Точное и смысловое сопоставление' },
                    { text: 'Неоднозначное остаётся на проверку' },
                    { text: 'Решения перенесены без подмены', tone: 'result' },
                ],
            },
        });

        function stageModelDisplayName(modelId) {
            const id = String(modelId || '');
            if (id === 'codex/gpt-5.6-sol') return 'Codex GPT-5.6 Sol';
            if (id.startsWith('codex/')) return `Codex ${id.slice(6).toUpperCase()}`;
            if (id === 'openai/gpt-5.4') return 'GPT-5.4 (OpenRouter)';
            if (id === 'claude-opus-5') return 'Claude Opus 5';
            if (id === 'claude-sonnet-5') return 'Claude Sonnet 5';
            const available = availableModels.value.find(model => model.id === id);
            if (available?.label && available.provider !== 'codex_cli') {
                return available.label.replace(' (CLI)', '');
            }
            return id || 'не задан';
        }

        function blockAnalysisAlgorithm() {
            const usageModel = String(stageTokens('blocks_analysis')?.model || '');
            const configuredModel = String(stageModelConfig.value?.block_batch || '');
            const model = configuredModel || usageModel || 'openai/gpt-5.4';
            const base = {
                title: '01 Анализ блоков',
                subtitle: 'Каждая модель получает одинаковые изображение и контекст.',
            };
            if (model.includes('ensemble/gpt-codex')) {
                const details = stageEnsembleDetails.value?.block_batch || {};
                const parallelModels = details.parallel_models || ['openai/gpt-5.4', 'codex/gpt-5.4'];
                const judge = stageModelDisplayName(details.judge_model || 'codex/gpt-5.4');
                const verifier = stageModelDisplayName(
                    details.final_verifier_model || stageModelConfig.value?.findings_critic
                );
                const modelBadge = (m) => {
                    const s = String(m || '').toLowerCase();
                    if (s.startsWith('openai') || (s.includes('gpt') && !s.includes('codex'))) return 'GPT';
                    if (s.includes('codex')) return 'Codex';
                    if (s.includes('claude') || s.includes('opus') || s.includes('sonnet')) return 'Claude';
                    return 'LLM';
                };
                const branches = parallelModels.map((m) => ({
                    label: modelBadge(m),
                    text: `${stageModelDisplayName(m)}: независимые замечания`,
                }));
                return {
                    ...base,
                    note: `Модели не видят ответы друг друга. После 03 Свода итог дополнительно проверяет ${verifier}.`,
                    steps: [
                        { text: 'Изображение + контекст блока' },
                        { type: 'split', branches },
                        { text: `Судья: ${judge} сравнивает результаты`, tone: 'judge' },
                        { text: 'Совпадения · расширения · новые · спорные' },
                        { text: `${judge}: gap-search пропущенных проблем` },
                        { text: `Замечания + бейджи ${[...new Set(branches.map((b) => b.label))].join(' / ')}`, tone: 'result' },
                    ],
                };
            }
            let detector = 'GPT-5.4';
            let badge = 'GPT';
            if (model.includes('codex')) {
                detector = 'Codex';
                badge = 'Codex';
            } else if (model.includes('claude') || model.includes('opus') || model.includes('sonnet')) {
                detector = 'Claude';
                badge = 'Claude';
            }
            return {
                ...base,
                steps: [
                    { text: 'Изображение + контекст блока' },
                    { text: `${detector} ищет замечания по чек-листу` },
                    { text: `Каждое замечание получает бейдж ${badge}` },
                    { text: 'Далее 03 Свод: объединение с текстом', tone: 'result' },
                ],
            };
        }

        function optimizationAlgorithm() {
            const configuredModel = String(stageModelConfig.value?.optimization || '');
            if (configuredModel.includes('ensemble/claude-codex-opt')) {
                const details = stageEnsembleDetails.value?.optimization || {};
                const parallelModels = details.parallel_models || [
                    'claude-opus-5', 'codex/gpt-5.6-sol',
                ];
                const judge = stageModelDisplayName(
                    details.judge_model || stageModelConfig.value?.optimization_critic
                );
                const fixer = stageModelDisplayName(
                    details.fix_model || stageModelConfig.value?.optimization_corrector
                );
                const effort = details.codex_reasoning_effort
                    ? ` / ${details.codex_reasoning_effort}`
                    : '';
                return {
                    title: '05 Оптимизация',
                    subtitle: 'Два независимых анализа запускаются параллельно.',
                    note: 'На этапе объединения модель не голосует: сильные смысловые дубли удаляются детерминированно. Решение о качестве принимает следующий Critic.',
                    steps: [
                        { text: 'Один снимок проекта + графические блоки' },
                        { type: 'split', branches: [
                            { label: 'Claude', text: `${stageModelDisplayName(parallelModels[0])}: полный контекст` },
                            { label: 'Codex', text: `${stageModelDisplayName(parallelModels[1])}${effort}: визуальный анализ` },
                        ] },
                        { text: 'Объединение + удаление сильных дублей' },
                        { text: `Судья C OPT Critic: ${judge}`, tone: 'judge' },
                        { text: `Исправление F OPT Fix: ${fixer}` },
                        { text: 'Проверенные предложения по оптимизации', tone: 'result' },
                    ],
                };
            }
            return {
                title: '05 Оптимизация',
                subtitle: 'Ищет варианты удешевления и упрощения.',
                steps: [
                    { text: 'Проект + найденные замечания' },
                    { text: 'Выбранная модель ищет оптимизации' },
                    { text: 'C OPT Critic проверяет эффект и риск', tone: 'judge' },
                    { text: 'Проверенные предложения по оптимизации', tone: 'result' },
                ],
            };
        }

        const activeStageAlgorithm = computed(() => {
            if (activeStageAlgorithmKey.value === 'blocks_analysis') {
                return blockAnalysisAlgorithm();
            }
            if (activeStageAlgorithmKey.value === 'optimization') {
                return optimizationAlgorithm();
            }
            return STAGE_ALGORITHMS[activeStageAlgorithmKey.value] || null;
        });

        function openStageAlgorithm(stageKey) {
            activeStageAlgorithmKey.value = stageKey;
            if (['blocks_analysis', 'optimization'].includes(stageKey)
                && Object.keys(stageModelConfig.value || {}).length === 0) {
                loadStageModels();
            }
        }

        function closeStageAlgorithm() {
            activeStageAlgorithmKey.value = null;
        }

        function canStartFrom(pipelineKey) {
            if (!currentProject.value) return false;
            if (isProjectRunning(currentProject.value.project_id)) return false;
            const status = currentProject.value.pipeline?.[pipelineKey];
            const baseAllowed = status === 'done' || status === 'error' || status === 'skipped' || status === 'pending' || status === 'partial' || status === 'interrupted';
            if (!baseAllowed) return false;

            const pipeline = currentProject.value.pipeline || {};
            const ready = (key) => pipeline[key] === 'done' || pipeline[key] === 'partial';
            const gemmaReady = () => ready('block_context') || ready('gemma_enrichment') || pipeline['gemma_enrichment'] === 'migration_required';
            // Старые/частично упавшие V2-прогоны могут иметь готовые 02-блоки,
            // но не иметь статуса подготовки контекста в latest. Backend восстановит prereq-файлы.
            const gemmaOk = () => gemmaReady() || ready('blocks_analysis');
            if (pipelineKey === 'block_context' || pipelineKey === 'gemma_enrichment') {
                return ready('crop_blocks');
            }
            if (pipelineKey === 'blocks_analysis') {
                return ready('crop_blocks') || gemmaOk();
            }
            if (pipelineKey === 'text_analysis') {
                return gemmaOk() && ready('blocks_analysis');
            }
            if ([
                'findings', 'findings_critic', 'findings_corrector',
                'norms_verified', 'optimization', 'optimization_critic',
                'optimization_corrector', 'debt_control', 'decision_carryover', 'excel',
            ].includes(pipelineKey)) {
                return gemmaOk() && ready('text_analysis') && ready('blocks_analysis');
            }
            return true;
        }

        function canRetryStage(stage) {
            if (!currentProject.value) return false;
            if (isProjectRunning(currentProject.value.project_id)) return false;
            const pipeline = currentProject.value.pipeline || {};
            const ready = (key) => pipeline[key] === 'done' || pipeline[key] === 'partial';
            const gemmaReady = () => ready('block_context') || ready('gemma_enrichment') || pipeline['gemma_enrichment'] === 'migration_required';
            const gemmaOk = () => gemmaReady() || ready('blocks_analysis');
            if (stage === 'block_context' || stage === 'gemma_enrichment') {
                return ready('crop_blocks');
            }
            if (stage === 'block_analysis') {
                return ready('crop_blocks') || gemmaOk();
            }
            if (stage === 'text_analysis') {
                return gemmaOk() && ready('blocks_analysis');
            }
            if ([
                'findings_merge', 'findings_critic', 'findings_review',
                'findings_corrector', 'norm_verify', 'optimization',
                'optimization_critic', 'optimization_corrector',
                'debt_control', 'decision_carryover', 'excel',
            ].includes(stage)) {
                return gemmaOk() && ready('text_analysis') && ready('blocks_analysis');
            }
            return true;
        }

        async function startFromStage(projectId, pipelineKey) {
            const stage = pipelineToStage[pipelineKey];
            if (!stage) return;
            const label = stageLabelMap[stage] || stage;
            retryDialog.value = {
                show: true,
                projectId,
                stage,
                stageLabel: label,
                mode: 'resume', // запустить этап + все последующие
            };
        }

        const resumeInfo = ref(null);

        async function loadResumeInfo(projectId) {
            try {
                const resp = await fetch(`/api/audit/${encodeURIComponent(projectId)}/resume-info`);
                if (resp.ok) {
                    resumeInfo.value = await resp.json();
                }
            } catch (e) { resumeInfo.value = null; }
        }

        async function cancelAudit(projectId) {
            try {
                await fetch(`/api/audit/${encodeURIComponent(projectId)}/cancel`, { method: 'DELETE' });
                auditRunning.value = false;
                // Оптимистично снимаем running-метку, чтобы кнопка сразу стала
                // «Запустить аудит», не дожидаясь следующего polling.
                if (liveStatus.value.running) delete liveStatus.value.running[projectId];
                // Обновляем очередь, чтобы «остановлен» отобразился и на
                // странице «Очередь» (item больше не «Выполняется»).
                refreshBatchQueue();
            } catch (e) { alert(e.message); }
        }

        async function cleanProject(projectId) {
            const name = currentProject.value?.name || projectId;
            // Очистка затрагивает только активную версию (её _output/),
            // остальные версии проекта не трогаются.
            const verEntry = activeVersionEntry.value;
            const verLabel = verEntry
                ? (verEntry.label || verEntry.version_id)
                : (activeVersionId.value || '');
            const verLine = verLabel ? ` (версия ${verLabel})` : '';
            if (!confirm(`Очистить результаты проекта "${name}"${verLine}?\n\nБудут удалены данные ТОЛЬКО этой версии:\n- Все блоки и нарезки\n- Все JSON-этапы (00-03)\n- Батчи и логи\n- Отчёты\n\nДругие версии, PDF и MD файлы сохраняются. Для projects_v2 backend сначала создаст backup.`)) {
                return;
            }
            try {
                // _apiUrl автоматически подмешивает ?version_id из activeVersionId,
                // чтобы бэкенд чистил именно активную версию.
                const cleanUrl = new URL(_apiUrl(`/projects/${encodeURIComponent(projectId)}/clean`), window.location.origin);
                cleanUrl.searchParams.set('_confirmed', 'true');
                const resp = await fetch(cleanUrl.pathname + cleanUrl.search, { method: 'DELETE' });
                const data = await resp.json();
                if (!resp.ok) {
                    alert(data.detail || 'Ошибка очистки');
                    return;
                }
                const backupLine = data.backup_id ? `\nBackup: ${data.backup_id}` : '';
                alert(`Очищено: ${data.deleted_files} файлов, ${data.freed_mb} MB освобождено${backupLine}`);
                if (data.backup_id && confirm(`Backup создан:\n${data.backup_id}\n\nВосстановить очищенные данные из backup сейчас?`)) {
                    await restoreCleanBackup(projectId, data.backup_id);
                    return;
                }
                // Обновляем данные проекта
                await refreshProjects();
                if (currentProject.value && currentProject.value.project_id === projectId) {
                    const updated = await apiGet(`/projects/${encodeURIComponent(projectId)}`);
                    if (updated) currentProject.value = updated;
                }
            } catch (e) { alert(e.message); }
        }

        async function restoreCleanBackup(projectId, backupId) {
            try {
                const resp = await fetch(`/api/projects/${encodeURIComponent(projectId)}/restore-clean`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        backup_id: backupId,
                        version_id: activeVersionId.value || null,
                    }),
                });
                const data = await resp.json();
                if (!resp.ok) {
                    alert(data.detail || 'Ошибка восстановления backup');
                    return;
                }
                const preRestore = data.pre_restore_backup_id
                    ? `\nТекущее состояние перед restore сохранено: ${data.pre_restore_backup_id}`
                    : '';
                alert(`Восстановлено из backup: ${data.backup_id}${preRestore}`);
                await refreshProjects();
                if (currentProject.value && currentProject.value.project_id === projectId) {
                    const updated = await apiGet(`/projects/${encodeURIComponent(projectId)}`);
                    if (updated) currentProject.value = updated;
                }
            } catch (e) { alert(e.message); }
        }


        function retryStage(projectId, stage) {
            const labels = {
                'crop_blocks': 'Кроп блоков', 'block_context': BLOCK_CONTEXT_STAGE_UI_LABEL,
                'gemma_enrichment': BLOCK_CONTEXT_STAGE_UI_LABEL,
                'text_analysis': 'Анализ текста',
                'block_analysis': 'Анализ блоков', 'findings_merge': 'Свод замечаний',
                'findings_critic': 'Верификатор', 'findings_review': 'Верификатор',
                'findings_corrector': 'Верификатор',
                'norm_verify': 'Верификация норм', 'optimization': 'Оптимизация',
                'optimization_critic': 'Critic оптимизации', 'optimization_corrector': 'Corrector оптимизации',
                'decision_carryover': 'Перенос вердиктов',
            };
            retryDialog.value = {
                show: true,
                projectId,
                stage,
                stageLabel: labels[stage] || stage,
                mode: 'retry', // только этот один этап
            };
        }

        async function _executeRetryStage(projectId, stage) {
            return _withSubmitLock(`retry:${projectId}:${stage}`, async () => {
                try {
                    auditRunning.value = true;
                    if (stage === 'optimization') {
                        await apiPost(`/optimization/${encodeURIComponent(projectId)}/run`);
                    } else {
                        await apiPost(`/audit/${encodeURIComponent(projectId)}/retry/${stage}`);
                    }
                    _afterAuditStart(projectId);
                } catch (e) { _friendlyAuditError(e); auditRunning.value = false; }
            });
        }

        async function retryStageToQueue() {
            const { projectId, stage, mode } = retryDialog.value;
            // Submit-lock на эту пару — двойной клик «Запустить» в retry-dialog
            // не должен порождать второй request.
            if (_isSubmitLocked(`retry-queue:${projectId}:${stage}`)) {
                console.warn('[submit-lock] retry-queue duplicate ignored');
                return;
            }
            return _withSubmitLock(`retry-queue:${projectId}:${stage}`, async () => {
            retryDialog.value.show = false;
            try {
                let resp;
                if (mode === 'resume') {
                    resp = await fetch(`/api/audit/batch/add-retry`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            project_id: projectId,
                            stage: stage,
                            version_id: activeVersionId.value || null,
                        }),
                    });
                } else {
                    resp = await fetch(_apiUrl(`/audit/${encodeURIComponent(projectId)}/retry/${stage}`), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                    });
                }
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || `API error: ${resp.status}`);
                }
                const data = await resp.json();
                if (data.queue) {
                    batchQueue.value = data.queue;
                    batchRunning.value = true;
                }
            } catch (e) { alert(e.message); }
            }); // end _withSubmitLock
        }

        async function skipStage(projectId, stage) {
            if (!confirm('Пропустить этап? Это может привести к неполному аудиту.')) return;
            try {
                await apiPost(`/audit/${encodeURIComponent(projectId)}/skip/${stage}`);
                await refreshProjects();
                if (currentProject.value && currentProject.value.project_id === projectId) {
                    const data = await apiGet(`/projects/${encodeURIComponent(projectId)}`);
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

        // ─── Objects (строительные объекты) ───
        const OBJECT_STORAGE_KEY = 'currentObjectId';
        function readStoredObjectId() {
            // sessionStorage изолирован по вкладкам и переживает reload. Старое
            // значение из localStorage читаем один раз для бесшовного перехода
            // с версии, где выбор ошибочно был общим для всех вкладок origin.
            try {
                const scoped = sessionStorage.getItem(OBJECT_STORAGE_KEY);
                if (scoped) return scoped;
            } catch (e) {}
            try { return localStorage.getItem(OBJECT_STORAGE_KEY) || null; } catch (e) { return null; }
        }
        function storeObjectId(id) {
            try {
                sessionStorage.setItem(OBJECT_STORAGE_KEY, id);
                // Миграционный ключ больше не должен связывать новые вкладки.
                try { localStorage.removeItem(OBJECT_STORAGE_KEY); } catch (e) {}
                return;
            } catch (e) {}
            // Редкий fallback для браузеров, где sessionStorage запрещён.
            try { localStorage.setItem(OBJECT_STORAGE_KEY, id); } catch (e) {}
        }
        const objectsList = ref([]);
        // Инициализируем из хранилища СИНХРОННО — чтобы уже самый первый
        // запрос (handleRoute до завершения loadObjects) нёс свой per-tab объект,
        // а не глобальный. loadObjects потом сверит id со списком с сервера и
        // откатит на current_id, если сохранённый объект больше не существует.
        const currentObjectId = ref(readStoredObjectId());
        const showObjectPicker = ref(false);
        const showAddObjectModal = ref(false);
        const newObjectName = ref('');

        // ─── Per-tab «текущий объект»: X-Object-Id на каждый /api/-запрос ───
        // «Текущий объект» на сервере больше НЕ глобальный на всех пользователей:
        // фронт сообщает выбранный объект заголовком, и бэкенд резолвит СВОЙ
        // объект per-request (CurrentObjectMiddleware). Один глобальный
        // перехватчик fetch покрывает и обёртки api()/apiPost(), и точечные
        // fetch (удаление версии, отмена аудита и т.д.) — иначе они резолвили бы
        // проект в ЧУЖОМ (глобальном) объекте, если сосед переключил объект.
        // Только same-origin `/api/`-запросы; кропы/LLM — абсолютные URL чужих
        // хостов — не трогаем. Fail-open: любая ошибка не ломает запрос.
        if (!window.__objHeaderPatched) {
            window.__objHeaderPatched = true;
            const _origFetch = window.fetch.bind(window);
            window.fetch = function (input, init) {
                try {
                    const oid = currentObjectId.value;
                    const url = typeof input === 'string'
                        ? input : (input && input.url) || '';
                    if (oid && url.startsWith('/api/')) {
                        init = init || {};
                        const h = new Headers(
                            init.headers
                            || (typeof input !== 'string' && input.headers)
                            || {});
                        if (!h.has('X-Object-Id')) h.set('X-Object-Id', oid);
                        init.headers = h;
                    }
                } catch (e) { /* fail-open */ }
                return _origFetch(input, init);
            };
        }

        // ─── Панели шапки (объект / расходы API / аккаунт LLM) ───
        // Одновременно открыта максимум одна: открытие любой закрывает остальные,
        // а клик вне (по любому другому элементу) закрывает все — см. onMounted.
        function toggleHeaderPopover(which) {
            const target = which === 'object'  ? showObjectPicker
                         : which === 'paid'    ? showPaidCost
                         : which === 'account' ? showAccountInfo
                         : null;
            if (!target) return;
            const willOpen = !target.value;
            showObjectPicker.value = false;
            showPaidCost.value = false;
            showAccountInfo.value = false;
            if (willOpen) target.value = true;
        }

        function closeHeaderPopovers() {
            showObjectPicker.value = false;
            showPaidCost.value = false;
            showAccountInfo.value = false;
        }

        async function loadObjects() {
            try {
                const data = await api('/objects');
                objectsList.value = data.objects || [];
                // Выбор объекта — per-tab, а не глобальный: при загрузке
                // предпочитаем СВОЙ последний объект из sessionStorage (если он
                // ещё существует), а не глобальный current_id с сервера, который
                // мог переключить другой инженер. Свежая вкладка без сохранённого
                // выбора берёт серверный current_id как дефолт.
                const saved = readStoredObjectId();
                const savedValid = saved && objectsList.value.some(o => o.id === saved);
                currentObjectId.value = savedValid ? saved : data.current_id;
                if (currentObjectId.value) storeObjectId(currentObjectId.value);
                // Показать имя выбранного объекта в шапке (не «Объект»-плейсхолдер).
                const cur = objectsList.value.find(o => o.id === currentObjectId.value);
                if (cur) objectName.value = cur.name;
            } catch (e) {
                console.error('Failed to load objects:', e);
            }
        }

        async function switchObject(id) {
            try {
                // Запоминаем выбор per-tab (sessionStorage) — переживёт reload и не
                // будет перебит глобальным current_id соседа. /objects/switch
                // по-прежнему зовём: он обновляет серверный дефолт для свежих
                // сессий, но per-tab корректность обеспечивает заголовок.
                currentObjectId.value = id;
                storeObjectId(id);
                await apiPost('/objects/switch', { id });
                const obj = objectsList.value.find(o => o.id === id);
                if (obj) objectName.value = obj.name;
                showObjectPicker.value = false;
                await Promise.all([refreshProjects(), loadProjectGroups()]);
                if (currentView.value === 'stage-comparison') {
                    scLoadObjects();
                }
                // База знаний фильтруется по выбранному объекту — перезагружаем при смене.
                if (currentView.value === 'knowledge-base') {
                    loadKBStats();
                    if (kbTab.value !== 'missing_norms') loadKnowledgeBase();
                }
            } catch (e) {
                console.error('Failed to switch object:', e);
            }
        }

        async function addNewObject() {
            const name = newObjectName.value.trim();
            if (!name) return;
            try {
                const data = await apiPost('/objects', { name });
                objectsList.value.push(data.object);
                newObjectName.value = '';
                showAddObjectModal.value = false;
                // Переключаемся на новый объект
                await switchObject(data.object.id);
            } catch (e) {
                console.error('Failed to add object:', e);
            }
        }

        // ─── Dashboard Aggregated Stats ───
        const auditedProjectsCount = computed(() => {
            return projects.value.filter(p => p.findings_count > 0).length;
        });

        const totalFindings = computed(() => {
            return projects.value.reduce((sum, p) => sum + (p.findings_count || 0), 0);
        });

        const totalBySeverity = computed(() => {
            const totals = {};
            for (const p of projects.value) {
                if (!p.findings_by_severity) continue;
                for (const [sev, count] of Object.entries(p.findings_by_severity)) {
                    totals[sev] = (totals[sev] || 0) + count;
                }
            }
            return totals;
        });

        function sevPercent(sev) {
            const total = totalFindings.value;
            if (!total) return 0;
            return Math.round(((totalBySeverity.value[sev] || 0) / total) * 100);
        }

        function sectionFindingsCount(code) {
            return projects.value
                .filter(p => p.section === code)
                .reduce((sum, p) => sum + (p.findings_count || 0), 0);
        }

        // Сводка по разделу для «Главной». Все столбцы считают УНИКАЛЬНЫЕ
        // проекты (карточки-версии одного проекта = один проект) и смотрят на
        // ПОСЛЕДНЮЮ версию — постановка Андрея Ивановича 2026-08-19:
        //   notStarted  — «Не запускались на проверку»: у последней версии нет
        //                 результатов аудита (isProjectUnanalyzed); те же
        //                 проекты выделяет «Выделить необработанные»;
        //   noDecisions — «Нет решений эксперта»: всё, что эксперт не закрыл
        //                 полностью, включая ни разу не проверенные проекты
        //                 (total − expertChecked);
        //   expertChecked — «Проверено Экспертом»: эксперт закрыл ПОСЛЕДНЮЮ
        //                 версию (isProjectExpertResolved); проверенные старые
        //                 версии не считаются, пока не проверена последняя;
        //   total       — уникальных проектов в разделе;
        //   findings    — замечания последних версий.
        // Ключуется тем же группированием, что projectsBySection, чтобы цифры
        // совпадали с карточками раздела.
        const sectionStatsMap = computed(() => {
            const m = {};
            for (const [code, items] of projectsBySection.value) {
                const latest = latestProjectCards(items);
                let expertChecked = 0, notStarted = 0, findings = 0;
                for (const p of latest) {
                    if (isProjectExpertResolved(p)) expertChecked++;
                    if (isProjectUnanalyzed(p)) notStarted++;
                    findings += (p.findings_count || 0);
                }
                m[code] = {
                    total: latest.length,
                    notStarted,
                    noDecisions: latest.length - expertChecked,
                    expertChecked,
                    findings,
                };
            }
            return m;
        });

        // Итого по всем разделам — сумма каждого числового столбца для
        // строки «Итого» внизу таблицы «Разделы проекта».
        const sectionStatsTotals = computed(() => {
            const t = { notStarted: 0, noDecisions: 0, expertChecked: 0, total: 0, findings: 0 };
            for (const code in sectionStatsMap.value) {
                const s = sectionStatsMap.value[code];
                t.notStarted += s.notStarted;
                t.noDecisions += s.noDecisions;
                t.expertChecked += s.expertChecked;
                t.total += s.total;
                t.findings += s.findings;
            }
            return t;
        });

        const filteredSectionProjects = computed(() => {
            if (!sidebarFilterSection.value) return [];
            return projects.value.filter(p => p.section === sidebarFilterSection.value);
        });

        // Есть ли в текущем разделе необработанные (без аудита) проекты —
        // критерий тот же, что у isProjectUnanalyzed: нет ни замечаний,
        // ни оптимизаций.
        const sectionHasUnanalyzed = computed(() => {
            const sec = sidebarFilterSection.value;
            if (!sec || sec === '__all__') return false;
            return projects.value.some(
                p => (p.section || 'OTHER') === sec && isProjectUnanalyzed(p)
            );
        });

        // Число «не проверенных» проектов текущего раздела — для надписи
        // «Не проверено (N)» и её скрытия, когда все проекты проверены.
        // Число в шапке раздела — по уникальным проектам (как и бейдж сайдбара).
        // `sectionUnreviewedPids` остаётся списком КАРТОЧЕК: флажок выделяет их
        // все, включая карточки-версии одного проекта.
        const sectionUnreviewedCount = computed(() => {
            const sec = sidebarFilterSection.value;
            if (!sec || sec === '__all__') return 0;
            return expertUncheckedCount(
                projects.value.filter(p => (p.section || 'OTHER') === sec));
        });

        const PROJECT_SCOPED_VIEWS = new Set([
            'project', 'blocks', 'log',
            'findings', 'optimization', 'discussions',
            'document', 'critic-v2-project',
        ]);
        const isProjectView = computed(() => PROJECT_SCOPED_VIEWS.has(currentView.value));

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
                    { code: 'EOM', name: 'Электроснабжение и электрооборудование', short_name: 'ЭОМ/ЭС', color: '#f39c12' },
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
            return 'EOM';
        }

        // ─── Группы проектов (папки внутри секции) ───
        const projectGroups = ref({});       // { section: [{id, name, order, project_ids}] }
        const showCreateGroup = ref(false);
        const newGroupName = ref('');
        const editingGroupId = ref(null);
        const editingGroupName = ref('');

        // Drag-and-drop для проектов и групп
        const dragProjectId = ref(null);
        const dragGroupId = ref(null);
        const dragOverGroupId = ref(null);

        async function loadProjectGroups() {
            try {
                const oid = currentObjectId.value;
                const qs = oid ? '?object_id=' + encodeURIComponent(oid) : '';
                const data = await api('/project-groups' + qs);
                projectGroups.value = data.groups || {};
            } catch (e) {
                console.error('Failed to load project groups:', e);
                // не сбрасывать текущие группы при ошибке сети
            }
        }

        async function saveProjectGroups(section) {
            try {
                const oid = currentObjectId.value;
                await fetch('/api/project-groups/' + encodeURIComponent(section), {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ groups: projectGroups.value[section] || [], object_id: oid || null }),
                });
            } catch (e) {
                console.error('Ошибка сохранения групп:', e);
            }
        }

        function createGroup(section, name) {
            if (!name || !name.trim()) return;
            const groups = projectGroups.value[section] || [];
            const maxOrder = groups.reduce((m, g) => Math.max(m, g.order || 0), -1);
            groups.push({ id: 'g_' + Date.now(), name: name.trim(), order: maxOrder + 1, project_ids: [] });
            projectGroups.value[section] = groups;
            saveProjectGroups(section);
        }

        function renameGroup(section, groupId, name) {
            const groups = projectGroups.value[section] || [];
            const g = groups.find(x => x.id === groupId);
            if (g) { g.name = name.trim(); saveProjectGroups(section); }
            editingGroupId.value = null;
            editingGroupName.value = '';
        }

        function startRenameGroup(group) {
            editingGroupId.value = group.id;
            editingGroupName.value = group.name;
        }

        async function deleteProjectGroup(section, groupId) {
            const groups = projectGroups.value[section] || [];
            projectGroups.value[section] = groups.filter(g => g.id !== groupId);
            saveProjectGroups(section);
        }

        const groupedSectionProjects = computed(() => {
            const section = sidebarFilterSection.value;
            if (!section || section === '__all__') return [];

            const sectionProjects = projects.value.filter(p => p.section === section);
            const groups = (projectGroups.value[section] || []).slice().sort((a, b) => (a.order || 0) - (b.order || 0));

            // Если групп нет — одна виртуальная без заголовка
            if (groups.length === 0) {
                return [{ id: '__ungrouped__', name: '', order: 0, project_ids: [], projects: sortSectionProjects(sectionProjects), isVirtual: true, noHeader: true }];
            }

            const assignedIds = new Set(groups.flatMap(g => g.project_ids || []));
            const result = groups.map(g => ({
                ...g,
                projects: sortSectionProjects(
                    (g.project_ids || []).map(id => sectionProjects.find(p => p.project_id === id)).filter(Boolean)
                ),
                isVirtual: false,
            }));

            const ungrouped = sectionProjects.filter(p => !assignedIds.has(p.project_id));
            if (ungrouped.length > 0) {
                result.push({ id: '__ungrouped__', name: 'Без группы', order: 99999, project_ids: [], projects: sortSectionProjects(ungrouped), isVirtual: true });
            }

            return result;
        });

        // ─── Сводная оптимизация раздела ───────────────────────────────
        // Read-only слой над актуальными версиями всех проектов раздела:
        // спецификации, принятые оптимизации и доказательные межпроектные
        // сигналы. Данные загружаются при открытии отдельной страницы раздела.
        const sectionOptimizationLoading = ref(false);
        const sectionOptimizationError = ref('');
        const sectionOptimizationData = ref(null);
        const sectionOptimizationLoadedKey = ref('');
        const sectionOptimizationTab = ref('specifications');
        const sectionOptimizationSearch = ref('');
        const sectionOptimizationProjectFilter = ref('');
        const sectionOptimizationCollapsedProjects = ref({});
        const sectionOptimizationExpandedSignals = ref({});
        const sectionOptimizationPipelineActionLoading = ref(false);
        const sectionOptimizationPipelineActionError = ref('');
        const sectionOptimizationReplicationActionLoading = ref(false);
        let _sectionOptimizationLoadSeq = 0;
        let _sectionOptimizationPipelinePollSeq = 0;
        const _sectionOptimizationReplicationPollTokens = new Map();
        const _sectionOptimizationMemoryCache = new Map();

        const sectionOptimizationMeta = computed(() => sectionOptimizationData.value?.meta || {});
        const sectionOptimizationProjectOptions = computed(() => sectionOptimizationData.value?.projects || []);
        const sectionOptimizationPipeline = computed(() => sectionOptimizationData.value?.pipeline || {
            status: 'not_started', stages: [], snapshot_generated_at: null,
        });
        const sectionOptimizationPipelineRunning = computed(() => (
            ['queued', 'running'].includes(sectionOptimizationPipeline.value.status)
        ));
        const sectionOptimizationReplications = computed(() => sectionOptimizationData.value?.replications || []);
        const sectionOptimizationAgentAvailable = computed(() => (
            (sectionOptimizationData.value?.analysis_stages || []).some(stage => stage.key === 'agent')
        ));
        const sectionOptimizationGraphicsAgentAvailable = computed(() => (
            sectionOptimizationData.value?.capabilities?.targeted_graphics_agent === true
        ));
        const sectionOptimizationReplicationCandidates = computed(() => (
            (sectionOptimizationData.value?.signals || [])
                .filter(signal => signal.kind === 'replicate_accepted_optimization')
        ));
        const sectionOptimizationReplicationPendingCount = computed(() => (
            sectionOptimizationReplicationCandidates.value
                .filter(signal => sectionOptimizationReplicationNeedsAgent(signal.signal_id)).length
        ));
        const sectionOptimizationReplicationProgressLabel = computed(() => {
            const total = sectionOptimizationReplicationCandidates.value.length;
            const processes = sectionOptimizationReplicationCandidates.value
                .map(signal => sectionOptimizationReplicationFor(signal.signal_id))
                .filter(Boolean);
            const running = processes.filter(item => ['queued', 'running'].includes(item.status)).length;
            const prepared = processes.filter(sectionOptimizationReplicationComplete).length;
            if (!total) return 'Нет кандидатов на тиражирование';
            if (running) return `Готовится: ${running} · подготовлено: ${prepared} из ${total}`;
            if (!sectionOptimizationReplicationPendingCount.value) return `Все ${total} кандидатов подготовлены`;
            return `Подготовлено: ${prepared} из ${total}`;
        });

        const sectionOptimizationFilteredSpecifications = computed(() => {
            let rows = sectionOptimizationData.value?.specification_rows || [];
            const projectId = sectionOptimizationProjectFilter.value;
            if (projectId) rows = rows.filter(row => row.project_id === projectId);
            const query = sectionOptimizationSearch.value.trim().toLowerCase();
            if (query) {
                rows = rows.filter(row => [
                    row.project_name, row.project_id, row.sheet, row.sheet_name,
                    row.category, row.position, row.name, row.designation,
                    row.type_mark, row.code, row.manufacturer, row.unit,
                    row.quantity, row.mass, row.note,
                ].some(value => String(value || '').toLowerCase().includes(query)));
            }
            return rows;
        });

        const sectionOptimizationSpecificationGroups = computed(() => {
            const rowsByProject = new Map();
            for (const row of sectionOptimizationFilteredSpecifications.value) {
                const projectId = row?.project_id || '';
                if (!rowsByProject.has(projectId)) rowsByProject.set(projectId, []);
                rowsByProject.get(projectId).push(row);
            }
            const projectId = sectionOptimizationProjectFilter.value;
            const queryActive = Boolean(sectionOptimizationSearch.value.trim());
            return sectionOptimizationProjectOptions.value
                .filter(project => !projectId || project.project_id === projectId)
                .map(project => {
                    const rows = rowsByProject.get(project.project_id) || [];
                    return {
                        project,
                        rows,
                        hasSpecification: Number(project.specification_rows || 0) > 0,
                    };
                })
                .filter(group => !queryActive || group.rows.length || !group.hasSpecification);
        });

        const sectionOptimizationFilteredAccepted = computed(() => {
            let items = sectionOptimizationData.value?.accepted_optimizations || [];
            const projectId = sectionOptimizationProjectFilter.value;
            if (projectId) items = items.filter(item => item.project_id === projectId);
            const query = sectionOptimizationSearch.value.trim().toLowerCase();
            if (query) {
                items = items.filter(item => [
                    item.project_name, item.project_id, item.id, item.section,
                    item.current, item.proposed, item.type, item.norm,
                    ...(item.spec_items || []),
                ].some(value => String(value || '').toLowerCase().includes(query)));
            }
            return items;
        });

        const sectionOptimizationFilteredSignals = computed(() => {
            let signals = sectionOptimizationData.value?.signals || [];
            const projectId = sectionOptimizationProjectFilter.value;
            if (projectId) signals = signals.filter(signal => (signal.project_ids || []).includes(projectId));
            const query = sectionOptimizationSearch.value.trim().toLowerCase();
            if (query) {
                signals = signals.filter(signal => [
                    signal.title, signal.reason, signal.next_step,
                    signal.match_basis, signal.representative_proposal,
                    ...(signal.project_ids || []),
                ].some(value => String(value || '').toLowerCase().includes(query)));
            }
            return signals;
        });

        function sectionOptimizationPipelineStage(stageKey) {
            const storedStage = (sectionOptimizationPipeline.value.stages || []).find(stage => stage.key === stageKey) || {
                key: stageKey,
                status: 'pending',
                message: 'Ожидает запуска',
            };
            const total = sectionOptimizationReplicationCandidates.value.length;
            const processes = sectionOptimizationReplicationCandidates.value
                .map(signal => sectionOptimizationReplicationFor(signal.signal_id))
                .filter(Boolean);
            if (stageKey === 'graphics') {
                if (!total) return storedStage;
                const active = processes.filter(process => ['queued', 'running'].includes(process.status)
                    && ['queued', 'running'].includes(process.graphics_status)).length;
                const required = processes.filter(process => process.graphics_required).length;
                const completed = processes.filter(process => process.graphics_status === 'complete').length;
                const agentReady = processes.filter(process => process.agent_status === 'complete').length;
                if (active) {
                    return {
                        ...storedStage,
                        status: 'running',
                        message: `Vision-проверка: готово ${completed} из ${required}`,
                    };
                }
                if (agentReady === total && processes.every(sectionOptimizationReplicationComplete)) {
                    return {
                        ...storedStage,
                        status: 'done',
                        message: required
                            ? `Графически проверено: ${completed} проектных запросов`
                            : 'Графическая проверка не потребовалась',
                    };
                }
                if (required) {
                    return {
                        ...storedStage,
                        status: 'waiting',
                        message: `Проверено ${completed} из ${required} графических запросов`,
                    };
                }
                return storedStage;
            }
            if (stageKey !== 'agent' || !total) return storedStage;

            const active = processes.filter(process => ['queued', 'running'].includes(process.agent_status)).length;
            const completed = processes.filter(process => process.agent_status === 'complete').length;
            const failed = processes.filter(process => process.agent_status === 'failed').length;
            if (active) {
                return {
                    ...storedStage,
                    status: 'running',
                    message: `Анализирует кандидатов: готово ${completed} из ${total}`,
                };
            }
            if (completed === total) {
                return {
                    ...storedStage,
                    status: 'done',
                    message: `Заключения готовы: ${completed} из ${total}`,
                };
            }
            if (failed) {
                return {
                    ...storedStage,
                    status: 'waiting',
                    message: `Готово ${completed} из ${total}; с ошибкой: ${failed}. Можно повторить запуск`,
                };
            }
            if (completed) {
                return {
                    ...storedStage,
                    status: 'waiting',
                    message: `Готово ${completed} из ${total}; остальные ожидают запуска`,
                };
            }
            return storedStage;
        }

        function sectionOptimizationPipelineStatusLabel(status) {
            const labels = {
                not_started: 'Не запускался',
                queued: 'В очереди',
                running: 'Выполняется',
                ready_for_review: 'Кандидаты готовы к запуску умного агента',
                failed: 'Завершился с ошибкой',
                interrupted: 'Прерван',
            };
            return labels[status] || 'Неизвестный статус';
        }

        function sectionOptimizationPipelineStageMarker(stageKey, index) {
            const status = sectionOptimizationPipelineStage(stageKey).status;
            if (status === 'done') return '✓';
            if (status === 'running') return '…';
            if (status === 'failed' || status === 'interrupted') return '!';
            if (status === 'waiting') return '•';
            return index + 1;
        }

        function sectionOptimizationPipelineUrl(sectionCode, suffix = '') {
            const objectId = currentObjectId.value || '';
            const query = objectId ? '?object_id=' + encodeURIComponent(objectId) : '';
            return '/optimization/section/' + encodeURIComponent(sectionCode) + '/pipeline' + suffix + query;
        }

        function sectionOptimizationReplicationsUrl(sectionCode, suffix = '') {
            const objectId = currentObjectId.value || '';
            const query = objectId ? '?object_id=' + encodeURIComponent(objectId) : '';
            return '/optimization/section/' + encodeURIComponent(sectionCode) + '/replications' + suffix + query;
        }

        function sectionOptimizationReplicationFor(signalId) {
            return sectionOptimizationReplications.value.find(item => item.signal_id === signalId) || null;
        }

        function sectionOptimizationReplicationComplete(process) {
            if (!process || process.agent_status !== 'complete') return false;
            // Бэкенд нормализует старые задачи на чтении, поэтому graphics_status
            // приходит всегда. Терпимость к отсутствию ключа здесь противоречила
            // бы гейту дублей: UI показывал бы «подготовлено» там, где бэкенд
            // задачу не признаёт.
            return ['not_required', 'complete'].includes(process.graphics_status);
        }

        // Графика не доведена, но досье умного агента оплачено и цело: полный
        // перезапуск стоил бы новой сессии агента, поэтому такие процессы
        // догоняет отдельная кнопка повтора графики.
        function sectionOptimizationReplicationCanRetryGraphics(process) {
            if (!process || process.agent_status !== 'complete') return false;
            if (['queued', 'running'].includes(process.status)) return false;
            if (process.graphics_status === 'running') return false;
            return ['pending', 'partial', 'failed'].includes(process.graphics_status);
        }

        function sectionOptimizationReplicationNeedsAgent(signalId) {
            const process = sectionOptimizationReplicationFor(signalId);
            if (!process) return true;
            if (['queued', 'running'].includes(process.status)) return false;
            if (['failed', 'interrupted'].includes(process.status)) return true;
            // Недоведённая графика — не повод гонять умного агента заново.
            if (sectionOptimizationReplicationCanRetryGraphics(process)) return false;
            return !sectionOptimizationReplicationComplete(process);
        }

        async function retrySectionOptimizationGraphics(process) {
            const sectionCode = sidebarFilterSection.value;
            if (!sectionCode || !process || sectionOptimizationReplicationActionLoading.value) return;
            if (!sectionOptimizationReplicationCanRetryGraphics(process)) return;
            sectionOptimizationReplicationActionLoading.value = true;
            sectionOptimizationPipelineActionError.value = '';
            try {
                const response = await apiPost(
                    sectionOptimizationReplicationsUrl(
                        sectionCode,
                        '/' + encodeURIComponent(process.replication_id) + '/graphics/retry',
                    ),
                    undefined,
                    { withVersion: false },
                );
                if (response?.replication) {
                    upsertSectionOptimizationReplication(response.replication);
                    void pollSectionOptimizationReplication(
                        sectionCode,
                        currentObjectId.value || '',
                        process.replication_id,
                    );
                }
            } catch (error) {
                sectionOptimizationPipelineActionError.value =
                    error?.message || 'Не удалось повторить графическую проверку';
            } finally {
                sectionOptimizationReplicationActionLoading.value = false;
            }
        }

        // Эксперт обязан отличать «графика проверена» от «графика не доведена»:
        // во втором случае вердикт агента не подкреплён чертежами, и это его
        // решение — довериться или нажать повтор.
        function sectionOptimizationReplicationGraphicsLabel(process) {
            if (process.graphics_status === 'failed') {
                return 'Графика не проверена · ожидает эксперта';
            }
            if (process.graphics_status === 'partial') {
                return 'Графика проверена частично · ожидает эксперта';
            }
            if (process.graphics_status === 'pending' && process.graphics_required) {
                return 'Графика не запускалась · ожидает эксперта';
            }
            if (process.graphics_required && process.graphics_status === 'complete') {
                return 'Графика проверена · ожидает эксперта';
            }
            return 'Агент завершил · ожидает эксперта';
        }

        function sectionOptimizationReplicationStatusLabel(process) {
            if (!process) return 'Ожидает запуска умного агента';
            if (process.agent_status !== 'complete' && process.status === 'awaiting_expert') {
                return 'Требуется запуск умного агента';
            }
            const labels = {
                queued: 'В очереди умного агента',
                running: process.graphics_status === 'running'
                    ? 'Графический агент анализирует блоки…'
                    : (process.graphics_status === 'queued'
                        ? 'Графическая проверка в очереди'
                        : (process.agent_status === 'running'
                            ? 'Умный агент анализирует…'
                            : (process.agent_status === 'queued' ? 'В очереди умного агента' : 'Готовится досье…'))),
                awaiting_expert: sectionOptimizationReplicationGraphicsLabel(process),
                approved: 'Тиражирование принято',
                rejected: 'Тиражирование отклонено',
                failed: process.agent_status === 'failed' ? 'Ошибка умного агента' : 'Ошибка подготовки',
                interrupted: 'Процесс прерван',
            };
            return labels[process.status] || 'Статус не определён';
        }

        function sectionOptimizationAgentVerdictLabel(verdict) {
            const labels = {
                applicable: 'Можно тиражировать',
                applicable_with_conditions: 'Можно с условиями',
                needs_graphics: 'Нужна графика',
                needs_data: 'Недостаточно данных',
                reject: 'Не тиражировать',
            };
            return labels[verdict] || 'Требует проверки';
        }

        function sectionOptimizationGraphicsConclusionLabel(conclusion) {
            const labels = {
                supports_replication: 'Графика подтверждает',
                contradicts_replication: 'Графика противоречит',
                inconclusive: 'Недостаточно доказательств',
                not_visible: 'Не видно на выбранных блоках',
            };
            return labels[conclusion] || 'Графика не проверена';
        }

        function openSectionOptimizationGraphicsBlock(projectId, blockId, page) {
            if (!projectId || !blockId) return;
            blockBackRoute.value = {
                hash: window.location.hash || `#/section/${encodeURIComponent(sidebarFilterSection.value)}/optimization`,
                expandedFinding: null,
                expandedOpt: null,
            };
            navigate(`/project/${encodeURIComponent(projectId)}/blocks`);
            nextTick(async () => {
                await new Promise(resolve => setTimeout(resolve, 300));
                if (page) selectedBlockPage.value = page;
                await nextTick();
                for (const pg of blockPages.value) {
                    const found = (pg.blocks || []).find(block => block.block_id === blockId);
                    if (!found) continue;
                    selectedBlockPage.value = pg.page_num;
                    await nextTick();
                    openBlock(found);
                    break;
                }
            });
        }

        function upsertSectionOptimizationReplication(replication) {
            if (!sectionOptimizationData.value || !replication?.replication_id) return;
            const items = [...sectionOptimizationReplications.value];
            const index = items.findIndex(item => item.replication_id === replication.replication_id);
            if (index >= 0) items[index] = replication;
            else items.unshift(replication);
            sectionOptimizationData.value = {
                ...sectionOptimizationData.value,
                replications: items,
            };
        }

        async function pollSectionOptimizationReplication(sectionCode, objectId, replicationId) {
            const token = Symbol(replicationId);
            _sectionOptimizationReplicationPollTokens.set(replicationId, token);
            while (_sectionOptimizationReplicationPollTokens.get(replicationId) === token
                && currentView.value === 'section-optimization'
                && sidebarFilterSection.value === sectionCode
                && (currentObjectId.value || '') === objectId) {
                try {
                    const replication = await api(
                        sectionOptimizationReplicationsUrl(sectionCode, '/' + encodeURIComponent(replicationId)),
                        { withVersion: false, timeoutMs: 25000, retries: 0 },
                    );
                    if (_sectionOptimizationReplicationPollTokens.get(replicationId) !== token) return;
                    upsertSectionOptimizationReplication(replication);
                    if (!['queued', 'running'].includes(replication.status)) {
                        _sectionOptimizationReplicationPollTokens.delete(replicationId);
                        return;
                    }
                } catch (error) {
                    if (_sectionOptimizationReplicationPollTokens.get(replicationId) === token) {
                        sectionOptimizationPipelineActionError.value = error?.message || String(error);
                        _sectionOptimizationReplicationPollTokens.delete(replicationId);
                    }
                    return;
                }
                await new Promise(resolve => setTimeout(resolve, 700));
            }
        }

        async function startAllSectionOptimizationReplications() {
            const sectionCode = sidebarFilterSection.value;
            if (!sectionCode || sectionOptimizationReplicationActionLoading.value
                || !sectionOptimizationAgentAvailable.value
                || !sectionOptimizationGraphicsAgentAvailable.value
                || !sectionOptimizationReplicationPendingCount.value) return;
            sectionOptimizationReplicationActionLoading.value = true;
            sectionOptimizationPipelineActionError.value = '';
            try {
                let response;
                try {
                    response = await apiPost(
                        sectionOptimizationReplicationsUrl(sectionCode, '/start-all'),
                        undefined,
                        { withVersion: false },
                    );
                } catch (bulkError) {
                    // Совместимость на время безопасного обновления backend:
                    // одна кнопка всё равно запускает все строки через уже
                    // существующий одиночный endpoint.
                    if (!/404|not found/i.test(bulkError?.message || '')) throw bulkError;
                    const pendingSignals = sectionOptimizationReplicationCandidates.value
                        .filter(signal => sectionOptimizationReplicationNeedsAgent(signal.signal_id));
                    const results = await Promise.allSettled(pendingSignals.map(signal => apiPost(
                        sectionOptimizationReplicationsUrl(sectionCode, '/start'),
                        {
                            signal_id: signal.signal_id,
                            target_project_ids: signal.target_project_ids || [],
                        },
                        { withVersion: false },
                    )));
                    response = {
                        replications: results
                            .filter(item => item.status === 'fulfilled' && item.value?.replication)
                            .map(item => item.value.replication),
                        failed_count: results.filter(item => item.status === 'rejected').length,
                    };
                }
                for (const replication of (response?.replications || [])) {
                    upsertSectionOptimizationReplication(replication);
                    void pollSectionOptimizationReplication(
                        sectionCode,
                        currentObjectId.value || '',
                        replication.replication_id,
                    );
                }
                if (response?.failed_count) {
                    sectionOptimizationPipelineActionError.value =
                        `Не удалось запустить кандидатов: ${response.failed_count}`;
                }
            } catch (error) {
                sectionOptimizationPipelineActionError.value = error?.message || String(error);
            } finally {
                sectionOptimizationReplicationActionLoading.value = false;
            }
        }

        async function pollSectionOptimizationPipeline(sectionCode, objectId) {
            const pollSeq = ++_sectionOptimizationPipelinePollSeq;
            while (pollSeq === _sectionOptimizationPipelinePollSeq
                && currentView.value === 'section-optimization'
                && sidebarFilterSection.value === sectionCode
                && (currentObjectId.value || '') === objectId) {
                try {
                    const pipeline = await api(
                        sectionOptimizationPipelineUrl(sectionCode, '/status'),
                        { withVersion: false, timeoutMs: 25000, retries: 0 },
                    );
                    if (pollSeq !== _sectionOptimizationPipelinePollSeq) return;
                    if (sectionOptimizationData.value) {
                        sectionOptimizationData.value = {
                            ...sectionOptimizationData.value,
                            pipeline,
                        };
                    }
                    if (!['queued', 'running'].includes(pipeline.status)) {
                        if (pipeline.status === 'ready_for_review') {
                            await loadSectionOptimization(sectionCode, sectionOptimizationTab.value, true);
                        }
                        return;
                    }
                } catch (error) {
                    if (pollSeq === _sectionOptimizationPipelinePollSeq) {
                        sectionOptimizationPipelineActionError.value = error?.message || String(error);
                    }
                    return;
                }
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
        }

        async function runSectionOptimizationPipeline() {
            const sectionCode = sidebarFilterSection.value;
            if (!sectionCode || sectionOptimizationPipelineActionLoading.value) return;
            sectionOptimizationPipelineActionLoading.value = true;
            sectionOptimizationPipelineActionError.value = '';
            try {
                const response = await apiPost(
                    sectionOptimizationPipelineUrl(sectionCode, '/run'),
                    undefined,
                    { withVersion: false },
                );
                if (sectionOptimizationData.value && response?.pipeline) {
                    sectionOptimizationData.value = {
                        ...sectionOptimizationData.value,
                        pipeline: response.pipeline,
                    };
                }
                void pollSectionOptimizationPipeline(sectionCode, currentObjectId.value || '');
            } catch (error) {
                sectionOptimizationPipelineActionError.value = error?.message || String(error);
            } finally {
                sectionOptimizationPipelineActionLoading.value = false;
            }
        }

        async function requestSectionOptimizationGraphicsPlan() {
            const sectionCode = sidebarFilterSection.value;
            if (!sectionCode || sectionOptimizationPipelineActionLoading.value) return;
            sectionOptimizationPipelineActionLoading.value = true;
            sectionOptimizationPipelineActionError.value = '';
            try {
                const response = await apiPost(
                    sectionOptimizationPipelineUrl(sectionCode, '/graphics-plan'),
                    undefined,
                    { withVersion: false },
                );
                if (sectionOptimizationData.value && response?.pipeline) {
                    sectionOptimizationData.value = {
                        ...sectionOptimizationData.value,
                        pipeline: response.pipeline,
                    };
                }
            } catch (error) {
                sectionOptimizationPipelineActionError.value = error?.message || String(error);
            } finally {
                sectionOptimizationPipelineActionLoading.value = false;
            }
        }

        async function loadSectionOptimization(sectionCode, initialTab = 'specifications', force = false) {
            sectionOptimizationError.value = '';
            sectionOptimizationTab.value = initialTab;
            sectionOptimizationSearch.value = '';
            sectionOptimizationProjectFilter.value = '';
            const objectId = currentObjectId.value || '';
            const cacheKey = `${objectId}|${sectionCode}`;

            const seq = ++_sectionOptimizationLoadSeq;
            const cached = !force ? _sectionOptimizationMemoryCache.get(cacheKey) : null;
            const visibleData = cached || (sectionOptimizationLoadedKey.value === cacheKey
                ? sectionOptimizationData.value
                : null);
            if (visibleData) {
                sectionOptimizationData.value = visibleData;
                sectionOptimizationLoadedKey.value = cacheKey;
                sectionOptimizationLoading.value = false;
            } else {
                sectionOptimizationLoading.value = true;
                sectionOptimizationData.value = null;
                sectionOptimizationCollapsedProjects.value = {};
                sectionOptimizationExpandedSignals.value = {};
            }
            try {
                const query = objectId
                    ? '?object_id=' + encodeURIComponent(objectId)
                    : '';
                const data = await api(
                    '/optimization/section/' + encodeURIComponent(sectionCode) + query,
                    { withVersion: false },
                );
                if (seq !== _sectionOptimizationLoadSeq
                    || sidebarFilterSection.value !== sectionCode
                    || (currentObjectId.value || '') !== objectId) return;
                if (!data?.meta || !Array.isArray(data.specification_rows)
                    || !Array.isArray(data.accepted_optimizations) || !Array.isArray(data.signals)) {
                    throw new Error('Backend ещё не обновлён: получен устаревший формат сводки раздела.');
                }
                sectionOptimizationData.value = data;
                sectionOptimizationLoadedKey.value = cacheKey;
                _sectionOptimizationMemoryCache.delete(cacheKey);
                _sectionOptimizationMemoryCache.set(cacheKey, data);
                while (_sectionOptimizationMemoryCache.size > 20) {
                    _sectionOptimizationMemoryCache.delete(_sectionOptimizationMemoryCache.keys().next().value);
                }
                if (['queued', 'running'].includes(data?.pipeline?.status)) {
                    void pollSectionOptimizationPipeline(sectionCode, objectId);
                }
                for (const replication of (data.replications || [])) {
                    if (['queued', 'running'].includes(replication.status)) {
                        void pollSectionOptimizationReplication(sectionCode, objectId, replication.replication_id);
                    }
                }
            } catch (error) {
                if (seq !== _sectionOptimizationLoadSeq) return;
                sectionOptimizationError.value = error?.message || String(error);
            } finally {
                if (seq === _sectionOptimizationLoadSeq) sectionOptimizationLoading.value = false;
            }
        }

        function setSectionOptimizationTab(tab) {
            sectionOptimizationTab.value = tab;
        }

        // Кнопка «Разделы» в сайдбаре: одним кликом открывает страницу
        // «Разделы проекта» и сворачивает/разворачивает список разделов под
        // собой. Отдельный пункт «Все разделы» удалён — он дублировал ту же
        // страницу и уводил бейджи на строку ниже.
        function toggleSectionsNav() {
            sidebarSectionsOpen.value = !sidebarSectionsOpen.value;
            navigate('/section/__all__');
        }

        function navigateToSectionOptimization(sectionCode, tab = 'specifications') {
            navigate('/section/' + encodeURIComponent(sectionCode) + '/optimization?tab=' + encodeURIComponent(tab));
        }

        function sectionOptimizationProjectLabel(projectId) {
            const project = sectionOptimizationProjectOptions.value.find(item => item.project_id === projectId);
            return project?.project_name || projectId;
        }

        function sectionOptimizationSpecificationTypeMark(row) {
            const values = [row?.type_mark, row?.designation]
                .map(value => String(value || '').trim())
                .filter(Boolean);
            return [...new Set(values)].join(' · ');
        }

        function sectionOptimizationSpecificationSectionTitle(row) {
            return row?.category || row?.sheet_name || 'Спецификация';
        }

        function sectionOptimizationSpecificationSectionKey(row) {
            if (!row) return '';
            return `${row.project_id || ''}|${sectionOptimizationSpecificationSectionTitle(row)}`;
        }

        function sectionOptimizationSpecificationProjectKey(row) {
            return row?.project_id || '';
        }

        function isSectionOptimizationProjectCollapsed(projectId) {
            return Boolean(sectionOptimizationCollapsedProjects.value[projectId]);
        }

        function toggleSectionOptimizationProject(projectId) {
            sectionOptimizationCollapsedProjects.value = {
                ...sectionOptimizationCollapsedProjects.value,
                [projectId]: !isSectionOptimizationProjectCollapsed(projectId),
            };
        }

        function expandAllSectionOptimizationProjects() {
            sectionOptimizationCollapsedProjects.value = {};
        }

        function collapseAllSectionOptimizationProjects() {
            const collapsed = {};
            for (const project of sectionOptimizationProjectOptions.value) {
                if (project?.project_id) collapsed[project.project_id] = true;
            }
            sectionOptimizationCollapsedProjects.value = collapsed;
        }

        function sectionOptimizationSignalAcceptedItems(signal) {
            if (!sectionOptimizationSignalHasAcceptedSources(signal)) return [];
            const evidenceRefs = new Set(signal.evidence_refs || []);
            return (sectionOptimizationData.value?.accepted_optimizations || [])
                .filter(item => evidenceRefs.has(item.source_ref));
        }

        function sectionOptimizationSignalSpecificationItems(signal) {
            const evidenceRefs = new Set(signal?.target_row_ids || signal?.evidence_refs || []);
            return (sectionOptimizationData.value?.specification_rows || [])
                .filter(item => evidenceRefs.has(item.row_id));
        }

        function sectionOptimizationSignalHasAcceptedSources(signal) {
            return ['merge_accepted_optimizations', 'replicate_accepted_optimization'].includes(signal?.kind);
        }

        function isSectionOptimizationSignalExpanded(signalId) {
            return Boolean(sectionOptimizationExpandedSignals.value[signalId]);
        }

        function toggleSectionOptimizationSignal(signalId) {
            sectionOptimizationExpandedSignals.value = {
                ...sectionOptimizationExpandedSignals.value,
                [signalId]: !isSectionOptimizationSignalExpanded(signalId),
            };
        }

        function sectionOptimizationSignalTypeLabel(signal) {
            const labels = {
                merge_accepted_optimizations: 'Объединение принятых решений',
                replicate_accepted_optimization: 'Тиражирование решения',
                technical_variance: 'Техническое расхождение',
                consolidated_procurement: 'Общая закупка',
            };
            return labels[signal?.kind] || 'Кандидат';
        }

        function sectionOptimizationSignalGraphicsLabel(signal) {
            if (!signal?.graphics_recommended) return 'Не требуется';
            const plannedSignals = sectionOptimizationPipeline.value.graphics_plan?.signal_ids || [];
            return plannedSignals.includes(signal.signal_id) ? 'План готов' : 'По запросу';
        }

        function formatSectionOptimizationQuantity(value) {
            if (value === null || value === undefined || value === '') return '—';
            return String(value);
        }

        watch(currentObjectId, (next, previous) => {
            if (next === previous) return;
            _sectionOptimizationLoadSeq++;
            _sectionOptimizationPipelinePollSeq++;
            sectionOptimizationLoading.value = false;
            sectionOptimizationError.value = '';
            sectionOptimizationPipelineActionError.value = '';
            sectionOptimizationData.value = null;
            sectionOptimizationLoadedKey.value = '';
            sectionOptimizationCollapsedProjects.value = {};
            sectionOptimizationExpandedSignals.value = {};
            sectionOptimizationReplicationActionLoading.value = false;
            _sectionOptimizationReplicationPollTokens.clear();
            if (currentView.value === 'section-optimization'
                && sidebarFilterSection.value
                && sidebarFilterSection.value !== '__all__') {
                loadSectionOptimization(
                    sidebarFilterSection.value,
                    sectionOptimizationTab.value,
                    true,
                );
            }
        });
        watch(sidebarFilterSection, (next, previous) => {
            if (next === previous) return;
            if (currentView.value === 'section-optimization') return;
            _sectionOptimizationLoadSeq++;
            sectionOptimizationLoading.value = false;
            sectionOptimizationError.value = '';
            const expectedKey = `${currentObjectId.value || ''}|${next || ''}`;
            if (expectedKey !== sectionOptimizationLoadedKey.value) {
                sectionOptimizationData.value = null;
                sectionOptimizationLoadedKey.value = '';
            }
        });

        // Навигация по проектам внутри раздела (Пред. / След.)
        const currentSectionProjectsList = computed(() => {
            if (!currentProject.value) return [];
            const section = currentProject.value.section;
            const allInSection = projects.value.filter(p => p.section === section);
            const groups = (projectGroups.value[section] || [])
                .slice().sort((a, b) => (a.order || 0) - (b.order || 0));
            const assigned = new Set(groups.flatMap(g => g.project_ids || []));
            const ordered = [];
            for (const group of groups) {
                for (const pid of (group.project_ids || [])) {
                    const p = allInSection.find(x => x.project_id === pid);
                    if (p) ordered.push(p);
                }
            }
            for (const p of allInSection) {
                if (!assigned.has(p.project_id)) ordered.push(p);
            }
            return ordered;
        });

        const prevProject = computed(() => {
            const list = currentSectionProjectsList.value;
            const idx = list.findIndex(p => p.project_id === currentProjectId.value);
            return idx > 0 ? list[idx - 1] : null;
        });

        const nextProject = computed(() => {
            const list = currentSectionProjectsList.value;
            const idx = list.findIndex(p => p.project_id === currentProjectId.value);
            return idx >= 0 && idx < list.length - 1 ? list[idx + 1] : null;
        });

        // Drag: проект → группа
        function onProjectDragStart(e, projectId) {
            dragProjectId.value = projectId;
            e.dataTransfer.effectAllowed = 'move';
            e.dataTransfer.setData('application/project-id', projectId);
        }

        function onGroupDragOver(e, groupId) {
            // Разрешить drop
            e.preventDefault();
            if (dragProjectId.value) {
                dragOverGroupId.value = groupId;
                e.dataTransfer.dropEffect = 'move';
            } else if (dragGroupId.value && dragGroupId.value !== groupId && groupId !== '__ungrouped__') {
                dragOverGroupId.value = groupId;
                e.dataTransfer.dropEffect = 'move';
                // Live-swap групп
                const section = sidebarFilterSection.value;
                const groups = projectGroups.value[section] || [];
                const now = Date.now();
                if (now - lastGroupDragSwap < 100) return;
                lastGroupDragSwap = now;
                const fromIdx = groups.findIndex(g => g.id === dragGroupId.value);
                const toIdx = groups.findIndex(g => g.id === groupId);
                if (fromIdx !== -1 && toIdx !== -1 && fromIdx !== toIdx) {
                    const [moved] = groups.splice(fromIdx, 1);
                    groups.splice(toIdx, 0, moved);
                    // Обновить order
                    groups.forEach((g, i) => g.order = i);
                }
            }
        }

        function onGroupDragLeave(e, groupId) {
            if (dragOverGroupId.value === groupId) {
                dragOverGroupId.value = null;
            }
        }

        function onProjectDropOnGroup(e, targetGroupId, section) {
            e.preventDefault();
            const projectId = dragProjectId.value || e.dataTransfer.getData('application/project-id');
            if (!projectId) return;

            const groups = projectGroups.value[section] || [];
            // Убрать проект из всех групп этой секции
            for (const g of groups) {
                g.project_ids = (g.project_ids || []).filter(id => id !== projectId);
            }
            // Добавить в целевую (если не "Без группы")
            if (targetGroupId !== '__ungrouped__') {
                const target = groups.find(g => g.id === targetGroupId);
                if (target) {
                    target.project_ids.push(projectId);
                }
            }
            projectGroups.value[section] = groups;
            saveProjectGroups(section);
            dragProjectId.value = null;
            dragOverGroupId.value = null;
        }

        // Drag: реордер групп
        let lastGroupDragSwap = 0;

        function onGroupHeaderDragStart(e, groupId) {
            dragGroupId.value = groupId;
            e.dataTransfer.effectAllowed = 'move';
            e.dataTransfer.setData('application/group-id', groupId);
        }

        function onGroupHeaderDragEnd() {
            if (dragGroupId.value) {
                const section = sidebarFilterSection.value;
                saveProjectGroups(section);
            }
            dragGroupId.value = null;
            dragOverGroupId.value = null;
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

        // ─── Upload folder from computer (browser folder upload) ───
        const uploadObjectId = ref('');
        const uploadDiscipline = ref('');
        const uploadProjectName = ref('');
        const uploadFiles = ref([]);          // raw File[] из <input webkitdirectory>
        const uploadScan = ref(null);         // {pdf, md, result, ocr, ignored:[]}
        const uploadScanError = ref('');      // блокирующая проблема (нет/неск. PDF)
        const uploadScanWarnings = ref([]);   // не блокирует (нет md/result/ocr)
        const uploadError = ref('');          // ошибка сервера
        const uploadLoading = ref(false);
        const uploadResult = ref(null);       // ответ сервера при успехе (single)
        // precheck (single) + multi-folder
        const uploadMode = ref('multi');     // только multi (одна папка убрана)
        const uploadPrecheck = ref(null);     // verdict {status, blocks, warnings, ...}
        const uploadPrecheckLoading = ref(false);
        const uploadOverrideWarning = ref(false);
        const uploadCandidates = ref([]);     // multi: [{folder, pdf, name, status, ...}]
        const uploadBatchResult = ref(null);  // multi: {uploaded, skipped, failed}
        const uploadBatchProgress = ref('');
        // авто-дисциплина + предложение версии (single)
        const uploadDetectedDiscipline = ref('');
        const uploadDisciplineSource = ref('');   // folder_name|pdf_name|document_text|fallback
        const uploadFolderName = ref('');         // имя выбранной папки (для детекции)
        const uploadAddMode = ref('new_project'); // 'new_project' | 'new_version'
        const uploadTargetProjectId = ref('');
        const uploadDisciplineManual = ref(false); // пользователь выбрал дисциплину вручную

        const _DISC_SOURCE_LABEL = {
            folder_name: 'по имени папки', pdf_name: 'по имени PDF',
            document_text: 'по тексту', fallback: 'по умолчанию',
        };
        function disciplineSourceLabel(src) { return _DISC_SOURCE_LABEL[src] || src || ''; }

        // следующая версия у target-проекта (переиспат. логику candidateNextVersionLabel)
        function versionLabelForTarget(pid) {
            const t = (projects.value || []).find(p => p.project_id === pid);
            if (!t) return 'V?';
            if (Array.isArray(t.versions_summary)) {
                const latest = t.versions_summary.find(v => v.is_latest);
                if (latest && latest.version_id !== 'v1' && (latest.pdf_count || 0) === 0) {
                    return (latest.label || 'V' + latest.version_no) + ' (пустая)';
                }
            }
            return 'V' + ((t.version_count || 1) + 1);
        }

        function fmtSize(bytes) {
            if (!bytes && bytes !== 0) return '';
            if (bytes < 1024) return bytes + ' B';
            if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
            return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
        }

        // только релевантные файлы (pdf + md + result + ocr + blocks + zip) из scan-like объекта.
        // ZIP-комплекты портала отправляются как есть — бэкенд распаковывает сам.
        function _uploadBundleFiles(s) {
            const out = [];
            if (s.pdf) out.push(s.pdf);
            if (s.md) out.push(s.md);
            if (s.result) out.push(s.result);
            if (s.ocr) out.push(s.ocr);
            if (s.blocks) out.push(s.blocks);
            for (const z of (s.zips || [])) out.push(z);
            return out;
        }

        // существующие проекты выбранного раздела — варианты «привязать как версию»
        const uploadTargetOptions = computed(() => {
            const sec = uploadDiscipline.value;
            if (!sec) return [];
            const cand = normalizeProjectName(uploadProjectName.value || '');
            const out = (projects.value || []).filter(p => p.section === sec).map(p =>
                Object.assign({}, p, {
                    _suggested: !!cand && normalizeProjectName(p.name || p.project_id) === cand,
                }));
            out.sort((a, b) => (a._suggested === b._suggested)
                ? String(a.name || a.project_id).localeCompare(String(b.name || b.project_id))
                : (a._suggested ? -1 : 1));
            return out;
        });

        const canSubmitUpload = computed(() => {
            if (!uploadObjectId.value || !uploadDiscipline.value) return false;
            if (!uploadScan.value || uploadScanError.value) return false;
            // PDF либо явно, либо внутри ZIP-комплекта (бэкенд распакует и проверит)
            if (!uploadScan.value.pdf && !(uploadScan.value.zips || []).length) return false;
            if (uploadAddMode.value === 'new_version') {
                // версия: нужен target; имя-дубли не блокируют (это и есть версия)
                return !!uploadTargetProjectId.value;
            }
            if (!uploadProjectName.value.trim()) return false;
            const pc = uploadPrecheck.value;
            if (pc) {
                if (pc.status === 'duplicate' || pc.status === 'error') return false;
                if (pc.status === 'warning' && !uploadOverrideWarning.value) return false;
            }
            return true;
        });

        function setUploadMode(m) {
            uploadMode.value = m;
            uploadScan.value = null; uploadPrecheck.value = null; uploadOverrideWarning.value = false;
            uploadScanError.value = ''; uploadScanWarnings.value = [];
            uploadCandidates.value = []; uploadBatchResult.value = null;
            uploadResult.value = null; uploadError.value = '';
            uploadAddMode.value = 'new_project'; uploadTargetProjectId.value = '';
            uploadDetectedDiscipline.value = ''; uploadDisciplineSource.value = '';
            uploadDiscipline.value = ''; uploadDisciplineManual.value = false;
        }

        function goToUploadFolder() {
            addProjectStep.value = 'upload';
            resetUploadFolder();
            uploadMode.value = 'multi';
            uploadObjectId.value = currentObjectId.value || '';
            if (!objectsList.value.length) loadObjects();
            if (!supportedDisciplines.value.length) loadDisciplines();
        }

        function resetUploadFolder() {
            // объект сохраняем; дисциплину СБРАСЫВАЕМ — иначе она «залипает» от
            // прошлой загрузки и расходится с авто-определением.
            uploadProjectName.value = '';
            uploadDiscipline.value = '';
            uploadDisciplineManual.value = false;
            uploadFiles.value = [];
            uploadScan.value = null;
            uploadScanError.value = '';
            uploadScanWarnings.value = [];
            uploadError.value = '';
            uploadResult.value = null;
            uploadPrecheck.value = null;
            uploadPrecheckLoading.value = false;
            uploadOverrideWarning.value = false;
            uploadCandidates.value = [];
            uploadBatchResult.value = null;
            uploadBatchProgress.value = '';
        }

        // пользователь вручную сменил дисциплину → фиксируем и перепроверяем
        function onUploadDisciplineChange() {
            uploadDisciplineManual.value = true;
            runSinglePrecheck();
        }

        function onUploadFolderSelected(ev) {
            uploadError.value = '';
            uploadPrecheck.value = null;
            uploadOverrideWarning.value = false;
            uploadAddMode.value = 'new_project';
            uploadTargetProjectId.value = '';
            uploadDetectedDiscipline.value = '';
            uploadDisciplineSource.value = '';
            uploadDisciplineManual.value = false;  // новая папка → снова авто-детект
            const all = Array.from(ev.target.files || []);
            uploadFiles.value = all;
            uploadFolderName.value = (all[0] && (all[0].webkitRelativePath || '').split('/')[0]) || '';
            if (!all.length) { uploadScan.value = null; return; }

            const pdfs = [], mds = [], results = [], ocrs = [], blocksFiles = [], zips = [], ignored = [];
            for (const f of all) {
                const name = (f.name || '').toLowerCase();
                if (name.endsWith('.pdf')) pdfs.push(f);
                else if (name.endsWith('_document.md')) mds.push(f);
                else if (name.endsWith('.md')) mds.push(f);
                else if (name.endsWith('_result.json')) results.push(f);
                else if (name.endsWith('_blocks.json')) blocksFiles.push(f);
                else if (name.endsWith('_ocr.html') || name.endsWith('_ocr.htm')) ocrs.push(f);
                else if (name.endsWith('.zip')) zips.push(f);  // ZIP-комплект портала — распакует бэкенд
                else ignored.push(f);
            }

            uploadScanError.value = '';
            uploadScanWarnings.value = [];
            if (pdfs.length === 0 && zips.length === 1) {
                // комплект внутри ZIP: PDF проверит precheck после распаковки на бэкенде;
                // имя проекта — из имени архива без браузерного суффикса « (N)»
                if (!uploadProjectName.value.trim()) {
                    uploadProjectName.value = zips[0].name.replace(/\.zip$/i, '').replace(/\s*\(\d+\)$/, '');
                }
            } else if (pdfs.length === 0 && zips.length > 1) {
                uploadScanError.value = 'В папке несколько ZIP. Оставьте один архив на проект (или используйте «Несколько проектов»).';
            } else if (pdfs.length === 0) {
                uploadScanError.value = 'В папке не найден PDF. Нужен ровно один PDF проекта (или ZIP-комплект портала).';
            } else if (pdfs.length > 1) {
                uploadScanError.value = 'В папке найдено несколько PDF. Выберите папку одного проекта.';
            } else {
                if (!uploadProjectName.value.trim()) {
                    uploadProjectName.value = pdfs[0].name.replace(/\.pdf$/i, '');
                }
            }
            const md = mds.find(m => (m.name || '').toLowerCase().endsWith('_document.md')) || mds[0] || null;
            const hasZip = zips.length > 0;
            if (!md && !hasZip) uploadScanWarnings.value.push('Нет *_document.md — текстовый анализ потребует OCR.');
            if (!results.length && !blocksFiles.length && !hasZip) uploadScanWarnings.value.push('Нет *_result.json / *_blocks.json — кроп блоков потребует подготовки.');
            if (!ocrs.length && !hasZip) uploadScanWarnings.value.push('Нет *_ocr.html — text_evidence будет ограничен.');

            uploadScan.value = {
                pdf: pdfs.length === 1 ? pdfs[0] : null,
                md, result: results[0] || null, ocr: ocrs[0] || null,
                blocks: blocksFiles[0] || null,
                zips,
                ignored,
            };
            // авто-precheck дублей (если задано имя/объект/дисциплина)
            runSinglePrecheck();
        }

        async function runSinglePrecheck() {
            const s = uploadScan.value;
            const hasSource = s && (s.pdf || (s.zips || []).length);
            if (!hasSource || !uploadObjectId.value || !uploadProjectName.value.trim()) {
                uploadPrecheck.value = null; return;
            }
            uploadPrecheckLoading.value = true;
            try {
                const fd = new FormData();
                fd.append('object_id', uploadObjectId.value);
                // дисциплину НЕ форсим — backend определит сам, если поле пустое
                if (uploadDiscipline.value) fd.append('discipline', uploadDiscipline.value);
                if (uploadFolderName.value) fd.append('folder_name', uploadFolderName.value);
                fd.append('project_name', uploadProjectName.value.trim());
                for (const f of _uploadBundleFiles(s)) fd.append('files', f, f.name);
                const resp = await fetch('/api/projects/upload-folder/precheck', { method: 'POST', body: fd });
                const data = await resp.json().catch(() => ({}));
                const pc = (resp.ok && data.precheck) ? data.precheck : null;
                uploadPrecheck.value = pc;
                if (pc) {
                    uploadDetectedDiscipline.value = pc.detected_discipline || '';
                    uploadDisciplineSource.value = pc.discipline_source || '';
                    // если пользователь ещё не выбрал дисциплину — подставить определённую
                    // синхронизируем дропдаун с авто-определением, пока пользователь
                    // не выбрал дисциплину вручную (иначе бейдж и дропдаун расходятся)
                    if (!uploadDisciplineManual.value && pc.detected_discipline) {
                        uploadDiscipline.value = pc.detected_discipline;
                    }
                    // авто-предложение версии: если нашёлся target и пользователь не
                    // переключал режим вручную — предложить «новая версия»
                    if (pc.suggested_target_project && uploadAddMode.value === 'new_project'
                        && !uploadTargetProjectId.value) {
                        uploadAddMode.value = 'new_version';
                        uploadTargetProjectId.value = pc.suggested_target_project;
                    }
                }
            } catch (e) {
                uploadPrecheck.value = null;
            } finally {
                uploadPrecheckLoading.value = false;
            }
        }

        async function submitUploadFolder() {
            if (uploadLoading.value) return;
            const s = uploadScan.value;
            if (!s || !s.pdf) { uploadError.value = 'Выберите папку с одним PDF.'; return; }
            if (!uploadObjectId.value || !uploadDiscipline.value) {
                uploadError.value = 'Заполните объект и дисциплину.'; return;
            }
            const isVersion = uploadAddMode.value === 'new_version';
            if (isVersion && !uploadTargetProjectId.value) {
                uploadError.value = 'Выберите проект-основание для новой версии.'; return;
            }
            if (!isVersion && !uploadProjectName.value.trim()) {
                uploadError.value = 'Укажите название проекта.'; return;
            }
            uploadError.value = '';
            uploadLoading.value = true;
            try {
                if (!isVersion) {
                    // новый проект: свежий precheck перед загрузкой (race-aware)
                    await runSinglePrecheck();
                    const pc = uploadPrecheck.value;
                    if (pc && (pc.status === 'duplicate' || pc.status === 'error')) {
                        uploadError.value = (pc.blocks[0] && pc.blocks[0].message) || 'Загрузка заблокирована (дубль).';
                        return;
                    }
                    if (pc && pc.status === 'warning' && !uploadOverrideWarning.value) {
                        uploadError.value = 'Найдено предупреждение — отметьте «Всё равно загрузить».';
                        return;
                    }
                }
                const fd = new FormData();
                fd.append('object_id', uploadObjectId.value);
                fd.append('discipline', uploadDiscipline.value);
                fd.append('project_name', uploadProjectName.value.trim() || (s.pdf.name || '').replace(/\.pdf$/i, ''));
                fd.append('upload_mode', isVersion ? 'new_version' : 'new_project');
                if (isVersion) fd.append('target_project_id', uploadTargetProjectId.value);
                for (const f of _uploadBundleFiles(s)) fd.append('files', f, f.name);

                const resp = await fetch('/api/projects/upload-folder', { method: 'POST', body: fd });
                const data = await resp.json().catch(() => ({}));
                if (!resp.ok) {
                    uploadError.value = data.detail || ('Ошибка загрузки (HTTP ' + resp.status + ')');
                    return;
                }
                uploadResult.value = data;
                await refreshProjects();
            } catch (e) {
                uploadError.value = 'Сбой загрузки: ' + (e && e.message ? e.message : e);
            } finally {
                uploadLoading.value = false;
            }
        }

        function openUploadedProject() {
            const pid = uploadResult.value && uploadResult.value.project_id;
            closeAddProject();
            if (pid) navigate('/project/' + encodeURIComponent(pid));
        }

        function openProjectById(pid) {
            closeAddProject();
            if (pid) navigate('/project/' + encodeURIComponent(pid));
        }

        // ─── Multi-folder upload ───
        // один кандидат из набора файлов (label = подпапка или basename PDF)
        function _buildUploadCandidate(label, files) {
            const pdfs = files.filter(f => /\.pdf$/i.test(f.name));
            // _results.md/_results.html/_blocks.json — новый комплект портала (2026-07);
            // старые суффиксы (_document.md/_ocr.html) в приоритете, их приём
            // удалить после 2026-08-14 (раздел ВК пока грузится по-старому).
            // ZIP-комплект отправляется как есть — бэкенд распаковывает сам.
            const md = files.find(f => /_document\.md$/i.test(f.name))
                || files.find(f => /_results\.md$/i.test(f.name))
                || files.find(f => /\.md$/i.test(f.name)) || null;
            const result = files.find(f => /_result\.json$/i.test(f.name)) || null;
            const ocr = files.find(f => /_ocr\.html?$/i.test(f.name))
                || files.find(f => /_results\.html?$/i.test(f.name)) || null;
            const blocks = files.find(f => /_blocks\.json$/i.test(f.name)) || null;
            const zips = files.filter(f => /\.zip$/i.test(f.name));
            const pdf = pdfs.length === 1 ? pdfs[0] : null;
            const zipStem = (zips.length === 1 && !pdf)
                ? zips[0].name.replace(/\.zip$/i, '').replace(/\s*\(\d+\)$/, '') : null;
            return {
                folder: label, files, pdf,
                pdfName: pdf ? pdf.name : (pdfs[0] ? pdfs[0].name : null), pdfCount: pdfs.length,
                md, result, ocr, blocks, zips, zipCount: zips.length,
                hasMd: !!md, hasResult: !!result, hasOcr: !!ocr, hasBlocks: !!blocks,
                name: pdf ? pdf.name.replace(/\.pdf$/i, '') : (zipStem || label),
                discipline: '', detectedDiscipline: '', disciplineSource: '',
                addMode: 'new_project', targetProjectId: '',
                status: 'pending', message: '', checked: false, precheck: null,
            };
        }

        function onMultiFolderSelected(ev) {
            uploadError.value = ''; uploadBatchResult.value = null;
            const all = Array.from(ev.target.files || []);
            if (!all.length) { uploadCandidates.value = []; return; }
            // Две поддерживаемые раскладки внутри выбранной родительской папки:
            //  (A) подпапки-проекты: "parent/sub/.../file" (глубина ≥3) → группа = sub;
            //  (B) плоские файлы: "parent/file" (глубина 2) → каждый PDF = отдельный
            //      кандидат-проект, sidecar'ы (_document.md / _result.json / _ocr.html)
            //      привязываются по префиксу имени PDF.
            const groups = {};           // sub → файлы (раскладка A)
            const flat = [];             // файлы прямо в выбранной папке (раскладка B)
            for (const f of all) {
                const rel = (f.webkitRelativePath || f.name).split('/');
                if (rel.length >= 3) {
                    const sub = rel[1];
                    (groups[sub] = groups[sub] || []).push(f);
                } else if (rel.length === 2) {
                    flat.push(f);
                }
            }
            const cands = [];
            // (A) кандидаты из подпапок
            for (const sub of Object.keys(groups).sort()) {
                cands.push(_buildUploadCandidate(sub, groups[sub]));
            }
            // (B) кандидаты из плоских PDF — каждый PDF отдельный проект
            const flatPdfs = flat.filter(f => /\.pdf$/i.test(f.name))
                .sort((a, b) => a.name.localeCompare(b.name, 'ru'));
            for (const pdf of flatPdfs) {
                const stem = pdf.name.replace(/\.pdf$/i, '').toLowerCase();
                const sidecars = flat.filter(f => f !== pdf
                    && /(_document\.md|\.md|_result\.json|_blocks\.json|_ocr\.html?|_results\.html?)$/i.test(f.name)
                    && f.name.toLowerCase().startsWith(stem));
                cands.push(_buildUploadCandidate(pdf.name.replace(/\.pdf$/i, ''), [pdf, ...sidecars]));
            }
            // (B') кандидаты из плоских ZIP — каждый ZIP-комплект портала = проект
            const flatZips = flat.filter(f => /\.zip$/i.test(f.name))
                .sort((a, b) => a.name.localeCompare(b.name, 'ru'));
            for (const z of flatZips) {
                const label = z.name.replace(/\.zip$/i, '').replace(/\s*\(\d+\)$/, '');
                cands.push(_buildUploadCandidate(label, [z]));
            }
            // запасной случай: на верхнем уровне есть файлы, но PDF не нашёлся —
            // отдать всё одним кандидатом (precheck честно покажет «нет PDF»).
            if (!cands.length && flat.length) {
                const top = ((all[0].webkitRelativePath || all[0].name || '').split('/')[0]) || 'project';
                cands.push(_buildUploadCandidate(top, flat));
            }
            uploadCandidates.value = cands;
            if (cands.length) recheckAllCandidates();
        }

        // precheck одной строки. discipline берётся per-row (c.discipline) либо,
        // если пусто, глобальный uploadDiscipline; если и он пуст — авто-детект.
        async function recheckCandidate(c) {
            if (!uploadObjectId.value) { c.status = 'error'; c.message = 'Выберите объект'; c.checked = false; return; }
            // ZIP-кандидат: PDF внутри архива, проверит бэкенд после распаковки
            if (c.pdfCount === 0 && !(c.zipCount > 0)) { c.status = 'error'; c.message = 'Нет PDF'; c.checked = false; return; }
            if (c.pdfCount > 1) { c.status = 'error'; c.message = 'Несколько PDF — оставьте один PDF на проект'; c.checked = false; return; }
            c.status = 'pending'; c.message = 'проверка…';
            try {
                const fd = new FormData();
                fd.append('object_id', uploadObjectId.value);
                const disc = c.discipline || uploadDiscipline.value || '';
                if (disc) fd.append('discipline', disc);
                fd.append('folder_name', c.folder);
                fd.append('project_name', (c.name || '').trim());
                for (const f of _uploadBundleFiles(c)) fd.append('files', f, f.name);
                const resp = await fetch('/api/projects/upload-folder/precheck', { method: 'POST', body: fd });
                const data = await resp.json().catch(() => ({}));
                const pc = (resp.ok && data.precheck) ? data.precheck : null;
                if (!pc) { c.status = 'error'; c.message = 'ошибка проверки'; c.checked = false; return; }
                c.precheck = pc; c.status = pc.status;
                c.detectedDiscipline = pc.detected_discipline || '';
                c.disciplineSource = pc.discipline_source || '';
                if (!c.discipline && pc.discipline) c.discipline = pc.discipline;  // авто
                // авто-предложение версии (если режим ещё не трогали вручную)
                if (pc.suggested_target_project && c.addMode === 'new_project' && !c.targetProjectId) {
                    c.addMode = 'new_version';
                    c.targetProjectId = pc.suggested_target_project;
                }
                c.message = (pc.blocks[0] && pc.blocks[0].message)
                    || (pc.warnings[0] && pc.warnings[0].message) || '';
                // для new_version имя-дубли не мешают; считаем строку готовой к загрузке
                if (c.addMode === 'new_version' && c.targetProjectId) {
                    c.checked = true;
                } else {
                    c.checked = (pc.status === 'ready' || pc.status === 'warning');
                }
            } catch (e) {
                c.status = 'error'; c.message = 'ошибка проверки'; c.checked = false;
            }
        }

        async function recheckAllCandidates() {
            if (!uploadCandidates.value.length) return;
            // последовательно, чтобы не залить backend параллельными precheck'ами
            for (const c of uploadCandidates.value) {
                await recheckCandidate(c);
            }
        }

        // варианты target для строки (проекты раздела строки), с пометкой совпадения
        function candTargetOptions(c) {
            const sec = c.discipline || uploadDiscipline.value;
            if (!sec) return [];
            const cand = normalizeProjectName(c.name || '');
            const out = (projects.value || []).filter(p => p.section === sec).map(p =>
                Object.assign({}, p, {
                    _suggested: !!cand && normalizeProjectName(p.name || p.project_id) === cand,
                }));
            out.sort((a, b) => (a._suggested === b._suggested)
                ? String(a.name || a.project_id).localeCompare(String(b.name || b.project_id))
                : (a._suggested ? -1 : 1));
            return out;
        }
        function candVersionLabel(c) {
            return c.targetProjectId ? versionLabelForTarget(c.targetProjectId) : 'V?';
        }
        function candUploadableRow(c) {
            if (c.addMode === 'new_version') return !!c.targetProjectId && c.status !== 'error';
            return c.status === 'ready' || c.status === 'warning';
        }

        function candUploadable(c) {
            if (c.addMode === 'new_version') return !!c.targetProjectId && c.status !== 'error';
            return c.status === 'ready' || c.status === 'warning';
        }
        function candStatusLabel(c) {
            return ({ ready: 'готов', warning: '⚠ предупреждение', duplicate: '⛔ дубль',
                      error: '⛔ ошибка', pending: '…', done: '✓ загружен' })[c.status] || c.status;
        }
        function candCount(status) {
            return uploadCandidates.value.filter(c => c.status === status).length;
        }
        const selectedCandidateCount = computed(() =>
            uploadCandidates.value.filter(c => c.checked && candUploadable(c)).length
        );

        async function submitMultiUpload() {
            const toUpload = uploadCandidates.value.filter(c => c.checked && candUploadable(c));
            if (!toUpload.length || uploadLoading.value) return;
            uploadError.value = ''; uploadLoading.value = true;
            const uploaded = [], skipped = [], failed = [];
            let i = 0;
            try {
                // ПОСЛЕДОВАТЕЛЬНО (await на каждый), не параллельно — защита от гонки
                // old_to_new_map.json и от перегрузки dual_write_shadow.
                for (const c of toUpload) {
                    i++; uploadBatchProgress.value = i + '/' + toUpload.length;
                    const fd = new FormData();
                    fd.append('object_id', uploadObjectId.value);
                    fd.append('discipline', c.discipline || uploadDiscipline.value || '');
                    fd.append('project_name', (c.name || '').trim());
                    const isVer = c.addMode === 'new_version';
                    fd.append('upload_mode', isVer ? 'new_version' : 'new_project');
                    if (isVer) fd.append('target_project_id', c.targetProjectId);
                    for (const f of _uploadBundleFiles(c)) fd.append('files', f, f.name);
                    try {
                        const resp = await fetch('/api/projects/upload-folder', { method: 'POST', body: fd });
                        const data = await resp.json().catch(() => ({}));
                        if (resp.status === 409) {
                            c.status = 'duplicate'; c.checked = false; c.message = data.detail || 'дубль';
                            skipped.push({ folder: c.folder, error: data.detail || 'дубль' });
                        } else if (!resp.ok) {
                            c.status = 'error'; c.checked = false; c.message = data.detail || ('HTTP ' + resp.status);
                            failed.push({ folder: c.folder, error: data.detail || ('HTTP ' + resp.status) });
                        } else {
                            c.status = 'done'; c.checked = false;
                            uploaded.push({ project_id: data.project_id, folder: c.folder });
                        }
                    } catch (e) {
                        c.status = 'error'; c.checked = false; c.message = String(e && e.message || e);
                        failed.push({ folder: c.folder, error: String(e && e.message || e) });
                    }
                }
                uploadBatchResult.value = { uploaded, skipped, failed };
                await refreshProjects();
            } finally {
                uploadLoading.value = false; uploadBatchProgress.value = '';
            }
        }

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

        // Нормализация имени для матчинга candidate ↔ существующий проект.
        // Убираем расширение, "(1)", "_document", "Изм.1", лишние пробелы,
        // приводим к нижнему регистру.
        function normalizeProjectName(name) {
            if (!name) return '';
            let s = String(name).toLowerCase();
            s = s.replace(/\.pdf$/, '');
            s = s.replace(/\.md$/, '');
            s = s.replace(/_document$/, '');
            s = s.replace(/_results$/, '');
            s = s.replace(/\s*\(\d+\)\s*$/g, '');
            s = s.replace(/[\s_\-]*изм\.?\s*\d+/g, '');
            s = s.replace(/[\s_\-]+/g, ' ');
            return s.trim();
        }

        function candidateBasename(f) {
            const pdf = (f && f.pdf_files && f.pdf_files[0]) || f.folder || '';
            return normalizeProjectName(pdf) || normalizeProjectName(f.folder);
        }

        // Список существующих проектов того же раздела, что candidate.
        function candidateTargetOptions(f) {
            const sec = f && f._selectedDiscipline;
            if (!sec) return [];
            const all = (projects.value || []).filter(p => p.section === sec);
            const candName = candidateBasename(f);
            // Помечаем "_suggested" — для подсказки в селекте
            const out = all.map(p => {
                const matched = !!candName
                    && normalizeProjectName(p.name || p.project_id) === candName;
                return Object.assign({}, p, { _suggested: matched });
            });
            // Sort: suggested first, then alpha
            out.sort((a, b) => {
                if (a._suggested && !b._suggested) return -1;
                if (!a._suggested && b._suggested) return 1;
                return String(a.name || a.project_id).localeCompare(String(b.name || b.project_id));
            });
            return out;
        }

        function candidateTargetName(f) {
            const opts = candidateTargetOptions(f);
            const t = opts.find(p => p.project_id === f._targetProjectId);
            return t ? (t.name || t.project_id) : f._targetProjectId;
        }

        // Имя следующей версии у выбранного target-проекта. Если у target
        // уже есть пустая latest-версия (V2+) — переиспользуем её.
        function candidateNextVersionLabel(f) {
            if (!f || !f._targetProjectId) return 'V?';
            const t = (projects.value || []).find(p => p.project_id === f._targetProjectId);
            if (!t) return 'V?';
            if (Array.isArray(t.versions_summary)) {
                const latest = t.versions_summary.find(v => v.is_latest);
                if (latest && latest.version_id !== 'v1' && (latest.pdf_count || 0) === 0) {
                    return (latest.label || 'V' + latest.version_no) + ' (пустая)';
                }
            }
            const next = (t.version_count || 1) + 1;
            return 'V' + next;
        }

        function _decorateCandidate(f, isExternal, detected) {
            f._detectedDiscipline = detected;
            f._selectedDiscipline = detected;
            f._isExternal = isExternal;
            f._selectedPdfs = [...f.pdf_files];
            f._selectedMds = [...f.md_files];
            f._addMode = 'new';
            f._targetProjectId = '';
            // Уверенное совпадение → дефолт «версия», иначе «новый проект»
            const opts = candidateTargetOptions(f);
            const suggested = opts.find(p => p._suggested);
            if (suggested) {
                f._addMode = 'version';
                f._targetProjectId = suggested.project_id;
            }
        }

        async function scanFolders() {
            addProjectLoading.value = true;
            try {
                const data = await api('/projects/scan');
                const folders = data.folders;
                for (const f of folders) {
                    const detected = await detectDiscipline(f.folder);
                    _decorateCandidate(f, false, detected);
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
                    _decorateCandidate(f, true, detected);
                }
                unregisteredFolders.value = folders;
            } catch (e) {
                alert('Ошибка сканирования: ' + e.message);
            }
            addProjectLoading.value = false;
        }

        function onCandidatePrimaryAction(f) {
            if (!f) return;
            if (f._addMode === 'version') {
                return registerProjectAsVersion(f.folder);
            }
            return registerProject(f.folder);
        }

        // Build a server-side path for a candidate file: backend allows files
        // under PROJECTS_DIR or under the external_root scanned. For "local"
        // candidates folderInfo.folder is `<section>/<name>` (relative to projects/);
        // for external, folderInfo.full_path is absolute root, filenames are relative.
        function _candidateFilePath(folderInfo, filename) {
            if (!filename) return null;
            if (folderInfo._isExternal && folderInfo.full_path) {
                return folderInfo.full_path.replace(/[\\/]+$/, '') + '/' + filename;
            }
            // local: folderInfo.folder is a path under projects/. Resolve as
            // <projects>/<folder>/<filename> via server side; we ship just the
            // logical path and backend resolves against PROJECTS_DIR.
            return 'projects/' + folderInfo.folder.replace(/[\\/]+$/, '') + '/' + filename;
        }

        async function registerProjectAsVersion(folder) {
            const folderInfo = unregisteredFolders.value.find(f => f.folder === folder);
            if (!folderInfo) return;
            if (!folderInfo._targetProjectId) {
                alert('Выберите проект-основание для версии');
                return;
            }
            const selPdfs = folderInfo._selectedPdfs && folderInfo._selectedPdfs.length > 0
                ? folderInfo._selectedPdfs : [folderInfo.pdf_files[0]];
            const selMds = folderInfo._selectedMds && folderInfo._selectedMds.length > 0
                ? folderInfo._selectedMds : (folderInfo.md_files.length > 0 ? [folderInfo.md_files[0]] : []);
            const pdfPath = _candidateFilePath(folderInfo, selPdfs[0]);
            const mdPath = selMds.length > 0 ? _candidateFilePath(folderInfo, selMds[0]) : null;
            const targetId = folderInfo._targetProjectId;
            const expectedVer = candidateNextVersionLabel(folderInfo);

            addProjectLoading.value = true;
            try {
                const body = {
                    target_project_id: targetId,
                    candidate_pdf_path: pdfPath,
                    candidate_md_path: mdPath,
                    expected_section: folderInfo._selectedDiscipline || null,
                    comment: 'Добавлено из окна Добавить проект',
                    source: 'section_add_project_modal',
                };
                if (folderInfo._isExternal && folderInfo.full_path) {
                    body.external_root = folderInfo.full_path;
                }
                // Flat-endpoint (target в body) — обходим %2F в URL.
                const resp = await fetch(
                    '/api/projects/versions/from-candidate',
                    {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify(body),
                    },
                );
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || `Ошибка: ${resp.status}`);
                }
                const data = await resp.json();
                const verLabel = (data.version && data.version.label) || expectedVer;
                if (typeof showToast === 'function') {
                    showToast(`Создана версия ${verLabel} для проекта ${candidateTargetName(folderInfo)}`);
                } else {
                    console.log(`Создана версия ${verLabel} для проекта ${candidateTargetName(folderInfo)}`);
                }
                unregisteredFolders.value = unregisteredFolders.value.filter(f => f.folder !== folder);
                await refreshProjects();
                if (unregisteredFolders.value.length === 0) {
                    showAddProject.value = false;
                }
            } catch (e) {
                alert('Ошибка создания версии: ' + e.message);
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
                            section: folderInfo._selectedDiscipline || 'EOM',
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
                            section: folderInfo._selectedDiscipline || 'EOM',
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
                // Если выбран режим «новая версия существующего» — идём через
                // новый endpoint и пропускаем register/register-external.
                if (folderInfo._addMode === 'version' && folderInfo._targetProjectId) {
                    try {
                        await registerProjectAsVersion(folderInfo.folder);
                    } catch (e) {
                        errors.push(`${folderInfo.folder}: ${e.message}`);
                    }
                    continue;
                }
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
                                section: folderInfo._selectedDiscipline || 'EOM',
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
                                section: folderInfo._selectedDiscipline || 'EOM',
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
            // Инвалидировать кеши — данные могли измениться (аудит завершён и т.д.)
            _cacheInvalidate('project');
            _cacheInvalidate('findings');
            _cacheInvalidate('optimization');
            _cacheInvalidate('blocks');
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

        // Монотонный счётчик загрузок проекта: защита от гонки, когда медленный
        // ответ по РАНЕЕ открытому проекту приходит позже и затирает уже
        // выбранный. Применяем результат только если это последняя загрузка.
        let _projectLoadSeq = 0;
        async function loadProject(id, forceRefresh) {
            // Закрываем PDF-панель только при реальной смене проекта; при
            // переключении вкладок/версий того же проекта она остаётся открытой.
            if (currentProjectId.value !== id) {
                showVersionPdf.value = false;
            }
            currentProjectId.value = id;
            // Смена проекта: сразу гасим данные ранее открытого проекта, чтобы
            // на время (возможно медленной) загрузки не висела «чужая» страница.
            const _loadedId = currentProject.value
                && (currentProject.value.project_id || currentProject.value.id);
            if (_loadedId && _loadedId !== id) {
                currentProject.value = null;
            }
            const _mySeq = ++_projectLoadSeq;
            // Кеш ключуется по (id, activeVersionId), чтобы V1/V2 одного проекта
            // не наступали друг на друга.
            const cacheKey = activeVersionId.value
                ? `${id}::${activeVersionId.value}`
                : id;
            // Для запущенного проекта 60-секундный кэш не используем: пока идёт
            // конвейер, снапшот успевает отстать на этап ещё до открытия страницы.
            if (!forceRefresh && !isProjectRunning(id)) {
                const cached = _cacheGet('project', cacheKey);
                if (cached) { currentProject.value = cached; projectLoading.value = false; return; }
            }
            projectLoading.value = true;
            try {
                // Версии (нужны для дропдауна и определения latest) и карточку
                // проекта грузим ПАРАЛЛЕЛЬНО: endpoint /projects/{id} не зависит
                // от списка версий, поэтому шапка появляется за один round-trip,
                // а не за два последовательных (раньше versions блокировали
                // отрисовку карточки).
                const [, project] = await Promise.all([
                    loadProjectVersions(id),
                    api(`/projects/${encodeURIComponent(id)}`),
                ]);
                // Пока ждали ответ, пользователь мог уйти на другой проект —
                // тогда наш результат устарел, не затираем актуальный.
                if (_mySeq !== _projectLoadSeq) return;
                // V2-leak fix: legacy webapp игнорирует ?version_id= в
                // /api/projects/{id} → возвращает V1 счётчики/pipeline даже
                // на V2 запрос. Для V2+ на legacy runner обнуляем поля,
                // чтобы UI вкладок ("Замечания: 2") не показывал V1 данные
                // как V2.
                if (
                    activeVersionId.value && activeVersionId.value !== 'v1'
                    && !serverCaps.v2AuditSupported
                ) {
                    project.findings_count = 0;
                    project.optimization_count = 0;
                    project.block_count = 0;
                    project.findings_by_severity = {};
                    project.optimization_by_type = {};
                    project.optimization_savings_pct = 0;
                }
                currentProject.value = project;
                _cacheSet('project', cacheKey, currentProject.value);
                loadResumeInfo(id);
                fetchProjectUsage(id);  // загрузить детальный usage
                // Migrated findings: для V2+ подгружаем отчёт (если есть).
                // Для V1 не дёргаем — там отчёта не бывает.
                if (activeVersionId.value && activeVersionId.value !== 'v1') {
                    loadMigratedFindingsReport(id, activeVersionId.value);
                } else {
                    _migratedReset();
                }
            } catch (e) {
                if (_mySeq !== _projectLoadSeq) return;
                console.error('Failed to load project:', e);
                currentProject.value = null;
            } finally {
                // Снимаем индикатор только для АКТУАЛЬНОЙ загрузки, чтобы не
                // погасить спиннер более свежего перехода.
                if (_mySeq === _projectLoadSeq) projectLoading.value = false;
            }
        }

        // Тихая перезагрузка карточки текущего проекта: без спиннера
        // «Загрузка проекта…» и без сброса currentProject на время запроса.
        // Используется live-обновлениями (смена этапа в poll, WS-reconnect),
        // где мигание всей страницы недопустимо.
        let _silentRefreshInFlight = false;
        async function refreshProjectCardSilently(pid) {
            if (_silentRefreshInFlight) return;
            _silentRefreshInFlight = true;
            try {
                const project = await api(`/projects/${encodeURIComponent(pid)}`);
                // Пока ждали ответ, пользователь мог уйти с карточки/проекта.
                if (currentView.value === 'project'
                    && currentProject.value
                    && currentProject.value.project_id === pid) {
                    currentProject.value = project;
                    const cacheKey = activeVersionId.value
                        ? `${pid}::${activeVersionId.value}`
                        : pid;
                    _cacheSet('project', cacheKey, project);
                }
            } catch (e) {
                // Фоновое обновление: ошибку не показываем, следующий тик повторит
            } finally {
                _silentRefreshInFlight = false;
            }
        }

        // ─── Версионность проекта: загрузка / создание / upload ────────
        async function loadProjectVersions(projectId, opts) {
            opts = opts || {};
            projectVersionsLoading.value = true;
            try {
                // Этот endpoint не зависит от activeVersionId — сам возвращает
                // все версии проекта.
                const data = await api(
                    `/projects/${encodeURIComponent(projectId)}/versions`,
                    { withVersion: false },
                );
                projectVersions.value = data.versions || [];
                // Если activeVersionId не задан или невалиден — выставляем latest.
                const ids = new Set(projectVersions.value.map(v => v.version_id));
                if (!activeVersionId.value || !ids.has(activeVersionId.value)) {
                    activeVersionId.value = data.latest_version_id || 'v1';
                }
                if (opts.loadFiles && activeVersionId.value) {
                    await loadVersionFiles(projectId, activeVersionId.value);
                }
                return data;
            } catch (e) {
                console.error('Failed to load versions:', e);
                projectVersions.value = [];
                return null;
            } finally {
                projectVersionsLoading.value = false;
            }
        }

        async function loadVersionFiles(projectId, versionId) {
            try {
                const data = await api(
                    `/projects/${encodeURIComponent(projectId)}/versions/${encodeURIComponent(versionId)}/files`,
                    { withVersion: false },
                );
                versionFiles.value = data.files || [];
                return data;
            } catch (e) {
                console.error('Failed to load version files:', e);
                versionFiles.value = [];
                return null;
            }
        }

        function selectVersion(versionId) {
            if (!currentProjectId.value || activeVersionId.value === versionId) return;
            // Очищаем кеши проектных данных, чтобы при переключении V2→V1
            // не мигал старый V2 контент (см. ТЗ).
            _cacheInvalidate('project');
            _cacheInvalidate('findings');
            _cacheInvalidate('optimization');
            _cacheInvalidate('blocks');
            currentProject.value = null;
            findingsData.value = null;
            _migratedReset();
            activeVersionId.value = versionId;
            // Синхронизируем URL: добавляем/обновляем ?version_id=
            const hash = window.location.hash.slice(1) || '/';
            const qIdx = hash.indexOf('?');
            const path = qIdx >= 0 ? hash.slice(0, qIdx) : hash;
            window.location.hash = window.VersionAPI
                ? window.VersionAPI.buildHashRoute(path, versionId)
                : path + '?version_id=' + encodeURIComponent(versionId);
        }

        async function deleteVersion(versionId) {
            const pid = currentProject.value?.project_id;
            if (!pid) return;
            const verLabel = projectVersions.value.find(v => v.version_id === versionId)?.label || versionId;
            if (!confirm(`Удалить версию ${verLabel} проекта "${currentProject.value?.name}"?\n\nБудут удалены:\n- Вся папка версии (PDF, MD, результаты аудита)\n- Запись о версии из манифеста\n\nДействие необратимо.`)) return;
            try {
                const resp = await fetch(`/api/projects/${encodeURIComponent(pid)}/versions/${encodeURIComponent(versionId)}`, { method: 'DELETE' });
                const data = await resp.json();
                if (!resp.ok) { alert(data.detail || 'Ошибка удаления версии'); return; }
                await refreshProjects();
                // Переключиться на новую latest версию
                const newLatest = data.new_latest_version_id;
                if (newLatest) selectVersion(newLatest);
            } catch (e) { alert(e.message); }
        }

        // ─── Переименование папки проекта ───
        function startRename() {
            if (!currentProject.value) return;
            renameValue.value = currentProject.value.name || currentProjectId.value || '';
            renameError.value = '';
            renameEditing.value = true;
            nextTick(() => {
                try {
                    const el = renameInput.value;
                    if (el) { el.focus(); el.select(); }
                } catch (e) { /* noop */ }
            });
        }

        function cancelRename() {
            renameEditing.value = false;
            renameError.value = '';
            renameValue.value = '';
        }

        async function submitRename() {
            if (renameBusy.value) return;
            const newName = (renameValue.value || '').trim();
            if (!newName) { renameError.value = 'Имя не может быть пустым'; return; }
            if (newName === (currentProject.value && currentProject.value.name)) {
                cancelRename();
                return;
            }
            if (newName.includes('/') || newName.includes('\\')) {
                renameError.value = "Имя не может содержать '/' или '\\'";
                return;
            }
            renameBusy.value = true;
            renameError.value = '';
            try {
                const data = await apiPatch(
                    `/projects/${encodeURIComponent(currentProjectId.value)}/rename`,
                    { name: newName },
                    { withVersion: false },
                );
                renameBusy.value = false;
                renameEditing.value = false;
                // project_id сменился (basename папки) → controlled-навигация на
                // новый проект с перезагрузкой данных (старые пути больше не валидны).
                const newId = data && data.project_id;
                if (newId && newId !== currentProjectId.value) {
                    _cacheInvalidate('project');
                    _cacheInvalidate('findings');
                    _cacheInvalidate('optimization');
                    _cacheInvalidate('blocks');
                    currentProject.value = null;
                    findingsData.value = null;
                    activeVersionId.value = null;
                    navigate('/project/' + newId);
                } else {
                    // basename не изменился — обновим имя на месте.
                    if (currentProject.value) currentProject.value.name = data.new_name;
                    await loadProject(currentProjectId.value, true);
                }
                if (data && Array.isArray(data.warnings) && data.warnings.length) {
                    console.warn('[rename] warnings:', data.warnings);
                }
            } catch (e) {
                renameBusy.value = false;
                renameError.value = e.message || 'Ошибка переименования';
            }
        }

        async function uploadFilesToVersion(filesList, replaceExisting) {
            if (!currentProjectId.value || !activeVersionId.value) return;
            if (!filesList || !filesList.length) return;
            versionUploading.value = true;
            versionUploadError.value = '';
            try {
                const fd = new FormData();
                for (const f of filesList) fd.append('files', f, f.name);
                fd.append('replace_existing', replaceExisting ? 'true' : 'false');
                const pid = encodeURIComponent(currentProjectId.value);
                const vid = encodeURIComponent(activeVersionId.value);
                const resp = await fetch(`/api/projects/${pid}/versions/${vid}/files`, {
                    method: 'POST',
                    body: fd,
                });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    const msg = window.VersionAPI
                        ? window.VersionAPI.describeUploadError(resp.status, err.detail || '')
                        : (err.detail || `Ошибка ${resp.status}`);
                    versionUploadError.value = msg;
                    return null;
                }
                // Перезагрузка: список файлов + версии + статус проекта
                await loadVersionFiles(currentProjectId.value, activeVersionId.value);
                await loadProjectVersions(currentProjectId.value);
                await loadProject(currentProjectId.value, true);
                return await resp.json();
            } catch (e) {
                versionUploadError.value = e.message;
                return null;
            } finally {
                versionUploading.value = false;
            }
        }

        function handleUploadInput(event) {
            const files = Array.from(event.target.files || []);
            if (!files.length) return;
            uploadFilesToVersion(files, false);
            event.target.value = '';
        }

        function handleUploadInputReplace(event) {
            const files = Array.from(event.target.files || []);
            if (!files.length) return;
            uploadFilesToVersion(files, true);
            event.target.value = '';
        }

        // ─── Migrated findings (контроль ранее согласованных замечаний) ───

        function _migratedReset() {
            migratedFindingsReport.value = null;
            migratedFindingsError.value = '';
        }

        async function loadMigratedFindingsReport(projectId, versionId) {
            const pid = projectId || currentProjectId.value;
            const vid = versionId || activeVersionId.value;
            if (!pid || !vid) { _migratedReset(); return null; }
            // V1 — отчёта нет и быть не может; не дёргаем сеть.
            if (vid === 'v1') { _migratedReset(); return null; }
            migratedFindingsReportLoading.value = true;
            migratedFindingsError.value = '';
            try {
                const url = VAPI
                    ? VAPI.migratedFindingsReportUrl(pid, vid)
                    : `/api/projects/${encodeURIComponent(pid)}/versions/${encodeURIComponent(vid)}/migrated-findings/report`;
                const resp = await fetch(url);
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    migratedFindingsError.value = err.detail || `Ошибка ${resp.status}`;
                    migratedFindingsReport.value = null;
                    return null;
                }
                const data = await resp.json();
                // Бэкенд возвращает {exists, report, project_id, version_id}
                migratedFindingsReport.value = data && data.exists ? data.report : null;
                return migratedFindingsReport.value;
            } catch (e) {
                migratedFindingsError.value = e.message || String(e);
                migratedFindingsReport.value = null;
                return null;
            } finally {
                migratedFindingsReportLoading.value = false;
            }
        }

        function migratedStatusLabel(status) {
            return VAPI ? VAPI.formatMigratedStatusLabel(status) : (status || '—');
        }
        function migratedStatusTone(status) {
            return VAPI ? VAPI.formatMigratedStatusTone(status) : 'muted';
        }
        function findingMigratedBadge(f) {
            return VAPI ? VAPI.findingMigratedBadge(f) : null;
        }

        // ─── Computed-helpers для UI ───
        const activeVersionEntry = computed(() => {
            if (!activeVersionId.value) return null;
            return projectVersions.value.find(v => v.version_id === activeVersionId.value) || null;
        });

        // serverCaps определён выше (вместе с VAPI), чтобы быть доступным
        // и для api()-guard'а v2-стабов, и для canStartAuditNow.
        const canStartAuditNow = computed(() => {
            if (!window.VersionAPI) return { ok: true, reason: '' };
            // Для legacy V1 без manifest всё ещё работаем по has_pdf currentProject.
            if (!activeVersionEntry.value) {
                if (currentProject.value && currentProject.value.has_pdf) {
                    return { ok: true, reason: '' };
                }
                return { ok: false, reason: 'PDF не найден' };
            }
            return window.VersionAPI.canStartAudit(
                activeVersionEntry.value,
                { serverCaps },
            );
        });

        function versionBadgeFor(project) {
            return (window.VersionAPI && window.VersionAPI.formatVersionBadge)
                ? window.VersionAPI.formatVersionBadge(project)
                : null;
        }

        // ─── Finding → Block map ───
        const findingBlockMap = ref({});   // {finding_id: [block_ids]}
        const findingBlockInfo = ref({});  // {block_id: {block_id, page, ocr_label}}
        const findingTextEvidence = ref({}); // {finding_id: [{text_block_id, role, text, page}]}
        const expandedFindingId = ref(null); // какой finding сейчас раскрыт

        async function loadFindingBlockMap(id) {
            try {
                const data = await api(`/findings/${id}/block-map`);
                findingBlockMap.value = data.block_map || {};
                findingBlockInfo.value = data.block_info || {};
                findingTextEvidence.value = data.text_evidence || {};
            } catch (e) {
                findingBlockMap.value = {};
                findingBlockInfo.value = {};
                findingTextEvidence.value = {};
            }
        }

        function toggleFindingBlocks(findingId) {
            expandedFindingId.value = expandedFindingId.value === findingId ? null : findingId;
        }

        function getFindingBlocks(findingId) {
            const blockIds = findingBlockMap.value[findingId] || [];
            return blockIds.map(bid => findingBlockInfo.value[bid] || { block_id: bid, page: null, ocr_label: '' });
        }

        function getFindingTextEvidence(findingId) {
            return findingTextEvidence.value[findingId] || [];
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

        // Полные данные findings (без фильтрации) — для client-side фильтрации
        const _findingsAll = ref(null);

        // ─── Inline Critic v2 для обычной таблицы Замечаний (experimental) ───
        // Карта bareFindingId → cv2 item. Production pipeline не трогаем —
        // только дополнительный fetch для отображения display-бейджа.
        const findingsCv2Map = ref({});           // { 'F-001': {tab, queue, score, ...}, ... }
        const findingsCv2Available = ref(false);  // true если endpoint вернул items
        const findingsCv2Warning = ref('');       // warning из endpoint (нет данных по проекту)
        const findingsCv2Loading = ref(false);    // pending state, рисуем "загрузка..." в фильтрах
        const cv2ShowHidden = ref(false);         // toggle "показать скрытые Critic v2"
        const cv2DisplayFilter = ref('');         // bucket key или '' = все

        // Session-scoped cache: { [projectId]: { map, available, warning } }
        // Инвалидируется при manual reload (loadFindings forceRefresh) или
        // при F5. Перезагрузка страницы — ОК, кеш бэкенда переживает.
        const _findingsCv2SessionCache = {};

        // Deferred-runner: обычная таблица должна отрендериться сначала.
        // Критик загружается в idle-callback, чтобы не конкурировать с DOM.
        function _scheduleIdle(fn) {
            if (typeof window !== 'undefined' && typeof window.requestIdleCallback === 'function') {
                window.requestIdleCallback(fn, { timeout: 1500 });
            } else {
                setTimeout(fn, 0);
            }
        }

        function _applyCv2Result(projectId, payload) {
            // Применяем результат к state — но только если проект всё ещё актуален
            // (юзер мог уйти на другой проект, пока fetch висел в воздухе).
            if (currentProjectId.value && currentProjectId.value !== projectId) return;
            findingsCv2Map.value = payload.map || {};
            findingsCv2Available.value = !!payload.available;
            findingsCv2Warning.value = payload.warning || '';
            findingsCv2Loading.value = false;
            _applyFindingsFilter();
        }

        async function _fetchCriticV2ForFindings(projectId) {
            // Read-only fetch. Не пишем файлов, не вызываем LLM, production не трогаем.
            // Возвращает payload {map, available, warning}.
            try {
                const resp = await fetch('/api/critic-v2/projects/' + encodeURIComponent(projectId) + '/triage-ui');
                if (!resp.ok) {
                    return { map: {}, available: false, warning: 'нет данных' };
                }
                const raw = await resp.json();
                const items = (raw && Array.isArray(raw.items)) ? raw.items : [];
                const warning = (raw && raw.warning) ? raw.warning : '';
                const map = {};
                for (const it of items) {
                    const bare = cv2BareFindingId(it.finding_id);
                    if (!bare) continue;
                    map[bare] = it;
                }
                return { map, available: items.length > 0, warning };
            } catch (e) {
                console.warn('[critic-v2 inline] load failed:', e);
                return { map: {}, available: false, warning: 'ошибка загрузки' };
            }
        }

        function _scheduleCriticV2Load(projectId, opts) {
            // opts.forceRefresh — пропустить session cache.
            const force = !!(opts && opts.forceRefresh);
            // Cache hit — мгновенно применяем, без сетевого вызова
            if (!force && _findingsCv2SessionCache[projectId]) {
                _applyCv2Result(projectId, _findingsCv2SessionCache[projectId]);
                return;
            }
            findingsCv2Loading.value = true;
            findingsCv2Warning.value = '';
            _scheduleIdle(async () => {
                // Между планированием и выполнением юзер мог уйти на другой проект
                if (currentProjectId.value && currentProjectId.value !== projectId) {
                    findingsCv2Loading.value = false;
                    return;
                }
                const payload = await _fetchCriticV2ForFindings(projectId);
                _findingsCv2SessionCache[projectId] = payload;
                _applyCv2Result(projectId, payload);
            });
        }

        let _findingsLoadSeq = 0;
        async function loadFindings(id, forceRefresh) {
            // Бампаем в самом начале (в т.ч. до cache-hit), чтобы ответ по
            // ранее открытому проекту, пришедший позже, не затёр таблицу.
            const _mySeq = ++_findingsLoadSeq;
            expandedFindingId.value = null;
            findingsPage.value = 1;
            // Сбрасываем inline-критика при смене проекта
            findingsCv2Map.value = {};
            findingsCv2Available.value = false;
            findingsCv2Warning.value = '';
            findingsCv2Loading.value = false;
            // Manual reload инвалидирует session-cache Critic v2 для этого проекта
            if (forceRefresh) {
                delete _findingsCv2SessionCache[id];
            }
            if (!forceRefresh) {
                const cached = _cacheGet('findings', id);
                if (cached) {
                    _findingsAll.value = cached;
                    _applyFindingsFilter();
                    // Critic v2 — deferred (idle), session-cached
                    _scheduleCriticV2Load(id, { forceRefresh: false });
                    return;
                }
            }
            findingsData.value = null;
            try {
                // Загружаем ВСЕ findings без фильтров — фильтруем на клиенте
                const data = await api(`/findings/${id}`);
                if (_mySeq !== _findingsLoadSeq) return;
                _findingsAll.value = data;
                _cacheSet('findings', id, data);
                _applyFindingsFilter();
                // Загрузить маппинг блоков параллельно
                loadFindingBlockMap(id);
                // Critic v2 — deferred (idle), session-cached, не блокирует таблицу
                _scheduleCriticV2Load(id, { forceRefresh: forceRefresh });
            } catch (e) {
                console.error('Failed to load findings:', e);
            }
        }

        function _applyFindingsFilter() {
            if (!_findingsAll.value) { findingsData.value = null; return; }
            const sev = filterSeverity.value;
            const search = filterSearch.value.toLowerCase();
            const cv2Map = findingsCv2Map.value || {};
            const cv2Has = findingsCv2Available.value;
            const showHidden = cv2ShowHidden.value;
            const displayFilter = cv2DisplayFilter.value;
            let items = _findingsAll.value.findings || [];
            if (sev) {
                items = items.filter(f => f.severity === sev);
            }
            if (search) {
                items = items.filter(f =>
                    (f.description || '').toLowerCase().includes(search) ||
                    (f.id || '').toLowerCase().includes(search) ||
                    (f.norm_ref || '').toLowerCase().includes(search) ||
                    (f.sub_findings || []).some(s => (s.problem || '').toLowerCase().includes(search))
                );
            }
            // Скрытие по Critic v2 — только если данные есть и юзер не открыл их явно
            if (cv2Has && !showHidden) {
                items = items.filter(f => {
                    const cv2 = cv2Map[f.id];
                    return !cv2 || !cv2IsHiddenByDefault(cv2);
                });
            }
            // Фильтр по bucket'у
            if (cv2Has && displayFilter) {
                items = items.filter(f => {
                    const cv2 = cv2Map[f.id];
                    if (!cv2) return false;
                    const score = cv2DisplayScore(cv2);
                    const b = cv2DisplayBucket(score);
                    return b && b.key === displayFilter;
                });
            }
            // Нормализуем сводку под legacy-контракт шаблона (строка «Всего:» и
            // бейджи): v2-canary исторически отдавал findings_count/findings_by_severity
            // вместо total/by_severity — поддерживаем обе формы.
            const src = _findingsAll.value;
            findingsData.value = {
                ...src,
                findings: items,
                total: src.total ?? src.findings_count ?? 0,
                by_severity: src.by_severity ?? src.findings_by_severity ?? {},
            };
        }

        // Сколько findings скрыто по умолчанию (для счётчика возле toggle).
        function cv2HiddenCount() {
            if (!findingsCv2Available.value || !_findingsAll.value) return 0;
            const cv2Map = findingsCv2Map.value || {};
            let n = 0;
            for (const f of (_findingsAll.value.findings || [])) {
                const cv2 = cv2Map[f.id];
                if (cv2 && cv2IsHiddenByDefault(cv2)) n += 1;
            }
            return n;
        }

        // Геттеры для шаблона: bare функции по id
        function findingCv2(id) {
            return (findingsCv2Map.value || {})[id] || null;
        }
        function findingCv2Score(id) {
            const it = findingCv2(id);
            return it ? cv2DisplayScore(it) : null;
        }
        function findingCv2Label(id) {
            const s = findingCv2Score(id);
            return s == null ? '' : cv2DisplayLabel(s);
        }
        function findingCv2Class(id) {
            const s = findingCv2Score(id);
            return s == null ? 'cv2-disp-na' : cv2DisplayClass(s);
        }
        function findingCv2Tooltip(id) {
            const it = findingCv2(id);
            if (!it) return '';
            const score = cv2DisplayScore(it);
            const lines = [
                'Critic v2 (экспериментально, замечания не удаляются)',
                'Оценка: ' + (score == null ? '—' : score) + ' (' + cv2DisplayLabel(score) + ')',
                'Очередь: ' + (CV2_LABELS.queue[it.queue] || it.queue || '—'),
            ];
            if (it.reason)            lines.push('Причина: ' + (CV2_LABELS.reason[it.reason] || it.reason));
            if (it.evidence_quality)  lines.push('Evidence: ' + (CV2_LABELS.evidence[it.evidence_quality] || it.evidence_quality));
            if (it.taxonomy_reason)   lines.push('Таксономия: ' + (CV2_LABELS.taxonomy[it.taxonomy_reason] || it.taxonomy_reason));
            if (it.source_dependency) lines.push('Источник: ' + (CV2_LABELS.source[it.source_dependency] || it.source_dependency));
            if (it.explanation)       lines.push('Пояснение: ' + cv2HumanizeExplanation(it.explanation));
            return lines.join('\n');
        }

        const evidenceValidationMap = ref({});
        const evidenceValidationAvailable = ref(false);
        const evidenceValidationLoading = ref(false);
        const evidenceValidationRunning = ref(false);

        async function _loadEvidenceValidation(projectId) {
            evidenceValidationLoading.value = true;
            try {
                const data = await api('/findings/' + encodeURIComponent(projectId) + '/evidence-validation');
                const map = {};
                for (const d of (data.decisions || [])) { map[d.finding_id] = d; }
                evidenceValidationMap.value = map;
                evidenceValidationAvailable.value = Object.keys(map).length > 0;
            } catch(e) {
                console.warn('[EV] load failed:', e);
                evidenceValidationAvailable.value = false;
                evidenceValidationMap.value = {};
            } finally {
                evidenceValidationLoading.value = false;
            }
        }

        async function runEvidenceValidation() {
            const id = currentProjectId.value;
            if (!id || evidenceValidationRunning.value) return;
            evidenceValidationRunning.value = true;
            try {
                await api('/findings/' + encodeURIComponent(id) + '/evidence-validation/run', { method: 'POST' });
                await _loadEvidenceValidation(id);
            } catch(e) {
                console.error('[EV] run failed:', e);
                alert('Ошибка запуска Evidence Verifier: ' + (e.message || e));
            } finally {
                evidenceValidationRunning.value = false;
            }
        }

        function findingEvDecision(fid) { return (evidenceValidationMap.value || {})[fid] || null; }
        function findingEvLabel(fid) {
            const d = findingEvDecision(fid);
            if (!d) return '';
            if (d.verification_path === 'skipped') return '—';
            const L = {accept:'принять',reject:'отклонить',borderline:'под вопросом',needs_human:'эксперт'};
            return L[d.llm_decision] || d.llm_decision;
        }
        function findingEvClass(fid) {
            const d = findingEvDecision(fid);
            if (!d || d.verification_path === 'skipped') return 'ev-val-na';
            const C = {accept:'ev-val-accept',reject:'ev-val-reject',borderline:'ev-val-border',needs_human:'ev-val-human'};
            return C[d.llm_decision] || 'ev-val-na';
        }
        function findingEvPathLabel(fid) {
            const d = findingEvDecision(fid);
            if (!d || !d.verification_path) return '';
            const P = {graphic:'графика',text:'текст',mixed:'смеш.',weak:'слабый',skipped:'пропуск'};
            return P[d.verification_path] || d.verification_path;
        }
        function findingEvTooltip(fid) {
            const d = findingEvDecision(fid);
            if (!d) return '';
            const lines = ['Evidence Verifier'];
            if (d.verification_path) lines.push('Путь: ' + findingEvPathLabel(fid));
            if (d.block_ids_used && d.block_ids_used.length) lines.push('Блоки: ' + d.block_ids_used.join(', '));
            lines.push('Решение: ' + d.llm_decision + ' (conf=' + (d.confidence || '?') + ')');
            if (d.explanation) lines.push(d.explanation.slice(0, 250));
            return lines.join('\n');
        }

        const kbValidationMap = ref({});
        const kbValidationAvailable = ref(false);

        const kbValidationLoading = ref(false);
        async function _loadKBValidation(projectId) {
            kbValidationLoading.value = true;
            try {
                const data = await api('/findings/' + encodeURIComponent(projectId) + '/kb-validation');
                const map = {};
                for (const d of (data.decisions || [])) { map[d.finding_id] = d; }
                kbValidationMap.value = map;
                kbValidationAvailable.value = Object.keys(map).length > 0;
            } catch(e) {
                console.warn('[KB-Agent] load failed:', e);
                kbValidationAvailable.value = false;
                kbValidationMap.value = {};
            } finally {
                kbValidationLoading.value = false;
            }
        }

        function findingKbDecision(id) { return (kbValidationMap.value || {})[id] || null; }
        function findingKbLabel(id) {
            const d = findingKbDecision(id);
            if (!d) return '';
            const L = {accept:'принять',reject:'отклонить',borderline:'под вопросом',needs_human:'эксперт'};
            return L[d.llm_decision] || d.llm_decision;
        }
        function findingKbClass(id) {
            const d = findingKbDecision(id);
            if (!d) return 'kb-val-na';
            const C = {accept:'kb-val-accept',reject:'kb-val-reject',borderline:'kb-val-border',needs_human:'kb-val-human'};
            return C[d.llm_decision] || 'kb-val-na';
        }
        function findingKbTooltip(id) {
            const d = findingKbDecision(id);
            if (!d) return '';
            const lines = ['KB-агент'];
            lines.push('Решение: ' + d.llm_decision + ' (conf=' + (d.confidence || '?') + ')');
            if (d.explanation) lines.push(d.explanation.slice(0, 250));
            return lines.join('\n');
        }

        // ─── Blocks (OCR) ───

        const blockFieldLabels = {
            designation: 'обозначение',
            description: 'описание',
            storeys: 'этажность',
            room_name: 'наименование помещения',
            room_no: 'номер',
            purpose: 'назначение',
            count: 'количество',
            grid_lines: 'оси',
            location: 'расположение',
            requirement_type: 'тип ссылки',
            requirement: 'требование',
            page: 'страница',
            sheet: 'лист',
            area_m2: 'площадь',
            length_mm: 'длина',
            width_mm: 'ширина',
            height_mm: 'высота',
            depth_mm: 'глубина',
            level: 'отметка',
            section: 'сечение',
            material: 'материал',
            mark: 'марка',
            floor: 'этаж',
            room: 'помещение',
            name: 'наименование',
            type: 'тип',
        };

        const blockFieldUnits = {
            area_m2: ' м²',
            length_mm: ' мм',
            width_mm: ' мм',
            height_mm: ' мм',
            depth_mm: ' мм',
            storeys: ' эт.',
        };

        function isBlockPlainObject(value) {
            return !!value && typeof value === 'object' && !Array.isArray(value);
        }

        function normalizeBlockText(value) {
            return String(value ?? '').replace(/\s+/g, ' ').trim();
        }

        function tryParseBlockJsonLike(value) {
            if (typeof value !== 'string') return value;
            const raw = value.trim();
            if (!raw || !/^[\[{]/.test(raw)) return value;
            try {
                return JSON.parse(raw);
            } catch {
                return value;
            }
        }

        function humanizeBlockFieldKey(key) {
            const raw = normalizeBlockText(key);
            if (!raw) return '';
            const lower = raw.toLowerCase();
            if (blockFieldLabels[lower]) return blockFieldLabels[lower];
            const tokens = lower.split(/[_\-.]+/).filter(Boolean);
            if (!tokens.length) return raw;
            const translated = tokens.map((token) => blockFieldLabels[token] || token);
            const label = translated.join(' ');
            return label ? label.charAt(0).toUpperCase() + label.slice(1) : raw;
        }

        function replaceEmbeddedBlockFieldLabels(text) {
            let result = normalizeBlockText(text);
            if (!result) return '';
            result = result.replace(/^Прочее\s+/i, '');
            for (const [key, label] of Object.entries(blockFieldLabels)) {
                result = result.replace(new RegExp(`\\b${key}\\b(?=\\s*:)`, 'gi'), label);
            }
            return result;
        }

        function formatBlockScalar(key, value) {
            if (value === null || value === undefined || value === '') return '';
            if (typeof value === 'boolean') return value ? 'да' : 'нет';
            if (typeof value === 'number') {
                const text = Number.isInteger(value) ? value.toLocaleString('ru-RU') : String(value);
                const unit = blockFieldUnits[String(key || '').toLowerCase()] || '';
                return unit ? `${text}${unit}` : text;
            }
            let text = replaceEmbeddedBlockFieldLabels(value);
            if (!text) return '';
            const unit = blockFieldUnits[String(key || '').toLowerCase()] || '';
            if (unit && !text.endsWith(unit)) text += unit;
            return text;
        }

        function flattenBlockValuePairs(value, path = []) {
            const parsed = tryParseBlockJsonLike(value);
            if (parsed === null || parsed === undefined) return [];

            if (Array.isArray(parsed)) {
                if (!parsed.length) return [];
                const pairs = [];
                const scalars = [];
                for (const item of parsed.slice(0, 10)) {
                    const inner = tryParseBlockJsonLike(item);
                    if (Array.isArray(inner) || isBlockPlainObject(inner)) {
                        pairs.push(...flattenBlockValuePairs(inner, path));
                    } else {
                        const text = formatBlockScalar(path[path.length - 1], inner);
                        if (text) scalars.push(text);
                    }
                }
                if (scalars.length) pairs.unshift([path, scalars.join(', ')]);
                return pairs;
            }

            if (isBlockPlainObject(parsed)) {
                const pairs = [];
                for (const [childKey, childValue] of Object.entries(parsed)) {
                    pairs.push(...flattenBlockValuePairs(childValue, [...path, String(childKey)]));
                }
                return pairs;
            }

            const text = formatBlockScalar(path[path.length - 1], parsed);
            return text ? [[path, text]] : [];
        }

        function labelBlockPath(path = []) {
            const parts = path
                .map((part) => normalizeBlockText(part))
                .filter((part) => part && !/^\d+$/.test(part))
                .map((part) => humanizeBlockFieldKey(part));
            if (!parts.length) return '';
            const [head, ...tail] = parts;
            const normalizedHead = head ? head.charAt(0).toUpperCase() + head.slice(1) : '';
            return tail.length ? `${normalizedHead}: ${tail.join(' / ')}` : normalizedHead;
        }

        function blockPairsToKvItems(pairs = []) {
            const items = [];
            for (const [path, text] of pairs) {
                if (!text) continue;
                const label = labelBlockPath(path);
                if (label) items.push({ key: label, value: text });
                else items.push(text);
            }
            return items;
        }

        function formatBlockInlineValue(value, key = '') {
            const parsed = tryParseBlockJsonLike(value);
            if (Array.isArray(parsed) || isBlockPlainObject(parsed)) {
                return flattenBlockValuePairs(parsed)
                    .map(([path, text]) => {
                        const label = labelBlockPath(path);
                        return label ? `${label}: ${text}` : text;
                    })
                    .filter(Boolean)
                    .join('; ');
            }
            if (typeof parsed === 'string') {
                return parsed
                    .split(/\r?\n/)
                    .map((line) => replaceEmbeddedBlockFieldLabels(line))
                    .filter(Boolean)
                    .join('; ');
            }
            return formatBlockScalar(key, parsed);
        }

        function formatBlockSummaryValue(value) {
            const parsed = tryParseBlockJsonLike(value);
            if (Array.isArray(parsed) || isBlockPlainObject(parsed)) {
                return flattenBlockValuePairs(parsed)
                    .map(([path, text]) => {
                        const label = labelBlockPath(path);
                        return label ? `${label}: ${text}` : text;
                    })
                    .filter(Boolean)
                    .join('\n');
            }
            if (typeof parsed === 'string') {
                return parsed
                    .split(/\r?\n/)
                    .map((line) => replaceEmbeddedBlockFieldLabels(line))
                    .filter(Boolean)
                    .join('\n');
            }
            return formatBlockScalar('', parsed);
        }

        function normalizeBlockEntityCaption(text) {
            const normalized = replaceEmbeddedBlockFieldLabels(text);
            return normalized.replace(/^Прочее\s+/i, '');
        }

        function normalizeBlockKvItems(items) {
            const parsed = tryParseBlockJsonLike(items);
            if (parsed === null || parsed === undefined) return [];
            if (isBlockPlainObject(parsed)) return blockPairsToKvItems(flattenBlockValuePairs(parsed));

            if (!Array.isArray(parsed)) {
                const text = formatBlockInlineValue(parsed);
                return text ? [text] : [];
            }

            const normalized = [];
            for (const item of parsed) {
                const parsedItem = tryParseBlockJsonLike(item);
                if (parsedItem === null || parsedItem === undefined) continue;

                if (isBlockPlainObject(parsedItem)) {
                    const rawKey = parsedItem.key || parsedItem.name || '';
                    if (Object.prototype.hasOwnProperty.call(parsedItem, 'value') || Object.prototype.hasOwnProperty.call(parsedItem, 'val') || rawKey) {
                        let key = normalizeBlockEntityCaption(rawKey);
                        if (key && /^[A-Za-z0-9_.-]+$/.test(key)) {
                            key = humanizeBlockFieldKey(key);
                        }
                        const valueKey = rawKey && /^[A-Za-z0-9_.-]+$/.test(rawKey) ? rawKey : '';
                        const valueText = formatBlockInlineValue(
                            Object.prototype.hasOwnProperty.call(parsedItem, 'value') ? parsedItem.value : parsedItem.val,
                            valueKey
                        );
                        if (key && valueText) normalized.push({ key, value: valueText });
                        else if (key) normalized.push(key);
                        else if (valueText) normalized.push(valueText);
                        continue;
                    }

                    normalized.push(...blockPairsToKvItems(flattenBlockValuePairs(parsedItem)));
                    continue;
                }

                if (Array.isArray(parsedItem)) {
                    normalized.push(...blockPairsToKvItems(flattenBlockValuePairs(parsedItem)));
                    continue;
                }

                const text = formatBlockInlineValue(parsedItem);
                if (text) normalized.push(text);
            }
            return normalized;
        }

        function normalizeBlockAnalysisRecord(entry) {
            if (!isBlockPlainObject(entry)) return entry;
            return {
                ...entry,
                label: normalizeBlockText(entry.label || ''),
                summary: formatBlockSummaryValue(entry.summary),
                key_values_read: normalizeBlockKvItems(entry.key_values_read),
            };
        }

        async function loadBlocks(id) {
            blocksProjectId.value = id;
            selectedBlock.value = null;
            // Каждый обычный вход в раздел начинается с полного корпуса блоков.
            // Переход к конкретному блоку из замечания ниже штатно переопределит
            // этот выбор после загрузки данных.
            selectedBlockPage.value = 'all';
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
            } catch (e) {
                console.error('Failed to load blocks:', e);
                blockPages.value = [];
            }
        }

        async function loadBlockAnalysis(id) {
            try {
                const data = await api(`/tiles/${id}/blocks/analysis`);
                const normalized = {};
                for (const [blockId, entry] of Object.entries(data.blocks || {})) {
                    normalized[blockId] = normalizeBlockAnalysisRecord(entry);
                }
                blockAnalysis.value = normalized;
            } catch (e) {
                blockAnalysis.value = {};
            }
        }

        // Классификация блоков по статусам из /blocks/analysis:
        //   no_findings — проанализирован сам, замечаний не выявлено
        //   skipped     — алгоритм не включал в анализ (без значимого содержимого)
        //   merged_into — свёрнут в родительский page/quadrant PNG
        // Раздел "Без сущностей" = no_findings + skipped (два подсписка)
        const noFindingsBlocksList = computed(() => {
            if (!blockPages.value.length) return [];
            const result = [];
            for (const pg of blockPages.value) {
                for (const b of (pg.blocks || [])) {
                    const an = blockAnalysis.value[b.block_id];
                    if (an && an.status === 'no_findings') result.push(b);
                }
            }
            return result;
        });

        const skippedBlocksList = computed(() => {
            if (!blockPages.value.length) return [];
            const result = [];
            for (const pg of blockPages.value) {
                for (const b of (pg.blocks || [])) {
                    const an = blockAnalysis.value[b.block_id];
                    if (an && an.status === 'skipped') result.push(b);
                }
            }
            return result;
        });

        // Алиас для обратной совместимости со счётчиком на кнопке "Без сущностей"
        const emptyBlocksList = computed(() =>
            [...noFindingsBlocksList.value, ...skippedBlocksList.value]
        );

        // Все блоки всех страниц подряд (для чипа "Все блоки" — просмотр за раз)
        const allBlocksList = computed(() => {
            if (!blockPages.value.length) return [];
            const result = [];
            for (const pg of blockPages.value) {
                for (const b of (pg.blocks || [])) result.push(b);
            }
            return result;
        });

        const currentPageBlocks = computed(() => {
            if (!blockPages.value.length) return null;
            // Виртуальная страница "Все блоки" — плоский список всех блоков
            if (selectedBlockPage.value === 'all') {
                return { page_num: 'all', blocks: allBlocksList.value };
            }
            // Виртуальная страница "Без сущностей" — плоский список для совместимости с prev/next навигацией
            if (selectedBlockPage.value === 'empty') {
                return { page_num: 'empty', blocks: emptyBlocksList.value };
            }
            if (!selectedBlockPage.value) return null;
            return blockPages.value.find(p => p.page_num === selectedBlockPage.value) || null;
        });

        // Статусные хелперы для рендера бейджей/карточек.
        function blockStatus(blockId) {
            const an = blockAnalysis.value[blockId];
            return (an && an.status) || null;
        }
        function blockHasNoVectorGraph(block) {
            return !!block && block.vector_text_available === false;
        }
        function blockParentId(blockId) {
            const an = blockAnalysis.value[blockId];
            return (an && an.parent_block_id) || null;
        }
        function blockMergedBadge(blockId) {
            // Человекочитаемая метка для merged_into: "В составе стр. 11 (четверть TL)"
            const parent = blockParentId(blockId);
            if (!parent) return '';
            // Разбираем parent вида "page_011_TL" или "page_008"
            const m = parent.match(/^page_(\d+)(?:_(TL|TR|BL|BR))?$/);
            if (!m) return `В составе ${parent}`;
            const pageNum = parseInt(m[1], 10);
            const quad = m[2];
            return quad ? `В составе стр. ${pageNum} (четверть ${quad})` : `В составе стр. ${pageNum}`;
        }
        function blockOriginalLabel(blockId) {
            const an = blockAnalysis.value[blockId];
            return (an && an.original_ocr_label) || '';
        }

        // Плоский список блоков в контексте текущей страницы (для prev/next навигации в overlay)
        const currentBlocksList = computed(() => {
            const pg = currentPageBlocks.value;
            return (pg && pg.blocks) ? pg.blocks : [];
        });

        const currentBlockIndex = computed(() => {
            if (!selectedBlock.value) return -1;
            const bid = selectedBlock.value.block_id;
            return currentBlocksList.value.findIndex(b => b.block_id === bid);
        });

        function navigateBlock(delta) {
            const list = currentBlocksList.value;
            if (!list.length) return;
            const idx = currentBlockIndex.value;
            if (idx < 0) return;
            const next = idx + delta;
            if (next < 0 || next >= list.length) return;
            openBlock(list[next]);
        }

        function openBlock(block) {
            selectedBlock.value = block;
            // txt-режим переживает навигацию: при открытии нового блока подгружаем его текст
            blockLlmText.value = null;
            blockLlmTextError.value = '';
            if (blockHasNoVectorGraph(block)) {
                // Для растрового блока TXT/области не имеют содержимого и не
                // должны переживать переход с предыдущего векторного блока.
                showBlockLlmText.value = false;
                showBlockRegions.value = false;
            } else if (showBlockLlmText.value || showBlockRegions.value) {
                loadBlockLlmText();
            }
            resetBlockZoom();
        }

        // Загрузить текст блока, реально уходящий в нейронку (Stage 01)
        // Кэш ответов llm-text по блоку: повторное открытие панели «txt»
        // не делает лишний запрос, а переключение блоков не показывает
        // текст предыдущего блока (ключ = проект|версия|блок).
        const blockLlmTextCache = new Map();

        function blockLlmTextCacheKey(block) {
            return [blocksProjectId.value || '', activeVersionId.value || '',
                    block && block.block_id || ''].join('|');
        }

        async function loadBlockLlmText() {
            const block = selectedBlock.value;
            if (!block) return;
            if (blockHasNoVectorGraph(block)) {
                showBlockLlmText.value = false;
                blockLlmText.value = null;
                blockLlmTextError.value = '';
                return;
            }
            const cacheKey = blockLlmTextCacheKey(block);
            if (blockLlmTextCache.has(cacheKey)) {
                blockLlmText.value = blockLlmTextCache.get(cacheKey);
                blockLlmTextError.value = '';
                return;
            }
            blockLlmText.value = null; // не показывать текст предыдущего блока
            blockLlmTextLoading.value = true;
            blockLlmTextError.value = '';
            try {
                // version_id ОБЯЗАТЕЛЕН: без него бэкенд резолвит latest-версию (напр. свежую V2
                // без вектор-слоя блока) → пустой enrichment и singleline_graph=null (граф схемы
                // пропадает), хотя в UI выбрана V1. Тот же класс бага, что чинили в blockImgUrl.
                const params = new URLSearchParams();
                if (block.page != null) params.set('page', block.page);
                const vid = activeVersionId.value;
                if (vid) params.set('version_id', vid);
                const qs = params.toString();
                const url = '/api/tiles/' + blocksProjectId.value + '/blocks/llm-text/' + block.block_id
                          + (qs ? '?' + qs : '');
                const resp = await fetch(url);
                if (!resp.ok) throw new Error('HTTP ' + resp.status);
                const payload = await resp.json();
                if (payload.vector_text_available === false) {
                    // Защита для старого списка блоков без заранее рассчитанного
                    // признака: endpoint является окончательным источником истины.
                    block.vector_text_available = false;
                    block.vector_graph_source_kind = payload.block_graph_package
                        ? payload.block_graph_package.source_kind : null;
                    block.vector_graph_message = payload.vector_graph_message
                        || 'Векторный граф блока отсутствует';
                    showBlockLlmText.value = false;
                    showBlockRegions.value = false;
                    blockLlmText.value = null;
                    return;
                }
                blockLlmText.value = payload;
                blockLlmTextCache.set(cacheKey, payload);
            } catch (e) {
                blockLlmText.value = null;
                blockLlmTextError.value = 'Не удалось загрузить текст блока: ' + (e.message || e);
            } finally {
                blockLlmTextLoading.value = false;
            }
        }

        function toggleBlockLlmText() {
            if (blockHasNoVectorGraph(selectedBlock.value)) {
                showBlockLlmText.value = false;
                blockLlmText.value = null;
                return;
            }
            showBlockLlmText.value = !showBlockLlmText.value;
            if (showBlockLlmText.value) {
                showBlockRegions.value = false; // txt и области взаимоисключающие
                const cur = selectedBlock.value;
                if (!blockLlmText.value
                        || String(blockLlmText.value.block_id) !== String(cur && cur.block_id)) {
                    loadBlockLlmText();
                }
            }
        }

        // Полное профильное Markdown-описание блока (shadow-профиль, напр.
        // «АР. План потолков и освещения»): рендер как форматированный документ.
        // Санитизация: исходный HTML экранируется ДО marked.parse (разметку
        // строит только сам markdown), затем страховочно вырезаются script/
        // iframe, on*-атрибуты и javascript:-ссылки.
        function renderMarkdownSafe(text) {
            if (!text) return '';
            const escaped = String(text)
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;');
            let html;
            if (typeof marked !== 'undefined') {
                try {
                    html = marked.parse(escaped, { breaks: false, gfm: true });
                } catch (e) {
                    html = escaped.replace(/\n/g, '<br>');
                }
            } else {
                html = escaped.replace(/\n/g, '<br>');
            }
            return html
                .replace(/<\s*(script|iframe|object|embed|form)[^>]*>/gi, '')
                .replace(/<\s*\/\s*(script|iframe|object|embed|form)\s*>/gi, '')
                .replace(/\son\w+\s*=\s*("[^"]*"|'[^']*'|[^\s>]+)/gi, '')
                .replace(/(href|src)\s*=\s*(["']?)\s*javascript:[^"'\s>]*\2/gi, '$1="#"');
        }

        // Режим профильного описания: 'audit' — краткий контекст для поиска
        // замечаний (по умолчанию), 'detail' — подробное поквартирное описание.
        // Полный технический рендер в UI не показывается (доступен через API).
        const blockMdMode = ref('audit');

        function setBlockMdMode(mode) {
            blockMdMode.value = mode === 'detail' ? 'detail' : 'audit';
        }

        const blockProfiledMarkdownHtml = computed(() => {
            const payload = blockLlmText.value;
            if (!payload) return '';
            // «Подробно» → compact; «Аудит» → audit. Fallback: audit → compact → full
            const md = blockMdMode.value === 'detail'
                ? (payload.profiled_graph_markdown_compact || payload.profiled_graph_markdown_audit
                   || payload.profiled_graph_markdown_full)
                : (payload.profiled_graph_markdown_audit || payload.profiled_graph_markdown_compact
                   || payload.profiled_graph_markdown_full);
            if (!md) return '';
            return renderMarkdownSafe(md);
        });

        // Полупрозрачные области линий поверх блока — визуальная проверка связи данных
        const showBlockRegions = ref(false);
        const blockRegionRects = computed(() => {
            const g = blockLlmText.value && blockLlmText.value.singleline_graph;
            if (!g || !g.panels) return [];
            const cl = v => Math.max(0, Math.min(1, v));
            const out = [];
            for (const pan of g.panels) {
                for (const f of (pan.feeders || [])) {
                    // области линии — список частей (текст + автомат); fallback на одиночный polygon/bbox
                    let polys = null;
                    if (f.polygons && f.polygons.length) {
                        polys = f.polygons.filter(p => p && p.length >= 3).map(p => p.map(q => [cl(q[0]), cl(q[1])]));
                    } else if (f.polygon && f.polygon.length >= 3) {
                        polys = [f.polygon.map(p => [cl(p[0]), cl(p[1])])];
                    } else if (f.bbox && f.bbox.length === 4) {
                        const x0 = cl(f.bbox[0]), y0 = cl(f.bbox[1]), x1 = cl(f.bbox[2]), y1 = cl(f.bbox[3]);
                        polys = [[[x0, y0], [x1, y0], [x1, y1], [x0, y1]]];
                    }
                    if (polys && polys.length) {
                        out.push({ qf: f.qf, consumer: f.consumer || '', status: f.status,
                                   polys: polys, labelX: polys[0][0][0], labelY: polys[0][0][1] });
                    }
                }
            }
            return out;
        });
        // Пространственные группы текста блока (кнопка «области») — чисто геометрия вектор-слоя,
        // bbox нормирован к региону блока (совпадает с region-image). Работает на ЛЮБОМ блоке с
        // вектор-слоем. Цвет группы — золотой угол по номеру (различимые оттенки для десятков групп).
        const blockTextGroupRects = computed(() => {
            const tg = blockLlmText.value && blockLlmText.value.text_groups;
            if (!tg || !tg.length) return [];
            const cl = v => Math.max(0, Math.min(1, v));
            return tg.map(g => {
                const b = g.bbox || [0, 0, 0, 0];
                const x0 = cl(b[0]), y0 = cl(b[1]), x1 = cl(b[2]), y1 = cl(b[3]);
                const hue = (g.n * 137.508) % 360;
                return {
                    n: g.n,
                    poly: [[x0, y0], [x1, y0], [x1, y1], [x0, y1]],
                    color: `hsl(${hue} 72% 45%)`,
                    single: (g.natoms || 1) < 2,
                    text: (g.text || []).join('\n'),
                    natoms: g.natoms || 1,
                    labelX: x0, labelY: y0,
                };
            });
        });

        function toggleBlockRegions() {
            showBlockRegions.value = !showBlockRegions.value;
            if (showBlockRegions.value) {
                showBlockLlmText.value = false; // показываем картинку, а не текст
                if (!blockLlmText.value || !blockLlmText.value.singleline_graph) loadBlockLlmText();
            }
        }

        // URL картинки блока с активной версией. Сырые `<img :src>` в шаблоне НЕ
        // проходят через api()/_apiUrl, поэтому ?version_id нужно добавлять здесь
        // вручную. Иначе бэкенд резолвит ТЕКУЩУЮ версию документа (например,
        // свежезалитую V2 без прогнанного аудита) → у неё нет папки блоков → 404
        // → пустые превью в сетке, хотя список блоков (через api()) грузится.
        function blockImgUrl(pid, blockId, kind = 'image') {
            const base = '/api/tiles/' + pid + '/blocks/' + kind + '/' + blockId;
            const vid = activeVersionId.value;
            return vid ? base + '?version_id=' + encodeURIComponent(vid) : base;
        }

        // База картинки блока: region-image (рендер из fitz) — ТОЛЬКО для областей ЛИНИЙ однолинейки
        // (их bbox из геометрии fitz-страницы, совпадает только с fitz-рендером). Для групп ТЕКСТА
        // и обычного вида — штатный кроп Chandra: группы текста нормированы к coords_norm блока и
        // ложатся на обычную картинку (аспект совпадает), поэтому подменять её НЕ нужно — иначе блок
        // «меняется» на region-image (который к тому же мог рендерить не ту страницу).
        const blockImageSrc = computed(() => {
            const b = selectedBlock.value;
            if (!b) return '';
            const needsRegionRender = showBlockRegions.value
                && blockRegionRects.value.length && !b.region_image_uses_crop;
            const kind = needsRegionRender ? 'region-image' : 'image';
            return blockImgUrl(blocksProjectId.value, b.block_id, kind);
        });

        // Рассчитать scale и offset для вписывания картинки в контейнер
        function computeFit() {
            const container = blockImageContainer.value;
            if (!container || !blockNatW.value || !blockNatH.value) return;
            const cw = container.clientWidth - 32;  // padding 16*2
            const ch = container.clientHeight - 48; // padding + label
            const scaleX = cw / blockNatW.value;
            const scaleY = ch / blockNatH.value;
            blockBaseScale.value = Math.min(scaleX, scaleY, 1); // не больше 1:1
        }

        function onBlockImageLoad(e) {
            const img = e.target;
            blockNatW.value = img.naturalWidth;
            blockNatH.value = img.naturalHeight;
            Vue.nextTick(() => {
                computeFit();
                // Центрировать изображение в контейнере
                centerBlockImage();
            });
        }

        function centerBlockImage() {
            const container = blockImageContainer.value;
            if (!container) return;
            const cw = container.clientWidth;
            const ch = container.clientHeight - 30; // label
            const scale = blockBaseScale.value * blockZoom.value;
            const imgW = blockNatW.value * scale;
            const imgH = blockNatH.value * scale;
            blockPanX.value = (cw - imgW) / 2;
            blockPanY.value = (ch - imgH) / 2;
        }

        const blockImageStyle = computed(() => {
            const scale = blockBaseScale.value * blockZoom.value;
            return {
                width: blockNatW.value + 'px',
                height: blockNatH.value + 'px',
                maxWidth: 'none',
                transform: `translate(${blockPanX.value}px, ${blockPanY.value}px) scale(${scale})`,
                transformOrigin: '0 0',
                cursor: blockZoom.value > 1 ? (blockPanning.value ? 'grabbing' : 'grab') : 'default',
                transition: blockPanning.value ? 'none' : 'transform 0.15s ease',
            };
        });

        function onBlockZoomWheel(e) {
            const container = blockImageContainer.value;
            if (!container) return;

            const rect = container.getBoundingClientRect();
            const mx = e.clientX - rect.left;
            const my = e.clientY - rect.top;

            const oldScale = blockBaseScale.value * blockZoom.value;
            const factor = e.deltaY > 0 ? 0.87 : 1.15;
            let newZoom = blockZoom.value * factor;
            newZoom = Math.min(Math.max(newZoom, 1), 12);
            const newScale = blockBaseScale.value * newZoom;

            if (newScale === oldScale) return;

            // Точка под курсором в координатах натурального изображения
            const imgX = (mx - blockPanX.value) / oldScale;
            const imgY = (my - blockPanY.value) / oldScale;

            // Новый pan: та же точка остаётся под курсором
            blockPanX.value = mx - imgX * newScale;
            blockPanY.value = my - imgY * newScale;
            blockZoom.value = newZoom;
        }

        function onBlockPanStart(e) {
            if (blockZoom.value <= 1) return;
            e.preventDefault();
            blockPanning.value = true;
            blockPanStartX.value = e.clientX - blockPanX.value;
            blockPanStartY.value = e.clientY - blockPanY.value;
            const onMove = (ev) => {
                if (!blockPanning.value) return;
                blockPanX.value = ev.clientX - blockPanStartX.value;
                blockPanY.value = ev.clientY - blockPanStartY.value;
            };
            const onUp = () => {
                blockPanning.value = false;
                window.removeEventListener('mousemove', onMove);
                window.removeEventListener('mouseup', onUp);
            };
            window.addEventListener('mousemove', onMove);
            window.addEventListener('mouseup', onUp);
        }

        function resetBlockZoom() {
            blockZoom.value = 1;
            centerBlockImage();
        }

        function blockHasAnalysis(blockId) {
            return !!blockAnalysis.value[blockId];
        }

        function blockFindingsCount(blockId) {
            // Бейдж количества на превью блока считаем по ФИНАЛЬНОМУ списку
            // (03_findings, getBlockFindings), а не по сырым Stage 01 findings —
            // чтобы число на превью совпадало с модалкой блока и не показывало
            // отфильтрованные критиком замечания.
            return getBlockFindings(blockId).length;
        }

        function blockMaxSeverity(blockId) {
            const findings = getBlockFindings(blockId);
            if (!findings.length) return null;
            const order = ['КРИТИЧЕСКОЕ', 'ЭКОНОМИЧЕСКОЕ', 'ЭКСПЛУАТАЦИОННОЕ', 'РЕКОМЕНДАТЕЛЬНОЕ', 'ПРОВЕРИТЬ ПО СМЕЖНЫМ'];
            let best = 999;
            for (const f of findings) {
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
                // Загрузить block-map, findings и необязательный shadow параллельно.
                // Отсутствие shadow-файла (обычный prod default) не ломает блоки.
                const [mapData, findingsResp, shadowResp] = await Promise.all([
                    api(`/findings/${id}/block-map`),
                    api(`/findings/${id}`),
                    api(`/findings/${id}/textlayer-highlights-shadow`).catch(() => null),
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
                textlayerHighlightsShadow.value = shadowResp && Array.isArray(shadowResp.records)
                    ? shadowResp
                    : null;
                showTextlayerHighlightsShadow.value = false;
            } catch (e) {
                blockToFindings.value = {};
                textlayerHighlightsShadow.value = null;
                showTextlayerHighlightsShadow.value = false;
            }
        }

        function getBlockFindings(blockId) {
            return blockToFindings.value[blockId] || [];
        }

        // Единственный пользовательский слой рамок: точные совпадения из
        // text-layer shadow. Старые LLM highlight_regions здесь не рендерятся.
        const currentBlockTextlayerHighlights = computed(() => {
            if (!selectedBlock.value || !textlayerHighlightsShadow.value) return [];
            const bid = String(selectedBlock.value.block_id || '').replace(/^block_/, '');
            const records = textlayerHighlightsShadow.value.records || [];
            const regions = [];
            for (const record of records) {
                for (const region of (record.computed_highlight_regions || [])) {
                    const regionBlockId = String(region.block_id || '').replace(/^block_/, '');
                    if (regionBlockId !== bid) continue;
                    regions.push({
                        ...region,
                        finding_id: record.finding_id || '',
                    });
                }
            }
            return regions;
        });

        function toggleTextlayerHighlightsShadow() {
            showTextlayerHighlightsShadow.value = !showTextlayerHighlightsShadow.value;
        }

        // ─── Optimization ───
        // ─── Document Viewer (MD) ────────────────────────────
        function cleanLatex(text) {
            if (!text) return text;
            // \text{ кг/м} → кг/м
            text = text.replace(/\\text\s*\{([^}]*)\}/g, '$1');
            // ^3 → ³, ^2 → ², ^{...} → (...)
            text = text.replace(/\^3/g, '³');
            text = text.replace(/\^2/g, '²');
            text = text.replace(/\^\{([^}]*)\}/g, '$1');
            // \cdot → ·, \times → ×, \leq → ≤, \geq → ≥, \pm → ±
            text = text.replace(/\\cdot/g, '·');
            text = text.replace(/\\times/g, '×');
            text = text.replace(/\\leq/g, '≤');
            text = text.replace(/\\geq/g, '≥');
            text = text.replace(/\\pm/g, '±');
            // \frac{a}{b} → a/b
            text = text.replace(/\\frac\s*\{([^}]*)\}\s*\{([^}]*)\}/g, '$1/$2');
            // remaining \command → remove backslash
            text = text.replace(/\\([a-zA-Z]+)/g, '$1');
            return text;
        }

        function renderMarkdown(text) {
            if (!text) return '';
            text = cleanLatex(text);
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

        let _optimizationLoadSeq = 0;
        async function loadOptimization(id, forceRefresh) {
            const _mySeq = ++_optimizationLoadSeq;
            currentProjectId.value = id;
            expandedOptId.value = null;
            optimizationPage.value = 1;
            if (!forceRefresh) {
                const cached = _cacheGet('optimization', id);
                if (cached) {
                    optimizationData.value = cached;
                    loadProject(id);
                    return;
                }
            }
            optimizationLoading.value = true;
            optimizationData.value = null;
            try {
                const proj = await api(`/projects/${id}`);
                // currentProject — общий стейт: не затираем его, если пользователь
                // уже ушёл на другой проект (currentProjectId ставится синхронно
                // при любой навигации).
                if (_mySeq !== _optimizationLoadSeq || currentProjectId.value !== id) return;
                currentProject.value = proj;
                _cacheSet('project', id, currentProject.value);
                const resp = await api(`/optimization/${id}`);
                if (_mySeq !== _optimizationLoadSeq || currentProjectId.value !== id) return;
                if (resp.has_data) {
                    // Нормализуем сводку под шаблон («Всего:» и бейджи по типам):
                    // в optimization.json total/by_type лежат под meta
                    // (meta.total_items / meta.by_type), а шаблон читает их с
                    // верхнего уровня. Поддерживаем обе формы.
                    const d = resp.data || {};
                    const meta = d.meta || {};
                    const norm = {
                        ...d,
                        total: d.total ?? meta.total_items ?? (Array.isArray(d.items) ? d.items.length : 0),
                        by_type: d.by_type ?? meta.by_type ?? {},
                    };
                    optimizationData.value = norm;
                    _cacheSet('optimization', id, norm);
                }
                loadOptBlockMap(id);
            } catch (e) {
                console.error('Failed to load optimization:', e);
            } finally {
                // Гасим спиннер даже при раннем return (гонка навигаций/версии) —
                // но только для актуальной загрузки, чтобы не погасить более свежую.
                if (_mySeq === _optimizationLoadSeq) optimizationLoading.value = false;
            }
        }

        async function startOptimization(id) {
            openModelConfig(id, null, async () => {
                try {
                    await apiPost(`/optimization/${id}/run`);
                    if (currentView.value === 'project') loadProject(id);
                } catch (e) {
                    _friendlyAuditError(e);
                }
            });
        }

        const _optTypeOrder = { 'cheaper_analog': 0, 'faster_install': 1, 'simpler_design': 2, 'lifecycle': 3 };
        const filteredOptimization = computed(() => {
            if (!optimizationData.value) return [];
            const items = optimizationData.value.items || [];
            let filtered = optimizationFilter.value ? items.filter(i => i.type === optimizationFilter.value) : items;
            if (optimizationSearch.value.trim()) {
                const q = optimizationSearch.value.toLowerCase();
                filtered = filtered.filter(i =>
                    (i.current || '').toLowerCase().includes(q) ||
                    (i.proposed || '').toLowerCase().includes(q) ||
                    (i.id || '').toLowerCase().includes(q) ||
                    (i.norm || '').toLowerCase().includes(q)
                );
            }
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

        function optTypeClass(type) {
            const map = { 'cheaper_analog': 'sev-opt-cheaper', 'faster_install': 'sev-opt-faster', 'simpler_design': 'sev-opt-simpler', 'lifecycle': 'sev-opt-lifecycle' };
            return map[type] || '';
        }

        // Статус нормы предложения. Поля пишет этап 04 (пересмотр оптимизаций):
        // norm_status = ok | revised | warning, norm_outcome = still_valid | revised | obsolete.
        // Пока этап не отработал, полей нет — тогда молчим, а не рисуем «не проверена»:
        // отсутствие проверки и провал проверки — разные вещи.
        const OPT_NORM_OUTCOME_LABEL = {
            still_valid: 'норма обновлена, предложение в силе',
            revised: 'предложение пересмотрено под новую норму',
            obsolete: 'новая норма обесценила предложение',
        };
        function optNormBadge(item) {
            if (!item || !item.norm_verified) return null;
            const status = String(item.norm_status || '');
            if (status === 'ok') return { text: '✓ норма проверена', tone: 'ok' };
            const reason = (item.norm_revision && item.norm_revision.revision_reason) || '';
            if (status === 'warning') {
                return { text: '⚠ норма не подтверждена', tone: 'warn', title: reason };
            }
            if (status === 'revised') {
                const outcome = String(item.norm_outcome || '');
                const label = OPT_NORM_OUTCOME_LABEL[outcome] || 'норма пересмотрена';
                const was = (item.norm_revision && item.norm_revision.original_norm) || '';
                return {
                    text: outcome === 'obsolete' ? '⚠ ' + label : '↻ ' + label,
                    tone: outcome === 'obsolete' ? 'warn' : 'revised',
                    title: [was ? 'Было: ' + was : '', reason].filter(Boolean).join('\n'),
                };
            }
            return null;
        }

        // Статус нормы ЗАМЕЧАНИЯ. Поле пишет этап 04 (norm_verify → 03a_norms_verified):
        // norm_verification = { status, edition_status, needs_revision, current_version,
        // replacement_doc, verified_via }. active → «действует»; отменён/заменён/устаревшая
        // редакция — предупреждение. Неизвестно/нет в базе → молчим: отсутствие проверки и
        // провал проверки — разные вещи (как в optNormBadge).
        function findingNormBadge(f) {
            const nv = f && f.norm_verification;
            if (!nv || typeof nv !== 'object') return null;
            const status = String(nv.status || '');
            const edition = String(nv.edition_status || '');
            const repl = nv.replacement_doc || '';
            const cur = nv.current_version || '';
            if (['obsolete', 'superseded', 'replaced', 'cancelled', 'withdrawn'].includes(status)) {
                return {
                    text: repl ? '⚠ заменён: ' + repl : '⚠ отменён',
                    tone: 'warn',
                    title: 'Норма недействующая' + (repl ? ', действует ' + repl : ''),
                };
            }
            if (nv.needs_revision === true || edition === 'obsolete' || edition === 'superseded') {
                return {
                    text: cur ? '↻ ред. устарела → ' + cur : '↻ редакция устарела',
                    tone: 'revised',
                    title: 'Актуальная редакция: ' + (cur || 'см. базу норм'),
                };
            }
            if (status === 'active' && (edition === 'active' || edition === '')) {
                return { text: '✓ действует', tone: 'active', title: 'Норма действует (сверено с базой норм)' };
            }
            return null; // unknown / not_found / norms_unsupported / norms_missing → молчим
        }

        // Цветной кружок для бейджа оптимизации — по аналогии с sevIcon (замечания),
        // чтобы карточки-счётчики выглядели одинаково.
        function optIcon(type) {
            const map = { 'cheaper_analog': '🟢', 'faster_install': '🔵', 'simpler_design': '🟠', 'lifecycle': '🟣' };
            return map[type] || '⚪';
        }

        // ─── Discussions (чат по замечаниям/оптимизациям) ─────────────

        async function loadDiscussionModels() {
            try {
                const data = await api('/discussions/models');
                discussionModels.value = data.models || [];
                if (!discussionModel.value && data.default) {
                    discussionModel.value = data.default;
                }
            } catch (e) {
                console.error('Failed to load discussion models:', e);
            }
        }

        async function loadDiscussionItems(projectId, type) {
            discussionLoading.value = true;
            discussionPage.value = 1;
            try {
                const data = await api(`/discussions/${encodeURIComponent(projectId)}/list?type=${type}`);
                discussionItems.value = data.items || [];
                // Load block maps for table view
                if (type === 'finding') {
                    loadFindingBlockMap(projectId);
                } else {
                    loadOptBlockMap(projectId);
                }
            } catch (e) {
                console.error('Failed to load discussion items:', e);
                discussionItems.value = [];
            }
            discussionLoading.value = false;
        }

        function switchDiscussionTab(type) {
            discussionTab.value = type;
            activeDiscussion.value = null;
            discussionMessages.value = [];
            revisionData.value = null;
            if (currentProjectId.value) {
                loadDiscussionItems(currentProjectId.value, type);
            }
        }

        async function openDiscussion(projectId, itemId) {
            activeDiscussion.value = itemId;
            activeDiscussionItem.value = null;
            activeDiscussionBlocks.value = [];
            showDiscussionBlocks.value = false;
            discussionMessages.value = [];
            discussionCost.value = 0;
            discussionContextTokens.value = null;
            revisionData.value = null;
            chatInput.value = '';
            try {
                // Параллельно: история чата + полные данные замечания + блоки
                const type = discussionTab.value;
                const isOpt = type === 'optimization';
                const pid = encodeURIComponent(projectId);

                const [discData, findingsResp, blockMapResp] = await Promise.all([
                    api(`/discussions/${pid}/${encodeURIComponent(itemId)}`),
                    isOpt
                        ? api(`/optimization/${pid}`)
                        : api(`/findings/${pid}`),
                    isOpt
                        ? api(`/findings/${pid}/optimization-block-map`).catch(() => null)
                        : api(`/findings/${pid}/block-map`).catch(() => null),
                ]);

                // История чата
                discussionMessages.value = discData.messages || [];
                discussionCost.value = discData.total_cost_usd || 0;

                // Полные данные замечания
                if (isOpt) {
                    const items = findingsResp.data?.items || [];
                    activeDiscussionItem.value = items.find(i => i.id === itemId) || null;
                } else {
                    const items = findingsResp.findings || [];
                    activeDiscussionItem.value = items.find(i => i.id === itemId) || null;
                }

                // Блоки
                if (blockMapResp) {
                    const blockIds = (blockMapResp.block_map || {})[itemId] || [];
                    const blockInfo = blockMapResp.block_info || {};
                    activeDiscussionBlocks.value = blockIds.map(bid => ({
                        block_id: bid,
                        page: blockInfo[bid]?.page,
                        ocr_label: blockInfo[bid]?.ocr_label || '',
                    }));
                }

                // Загрузить оценку токенов (в фоне)
                loadDiscussionTokens(projectId, itemId);

                // Fallback для списка
                if (!discussionItems.value.length) {
                    const listData = await api(`/discussions/${pid}/list?type=${type}`);
                    discussionItems.value = listData.items || [];
                }
            } catch (e) {
                console.error('Failed to load discussion:', e);
            }
            await Vue.nextTick();
            scrollChatToBottom();
        }

        async function loadDiscussionTokens(projectId, itemId) {
            try {
                const pid = encodeURIComponent(projectId);
                const iid = encodeURIComponent(itemId);
                const type = discussionTab.value;
                discussionContextTokens.value = await api(`/discussions/${pid}/${iid}/estimate-tokens?type=${type}`);
            } catch (e) {
                console.error('Failed to estimate tokens:', e);
                discussionContextTokens.value = null;
            }
        }

        function closeDiscussion() {
            activeDiscussion.value = null;
            discussionMessages.value = [];
            revisionData.value = null;
            if (currentProjectId.value) {
                loadDiscussionItems(currentProjectId.value, discussionTab.value);
                navigate('/project/' + currentProjectId.value + '/discussions');
            }
        }

        async function downloadAuditPackage() {
            if (!currentProjectId.value) return;
            auditPackageLoading.value = true;
            try {
                const vid = activeVersionId.value;
                const qs = vid ? `?version_id=${encodeURIComponent(vid)}` : '';
                const url = `/api/export/audit-package/${encodeURIComponent(currentProjectId.value)}${qs}`;
                const resp = await fetch(url);
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || `Ошибка ${resp.status}`);
                }
                const blob = await resp.blob();
                const a = document.createElement('a');
                a.href = URL.createObjectURL(blob);
                const disposition = resp.headers.get('Content-Disposition') || '';
                // Prefer filename* (RFC 5987, supports UTF-8) over plain filename
                const matchStar = disposition.match(/filename\*=UTF-8''([^;]+)/i);
                const matchPlain = disposition.match(/filename="?([^";]+)"?/);
                let dlName = `audit_package_${currentProjectId.value}.zip`;
                if (matchStar) { try { dlName = decodeURIComponent(matchStar[1]); } catch(e) { /* fallback */ } }
                else if (matchPlain) { dlName = matchPlain[1]; }
                a.download = dlName;
                document.body.appendChild(a);
                a.click();
                a.remove();
                URL.revokeObjectURL(a.href);
            } catch (e) {
                alert('Ошибка скачивания: ' + e.message);
            } finally {
                auditPackageLoading.value = false;
            }
        }

        async function downloadBatchAuditPackages() {
            const ids = Array.from(selectedProjects.value);
            if (!ids.length) return;
            batchPackageLoading.value = true;
            let downloaded = 0;
            let errors = [];
            for (const pid of ids) {
                try {
                    const url = `/api/export/audit-package/${encodeURIComponent(pid)}`;
                    const resp = await fetch(url);
                    if (!resp.ok) {
                        const err = await resp.json().catch(() => ({}));
                        errors.push(`${pid}: ${err.detail || resp.status}`);
                        continue;
                    }
                    const blob = await resp.blob();
                    const a = document.createElement('a');
                    a.href = URL.createObjectURL(blob);
                    const disposition = resp.headers.get('Content-Disposition') || '';
                    const matchStar = disposition.match(/filename\*=UTF-8''([^;]+)/i);
                    const matchPlain = disposition.match(/filename="?([^";]+)"?/);
                    let dlName = `audit_package_${pid}.zip`;
                    if (matchStar) { try { dlName = decodeURIComponent(matchStar[1]); } catch(e) {} }
                    else if (matchPlain) { dlName = matchPlain[1]; }
                    a.download = dlName;
                    document.body.appendChild(a);
                    a.click();
                    a.remove();
                    URL.revokeObjectURL(a.href);
                    downloaded++;
                } catch (e) {
                    errors.push(`${pid}: ${e.message}`);
                }
            }
            batchPackageLoading.value = false;
            if (errors.length > 0) {
                alert(`Скачано: ${downloaded}/${ids.length}\nОшибки:\n${errors.join('\n')}`);
            }
        }

        async function cropBatchBlocks() {
            // ↓ Кнопка «Подготовить данные» = ровно два шага:
            //   1) скачать кропы блоков по crop_url (существующие PNG переиспользуются,
            //      докачивается только недостающее);
            //   2) собрать контекст блоков (CTX) — локально, без нейросети и токенов.
            // Фильтров по наличию аудита нет: готовим все отмеченные проекты.
            const targets = Array.from(selectedProjects.value);
            if (!targets.length) return;
            const confirmMsg = `Подготовить данные для ${targets.length} проектов?\n\n` +
                               `1. Скачивание кропов блоков по ссылкам (crop_url)\n` +
                               `2. Сборка контекста блоков (CTX) — всегда заново\n\n` +
                               `Прогресс — в очереди подготовки и в логе проекта.`;
            if (!confirm(confirmMsg)) return;

            batchCropLoading.value = true;
            let done = 0;
            const errors = [];
            for (const pid of targets) {
                batchCropProgress.value = `${done}/${targets.length}`;
                try {
                    // force=true → CTX пересобирается даже если сводка уже валидна.
                    // На кропы force не влияет: они всегда докачиваются, а не перекачиваются.
                    const url = `/api/audit/${encodeURIComponent(pid)}/prepare-data?force=true`;
                    const resp = await fetch(url, {method: 'POST'});
                    if (!resp.ok) {
                        const err = await resp.json().catch(() => ({}));
                        errors.push(`${pid}: ${err.detail || resp.status}`);
                    } else {
                        done++;
                    }
                } catch (e) {
                    errors.push(`${pid}: ${e.message}`);
                }
            }
            batchCropLoading.value = false;
            batchCropProgress.value = '';
            const msg = `Подготовка запущена: ${done}/${targets.length} проектов.\n` +
                        `Прогресс — в WebSocket-логе (откройте проект для деталей).` +
                        (errors.length ? `\n\nОшибки:\n${errors.join('\n')}` : '');
            alert(msg);
            await refreshProjects();
        }

        // Resolved findings — count and download
        const resolvedFindingsCount = computed(() => {
            return discussionItems.value.filter(item =>
                item.discussion_status === 'confirmed' || item.discussion_status === 'revised'
            ).length;
        });
        const allDiscussionsResolved = computed(() => {
            const items = discussionItems.value;
            if (items.length === 0) return false;
            return items.every(item =>
                item.discussion_status === 'confirmed' ||
                item.discussion_status === 'rejected' ||
                item.discussion_status === 'revised'
            );
        });

        async function downloadResolvedFindings() {
            if (resolvedFindingsLoading.value) return;
            resolvedFindingsLoading.value = true;
            try {
                const pid = currentProjectId.value;
                const vid = activeVersionId.value;
                const vq = vid ? `&version_id=${encodeURIComponent(vid)}` : '';
                const resp = await fetch(`/api/discussions/${encodeURIComponent(pid)}/resolved/excel?type=${discussionTab.value}${vq}`);
                if (!resp.ok) throw new Error(await resp.text());
                const blob = await resp.blob();
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = `resolved_${pid.replace(/\//g, '_')}_${discussionTab.value}.xlsx`;
                a.click();
                URL.revokeObjectURL(url);
            } catch (e) {
                console.error('Download resolved findings error:', e);
                alert('Ошибка скачивания: ' + e.message);
            } finally {
                resolvedFindingsLoading.value = false;
            }
        }

        function handleChatFileSelect(event) {
            const file = event.target.files[0];
            if (!file || !file.type.startsWith('image/')) return;
            const reader = new FileReader();
            reader.onload = (e) => { chatAttachedImage.value = e.target.result; };
            reader.readAsDataURL(file);
            event.target.value = ''; // reset input
        }

        function handleChatPaste(event) {
            const items = event.clipboardData?.items;
            if (!items) return;
            for (const item of items) {
                if (item.type.startsWith('image/')) {
                    event.preventDefault();
                    const file = item.getAsFile();
                    const reader = new FileReader();
                    reader.onload = (e) => { chatAttachedImage.value = e.target.result; };
                    reader.readAsDataURL(file);
                    return;
                }
            }
        }

        async function sendDiscussionMessage() {
            const msg = chatInput.value.trim();
            const hasImage = !!chatAttachedImage.value;
            if ((!msg && !hasImage) || discussionSending.value) return;

            discussionSending.value = true;
            const imageData = chatAttachedImage.value;
            chatInput.value = '';
            chatAttachedImage.value = null;
            // Сбросить высоту textarea
            const ta = document.querySelector('.chat-textarea');
            if (ta) ta.style.height = 'auto';

            // Добавить user-сообщение (с фото если есть)
            discussionMessages.value.push({
                role: 'user', content: msg, timestamp: new Date().toISOString(),
                image: imageData || null,
            });

            // Добавить пустое assistant-сообщение для стриминга
            const assistantMsg = Vue.reactive({
                role: 'assistant', content: '', timestamp: new Date().toISOString(),
                input_tokens: 0, output_tokens: 0, cost_usd: 0, streaming: true,
            });
            discussionMessages.value.push(assistantMsg);
            await Vue.nextTick();
            scrollChatToBottom();

            try {
                const vid = activeVersionId.value;
                const vq = vid ? `&version_id=${encodeURIComponent(vid)}` : '';
                const url = `/api/discussions/${encodeURIComponent(currentProjectId.value)}/${encodeURIComponent(activeDiscussion.value)}/chat/stream?type=${discussionTab.value}${vq}`;
                const response = await fetch(url, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ message: msg || '(фото)', model: discussionModel.value, image: imageData || undefined }),
                });

                const reader = response.body.getReader();
                const decoder = new TextDecoder();
                let buffer = '';
                let scrollThrottle = 0;

                while (true) {
                    const { done, value } = await reader.read();
                    if (done) break;

                    buffer += decoder.decode(value, { stream: true });
                    const parts = buffer.split('\n\n');
                    buffer = parts.pop();

                    for (const part of parts) {
                        if (!part.startsWith('data: ')) continue;
                        let data;
                        try { data = JSON.parse(part.slice(6)); } catch { continue; }

                        if (data.type === 'start') {
                            // Соединение установлено, LLM думает
                            continue;
                        } else if (data.type === 'delta') {
                            assistantMsg.content += data.text;
                            // Скролл с throttle
                            if (++scrollThrottle % 5 === 0) {
                                await Vue.nextTick();
                                scrollChatToBottom();
                            }
                        } else if (data.type === 'done') {
                            assistantMsg.content = data.text;
                            assistantMsg.input_tokens = data.input_tokens || 0;
                            assistantMsg.output_tokens = data.output_tokens || 0;
                            assistantMsg.cost_usd = data.cost_usd || 0;
                            assistantMsg.streaming = false;
                        } else if (data.type === 'saved') {
                            discussionCost.value = data.total_cost_usd || 0;
                            // Обновить оценку токенов (история выросла)
                            loadDiscussionTokens(currentProjectId.value, activeDiscussion.value);
                        } else if (data.type === 'error') {
                            assistantMsg.content = 'Ошибка: ' + data.message;
                            assistantMsg.streaming = false;
                        }
                    }
                }
            } catch (e) {
                assistantMsg.content = 'Ошибка: ' + (e.message || e);
                assistantMsg.streaming = false;
            }

            assistantMsg.streaming = false;
            discussionSending.value = false;
            await Vue.nextTick();
            scrollChatToBottom();
        }

        function startEditMessage(idx) {
            editingMessageIdx.value = idx;
            editingMessageText.value = discussionMessages.value[idx].content;
        }

        function cancelEditMessage() {
            editingMessageIdx.value = null;
            editingMessageText.value = '';
        }

        async function submitEditMessage() {
            const idx = editingMessageIdx.value;
            if (idx === null) return;
            const newText = editingMessageText.value.trim();
            if (!newText) return;

            // Обрезать: удалить это сообщение и всё после него
            discussionMessages.value = discussionMessages.value.slice(0, idx);
            editingMessageIdx.value = null;
            editingMessageText.value = '';

            // Сохранить обрезанную историю на сервер
            try {
                await apiPost(
                    `/discussions/${encodeURIComponent(currentProjectId.value)}/${encodeURIComponent(activeDiscussion.value)}/truncate`,
                    { keep_count: idx }
                );
            } catch (e) {
                console.error('Failed to truncate:', e);
            }

            // Отправить изменённое сообщение как новое
            chatInput.value = newText;
            await sendDiscussionMessage();
        }

        async function resolveDiscussion(status) {
            if (!activeDiscussion.value) return;
            const summary = status === 'rejected'
                ? 'Отклонено по результатам обсуждения'
                : status === 'confirmed'
                    ? 'Подтверждено по результатам обсуждения'
                    : '';
            try {
                await apiPost(
                    `/discussions/${encodeURIComponent(currentProjectId.value)}/${encodeURIComponent(activeDiscussion.value)}/resolve?type=${discussionTab.value}`,
                    { status, summary }
                );
                // Обновить список
                loadDiscussionItems(currentProjectId.value, discussionTab.value);
                if (status !== 'revised') {
                    closeDiscussion();
                }
            } catch (e) {
                alert('Ошибка: ' + (e.message || e));
            }
        }

        async function requestRevision() {
            if (!activeDiscussion.value) return;
            revisionLoading.value = true;
            revisionData.value = null;
            try {
                const data = await apiPost(
                    `/discussions/${encodeURIComponent(currentProjectId.value)}/${encodeURIComponent(activeDiscussion.value)}/revise?type=${discussionTab.value}`,
                    { model: discussionModel.value }
                );
                revisionData.value = data;
                discussionCost.value = data.total_cost_usd || discussionCost.value;
            } catch (e) {
                alert('Ошибка генерации: ' + (e.message || e));
            }
            revisionLoading.value = false;
        }

        async function applyRevision() {
            if (!revisionData.value?.revised) return;
            try {
                await apiPost(
                    `/discussions/${encodeURIComponent(currentProjectId.value)}/${encodeURIComponent(activeDiscussion.value)}/apply-revision?type=${discussionTab.value}`,
                    revisionData.value.revised
                );
                await resolveDiscussion('revised');
                revisionData.value = null;
            } catch (e) {
                alert('Ошибка применения: ' + (e.message || e));
            }
        }

        function rejectRevision() {
            revisionData.value = null;
        }

        const _fieldNames = {
            id: 'ID', title: 'Заголовок', description: 'Описание', category: 'Категория',
            severity: 'Критичность', recommendation: 'Рекомендация', norm_ref: 'Ссылка на норму',
            norm_quote: 'Цитата нормы', norm_confidence: 'Уверенность', page: 'Страница PDF',
            sheet: 'Лист', evidence: 'Обоснование', related_block_ids: 'Связанные блоки',
            status: 'Статус', type: 'Тип', savings_pct: 'Экономия %', savings_basis: 'Основа расчёта',
            spec_items: 'Позиции спецификации', current: 'Текущее решение', proposed: 'Предложение',
            justification: 'Обоснование', vendor: 'Производитель', grounding: 'Привязка',
            tags: 'Теги', notes: 'Примечания', comment: 'Комментарий',
            problem: 'Проблема', norm: 'Норматив', solution: 'Решение', risk: 'Риск',
            location: 'Расположение', source: 'Источник', priority: 'Приоритет',
            affected_systems: 'Затронутые системы', cost_impact: 'Влияние на стоимость',
            responsible: 'Ответственный', deadline: 'Срок', reference: 'Ссылка',
            reason: 'Причина', impact: 'Последствия', action: 'Действие',
            finding_id: 'ID замечания', block_id: 'ID блока', sheet_name: 'Название листа',
            summary: 'Резюме', details: 'Детали', fix: 'Исправление',
        };
        function formatRevisionField(key) {
            return _fieldNames[key] || key;
        }
        function formatRevisionValue(val) {
            if (val === null || val === undefined) return '—';
            if (Array.isArray(val)) return val.join(', ');
            if (typeof val === 'object') return JSON.stringify(val, null, 2);
            return String(val);
        }

        function scrollChatToBottom() {
            const el = chatMessagesContainer.value;
            if (el) el.scrollTop = el.scrollHeight;
        }

        function autoResizeChatInput(event) {
            const el = event.target;
            el.style.height = 'auto';
            const maxH = 200; // ~4x от начальной высоты 48px
            el.style.height = Math.min(el.scrollHeight, maxH) + 'px';
        }

        function onChatClick(event) {
            // Делегирование: перехватить клик по block-id-link
            const link = event.target.closest('.block-id-link');
            if (link) {
                event.preventDefault();
                const blockId = link.dataset.blockId;
                if (blockId && currentProjectId.value) {
                    navigateToBlock(blockId, null);
                }
            }
        }

        const activeDiscussionItems = computed(() => {
            return discussionItems.value.filter(i => i.discussion_status !== 'rejected');
        });

        const rejectedDiscussionItems = computed(() => {
            return discussionItems.value.filter(i => i.discussion_status === 'rejected');
        });

        const discussionSeverityCounts = computed(() => {
            const counts = {};
            for (const item of activeDiscussionItems.value) {
                const sev = item.severity || 'Неизвестно';
                counts[sev] = (counts[sev] || 0) + 1;
            }
            return counts;
        });

        const discussionOptTypeCounts = computed(() => {
            const counts = {};
            for (const item of activeDiscussionItems.value) {
                const t = item.opt_type || 'other';
                counts[t] = (counts[t] || 0) + 1;
            }
            return counts;
        });

        function discussionStatusIcon(status) {
            if (status === 'confirmed') return '\u2705';
            if (status === 'rejected') return '\u274C';
            if (status === 'revised') return '\u270F\uFE0F';
            return '';
        }

        function formatCostUSD(val) {
            if (!val || val < 0.001) return '$0.00';
            return '$' + val.toFixed(3);
        }

        function renderDiscussionContent(text) {
            // Сначала markdown
            let html = renderMarkdown ? renderMarkdown(text) : text;
            // Затем заменить block_id паттерны на кликабельные ссылки
            // Паттерн: XXXX-XXXX-XXX (3-5 символов через дефис, 3 группы)
            const blockIdRe = /\b([A-Z0-9]{3,5}-[A-Z0-9]{3,5}-[A-Z0-9]{2,4})\b/g;
            const pid = currentProjectId.value;
            if (pid) {
                html = html.replace(blockIdRe, (match) => {
                    return `<a href="#" class="block-id-link" data-block-id="${match}" title="Открыть блок ${match}">${match}</a>`;
                });
            }
            return html;
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

        function cleanSubProblem(text) {
            if (!text) return '';
            return text
                .replace(/\s*\(на разных листах проекта\)\s*/gi, '')
                .replace(/\s*\(на разных листах\)\s*/gi, '')
                .trim();
        }

        // ─── Computed ───
        const filteredFindings = computed(() => {
            if (!findingsData.value) return [];
            return findingsData.value.findings;
        });

        // Сортировка по столбцу Critic v2: null → 'desc' (100→0) → 'asc' (0→100) → null
        const cv2SortDir = ref(null);
        function toggleCv2Sort() {
            if (cv2SortDir.value === null) cv2SortDir.value = 'desc';
            else if (cv2SortDir.value === 'desc') cv2SortDir.value = 'asc';
            else cv2SortDir.value = null;
            findingsPage.value = 1;
        }

        // Сортировка: отклонённые всегда внизу (если есть решения).
        // Если активна сортировка по Critic v2 — она имеет приоритет, nulls в конец.
        const sortedFindings = computed(() => {
            const items = filteredFindings.value;
            if (cv2SortDir.value) {
                const dir = cv2SortDir.value === 'asc' ? 1 : -1;
                return [...items].sort((a, b) => {
                    const sa = findingCv2Score(a.id);
                    const sb = findingCv2Score(b.id);
                    const aNull = sa == null, bNull = sb == null;
                    if (aNull && bNull) return 0;
                    if (aNull) return 1;
                    if (bNull) return -1;
                    return (sa - sb) * dir;
                });
            }
            if (!Object.keys(expertDecisions.value).length) return items;
            const accepted = [], pending = [], rejected = [];
            for (const f of items) {
                const d = getExpertDecision(f.id);
                if (d === 'rejected') rejected.push(f);
                else if (d === 'accepted') accepted.push(f);
                else pending.push(f);
            }
            return [...pending, ...accepted, ...rejected];
        });

        const sortedOptimization = computed(() => {
            const items = filteredOptimization.value;
            if (!Object.keys(expertDecisions.value).length) return items;
            const accepted = [], pending = [], rejected = [];
            for (const item of items) {
                const d = getExpertDecision(item.id);
                if (d === 'rejected') rejected.push(item);
                else if (d === 'accepted') accepted.push(item);
                else pending.push(item);
            }
            return [...pending, ...accepted, ...rejected];
        });

        // ─── Paginated views ───
        // Замечания (findings) выводятся ОДНИМ списком без пагинации (по просьбе Андрея
        // Ивановича 07-04): эксперту удобнее видеть все замечания сразу, не листая страницы.
        // totalPages=1 → пагинатор `v-if="findingsTotalPages > 1"` в шаблоне сам скрывается.
        // Оптимизация/дискуссии пагинацию сохраняют (PAGE_SIZE).
        const paginatedFindings = computed(() => sortedFindings.value);
        const findingsTotalPages = computed(() => 1);

        const paginatedOptimization = computed(() => {
            const all = sortedOptimization.value;
            const start = (optimizationPage.value - 1) * PAGE_SIZE;
            return all.slice(start, start + PAGE_SIZE);
        });
        const optimizationTotalPages = computed(() => Math.max(1, Math.ceil(sortedOptimization.value.length / PAGE_SIZE)));

        const paginatedDiscussion = computed(() => {
            const all = activeDiscussionItems.value;
            const start = (discussionPage.value - 1) * PAGE_SIZE;
            return all.slice(start, start + PAGE_SIZE);
        });
        const discussionTotalPages = computed(() => Math.max(1, Math.ceil(activeDiscussionItems.value.length / PAGE_SIZE)));

        // Сброс страницы при изменении фильтров
        watch(filterSeverity, () => { findingsPage.value = 1; });
        watch(filterSearch, () => { findingsPage.value = 1; });
        watch(optimizationFilter, () => { optimizationPage.value = 1; });
        watch(optimizationSearch, () => { optimizationPage.value = 1; });
        watch(discussionTab, () => { discussionPage.value = 1; });

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
            if (status === 'migration_required') return 'step-partial';
            if (status === 'running') return 'step-running';
            if (status === 'skipped') return 'step-skipped';
            if (status === 'disabled') return 'step-disabled';  // фича интегрирована, но выключена (EV)
            return '';
        }

        // Объединённый статус critic + corrector → один pill "CF"
        function combinedCriticStatus(criticStatus, correctorStatus) {
            // Если хоть один running — running
            if (criticStatus === 'running' || correctorStatus === 'running') return 'running';
            // Если хоть один error — error
            if (criticStatus === 'error' || correctorStatus === 'error') return 'error';
            // Если оба done — done
            if (criticStatus === 'done' && correctorStatus === 'done') return 'done';
            // Если critic done, corrector skipped (не нужен) — done
            if (criticStatus === 'done' && (correctorStatus === 'skipped' || !correctorStatus)) return 'done';
            // Partial
            if (criticStatus === 'partial' || correctorStatus === 'partial') return 'partial';
            // Critic done но corrector ещё idle — partial (в процессе)
            if (criticStatus === 'done') return 'partial';
            // Skipped
            if (criticStatus === 'skipped') return 'skipped';
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

        // Подсказки для компактных пилюль на карточке проекта: сама карточка
        // показывает только итог, а разбивка по severity/типам живёт в tooltip.
        function findingsBreakdownTitle(project) {
            if (!project) return '';
            const by = project.findings_by_severity || {};
            const parts = Object.keys(by)
                .filter(sev => by[sev])
                .map(sev => `${sev}: ${by[sev]}`);
            const lines = [`Замечания — всего: ${project.findings_count || 0}`];
            if (parts.length) lines.push(parts.join(' · '));
            lines.push('Нажмите, чтобы открыть список замечаний');
            return lines.join('\n');
        }

        function optimizationBreakdownTitle(project) {
            if (!project) return '';
            const by = project.optimization_by_type || {};
            const parts = Object.keys(by)
                .filter(type => by[type])
                .map(type => `${optTypeLabel(type)}: ${by[type]}`);
            const lines = [`Оптимизация — всего: ${project.optimization_count || 0}`];
            if (parts.length) lines.push(parts.join(' · '));
            lines.push('Нажмите, чтобы открыть предложения по оптимизации');
            return lines.join('\n');
        }

        function findingDetectorBadge(finding) {
            if (!finding) return null;
            const provenance = finding.provenance || {};
            const foundBy = Array.isArray(provenance.found_by) ? provenance.found_by : [];
            let summary = finding.detector_summary || provenance.detector_summary || '';
            if (!summary) {
                const hasGpt = foundBy.includes('gpt_openrouter');
                const hasCodex = foundBy.includes('codex');
                summary = hasGpt && hasCodex ? 'gpt_codex' : (hasGpt ? 'gpt' : (hasCodex ? 'codex' : ''));
            }
            const meta = {
                gpt: { text: 'GPT', tone: 'gpt', title: 'Найдено GPT через OpenRouter' },
                codex: { text: 'Codex', tone: 'codex', title: 'Найдено независимым проходом Codex' },
                gpt_codex: { text: 'GPT + Codex', tone: 'both', title: 'Независимо найдено GPT и Codex' },
                claude: { text: 'Claude', tone: 'claude', title: 'Найдено Claude' },
                claude_codex: { text: 'Claude + Codex', tone: 'both', title: 'Независимо найдено Claude и Codex' },
            }[summary];
            if (!meta) return null;
            const detections = Array.isArray(provenance.detections) ? provenance.detections : [];
            const models = [...new Set(detections.map(d => d && d.model).filter(Boolean))];
            const isGapSearch = detections.some(d => d && d.mode === 'gap_search');
            const baseTitle = summary === 'codex' && isGapSearch
                ? 'Найдено дополнительным gap-search Codex'
                : meta.title;
            const comparison = finding.detector_comparison || {};
            const relation = comparison.primary_relation || comparison.relation || '';
            const relationLabel = {
                match: 'совпадение GPT/Codex',
                extension: 'расширение другого замечания',
                new: comparison.gap_search || comparison.origin === 'gap_search'
                    ? 'новое: найдено gap-search'
                    : 'новое: найдено одним детектором',
                disputed: 'спорное: детекторы расходятся',
            }[relation];
            const titleParts = [
                models.length ? `${baseTitle}: ${models.join(', ')}` : baseTitle,
                relationLabel,
            ].filter(Boolean);
            return {
                ...meta,
                title: titleParts.join(' · '),
            };
        }

        function sevIcon(severity) {
            const s = (severity || '').toUpperCase();
            if (s.includes('КРИТИЧ')) return '\uD83D\uDD34';
            if (s.includes('ЭКОНОМ')) return '\uD83D\uDFE0';
            if (s.includes('ЭКСПЛУАТ')) return '\uD83D\uDFE1';
            if (s.includes('РЕКОМЕНД')) return '\uD83D\uDD35';
            return '\u26AA';
        }

        let searchTimeout = null;
        function debounceSearch() {
            // Client-side — watch(filterSearch) уже вызывает _applyFindingsFilter
            // debounceSearch оставлен для совместимости с HTML-биндингами
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
                findingIndex.value[pid] = {};
                findingStage.value = { ...findingStage.value, [pid]: '' };
                // Очищаем и на сервере
                fetch(`/api/audit/${encodeURIComponent(pid)}/log`, { method: 'DELETE' }).catch(() => {});
            }
        }

        function copyLog(event) {
            const sections = logSections.value;
            if (!sections.length) return;
            const text = sections.map(section => {
                const body = section.entries.map(serializeLogEntry).filter(Boolean).join('\n');
                return `═══ ${section.title} ═══\n${body}`;
            }).join('\n\n');
            const btn = event?.target;
            const done = () => {
                if (btn) { btn.textContent = 'Скопировано!'; setTimeout(() => btn.textContent = 'Скопировать', 1500); }
            };
            if (navigator.clipboard) {
                navigator.clipboard.writeText(text).then(done).catch(() => {
                    fallbackCopy(text); done();
                });
            } else {
                fallbackCopy(text); done();
            }
        }

        function fallbackCopy(text) {
            const ta = document.createElement('textarea');
            ta.value = text;
            ta.style.position = 'fixed';
            ta.style.opacity = '0';
            document.body.appendChild(ta);
            ta.select();
            document.execCommand('copy');
            document.body.removeChild(ta);
        }

        function stripCliSummaryCodeFence(text) {
            const raw = String(text || '').trim();
            const m = raw.match(/^```(?:json)?\s*([\s\S]*?)\s*```$/i);
            return m ? m[1].trim() : raw;
        }

        function tryParseCliSummaryJson(text) {
            const raw = stripCliSummaryCodeFence(text);
            if (!raw || !/^[\[{]/.test(raw)) return null;
            try {
                return JSON.parse(raw);
            } catch (e) {
                return null;
            }
        }

        function basenamePath(path) {
            const raw = String(path || '').trim();
            if (!raw) return '';
            const parts = raw.split(/[\\/]/);
            return parts[parts.length - 1] || raw;
        }

        function isPlainObject(value) {
            return !!value && typeof value === 'object' && !Array.isArray(value);
        }

        function isPrimitive(value) {
            return value === null || ['string', 'number', 'boolean'].includes(typeof value);
        }

        function humanizeCliSummaryKey(key) {
            const labels = {
                status: 'Статус',
                file: 'Файл',
                project_id: 'Проект',
                review_date: 'Дата проверки',
                audit_completed: 'Дата аудита',
                audit_mode: 'Режим аудита',
                source: 'Источник',
                total_reviewed: 'Проверено',
                total_findings: 'Итоговых замечаний',
                total_items: 'Предложений',
                blocks_analyzed: 'Блоков проанализировано',
                text_analysis_merged: 'Добавлено из текста',
                pass: 'Подтверждено',
                passed: 'Подтверждено',
                fixed: 'Исправлено',
                removed: 'Удалено',
                downgraded: 'Понижено',
                weak_evidence: 'Слабая доказательная база',
                not_practical: 'Непрактично',
                no_evidence: 'Нет подтверждения',
                phantom_block: 'Фантомный блок',
                page_mismatch: 'Не та страница',
                contradicts_text: 'Противоречит тексту',
                vendor_violation: 'Нарушение vendor-листа',
                conflicts_with_finding: 'Конфликт с замечанием',
                unrealistic_savings: 'Недостоверная экономия',
                no_traceability: 'Нет трассируемости',
                wrong_page: 'Неверная страница',
                too_vague: 'Слишком расплывчато',
                technical_issue: 'Техническая проблема',
                review_applied: 'Review применён',
                high_relevance: 'Высокая релевантность',
                medium_relevance: 'Средняя релевантность',
                low_relevance: 'Низкая релевантность',
                likely_formal_only: 'Вероятно формальные',
                high_severity_formal_only: 'Формальные высокой критичности',
            };
            if (labels[key]) return labels[key];
            const text = String(key || '').replace(/_/g, ' ').trim();
            return text ? text.charAt(0).toUpperCase() + text.slice(1) : '';
        }

        function formatCliSummaryPrimitive(key, value) {
            if (value === null || value === undefined || value === '') return '';
            if (typeof value === 'boolean') return value ? 'да' : 'нет';
            if (typeof value === 'number') return Number.isInteger(value) ? value.toLocaleString() : String(value);
            if (key === 'file') return '`' + basenamePath(value) + '`';
            return String(value);
        }

        function buildCliSummaryBulletLines(obj, opts = {}) {
            if (!isPlainObject(obj)) return [];
            const preferred = opts.preferred || [];
            const hidden = new Set(opts.hidden || []);
            const keys = [
                ...preferred.filter((k) => Object.prototype.hasOwnProperty.call(obj, k)),
                ...Object.keys(obj).filter((k) => !preferred.includes(k)),
            ];
            const lines = [];
            for (const key of keys) {
                if (hidden.has(key)) continue;
                const value = obj[key];
                if (!isPrimitive(value) || value === '' || value === null || value === undefined) continue;
                lines.push(`- **${humanizeCliSummaryKey(key)}:** ${formatCliSummaryPrimitive(key, value)}`);
            }
            return lines;
        }

        function summarizeCliSummaryJson(data, stage = '') {
            if (!isPlainObject(data)) return '';

            const lines = [];
            const meta = isPlainObject(data.meta) ? data.meta : {};
            const reviewStats = isPlainObject(data.review_stats) ? data.review_stats : (isPlainObject(meta.review_stats) ? meta.review_stats : null);
            const verdicts = isPlainObject(data.verdicts) ? data.verdicts : (isPlainObject(meta.verdicts) ? meta.verdicts : null);
            const qualitySummary = isPlainObject(data.quality_summary) ? data.quality_summary : (isPlainObject(meta.quality_summary) ? meta.quality_summary : null);
            const bySeverity = isPlainObject(data.by_severity) ? data.by_severity : (isPlainObject(meta.by_severity) ? meta.by_severity : null);
            const topLevelSummary = isPlainObject(data.summary) ? data.summary : null;
            const countableSummary = topLevelSummary && Object.values(topLevelSummary).every((v) => typeof v === 'number') ? topLevelSummary : null;

            if (data.file) lines.push(`**Файл:** \`${basenamePath(data.file)}\``);
            if (data.status) lines.push(`**Статус:** \`${data.status}\``);

            const summaryLines = [];
            const totalReviewed =
                data.total_reviewed ??
                (countableSummary ? countableSummary.total_reviewed : null) ??
                meta.total_reviewed ??
                (reviewStats ? reviewStats.total_reviewed : null);
            if (typeof totalReviewed === 'number') summaryLines.push(`- **Проверено:** ${totalReviewed.toLocaleString()}`);

            const totalFindings = data.total_findings ?? meta.total_findings;
            if (typeof totalFindings === 'number') summaryLines.push(`- **Итоговых замечаний:** ${totalFindings.toLocaleString()}`);

            const totalItems = data.total_items ?? meta.total_items;
            if (typeof totalItems === 'number') summaryLines.push(`- **Предложений:** ${totalItems.toLocaleString()}`);

            const blocksAnalyzed = data.blocks_analyzed ?? meta.blocks_analyzed;
            if (typeof blocksAnalyzed === 'number') summaryLines.push(`- **Блоков проанализировано:** ${blocksAnalyzed.toLocaleString()}`);

            const textMerged = data.text_analysis_merged ?? meta.text_analysis_merged;
            if (typeof textMerged === 'number') summaryLines.push(`- **Добавлено из текста:** ${textMerged.toLocaleString()}`);

            const verdictSummary = countableSummary || verdicts;
            if (verdictSummary) {
                summaryLines.push(...buildCliSummaryBulletLines(verdictSummary, {
                    preferred: ['pass', 'passed', 'weak_evidence', 'not_practical', 'no_evidence', 'phantom_block', 'page_mismatch', 'contradicts_text', 'vendor_violation', 'conflicts_with_finding', 'unrealistic_savings', 'no_traceability', 'wrong_page', 'too_vague', 'technical_issue'],
                    hidden: ['total_reviewed'],
                }));
            }

            if (summaryLines.length) {
                lines.push('', '**Краткая сводка:**', '', ...summaryLines);
            }

            if (reviewStats) {
                lines.push('', '**Результат корректировки:**', '', ...buildCliSummaryBulletLines(reviewStats, {
                    preferred: ['total_reviewed', 'passed', 'fixed', 'removed', 'downgraded'],
                }));
            }

            if (bySeverity) {
                lines.push('', '**По критичности:**', '', ...buildCliSummaryBulletLines(bySeverity));
            }

            if (qualitySummary) {
                lines.push('', '**Качество выборки:**', '', ...buildCliSummaryBulletLines(qualitySummary, {
                    preferred: ['total', 'high_relevance', 'medium_relevance', 'low_relevance', 'likely_formal_only', 'high_severity_formal_only'],
                }));
            }

            if (typeof data.findings === 'string' && data.findings.trim()) {
                lines.push('', `**Результат:** ${data.findings.trim()}`);
            }
            if (typeof data.removed_findings === 'string' && data.removed_findings.trim()) {
                lines.push('', `**Удалено:** ${data.removed_findings.trim()}`);
            }

            if (Array.isArray(data.fixed) && data.fixed.length) {
                lines.push('', `**Изменено:** ${data.fixed.length}`);
                for (const item of data.fixed.slice(0, 5)) {
                    const itemId = item?.id || item?.item_id || 'item';
                    const details = item?.changes || item?.verdict || 'обновлено';
                    lines.push(`- **${itemId}:** ${details}`);
                }
            }

            if (topLevelSummary && topLevelSummary !== countableSummary) {
                const entries = Object.entries(topLevelSummary).slice(0, 5);
                const pointLines = [];
                for (const [key, value] of entries) {
                    if (!isPrimitive(value)) continue;
                    pointLines.push(`- **${key}:** ${formatCliSummaryPrimitive(key, value)}`);
                }
                if (pointLines.length) lines.push('', '**Ключевые пункты:**', '', ...pointLines);
            }

            if (Array.isArray(data.reviews) && data.reviews.length && !verdicts) {
                const counts = {};
                for (const review of data.reviews) {
                    const verdict = review?.verdict || 'other';
                    counts[verdict] = (counts[verdict] || 0) + 1;
                }
                lines.push('', '**Вердикты:**', '', ...buildCliSummaryBulletLines(counts));
            }

            const fallbackFields = {};
            const usedTopKeys = new Set(['meta', 'review_stats', 'verdicts', 'quality_summary', 'by_severity', 'summary', 'findings', 'removed_findings', 'fixed', 'reviews']);
            for (const [key, value] of Object.entries(data)) {
                if (usedTopKeys.has(key)) continue;
                if (!isPrimitive(value) || value === '' || value === null || value === undefined) continue;
                fallbackFields[key] = value;
            }
            const fallbackLines = buildCliSummaryBulletLines(fallbackFields, {
                preferred: ['project_id', 'review_date', 'audit_completed', 'audit_mode', 'source'],
                hidden: ['status', 'file', 'total_reviewed', 'total_findings', 'total_items', 'blocks_analyzed', 'text_analysis_merged'],
            });
            if (fallbackLines.length) {
                lines.push('', '**Детали:**', '', ...fallbackLines);
            }

            const markdown = lines.join('\n').trim();
            if (!markdown) {
                if (stage) return `**Этап:** \`${stage}\`\n\nПодробная сводка возвращена в JSON, но не распознана автоматически.`;
                return 'Подробная сводка возвращена в JSON, но не распознана автоматически.';
            }
            return markdown;
        }

        function normalizeCliSummaryContent(text, stage = '') {
            const raw = String(text || '').trim();
            if (!raw) {
                const empty = 'Подробная сводка результата не сохранена в этом запуске.';
                return { markdown: empty, text: empty };
            }
            const parsed = tryParseCliSummaryJson(raw);
            const markdown = parsed ? summarizeCliSummaryJson(parsed, stage) : raw;
            const plain = markdown
                .replace(/\*\*([^*]+)\*\*/g, '$1')
                .replace(/`([^`]+)`/g, '$1')
                .replace(/\n{3,}/g, '\n\n')
                .trim();
            return { markdown, text: plain };
        }

        function buildCliSummaryShortMessage(source) {
            if (source && typeof source.message === 'string' && source.message.trim()) {
                return source.message;
            }
            const isError = !!source?.is_error;
            const parts = [];
            const durationSec = Number(source?.duration_sec || 0);
            const costUsd = Number(source?.cost_usd || 0);
            const outputTokens = Number(source?.output_tokens || 0);
            const cacheCreation = Number(source?.cache_creation || 0);
            const cacheRead = Number(source?.cache_read || 0);
            if (durationSec > 0) {
                const minutes = Math.floor(durationSec / 60);
                const seconds = Math.round(durationSec % 60);
                parts.push(minutes > 0 ? `${minutes}м ${seconds}с` : `${seconds}с`);
            }
            if (costUsd > 0) parts.push(`$${costUsd.toFixed(2)}`);
            if (outputTokens > 0) parts.push(`${outputTokens.toLocaleString()} out`);
            if (cacheCreation > 0) parts.push(`${cacheCreation.toLocaleString()} cache_new`);
            if (cacheRead > 0) parts.push(`${cacheRead.toLocaleString()} cache_hit`);
            const prefix = isError ? '✗ Claude завершил с ошибкой' : '✓ Claude завершил';
            return parts.length ? `${prefix}: ${parts.join(', ')}` : prefix;
        }

        function looksLikeCliSummary(source) {
            if (!source) return false;
            if (source.kind === 'cli_summary') return true;
            if (typeof source.result_md === 'string') return true;
            return /Claude завершил/.test(String(source.message || ''));
        }

        function buildCliSummaryEntry(source, time = '') {
            if (!looksLikeCliSummary(source)) return null;
            const stage = source.stage || '';
            const normalized = normalizeCliSummaryContent(source.result_md || '', stage);
            return {
                kind: 'cli_summary',
                time: time,
                stage: stage,
                message: buildCliSummaryShortMessage(source),
                resultHtml: renderSimpleMarkdown(normalized.markdown),
                resultText: normalized.text,
                duration_sec: Number(source.duration_sec || 0),
                cost_usd: Number(source.cost_usd || 0),
                output_tokens: Number(source.output_tokens || 0),
                cache_read: Number(source.cache_read || 0),
                cache_creation: Number(source.cache_creation || 0),
                model: source.model || '',
                is_error: !!source.is_error,
                expanded: true,
            };
        }

        function serializeLogEntry(entry) {
            if (!entry) return '';
            if (entry.kind === 'cli_summary') {
                const header = `[${entry.time || 'summary'}] ${entry.message || 'Claude завершил этап'}`;
                const body = (entry.resultText || '').trim();
                if (!body) return header;
                const indented = body.split('\n').map(line => line ? `    ${line}` : '').join('\n').trimEnd();
                return `${header}\n${indented}`;
            }
            if (entry.kind === 'finding') {
                const statusIcon = entry.status === 'confirmed' ? '✓' : (entry.status === 'rejected' ? '✕' : '…');
                const parts = [entry.finding_id || 'finding', entry.problem || ''].filter(Boolean);
                const base = `[${entry.time || 'finding'}] ${statusIcon} ${parts.join(' — ')}`.trim();
                if (entry.status === 'rejected' && entry.rejectReason) {
                    return `${base}\n    Отклонено: ${entry.rejectReason}`;
                }
                return base;
            }
            const message = entry.message === undefined || entry.message === null ? '' : String(entry.message);
            if (!message) return '';
            return `[${entry.time || ''}] ${message}`.trimEnd();
        }

        async function loadProjectLog(projectId) {
            /**  Загрузить историю логов из файла проекта + восстановить структурированные карточки. */
            if (!projectId) return;
            logLoading.value = true;
            logTruncatedNotice.value = '';
            // Отметка живых строк на момент старта запроса. Запрос идёт заметное
            // время (полный прогон — сотни килобайт), и всё, что прилетит по WS
            // за этот срок, попадёт в projectLogs через pushToProjectLog. Без
            // отметки присваивание в конце затрёт эти строки — молча, ровно в
            // тот момент, когда идёт живой аудит и терять их нельзя.
            const liveBefore = (projectLogs.value[projectId] || []).length;
            try {
                // Лимит 5000: полный прогон должен помещаться целиком —
                // усечение истории выглядит как «лог перезаписался с этапа X»
                // (наблюдалось при limit=500: флуд одного этапа вытеснял
                // все предыдущие секции из окна выборки).
                const resp = await fetch(`/api/audit/${encodeURIComponent(projectId)}/log?limit=5000`);
                if (resp.ok) {
                    const data = await resp.json();
                    const entries = (data.entries || []).map(e => {
                        const time = e.timestamp ? new Date(e.timestamp).toLocaleTimeString() : '';
                        // Структурированная запись cli_summary — восстанавливаем красивую карточку
                        const summaryEntry = buildCliSummaryEntry(e, time);
                        if (summaryEntry) return summaryEntry;
                        return {
                            kind: 'log',
                            time: time,
                            level: e.level || 'info',
                            message: e.message || '',
                            stage: e.stage || '',
                        };
                    });
                    // Файл длиннее окна выборки — честно сказать об усечении
                    // баннером над секциями, а не молча показывать лог
                    // «с середины процесса».
                    logTruncatedNotice.value = (data.has_more && entries.length)
                        ? `⚠ Показаны последние ${entries.length} из ${data.total} записей — начало лога усечено`
                        : '';
                    // Дописать строки, прилетевшие по WS ПОКА шёл запрос: в
                    // файле их ещё нет, а на экране они уже были.
                    const arrivedDuringFetch = (projectLogs.value[projectId] || []).slice(liveBefore);
                    projectLogs.value[projectId] = entries.concat(arrivedDuringFetch);
                    findingIndex.value[projectId] = {};

                    // Восстановить finding-карточки из 03_findings.json + 03_findings_review.json
                    await restoreFindingCards(projectId);
                }
            } catch (e) {
                console.error('Failed to load project log:', e);
            } finally {
                logLoading.value = false;
            }
        }

        async function restoreFindingCards(projectId) {
            /** Восстановить finding-карточки после refresh из файлов _output/. */
            try {
                const resp = await fetch(`/api/findings/${encodeURIComponent(projectId)}`);
                if (!resp.ok) return;
                const fd = await resp.json();
                const findings = (fd && fd.findings) || [];
                if (findings.length === 0) return;

                if (!findingIndex.value[projectId]) findingIndex.value[projectId] = {};

                // Добавить карточку «Размышление завершено» + карточки всех замечаний
                const pseudoTime = '';
                for (const f of findings) {
                    const card = {
                        kind: 'finding',
                        time: pseudoTime,
                        stage: 'findings_merge',
                        finding_id: f.id || '',
                        severity: f.severity || '',
                        category: f.category || '',
                        problem: f.problem || f.title || '',
                        sheet: f.sheet,
                        page: f.page,
                        status: 'confirmed',  // все замечания в итоговом файле уже прошли critic/corrector
                        rejectVerdict: '',
                        rejectReason: '',
                    };
                    projectLogs.value[projectId].push(card);
                    if (card.finding_id) {
                        findingIndex.value[projectId][card.finding_id] = card;
                    }
                }
                findingStage.value = {
                    ...findingStage.value,
                    [projectId]: 'done',
                };
            } catch (e) {
                console.warn('Failed to restore finding cards:', e);
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
            // Счётчик переподключений должен пережить пересоздание сокета
            // (closeProjectWS его сбрасывает): по нему onopen понимает, что это
            // reconnect и нужен ресинк, а onclose наращивает backoff.
            const prevReconnects = (wsCurrentProjectId === projectId) ? wsProjectReconnects : 0;
            closeProjectWS();
            wsCurrentProjectId = projectId;
            wsProjectReconnects = prevReconnects;
            const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
            wsProject = new WebSocket(`${proto}//${location.host}/ws/audit/${encodeURIComponent(projectId)}`);
            wsProject.onopen = () => {
                wsConnected.value = true;
                const wasReconnect = wsProjectReconnects > 0;
                wsProjectReconnects = 0;
                // После обрыва пропущенные WS-события никто не дошлёт —
                // ресинхронизируемся явно: live-статус + карточка проекта +
                // лог из файла (мог прийти log_reset/log_stage_reset).
                if (wasReconnect) {
                    pollLiveStatus();
                    refreshProjectCardSilently(projectId);
                    if (logProjectId.value === projectId) {
                        loadProjectLog(projectId);
                    }
                }
            };
            wsProject.onclose = () => {
                wsConnected.value = false;
                // Переподключаемся, пока мы в project-режиме для этого проекта.
                // Потолка попыток нет: раньше после 5 неудач (сон ноутбука,
                // рестарт туннеля) live-обновления молча умирали до F5.
                if (wsMode === 'project' && wsCurrentProjectId === projectId) {
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
            wsGlobal.onopen = () => {
                wsConnected.value = true;
                // После обрыва WS могли потеряться progress-события обеих
                // очередей, поэтому восстанавливаем их состояние через REST.
                fetchPrepareQueue();
                refreshBatchQueue();
            };
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
            // Проставляем kind='log' по умолчанию для обратной совместимости
            if (!entry.kind) entry.kind = 'log';
            projectLogs.value[projectId].push(entry);
            // Авто-скролл если просматриваем этот проект
            if (logProjectId.value === projectId && logAutoScroll.value) {
                nextTick(() => {
                    const el = logContainer.value;
                    if (el) el.scrollTop = el.scrollHeight;
                });
            }
        }

        function pushFindingCard(projectId, card) {
            /** Добавить карточку замечания в unified-поток и проиндексировать по finding_id. */
            if (!projectId) return;
            if (!projectLogs.value[projectId]) projectLogs.value[projectId] = [];
            if (!findingIndex.value[projectId]) findingIndex.value[projectId] = {};
            projectLogs.value[projectId].push(card);
            if (card.finding_id) {
                findingIndex.value[projectId][card.finding_id] = card;
            }
            if (logProjectId.value === projectId && logAutoScroll.value) {
                nextTick(() => {
                    const el = logContainer.value;
                    if (el) el.scrollTop = el.scrollHeight;
                });
            }
        }

        function applyFindingVerdict(projectId, verdictMsg) {
            /** Обновить статус карточки по вердикту критика. */
            const idx = findingIndex.value[projectId];
            if (!idx) return;
            const card = idx[verdictMsg.finding_id];
            if (!card) return;
            if (verdictMsg.verdict === 'pass') {
                card.status = 'confirmed';
            } else {
                card.status = 'rejected';
                card.rejectVerdict = verdictMsg.verdict || '';
                card.rejectReason = verdictMsg.details || '';
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
                    stage: msg.data.stage || '',
                });
            } else if (msg.type === 'log_reset') {
                // Свежий прогон: лог начат с нуля — очищаем вкладку целиком
                projectLogs.value[pid] = [];
                findingIndex.value[pid] = {};
                findingStage.value = { ...findingStage.value, [pid]: '' };
                if (logProjectId.value === pid) logSectionCollapsed.value = {};
            } else if (msg.type === 'log_stage_reset') {
                // Перезапуск этапа: удаляем только записи его секции
                const stages = new Set(msg.data.stages || []);
                const list = projectLogs.value[pid];
                if (list && stages.size) {
                    projectLogs.value[pid] = list.filter(e => !stages.has(e.stage || ''));
                    // Карточки замечаний живут в секции свода — при её сбросе
                    // индекс finding_id → карточка больше не актуален
                    if (stages.has('findings_merge')) {
                        findingIndex.value[pid] = {};
                    }
                }
            } else if (msg.type === 'progress') {
                // Update current project if viewing it
                if (currentProject.value && currentProject.value.project_id === pid) {
                    currentProject.value.completed_batches = msg.data.current;
                    currentProject.value.total_batches = msg.data.total;
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
                    stage: 'excel',
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
                // Детальный список «Статус конвейера» приходит тем же сообщением.
                // Без этого список обновлялся только при полной перезагрузке
                // карточки и отставал от баннера на целый этап.
                const summary = msg.data.pipeline_summary;
                if (Array.isArray(summary)
                    && currentProject.value && currentProject.value.project_id === pid) {
                    currentProject.value.pipeline_summary = summary;
                }
            } else if (msg.type === 'error') {
                pushToProjectLog(pid, {
                    time: time,
                    level: 'error',
                    stage: msg.data.stage || '',
                    message: msg.data.message || 'Неизвестная ошибка',
                });
            } else if (msg.type === 'batch_progress') {
                batchQueue.value = msg.data;
                batchRunning.value = (msg.data.status || 'running') === 'running' && !msg.data.complete;
                if (msg.data.complete) {
                    refreshProjects();
                    selectedProjects.value = new Set();
                    selectAllChecked.value = false;
                }
            } else if (msg.type === 'prepare_queue_progress') {
                prepareQueue.value = msg.data;
                // Когда любой prepare-job завершается — обновим карточки проектов
                if (msg.data.status === 'idle' || (msg.data.completed + msg.data.failed === msg.data.total)) {
                    refreshProjects();
                }
            } else if (msg.type === 'finding_stage') {
                // Смена фазы «размышления модели»
                findingStage.value = {
                    ...findingStage.value,
                    [pid]: msg.data.stage || '',
                };
                // При начале новой фазы merge — новый свод полностью заменяет
                // набор замечаний: убираем и индекс, и старые finding-карточки
                // (в т.ч. восстановленные после F5), иначе при retry свода они
                // задваиваются. Лог-строки не трогаем: карточки отражают
                // ТЕКУЩИЙ 03_findings.json, а не историю процесса.
                if (msg.data.stage === 'merge') {
                    findingIndex.value[pid] = {};
                    const list = projectLogs.value[pid];
                    if (list && list.length) {
                        projectLogs.value[pid] = list.filter(e => e.kind !== 'finding');
                    }
                }
            } else if (msg.type === 'finding_added') {
                pushFindingCard(pid, {
                    kind: 'finding',
                    time: time,
                    stage: 'findings_merge',
                    finding_id: msg.data.finding_id,
                    severity: msg.data.severity || '',
                    category: msg.data.category || '',
                    problem: msg.data.problem || '',
                    sheet: msg.data.sheet,
                    page: msg.data.page,
                    status: 'pending',
                    rejectVerdict: '',
                    rejectReason: '',
                });
            } else if (msg.type === 'finding_verdict') {
                applyFindingVerdict(pid, msg.data);
            } else if (msg.type === 'cli_summary') {
                const summaryEntry = buildCliSummaryEntry(msg.data || {}, time);
                if (summaryEntry) pushToProjectLog(pid, summaryEntry);
            }
        }

        // ─── Простой Markdown-рендер (без внешних библиотек) ───
        function renderSimpleMarkdown(text) {
            if (!text) return '';
            // 1. Экранирование HTML
            const escape = (s) => s
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;');
            let s = escape(text);

            // 2. Таблицы — превращаем pipe-таблицы в <table>
            // Паттерн: несколько строк подряд, все начинаются с |
            const lines = s.split('\n');
            const out = [];
            let i = 0;
            while (i < lines.length) {
                const line = lines[i];
                if (line.trim().startsWith('|') && line.trim().endsWith('|')) {
                    // Собираем все строки таблицы
                    const tableLines = [];
                    while (i < lines.length && lines[i].trim().startsWith('|') && lines[i].trim().endsWith('|')) {
                        tableLines.push(lines[i].trim());
                        i++;
                    }
                    if (tableLines.length >= 2) {
                        // Первая — заголовок, вторая — разделитель, остальные — данные
                        const parseRow = (row) => row.slice(1, -1).split('|').map(c => c.trim());
                        const header = parseRow(tableLines[0]);
                        const rows = tableLines.slice(2).map(parseRow);
                        let tbl = '<table class="md-table"><thead><tr>';
                        header.forEach(h => { tbl += '<th>' + h + '</th>'; });
                        tbl += '</tr></thead><tbody>';
                        rows.forEach(r => {
                            tbl += '<tr>';
                            r.forEach(c => { tbl += '<td>' + c + '</td>'; });
                            tbl += '</tr>';
                        });
                        tbl += '</tbody></table>';
                        out.push(tbl);
                        continue;
                    } else {
                        out.push(...tableLines);
                    }
                } else {
                    out.push(line);
                    i++;
                }
            }
            s = out.join('\n');

            // 3. Инлайн: **bold**, `code`
            s = s.replace(/\*\*([^*\n]+)\*\*/g, '<strong>$1</strong>');
            s = s.replace(/`([^`\n]+)`/g, '<code>$1</code>');

            // 4. Списки: строки, начинающиеся с "- "
            s = s.replace(/(^|\n)- (.+)/g, '$1<li>$2</li>');
            s = s.replace(/(<li>[^]*?<\/li>(?:\n<li>[^]*?<\/li>)*)/g, (m) => '<ul>' + m.replace(/\n/g, '') + '</ul>');

            // 5. Переносы строк (вне таблиц/списков)
            s = s.replace(/\n/g, '<br>');
            // Убираем лишние <br> вокруг блочных элементов
            s = s.replace(/<br>(<table|<ul|<\/table>|<\/ul>)/g, '$1');
            s = s.replace(/(<\/table>|<\/ul>)<br>/g, '$1');
            return s;
        }

        // ─── Expert Review (экспертная оценка) ───
        async function toggleExpertReview() {
            expertReviewMode.value = !expertReviewMode.value;
            if (expertReviewMode.value && currentProjectId.value) {
                await loadExpertDecisions();
            }
        }

        // ─── Пользователи (сотрудники) ───
        async function loadUsers() {
            usersLoading.value = true;
            try {
                const resp = await fetch('/api/users');
                const data = await resp.json();
                usersList.value = data.users || [];
                usersCurrentId.value = data.current_id || null;
                usersAuthEnabled.value = !!data.auth_enabled;
                usersLoggedInUsername.value = data.logged_in_username || null;
                usersLoggedInMatched.value = !!data.logged_in_matched;
            } catch (e) {
                console.error('Load users error:', e);
            } finally {
                usersLoading.value = false;
            }
        }

        function currentUserName() {
            const u = usersList.value.find(x => x.id === usersCurrentId.value);
            return u ? u.name : '';
        }

        // ─────────────────────────────────────────────────────────────────
        // График производства работ (production work schedule)
        //
        // Реальные события грузятся из backend GET /api/schedule?from=&to=
        // (агрегация knowledge_base/decisions_log.json). Если API недоступен
        // или вернул пустой результат — DEV-fallback на mock-данные ниже
        // (schedEvents → _schedMockEvents, schedEngineers → _SCHED_MOCK_ENGINEERS).
        // План (schedPlans) пока локальный/mock — backend-стор work_plans.json
        // делается отдельным этапом.
        // ─────────────────────────────────────────────────────────────────
        const schedMode = ref('month');                // всегда 'month' — переключатель «Неделя» удалён из UI, week-ветки в коде не используются
        const schedAnchor = ref(_schedStartOfDay(new Date()));  // опорный день периода
        const schedPopover = ref(null);                // {engId, key} — раскрытый список проектов в ячейке
        const schedFiltersOpen = ref(false);
        const schedHiddenEngineers = ref([]);          // id скрытых инженеров (фильтр)
        const schedPlanEdit = ref(false);              // режим редактирования плана (для админа)

        // Состояние загрузки из API.
        const schedApiEvents = ref(null);              // массив событий из /api/schedule или null (не загружено)
        const schedApiEngineers = ref(null);           // массив инженеров из API или null
        const schedLoading = ref(false);
        const schedError = ref(false);                 // запрос упал → показываем mock

        // DEV-fallback: инженеры графика, если API недоступен/пуст.
        const _SCHED_MOCK_ENGINEERS = [
            { id: 'uzun',     name: 'Узун А. И.' },
            { id: 'grivapsh', name: 'Гривапш А. А.' },
            { id: 'kuldyaev', name: 'Кульдяев Ф. С.' },
            { id: 'olar',     name: 'Оларь М. И.' },
            { id: 'repnikov', name: 'Репников И. А.' },
        ];

        // Закреплённый состав бригады — эти инженеры показываются в графике
        // ВСЕГДА (даже если за период у них нет ни одного решения в
        // decisions_log). id = eng_slug ФИО из users.json — он совпадает с id,
        // которые присылает /api/schedule, поэтому дедуп по id «склеивает»
        // строку, если у инженера всё-таки есть события за период.
        const _SCHED_REQUIRED_ENGINEERS = [
            { id: 'kuldyaev-f-s', name: 'Кульдяев Ф. С.', role: 'expert' },
            { id: 'repnikov-i-a', name: 'Репников И. А.', role: 'expert' },
            { id: 'grivapsh-a-a', name: 'Гривапш А. А.', role: 'expert' },
            { id: 'kalinina-a',   name: 'Калинина А.',    role: 'expert' },
        ];

        // План работ грузится из backend GET /api/schedule/plan?period_type=&from=&to=
        // и редактируется админом через PUT /api/schedule/plan.
        const schedPlanMap = ref({});      // engineer_id -> plan (сохранённый, текущий период)
        const schedPlanDraft = ref({});    // engineer_id -> plan (буфер редактирования)
        const schedPlanSaving = ref(false);
        const schedPlanMsg = ref(null);    // {kind:'ok'|'err', text}
        // План по умолчанию, если записи в work_plans.json нет — чтобы статистика
        // была осмысленной (backend дефолты сам не придумывает).
        const _SCHED_DEFAULT_PLAN = { week: 5, month: 20 };

        // ── date helpers (всё локально, без зависимостей) ──
        function _schedStartOfDay(d) { const x = new Date(d); x.setHours(0, 0, 0, 0); return x; }
        function _schedKey(d) {
            const x = new Date(d);
            const m = String(x.getMonth() + 1).padStart(2, '0');
            const day = String(x.getDate()).padStart(2, '0');
            return `${x.getFullYear()}-${m}-${day}`;
        }
        function _schedMonday(d) {
            const x = _schedStartOfDay(d);
            const wd = (x.getDay() + 6) % 7;   // 0 = понедельник
            x.setDate(x.getDate() - wd);
            return x;
        }
        function _schedAddDays(d, n) { const x = new Date(d); x.setDate(x.getDate() + n); return x; }
        const _SCHED_DOW = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'];
        const _SCHED_MON_GEN = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня',
            'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'];
        const _SCHED_MON_NOM = ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь',
            'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'];

        // DEV-fallback MOCK события «инженер загрузил/проверил проект в этот день».
        // Используются только если API недоступен или вернул пусто. Генерируются
        // относительно ТЕКУЩЕЙ недели/месяца, чтобы демо всегда попадало в период.
        const _schedMockEvents = computed(() => {
            const today = _schedStartOfDay(new Date());
            const monday = _schedMonday(today);
            const monthFirst = new Date(today.getFullYear(), today.getMonth(), 1);
            const daysInMonth = new Date(today.getFullYear(), today.getMonth() + 1, 0).getDate();
            const ev = [];
            const add = (engId, dayDate, short, full, section) =>
                ev.push({ engId, key: _schedKey(dayDate), short, full, section });
            const dom = (n) => { const x = new Date(monthFirst); x.setDate(Math.min(n, daysInMonth)); return x; };

            // ── Текущая неделя (offset 0=Пн … 6=Вс), со стэк-ячейками для «+N» ──
            // Узун А. И.
            add('uzun', _schedAddDays(monday, 0), '214. Alia', '214. Москфильмовская «Alia» (ASTERUS)', 'AR');
            add('uzun', _schedAddDays(monday, 2), '214. Alia', '214. Москфильмовская «Alia» (ASTERUS)', 'AR');
            add('uzun', _schedAddDays(monday, 2), '213. Metromash', '213. Мосфильмовская 31А «King&Sons» (Metromash)', 'AR');
            add('uzun', _schedAddDays(monday, 2), 'ДС3-АР', 'АА-БЭ-03-ДС3-АР (Балчуг)', 'AR'); // Ср → «214. Alia +2»
            // Гривапш А. А.
            add('grivapsh', _schedAddDays(monday, 1), 'Asterus', 'Asterus — общий комплекс', 'EOM');
            add('grivapsh', _schedAddDays(monday, 1), 'ОДИ', 'ОДИ — отдельные доработки', 'EOM'); // Вт → «Asterus +1»
            add('grivapsh', _schedAddDays(monday, 3), 'ОДИ', 'ОДИ — отдельные доработки', 'EOM');
            // Кульдяев Ф. С.
            add('kuldyaev', _schedAddDays(monday, 0), 'ИКЕО', 'ИКЕО — интегрированная комплексная ...', 'SS');
            add('kuldyaev', _schedAddDays(monday, 3), 'ЭЭ', 'ЭЭ — электроснабжение', 'EOM');
            add('kuldyaev', _schedAddDays(monday, 4), 'ИКЕО', 'ИКЕО — интегрированная комплексная ...', 'SS');
            // Оларь М. И.
            add('olar', _schedAddDays(monday, 1), 'ПОС', 'ПОС — проект организации строительства', 'POS');
            add('olar', _schedAddDays(monday, 2), 'ПЗУ', 'ПЗУ — планировочная организация ЗУ', 'GP');
            // Репников И. А.
            add('repnikov', _schedAddDays(monday, 0), 'АР1', 'АР1 — архитектурные решения, корп. 1', 'AR');
            add('repnikov', _schedAddDays(monday, 0), 'АР2', 'АР2 — архитектурные решения, корп. 2', 'AR'); // Пн → «АР1 +1»
            add('repnikov', _schedAddDays(monday, 2), 'КЖ', 'КЖ — конструкции железобетонные', 'KJ');
            add('repnikov', _schedAddDays(monday, 4), 'АР1', 'АР1 — архитектурные решения, корп. 1', 'AR');

            // ── Дополнительные события по месяцу (для режима «Месяц») ──
            add('uzun', dom(3), 'ДС3-АР', 'АА-БЭ-03-ДС3-АР (Балчуг)', 'AR');
            add('grivapsh', dom(6), 'Asterus', 'Asterus — общий комплекс', 'EOM');
            add('kuldyaev', dom(9), 'ЭЭ', 'ЭЭ — электроснабжение', 'EOM');
            add('repnikov', dom(11), 'КЖ', 'КЖ — конструкции железобетонные', 'KJ');
            add('olar', dom(24), 'ПОС', 'ПОС — проект организации строительства', 'POS');
            add('uzun', dom(26), '213. Metromash', '213. Мосфильмовская 31А «King&Sons» (Metromash)', 'AR');
            add('repnikov', dom(27), 'АР2', 'АР2 — архитектурные решения, корп. 2', 'AR');
            add('kuldyaev', dom(28), 'ИКЕО', 'ИКЕО — интегрированная комплексная ...', 'SS');
            return ev;
        });

        // Эффективный источник: показываем РЕАЛЬНЫЕ данные, как только backend
        // успешно ответил (массив событий получен — пусть даже пустой). На mock
        // (демо) откатываемся ТОЛЬКО если запрос упал / ещё не загружен. Это
        // отличает «за период нет решений» (живой пустой график) от «API
        // недоступен» — раньше пустой ответ ошибочно прятал реальных инженеров.
        const schedApiOk = computed(() => Array.isArray(schedApiEvents.value));
        const schedUsingMock = computed(() => !schedApiOk.value);
        const schedEvents = computed(() =>
            schedUsingMock.value ? _schedMockEvents.value : schedApiEvents.value);
        const schedEngineers = computed(() => {
            if (schedUsingMock.value) return _SCHED_MOCK_ENGINEERS;
            // Живые данные: API-инженеры (те, у кого были решения за период) плюс
            // ЗАКРЕПЛЁННЫЙ состав — он показывается ВСЕГДА, даже без решений.
            // Дедуп по id (id = eng_slug ФИО, совпадает с id из API): реальная
            // запись перекрывает заглушку. Сортировка по имени.
            const byId = new Map();
            for (const e of _SCHED_REQUIRED_ENGINEERS) byId.set(e.id, { ...e });
            for (const e of (schedApiEngineers.value || [])) {
                if (e && e.id) byId.set(e.id, e);
            }
            return Array.from(byId.values()).sort((a, b) =>
                (a.name || '').toLowerCase().localeCompare((b.name || '').toLowerCase(), 'ru'));
        });

        // Баннер состояния над графиком.
        const schedNoticeKind = computed(() => {
            if (schedLoading.value) return 'loading';
            if (schedError.value) return 'error';
            if (schedUsingMock.value) return 'mock';
            return 'live';
        });
        const schedNoticeText = computed(() => ({
            loading: 'Загрузка графика…',
            error:   'Не удалось загрузить данные графика — показаны тестовые данные.',
            mock:    'Реальных событий за период нет — показаны демонстрационные данные.',
            live:    'Данные из knowledge_base/decisions_log.json.',
        }[schedNoticeKind.value]));

        function _schedPeriodRange() {
            if (schedMode.value === 'week') {
                const mon = _schedMonday(schedAnchor.value);
                return { from: _schedKey(mon), to: _schedKey(_schedAddDays(mon, 6)) };
            }
            const a = schedAnchor.value;
            const first = new Date(a.getFullYear(), a.getMonth(), 1);
            const last = new Date(a.getFullYear(), a.getMonth() + 1, 0);
            return { from: _schedKey(first), to: _schedKey(last) };
        }

        async function schedLoad() {
            const { from, to } = _schedPeriodRange();
            schedLoading.value = true;
            schedError.value = false;
            try {
                const resp = await fetch(`/api/schedule?from=${from}&to=${to}`);
                if (!resp.ok) throw new Error('HTTP ' + resp.status);
                const data = await resp.json();
                schedApiEvents.value = Array.isArray(data.events) ? data.events : [];
                schedApiEngineers.value = Array.isArray(data.engineers) ? data.engineers : [];
            } catch (e) {
                console.error('Schedule load error:', e);
                schedError.value = true;       // → DEV fallback на mock-данные
                schedApiEvents.value = null;
                schedApiEngineers.value = null;
            } finally {
                schedLoading.value = false;
            }
            // План грузим после событий (UI работает и без планов).
            schedLoadPlans();
        }

        // Перезагрузка при смене режима/периода (Неделя/Месяц, ‹ › Сегодня).
        // Если идёт правка плана — выходим из неё: черновик относится к СТАРОМУ
        // периоду, его нельзя сохранять в новый (иначе затрём чужой период).
        watch([schedMode, schedAnchor], () => {
            if (schedPlanEdit.value) schedCancelPlanEdit();
            schedLoad();
        });

        const schedVisibleEngineers = computed(() =>
            schedEngineers.value.filter(e => !schedHiddenEngineers.value.includes(e.id)));

        function _schedDayMeta(d, todayKey) {
            const dow = (d.getDay() + 6) % 7;
            return {
                key: _schedKey(d),
                dom: d.getDate(),
                dowLabel: _SCHED_DOW[dow],
                isToday: _schedKey(d) === todayKey,
                isWeekend: dow >= 5,
            };
        }

        // Колонки графика: 7 дней (неделя) либо все дни месяца.
        const schedDays = computed(() => {
            const todayKey = _schedKey(new Date());
            const out = [];
            if (schedMode.value === 'week') {
                const mon = _schedMonday(schedAnchor.value);
                for (let i = 0; i < 7; i++) out.push(_schedDayMeta(_schedAddDays(mon, i), todayKey));
            } else {
                const a = schedAnchor.value;
                const first = new Date(a.getFullYear(), a.getMonth(), 1);
                const days = new Date(a.getFullYear(), a.getMonth() + 1, 0).getDate();
                for (let i = 0; i < days; i++) out.push(_schedDayMeta(_schedAddDays(first, i), todayKey));
            }
            return out;
        });
        const schedDayKeys = computed(() => new Set(schedDays.value.map(d => d.key)));

        // Индекс событий по ячейке engId|key.
        const schedEventIndex = computed(() => {
            const idx = {};
            for (const e of schedEvents.value) (idx[e.engId + '|' + e.key] ||= []).push(e);
            return idx;
        });
        function schedCell(engId, key) { return schedEventIndex.value[engId + '|' + key] || []; }

        const schedPeriodLabel = computed(() => {
            if (schedMode.value === 'week') {
                const mon = _schedMonday(schedAnchor.value);
                const sun = _schedAddDays(mon, 6);
                return `${mon.getDate()} ${_SCHED_MON_GEN[mon.getMonth()]} — ` +
                    `${sun.getDate()} ${_SCHED_MON_GEN[sun.getMonth()]} ${sun.getFullYear()}`;
            }
            const a = schedAnchor.value;
            return `${_SCHED_MON_NOM[a.getMonth()]} ${a.getFullYear()}`;
        });

        function schedSetMode(m) { if (schedMode.value !== m) { schedMode.value = m; schedPopover.value = null; } }
        function schedPrev() {
            schedAnchor.value = schedMode.value === 'week'
                ? _schedAddDays(_schedMonday(schedAnchor.value), -7)
                : new Date(schedAnchor.value.getFullYear(), schedAnchor.value.getMonth() - 1, 1);
            schedPopover.value = null;
        }
        function schedNext() {
            schedAnchor.value = schedMode.value === 'week'
                ? _schedAddDays(_schedMonday(schedAnchor.value), 7)
                : new Date(schedAnchor.value.getFullYear(), schedAnchor.value.getMonth() + 1, 1);
            schedPopover.value = null;
        }
        function schedToday() { schedAnchor.value = _schedStartOfDay(new Date()); schedPopover.value = null; }

        function schedToggleCell(engId, key, hasEvents, ev) {
            if (!hasEvents) { schedPopover.value = null; return; }
            const p = schedPopover.value;
            if (p && p.engId === engId && p.key === key) { schedPopover.value = null; return; }
            // fixed-позиционирование от кликнутого элемента — чтобы поповер не резался
            // overflow-контейнером таблицы (.sched-gridwrap) и был виден целиком
            let pos = null;
            const el = ev && (ev.currentTarget || ev.target);
            if (el && el.getBoundingClientRect) {
                const r = el.getBoundingClientRect();
                const vw = window.innerWidth || 1200;
                const vh = window.innerHeight || 800;
                const count = schedCell(engId, key).length;
                const estH = 44 + count * 22;              // прикидка высоты поповера
                const PW = 320;                            // max-width поповера
                const flipUp = (r.bottom + estH > vh - 8) && (r.top - estH > 8);
                let left = Math.max(8, r.left);
                if (left + PW > vw - 8) left = Math.max(8, vw - 8 - PW);
                pos = {
                    left: Math.round(left),
                    top: flipUp ? null : Math.round(r.bottom - 2),
                    bottom: flipUp ? Math.round(vh - r.top - 2) : null,
                };
            }
            schedPopover.value = { engId, key, pos };
        }
        function schedIsPopover(engId, key) {
            const p = schedPopover.value;
            return !!p && p.engId === engId && p.key === key;
        }
        function schedPopoverStyle() {
            const p = schedPopover.value;
            if (!p || !p.pos) return {};
            const st = { position: 'fixed', left: p.pos.left + 'px', right: 'auto' };
            if (p.pos.top != null) st.top = p.pos.top + 'px';
            if (p.pos.bottom != null) st.bottom = p.pos.bottom + 'px';
            return st;
        }
        function schedClosePopover() { schedPopover.value = null; }

        function schedToggleEngineer(id) {
            const arr = schedHiddenEngineers.value;
            schedHiddenEngineers.value = arr.includes(id) ? arr.filter(x => x !== id) : [...arr, id];
        }
        function schedIsEngineerHidden(id) { return schedHiddenEngineers.value.includes(id); }

        // Право редактировать план: при выключенной auth (dev) — разрешено;
        // при включённой — только сотрудник с ролью admin. Backend защищает PUT
        // независимо от этого флага (это лишь видимость кнопки).
        const schedIsAdmin = computed(() => {
            if (!usersAuthEnabled.value) return true;
            const u = usersList.value.find(x => x.id === usersCurrentId.value);
            return !!u && u.role === 'admin';
        });

        // ── Расход подписки Claude по инженерам (модалка) ──────────
        const subSpendOpen = ref(false);
        const subSpendLoading = ref(false);
        const subSpendData = ref(null);
        const _SUB_SPEND_COLORS = {
            uzun: '#8b5cf6', repnikov: '#ec4899', grivapsch: '#f472b6',
            kuldiaev: '#22c55e', kalinina: '#f59e0b',
        };
        const _SUB_SPEND_WD = ['вс', 'пн', 'вт', 'ср', 'чт', 'пт', 'сб'];
        // Родительный падеж для «с понедельника 17.08 …»: день сброса приходит
        // с бэкенда (week_start_date), в UI ничего не зашито жёстко.
        const _SUB_SPEND_WD_GEN = [
            'воскресенья', 'понедельника', 'вторника', 'среды',
            'четверга', 'пятницы', 'субботы',
        ];
        function _subSpendWdIndex(isoDate) {
            const p = String(isoDate || '').split('-').map(Number);
            if (p.length !== 3 || p.some(isNaN)) return null;
            return new Date(p[0], p[1] - 1, p[2]).getDay();
        }
        function subSpendColor(id) { return _SUB_SPEND_COLORS[id] || '#94a3b8'; }
        function subSpendInitials(name) {
            const parts = String(name || '').split(/\s+/).filter(Boolean);
            return parts.slice(0, 2).map(s => s[0]).join('').toUpperCase();
        }
        function subSpendDayLabel(iso) {
            const p = String(iso || '').split('-').map(Number);
            if (p.length !== 3) return iso;
            const wd = _SUB_SPEND_WD[new Date(p[0], p[1] - 1, p[2]).getDay()];
            return `${wd} ${String(p[2]).padStart(2, '0')}.${String(p[1]).padStart(2, '0')}`;
        }
        function subSpendTok(n) {
            n = Number(n) || 0;
            if (n >= 1e9) return (n / 1e9).toFixed(n >= 1e10 ? 0 : 1) + ' млрд';
            if (n >= 1e6) return Math.round(n / 1e6) + 'М';
            if (n >= 1e3) return Math.round(n / 1e3) + 'К';
            return String(n);
        }
        const subSpendWeekText = computed(() => {
            const d = subSpendData.value;
            if (!d || !d.week_start_date) return '';
            const [y, m, day] = d.week_start_date.split('-');
            const wdIdx = _subSpendWdIndex(d.week_start_date);
            const wd = wdIdx === null ? '' : _SUB_SPEND_WD_GEN[wdIdx] + ' ';
            return `с ${wd}${day}.${m} ${d.week_start_time || '17:00'} (сброс лимитов)`;
        });
        // Короткая подпись плана: «сброс пн 17:00 MSK».
        const subSpendResetText = computed(() => {
            const d = subSpendData.value;
            const wdIdx = d ? _subSpendWdIndex(d.week_start_date) : null;
            const wd = wdIdx === null ? 'пн' : _SUB_SPEND_WD[wdIdx];
            const time = (d && d.week_start_time) || '17:00';
            const plan = (d && d.plan) || 'Claude Max 20x';
            return `${plan} · сброс ${wd} ${time} MSK · оценка $`;
        });
        async function subSpendLoad() {
            subSpendLoading.value = true;
            try {
                const resp = await fetch('/api/usage/subscription-by-person');
                if (!resp.ok) throw new Error('HTTP ' + resp.status);
                subSpendData.value = await resp.json();
            } catch (e) {
                subSpendData.value = null;
            } finally {
                subSpendLoading.value = false;
            }
        }

        function schedPlanFor(engId) {
            const v = schedPlanMap.value[engId];
            return (typeof v === 'number') ? v : (_SCHED_DEFAULT_PLAN[schedMode.value] || 0);
        }
        function schedDraftFor(engId) {
            const v = schedPlanDraft.value[engId];
            return (typeof v === 'number') ? v : schedPlanFor(engId);
        }
        function schedSetPlanDraft(engId, val) {
            const n = Math.max(0, Math.min(999, parseInt(val, 10) || 0));
            schedPlanDraft.value = { ...schedPlanDraft.value, [engId]: n };
        }
        function schedTogglePlanEdit() {
            if (schedPlanEdit.value) { schedPlanEdit.value = false; return; }
            // Сеем черновик для ВСЕХ инженеров (не только видимых), чтобы PUT не
            // затёр план скрытых фильтром: PUT перезаписывает период целиком.
            const draft = {};
            for (const e of schedEngineers.value) draft[e.id] = schedPlanFor(e.id);
            schedPlanDraft.value = draft;
            schedPlanMsg.value = null;
            schedPlanEdit.value = true;
        }
        function schedCancelPlanEdit() { schedPlanEdit.value = false; schedPlanMsg.value = null; }

        async function schedLoadPlans() {
            const { from, to } = _schedPeriodRange();
            try {
                const resp = await fetch(`/api/schedule/plan?from=${from}&to=${to}&period_type=${schedMode.value}`);
                if (!resp.ok) throw new Error('HTTP ' + resp.status);
                const data = await resp.json();
                const map = {};
                for (const p of (data.plans || [])) {
                    if (p && p.engineer_id != null) map[p.engineer_id] = Number(p.plan) || 0;
                }
                schedPlanMap.value = map;
            } catch (e) {
                console.error('Schedule plans load error:', e);
                schedPlanMap.value = {};   // нет планов → дефолты в schedPlanFor
            }
        }

        async function schedSavePlans() {
            const { from, to } = _schedPeriodRange();
            schedPlanSaving.value = true;
            schedPlanMsg.value = null;
            try {
                // Шлём план ВСЕХ инженеров (PUT перезаписывает период целиком).
                const plans = schedEngineers.value.map(e => ({
                    engineer_id: e.id, engineer_name: e.name, plan: schedDraftFor(e.id),
                }));
                const resp = await fetch('/api/schedule/plan', {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        period_type: schedMode.value,
                        period_start: from, period_end: to,
                        object_id: null, plans,
                    }),
                });
                if (!resp.ok) {
                    let detail = 'HTTP ' + resp.status;
                    try { const j = await resp.json(); detail = j.detail || detail; } catch (_) {}
                    throw new Error(detail);
                }
                schedPlanEdit.value = false;
                await schedLoadPlans();
                schedPlanMsg.value = { kind: 'ok', text: 'План сохранён' };
            } catch (e) {
                console.error('Schedule plan save error:', e);
                schedPlanMsg.value = { kind: 'err', text: 'Не удалось сохранить план: ' + (e.message || e) };
            } finally {
                schedPlanSaving.value = false;
            }
        }

        function schedFactFor(engId) {
            const keys = schedDayKeys.value;
            return schedEvents.value.filter(e => e.engId === engId && keys.has(e.key)).length;
        }
        function schedPctClass(pct) {
            if (pct >= 100) return 'sched-ok';
            if (pct >= 70) return 'sched-warn';
            return 'sched-low';
        }
        // Цвет выполнения — фирменный бирюзовый.
        function schedPctColor(pct) {
            return 'var(--teal)';
        }
        // Замечания: согласованные/несогласованные за период. Backend пока НЕ
        // отдаёт эти счётчики в /api/schedule (решение схлопывается при
        // агрегации decisions_log) → null → в UI показывается «—». Если поля
        // появятся в engineers[] (agreed/disagreed или remarks_agreed/
        // remarks_disagreed) — подхватятся автоматически, без правок фронта.
        function _schedRemarkCount(e, ...keys) {
            for (const k of keys) if (typeof e[k] === 'number') return e[k];
            return null;
        }
        // Процент принятых (согласованных) = принято / (принято + отклонено).
        // null, если счётчиков нет вовсе или суммарно 0 (делить не на что).
        function _schedAcceptPct(agreed, disagreed) {
            if (agreed == null && disagreed == null) return null;
            const total = (agreed || 0) + (disagreed || 0);
            if (total <= 0) return null;
            return Math.round(((agreed || 0) / total) * 100);
        }
        const schedStats = computed(() => schedVisibleEngineers.value.map(e => {
            const fact = schedFactFor(e.id);
            const plan = schedPlanFor(e.id);
            const pct = plan > 0 ? Math.round((fact / plan) * 100) : (fact > 0 ? 100 : 0);
            const agreed = _schedRemarkCount(e, 'agreed', 'remarks_agreed');
            const disagreed = _schedRemarkCount(e, 'disagreed', 'remarks_disagreed');
            const remarkPct = _schedAcceptPct(agreed, disagreed);
            return { id: e.id, name: e.name, fact, plan, pct, remaining: Math.max(0, plan - fact), agreed, disagreed, remarkPct };
        }));
        const schedTotals = computed(() => {
            const s = schedStats.value;
            const fact = s.reduce((a, x) => a + x.fact, 0);
            const plan = s.reduce((a, x) => a + x.plan, 0);
            const pct = plan > 0 ? Math.round((fact / plan) * 100) : (fact > 0 ? 100 : 0);
            const hasRemarks = s.some(x => x.agreed != null || x.disagreed != null);
            const agreed = hasRemarks ? s.reduce((a, x) => a + (x.agreed || 0), 0) : null;
            const disagreed = hasRemarks ? s.reduce((a, x) => a + (x.disagreed || 0), 0) : null;
            const remarkPct = _schedAcceptPct(agreed, disagreed);
            return { fact, plan, pct, remaining: Math.max(0, plan - fact), engineers: s.length, agreed, disagreed, remarkPct };
        });

        // ── Display-хелперы графика (только отображение, без backend-логики) ──
        // Инициалы инженера для аватара: «Гривапш А. А.» → «ГА».
        function schedInitials(name) {
            const parts = String(name || '').trim().split(/\s+/).filter(Boolean);
            if (!parts.length) return '?';
            if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
            return (parts[0][0] + parts[1][0]).toUpperCase();
        }
        // Статус выполнения для строки статистики (бейдж).
        function schedStatusFor(s) {
            if (!s || s.plan <= 0) return { label: 'Нет плана', cls: 'muted' };
            if (s.pct > 100) return { label: 'Перевыполнение', cls: 'over' };
            if (s.pct >= 100) return { label: 'В плане', cls: 'ok' };
            return { label: 'Отстаёт', cls: 'low' };
        }
        // Детерминированный мягкий цвет аватара по имени/id.
        const _SCHED_AVATAR_PALETTE = [
            { background: '#ede9fe', color: '#6d28d9' },
            { background: '#dbeafe', color: '#1d4ed8' },
            { background: '#dcfce7', color: '#15803d' },
            { background: '#fef3c7', color: '#b45309' },
            { background: '#fce7f3', color: '#be185d' },
            { background: '#ccfbf1', color: '#0f766e' },
        ];
        function schedAvatarStyle(seed) {
            const s = String(seed || '');
            let h = 0;
            for (let i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) >>> 0;
            return _SCHED_AVATAR_PALETTE[h % _SCHED_AVATAR_PALETTE.length];
        }

        // Канонический project_id для API экспертной разметки: реальные папки
        // проектов/контейнеров — без `.pdf`. В id из version-имени V2 может
        // протечь `.pdf` (отображается в `name`), и тогда backend резолвит путь
        // на корень объекта → orphan `_output`. Срезаем хвостовой `.pdf` (вторая
        // линия защиты; основная — на backend resolve_project_dir/save).
        function expertReviewProjectId() {
            return String(currentProjectId.value || '').replace(/\.pdf$/i, '');
        }

        async function loadExpertDecisions() {
            if (!currentProjectId.value) return;
            const map = {};
            try {
                const vid = activeVersionId.value;
                const vq = vid ? `?version_id=${encodeURIComponent(vid)}` : '';
                const resp = await fetch(`/api/knowledge-base/expert-review/${encodeURIComponent(expertReviewProjectId())}${vq}`);
                const data = await resp.json();
                if (data.has_review && data.data && data.data.decisions) {
                    for (const d of data.data.decisions) {
                        map[d.item_id] = { decision: d.decision, rejection_reason: d.rejection_reason || '', item_type: d.item_type || 'finding', carried_over: !!d.carried_over, carried_from_version: d.carried_from_version || '' };
                    }
                }
            } catch (e) { console.warn('Failed to load expert review:', e); }
            expertDecisions.value = map;
        }

        function setExpertDecision(itemId, itemType, decision) {
            const existing = expertDecisions.value[itemId] || { decision: null, rejection_reason: '' };
            if (existing.decision === decision) {
                // Toggle off
                existing.decision = null;
            } else {
                existing.decision = decision;
            }
            existing.item_type = itemType;
            expertDecisions.value = { ...expertDecisions.value, [itemId]: existing };

            // Синхронизация с системой обсуждений (confirmed/rejected/open)
            if (currentProjectId.value) {
                const discType = itemId.startsWith('OPT') ? 'optimization' : 'finding';
                const vid = activeVersionId.value;
                const vq = vid ? `&version_id=${encodeURIComponent(vid)}` : '';
                if (existing.decision) {
                    const status = existing.decision === 'accepted' ? 'confirmed' : 'rejected';
                    const reason = existing.rejection_reason || '';
                    const summary = reason || (status === 'confirmed' ? 'Принято экспертом' : 'Отклонено экспертом');
                    fetch(`/api/discussions/${encodeURIComponent(currentProjectId.value)}/${encodeURIComponent(itemId)}/resolve?type=${discType}${vq}`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ status, summary }),
                    }).catch(() => {});
                } else {
                    fetch(`/api/discussions/${encodeURIComponent(currentProjectId.value)}/${encodeURIComponent(itemId)}/resolve?type=${discType}${vq}`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ status: 'open', summary: '' }),
                    }).catch(() => {});
                }
            }
        }

        function setExpertReason(itemId, reason) {
            const existing = expertDecisions.value[itemId] || { decision: 'rejected', rejection_reason: '' };
            existing.rejection_reason = reason;
            expertDecisions.value = { ...expertDecisions.value, [itemId]: existing };
        }

        async function submitExpertReview() {
            if (!currentProjectId.value) return;
            expertReviewSaving.value = true;
            try {
                const decisions = [];
                const removedIds = [];
                for (const [itemId, d] of Object.entries(expertDecisions.value)) {
                    if (d.decision) {
                        decisions.push({
                            item_id: itemId,
                            item_type: d.item_type || (itemId.startsWith('OPT') ? 'optimization' : 'finding'),
                            decision: d.decision,
                            rejection_reason: d.rejection_reason || null,
                            timestamp: new Date().toISOString(),
                        });
                    } else {
                        removedIds.push(itemId);
                    }
                }
                const vidPost = activeVersionId.value;
                const vqPost = vidPost ? `?version_id=${encodeURIComponent(vidPost)}` : '';
                const resp = await fetch(`/api/knowledge-base/expert-review/${encodeURIComponent(expertReviewProjectId())}${vqPost}`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ decisions, removed_ids: removedIds, reviewer: currentUserName() }),
                });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || `Ошибка сохранения: ${resp.statusText}`);
                }
                const result = await resp.json();
                // Синхронизировать принятые/отклонённые решения с системой обсуждений
                const vid2 = activeVersionId.value;
                const vq2 = vid2 ? `&version_id=${encodeURIComponent(vid2)}` : '';
                for (const d of decisions) {
                    const discType = d.item_id.startsWith('OPT') ? 'optimization' : 'finding';
                    const status = d.decision === 'accepted' ? 'confirmed' : 'rejected';
                    const summary = d.rejection_reason || (status === 'confirmed' ? 'Принято экспертом' : 'Отклонено экспертом');
                    fetch(`/api/discussions/${encodeURIComponent(currentProjectId.value)}/${encodeURIComponent(d.item_id)}/resolve?type=${discType}${vq2}`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ status, summary }),
                    }).catch(() => {});
                }
                // Сбросить статус для отменённых решений
                for (const itemId of removedIds) {
                    const discType = itemId.startsWith('OPT') ? 'optimization' : 'finding';
                    fetch(`/api/discussions/${encodeURIComponent(currentProjectId.value)}/${encodeURIComponent(itemId)}/resolve?type=${discType}${vq2}`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ status: 'open', summary: '' }),
                    }).catch(() => {});
                }
                // Сразу пересчитать две галочки и sidebar-индикатор, не дожидаясь
                // перехода на другую страницу или фонового обновления.
                await refreshProjects();
                alert(`Сохранено: ${result.accepted} принято, ${result.rejected} отклонено`);
            } catch (e) {
                console.error('Submit expert review error:', e);
                alert('Ошибка сохранения: ' + (e.message || e));
            } finally {
                expertReviewSaving.value = false;
            }
        }

        function getExpertDecision(itemId) {
            return (expertDecisions.value[itemId] || {}).decision || null;
        }
        function getExpertReason(itemId) {
            return (expertDecisions.value[itemId] || {}).rejection_reason || '';
        }
        // Авто-перенос вердикта из предыдущей версии (decision carryover).
        function isCarriedOver(itemId) {
            return !!(expertDecisions.value[itemId] || {}).carried_over;
        }
        function carriedFromVersion(itemId) {
            const v = (expertDecisions.value[itemId] || {}).carried_from_version || '';
            return v ? String(v).toUpperCase() : '';
        }
        function expertReviewSummary(itemType) {
            // itemType ('finding' | 'optimization') — счётчик только своего типа:
            // вкладка «Оптимизация» не должна показывать числа решений по замечаниям.
            let vals = Object.values(expertDecisions.value);
            if (itemType) vals = vals.filter(d => (d.item_type || 'finding') === itemType);
            return {
                total: vals.filter(d => d.decision).length,
                accepted: vals.filter(d => d.decision === 'accepted').length,
                rejected: vals.filter(d => d.decision === 'rejected').length,
                // Перенесённые из прошлой версии без вердикта — «возможные повторы»
                // (те, что помечены «↩ Возможный повтор из … — проверьте»).
                possibleRepeats: vals.filter(d => d.carried_over && !d.decision).length,
            };
        }

        // ─── Knowledge Base (база знаний) ───
        async function loadKnowledgeBase() {
            kbLoading.value = true;
            try {
                const params = new URLSearchParams({ status: kbTab.value, limit: '200', offset: '0' });
                if (kbSearch.value) params.set('search', kbSearch.value);
                if (kbItemType.value) params.set('item_type', kbItemType.value);
                // Замечания фильтруются по глобально выбранному объекту (верхний селектор «Объект»).
                if (currentObjectId.value) params.set('object_id', currentObjectId.value);
                const resp = await fetch(`/api/knowledge-base/entries?${params}`);
                const data = await resp.json();
                kbEntries.value = data.entries || [];
            } catch (e) {
                console.error('Load KB error:', e);
            } finally {
                kbLoading.value = false;
            }
        }

        async function loadKBStats() {
            try {
                const q = currentObjectId.value ? `?object_id=${encodeURIComponent(currentObjectId.value)}` : '';
                const resp = await fetch(`/api/knowledge-base/stats${q}`);
                kbStats.value = await resp.json();
            } catch (e) { console.warn('KB stats error:', e); }
        }

        function onKbObjectChange() {
            loadKBStats();
            if (kbTab.value !== 'missing_norms') loadKnowledgeBase();
        }

        // Провалиться из БЗ в раздел Замечания/Оптимизация соответствующего проекта.
        async function openKBItem(entry) {
            if (!entry || !entry.source_project) return;
            if (entry.object_id && entry.object_id !== currentObjectId.value) {
                await switchObject(entry.object_id);
            }
            const tab = entry.item_type === 'optimization' ? 'optimization' : 'findings';
            navigate('/project/' + entry.source_project + '/' + tab);
        }

        function switchKBTab(tab) {
            kbTab.value = tab;
            if (tab === 'missing_norms') {
                loadMissingNorms();
            } else {
                loadKnowledgeBase();
            }
        }

        // Дропдаун выбора типа («Замечания» / «Оптимизации») в шапке колонки «Тип».
        function toggleKbTypeMenu() {
            kbTypeMenuOpen.value = !kbTypeMenuOpen.value;
        }

        function setKbItemType(t) {
            kbTypeMenuOpen.value = false;
            if (kbItemType.value === t) return;
            kbItemType.value = t;
            loadKnowledgeBase();
        }

        async function loadMissingNorms() {
            kbLoading.value = true;
            try {
                const params = new URLSearchParams();
                if (missingNormsFilter.value) params.set('status', missingNormsFilter.value);
                const resp = await fetch(`/api/knowledge-base/missing-norms?${params}`);
                const data = await resp.json();
                missingNorms.value = data.norms || [];
                missingNormsStats.value = data.stats || {};
            } catch (e) {
                console.error('Missing norms load error:', e);
            } finally {
                kbLoading.value = false;
            }
        }

        async function markNormAdded(docNumber) {
            try {
                await fetch(`/api/knowledge-base/missing-norms/${encodeURIComponent(docNumber)}/mark-added`, { method: 'POST' });
                loadMissingNorms();
            } catch (e) { console.error('Mark added error:', e); }
        }

        async function dismissNorm(docNumber) {
            try {
                await fetch(`/api/knowledge-base/missing-norms/${encodeURIComponent(docNumber)}/dismiss`, { method: 'POST' });
                loadMissingNorms();
            } catch (e) { console.error('Dismiss norm error:', e); }
        }

        async function restoreNorm(docNumber) {
            try {
                await fetch(`/api/knowledge-base/missing-norms/${encodeURIComponent(docNumber)}/restore`, { method: 'POST' });
                loadMissingNorms();
            } catch (e) { console.error('Restore norm error:', e); }
        }

        async function confirmCustomer(entryIds) {
            try {
                await fetch('/api/knowledge-base/customer-confirm', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ entry_ids: entryIds }),
                });
                loadKnowledgeBase();
                loadKBStats();
            } catch (e) { console.error('Customer confirm error:', e); }
        }

        async function unconfirmCustomer(entryIds) {
            try {
                await fetch('/api/knowledge-base/customer-unconfirm', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ entry_ids: entryIds }),
                });
                loadKnowledgeBase();
                loadKBStats();
            } catch (e) { console.error('Customer unconfirm error:', e); }
        }

        async function revokeKBDecision(entry) {
            try {
                await fetch('/api/knowledge-base/revoke', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ entry_id: entry.id, project_id: entry.source_project, item_id: entry.item_id }),
                });
                // Убрать из локального кеша решений
                if (expertDecisions.value[entry.item_id]) {
                    const updated = { ...expertDecisions.value };
                    delete updated[entry.item_id];
                    expertDecisions.value = updated;
                }
                loadKnowledgeBase();
                loadKBStats();
            } catch (e) { console.error('Revoke error:', e); }
        }

        async function _postExcelDecisions(file) {
            // Возвращает {ok: true, data} | {ok: false, message}
            const formData = new FormData();
            formData.append('file', file);
            let resp;
            try {
                const vid = activeVersionId.value;
                const pid = currentProjectId.value;
                const qs = new URLSearchParams();
                if (vid) qs.set('version_id', vid);
                if (pid) qs.set('project_id', pid);
                const q = qs.toString();
                const url = q ? `/api/knowledge-base/upload-excel?${q}` : '/api/knowledge-base/upload-excel';
                resp = await fetch(url, { method: 'POST', body: formData });
            } catch (netErr) {
                console.error('[upload-excel] network error:', netErr);
                return {
                    ok: false,
                    message: `Сеть/туннель: запрос не дошёл до сервера (${netErr.name || 'NetworkError'}). ` +
                        `Файл: ${file.name} (${(file.size / 1024).toFixed(0)} КБ). ` +
                        `Попробуйте ещё раз или пришлите файл напрямую.`,
                };
            }
            const rawText = await resp.text().catch(() => '');
            let data = null;
            try { data = rawText ? JSON.parse(rawText) : null; } catch (_) { /* not JSON */ }
            if (!resp.ok) {
                const detail = (data && (data.detail || data.message)) || rawText.slice(0, 300) || resp.statusText;
                console.error(`[upload-excel] HTTP ${resp.status}:`, rawText);
                return { ok: false, message: `Сервер ответил ${resp.status}: ${detail}` };
            }
            if (!data || data.status !== 'ok') {
                console.error('[upload-excel] unexpected response:', rawText);
                return { ok: false, message: 'Неожиданный ответ сервера (см. консоль).' };
            }
            return { ok: true, data };
        }

        async function uploadDecisionsExcel(event) {
            const file = event.target.files[0];
            if (!file) return;
            kbUploadLoading.value = true;
            try {
                const result = await _postExcelDecisions(file);
                if (!result.ok) {
                    alert(result.message);
                    return;
                }
                alert('Решения загружены: ' + Object.keys(result.data.projects).length + ' проектов');
                loadKnowledgeBase();
                loadKBStats();
            } finally {
                kbUploadLoading.value = false;
                event.target.value = '';
            }
        }

        async function uploadAndApplyDecisions(event) {
            const file = event.target.files[0];
            if (!file) return;
            kbUploadLoading.value = true;
            try {
                const result = await _postExcelDecisions(file);
                if (!result.ok) {
                    alert(result.message);
                    return;
                }
                const count = Object.keys(result.data.projects).length;
                // Загрузить решения для текущего проекта и включить режим оценки
                if (currentProjectId.value) {
                    try {
                        const vidUp = activeVersionId.value;
                        const vqUp = vidUp ? `?version_id=${encodeURIComponent(vidUp)}` : '';
                        const revResp = await fetch(`/api/knowledge-base/expert-review/${encodeURIComponent(expertReviewProjectId())}${vqUp}`);
                        const revData = await revResp.json();
                        if (revData.has_review && revData.data && revData.data.decisions) {
                            const map = {};
                            for (const d of revData.data.decisions) {
                                map[d.item_id] = { decision: d.decision, rejection_reason: d.rejection_reason || '', item_type: d.item_type || 'finding', carried_over: !!d.carried_over, carried_from_version: d.carried_from_version || '' };
                            }
                            expertDecisions.value = map;
                            expertReviewMode.value = true;
                        }
                    } catch (e) {
                        console.warn('[upload-excel] не удалось дозагрузить expert-review:', e);
                    }
                }
                alert(`Решения загружены (${count} проектов). Колонки заполнены автоматически.`);
            } finally {
                kbUploadLoading.value = false;
                event.target.value = '';
            }
        }

        // Watch severity filter
        // Client-side фильтрация — без перезапроса с сервера
        watch(filterSeverity, () => _applyFindingsFilter());
        watch(filterSearch, () => _applyFindingsFilter());
        watch(currentView, () => closeStageAlgorithm());
        // Inline Critic v2 toggles
        watch(cv2ShowHidden, () => { findingsPage.value = 1; _applyFindingsFilter(); });
        watch(cv2DisplayFilter, () => { findingsPage.value = 1; _applyFindingsFilter(); });

        // ─── Init ───
        function _stageAlgorithmKeydown(ev) {
            if (ev.key === 'Escape' && activeStageAlgorithmKey.value) {
                closeStageAlgorithm();
            }
        }
        onMounted(() => {
            window.addEventListener('hashchange', handleRoute);
            window.addEventListener('keydown', _stageAlgorithmKeydown);
            // Клик вне дропдауна «Тип» закрывает его (сам тогл/меню используют @click.stop).
            window.addEventListener('click', () => { if (kbTypeMenuOpen.value) kbTypeMenuOpen.value = false; });
            // Клик вне панелей шапки (объект/расходы/аккаунт) закрывает их.
            // Триггеры и сами панели используют @click.stop, поэтому не самозакрываются.
            window.addEventListener('click', closeHeaderPopovers);
            handleRoute();
            connectGlobalWS();
            startPolling();
            // Параллельная загрузка — сначала объект (нужен currentObjectId), потом группы
            Promise.all([
                loadDisciplines(),
                loadObjects().then(() => loadProjectGroups()),
                loadUsers(),
                pollGlobalUsage(),
                fetchAccountInfo(),
                fetchPaidCost(),
                fetchPaidApiStatus(),
                fetchPaidEvents(),
                fetchPaidBlockedEvents(),
                fetchPaidCostDaily(),
            ]);
            usagePollTimer = setInterval(() => {
                pollGlobalUsage();
                fetchPaidCost();
                fetchPaidApiStatus();
                fetchPaidEvents();
                fetchPaidBlockedEvents();
                fetchPaidCostDaily();
            }, 60000);
        });

        onUnmounted(() => {
            window.removeEventListener('hashchange', handleRoute);
            window.removeEventListener('keydown', _stageAlgorithmKeydown);
            window.removeEventListener('click', closeHeaderPopovers);
            stopPolling();
            if (usagePollTimer) { clearInterval(usagePollTimer); usagePollTimer = null; }
        });

        // ───────────────────────────────────────────────────────────────
        // External register (СУ-10) — реестр уже-отправленных заказчику
        // findings + сопоставление с нашими 03_findings.json
        // ───────────────────────────────────────────────────────────────

        // Бейдж для finding'а: отрисовываем поверх его карточки, если у finding'а
        // есть external_register payload.
        function findingExtRegBadge(f) {
            const er = f && f.external_register;
            if (!er) return null;
            const labelMap = {
                'Учтено': {text: 'Учтено', tone: 'green'},
                'Внесено': {text: 'Внесено', tone: 'green'},
                'Отклонено': {text: 'Отклонено', tone: 'red'},
                'По согласованию Заказчика': {text: 'По согл.', tone: 'amber'},
                'Не определено': {text: '—', tone: 'grey'},
            };
            const meta = labelMap[er.customer_response] || labelMap['Не определено'];
            return {
                text: 'Принято · ' + meta.text,
                tone: meta.tone,
                title: (er.customer_comment || '') + '\nКлюч: ' + (er.comment_key || ''),
            };
        }

        // ─── Documentation comparison: source shell + vector PDF viewer ───
        const scTab = ref('upload');
        const scObjects = ref([]);
        const scObjectsLoading = ref(false);
        const scObjectsError = ref('');
        const scStageUploadBusy = reactive({stage_1: false, stage_2: false});
        const scStageUploadError = ref('');
        const scStageFolderDialogOpen = ref(false);
        const scStageFolderDialogStage = ref('');
        const scStageFolderDialogName = ref('');
        const scStageFolderCandidates = ref([]);
        const scStageFolderInput = ref(null);
        const scStageBatchCurrent = ref(0);
        const scStageBatchTotal = ref(0);
        const scSessionLoading = ref(false);
        const scSession = ref(null);
        const scSessionError = ref('');
        const scSelectedPdf = reactive({left: '', right: ''});
        const scDocumentOrder = reactive({left: [], right: []});
        const scDraggingDocument = ref(null);
        const scDocumentDragOver = reactive({left: null, right: null});
        const scDraggingPairRow = ref(null);
        const scPairRowDragOver = ref(null);
        const scPendingPairSelection = ref(null);
        const scConfirmedDocumentPairs = reactive({});
        const scPairingSaving = ref(false);
        const scPairingMatching = ref(false);
        const scPairingDirty = ref(false);
        const scPairingSaveError = ref('');
        const scPairingSaveMessage = ref('');
        const scPairRowStates = reactive({});
        const scActivePair = ref(null);
        const scPairData = ref(null);
        const scPairLoading = ref(false);
        const scProcessing = ref(false);
        const scProcessingError = ref('');
        const scMatchState = ref(null);
        const scLinkSaving = ref(false);
        const scLinkEditorOpen = ref(false);
        const scLinkEditorMode = ref('replace');
        const scLinkEditorRightPage = ref('');
        const scLinkEditorLeftPages = ref([]);
        const scLinkEditorRightPages = ref([]);
        const scLinkEditorSourceIndex = ref(null);
        const scSheetMapCollapsed = ref(false);
        try {
            scSheetMapCollapsed.value = localStorage.getItem('stage-comparison:sheet-map-collapsed') === '1';
        } catch (_) {}
        const scCurrentPage = reactive({left: 1, right: 1});
        const scViewerEmpty = reactive({left: false, right: false});
        // ─── Просмотрщик листов: общий видовой порт двух панелей ───────────
        // Панели показывают РАЗНЫЕ листы разного формата (A4 стадии П против
        // A1 стадии РД), поэтому синхронизировать пиксели прокрутки нельзя.
        // Состояние вида хранится в нормированных координатах листа
        // (cx/cy ∈ [0,1] — точка листа в центре панели, zoom=1 — лист целиком),
        // и каждая панель переводит их в свои единицы. Одна и та же область
        // чертежа оказывается в одном и том же месте обеих панелей.
        //
        // Само состояние НЕ реактивно: pan/zoom пишет transform прямо в DOM
        // внутри requestAnimationFrame. Vue обновляет небольшой набор тайлов
        // только после паузы жеста, а не на каждое движение колеса.
        const SC_ZOOM_MIN = 0.1;
        const SC_ZOOM_MAX = 100;
        const SC_PREVIEW_WIDTH = 1400;
        const SC_CONTINUOUS_PREVIEW_WIDTH = 1000;
        const SC_TILE_SIZE = 512;
        const SC_TILE_MAX_LEVEL = 6;
        const scSyncView = ref(true);
        const scViewMode = ref('paged');
        try {
            const savedViewMode = localStorage.getItem('stage-comparison:view-mode');
            if (savedViewMode === 'continuous') scViewMode.value = savedViewMode;
        } catch (_) {}
        const scZoomPercent = ref(100);
        const scContinuousZoom = ref(1);
        const scPaneRefs = reactive({left: null, right: null});
        const scStageRefs = reactive({left: null, right: null});
        const scContinuousPaneRefs = reactive({left: null, right: null});
        const scPagePreview = reactive({left: '', right: ''});
        const scPageTiles = reactive({left: [], right: []});
        const scPageSignatures = reactive({left: '', right: ''});
        const scPageLoading = reactive({left: false, right: false});
        const scPageError = reactive({left: '', right: ''});
        const scContinuousPreview = reactive({left: {}, right: {}});
        const scContinuousLoading = reactive({left: {}, right: {}});
        const scContinuousError = reactive({left: {}, right: {}});
        const scContinuousDims = reactive({left: {}, right: {}});
        const scContinuousSignatures = reactive({left: {}, right: {}});
        const scContinuousTiles = reactive({left: {}, right: {}});
        const scContinuousCurrentSlot = reactive({left: '', right: ''});
        const scViews = {left: {zoom: 1, cx: 0.5, cy: 0.5}, right: {zoom: 1, cx: 0.5, cy: 0.5}};
        const scPageDims = {left: {w: 0, h: 0}, right: {w: 0, h: 0}};
        const scPaneSize = {left: {w: 0, h: 0}, right: {w: 0, h: 0}};
        const scPageInfoRequest = {left: null, right: null};
        const scContinuousRequests = {left: new Map(), right: new Map()};
        const scTileRefreshTimer = {left: 0, right: 0};
        const scContinuousTileRefreshTimer = {left: 0, right: 0};
        const scContinuousProgrammaticTarget = {left: null, right: null};
        const scContinuousProgrammaticTimer = {left: 0, right: 0};
        const scContinuousPageFromScroll = {left: null, right: null};
        const scContinuousScrollFrame = {left: 0, right: 0};
        let scViewFrame = 0;
        let scPanState = null;
        let scContinuousPanState = null;
        let scPanBoostTimer = 0;

        const scSelectedObject = computed(() =>
            scObjects.value.find(item => item.id === currentObjectId.value) || null
        );
        const scDocumentsLeft = computed(() =>
            (scSession.value && scSession.value.documents && scSession.value.documents.stage_1) || []
        );
        const scDocumentsRight = computed(() =>
            (scSession.value && scSession.value.documents && scSession.value.documents.stage_2) || []
        );
        const scStageFolderSelectedCount = computed(() =>
            scStageFolderCandidates.value.filter(candidate =>
                candidate.checked && candidate.status !== 'done'
            ).length
        );
        const scStageFolderSelectableCount = computed(() =>
            scStageFolderCandidates.value.filter(candidate => candidate.status !== 'done').length
        );
        const scStageFolderDoneCount = computed(() =>
            scStageFolderCandidates.value.filter(candidate => candidate.status === 'done').length
        );
        const scStageFolderErrorCount = computed(() =>
            scStageFolderCandidates.value.filter(candidate => candidate.status === 'error').length
        );
        const scStageUploadIsBusy = computed(() =>
            scStageUploadBusy.stage_1 || scStageUploadBusy.stage_2
        );
        const scPairs = computed(() => (scSession.value && scSession.value.pairs) || []);
        const scPairingSaved = computed(() => Boolean(
            scSession.value && scSession.value.document_pairing
            && !scPairingDirty.value && !scPairingSaving.value
        ));
        const scPairRows = computed(() => {
            const documents = {
                left: new Map(scDocumentsLeft.value.map(item => [item.pdf_path, item])),
                right: new Map(scDocumentsRight.value.map(item => [item.pdf_path, item])),
            };
            const length = Math.max(scDocumentOrder.left.length, scDocumentOrder.right.length);
            return Array.from({length}, (_, index) => {
                const left = documents.left.get(scDocumentOrder.left[index]) || null;
                const right = documents.right.get(scDocumentOrder.right[index]) || null;
                const pair = left && right
                    ? scPairs.value.find(item =>
                        (item.left || {}).pdf_path === left.pdf_path
                        && (item.right || {}).pdf_path === right.pdf_path
                    ) || null
                    : null;
                return {index, left, right, pair};
            });
        });
        const scSuggestions = computed(() =>
            (scMatchState.value && scMatchState.value.suggestions
                && scMatchState.value.suggestions.suggestions) || []
        );
        const scLeftSuggestion = computed(() =>
            scSuggestions.value.find(item => Number(item.left_page) === Number(scCurrentPage.left)) || null
        );
        const scSheetLinks = computed(() =>
            (scMatchState.value && scMatchState.value.links && scMatchState.value.links.links) || []
        );
        const scUnlinkedLeftPages = computed(() =>
            (scMatchState.value && scMatchState.value.links
                && scMatchState.value.links.unlinked_left_pages) || []
        );
        const scSheetMapRows = computed(() => {
            const payload = scMatchState.value && scMatchState.value.suggestions;
            if (!payload) return [];

            const rows = [];
            const representedLeft = new Set();
            const representedRight = new Set();
            const explicitlyUnlinked = new Set(scUnlinkedLeftPages.value.map(Number));
            let sequence = 0;
            const addRow = (row) => {
                const leftPages = [...new Set((row.leftPages || []).map(Number).filter(Boolean))]
                    .sort((a, b) => a - b);
                const rightPages = [...new Set((row.rightPages || []).map(Number).filter(Boolean))]
                    .sort((a, b) => a - b);
                leftPages.forEach(page => representedLeft.add(page));
                rightPages.forEach(page => representedRight.add(page));
                rows.push({
                    ...row,
                    leftPages,
                    rightPages,
                    sequence: sequence++,
                    sortLeft: leftPages.length ? leftPages[0] : Number.POSITIVE_INFINITY,
                    sortRight: rightPages.length ? rightPages[0] : Number.POSITIVE_INFINITY,
                });
            };

            scSheetLinks.value.forEach((link, explicitLinkIndex) => {
                addRow({
                    key: `explicit-${explicitLinkIndex}`,
                    leftPages: link.left_pages || [],
                    rightPages: link.right_pages || [],
                    source: link.source || 'manual',
                    confidence: link.confidence || (link.source === 'manual' ? 'manual' : 'medium'),
                    reason: link.reason || [],
                    explicitLinkIndex,
                });
            });

            for (const suggestion of scSuggestions.value) {
                const leftPage = Number(suggestion.left_page);
                if (!leftPage || representedLeft.has(leftPage)) continue;
                const rightPage = explicitlyUnlinked.has(leftPage)
                    ? null
                    : Number(suggestion.primary_right_page) || null;
                addRow({
                    key: `suggestion-${leftPage}`,
                    leftPages: [leftPage],
                    rightPages: rightPage ? [rightPage] : [],
                    source: 'auto',
                    confidence: rightPage ? (suggestion.confidence || 'medium') : 'unmatched',
                    reason: suggestion.reason || [],
                    explicitLinkIndex: null,
                });
            }

            for (const sheet of payload.left_sheet_index || []) {
                const leftPage = Number(sheet.pdf_page);
                if (!leftPage || representedLeft.has(leftPage)) continue;
                addRow({
                    key: `left-only-${leftPage}`,
                    leftPages: [leftPage],
                    rightPages: [],
                    source: 'unmatched',
                    confidence: 'unmatched',
                    reason: [],
                    explicitLinkIndex: null,
                });
            }

            for (const sheet of payload.right_sheet_index || []) {
                const rightPage = Number(sheet.pdf_page);
                if (!rightPage || representedRight.has(rightPage)) continue;
                addRow({
                    key: `right-only-${rightPage}`,
                    leftPages: [],
                    rightPages: [rightPage],
                    source: 'unmatched',
                    confidence: 'unmatched',
                    reason: [],
                    explicitLinkIndex: null,
                });
            }

            return rows.sort((left, right) => (
                left.sortLeft - right.sortLeft
                || left.sortRight - right.sortRight
                || left.sequence - right.sequence
            ));
        });
        // Непрерывный просмотр строится по ОБЩИМ строкам карты, а не по двум
        // независимым спискам PDF-страниц. Если лист существует только в одной
        // стадии, в другой остаётся слот-заглушка — последующие пары больше не
        // съезжают вверх и сохраняют одинаковую вертикальную последовательность.
        const scContinuousSlots = computed(() => {
            const rows = scSheetMapRows.value;
            if (!rows.length) {
                const length = Math.max(scPageCount('left'), scPageCount('right'));
                return Array.from({length}, (_, index) => ({
                    key: `page-slot-${index + 1}`,
                    leftPage: index < scPageCount('left') ? index + 1 : null,
                    rightPage: index < scPageCount('right') ? index + 1 : null,
                }));
            }
            return rows.flatMap(row => {
                const leftPages = (row.leftPages || []).map(Number).filter(Boolean);
                const rightPages = (row.rightPages || []).map(Number).filter(Boolean);
                const length = Math.max(1, leftPages.length, rightPages.length);
                return Array.from({length}, (_, index) => ({
                    key: `${row.key}:${index}`,
                    rowKey: row.key,
                    leftPage: leftPages[index] || null,
                    rightPage: rightPages[index] || null,
                }));
            });
        });
        const scCurrentExplicitLinks = computed(() => scSheetLinks.value.filter(link =>
            (link.left_pages || []).map(Number).includes(Number(scCurrentPage.left))
        ));
        const scCurrentRightPages = computed(() => {
            if (scUnlinkedLeftPages.value.map(Number).includes(Number(scCurrentPage.left))) return [];
            const explicit = [...new Set(scCurrentExplicitLinks.value.flatMap(link => link.right_pages || []).map(Number))];
            if (explicit.length) return explicit.sort((a, b) => a - b);
            const primary = scLeftSuggestion.value && scLeftSuggestion.value.primary_right_page;
            return primary ? [Number(primary)] : [];
        });
        const scMatchSummary = computed(() => (scMatchState.value && scMatchState.value.summary) || {
            auto_high: 0, needs_review: 0, manual_links: 0, unmatched_left: 0, unmatched_right: 0,
            unmatched_left_pages: [], unmatched_right_pages: [],
        });
        const scCurrentStatus = computed(() => {
            if (scUnlinkedLeftPages.value.map(Number).includes(Number(scCurrentPage.left))) {
                return {tone: 'unmatched', label: 'Не сопоставлено'};
            }
            if (scCurrentExplicitLinks.value.some(link => link.source === 'manual')) {
                return {tone: 'manual', label: 'Вручную'};
            }
            const suggestion = scLeftSuggestion.value;
            if (suggestion && suggestion.primary_right_page && suggestion.confidence === 'high') {
                return {tone: 'high', label: 'Автоматически — высокая уверенность'};
            }
            if (suggestion && suggestion.primary_right_page) {
                return {tone: 'review', label: 'Предложение — проверить'};
            }
            return {tone: 'unmatched', label: 'Не сопоставлено'};
        });
        const scRightOptions = computed(() => {
            const suggestionsPayload = scMatchState.value && scMatchState.value.suggestions;
            const all = (suggestionsPayload && suggestionsPayload.right_sheet_index) || [];
            const suggestion = scLeftSuggestion.value;
            const orderedPages = [];
            if (suggestion && suggestion.primary_right_page) orderedPages.push(Number(suggestion.primary_right_page));
            for (const item of (suggestion && suggestion.alternatives) || []) orderedPages.push(Number(item.right_page));
            for (const sheet of all) orderedPages.push(Number(sheet.pdf_page));
            const byPage = new Map(all.map(sheet => [Number(sheet.pdf_page), sheet]));
            return [...new Set(orderedPages)].map(page => byPage.get(page)).filter(Boolean);
        });

        function scStageInfo(stageName) {
            const object = scSelectedObject.value;
            return object && (object.stages || []).find(stage => stage.name === stageName) || null;
        }

        function scPairOrderStorageKey() {
            return scSession.value ? `stage-comparison:pair-order:${scSession.value.id}` : '';
        }

        function scPairPathsKey(leftPdf, rightPdf) {
            return `${leftPdf}::${rightPdf}`;
        }

        function scClearConfirmedDocumentPairs() {
            Object.keys(scConfirmedDocumentPairs).forEach(key => {
                delete scConfirmedDocumentPairs[key];
            });
        }

        function scRestoreConfirmedDocumentPairs(savedPairs) {
            scClearConfirmedDocumentPairs();
            const leftIndexes = new Map();
            const rightIndexes = new Map();
            scDocumentOrder.left.forEach((path, index) => {
                if (path) leftIndexes.set(path, index);
            });
            scDocumentOrder.right.forEach((path, index) => {
                if (path) rightIndexes.set(path, index);
            });
            if (!Array.isArray(savedPairs)) return;
            savedPairs.forEach(pair => {
                const leftPdf = pair && (pair.leftPdf || pair.left_pdf);
                const rightPdf = pair && (pair.rightPdf || pair.right_pdf);
                if (!leftPdf || !rightPdf || leftIndexes.get(leftPdf) !== rightIndexes.get(rightPdf)) return;
                scConfirmedDocumentPairs[scPairPathsKey(leftPdf, rightPdf)] = {leftPdf, rightPdf};
            });
        }

        function scReconcileDocumentOrder(saved, documents) {
            const available = new Set(documents.map(item => item.pdf_path));
            const used = new Set();
            const order = [];
            if (Array.isArray(saved)) {
                saved.forEach(path => {
                    if (path && available.has(path) && !used.has(path)) {
                        order.push(path);
                        used.add(path);
                    } else {
                        order.push(null);
                    }
                });
            }
            for (const document of documents) {
                if (used.has(document.pdf_path)) continue;
                const emptyIndex = order.indexOf(null);
                if (emptyIndex >= 0) order[emptyIndex] = document.pdf_path;
                else order.push(document.pdf_path);
                used.add(document.pdf_path);
            }
            return order;
        }

        // Строка, пустая с ОБЕИХ сторон, не выражает ничего: перенос документа
        // сделан обменом местами, поэтому «пустая пара» не нужна даже как цель
        // перетаскивания. Дырку на ОДНОЙ стороне трогать нельзя — это документ
        // без пары, и он обязан стоять напротив пустого места.
        //
        // Убираем именно из данных, а не из вывода: обработчики перетаскивания
        // адресуются как scPairRows.value[row.index], и фильтр отображения
        // разошёлся бы с этой адресацией.
        function scPackDocumentRows(left, right) {
            const length = Math.max(left.length, right.length);
            const packed = {left: [], right: []};
            for (let index = 0; index < length; index += 1) {
                const leftPdf = left[index] || null;
                const rightPdf = right[index] || null;
                if (!leftPdf && !rightPdf) continue;
                packed.left.push(leftPdf);
                packed.right.push(rightPdf);
            }
            return packed;
        }

        function scCompactDocumentOrder() {
            const packed = scPackDocumentRows(scDocumentOrder.left, scDocumentOrder.right);
            if (packed.left.length === scDocumentOrder.left.length
                    && packed.right.length === scDocumentOrder.right.length) return false;
            scDocumentOrder.left = packed.left;
            scDocumentOrder.right = packed.right;
            return true;
        }

        function scInitializeDocumentOrder(useSaved = true) {
            let saved = {};
            let loadedFromServer = false;
            const serverPairing = scSession.value && scSession.value.document_pairing;
            if (useSaved && serverPairing && serverPairing.version === 1) {
                saved = {
                    left: serverPairing.left_order,
                    right: serverPairing.right_order,
                    confirmedPairs: serverPairing.confirmed_pairs,
                };
                loadedFromServer = true;
            }
            const storageKey = scPairOrderStorageKey();
            if (useSaved && !loadedFromServer && storageKey) {
                try { saved = JSON.parse(localStorage.getItem(storageKey) || '{}'); } catch (_) { saved = {}; }
            }
            const packed = scPackDocumentRows(
                scReconcileDocumentOrder(saved.left, scDocumentsLeft.value),
                scReconcileDocumentOrder(saved.right, scDocumentsRight.value),
            );
            scDocumentOrder.left = packed.left;
            scDocumentOrder.right = packed.right;
            scPendingPairSelection.value = null;
            scRestoreConfirmedDocumentPairs(useSaved ? saved.confirmedPairs : []);
            scPairingDirty.value = false;
            scPairingSaveError.value = '';
            scPairingSaveMessage.value = loadedFromServer ? 'Загружено сохранённое сопоставление' : '';
        }

        function scPersistDocumentOrder(markDirty = true) {
            const storageKey = scPairOrderStorageKey();
            if (!storageKey) return;
            try {
                localStorage.setItem(storageKey, JSON.stringify({
                    left: scDocumentOrder.left,
                    right: scDocumentOrder.right,
                    confirmedPairs: Object.values(scConfirmedDocumentPairs),
                }));
            } catch (_) {}
            if (markDirty) {
                scPairingDirty.value = true;
                scPairingSaveError.value = '';
                scPairingSaveMessage.value = '';
            }
        }

        function scDocumentPairingPayload() {
            return {
                left_order: [...scDocumentOrder.left],
                right_order: [...scDocumentOrder.right],
                confirmed_pairs: Object.values(scConfirmedDocumentPairs).map(pair => ({
                    left_pdf: pair.leftPdf,
                    right_pdf: pair.rightPdf,
                })),
            };
        }

        async function scSaveDocumentPairing() {
            if (!scSession.value || scPairingSaving.value || scPairingMatching.value) return;
            const payload = scDocumentPairingPayload();
            const snapshot = JSON.stringify(payload);
            scPairingSaving.value = true;
            scPairingSaveError.value = '';
            scPairingSaveMessage.value = '';
            try {
                const response = await fetch(
                    `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/document-pairing`,
                    {
                        method: 'PUT',
                        headers: {'Content-Type': 'application/json'},
                        body: snapshot,
                    },
                );
                const data = await response.json().catch(() => ({}));
                if (!response.ok) throw new Error(data.detail || ('HTTP ' + response.status));
                scSession.value = {...scSession.value, document_pairing: data};
                scPairingDirty.value = JSON.stringify(scDocumentPairingPayload()) !== snapshot;
                scPairingSaveMessage.value = scPairingDirty.value
                    ? 'Сохранено; после отправки появились новые изменения'
                    : 'Сопоставление проектов сохранено';
                scPersistDocumentOrder(false);
            } catch (error) {
                scPairingSaveError.value = String(error.message || error);
            } finally {
                scPairingSaving.value = false;
            }
        }

        async function scAutoMatchDocumentProjects() {
            if (!scSession.value || scPairingMatching.value || scPairingSaving.value) return;
            scPairingMatching.value = true;
            scPairingSaveError.value = '';
            scPairingSaveMessage.value = '';
            try {
                const response = await fetch(
                    `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/document-pairing/suggest`,
                    {method: 'POST'},
                );
                const data = await response.json().catch(() => ({}));
                if (!response.ok) throw new Error(data.detail || ('HTTP ' + response.status));
                scDocumentOrder.left = [...(data.left_order || [])];
                scDocumentOrder.right = [...(data.right_order || [])];
                scCompactDocumentOrder();
                scPendingPairSelection.value = null;
                scRestoreConfirmedDocumentPairs(data.confirmed_pairs || []);
                scFinishDocumentDrag();
                scFinishPairRowDrag();
                scPersistDocumentOrder();
                scPairingSaveMessage.value = `Сопоставлено автоматически: ${Number(data.matched_count) || 0}. `
                    + `Остались внизу: П — ${Number(data.unmatched_left_count) || 0}, `
                    + `РД — ${Number(data.unmatched_right_count) || 0}`;
            } catch (error) {
                scPairingSaveError.value = String(error.message || error);
            } finally {
                scPairingMatching.value = false;
            }
        }

        function scStartDocumentDrag(event, side, index) {
            const row = scPairRows.value[index];
            if (!['left', 'right'].includes(side) || !scDocumentOrder[side][index]
                    || (row && scPairRowBusy(row))) return;
            scFinishPairRowDrag();
            scDraggingDocument.value = {side, index};
            scPendingPairSelection.value = null;
            if (event.dataTransfer) {
                event.dataTransfer.effectAllowed = 'move';
                event.dataTransfer.setData('text/plain', `${side}:${index}`);
            }
        }

        function scDragDocumentOver(event, side, index) {
            const dragging = scDraggingDocument.value;
            if (!dragging || dragging.side !== side) return;
            const sourceRow = scPairRows.value[dragging.index];
            const targetRow = scPairRows.value[index];
            if ((sourceRow && scPairRowBusy(sourceRow)) || (targetRow && scPairRowBusy(targetRow))) return;
            if (event.dataTransfer) event.dataTransfer.dropEffect = 'move';
            scDocumentDragOver[side] = index;
        }

        function scDropDocument(side, index) {
            const dragging = scDraggingDocument.value;
            if (!dragging || dragging.side !== side) return;
            const sourceRow = scPairRows.value[dragging.index];
            const targetRow = scPairRows.value[index];
            if ((sourceRow && scPairRowBusy(sourceRow)) || (targetRow && scPairRowBusy(targetRow))) return;
            const values = [...scDocumentOrder[side]];
            while (values.length <= index) values.push(null);
            scRemoveConfirmedPairsForPaths([values[dragging.index], values[index]]);
            [values[dragging.index], values[index]] = [values[index], values[dragging.index]];
            scDocumentOrder[side] = values;
            scCompactDocumentOrder();   // обмен мог оставить строку пустой с обеих сторон
            scPersistDocumentOrder();
            scFinishDocumentDrag();
        }

        function scFinishDocumentDrag() {
            scDraggingDocument.value = null;
            scDocumentDragOver.left = null;
            scDocumentDragOver.right = null;
        }

        function scIsDraggingDocument(side, index) {
            const dragging = scDraggingDocument.value;
            return Boolean(dragging && dragging.side === side && dragging.index === index);
        }

        function scStartPairRowDrag(event, row) {
            if (!row || scPairRowBusy(row)) return;
            scFinishDocumentDrag();
            scPendingPairSelection.value = null;
            scDraggingPairRow.value = row.index;
            if (event.dataTransfer) {
                event.dataTransfer.effectAllowed = 'move';
                event.dataTransfer.setData('text/plain', `pair:${row.index}`);
            }
        }

        function scDragPairRowOver(event, index) {
            const sourceIndex = scDraggingPairRow.value;
            if (sourceIndex === null || sourceIndex === undefined) return;
            const targetRow = scPairRows.value[index];
            if (targetRow && scPairRowBusy(targetRow)) return;
            if (event.dataTransfer) event.dataTransfer.dropEffect = 'move';
            scPairRowDragOver.value = index;
        }

        function scDropPairRow(index) {
            const sourceIndex = scDraggingPairRow.value;
            if (sourceIndex === null || sourceIndex === undefined) return;
            const sourceRow = scPairRows.value[sourceIndex];
            const targetRow = scPairRows.value[index];
            if ((sourceRow && scPairRowBusy(sourceRow)) || (targetRow && scPairRowBusy(targetRow))) {
                scFinishPairRowDrag();
                return;
            }
            if (sourceIndex !== index) {
                for (const side of ['left', 'right']) {
                    const values = [...scDocumentOrder[side]];
                    const [document] = values.splice(sourceIndex, 1);
                    values.splice(index, 0, document);
                    scDocumentOrder[side] = values;
                }
                scPersistDocumentOrder();
            }
            scFinishPairRowDrag();
        }

        function scFinishPairRowDrag() {
            scDraggingPairRow.value = null;
            scPairRowDragOver.value = null;
        }

        function scIsDraggingPairRow(index) {
            return scDraggingPairRow.value === index;
        }

        function scPairRowKey(row) {
            if (!row || !row.left || !row.right) return `incomplete:${row ? row.index : 'none'}`;
            return scPairPathsKey(row.left.pdf_path, row.right.pdf_path);
        }

        function scRemoveConfirmedPairsForPaths(paths) {
            const affected = new Set((paths || []).filter(Boolean));
            Object.entries(scConfirmedDocumentPairs).forEach(([key, pair]) => {
                if (affected.has(pair.leftPdf) || affected.has(pair.rightPdf)) {
                    delete scConfirmedDocumentPairs[key];
                }
            });
        }

        function scIsPairRowConfirmed(row) {
            return Boolean(row && row.left && row.right && scConfirmedDocumentPairs[scPairRowKey(row)]);
        }

        function scIsPairDocumentPending(side, row) {
            const pending = scPendingPairSelection.value;
            const document = row && row[side];
            return Boolean(pending && document && pending.side === side
                && pending.pdfPath === document.pdf_path);
        }

        function scSelectPairDocument(side, row) {
            if (!['left', 'right'].includes(side) || !row || !row[side] || scPairRowBusy(row)) return;
            const document = row[side];
            const clicked = {side, index: row.index, pdfPath: document.pdf_path};
            const pending = scPendingPairSelection.value;
            const rowWasConfirmed = scIsPairRowConfirmed(row);

            if (pending && pending.side === side && pending.pdfPath === clicked.pdfPath) {
                scPendingPairSelection.value = null;
                return;
            }
            if (!pending || pending.side === side) {
                if (rowWasConfirmed) {
                    scRemoveConfirmedPairsForPaths([
                        row.left && row.left.pdf_path,
                        row.right && row.right.pdf_path,
                    ]);
                    scPersistDocumentOrder();
                }
                scPendingPairSelection.value = clicked;
                return;
            }

            const anchorRow = scPairRows.value[pending.index];
            if (!anchorRow || scPairRowBusy(anchorRow)
                    || scDocumentOrder[pending.side][pending.index] !== pending.pdfPath) {
                scPendingPairSelection.value = clicked;
                return;
            }

            const affectedPaths = [
                anchorRow.left && anchorRow.left.pdf_path,
                anchorRow.right && anchorRow.right.pdf_path,
                row.left && row.left.pdf_path,
                row.right && row.right.pdf_path,
            ];
            scRemoveConfirmedPairsForPaths(affectedPaths);

            if (row.index !== pending.index) {
                const values = [...scDocumentOrder[side]];
                [values[pending.index], values[row.index]] = [values[row.index], values[pending.index]];
                scDocumentOrder[side] = values;
            }

            const leftPdf = pending.side === 'left' ? pending.pdfPath : clicked.pdfPath;
            const rightPdf = pending.side === 'right' ? pending.pdfPath : clicked.pdfPath;
            scConfirmedDocumentPairs[scPairPathsKey(leftPdf, rightPdf)] = {leftPdf, rightPdf};
            scPendingPairSelection.value = null;
            scPersistDocumentOrder();
        }

        function scMutablePairRowState(row) {
            const key = scPairRowKey(row);
            if (!scPairRowStates[key]) scPairRowStates[key] = {status: 'idle', error: ''};
            return scPairRowStates[key];
        }

        function scPairRowStatus(row) {
            if (!row.left || !row.right) return {tone: 'incomplete', label: 'Нужен документ с обеих сторон'};
            const state = scPairRowStates[scPairRowKey(row)];
            if (state && state.status === 'opening') return {tone: 'running', label: 'Открытие…'};
            if (state && state.status === 'processing') return {tone: 'running', label: 'Идёт сравнение…'};
            if (state && state.status === 'error') return {tone: 'error', label: 'Ошибка'};
            if (state && state.status === 'done') return {tone: 'done', label: 'Сравнение готово'};
            if (state && state.status === 'opened') return {tone: 'opened', label: 'Пара открыта'};
            if (scIsPairRowConfirmed(row)) return {tone: 'confirmed', label: 'Пара сопоставлена'};
            if (row.pair) return {tone: 'saved', label: 'Пара сохранена'};
            return {tone: 'idle', label: 'Не запускалось'};
        }

        function scPairRowBusy(row) {
            const state = scPairRowStates[scPairRowKey(row)];
            return Boolean(state && ['opening', 'processing'].includes(state.status));
        }

        function scPairRowError(row) {
            const state = scPairRowStates[scPairRowKey(row)];
            return state && state.status === 'error' ? state.error : '';
        }

        function scApplyDocumentDefaults() {
            const leftPaths = new Set(scDocumentsLeft.value.map(item => item.pdf_path));
            const rightPaths = new Set(scDocumentsRight.value.map(item => item.pdf_path));
            if (!leftPaths.has(scSelectedPdf.left)) {
                scSelectedPdf.left = scDocumentOrder.left.find(Boolean)
                    || (scDocumentsLeft.value[0] ? scDocumentsLeft.value[0].pdf_path : '');
            }
            if (!rightPaths.has(scSelectedPdf.right)) {
                scSelectedPdf.right = scDocumentOrder.right.find(Boolean)
                    || (scDocumentsRight.value[0] ? scDocumentsRight.value[0].pdf_path : '');
            }
        }

        async function scRefreshSession() {
            const object = scSelectedObject.value;
            const left = object && (object.stages || []).find(stage => stage.name === 'stage_1');
            const right = object && (object.stages || []).find(stage => stage.name === 'stage_2');
            if (!left || !right || !left.pdf_count || !right.pdf_count) {
                scSession.value = null;
                scDocumentOrder.left = [];
                scDocumentOrder.right = [];
                scPendingPairSelection.value = null;
                scClearConfirmedDocumentPairs();
                scFinishDocumentDrag();
                scFinishPairRowDrag();
                scActivePair.value = null;
                scPairData.value = null;
                scMatchState.value = null;
                scPairingDirty.value = false;
                scPairingMatching.value = false;
                scPairingSaveError.value = '';
                scPairingSaveMessage.value = '';
                return;
            }
            scSessionLoading.value = true;
            scSessionError.value = '';
            try {
                const response = await fetch('/api/stage-comparison/sessions', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({stage_a_path: left.path, stage_b_path: right.path}),
                });
                const data = await response.json().catch(() => ({}));
                if (!response.ok) throw new Error(data.detail || ('HTTP ' + response.status));
                const previousSessionId = scSession.value && scSession.value.id;
                scSession.value = data;
                if (previousSessionId !== data.id) {
                    Object.keys(scPairRowStates).forEach(key => { delete scPairRowStates[key]; });
                }
                scInitializeDocumentOrder(true);
                scApplyDocumentDefaults();
                const activeId = scActivePair.value && scActivePair.value.id;
                if (activeId && !scPairs.value.some(pair => pair.id === activeId)) {
                    scActivePair.value = null;
                    scPairData.value = null;
                    scMatchState.value = null;
                }
            } catch (error) {
                scSessionError.value = String(error.message || error);
            } finally {
                scSessionLoading.value = false;
            }
        }

        async function scLoadObjects() {
            scObjectsLoading.value = true;
            scObjectsError.value = '';
            try {
                const response = await fetch('/api/stage-comparison/objects');
                const data = await response.json().catch(() => ({}));
                if (!response.ok) throw new Error(data.detail || ('HTTP ' + response.status));
                scObjects.value = data.items || [];
                await scRefreshSession();
            } catch (error) {
                scObjectsError.value = String(error.message || error);
            } finally {
                scObjectsLoading.value = false;
            }
        }

        function scBuildStageFolderCandidates(files, rootName) {
            const prepared = files.map((file, index) => {
                const full = file.webkitRelativePath || file.name;
                const parts = String(full).split('/').filter(Boolean);
                const relative = parts[0] === rootName ? parts.slice(1) : parts;
                return {
                    file, index, full, relative,
                    name: relative[relative.length - 1] || file.name,
                };
            });
            const candidates = [];
            const used = new Set();
            const makeCandidate = (payload) => Object.assign({
                checked: true,
                status: 'ready',
                progress: 0,
                message: '',
                result: null,
                totalBytes: payload.files.reduce(
                    (sum, item) => sum + Number(item.file.size || 0), 0,
                ),
            }, payload);

            // Каждый ZIP портала — самостоятельный проект и отдельный HTTP-запрос.
            for (const item of prepared) {
                if (!item.name.toLowerCase().endsWith('.zip')) continue;
                used.add(item.index);
                candidates.push(makeCandidate({
                    id: 'zip:' + item.index,
                    name: item.name.replace(/\.zip$/i, ''),
                    source: item.name,
                    kind: 'ZIP-комплект',
                    pdfCount: null,
                    files: [item],
                }));
            }

            // Подпапка с PDF считается одним проектом вместе с соседними файлами.
            const nested = new Map();
            for (const item of prepared) {
                if (used.has(item.index) || item.relative.length < 2) continue;
                const key = item.relative[0];
                if (!nested.has(key)) nested.set(key, []);
                nested.get(key).push(item);
            }
            for (const [name, items] of nested.entries()) {
                const pdfCount = items.filter(item => item.name.toLowerCase().endsWith('.pdf')).length;
                if (!pdfCount) continue;
                items.forEach(item => used.add(item.index));
                candidates.push(makeCandidate({
                    id: 'folder:' + name,
                    name,
                    source: name,
                    kind: 'папка проекта',
                    pdfCount,
                    files: items,
                }));
            }

            // PDF в корне — самостоятельный проект; одноимённые артефакты идут с ним.
            const rootFiles = prepared.filter(item =>
                !used.has(item.index) && item.relative.length === 1
            );
            const rootPdfs = rootFiles.filter(item => item.name.toLowerCase().endsWith('.pdf'));
            for (const pdf of rootPdfs) {
                const stem = pdf.name.replace(/\.pdf$/i, '').toLowerCase();
                const related = rootFiles.filter(item => {
                    if (used.has(item.index)) return false;
                    const low = item.name.toLowerCase();
                    return item === pdf || low.startsWith(stem)
                        || (rootPdfs.length === 1 && /^(document\.|result\.json|ocr\.)/.test(low));
                });
                related.forEach(item => used.add(item.index));
                candidates.push(makeCandidate({
                    id: 'pdf:' + pdf.index,
                    name: pdf.name.replace(/\.pdf$/i, ''),
                    source: pdf.name,
                    kind: 'PDF-проект',
                    pdfCount: 1,
                    files: related,
                }));
            }
            return candidates.sort((a, b) => a.name.localeCompare(b.name, 'ru'));
        }

        function scStageCandidateStatusText(candidate) {
            const labels = {
                queued: 'в очереди',
                processing: 'обработка',
                done: 'загружен',
                error: 'ошибка',
            };
            if (candidate.status === 'uploading') return `загрузка ${candidate.progress || 0}%`;
            if (candidate.status === 'ready') return candidate.checked ? 'выбран' : 'не выбран';
            return labels[candidate.status] || candidate.status;
        }

        function scToggleAllStageCandidates(checked) {
            scStageFolderCandidates.value.forEach(candidate => {
                if (candidate.status !== 'done') candidate.checked = checked;
            });
        }

        function scResetStageFolderDialog() {
            scStageFolderDialogStage.value = '';
            scStageFolderDialogName.value = '';
            scStageFolderCandidates.value = [];
            scStageUploadError.value = '';
            if (scStageFolderInput.value) scStageFolderInput.value.value = '';
            scStageFolderInput.value = null;
            scStageBatchCurrent.value = 0;
            scStageBatchTotal.value = 0;
        }

        function scOpenStageFolderDialog() {
            if (!currentObjectId.value || scStageUploadIsBusy.value) return;
            scResetStageFolderDialog();
            scStageFolderDialogOpen.value = true;
        }

        function scCloseStageFolderDialog() {
            if (scStageUploadIsBusy.value) return;
            scStageFolderDialogOpen.value = false;
            scResetStageFolderDialog();
        }

        function scUploadStageCandidate(objectId, stageName, candidate, retainBackup) {
            const form = new FormData();
            candidate.files.forEach(item => form.append('files', item.file, item.file.name));
            form.append('relative_paths', JSON.stringify(candidate.files.map(item => item.full)));
            form.append('folder_name', candidate.name);
            form.append('retain_backup', retainBackup ? 'true' : 'false');
            const url = `/api/stage-comparison/objects/${encodeURIComponent(objectId)}/stages/${stageName}/upload-folder`;

            candidate.status = 'uploading';
            candidate.progress = 0;
            candidate.message = '';
            candidate.result = null;
            return new Promise((resolve, reject) => {
                const request = new XMLHttpRequest();
                request.open('POST', url);
                request.responseType = 'json';
                request.upload.onprogress = (event) => {
                    if (!event.lengthComputable) return;
                    candidate.progress = Math.min(99, Math.round((event.loaded / event.total) * 100));
                };
                request.upload.onload = () => {
                    candidate.progress = 100;
                    candidate.status = 'processing';
                };
                request.onload = () => {
                    let data = request.response;
                    let responseText = '';
                    try { responseText = request.responseText || ''; } catch (_) { responseText = ''; }
                    if (!data && responseText) {
                        try { data = JSON.parse(responseText); } catch (_) { data = {}; }
                    }
                    data = data || {};
                    if (request.status >= 200 && request.status < 300) resolve(data);
                    else {
                        const fallback = request.status === 413
                            ? 'Проект превышает допустимый размер сервера (HTTP 413)'
                            : ('HTTP ' + request.status);
                        reject(new Error(data.detail || fallback));
                    }
                };
                request.onerror = () => reject(new Error('Сетевая ошибка при передаче проекта'));
                request.onabort = () => reject(new Error('Загрузка отменена'));
                request.send(form);
            });
        }

        async function scUploadStageFolder(event) {
            const input = event && event.target;
            const files = Array.from((input && input.files) || []);
            if (!currentObjectId.value || !files.length) return;
            const relativePaths = files.map(file => file.webkitRelativePath || file.name);
            const folderName = String(relativePaths[0] || '').split('/')[0] || 'folder';
            const candidates = scBuildStageFolderCandidates(files, folderName);
            scStageFolderDialogName.value = folderName;
            scStageFolderCandidates.value = candidates;
            scStageFolderInput.value = input;
            scStageUploadError.value = candidates.length
                ? ''
                : 'В выбранной папке не найдены PDF или ZIP-проекты.';
            scStageFolderDialogOpen.value = true;
        }

        async function scSubmitSelectedStageProjects() {
            const selected = scStageFolderCandidates.value.filter(candidate =>
                candidate.checked && candidate.status !== 'done'
            );
            if (!selected.length) return;
            const stageName = scStageFolderDialogStage.value;
            const objectId = currentObjectId.value;
            if (!objectId || !['stage_1', 'stage_2'].includes(stageName)) return;

            selected.forEach(candidate => {
                candidate.status = 'queued';
                candidate.progress = 0;
                candidate.message = '';
            });
            scStageUploadBusy[stageName] = true;
            scStageUploadError.value = '';
            scStageBatchCurrent.value = 0;
            scStageBatchTotal.value = selected.length;
            let successful = 0;
            let retainBackup = true;
            let closeAfterSuccess = false;
            try {
                for (let index = 0; index < selected.length; index += 1) {
                    const candidate = selected[index];
                    scStageBatchCurrent.value = index + 1;
                    try {
                        const result = await scUploadStageCandidate(
                            objectId, stageName, candidate, retainBackup,
                        );
                        candidate.status = 'done';
                        candidate.progress = 100;
                        candidate.checked = false;
                        candidate.result = result;
                        candidate.message = `${Number(result.documents_imported || result.pdf_count || 0)} PDF`;
                        successful += 1;
                        if (result.backup_path) retainBackup = false;
                    } catch (error) {
                        candidate.status = 'error';
                        candidate.progress = 0;
                        candidate.message = String(error.message || error);
                    }
                }
                if (successful) {
                    scActivePair.value = null;
                    scPairData.value = null;
                    await scLoadObjects();
                }
                const failed = selected.length - successful;
                if (failed) {
                    scStageUploadError.value = `Не загружено проектов: ${failed}. Подробности указаны в строках.`;
                }
                closeAfterSuccess = failed === 0;
            } finally {
                scStageUploadBusy[stageName] = false;
                if (scStageFolderInput.value) scStageFolderInput.value.value = '';
                if (closeAfterSuccess) scCloseStageFolderDialog();
            }
        }

        function scRememberPair(pair) {
            if (!pair || !scSession.value) return;
            if (!scPairs.value.some(item => item.id === pair.id)) {
                scSession.value.pairs = [...scPairs.value, pair];
            }
        }

        function scActivatePairData(data) {
            if (!data || !data.pair) return;
            scRememberPair(data.pair);
            scPairData.value = data;
            scActivePair.value = data.pair;
            scMatchState.value = data.sheet_matching || null;
            scSelectedPdf.left = data.pair.left.pdf_path;
            scSelectedPdf.right = data.pair.right.pdf_path;
            scViewerEmpty.left = false;
            scViewerEmpty.right = false;
            scContinuousPreview.left = {};
            scContinuousPreview.right = {};
            scContinuousLoading.left = {};
            scContinuousLoading.right = {};
            scContinuousError.left = {};
            scContinuousError.right = {};
            scContinuousDims.left = {};
            scContinuousDims.right = {};
            scContinuousSignatures.left = {};
            scContinuousSignatures.right = {};
            scContinuousTiles.left = {};
            scContinuousTiles.right = {};
            for (const side of ['left', 'right']) {
                for (const controller of scContinuousRequests[side].values()) controller.abort();
                scContinuousRequests[side].clear();
                scContinuousProgrammaticTarget[side] = null;
                scContinuousPageFromScroll[side] = null;
                scContinuousCurrentSlot[side] = '';
                if (scContinuousTileRefreshTimer[side]) clearTimeout(scContinuousTileRefreshTimer[side]);
                scContinuousTileRefreshTimer[side] = 0;
                if (scContinuousProgrammaticTimer[side]) clearTimeout(scContinuousProgrammaticTimer[side]);
                scContinuousProgrammaticTimer[side] = 0;
                if (scContinuousScrollFrame[side]) cancelAnimationFrame(scContinuousScrollFrame[side]);
                scContinuousScrollFrame[side] = 0;
            }
            scCurrentPage.left = 1;
            scCurrentPage.right = 1;
            scZoomFit();
            scTab.value = 'links';
            scFocusLeftPage(1);
        }

        async function scCreatePairForDocuments(leftPdf, rightPdf) {
            if (!scSession.value || !leftPdf || !rightPdf) return null;
            const response = await fetch(
                `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs`,
                {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({left_pdf: leftPdf, right_pdf: rightPdf}),
                },
            );
            const data = await response.json().catch(() => ({}));
            if (!response.ok) throw new Error(data.detail || ('HTTP ' + response.status));
            scRememberPair(data.pair);
            return data;
        }

        async function scOpenSelectedPair() {
            if (!scSession.value || !scSelectedPdf.left || !scSelectedPdf.right) return;
            scPairLoading.value = true;
            scSessionError.value = '';
            try {
                const data = await scCreatePairForDocuments(scSelectedPdf.left, scSelectedPdf.right);
                scActivatePairData(data);
                return data;
            } catch (error) {
                scSessionError.value = String(error.message || error);
            } finally {
                scPairLoading.value = false;
            }
        }

        async function scOpenPairRow(row) {
            if (!row || !row.left || !row.right || scPairRowBusy(row)) return;
            const state = scMutablePairRowState(row);
            state.status = 'opening';
            state.error = '';
            try {
                const data = await scCreatePairForDocuments(row.left.pdf_path, row.right.pdf_path);
                scActivatePairData(data);
                state.status = data.sheet_matching && data.sheet_matching.suggestions ? 'done' : 'opened';
            } catch (error) {
                state.status = 'error';
                state.error = String(error.message || error);
            }
        }

        async function scProcessPairRow(row) {
            if (!row || !row.left || !row.right || scPairRowBusy(row)) return;
            const state = scMutablePairRowState(row);
            state.status = 'processing';
            state.error = '';
            try {
                const data = await scCreatePairForDocuments(row.left.pdf_path, row.right.pdf_path);
                const matching = await scProcessPair(data.pair);
                data.sheet_matching = matching;
                scActivatePairData(data);
                state.status = 'done';
            } catch (error) {
                state.status = 'error';
                state.error = String(error.message || error);
            }
        }

        async function scOpenPair(pair) {
            if (!scSession.value || !pair) return;
            scPairLoading.value = true;
            try {
                const response = await fetch(
                    `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(pair.id)}`,
                );
                const data = await response.json().catch(() => ({}));
                if (!response.ok) throw new Error(data.detail || ('HTTP ' + response.status));
                scActivatePairData(data);
                const state = scMutablePairRowState({
                    index: -1,
                    left: data.pair.left,
                    right: data.pair.right,
                });
                state.status = data.sheet_matching && data.sheet_matching.suggestions ? 'done' : 'opened';
                state.error = '';
                return data;
            } catch (error) {
                scSessionError.value = String(error.message || error);
            } finally {
                scPairLoading.value = false;
            }
        }

        function scPairUrl(pairId, suffix) {
            if (!scSession.value) return '';
            return `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}`
                + `/pairs/${encodeURIComponent(pairId)}${suffix}`;
        }

        async function scProcessPair(pair) {
            if (!pair) return null;
            const response = await fetch(scPairUrl(pair.id, '/sheet-match-suggestions'), {method: 'POST'});
            const data = await response.json().catch(() => ({}));
            if (!response.ok) throw new Error(data.detail || ('HTTP ' + response.status));
            if (scActivePair.value && scActivePair.value.id === pair.id) {
                scMatchState.value = data;
                scFocusLeftPage(scCurrentPage.left);
            }
            return data;
        }

        async function scProcessCurrentSelection() {
            scProcessing.value = true;
            scProcessingError.value = '';
            try {
                const data = await scOpenSelectedPair();
                if (!data) return;
                await scProcessPair(data.pair);
                scTab.value = 'links';
            } catch (error) {
                scProcessingError.value = String(error.message || error);
            } finally {
                scProcessing.value = false;
            }
        }

        function scSheetIndexEntryFor(side, page) {
            const payload = scMatchState.value && scMatchState.value.suggestions;
            const sheetIndex = payload && payload[`${side}_sheet_index`] || [];
            return sheetIndex.find(item => Number(item.pdf_page) === Number(page)) || null;
        }

        function scSheetIndexTitle(sheet) {
            if (!sheet || !sheet.title) return '';
            let title = String(sheet.title)
                .replace(/\s+/g, ' ')
                .replace(/[.;,\s]+$/g, '')
                .trim();
            if (title.length > 72) title = title.slice(0, 69).trimEnd() + '…';
            return title;
        }

        function scSheetMapSideLabel(pages, side) {
            const uniquePages = [...new Set((pages || []).map(Number).filter(Boolean))]
                .sort((a, b) => a - b);
            if (!uniquePages.length) return 'Лист отсутствует';
            const sheetNumber = (page) => {
                const sheet = scSheetIndexEntryFor(side, page);
                return String((sheet && sheet.sheet_number) || page);
            };
            if (uniquePages.length > 1) {
                return `Листы ${uniquePages.map(sheetNumber).join(', ')}`;
            }
            const page = uniquePages[0];
            const sheet = scSheetIndexEntryFor(side, page);
            if (!sheet || !sheet.sheet_number) return `Страница ${page}`;
            const title = scSheetIndexTitle(sheet);
            return `Лист ${sheet.sheet_number}${title ? ' — ' + title : ''}`;
        }

        function scSheetMapStatus(row) {
            if (!row.leftPages.length || !row.rightPages.length) {
                return {tone: 'unmatched', label: 'Нет пары'};
            }
            if (row.source === 'manual' || row.confidence === 'manual') {
                return {tone: 'manual', label: 'Ручная'};
            }
            if (row.confidence === 'high') return {tone: 'high', label: 'Высокая'};
            return {tone: 'review', label: 'Проверить'};
        }

        function scToggleSheetMap() {
            scSheetMapCollapsed.value = !scSheetMapCollapsed.value;
            if (scSheetMapCollapsed.value) scLinkEditorOpen.value = false;
            try {
                localStorage.setItem(
                    'stage-comparison:sheet-map-collapsed',
                    scSheetMapCollapsed.value ? '1' : '0',
                );
            } catch (_) {}
        }

        function scSheetMapRowActive(row) {
            const activeSlot = scContinuousSlots.value.find(slot =>
                slot.key === scContinuousCurrentSlot.left
                || slot.key === scContinuousCurrentSlot.right
            );
            const continuousRowMatches = scViewMode.value === 'continuous'
                && activeSlot && activeSlot.rowKey === row.key;
            const leftMatches = row.leftPages.length
                ? !scViewerEmpty.left && row.leftPages.includes(Number(scCurrentPage.left))
                : continuousRowMatches || scViewerEmpty.left;
            const rightMatches = row.rightPages.length
                ? !scViewerEmpty.right && row.rightPages.includes(Number(scCurrentPage.right))
                : continuousRowMatches || scViewerEmpty.right;
            return leftMatches && rightMatches;
        }

        function scSetViewerEmpty(side, empty) {
            scViewerEmpty[side] = Boolean(empty);
            if (!empty) return;
            if (scPageInfoRequest[side]) scPageInfoRequest[side].abort();
            scPageInfoRequest[side] = null;
            scPageLoading[side] = false;
            scPageError[side] = '';
            scPagePreview[side] = '';
            scPageTiles[side] = [];
            scPageSignatures[side] = '';
            scPageDims[side] = {w: 0, h: 0};
            if (scTileRefreshTimer[side]) clearTimeout(scTileRefreshTimer[side]);
            scTileRefreshTimer[side] = 0;
            scScheduleView();
            for (const controller of scContinuousRequests[side].values()) controller.abort();
            scContinuousRequests[side].clear();
            scContinuousPreview[side] = {};
            scContinuousLoading[side] = {};
            scContinuousError[side] = {};
            scContinuousDims[side] = {};
            scContinuousSignatures[side] = {};
            scContinuousTiles[side] = {};
            scContinuousProgrammaticTarget[side] = null;
            scContinuousPageFromScroll[side] = null;
            scContinuousCurrentSlot[side] = '';
            if (scContinuousTileRefreshTimer[side]) clearTimeout(scContinuousTileRefreshTimer[side]);
            scContinuousTileRefreshTimer[side] = 0;
            if (scContinuousProgrammaticTimer[side]) clearTimeout(scContinuousProgrammaticTimer[side]);
            scContinuousProgrammaticTimer[side] = 0;
            if (scContinuousScrollFrame[side]) cancelAnimationFrame(scContinuousScrollFrame[side]);
            scContinuousScrollFrame[side] = 0;
        }

        function scOpenSheetMapRow(row) {
            if (scViewMode.value === 'continuous') {
                const slot = scContinuousSlots.value.find(item => item.rowKey === row.key);
                for (const side of ['left', 'right']) {
                    const pages = side === 'left' ? row.leftPages : row.rightPages;
                    scContinuousPageFromScroll[side] = pages[0] || scCurrentPage[side];
                    scSetViewerEmpty(side, scPageCount(side) <= 0);
                    if (pages.length) {
                        scCurrentPage[side] = pages[0];
                        scLoadContinuousWindow(side, pages[0]);
                    }
                    if (slot) scContinuousCurrentSlot[side] = slot.key;
                }
                if (slot) {
                    nextTick(() => {
                        scScrollContinuousToSlot('left', slot.key);
                        scScrollContinuousToSlot('right', slot.key);
                    });
                }
                scLinkEditorOpen.value = false;
                return;
            }
            for (const side of ['left', 'right']) {
                const pages = side === 'left' ? row.leftPages : row.rightPages;
                scSetViewerEmpty(side, !pages.length);
                if (pages.length) scCurrentPage[side] = pages[0];
            }
            scLinkEditorOpen.value = false;
        }

        function scReasonLabel(reason) {
            const labels = {
                same_sheet_number_and_title: 'одинаковые номер и название листа',
                same_unique_title: 'уникальное название листа',
                similar_title: 'похожее название листа',
                title_candidate: 'кандидат по названию',
                user_corrected: 'исправлено инженером', user_accepted: 'принято инженером',
            };
            return labels[reason] || reason;
        }

        function scNavigateContinuousPage(side, page) {
            const sourcePage = Math.min(
                scPageCount(side) || 1,
                Math.max(1, Number(page) || 1),
            );
            const slot = scContinuousSlotForPage(side, sourcePage);
            const targetSide = side === 'left' ? 'right' : 'left';
            const targetPage = slot
                && (targetSide === 'left' ? slot.leftPage : slot.rightPage);
            scContinuousPageFromScroll[side] = sourcePage;
            scSetViewerEmpty(side, scPageCount(side) <= 0);
            scCurrentPage[side] = sourcePage;
            scLoadContinuousWindow(side, sourcePage);
            if (slot) {
                scContinuousCurrentSlot[side] = slot.key;
                scContinuousCurrentSlot[targetSide] = slot.key;
                scContinuousPageFromScroll[targetSide] = targetPage || scCurrentPage[targetSide];
                scSetViewerEmpty(targetSide, scPageCount(targetSide) <= 0);
                if (targetPage) {
                    scCurrentPage[targetSide] = targetPage;
                    scLoadContinuousWindow(targetSide, targetPage);
                }
                nextTick(() => {
                    scScrollContinuousToSlot(side, slot.key);
                    scScrollContinuousToSlot(targetSide, slot.key);
                });
            }
        }

        function scFocusLeftPage(page) {
            if (scViewMode.value === 'continuous') {
                scNavigateContinuousPage('left', page);
                scLinkEditorOpen.value = false;
                return;
            }
            scSetViewerEmpty('left', false);
            scCurrentPage.left = Math.min(scPageCount('left') || 1, Math.max(1, Number(page) || 1));
            const rightPages = scCurrentRightPages.value;
            scSetViewerEmpty('right', !rightPages.length);
            if (rightPages.length) scCurrentPage.right = rightPages[0];
            scLinkEditorOpen.value = false;
        }

        function scSwitchRightPage(page) {
            if (scViewMode.value === 'continuous') {
                scNavigateContinuousPage('right', page);
                return;
            }
            scSetViewerEmpty('right', false);
            scCurrentPage.right = Math.min(scPageCount('right') || 1, Math.max(1, Number(page) || 1));
        }

        function scOpenSheetMapEditor(row, mode) {
            if (!row.leftPages.length) return;
            scOpenSheetMapRow(row);
            scLinkEditorMode.value = mode;
            scLinkEditorLeftPages.value = [...row.leftPages];
            scLinkEditorRightPages.value = [...row.rightPages];
            scLinkEditorSourceIndex.value = Number.isInteger(row.explicitLinkIndex)
                ? row.explicitLinkIndex
                : null;
            const suggestion = scSuggestions.value.find(item =>
                Number(item.left_page) === Number(row.leftPages[0])
            );
            const suggested = suggestion && suggestion.primary_right_page;
            scLinkEditorRightPage.value = String(
                row.rightPages[0] || suggested || (scRightOptions.value[0] || {}).pdf_page || ''
            );
            scLinkEditorOpen.value = true;
        }

        function scCloseSheetMapEditor() {
            scLinkEditorOpen.value = false;
            scLinkEditorLeftPages.value = [];
            scLinkEditorRightPages.value = [];
            scLinkEditorSourceIndex.value = null;
            scLinkEditorRightPage.value = '';
        }

        function scOpenLinkEditor(mode) {
            const row = scSheetMapRows.value.find(item =>
                item.leftPages.includes(Number(scCurrentPage.left))
                && (!item.rightPages.length || item.rightPages.includes(Number(scCurrentPage.right)))
            ) || scSheetMapRows.value.find(item =>
                item.leftPages.includes(Number(scCurrentPage.left))
            );
            if (row) scOpenSheetMapEditor(row, mode);
        }

        function scChooseUnmatchedRight(page) {
            scSwitchRightPage(page);
            scLinkEditorMode.value = 'add';
            scLinkEditorRightPage.value = String(page);
            scLinkEditorOpen.value = true;
        }

        function scWithoutLeft(links, leftPage) {
            const output = [];
            for (const link of links) {
                if (!(link.left_pages || []).map(Number).includes(Number(leftPage))) {
                    output.push(link);
                    continue;
                }
                const remaining = (link.left_pages || []).map(Number).filter(page => page !== Number(leftPage));
                if (remaining.length) output.push({...link, left_pages: remaining});
            }
            return output;
        }

        async function scPersistLinks(links, unlinkedLeftPages) {
            if (!scActivePair.value) return;
            scLinkSaving.value = true;
            scProcessingError.value = '';
            try {
                const response = await fetch(scPairUrl(scActivePair.value.id, '/sheet-links'), {
                    method: 'PUT',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({links, unlinked_left_pages: unlinkedLeftPages}),
                });
                const data = await response.json().catch(() => ({}));
                if (!response.ok) throw new Error(data.detail || ('HTTP ' + response.status));
                scMatchState.value = data;
                scFocusLeftPage(scCurrentPage.left);
            } catch (error) {
                scProcessingError.value = String(error.message || error);
            } finally {
                scLinkSaving.value = false;
            }
        }

        function scSheetMapOptions(side) {
            const suggestionsPayload = scMatchState.value && scMatchState.value.suggestions;
            const indexed = (suggestionsPayload && suggestionsPayload[`${side}_sheet_index`]) || [];
            const byPage = new Map(indexed.map(sheet => [Number(sheet.pdf_page), sheet]));
            const count = scPageCount(side);
            return Array.from({length: count}, (_, index) => {
                const page = index + 1;
                return byPage.get(page) || {pdf_page: page};
            });
        }

        function scSheetMapSelectionValue(row, side) {
            const pages = side === 'left' ? row.leftPages : row.rightPages;
            if (!pages.length) return '__empty__';
            if (pages.length === 1) return String(pages[0]);
            return `many:${pages.join(',')}`;
        }

        async function scApplySheetMapSelection(row, side, rawValue) {
            if (scLinkSaving.value) return;
            const selected = rawValue === '__empty__' ? null : Number(rawValue);
            if (selected !== null && (!Number.isInteger(selected) || selected < 1)) return;

            const oldLeft = [...row.leftPages].map(Number);
            const oldRight = [...row.rightPages].map(Number);
            const nextLeft = side === 'left' ? (selected ? [selected] : []) : oldLeft;
            const nextRight = side === 'right' ? (selected ? [selected] : []) : oldRight;
            scOpenSheetMapRow(row);

            let links = scSheetLinks.value.map(link => ({
                ...link,
                left_pages: [...(link.left_pages || [])].map(Number),
                right_pages: [...(link.right_pages || [])].map(Number),
            }));
            if (Number.isInteger(row.explicitLinkIndex) && links[row.explicitLinkIndex]) {
                links.splice(row.explicitLinkIndex, 1);
            } else {
                for (const leftPage of oldLeft) links = scWithoutLeft(links, leftPage);
            }
            // Один лист П не должен одновременно оставаться в старой ручной
            // связи и в только что выбранной строке.
            for (const leftPage of nextLeft) links = scWithoutLeft(links, leftPage);
            if (nextLeft.length && nextRight.length) {
                links.push({
                    left_pages: nextLeft,
                    right_pages: nextRight,
                    source: 'manual', confidence: 'manual', reason: ['user_corrected'],
                });
            }

            const stillLinked = new Set(links.flatMap(link => (link.left_pages || []).map(Number)));
            const unlinked = new Set(scUnlinkedLeftPages.value.map(Number));
            for (const leftPage of oldLeft) {
                if (!stillLinked.has(leftPage)) unlinked.add(leftPage);
            }
            for (const leftPage of nextLeft) {
                if (nextRight.length) unlinked.delete(leftPage);
                else unlinked.add(leftPage);
            }

            scSetViewerEmpty('left', !nextLeft.length);
            scSetViewerEmpty('right', !nextRight.length);
            if (nextLeft.length) scCurrentPage.left = nextLeft[0];
            if (nextRight.length) scCurrentPage.right = nextRight[0];
            await scPersistLinks(links, [...unlinked].sort((a, b) => a - b));
            // scPersistLinks обновляет обычный фокус после ответа. Для строки
            // с пустой стороной восстанавливаем именно выбранное состояние.
            scSetViewerEmpty('left', !nextLeft.length);
            scSetViewerEmpty('right', !nextRight.length);
            if (nextLeft.length) scCurrentPage.left = nextLeft[0];
            if (nextRight.length) scCurrentPage.right = nextRight[0];
        }

        async function scAcceptSuggestion(row = null) {
            const leftPage = Number(
                row && row.leftPages && row.leftPages[0] || scCurrentPage.left
            );
            const suggestion = scSuggestions.value.find(item =>
                Number(item.left_page) === leftPage
            );
            if (!suggestion || !suggestion.primary_right_page) return;
            if (row) scOpenSheetMapRow(row);
            const links = scWithoutLeft(scSheetLinks.value.map(link => ({...link})), leftPage);
            links.push({
                left_pages: [leftPage], right_pages: [Number(suggestion.primary_right_page)],
                source: 'auto', confidence: suggestion.confidence,
                reason: [...(suggestion.reason || []), 'user_accepted'],
            });
            await scPersistLinks(links, scUnlinkedLeftPages.value.filter(page => Number(page) !== leftPage));
        }

        async function scApplyLinkEditor() {
            const selected = Number(scLinkEditorRightPage.value);
            if (!selected) return;
            const leftPages = scLinkEditorLeftPages.value.length
                ? [...scLinkEditorLeftPages.value]
                : [Number(scCurrentPage.left)];
            const rightPages = scLinkEditorMode.value === 'add'
                ? [...new Set([...scLinkEditorRightPages.value, selected])]
                : [selected];
            let links = scSheetLinks.value.map(link => ({
                ...link,
                left_pages: [...(link.left_pages || [])],
                right_pages: [...(link.right_pages || [])],
            }));
            const sourceIndex = scLinkEditorSourceIndex.value;
            if (Number.isInteger(sourceIndex) && links[sourceIndex]) {
                links.splice(sourceIndex, 1);
            } else {
                for (const leftPage of leftPages) links = scWithoutLeft(links, leftPage);
            }
            links.push({
                left_pages: leftPages, right_pages: rightPages,
                source: 'manual', confidence: 'manual', reason: ['user_corrected'],
            });
            const linkedLeft = new Set(leftPages.map(Number));
            await scPersistLinks(
                links,
                scUnlinkedLeftPages.value.filter(page => !linkedLeft.has(Number(page))),
            );
        }

        async function scDeleteSheetMapRow(row) {
            if (!row.leftPages.length || !row.rightPages.length) return;
            scOpenSheetMapRow(row);
            let links = scSheetLinks.value.map(link => ({
                ...link,
                left_pages: [...(link.left_pages || [])],
                right_pages: [...(link.right_pages || [])],
            }));
            if (Number.isInteger(row.explicitLinkIndex) && links[row.explicitLinkIndex]) {
                links.splice(row.explicitLinkIndex, 1);
            } else {
                for (const leftPage of row.leftPages) links = scWithoutLeft(links, leftPage);
            }
            const stillLinked = new Set(links.flatMap(link => (link.left_pages || []).map(Number)));
            const unlinked = new Set(scUnlinkedLeftPages.value.map(Number));
            row.leftPages.forEach(page => {
                if (!stillLinked.has(Number(page))) unlinked.add(Number(page));
            });
            await scPersistLinks(links, [...unlinked].sort((a, b) => a - b));
        }

        async function scDeleteCurrentLink() {
            const leftPage = Number(scCurrentPage.left);
            const links = scWithoutLeft(scSheetLinks.value.map(link => ({...link})), leftPage);
            const unlinked = [...new Set([...scUnlinkedLeftPages.value.map(Number), leftPage])];
            await scPersistLinks(links, unlinked);
        }

        // ─── Панель миниатюр листов ───────────────────────────────────────
        // Нужна только для ориентации: мини-кропы пар листов слева, клик —
        // переход. Никаких действий над страницами (поворот, печать, удаление)
        // здесь нет и не предполагается.
        const scThumbsOpen = ref(false);
        try { scThumbsOpen.value = localStorage.getItem('stage-comparison:thumbs') === '1'; } catch (_) {}
        watch(scThumbsOpen, open => {
            try { localStorage.setItem('stage-comparison:thumbs', open ? '1' : '0'); } catch (_) {}
        });

        function scToggleThumbs() {
            scThumbsOpen.value = !scThumbsOpen.value;
        }

        function scThumbUrl(side, page) {
            if (!scSession.value || !scActivePair.value || !page) return '';
            const sessionId = encodeURIComponent(scSession.value.id);
            const pairId = encodeURIComponent(scActivePair.value.id);
            return `/api/stage-comparison/sessions/${sessionId}/pairs/${pairId}`
                + `/page-thumb?side=${side}&page=${page}&width=200`;
        }

        // Карта листов уже описывает пары, поэтому полоса строится из неё —
        // иначе связи пришлось бы считать второй раз и они бы разъезжались.
        // Пока карты нет, показываем страницы как есть: панель должна работать
        // сразу после открытия пары, до всякого сопоставления.
        const scThumbRows = computed(() => {
            const mapped = scSheetMapRows.value;
            if (mapped.length) {
                return mapped.map((row, index) => ({
                    key: 'thumb-map-' + (row.key || index),
                    index,
                    leftPage: (row.leftPages || [])[0] || null,
                    rightPage: (row.rightPages || [])[0] || null,
                    source: row,
                }));
            }
            const leftCount = scPageCount('left');
            const rightCount = scPageCount('right');
            const length = Math.max(leftCount, rightCount);
            return Array.from({length}, (_, index) => ({
                key: 'thumb-page-' + index,
                index,
                leftPage: index < leftCount ? index + 1 : null,
                rightPage: index < rightCount ? index + 1 : null,
                source: null,
            }));
        });

        // ─── Перестановка листов в полосе ─────────────────────────────────
        // Двигаем ОДНУ сторону: строка полосы — это пара, и её положение
        // выводится из связей, а не хранится отдельно. Поэтому перестановка —
        // это переназначение связей, после которого карта листов сверху
        // пересобирается сама из тех же данных.
        const scThumbSelection = reactive({side: null, pages: []});
        const scThumbDragOver = reactive({index: null, side: null});
        let scThumbDrag = null;
        let scThumbAutoScroll = null;

        function scThumbPage(row, side) {
            return side === 'left' ? row.leftPage : row.rightPage;
        }

        function scThumbSelected(side, page) {
            return Boolean(page)
                && scThumbSelection.side === side
                && scThumbSelection.pages.includes(Number(page));
        }

        function scThumbDraggable(row, side) {
            // В запасном режиме (карта листов ещё не построена) переставлять
            // нечего: связей нет, сохранять результат некуда.
            return Boolean(row.source && scThumbPage(row, side) && !scLinkSaving.value);
        }

        function scSelectThumbCell(event, row, side) {
            const page = Number(scThumbPage(row, side));
            if (!page || !row.source) return false;
            const additive = event.ctrlKey || event.metaKey;
            const ranged = event.shiftKey;
            if (!additive && !ranged) return false;
            if (scThumbSelection.side !== side) {
                scThumbSelection.side = side;
                scThumbSelection.pages = [];
            }
            if (ranged && scThumbSelection.pages.length) {
                const order = scThumbRows.value
                    .map(item => Number(scThumbPage(item, side)))
                    .filter(Boolean);
                const anchor = order.indexOf(Number(scThumbSelection.pages[scThumbSelection.pages.length - 1]));
                const target = order.indexOf(page);
                if (anchor >= 0 && target >= 0) {
                    const [from, to] = anchor <= target ? [anchor, target] : [target, anchor];
                    const range = order.slice(from, to + 1);
                    scThumbSelection.pages = [...new Set([...scThumbSelection.pages, ...range])];
                    return true;
                }
            }
            const at = scThumbSelection.pages.indexOf(page);
            if (at >= 0) scThumbSelection.pages.splice(at, 1);
            else scThumbSelection.pages.push(page);
            if (!scThumbSelection.pages.length) scThumbSelection.side = null;
            return true;
        }

        // Обычный клик — переход к паре, клик с Ctrl/Shift — пометка листа.
        // Разделение по модификатору, а не по отдельной зоне: полоса узкая, и
        // отдельный чекбокс съел бы место, ради которого её и открывают.
        function scThumbCellClick(event, row, side) {
            if (scSelectThumbCell(event, row, side)) return;
            scOpenThumbRow(row);
        }

        function scClearThumbSelection() {
            scThumbSelection.side = null;
            scThumbSelection.pages = [];
        }

        // Список длиннее экрана, а перетаскивание идёт снизу вверх: без
        // подкрутки у края до пустых мест наверху просто не дотянуться.
        // dragover при неподвижном курсоре не приходит, поэтому крутим в
        // отдельном кадре по последней известной точке.
        function scThumbAutoScrollTick() {
            if (!scThumbAutoScroll) return;
            const {list, y} = scThumbAutoScroll;
            const rect = list.getBoundingClientRect();
            const zone = 48;
            let step = 0;
            if (y - rect.top < zone) step = -Math.ceil((zone - (y - rect.top)) / 3);
            else if (rect.bottom - y < zone) step = Math.ceil((zone - (rect.bottom - y)) / 3);
            if (step) list.scrollTop += step;
            scThumbAutoScroll.frame = requestAnimationFrame(scThumbAutoScrollTick);
        }

        function scStopThumbAutoScroll() {
            if (!scThumbAutoScroll) return;
            cancelAnimationFrame(scThumbAutoScroll.frame);
            scThumbAutoScroll = null;
        }

        function scThumbDragStart(event, row, side) {
            if (!scThumbDraggable(row, side)) {
                event.preventDefault();
                return;
            }
            const page = Number(scThumbPage(row, side));
            if (!scThumbSelected(side, page)) {
                scThumbSelection.side = side;
                scThumbSelection.pages = [page];
            }
            // Тащим в порядке строк, а не в порядке кликов — иначе выбранные
            // листы легли бы вперемешку.
            const order = scThumbRows.value
                .map(item => Number(scThumbPage(item, side)))
                .filter(item => scThumbSelection.pages.includes(item));
            scThumbDrag = {side, pages: order};
            if (event.dataTransfer) {
                event.dataTransfer.effectAllowed = 'move';
                event.dataTransfer.setData('text/plain', `${side}:${order.join(',')}`);
            }
            const list = document.querySelector('.sc-thumbs__list');
            if (list) {
                scStopThumbAutoScroll();
                scThumbAutoScroll = {list, y: event.clientY, frame: 0};
                scThumbAutoScroll.frame = requestAnimationFrame(scThumbAutoScrollTick);
            }
        }

        function scThumbDragOverCell(event, row, side) {
            if (!scThumbDrag || scThumbDrag.side !== side) return;
            if (event.dataTransfer) event.dataTransfer.dropEffect = 'move';
            if (scThumbAutoScroll) scThumbAutoScroll.y = event.clientY;
            scThumbDragOver.index = row.index;
            scThumbDragOver.side = side;
        }

        function scThumbDragEnd() {
            scThumbDrag = null;
            scThumbDragOver.index = null;
            scThumbDragOver.side = null;
            scStopThumbAutoScroll();
        }

        function scThumbDropOn(row, side) {
            const drag = scThumbDrag;
            scThumbDragEnd();
            if (!drag || drag.side !== side || !row.source) return;
            scMoveThumbSheets(side, drag.pages, row.index);
        }

        // Раскладываем по ближайшим СВОБОДНЫМ местам вниз от цели и не трогаем
        // уже сложившиеся пары: перестановка не должна молча разрывать чужую
        // связь, которую инженер подтвердил раньше.
        function scPlanThumbMove(side, pages, targetIndex) {
            const key = side === 'left' ? 'leftPages' : 'rightPages';
            const moving = pages.map(Number).filter(Boolean);
            const rows = scSheetMapRows.value.map(row => ({
                key: row.key,
                leftPages: [...row.leftPages],
                rightPages: [...row.rightPages],
            }));
            const movingSet = new Set(moving);
            rows.forEach(row => {
                row[key] = row[key].filter(page => !movingSet.has(Number(page)));
            });
            const queue = [...moving];
            for (let index = Math.max(0, targetIndex); index < rows.length && queue.length; index += 1) {
                if (rows[index][key].length) continue;
                rows[index][key] = [queue.shift()];
            }
            queue.forEach(page => {
                const row = {key: `moved-${side}-${page}`, leftPages: [], rightPages: []};
                row[key] = [page];
                rows.push(row);
            });
            return rows;
        }

        // Явными связями делаем только ИЗМЕНЁННЫЕ строки. Иначе разовая
        // перестановка перекрасила бы всю карту в «Ручная» и стёрла бы
        // уверенность автосопоставления на строках, которых никто не трогал.
        function scLinksFromThumbRows(rows) {
            const before = new Map(scSheetMapRows.value.map(row => [row.key, row]));
            const links = [];
            const unlinked = new Set(scUnlinkedLeftPages.value.map(Number));
            for (const row of rows) {
                const previous = before.get(row.key);
                const changed = !previous
                    || previous.leftPages.join(',') !== row.leftPages.join(',')
                    || previous.rightPages.join(',') !== row.rightPages.join(',');
                if (row.leftPages.length && row.rightPages.length) {
                    row.leftPages.forEach(page => unlinked.delete(Number(page)));
                    if (changed) {
                        links.push({
                            left_pages: row.leftPages,
                            right_pages: row.rightPages,
                            source: 'manual',
                            confidence: 'manual',
                            reason: ['user_reordered'],
                        });
                    } else if (previous && previous.explicitLinkIndex !== null) {
                        links.push({
                            left_pages: row.leftPages,
                            right_pages: row.rightPages,
                            source: previous.source || 'manual',
                            confidence: previous.confidence || 'manual',
                            reason: previous.reason || [],
                        });
                    }
                } else if (row.leftPages.length) {
                    // лист без пары обязан остаться без пары: без явной пометки
                    // автосопоставление вернуло бы ему прежний правый лист
                    row.leftPages.forEach(page => unlinked.add(Number(page)));
                }
            }
            return {links, unlinked: [...unlinked].sort((a, b) => a - b)};
        }

        async function scMoveThumbSheets(side, pages, targetIndex) {
            if (!pages.length || scLinkSaving.value) return;
            const planned = scPlanThumbMove(side, pages, targetIndex);
            const {links, unlinked} = scLinksFromThumbRows(planned);
            scClearThumbSelection();
            await scPersistLinks(links, unlinked);
        }

        function scThumbRowActive(row) {
            if (row.source) return scSheetMapRowActive(row.source);
            return Number(row.leftPage) === Number(scCurrentPage.left);
        }

        function scThumbRowTitle(row) {
            if (!row.source) {
                return `Страница ${row.leftPage || '—'} ↔ страница ${row.rightPage || '—'}`;
            }
            return scSheetMapSideLabel(row.source.leftPages, 'left')
                + ' ↔ ' + scSheetMapSideLabel(row.source.rightPages, 'right');
        }

        function scOpenThumbRow(row) {
            if (row.source) {
                scOpenSheetMapRow(row.source);
                return;
            }
            if (row.leftPage) scFocusLeftPage(row.leftPage);
            if (row.rightPage) scSwitchRightPage(row.rightPage);
        }

        // Полоса длиннее экрана, и при листании в просмотрщике активная пара
        // уходит за край. Крутим сам список, а не страницу: scrollIntoView
        // утащил бы за собой панели просмотрщика.
        function scRevealActiveThumb() {
            const list = document.querySelector('.sc-thumbs__list');
            const row = list && list.querySelector('.sc-thumbs__row.is-active');
            if (!row) return;
            const listRect = list.getBoundingClientRect();
            const rowRect = row.getBoundingClientRect();
            if (rowRect.top < listRect.top) {
                list.scrollTop -= listRect.top - rowRect.top;
            } else if (rowRect.bottom > listRect.bottom) {
                list.scrollTop += rowRect.bottom - listRect.bottom;
            }
        }

        watch(
            () => [scCurrentPage.left, scCurrentPage.right, scThumbsOpen.value],
            () => {
                if (currentView.value === 'stage-comparison' && scThumbsOpen.value) {
                    scRevealActiveThumb();
                }
            },
            {flush: 'post'},
        );

        function scPageCount(side) {
            return Number(scPairData.value && scPairData.value[`${side}_page_count`] || 0);
        }

        function scPageApiBase() {
            if (!scSession.value || !scActivePair.value) return '';
            const sessionId = encodeURIComponent(scSession.value.id);
            const pairId = encodeURIComponent(scActivePair.value.id);
            return `/api/stage-comparison/sessions/${sessionId}/pairs/${pairId}`;
        }

        function scPageInfoUrl(side, page = scCurrentPage[side]) {
            const base = scPageApiBase();
            return base ? `${base}/page-info?side=${side}&page=${page}` : '';
        }

        function scPagePreviewUrl(side, page, width, signature) {
            const base = scPageApiBase();
            if (!base) return '';
            return `${base}/page-preview?side=${side}&page=${page}&width=${width}`
                + `&v=${encodeURIComponent(signature || '')}`;
        }

        function scPageTileUrl(side, page, level, x, y, signature) {
            const base = scPageApiBase();
            if (!base) return '';
            return `${base}/page-tile?side=${side}&page=${page}&level=${level}&x=${x}&y=${y}`
                + `&v=${encodeURIComponent(signature || '')}`;
        }

        function scChangePage(side, delta) {
            const limit = scPageCount(side);
            if (scViewMode.value === 'continuous') {
                scNavigateContinuousPage(
                    side,
                    Math.min(limit || 1, Math.max(1, scCurrentPage[side] + delta)),
                );
                return;
            }
            scSetViewerEmpty(side, false);
            scCurrentPage[side] = Math.min(limit || 1, Math.max(1, scCurrentPage[side] + delta));
            if (side === 'left') scFocusLeftPage(scCurrentPage.left);
        }

        // ─── Просмотрщик: геометрия ───────────────────────────────────────
        // При связанном виде обе панели читают ОДИН объект состояния, поэтому
        // «синхронно» получается по построению, без копирования и дрожания.
        function scViewFor(side) {
            return scSyncView.value ? scViews.left : scViews[side];
        }

        function scMeasurePane(side) {
            const pane = scPaneRefs[side];
            if (!pane) return;
            scPaneSize[side] = {w: pane.clientWidth, h: pane.clientHeight};
        }

        // Масштаб «лист целиком» — своя величина для каждой панели, потому что
        // форматы листов и ширины панелей различаются.
        function scFitScale(side) {
            const dims = scPageDims[side];
            const pane = scPaneSize[side];
            if (!dims.w || !dims.h || !pane.w || !pane.h) return 1;
            return Math.min(pane.w / dims.w, pane.h / dims.h);
        }

        function scScaleFor(side) {
            return scFitScale(side) * scViewFor(side).zoom;
        }

        function scClampView(side) {
            const view = scViewFor(side);
            view.zoom = Math.min(SC_ZOOM_MAX, Math.max(SC_ZOOM_MIN, view.zoom));
            const sides = scSyncView.value ? ['left', 'right'] : [side];
            for (const axis of ['x', 'y']) {
                const key = axis === 'x' ? 'cx' : 'cy';
                let half = Infinity;
                for (const item of sides) {
                    const dims = scPageDims[item];
                    const pane = scPaneSize[item];
                    const span = (axis === 'x' ? dims.w : dims.h) * scScaleFor(item);
                    if (!span || !pane.w || !pane.h) continue;
                    half = Math.min(half, (axis === 'x' ? pane.w : pane.h) / 2 / span);
                }
                if (!Number.isFinite(half)) {
                    view[key] = Math.min(1, Math.max(0, view[key]));
                } else if (half >= 0.5) {
                    view[key] = 0.5;                     // лист виден целиком
                } else {
                    view[key] = Math.min(1 - half, Math.max(half, view[key]));
                }
            }
        }

        // Preview остаётся подложкой на всём листе. После паузы в pan/zoom
        // поверх него появляется только небольшая рамка видимых 512px-тайлов.
        // Поэтому движение не запускает Vue-рендер на каждый кадр и не создаёт
        // гигантский canvas либо DOM из SVG-команд.
        function scRefreshTiles(side) {
            if (scViewMode.value !== 'paged' || scViewerEmpty[side]) {
                scPageTiles[side] = [];
                return;
            }
            const dims = scPageDims[side];
            const pane = scPaneSize[side];
            const scale = scScaleFor(side);
            const page = Number(scCurrentPage[side]);
            const signature = scPageSignatures[side];
            if (!dims.w || !dims.h || !pane.w || !pane.h || !scale || !signature) {
                scPageTiles[side] = [];
                return;
            }
            const pixelRatio = Math.min(2, Math.max(1, Number(window.devicePixelRatio) || 1));
            const wantedDensity = scale * pixelRatio;
            const previewDensity = SC_PREVIEW_WIDTH / dims.w;
            if (wantedDensity <= previewDensity * 1.05) {
                scPageTiles[side] = [];
                return;
            }
            const level = Math.min(
                SC_TILE_MAX_LEVEL,
                Math.max(0, Math.ceil(Math.log2(wantedDensity))),
            );
            const tileScale = 2 ** level;
            const span = SC_TILE_SIZE / tileScale;
            const view = scViewFor(side);
            const halfWidth = pane.w / (2 * scale);
            const halfHeight = pane.h / (2 * scale);
            const x0 = Math.max(0, view.cx * dims.w - halfWidth);
            const y0 = Math.max(0, view.cy * dims.h - halfHeight);
            const x1 = Math.min(dims.w, view.cx * dims.w + halfWidth);
            const y1 = Math.min(dims.h, view.cy * dims.h + halfHeight);
            const columns = Math.max(1, Math.ceil(dims.w / span));
            const rows = Math.max(1, Math.ceil(dims.h / span));
            const startX = Math.max(0, Math.floor(x0 / span) - 1);
            const startY = Math.max(0, Math.floor(y0 / span) - 1);
            const endX = Math.min(columns - 1, Math.floor(Math.max(0, x1 - 0.001) / span) + 1);
            const endY = Math.min(rows - 1, Math.floor(Math.max(0, y1 - 0.001) / span) + 1);
            const tiles = [];
            for (let y = startY; y <= endY; y += 1) {
                for (let x = startX; x <= endX; x += 1) {
                    tiles.push({
                        key: `${signature}:${page}:${level}:${x}:${y}`,
                        url: scPageTileUrl(side, page, level, x, y, signature),
                        left: x * span,
                        top: y * span,
                        width: Math.min(span, dims.w - x * span),
                        height: Math.min(span, dims.h - y * span),
                    });
                }
            }
            const before = scPageTiles[side].map(tile => tile.key).join('|');
            const after = tiles.map(tile => tile.key).join('|');
            if (before !== after) scPageTiles[side] = tiles;
        }

        function scScheduleTileRefresh(side, immediate = false) {
            if (scTileRefreshTimer[side]) clearTimeout(scTileRefreshTimer[side]);
            scTileRefreshTimer[side] = setTimeout(() => {
                scTileRefreshTimer[side] = 0;
                scRefreshTiles(side);
            }, immediate ? 0 : 90);
        }

        function scApplyView() {
            for (const side of ['left', 'right']) {
                const stage = scStageRefs[side];
                const pane = scPaneRefs[side];
                if (!stage || !pane) continue;
                const dims = scPageDims[side];
                if (!dims.w || !dims.h) {
                    stage.style.transform = '';
                    continue;
                }
                const view = scViewFor(side);
                const scale = scScaleFor(side);
                const left = scPaneSize[side].w / 2 - view.cx * dims.w * scale;
                const top = scPaneSize[side].h / 2 - view.cy * dims.h * scale;
                stage.style.width = dims.w + 'px';
                stage.style.height = dims.h + 'px';
                stage.style.transform =
                    `translate(${left.toFixed(2)}px, ${top.toFixed(2)}px) scale(${scale})`;
            }
            const percent = Math.round(scViewFor('left').zoom * 100);
            if (percent !== scZoomPercent.value) scZoomPercent.value = percent;
            if (scViewMode.value === 'paged') {
                scScheduleTileRefresh('left');
                scScheduleTileRefresh('right');
            }
        }

        // Кадры склеиваются: несколько событий в одном кадре дают один расчёт.
        // Стороны при этом клампятся обе, иначе склейка события правой панели
        // с событием левой потеряла бы ограничение одной из них.
        function scScheduleView() {
            if (scViewFrame) return;
            scViewFrame = requestAnimationFrame(() => {
                scViewFrame = 0;
                scClampView('left');
                if (!scSyncView.value) scClampView('right');
                scApplyView();
            });
        }

        // ─── Просмотрщик: ввод ────────────────────────────────────────────
        function scWheelPixels(event) {
            // DOM_DELTA_LINE / DOM_DELTA_PAGE приводим к пикселям, иначе один
            // «щелчок» мыши в Firefox сдвигал бы лист на три пикселя.
            const factor = event.deltaMode === 1 ? 16 : event.deltaMode === 2 ? 800 : 1;
            return {x: event.deltaX * factor, y: event.deltaY * factor};
        }

        function scZoomAt(side, multiplier, clientX, clientY) {
            scDropPanBoost();
            const pane = scPaneRefs[side];
            const dims = scPageDims[side];
            const view = scViewFor(side);
            if (!pane || !dims.w || !dims.h) {
                view.zoom *= multiplier;
                scScheduleView();
                return;
            }
            const rect = pane.getBoundingClientRect();
            const offsetX = (clientX == null ? rect.width / 2 : clientX - rect.left) - rect.width / 2;
            const offsetY = (clientY == null ? rect.height / 2 : clientY - rect.top) - rect.height / 2;
            const before = scScaleFor(side);
            // Нормированная точка листа под курсором — она обязана остаться на
            // месте, и на второй панели тоже (координаты общие).
            const anchorX = view.cx + offsetX / (dims.w * before);
            const anchorY = view.cy + offsetY / (dims.h * before);
            view.zoom = Math.min(SC_ZOOM_MAX, Math.max(SC_ZOOM_MIN, view.zoom * multiplier));
            const after = scScaleFor(side);
            view.cx = anchorX - offsetX / (dims.w * after);
            view.cy = anchorY - offsetY / (dims.h * after);
            scScheduleView();
        }

        function scSetPanBoost(active) {
            for (const side of ['left', 'right']) {
                const stage = scStageRefs[side];
                if (stage) stage.style.willChange = active ? 'transform' : '';
            }
        }

        // Сдвиг не меняет масштаб, поэтому композитор может двигать готовую
        // preview+tile подложку. При зуме снимаем временный promotion, чтобы
        // браузер не держал лишний GPU-слой; новый уровень тайлов придёт после
        // короткой паузы жеста.
        function scBoostPan() {
            if (scPanBoostTimer) clearTimeout(scPanBoostTimer);
            else scSetPanBoost(true);
            scPanBoostTimer = setTimeout(() => {
                scPanBoostTimer = 0;
                scSetPanBoost(false);
            }, 200);
        }

        function scDropPanBoost() {
            if (!scPanBoostTimer) return;
            clearTimeout(scPanBoostTimer);
            scPanBoostTimer = 0;
            scSetPanBoost(false);
        }

        function scPanBy(side, deltaX, deltaY) {
            const dims = scPageDims[side];
            const scale = scScaleFor(side);
            if (!dims.w || !dims.h || !scale) return;
            const view = scViewFor(side);
            view.cx += deltaX / (dims.w * scale);
            view.cy += deltaY / (dims.h * scale);
            scBoostPan();
            scScheduleView();
        }

        function scOnViewerWheel(side, event) {
            event.preventDefault();          // Ctrl+колесо иначе зумит браузер
            scMeasurePane(side);
            const delta = scWheelPixels(event);
            if (event.ctrlKey || event.metaKey) {
                scZoomAt(side, Math.exp(-delta.y * 0.0025), event.clientX, event.clientY);
                return;
            }
            const horizontal = event.shiftKey && !delta.x ? delta.y : delta.x;
            const vertical = event.shiftKey && !delta.x ? 0 : delta.y;
            scPanBy(side, horizontal, vertical);
        }

        function scOnViewerPanStart(side, event) {
            if (event.button !== 0) return;
            const pane = scPaneRefs[side];
            if (!pane) return;
            scMeasurePane(side);
            scPanState = {side, x: event.clientX, y: event.clientY, pointerId: event.pointerId};
            pane.classList.add('is-panning');
            // Захват указателя удерживает сдвиг, когда курсор ушёл за панель.
            // На неактивном pointerId он бросает NotFoundError — сдвиг работает
            // и без захвата, поэтому отказ гасим.
            try { pane.setPointerCapture(event.pointerId); } catch (error) { /* без захвата */ }
            event.preventDefault();
        }

        function scOnViewerPanMove(event) {
            if (!scPanState || event.pointerId !== scPanState.pointerId) return;
            scPanBy(scPanState.side, scPanState.x - event.clientX, scPanState.y - event.clientY);
            scPanState.x = event.clientX;
            scPanState.y = event.clientY;
        }

        function scOnViewerPanEnd(event) {
            if (!scPanState || event.pointerId !== scPanState.pointerId) return;
            const pane = scPaneRefs[scPanState.side];
            if (pane) {
                pane.classList.remove('is-panning');
                try { pane.releasePointerCapture(event.pointerId); } catch (error) { /* не был захвачен */ }
            }
            scPanState = null;
        }

        function scOnViewerDoubleClick(side, event) {
            scMeasurePane(side);
            scZoomAt(side, event.altKey ? 1 / 2 : 2, event.clientX, event.clientY);
        }

        function scZoomBy(multiplier) {
            if (scViewMode.value === 'continuous') {
                const side = scViewerEmpty.left && !scViewerEmpty.right ? 'right' : 'left';
                scContinuousZoomAt(side, multiplier, null, null);
                return;
            }
            scMeasurePane('left');
            scZoomAt('left', multiplier, null, null);
        }

        function scZoomFit() {
            if (scViewMode.value === 'continuous') {
                scContinuousZoom.value = 1;
                scZoomPercent.value = 100;
                nextTick(() => {
                    for (const side of ['left', 'right']) {
                        scScrollContinuousToPage(side, scCurrentPage[side]);
                        scScheduleContinuousTileRefresh(side, true);
                    }
                });
                return;
            }
            for (const side of ['left', 'right']) {
                scMeasurePane(side);
                const view = scViews[side];
                view.zoom = 1;
                view.cx = 0.5;
                view.cy = 0.5;
            }
            scZoomPercent.value = 100;
            scScheduleView();
        }

        // 1:1 — один пункт PDF на один CSS-пиксель левой панели. Для листа A1 в
        // узкой панели это уже сильное увеличение, поэтому опора — левая сторона.
        function scZoomActualSize() {
            if (scViewMode.value === 'continuous') {
                const side = scViewerEmpty.left && !scViewerEmpty.right ? 'right' : 'left';
                scContinuousZoomAt(side, 1 / scContinuousZoom.value, null, null);
                return;
            }
            scMeasurePane('left');
            const fit = scFitScale('left');
            if (!fit) return;
            scZoomAt('left', (1 / fit) / scViewFor('left').zoom, null, null);
        }

        function scSetPaneRef(side, element) {
            const previous = scPaneRefs[side];
            if (previous === element) return;
            if (previous && previous.__scDetachViewer) previous.__scDetachViewer();
            scPaneRefs[side] = element || null;
            if (!element) return;
            const onWheel = event => scOnViewerWheel(side, event);
            const onDown = event => scOnViewerPanStart(side, event);
            const onDouble = event => scOnViewerDoubleClick(side, event);
            // wheel вешаем вручную: нужен строго {passive: false}, иначе
            // preventDefault для Ctrl+колеса игнорируется и зумится страница.
            element.addEventListener('wheel', onWheel, {passive: false});
            element.addEventListener('pointerdown', onDown);
            element.addEventListener('pointermove', scOnViewerPanMove);
            element.addEventListener('pointerup', scOnViewerPanEnd);
            element.addEventListener('pointercancel', scOnViewerPanEnd);
            element.addEventListener('dblclick', onDouble);
            const observer = typeof ResizeObserver === 'function'
                ? new ResizeObserver(() => { scMeasurePane(side); scScheduleView(); })
                : null;
            if (observer) observer.observe(element);
            element.__scDetachViewer = () => {
                element.removeEventListener('wheel', onWheel);
                element.removeEventListener('pointerdown', onDown);
                element.removeEventListener('pointermove', scOnViewerPanMove);
                element.removeEventListener('pointerup', scOnViewerPanEnd);
                element.removeEventListener('pointercancel', scOnViewerPanEnd);
                element.removeEventListener('dblclick', onDouble);
                if (observer) observer.disconnect();
                delete element.__scDetachViewer;
            };
            scMeasurePane(side);
            scScheduleView();
        }

        function scSetStageRef(side, element) {
            scStageRefs[side] = element || null;
            if (element) scScheduleView();
        }

        function scContinuousEntries(side) {
            const pageKey = side === 'left' ? 'leftPage' : 'rightPage';
            const counterpartKey = side === 'left' ? 'rightPage' : 'leftPage';
            return scContinuousSlots.value.map(slot => ({
                key: slot.key,
                page: slot[pageKey] || null,
                counterpartPage: slot[counterpartKey] || null,
                placeholder: !slot[pageKey],
            }));
        }

        function scContinuousSlotForPage(side, page) {
            const pageKey = side === 'left' ? 'leftPage' : 'rightPage';
            return scContinuousSlots.value.find(slot =>
                Number(slot[pageKey]) === Number(page)
            ) || null;
        }

        function scContinuousCurrentIsPlaceholder(side) {
            const slot = scContinuousSlots.value.find(item =>
                item.key === scContinuousCurrentSlot[side]
            );
            const pageKey = side === 'left' ? 'leftPage' : 'rightPage';
            return Boolean(slot && !slot[pageKey]);
        }

        function scContinuousSlotActive(side, entry) {
            if (scContinuousCurrentSlot[side]) {
                return entry.key === scContinuousCurrentSlot[side];
            }
            return Boolean(entry.page && Number(entry.page) === Number(scCurrentPage[side]));
        }

        function scContinuousPageStyle(side, entry) {
            const page = typeof entry === 'object' ? entry.page : entry;
            const counterpartPage = typeof entry === 'object' ? entry.counterpartPage : null;
            const targetSide = side === 'left' ? 'right' : 'left';
            const dims = page
                ? scContinuousDims[side][page]
                : counterpartPage ? scContinuousDims[targetSide][counterpartPage] : null;
            const ratio = dims && dims.w && dims.h ? `${dims.w} / ${dims.h}` : '1.414 / 1';
            return {
                width: `${(scContinuousZoom.value * 100).toFixed(2)}%`,
                aspectRatio: ratio,
            };
        }

        function scContinuousAnchorAt(side, clientX = null, clientY = null) {
            const pane = scContinuousPaneRefs[side];
            if (!pane || scViewerEmpty[side]) return;
            const paneRect = pane.getBoundingClientRect();
            const viewportX = Math.min(
                pane.clientWidth,
                Math.max(0, clientX == null ? pane.clientWidth / 2 : clientX - paneRect.left),
            );
            const viewportY = Math.min(
                pane.clientHeight,
                Math.max(0, clientY == null ? pane.clientHeight / 2 : clientY - paneRect.top),
            );
            const contentX = pane.scrollLeft + viewportX;
            const contentY = pane.scrollTop + viewportY;
            let chosen = null;
            let nearestDistance = Infinity;
            for (const sheet of pane.querySelectorAll('[data-sc-slot]')) {
                const top = sheet.offsetTop;
                const bottom = top + sheet.offsetHeight;
                if (contentY >= top && contentY <= bottom) {
                    chosen = sheet;
                    break;
                }
                const distance = contentY < top ? top - contentY : contentY - bottom;
                if (distance < nearestDistance) {
                    nearestDistance = distance;
                    chosen = sheet;
                }
            }
            if (!chosen || !chosen.offsetWidth || !chosen.offsetHeight) return;
            return {
                slot: String(chosen.dataset.scSlot || ''),
                page: Number(chosen.dataset.scPage) || null,
                counterpartPage: Number(chosen.dataset.scCounterpartPage) || null,
                x: Math.min(1, Math.max(0, (contentX - chosen.offsetLeft) / chosen.offsetWidth)),
                y: Math.min(1, Math.max(0, (contentY - chosen.offsetTop) / chosen.offsetHeight)),
                viewportX: pane.clientWidth ? viewportX / pane.clientWidth : 0.5,
                viewportY: pane.clientHeight ? viewportY / pane.clientHeight : 0.5,
            };
        }

        function scSetContinuousPanePosition(side, scrollLeft, scrollTop) {
            const pane = scContinuousPaneRefs[side];
            if (!pane) return;
            pane.scrollLeft = Math.max(0, Number(scrollLeft) || 0);
            pane.scrollTop = Math.max(0, Number(scrollTop) || 0);
            scContinuousProgrammaticTarget[side] = {
                left: pane.scrollLeft,
                top: pane.scrollTop,
            };
            if (scContinuousProgrammaticTimer[side]) {
                clearTimeout(scContinuousProgrammaticTimer[side]);
            }
            scContinuousProgrammaticTimer[side] = setTimeout(() => {
                scContinuousProgrammaticTimer[side] = 0;
                scContinuousProgrammaticTarget[side] = null;
            }, 180);
            scScheduleContinuousTileRefresh(side);
        }

        function scConsumeContinuousProgrammaticScroll(side, pane) {
            const target = scContinuousProgrammaticTarget[side];
            if (!target) return false;
            const reached = Math.abs(pane.scrollLeft - target.left) < 2
                && Math.abs(pane.scrollTop - target.top) < 2;
            scContinuousProgrammaticTarget[side] = null;
            if (scContinuousProgrammaticTimer[side]) {
                clearTimeout(scContinuousProgrammaticTimer[side]);
                scContinuousProgrammaticTimer[side] = 0;
            }
            return reached;
        }

        function scSetContinuousAnchor(side, page, anchor) {
            const pane = scContinuousPaneRefs[side];
            if (!pane || scViewerEmpty[side] || !anchor) return;
            const sheet = anchor.slot
                ? pane.querySelector(`[data-sc-slot="${anchor.slot}"]`)
                : pane.querySelector(`[data-sc-page="${Number(page)}"]`);
            if (!sheet) return;
            const viewportX = (Number(anchor.viewportX) || 0) * pane.clientWidth;
            const viewportY = (Number(anchor.viewportY) || 0) * pane.clientHeight;
            scSetContinuousPanePosition(
                side,
                sheet.offsetLeft + Math.min(1, Math.max(0, Number(anchor.x) || 0)) * sheet.offsetWidth
                    - viewportX,
                sheet.offsetTop + Math.min(1, Math.max(0, Number(anchor.y) || 0)) * sheet.offsetHeight
                    - viewportY,
            );
        }

        function scScrollContinuousToPage(side, page) {
            const slot = scContinuousSlotForPage(side, page);
            if (slot) scContinuousCurrentSlot[side] = slot.key;
            scSetContinuousAnchor(side, page, {
                slot: slot && slot.key,
                x: 0.5, y: 0.5, viewportX: 0.5, viewportY: 0.5,
            });
        }

        function scScrollContinuousToSlot(side, slot) {
            scContinuousCurrentSlot[side] = slot;
            scSetContinuousAnchor(side, null, {
                slot, x: 0.5, y: 0.5, viewportX: 0.5, viewportY: 0.5,
            });
        }

        function scContinuousCounterpart(side, page) {
            const slot = scContinuousSlotForPage(side, page);
            if (slot) {
                return side === 'left' ? slot.rightPage : slot.leftPage;
            }
            const sourceKey = side === 'left' ? 'leftPages' : 'rightPages';
            const targetKey = side === 'left' ? 'rightPages' : 'leftPages';
            const row = scSheetMapRows.value.find(item =>
                (item[sourceKey] || []).map(Number).includes(Number(page))
            );
            if (row) {
                const mapped = (row[targetKey] || []).map(Number).filter(Boolean);
                return mapped.length ? mapped[0] : null;
            }
            const targetSide = side === 'left' ? 'right' : 'left';
            const sourceCount = scPageCount(side);
            const targetCount = scPageCount(targetSide);
            if (!targetCount) return null;
            if (sourceCount <= 1) return 1;
            return Math.min(
                targetCount,
                Math.max(1, Math.round((Number(page) - 1) * (targetCount - 1) / (sourceCount - 1)) + 1),
            );
        }

        function scSetContinuousPageFromScroll(side, page) {
            const limited = Math.min(scPageCount(side) || 1, Math.max(1, Number(page) || 1));
            if (limited === Number(scCurrentPage[side])) return;
            scContinuousPageFromScroll[side] = limited;
            scCurrentPage[side] = limited;
        }

        function scUpdateContinuousPages(side, anchorOrPage) {
            const anchor = typeof anchorOrPage === 'object'
                ? anchorOrPage
                : {page: Number(anchorOrPage) || null};
            if (anchor.slot) scContinuousCurrentSlot[side] = anchor.slot;
            if (anchor.page) scSetContinuousPageFromScroll(side, anchor.page);
            scLinkEditorOpen.value = false;
            if (!scSyncView.value) return null;
            const targetSide = side === 'left' ? 'right' : 'left';
            const slot = anchor.slot
                ? scContinuousSlots.value.find(item => item.key === anchor.slot)
                : null;
            const targetPage = anchor.counterpartPage || (slot
                ? (targetSide === 'left' ? slot.leftPage : slot.rightPage)
                : scContinuousCounterpart(side, anchor.page));
            if (anchor.slot) scContinuousCurrentSlot[targetSide] = anchor.slot;
            if (!scPageCount(targetSide)) return null;
            if (scViewerEmpty[targetSide]) {
                scContinuousPageFromScroll[targetSide] = targetPage || scCurrentPage[targetSide];
            }
            scSetViewerEmpty(targetSide, false);
            if (targetPage) scSetContinuousPageFromScroll(targetSide, targetPage);
            return {side: targetSide, page: targetPage || null};
        }

        function scSyncContinuousAnchor(side, anchor) {
            if (!scSyncView.value || !anchor) return;
            const targetSide = side === 'left' ? 'right' : 'left';
            const targetPage = anchor.counterpartPage
                || (anchor.page ? scContinuousCounterpart(side, anchor.page) : null);
            if (!scPageCount(targetSide)) return;
            scSetViewerEmpty(targetSide, false);
            if (targetPage) scLoadContinuousWindow(targetSide, targetPage);
            nextTick(() => scSetContinuousAnchor(targetSide, targetPage, anchor));
        }

        function scContinuousZoomAt(side, multiplier, clientX, clientY) {
            const anchor = scContinuousAnchorAt(side, clientX, clientY);
            if (anchor) {
                const target = scUpdateContinuousPages(side, anchor);
                if (target && target.page) scLoadContinuousWindow(target.side, target.page);
            }
            scContinuousZoom.value = Math.min(
                SC_ZOOM_MAX,
                Math.max(0.5, scContinuousZoom.value * multiplier),
            );
            scZoomPercent.value = Math.round(scContinuousZoom.value * 100);
            nextTick(() => {
                if (anchor) {
                    scSetContinuousAnchor(side, anchor.page, anchor);
                    scSyncContinuousAnchor(side, anchor);
                }
                scScheduleContinuousTileRefresh('left');
                scScheduleContinuousTileRefresh('right');
            });
        }

        function scSetContinuousPaneRef(side, element) {
            const previous = scContinuousPaneRefs[side];
            if (previous === element) return;
            if (previous && previous.__scDetachContinuousViewer) {
                previous.__scDetachContinuousViewer();
            }
            scContinuousPaneRefs[side] = element || null;
            if (!element) return;
            const observer = typeof ResizeObserver === 'function'
                ? new ResizeObserver(() => scScheduleContinuousTileRefresh(side))
                : null;
            if (observer) observer.observe(element);
            element.__scDetachContinuousViewer = () => {
                if (observer) observer.disconnect();
                delete element.__scDetachContinuousViewer;
            };
            nextTick(() => scScrollContinuousToPage(side, scCurrentPage[side]));
        }

        function scSetViewMode(mode) {
            if (mode !== 'paged' && mode !== 'continuous') return;
            if (scViewMode.value === mode) return;
            scViewMode.value = mode;
            try { localStorage.setItem('stage-comparison:view-mode', mode); } catch (_) {}
            if (mode === 'continuous') {
                for (const side of ['left', 'right']) {
                    scContinuousPageFromScroll[side] = scCurrentPage[side];
                    if (scPageInfoRequest[side]) scPageInfoRequest[side].abort();
                    scPageInfoRequest[side] = null;
                    scPageLoading[side] = false;
                    scPageTiles[side] = [];
                    scSetViewerEmpty(side, scPageCount(side) <= 0);
                }
                scContinuousZoom.value = 1;
                scZoomPercent.value = 100;
                const selectedSlot = scContinuousSlotForPage('left', scCurrentPage.left)
                    || scContinuousSlotForPage('right', scCurrentPage.right);
                for (const side of ['left', 'right']) {
                    const page = selectedSlot
                        ? (side === 'left' ? selectedSlot.leftPage : selectedSlot.rightPage)
                        : scCurrentPage[side];
                    if (selectedSlot) scContinuousCurrentSlot[side] = selectedSlot.key;
                    if (!scViewerEmpty[side] && page) scLoadContinuousWindow(side, page);
                }
                nextTick(() => {
                    for (const side of ['left', 'right']) {
                        if (selectedSlot) scScrollContinuousToSlot(side, selectedSlot.key);
                        else scScrollContinuousToPage(side, scCurrentPage[side]);
                    }
                });
            } else {
                const selectedSlot = scContinuousSlots.value.find(slot =>
                    slot.key === scContinuousCurrentSlot.left
                    || slot.key === scContinuousCurrentSlot.right
                );
                for (const side of ['left', 'right']) {
                    for (const controller of scContinuousRequests[side].values()) controller.abort();
                    scContinuousRequests[side].clear();
                    scContinuousTiles[side] = {};
                    const page = selectedSlot
                        && (side === 'left' ? selectedSlot.leftPage : selectedSlot.rightPage);
                    if (selectedSlot) scSetViewerEmpty(side, !page);
                    if (page) scCurrentPage[side] = page;
                    scLoadPageRaster(side);
                }
                nextTick(scScheduleView);
            }
        }

        function scOnContinuousWheel(side, event) {
            if (!event.ctrlKey && !event.metaKey) return;
            event.preventDefault();
            scContinuousZoomAt(
                side,
                Math.exp(-scWheelPixels(event).y * 0.0025),
                event.clientX,
                event.clientY,
            );
        }

        function scOnContinuousPanStart(side, event) {
            if (event.button !== 0) return;
            const pane = scContinuousPaneRefs[side];
            if (!pane) return;
            const rect = pane.getBoundingClientRect();
            // Не перехватываем системные полосы прокрутки: их тоже можно
            // продолжать таскать обычным способом, даже когда включена «рука».
            if (event.clientX >= rect.left + pane.clientWidth
                || event.clientY >= rect.top + pane.clientHeight) return;
            scContinuousProgrammaticTarget[side] = null;
            scContinuousPanState = {
                side, x: event.clientX, y: event.clientY, pointerId: event.pointerId,
            };
            pane.classList.add('is-panning');
            try { pane.setPointerCapture(event.pointerId); } catch (error) { /* без захвата */ }
            event.preventDefault();
        }

        function scOnContinuousPanMove(event) {
            if (!scContinuousPanState || event.pointerId !== scContinuousPanState.pointerId) return;
            const pane = scContinuousPaneRefs[scContinuousPanState.side];
            if (!pane) return;
            pane.scrollLeft += scContinuousPanState.x - event.clientX;
            pane.scrollTop += scContinuousPanState.y - event.clientY;
            scContinuousPanState.x = event.clientX;
            scContinuousPanState.y = event.clientY;
            event.preventDefault();
        }

        function scOnContinuousPanEnd(event) {
            if (!scContinuousPanState || event.pointerId !== scContinuousPanState.pointerId) return;
            const pane = scContinuousPaneRefs[scContinuousPanState.side];
            if (pane) {
                pane.classList.remove('is-panning');
                try { pane.releasePointerCapture(event.pointerId); } catch (error) { /* не был захвачен */ }
            }
            scContinuousPanState = null;
        }

        function scOnContinuousDoubleClick(side, event) {
            scContinuousZoomAt(side, event.altKey ? 1 / 2 : 2, event.clientX, event.clientY);
        }

        function scOnContinuousScroll(side, event) {
            scScheduleContinuousTileRefresh(side);
            if (scContinuousScrollFrame[side]) return;
            const pane = event.currentTarget;
            scContinuousScrollFrame[side] = requestAnimationFrame(() => {
                scContinuousScrollFrame[side] = 0;
                if (scConsumeContinuousProgrammaticScroll(side, pane)) return;
                const anchor = scContinuousAnchorAt(side);
                if (!anchor) return;
                if (anchor.page) scLoadContinuousWindow(side, anchor.page);
                const target = scUpdateContinuousPages(side, anchor);
                if (target && target.page) scLoadContinuousWindow(target.side, target.page);
                scSyncContinuousAnchor(side, anchor);
            });
        }

        // ─── Просмотрщик: raster preview + тайлы высокого разрешения ──────
        async function scLoadContinuousPage(side, page) {
            const url = scPageInfoUrl(side, page);
            if (!url || scViewerEmpty[side] || scViewMode.value !== 'continuous') return;
            if (scContinuousPreview[side][page] && scContinuousDims[side][page]) return;
            if (scContinuousDims[side][page] && scContinuousSignatures[side][page]) {
                scContinuousLoading[side][page] = true;
                scContinuousError[side][page] = '';
                scContinuousPreview[side][page] = scPagePreviewUrl(
                    side,
                    page,
                    SC_CONTINUOUS_PREVIEW_WIDTH,
                    scContinuousSignatures[side][page],
                );
                return;
            }
            if (scContinuousRequests[side].has(page)) return;
            const controller = new AbortController();
            scContinuousRequests[side].set(page, controller);
            scContinuousLoading[side][page] = true;
            scContinuousError[side][page] = '';
            try {
                const response = await fetch(url, {signal: controller.signal});
                if (!response.ok) throw new Error('HTTP ' + response.status);
                const info = await response.json();
                if (scContinuousRequests[side].get(page) !== controller) return;
                const width = Number(info.width);
                const height = Number(info.height);
                if (!(width > 0 && height > 0)) throw new Error('Некорректный размер листа');
                // Загрузка точного формата листа меняет высоту всей ленты.
                // Сохраняем точку под центром окна, чтобы лента не уезжала и
                // обработчик scroll не принимал сдвиг разметки за жест пользователя.
                const anchors = {
                    left: scContinuousAnchorAt('left'),
                    right: scContinuousAnchorAt('right'),
                };
                scContinuousDims[side][page] = {w: width, h: height};
                scContinuousSignatures[side][page] = String(info.signature || '');
                scContinuousPreview[side][page] = scPagePreviewUrl(
                    side, page, SC_CONTINUOUS_PREVIEW_WIDTH, scContinuousSignatures[side][page],
                );
                nextTick(() => {
                    for (const anchorSide of ['left', 'right']) {
                        const anchor = anchors[anchorSide];
                        if (anchor) scSetContinuousAnchor(anchorSide, anchor.page, anchor);
                    }
                    scScheduleContinuousTileRefresh(side);
                });
            } catch (error) {
                if (controller.signal.aborted) return;
                scContinuousError[side][page] = 'Не удалось загрузить: ' + String(error.message || error);
                scContinuousLoading[side][page] = false;
            } finally {
                if (scContinuousRequests[side].get(page) === controller) {
                    scContinuousRequests[side].delete(page);
                }
            }
        }

        function scOnContinuousPreviewLoad(side, page) {
            scContinuousLoading[side][page] = false;
            scContinuousError[side][page] = '';
            scScheduleContinuousTileRefresh(side, true);
        }

        function scOnContinuousPreviewError(side, page) {
            scContinuousLoading[side][page] = false;
            scContinuousError[side][page] = 'Не удалось загрузить preview страницы';
        }

        function scRefreshContinuousTiles(side) {
            if (scViewMode.value !== 'continuous' || scViewerEmpty[side]) return;
            const pane = scContinuousPaneRefs[side];
            if (!pane) return;
            const visiblePages = new Set();
            const viewportLeft = pane.scrollLeft;
            const viewportTop = pane.scrollTop;
            const viewportRight = viewportLeft + pane.clientWidth;
            const viewportBottom = viewportTop + pane.clientHeight;
            const pixelRatio = Math.max(1, Number(window.devicePixelRatio) || 1);
            for (const sheet of pane.querySelectorAll('[data-sc-page]')) {
                const page = Number(sheet.dataset.scPage);
                const dims = scContinuousDims[side][page];
                const signature = scContinuousSignatures[side][page];
                if (!dims || !signature || !sheet.offsetWidth || !sheet.offsetHeight) continue;
                const sheetLeft = sheet.offsetLeft;
                const sheetTop = sheet.offsetTop;
                const sheetRight = sheetLeft + sheet.offsetWidth;
                const sheetBottom = sheetTop + sheet.offsetHeight;
                if (
                    sheetRight <= viewportLeft || sheetLeft >= viewportRight
                    || sheetBottom <= viewportTop || sheetTop >= viewportBottom
                ) continue;
                visiblePages.add(page);
                const cssScale = sheet.offsetWidth / dims.w;
                const requiredScale = cssScale * pixelRatio;
                const previewScale = SC_CONTINUOUS_PREVIEW_WIDTH / dims.w;
                if (requiredScale <= previewScale * 1.12) {
                    scContinuousTiles[side][page] = [];
                    continue;
                }
                const level = Math.min(
                    SC_TILE_MAX_LEVEL,
                    Math.max(0, Math.ceil(Math.log2(requiredScale))),
                );
                const span = SC_TILE_SIZE / (2 ** level);
                const visibleLeft = Math.max(0, (viewportLeft - sheetLeft) / cssScale);
                const visibleTop = Math.max(0, (viewportTop - sheetTop) / cssScale);
                const visibleRight = Math.min(dims.w, (viewportRight - sheetLeft) / cssScale);
                const visibleBottom = Math.min(dims.h, (viewportBottom - sheetTop) / cssScale);
                const firstX = Math.max(0, Math.floor(visibleLeft / span) - 1);
                const firstY = Math.max(0, Math.floor(visibleTop / span) - 1);
                const lastX = Math.min(Math.ceil(dims.w / span) - 1, Math.floor(visibleRight / span) + 1);
                const lastY = Math.min(Math.ceil(dims.h / span) - 1, Math.floor(visibleBottom / span) + 1);
                const tiles = [];
                for (let y = firstY; y <= lastY; y += 1) {
                    for (let x = firstX; x <= lastX; x += 1) {
                        const tileWidth = Math.min(span, dims.w - x * span);
                        const tileHeight = Math.min(span, dims.h - y * span);
                        tiles.push({
                            key: `${page}:${level}:${x}:${y}:${signature}`,
                            url: scPageTileUrl(side, page, level, x, y, signature),
                            left: x * span / dims.w * 100,
                            top: y * span / dims.h * 100,
                            width: tileWidth / dims.w * 100,
                            height: tileHeight / dims.h * 100,
                        });
                    }
                }
                const before = (scContinuousTiles[side][page] || []).map(tile => tile.key).join('|');
                const after = tiles.map(tile => tile.key).join('|');
                if (before !== after) scContinuousTiles[side][page] = tiles;
            }
            for (const page of Object.keys(scContinuousTiles[side]).map(Number)) {
                if (!visiblePages.has(page)) delete scContinuousTiles[side][page];
            }
        }

        function scScheduleContinuousTileRefresh(side, immediate = false) {
            if (scContinuousTileRefreshTimer[side]) {
                clearTimeout(scContinuousTileRefreshTimer[side]);
            }
            scContinuousTileRefreshTimer[side] = setTimeout(() => {
                scContinuousTileRefreshTimer[side] = 0;
                scRefreshContinuousTiles(side);
            }, immediate ? 0 : 90);
        }

        function scLoadContinuousWindow(side, centerPage) {
            if (scViewerEmpty[side] || scViewMode.value !== 'continuous') return;
            const count = scPageCount(side);
            const center = Math.min(count || 1, Math.max(1, Number(centerPage) || 1));
            const wanted = new Set();
            for (let page = Math.max(1, center - 2); page <= Math.min(count, center + 2); page += 1) {
                wanted.add(page);
            }
            for (const [page, controller] of scContinuousRequests[side].entries()) {
                if (!wanted.has(Number(page))) {
                    controller.abort();
                    scContinuousRequests[side].delete(page);
                }
            }
            for (const page of Object.keys(scContinuousPreview[side]).map(Number)) {
                if (!wanted.has(page)) delete scContinuousPreview[side][page];
            }
            for (const page of Object.keys(scContinuousTiles[side]).map(Number)) {
                if (!wanted.has(page)) delete scContinuousTiles[side][page];
            }
            for (const page of wanted) scLoadContinuousPage(side, page);
        }

        async function scLoadPageRaster(side) {
            const url = scViewerEmpty[side] ? '' : scPageInfoUrl(side);
            if (scPageInfoRequest[side]) scPageInfoRequest[side].abort();
            scPageTiles[side] = [];
            scPagePreview[side] = '';
            scPageSignatures[side] = '';
            if (!url) {
                scPageInfoRequest[side] = null;
                scPageLoading[side] = false;
                scPageError[side] = '';
                scPagePreview[side] = '';
                scPageSignatures[side] = '';
                scPageDims[side] = {w: 0, h: 0};
                scScheduleView();
                return;
            }
            const controller = new AbortController();
            scPageInfoRequest[side] = controller;
            scPageLoading[side] = true;
            scPageError[side] = '';
            try {
                const response = await fetch(url, {signal: controller.signal});
                if (!response.ok) throw new Error('HTTP ' + response.status);
                const info = await response.json();
                if (scPageInfoRequest[side] !== controller) return;
                const width = Number(info.width);
                const height = Number(info.height);
                if (!(width > 0 && height > 0)) throw new Error('Некорректный размер листа');
                scPageDims[side] = {w: width, h: height};
                scPageSignatures[side] = String(info.signature || '');
                scPagePreview[side] = scPagePreviewUrl(
                    side, Number(scCurrentPage[side]), SC_PREVIEW_WIDTH, scPageSignatures[side],
                );
                scMeasurePane(side);
                scScheduleView();
            } catch (error) {
                if (controller.signal.aborted) return;
                scPageError[side] = 'Не удалось загрузить страницу: ' + String(error.message || error);
                scPageLoading[side] = false;
                scPagePreview[side] = '';
                scPageSignatures[side] = '';
                scPageDims[side] = {w: 0, h: 0};
            } finally {
                if (scPageInfoRequest[side] === controller) {
                    scPageInfoRequest[side] = null;
                }
            }
        }

        function scOnPagePreviewLoad(side, event) {
            if (event.currentTarget.getAttribute('src') !== scPagePreview[side]) return;
            scPageLoading[side] = false;
            scPageError[side] = '';
            scScheduleTileRefresh(side, true);
        }

        function scOnPagePreviewError(side, event) {
            if (event.currentTarget.getAttribute('src') !== scPagePreview[side]) return;
            scPageLoading[side] = false;
            scPageError[side] = 'Не удалось загрузить preview страницы';
        }

        function scRefreshViewerSide(side) {
            if (currentView.value !== 'stage-comparison') return;
            if (scViewMode.value === 'continuous') {
                const pageFromScroll = Number(scContinuousPageFromScroll[side])
                    === Number(scCurrentPage[side]);
                scContinuousPageFromScroll[side] = null;
                if (scViewerEmpty[side]) {
                    scSetViewerEmpty(side, true);
                    return;
                }
                scLoadContinuousWindow(side, scCurrentPage[side]);
                if (!pageFromScroll) {
                    nextTick(() => scScrollContinuousToPage(side, scCurrentPage[side]));
                }
            } else {
                scLoadPageRaster(side);
            }
        }

        watch(
            () => [
                scActivePair.value && scActivePair.value.id,
                scCurrentPage.left, scViewerEmpty.left, scViewMode.value,
            ],
            () => scRefreshViewerSide('left'),
        );
        watch(
            () => [
                scActivePair.value && scActivePair.value.id,
                scCurrentPage.right, scViewerEmpty.right, scViewMode.value,
            ],
            () => scRefreshViewerSide('right'),
        );
        watch(scSyncView, linked => {
            if (scViewMode.value === 'continuous') {
                if (linked) {
                    const anchor = scContinuousAnchorAt('left') || scContinuousAnchorAt('right');
                    if (anchor) {
                        const sourceSide = scViewerEmpty.left ? 'right' : 'left';
                        const target = scUpdateContinuousPages(sourceSide, anchor);
                        if (target && target.page) scLoadContinuousWindow(target.side, target.page);
                        scSyncContinuousAnchor(sourceSide, anchor);
                    }
                }
                return;
            }
            // Отвязали — правая панель продолжает с того же места, а не прыгает
            // к своему старому виду; связали — подхватывает вид левой.
            if (linked) scViews.left = {...scViews.left};
            else scViews.right = {...scViews.left};
            scScheduleView();
        });

        watch(currentObjectId, () => {
            if (currentView.value === 'stage-comparison') scLoadObjects();
        });

        return {
            // Theme
            theme, toggleTheme,
            // Пользователи (сотрудники) — подпись решений + админ-гейт графика
            usersList, usersCurrentId,
            usersAuthEnabled, usersLoggedInUsername, usersLoggedInMatched,
            loadUsers, currentUserName,
            // График производства работ (API /api/schedule + dev mock fallback)
            schedMode, schedAnchor, schedPopover, schedFiltersOpen,
            schedHiddenEngineers, schedPlanEdit, schedEngineers,
            schedEvents, schedVisibleEngineers, schedDays, schedPeriodLabel,
            schedCell, schedSetMode, schedPrev, schedNext, schedToday,
            schedToggleCell, schedIsPopover, schedPopoverStyle, schedClosePopover,
            schedToggleEngineer, schedIsEngineerHidden,
            schedTogglePlanEdit, schedPlanFor, schedFactFor, schedPctClass, schedPctColor,
            schedStats, schedTotals, schedInitials, schedStatusFor, schedAvatarStyle,
            schedLoading, schedError, schedUsingMock, schedNoticeKind, schedNoticeText, schedLoad,
            // План работ (backend work_plans.json, admin-gated)
            schedPlanMap, schedPlanDraft, schedPlanSaving, schedPlanMsg, schedIsAdmin,
            schedDraftFor, schedSetPlanDraft, schedCancelPlanEdit, schedLoadPlans, schedSavePlans,
            // Расход подписки по инженерам
            subSpendOpen, subSpendLoading, subSpendData, subSpendWeekText, subSpendResetText,
            subSpendLoad, subSpendColor, subSpendInitials, subSpendDayLabel, subSpendTok,
            // State
            currentView, currentProject, currentProjectId, visiblePipelineSummary,
            distributed,
            projectLoading, projects, loading, isProjectView,
            findingsData, filterSeverity, filterSearch, severityOptions,
            // KB-Validation
            kbValidationAvailable, kbValidationLoading, findingKbDecision, findingKbLabel, findingKbClass, findingKbTooltip,
            evidenceValidationAvailable, evidenceValidationLoading, evidenceValidationRunning,
            findingEvDecision, findingEvLabel, findingEvClass, findingEvPathLabel, findingEvTooltip, runEvidenceValidation,
            // Inline Critic v2 (experimental, в обычной таблице)
            findingsCv2Available, findingsCv2Warning, findingsCv2Loading,
            cv2ShowHidden, cv2DisplayFilter, cv2DebugVisible,
            cv2HiddenCount, findingCv2Score, findingCv2Label, findingCv2Class, findingCv2Tooltip,
            cv2SortDir, toggleCv2Sort,
            CV2_DISPLAY_BUCKETS,
            findingBlockMap, findingBlockInfo, expandedFindingId, cleanSubProblem,
            toggleFindingBlocks, getFindingBlocks, getFindingTextEvidence, findingTextEvidence, navigateToBlock, blockBackRoute, goBackFromBlock,
            // Blocks (OCR)
            blocksProjectId, blockPages, blockCropErrors, blockTotalExpected,
            selectedBlockPage, selectedBlock,
            blockAnalysis, selectedBlockAnalysis, currentPageBlocks, allBlocksList,
            emptyBlocksList, noFindingsBlocksList, skippedBlocksList,
            blockStatus, blockHasNoVectorGraph, blockParentId, blockMergedBadge, blockOriginalLabel,
            currentBlocksList, currentBlockIndex, navigateBlock,
            blockHasAnalysis, blockFindingsCount, blockMaxSeverity,
            openBlock, loadBlocks, blockToFindings, getBlockFindings,
            blockImageContainer, blockImageStyle, onBlockZoomWheel, onBlockPanStart, resetBlockZoom, onBlockImageLoad,
            blockNatW, blockNatH,
            textlayerHighlightsShadow, showTextlayerHighlightsShadow, currentBlockTextlayerHighlights,
            toggleTextlayerHighlightsShadow,
            // «txt»-режим: текст блока, уходящий в нейронку
            showBlockLlmText, blockLlmText, blockLlmTextLoading, blockLlmTextError, toggleBlockLlmText,
            // полное профильное Markdown-описание блока (shadow-профиль)
            blockProfiledMarkdownHtml, renderMarkdownSafe, blockMdMode, setBlockMdMode,
            showBlockRegions, blockRegionRects, blockTextGroupRects, toggleBlockRegions, blockImageSrc, blockImgUrl,
            logProjectId, logEntries, logAutoScroll, logContainer, logLoading,
            logTruncatedNotice,
            logSections, isLogSectionCollapsed, toggleLogSection,
            currentFindingStage,
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
            navigate, refreshProjects, stepClass, combinedCriticStatus, sevClass, sevIcon, findingDetectorBadge,
            findingsBreakdownTitle, optimizationBreakdownTitle,
            debounceSearch, clearLog, copyLog,
            // Prompts
            promptsProjectId, templates, promptsLoading,
            activePromptTab, promptsDiscipline,
            disciplines, showDisciplineDropdown, currentDiscipline,
            loadTemplates, loadPromptDisciplines,
            switchDiscipline, saveTemplate, highlightPlaceholders, syncScroll,
            // Audit actions
            auditRunning, allRunning,
            startPrepare, startMainAudit,
            startAudit, startStandardAudit, startProAudit,
            startNormVerify, startOptimization, cancelAudit, generateExcel,
            startAllProjects, resumePipeline, resumeToQueue, resumeInfo,
            startFromStage, canStartFrom, pipelineToStage,
            activeStageAlgorithmKey, activeStageAlgorithm,
            openStageAlgorithm, closeStageAlgorithm,
            retryStage, retryDialog, retryStageToQueue,
            canRetryStage,
            skipStage, cleanProject,
            // Batch selection
            selectedProjects, selectAllChecked, selectedCount,
            batchRunning, batchQueue,
            showBatchModal, batchMode, batchScope, batchModalCount, batchAllMode,
            // Edit projects (смена раздела / скрытие)
            showEditProjectsModal, editProjectsNewSection, editProjectsLoading,
            editProjectsSelected, openEditProjectsModal,
            applyNewSectionToSelected, hideSelectedFromUI, deleteSelectedProjects,
            // Edit projects — merge as version of existing (per-row)
            editProjectsMergeMap, editProjectsMergeReadyCount,
            mergeTargetsFor, mergeNextLabelFor, mergeTargetNameFor,
            applyMergeAllAsVersion,
            // Pause
            showPauseModal, isPaused, pauseMode, anyRunning,
            pausePipeline, resumePipelineGlobal,
            // Model config
            showModelConfig, stageModelConfig, availableModels, visibleStageModels, stageLabels,
            stageModelSaveError,
            stageModelRestrictions, stageModelHints, isModelAllowed,
            isBaseStageModelChecked, isCodexStageChecked, isCodexStageAllowed,
            selectBaseStageModel, toggleStageCodex,
            modelPresets, activePreset, activePresetHint, isCustomStageConfig, applyPreset,
            stageBatchModes, isFindingsOnlyMode,
            loadStageModels, saveStageModels, openModelConfig, saveAndStartAudit,
            startAuditDirect,
            modelConfigPendingProjectId,
            toggleProjectSelection, toggleSelectAll, isProjectSelected,
            isSectionSelected, toggleSectionSelection,
            toggleUnanalyzedSelection, isUnanalyzedSelected,
            expertUncheckedCount, uniqueProjectCount,
            sectionUnreviewedCount, isSectionUnreviewedSelected, toggleSectionUnreviewedSelection,
            sectionExcelLoading, exportSectionExcel,
            openBatchModal, confirmBatchAction, startBatchAction, cancelBatch, addToBatch,
            batchActionLabel, queueItemTiming,
            // Queue management
            queueAddMode, queueAddAction, queueAddSelected, queueDragIdx, queueDragOverIdx,
            refreshBatchQueue, removeFromQueue, updateQueueItemAction, reorderQueue,
            visibleQueueItems, finishedQueueCount, hideFinishedQueueItems,
            clearQueueHistory, resumeBatchQueue,
            onQueueDragStart, onQueueDragOver, onQueueDragEnd,
            toggleQueueAddProject, confirmQueueAdd, startQueueFromView,
            queueAvailableProjects,
            // Add project
            showAddProject, addProjectStep, unregisteredFolders, addProjectLoading,
            openAddModal, goToAddSection, addSection,
            newSectionName, newSectionCode, newSectionColor,
            scanFolders, scanExternalFolder, registerProject, registerAllProjects, closeAddProject,
            externalPath, projectSource,
            // Upload folder from computer
            uploadObjectId, uploadDiscipline, uploadProjectName, uploadScan,
            uploadScanError, uploadScanWarnings, uploadError, uploadLoading, uploadResult,
            canSubmitUpload, fmtSize, goToUploadFolder, resetUploadFolder,
            onUploadFolderSelected, submitUploadFolder, openUploadedProject,
            uploadMode, uploadPrecheck, uploadPrecheckLoading, uploadOverrideWarning,
            uploadCandidates, uploadBatchResult, uploadBatchProgress,
            setUploadMode, runSinglePrecheck, openProjectById, onMultiFolderSelected,
            recheckAllCandidates, candUploadable, candStatusLabel, candCount,
            selectedCandidateCount, submitMultiUpload,
            uploadDetectedDiscipline, uploadDisciplineSource, uploadAddMode,
            uploadTargetProjectId, uploadTargetOptions, versionLabelForTarget,
            disciplineSourceLabel, recheckCandidate, candTargetOptions, candVersionLabel,
            onUploadDisciplineChange,
            // Add project — version-of-existing mode
            onCandidatePrimaryAction, registerProjectAsVersion,
            candidateTargetOptions, candidateTargetName, candidateNextVersionLabel,
            normalizeProjectName,
            // Objects
            objectsList, currentObjectId, showObjectPicker, showAddObjectModal, newObjectName,
            loadObjects, switchObject, addNewObject,
            toggleHeaderPopover, closeHeaderPopovers,
            // Dashboard stats
            auditedProjectsCount, totalFindings, totalBySeverity, sevPercent,
            sectionFindingsCount, sectionStatsMap, sectionStatsTotals, filteredSectionProjects,
            sectionHasUnanalyzed,
            // Disciplines
            supportedDisciplines, getDisciplineColor, disciplineLabel, disciplineBadgeStyle,
            objectName, projectsBySection, collapsedSections, toggleSection,
            sidebarSectionsOpen, sidebarFilterSection, toggleSectionsNav,
            allSectionsCollapsed, toggleAllSections,
            showEditSection, editSectionCode, editSectionName, editSectionColor,
            openEditSection, saveEditSection, deleteSection,
            dragSectionCode, dragOverCode,
            onSectionDragStart, onSectionDragOver, onSectionDragEnd,
            // Project groups
            projectGroups, groupedSectionProjects,
            // Сводная оптимизация раздела
            sectionOptimizationLoading, sectionOptimizationError,
            sectionOptimizationData, sectionOptimizationLoadedKey, sectionOptimizationMeta,
            sectionOptimizationTab, sectionOptimizationSearch, sectionOptimizationProjectFilter,
            sectionOptimizationCollapsedProjects, sectionOptimizationExpandedSignals, sectionOptimizationProjectOptions,
            sectionOptimizationPipeline, sectionOptimizationPipelineRunning,
            sectionOptimizationPipelineActionLoading, sectionOptimizationPipelineActionError,
            sectionOptimizationReplicationActionLoading, sectionOptimizationReplications,
            sectionOptimizationAgentAvailable,
            sectionOptimizationGraphicsAgentAvailable,
            sectionOptimizationReplicationPendingCount, sectionOptimizationReplicationProgressLabel,
            sectionOptimizationFilteredSpecifications, sectionOptimizationSpecificationGroups,
            sectionOptimizationFilteredAccepted,
            sectionOptimizationFilteredSignals, loadSectionOptimization,
            sectionOptimizationPipelineStage, sectionOptimizationPipelineStatusLabel,
            sectionOptimizationPipelineStageMarker,
            runSectionOptimizationPipeline, requestSectionOptimizationGraphicsPlan,
            startAllSectionOptimizationReplications, sectionOptimizationReplicationFor,
            retrySectionOptimizationGraphics, sectionOptimizationReplicationCanRetryGraphics,
            sectionOptimizationReplicationStatusLabel, sectionOptimizationAgentVerdictLabel,
            sectionOptimizationGraphicsConclusionLabel, openSectionOptimizationGraphicsBlock,
            setSectionOptimizationTab, navigateToSectionOptimization, sectionOptimizationProjectLabel,
            sectionOptimizationSpecificationTypeMark,
            sectionOptimizationSpecificationSectionTitle, sectionOptimizationSpecificationSectionKey,
            sectionOptimizationSpecificationProjectKey,
            isSectionOptimizationProjectCollapsed, toggleSectionOptimizationProject,
            expandAllSectionOptimizationProjects, collapseAllSectionOptimizationProjects,
            sectionOptimizationSignalAcceptedItems, sectionOptimizationSignalSpecificationItems,
            sectionOptimizationSignalHasAcceptedSources, sectionOptimizationSignalTypeLabel,
            sectionOptimizationSignalGraphicsLabel, isSectionOptimizationSignalExpanded,
            toggleSectionOptimizationSignal, formatSectionOptimizationQuantity,
            currentSectionProjectsList, prevProject, nextProject,
            showCreateGroup, newGroupName, editingGroupId, editingGroupName,
            createGroup, renameGroup, startRenameGroup, deleteProjectGroup,
            dragProjectId, dragGroupId, dragOverGroupId,
            onProjectDragStart, onGroupDragOver, onGroupDragLeave, onProjectDropOnGroup,
            onGroupHeaderDragStart, onGroupHeaderDragEnd,
            // Model switcher
            // Paid cost
            paidCost, showPaidCost, fetchPaidCost, resetPaidCost,
            formatCostShort, formatSignedCost, formatPaidMonth,
            paidApiStatus, paidEvents, paidBlockedEvents,
            fetchPaidApiStatus, fetchPaidEvents, fetchPaidBlockedEvents,
            // Paid-cost daily dashboard
            paidDailyDays, paidDailyTotals, paidDailyPeriod,
            paidDailySelectedDate, paidDailySelectedDay, paidDailyExpanded,
            fetchPaidCostDaily, setPaidDailyPeriod, selectPaidDailyDate,
            formatCostFull, entriesSortedDesc,
            // Usage (global dashboard)
            globalUsage, showUsageDetails, sonnetPercent,
            accountInfo, showAccountInfo, fetchAccountInfo,
            accountSwitching, accountAuthUrl, switchAccount,
            formatTokens, formatCost, formatDurationSec, refreshGlobalUsage, resetSessionCounter, clearUsageCounter,
            editUsagePercent, resetUsageOffsets,
            usageCounters,
            // Usage (per-project)
            projectUsage, currentProjectUsage, usagePaidCost, usageFreeCost, pipelineTotalDuration, stageTokens, stageTokensFormatted, stageModel, stageDurationForProject, formatDuration,
            // Pipeline summary
            // Optimization
            optimizationData, optimizationLoading, optimizationFilter, optimizationSearch,
            optBlockMap, optBlockInfo, expandedOptId,
            toggleOptBlocks, getOptBlocks,
            filteredOptimization, optimizationTypeLabels, optimizationTypeColors,
            optTypeLabel, optTypeColor, optTypeClass, optIcon, optNormBadge, findingNormBadge, loadOptimization,
            // Document viewer
            documentProjectId, documentPages, documentCurrentPage, documentPageData, documentLoading,
            loadDocument, loadDocumentPage, docPrevPage, docNextPage, renderMarkdown,
            // Discussions
            discussionItems, discussionTab, discussionModel, discussionModels,
            activeDiscussion, activeDiscussionItem, activeDiscussionBlocks, showDiscussionBlocks, discussionMessages, discussionLoading, discussionSending,
            discussionCost, discussionContextTokens, chatInput, chatMessagesContainer,
            revisionData, revisionLoading,
            activeDiscussionItems, rejectedDiscussionItems, discussionSeverityCounts, discussionOptTypeCounts,
            loadDiscussionModels, loadDiscussionItems, switchDiscussionTab,
            openDiscussion, closeDiscussion, sendDiscussionMessage, downloadAuditPackage, auditPackageLoading,
            downloadBatchAuditPackages, batchPackageLoading,
            cropBatchBlocks, batchCropLoading, batchCropProgress,
            prepareQueue, clearPrepareQueue, formatEta, fetchPrepareQueue,
            preparePause, prepareResume, prepareCancel,
            chatAttachedImage, handleChatFileSelect, handleChatPaste,
            resolvedFindingsCount, allDiscussionsResolved, resolvedFindingsLoading, downloadResolvedFindings,
            editingMessageIdx, editingMessageText,
            startEditMessage, cancelEditMessage, submitEditMessage,
            resolveDiscussion, requestRevision, applyRevision, rejectRevision, formatRevisionField, formatRevisionValue,
            discussionStatusIcon, formatCostUSD, renderDiscussionContent, onChatClick, autoResizeChatInput,
            // Computed
            filteredFindings, sortedFindings, sortedOptimization,
            // Pagination
            PAGE_SIZE, findingsPage, optimizationPage, discussionPage,
            paginatedFindings, findingsTotalPages,
            paginatedOptimization, optimizationTotalPages,
            paginatedDiscussion, discussionTotalPages,
            // Expert Review
            expertReviewMode, expertDecisions, expertReviewSaving,
            toggleExpertReview, loadExpertDecisions, setExpertDecision, setExpertReason, submitExpertReview,
            getExpertDecision, getExpertReason, isCarriedOver, carriedFromVersion, expertReviewSummary,
            // Knowledge Base
            kbTab, kbEntries, kbStats, kbLoading, kbSearch, kbSectionFilter,
            kbObjectFilter, onKbObjectChange, openKBItem,
            kbItemType, kbTypeMenuOpen, toggleKbTypeMenu, setKbItemType,
            kbUploadLoading,
            loadKnowledgeBase, loadKBStats, switchKBTab,
            missingNorms, missingNormsStats, missingNormsFilter,
            loadMissingNorms, markNormAdded, dismissNorm, restoreNorm,
            confirmCustomer, unconfirmCustomer, revokeKBDecision,
            uploadDecisionsExcel, uploadAndApplyDecisions,
            // Critic v2 UI Triage View (experimental, offline)
            cv2Export, cv2LoadError, cv2ActiveTab, cv2Filter,
            cv2OnFileSelected, cv2ResetFilters, cv2ParseExport, cv2ScoreBucket,
            cv2ItemMatchesFilter, cv2HasHumanDecisions,
            cv2FilterOptions, cv2ItemsByTab, cv2VisibleCountByTab,
            cv2EffectiveTab, cv2DebugCounts,
            // Critic v2 UI Feedback (frontend-only)
            cv2Feedback, cv2FeedbackSummary,
            cv2EnsureFeedback, cv2HasFeedback,
            cv2SetTriageCorrect, cv2SetPreferredTab,
            cv2SetPriority, cv2SetReviewerNote,
            cv2QuickRoute, cv2QuickUnsure,
            cv2BuildFeedbackExport, cv2ExportFeedback,
            // Critic v2 UI Feedback Import
            cv2ImportStatus, cv2ImportMessage, cv2AvailableFeedbackFiles,
            cv2ImportFeedbackFromObject, cv2OnFeedbackFileSelected,
            cv2RefreshFeedbackFiles, cv2ImportFeedbackFromServer,
            // Critic v2 project-scoped view (read-only)
            cv2ProjLoading, cv2ProjLoadError, cv2ProjHint,
            cv2ProjDisagreementsMode, cv2ProjSubMode, cv2SetProjSubMode,
            cv2LoadProject,
            // Critic v2 auto-load feedback for project view
            cv2AutoLoadedFeedbackFile, cv2AutoLoadedFeedbackMeta,
            cv2AvailableFeedbackMatches, cv2AutoLoadStatus, cv2AutoLoadMessage,
            cv2SwitchFeedbackFile,
            // Critic v2 assisted_round1 review-package (read-only)
            cv2AssistedItems, cv2AssistedAllTotal, cv2AssistedMatchedTotal,
            cv2AssistedLoading, cv2AssistedError,
            cv2AssistedFilterOnly, cv2AssistedById,
            cv2AssistedStatusOf, cv2AssistedReport, cv2AssistedFocusFinding,
            cv2AssistedStatusByFid, cv2AssignmentTab, cv2RoutingTab,
            // Critic v2 — Russian labels (UI-only, backend tokens unchanged)
            cv2Label, cv2HumanizeExplanation,
            // Critic v2 — alignment vs expert_review (UI-only)
            cv2AlignmentOf, cv2IsDisagreement, cv2AlignmentSummary,
            // ─── Версионность проекта ───
            activeVersionId, projectVersions, projectVersionsLoading,
            versionFiles, versionUploading, versionUploadError,
            showVersionPdf, versionPdfUrl, activeVersionLabel, toggleVersionPdf,
            renameEditing, renameValue, renameError, renameBusy, renameInput,
            startRename, cancelRename, submitRename,
            loadProjectVersions, loadVersionFiles, selectVersion, deleteVersion,
            uploadFilesToVersion,
            handleUploadInput, handleUploadInputReplace,
            activeVersionEntry, canStartAuditNow, versionBadgeFor,
            // ─── Migrated findings (контроль ранее согласованных замечаний) ───
            migratedFindingsReport, migratedFindingsReportLoading,
            migratedFindingsError,
            loadMigratedFindingsReport,
            migratedStatusLabel, migratedStatusTone, findingMigratedBadge,
            findingExtRegBadge,
            // Documentation comparison shell
            scTab, scObjects, scObjectsLoading, scObjectsError, scSelectedObject,
            scStageInfo, scStageUploadBusy, scStageUploadIsBusy, scStageUploadError,
            scOpenStageFolderDialog, scUploadStageFolder,
            scStageFolderDialogOpen, scStageFolderDialogStage, scStageFolderDialogName,
            scStageFolderCandidates, scStageFolderSelectedCount, scStageFolderSelectableCount,
            scStageFolderDoneCount, scStageFolderErrorCount,
            scStageBatchCurrent, scStageBatchTotal,
            scStageCandidateStatusText, scToggleAllStageCandidates,
            scCloseStageFolderDialog, scSubmitSelectedStageProjects,
            scSessionLoading, scSession, scSessionError,
            scDocumentsLeft, scDocumentsRight, scDocumentOrder, scPairRows,
            scDraggingDocument, scDocumentDragOver,
            scDraggingPairRow, scPairRowDragOver,
            scPendingPairSelection, scConfirmedDocumentPairs,
            scPairingSaving, scPairingMatching, scPairingDirty, scPairingSaved,
            scPairingSaveError, scPairingSaveMessage,
            scPairs, scSelectedPdf,
            scActivePair, scPairData, scPairLoading,
            scProcessing, scProcessingError,
            scMatchState, scMatchSummary, scSuggestions, scLeftSuggestion,
            scSheetLinks, scSheetMapRows, scCurrentExplicitLinks, scCurrentRightPages, scCurrentStatus,
            scRightOptions, scUnlinkedLeftPages, scLinkSaving,
            scSheetMapCollapsed, scToggleSheetMap,
            scLinkEditorOpen, scLinkEditorMode, scLinkEditorRightPage,
            scLinkEditorLeftPages, scLinkEditorRightPages,
            scLoadObjects, scOpenSelectedPair, scOpenPair, scOpenPairRow,
            scProcessCurrentSelection, scProcessPairRow,
            scStartDocumentDrag, scDragDocumentOver,
            scDropDocument, scFinishDocumentDrag, scIsDraggingDocument,
            scStartPairRowDrag, scDragPairRowOver, scDropPairRow,
            scFinishPairRowDrag, scIsDraggingPairRow,
            scSelectPairDocument, scIsPairDocumentPending, scIsPairRowConfirmed,
            scPairRowStatus, scPairRowBusy, scPairRowError,
            scAutoMatchDocumentProjects, scSaveDocumentPairing,
            scSheetIndexEntryFor, scSheetIndexTitle, scReasonLabel,
            // Панель миниатюр листов (только отображение и переход)
            scThumbsOpen, scToggleThumbs, scThumbRows, scThumbUrl,
            scThumbRowActive, scThumbRowTitle, scOpenThumbRow,
            scThumbPage, scThumbSelected, scThumbDraggable, scThumbSelection,
            scThumbDragOver, scSelectThumbCell, scClearThumbSelection,
            scThumbDragStart, scThumbDragOverCell, scThumbDragEnd, scThumbDropOn,
            scThumbCellClick,
            scSheetMapSideLabel, scSheetMapStatus, scSheetMapRowActive,
            scSheetMapOptions, scSheetMapSelectionValue, scApplySheetMapSelection,
            scOpenSheetMapRow, scOpenSheetMapEditor, scCloseSheetMapEditor,
            scFocusLeftPage, scSwitchRightPage, scOpenLinkEditor, scChooseUnmatchedRight,
            scAcceptSuggestion, scApplyLinkEditor, scDeleteSheetMapRow, scDeleteCurrentLink,
            scCurrentPage, scViewerEmpty, scPageCount, scChangePage,
            scSyncView, scViewMode, scSetViewMode,
            scZoomPercent, scZoomBy, scZoomFit, scZoomActualSize,
            scPagePreview, scPageTiles, scPageLoading, scPageError,
            scOnPagePreviewLoad, scOnPagePreviewError, scSetStageRef,
            scSetPaneRef,
            scContinuousPreview, scContinuousLoading, scContinuousError, scContinuousTiles,
            scContinuousEntries, scContinuousPageStyle, scContinuousSlotActive,
            scContinuousCurrentIsPlaceholder, scSetContinuousPaneRef,
            scOnContinuousPreviewLoad, scOnContinuousPreviewError,
            scOnContinuousWheel, scOnContinuousScroll,
            scOnContinuousPanStart, scOnContinuousPanMove, scOnContinuousPanEnd,
            scOnContinuousDoubleClick,
        };
    }
});

window.DistributedFeature.registerComponents(app);
app.mount('#app');
