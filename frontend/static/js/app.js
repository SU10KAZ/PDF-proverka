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

        // ─── Stage-comparison dev-tools: legacy debug-subtabs + session-wide
        // Opus batch button. Скрыты от обычного пользователя. Включить:
        //   localStorage.setItem('sc_dev', '1')   — постоянно
        //   ?scdev=1 в URL                        — на текущую сессию
        //   window.scEnableDev() / scDisableDev() — из консоли.
        function _readScDevFlag() {
            try {
                if (typeof window === 'undefined') return false;
                const url = new URL(window.location.href);
                if (url.searchParams.get('scdev') === '1') return true;
                if (window.localStorage && window.localStorage.getItem('sc_dev') === '1') return true;
            } catch (_) { /* SSR / sandboxed iframe */ }
            return false;
        }
        const scDevTools = ref(_readScDevFlag());
        if (typeof window !== 'undefined') {
            window.scEnableDev = function () {
                try { window.localStorage.setItem('sc_dev', '1'); } catch (_) {}
                scDevTools.value = true;
                console.info('[sc] dev-tools enabled (localStorage.sc_dev=1)');
            };
            window.scDisableDev = function () {
                try { window.localStorage.removeItem('sc_dev'); } catch (_) {}
                scDevTools.value = false;
                console.info('[sc] dev-tools disabled');
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
        const paidCost = ref({ display_usd: 0, total_lifetime_usd: 0 });
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

        // ─── LM Studio remote management ───────────────────────────────
        function _lmsSetMsg(kind, text) {
            lmsMessage.value = { kind, text };
            setTimeout(() => { if (lmsMessage.value && lmsMessage.value.text === text) lmsMessage.value = null; }, 6000);
        }

        async function lmsRefresh() {
            lmsLoading.value = true;
            try {
                const [r1, r2] = await Promise.all([
                    fetch('/api/lms/models/loaded'),
                    fetch('/api/lms/models/all'),
                ]);
                if (!r1.ok || !r2.ok) {
                    const err = await r1.json().catch(() => ({}));
                    _lmsSetMsg('error', err.detail || 'Ошибка получения списка моделей');
                    return;
                }
                const d1 = await r1.json();
                const d2 = await r2.json();
                lmsLoaded.value = d1.loaded || [];
                lmsAll.value = d2.models || [];
                // Заполнить дефолты context_length для каждой модели
                for (const m of lmsAll.value) {
                    if (lmsLoadCtx.value[m.id] === undefined) {
                        lmsLoadCtx.value[m.id] = m.loaded_context_length || 16384;
                    }
                }
            } catch (e) {
                _lmsSetMsg('error', `Сеть: ${e.message}`);
            } finally {
                lmsLoading.value = false;
            }
        }

        async function lmsLoad(modelId) {
            const ctx = parseInt(lmsLoadCtx.value[modelId] || 16384, 10);
            if (!ctx || ctx < 256) { _lmsSetMsg('error', 'Некорректный context_length'); return; }
            lmsLoading.value = true;
            try {
                const r = await fetch('/api/lms/models/load', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({model_key: modelId, context_length: ctx}),
                });
                const data = await r.json();
                if (!r.ok) { _lmsSetMsg('error', data.detail || `HTTP ${r.status}`); return; }
                _lmsSetMsg('ok', `Загружено: ${data.identifier} (ctx=${data.context_length})`);
                await lmsRefresh();
            } catch (e) {
                _lmsSetMsg('error', `Сеть: ${e.message}`);
            } finally { lmsLoading.value = false; }
        }

        async function lmsUnload(identifier) {
            if (!confirm(`Выгрузить ${identifier}?`)) return;
            lmsLoading.value = true;
            try {
                const r = await fetch('/api/lms/models/unload', {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({identifier}),
                });
                const data = await r.json();
                if (!r.ok) { _lmsSetMsg('error', data.detail || `HTTP ${r.status}`); return; }
                _lmsSetMsg('ok', `Выгружено: ${identifier}`);
                await lmsRefresh();
            } catch (e) {
                _lmsSetMsg('error', `Сеть: ${e.message}`);
            } finally { lmsLoading.value = false; }
        }

        async function lmsReload(modelId) {
            const ctx = parseInt(lmsLoadCtx.value[modelId] || 16384, 10);
            if (!ctx || ctx < 256) { _lmsSetMsg('error', 'Некорректный context_length'); return; }
            if (!confirm(`Выгрузить ВСЕ instance'ы ${modelId} и загрузить заново с context=${ctx}?`)) return;
            lmsLoading.value = true;
            try {
                const r = await fetch(`/api/lms/models/${encodeURIComponent(modelId)}/reload?context_length=${ctx}`, {method: 'POST'});
                const data = await r.json();
                if (!r.ok) { _lmsSetMsg('error', data.detail || `HTTP ${r.status}`); return; }
                _lmsSetMsg('ok', `Reload: выгружено ${data.unloaded}, загружено ${data.identifier} (ctx=${data.context_length})`);
                await lmsRefresh();
            } catch (e) {
                _lmsSetMsg('error', `Сеть: ${e.message}`);
            } finally { lmsLoading.value = false; }
        }

        function lmsApplyPresetCtx(ctx) {
            // Применить пресет ко всем моделям в форме (заполнит inputs)
            for (const m of lmsAll.value) {
                lmsLoadCtx.value[m.id] = ctx;
            }
            _lmsSetMsg('ok', `Применён context=${ctx} ко всем формам. Нажмите «Загрузить» у нужной модели.`);
        }

        async function lmsCheckHealth() {
            try {
                const r = await fetch('/api/lms/health');
                if (!r.ok) {
                    lmsHealth.value = null;
                    return;
                }
                lmsHealth.value = await r.json();
                lmsHealthCheckedAt.value = Date.now();
            } catch (e) {
                lmsHealth.value = null;
            }
        }

        function startLmsHealthPolling() {
            if (lmsHealthTimer) return;
            lmsCheckHealth();  // immediate
            lmsHealthTimer = setInterval(lmsCheckHealth, 30000);  // every 30s
        }

        function stopLmsHealthPolling() {
            if (lmsHealthTimer) { clearInterval(lmsHealthTimer); lmsHealthTimer = null; }
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

            if (hash === '/knowledge-base') {
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
            } else if (hash === '/model-control') {
                currentView.value = 'model-control';
                connectGlobalWS();
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
                scLoadSavedConfig();
                // Каноничная конфигурация v2: пробуем автоматически открыть
                // ранее сохранённую рабочую сессию. История сессий обычному
                // пользователю не показывается.
                scLoadCanonicalConfig();
                scTryOpenCanonical();
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
                sidebarSectionsOpen.value = true;
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
        // ─── LM Studio remote management ───
        const lmsLoaded = ref([]);       // загруженные сейчас instance'ы
        const lmsAll = ref([]);          // все скачанные модели
        const lmsLoadCtx = ref({});      // {model_id: ctx_value} — для inputs в таблице
        const lmsLoading = ref(false);
        const lmsMessage = ref(null);    // {kind: 'error'|'ok', text}
        const lmsHealth = ref(null);     // {health: {alive, latency_ms, ...}, inflight: {...}, loaded_count}
        const lmsHealthCheckedAt = ref(null);  // timestamp ms последней проверки
        let lmsHealthTimer = null;       // setInterval handle для periodic poll

        const lmsHealthStatus = computed(() => {
            const h = lmsHealth.value;
            if (!h) return 'unknown';
            if (!h.loaded_count || h.loaded_count === 0) return 'unloaded';
            if (h.health && h.health.alive === false) return 'error';
            if (h.inflight && h.inflight.total_active > 0) return 'busy';
            if (h.health && h.health.alive === true) return 'ok';
            return 'unknown';
        });

        const lmsHealthTitle = computed(() => {
            const h = lmsHealth.value;
            if (!h) return 'LM Studio: проверка...';
            const status = lmsHealthStatus.value;
            if (status === 'ok') {
                return `LM Studio: работает (${h.health.latency_ms} ms)`;
            } else if (status === 'busy') {
                return `LM Studio: занята (${h.inflight.total_active} активных запросов)`;
            } else if (status === 'unloaded') {
                return 'LM Studio: нет загруженной модели';
            } else if (status === 'error') {
                return `LM Studio: ${h.health.error || 'не отвечает'}`;
            }
            return 'LM Studio: статус неизвестен';
        });
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

            // Пред-предупреждение: если у части source есть готовые findings —
            // спрашиваем явное подтверждение, что их потеря приемлема.
            const withArtifacts = pairs.filter(p => _projectHasAuditArtifacts(p.source));
            let discardAllowed = false;
            if (!confirm(
                `Привязать ${pairs.length} проект(ов) как версии существующих?\n` +
                `Исходные карточки будут удалены. V1 каждого target не изменится.`
            )) return;
            if (withArtifacts.length > 0) {
                const names = withArtifacts.map(p => p.source.name || p.source.project_id).join('\n  • ');
                if (!confirm(
                    `У ${withArtifacts.length} source-проект(ов) есть готовые результаты аудита:\n  • ${names}\n\n`
                    + `Они БУДУТ ПОТЕРЯНЫ при слиянии (новая версия начинается с нуля).\n`
                    + `Продолжить?`
                )) return;
                discardAllowed = true;
            }

            editProjectsLoading.value = true;
            const errors = [];
            const okList = [];
            try {
                for (const { source, targetId } of pairs) {
                    try {
                        let res = await _mergeOnePair(source, targetId, { discardSourceOutput: discardAllowed });
                        // Backend может сам определить артефакты, которых фронт не увидел —
                        // в этом случае возвращает 409 с code=source_output_not_empty.
                        if (!res.ok && res.status === 409
                            && res.detail && typeof res.detail === 'object'
                            && res.detail.code === 'source_output_not_empty') {
                            if (!confirm(
                                `${source.name || source.project_id}: ${res.detail.message}\n\nПродолжить и потерять _output?`
                            )) {
                                throw new Error('Отменено пользователем');
                            }
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

        // Выделить все НЕпроанализированные (findings_count == 0) проекты раздела,
        // добавляя их к текущему выделению.
        function selectUnanalyzedInSection(sectionCode) {
            const pids = projects.value
                .filter(p => (p.section || 'OTHER') === sectionCode && !(p.findings_count > 0))
                .map(p => p.project_id);
            const s = new Set(selectedProjects.value);
            for (const id of pids) s.add(id);
            selectedProjects.value = s;
            selectAllChecked.value = s.size === projects.value.length && s.size > 0;
        }

        // Проект «не проверен» (по последней загруженной версии), если у него
        // есть аудит (замечания или оптимизации), но эксперт НЕ довёл оценку
        // до конца — статус != 'complete' (нет отметок ИЛИ частично).
        // Проекты без аудита не считаются — для них есть «Выделить необработанные».
        function isProjectUnreviewed(p) {
            const hasAudit = (p.findings_count || 0) > 0 || (p.optimization_count || 0) > 0;
            return hasAudit && p.expert_review_status !== 'complete';
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

        // Сводка по разделу для «Главной». Определения совпадают с галочками
        // на карточке проекта в разделе:
        //   checked  — эксперт отработал проект ПОЛНОСТЬЮ (обе галочки:
        //              замечания + оптимизации) → expert_review_status==='complete';
        //   waiting  — нет ни одной отметки эксперта → expert_review_status пуст;
        //   (частично отработанные, expert_review_status==='partial', не попадают
        //    ни в checked, ни в waiting — у них одна галочка/точка).
        //   total    — общее число проектов раздела;
        //   findings — суммарно замечаний.
        // Ключуется тем же группированием, что projectsBySection, чтобы цифры
        // совпадали с items.length и с карточками раздела.
        const sectionStatsMap = computed(() => {
            const m = {};
            for (const [code, items] of projectsBySection.value) {
                let checked = 0, waiting = 0, findings = 0;
                for (const p of items) {
                    const rs = p.expert_review_status;
                    if (rs === 'complete') checked++;
                    else if (!rs) waiting++;
                    findings += (p.findings_count || 0);
                }
                m[code] = { total: items.length, checked, waiting, findings };
            }
            return m;
        });

        // Итого по всем разделам — сумма каждого числового столбца для
        // строки «Итого» внизу таблицы «Разделы проекта».
        const sectionStatsTotals = computed(() => {
            const t = { checked: 0, waiting: 0, total: 0, findings: 0 };
            for (const code in sectionStatsMap.value) {
                const s = sectionStatsMap.value[code];
                t.checked += s.checked;
                t.waiting += s.waiting;
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
        // критерий тот же, что у selectUnanalyzedInSection: findings_count == 0.
        const sectionHasUnanalyzed = computed(() => {
            const sec = sidebarFilterSection.value;
            if (!sec || sec === '__all__') return false;
            return projects.value.some(
                p => (p.section || 'OTHER') === sec && !(p.findings_count > 0)
            );
        });

        // Число «не проверенных» проектов текущего раздела — для надписи
        // «Не проверено (N)» и её скрытия, когда все проекты проверены.
        const sectionUnreviewedCount = computed(() => {
            const sec = sidebarFilterSection.value;
            if (!sec || sec === '__all__') return 0;
            return sectionUnreviewedPids(sec).length;
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
                return [{ id: '__ungrouped__', name: '', order: 0, project_ids: [], projects: sectionProjects, isVirtual: true, noHeader: true }];
            }

            const assignedIds = new Set(groups.flatMap(g => g.project_ids || []));
            const result = groups.map(g => ({
                ...g,
                projects: (g.project_ids || []).map(id => sectionProjects.find(p => p.project_id === id)).filter(Boolean),
                isVirtual: false,
            }));

            const ungrouped = sectionProjects.filter(p => !assignedIds.has(p.project_id));
            if (ungrouped.length > 0) {
                result.push({ id: '__ungrouped__', name: 'Без группы', order: 99999, project_ids: [], projects: ungrouped, isVirtual: true });
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
            return `с пятницы ${day}.${m} ${d.week_start_time || '19:00'} (сброс лимитов)`;
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
        // Закрываем inline-match popover при клике вне него (любой клик
        // на document'е, кроме самого popover'а — оба внутренних обработчика
        // используют @click.stop, чтобы не триггериться сами на себя).
        function _scInlineMatchOutsideClick(ev) {
            if (!scInlineMatchPairId.value) return;
            // Если клик пришёл внутри popover или по имени правого PDF —
            // сам popover закроет/перезапустит. Сюда долетают только клики
            // ВНЕ popover'а.
            scCloseInlineMatch();
        }
        function _stageAlgorithmKeydown(ev) {
            if (ev.key === 'Escape' && activeStageAlgorithmKey.value) {
                closeStageAlgorithm();
            }
        }
        onMounted(() => {
            window.addEventListener('hashchange', handleRoute);
            window.addEventListener('click', _scInlineMatchOutsideClick);
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
            window.removeEventListener('click', _scInlineMatchOutsideClick);
            window.removeEventListener('keydown', _stageAlgorithmKeydown);
            window.removeEventListener('click', closeHeaderPopovers);
            stopPolling();
            if (usagePollTimer) { clearInterval(usagePollTimer); usagePollTimer = null; }
            stopLmsHealthPolling();
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

        // ──────────────────────────────────────────────────────────────────
        // Stage Comparison (раздел «Сравнение стадий») — v2 (slot-based)
        // ──────────────────────────────────────────────────────────────────
        const scTab            = ref('upload');     // upload | links | diffs
        const scDiffSubtab     = ref('unified');    // unified (primary) | text (debug) | graphic (debug)
        // ─── Вкладка «Расхождения» построена на V2-алгоритме (pair-scoped) ───
        // scV2View: 'v2' (основной и единственный пользовательский режим —
        // список текущей PDF-пары + ручная верификация) | 'current'
        // (классический unified-список, оставлен ТОЛЬКО для отладки за
        // scDevTools; переключатель [Расхождения][V2] из UI убран).
        const scV2View         = ref('v2');
        const scV2Data         = ref(null);          // {session_id, pair_id, summary, items}
        const scV2Loading      = ref(false);
        const scV2Error        = ref('');
        const scV2SaveBusy      = ref(false);
        const scV2Selected     = reactive({});       // {[change_id]: true}
        const scV2Filters      = reactive({
            severity: '', source_layer: '', quality_label: '',
            review_status: '', cost_impact: '', impact_class: '', search: '',
        });
        // Исключённые (админ/оформление/шум) изменения в V2-ведомости по
        // умолчанию не показываются: бэкенд при include_excluded=false отдаёт
        // только инженерно значимые строки. Кнопка «Показать формальные»
        // выставляет этот флаг → запрос идёт с include_excluded=true.
        const scV2ShowFormal   = ref(false);
        // Опции ручного статуса (значение + человекочитаемая метка). Покрывают
        // все действия инженера: подтвердить/отклонить/уточнить/стоимость/маршрут.
        const scV2StatusOptions = [
            { v: 'not_reviewed',        l: '— не проверено' },
            { v: 'confirmed',           l: '✓ Подтверждено' },
            { v: 'rejected',            l: '✗ Отклонено' },
            { v: 'needs_clarification', l: '? Требует уточнения' },
            { v: 'cost_impact',         l: '₽ Влияет на стоимость' },
            { v: 'no_cost_impact',      l: 'Не влияет на стоимость' },
            { v: 'send_to_designer',    l: '→ Проектировщику' },
            { v: 'send_to_estimate',    l: '→ В сметный отдел' },
        ];
        const scStageAPath     = ref('');
        const scStageBPath     = ref('');
        const scScanning       = ref(false);
        // Канонический «сохранённый config»: одной кнопкой подставляется в форму.
        // {saved, stage_a_path, stage_b_path, object_label, stage_a_label, stage_b_label, saved_at, note}
        const scSavedConfig    = ref(null);
        const scSavedConfigSaving = ref(false);
        const scSavedConfigMsg = ref('');
        // Каноничная конфигурация (новая модель v2): объекту соответствует одна
        // активная конфигурация (canonical_session_id + pairs + режимы).
        // {saved, canonical_session_id, canonical_session_available, config_hash,
        //  pairs:[{pair_id, left_filename, right_filename, disabled, analysis_mode, order, ...}],
        //  updated_at, ...}
        const scCanonicalConfig = ref(null);
        const scCanonicalStale  = ref(false);  // true если saved hash != current hash
        // Drag-and-drop reorder для таблицы пар.
        // scPairDragFromIdx — индекс перетаскиваемой строки в scPairs;
        // scPairDragOverIdx — индекс цели hover'а; используется для visual feedback.
        const scPairDragFromIdx = ref(-1);
        const scPairDragOverIdx = ref(-1);
        const scPairOrderSaving = ref(false);
        const scPairOrderError  = ref('');
        // Автообъекты (Объект → stage_A / stage_B) ─────────────────────
        const scObjects        = ref([]);    // [{id, name, root_path, stages, default_stage_a, default_stage_b}]
        const scObjectsRoots   = ref([]);
        const scObjectsLoading = ref(false);
        const scObjectsError   = ref('');
        const scSelectedObjectId = ref('');
        const scSelectedStageA = ref('');    // имя выбранной stage_*, не путь
        const scSelectedStageB = ref('');
        const scLinking        = ref(false);
        const scError          = ref('');
        const scWarnings       = ref([]);
        const scSession        = ref(null);          // {id, pairs, ...}
        const scSessions       = ref([]);            // history list
        const scSessionsListOpen = ref(false);
        // Автоподгрузка ранее сохранённой сессии для выбранных stage_a/stage_b
        // (срабатывает на обновлении страницы и при смене объекта/стадии).
        const scAutoLoadInfo   = ref(null);          // {session_id, created_at, pairs_total, pairs_matched} | null
        const scAutoLoading    = ref(false);
        const scActivePair     = ref(null);          // pair-summary object
        const scPairData       = ref(null);          // full pair view with blocks
        const scCurrentPage    = ref(1);             // legacy, не используется в slot-based viewer
        const scCanvasRefs     = reactive({});       // legacy (single img ref)
        const scCanvasNat      = reactive({left: null, right: null});
        // Slot-based refs
        const scPaneRefs       = reactive({});       // {left,right: scrollable container}
        const scSlotRefs       = reactive({});       // 'left:1' | 'right:1' → DOM-node
        // Per-slot content heights (left/right img clientHeight). Используется чтобы
        // обоим панелям дать одинаковую высоту слота — иначе левая и правая стороны
        // расходятся по скроллу когда страницы разной длины.
        const scSlotHeights    = reactive({});       // {[slotId]: {left?: number, right?: number}}
        const scSelectedLeft   = ref(null);
        const scSelectedRight  = ref(null);
        const scSelectedSlotLeft  = ref(null);
        const scSelectedSlotRight = ref(null);
        // Поповер «упавшие блоки» на цифре «упало» в таблице пар.
        const scFailedPopoverPairId = ref(null);   // id пары с открытым поповером (null = закрыт)
        const scFailedBlocks        = ref([]);
        const scFailedBlocksLoading = ref(false);
        const scFailedBlocksError   = ref('');
        // Pair config template (Save button + auto-apply banner)
        const scTemplateSaving       = ref(false);
        const scTemplateLastSaveMsg  = ref('');   // короткое подтверждение «сохранён шаблон …»
        const scTemplateError        = ref('');
        // Семантический LLM-анализ текста (Claude Sonnet через Claude Code) — session-only.
        // Per-pair результат остаётся доступен по GET-endpoint'у (для отладки), но в UI
        // мы агрегируем все пары в один плоский список (см. scTextLLMFlat ниже).
        const scTextLLMDiff    = ref(null);     // legacy: используется кодом, который инспектирует текущую пару (blocks-view)
        const scTextLLMConfig  = ref(null);     // {enabled, provider, model, available, reason}
        // Batch / session preflight + job (единственный путь запуска)
        const scTextLLMBatchPreflight  = ref(null);   // агрегированный preflight по сессии
        const scTextLLMBatchLoading    = ref(false);
        const scTextLLMBatchOpen       = ref(false);
        const scTextLLMBatchError      = ref('');
        const scTextLLMBatchForce      = ref(false);
        const scTextLLMBatchJob        = ref(null);   // {id, status, items, progress, current_pair_id}
        const scTextLLMBatchPolling    = ref(false);
        // Session-level flat aggregation (GET .../text-llm-diff-flat)
        const scTextLLMFlat            = ref(null);   // {session_id, summary, items}
        const scTextLLMFlatLoading     = ref(false);
        const scTextLLMFlatError       = ref('');
        // Filters для plotting flat list
        const scTextFlatFilterPair     = ref('');
        const scTextFlatFilterType     = ref('');
        const scTextFlatFilterCategory = ref('');
        const scTextFlatFilterSeverity = ref('');
        const scTextFlatFilterHumanReview = ref(false);
        const scTextFlatSearch         = ref('');
        const scGraphicSummary = ref(null);
        const scGraphicPreview = ref(null);
        const scGraphicDiffRunning = ref(false);
        // MD enrichment (Qwen image descriptions for enriched MD)
        const scMdEnrichmentSummary     = ref(null);     // {pair_id, left:{...}, right:{...}}
        const scMdEnrichmentLoading     = ref(false);
        const scMdEnrichmentRunning     = ref(false);
        const scMdEnrichmentError       = ref('');
        const scMdEnrichmentConfirmOpen = ref(false);
        // Background job (Qwen run_model=true): UI больше не вызывает sync endpoint
        // напрямую, чтобы не упираться в HTTP 524 от ngrok/Cloudflare.
        const scMdEnrichmentJob         = ref(null);     // {id,status,items[],progress,...}
        const scMdEnrichmentJobPolling  = ref(false);
        const scMdEnrichmentJobTimedOut = ref(false);    // true если стартовый POST упал по таймауту/524 — job мог продолжаться
        // Переключатель «PDF ↔ MD» в двухпанельном вьювере: показывает
        // left_enriched.md / right_enriched.md вместо рендеренных страниц PDF.
        const scShowMd                  = ref(false);
        const scMdView                  = reactive({left: null, right: null}); // {side,filename,exists,content,char_count}
        const scMdViewLoading           = ref(false);
        const scMdViewError             = ref('');
        const scMdRenderMode            = ref('html');   // 'html' (отрендеренный markdown) | 'highlight' (подсветка строк)
        const scMdPaneRefs              = reactive({});   // {left,right: scrollable MD container}
        // ── Stage 1: «Распознать графику» (session-level Qwen enrichment job) ──
        // Используется отдельно от per-pair job (scMdEnrichmentJob): запускается
        // одной кнопкой на этапе «1. Загрузка документации», обрабатывает все
        // PDF-пары сессии по очереди, не параллелит Qwen-запросы. Прогресс
        // отображается агрегированно: total/done/partial/error по парам,
        // image-блоки done/total, текущая пара/сторона/блок, cache_hits.
        const scRecogJob              = ref(null);   // {id, aggregate:{...}, items, ...}
        const scRecogPolling          = ref(false);
        const scRecogStarting         = ref(false);
        const scRecogError            = ref('');
        const scRecogStartedAtClient  = ref(null);  // fallback elapsed_sec до прихода aggregate
        // Per-pair analysis mode: 'block_links' (default) | 'concept_no_block_links'.
        // Если concept_no_block_links — unified pipeline сравнивает enriched MD
        // целиком, не требуя связей блоков (см. backend/store.py).
        const scAnalysisMode            = ref('block_links');
        const scAnalysisModeSaving      = ref(false);
        const scAnalysisModeError       = ref('');
        // ── Unified analysis (Qwen enrichment → Opus comparison) ──────────
        // Это primary UX «Сравнение стадий»: одна кнопка «Проанализировать и
        // сравнить» вместо отдельных text-llm + md-enrichment + graphic-diff.
        const scUnifiedConfig           = ref(null);     // {enabled, provider, model, available, reason}
        const scUnifiedPairStatus       = ref(null);     // {pair_id, enrichment:{}, comparison:{}}
        const scUnifiedPairLoading      = ref(false);
        const scUnifiedFlat             = ref(null);     // {session_id, summary, items}
        const scUnifiedFlatLoading      = ref(false);
        const scUnifiedFlatError        = ref('');
        // По умолчанию вкладка «Расхождения» показывает findings ТОЛЬКО активной
        // PDF-пары. Пользователь явно переключает на «все пары» через toggle —
        // это защищает от ситуации, когда переход «Связь блоков» → «Расхождения»
        // показывает stale aggregate findings другой пары. См. unified-diff-flat
        // backend filter (?pair_id=).
        const scUnifiedShowAllPairs     = ref(false);
        const scUnifiedFlatScopePairId  = ref(null);     // pair_id, по которому реально загружен текущий scUnifiedFlat
        const scUnifiedPreflight        = ref(null);     // pair- или session-level
        const scUnifiedPreflightScope   = ref('pair');   // 'pair' | 'session'
        const scUnifiedPreflightOpen    = ref(false);
        const scUnifiedPreflightLoading = ref(false);
        const scUnifiedPreflightError   = ref('');
        const scUnifiedForceEnrichment  = ref(false);
        const scUnifiedForceCompare     = ref(false);
        const scUnifiedRunning          = ref(false);    // single-pair run в полёте
        const scUnifiedJob              = ref(null);     // {id, status, items, progress}
        const scUnifiedJobPolling       = ref(false);
        const scUnifiedError            = ref('');
        // ── Opus session batch (этап «1. Загрузка документации») ─────────
        // Запускается кнопкой «🔍 Проанализировать и сравнить» на upload tab.
        // Не запускает Qwen. Пропускает too_large/not_ready/уже done пары.
        // Активный job восстанавливается через /unified-analysis-jobs/active.
        const scOpusJob               = ref(null);   // {id, status, items, aggregate, ...}
        const scOpusPolling           = ref(false);
        const scOpusStarting          = ref(false);
        const scOpusError             = ref('');
        const scOpusPreflight         = ref(null);   // {total_pairs, will_run, skip_*, items}
        const scOpusPreflightLoading  = ref(false);
        const scOpusStartedAtClient   = ref(null);   // fallback elapsed
        // Per-pair Opus fallback (evidence_first_s2_fallback) для too_large пар.
        // Запускается кликом по бейджу «⚠ файл большой» в колонке «Сравнение».
        // Трекается отдельно от session job, чтобы не затирать бейджи остальных
        // пар (single-pair job содержит только одну пару).
        const scOpusFallbackByPair    = ref({});     // pairId → последний job item
        const scOpusFallbackPolling   = ref({});     // pairId → bool (идёт poll)
        const scOpusFallbackStarting  = ref({});     // pairId → bool (создаём job)
        // Персистентные статусы сравнения с диска (comparison_result.json по
        // парам). Источник истины для колонки «Сравнение» — чтобы она не
        // зависела от того, какой unified-job сейчас «активен» (одно-парный
        // fallback/retry раньше затеняли полный результат сессии → «—»).
        // pairId → {status, changes_count, strategy, via_fallback}.
        const scPairCompareStatus     = ref({});
        // Filters для unified flat-таблицы
        const scUnifiedFilterPair        = ref('');
        const scUnifiedFilterSourceLayer = ref('');
        const scUnifiedFilterType        = ref('');
        const scUnifiedFilterCategory    = ref('');
        const scUnifiedFilterSeverity    = ref('');
        const scUnifiedFilterHumanReview = ref(false);
        const scUnifiedSearch            = ref('');
        // Sort state for unified table (excel-style view).
        // Field: '' (default order) | 'no' | 'sheet' | 'impact'.
        // Dir: 'asc' | 'desc'. Default order = stable global numbering by original position.
        const scUnifiedSortField         = ref('');
        const scUnifiedSortDir           = ref('asc');

        // Grouped-режим вкладки «Расхождения» удалён по запросу: вкладка всегда
        // показывает плоский список всех расхождений (unified-diff-flat).
        // Экспертная оценка расхождений: ключ хранения — стабильный raw `id`
        // (chg_…/uf_…). Решения по группам агрегируются из source_finding_ids.
        const scExpertReviewMode      = ref(false);
        const scExpertDecisions       = ref({});   // {raw_id: {decision, rejection_reason, needs_review, conflict, transferred}}
        const scExpertReviewSaving    = ref(false);
        const scExpertReviewLoaded    = ref(false);
        // Перенос решений из «Расхождений» в V2 (на всю сессию, с участием Claude).
        const scV2TransferBusy        = ref(false);
        // Per-pair статус разметки для колонки «Проверено экспертом» на этапе
        // «1. Загрузка документации»: {pair_id: {total, decided, fully_verified}}.
        const scExpertPerPair         = ref({});
        // Sync scroll/zoom (sync zoom toggle убран — масштаб всегда общий)
        const scZoom           = ref(1.0);
        const scSyncScroll     = ref(true);
        const scIsSyncing      = { value: false };  // mutex (не ref — внутренний)
        // Виртуализация: какие slot'ы реально отрендерены
        const scVisibleSlot    = ref(1);            // slot, ближе всего к viewport (для виртуализации; берётся с левой панели)
        const scVisibleSlotLeft  = ref(1);          // slot в центре левой панели
        const scVisibleSlotRight = ref(1);          // slot в центре правой панели
        const scRenderBufferBefore = 3;
        const scRenderBufferAfter  = 5;
        // Alignment (внутренний page_alignment продолжает использоваться для синхронизации страниц,
        // пустых листов, перестановки страниц, auto-link и graphic-summary; пользовательский UI
        // «Карта листов» удалён, остались только per-pane icon-кнопки)
        const scAlignment      = ref(null);     // {items: [...], left_page_count, right_page_count}
        const scAlignmentActionRunning = ref(false);  // защита от двойного клика на ⊕/↑/↓
        const scAlignmentActionError   = ref('');     // последняя ошибка действия (если есть)
        // Ручное сопоставление PDF
        const scUnmatched      = ref({left_unmatched: [], right_unmatched: [], left_all: [], right_all: []});
        const scMatchPairDialogOpen = ref(false);
        const scMatchPairTargetPair = ref(null);
        const scMatchPairChoiceRight = ref('');
        const scMatchPairError = ref('');
        const scMatchPairSaving = ref(false);
        // Inline-сопоставление: клик по имени правого PDF → выпадающий
        // список вместо отдельной кнопки «Сопоставить».
        const scInlineMatchPairId = ref('');
        const scInlineMatchChoice = ref('');
        const scInlineMatchFilter = ref('');
        const scInlineMatchSaving = ref(false);
        const scInlineMatchError  = ref('');
        // Массовое подтверждение всех «возможных» сопоставлений.
        const scConfirmAllRunning = ref(false);
        const scConfirmAllError   = ref('');
        const scCreatePairDialogOpen = ref(false);
        const scCreatePairLeft = ref('');
        const scCreatePairRight = ref('');
        const scCreatePairError = ref('');
        const scCreatePairSaving = ref(false);
        // ── Report tab (read-only сводка согласованных расхождений) ──────
        // Вкладка «4. Отчёт»: собирает по всем парам проектов только те
        // расхождения, что эксперт согласовал на этапе «3. Расхождения».
        // Ничего не верифицируется здесь — только просмотр + один XLSX-экспорт.
        const scReportLoading        = ref(false);
        const scReportError          = ref('');
        const scReportExpandedPairs  = ref(new Set());   // pair_id, развёрнутые в аккордеоне
        const scReportPairItems      = reactive({});     // pair_id -> [согласованные расхождения]
        const scReportPairLoadingMap = reactive({});     // pair_id -> bool (идёт загрузка пары)
        // Фоновая предзагрузка всех пар после открытия вкладки.
        const scReportPrefetching    = ref(false);
        const scReportPrefetchDone   = ref(0);
        const scReportPrefetchTotal  = ref(0);
        let   _scReportPrefetchGen   = 0;                // токен отмены устаревшей предзагрузки
        // Сверка V1↔V2: pid -> число УНИКАЛЬНЫХ согласованных находок (после
        // схлопывания chg_X и его V2-двойника v2_<sha1(pid::chg_X)>). Считается
        // при открытии вкладки из scExpertDecisions, до ленивой загрузки пар,
        // чтобы бейджи сразу показывали дедуплицированное число.
        const scReportReconciledCounts = ref({});

        const scPairs = computed(() => scSession.value ? (scSession.value.pairs || []) : []);

        // ── Qwen→Opus pipeline (per-pair processing, decoupled lanes) ─────────
        const scQOSelected = reactive({});            // pairId → bool
        const scQOJob = ref(null);                    // active pipeline job state
        const scQOConfirm = ref(null);                // preflight payload for confirm modal
        const scQORunning = ref(false);               // start request in flight / job running
        const scQOPreflighting = ref(false);          // preflight request in flight → button feedback
        const scQOClearBeforeRun = ref(false);        // confirm modal: clear findings+review before run (legacy compat)
        // Режим запуска из диалога «Обработать выбранные»:
        //   'normal'                 — Qwen → Opus (как раньше);
        //   'clear_and_run'          — clear-analysis, затем Qwen → Opus;
        //   'opus_only'              — только Opus по готовым enriched MD (без Qwen);
        //   'clear_result_opus_only' — очистить comparison_result, затем только Opus.
        const scQOMode = ref('normal');
        const scQOClearing = ref(false);              // clear-analysis request in flight
        let scQOPollTimer = null;
        const scQOClock = ref(Date.now());            // 1s tick → live elapsed timers
        let scQOClockTimer = null;
        const scQOActiveRecog = ref(null);            // live md-enrichment aggregate of the running Qwen pair
        // Latest persisted Qwen/Opus timing per pair (from qopipe job files).
        // Survives F5: in-memory scQOJob теряется после refresh, а колонки
        // 🟦/🟪 должны показывать времена завершённого прогона. Грузится на
        // загрузке сессии через /pipeline-qwen-opus/pair-timings.
        const scQOPairTimings = ref({});              // pairId → {qwen_*, opus_*, status,…}
        const scQODetailsOpen = ref(true);            // expand per-pair live timeline
        const scQOSelectedCount = computed(() => scPairs.value.filter(p => scQOSelected[p.id]).length);
        const scQOAllSelected = computed(() => {
            const sel = scPairs.value.filter(p => p.left && p.right);
            return sel.length > 0 && sel.every(p => scQOSelected[p.id]);
        });

        const scPairsCounts = computed(() => {
            const c = {matched:0, maybe:0, unmatched:0};
            for (const p of scPairs.value) {
                if (p.status in c) c[p.status] += 1;
            }
            return c;
        });
        const scAlignmentItems = computed(() => {
            if (scAlignment.value && Array.isArray(scAlignment.value.items)) return scAlignment.value.items;
            // Fallback: alignment может прийти в составе pair view
            if (scPairData.value && scPairData.value.alignment && Array.isArray(scPairData.value.alignment.items)) {
                return scPairData.value.alignment.items;
            }
            return [];
        });
        const scAllLinksForGraphic = computed(() => {
            if (!scGraphicSummary.value) return [];
            return [
                ...(scGraphicSummary.value.manual_links || []),
                ...(scGraphicSummary.value.auto_links || []),
            ];
        });
        const scStaleLinksCount = computed(() => {
            const links = (scPairData.value && scPairData.value.links) || [];
            return links.filter(l => String(l.method || '').endsWith('_stale')).length;
        });

        // ── Link visualization (replaced "Связи блоков" table) ─────────────
        // Палитра для цветных плашек на связанных блоках. Каждая активная связь
        // получает номер (по порядку в links[]) и цвет = palette[(n-1) % N].
        const SC_LINK_PALETTE = [
            "#2563eb", "#16a34a", "#dc2626", "#9333ea",
            "#ea580c", "#0891b2", "#ca8a04", "#be123c",
        ];
        function scLinkColor(index) {
            const i = Math.max(1, parseInt(index, 10) || 1) - 1;
            return SC_LINK_PALETTE[i % SC_LINK_PALETTE.length];
        }
        // Активные (не-stale) связи нумеруются 1..N подряд, чтобы не было дырок
        // после удаления. Stale-связи попадают в карту тоже, но без номера
        // (вместо номера ставим '?', цвет — серый). Это удобнее, чем держать
        // отдельные структуры.
        const scLinkVisualMap = computed(() => {
            const links = (scPairData.value && scPairData.value.links) || [];
            const map = new Map();
            let activeNumber = 0;
            for (let i = 0; i < links.length; i++) {
                const l = links[i];
                const method = String(l.method || '');
                const isStale = method.endsWith('_stale');
                const isCross = method === 'manual_cross_page';
                const isManual = method.startsWith('manual');
                let number = null;
                let color = '#9ca3af';  // нейтральный для stale
                if (!isStale) {
                    activeNumber += 1;
                    number = activeNumber;
                    color = scLinkColor(number);
                }
                const info = {
                    number, color, link: l,
                    isStale, isCross, isManual,
                    isAuto: !isManual && !isStale,
                    key: l.left_block_id + '::' + l.right_block_id,
                };
                map.set('left|' + l.left_block_id, info);
                map.set('right|' + l.right_block_id, info);
            }
            return map;
        });
        function scBlockLinkInfo(side, blockId) {
            return scLinkVisualMap.value.get(side + '|' + blockId) || null;
        }
        // Активная (выбранная пользователем) связь — для компактной панели удаления
        const scActiveLinkKey = ref(null);  // 'left_id::right_id'
        const scActiveLink = computed(() => {
            if (!scActiveLinkKey.value) return null;
            const links = (scPairData.value && scPairData.value.links) || [];
            const [lid, rid] = scActiveLinkKey.value.split('::');
            return links.find(l => l.left_block_id === lid && l.right_block_id === rid) || null;
        });
        const scActiveLinkInfo = computed(() => {
            if (!scActiveLink.value) return null;
            return scLinkVisualMap.value.get('left|' + scActiveLink.value.left_block_id) || null;
        });
        function scStatusLabel(s) {
            return {matched: 'Сопоставлено', maybe: 'Возможно', unmatched: 'Не найдено', manual: 'Вручную', disabled: 'Скрыто'}[s] || s;
        }
        function scDiffTypeLabel(t) {
            return {added: 'добавлено', removed: 'удалено', modified: 'изменено'}[t] || t;
        }
        function scIsPdfUsedRight(pdfPath) {
            if (!scSession.value) return false;
            for (const p of scSession.value.pairs || []) {
                if (p.status === 'disabled') continue;
                if (p.right && p.right.pdf_path === pdfPath) return true;
            }
            return false;
        }

        async function scLoadObjects() {
            scObjectsLoading.value = true;
            scObjectsError.value = '';
            try {
                const r = await fetch('/api/stage-comparison/objects');
                if (!r.ok) throw new Error('HTTP ' + r.status);
                const data = await r.json();
                scObjects.value = data.items || [];
                scObjectsRoots.value = data.roots || [];
                scAutoSelectFromTopObject();
            } catch (e) {
                scObjectsError.value = String(e.message || e);
            } finally {
                scObjectsLoading.value = false;
            }
        }

        // ── Saved canonical config (one-click apply/save) ───────────────
        // Кнопка «★ Применить сохранённую конфигурацию» подставляет в форму
        // stage_a_path / stage_b_path из persistent config'а; «💾 Сохранить
        // как канонические» перезаписывает config текущими путями.

        async function scLoadSavedConfig() {
            try {
                const r = await fetch('/api/stage-comparison/saved-config');
                if (!r.ok) return;
                scSavedConfig.value = await r.json();
            } catch (_) { /* silent — UI просто не покажет кнопку */ }
        }

        async function scApplySavedConfig() {
            if (!scSavedConfig.value || !scSavedConfig.value.saved) return;
            scStageAPath.value = scSavedConfig.value.stage_a_path || '';
            scStageBPath.value = scSavedConfig.value.stage_b_path || '';
            // Если есть object/stage labels — попытаемся подсветить дропдауны.
            const lbl = scSavedConfig.value.object_label;
            const objs = scObjects.value || [];
            if (lbl) {
                const matched = objs.find(o => o.name === lbl || o.id === lbl);
                if (matched) {
                    scSelectedObjectId.value = matched.id;
                    if (scSavedConfig.value.stage_a_label) {
                        scSelectedStageA.value = scSavedConfig.value.stage_a_label;
                    }
                    if (scSavedConfig.value.stage_b_label) {
                        scSelectedStageB.value = scSavedConfig.value.stage_b_label;
                    }
                }
            }
            scSavedConfigMsg.value = 'Канонические пути подставлены — нажмите «Сканировать»';
            setTimeout(() => { scSavedConfigMsg.value = ''; }, 3500);
        }

        async function scSaveCurrentAsCanonical() {
            if (!scStageAPath.value || !scStageBPath.value) return;
            scSavedConfigSaving.value = true;
            scSavedConfigMsg.value = '';
            try {
                const obj = (scObjects.value || []).find(o => o.id === scSelectedObjectId.value);
                const body = {
                    stage_a_path: scStageAPath.value,
                    stage_b_path: scStageBPath.value,
                    object_label: obj ? obj.name : null,
                    stage_a_label: scSelectedStageA.value || null,
                    stage_b_label: scSelectedStageB.value || null,
                };
                const r = await fetch('/api/stage-comparison/saved-config', {
                    method: 'PUT',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(body),
                });
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    throw new Error(j.detail || ('HTTP ' + r.status));
                }
                scSavedConfig.value = await r.json();
                scSavedConfigMsg.value = '✓ Сохранено как канонические';
                setTimeout(() => { scSavedConfigMsg.value = ''; }, 3500);
            } catch (e) {
                scSavedConfigMsg.value = '✗ ' + String(e.message || e);
            } finally {
                scSavedConfigSaving.value = false;
            }
        }

        // ── Каноничная конфигурация v2 (canonical-config) ──────────────────
        // Объекту соответствует одна актуальная конфигурация: канонический
        // session_id + pairs + режимы. Кнопка «Сохранить как каноничную»
        // делает POST /sessions/{sid}/save-canonical; при открытии раздела
        // система пробует автоматически открыть каноничную сессию.

        async function scLoadCanonicalConfig() {
            try {
                const r = await fetch('/api/stage-comparison/canonical-config');
                if (!r.ok) return;
                scCanonicalConfig.value = await r.json();
            } catch (_) { /* silent — UI просто не покажет блок */ }
        }

        async function scSaveSessionAsCanonical() {
            if (!scSession.value || !scSession.value.id) return;
            scSavedConfigSaving.value = true;
            scSavedConfigMsg.value = '';
            try {
                const obj = (scObjects.value || []).find(o => o.id === scSelectedObjectId.value);
                const body = {
                    object_label: obj ? obj.name : null,
                    stage_a_label: scSelectedStageA.value || null,
                    stage_b_label: scSelectedStageB.value || null,
                };
                const r = await fetch(
                    '/api/stage-comparison/sessions/' + encodeURIComponent(scSession.value.id) + '/save-canonical',
                    {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify(body),
                    },
                );
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    throw new Error(j.detail || ('HTTP ' + r.status));
                }
                scCanonicalConfig.value = await r.json();
                // savedConfig (legacy paths-only) тоже синхронизируем — после
                // save-canonical обновлены и пути, и canonical_session_id.
                scSavedConfig.value = scCanonicalConfig.value;
                scCanonicalStale.value = false;
                scSavedConfigMsg.value = '✓ Каноничная конфигурация сохранена';
                setTimeout(() => { scSavedConfigMsg.value = ''; }, 3500);
            } catch (e) {
                scSavedConfigMsg.value = '✗ ' + String(e.message || e);
            } finally {
                scSavedConfigSaving.value = false;
            }
        }

        // Автоматически открыть каноничную конфигурацию (если есть и сессия
        // существует). Возвращает true если открыли каноничную сессию.
        async function scTryOpenCanonical() {
            try {
                const r = await fetch('/api/stage-comparison/canonical-config/open');
                if (!r.ok) return false;
                const j = await r.json();
                if (!j.saved) return false;
                scCanonicalConfig.value = {saved: true, ...(j.canonical_config || {})};
                scCanonicalStale.value = !!j.config_stale;
                if (!j.canonical_session_id || !j.canonical_session_available || !j.session) {
                    // canonical metadata есть, но сессия не найдена — UI покажет
                    // предупреждение через scCanonicalConfig + scSession=null.
                    return false;
                }
                scSession.value = j.session;
                // Подставляем пути в форму, чтобы при ручном «Открыть проект»
                // тоже всё совпадало.
                if (j.session.stage_a_path) scStageAPath.value = j.session.stage_a_path;
                if (j.session.stage_b_path) scStageBPath.value = j.session.stage_b_path;
                // Баннер автоподгрузки старой модели не показываем — он про историю сессий.
                scAutoLoadInfo.value = null;
                try { await scRecogRestoreActive(); } catch (_) {}
                try { await scOpusRestoreActive(); } catch (_) {}
                try { await scQORestoreActive(); } catch (_) {}
                try { await scLoadPairCompareStatuses(); } catch (_) {}
                try { await scOpusLoadPreflight(); } catch (_) {}
                return true;
            } catch (_) { return false; }
        }

        // На первой загрузке /stage-comparison `scLoadObjects()` стартует параллельно с
        // `loadObjects()` (который заполняет `objectName`). Любой из них может
        // финишировать первым — поэтому ещё watch'им obj/objects и переподставляем
        // объект, когда обе стороны готовы.
        watch([objectName, scObjects], () => {
            if (currentView.value !== 'stage-comparison') return;
            if (scSelectedObjectId.value) return;
            scAutoSelectFromTopObject();
        });

        function scAutoSelectFromTopObject() {
            const top = (objectName.value || '').trim();
            if (!top) return;
            const match = scObjects.value.find(o => (o.name || '').trim() === top);
            if (!match) return;
            if (scSelectedObjectId.value === match.id) return;
            scSelectedObjectId.value = match.id;
            scSelectedStageA.value = '';
            scSelectedStageB.value = '';
            scApplySelectedObject();
        }

        function scApplySelectedObject() {
            const obj = scObjects.value.find(o => o.id === scSelectedObjectId.value);
            if (!obj) { scStageAPath.value = ''; scStageBPath.value = ''; return; }
            // Подставляем дефолтные стадии при первом выборе или если ранее выбранных уже нет
            const stageNames = (obj.stages || []).map(s => s.name);
            if (!stageNames.includes(scSelectedStageA.value)) {
                scSelectedStageA.value = obj.default_stage_a?.name || stageNames[0] || '';
            }
            if (!stageNames.includes(scSelectedStageB.value)) {
                scSelectedStageB.value = obj.default_stage_b?.name || stageNames[stageNames.length - 1] || '';
            }
            const sA = (obj.stages || []).find(s => s.name === scSelectedStageA.value);
            const sB = (obj.stages || []).find(s => s.name === scSelectedStageB.value);
            scStageAPath.value = sA ? sA.path : '';
            scStageBPath.value = sB ? sB.path : '';
        }

        const scSelectedObject = computed(() =>
            scObjects.value.find(o => o.id === scSelectedObjectId.value) || null
        );

        // Открыть проект: если для текущих stage_a_path/stage_b_path уже
        // есть сессия — подгрузить её (без перерасчётов). Иначе создать
        // новую через scScanFolders. Это «вариант A» — у проекта один
        // живой контекст, история скрыта.
        async function scOpenProject() {
            scError.value = '';
            scWarnings.value = [];
            const a = _scNormalizePath(scStageAPath.value);
            const b = _scNormalizePath(scStageBPath.value);
            if (!a || !b) return;
            // Если уже открыта сессия ровно для этих путей — ничего не делаем
            if (scSession.value && scSession.value.stage_a_path
                && _scNormalizePath(scSession.value.stage_a_path) === a
                && _scNormalizePath(scSession.value.stage_b_path) === b) {
                return;
            }
            // Пробуем найти существующую (тихий fetch)
            scAutoLoading.value = true;
            try {
                const r = await fetch('/api/stage-comparison/sessions');
                if (r.ok) {
                    const j = await r.json();
                    const list = j.sessions || [];
                    scSessions.value = list;
                    const match = list.find(s =>
                        _scNormalizePath(s.stage_a_path) === a &&
                        _scNormalizePath(s.stage_b_path) === b
                    );
                    if (match) {
                        const rs = await fetch('/api/stage-comparison/sessions/' + encodeURIComponent(match.id));
                        if (rs.ok) {
                            scSession.value = await rs.json();
                            scAutoLoadInfo.value = {
                                session_id: match.id,
                                created_at: match.created_at,
                                pairs_total: match.pairs_total,
                                pairs_matched: match.pairs_matched,
                            };
                            try { await scRecogRestoreActive(); } catch (_) {}
                            try { await scOpusRestoreActive(); } catch (_) {}
                            try { await scQORestoreActive(); } catch (_) {}
                            try { await scLoadPairCompareStatuses(); } catch (_) {}
                            try { await scOpusLoadPreflight(); } catch (_) {}
                            return;
                        }
                    }
                }
            } catch (_) { /* fall through to scan */ }
            finally {
                scAutoLoading.value = false;
            }
            // Существующей сессии нет → создаём новую
            await scScanFolders();
        }

        async function scScanFolders() {
            scError.value = '';
            scWarnings.value = [];
            scScanning.value = true;
            // Если пользователь явно жмёт «Сканировать» — снимаем баннер
            // авто-подгрузки: дальнейшее состояние принадлежит новой сессии.
            scAutoLoadInfo.value = null;
            try {
                const resp = await fetch('/api/stage-comparison/sessions', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({stage_a_path: scStageAPath.value, stage_b_path: scStageBPath.value}),
                });
                if (!resp.ok) {
                    const j = await resp.json().catch(() => ({detail: 'HTTP ' + resp.status}));
                    throw new Error(j.detail || ('HTTP ' + resp.status));
                }
                const data = await resp.json();
                scSession.value = {
                    id: data.session_id,
                    pairs: data.pairs || [],
                    stage_a_path: data.stage_a_path,
                    stage_b_path: data.stage_b_path,
                    created_at: data.created_at,
                };
                scWarnings.value = data.warnings || [];
            } catch (e) {
                scError.value = String(e.message || e);
            } finally {
                scScanning.value = false;
            }
        }

        // Сравнение путей: нормализуем trailing slash и пробелы, чтобы не
        // зависеть от того, как именно бекенд их вернул в session.json.
        function _scNormalizePath(p) {
            if (!p) return '';
            const s = String(p).trim();
            return s.endsWith('/') ? s.slice(0, -1) : s;
        }

        // Найти самую свежую сессию для текущих stage_a_path / stage_b_path и
        // подгрузить её. Запускается при смене объекта/стадии и при первичном
        // монтировании (после восстановления выбранного объекта с бекенда).
        async function scTryAutoLoadSession() {
            const a = _scNormalizePath(scStageAPath.value);
            const b = _scNormalizePath(scStageBPath.value);
            if (!a || !b) return;
            // Если уже открыта сессия ровно для этих путей — не трогаем.
            if (scSession.value && scSession.value.stage_a_path
                && _scNormalizePath(scSession.value.stage_a_path) === a
                && _scNormalizePath(scSession.value.stage_b_path) === b) {
                return;
            }
            scAutoLoading.value = true;
            try {
                const r = await fetch('/api/stage-comparison/sessions');
                if (!r.ok) return;
                const j = await r.json();
                const list = j.sessions || [];
                scSessions.value = list;
                const match = list.find(s =>
                    _scNormalizePath(s.stage_a_path) === a &&
                    _scNormalizePath(s.stage_b_path) === b
                );
                if (!match) {
                    // Для этих путей сессии ещё нет — старую сессию (если она
                    // от других путей) убираем, баннер тоже.
                    if (scSession.value && (
                        _scNormalizePath(scSession.value.stage_a_path) !== a ||
                        _scNormalizePath(scSession.value.stage_b_path) !== b)) {
                        scSession.value = null;
                        scActivePair.value = null;
                        scPairData.value = null;
                    }
                    scAutoLoadInfo.value = null;
                    return;
                }
                // Грузим. Не используем scLoadSession напрямую, потому что та
                // закрывает список сессий и не выставляет наш баннер.
                const rs = await fetch('/api/stage-comparison/sessions/' + encodeURIComponent(match.id));
                if (!rs.ok) return;
                const data = await rs.json();
                scSession.value = data;
                // Этот путь (object-autoselect после refresh) минует scLoadSession,
                // поэтому persisted Qwen/Opus времена, статусы сравнения и активные
                // job'ы тут надо подтянуть явно — иначе после F5 колонки 🟦/🟪
                // показывают «—», а колонка «Сравнение» теряет статус/режим
                // (fallback) у реально сравнённых пар.
                try { await scQORestoreActive(); } catch (_) {}
                try { await scQOLoadPairTimings(); } catch (_) {}
                try { await scLoadPairCompareStatuses(); } catch (_) {}
                scAutoLoadInfo.value = {
                    session_id: match.id,
                    created_at: match.created_at,
                    pairs_total: match.pairs_total,
                    pairs_matched: match.pairs_matched,
                };
            } catch (e) {
                // Тихо — авто-подгрузка не должна ломать UX выбора объекта.
                console.warn('[stage-comparison] auto-load failed:', e);
            } finally {
                scAutoLoading.value = false;
            }
        }

        // Авто-подгрузка триггерится при изменении путей (включая первичную
        // подстановку из объекта после рефреша страницы).
        watch([scStageAPath, scStageBPath], () => {
            if (currentView.value !== 'stage-comparison') return;
            scTryAutoLoadSession();
        });

        async function scLoadSessionsList() {
            scSessionsListOpen.value = !scSessionsListOpen.value;
            if (!scSessionsListOpen.value) return;
            try {
                const r = await fetch('/api/stage-comparison/sessions');
                const j = await r.json();
                scSessions.value = j.sessions || [];
            } catch (e) {
                scError.value = 'Не удалось загрузить список сессий: ' + e;
            }
        }
        // Fetch only — без toggle. Используется <details> в истории сессий.
        async function scFetchSessionsList() {
            try {
                const r = await fetch('/api/stage-comparison/sessions');
                if (!r.ok) return;
                const j = await r.json();
                scSessions.value = j.sessions || [];
            } catch (_) { /* silent */ }
        }

        async function scLoadSession(sessionId) {
            try {
                const r = await fetch('/api/stage-comparison/sessions/' + encodeURIComponent(sessionId));
                if (!r.ok) throw new Error('HTTP ' + r.status);
                const data = await r.json();
                scSession.value = data;
                scSessionsListOpen.value = false;
                // Пользователь явно выбрал сессию — баннер «авто-подгрузка»
                // больше не релевантен.
                scAutoLoadInfo.value = null;
                // Restore last/active session-scope Qwen recognition job, чтобы
                // на этапе 1 сразу показать актуальный прогресс/последний прогон.
                try { await scRecogRestoreActive(); } catch (_) {}
                // То же самое для Opus session batch + preflight, иначе после
                // F5 на вкладке «Загрузка документации» UI не знает, что job
                // активен (watch(scTab) срабатывает только при смене вкладки).
                try { await scOpusRestoreActive(); } catch (_) {}
                // Re-attach к запущенному Qwen→Opus pipeline job после F5, иначе
                // панель «Pipeline Qwen→Opus» пропадает, хотя job ещё работает.
                try { await scQORestoreActive(); } catch (_) {}
                // Persisted Qwen/Opus времена по парам — чтобы колонки 🟦/🟪
                // показывали значения после refresh (in-memory job потерян).
                try { await scQOLoadPairTimings(); } catch (_) {}
                try { await scLoadPairCompareStatuses(); } catch (_) {}
                try { await scOpusLoadPreflight(); } catch (_) {}
                // Per-pair статус «Проверено экспертом» для таблицы загрузки.
                try { await scLoadExpertPerPair(); } catch (_) {}
                // Показать результат последнего пакетного авто-сопоставления
                // листов (и переподключиться к живому job после F5).
                try { await scAutoMatchLoadLast(); } catch (_) {}
            } catch (e) {
                scError.value = 'Не удалось загрузить сессию: ' + e;
            }
        }

        function scQOToggleAll(ev) {
            const on = ev && ev.target ? ev.target.checked : !scQOAllSelected.value;
            scPairs.value.forEach(p => { if (p.left && p.right) scQOSelected[p.id] = on; });
        }
        function scQOPairLabel(pid) {
            const p = scPairs.value.find(x => x.id === pid);
            return p ? (p.label || (p.left && p.left.filename) || pid) : pid;
        }
        function scQOPairBadge(pid) {
            const job = scQOJob.value;
            if (!job) return null;
            const it = (job.items || []).find(i => i.pair_id === pid);
            if (!it) return null;
            let label, color, title;
            if (it.qwen_status === 'failed') { label = 'Qwen ✗'; color = '#b91c1c'; title = it.qwen_error || 'Qwen: ошибка'; }
            else if (it.opus_status === 'done') { label = '✓ ' + (it.changes_count || 0); color = '#16a34a'; title = 'Opus готов · изменений: ' + (it.changes_count || 0); }
            else if (it.opus_status === 'failed') { label = 'Opus ✗'; color = '#b91c1c'; title = it.opus_error || 'Opus: ошибка'; }
            else if (it.opus_status === 'running') { label = 'Opus…'; color = '#2563eb'; title = 'Opus: сравнение'; }
            else if (it.qwen_status === 'done') { label = 'Qwen ✓'; color = '#0d9488'; title = 'Qwen готов · ждёт/идёт Opus'; }
            else if (it.qwen_status === 'running') { label = 'Qwen…'; color = '#2563eb'; title = 'Qwen: обогащение'; }
            else { label = '…'; color = '#6b7280'; title = 'В очереди'; }
            return { label, color, title };
        }

        // ── live timing / dynamics for the running Qwen→Opus pipeline ────────
        // All durations tick via scQOClock (1s), so the panel shows live elapsed
        // for running items and frozen totals for finished ones.
        function scQOParseTs(s) {
            if (!s) return null;
            const ms = Date.parse(s);              // ISO "...Z" → UTC ms
            return Number.isFinite(ms) ? ms : null;
        }
        function scQOClockStart() {
            if (scQOClockTimer) return;
            scQOClock.value = Date.now();
            scQOClockTimer = setInterval(() => { scQOClock.value = Date.now(); }, 1000);
        }
        function scQOClockStop() {
            if (scQOClockTimer) { clearInterval(scQOClockTimer); scQOClockTimer = null; }
        }
        function scQOItemFor(pid) {
            if (!pid) return null;
            const job = scQOJob.value;
            const jobLive = job && ['running', 'queued'].includes(job.status);
            // 1) ЖИВОЙ job (running/queued) — приоритет: содержит live-прогресс.
            if (jobLive) {
                const it = (job.items || []).find(i => i.pair_id === pid);
                if (it) return it;
            }
            // 2) persisted timing (переживает F5; авторитетно для ПОСЛЕДНЕГО
            //    прогона по паре — включая ручной repair через unified/large_sheet/
            //    md, который не идёт через qopipe). Берётся РАНЬШЕ терминального
            //    in-memory job, иначе устаревший qopipe-item (failed/skipped)
            //    перебивал бы свежий repair-результат.
            const t = scQOPairTimings.value && scQOPairTimings.value[pid];
            if (t) return t;
            // 3) терминальный in-memory job — последний резерв (сразу после
            //    завершения, пока persisted-карта ещё не обновилась).
            if (job) {
                const it = (job.items || []).find(i => i.pair_id === pid);
                if (it) return it;
            }
            return null;
        }
        // Подтянуть последние Qwen/Opus времена по всем парам из persisted
        // qopipe job-файлов (read-only, маленькие json). Вызывается на загрузке
        // сессии и после завершения прогона, чтобы колонки 🟦/🟪 показывали
        // времена и ПОСЛЕ refresh.
        async function scQOLoadPairTimings() {
            if (!scSession.value || !scSession.value.id) return;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pipeline-qwen-opus/pair-timings`;
                const data = await fetch(url).then(r => r.json());
                scQOPairTimings.value = (data && data.timings) || {};
            } catch (_) { /* fail-soft: остаётся прежняя карта / пусто → «—» */ }
        }
        // Overall job elapsed: from the first Qwen start (else created_at) to now
        // while live, frozen at updated_at once terminal.
        function scQOElapsedMs() {
            const job = scQOJob.value;
            if (!job) return 0;
            let startMs = null;
            for (const it of job.items || []) {
                const t = scQOParseTs(it.qwen_started_at);
                if (t != null && (startMs == null || t < startMs)) startMs = t;
            }
            if (startMs == null) startMs = scQOParseTs(job.created_at);
            if (startMs == null) return 0;
            const live = ['running', 'queued'].includes(job.status);
            const endMs = live ? scQOClock.value : (scQOParseTs(job.updated_at) || scQOClock.value);
            return Math.max(0, endMs - startMs);
        }
        // Duration (ms) of one lane (qwen|opus) of a pair. Finished → fixed;
        // running → live; not started → null.
        function scQOItemLaneMs(it, lane) {
            if (!it) return null;
            const st = scQOParseTs(it[lane + '_started_at']);
            if (st == null) {
                // Нет start-таймстампа (persisted repair/manual timing) → берём
                // готовую длительность *_duration_sec, если она есть.
                const d = it[lane + '_duration_sec'];
                return (typeof d === 'number' && d >= 0) ? d * 1000 : null;
            }
            const fin = scQOParseTs(it[lane + '_finished_at']);
            return Math.max(0, (fin != null ? fin : scQOClock.value) - st);
        }
        function scQOItemLaneLabel(it, lane) {
            const ms = scQOItemLaneMs(it, lane);
            if (ms == null) return '';
            return formatDuration(ms) || '0с';
        }
        // Compact lane cell for the per-pair timeline: status glyph + duration.
        function scQOLaneCell(it, lane) {
            if (!it) return '—';
            const st = it[lane + '_status'];
            const dur = scQOItemLaneLabel(it, lane);
            // «✓ 22,8м» если длительность известна; иначе просто «✓» (repair-
            // сигнал без per-pair длительности — лучше, чем вводящее «✓ 0с»).
            if (st === 'done') return dur ? '✓ ' + dur : '✓';
            if (st === 'running') {
                // Qwen-дорожка обрабатываемой сейчас пары: показать прогресс по
                // блокам «… N/M» (живой из активной md-enrichment job), иначе —
                // длительность.
                if (lane === 'qwen') {
                    const job = scQOJob.value;
                    const curPid = job && job.qwen_worker && job.qwen_worker.current_pair_id;
                    if (curPid && curPid === it.pair_id && typeof scQOCurrentBlock === 'function') {
                        const cb = scQOCurrentBlock();
                        if (cb && cb.total) return '… ' + cb.index + '/' + cb.total;
                    }
                }
                return '… ' + (dur || '0с');
            }
            if (st === 'failed') return '✗' + (dur ? ' ' + dur : '');
            if (st === 'queued') return '⏱';
            if (st === 'skipped') return '⊘';
            return '—';  // waiting_qwen / waiting / not run
        }
        function scQOLaneColor(it, lane) {
            if (!it) return '#9ca3af';
            const st = it[lane + '_status'];
            if (st === 'running') return '#2563eb';
            if (st === 'done') return '#16a34a';
            if (st === 'failed') return '#b91c1c';
            return '#9ca3af';
        }
        // Total wall-clock for a pair across both lanes (min start → max finish,
        // live if still in flight).
        function scQOItemTotalLabel(it) {
            if (!it) return '';
            const starts = [scQOParseTs(it.qwen_started_at), scQOParseTs(it.opus_started_at)]
                .filter(x => x != null);
            if (!starts.length) return '';
            const start = Math.min(...starts);
            const terminal = ['done', 'failed', 'skipped'].includes(it.status);
            let end = scQOClock.value;
            if (terminal) {
                const fins = [scQOParseTs(it.qwen_finished_at), scQOParseTs(it.opus_finished_at)]
                    .filter(x => x != null);
                if (fins.length) end = Math.max(...fins);
            }
            return formatDuration(Math.max(0, end - start)) || '0с';
        }
        // Live image-block detail of the Qwen lane's current pair, taken from the
        // active md-enrichment aggregate (refreshed during polling).
        function scQOCurrentBlock() {
            const job = scQOJob.value;
            const curPid = job && job.qwen_worker && job.qwen_worker.current_pair_id;
            if (!curPid) return null;
            const agg = scQOActiveRecog.value && scQOActiveRecog.value.aggregate;
            if (!agg) return null;
            if (agg.current_pair_id && agg.current_pair_id !== curPid) return null;
            const tot = Number(agg.current_total_blocks || 0);
            if (!tot) return null;
            return {
                index: Number(agg.current_block_index || 0), total: tot,
                page: agg.current_page, side: agg.current_side,
                avg_sec: (agg.diagnostics && agg.diagnostics.avg_duration_sec) || 0,
                eta_sec: agg.eta_sec, message: agg.current_status_message || '',
            };
        }
        // Job-wide image-block totals: how many image blocks Qwen must process
        // across ALL pairs in this run, and how many are already processed.
        // Per-pair totals come from the on-disk recognition metrics
        // (scRecogPairBlocks — block counts are stable across re-runs, so they
        // are a reliable estimate of the work). "Готово" counts the full block
        // total of every pair whose Qwen lane already finished, plus the live
        // in-flight progress of the pair Qwen is processing right now. Returns
        // null until at least one pair's block total is known.
        function scQOBlocksOverall() {
            const job = scQOJob.value;
            if (!job || !Array.isArray(job.items)) return null;
            const curPid = job.qwen_worker && job.qwen_worker.current_pair_id;
            const cur = scQOCurrentBlock();
            let total = 0, done = 0, known = false;
            for (const it of job.items) {
                const pb = (typeof scRecogPairBlocks === 'function') ? scRecogPairBlocks(it.pair_id) : null;
                const pairTotal = (pb && pb.available) ? Number(pb.total || 0) : 0;
                if (pairTotal > 0) known = true;
                total += pairTotal;
                if (it.qwen_status === 'done') {
                    done += pairTotal;
                } else if (curPid && it.pair_id === curPid && cur) {
                    done += Math.min(Number(cur.index || 0), pairTotal || Number(cur.index || 0));
                }
            }
            if (!known) return null;
            return {
                total, done,
                pairsDone: Number((job.qwen_worker && job.qwen_worker.done) || 0),
                pairsTotal: Number((job.qwen_worker && job.qwen_worker.total) || job.items.length),
            };
        }
        // Remaining-time estimate. Lanes are decoupled, so the wall-clock floor is
        // the slower of the two remaining workloads (avg finished lane duration ×
        // pairs still needing that lane, minus already-elapsed for in-flight ones).
        function scQOEtaSec() {
            const job = scQOJob.value;
            if (!job || !['running', 'queued'].includes(job.status)) return null;
            const items = job.items || [];
            const avg = (lane) => {
                const ds = items
                    .filter(it => scQOParseTs(it[lane + '_finished_at']) != null
                                  && scQOParseTs(it[lane + '_started_at']) != null)
                    .map(it => scQOItemLaneMs(it, lane))
                    .filter(ms => ms != null && ms > 0);
                return ds.length ? ds.reduce((a, b) => a + b, 0) / ds.length / 1000 : null;
            };
            const laneRemain = (lane, avgSec) => {
                if (avgSec == null) return 0;
                let rem = 0;
                for (const it of items) {
                    const st = it[lane + '_status'];
                    if (st === 'running') {
                        const elapsed = (scQOItemLaneMs(it, lane) || 0) / 1000;
                        rem += Math.max(0, avgSec - elapsed);
                    } else if (!['done', 'failed', 'skipped'].includes(st)) {
                        rem += avgSec;
                    }
                }
                return rem;
            };
            const eta = Math.max(laneRemain('qwen', avg('qwen')), laneRemain('opus', avg('opus')));
            return eta > 0 ? Math.round(eta) : null;
        }
        async function scQORefreshActiveRecog() {
            // Pull the running internal md-enrichment job for live image-block
            // detail. Merge ONLY the active pair's status into scRecogJob so the
            // "Блоки" column stays live for the running pair without dropping the
            // already-recognised pairs (the active job is pair-scoped).
            if (!scSession.value || !scSession.value.id) return;
            const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/md-enrichment-jobs/active`;
            const data = await fetch(url).then(r => r.json());
            const job = data && data.job;
            if (!job) return;
            scQOActiveRecog.value = job;
            const agg = job.aggregate;
            if (!agg || !agg.pair_statuses) return;
            if (!scRecogJob.value || !scRecogJob.value.aggregate) {
                scRecogJob.value = job;
                return;
            }
            const merged = { ...(scRecogJob.value.aggregate.pair_statuses || {}), ...agg.pair_statuses };
            scRecogJob.value = {
                ...scRecogJob.value,
                aggregate: { ...scRecogJob.value.aggregate, pair_statuses: merged },
            };
        }

        async function scQOPreflight(pairIds) {
            const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pipeline-qwen-opus/preflight`;
            const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ scope: 'selected', pair_ids: pairIds, force_qwen: true, force_opus: true }) });
            return await r.json();
        }
        async function scQOOpenConfirm() {
            const ids = scPairs.value.filter(p => scQOSelected[p.id]).map(p => p.id);
            if (!ids.length || scQOPreflighting.value) return;
            scQOClearBeforeRun.value = false;  // safe default each open
            scQOMode.value = 'normal';         // safe default each open
            scQOPreflighting.value = true;
            try { scQOConfirm.value = await scQOPreflight(ids); }
            catch (e) { alert('Не удалось подготовить прогон (preflight): ' + ((e && e.message) || e)); }
            finally { scQOPreflighting.value = false; }
        }
        async function scQOProcessPair(pid) {
            if (scQOPreflighting.value) return;
            scQOClearBeforeRun.value = false;  // safe default each open
            scQOMode.value = 'normal';         // safe default each open
            scQOPreflighting.value = true;
            try { scQOConfirm.value = await scQOPreflight([pid]); }
            catch (e) { alert('Не удалось подготовить прогон (preflight): ' + ((e && e.message) || e)); }
            finally { scQOPreflighting.value = false; }
        }
        // POST clear-analysis (backup → удалить найденные расхождения + ручные
        // отметки проверки по выбранным парам). Возвращает ответ backend'а.
        async function scQOClearAnalysis(pairIds) {
            if (!scSession.value || !scSession.value.id || !pairIds || !pairIds.length)
                return { ok: true, cleared_pairs: 0, skipped: [] };
            const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/clear-analysis`;
            const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ pair_ids: pairIds, clear_findings: true, clear_review: true, clear_enrichment: false }) });
            if (!r.ok) throw new Error('clear-analysis HTTP ' + r.status);
            return await r.json();
        }
        async function scQOStartConfirmed() {
            const ids = (scQOConfirm.value && scQOConfirm.value.pair_ids) || [];
            if (!ids.length) { scQOConfirm.value = null; return; }
            const mode = scQOMode.value || 'normal';
            // Режимы «Только Opus»: unified-analysis по готовым enriched MD без Qwen.
            if (mode === 'opus_only' || mode === 'clear_result_opus_only') {
                await scQOStartOpusOnly(ids, mode === 'clear_result_opus_only');
                scQOMode.value = 'normal';
                scQOConfirm.value = null;
                return;
            }
            // Режим «Очистить и запустить»: 1) clear-analysis, 2) обычный pipeline.
            // Очистка молча НЕ идёт.
            if (mode === 'clear_and_run') {
                scQOClearing.value = true;
                try {
                    const res = await scQOClearAnalysis(ids);
                    const blocked = ((res && res.skipped) || [])
                        .filter(s => /running job/.test(s.reason || '')).map(s => s.pair_id);
                    if (blocked.length) {
                        scQOClearing.value = false;
                        alert('Очистка пропущена для пар с активным прогоном: ' + blocked.join(', ') +
                            '.\nОстановите job и повторите.');
                        return;  // не запускаем — пусть пользователь разрулит
                    }
                    // cleared пары должны выглядеть как непроверенные
                    if (typeof scLoadPairCompareStatuses === 'function') { try { await scLoadPairCompareStatuses(); } catch (e) {} }
                    if (typeof scLoadUnifiedFlat === 'function') { try { await scLoadUnifiedFlat(); } catch (e) {} }
                } catch (e) {
                    scQOClearing.value = false;
                    alert('Не удалось очистить анализ выбранных пар: ' + ((e && e.message) || e));
                    return;
                }
                scQOClearing.value = false;
            }
            await scQOStart(ids);
            scQOClearBeforeRun.value = false;
            scQOMode.value = 'normal';
            scQOConfirm.value = null;
        }
        // Запустить ТОЛЬКО Opus по выбранным парам (без Qwen) через endpoint
        // /pairs/opus-only. clearResult=true → очистить текущий comparison_result
        // (с backup) перед Opus. Пары без enriched MD пропускаются с предупреждением.
        // Серверный unified-job подхватывается обычным трекером (scOpusRestoreActive).
        async function scQOStartOpusOnly(pairIds, clearResult) {
            if (!scSession.value || !scSession.value.id || !pairIds || !pairIds.length) return;
            scQORunning.value = true;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/opus-only`;
                const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ pair_ids: pairIds, force: true, backup_existing: true,
                                           clear_comparison_result: !!clearResult }) });
                if (!r.ok) { scQORunning.value = false; alert('Только Opus: HTTP ' + r.status); return; }
                const res = await r.json();
                const skipped = (res && res.skipped) || [];
                if (skipped.length) {
                    const txt = skipped.map(s => s.pair_id + ' (' + (s.reason || '?') + ')').join(', ');
                    alert('Часть пар пропущена (только Opus): ' + txt +
                        '.\nПары без enriched MD сначала нужно распознать (Qwen).');
                }
                // Подхватить запущенный unified-job (если стартовал) + обновить статусы.
                if (res && res.job_id && typeof scOpusRestoreActive === 'function') {
                    try { await scOpusRestoreActive(); } catch (e) {}
                }
                if (typeof scQOLoadPairTimings === 'function') { try { await scQOLoadPairTimings(); } catch (e) {} }
                if (typeof scLoadPairCompareStatuses === 'function') { try { await scLoadPairCompareStatuses(); } catch (e) {} }
            } catch (e) {
                alert('Не удалось запустить только Opus: ' + ((e && e.message) || e));
            } finally {
                scQORunning.value = false;
            }
        }
        async function scQOStart(pairIds) {
            if (!scSession.value || !scSession.value.id) return;
            scQORunning.value = true;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pipeline-qwen-opus`;
                const r = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ scope: 'selected', pair_ids: pairIds, force_qwen: true, force_opus: true, prebuild_large_sheets: true, confirm: true }) });
                const job = await r.json();
                // Health-gate: backend заблокировал старт (LLM/ngrok недоступен).
                if (job && job.status === 'rejected_llm_unavailable') {
                    scQORunning.value = false;
                    alert('Запуск заблокирован: локальный LLM/ngrok недоступен (' +
                        (job.reason || 'unknown') + ').\nВосстановите ngrok-туннель и загрузите модель в LM Studio, затем повторите.');
                    return;
                }
                scQOJob.value = job;
                if (job && job.job_id) { scQORemember(job.job_id); scQOPollJob(job.job_id); }
                else scQORunning.value = false;
            } catch (e) { scQORunning.value = false; }
        }
        // Persist the running pipeline job id per-session so an F5 / tab reopen
        // can re-attach to it (the panel state lives only in memory otherwise,
        // so without this a reload makes the whole "Pipeline Qwen→Opus" box
        // vanish even though the backend job keeps running).
        function scQOJobKey() {
            return scSession.value && scSession.value.id ? ('sc_qo_job:' + scSession.value.id) : null;
        }
        function scQORemember(jobId) {
            const k = scQOJobKey();
            if (k && jobId) { try { localStorage.setItem(k, jobId); } catch (_) {} }
        }
        function scQOForget() {
            const k = scQOJobKey();
            if (k) { try { localStorage.removeItem(k); } catch (_) {} }
        }
        // Re-attach to a still-running pipeline job after a page reload. Called
        // from scLoadSession. Frontend-only: reads the remembered job id and
        // resumes polling via the existing status endpoint.
        async function scQORestoreActive() {
            const k = scQOJobKey();
            if (!k) return;
            let jobId = null;
            try { jobId = localStorage.getItem(k); } catch (_) {}
            if (!jobId) return;
            const terminal = ['done', 'partial', 'failed', 'cancelled', 'rejected_no_confirm', 'failed_interrupted'];
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pipeline-qwen-opus/${encodeURIComponent(jobId)}`;
                const r = await fetch(url);
                if (!r.ok) { scQOForget(); return; }
                const job = await r.json();
                if (!job || !job.job_id) { scQOForget(); return; }
                scQOJob.value = job;
                if (terminal.includes(job.status)) {
                    // finished while the page was closed — show the last run once,
                    // then stop tracking it so it doesn't keep re-appearing.
                    scQOForget();
                } else {
                    scQOPollJob(job.job_id);  // resume live polling
                }
            } catch (_) { /* keep pointer; transient network error */ }
        }
        function scQOPollJob(jobId) {
            if (scQOPollTimer) { clearTimeout(scQOPollTimer); scQOPollTimer = null; }
            const terminal = ['done', 'partial', 'failed', 'cancelled', 'rejected_no_confirm', 'failed_interrupted'];
            const tick = async () => {
                try {
                    const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pipeline-qwen-opus/${encodeURIComponent(jobId)}`;
                    const job = await fetch(url).then(r => r.json());
                    scQOJob.value = job;
                    // live image-block detail while Qwen is enriching a pair
                    if (job.qwen_worker && job.qwen_worker.current_pair_id) {
                        try { await scQORefreshActiveRecog(); } catch (e) {}
                    }
                    if (terminal.includes(job.status)) {
                        scQORunning.value = false;
                        scQOClockStop();
                        scQOActiveRecog.value = null;
                        scQOForget();
                        // Закешировать времена завершённого прогона, чтобы они
                        // пережили последующий refresh (scQOJob будет очищен).
                        if (typeof scQOLoadPairTimings === 'function') { try { await scQOLoadPairTimings(); } catch (e) {} }
                        if (typeof scLoadPairCompareStatuses === 'function') { try { await scLoadPairCompareStatuses(); } catch (e) {} }
                        if (typeof scLoadUnifiedFlat === 'function') { try { await scLoadUnifiedFlat(); } catch (e) {} }
                        return;
                    }
                    scQOPollTimer = setTimeout(tick, 3000);
                } catch (e) { scQOPollTimer = setTimeout(tick, 5000); }
            };
            scQORunning.value = true;
            scQOClockStart();
            tick();
        }
        async function scQOCancel() {
            if (!scQOJob.value || !scQOJob.value.job_id) return;
            const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pipeline-qwen-opus/${encodeURIComponent(scQOJob.value.job_id)}/cancel`;
            try { await fetch(url, { method: 'POST' }); } catch (e) {}
        }

        // ─── Pipeline V2 controlled run («Запустить V2» в «Связь блоков») ──
        // Кнопка POST'ит на /pipeline-v2/{sid}/pairs/{pid}/run (state-changing
        // controlled endpoint), затем polling'ит run-status. ui-payload —
        // read-only, его не трогаем. Прогон offline (без моделей).
        const scPv2RunByPair = reactive({});        // pid -> job {status, ...}
        const scPv2RunArtifactPairs = reactive({}); // pid -> true (артефакты есть)
        const scPv2RunModal = ref(null);            // confirm-модалка
        let scPv2RunTimers = {};                    // pid -> setTimeout id

        function scPv2RunState(pid) {
            const j = scPv2RunByPair[pid];
            const s = j && j.status;
            if (s === 'queued' || s === 'running') return 'running';
            if (s === 'completed') return 'completed';
            if (s === 'failed' || s === 'failed_interrupted' || s === 'cancelled') return 'failed';
            if (scPv2RunArtifactPairs[pid]) return 'has_artifacts';
            return 'idle';
        }
        function scPv2RunBtnLabel(pid) {
            const st = scPv2RunState(pid);
            if (st === 'running') return '⏳ V2…';
            if (st === 'failed') return '↻ V2';
            if (st === 'has_artifacts' || st === 'completed') return '↻ V2';
            return '▶ V2';
        }
        function scPv2RunBtnTitle(pid) {
            const st = scPv2RunState(pid);
            if (st === 'running') return 'Pipeline V2 выполняется…';
            if (st === 'failed') return 'Повторить Pipeline V2 (последний прогон не удался)';
            if (st === 'has_artifacts' || st === 'completed') return 'Перезапустить Pipeline V2 (создаст backup существующих артефактов)';
            return 'Запустить backend Pipeline V2 и создать артефакты для этой пары';
        }
        function scPv2RunErrorFor(pid) {
            const j = scPv2RunByPair[pid];
            return (j && (j.status === 'failed' || j.status === 'failed_interrupted') && j.error) ? j.error : '';
        }
        async function scPv2RunLoadArtifactPairs() {
            if (!scSession.value || !scSession.value.id) return;
            try {
                const url = `/api/stage-comparison/pipeline-v2/${encodeURIComponent(scSession.value.id)}/ui-payload`;
                const data = await fetch(url).then(r => r.ok ? r.json() : null);
                const list = (data && (data.available_pairs
                    || (data.payload && data.payload.available_pairs))) || [];
                Object.keys(scPv2RunArtifactPairs).forEach(k => delete scPv2RunArtifactPairs[k]);
                list.forEach(pid => { scPv2RunArtifactPairs[pid] = true; });
            } catch (e) { /* read-only best-effort */ }
        }
        function scPv2RunOpenModal(p) {
            if (!p || !p.left || !p.right) return;
            if (scPv2RunState(p.id) === 'running') return;
            const st = scPv2RunState(p.id);
            scPv2RunModal.value = {
                pair_id: p.id,
                left_name: (p.left && p.left.filename) || '?',
                right_name: (p.right && p.right.filename) || '?',
                is_rerun: (st === 'has_artifacts' || st === 'completed' || st === 'failed'),
                typed: '', busy: false, error: ''
            };
        }
        function scPv2RunStopPoll(pid) {
            if (scPv2RunTimers[pid]) { clearTimeout(scPv2RunTimers[pid]); delete scPv2RunTimers[pid]; }
        }
        function scPv2RunPoll(pid, jobId) {
            scPv2RunStopPoll(pid);
            const terminal = ['completed', 'failed', 'cancelled', 'failed_interrupted'];
            const tick = async () => {
                if (!scSession.value || !scSession.value.id) return;
                try {
                    const url = `/api/stage-comparison/pipeline-v2/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(pid)}/run-status/${encodeURIComponent(jobId)}`;
                    const r = await fetch(url);
                    if (r.ok) {
                        const job = await r.json();
                        scPv2RunByPair[pid] = job;
                        if (terminal.includes(job.status)) {
                            scPv2RunStopPoll(pid);
                            if (job.status === 'completed') scPv2RunArtifactPairs[pid] = true;
                            try { await scPv2RunLoadArtifactPairs(); } catch (e) {}
                            if (typeof scPv2LpLoad === 'function' && scPv2LpVisible.value
                                && scPv2LpPairId.value === pid) {
                                try { await scPv2LpLoad(); } catch (e) {}
                            }
                            return;
                        }
                    }
                    scPv2RunTimers[pid] = setTimeout(tick, 3000);
                } catch (e) { scPv2RunTimers[pid] = setTimeout(tick, 5000); }
            };
            tick();
        }
        async function scPv2RunSubmit() {
            const mdl = scPv2RunModal.value;
            if (!mdl || !scSession.value || !scSession.value.id) return;
            if (mdl.typed !== mdl.pair_id) {
                mdl.error = 'Введите точный pair_id для подтверждения'; return;
            }
            mdl.busy = true; mdl.error = '';
            const pid = mdl.pair_id;
            try {
                const url = `/api/stage-comparison/pipeline-v2/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(pid)}/run`;
                const r = await fetch(url, {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        mode: 'dry_run', confirm: true,
                        confirm_session_id: scSession.value.id, confirm_pair_id: pid,
                        rerun_existing: !!mdl.is_rerun, create_backup: true
                    })
                });
                if (!r.ok) {
                    let msg = 'HTTP ' + r.status;
                    try { const e = await r.json(); if (e && e.detail) msg = e.detail; } catch (x) {}
                    if (r.status === 409) msg = 'Уже есть артефакты или идёт прогон (' + msg + ')';
                    mdl.busy = false; mdl.error = msg; return;
                }
                const data = await r.json();
                scPv2RunByPair[pid] = { status: 'queued', job_id: data.job_id };
                scPv2RunModal.value = null;
                scPv2RunPoll(pid, data.job_id);
            } catch (e) {
                mdl.busy = false; mdl.error = (e && e.message) || String(e);
            }
        }
        // Подгрузить пары с артефактами при смене сессии (бейдж «Запустить» vs
        // «Перезапустить»).
        watch(() => scSession.value && scSession.value.id, (sid) => {
            Object.keys(scPv2RunByPair).forEach(k => delete scPv2RunByPair[k]);
            Object.keys(scPv2RunArtifactPairs).forEach(k => delete scPv2RunArtifactPairs[k]);
            if (sid) { scPv2RunLoadArtifactPairs(); }
        });

        async function scOpenPair(pair) {
            if (!pair || !pair.left || !pair.right) return;
            scActivePair.value = pair;
            scSelectedLeft.value = null;
            scSelectedRight.value = null;
            scSelectedSlotLeft.value = null;
            scSelectedSlotRight.value = null;
            scTextLLMDiff.value = null;
            scGraphicSummary.value = null;
            scGraphicPreview.value = null;
            // Сбрасываем V2-данные прошлой пары, чтобы не показать stale-список.
            scV2Data.value = null;
            scV2Error.value = '';
            for (const k of Object.keys(scV2Selected)) delete scV2Selected[k];
            scCanvasNat.left = null;
            scCanvasNat.right = null;
            scAlignment.value = null;
            scAlignmentActionError.value = '';
            scActiveLinkKey.value = null;
            scVisibleSlot.value = 1;
            scVisibleSlotLeft.value = 1;
            scVisibleSlotRight.value = 1;
            // Очищаем slot-refs и кеш высот прошлой пары
            for (const k of Object.keys(scSlotRefs)) delete scSlotRefs[k];
            for (const k of Object.keys(scSlotHeights)) delete scSlotHeights[k];
            await scLoadPairData();
            await scLoadAlignment();
            // Загружаем analysis_mode чтобы кнопка «Блоки без связей» сразу
            // отражала текущее состояние.
            try { await scLoadAnalysisMode(); } catch (_) {}
            // Если открыт MD-вьювер — подтянуть enriched MD новой пары.
            if (scShowMd.value) scLoadEnrichedMd();
            scTab.value = 'links';
        }

        // ─── Поповер «упавшие блоки» ────────────────────────────────────────
        function scToggleFailedPopover(pair) {
            if (!pair || !pair.id) return;
            if (scFailedPopoverPairId.value === pair.id) {
                scFailedPopoverPairId.value = null;
                return;
            }
            scFailedPopoverPairId.value = pair.id;
            scLoadFailedBlocks(pair.id);
        }

        async function scLoadFailedBlocks(pairId) {
            if (!scSession.value || !pairId) return;
            scFailedBlocks.value = [];
            scFailedBlocksError.value = '';
            scFailedBlocksLoading.value = true;
            const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(pairId)}/failed-blocks`;
            try {
                const r = await fetch(url);
                if (!r.ok) throw new Error('HTTP ' + r.status);
                const data = await r.json();
                // Если поповер за время запроса переключили на другую пару — не перетираем.
                if (scFailedPopoverPairId.value !== pairId) return;
                scFailedBlocks.value = Array.isArray(data.blocks) ? data.blocks : [];
            } catch (e) {
                if (scFailedPopoverPairId.value === pairId) {
                    scFailedBlocksError.value = 'Не удалось загрузить список: ' + e;
                }
            } finally {
                if (scFailedPopoverPairId.value === pairId) scFailedBlocksLoading.value = false;
            }
        }

        async function scGotoFailedBlock(pair, fb) {
            if (!pair || !fb) return;
            scFailedPopoverPairId.value = null;
            // Если пара ещё не открыта (или открыта другая) — открыть и дождаться загрузки.
            if (!scActivePair.value || scActivePair.value.id !== pair.id) {
                await scOpenPair(pair);
            } else if (scTab.value !== 'links') {
                scTab.value = 'links';
            }
            // На MD-вьювере подсветка блока-оверлея не сработает — переключимся на PDF.
            if (scShowMd.value) {
                try { await scToggleMdView(); } catch (_) {}
            }
            scFocusBlock(fb.side, fb.page, fb.side_block_id);
        }

        function scFocusBlock(side, page, blockId) {
            if (!side) return;
            // Сделать слот страницы видимым и проскроллить к нему (reuse существующей навигации).
            _scScrollPdfToPage(side, page);
            if (!blockId) return;  // нет side_block_id — ограничиваемся прокруткой к странице
            const elId = 'sc-block-' + side + '-' + blockId;
            let attempts = 0;
            const tryFocus = () => {
                const el = document.getElementById(elId);
                if (!el) {
                    if (attempts++ < 8) { setTimeout(tryFocus, 150); }
                    return;
                }
                el.scrollIntoView({behavior: 'smooth', block: 'center'});
                el.classList.add('sc-block-focus-flash');
                setTimeout(() => el.classList.remove('sc-block-focus-flash'), 2200);
            };
            // nextTick + задержка: слот/картинки рендерятся асинхронно после смены видимого слота.
            nextTick(() => setTimeout(tryFocus, 200));
        }

        async function scLoadPairData() {
            if (!scSession.value || !scActivePair.value) return;
            const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}`;
            try {
                const r = await fetch(url);
                if (!r.ok) throw new Error('HTTP ' + r.status);
                scPairData.value = await r.json();
                // Подсасываем alignment если он пришёл в pair view
                if (scPairData.value.alignment && Array.isArray(scPairData.value.alignment.items)) {
                    scAlignment.value = {
                        items: scPairData.value.alignment.items,
                        left_page_count: scPairData.value.left_page_count,
                        right_page_count: scPairData.value.right_page_count,
                    };
                }
            } catch (e) {
                scError.value = 'Не удалось загрузить пару: ' + e;
            }
        }

        async function scLoadAlignment() {
            if (!scSession.value || !scActivePair.value) return;
            const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/page-alignment`;
            try {
                const r = await fetch(url);
                if (!r.ok) throw new Error('HTTP ' + r.status);
                const data = await r.json();
                scAlignment.value = {
                    items: (data.alignment && data.alignment.items) || [],
                    left_page_count: data.left_page_count || 0,
                    right_page_count: data.right_page_count || 0,
                };
            } catch (e) {
                scError.value = 'Не удалось загрузить карту страниц: ' + e;
            }
        }

        function scPageImageUrl(side, page) {
            if (!scSession.value || !scActivePair.value) return '';
            if (!page) return '';
            return `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/page-image?side=${side}&page=${page}&target_long_side=1400`;
        }

        // Загрузка содержимого left_enriched.md / right_enriched.md для MD-вьювера.
        async function scLoadEnrichedMd() {
            if (!scSession.value || !scActivePair.value) return;
            scMdViewLoading.value = true;
            scMdViewError.value = '';
            scMdView.left = null;
            scMdView.right = null;
            const sid = encodeURIComponent(scSession.value.id);
            const pid = encodeURIComponent(scActivePair.value.id);
            try {
                const [left, right] = await Promise.all(['left', 'right'].map(async (side) => {
                    const r = await fetch(`/api/stage-comparison/sessions/${sid}/pairs/${pid}/enriched-md?side=${side}`);
                    if (!r.ok) throw new Error('HTTP ' + r.status);
                    return await r.json();
                }));
                scMdView.left = left;
                scMdView.right = right;
            } catch (e) {
                scMdViewError.value = 'Не удалось загрузить enriched MD: ' + e;
            } finally {
                scMdViewLoading.value = false;
            }
        }

        // ── Якорение позиции при переключении PDF ↔ MD ──────────────────────
        // PDF-страница N стороны ↔ заголовок `## СТРАНИЦА N` в enriched MD.
        // Узлы страниц в MD: `.sc-md-h-page` (HTML-режим) / `.sc-md-page` (подсветка).
        function _scPdfTopPageBySide() {
            const res = {left: null, right: null};
            const items = scAlignmentItems.value || [];
            for (const side of ['left', 'right']) {
                const info = _scTopSlotInfo(side);
                if (!info) continue;
                const it = items.find(x => x.slot === info.slot);
                if (!it) continue;
                res[side] = side === 'left' ? it.left_page : it.right_page;
            }
            return res;
        }
        function _scMdPageNodes(side) {
            const pane = scMdPaneRefs[side];
            if (!pane) return [];
            const out = [];
            pane.querySelectorAll('.sc-md-h-page, .sc-md-page').forEach((n) => {
                const m = (n.textContent || '').match(/СТРАНИЦА\s+(\d+)/);
                if (m) out.push({page: parseInt(m[1], 10), node: n});
            });
            return out;
        }
        function _scScrollMdPaneToPage(side, page) {
            const pane = scMdPaneRefs[side];
            if (!pane || page == null) return;
            const nodes = _scMdPageNodes(side);
            if (!nodes.length) return;
            let hit = nodes.find(x => x.page === page);
            if (!hit) hit = nodes.filter(x => x.page <= page).pop() || nodes[0];
            const paneRect = pane.getBoundingClientRect();
            const nodeRect = hit.node.getBoundingClientRect();
            const sticky = pane.firstElementChild ? pane.firstElementChild.offsetHeight : 0;
            pane.scrollTop = Math.max(0, pane.scrollTop + (nodeRect.top - paneRect.top) - sticky);
        }
        function _scMdTopPage(side) {
            const pane = scMdPaneRefs[side];
            if (!pane) return null;
            const nodes = _scMdPageNodes(side);
            if (!nodes.length) return null;
            const probe = pane.getBoundingClientRect().top + 60;
            let best = null;
            for (const x of nodes) {
                const top = x.node.getBoundingClientRect().top;
                if (top <= probe) best = x.page;
                else if (best == null) best = x.page;  // все ниже probe → первая
            }
            return best;
        }
        function _scScrollPdfToPage(side, page) {
            if (page == null) return;
            const items = scAlignmentItems.value || [];
            let slot = items.find(it => (side === 'left' ? it.left_page : it.right_page) === page);
            if (!slot) slot = items.find(it => it.left_page === page || it.right_page === page);
            if (!slot) return;
            // Сделать целевой slot видимым для виртуализации, затем проскроллить к его верху.
            scVisibleSlot.value = slot.slot;
            scVisibleSlotLeft.value = slot.slot;
            scVisibleSlotRight.value = slot.slot;
            nextTick(() => {
                requestAnimationFrame(() => {
                    for (const s of ['left', 'right']) {
                        const pane = scPaneRefs[s];
                        const node = scSlotRefs[s + ':' + slot.slot];
                        if (pane && node) pane.scrollTop = Math.max(0, node.offsetTop);
                    }
                    _scUpdateVisibleSlot();
                });
            });
        }

        // Переключение режима PDF ↔ MD в двухпанельном вьювере с сохранением
        // позиции: на стр. N PDF → к `## СТРАНИЦА N` в MD, и обратно.
        async function scToggleMdView() {
            if (!scShowMd.value) {
                // PDF → MD
                const anchor = _scPdfTopPageBySide();
                scShowMd.value = true;
                await scLoadEnrichedMd();
                await nextTick();
                requestAnimationFrame(() => {
                    _scScrollMdPaneToPage('left', anchor.left);
                    _scScrollMdPaneToPage('right', anchor.right);
                });
            } else {
                // MD → PDF
                const page = _scMdTopPage('left');
                const side = page != null ? 'left' : 'right';
                const targetPage = page != null ? page : _scMdTopPage('right');
                scShowMd.value = false;
                await nextTick();
                _scScrollPdfToPage(side, targetPage);
            }
        }

        // Синхронная прокрутка MD-панелей (как в PDF, но пропорционально —
        // у двух enriched MD разная длина, поэтому синхронизируем по доле
        // прокрутки). Управляется тем же чекбоксом «Синхронизировать прокрутку».
        let _scMdSyncing = false;
        function scOnMdPaneScroll(side, ev) {
            if (!scSyncScroll.value || _scMdSyncing) return;
            const other = side === 'left' ? 'right' : 'left';
            const pane = ev.target;
            const otherPane = scMdPaneRefs[other];
            if (!pane || !otherPane) return;
            const denom = pane.scrollHeight - pane.clientHeight;
            const ratio = denom > 0 ? pane.scrollTop / denom : 0;
            const otherDenom = otherPane.scrollHeight - otherPane.clientHeight;
            _scMdSyncing = true;
            otherPane.scrollTop = ratio * otherDenom;
            requestAnimationFrame(() => {
                requestAnimationFrame(() => { _scMdSyncing = false; });
            });
        }

        // Подсветка enriched MD по типу строки:
        //   ## СТРАНИЦА …            → жёлтый
        //   ### BLOCK [TEXT] …       → зелёный
        //   ### Графический блок … / ### BLOCK [IMAGE] … → красный (image-блоки)
        // Возвращает безопасный HTML (контент экранируется), по одной <div> на строку.
        function scMdHighlightHtml(text) {
            if (!text) return '';
            const esc = (s) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
            return text.split('\n').map((line) => {
                let cls = 'sc-md-line';
                if (/^##\s+СТРАНИЦА(?:\s|$)/.test(line)) cls += ' sc-md-page';
                else if (/^###\s+BLOCK\s+\[TEXT\]/.test(line)) cls += ' sc-md-text';
                else if (/^###\s+BLOCK\s+\[IMAGE\]/.test(line) || /^###\s+Графический блок/.test(line)) cls += ' sc-md-image';
                const safe = esc(line);
                return `<div class="${cls}">${safe === '' ? '&nbsp;' : safe}</div>`;
            }).join('');
        }

        // Чинит GFM-таблицы, у которых подпись/текст приклеены к строке
        // заголовка на одной строке (артефакт OCR): число ячеек заголовка
        // получается больше, чем у строки-разделителя `|---|---|`, и marked
        // не распознаёт таблицу. Отделяем лишний префикс в отдельный абзац.
        function _scRepairGluedTables(md) {
            const isDelim = (l) => /^\s*\|?\s*:?-{2,}:?\s*(\|\s*:?-{2,}:?\s*)*\|?\s*$/.test(l);
            const cells = (l) => {
                let s = l.trim();
                if (s.startsWith('|')) s = s.slice(1);
                if (s.endsWith('|')) s = s.slice(0, -1);
                return s.split('|');
            };
            const lines = md.split('\n');
            const out = [];
            for (let i = 0; i < lines.length; i++) {
                const cur = lines[i], nxt = lines[i + 1];
                if (nxt !== undefined && isDelim(nxt) && cur.includes('|')) {
                    const dcols = cells(nxt).length;
                    const parts = cells(cur);
                    if (parts.length > dcols) {
                        const header = '| ' + parts.slice(parts.length - dcols).map(c => c.trim()).join(' | ') + ' |';
                        const caption = parts.slice(0, parts.length - dcols).join('|').trim();
                        if (caption) { out.push(caption); out.push(''); }
                        out.push(header);
                        continue;
                    }
                }
                out.push(cur);
            }
            return out.join('\n');
        }

        // Красивый рендер enriched MD как HTML (через marked) + цветовая
        // подсветка заголовков по типу блока (## СТРАНИЦА / BLOCK [TEXT] / image).
        function scMdRenderHtml(text) {
            if (!text) return '';
            text = _scRepairGluedTables(text);
            let html;
            if (typeof marked !== 'undefined') {
                try { html = marked.parse(text, { breaks: true, gfm: true }); }
                catch (e) { html = '<pre>' + text.replace(/&/g, '&amp;').replace(/</g, '&lt;') + '</pre>'; }
            } else {
                html = '<pre>' + text.replace(/&/g, '&amp;').replace(/</g, '&lt;') + '</pre>';
            }
            return html
                .replace(/<h2([^>]*)>(\s*СТРАНИЦА)/g, '<h2$1 class="sc-md-h-page">$2')
                .replace(/<h3([^>]*)>(\s*BLOCK\s*\[TEXT\])/g, '<h3$1 class="sc-md-h-text">$2')
                .replace(/<h3([^>]*)>(\s*(?:BLOCK\s*\[IMAGE\]|Графический блок))/g, '<h3$1 class="sc-md-h-image">$2');
        }

        let _scSlotRO = null;
        function _scEnsureSlotRO() {
            if (_scSlotRO || typeof ResizeObserver === 'undefined') return _scSlotRO;
            _scSlotRO = new ResizeObserver((entries) => {
                for (const e of entries) {
                    const el = e.target;
                    const side = el.dataset.scSide;
                    const slotId = parseInt(el.dataset.scSlot, 10);
                    if (!side || !Number.isFinite(slotId)) continue;
                    const h = Math.round(e.contentRect.height);
                    if (h <= 0) continue;
                    const prev = scSlotHeights[slotId] || {};
                    if (prev[side] !== h) {
                        scSlotHeights[slotId] = {...prev, [side]: h};
                    }
                }
            });
            return _scSlotRO;
        }

        function scOnPageImageLoad(side, slot, ev) {
            const img = ev.target;
            // Запомнить natural dims последней страницы стороны (для возможных перерасчётов)
            scCanvasNat[side] = {w: img.naturalWidth, h: img.naturalHeight, dw: img.clientWidth, dh: img.clientHeight};
            // Зарегистрировать высоту контента для синхронизации высоты слота
            // между левой и правой панелями. Без этого slot N справа и слева могут
            // быть разной высоты — и скролл разъезжается.
            if (slot && slot.slot != null) {
                img.dataset.scSide = side;
                img.dataset.scSlot = String(slot.slot);
                const ro = _scEnsureSlotRO();
                if (ro) ro.observe(img);
                const h = Math.round(img.clientHeight);
                if (h > 0) {
                    const prev = scSlotHeights[slot.slot] || {};
                    if (prev[side] !== h) {
                        scSlotHeights[slot.slot] = {...prev, [side]: h};
                    }
                }
            }
        }

        // Совместимость со старым кодом (если где-то ещё используется)
        function scOnImageLoad(side, ev) { scOnPageImageLoad(side, null, ev); }

        function scSlotBlocks(side, slot) {
            if (!scPairData.value) return [];
            const page = side === 'left' ? slot.left_page : slot.right_page;
            if (!page) return [];
            const blocks = side === 'left' ? (scPairData.value.left_blocks || []) : (scPairData.value.right_blocks || []);
            return blocks.filter(b => (b.page || 1) === page);
        }

        function scBlankPageStyle() {
            // Растягивается на всю свободную высоту слота через flex (см. .sc-blank-page).
            // min-height — страховка, когда у slot нет картинки ни с одной стороны
            // и min-height из scSlotContainerStyle не применился.
            return {minHeight: Math.round(280 * scZoom.value) + 'px', width: '100%'};
        }

        function scBlockOverlayStyle(side, block, slot) {
            // Coords из result.json. bbox_norm в [0,1] — самый надёжный.
            let nx0, ny0, nx1, ny1;
            if (block.bbox_norm) {
                [nx0, ny0, nx1, ny1] = block.bbox_norm;
            } else if (block.bbox && block.page_width && block.page_height) {
                nx0 = block.bbox[0] / block.page_width;
                ny0 = block.bbox[1] / block.page_height;
                nx1 = block.bbox[2] / block.page_width;
                ny1 = block.bbox[3] / block.page_height;
            } else {
                return {display: 'none'};
            }
            const style = {
                position: 'absolute',
                left:   (nx0 * 100).toFixed(3) + '%',
                top:    (ny0 * 100).toFixed(3) + '%',
                width:  ((nx1 - nx0) * 100).toFixed(3) + '%',
                height: ((ny1 - ny0) * 100).toFixed(3) + '%',
            };
            // Если блок связан — окрашиваем рамку цветом связи. Selected
            // приоритет выше (рамка остаётся синей при ручном выборе нового блока).
            const sel = (side === 'left' ? scSelectedLeft : scSelectedRight).value;
            if (sel !== block.id) {
                const info = scBlockLinkInfo(side, block.id);
                if (info) {
                    style.borderColor = info.color;
                }
            }
            return style;
        }

        // Legacy совместимость: возвращает method первой найденной связи.
        function scIsBlockLinked(side, blockId) {
            const info = scBlockLinkInfo(side, blockId);
            return info ? (info.link.method || null) : null;
        }
        function scBlockOverlayClass(side, blockId, slot) {
            const cls = [];
            const sel = (side === 'left' ? scSelectedLeft : scSelectedRight).value;
            if (sel === blockId) cls.push('selected');
            const info = scBlockLinkInfo(side, blockId);
            if (info) {
                cls.push('linked');
                if (info.isStale) cls.push('linked-stale');
                if (info.isCross) cls.push('linked-cross');
                if (info.isManual && !info.isCross && !info.isStale) cls.push('linked-manual');
                if (scActiveLinkKey.value === info.key) cls.push('linked-active');
            }
            return cls.join(' ');
        }
        function scSelectBlock(side, blockId, slot) {
            // Если кликнули на уже связанный блок — выбираем эту связь как
            // активную (для компактной панели удаления), не трогая
            // selected-state создания новой связи.
            const info = scBlockLinkInfo(side, blockId);
            if (info) {
                scActiveLinkKey.value = (scActiveLinkKey.value === info.key) ? null : info.key;
                // Сбрасываем selected на этой стороне, чтобы не путать с активной связью
                if (side === 'left') {
                    scSelectedLeft.value = null;
                    scSelectedSlotLeft.value = null;
                } else {
                    scSelectedRight.value = null;
                    scSelectedSlotRight.value = null;
                }
                return;
            }
            // Несвязанный блок — обычное toggle-выделение для создания новой связи
            scActiveLinkKey.value = null;
            if (side === 'left') {
                scSelectedLeft.value = scSelectedLeft.value === blockId ? null : blockId;
                scSelectedSlotLeft.value = slot ? slot.slot : null;
            } else {
                scSelectedRight.value = scSelectedRight.value === blockId ? null : blockId;
                scSelectedSlotRight.value = slot ? slot.slot : null;
            }
        }
        // Явный helper из ТЗ — обёртка, использующая scSelectBlock с учётом инфо
        function scSelectLinkedBlock(side, blockId) {
            const info = scBlockLinkInfo(side, blockId);
            if (!info) return false;
            scActiveLinkKey.value = (scActiveLinkKey.value === info.key) ? null : info.key;
            return true;
        }
        function scLinkVisualIndex(link) {
            if (!link) return null;
            const info = scLinkVisualMap.value.get('left|' + link.left_block_id);
            return info ? info.number : null;
        }

        // ── Sync scroll ────────────────────────────────────────────────────
        // Алгоритм: для левой панели определяем slot, в центре viewport, и
        // относительную позицию внутри его карточки. На правой панели
        // ставим scrollTop так, чтобы тот же slot оказался в том же месте.
        function _scTopSlotInfo(side) {
            const pane = scPaneRefs[side];
            if (!pane) return null;
            const items = scAlignmentItems.value;
            const probeY = pane.scrollTop + pane.clientHeight * 0.4;  // 40% от верха
            let best = null;
            for (const it of items) {
                const node = scSlotRefs[side + ':' + it.slot];
                if (!node) continue;
                const top = node.offsetTop;
                const bot = top + node.offsetHeight;
                if (probeY >= top && probeY < bot) {
                    const rel = (probeY - top) / Math.max(1, node.offsetHeight);
                    return {slot: it.slot, rel: rel};
                }
                if (best == null || Math.abs(probeY - (top + node.offsetHeight/2)) <
                                    Math.abs(probeY - (best.center))) {
                    best = {slot: it.slot, rel: 0.5, center: top + node.offsetHeight/2};
                }
            }
            return best;
        }

        function _scScrollToSlot(side, slotId, rel) {
            const pane = scPaneRefs[side];
            const node = scSlotRefs[side + ':' + slotId];
            if (!pane || !node) return;
            const target = node.offsetTop + node.offsetHeight * rel - pane.clientHeight * 0.4;
            pane.scrollTop = Math.max(0, target);
        }

        function scOnPaneScroll(side, ev) {
            // Виртуализация: обновляем видимый slot всегда
            _scUpdateVisibleSlot();
            if (!scSyncScroll.value) return;
            if (scIsSyncing.value) return;
            const info = _scTopSlotInfo(side);
            const other = side === 'left' ? 'right' : 'left';
            const pane = scPaneRefs[side];
            const otherPane = scPaneRefs[other];
            scIsSyncing.value = true;
            try {
                // Slot Id в обоих панелях одинаковый — это карта по вертикали.
                if (info) _scScrollToSlot(other, info.slot, info.rel);
                // Горизонталь: зум общий → картинки одинаковой ширины,
                // повторяем scrollLeft 1:1.
                if (pane && otherPane) {
                    otherPane.scrollLeft = pane.scrollLeft;
                }
            } finally {
                // Снимаем флаг в следующем тике, чтобы scroll event'ы успели отработать
                requestAnimationFrame(() => {
                    requestAnimationFrame(() => { scIsSyncing.value = false; });
                });
            }
        }

        // ── Zoom ───────────────────────────────────────────────────────────
        function _scSaveAnchor() {
            // Сохраним позицию текущего slot перед изменением zoom,
            // чтобы после ресайза вернуться.
            return _scTopSlotInfo('left') || _scTopSlotInfo('right');
        }
        function _scRestoreAnchor(anchor) {
            if (!anchor) return;
            nextTick(() => {
                _scScrollToSlot('left', anchor.slot, anchor.rel);
                if (scSyncScroll.value) _scScrollToSlot('right', anchor.slot, anchor.rel);
            });
        }
        function scZoomBy(factor) {
            const anchor = _scSaveAnchor();
            scZoom.value = Math.max(0.2, Math.min(4.0, scZoom.value * factor));
            _scRestoreAnchor(anchor);
        }
        function scZoomReset() {
            const anchor = _scSaveAnchor();
            scZoom.value = 1.0;
            _scRestoreAnchor(anchor);
        }
        function scZoomFitWidth() {
            // «По ширине» — pane целиком вмещает картинку без горизонтального скролла.
            // Картинка рендерится с width: zoom*100% относительно pane'а, поэтому zoom=1 ≈ fit.
            scZoomReset();
        }

        // ── Ctrl+wheel zoom-to-cursor (Adobe Acrobat style) ────────────────
        // Привязываемся к slot'у под курсором (ID + относительная позиция
        // внутри его карточки) — заголовки/маргины слотов и виртуализация
        // не масштабируются с zoom'ом, поэтому "contentY * factor" уехал бы
        // на чужой лист. После reflow перечитываем offsetTop того же slot'а
        // и ставим скролл так, чтобы та же относительная точка осталась
        // под курсором.
        function scOnPaneWheel(side, ev) {
            if (!(ev.ctrlKey || ev.metaKey)) return;        // обычное колесо — стандартный скролл
            ev.preventDefault();
            const pane = scPaneRefs[side];
            if (!pane) return;
            const rect = pane.getBoundingClientRect();
            const cursorX = ev.clientX - rect.left;
            const cursorY = ev.clientY - rect.top;
            const startScrollLeft = pane.scrollLeft;
            const contentY = pane.scrollTop + cursorY;
            // Slot под курсором + относительная позиция внутри slot'а (0..1)
            const items = scAlignmentItems.value || [];
            let anchorSlot = null;
            let anchorRel = 0;
            for (const it of items) {
                const node = scSlotRefs[side + ':' + it.slot];
                if (!node) continue;
                const top = node.offsetTop;
                const bot = top + node.offsetHeight;
                if (contentY >= top && contentY < bot) {
                    anchorSlot = it.slot;
                    anchorRel = (contentY - top) / Math.max(1, node.offsetHeight);
                    break;
                }
            }
            const oldZoom = scZoom.value;
            const step = ev.deltaY < 0 ? 1.1 : 1 / 1.1;
            const newZoom = Math.max(0.2, Math.min(4.0, oldZoom * step));
            if (newZoom === oldZoom) return;
            const factor = newZoom / oldZoom;
            scZoom.value = newZoom;
            scIsSyncing.value = true;                       // отключаем sync-scroll на этот тик
            nextTick(() => {
                // Вертикаль: восстанавливаем точно ту же точку того же slot'а
                if (anchorSlot != null) {
                    const node = scSlotRefs[side + ':' + anchorSlot];
                    if (node) {
                        const newContentY = node.offsetTop + node.offsetHeight * anchorRel;
                        pane.scrollTop = Math.max(0, newContentY - cursorY);
                    }
                }
                // Горизонталь: картинка масштабируется линейно с zoom'ом,
                // contentX_new = factor * (startScrollLeft + cursorX)
                pane.scrollLeft = Math.max(0, startScrollLeft * factor + cursorX * (factor - 1));
                // Противоположная панель: тот же slot, та же relPos, тот же cursorY
                const otherSide = side === 'left' ? 'right' : 'left';
                const otherPane = scPaneRefs[otherSide];
                if (otherPane && anchorSlot != null) {
                    const otherNode = scSlotRefs[otherSide + ':' + anchorSlot];
                    if (otherNode) {
                        const otherContentY = otherNode.offsetTop + otherNode.offsetHeight * anchorRel;
                        otherPane.scrollTop = Math.max(0, otherContentY - cursorY);
                    }
                }
                requestAnimationFrame(() => {
                    requestAnimationFrame(() => { scIsSyncing.value = false; });
                });
            });
        }

        // ── LMB hand-pan (Adobe Acrobat style) ─────────────────────────────
        // Зажатие ЛКМ + перетаскивание двигает pane как «рука». Если движение
        // меньше порога — это обычный клик (попадёт в scSelectBlock на блоке).
        // Если больше — глотаем следующий click чтобы случайно не выделить блок.
        function scOnPanePanStart(side, ev) {
            if (ev.button !== 0) return;                    // только ЛКМ
            if (ev.ctrlKey || ev.metaKey || ev.altKey || ev.shiftKey) return;
            const target = ev.target;
            if (target && target.closest && target.closest(
                'button, a, input, select, textarea, label, .sc-block-link-badge'
            )) return;
            const pane = scPaneRefs[side];
            if (!pane) return;
            const startX = ev.clientX;
            const startY = ev.clientY;
            const startScrollLeft = pane.scrollLeft;
            const startScrollTop  = pane.scrollTop;
            let moved = false;
            const PAN_THRESHOLD = 5;
            const onMove = (e) => {
                const dx = e.clientX - startX;
                const dy = e.clientY - startY;
                if (!moved && (Math.abs(dx) + Math.abs(dy) > PAN_THRESHOLD)) {
                    moved = true;
                    pane.style.cursor = 'grabbing';
                    pane.classList.add('sc-viewer__pane--panning');
                }
                if (moved) {
                    pane.scrollLeft = startScrollLeft - dx;
                    pane.scrollTop  = startScrollTop  - dy;
                    e.preventDefault();
                }
            };
            const onUp = (_e) => {
                window.removeEventListener('mousemove', onMove, true);
                window.removeEventListener('mouseup', onUp, true);
                pane.style.cursor = '';
                pane.classList.remove('sc-viewer__pane--panning');
                if (moved) {
                    const swallow = (ce) => {
                        ce.stopPropagation();
                        ce.preventDefault();
                        window.removeEventListener('click', swallow, true);
                    };
                    window.addEventListener('click', swallow, true);
                    requestAnimationFrame(() => {
                        requestAnimationFrame(() => {
                            window.removeEventListener('click', swallow, true);
                        });
                    });
                }
            };
            window.addEventListener('mousemove', onMove, true);
            window.addEventListener('mouseup', onUp, true);
        }

        // ── Per-pane alignment actions (icon-кнопки рядом с PDF-окнами) ────
        // Старый UI «Карта листов» удалён. Эти функции вызывают backend
        // endpoints insert-blank-side / move-page-side и обновляют viewer.
        function scCurrentSlotForSide(side) {
            const items = scAlignmentItems.value || [];
            if (!items.length) return 0;
            const ref = side === 'left' ? scVisibleSlotLeft : scVisibleSlotRight;
            const slot = ref.value || scVisibleSlot.value || 1;
            const max = items[items.length - 1].slot;
            return Math.max(1, Math.min(slot, max));
        }
        function _scSideRowForSlot(slot) {
            const items = scAlignmentItems.value || [];
            return items.find(it => it.slot === slot) || null;
        }
        function scCanInsertBlankSide(_side) {
            return !!(scSession.value && scActivePair.value && (scAlignmentItems.value || []).length > 0);
        }
        function scCanMovePageSide(side, direction) {
            if (!scSession.value || !scActivePair.value) return false;
            const items = scAlignmentItems.value || [];
            if (!items.length) return false;
            const slot = scCurrentSlotForSide(side);
            const row = _scSideRowForSlot(slot);
            if (!row) return false;
            const key = side === 'left' ? 'left_page' : 'right_page';
            if (row[key] == null) return false;            // на этой стороне null → двигать нечего
            if (direction === 'up' && slot <= 1) return false;
            if (direction === 'down' && slot >= items.length) return false;
            return true;
        }

        async function _scRefreshAfterAlignment(data) {
            // Обновить локальный alignment, перезагрузить pair data (links могут
            // стать stale/cross-page), при необходимости — graphic-summary и
            // findings/warnings, если они уже были загружены пользователем.
            scAlignment.value = {
                items: data.items || [],
                left_page_count: data.left_page_count || 0,
                right_page_count: data.right_page_count || 0,
            };
            await scLoadPairData();
            if (scGraphicSummary.value) {
                try { await scLoadGraphicSummary(); } catch (_) {}
            }
            if (scTab.value === 'report') {
                try { await scReportLoad(); } catch (_) {}
            }
            await nextTick();
            _scUpdateVisibleSlot();
        }

        async function scInsertBlankSide(side) {
            if (!scSession.value || !scActivePair.value) return;
            if (!scCanInsertBlankSide(side)) return;
            if (scAlignmentActionRunning.value) return;
            const slot = scCurrentSlotForSide(side) || 1;
            scAlignmentActionRunning.value = true;
            scAlignmentActionError.value = '';
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/page-alignment/insert-blank-side`;
                const r = await fetch(url, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({slot: slot, side: side}),
                });
                const data = await r.json().catch(() => ({}));
                if (!r.ok) {
                    scAlignmentActionError.value = String(data.detail || ('HTTP ' + r.status));
                    return;
                }
                await _scRefreshAfterAlignment(data);
            } catch (e) {
                scAlignmentActionError.value = String(e.message || e);
            } finally {
                scAlignmentActionRunning.value = false;
            }
        }

        async function scMovePageSide(side, direction) {
            if (!scSession.value || !scActivePair.value) return;
            if (!scCanMovePageSide(side, direction)) return;
            if (scAlignmentActionRunning.value) return;
            const slot = scCurrentSlotForSide(side);
            scAlignmentActionRunning.value = true;
            scAlignmentActionError.value = '';
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/page-alignment/move-page-side`;
                const r = await fetch(url, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({slot: slot, side: side, direction: direction}),
                });
                const data = await r.json().catch(() => ({}));
                if (!r.ok) {
                    scAlignmentActionError.value = String(data.detail || ('HTTP ' + r.status));
                    return;
                }
                await _scRefreshAfterAlignment(data);
                // Удерживаем фокус на том же slot: после swap "вверх" страница
                // переехала вверх → переключим visible-slot туда же, чтобы
                // следующий клик ↑/↓ работал на ту же страницу.
                const newSlot = direction === 'up'
                    ? Math.max(1, slot - 1)
                    : Math.min((scAlignmentItems.value || []).length, slot + 1);
                if (side === 'left') scVisibleSlotLeft.value = newSlot;
                else scVisibleSlotRight.value = newSlot;
                scAlignmentGoToSlot(newSlot);
            } catch (e) {
                scAlignmentActionError.value = String(e.message || e);
            } finally {
                scAlignmentActionRunning.value = false;
            }
        }

        // ── Перейти к slot в просмотрщике ─────────────────────────────────
        function scAlignmentGoToSlot(slotId) {
            // Не закрываем модалку
            const leftNode = scSlotRefs['left:' + slotId];
            const rightNode = scSlotRefs['right:' + slotId];
            const pane = scPaneRefs.left;
            if (pane && leftNode) {
                pane.scrollTop = leftNode.offsetTop - pane.clientHeight * 0.3;
            }
            const paneR = scPaneRefs.right;
            if (paneR && rightNode) {
                paneR.scrollTop = rightNode.offsetTop - paneR.clientHeight * 0.3;
            }
        }

        // ── Виртуализация (Задача 3) ──────────────────────────────────────
        function scSetSlotRef(side, slotId, el) {
            const key = side + ':' + slotId;
            if (el) {
                scSlotRefs[key] = el;
            } else {
                delete scSlotRefs[key];
            }
        }
        function scIsSlotRendered(slotId) {
            const visible = scVisibleSlot.value;
            return slotId >= (visible - scRenderBufferBefore) && slotId <= (visible + scRenderBufferAfter);
        }
        // Высота «обвязки» слота: 1px верхняя рамка + header (padding 4*2 + ~14px текст + 1px border-bottom) ≈ 24px.
        const _SC_SLOT_CHROME_PX = 26;

        function scSlotContainerStyle(side, slot) {
            // Синхронизируем высоту слота между левой и правой панелями:
            // min-height = max(leftImgHeight, rightImgHeight) + chrome. Если одна из
            // сторон пустая/placeholder, slot всё равно вырастает до высоты «полной»
            // стороны и скролл остаётся синхронным.
            const h = scSlotHeights[slot.slot];
            if (!h) return {};
            const m = Math.max(h.left || 0, h.right || 0);
            if (m <= 0) return {};
            return {minHeight: (m + _SC_SLOT_CHROME_PX) + 'px'};
        }
        function scSlotPlaceholderStyle() {
            // Высота управляется flex (см. .sc-slot-placeholder). min-height —
            // только когда slot ещё не имеет min-height из scSlotContainerStyle.
            return {minHeight: Math.round(280 * scZoom.value) + 'px'};
        }
        function _scComputeVisibleSlotForSide(side) {
            const pane = scPaneRefs[side];
            if (!pane) return null;
            const items = scAlignmentItems.value;
            const probeY = pane.scrollTop + pane.clientHeight * 0.4;
            let best = null;
            let bestDist = Infinity;
            for (const it of items) {
                const node = scSlotRefs[side + ':' + it.slot];
                if (!node) continue;
                const center = node.offsetTop + node.offsetHeight / 2;
                const d = Math.abs(probeY - center);
                if (d < bestDist) { bestDist = d; best = it.slot; }
            }
            return best;
        }
        function _scUpdateVisibleSlot() {
            const lhs = _scComputeVisibleSlotForSide('left');
            const rhs = _scComputeVisibleSlotForSide('right');
            if (lhs != null && lhs !== scVisibleSlotLeft.value) scVisibleSlotLeft.value = lhs;
            if (rhs != null && rhs !== scVisibleSlotRight.value) scVisibleSlotRight.value = rhs;
            // Виртуализация рендерит общий буфер: для неё берём слот левой панели
            // (с фолбэком на правую), как раньше.
            const best = lhs != null ? lhs : (rhs != null ? rhs : scVisibleSlot.value || 1);
            if (best !== scVisibleSlot.value) scVisibleSlot.value = best;
        }

        // ── Unmatched / manual pair management (Задача 2) ─────────────────
        async function scLoadUnmatched() {
            if (!scSession.value) return;
            try {
                const r = await fetch(`/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/unmatched`);
                if (r.ok) scUnmatched.value = await r.json();
            } catch (e) {
                scError.value = 'Не удалось загрузить список несопоставленных PDF: ' + e;
            }
        }
        function scOpenMatchPairDialog(pair) {
            scMatchPairTargetPair.value = pair;
            scMatchPairChoiceRight.value = (pair.right && pair.right.pdf_path) || '';
            scMatchPairError.value = '';
            scMatchPairDialogOpen.value = true;
            scLoadUnmatched();
        }
        function scCloseMatchPairDialog() {
            scMatchPairDialogOpen.value = false;
            scMatchPairTargetPair.value = null;
        }

        // ── Inline сопоставление правого PDF (клик по filename → dropdown) ─
        function scOpenInlineMatch(pair) {
            if (!pair || !pair.id) return;
            if (scInlineMatchPairId.value === pair.id) {
                scCloseInlineMatch();
                return;
            }
            scInlineMatchPairId.value = pair.id;
            scInlineMatchChoice.value = (pair.right && pair.right.pdf_path) || '';
            scInlineMatchFilter.value = '';
            scInlineMatchError.value = '';
            // Загружаем список несопоставленных правых PDF (если ещё не загружен)
            if (!(scUnmatched.value && (scUnmatched.value.right_unmatched || []).length)) {
                scLoadUnmatched();
            }
        }
        function scCloseInlineMatch() {
            scInlineMatchPairId.value = '';
            scInlineMatchChoice.value = '';
            scInlineMatchFilter.value = '';
            scInlineMatchError.value = '';
        }
        // Список опций для inline-выпадашки: текущий выбор + несопоставленные
        // правые PDF, с фильтрацией по подстроке.
        function scInlineMatchOptions() {
            const cur = scInlineMatchChoice.value || '';
            const all = (scUnmatched.value && scUnmatched.value.right_unmatched) || [];
            const filter = (scInlineMatchFilter.value || '').trim().toLowerCase();
            const out = [];
            // Текущий правый PDF (если есть) — отдельной строкой сверху, чтобы
            // оператор видел исходное состояние и мог понять, что меняет.
            if (cur) {
                out.push({pdf_path: cur, filename: cur.split('/').pop(), is_current: true});
            }
            for (const f of all) {
                if (!f || !f.pdf_path) continue;
                if (f.pdf_path === cur) continue;  // не дублируем текущий
                const name = (f.filename || f.pdf_path.split('/').pop() || '').toLowerCase();
                if (filter && !name.includes(filter)) continue;
                out.push({pdf_path: f.pdf_path, filename: f.filename || f.pdf_path.split('/').pop()});
            }
            return out;
        }
        async function scInlineMatchConfirm() {
            if (!scSession.value || !scInlineMatchPairId.value) return;
            scInlineMatchSaving.value = true;
            scInlineMatchError.value = '';
            try {
                const sid = scSession.value.id;
                const pid = scInlineMatchPairId.value;
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(sid)}/pairs/${encodeURIComponent(pid)}/match`;
                const body = {right_pdf: scInlineMatchChoice.value || null};
                const r = await fetch(url, {
                    method: 'PUT',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(body),
                });
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    throw new Error(j.detail || ('HTTP ' + r.status));
                }
                await scLoadSession(scSession.value.id);
                await scLoadUnmatched();
                scCloseInlineMatch();
            } catch (e) {
                scInlineMatchError.value = String(e.message || e);
            } finally {
                scInlineMatchSaving.value = false;
            }
        }
        async function scConfirmAllMaybe() {
            if (!scSession.value) return;
            const n = scPairsCounts.value.maybe;
            if (!n) return;
            if (!window.confirm(`Подтвердить ${n} автосопоставлен${n === 1 ? 'ие' : (n < 5 ? 'ия' : 'ий')}? Статус «Возможно» сменится на «Сопоставлено».`)) {
                return;
            }
            scConfirmAllRunning.value = true;
            scConfirmAllError.value = '';
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/confirm-all`;
                const r = await fetch(url, {method: 'POST'});
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    throw new Error(j.detail || ('HTTP ' + r.status));
                }
                await scLoadSession(scSession.value.id);
            } catch (e) {
                scConfirmAllError.value = String(e.message || e);
            } finally {
                scConfirmAllRunning.value = false;
            }
        }

        async function scSavePairMatch() {
            if (!scMatchPairTargetPair.value) return;
            scMatchPairSaving.value = true;
            scMatchPairError.value = '';
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scMatchPairTargetPair.value.id)}/match`;
                const body = {right_pdf: scMatchPairChoiceRight.value || null};
                const r = await fetch(url, {
                    method: 'PUT',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(body),
                });
                if (!r.ok) { const j = await r.json().catch(() => ({})); throw new Error(j.detail || ('HTTP ' + r.status)); }
                // Обновим список пар
                await scLoadSession(scSession.value.id);
                scMatchPairDialogOpen.value = false;
            } catch (e) {
                scMatchPairError.value = String(e.message || e);
            } finally {
                scMatchPairSaving.value = false;
            }
        }
        function scOpenCreatePairDialog() {
            scCreatePairLeft.value = '';
            scCreatePairRight.value = '';
            scCreatePairError.value = '';
            scCreatePairDialogOpen.value = true;
            scLoadUnmatched();
        }
        async function scSaveCreatePair() {
            if (!scSession.value) return;
            scCreatePairSaving.value = true;
            scCreatePairError.value = '';
            try {
                if (!scCreatePairLeft.value && !scCreatePairRight.value) {
                    scCreatePairError.value = 'Нужно выбрать хотя бы один PDF';
                    return;
                }
                const r = await fetch(`/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs`, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        left_pdf: scCreatePairLeft.value || null,
                        right_pdf: scCreatePairRight.value || null,
                    }),
                });
                if (!r.ok) { const j = await r.json().catch(() => ({})); throw new Error(j.detail || ('HTTP ' + r.status)); }
                await scLoadSession(scSession.value.id);
                scCreatePairDialogOpen.value = false;
            } catch (e) {
                scCreatePairError.value = String(e.message || e);
            } finally {
                scCreatePairSaving.value = false;
            }
        }
        async function scDeletePair(pair) {
            const hard = confirm(`Удалить пару?\n\nOK — удалить полностью.\nОтмена — только скрыть (можно вернуть редактированием JSON).`);
            if (!scSession.value) return;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(pair.id)}` + (hard ? '?hard=true' : '');
                const r = await fetch(url, {method: 'DELETE'});
                if (!r.ok) { const j = await r.json().catch(() => ({})); throw new Error(j.detail || ('HTTP ' + r.status)); }
                // Если активная пара — снимаем её
                if (scActivePair.value && scActivePair.value.id === pair.id) {
                    scActivePair.value = null;
                    scPairData.value = null;
                    scTab.value = 'upload';
                }
                await scLoadSession(scSession.value.id);
            } catch (e) {
                scError.value = 'Не удалось удалить пару: ' + e;
            }
        }

        // ── Drag-and-drop pair reorder ───────────────────────────────────
        function scOnPairDragStart(event, idx) {
            scPairDragFromIdx.value = idx;
            scPairOrderError.value = '';
            try {
                event.dataTransfer.effectAllowed = 'move';
                // dataTransfer.setData нужен для Firefox чтобы drag реально стартанул.
                event.dataTransfer.setData('text/plain', String(idx));
            } catch (_) { /* defensive */ }
        }
        function scOnPairDragOver(event, idx) {
            if (scPairDragFromIdx.value < 0) return;
            try { event.dataTransfer.dropEffect = 'move'; } catch (_) {}
            if (scPairDragOverIdx.value !== idx) scPairDragOverIdx.value = idx;
        }
        function scOnPairDragLeave(event, idx) {
            if (scPairDragOverIdx.value === idx) scPairDragOverIdx.value = -1;
        }
        async function scOnPairDrop(event, toIdx) {
            event.preventDefault();
            const fromIdx = scPairDragFromIdx.value;
            scPairDragOverIdx.value = -1;
            scPairDragFromIdx.value = -1;
            if (fromIdx < 0 || fromIdx === toIdx) return;
            if (!scSession.value) return;
            const pairs = (scSession.value.pairs || []).slice();
            if (fromIdx >= pairs.length || toIdx >= pairs.length) return;
            const [moved] = pairs.splice(fromIdx, 1);
            pairs.splice(toIdx, 0, moved);
            // Оптимистичный апдейт UI — мгновенный visual reorder.
            // scPairs — computed из scSession.value.pairs, поэтому мутируем источник.
            scSession.value.pairs = pairs;
            scPairOrderSaving.value = true;
            scPairOrderError.value = '';
            try {
                const sid = scSession.value.id;
                const r = await fetch(`/api/stage-comparison/sessions/${encodeURIComponent(sid)}/pair-order`, {
                    method: 'PUT',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({pair_ids: pairs.map(p => p.id)}),
                });
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    throw new Error(j.detail || ('HTTP ' + r.status));
                }
                // Backend вернёт нормализованный order — синкуем UI с ним.
                const body = await r.json();
                const order = body.pair_order || [];
                if (order.length) {
                    const byId = Object.fromEntries(pairs.map(p => [p.id, p]));
                    scSession.value.pairs = order.map(id => byId[id]).filter(Boolean);
                }
            } catch (e) {
                scPairOrderError.value = 'Не удалось сохранить порядок: ' + (e.message || e);
                // Откатываемся: перечитываем session.
                try { await scLoadSession(scSession.value.id); } catch (_) {}
            } finally {
                scPairOrderSaving.value = false;
            }
        }
        function scOnPairDragEnd() {
            scPairDragFromIdx.value = -1;
            scPairDragOverIdx.value = -1;
        }

        // ── Report tab: read-only сводка согласованных расхождений ──────
        // Открытие быстрое: грузим только решения эксперта (один JSON) —
        // из них сразу считаем число согласованного по каждой паре. Сами
        // расхождения пары (unified flat) грузим лениво, по разворачиванию,
        // и только для нужной пары (?pair_id=…), не всю сессию разом.
        const _SC_REPORT_SEV_RANK = { high: 0, medium: 1, low: 2, unknown: 3 };

        // Сырое число accepted-ключей по паре (без дедупа V1/V2). Используется
        // как fallback до того, как построена сверка scReportReconciledCounts.
        const scReportApprovedCountByPair = computed(() => {
            const counts = {};
            for (const [key, entry] of Object.entries(scExpertDecisions.value || {})) {
                if (!entry || entry.decision !== 'accepted') continue;
                const sep = String(key).indexOf('::');
                if (sep < 0) continue;
                const pid = String(key).slice(0, sep);
                counts[pid] = (counts[pid] || 0) + 1;
            }
            return counts;
        });

        // ── Сверка V1 («Расхождения») ↔ V2 ──────────────────────────────────
        // В expert_review.json одно и то же расхождение может быть согласовано
        // дважды: под классическим id `chg_X` (вид «Расхождения») и под его
        // V2-двойником `v2_<sha1(pid::chg_X)>` (вид «V2»). Без дедупа бейдж
        // «N согласовано» задваивается. Считаем V2-id так же, как backend
        // make_v2_id, и схлопываем двойники в одну находку.
        // Чистый JS SHA-1 (синхронный). Не зависит от crypto.subtle, который
        // доступен только в secure-context (HTTPS/localhost) — портал могут
        // открыть и по голому HTTP на IP сервера. TextEncoder даёт UTF-8 байты
        // и доступен везде. Результат побайтно совпадает с hashlib.sha1.
        function _scSha1Hex(str) {
            const bytes = new TextEncoder().encode(String(str));
            const l = bytes.length;
            const total = (((l + 8) >>> 6) + 1) << 6;   // кратно 64, оставляя 8 байт под длину
            const msg = new Uint8Array(total);
            msg.set(bytes);
            msg[l] = 0x80;
            const dv = new DataView(msg.buffer);
            const bitLen = l * 8;
            dv.setUint32(total - 4, bitLen >>> 0, false);
            dv.setUint32(total - 8, Math.floor(bitLen / 0x100000000) >>> 0, false);
            let h0 = 0x67452301, h1 = 0xEFCDAB89, h2 = 0x98BADCFE, h3 = 0x10325476, h4 = 0xC3D2E1F0;
            const w = new Int32Array(80);
            for (let i = 0; i < total; i += 64) {
                for (let j = 0; j < 16; j++) w[j] = dv.getInt32(i + j * 4, false);
                for (let j = 16; j < 80; j++) { const n = w[j - 3] ^ w[j - 8] ^ w[j - 14] ^ w[j - 16]; w[j] = (n << 1) | (n >>> 31); }
                let a = h0, b = h1, c = h2, d = h3, e = h4;
                for (let j = 0; j < 80; j++) {
                    let f, k;
                    if (j < 20) { f = (b & c) | ((~b) & d); k = 0x5A827999; }
                    else if (j < 40) { f = b ^ c ^ d; k = 0x6ED9EBA1; }
                    else if (j < 60) { f = (b & c) | (b & d) | (c & d); k = 0x8F1BBCDC; }
                    else { f = b ^ c ^ d; k = 0xCA62C1D6; }
                    const t = (((a << 5) | (a >>> 27)) + f + e + k + w[j]) | 0;
                    e = d; d = c; c = (b << 30) | (b >>> 2); b = a; a = t;
                }
                h0 = (h0 + a) | 0; h1 = (h1 + b) | 0; h2 = (h2 + c) | 0; h3 = (h3 + d) | 0; h4 = (h4 + e) | 0;
            }
            const hx = (n) => ('00000000' + ((n >>> 0).toString(16))).slice(-8);
            return hx(h0) + hx(h1) + hx(h2) + hx(h3) + hx(h4);
        }
        // Зеркало backend v2_review.make_v2_id: база = стабильный chg_-id, иначе
        // (uf_/без id) — хэш контента.
        function _scV2IdForItem(pairId, it) {
            const raw = String((it && it.id) || '').trim();
            let base;
            if (raw && !raw.startsWith('uf_')) {
                base = raw;
            } else {
                const ql = (it && it.evidence_left && it.evidence_left.quote) || '';
                const qr = (it && it.evidence_right && it.evidence_right.quote) || '';
                base = String((it && it.title) || '') + String((it && it.old_value) || '')
                     + String((it && it.new_value) || '') + String(ql) + String(qr);
            }
            return 'v2_' + _scSha1Hex(`${pairId}::${base}`).slice(0, 16);
        }
        function _scKeyAccepted(key) {
            return ((scExpertDecisions.value[key] || {}).decision) === 'accepted';
        }
        // Строит scReportReconciledCounts: для каждого accepted-классического
        // ключа считаем находку и помечаем его V2-двойника как «покрытого»;
        // accepted-V2-ключи, чей двойник не покрыт классическим, — это уникальные
        // V2-находки и добавляются отдельно.
        function _scBuildReconciledCounts() {
            const dec = scExpertDecisions.value || {};
            const classic = [];
            const v2keys = [];
            for (const [key, entry] of Object.entries(dec)) {
                if (!entry || entry.decision !== 'accepted') continue;
                const sep = String(key).indexOf('::');
                if (sep < 0) continue;
                const pid = String(key).slice(0, sep);
                const rid = String(key).slice(sep + 2);
                if (rid.startsWith('v2_')) v2keys.push({ pid, rid, key });
                else classic.push({ pid, rid, key });
            }
            const counts = {};
            const twinSet = new Set();   // `${pid}::${v2id}` для accepted-классики
            for (const { pid, rid } of classic) {
                counts[pid] = (counts[pid] || 0) + 1;   // каждая классика = находка
                if (rid && !rid.startsWith('uf_')) {
                    twinSet.add(`${pid}::v2_${_scSha1Hex(`${pid}::${rid}`).slice(0, 16)}`);
                }
            }
            for (const { pid, key } of v2keys) {
                if (!twinSet.has(key)) counts[pid] = (counts[pid] || 0) + 1;   // V2-only
            }
            scReportReconciledCounts.value = counts;
        }

        function scReportApprovedCountFor(pairId) {
            const pid = String(pairId);
            // Точный счёт по загруженной паре: дедуп V1/V2 уже применён к строкам,
            // и осиротевшие решения (id из старого прогона) сюда не попадают.
            // Прямой доступ к свойству — чтобы computed пересчитался при загрузке.
            const loaded = scReportPairItems[pid];
            if (Array.isArray(loaded)) return loaded.length;
            const rc = scReportReconciledCounts.value || {};
            if (Object.prototype.hasOwnProperty.call(rc, pid)) return rc[pid];
            return scReportApprovedCountByPair.value[pid] || 0;   // до сверки
        }
        const scReportTotalApproved = computed(() => {
            let n = 0;
            for (const p of (scPairs.value || [])) n += scReportApprovedCountFor(p.id);
            return n;
        });
        const scReportPairsWithApprovedCount = computed(() =>
            (scPairs.value || []).filter(p => scReportApprovedCountFor(p.id) > 0).length
        );
        // Кэш лениво загруженных согласованных расхождений по паре.
        function scReportApprovedFor(pairId) {
            return scReportPairItems[String(pairId)] || [];
        }
        function scReportPairLoading(pairId) {
            return !!scReportPairLoadingMap[String(pairId)];
        }
        function scReportPairLoaded(pairId) {
            return Object.prototype.hasOwnProperty.call(scReportPairItems, String(pairId));
        }
        async function scReportLoadPair(pairId) {
            if (!scSession.value) return;
            const pid = String(pairId);
            if (scReportPairLoadingMap[pid]) return;
            scReportPairLoadingMap[pid] = true;
            try {
                const sid = encodeURIComponent(scSession.value.id);
                const r = await fetch(`/api/stage-comparison/sessions/${sid}/unified-diff-flat?pair_id=${encodeURIComponent(pid)}`);
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    throw new Error(j.detail || ('HTTP ' + r.status));
                }
                const data = await r.json();
                const all = (data && data.items) || [];
                // Сверка с решениями эксперта: V1 (классический id) и V2-двойник.
                const rows = [];
                for (const it of all) {
                    if (!it || typeof it !== 'object') continue;
                    it._v2id = _scV2IdForItem(pid, it);
                    const v1 = _scKeyAccepted(`${pid}::${it.id}`);
                    const v2 = _scKeyAccepted(`${pid}::${it._v2id}`);
                    if (!v1 && !v2) continue;
                    // both → консенсус (основной список); v1/v2 → уникальное (в конец).
                    it._approvedIn = (v1 && v2) ? 'both' : (v1 ? 'v1' : 'v2');
                    rows.push(it);
                }
                const groupRank = (it) => (it._approvedIn === 'both' ? 0 : 1);
                rows.sort((a, b) => {
                    const ga = groupRank(a), gb = groupRank(b);
                    if (ga !== gb) return ga - gb;                  // консенсус выше уникальных
                    if (ga === 1) {                                 // внутри уникальных: V1, потом V2
                        const oa = a._approvedIn === 'v1' ? 0 : 1;
                        const ob = b._approvedIn === 'v1' ? 0 : 1;
                        if (oa !== ob) return oa - ob;
                    }
                    const ra = _SC_REPORT_SEV_RANK[a.severity] ?? 3;
                    const rb = _SC_REPORT_SEV_RANK[b.severity] ?? 3;
                    if (ra !== rb) return ra - rb;
                    const pa = Array.isArray(a.page) ? (a.page[0] || 0) : (a.page || 0);
                    const pb = Array.isArray(b.page) ? (b.page[0] || 0) : (b.page || 0);
                    return pa - pb;
                });
                // Флаг первой уникальной строки — под неё рисуется разделитель.
                rows.forEach((it, i) => {
                    it._firstUnique = it._approvedIn !== 'both'
                        && (i === 0 || rows[i - 1]._approvedIn === 'both');
                });
                scReportPairItems[pid] = rows;
            } catch (e) {
                scReportError.value = String(e.message || e);
                scReportPairItems[pid] = [];
            } finally {
                scReportPairLoadingMap[pid] = false;
            }
        }
        function scReportIsPairExpanded(pairId) {
            return scReportExpandedPairs.value.has(String(pairId));
        }
        function scReportTogglePair(pairId) {
            const pid = String(pairId);
            const s = new Set(scReportExpandedPairs.value);
            if (s.has(pid)) {
                s.delete(pid);
            } else {
                s.add(pid);
                if (!scReportPairLoaded(pid)) scReportLoadPair(pid);   // ленивая загрузка
            }
            scReportExpandedPairs.value = s;
        }
        function scReportExportUrl() {
            if (!scSession.value) return '';
            const sid = encodeURIComponent(scSession.value.id);
            return `/api/stage-comparison/sessions/${sid}/unified-diff-flat/export.xlsx?accepted_only=true&grouped=true`;
        }

        // Фоновая предзагрузка расхождений всех пар (только тех, где есть
        // согласованное) — чтобы разворачивание было мгновенным. Пары без
        // согласованного не грузим: они и так показывают «нет согласованных».
        async function scReportPrefetchAll(gen) {
            const targets = (scPairs.value || [])
                .filter(p => scReportApprovedCountFor(p.id) > 0 && !scReportPairLoaded(p.id));
            scReportPrefetchTotal.value = targets.length;
            scReportPrefetchDone.value = 0;
            if (!targets.length) { scReportPrefetching.value = false; return; }
            scReportPrefetching.value = true;
            try {
                for (const p of targets) {
                    if (gen !== _scReportPrefetchGen) return;   // началась новая загрузка — отменяемся
                    await scReportLoadPair(p.id);
                    scReportPrefetchDone.value++;
                }
            } finally {
                if (gen === _scReportPrefetchGen) scReportPrefetching.value = false;
            }
        }

        async function scReportLoad() {
            if (!scSession.value) return;
            scReportLoading.value = true;
            scReportError.value = '';
            const gen = ++_scReportPrefetchGen;             // отменяет прошлую предзагрузку
            scReportPrefetching.value = false;
            // Сброс кэшей: при перезагрузке всё перечитается.
            for (const k of Object.keys(scReportPairItems)) delete scReportPairItems[k];
            for (const k of Object.keys(scReportPairLoadingMap)) delete scReportPairLoadingMap[k];
            try {
                // Грузим только решения эксперта — этого хватает для счётчиков,
                // вкладка открывается мгновенно.
                await scLoadExpertDecisions();
                // Сверяем V1/V2-двойники → дедуплицированные счётчики бейджей.
                await _scBuildReconciledCounts();
            } catch (e) {
                scReportError.value = String(e.message || e);
            } finally {
                scReportLoading.value = false;
            }
            // А расхождения всех пар догружаем в фоне (не блокируя открытие).
            scReportPrefetchAll(gen);
        }

        async function scOpenReportTab() {
            if (!scSession.value) return;
            scTab.value = 'report';
            await scReportLoad();
        }

        async function scCreateLink() {
            if (!scSelectedLeft.value || !scSelectedRight.value) return;
            scLinking.value = true;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/links`;
                const r = await fetch(url, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({left_block_id: scSelectedLeft.value, right_block_id: scSelectedRight.value}),
                });
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    throw new Error(j.detail || ('HTTP ' + r.status));
                }
                scSelectedLeft.value = null;
                scSelectedRight.value = null;
                await scLoadPairData();
            } catch (e) {
                scError.value = 'Не удалось связать блоки: ' + e;
            } finally {
                scLinking.value = false;
            }
        }

        async function scDeleteLink(link) {
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/links`;
                await fetch(url, {
                    method: 'DELETE',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({left_block_id: link.left_block_id, right_block_id: link.right_block_id}),
                });
                // Если удалили активную связь — сбрасываем выбор
                const key = link.left_block_id + '::' + link.right_block_id;
                if (scActiveLinkKey.value === key) scActiveLinkKey.value = null;
                await scLoadPairData();
            } catch (e) {
                scError.value = 'Не удалось удалить связь: ' + e;
            }
        }

        async function scDeleteActiveLink() {
            if (!scActiveLink.value) return;
            await scDeleteLink(scActiveLink.value);
        }

        async function scClearStaleLinks() {
            const links = (scPairData.value && scPairData.value.links) || [];
            const stale = links.filter(l => String(l.method || '').endsWith('_stale'));
            if (!stale.length) return;
            // Удаляем последовательно — backend сам пересчитает оставшиеся.
            for (const l of stale) {
                try {
                    const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/links`;
                    await fetch(url, {
                        method: 'DELETE',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({left_block_id: l.left_block_id, right_block_id: l.right_block_id}),
                    });
                } catch (e) {
                    /* продолжаем — каждая ошибка не должна останавливать batch */
                }
            }
            scActiveLinkKey.value = null;
            await scLoadPairData();
        }

        async function scRunAutoLink() {
            scLinking.value = true;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/auto-link`;
                await fetch(url, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({iou_threshold: 0.5}),
                });
                await scLoadPairData();
            } catch (e) {
                scError.value = 'Auto-link error: ' + e;
            } finally {
                scLinking.value = false;
            }
        }

        // ── Сопоставление листов по штампам (предложение page-alignment) ──
        // Находит листы по имени из штампа (схема ГРЩ стр.21 ↔ стр.56) и
        // предлагает поставить их напротив друг друга. Применение = обычный
        // PUT page-alignment (ничего нового на сервере не мутирует впустую).
        const scStampProposals = ref(null);   // {suggested_items, confidence, ...}
        const scStampLoading   = ref(false);
        const scStampApplying  = ref(false);
        const scStampError     = ref('');
        const scStampSelected  = ref({});      // `${L}_${R}` -> bool (matched rows)
        const scStampUseLlm    = ref(true);    // доматчивать остаток через Haiku

        function scStampRowKey(it) {
            return (it.left_page == null ? '_' : it.left_page) + '_'
                 + (it.right_page == null ? '_' : it.right_page);
        }
        // Строка «выбираемая» (есть чекбокс) = уверенный матч ИЛИ позиционное
        // выравнивание (его можно отклонить). Истинно односторонние — только показ.
        function scStampIsSelectable(it) {
            return !!it.match || it.match_type === 'positional_alignment';
        }
        // Все строки предложения (matched + positional + true one-sided) — для показа.
        const scStampAllRows = computed(() =>
            (scStampProposals.value && scStampProposals.value.suggested_items) || []);
        const scStampMatchedRows = computed(() =>
            scStampAllRows.value.filter(it => it.match));
        const scStampSelectableRows = computed(() =>
            scStampAllRows.value.filter(scStampIsSelectable));
        const scStampSelectedCount = computed(() =>
            scStampSelectableRows.value.filter(it => scStampSelected.value[scStampRowKey(it)]).length);

        // One-click авто-сопоставление листов: умный matching + авто-применение
        // надёжных пар + отчёт. dryRun=true → preview без сохранения.
        const scAutoMatchApplyLoading = ref(false);
        const scAutoMatchApplyResult = ref(null);
        const scAutoMatchApplyError = ref(null);
        async function scAutoMatchApplySheets(dryRun = false) {
            if (!scSession.value || !scActivePair.value) return;
            scAutoMatchApplyLoading.value = true;
            scAutoMatchApplyError.value = null;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/page-alignment/auto-match-apply`;
                const r = await fetch(url, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ use_llm: false, overwrite_existing: false, dry_run: !!dryRun }),
                });
                if (!r.ok) throw new Error('HTTP ' + r.status);
                scAutoMatchApplyResult.value = await r.json();
                // Реально применили — перечитать карту страниц и связи блоков.
                if (scAutoMatchApplyResult.value && scAutoMatchApplyResult.value.applied_to_disk) {
                    await scLoadAlignment();
                    await scLoadPairData();
                }
            } catch (e) {
                scAutoMatchApplyError.value = 'Ошибка авто-сопоставления листов: ' + e;
            } finally {
                scAutoMatchApplyLoading.value = false;
            }
        }
        async function scSuggestByStamp() {
            if (!scSession.value || !scActivePair.value) return;
            scStampError.value = '';
            scStampLoading.value = true;
            scStampProposals.value = null;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/page-alignment/suggest-by-stamp`;
                // use_llm=true: после детерминированного матчинга остаток
                // НЕсматченных листов доматчивает Haiku (семантически
                // эквивалентные имена). Fail-soft на стороне backend.
                const r = await fetch(url, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({use_llm: scStampUseLlm.value}),
                });
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    throw new Error(j.detail || ('HTTP ' + r.status));
                }
                const d = await r.json();
                scStampProposals.value = d;
                const sel = {};
                (d.suggested_items || []).forEach(it => {
                    // matched + positional отмечены по умолчанию (positional нужно
                    // для сохранения визуальной карты, но его можно снять).
                    if (it.match || it.match_type === 'positional_alignment')
                        sel[scStampRowKey(it)] = true;
                });
                scStampSelected.value = sel;
            } catch (e) {
                scStampError.value = 'Сопоставление по штампам: ' + (e.message || e);
            } finally {
                scStampLoading.value = false;
            }
        }

        function scStampToggleRow(it) {
            const k = scStampRowKey(it);
            scStampSelected.value = {...scStampSelected.value, [k]: !scStampSelected.value[k]};
        }

        // Человекочитаемые подписи типов матча / risk-флагов (display-only поля).
        const SC_STAMP_TYPE_LABELS = {
            exact_name: 'точное',
            exact_canonical_name: 'каноническое',
            equipment_canonical_match: '⚡ по оборудованию',
            exact_multipart_group: '📑 многостр. лист',
            multipart_group: '📑 многостр. часть',
            multipart_continuation: '📑 продолжение',
            fuzzy_name: 'похожее',
            fuzzy_structural: 'по признакам',
            text_layer: 'текст-слой',
            llm_semantic: '🧠 по смыслу',
            derived_name_match: 'по содержимому',
            positional_alignment: 'позиционно',
            left_only: 'только слева',
            right_only: 'только справа',
        };
        const SC_STAMP_RISK_LABELS = {
            low_margin: 'слабый отрыв',
            duplicate_sheet_name: 'дубль имени',
            text_layer_fallback: 'текст-слой',
            llm_semantic: 'ИИ',
            derived_name: 'из содержимого',
            unconfirmed_alignment: 'без уверенного матча',
        };
        // Имя для показа: штамп-имя, иначе derived-заголовок из содержимого.
        function scStampDisplayName(it, side) {
            const nm = (it[side + '_sheet_name'] || '').trim();
            if (nm) return nm;
            const dv = (it[side + '_derived_sheet_name'] || '').trim();
            return dv ? dv + ' (из содержимого)' : '(без названия)';
        }
        function scStampNameIsDerived(it, side) {
            return !((it[side + '_sheet_name'] || '').trim())
                 && !!((it[side + '_derived_sheet_name'] || '').trim());
        }
        function scStampTypeLabel(mt) { return SC_STAMP_TYPE_LABELS[mt] || 'похожее'; }
        function scStampRiskLabel(f) { return SC_STAMP_RISK_LABELS[f] || f; }
        function scStampTypeColor(it) {
            if (['exact_name', 'exact_canonical_name', 'equipment_canonical_match', 'exact_multipart_group', 'multipart_group'].includes(it.match_type)) return '#15803d';
            if (it.match_type === 'llm_semantic') return '#6d28d9';
            if (it.match_type === 'derived_name_match') return '#0e7490';
            if (it.match_type === 'positional_alignment') return '#0891b2';
            if (['left_only', 'right_only', 'multipart_continuation'].includes(it.match_type)) return '#6b7280';
            return it.needs_review ? '#b45309' : '#374151';
        }
        function scStampRowTitle(it) {
            const parts = [];
            if (it.reason) parts.push(it.reason);
            if (it.positive_evidence && it.positive_evidence.length)
                parts.push('за: ' + it.positive_evidence.join('; '));
            if (it.negative_evidence && it.negative_evidence.length)
                parts.push('против: ' + it.negative_evidence.join('; '));
            return parts.join('\n');
        }

        // ── Пакетное авто-сопоставление листов (раздел «1. Загрузка документации») ──
        // Проходит по ВСЕМ парам сессии, безопасные совпадения применяет в
        // page_alignment, рискованные оставляет на ручную проверку. Прогресс —
        // polling job-эндпоинта. Ручное выравнивание по умолчанию не затирается.
        const scAutoMatchJob      = ref(null);
        const scAutoMatchStarting = ref(false);
        const scAutoMatchError    = ref('');
        const scAutoMatchUseLlm   = ref(true);
        const scAutoMatchOverwrite = ref(false);
        const scAutoMatchAsk      = ref(false);   // показать popup-вопрос перед стартом
        let   scAutoMatchTimer    = null;
        const scAutoMatchRunning  = computed(() =>
            scAutoMatchJob.value && ['queued', 'running'].includes(scAutoMatchJob.value.status));

        function scAutoMatchStopPolling() {
            if (scAutoMatchTimer) { clearInterval(scAutoMatchTimer); scAutoMatchTimer = null; }
        }
        // Клик по кнопке открывает popup-вопрос (ИИ-доматчинг / перезапись),
        // запуск — только после подтверждения в окне.
        function scAutoMatchOpenDialog() {
            if (scAutoMatchStarting.value || scAutoMatchRunning.value) return;
            scAutoMatchError.value = '';
            scAutoMatchAsk.value = true;
        }
        function scAutoMatchCloseDialog() { scAutoMatchAsk.value = false; }
        async function scAutoMatchConfirm() {
            scAutoMatchAsk.value = false;
            await scAutoMatchStart();
        }
        async function scAutoMatchPoll() {
            if (!scSession.value || !scAutoMatchJob.value) return;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/page-alignment/auto-match/${encodeURIComponent(scAutoMatchJob.value.id)}`;
                const r = await fetch(url);
                if (!r.ok) return;
                const j = await r.json();
                scAutoMatchJob.value = j;
                if (!['queued', 'running'].includes(j.status)) scAutoMatchStopPolling();
            } catch (_) { /* keep polling */ }
        }
        async function scAutoMatchStart() {
            if (!scSession.value || scAutoMatchStarting.value || scAutoMatchRunning.value) return;
            scAutoMatchError.value = '';
            scAutoMatchStarting.value = true;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/page-alignment/auto-match`;
                const r = await fetch(url, {
                    method: 'POST', headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({use_llm: scAutoMatchUseLlm.value,
                                          overwrite_existing: scAutoMatchOverwrite.value,
                                          auto_apply: true}),
                });
                if (!r.ok) {
                    const e = await r.json().catch(() => ({}));
                    throw new Error(e.detail || ('HTTP ' + r.status));
                }
                scAutoMatchJob.value = await r.json();
                scAutoMatchStopPolling();
                scAutoMatchTimer = setInterval(scAutoMatchPoll, 1200);
            } catch (e) {
                scAutoMatchError.value = 'Авто сопоставление: ' + (e.message || e);
            } finally {
                scAutoMatchStarting.value = false;
            }
        }
        async function scAutoMatchCancel() {
            if (!scSession.value || !scAutoMatchJob.value) return;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/page-alignment/auto-match/${encodeURIComponent(scAutoMatchJob.value.id)}/cancel`;
                const r = await fetch(url, {method: 'POST'});
                if (r.ok) scAutoMatchJob.value = await r.json();
            } catch (_) { /* ignore */ }
        }
        async function scAutoMatchLoadLast() {
            scAutoMatchStopPolling();
            if (!scSession.value) { scAutoMatchJob.value = null; return; }
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/page-alignment/auto-match-last`;
                const r = await fetch(url);
                if (!r.ok) { scAutoMatchJob.value = null; return; }
                const j = await r.json();
                const job = j.latest_job || j.last_run || null;
                scAutoMatchJob.value = job;
                // Если последний job ещё жив — продолжить polling.
                if (job && ['queued', 'running'].includes(job.status))
                    scAutoMatchTimer = setInterval(scAutoMatchPoll, 1200);
            } catch (_) { scAutoMatchJob.value = null; }
        }

        function scCloseStampProposals() {
            scStampProposals.value = null;
            scStampError.value = '';
        }

        async function scApplyStampProposals() {
            if (!scStampProposals.value || scStampApplying.value) return;
            scStampApplying.value = true;
            scStampError.value = '';
            try {
                const props = scStampProposals.value.suggested_items || [];
                const items = [];
                for (const it of props) {
                    const selectable = it.match || it.match_type === 'positional_alignment';
                    if (selectable && scStampSelected.value[scStampRowKey(it)]) {
                        // подтверждённый матч ИЛИ принятое позиционное выравнивание
                        // → пара напротив друг друга
                        items.push({left_page: it.left_page, right_page: it.right_page,
                                    mode: 'manual', note: it.note || ''});
                    } else if (selectable) {
                        // отклонён (снят чекбокс) → расцепить на два односторонних слота
                        if (it.left_page != null)
                            items.push({left_page: it.left_page, right_page: null,
                                        mode: 'manual', note: 'не подтверждено'});
                        if (it.right_page != null)
                            items.push({left_page: null, right_page: it.right_page,
                                        mode: 'manual', note: 'не подтверждено'});
                    } else {
                        // истинно односторонний лист как есть
                        items.push({left_page: it.left_page, right_page: it.right_page,
                                    mode: 'manual', note: it.note || ''});
                    }
                }
                items.forEach((it, i) => { it.slot = i + 1; });
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/page-alignment?force=true`;
                const r = await fetch(url, {
                    method: 'PUT',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({items, force: true}),
                });
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    const ve = j.detail && j.detail.validation_errors;
                    throw new Error(ve ? JSON.stringify(ve) : (j.detail || ('HTTP ' + r.status)));
                }
                await scLoadAlignment();
                await scLoadPairData();
                scStampProposals.value = null;
            } catch (e) {
                scStampError.value = 'Применение карты: ' + (e.message || e);
            } finally {
                scStampApplying.value = false;
            }
        }

        // ── Pair config template (save links + alignment) ─────────────────
        async function scSavePairTemplate() {
            if (!scSession.value || !scActivePair.value) return;
            if (scTemplateSaving.value) return;
            scTemplateError.value = '';
            scTemplateLastSaveMsg.value = '';
            scTemplateSaving.value = true;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/save-template`;
                const r = await fetch(url, {method: 'POST'});
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    throw new Error(j.detail || ('HTTP ' + r.status));
                }
                const d = await r.json();
                scTemplateLastSaveMsg.value = `Шаблон сохранён (${d.links_count || 0} связей) — будет применён при повторном открытии пары.`;
                // Локально включаем ✓ в «Загрузке документации» без полного перезапроса сессии.
                if (scActivePair.value) scActivePair.value.has_template = true;
                const sess = scSession.value;
                if (sess && Array.isArray(sess.pairs)) {
                    const row = sess.pairs.find(p => p.id === scActivePair.value?.id);
                    if (row) row.has_template = true;
                }
                // через несколько секунд скрыть, чтобы не висел на UI
                setTimeout(() => { scTemplateLastSaveMsg.value = ''; }, 8000);
            } catch (e) {
                scTemplateError.value = 'Не удалось сохранить шаблон: ' + e;
            } finally {
                scTemplateSaving.value = false;
            }
        }
        // ── Семантический LLM-анализ текста (Claude Sonnet) ────────────────
        async function scLoadTextLLMConfig() {
            if (!scSession.value) return;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/text-llm-config`;
                const r = await fetch(url);
                if (r.ok) scTextLLMConfig.value = await r.json();
            } catch (e) { /* silent */ }
        }
        async function scLoadTextLLMDiff() {
            if (!scSession.value || !scActivePair.value) return;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/text-llm-diff`;
                const r = await fetch(url);
                if (!r.ok) return;
                scTextLLMDiff.value = await r.json();
            } catch (e) { /* silent */ }
        }
        // ── Analysis mode (block_links | concept_no_block_links) ─────────
        async function scLoadAnalysisMode() {
            if (!scSession.value || !scActivePair.value) {
                scAnalysisMode.value = 'block_links';
                return;
            }
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/analysis-mode`;
                const r = await fetch(url);
                if (!r.ok) {
                    scAnalysisMode.value = 'block_links';
                    return;
                }
                const data = await r.json();
                scAnalysisMode.value = data.analysis_mode || 'block_links';
            } catch (e) {
                scAnalysisMode.value = 'block_links';
            }
        }
        async function scSetAnalysisMode(mode) {
            if (!scSession.value || !scActivePair.value) return;
            scAnalysisModeError.value = '';
            scAnalysisModeSaving.value = true;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/analysis-mode`;
                const r = await fetch(url, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({mode}),
                });
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    throw new Error(j.detail || ('HTTP ' + r.status));
                }
                const data = await r.json();
                scAnalysisMode.value = data.analysis_mode || mode;
            } catch (e) {
                scAnalysisModeError.value = 'Не удалось сохранить режим: ' + e;
            } finally {
                scAnalysisModeSaving.value = false;
            }
        }
        async function scToggleAnalysisMode() {
            const next = scAnalysisMode.value === 'concept_no_block_links'
                       ? 'block_links'
                       : 'concept_no_block_links';
            if (next === 'concept_no_block_links') {
                if (!confirm(
                    'Для этой PDF-пары будет отключена логика обязательного сопоставления блоков. ' +
                    'Анализ будет выполнен по концепции документа целиком: Qwen подготовит ' +
                    'enriched MD для каждой стороны, затем Opus сравнит две enriched версии. Продолжить?'
                )) return;
            } else {
                if (!confirm('Вернуть обычный режим со связями блоков для этой PDF-пары?')) return;
            }
            await scSetAnalysisMode(next);
        }

        // ── MD enrichment (Qwen image descriptions) ──────────────────────
        async function scLoadMdEnrichmentSummary() {
            if (!scSession.value || !scActivePair.value) return;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/md-enrichment`;
                const r = await fetch(url);
                if (!r.ok) return;
                scMdEnrichmentSummary.value = await r.json();
            } catch (e) { /* silent */ }
        }
        async function scMdEnrichmentDryRun() {
            if (!scSession.value || !scActivePair.value) return;
            scMdEnrichmentError.value = '';
            scMdEnrichmentLoading.value = true;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/md-enrichment`;
                const r = await fetch(url, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({side: 'both', force: false, run_model: false}),
                });
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    throw new Error(j.detail || ('HTTP ' + r.status));
                }
                await scLoadMdEnrichmentSummary();
            } catch (e) {
                scMdEnrichmentError.value = 'Dry-run не выполнен: ' + e;
            } finally {
                scMdEnrichmentLoading.value = false;
            }
        }
        function scMdEnrichmentRequestConfirm() {
            scMdEnrichmentConfirmOpen.value = true;
        }
        async function scMdEnrichmentRunModel() {
            // Запуск Qwen перенесён на background job — sync endpoint с
            // run_model=true давал HTTP 524 на ngrok/Cloudflare при ≥7 image
            // блоках (~50–60 с на блок). Теперь UI всегда идёт через job +
            // polling и видит block-level прогресс.
            scMdEnrichmentConfirmOpen.value = false;
            if (!scSession.value || !scActivePair.value) return;
            scMdEnrichmentError.value = '';
            scMdEnrichmentJobTimedOut.value = false;
            scMdEnrichmentRunning.value = true;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/md-enrichment-jobs`;
                const r = await fetch(url, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        scope: 'pair',
                        pair_id: scActivePair.value.id,
                        side: 'both',
                        force: false,
                        confirm: true,
                    }),
                });
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    const msg = (j && j.detail && (j.detail.message || j.detail)) || ('HTTP ' + r.status);
                    if (r.status === 524 || r.status === 504 || r.status === 408) {
                        // job всё равно мог стартовать на сервере
                        scMdEnrichmentJobTimedOut.value = true;
                    }
                    throw new Error(typeof msg === 'string' ? msg : JSON.stringify(msg));
                }
                const job = await r.json();
                scMdEnrichmentJob.value = job;
                if (job && job.id && (job.status === 'queued' || job.status === 'running')) {
                    scPollMdEnrichmentJob(job.id);
                } else {
                    // job done/failed/rejected моментально (например, rejected_no_confirm)
                    await scLoadMdEnrichmentSummary();
                }
            } catch (e) {
                if (e && (e.name === 'TypeError' || /timeout|abort|fetch/i.test(String(e)))) {
                    // network / fetch fail — job могла стартовать
                    scMdEnrichmentJobTimedOut.value = true;
                }
                scMdEnrichmentError.value = scMdEnrichmentJobTimedOut.value
                    ? 'Запрос был прерван по таймауту. Проверьте статус задачи — обработка могла продолжиться на сервере.'
                    : 'Запуск Qwen не удался: ' + e;
            } finally {
                // running остаётся true пока polling не закончится; здесь сбрасываем
                // только если job не запустился.
                if (!scMdEnrichmentJob.value || !scMdEnrichmentJob.value.id) {
                    scMdEnrichmentRunning.value = false;
                }
            }
        }
        async function scPollMdEnrichmentJob(jobId) {
            if (!scSession.value || !jobId) return;
            if (scMdEnrichmentJobPolling.value) return;
            scMdEnrichmentJobPolling.value = true;
            try {
                while (true) {
                    const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/md-enrichment-jobs/${encodeURIComponent(jobId)}`;
                    let r;
                    try {
                        r = await fetch(url);
                    } catch (_) {
                        // transient network — попробуем снова через интервал
                        await new Promise(res => setTimeout(res, 3000));
                        continue;
                    }
                    if (!r.ok) break;
                    const job = await r.json();
                    scMdEnrichmentJob.value = job;
                    if (['done', 'failed', 'cancelled', 'rejected_no_confirm'].includes(job.status)) break;
                    await new Promise(res => setTimeout(res, 3000));
                }
                try { await scLoadMdEnrichmentSummary(); } catch (_) {}
            } finally {
                scMdEnrichmentJobPolling.value = false;
                scMdEnrichmentRunning.value = false;
            }
        }
        async function scCancelMdEnrichmentJob() {
            const job = scMdEnrichmentJob.value;
            if (!job || !job.id || !scSession.value) return;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/md-enrichment-jobs/${encodeURIComponent(job.id)}/cancel`;
                const r = await fetch(url, {method: 'POST'});
                if (r.ok) scMdEnrichmentJob.value = await r.json();
            } catch (_) { /* silent */ }
        }
        async function scRefreshMdEnrichmentJob() {
            // Вручную обновить статус — нужно когда стартовый POST оборвался
            // (524/504/timeout) и job всё ещё может крутиться на сервере.
            scMdEnrichmentError.value = '';
            scMdEnrichmentJobTimedOut.value = false;
            const job = scMdEnrichmentJob.value;
            if (job && job.id) {
                if (!scMdEnrichmentJobPolling.value) {
                    scPollMdEnrichmentJob(job.id);
                }
                return;
            }
            // Если job_id не получили — обновим summary; пользователь увидит,
            // что enrichment мог завершиться/частично завершиться.
            try { await scLoadMdEnrichmentSummary(); } catch (_) {}
        }

        async function scRecogPoll(jobId) {
            if (!scSession.value || !jobId) return;
            if (scRecogPolling.value) return;
            scRecogPolling.value = true;
            try {
                while (true) {
                    const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/md-enrichment-jobs/${encodeURIComponent(jobId)}`;
                    let r;
                    try {
                        r = await fetch(url);
                    } catch (_) {
                        await new Promise(res => setTimeout(res, 3000));
                        continue;
                    }
                    if (!r.ok) break;
                    const job = await r.json();
                    scRecogJob.value = job;
                    if (['done', 'failed', 'cancelled', 'rejected_no_confirm', 'failed_interrupted'].includes(job.status)) break;
                    await new Promise(res => setTimeout(res, 3000));
                }
            } finally {
                scRecogPolling.value = false;
            }
        }

        async function scRecogRestoreActive() {
            // Подтянуть последнюю активную (или вообще последнюю) job сессии,
            // чтобы UI при возврате на этап 1 показал актуальный прогресс.
            if (!scSession.value || !scSession.value.id) return;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/md-enrichment-jobs/active`;
                const r = await fetch(url);
                if (!r.ok) return;
                const data = await r.json();
                const job = data && data.job;
                if (!job) return;
                scRecogJob.value = job;
                if (job.id && (job.status === 'queued' || job.status === 'running') && !scRecogPolling.value) {
                    scRecogStartedAtClient.value = Date.now();
                    scRecogPoll(job.id);
                }
            } catch (_) { /* silent */ }
        }

        // Per-pair status lookup для рендера таблицы пар на этапе 1.
        function scRecogPairStatus(pairId) {
            const agg = scRecogJob.value && scRecogJob.value.aggregate;
            if (!agg || !agg.pair_statuses) return null;
            return agg.pair_statuses[pairId] || null;
        }

        function scRecogPairBadge(pairId) {
            const ps = scRecogPairStatus(pairId);
            if (!ps) return {label: '—', cls: 'sc-status-unmatched', title: 'графика не распознавалась'};
            const s = ps.status;
            // Сколько блоков восстановлено salvage'ом — для тултипа на
            // done_with_salvage. Метрика приходит из backend block_metrics.
            const lSalv = ((ps.block_metrics || {}).left || {}).blocks_salvaged || 0;
            const rSalv = ((ps.block_metrics || {}).right || {}).blocks_salvaged || 0;
            const salvSum = lSalv + rSalv;
            const m = {
                done:               {label: '✓ распознано',           cls: 'sc-status-matched',   title: 'enriched MD готов'},
                done_with_salvage:  {label: '✓ распознано (salvage)', cls: 'sc-status-matched',   title: salvSum ? `enriched MD готов · восстановлено блоков: ${salvSum}` : 'enriched MD готов · использовался salvage'},
                partial:            {label: '⚠ частично',         cls: 'sc-status-maybe',     title: 'есть нераспознанные блоки'},
                error:              {label: '✗ ошибка',           cls: 'sc-status-unmatched', title: 'есть failed-блоки'},
                running:            {label: '… идёт',             cls: 'sc-status-maybe',     title: 'обработка'},
                queued:             {label: '⏱ в очереди',         cls: 'sc-status-unmatched', title: 'ждёт обработки'},
                skipped:            {label: '✓ из кэша',          cls: 'sc-status-matched',   title: 'пропущено как уже готовое'},
                cancelled:          {label: '⊘ отменено',         cls: 'sc-status-unmatched', title: 'отменено пользователем'},
                not_run:            {label: '—',                  cls: 'sc-status-unmatched', title: 'не запускалось'},
            };
            const base = m[s] || {label: s || '—', cls: 'sc-status-unmatched', title: s || ''};
            // Подсказка из backend (parse_error_detail → human text) — оператор
            // сразу видит, ПОЧЕМУ пара упала, без открытия pair-view. Только
            // для реальных проблемных состояний; done_with_salvage сюда не
            // попадает: backend specifically не выставляет problem_hint в этом
            // состоянии.
            if (ps.problem_hint && (s === 'error' || s === 'partial')) {
                return {...base, title: `${base.title} · ${ps.problem_hint}`};
            }
            return base;
        }

        // Блок-уровневые счётчики графики для колонок таблицы пар:
        // всего блоков / готово / упало (сумма left+right). Данные те же,
        // что и у бейджа «Графика» — из aggregate.pair_statuses.block_metrics.
        function scRecogPairBlocks(pairId) {
            const ps = scRecogPairStatus(pairId);
            const bm = ps && ps.block_metrics;
            if (!bm) return {available: false, total: 0, done: 0, failed: 0, partial: 0};
            let available = false, totalField = 0, bDone = 0, bPartial = 0, bError = 0;
            for (const side of ['left', 'right']) {
                const m = bm[side] || {};
                if (m.block_metrics_available) available = true;
                totalField += Number(m.blocks_total   || 0);
                bDone      += Number(m.blocks_done     || 0);
                bPartial   += Number(m.blocks_partial  || 0);
                bError     += Number(m.blocks_error    || 0);
            }
            // «Готово» = распознанные блоки, включая salvage/partial — так же,
            // как глобальный индикатор «38 / 43 · failed N» (described/total).
            const done = bDone + bPartial;
            const failed = bError;
            // blocks_total приходит из backend (включает pending во время прогона).
            // Если backend ещё без этого поля — fallback на described+failed.
            const total = totalField > 0 ? totalField : (done + failed);
            return {available, total, done, failed, partial: bPartial};
        }

        // Per-pair статус Opus-сравнения для колонки «Сравнение». Источник —
        // активный/последний unified-job (scOpusJob.items), как у бейджа графики.
        function scOpusPairItem(pairId) {
            // Per-pair fallback job имеет приоритет: его статус свежее и относится
            // именно к этой паре (single-pair job). Иначе — session job.
            const fb = scOpusFallbackByPair.value[pairId];
            if (fb) return fb;
            const job = scOpusJob.value;
            if (!job || !Array.isArray(job.items)) return null;
            let found = null;
            for (const it of job.items) {
                if (it && it.pair_id === pairId) found = it;  // последний item пары
            }
            return found;
        }

        // Приоритет источников для бейджа колонки «Сравнение»:
        //   1. идёт запуск fallback (scOpusFallbackStarting);
        //   2. живой job В ПРОЦЕССЕ (running/queued) — показываем активность,
        //      в т.ч. running fallback (не теряем его при reload);
        //   3. персистентный comparison_result.json с диска (источник истины
        //      для завершённых) — чинит «—» у уже сравнённых пар, когда
        //      активен лишь одно-парный fallback/retry-job;
        //   4. завершённый живой item (если persistent ещё не подгружен);
        //   5. «—» (реально не запускалось).
        function scOpusPairBadge(pairId) {
            if (scOpusFallbackStarting.value[pairId]) {
                return {label: '… запуск fallback', cls: 'sc-status-maybe',
                        title: 'Запускаю сравнение через Opus fallback (evidence_first)…'};
            }
            const live = scOpusPairItem(pairId);
            const liveSt = live ? String(live.status || '').toLowerCase() : '';
            if (live && ['running', 'comparing', 'enriching', 'queued'].includes(liveSt)) {
                return scOpusBadgeFromRecord(live);
            }
            const persisted = scPairCompareStatus.value[pairId];
            if (persisted && persisted.status && persisted.status !== 'not_run') {
                return scOpusBadgeFromRecord({
                    status: persisted.status,
                    comparison_status: persisted.status,
                    changes_count: persisted.changes_count,
                    _via_fallback: !!persisted.via_fallback,
                    _present_one_side: persisted.present_one_side_count,
                    _requires_human_review: persisted.requires_human_review_count,
                    _mode: persisted.mode,
                });
            }
            if (live) return scOpusBadgeFromRecord(live);
            return {label: '—', cls: 'sc-status-unmatched', title: 'сравнение не запускалось'};
        }

        // Интерпретирует «запись о сравнении» (живой job item ИЛИ персистентный
        // comparison_result.json) в бейдж. Едина для обоих источников.
        function scOpusBadgeFromRecord(it) {
            const st = String(it.status || '').toLowerCase();
            const cmp = String(it.comparison_status || '').toLowerCase();
            const action = String(it.preflight_action || '').toLowerCase();
            const reason = String(it.preflight_reason || it.error || '').toLowerCase();
            const tooLarge = action === 'skip_too_large' || cmp === 'too_large' || st === 'too_large' || reason.indexOf('exceeds_limit') >= 0;
            if (tooLarge) {
                // Кликабельно: запускает evidence_first_s2_fallback на готовых
                // enriched MD (без повторного Qwen, без поднятия общего лимита).
                return {label: '⚠ файл большой ▸ fallback', cls: 'sc-status-maybe', canFallback: true,
                        title: 'enriched MD превышает лимит. Нажмите, чтобы сравнить эту пару через Opus fallback '
                             + '(evidence_first: section split + verification) на уже готовых enriched MD — '
                             + 'без повторного Qwen и без изменения общего лимита.'};
            }
            const viaFb = !!it._via_fallback;
            if (st === 'running' || st === 'comparing' || st === 'enriching') {
                const fp = it._fallback_progress;
                if (viaFb && fp && Number(fp.total_chunks) > 0) {
                    const idx = Number(fp.current_chunk_index || 0);
                    const total = Number(fp.total_chunks || 0);
                    const eta = Number(fp.eta_sec || 0);
                    const etaTxt = eta ? ' · ~' + scFormatDuration(eta) : '';
                    return {label: `… fallback ${idx}/${total}`, cls: 'sc-status-maybe',
                            title: `Opus fallback (evidence_first): чанк ${idx} из ${total}`
                                 + (eta ? ` · осталось ~${scFormatDuration(eta)}` : '')
                                 + (fp.current_chunk_title ? ` · ${fp.current_chunk_title}` : '')};
                }
                return {label: viaFb ? '… fallback' : '… идёт', cls: 'sc-status-maybe',
                        title: viaFb ? 'идёт сравнение через Opus fallback (evidence_first)' : 'идёт сравнение'};
            }
            if (st === 'queued') {
                return {label: viaFb ? '⏱ fallback в очереди' : '⏱ в очереди', cls: 'sc-status-unmatched', title: 'ждёт сравнения'};
            }
            if (st === 'done' || cmp === 'done') {
                const n = Number(it.changes_count || 0);
                const suffix = viaFb ? ' (fallback)' : '';
                const pos = Number(it._present_one_side || 0);
                const rhr = Number(it._requires_human_review || 0);
                // Бейдж «✓ сравнено» НЕ зависит от экспертной проверки: «Проверено
                // 0/N» означает лишь, что инженер ещё не размечал расхождения.
                let title = (n ? `сравнено · расхождений: ${n}` : 'сравнено')
                          + (viaFb ? ' · через Opus fallback (evidence_first)' : '');
                if (pos) title += ` · видно с одной стороны: ${pos}`;
                if (rhr) title += ` · на проверку инженером: ${rhr}`;
                return {label: '✓ сравнено' + suffix, cls: 'sc-status-matched', title};
            }
            if (st === 'failed' || st === 'failed_interrupted' || st === 'error') {
                return {label: '✗ ошибка', cls: 'sc-status-unmatched', title: it.error || 'ошибка сравнения'};
            }
            if (st === 'cancelled') {
                return {label: '⊘ отменено', cls: 'sc-status-unmatched', title: 'отменено'};
            }
            if (st === 'skipped') {
                if (action === 'skip_not_ready' || reason.indexOf('missing') >= 0 || reason.indexOf('not_ready') >= 0) {
                    return {label: 'не готово', cls: 'sc-status-unmatched', title: 'графика ещё не распознана для пары'};
                }
                return {label: '✓ сравнено', cls: 'sc-status-matched', title: 'сравнение уже выполнено ранее'};
            }
            return {label: st || '—', cls: 'sc-status-unmatched', title: st || ''};
        }

        function scRecogElapsedLabel() {
            const agg = scRecogJob.value && scRecogJob.value.aggregate;
            if (!agg) return '—';
            return scFormatDuration(agg.elapsed_sec || 0);
        }

        // Универсальный форматер длительности (для elapsed + ETA + средние).
        function scFormatDuration(sec) {
            const s = Math.max(0, Math.round(Number(sec) || 0));
            if (s < 60) return s + 'с';
            const m = Math.floor(s / 60), ss = s % 60;
            if (m < 60) return m + 'м ' + ss + 'с';
            const h = Math.floor(m / 60), mm = m % 60;
            return h + 'ч ' + mm + 'м';
        }

        // Считаем «пара X / Y» для шапки live-индикатора. running_pair_id
        // вычисляется backend'ом, но мы считаем порядковый номер по
        // pair_order в session.json, чтобы UX был «3 / 20 пар» как в табличке.
        function scRecogPairProgress() {
            const agg = scRecogJob.value && scRecogJob.value.aggregate;
            const sess = scSession.value;
            const pairs = (sess && sess.pairs) || [];
            const total = pairs.length || (agg && agg.total_pairs) || 0;
            if (!agg || !agg.current_pair_id) return {current: 0, total: total};
            // Индекс текущей пары в session.pair_order — это и есть «X из Y»
            const idx = pairs.findIndex(p => p && p.id === agg.current_pair_id);
            return {current: idx >= 0 ? idx + 1 : (agg.done_pairs || 0) + 1, total: total};
        }

        // Загрузить плоский session-level список текстовых изменений.
        async function scLoadTextLLMFlat() {
            if (!scSession.value) return;
            scTextLLMFlatLoading.value = true;
            scTextLLMFlatError.value = '';
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/text-llm-diff-flat`;
                const r = await fetch(url);
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    throw new Error(j.detail || ('HTTP ' + r.status));
                }
                scTextLLMFlat.value = await r.json();
            } catch (e) {
                scTextLLMFlatError.value = String(e.message || e);
            } finally {
                scTextLLMFlatLoading.value = false;
            }
        }
        // ── Unified analysis (Qwen enrichment → Opus comparison) ────────
        async function scLoadUnifiedConfig() {
            try {
                const r = await fetch('/api/stage-comparison/enriched-compare-config');
                if (!r.ok) return;
                scUnifiedConfig.value = await r.json();
            } catch (_) { /* silent */ }
        }
        async function scLoadUnifiedPairStatus() {
            if (!scSession.value || !scActivePair.value) return;
            scUnifiedPairLoading.value = true;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/unified-analysis`;
                const r = await fetch(url);
                if (!r.ok) return;
                scUnifiedPairStatus.value = await r.json();
            } catch (e) { /* silent */ }
            finally { scUnifiedPairLoading.value = false; }
        }
        // Подпись для чипа направления изменения в колонке «№».
        const _SC_DIRECTION_LABELS = {
            complication: 'усложнение',
            simplification: 'упрощение',
            neutral: 'нейтрально',
            unknown: '—',
        };
        function scDirectionLabel(d) {
            return _SC_DIRECTION_LABELS[d || 'unknown'] || '—';
        }
        // Денежный эффект изменения для чипа в колонке «№»:
        // increase → удорожание, decrease → удешевление, иначе → нейтрально.
        const _SC_COST_DIRECTION_LABELS = {
            increase: 'удорожание',
            decrease: 'удешевление',
            unknown: 'нейтрально',
            neutral: 'нейтрально',
        };
        function scCostDirectionLabel(d) {
            return _SC_COST_DIRECTION_LABELS[d || 'unknown'] || 'нейтрально';
        }
        function scCostDirectionStyle(d) {
            if (d === 'increase') return 'background:#fee2e2; color:#991b1b';
            if (d === 'decrease') return 'background:#dcfce7; color:#166534';
            return 'background:#f1f5f9; color:#475569';
        }
        async function scLoadUnifiedFlat() {
            if (!scSession.value) return;
            scUnifiedFlatLoading.value = true;
            scUnifiedFlatError.value = '';
            try {
                // По умолчанию — фильтр по активной паре. UI «Расхождения»
                // всегда привязана к scActivePair, кроме явного «Показать все».
                const sid = encodeURIComponent(scSession.value.id);
                let url = `/api/stage-comparison/sessions/${sid}/unified-diff-flat`;
                let scopePid = null;
                if (!scUnifiedShowAllPairs.value && scActivePair.value && scActivePair.value.id) {
                    scopePid = scActivePair.value.id;
                    url += `?pair_id=${encodeURIComponent(scopePid)}`;
                }
                const r = await fetch(url);
                if (!r.ok) throw new Error('HTTP ' + r.status);
                scUnifiedFlat.value = await r.json();
                scUnifiedFlatScopePairId.value = scopePid;
            } catch (e) {
                scUnifiedFlatError.value = String(e.message || e);
            } finally {
                scUnifiedFlatLoading.value = false;
            }
        }

        // ─── Stage Comparison: V2 режим вкладки «Расхождения» ───
        // Pair-scoped список расхождений текущей PDF-пары + ручная верификация.
        // Ничего не запускает (read-only к comparison_result.json); статусы
        // хранятся на сервере в pairs/<pid>/v2_review_status.json.
        function _scV2Base() {
            if (!scSession.value || !scActivePair.value) return '';
            const sid = encodeURIComponent(scSession.value.id);
            const pid = encodeURIComponent(scActivePair.value.id);
            return `/api/stage-comparison/sessions/${sid}/pairs/${pid}/v2`;
        }
        function scSetV2View(view) {
            scV2View.value = (view === 'v2') ? 'v2' : 'current';
            if (scV2View.value === 'v2') {
                scLoadV2Changes();
            }
        }
        async function scLoadV2Changes() {
            if (!scSession.value || !scActivePair.value) return;
            scV2Loading.value = true;
            scV2Error.value = '';
            try {
                const qs = scV2ShowFormal.value ? '?include_excluded=true' : '';
                const r = await fetch(`${_scV2Base()}/changes${qs}`);
                if (!r.ok) throw new Error('HTTP ' + r.status);
                scV2Data.value = await r.json();
            } catch (e) {
                scV2Error.value = 'Не удалось загрузить расхождения: ' + String(e.message || e);
                scV2Data.value = null;
            } finally {
                scV2Loading.value = false;
            }
        }
        // «Показать формальные» — подмешать админ/оформительские изменения
        // (include_excluded=true) и перезагрузить ведомость текущей пары.
        function scV2ToggleShowFormal() {
            scV2ShowFormal.value = !scV2ShowFormal.value;
            return scLoadV2Changes();
        }
        // ─── Профиль анализа результата (Быстрый / Глубокий ГРЩ) ───
        // Бейдж в шапке V2: каким профилем графического извлечения получен
        // результат. rich_grsh даёт пофидерные отличия ГРЩ (эталон), default —
        // быстрый. Старые результаты без метаданных → «неизвестен».
        function scV2ProfileBadge() {
            const ap = scV2Data.value && scV2Data.value.analysis_profile;
            if (!ap) return null;
            const name = String(ap.analysis_profile || 'unknown');
            if (name === 'rich_grsh') {
                return { name, label: 'Глубокий ГРЩ', style: 'background:#dcfce7;color:#166534',
                         title: 'Глубокий ГРЩ: пофидерное графическое извлечение однолинейных схем (эталонный профиль).' };
            }
            if (name === 'default') {
                return { name, label: 'Быстрый', style: 'background:#e2e8f0;color:#475569',
                         title: 'Быстрый режим: глубокое графическое извлечение ГРЩ выключено. Для ГРЩ-листов может найти меньше отличий.' };
            }
            return { name, label: 'неизвестен', style: 'background:#fef3c7;color:#92400e',
                     title: 'Профиль результата неизвестен (старый результат без метаданных профиля анализа).' };
        }
        function scV2DenseWarning() {
            const ap = scV2Data.value && scV2Data.value.analysis_profile;
            return !!(ap && ap.dense_graphics_default_profile);
        }
        function scV2DowngradeBlocked() {
            const ap = scV2Data.value && scV2Data.value.analysis_profile;
            return !!(ap && ap.profile_downgrade_blocked);
        }
        const scV2FilteredItems = computed(() => {
            const data = scV2Data.value;
            if (!data || !Array.isArray(data.items)) return [];
            const f = scV2Filters;
            const q = (f.search || '').trim().toLowerCase();
            return data.items.filter((it) => {
                if (f.severity && String(it.severity || '') !== f.severity) return false;
                if (f.source_layer && String(it.source_layer || '') !== f.source_layer) return false;
                if (f.quality_label && String(it.quality_label || '') !== f.quality_label) return false;
                if (f.review_status && String(it.review_status || '') !== f.review_status) return false;
                if (f.cost_impact && String(it.cost_impact || '') !== f.cost_impact) return false;
                if (f.impact_class && String(it.impact_class || '') !== f.impact_class) return false;
                if (q) {
                    const hay = [it.title, it.summary, it.old_value, it.new_value,
                                 it.evidence_left, it.evidence_right, it.sheet]
                                .map((x) => String(x || '').toLowerCase()).join(' ');
                    if (!hay.includes(q)) return false;
                }
                return true;
            });
        });
        const scV2SelectedIds = computed(() =>
            Object.keys(scV2Selected).filter((id) => scV2Selected[id]));
        const scV2AllSelected = computed(() => {
            const items = scV2FilteredItems.value;
            return items.length > 0 && items.every((it) => scV2Selected[it.id]);
        });
        function scV2ToggleAll(ev) {
            const on = ev && ev.target ? ev.target.checked : !scV2AllSelected.value;
            scV2FilteredItems.value.forEach((it) => { scV2Selected[it.id] = on; });
        }
        function scV2ToggleOne(id) {
            scV2Selected[id] = !scV2Selected[id];
        }
        function scV2SummaryCards() {
            const s = (scV2Data.value && scV2Data.value.summary) || {};
            return [
                { key: 'total',  label: 'всего',        value: s.total || 0,             bg: '#f8fafc', border: '#e2e8f0', fg: '#0f172a' },
                { key: 'high',   label: 'high',         value: s.high || 0,              bg: '#fee2e2', border: '#fecaca', fg: '#991b1b' },
                { key: 'medium', label: 'medium',       value: s.medium || 0,            bg: '#fef3c7', border: '#fde68a', fg: '#92400e' },
                { key: 'low',    label: 'low',          value: s.low || 0,               bg: '#dbeafe', border: '#bfdbfe', fg: '#1e40af' },
                { key: 'good',   label: 'good',         value: s.good || 0,              bg: '#dcfce7', border: '#bbf7d0', fg: '#166534' },
                { key: 'needs',  label: 'на проверку',  value: s.needs_human_review || 0, bg: '#fef9c3', border: '#fde68a', fg: '#854d0e' },
                { key: 'conf',   label: 'подтв.',       value: s.confirmed || 0,         bg: '#dcfce7', border: '#86efac', fg: '#15803d' },
                { key: 'rej',    label: 'отклон.',      value: s.rejected || 0,          bg: '#fee2e2', border: '#fca5a5', fg: '#b91c1c' },
                { key: 'notrev', label: 'не пров.',     value: s.not_reviewed || 0,      bg: '#f1f5f9', border: '#e2e8f0', fg: '#475569' },
            ];
        }
        // Однозначные счётчики ревью V2 для шапки. Раньше «10 из 38» можно было
        // прочитать как «принято + отклонено», хотя в 10 входят ещё и
        // автоматически исключённые (формальные) изменения. Разводим понятия:
        //   total          = engineering_total + excluded_total   (все raw-расхождения)
        //   processed      = confirmed + rejected + excluded       («обработано»)
        //   expert_decided = confirmed + rejected                  («экспертно решено»)
        //   not_reviewed   = total - processed
        // «Принято/Отклонено» считаем строго по ИНЖЕНЕРНЫМ (не исключённым)
        // строкам — так toggle «Показать формальные» (include_excluded) не двоит
        // счётчик, а engineering_total/excluded_total берём из backend-summary
        // (он считает корректно и стабилен независимо от toggle). Тот же скоуп по
        // текущим items + review_status, что и в scExpertReviewSummary — сироты
        // после регенерации сравнения в счётчик не попадают.
        function scV2ReviewProgress() {
            const s = (scV2Data.value && scV2Data.value.summary) || {};
            const items = (scV2Data.value && Array.isArray(scV2Data.value.items))
                ? scV2Data.value.items : [];
            let confirmed = 0, rejected = 0;
            for (const it of items) {
                if (it && it.excluded_from_main) continue;   // исключённые → в «Исключено»
                const rs = String((it && it.review_status) || '');
                if (rs === 'confirmed') confirmed++;
                else if (rs === 'rejected') rejected++;
            }
            const excluded = Number(s.excluded_total) || 0;
            const engineering = (s.engineering_total != null)
                ? (Number(s.engineering_total) || 0)
                : (Number(s.total) || 0);
            const total = engineering + excluded;
            const processed = confirmed + rejected + excluded;
            const expert_decided = confirmed + rejected;
            const not_reviewed = Math.max(0, total - processed);
            return { total, processed, expert_decided, confirmed, rejected, excluded, not_reviewed };
        }
        function scV2SourceLabel(s) { return scUnifiedSourceLabel(s); }
        // ─── Impact class (инженерная значимость) ───
        const _scV2ImpactLabels = {
            construction_cost_impact: 'стоимость',
            construction_technical_impact: 'строительство',
            procurement_impact: 'закупка',
            schedule_or_risk_impact: 'сроки/риск',
            design_solution_impact: 'проектное решение',
            engineering_system_impact: 'инж. система',
            manual_review_required: 'на проверку',
            admin_only: 'административное',
            documentation_only: 'оформление',
            cosmetic_or_noise: 'косметика/шум',
            unknown: 'не классиф.',
        };
        const _scV2ExcludedClasses = ['admin_only', 'documentation_only', 'cosmetic_or_noise'];
        function scV2ImpactLabel(cls) { return _scV2ImpactLabels[cls] || cls || '—'; }
        function scV2IsExcludedClass(cls) { return _scV2ExcludedClasses.includes(cls); }
        function scV2ImpactBadgeStyle(cls) {
            if (_scV2ExcludedClasses.includes(cls)) return 'background:#e5e7eb; color:#4b5563';
            if (cls === 'construction_cost_impact' || cls === 'procurement_impact') return 'background:#fef3c7; color:#92400e';
            if (cls === 'manual_review_required') return 'background:#fef9c3; color:#854d0e';
            if (cls === 'unknown') return 'background:#f1f5f9; color:#475569';
            return 'background:#dcfce7; color:#166534';
        }
        const scV2ImpactClassOptions = [
            { v: '', l: 'Impact: все' },
            { v: 'construction_technical_impact', l: 'строительство' },
            { v: 'construction_cost_impact', l: 'стоимость' },
            { v: 'procurement_impact', l: 'закупка' },
            { v: 'schedule_or_risk_impact', l: 'сроки/риск' },
            { v: 'design_solution_impact', l: 'проектное решение' },
            { v: 'engineering_system_impact', l: 'инж. система' },
            { v: 'manual_review_required', l: 'на проверку' },
            { v: 'admin_only', l: 'административное' },
            { v: 'documentation_only', l: 'оформление' },
            { v: 'cosmetic_or_noise', l: 'косметика/шум' },
            { v: 'unknown', l: 'не классиф.' },
        ];
        function scV2ExportXlsxUrl() {
            const base = _scV2Base();
            if (!base) return '';
            return `${base}/export.xlsx`;
        }
        // Локально применяет статус/комментарий к строке (оптимистично), затем
        // перечитывает список, чтобы summary совпадал с сервером.
        async function _scV2ApplyPatch(changeId, patch) {
            const base = _scV2Base();
            if (!base) return false;
            scV2SaveBusy.value = true;
            scV2Error.value = '';
            try {
                const r = await fetch(`${base}/changes/${encodeURIComponent(changeId)}`, {
                    method: 'PATCH',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(patch),
                });
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    throw new Error(j.detail || ('HTTP ' + r.status));
                }
                return true;
            } catch (e) {
                scV2Error.value = 'Сохранение не удалось: ' + String(e.message || e);
                return false;
            } finally {
                scV2SaveBusy.value = false;
            }
        }
        async function scV2SetStatus(item, status) {
            const ok = await _scV2ApplyPatch(item.id, { review_status: status });
            if (ok) { item.review_status = status; await scLoadV2Changes(); }
        }
        async function scV2SaveComment(item, value) {
            const ok = await _scV2ApplyPatch(item.id, { review_comment: String(value || '') });
            if (ok) { item.review_comment = String(value || ''); await scLoadV2Changes(); }
        }
        async function scV2BulkStatus(status) {
            const base = _scV2Base();
            const ids = scV2SelectedIds.value;
            if (!base || !ids.length) return;
            scV2SaveBusy.value = true;
            scV2Error.value = '';
            try {
                const r = await fetch(`${base}/changes`, {
                    method: 'PATCH',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ ids, patch: { review_status: status } }),
                });
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    throw new Error(j.detail || ('HTTP ' + r.status));
                }
                await scLoadV2Changes();
            } catch (e) {
                scV2Error.value = 'Массовое сохранение не удалось: ' + String(e.message || e);
            } finally {
                scV2SaveBusy.value = false;
            }
        }
        // «Перейти к месту» — открыть пару и перейти к листу/слоту (если есть
        // location), иначе go-to сам отработает мягко. Делегируем существующему
        // резолверу (тот же, что у unified-расхождений).
        function scV2Goto(item) { return scGotoTextChange(item); }

        // ─── Stage Comparison: Экспертная оценка расхождений ───
        // Хранение per-session по raw `id`. Группа агрегирует через source_finding_ids.
        // Активная пара, в контексте которой эксперт размечает. Вкладка
        // «Расхождения» привязана к scActivePair, кроме режима «показать все».
        function _scExpertScopePairId() {
            if (!scUnifiedShowAllPairs.value && scActivePair.value && scActivePair.value.id) {
                return String(scActivePair.value.id);
            }
            return null;
        }
        // Голые raw change id (chg_…/uf_…) строки или группы.
        function _scRawChangeIds(itemOrGroup) {
            if (!itemOrGroup || typeof itemOrGroup !== 'object') return [];
            const sf = itemOrGroup.source_finding_ids;
            if (Array.isArray(sf) && sf.length) return sf.filter(Boolean).map(String);
            if (itemOrGroup.id) return [String(itemOrGroup.id)];
            return [];
        }
        // Пары, к которым относится строка/группа: в скоупе активной пары — она;
        // в режиме «показать все» — own pair_id (flat) либо affected_pair_ids (группа).
        function _scPairIdsFor(itemOrGroup) {
            const scope = _scExpertScopePairId();
            if (scope) return [scope];
            if (itemOrGroup && itemOrGroup.pair_id) return [String(itemOrGroup.pair_id)];
            const ap = itemOrGroup && itemOrGroup.affected_pair_ids;
            if (Array.isArray(ap) && ap.length) return ap.filter(Boolean).map(String);
            return [];
        }
        // СОСТАВНЫЕ ключи решений `<pair_id>::<raw_id>`. Скоуп по паре — поэтому
        // одинаковые штамповые id (chg_customer, chg_stamp_org …) в разных
        // парах больше не делят один вердикт.
        function _scExpertRawIds(itemOrGroup) {
            const raws = _scRawChangeIds(itemOrGroup);
            if (!raws.length) return [];
            const pids = _scPairIdsFor(itemOrGroup);
            if (!pids.length) return raws;   // нет контекста пары — legacy fallback
            const keys = [];
            for (const pid of pids) {
                for (const r of raws) keys.push(`${pid}::${r}`);
            }
            return keys;
        }
        function scGetExpertDecision(itemOrGroup) {
            // Возвращает 'accepted' | 'rejected' | 'mixed' | null
            const ids = _scExpertRawIds(itemOrGroup);
            if (!ids.length) return null;
            const decisions = new Set();
            for (const rid of ids) {
                const d = (scExpertDecisions.value[rid] || {}).decision;
                if (d) decisions.add(d);
            }
            if (decisions.size === 0) return null;
            if (decisions.size > 1) return 'mixed';
            return [...decisions][0];
        }
        // Решение ДЛЯ ОТОБРАЖЕНИЯ в колонке «Решение». Сначала явное
        // живое/перенесённое expert-решение (scGetExpertDecision, ключ pid::id),
        // а если его нет — канонический review_status (его backend резолвит по
        // стабильному raw_id, и именно по нему summary считает «Принято/Отклонено»).
        // Без этого fallback строка, попавшая в summary как confirmed/rejected,
        // не получала галочку, если решение эксперта сохранено под ключом
        // pid::raw_id, а не pid::v2_id (расхождение «Принято: 8» в summary vs
        // меньше галочек в таблице — orphan-ключи legacy expert_review).
        // ВАЖНО: только ОТОБРАЖЕНИЕ. Логика редактирования (scSetExpertDecision
        // toggle, _scDecisionRank) по-прежнему опирается на scGetExpertDecision —
        // только явные клики, иначе первый клик по «унаследованной» строке снял
        // бы отметку вместо подтверждения.
        function scResolvedDecision(itemOrGroup) {
            const d = scGetExpertDecision(itemOrGroup);
            if (d) return d;   // 'accepted' | 'rejected' | 'mixed'
            const rs = itemOrGroup && itemOrGroup.review_status;
            if (rs === 'confirmed') return 'accepted';
            if (rs === 'rejected') return 'rejected';
            return null;
        }
        // Приоритет в очереди: принятые сверху (0), нерешённые/смешанные в
        // середине (1), отклонённые внизу (2).
        function _scDecisionRank(itemOrGroup) {
            const d = scGetExpertDecision(itemOrGroup);
            if (d === 'accepted') return 0;
            if (d === 'rejected') return 2;
            return 1;
        }
        function scGetExpertReason(itemOrGroup) {
            const ids = _scExpertRawIds(itemOrGroup);
            const reasons = [];
            const seen = new Set();
            for (const rid of ids) {
                const r = (scExpertDecisions.value[rid] || {}).rejection_reason || '';
                if (r && !seen.has(r)) { reasons.push(r); seen.add(r); }
            }
            return reasons.join(' / ');
        }
        // Флаги переноса для значка в ячейке «Решение»: была ли отметка
        // перенесена из «Расхождений», нужна ли ручная проверка, есть ли конфликт.
        function scExpertItemFlags(itemOrGroup) {
            const ids = _scExpertRawIds(itemOrGroup);
            const out = { transferred: false, needs_review: false, conflict: false };
            for (const rid of ids) {
                const e = scExpertDecisions.value[rid];
                if (!e) continue;
                if (e.transferred) out.transferred = true;
                if (e.needs_review) out.needs_review = true;
                if (e.conflict) out.conflict = true;
            }
            return out;
        }
        function scSetExpertDecision(itemOrGroup, decision) {
            const ids = _scExpertRawIds(itemOrGroup);
            if (!ids.length) return;
            const map = { ...scExpertDecisions.value };
            const current = scGetExpertDecision(itemOrGroup);
            // Toggle off если повторный клик по уже активному решению.
            const toggleOff = (current === decision);
            // Сохраняем общую причину при переключении вердикта.
            const sharedReason = scGetExpertReason(itemOrGroup);
            for (const rid of ids) {
                if (toggleOff) {
                    // Оставляем ключ с decision=null — submit зафиксирует removed.
                    map[rid] = { decision: null, rejection_reason: '' };
                } else {
                    map[rid] = {
                        decision,
                        rejection_reason: sharedReason || ((map[rid] || {}).rejection_reason || ''),
                    };
                }
            }
            scExpertDecisions.value = map;
        }
        function scSetExpertReason(itemOrGroup, reason) {
            const ids = _scExpertRawIds(itemOrGroup);
            if (!ids.length) return;
            const map = { ...scExpertDecisions.value };
            for (const rid of ids) {
                const existing = map[rid] || { decision: 'rejected', rejection_reason: '' };
                map[rid] = { ...existing, rejection_reason: reason };
            }
            scExpertDecisions.value = map;
        }
        // Решения, относящиеся к текущей PDF-паре. Ключи составные
        // `<pair_id>::<raw_id>`, поэтому фильтруем по префиксу активной пары.
        // Без активной пары (теоретически) — возвращаем все.
        function _scExpertDecisionsForActivePair() {
            const pid = _scExpertScopePairId();
            const prefix = pid ? pid + '::' : null;
            const out = [];
            for (const [k, d] of Object.entries(scExpertDecisions.value)) {
                if (prefix && !k.startsWith(prefix)) continue;
                out.push([k, d]);
            }
            return out;
        }
        // Составные ключи `<pair>::<raw>`, у которых СЕЙЧАС есть строка в
        // таблице «Расхождения» (из загруженного scUnifiedFlat). Решения по
        // raw_id, которого уже нет в changes (остался от прошлой регенерации
        // сравнения), снять галочкой нечем — поэтому в счётчик они не входят.
        // Возвращает null (= не скоупим, legacy) для V2-вида (там строки
        // хранятся отдельно), пока flat не загружен, или когда flat загружен
        // для другой пары (момент переключения).
        function _scSummaryKnownKeys() {
            if (scV2View.value === 'v2') return null;
            const flat = scUnifiedFlat.value;
            if (!flat || !Array.isArray(flat.items)) return null;
            if (!scUnifiedShowAllPairs.value && scActivePair.value
                && scUnifiedFlatScopePairId.value
                && String(scUnifiedFlatScopePairId.value) !== String(scActivePair.value.id)) {
                return null;
            }
            const keys = new Set();
            for (const it of flat.items) {
                for (const k of _scExpertRawIds(it)) keys.add(k);
            }
            return keys;
        }
        function scExpertReviewSummary() {
            // Счётчики «Принято/Отклонено» — только по ТЕКУЩЕЙ паре.
            const v2view = (scV2View.value === 'v2');
            if (v2view) {
                // V2: считаем строго по текущим загруженным изменениям пары
                // (scV2Data.items), а НЕ по всем expert-решениям сессии. id вида
                // `v2_<hash>` — контент-хеши: при регенерации сравнения они
                // меняются, оставляя осиротевшие expert-решения от прошлых
                // прогонов. Раньше V2-ветка не скоупила решения по текущим
                // строкам и накручивала этих сирот (на ИОС1.1 шапка показывала
                // 26/4 вместо честных 10). Теперь источник решения каждой строки
                // тот же, что и в самой таблице: живой экспертный клик
                // (ключ = текущий pid::item.id, сиротой быть не может) ИЛИ
                // канонический review_status (confirmed/rejected; regen-устойчив,
                // т.к. бэкенд резолвит его по стабильному raw_id). Согласовано с
                // backend `_per_pair_status` (тот же «размечено N из M»).
                const items = (scV2Data.value && Array.isArray(scV2Data.value.items))
                    ? scV2Data.value.items : [];
                let accepted = 0, rejected = 0;
                for (const it of items) {
                    let d = null;
                    if (scExpertReviewMode.value) {
                        const ed = scGetExpertDecision(it);   // ключ = текущий pid::item.id
                        if (ed === 'accepted' || ed === 'rejected') d = ed;
                    }
                    if (!d) {
                        const rs = String((it && it.review_status) || '');
                        if (rs === 'confirmed') d = 'accepted';
                        else if (rs === 'rejected') d = 'rejected';
                    }
                    if (d === 'accepted') accepted++;
                    else if (d === 'rejected') rejected++;
                }
                return { accepted, rejected, total: accepted + rejected };
            }
            // Классический вид «Расхождения» — поведение без изменений: считаем по
            // chg_/uf_ ключам, скоуп по строкам загруженного scUnifiedFlat
            // (orphan-решения по исчезнувшим raw_id в счётчик не входят).
            const known = _scSummaryKnownKeys();   // не-null только для классического вида
            const vals = _scExpertDecisionsForActivePair()
                .filter(([k]) => {
                    const raw = String(k).split('::').slice(1).join('::');
                    if (raw.startsWith('v2_')) return false;   // только chg_/uf_
                    return known === null || known.has(k);
                })
                .map(([, d]) => d);
            return {
                accepted: vals.filter(d => d.decision === 'accepted').length,
                rejected: vals.filter(d => d.decision === 'rejected').length,
                total: vals.filter(d => d.decision).length,
            };
        }
        async function scLoadExpertDecisions() {
            if (!scSession.value) return;
            try {
                const sid = encodeURIComponent(scSession.value.id);
                const r = await fetch(`/api/stage-comparison/sessions/${sid}/expert-review`);
                if (!r.ok) throw new Error('HTTP ' + r.status);
                const data = await r.json();
                const decisions = data.decisions || {};
                const map = {};
                for (const [rid, entry] of Object.entries(decisions)) {
                    if (entry && entry.decision) {
                        map[rid] = {
                            decision: entry.decision,
                            rejection_reason: entry.rejection_reason || '',
                            // Метаданные переноса из «Расхождений» (если есть).
                            needs_review: !!entry.needs_review,
                            conflict: !!entry.conflict,
                            transferred: !!entry.transferred,
                        };
                    }
                }
                scExpertDecisions.value = map;
                scExpertReviewLoaded.value = true;
            } catch (e) {
                console.warn('Failed to load SC expert review:', e);
            }
        }
        async function scToggleExpertReview() {
            scExpertReviewMode.value = !scExpertReviewMode.value;
            if (scExpertReviewMode.value && !scExpertReviewLoaded.value && scSession.value) {
                await scLoadExpertDecisions();
            }
        }
        // Per-pair статус разметки для колонки «Проверено экспертом».
        async function scLoadExpertPerPair() {
            if (!scSession.value) return;
            try {
                const sid = encodeURIComponent(scSession.value.id);
                const r = await fetch(`/api/stage-comparison/sessions/${sid}/expert-review?include_pairs=true`);
                if (!r.ok) throw new Error('HTTP ' + r.status);
                const data = await r.json();
                scExpertPerPair.value = data.per_pair || {};
            } catch (e) {
                console.warn('Failed to load SC expert per-pair status:', e);
            }
        }
        // Бейдж колонки «Проверено экспертом». Галочка появляется только когда
        // у пары есть расхождения и КАЖДОЕ размечено эксперт (accept/reject).
        function scPairExpertBadge(pairId) {
            const st = scExpertPerPair.value[pairId];
            if (!st || !st.total) {
                return {label: '—', cls: 'sc-status-unmatched', title: 'расхождений нет либо сравнение не выполнено'};
            }
            if (st.fully_verified) {
                return {label: '✓ проверено', cls: 'sc-status-matched',
                        title: `все ${st.total} расхождений размечены экспертом`};
            }
            return {label: `${st.decided}/${st.total}`, cls: 'sc-status-maybe',
                    title: `размечено ${st.decided} из ${st.total} расхождений — поставьте ✓/✗ против каждого на этапе «3. Расхождения»`};
        }
        async function scSubmitExpertReview() {
            if (!scSession.value) return;
            scExpertReviewSaving.value = true;
            try {
                // Сохраняем решения ТОЛЬКО текущей пары — чужие пары backend не
                // трогает (apply_batch обновляет лишь присланные ключи). Это
                // исключает кросс-парное «сохранилось всё».
                const decisions = [];
                const removedIds = [];
                for (const [rid, d] of _scExpertDecisionsForActivePair()) {
                    if (d && d.decision) {
                        decisions.push({
                            item_id: rid,
                            decision: d.decision,
                            rejection_reason: d.rejection_reason || '',
                        });
                    } else if (d) {
                        // Ключ есть, decision=null — пользователь снял решение.
                        removedIds.push(rid);
                    }
                }
                const sid = encodeURIComponent(scSession.value.id);
                const r = await fetch(`/api/stage-comparison/sessions/${sid}/expert-review`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ decisions, removed_ids: removedIds, reviewer: currentUserName() }),
                });
                if (!r.ok) {
                    const err = await r.json().catch(() => ({}));
                    throw new Error(err.detail || ('HTTP ' + r.status));
                }
                await r.json();
                // Сводка по текущей паре (а не по всей сессии).
                const sum = scExpertReviewSummary();
                alert(`Сохранено по текущей паре: ${sum.accepted} принято, ${sum.rejected} отклонено`);
                // Обновить per-pair статус для колонки «Проверено экспертом».
                try { await scLoadExpertPerPair(); } catch (_) {}
            } catch (e) {
                console.error('SC expert review submit error:', e);
                alert('Ошибка сохранения: ' + (e.message || e));
            } finally {
                scExpertReviewSaving.value = false;
            }
        }
        // Перенести решения «принято/отклонено» + комментарии из классических
        // «Расхождений» в V2 по ВСЕЙ сессии. Точные совпадения — мгновенно,
        // переименованные/слитые находки сопоставляет Claude (может занять
        // пару минут на пары, перепрогнанные Opus).
        async function scV2TransferReviews() {
            if (!scSession.value || scV2TransferBusy.value) return;
            if (!confirm('Перенести экспертные оценки из «Расхождений» в V2 по всей сессии?\n\nТочные совпадения переносятся сразу; переименованные находки сверяет Claude — это может занять пару минут.')) return;
            scV2TransferBusy.value = true;
            try {
                const sid = encodeURIComponent(scSession.value.id);
                const r = await fetch(`/api/stage-comparison/sessions/${sid}/v2-review/transfer`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ use_claude: true }),
                });
                if (!r.ok) {
                    const err = await r.json().catch(() => ({}));
                    throw new Error(err.detail || ('HTTP ' + r.status));
                }
                const rep = await r.json();
                const t = rep.totals || {};
                // Перезагрузить решения и список V2, показать оценку.
                scExpertReviewLoaded.value = false;
                await scLoadExpertDecisions();
                scExpertReviewMode.value = true;
                try { await scLoadV2Changes(); } catch (_) {}
                try { await scLoadExpertPerPair(); } catch (_) {}
                let msg = `Перенос завершён по ${t.pairs_processed || 0} парам.\n`
                    + `Перенесено: ${t.applied || 0} (точных ${t.exact || 0}, по смыслу ${t.semantic || 0}).\n`
                    + `Совпало с уже отмеченным: ${t.consistent_existing || 0}.\n`
                    + `Конфликтов: ${t.conflicts || 0} · «проверить»: ${t.needs_review || 0} · не сопоставлено: ${t.unmatched_source || 0}.`;
                if (!rep.claude_available && (t.unmatched_source || 0) > 0) {
                    msg += `\n\n⚠ Claude был недоступен (${rep.claude_unavailable_reason || 'причина неизвестна'}) — перенесены только точные совпадения.`;
                }
                alert(msg);
            } catch (e) {
                console.error('SC v2 transfer error:', e);
                alert('Ошибка переноса: ' + (e.message || e));
            } finally {
                scV2TransferBusy.value = false;
            }
        }
        async function scUnifiedToggleShowAllPairs() {
            // Toggle между «текущая пара» и «вся сессия». В режиме «вся сессия»
            // показывается баннер-предупреждение, чтобы пользователь не путал.
            if (scUnifiedShowAllPairs.value) {
                // выключаем — возвращаемся к текущей паре
                scUnifiedShowAllPairs.value = false;
            } else {
                if (!confirm('Показать расхождения по ВСЕМ PDF-парам сессии? Сейчас отображается текущая пара.')) return;
                scUnifiedShowAllPairs.value = true;
            }
            // Сбросить локальный pair-фильтр, чтобы он не дублировал backend-фильтр.
            scUnifiedFilterPair.value = '';
            await scLoadUnifiedFlat();
        }
        async function scOpenUnifiedPairPreflight() {
            if (!scSession.value || !scActivePair.value) return;
            scUnifiedPreflightScope.value = 'pair';
            scUnifiedPreflightError.value = '';
            scUnifiedPreflight.value = null;
            scUnifiedPreflightLoading.value = true;
            scUnifiedPreflightOpen.value = true;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/unified-analysis/preflight`;
                const r = await fetch(url, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        force_enrichment: !!scUnifiedForceEnrichment.value,
                        force_compare: !!scUnifiedForceCompare.value,
                    }),
                });
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    throw new Error(j.detail || ('HTTP ' + r.status));
                }
                scUnifiedPreflight.value = await r.json();
            } catch (e) {
                scUnifiedPreflightError.value = 'Preflight не выполнен: ' + e;
            } finally {
                scUnifiedPreflightLoading.value = false;
            }
        }
        async function scOpenUnifiedSessionPreflight() {
            if (!scSession.value) return;
            scUnifiedPreflightScope.value = 'session';
            scUnifiedPreflightError.value = '';
            scUnifiedPreflight.value = null;
            scUnifiedPreflightLoading.value = true;
            scUnifiedPreflightOpen.value = true;
            // Session-preflight = aggregated preflight по всем парам через jobs
            // endpoint без confirm (rejected_no_confirm возвращает items[]).
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/unified-analysis-jobs`;
                const r = await fetch(url, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        scope: 'session',
                        confirm: false,
                        force_enrichment: !!scUnifiedForceEnrichment.value,
                        force_compare: !!scUnifiedForceCompare.value,
                    }),
                });
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    throw new Error(j.detail || ('HTTP ' + r.status));
                }
                const job = await r.json();
                // Используем item-список из rejected job как preview
                scUnifiedPreflight.value = {
                    scope: 'session',
                    total_pairs: (job.items || []).length,
                    runnable_pairs: (job.items || []).length,
                    items: (job.items || []).map(it => ({pair_id: it.pair_id, status: it.status})),
                    note: 'Preflight для сессии: будет запущено по всем парам с MD.',
                };
            } catch (e) {
                scUnifiedPreflightError.value = 'Preflight не выполнен: ' + e;
            } finally {
                scUnifiedPreflightLoading.value = false;
            }
        }
        function scCloseUnifiedPreflight() {
            scUnifiedPreflightOpen.value = false;
            scUnifiedPreflightError.value = '';
        }
        async function scRunUnifiedPair() {
            if (!scSession.value || !scActivePair.value) return;
            scUnifiedError.value = '';
            scUnifiedRunning.value = true;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/unified-analysis`;
                const r = await fetch(url, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        confirm: true,
                        force_enrichment: !!scUnifiedForceEnrichment.value,
                        force_compare: !!scUnifiedForceCompare.value,
                    }),
                });
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    throw new Error(j.detail || ('HTTP ' + r.status));
                }
                const data = await r.json();
                scUnifiedPairStatus.value = null; // force reload
                await scLoadUnifiedPairStatus();
                await scLoadUnifiedFlat();
                scCloseUnifiedPreflight();
                return data;
            } catch (e) {
                scUnifiedError.value = 'Запуск unified-анализа не удался: ' + e;
            } finally {
                scUnifiedRunning.value = false;
            }
        }
        async function scRunUnifiedSession() {
            if (!scSession.value) return;
            scUnifiedError.value = '';
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/unified-analysis-jobs`;
                const r = await fetch(url, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        scope: 'session',
                        confirm: true,
                        force_enrichment: !!scUnifiedForceEnrichment.value,
                        force_compare: !!scUnifiedForceCompare.value,
                    }),
                });
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    throw new Error(j.detail || ('HTTP ' + r.status));
                }
                const job = await r.json();
                scUnifiedJob.value = job;
                scCloseUnifiedPreflight();
                if (job.id && (job.status === 'queued' || job.status === 'running')) {
                    scPollUnifiedJob(job.id);
                }
            } catch (e) {
                scUnifiedError.value = 'Запуск unified-анализа сессии не удался: ' + e;
            }
        }
        async function scPollUnifiedJob(jobId) {
            if (!scSession.value || !jobId) return;
            if (scUnifiedJobPolling.value) return;
            scUnifiedJobPolling.value = true;
            try {
                while (true) {
                    const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/unified-analysis-jobs/${encodeURIComponent(jobId)}`;
                    const r = await fetch(url);
                    if (!r.ok) break;
                    const job = await r.json();
                    scUnifiedJob.value = job;
                    if (['done', 'failed', 'cancelled', 'rejected_no_confirm'].includes(job.status)) break;
                    await new Promise(res => setTimeout(res, 3000));
                }
                try { await scLoadUnifiedFlat(); } catch (_) {}
                try { await scLoadPairCompareStatuses(); } catch (_) {}
                if (scActivePair.value) {
                    try { await scLoadUnifiedPairStatus(); } catch (_) {}
                }
            } finally {
                scUnifiedJobPolling.value = false;
            }
        }
        async function scCancelUnifiedJob() {
            const job = scUnifiedJob.value;
            if (!job || !job.id) return;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/unified-analysis-jobs/${encodeURIComponent(job.id)}/cancel`;
                const r = await fetch(url, {method: 'POST'});
                if (r.ok) scUnifiedJob.value = await r.json();
            } catch (e) { /* silent */ }
        }

        // ── Opus session batch helpers (этап «1. Загрузка документации») ──

        // Preflight (dry-run) — обновляет панель счётчиков перед запуском.
        async function scOpusLoadPreflight() {
            if (!scSession.value || !scSession.value.id) { scOpusPreflight.value = null; return; }
            scOpusPreflightLoading.value = true;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/unified-analysis-jobs/preflight`;
                const r = await fetch(url, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({scope: 'session', force_compare: false}),
                });
                if (!r.ok) { scOpusPreflight.value = null; return; }
                scOpusPreflight.value = await r.json();
            } catch (_) {
                scOpusPreflight.value = null;
            } finally {
                scOpusPreflightLoading.value = false;
            }
        }

        async function scOpusPoll(jobId) {
            if (!scSession.value || !jobId) return;
            if (scOpusPolling.value) return;
            scOpusPolling.value = true;
            try {
                while (true) {
                    const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/unified-analysis-jobs/${encodeURIComponent(jobId)}`;
                    let r;
                    try { r = await fetch(url); } catch (_) {
                        await new Promise(res => setTimeout(res, 3000));
                        continue;
                    }
                    if (!r.ok) break;
                    const job = await r.json();
                    scOpusJob.value = job;
                    if (['done', 'failed', 'cancelled', 'rejected_no_confirm', 'failed_interrupted'].includes(job.status)) break;
                    await new Promise(res => setTimeout(res, 3000));
                }
                // После job — синхронизировать flat/grouped с диска (backend
                // их уже пересобрал per-pair и финально).
                try { await scLoadUnifiedFlat(); } catch (_) {}
                try { await scLoadPairCompareStatuses(); } catch (_) {}
                if (scActivePair.value) {
                    try { await scLoadUnifiedPairStatus(); } catch (_) {}
                }
            } finally {
                scOpusPolling.value = false;
            }
        }

        // Сохранить/обновить item конкретной пары из single-pair fallback job.
        // Тегаем _via_fallback, чтобы бейдж показал «(fallback)».
        function scOpusSetFallbackItem(pairId, job) {
            const items = (job && Array.isArray(job.items)) ? job.items : [];
            let it = null;
            for (const x of items) { if (x && x.pair_id === pairId) it = x; }
            if (!it) {
                // job ещё без items для пары — отразим общий статус job.
                it = {pair_id: pairId, status: (job && job.status) || 'queued'};
            }
            // Live per-chunk прогресс из aggregate (single-pair job его несёт).
            const agg = job && job.aggregate;
            const fbProg = (agg && agg.current_pair_id === pairId) ? agg.current_pair_fallback : null;
            scOpusFallbackByPair.value = {
                ...scOpusFallbackByPair.value,
                [pairId]: {...it, _via_fallback: true, _fallback_progress: fbProg || null},
            };
        }

        // Клик по бейджу «⚠ файл большой ▸ fallback»: запустить
        // evidence_first_s2_fallback для одной пары на готовых enriched MD.
        // НЕ запускает Qwen, НЕ поднимает общий лимит, НЕ меняет алгоритм —
        // только явный per-pair gate (force_fallback). Single-pair job, чтобы
        // не затирать бейджи остальных пар.
        async function scOpusRunFallbackForPair(pairId) {
            if (!scSession.value || !scSession.value.id || !pairId) return;
            if (scOpusFallbackStarting.value[pairId]) return;
            // Не стартуем, если по этой паре уже идёт fallback.
            const existing = scOpusFallbackByPair.value[pairId];
            if (existing && ['queued', 'running', 'comparing', 'enriching'].includes(String(existing.status || '').toLowerCase())) return;
            scOpusFallbackStarting.value = {...scOpusFallbackStarting.value, [pairId]: true};
            scOpusError.value = '';
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/unified-analysis-jobs`;
                const r = await fetch(url, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        scope: 'selected',
                        pair_ids: [pairId],
                        confirm: true,
                        // Qwen НЕ запускаем — enriched MD уже готов.
                        force_enrichment: false,
                        // Пересчитываем сравнение этой пары.
                        force_compare: true,
                        // Не фильтруем по too_large — наоборот, гоним fallback.
                        skip_ineligible: false,
                        // Явный per-pair override: fallback даже при флаге OFF.
                        force_fallback: true,
                    }),
                });
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    const msg = (j && j.detail && (j.detail.message || j.detail)) || ('HTTP ' + r.status);
                    throw new Error(typeof msg === 'string' ? msg : JSON.stringify(msg));
                }
                const job = await r.json();
                scOpusSetFallbackItem(pairId, job);
                if (job && job.id && (job.status === 'queued' || job.status === 'running')) {
                    scOpusPollFallback(job.id, pairId);
                }
            } catch (e) {
                scOpusError.value = String(e.message || e);
            } finally {
                scOpusFallbackStarting.value = {...scOpusFallbackStarting.value, [pairId]: false};
            }
        }

        // Polling single-pair fallback job. Пишет в scOpusFallbackByPair, не
        // трогает scOpusJob (бейджи остальных пар не страдают).
        async function scOpusPollFallback(jobId, pairId) {
            if (!scSession.value || !jobId || !pairId) return;
            if (scOpusFallbackPolling.value[pairId]) return;
            scOpusFallbackPolling.value = {...scOpusFallbackPolling.value, [pairId]: true};
            try {
                while (true) {
                    const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/unified-analysis-jobs/${encodeURIComponent(jobId)}`;
                    let r;
                    try { r = await fetch(url); } catch (_) {
                        await new Promise(res => setTimeout(res, 3000));
                        continue;
                    }
                    if (!r.ok) break;
                    const job = await r.json();
                    scOpusSetFallbackItem(pairId, job);
                    if (['done', 'failed', 'cancelled', 'rejected_no_confirm', 'failed_interrupted'].includes(job.status)) break;
                    await new Promise(res => setTimeout(res, 3000));
                }
                // Подтянуть свежие расхождения с диска (backend пересобрал).
                try { await scLoadUnifiedFlat(); } catch (_) {}
                try { await scLoadPairCompareStatuses(); } catch (_) {}
                if (scActivePair.value) {
                    try { await scLoadUnifiedPairStatus(); } catch (_) {}
                }
            } finally {
                scOpusFallbackPolling.value = {...scOpusFallbackPolling.value, [pairId]: false};
            }
        }

        async function scOpusRestoreActive() {
            if (!scSession.value || !scSession.value.id) return;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/unified-analysis-jobs/active`;
                const r = await fetch(url);
                if (!r.ok) return;
                const data = await r.json();
                const job = data && data.job;
                if (!job) return;
                scOpusJob.value = job;
                if (job.id && (job.status === 'queued' || job.status === 'running') && !scOpusPolling.value) {
                    scOpusStartedAtClient.value = Date.now();
                    scOpusPoll(job.id);
                }
            } catch (_) { /* silent */ }
        }

        // Подтянуть персистентные статусы сравнения с диска (источник истины
        // для колонки «Сравнение»). Read-only, без LLM. Вызывается при открытии
        // сессии и после завершения любого сравнения (session/fallback).
        async function scLoadPairCompareStatuses() {
            if (!scSession.value || !scSession.value.id) return;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/comparison-statuses`;
                const r = await fetch(url);
                if (!r.ok) return;
                const data = await r.json();
                scPairCompareStatus.value = (data && data.statuses) || {};
            } catch (_) { /* silent */ }
        }

        // Live per-chunk прогресс fallback'а текущей пары (evidence_first_s2).
        // Источник — aggregate.current_pair_fallback. Возвращает null, если
        // пара сравнивается обычным (не-fallback) путём.
        function scOpusFallbackLabel() {
            const j = scOpusJob.value;
            const fb = j && j.aggregate && j.aggregate.current_pair_fallback;
            if (!fb) return null;
            const total = Number(fb.total_chunks || 0);
            const idx = Number(fb.current_chunk_index || 0);
            const done = Number(fb.done_chunks || 0);
            const phase = String(fb.phase || '');
            if (phase === 'starting' || !total) {
                return {text: 'fallback · готовлю разбиение…', eta: '', pct: 0, title: ''};
            }
            const eta = fb.eta_sec ? scFormatDuration(fb.eta_sec) : '';
            const pct = total ? Math.min(100, Math.round(done / total * 100)) : 0;
            const head = (phase === 'done')
                ? `fallback · чанк ${total} / ${total} · финализация…`
                : `fallback · чанк ${idx} / ${total}`;
            return {text: head, eta, pct, title: String(fb.current_chunk_title || '')};
        }

        function scOpusElapsedLabel() {
            const j = scOpusJob.value;
            if (!j) return '—';
            const agg = j.aggregate || {};
            if (agg.duration_sum_sec) return scFormatDuration(agg.duration_sum_sec);
            if (scOpusStartedAtClient.value) {
                return scFormatDuration((Date.now() - scOpusStartedAtClient.value) / 1000);
            }
            return '—';
        }

        function scOpusCurrentPairLabel() {
            const j = scOpusJob.value;
            if (!j || !j.aggregate || !j.aggregate.current_pair_id) return '—';
            const pid = j.aggregate.current_pair_id;
            const pairs = (scSession.value && scSession.value.pairs) || [];
            const p = pairs.find(x => x.id === pid);
            if (!p) return pid;
            const left = (p.left || {}).filename || '—';
            const right = (p.right || {}).filename || '—';
            return `${left} ↔ ${right}`;
        }

        // computed: текст title для кнопки запуска Opus batch (disabled-причины).
        const scOpusStartTitle = computed(() => {
            if (!scPairs.value || !scPairs.value.length) return 'Сначала сопоставьте PDF-пары';
            if (scRecogJob.value && ['queued','running'].includes(scRecogJob.value.status)) {
                return 'Дождитесь окончания распознавания графики';
            }
            if (scOpusPreflight.value && scOpusPreflight.value.will_run === 0) {
                return 'Нет ни одной готовой к сравнению пары (нет enriched MD или все слишком большие). Запустите «Распознать графику».';
            }
            return 'Запустить Opus сравнение всех готовых PDF-пар сессии';
        });
        // Stage 1: при возврате на вкладку «Загрузка документации» подтягиваем
        // актуальный статус Qwen-job, а если она running — снова запускаем polling.
        watch(() => scTab.value, (newTab) => {
            if (newTab === 'upload') {
                if (!scSession.value || !scSession.value.id) return;
                scRecogRestoreActive();
                scOpusRestoreActive();
                scQORestoreActive();
                scOpusLoadPreflight();
                // Возврат с «3. Расхождения» — обновить колонку «Проверено экспертом».
                scLoadExpertPerPair();
                return;
            }
            // При входе на «3. Расхождения» нужно явно запустить загрузку
            // активного subtab'а. Раньше это делала кнопка-переключатель
            // «Расхождения» (scSwitchDiffSubtab), но она убрана из UI — без
            // этого триггера таблица не появлялась до клика по «Сгруппировано»
            // / «Все расхождения».
            if (newTab === 'diffs') {
                if (!scActivePair.value) return;
                scSwitchDiffSubtab(scDiffSubtab.value || 'unified');
            }
        });
        // После завершения Qwen-job — обновить Opus preflight, чтобы кнопка
        // «Проанализировать и сравнить» автоматически разблокировалась.
        watch(() => scRecogJob.value && scRecogJob.value.status, (newStatus, oldStatus) => {
            if (!scSession.value || !scSession.value.id) return;
            if (scTab.value !== 'upload') return;
            const finished = ['done','failed','cancelled','rejected_no_confirm','failed_interrupted'];
            if (oldStatus && !finished.includes(oldStatus) && finished.includes(newStatus)) {
                scOpusLoadPreflight();
            }
        });
        // Stage 3: при смене активной пары подгрузить per-pair md-enrichment
        // статус, чтобы баннер «Графика распознана/ещё не распознана» был
        // актуален без явного клика на dry-run.
        watch(() => scActivePair.value && scActivePair.value.id, (newPid) => {
            if (!newPid) {
                scMdEnrichmentSummary.value = null;
                return;
            }
            scLoadMdEnrichmentSummary();
        });
        // Если активная пара меняется (например, через scOpenPair или «Перейти»)
        // и пользователь сейчас на вкладке «Расхождения» в scope=pair —
        // нужно перезагрузить unified flat под новую пару, иначе UI покажет
        // findings старой пары.
        watch(() => scActivePair.value && scActivePair.value.id, (newPid, oldPid) => {
            if (newPid === oldPid) return;
            if (scTab.value !== 'diffs') return;
            if (scDiffSubtab.value !== 'unified') return;
            // V2 — основной режим вкладки: всегда перегружаем список новой пары.
            if (scV2View.value === 'v2') scLoadV2Changes();
            if (scUnifiedShowAllPairs.value) return;
            scUnifiedFilterPair.value = '';
            scLoadUnifiedFlat();
            if (newPid) scLoadUnifiedPairStatus();
        });
        // ── Computed filters для unified flat ─────────────────────────────
        const scUnifiedPairOptions = computed(() => {
            const items = (scUnifiedFlat.value && scUnifiedFlat.value.items) || [];
            const seen = new Set();
            const out = [];
            for (const it of items) {
                const k = it.pair_label || it.pair_id;
                if (k && !seen.has(k)) { seen.add(k); out.push(k); }
            }
            return out.sort();
        });
        const scUnifiedSourceLayerOptions = computed(() => {
            const items = (scUnifiedFlat.value && scUnifiedFlat.value.items) || [];
            return Array.from(new Set(items.map(it => it.source_layer).filter(Boolean))).sort();
        });
        const scUnifiedTypeOptions = computed(() => {
            const items = (scUnifiedFlat.value && scUnifiedFlat.value.items) || [];
            return Array.from(new Set(items.map(it => it.type).filter(Boolean))).sort();
        });
        const scUnifiedCategoryOptions = computed(() => {
            const items = (scUnifiedFlat.value && scUnifiedFlat.value.items) || [];
            return Array.from(new Set(items.map(it => it.category).filter(Boolean))).sort();
        });
        const scUnifiedItemsFiltered = computed(() => {
            const items = (scUnifiedFlat.value && scUnifiedFlat.value.items) || [];
            const fp = scUnifiedFilterPair.value;
            const fs = scUnifiedFilterSourceLayer.value;
            const ft = scUnifiedFilterType.value;
            const fc = scUnifiedFilterCategory.value;
            const fsev = scUnifiedFilterSeverity.value;
            const fhr = scUnifiedFilterHumanReview.value;
            const q = (scUnifiedSearch.value || '').toLowerCase().trim();
            const out = [];
            // Стабильный №: позиция в исходном items, не зависит от filter/sort.
            for (let i = 0; i < items.length; i++) {
                const it = items[i];
                if (fp && (it.pair_label || it.pair_id) !== fp) continue;
                if (fs && it.source_layer !== fs) continue;
                if (ft && it.type !== ft) continue;
                if (fc && it.category !== fc) continue;
                if (fsev && (it.severity || '') !== fsev) continue;
                if (fhr && !it.requires_human_review) continue;
                if (q) {
                    const hay = [
                        it.title, it.summary, it.old_value, it.new_value,
                        it.construction_impact,
                        it.evidence_left && it.evidence_left.quote,
                        it.evidence_right && it.evidence_right.quote,
                        it.sheet,
                    ].filter(Boolean).join(' ').toLowerCase();
                    if (!hay.includes(q)) continue;
                }
                out.push({ ...it, _displayNo: i + 1 });
            }
            return out;
        });
        // Сортируем уже отфильтрованный список.
        // Поля для sort: 'no' (стабильный №), 'sheet' (лист+страница), 'impact'
        // (severity * 4 + cost_impact ранк). '' = исходный порядок.
        const _SC_SEV_RANK = { high: 3, medium: 2, low: 1 };
        const _SC_COST_RANK = { high: 3, likely: 2, possible: 1, none: 0 };
        function _scImpactRank(it) {
            const sev = _SC_SEV_RANK[it.severity || ''] || 0;
            const cost = _SC_COST_RANK[it.cost_impact || 'none'] || 0;
            return sev * 4 + cost;
        }
        const scUnifiedItemsSorted = computed(() => {
            const items = scUnifiedItemsFiltered.value || [];
            const field = scUnifiedSortField.value;
            const dir = scUnifiedSortDir.value === 'desc' ? -1 : 1;
            // Вторичный компаратор: выбранный столбец (или исходный порядок).
            const byField = (a, b) => {
                if (field === 'no') {
                    return (a.it._displayNo - b.it._displayNo) * dir;
                } else if (field === 'sheet') {
                    const sa = (a.it.sheet || '') + '';
                    const sb = (b.it.sheet || '') + '';
                    const c = sa.localeCompare(sb, 'ru', { numeric: true, sensitivity: 'base' });
                    if (c !== 0) return c * dir;
                    const pa = Array.isArray(a.it.page) ? (a.it.page[0] || 0) : (a.it.page || 0);
                    const pb = Array.isArray(b.it.page) ? (b.it.page[0] || 0) : (b.it.page || 0);
                    return (pa - pb) * dir;
                } else if (field === 'impact') {
                    return (_scImpactRank(a.it) - _scImpactRank(b.it)) * dir;
                }
                return a.idx - b.idx; // исходный порядок
            };
            // Первичный ключ — экспертное решение: принятые сверху, отклонённые внизу.
            const arr = items.map((it, idx) => ({ it, idx }));
            arr.sort((a, b) => {
                const r = _scDecisionRank(a.it) - _scDecisionRank(b.it);
                if (r !== 0) return r;
                return byField(a, b);
            });
            return arr.map(x => x.it);
        });
        function scUnifiedToggleSort(field) {
            if (scUnifiedSortField.value === field) {
                if (scUnifiedSortDir.value === 'asc') {
                    scUnifiedSortDir.value = 'desc';
                } else {
                    // 3-й клик → сброс к исходному порядку
                    scUnifiedSortField.value = '';
                    scUnifiedSortDir.value = 'asc';
                }
            } else {
                scUnifiedSortField.value = field;
                scUnifiedSortDir.value = 'asc';
            }
        }
        function scUnifiedSortIndicator(field) {
            if (scUnifiedSortField.value !== field) return '';
            return scUnifiedSortDir.value === 'desc' ? '▼' : '▲';
        }

        // URL для экспорта таблицы расхождений в Excel (учитывает scope по паре).
        function scUnifiedExportXlsxUrl() {
            if (!scSession.value) return '';
            const sid = encodeURIComponent(scSession.value.id);
            const params = new URLSearchParams();
            if (!scUnifiedShowAllPairs.value && scActivePair.value) {
                params.set('pair_id', scActivePair.value.id);
            }
            const qs = params.toString();
            return `/api/stage-comparison/sessions/${sid}/unified-diff-flat/export.xlsx${qs ? '?' + qs : ''}`;
        }
        // Source-layer labels (UI)
        // Источник изменения схлопнут до двух значений: «текст» (текстовый слой,
        // таблицы, штампы) и «изображение» (Qwen-описание картинки, схема,
        // смешанный визуальный+текстовый источник).
        const _SC_UNIFIED_SOURCE_LABELS = {
            text: 'текст',
            image_enrichment: 'изображение',
            scheme_analysis: 'изображение',
            table: 'текст',
            stamp: 'текст',
            mixed: 'изображение',
        };
        function scUnifiedSourceLabel(s) { return _SC_UNIFIED_SOURCE_LABELS[s] || s || '—'; }
        // Разбивает значение расхождения на строки по ';' — каждый пункт
        // (напр. «Корпус 1 — 4 эт.») рендерится с новой строки. Точка с запятой
        // сохраняется в конце каждого пункта, кроме последнего.
        function scUnifiedLines(val) {
            if (val === null || val === undefined) return [];
            const text = String(val).trim();
            if (!text) return [];
            if (!text.includes(';')) return [text];
            const parts = text.split(';').map(s => s.trim()).filter(s => s.length);
            if (!parts.length) return [text];
            return parts.map((s, i) => (i < parts.length - 1 ? s + ';' : s));
        }
        // Go-to-place для unified finding (по сути то же, что для текстового)
        async function scGotoUnifiedChange(item) {
            // Делегируем существующий go-to (он обрабатывает alignment_slot).
            return scGotoTextChange(item);
        }

        // Переключение под-вкладки diff (unified/text/graphic) с lazy-loading.
        function scSwitchDiffSubtab(name) {
            scDiffSubtab.value = name;
            if (name === 'unified') {
                scLoadUnifiedConfig();
                // При заходе на «Расхождения» — всегда возвращаемся к scope
                // «текущая пара». Это устраняет stale показ findings другой
                // пары при переходе из вкладки «Связь блоков».
                scUnifiedShowAllPairs.value = false;
                scUnifiedFilterPair.value = '';
                scLoadUnifiedFlat();
                if (scActivePair.value) {
                    scLoadUnifiedPairStatus();
                }
                // Если активен V2-подрежим — подгружаем его список текущей пары.
                if (scV2View.value === 'v2') {
                    scLoadV2Changes();
                }
            } else if (name === 'text') {
                scLoadTextLLMConfig();
                scLoadTextLLMFlat();
                if (scActivePair.value) {
                    scLoadTextLLMDiff();
                    scLoadMdEnrichmentSummary();
                }
            } else if (name === 'graphic') {
                scLoadGraphicSummary();
            }
        }
        // Batch preflight + run для всей сессии.
        async function scOpenBatchTextLLM() {
            if (!scSession.value) return;
            scTextLLMBatchError.value = '';
            scTextLLMBatchPreflight.value = null;
            scTextLLMBatchLoading.value = true;
            scTextLLMBatchOpen.value = true;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/text-llm-preflight`;
                const r = await fetch(url, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({scope: 'session', force: !!scTextLLMBatchForce.value}),
                });
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    throw new Error(j.detail || ('HTTP ' + r.status));
                }
                scTextLLMBatchPreflight.value = await r.json();
            } catch (e) {
                scTextLLMBatchError.value = 'Preflight не выполнен: ' + e;
            } finally {
                scTextLLMBatchLoading.value = false;
            }
        }
        async function scRefreshBatchPreflight() {
            // Перезапросить preflight, если пользователь переключил force-checkbox
            if (!scSession.value) return;
            await scOpenBatchTextLLM();
        }
        function scCloseBatchPreflight() {
            scTextLLMBatchOpen.value = false;
            scTextLLMBatchError.value = '';
            // job не сбрасываем — пользователь может хотеть видеть прогресс
        }
        async function scConfirmBatchRun() {
            const pf = scTextLLMBatchPreflight.value;
            if (!pf || !pf.can_run_batch) return;
            scTextLLMBatchError.value = '';
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/text-llm-diff-jobs`;
                const r = await fetch(url, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({
                        scope: 'session',
                        confirm: true,
                        force: !!scTextLLMBatchForce.value,
                    }),
                });
                if (!r.ok) {
                    const j = await r.json().catch(() => ({}));
                    const msg = (j && j.detail && (j.detail.message || j.detail)) || ('HTTP ' + r.status);
                    throw new Error(typeof msg === 'string' ? msg : JSON.stringify(msg));
                }
                const job = await r.json();
                scTextLLMBatchJob.value = job;
                scCloseBatchPreflight();
                if (job.status === 'queued' || job.status === 'running') {
                    scPollTextLLMJob(job.id);
                }
            } catch (e) {
                scTextLLMBatchError.value = 'Запуск batch не удался: ' + e;
            }
        }
        async function scPollTextLLMJob(jobId) {
            if (!scSession.value || !jobId) return;
            if (scTextLLMBatchPolling.value) return;
            scTextLLMBatchPolling.value = true;
            try {
                while (true) {
                    const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/text-llm-diff-jobs/${encodeURIComponent(jobId)}`;
                    const r = await fetch(url);
                    if (!r.ok) break;
                    const job = await r.json();
                    scTextLLMBatchJob.value = job;
                    if (['done', 'failed', 'cancelled', 'rejected_no_confirm'].includes(job.status)) break;
                    await new Promise(res => setTimeout(res, 3000));
                }
                // По завершении подтянем session-level плоский список + per-pair (для blocks-view)
                try { await scLoadTextLLMFlat(); } catch (_) {}
                if (scActivePair.value) {
                    try { await scLoadTextLLMDiff(); } catch (_) {}
                }
            } finally {
                scTextLLMBatchPolling.value = false;
            }
        }
        async function scCancelTextLLMJob() {
            const job = scTextLLMBatchJob.value;
            if (!job || !job.id) return;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/text-llm-diff-jobs/${encodeURIComponent(job.id)}/cancel`;
                const r = await fetch(url, {method: 'POST'});
                if (r.ok) scTextLLMBatchJob.value = await r.json();
            } catch (e) { /* silent */ }
        }
        function scHumanizeDuration(seconds) {
            const s = Math.max(0, Math.round(seconds || 0));
            if (s < 60) return s + ' с';
            const m = Math.floor(s / 60), rs = s % 60;
            if (s < 3600) return rs ? `${m} мин ${rs} с` : `${m} мин`;
            const h = Math.floor(s / 3600), rm = Math.floor((s % 3600) / 60);
            return rm ? `${h} ч ${rm} мин` : `${h} ч`;
        }
        const _SC_TEXT_LLM_TYPE_LABELS = {
            added: 'Добавлено', removed: 'Удалено', changed: 'Изменено',
            equipment_changed: 'Изменение оборудования',
            material_changed: 'Изменение материала',
            calculation_changed: 'Изменение расчётных данных',
            requirement_changed: 'Изменение требований',
            design_logic_changed: 'Изменение проектной логики',
            section_changed: 'Изменение состава проекта',
            declared_by_designer: 'Заявлено проектировщиком',
        };
        const _SC_TEXT_LLM_CATEGORY_LABELS = {
            design_solution: 'Проектное решение',
            equipment: 'Оборудование',
            material: 'Материал',
            calculation: 'Расчёт',
            requirement: 'Требование',
            composition: 'Состав',
            construction_technology: 'Технология',
            safety: 'Безопасность',
            fire_safety: 'Пожарная безопасность',
            engineering_systems: 'Инж. системы',
            architecture: 'Архитектура',
            structures: 'Конструкции',
            other: 'Прочее',
        };
        const _SC_TEXT_LLM_SEVERITY_LABELS = {high: 'Важно', medium: 'Средне', low: 'Низкое'};
        const _SC_TEXT_LLM_STATUS_LABELS = {
            done: 'Готово', not_run: 'Не выполнялся',
            disabled: 'Выключено', provider_not_available: 'Provider недоступен',
            missing_md: 'Нет MD', too_large: 'Слишком большой ввод',
            error: 'Ошибка', timeout: 'Таймаут', blocked: 'Заблокировано',
        };
        function scTextLLMTypeLabel(t) { return _SC_TEXT_LLM_TYPE_LABELS[t] || t || '—'; }
        function scTextLLMCategoryLabel(c) { return _SC_TEXT_LLM_CATEGORY_LABELS[c] || c || 'Прочее'; }
        function scTextLLMSeverityLabel(s) { return _SC_TEXT_LLM_SEVERITY_LABELS[s] || s || '—'; }
        function scTextLLMStatusLabel(s) { return _SC_TEXT_LLM_STATUS_LABELS[s] || s || '—'; }

        // ── Computed-ы для session-level flat-таблицы ─────────────────────
        const scTextFlatPairOptions = computed(() => {
            const items = (scTextLLMFlat.value && scTextLLMFlat.value.items) || [];
            const seen = new Set();
            const out = [];
            for (const it of items) {
                const k = it.pair_label || it.pair_id;
                if (k && !seen.has(k)) { seen.add(k); out.push(k); }
            }
            return out.sort();
        });
        const scTextFlatTypeOptions = computed(() => {
            const items = (scTextLLMFlat.value && scTextLLMFlat.value.items) || [];
            return Array.from(new Set(items.map(it => it.type).filter(Boolean))).sort();
        });
        const scTextFlatCategoryOptions = computed(() => {
            const items = (scTextLLMFlat.value && scTextLLMFlat.value.items) || [];
            return Array.from(new Set(items.map(it => it.category).filter(Boolean))).sort();
        });
        const scTextFlatItemsFiltered = computed(() => {
            const items = (scTextLLMFlat.value && scTextLLMFlat.value.items) || [];
            const fp = scTextFlatFilterPair.value;
            const ft = scTextFlatFilterType.value;
            const fc = scTextFlatFilterCategory.value;
            const fs = scTextFlatFilterSeverity.value;
            const fhr = scTextFlatFilterHumanReview.value;
            const q = (scTextFlatSearch.value || '').toLowerCase().trim();
            return items.filter(it => {
                if (fp && (it.pair_label || it.pair_id) !== fp) return false;
                if (ft && it.type !== ft) return false;
                if (fc && it.category !== fc) return false;
                if (fs && (it.severity || '') !== fs) return false;
                if (fhr && !it.requires_human_review) return false;
                if (q) {
                    const hay = [
                        it.title, it.summary, it.old_value, it.new_value,
                        it.construction_impact,
                        it.evidence_left && it.evidence_left.quote,
                        it.evidence_right && it.evidence_right.quote,
                        it.sheet,
                    ].filter(Boolean).join(' ').toLowerCase();
                    if (!hay.includes(q)) return false;
                }
                return true;
            });
        });

        // ── Go-to-place для текстового изменения ──────────────────────────
        // 1) Открыть PDF-пару (если не активна),
        // 2) переключиться на под-вкладку «Связь блоков»,
        // 3) проскроллить к alignment_slot и подсветить.
        async function scGotoTextChange(item) {
            if (!item || !scSession.value) return;
            const targetPair = (scSession.value.pairs || []).find(p => p.id === item.pair_id);
            if (!targetPair) {
                scError.value = 'Не нашёл PDF-пару для этого изменения.';
                return;
            }
            if (!scActivePair.value || scActivePair.value.id !== targetPair.id) {
                await scOpenPair(targetPair);
            }
            scTab.value = 'links';
            const slot = item.alignment_slot;
            if (!slot) {
                scError.value = 'Точную страницу определить не удалось. Изменение найдено по тексту MD.';
                return;
            }
            // Дождаться рендера панелей и проскроллить к слоту с обеих сторон.
            await nextTick();
            setTimeout(() => {
                for (const side of ['left', 'right']) {
                    const el = document.getElementById('sc-slot-' + side + '-' + slot);
                    if (el && el.scrollIntoView) {
                        el.scrollIntoView({behavior: 'smooth', block: 'center'});
                        el.classList.add('sc-slot--highlight');
                        setTimeout(() => el.classList.remove('sc-slot--highlight'), 2400);
                    }
                }
            }, 80);
        }

        // Лейблы для finding.type (универсальные, в т.ч. новые text_* типы)
        const _SC_FINDING_TYPE_LABELS = {
            text_added: 'Добавлено в тексте',
            text_removed: 'Удалено из текста',
            text_changed: 'Изменено в тексте',
            text_equipment_changed: 'Изменение оборудования',
            text_material_changed: 'Изменение материала',
            text_calculation_changed: 'Изменение расчётных данных',
            text_requirement_changed: 'Изменение требований',
            text_design_logic_changed: 'Изменение проектной логики',
            text_section_changed: 'Изменение состава проекта',
            text_declared_change: 'Заявлено проектировщиком',
            graphic_added: 'Графика добавлена',
            graphic_removed: 'Графика удалена',
            graphic_changed: 'Графика изменена',
            page_added: 'Новый лист',
            page_removed: 'Лист удалён',
            page_reordered: 'Лист перенесён',
            stale_link: 'Устаревшая связь',
        };
        function scFindingTypeLabel(t) { return _SC_FINDING_TYPE_LABELS[t] || t || '—'; }

        async function scLoadGraphicSummary() {
            if (!scSession.value || !scActivePair.value) return;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/graphic-summary`;
                const r = await fetch(url);
                if (!r.ok) throw new Error('HTTP ' + r.status);
                scGraphicSummary.value = await r.json();
            } catch (e) {
                scError.value = 'Не удалось загрузить graphic summary: ' + e;
            }
        }

        function scFindGraphicDiff(link) {
            if (!scGraphicSummary.value) return null;
            const compared = scGraphicSummary.value.compared || [];
            return compared.find(d => d.left_block_id === link.left_block_id && d.right_block_id === link.right_block_id) || null;
        }

        async function scPrepareGraphicDiff(link) {
            await scRunGraphicDiff(link, false);
        }

        async function scRunGraphicDiff(link, runPaid) {
            scGraphicDiffRunning.value = true;
            try {
                const url = `/api/stage-comparison/sessions/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(scActivePair.value.id)}/graphic-diff`;
                const r = await fetch(url, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({left_block_id: link.left_block_id, right_block_id: link.right_block_id, run_paid: !!runPaid}),
                });
                if (!r.ok) {
                    const j = await r.json().catch(() => ({detail: 'HTTP ' + r.status}));
                    throw new Error(j.detail || ('HTTP ' + r.status));
                }
                const data = await r.json();
                // Backend теперь возвращает 200 OK с status в body (blocked/error/done/prepared).
                if (data.status === 'blocked') {
                    scGraphicPreview.value = {
                        ...data,
                        note: 'Платный API заблокирован: ' + (data.error || 'unknown') + '. Проверьте PAID_API_ENABLED.',
                    };
                    await scLoadGraphicSummary();
                    return;
                }
                if (data.status === 'error') {
                    scGraphicPreview.value = {
                        ...data,
                        note: 'LLM error: ' + (data.error || 'unknown'),
                    };
                    await scLoadGraphicSummary();
                    return;
                }
                scGraphicPreview.value = data;
                if (data.status === 'done') await scLoadGraphicSummary();
            } catch (e) {
                scError.value = 'Graphic diff failed: ' + e;
            } finally {
                scGraphicDiffRunning.value = false;
            }
        }

        // ─── Stage Comparison: Pipeline V2 (β) — read-only панель ────────
        // Панель ТОЛЬКО читает GET /api/stage-comparison/pipeline-v2/{sid}/
        // ui-payload (+?pair_id=). Ничего не запускает (ни Pipeline V2, ни
        // Qwen/Opus), ничего не пишет. not_found — нормальное состояние,
        // пока артефакты dry-run не записаны в comparison/sessions/.
        const scPv2Loading = ref(false);
        const scPv2Error = ref('');          // транспорт / HTTP / доступ
        const scPv2Resp = ref(null);         // envelope {status, payload, …}
        const scPv2PairId = ref('');         // '' = session-level
        const scPv2LoadedFor = ref('');      // ключ sid|pair последней загрузки
        const scPv2Open = reactive({});      // sectionKey -> развёрнута ли
        const scPv2Filters = reactive({ entity_type: '', risk_level: '',
                                        critic_verdict: '', delta_type: '' });

        const scPv2Payload = computed(() =>
            (scPv2Resp.value && scPv2Resp.value.payload) || null);
        const scPv2Sections = computed(() =>
            (scPv2Payload.value && scPv2Payload.value.sections) || []);
        const scPv2Headline = computed(() =>
            (scPv2Payload.value && scPv2Payload.value.headline) || null);
        // Графика / Vision grounding — read-only сводка из ui-payload
        // (backend пишет graphic_vision / graphic_vision_grounding). Если поля
        // нет — секции просто не рендерятся (empty state), панель не падает.
        const scPv2GraphicVision = computed(() =>
            (scPv2Payload.value && scPv2Payload.value.graphic_vision) || null);
        const scPv2GraphicGrounding = computed(() =>
            (scPv2Payload.value && scPv2Payload.value.graphic_vision_grounding) || null);
        // суммарно «отклонено как галлюцинация»: artificial ряды +
        // designator-range + no-op изменения
        const scPv2GroundingRejectedTotal = computed(() => {
            const g = scPv2GraphicGrounding.value;
            if (!g) return 0;
            const n = (x) => (typeof x === 'number' && x > 0 ? x : 0);
            return n(g.artificial_series_rejected) + n(g.designator_range_rejected)
                + n(g.noop_changes_rejected);
        });
        // ─── Grounded vision evidence (per-delta badges + summary) ──────────
        // payload.grounded_evidence: {available, counts…, cards:[…]}. Связь
        // deterministic-дельты с подтверждённой графикой (mark-only).
        const scPv2GroundedEvidence = computed(() =>
            (scPv2Payload.value && scPv2Payload.value.grounded_evidence) || null);
        const scPv2GeAvailable = computed(() => {
            const g = scPv2GroundedEvidence.value;
            return !!(g && g.available);
        });
        // карточки с подтверждением/конфликтом вперёд; «none» не показываем
        const scPv2GeInterestingCards = computed(() => {
            const g = scPv2GroundedEvidence.value;
            const cards = (g && Array.isArray(g.cards)) ? g.cards : [];
            return cards.filter(c => c && c.evidence_level && c.evidence_level !== 'none');
        });
        // визуальный стиль badge по уровню evidence
        function scPv2GeBadgeStyle(level) {
            const L = (level || '').toLowerCase();
            if (L === 'grounded') return {emoji: '✅', text: 'Grounded vision',
                bg: '#dcfce7', fg: '#166534', border: '#86efac'};
            if (L === 'weak') return {emoji: '🟡', text: 'Weak vision',
                bg: '#fef9c3', fg: '#854d0e', border: '#fde047'};
            if (L === 'conflict' || L === 'rejected_only')
                return {emoji: '⚠', text: 'Rejected/conflict', bg: '#ffedd5',
                    fg: '#9a3412', border: '#fdba74'};
            return null;   // none / unknown — badge не показываем
        }
        function scPv2GeAnchorText(a) {
            if (!a) return '';
            const parts = [];
            if (a.designator) parts.push(String(a.designator).toUpperCase());
            const ov = a.old_anchor || '';
            const nv = a.new_anchor || '';
            if (ov || nv) parts.push((ov || '—') + ' → ' + (nv || '—'));
            return parts.join(': ');
        }
        // Цвет/подпись чипа вердикта критика. evidence ≠ critic — это РАЗНЫЕ
        // сигналы (grounded vision может идти с needs_human_review). null →
        // вызывающий показывает серое «нет объяснения».
        function scPv2CriticVerdictStyle(v) {
            const V = (v || '').toLowerCase();
            if (V === 'accept') return {text: 'accept',
                bg: '#dcfce7', fg: '#166534', border: '#86efac'};
            if (V === 'needs_human_review') return {text: 'на проверку',
                bg: '#fef9c3', fg: '#854d0e', border: '#fde047'};
            if (V === 'possible_weak_graphic') return {text: 'слабая графика',
                bg: '#ffedd5', fg: '#9a3412', border: '#fdba74'};
            if (V === 'possible_ocr_noise') return {text: 'возможно OCR-шум',
                bg: '#f1f5f9', fg: '#475569', border: '#cbd5e1'};
            if (V === 'reject') return {text: 'отклонено',
                bg: '#fee2e2', fg: '#991b1b', border: '#fecaca'};
            if (V === 'failed' || V === 'skipped') return {text: 'сбой/пропуск',
                bg: '#fee2e2', fg: '#991b1b', border: '#fecaca'};
            return null;   // missing
        }
        // «Показать инженеру» из should_show_to_engineer (true/false/—).
        function scPv2ShowText(v) {
            if (v === true) return 'да';
            if (v === false) return 'нет';
            return '—';
        }
        // Breakdown grounded-evidence карточек по вердикту критика (join по
        // секционным карточкам, где есть и grounded_evidence, и critic_verdict).
        const scPv2GeVerdictBreakdown = computed(() => {
            const out = {accept: 0, needs_review: 0, weak_other: 0, total: 0};
            for (const sec of (scPv2Sections.value || [])) {
                for (const c of ((sec && sec.cards) || [])) {
                    const ge = c && c.grounded_evidence;
                    if (!ge || !ge.evidence_level || ge.evidence_level === 'none') continue;
                    out.total++;
                    const lvl = ge.evidence_level;
                    const v = (c.critic_verdict || '').toLowerCase();
                    if (lvl === 'conflict' || lvl === 'rejected_only' || lvl === 'weak') {
                        out.weak_other++;            // не-grounded evidence
                    } else if (v === 'accept') {
                        out.accept++;                // grounded + критик принял
                    } else {
                        out.needs_review++;          // grounded, но на проверку/прочее
                    }
                }
            }
            return out;
        });
        // ─── Grounding detail drawer (read-only) ────────────────────────────
        // Грузит GET …/pipeline-v2/{sid}/graphic-vision-grounding?pair_id=…
        // Один fetch (limit 500) → клиентская фильтрация по табам.
        const scPv2GdOpen = ref(false);
        const scPv2GdLoading = ref(false);
        const scPv2GdError = ref('');
        const scPv2GdResp = ref(null);       // detail-ответ {summary, flat, …}
        const scPv2GdFilter = ref('all');    // all|grounded|weak|ungrounded|rejected|changes
        let scPv2GdReqSeq = 0;
        const scPv2GdCards = computed(() => {
            const f = scPv2GdResp.value && scPv2GdResp.value.flat;
            if (!f) return [];
            return [].concat(f.entities || [], f.changes || [], f.rejected || []);
        });
        function scPv2GdMatch(card, tab) {
            if (tab === 'all') return true;
            if (tab === 'changes') return card.card_type === 'change';
            if (tab === 'grounded') return card.status === 'grounded';
            if (tab === 'weak') return card.status === 'weakly_grounded';
            if (tab === 'ungrounded')
                return card.status === 'ungrounded' || card.status === 'no_anchor_available';
            if (tab === 'rejected')
                return typeof card.status === 'string' && card.status.indexOf('rejected_') === 0;
            return true;
        }
        const scPv2GdFilteredCards = computed(() =>
            scPv2GdCards.value.filter(c => scPv2GdMatch(c, scPv2GdFilter.value)));
        const scPv2GdPagination = computed(() =>
            (scPv2GdResp.value && scPv2GdResp.value.pagination) || null);
        function scPv2GdStatusColor(status) {
            if (status === 'grounded') return { bg: '#dcfce7', fg: '#166534', br: '#bbf7d0' };
            if (status === 'weakly_grounded') return { bg: '#fef9c3', fg: '#854d0e', br: '#fde68a' };
            if (typeof status === 'string' && status.indexOf('rejected_') === 0)
                return { bg: '#fee2e2', fg: '#991b1b', br: '#fecaca' };
            return { bg: '#f3f4f6', fg: '#6b7280', br: '#e5e7eb' };  // ungrounded
        }
        async function scPv2GdLoad() {
            if (!scSession.value || !scSession.value.id) return;
            const myReq = ++scPv2GdReqSeq;
            scPv2GdLoading.value = true;
            scPv2GdError.value = '';
            const sid = scSession.value.id;
            const pid = scPv2PairId.value;
            let url = '/api/stage-comparison/pipeline-v2/'
                + encodeURIComponent(sid) + '/graphic-vision-grounding?limit=500';
            if (pid) url += '&pair_id=' + encodeURIComponent(pid);
            try {
                const r = await fetch(url);
                if (myReq !== scPv2GdReqSeq) return;
                if (r.status === 401 || r.status === 403) {
                    scPv2GdResp.value = null;
                    scPv2GdError.value = 'Доступ запрещён (' + r.status + ').';
                    return;
                }
                if (!r.ok) throw new Error('HTTP ' + r.status);
                const j = await r.json();
                if (myReq !== scPv2GdReqSeq) return;
                scPv2GdResp.value = j;
            } catch (e) {
                if (myReq !== scPv2GdReqSeq) return;
                scPv2GdResp.value = null;
                scPv2GdError.value = String((e && e.message) || e);
            } finally {
                if (myReq === scPv2GdReqSeq) scPv2GdLoading.value = false;
            }
        }
        function scPv2GdOpenDrawer() {
            scPv2GdOpen.value = true;
            scPv2GdFilter.value = 'all';
            scPv2GdLoad();
        }
        function scPv2GdClose() { scPv2GdOpen.value = false; }
        // Мост grounding-карточка → block link preview (read-only UX, без
        // автоприменения связей): баннер «открыта связь по карточке» и warning,
        // если matching block_link не найден.
        const scPv2GdJumpBanner = ref('');    // label карточки, по которой прыгнули
        const scPv2GdJumpWarning = ref('');   // связь не найдена
        const scPv2FilterOptions = computed(() => {
            const f = (scPv2Payload.value && scPv2Payload.value.filters) || {};
            return {
                entity_type: f.entity_types || [],
                risk_level: f.risk_levels || [],
                critic_verdict: f.critic_verdicts || [],
                delta_type: f.delta_types || [],
            };
        });
        const scPv2HasFilterOptions = computed(() => {
            const o = scPv2FilterOptions.value;
            return Object.values(o).some(list => list && list.length);
        });
        const scPv2FiltersActive = computed(() =>
            Object.values(scPv2Filters).some(v => v));

        const SC_PV2_SECTION_EMOJI = {
            confirmed_changes: '✅', needs_review: '🟡',
            weak_graphic_review: '🟠', likely_noise_hidden_by_default: '⚪',
            llm_failed_or_skipped: '🔴',
        };
        function scPv2SectionEmoji(key) {
            return SC_PV2_SECTION_EMOJI[key] || '▫';
        }

        // Карточка проходит фильтры? Отсутствующие поля карточки не валят
        // фильтрацию (null != выбранное значение → просто скрыта).
        function scPv2CardMatches(card, filters) {
            if (!card) return false;
            const f = filters || scPv2Filters;
            return (!f.entity_type || card.entity_type === f.entity_type)
                && (!f.risk_level || card.risk_level === f.risk_level)
                && (!f.critic_verdict || card.critic_verdict === f.critic_verdict)
                && (!f.delta_type || card.delta_type === f.delta_type);
        }
        function scPv2CardsFor(sec) {
            const cards = (sec && sec.cards) || [];
            if (!scPv2FiltersActive.value) return cards;
            return cards.filter(c => scPv2CardMatches(c));
        }
        function scPv2ResetFilters() {
            scPv2Filters.entity_type = '';
            scPv2Filters.risk_level = '';
            scPv2Filters.critic_verdict = '';
            scPv2Filters.delta_type = '';
        }

        // default_visible → секция развёрнута; noise/llm_failed свёрнуты.
        function scPv2ApplyDefaultOpen(payload) {
            for (const sec of (payload && payload.sections) || []) {
                if (sec && sec.key) scPv2Open[sec.key] = !!sec.default_visible;
            }
        }
        function scPv2ToggleSection(key) {
            scPv2Open[key] = !scPv2Open[key];
        }

        function scPv2StatusBadge(status) {
            return { ok: '✓ ok', partial: '◐ partial', not_found: '∅',
                     error: '⚠ error' }[status] || (status || '');
        }

        // Объединённые warnings: envelope (чтение артефактов) + payload
        // (adapter/summary warnings вида «section_X: … without card data»).
        const scPv2AllWarnings = computed(() => {
            const env = (scPv2Resp.value && scPv2Resp.value.warnings) || [];
            const pl = (scPv2Payload.value && scPv2Payload.value.warnings) || [];
            return [...env, ...pl];
        });

        // sequence-guard: авторитетен только ПОСЛЕДНИЙ запрос — поздний ответ
        // старой пары/сессии не должен перетирать актуальный (race).
        let scPv2ReqSeq = 0;

        async function scPv2Load() {
            if (!scSession.value || !scSession.value.id) return;
            const myReq = ++scPv2ReqSeq;
            scPv2Loading.value = true;
            scPv2Error.value = '';
            const sid = scSession.value.id;
            const pid = scPv2PairId.value;
            let url = '/api/stage-comparison/pipeline-v2/'
                + encodeURIComponent(sid) + '/ui-payload';
            if (pid) url += '?pair_id=' + encodeURIComponent(pid);
            try {
                const r = await fetch(url);
                if (myReq !== scPv2ReqSeq) return;     // устаревший ответ
                if (r.status === 401 || r.status === 403) {
                    // portal_auth-интерсептор обычно сам уводит на /login;
                    // это запасной понятный текст, если редиректа не было
                    scPv2Resp.value = null;
                    scPv2Error.value = 'Доступ запрещён (' + r.status
                        + '). Войдите в портал заново.';
                    return;
                }
                if (!r.ok) throw new Error('HTTP ' + r.status);
                const j = await r.json();
                if (myReq !== scPv2ReqSeq) return;     // устаревший ответ
                scPv2Resp.value = j;
                if (j && j.payload) scPv2ApplyDefaultOpen(j.payload);
                scPv2LoadedFor.value = sid + '|' + pid;
            } catch (e) {
                if (myReq !== scPv2ReqSeq) return;
                scPv2Resp.value = null;
                scPv2Error.value = String((e && e.message) || e);
            } finally {
                if (myReq === scPv2ReqSeq) scPv2Loading.value = false;
            }
        }
        // Ленивая загрузка при входе на вкладку / смене сессии-пары.
        function scPv2EnsureLoaded() {
            if (!scSession.value || !scSession.value.id) return;
            const key = scSession.value.id + '|' + scPv2PairId.value;
            if (scPv2LoadedFor.value !== key && !scPv2Loading.value) scPv2Load();
        }
        function scPv2OpenPair(pid) {
            scPv2PairId.value = pid || '';
            scPv2Load();
        }
        // Смена пары → сброс фильтров: значения старой пары могли исчезнуть
        // из options новой (селекты выглядели бы пустыми и скрывали карточки).
        // Grounding-drawer тоже закрываем: detail старой пары не должен висеть.
        watch(scPv2PairId, () => {
            scPv2ResetFilters();
            scPv2GdReqSeq++;
            scPv2GdOpen.value = false;
            scPv2GdResp.value = null;
            scPv2GdError.value = '';
            scPv2GdFilter.value = 'all';
            scPv2GdJumpBanner.value = '';
            scPv2GdJumpWarning.value = '';
        });
        // Смена сессии → полный сброс панели: pair из старой сессии не должен
        // утекать в запросы новой, фильтры и payload устаревают; инвалидация
        // sequence отменяет in-flight ответы старой сессии.
        watch(() => (scSession.value && scSession.value.id) || '', () => {
            scPv2ReqSeq++;
            scPv2PairId.value = '';
            scPv2Resp.value = null;
            scPv2LoadedFor.value = '';
            scPv2Error.value = '';
            scPv2Loading.value = false;
            scPv2ResetFilters();
        });

        // ─── Stage Comparison: Pipeline V2 Block Link Preview ────────────
        // Read-only режим «Pipeline V2 — предложенные связи» в разделе
        // «Связь блоков»: GET /api/stage-comparison/pipeline-v2/{sid}/
        // block-link-preview?pair_id=. Ничего не применяет: ручные связи
        // блоков пары не читаются и не изменяются, никаких job'ов.
        const scPv2LpVisible = ref(false);     // панель открыта
        const scPv2LpLoading = ref(false);
        const scPv2LpError = ref('');
        const scPv2LpResp = ref(null);         // envelope {status, payload, …}
        const scPv2LpPairId = ref('');         // выбранная пара (дефолт — активная)
        const scPv2LpFilter = ref('all');      // all|strong|weak|manual_review|unmatched|graphic|visual_changed|visual_identical
        const scPv2LpSelectedPage = ref('');   // page_link_id
        const scPv2LpSelectedLink = ref('');   // block_link_id / un_<side>_<id>
        let scPv2LpReqSeq = 0;

        const SC_PV2_LP_COLORS = { green: '#16a34a', yellow: '#ca8a04',
                                   orange: '#ea580c', gray: '#6b7280',
                                   blue: '#2563eb' };
        const SC_PV2_LP_FILTERS = [
            { key: 'all', label: 'Все' },
            { key: 'strong', label: '🟢 strong' },
            { key: 'weak', label: '🟡 weak' },
            { key: 'manual_review', label: '🟠 manual review' },
            { key: 'unmatched', label: '⚪ без пары' },
            { key: 'graphic', label: '📐 только графика' },
            { key: 'visual_changed', label: '👁 visual changed' },
            { key: 'visual_identical', label: '👁 visual identical' },
        ];

        const scPv2LpReport = computed(() =>
            (scPv2LpResp.value && scPv2LpResp.value.payload) || null);
        const scPv2LpSummary = computed(() =>
            (scPv2LpReport.value && scPv2LpReport.value.summary) || null);
        const scPv2LpPageLinks = computed(() =>
            (scPv2LpReport.value && scPv2LpReport.value.page_links) || []);
        const scPv2LpNotFound = computed(() =>
            !!scPv2LpResp.value && scPv2LpResp.value.status === 'not_found');
        const scPv2LpRespError = computed(() =>
            (scPv2LpResp.value && scPv2LpResp.value.status === 'error')
                ? ((scPv2LpResp.value.warnings || []).join('; ')
                   || scPv2LpResp.value.message || 'error')
                : '');
        // Связи + односторонние блоки в одном списке (kind: link|unmatched).
        const scPv2LpAllLinks = computed(() => {
            const r = scPv2LpReport.value;
            if (!r) return [];
            const links = (r.block_links || []).map(l =>
                ({ ...l, kind: 'link' }));
            const un = r.unmatched || {};
            const one = [...(un.left_blocks || []), ...(un.right_blocks || [])]
                .map(u => ({ ...u, kind: 'unmatched',
                             block_link_id: 'un_' + u.side + '_' + u.block_id }));
            return [...links, ...one];
        });
        function scPv2LpLinkMatchesFilter(l) {
            const f = scPv2LpFilter.value;
            if (!f || f === 'all') return true;
            if (f === 'unmatched') return l.kind === 'unmatched';
            if (f === 'graphic') return !!l.is_graphic;
            if (f === 'visual_changed') return l.visual_status === 'changed_visual';
            if (f === 'visual_identical')
                return l.visual_status === 'identical_visual'
                    || l.visual_status === 'minor_visual';
            return l.link_status === f;
        }
        const scPv2LpFilteredLinks = computed(() =>
            scPv2LpAllLinks.value.filter(l => scPv2LpLinkMatchesFilter(l)));
        const scPv2LpSelectedPageLink = computed(() =>
            scPv2LpPageLinks.value.find(
                p => p.page_link_id === scPv2LpSelectedPage.value) || null);
        const scPv2LpSelectedLinkObj = computed(() =>
            scPv2LpAllLinks.value.find(
                l => l.block_link_id === scPv2LpSelectedLink.value) || null);
        // Блоки для overlay выбранной пары страниц (обе стороны).
        const scPv2LpPageOverlays = computed(() => {
            const p = scPv2LpSelectedPageLink.value;
            const out = { left: [], right: [] };
            if (!p) return out;
            for (const l of scPv2LpAllLinks.value) {
                if (l.kind === 'link') {
                    if (l.page_match_id !== p.page_link_id) continue;
                    if (l.left_bbox_norm)
                        out.left.push({ entry: l, side: 'left', bbox: l.left_bbox_norm });
                    if (l.right_bbox_norm)
                        out.right.push({ entry: l, side: 'right', bbox: l.right_bbox_norm });
                } else {
                    // unmatched: только на своей стороне и своей странице
                    const page = l.side === 'left' ? p.left_page_number
                                                   : p.right_page_number;
                    if (page != null && l.page_number === page && l.bbox_norm)
                        out[l.side].push({ entry: l, side: l.side, bbox: l.bbox_norm });
                }
            }
            return out;
        });
        function scPv2LpEffectivePairId() {
            return scPv2LpPairId.value
                || (scActivePair.value && scActivePair.value.id) || '';
        }
        function scPv2LpPageImageUrl(side, page) {
            const sid = scSession.value && scSession.value.id;
            // картинки ДОЛЖНЫ соответствовать паре загруженного отчёта
            // (envelope.pair_id), а не живому селектору — иначе при смене
            // пары в селекторе bbox старой пары лёг бы на листы новой
            const pid = (scPv2LpResp.value && scPv2LpResp.value.pair_id)
                || scPv2LpEffectivePairId();
            if (!sid || !pid || !page) return '';
            return `/api/stage-comparison/sessions/${encodeURIComponent(sid)}/pairs/${encodeURIComponent(pid)}/page-image?side=${side}&page=${page}&target_long_side=1400`;
        }
        function scPv2LpOverlayStyle(ov) {
            const b = ov.bbox || [0, 0, 0, 0];
            const sel = scPv2LpSelectedLink.value
                && ov.entry.block_link_id === scPv2LpSelectedLink.value;
            const color = SC_PV2_LP_COLORS[(ov.entry.ui && ov.entry.ui.color) || 'gray']
                || SC_PV2_LP_COLORS.gray;
            return {
                position: 'absolute',
                left: (b[0] * 100) + '%',
                top: (b[1] * 100) + '%',
                width: (Math.max(0, b[2] - b[0]) * 100) + '%',
                height: (Math.max(0, b[3] - b[1]) * 100) + '%',
                border: sel ? ('3px solid ' + SC_PV2_LP_COLORS.blue)
                            : ('2px solid ' + color),
                boxShadow: sel ? ('inset 0 0 0 2px ' + color) : 'none',
                background: sel ? 'rgba(37,99,235,.12)' : (color + '22'),
                cursor: 'pointer',
                boxSizing: 'border-box',
            };
        }
        function scPv2LpStatusColor(l) {
            return SC_PV2_LP_COLORS[(l && l.ui && l.ui.color) || 'gray']
                || SC_PV2_LP_COLORS.gray;
        }
        function scPv2LpSelectLink(l) {
            if (!l) return;
            scPv2LpSelectedLink.value =
                scPv2LpSelectedLink.value === l.block_link_id
                    ? '' : l.block_link_id;
            // выбор из списка подтягивает страницу связи
            if (scPv2LpSelectedLink.value) {
                if (l.kind === 'link' && l.page_match_id) {
                    scPv2LpSelectedPage.value = l.page_match_id;
                } else if (l.kind === 'unmatched') {
                    const p = scPv2LpPageLinks.value.find(pl =>
                        (l.side === 'left' ? pl.left_page_number
                                           : pl.right_page_number) === l.page_number);
                    if (p) scPv2LpSelectedPage.value = p.page_link_id;
                }
            }
        }
        function scPv2LpSelectPage(pid) {
            scPv2LpSelectedPage.value = pid || '';
            scPv2LpSelectedLink.value = '';
        }
        async function scPv2LpLoad() {
            const sid = scSession.value && scSession.value.id;
            const pid = scPv2LpEffectivePairId();
            if (!sid || !pid) return;
            const myReq = ++scPv2LpReqSeq;
            scPv2LpLoading.value = true;
            scPv2LpError.value = '';
            const url = '/api/stage-comparison/pipeline-v2/'
                + encodeURIComponent(sid)
                + '/block-link-preview?pair_id=' + encodeURIComponent(pid);
            try {
                const r = await fetch(url);
                if (myReq !== scPv2LpReqSeq) return;   // устаревший ответ
                if (r.status === 401 || r.status === 403) {
                    scPv2LpResp.value = null;
                    scPv2LpError.value = 'Доступ запрещён (' + r.status
                        + '). Войдите в портал заново.';
                    return;
                }
                if (!r.ok) throw new Error('HTTP ' + r.status);
                const j = await r.json();
                if (myReq !== scPv2LpReqSeq) return;   // устаревший ответ
                scPv2LpResp.value = j;
                scPv2LpSelectedLink.value = '';
                // автоселект первой пары страниц со связями
                const pages = (j && j.payload && j.payload.page_links) || [];
                const first = pages.find(p =>
                    (p.block_link_ids || []).length) || pages[0];
                scPv2LpSelectedPage.value = first ? first.page_link_id : '';
            } catch (e) {
                if (myReq !== scPv2LpReqSeq) return;
                scPv2LpResp.value = null;
                scPv2LpError.value = String((e && e.message) || e);
            } finally {
                if (myReq === scPv2LpReqSeq) scPv2LpLoading.value = false;
            }
        }
        function scPv2LpToggle() {
            scPv2LpVisible.value = !scPv2LpVisible.value;
            if (scPv2LpVisible.value && !scPv2LpResp.value
                    && !scPv2LpLoading.value) {
                scPv2LpPairId.value = scPv2LpEffectivePairId();
                scPv2LpLoad();
            }
        }
        // Смена сессии/активной пары → полный сброс панели (поздние ответы
        // старой пары отменяются инвалидацией sequence).
        function scPv2LpReset() {
            scPv2LpReqSeq++;
            scPv2LpVisible.value = false;
            scPv2LpResp.value = null;
            scPv2LpError.value = '';
            scPv2LpLoading.value = false;
            scPv2LpPairId.value = '';
            scPv2LpFilter.value = 'all';
            scPv2LpSelectedPage.value = '';
            scPv2LpSelectedLink.value = '';
        }
        watch(() => (scSession.value && scSession.value.id) || '', scPv2LpReset);
        watch(() => (scActivePair.value && scActivePair.value.id) || '', scPv2LpReset);
        // Смена пары в селекторе панели → сброс загруженного отчёта до
        // явного «Загрузить»: данные и изображения не могут разъехаться
        // по парам (поздние ответы отменяет sequence guard).
        watch(scPv2LpPairId, () => {
            scPv2LpReqSeq++;
            scPv2LpResp.value = null;
            scPv2LpError.value = '';
            scPv2LpLoading.value = false;
            scPv2LpSelectedPage.value = '';
            scPv2LpSelectedLink.value = '';
        });

        // ── Мост: grounding-карточка → block link preview ───────────────────
        // Выводит block ids из карточки (или из item_id gv_<L>__<R>).
        function scPv2GdBlockIdsFromCard(card) {
            let left = (card && card.left_block_id) || null;
            let right = (card && card.right_block_id) || null;
            if ((!left || !right) && card && card.item_id) {
                const parts = String(card.item_id).replace(/^gv_/, '').split('__');
                if (parts.length === 2) {
                    left = left || parts[0];
                    right = right || parts[1];
                }
            }
            if (!left && card && card.side === 'old' && card.block_id) left = card.block_id;
            if (!right && card && card.side === 'new' && card.block_id) right = card.block_id;
            if (!left && card && card.block_id) left = card.block_id;   // either
            return { left, right };
        }
        function scPv2GdCardHasTarget(card) {
            if (!card) return false;
            return !!(card.left_block_id || card.right_block_id || card.item_id
                || card.block_id || card.left_page_number != null
                || card.right_page_number != null || card.page_number != null);
        }
        function scPv2GdMatchLink(target) {
            const links = scPv2LpAllLinks.value || [];
            // 1) exact: оба block_id совпали
            let m = links.find(l => l.kind === 'link'
                && l.left_block_id === target.left_block_id
                && l.right_block_id === target.right_block_id);
            if (m) return m;
            // 2) по одному block_id
            m = links.find(l => l.kind === 'link'
                && (l.left_block_id === target.left_block_id
                    || l.right_block_id === target.right_block_id));
            if (m) return m;
            // 3) односторонний блок (unmatched)
            m = links.find(l => l.kind === 'unmatched'
                && (l.block_id === target.left_block_id
                    || l.block_id === target.right_block_id));
            if (m) return m;
            // 4) по номерам страниц
            if (target.left_page_number != null && target.right_page_number != null) {
                m = links.find(l => l.kind === 'link'
                    && l.left_page_number === target.left_page_number
                    && l.right_page_number === target.right_page_number);
                if (m) return m;
            }
            return null;
        }
        function scPv2GdSelectMatchingLink(target) {
            const link = scPv2GdMatchLink(target);
            if (link) {
                scPv2LpSelectedLink.value = '';     // сброс → toggle выберет
                scPv2LpSelectLink(link);            // ставит link + page
                scPv2GdJumpWarning.value = '';
                nextTick(() => {
                    try {
                        const el = (typeof document !== 'undefined')
                            && document.querySelector('[data-bllink="' + link.block_link_id + '"]');
                        if (el && el.scrollIntoView)
                            el.scrollIntoView({ block: 'center', behavior: 'smooth' });
                    } catch (_) { /* no-op в jsdom/без DOM */ }
                });
                return true;
            }
            scPv2GdJumpWarning.value =
                'Связь блоков для этой grounding-карточки не найдена. Откройте пару вручную.';
            return false;
        }
        // Главный обработчик кнопки «🔗 К связи блоков» в grounding-карточке.
        async function scPv2OpenBlockLinkFromGrounding(card) {
            if (!card) return;
            const ids = scPv2GdBlockIdsFromCard(card);
            const pid = scPv2PairId.value
                || (scActivePair.value && scActivePair.value.id) || '';
            const target = {
                pair_id: pid, item_id: card.item_id || '',
                left_block_id: ids.left, right_block_id: ids.right,
                left_page_number: (card.left_page_number != null ? card.left_page_number
                    : (card.side === 'old' ? card.page_number : null)),
                right_page_number: (card.right_page_number != null ? card.right_page_number
                    : (card.side === 'new' ? card.page_number : null)),
                label: card.value || '',
            };
            // 1) закрыть grounding drawer
            scPv2GdOpen.value = false;
            scPv2GdJumpWarning.value = '';
            // 2) активировать пару + вкладку «Связь блоков» (если другая активна;
            //    scActivePair-watch сбросит LP-панель — поэтому ДО открытия LP)
            if (pid && (!scActivePair.value || scActivePair.value.id !== pid)) {
                const pairObj = (scPairs.value || []).find(p => p && p.id === pid);
                if (pairObj && pairObj.left && pairObj.right) {
                    await scOpenPair(pairObj);
                } else {
                    scTab.value = 'links';
                }
            } else {
                scTab.value = 'links';
            }
            // 3) открыть LP-панель на нужной паре
            scPv2LpPairId.value = pid;
            scPv2LpVisible.value = true;
            // 4) загрузить отчёт пары, если не загружен / другой пары
            const loadedPid = scPv2LpResp.value && scPv2LpResp.value.pair_id;
            if (!scPv2LpResp.value || loadedPid !== pid) {
                await scPv2LpLoad();
            }
            // 5) баннер + поиск/выбор связи
            scPv2GdJumpBanner.value = target.label;
            scPv2GdSelectMatchingLink(target);
        }
        function scPv2GdClearJumpBanner() {
            scPv2GdJumpBanner.value = '';
            scPv2GdJumpWarning.value = '';
        }

        // ─── Stage Comparison: Pipeline V2 Entity Alignment Preview ──────────
        // Read-only «Сущности и маппинг»: GET /api/stage-comparison/pipeline-v2/
        // {sid}/entity-alignment-preview?pair_id=. Классифицирует графические
        // пары OLD↔NEW (same_entity_likely / possible_rename / scope_reorganized
        // / mismatch_likely / link_validation_candidate) + unpaired-сущности.
        // Ничего не применяет: подтверждения/перепривязки маппинга НЕТ (это
        // отдельный будущий этап). Никаких job'ов, моделей, мутаций.
        const scPv2EaVisible = ref(false);
        const scPv2EaLoading = ref(false);
        const scPv2EaError = ref('');
        const scPv2EaResp = ref(null);         // detail {status, summary, pairs, unpaired_entities, …}
        const scPv2EaPairId = ref('');
        const scPv2EaFilter = ref('all');      // all|<classification>|unpaired
        let scPv2EaReqSeq = 0;

        const SC_PV2_EA_CLASS_META = {
            same_entity_likely:        { label: 'Same entity', icon: '🟢', color: '#16a34a', bg: '#dcfce7', fg: '#166534' },
            possible_rename:           { label: 'Возможно переименование', icon: '🔵', color: '#2563eb', bg: '#dbeafe', fg: '#1e40af' },
            scope_reorganized:         { label: 'Реорганизация', icon: '🟠', color: '#ea580c', bg: '#ffedd5', fg: '#9a3412' },
            mismatch_likely:           { label: 'Mismatch', icon: '🔴', color: '#dc2626', bg: '#fee2e2', fg: '#991b1b' },
            link_validation_candidate: { label: 'Проверка связи', icon: '🟣', color: '#7c3aed', bg: '#ede9fe', fg: '#5b21b6' },
        };
        const SC_PV2_EA_FILTERS = [
            { key: 'all', label: 'Все' },
            { key: 'same_entity_likely', label: '🟢 Same entity' },
            { key: 'scope_reorganized', label: '🟠 Реорганизация' },
            { key: 'mismatch_likely', label: '🔴 Mismatch' },
            { key: 'link_validation_candidate', label: '🟣 Проверка связи' },
            { key: 'unpaired', label: '⚪ Без пары' },
        ];

        const scPv2EaSummary = computed(() =>
            (scPv2EaResp.value && scPv2EaResp.value.summary) || null);
        const scPv2EaPairs = computed(() =>
            (scPv2EaResp.value && scPv2EaResp.value.pairs) || []);
        const scPv2EaUnpaired = computed(() =>
            (scPv2EaResp.value && scPv2EaResp.value.unpaired_entities) || { left: [], right: [] });
        const scPv2EaNotFound = computed(() =>
            !!scPv2EaResp.value && scPv2EaResp.value.status === 'not_found');
        const scPv2EaRespError = computed(() =>
            (scPv2EaResp.value && scPv2EaResp.value.status === 'error')
                ? ((scPv2EaResp.value.warnings || []).join('; ')
                   || scPv2EaResp.value.message || 'error')
                : '');
        const scPv2EaShowUnpaired = computed(() =>
            scPv2EaFilter.value === 'all' || scPv2EaFilter.value === 'unpaired');
        const scPv2EaShowPairs = computed(() => scPv2EaFilter.value !== 'unpaired');
        const scPv2EaFilteredPairs = computed(() => {
            const f = scPv2EaFilter.value;
            if (f === 'unpaired') return [];
            if (!f || f === 'all') return scPv2EaPairs.value;
            return scPv2EaPairs.value.filter((p) => p.classification === f);
        });
        function scPv2EaClassMeta(c) {
            return SC_PV2_EA_CLASS_META[c]
                || { label: c || '—', icon: '⚪', color: '#6b7280', bg: '#f3f4f6', fg: '#6b7280' };
        }
        function scPv2EaConfPct(p) {
            const c = p && p.confidence;
            return (typeof c === 'number') ? Math.round(c * 100) + '%' : '';
        }
        function scPv2EaEffectivePairId() {
            return scPv2EaPairId.value
                || (scActivePair.value && scActivePair.value.id) || '';
        }
        async function scPv2EaLoad() {
            const sid = scSession.value && scSession.value.id;
            const pid = scPv2EaEffectivePairId();
            if (!sid || !pid) return;
            const myReq = ++scPv2EaReqSeq;
            scPv2EaLoading.value = true;
            scPv2EaError.value = '';
            const url = '/api/stage-comparison/pipeline-v2/'
                + encodeURIComponent(sid)
                + '/entity-alignment-preview?pair_id=' + encodeURIComponent(pid)
                + '&limit=500';
            try {
                const r = await fetch(url);
                if (myReq !== scPv2EaReqSeq) return;   // устаревший ответ
                if (r.status === 401 || r.status === 403) {
                    scPv2EaResp.value = null;
                    scPv2EaError.value = 'Доступ запрещён (' + r.status
                        + '). Войдите в портал заново.';
                    return;
                }
                if (!r.ok) throw new Error('HTTP ' + r.status);
                const j = await r.json();
                if (myReq !== scPv2EaReqSeq) return;   // устаревший ответ
                scPv2EaResp.value = j;
                // инициализировать draft-объекты решений (seed из manual_mapping)
                for (const k of Object.keys(scPv2EaDrafts)) delete scPv2EaDrafts[k];
                for (const k of Object.keys(scPv2EaSaveErr)) delete scPv2EaSaveErr[k];
                scPv2EaSaveHint.value = '';
                for (const p of (j.pairs || [])) {
                    scPv2EaDraft(scPv2EaPairKey(p),
                                 (p.manual_mapping && p.manual_mapping.decision) || '');
                }
                const un = j.unpaired_entities || {};
                for (const e of (un.left || [])) {
                    scPv2EaDraft(scPv2EaUnpairedKey(e, 'left'),
                                 (e.manual_mapping && e.manual_mapping.decision) || '');
                }
                for (const e of (un.right || [])) {
                    scPv2EaDraft(scPv2EaUnpairedKey(e, 'right'),
                                 (e.manual_mapping && e.manual_mapping.decision) || '');
                }
                // подгрузить link validation, exclusion preview, skip readiness
                // и controlled enforce preflight для той же пары (read-only)
                scPv2LvLoad();
                scPv2XpLoad();
                scPv2SrLoad();
                scPv2CeLoad();
                scPv2CdrLoad();
            } catch (e) {
                if (myReq !== scPv2EaReqSeq) return;
                scPv2EaResp.value = null;
                scPv2EaError.value = String((e && e.message) || e);
            } finally {
                if (myReq === scPv2EaReqSeq) scPv2EaLoading.value = false;
            }
        }
        function scPv2EaToggle() {
            scPv2EaVisible.value = !scPv2EaVisible.value;
            if (scPv2EaVisible.value && !scPv2EaResp.value
                    && !scPv2EaLoading.value) {
                scPv2EaPairId.value = scPv2EaEffectivePairId();
                scPv2EaLoad();
            }
        }
        function scPv2EaReset() {
            scPv2EaReqSeq++;
            scPv2EaVisible.value = false;
            scPv2EaResp.value = null;
            scPv2EaError.value = '';
            scPv2EaLoading.value = false;
            scPv2EaPairId.value = '';
            scPv2EaFilter.value = 'all';
            scPv2LvReset();
            scPv2XpReset();
            scPv2SrReset();
            scPv2CeReset();
            scPv2CdrReset();
        }
        watch(() => (scSession.value && scSession.value.id) || '', scPv2EaReset);
        watch(() => (scActivePair.value && scActivePair.value.id) || '', scPv2EaReset);
        watch(scPv2EaPairId, () => {
            scPv2EaReqSeq++;
            scPv2EaResp.value = null;
            scPv2EaError.value = '';
            scPv2EaLoading.value = false;
            scPv2LvReset();
            scPv2XpReset();
            scPv2SrReset();
            scPv2CeReset();
            scPv2CdrReset();
        });
        // Read-only jump: карточка выравнивания сущностей → block link preview.
        // Переиспользует существующий мост (scPv2OpenBlockLinkFromGrounding):
        // НЕ применяет связь, только подсвечивает её в панели «Связь блоков».
        function scPv2EaOpenBlockLink(p) {
            if (!p) return;
            scPv2OpenBlockLinkFromGrounding({
                item_id: '',
                left_block_id: p.left_block_id,
                right_block_id: p.right_block_id,
                left_page_number: p.left_page_number,
                right_page_number: p.right_page_number,
                value: (p.left_entity_label || p.right_entity_label
                        || p.entity_family || ''),
            });
        }

        // ── Link validation (read-only, mark-only) ───────────────────────────
        // GET /api/stage-comparison/pipeline-v2/{sid}/link-validation?pair_id=.
        // Vision-проверка manual mapping пар (same/reorganized/different) +
        // agreement с ручным решением. НЕ grounded-факт, НЕ для delta. Read-only:
        // ничего не запускает/применяет/создаёт.
        const scPv2LvResp = ref(null);          // detail {status, summary, items, …}
        const scPv2LvLoading = ref(false);
        const scPv2LvError = ref('');
        let scPv2LvReqSeq = 0;

        const SC_PV2_LV_DECISION_META = {
            valid_mapping: { label: 'valid_mapping', icon: '🟢', bg: '#dcfce7', fg: '#166534' },
            manual_review: { label: 'manual_review', icon: '🟡', bg: '#fef9c3', fg: '#854d0e' },
            reject_mapping: { label: 'reject_mapping', icon: '🔴', bg: '#fee2e2', fg: '#991b1b' },
        };

        const scPv2LvSummary = computed(() =>
            (scPv2LvResp.value && scPv2LvResp.value.summary) || null);
        const scPv2LvItems = computed(() =>
            (scPv2LvResp.value && scPv2LvResp.value.items) || []);
        const scPv2LvAvailable = computed(() =>
            !!scPv2LvResp.value && scPv2LvResp.value.status === 'ok'
            && scPv2LvResp.value.available);
        const scPv2LvNotFound = computed(() =>
            !!scPv2LvResp.value && scPv2LvResp.value.status === 'not_found');
        const scPv2LvRespError = computed(() =>
            (scPv2LvResp.value && scPv2LvResp.value.status === 'error')
                ? ((scPv2LvResp.value.warnings || []).join('; ')
                   || scPv2LvResp.value.message || 'error')
                : '');
        function scPv2LvDecisionMeta(d) {
            return SC_PV2_LV_DECISION_META[d]
                || { label: d || '—', icon: '⚪', bg: '#f3f4f6', fg: '#374151' };
        }
        function scPv2LvConfPct(it) {
            const c = it && it.validation && it.validation.confidence;
            return (typeof c === 'number') ? Math.round(c * 100) + '%' : '';
        }
        async function scPv2LvLoad() {
            const sid = scSession.value && scSession.value.id;
            const pid = scPv2EaEffectivePairId();
            if (!sid || !pid) return;
            const myReq = ++scPv2LvReqSeq;
            scPv2LvLoading.value = true;
            scPv2LvError.value = '';
            const url = '/api/stage-comparison/pipeline-v2/'
                + encodeURIComponent(sid)
                + '/link-validation?pair_id=' + encodeURIComponent(pid) + '&limit=200';
            try {
                const r = await fetch(url);
                if (myReq !== scPv2LvReqSeq) return;
                if (r.status === 401 || r.status === 403) {
                    scPv2LvResp.value = null;
                    scPv2LvError.value = 'Доступ запрещён (' + r.status + ').';
                    return;
                }
                if (!r.ok) throw new Error('HTTP ' + r.status);
                const j = await r.json();
                if (myReq !== scPv2LvReqSeq) return;
                scPv2LvResp.value = j;
            } catch (e) {
                if (myReq !== scPv2LvReqSeq) return;
                scPv2LvResp.value = null;
                scPv2LvError.value = String((e && e.message) || e);
            } finally {
                if (myReq === scPv2LvReqSeq) scPv2LvLoading.value = false;
            }
        }
        function scPv2LvReset() {
            scPv2LvReqSeq++;
            scPv2LvResp.value = null;
            scPv2LvError.value = '';
            scPv2LvLoading.value = false;
        }
        // read-only jump: validation item → block link preview (reuse мост)
        function scPv2LvOpenBlockLink(it) {
            if (!it) return;
            scPv2OpenBlockLinkFromGrounding({
                item_id: '', left_block_id: it.left_block_id,
                right_block_id: it.right_block_id,
                left_page_number: it.left_page_number,
                right_page_number: it.right_page_number,
                value: (it.left_entity_label || it.right_entity_label || ''),
            });
        }

        // ── Exclusion Preview v2 (read-only, mark-only) ─────────────────────

        const scPv2XpResp = ref(null);          // detail {status, summary, items, …}
        const scPv2XpLoading = ref(false);
        const scPv2XpError = ref('');
        let scPv2XpReqSeq = 0;

        const SC_PV2_XP_CLASS_META = {
            candidate_exclude:       { icon: '🔴', bg: '#fee2e2', fg: '#991b1b', label: 'исключить' },
            review_only:             { icon: '🟡', bg: '#fef9c3', fg: '#854d0e', label: 'ручная проверка' },
            keep:                    { icon: '🟢', bg: '#dcfce7', fg: '#166534', label: 'оставить' },
            link_validation_required:{ icon: '🔵', bg: '#ede9fe', fg: '#4c1d95', label: 'нужна LV' },
        };

        const scPv2XpSummary = computed(() =>
            (scPv2XpResp.value && scPv2XpResp.value.summary) || null);
        const scPv2XpItems = computed(() =>
            (scPv2XpResp.value && scPv2XpResp.value.items) || []);
        const scPv2XpAvailable = computed(() =>
            !!scPv2XpResp.value && scPv2XpResp.value.status === 'ok'
            && scPv2XpResp.value.available);
        const scPv2XpNotFound = computed(() =>
            !!scPv2XpResp.value && scPv2XpResp.value.status === 'not_found');
        const scPv2XpRespError = computed(() =>
            (scPv2XpResp.value && scPv2XpResp.value.status === 'error')
                ? ((scPv2XpResp.value.warnings || []).join('; ')
                   || scPv2XpResp.value.message || 'error')
                : '');
        function scPv2XpClassMeta(cls) {
            return SC_PV2_XP_CLASS_META[cls]
                || { icon: '⚪', bg: '#f3f4f6', fg: '#374151', label: cls || '—' };
        }
        function scPv2XpConfPct(it) {
            const c = it && it.confidence;
            return (typeof c === 'number') ? Math.round(c * 100) + '%' : '';
        }
        async function scPv2XpLoad() {
            const sid = scSession.value && scSession.value.id;
            const pid = scPv2EaEffectivePairId();
            if (!sid || !pid) return;
            const myReq = ++scPv2XpReqSeq;
            scPv2XpLoading.value = true;
            scPv2XpError.value = '';
            const url = '/api/stage-comparison/pipeline-v2/'
                + encodeURIComponent(sid)
                + '/exclusion-preview-v2?pair_id=' + encodeURIComponent(pid) + '&limit=200';
            try {
                const r = await fetch(url);
                if (myReq !== scPv2XpReqSeq) return;
                if (r.status === 401 || r.status === 403) {
                    scPv2XpResp.value = null;
                    scPv2XpError.value = 'Доступ запрещён (' + r.status + ').';
                    return;
                }
                if (!r.ok) throw new Error('HTTP ' + r.status);
                const j = await r.json();
                if (myReq !== scPv2XpReqSeq) return;
                scPv2XpResp.value = j;
            } catch (e) {
                if (myReq !== scPv2XpReqSeq) return;
                scPv2XpResp.value = null;
                scPv2XpError.value = String((e && e.message) || e);
            } finally {
                if (myReq === scPv2XpReqSeq) scPv2XpLoading.value = false;
            }
        }
        function scPv2XpReset() {
            scPv2XpReqSeq++;
            scPv2XpResp.value = null;
            scPv2XpError.value = '';
            scPv2XpLoading.value = false;
        }

        // ── Skip Readiness (read-only, mark-only) ────────────────────────────
        // ТОЛЬКО чтение GET .../skip-readiness — НЕ запускает модели,
        // НЕ пишет файлы, НЕ применяет пропуски. Observe / mark-only.

        const scPv2SrResp = ref(null);          // detail {status, summary, items, …}
        const scPv2SrLoading = ref(false);
        const scPv2SrError = ref('');
        let scPv2SrReqSeq = 0;

        const SC_PV2_SR_READINESS_META = {
            ready_to_skip: { icon: '✅', bg: '#dcfce7', fg: '#166534', label: 'к пропуску' },
            blocked:       { icon: '🔴', bg: '#fee2e2', fg: '#991b1b', label: 'заблокирован' },
            needs_review:  { icon: '🟡', bg: '#fef9c3', fg: '#854d0e', label: 'нужна проверка' },
            keep:          { icon: '🟢', bg: '#f0fdf4', fg: '#166534', label: 'оставить' },
        };

        const scPv2SrSummary = computed(() =>
            (scPv2SrResp.value && scPv2SrResp.value.summary) || null);
        const scPv2SrItems = computed(() =>
            (scPv2SrResp.value && scPv2SrResp.value.items) || []);
        const scPv2SrAvailable = computed(() =>
            !!scPv2SrResp.value && scPv2SrResp.value.status === 'ok'
            && scPv2SrResp.value.available);
        const scPv2SrNotFound = computed(() =>
            !!scPv2SrResp.value && scPv2SrResp.value.status === 'not_found');
        const scPv2SrRespError = computed(() =>
            (scPv2SrResp.value && scPv2SrResp.value.status === 'error')
                ? ((scPv2SrResp.value.warnings || []).join('; ')
                   || scPv2SrResp.value.message || 'error')
                : '');
        function scPv2SrReadinessMeta(status) {
            return SC_PV2_SR_READINESS_META[status]
                || { icon: '⚪', bg: '#f3f4f6', fg: '#374151', label: status || '—' };
        }
        function scPv2SrConfPct(it) {
            const c = it && it.confidence;
            return (typeof c === 'number') ? Math.round(c * 100) + '%' : '';
        }
        async function scPv2SrLoad() {
            const sid = scSession.value && scSession.value.id;
            const pid = scPv2EaEffectivePairId();
            if (!sid || !pid) return;
            const myReq = ++scPv2SrReqSeq;
            scPv2SrLoading.value = true;
            scPv2SrError.value = '';
            const url = '/api/stage-comparison/pipeline-v2/'
                + encodeURIComponent(sid)
                + '/skip-readiness?pair_id=' + encodeURIComponent(pid) + '&limit=200';
            try {
                const r = await fetch(url);
                if (myReq !== scPv2SrReqSeq) return;
                if (r.status === 401 || r.status === 403) {
                    scPv2SrResp.value = null;
                    scPv2SrError.value = 'Доступ запрещён (' + r.status + ').';
                    return;
                }
                if (!r.ok) throw new Error('HTTP ' + r.status);
                const j = await r.json();
                if (myReq !== scPv2SrReqSeq) return;
                scPv2SrResp.value = j;
            } catch (e) {
                if (myReq !== scPv2SrReqSeq) return;
                scPv2SrResp.value = null;
                scPv2SrError.value = String((e && e.message) || e);
            } finally {
                if (myReq === scPv2SrReqSeq) scPv2SrLoading.value = false;
            }
        }
        function scPv2SrReset() {
            scPv2SrReqSeq++;
            scPv2SrResp.value = null;
            scPv2SrError.value = '';
            scPv2SrLoading.value = false;
        }

        // ── Controlled Enforce Preflight (read-only, observe-only) ───────────
        // ТОЛЬКО чтение GET .../controlled-enforce-preflight. Это НЕ enforce:
        // НЕ пропускает/исключает, НЕ применяет, НЕ запускает модели/jobs,
        // НЕ создаёт замечаний, НЕ меняет block links. Observe / preflight-only.

        const scPv2CeResp = ref(null);          // detail {status, summary, …}
        const scPv2CeLoading = ref(false);
        const scPv2CeError = ref('');
        let scPv2CeReqSeq = 0;

        const SC_PV2_CE_STATUS_META = {
            blocked:          { icon: '🔴', bg: '#fee2e2', fg: '#991b1b', label: 'enforce заблокирован' },
            preflight_ok:     { icon: '🟢', bg: '#dcfce7', fg: '#166534', label: 'preflight ok (не применяется)' },
            no_eligible_items:{ icon: '🟡', bg: '#fef9c3', fg: '#854d0e', label: 'нет eligible' },
        };
        const SC_PV2_CE_BLOCK_META = {
            blocked:      { icon: '🔴', bg: '#fee2e2', fg: '#991b1b', label: 'заблокирован' },
            needs_review: { icon: '🟡', bg: '#fef9c3', fg: '#854d0e', label: 'нужна проверка' },
            keep:         { icon: '🟢', bg: '#f0fdf4', fg: '#166534', label: 'оставить' },
            ready_to_skip:{ icon: '✅', bg: '#dcfce7', fg: '#166534', label: 'к пропуску' },
        };

        const scPv2CeSummary = computed(() =>
            (scPv2CeResp.value && scPv2CeResp.value.summary) || null);
        const scPv2CeGuards = computed(() =>
            (scPv2CeResp.value && scPv2CeResp.value.global_guards) || null);
        const scPv2CeRuntimeRoot = computed(() =>
            (scPv2CeResp.value && scPv2CeResp.value.runtime_root) || null);
        const scPv2CeFatalBlocks = computed(() =>
            (scPv2CeResp.value && scPv2CeResp.value.fatal_blocks) || []);
        const scPv2CeBlockedItems = computed(() =>
            (scPv2CeResp.value && scPv2CeResp.value.blocked_items) || []);
        const scPv2CeEligibleItems = computed(() =>
            (scPv2CeResp.value && scPv2CeResp.value.eligible_items) || []);
        const scPv2CeReportStatus = computed(() =>
            (scPv2CeResp.value && scPv2CeResp.value.report_status) || '');
        const scPv2CeAvailable = computed(() =>
            !!scPv2CeResp.value && scPv2CeResp.value.status === 'ok'
            && scPv2CeResp.value.available);
        const scPv2CeNotFound = computed(() =>
            !!scPv2CeResp.value && scPv2CeResp.value.status === 'not_found');
        const scPv2CeRespError = computed(() =>
            (scPv2CeResp.value && scPv2CeResp.value.status === 'error')
                ? ((scPv2CeResp.value.warnings || []).join('; ')
                   || scPv2CeResp.value.message || 'error')
                : '');
        function scPv2CeStatusMeta(status) {
            return SC_PV2_CE_STATUS_META[status]
                || { icon: '⚪', bg: '#f3f4f6', fg: '#374151', label: status || '—' };
        }
        function scPv2CeBlockMeta(status) {
            return SC_PV2_CE_BLOCK_META[status]
                || { icon: '⚪', bg: '#f3f4f6', fg: '#374151', label: status || '—' };
        }
        async function scPv2CeLoad() {
            const sid = scSession.value && scSession.value.id;
            const pid = scPv2EaEffectivePairId();
            if (!sid || !pid) return;
            const myReq = ++scPv2CeReqSeq;
            scPv2CeLoading.value = true;
            scPv2CeError.value = '';
            const url = '/api/stage-comparison/pipeline-v2/'
                + encodeURIComponent(sid)
                + '/controlled-enforce-preflight?pair_id='
                + encodeURIComponent(pid) + '&limit=200';
            try {
                const r = await fetch(url);
                if (myReq !== scPv2CeReqSeq) return;
                if (r.status === 401 || r.status === 403) {
                    scPv2CeResp.value = null;
                    scPv2CeError.value = 'Доступ запрещён (' + r.status + ').';
                    return;
                }
                if (!r.ok) throw new Error('HTTP ' + r.status);
                const j = await r.json();
                if (myReq !== scPv2CeReqSeq) return;
                scPv2CeResp.value = j;
            } catch (e) {
                if (myReq !== scPv2CeReqSeq) return;
                scPv2CeResp.value = null;
                scPv2CeError.value = String((e && e.message) || e);
            } finally {
                if (myReq === scPv2CeReqSeq) scPv2CeLoading.value = false;
            }
        }
        function scPv2CeReset() {
            scPv2CeReqSeq++;
            scPv2CeResp.value = null;
            scPv2CeError.value = '';
            scPv2CeLoading.value = false;
        }

        // ── Controlled Enforce Dry-run (read-only, observe-only) ─────────────
        // ТОЛЬКО чтение GET .../controlled-enforce-dry-run. Показывает «что было
        // бы пропущено», но НИЧЕГО не применяет/не пишет. Dry-run only.

        const scPv2CdrResp = ref(null);
        const scPv2CdrLoading = ref(false);
        const scPv2CdrError = ref('');
        let scPv2CdrReqSeq = 0;

        const scPv2CdrSummary = computed(() =>
            (scPv2CdrResp.value && scPv2CdrResp.value.summary) || null);
        const scPv2CdrTransitions = computed(() =>
            (scPv2CdrResp.value && scPv2CdrResp.value.logical_transitions) || []);
        const scPv2CdrItems = computed(() =>
            (scPv2CdrResp.value && scPv2CdrResp.value.would_skip_items) || []);
        const scPv2CdrReportStatus = computed(() =>
            (scPv2CdrResp.value && scPv2CdrResp.value.report_status) || '');
        const scPv2CdrAvailable = computed(() =>
            !!scPv2CdrResp.value && scPv2CdrResp.value.status === 'ok'
            && scPv2CdrResp.value.available);
        const scPv2CdrNotFound = computed(() =>
            !!scPv2CdrResp.value && scPv2CdrResp.value.status === 'not_found');
        const scPv2CdrRespError = computed(() =>
            (scPv2CdrResp.value && scPv2CdrResp.value.status === 'error')
                ? ((scPv2CdrResp.value.warnings || []).join('; ')
                   || scPv2CdrResp.value.message || 'error')
                : '');
        async function scPv2CdrLoad() {
            const sid = scSession.value && scSession.value.id;
            const pid = scPv2EaEffectivePairId();
            if (!sid || !pid) return;
            const myReq = ++scPv2CdrReqSeq;
            scPv2CdrLoading.value = true;
            scPv2CdrError.value = '';
            const url = '/api/stage-comparison/pipeline-v2/'
                + encodeURIComponent(sid)
                + '/controlled-enforce-dry-run?pair_id='
                + encodeURIComponent(pid) + '&limit=200';
            try {
                const r = await fetch(url);
                if (myReq !== scPv2CdrReqSeq) return;
                if (r.status === 401 || r.status === 403) {
                    scPv2CdrResp.value = null;
                    scPv2CdrError.value = 'Доступ запрещён (' + r.status + ').';
                    return;
                }
                if (!r.ok) throw new Error('HTTP ' + r.status);
                const j = await r.json();
                if (myReq !== scPv2CdrReqSeq) return;
                scPv2CdrResp.value = j;
            } catch (e) {
                if (myReq !== scPv2CdrReqSeq) return;
                scPv2CdrResp.value = null;
                scPv2CdrError.value = String((e && e.message) || e);
            } finally {
                if (myReq === scPv2CdrReqSeq) scPv2CdrLoading.value = false;
            }
        }
        function scPv2CdrReset() {
            scPv2CdrReqSeq++;
            scPv2CdrResp.value = null;
            scPv2CdrError.value = '';
            scPv2CdrLoading.value = false;
        }

        // ── Controlled Enforce STATE (read-only видимость active state) ──────
        // Источник — ui-payload (controlled_enforce_state section). Это ВИДИМОСТЬ
        // active controlled exclusion state, НЕ enforce/apply. Никаких действий.
        const scPv2CesSection = computed(() =>
            (scPv2Payload.value && scPv2Payload.value.controlled_enforce_state) || null);
        const scPv2CesAvailable = computed(() =>
            !!(scPv2CesSection.value && scPv2CesSection.value.available));

        // ── Selection Observe (read-only observe-mode, Qwen НЕ вызывался) ────
        // Источник — ui-payload (controlled_enforce_selection_observe section).
        const scPv2CesoSection = computed(() =>
            (scPv2Payload.value
             && scPv2Payload.value.controlled_enforce_selection_observe) || null);
        const scPv2CesoAvailable = computed(() =>
            !!(scPv2CesoSection.value && scPv2CesoSection.value.available));

        // ── Enrichment Selection Observe (read-only observe-plan, Qwen НЕ зван) ──
        // Источник — ui-payload (enrichment_selection_observe section).
        const scPv2EsoSection = computed(() =>
            (scPv2Payload.value
             && scPv2Payload.value.enrichment_selection_observe) || null);
        const scPv2EsoAvailable = computed(() =>
            !!(scPv2EsoSection.value && scPv2EsoSection.value.available));
        // redundant_state_matches — пары, уже исключённые ДО хука (real path)
        const scPv2EsoRedundant = computed(() =>
            (scPv2EsoSection.value && scPv2EsoSection.value.redundant_state_matches) || []);

        // ── Controlled Enforce State DEACTIVATE (rollback, write-слой) ───────
        // POST .../controlled-enforce-state/deactivate — пишет ТОЛЬКО state
        // (active=false + audit + history). Требует точного confirmation.
        // Никаких enforce/qwen/jobs/findings/links. Жёсткий confirm.
        const SC_PV2_CDS_PHRASE = 'DEACTIVATE_CONTROLLED_STATE';
        const scPv2CdsOpen = ref(false);
        const scPv2CdsConfirmText = ref('');
        const scPv2CdsComment = ref('');
        const scPv2CdsRunId = ref('');
        const scPv2CdsBusy = ref(false);
        const scPv2CdsError = ref('');
        const scPv2CdsDone = ref(false);
        const scPv2CdsConfirmOk = computed(() =>
            scPv2CdsConfirmText.value === SC_PV2_CDS_PHRASE);

        async function scPv2CdsBegin() {
            scPv2CdsOpen.value = true;
            scPv2CdsConfirmText.value = '';
            scPv2CdsComment.value = '';
            scPv2CdsError.value = '';
            scPv2CdsDone.value = false;
            scPv2CdsRunId.value = (scPv2CesSection.value && scPv2CesSection.value.run_id) || '';
            // авторитетный run_id — из read-only GET state endpoint
            const sid = scSession.value && scSession.value.id;
            const pid = scPv2EaEffectivePairId();
            if (!sid || !pid) return;
            try {
                const r = await fetch('/api/stage-comparison/pipeline-v2/'
                    + encodeURIComponent(sid) + '/controlled-enforce-state?pair_id='
                    + encodeURIComponent(pid));
                if (r.ok) {
                    const j = await r.json();
                    if (j && j.run_id) scPv2CdsRunId.value = j.run_id;
                }
            } catch (e) { /* run_id может остаться из секции */ }
        }
        function scPv2CdsCancel() {
            scPv2CdsOpen.value = false;
            scPv2CdsConfirmText.value = '';
            scPv2CdsError.value = '';
        }
        async function scPv2CdsSubmit() {
            if (!scPv2CdsConfirmOk.value || scPv2CdsBusy.value) return;
            const sid = scSession.value && scSession.value.id;
            const pid = scPv2EaEffectivePairId();
            if (!sid || !pid) { scPv2CdsError.value = 'Нет сессии/пары'; return; }
            if (!scPv2CdsRunId.value) { scPv2CdsError.value = 'run_id не загружен'; return; }
            scPv2CdsBusy.value = true;
            scPv2CdsError.value = '';
            try {
                const r = await fetch('/api/stage-comparison/pipeline-v2/'
                    + encodeURIComponent(sid)
                    + '/controlled-enforce-state/deactivate?pair_id='
                    + encodeURIComponent(pid), {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        run_id: scPv2CdsRunId.value,
                        confirmation: SC_PV2_CDS_PHRASE,
                        comment: scPv2CdsComment.value
                            || 'manual rollback / deactivate controlled state',
                        updated_by: 'operator',
                    }),
                });
                if (!r.ok) {
                    let msg = 'HTTP ' + r.status;
                    try { const j = await r.json(); msg = (j && (j.detail || j.message)) || msg; } catch (e) {}
                    throw new Error(msg);
                }
                scPv2CdsDone.value = true;
                scPv2CdsOpen.value = false;
                scPv2Load();   // перечитать ui-payload — панель отразит inactive
            } catch (e) {
                scPv2CdsError.value = String((e && e.message) || e);
            } finally {
                scPv2CdsBusy.value = false;
            }
        }

        // ── Operator review write-layer для Exclusion Preview v2 ─────────────
        // PUT .../exclusion-review-overrides — отдельный обратимый artifact.
        // mark-only: НЕ применяет исключения, НЕ запускает jobs/Qwen/Opus/Claude,
        // НЕ меняет exclusion_preview_v2_report.json, НЕ создаёт замечаний.

        const scPv2XrDrafts = reactive({});       // item_id → { decision, comment }
        const scPv2XrSaving = reactive({});        // item_id → bool
        const scPv2XrSaveErr = reactive({});       // item_id → string

        const SC_PV2_XR_DECISION_META = {
            approve_exclude:    { label: 'approve_exclude', style: 'background:#dcfce7; color:#166534' },
            reject_exclude:     { label: 'reject_exclude',  style: 'background:#fee2e2; color:#991b1b' },
            needs_review:       { label: 'needs_review',    style: 'background:#fef9c3; color:#854d0e' },
            keep:               { label: 'keep',            style: 'background:#f0fdf4; color:#15803d' },
            run_link_validation:{ label: 'run_link_valid.', style: 'background:#ede9fe; color:#4c1d95' },
        };

        function scPv2XrDecisionMeta(decision) {
            return SC_PV2_XR_DECISION_META[decision]
                || { label: decision || '—', style: 'background:#f3f4f6; color:#374151' };
        }

        function scPv2XrGetDraft(it) {
            const key = it && (it.item_id || it.left_block_id || String(it._index || ''));
            if (!key) return { decision: '', comment: '' };
            if (!scPv2XrDrafts[key]) {
                // pre-fill from existing decision if present
                const rev = it.operator_review;
                scPv2XrDrafts[key] = reactive({
                    decision: (rev && rev.status === 'reviewed' && rev.operator_decision) || '',
                    comment:  (rev && rev.comment) || '',
                });
            }
            return scPv2XrDrafts[key];
        }

        async function scPv2XrSaveDecision(it) {
            const key = it && (it.item_id || it.left_block_id || '');
            if (!key) return;
            const draft = scPv2XrGetDraft(it);
            if (!draft.decision) return;
            const sid = scSession.value && scSession.value.id;
            const pid = scPv2EaEffectivePairId();
            if (!sid || !pid) return;
            scPv2XrSaving[key] = true;
            scPv2XrSaveErr[key] = '';
            try {
                const body = {
                    decision: {
                        exclusion_item_id:   it.item_id || null,
                        left_block_id:       it.left_block_id || null,
                        right_block_id:      it.right_block_id || null,
                        left_entity_label:   it.left_entity_label || null,
                        right_entity_label:  it.right_entity_label || null,
                        preview_classification: it.classification || null,
                        preview_severity:    it.severity || null,
                        operator_decision:   draft.decision,
                        comment:             draft.comment || null,
                    },
                    created_by: null,
                };
                const r = await fetch(
                    '/api/stage-comparison/pipeline-v2/'
                    + encodeURIComponent(sid)
                    + '/exclusion-review-overrides?pair_id='
                    + encodeURIComponent(pid),
                    { method: 'PUT',
                      headers: { 'Content-Type': 'application/json' },
                      body: JSON.stringify(body) });
                if (!r.ok) {
                    const t = await r.text().catch(() => '');
                    throw new Error('HTTP ' + r.status + ': ' + t);
                }
                // reload preview to get fresh operator_review in items
                await scPv2XpLoad();
            } catch (e) {
                scPv2XrSaveErr[key] = String((e && e.message) || e);
            } finally {
                scPv2XrSaving[key] = false;
            }
        }

        async function scPv2XrDeleteDecision(it) {
            const rev = it && it.operator_review;
            if (!rev || !rev.decision_id) return;
            const sid = scSession.value && scSession.value.id;
            const pid = scPv2EaEffectivePairId();
            if (!sid || !pid) return;
            const key = it.item_id || it.left_block_id || '';
            scPv2XrSaving[key] = true;
            scPv2XrSaveErr[key] = '';
            try {
                const r = await fetch(
                    '/api/stage-comparison/pipeline-v2/'
                    + encodeURIComponent(sid)
                    + '/exclusion-review-overrides/'
                    + encodeURIComponent(rev.decision_id)
                    + '?pair_id=' + encodeURIComponent(pid),
                    { method: 'DELETE' });
                if (!r.ok) {
                    const t = await r.text().catch(() => '');
                    throw new Error('HTTP ' + r.status + ': ' + t);
                }
                // clear local draft and reload
                const draftKey = it.item_id || it.left_block_id || '';
                if (scPv2XrDrafts[draftKey]) {
                    scPv2XrDrafts[draftKey].decision = '';
                    scPv2XrDrafts[draftKey].comment = '';
                }
                await scPv2XpLoad();
            } catch (e) {
                scPv2XrSaveErr[key] = String((e && e.message) || e);
            } finally {
                scPv2XrSaving[key] = false;
            }
        }

        // ── Manual entity mapping (write-слой поверх preview) ─────────────────
        // PUT .../entity-mapping-overrides — отдельный обратимый artifact. НЕ
        // применяет block links, НЕ запускает vision/Qwen/Opus, НЕ создаёт
        // замечаний. Только сохраняет ручное решение и обновляет UI-state.
        const SC_PV2_EA_DECISIONS = [
            { key: 'confirmed_same_entity', label: '✅ Та же сущность' },
            { key: 'confirmed_rename', label: '🔁 Переименование' },
            { key: 'confirmed_reorganized', label: '🟠 Реорганизация' },
            { key: 'rejected_mapping', label: '❌ Отклонить связь' },
            { key: 'no_match', label: '⚪ Нет пары' },
        ];
        const SC_PV2_EA_MANUAL_META = {
            mapped:   { label: 'Подтверждено', color: '#16a34a', bg: '#dcfce7', fg: '#166534' },
            rejected: { label: 'Отклонено', color: '#dc2626', bg: '#fee2e2', fg: '#991b1b' },
            no_match: { label: 'Нет пары', color: '#6b7280', bg: '#f3f4f6', fg: '#374151' },
        };
        // decision → цвет (для бейджа сохранённого решения)
        const SC_PV2_EA_DECISION_META = {
            confirmed_same_entity: { label: 'confirmed_same_entity', color: '#16a34a' },
            confirmed_rename:      { label: 'confirmed_rename', color: '#2563eb' },
            confirmed_reorganized: { label: 'confirmed_reorganized', color: '#ea580c' },
            rejected_mapping:      { label: 'rejected_mapping', color: '#dc2626' },
            no_match:              { label: 'no_match', color: '#6b7280' },
        };
        const scPv2EaDrafts = reactive({});     // key → {decision, comment, counterpart}
        const scPv2EaSaving = reactive({});      // key → bool
        const scPv2EaSaveErr = reactive({});     // key → string
        const scPv2EaSaveHint = ref('');         // глобальная подсказка после сохранения

        function scPv2EaPairKey(p) {
            return (p && (p.pair_key
                || ((p.left_block_id || '') + '__' + (p.right_block_id || '')))) || '';
        }
        function scPv2EaUnpairedKey(e, side) {
            const b = (e && e.block_ids && e.block_ids[0]) || '';
            return side + ':' + ((e && e.entity_label) || '') + ':' + b;
        }
        function scPv2EaDraft(key, initDecision) {
            if (!scPv2EaDrafts[key]) {
                scPv2EaDrafts[key] = { decision: initDecision || '', comment: '',
                                       counterpart: '' };
            }
            return scPv2EaDrafts[key];
        }
        function scPv2EaDecisionMeta(decision) {
            return SC_PV2_EA_DECISION_META[decision]
                || { label: decision || '', color: '#6b7280' };
        }
        function scPv2EaManualMeta(status) {
            return SC_PV2_EA_MANUAL_META[status] || null;
        }
        async function _scPv2EaPutMapping(mapping) {
            const sid = scSession.value && scSession.value.id;
            const pid = (scPv2EaResp.value && scPv2EaResp.value.pair_id)
                || scPv2EaEffectivePairId();
            if (!sid || !pid) throw new Error('Нет активной пары');
            const url = '/api/stage-comparison/pipeline-v2/'
                + encodeURIComponent(sid) + '/entity-mapping-overrides?pair_id='
                + encodeURIComponent(pid);
            const by = (typeof currentUserName === 'function' ? currentUserName() : '')
                || usersLoggedInUsername.value || '';
            const r = await fetch(url, {
                method: 'PUT',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ mapping, created_by: by }),
            });
            if (r.status === 401 || r.status === 403) {
                throw new Error('Доступ запрещён (' + r.status + ')');
            }
            if (!r.ok) {
                let msg = 'HTTP ' + r.status;
                try { const j = await r.json(); if (j && j.detail) msg = j.detail; }
                catch (_) { /* no-op */ }
                throw new Error(msg);
            }
            return r.json();
        }
        function _scPv2EaApplyManual(targetObj, override, summary) {
            if (override) {
                targetObj.manual_mapping = {
                    status: override.manual_decision
                        && (['confirmed_same_entity', 'confirmed_rename', 'confirmed_reorganized']
                            .includes(override.manual_decision) ? 'mapped'
                            : override.manual_decision === 'rejected_mapping' ? 'rejected'
                            : override.manual_decision === 'no_match' ? 'no_match' : 'none'),
                    decision: override.manual_decision,
                    mapping_id: override.mapping_id,
                    comment: override.comment || null,
                    updated_at: override.updated_at,
                };
            }
            if (summary && scPv2EaResp.value && scPv2EaResp.value.summary) {
                scPv2EaResp.value.summary.manual_mapping = summary;
            }
            scPv2EaSaveHint.value = 'Ручной mapping сохранён. Он будет использован в '
                + 'следующем этапе отбора vision-кандидатов, но сейчас ничего не '
                + 'запускается (block links и vision не трогаются).';
        }
        async function scPv2EaSavePair(p) {
            if (!p) return;
            const key = scPv2EaPairKey(p);
            const draft = scPv2EaDraft(key);
            if (!draft.decision) { scPv2EaSaveErr[key] = 'Выберите решение'; return; }
            scPv2EaSaving[key] = true;
            scPv2EaSaveErr[key] = '';
            try {
                const res = await _scPv2EaPutMapping({
                    left_entity_label: p.left_entity_label,
                    right_entity_label: p.right_entity_label,
                    left_block_id: p.left_block_id,
                    right_block_id: p.right_block_id,
                    left_page_number: p.left_page_number,
                    right_page_number: p.right_page_number,
                    source_classification: p.classification,
                    pair_key: p.pair_key,
                    manual_decision: draft.decision,
                    comment: draft.comment || null,
                });
                _scPv2EaApplyManual(p, res.override, res.summary);
            } catch (e) {
                scPv2EaSaveErr[key] = String((e && e.message) || e);
            } finally {
                scPv2EaSaving[key] = false;
            }
        }
        // Кандидаты-counterpart для unpaired-сущности (с противоположной стороны).
        function scPv2EaUnpairedCounterparts(side) {
            const u = scPv2EaUnpaired.value || {};
            const other = side === 'left' ? (u.right || []) : (u.left || []);
            return other;
        }
        async function scPv2EaSaveUnpaired(e, side) {
            if (!e) return;
            const key = scPv2EaUnpairedKey(e, side);
            const draft = scPv2EaDraft(key);
            if (!draft.decision) { scPv2EaSaveErr[key] = 'Выберите решение'; return; }
            const myBlock = (e.block_ids && e.block_ids[0]) || null;
            // counterpart нужен только для confirmed_*
            const isConfirmed = ['confirmed_same_entity', 'confirmed_rename',
                                 'confirmed_reorganized'].includes(draft.decision);
            let cp = null;
            if (isConfirmed) {
                const list = scPv2EaUnpairedCounterparts(side);
                cp = list.find((x) => ((x.block_ids && x.block_ids[0]) || x.entity_label)
                    === draft.counterpart);
                if (!cp) { scPv2EaSaveErr[key] = 'Выберите counterpart с другой стороны'; return; }
            }
            const cpBlock = cp ? ((cp.block_ids && cp.block_ids[0]) || null) : null;
            const mapping = side === 'left'
                ? { left_entity_label: e.entity_label, left_block_id: myBlock,
                    right_entity_label: cp ? cp.entity_label : null, right_block_id: cpBlock }
                : { right_entity_label: e.entity_label, right_block_id: myBlock,
                    left_entity_label: cp ? cp.entity_label : null, left_block_id: cpBlock };
            mapping.manual_decision = draft.decision;
            mapping.comment = draft.comment || null;
            mapping.source_classification = 'unpaired';
            scPv2EaSaving[key] = true;
            scPv2EaSaveErr[key] = '';
            try {
                const res = await _scPv2EaPutMapping(mapping);
                _scPv2EaApplyManual(e, res.override, res.summary);
            } catch (err) {
                scPv2EaSaveErr[key] = String((err && err.message) || err);
            } finally {
                scPv2EaSaving[key] = false;
            }
        }

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
            subSpendOpen, subSpendLoading, subSpendData, subSpendWeekText,
            subSpendLoad, subSpendColor, subSpendInitials, subSpendDayLabel, subSpendTok,
            // State
            currentView, currentProject, currentProjectId, visiblePipelineSummary,
            projectLoading, projects, loading, isProjectView,
            findingsData, filterSeverity, filterSearch, severityOptions,
            // KB-Validation
            kbValidationAvailable, kbValidationLoading, findingKbDecision, findingKbLabel, findingKbClass, findingKbTooltip,
            evidenceValidationAvailable, evidenceValidationLoading, evidenceValidationRunning,
            findingEvDecision, findingEvLabel, findingEvClass, findingEvPathLabel, findingEvTooltip, runEvidenceValidation,
            // Inline Critic v2 (experimental, в обычной таблице)
            findingsCv2Available, findingsCv2Warning, findingsCv2Loading,
            cv2ShowHidden, cv2DisplayFilter, cv2DebugVisible, scDevTools,
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
            isSectionSelected, toggleSectionSelection, selectUnanalyzedInSection,
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
            sidebarSectionsOpen, sidebarFilterSection,
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
            paidCost, showPaidCost, fetchPaidCost, resetPaidCost, formatCostShort,
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
            lmsLoaded, lmsAll, lmsLoadCtx, lmsLoading, lmsMessage,
            lmsRefresh, lmsLoad, lmsUnload, lmsReload, lmsApplyPresetCtx,
            lmsHealth, lmsHealthCheckedAt, lmsHealthStatus, lmsHealthTitle, lmsCheckHealth,
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
            // ─── Stage Comparison ───
            scTab, scDiffSubtab, scStageAPath, scStageBPath,
            // Pipeline V2 (β) — read-only панель
            scPv2Loading, scPv2Error, scPv2Resp, scPv2PairId,
            scPv2Open, scPv2Filters, scPv2Payload, scPv2Sections,
            scPv2Headline, scPv2GraphicVision, scPv2GraphicGrounding,
            scPv2GroundingRejectedTotal,
            scPv2GroundedEvidence, scPv2GeAvailable, scPv2GeInterestingCards,
            scPv2GeBadgeStyle, scPv2GeAnchorText,
            scPv2CriticVerdictStyle, scPv2ShowText, scPv2GeVerdictBreakdown,
            scPv2GdOpen, scPv2GdLoading, scPv2GdError, scPv2GdResp,
            scPv2GdFilter, scPv2GdFilteredCards, scPv2GdPagination,
            scPv2GdStatusColor, scPv2GdOpenDrawer, scPv2GdClose,
            scPv2GdJumpBanner, scPv2GdJumpWarning, scPv2GdCardHasTarget,
            scPv2OpenBlockLinkFromGrounding, scPv2GdClearJumpBanner,
            scPv2FilterOptions, scPv2HasFilterOptions,
            scPv2FiltersActive, scPv2SectionEmoji, scPv2CardsFor,
            scPv2ResetFilters, scPv2ToggleSection, scPv2StatusBadge,
            scPv2AllWarnings, scPv2Load, scPv2EnsureLoaded, scPv2OpenPair,
            // Pipeline V2 Block Link Preview («Связь блоков», read-only)
            scPv2LpVisible, scPv2LpLoading, scPv2LpError, scPv2LpResp,
            scPv2LpPairId, scPv2LpFilter, scPv2LpSelectedPage, scPv2LpSelectedLink,
            scPv2LpReport, scPv2LpSummary, scPv2LpPageLinks, scPv2LpNotFound,
            scPv2LpRespError, scPv2LpFilteredLinks, scPv2LpSelectedPageLink,
            scPv2LpSelectedLinkObj, scPv2LpPageOverlays,
            scPv2LpPageImageUrl, scPv2LpOverlayStyle, scPv2LpStatusColor,
            scPv2LpSelectLink, scPv2LpSelectPage, scPv2LpLoad, scPv2LpToggle,
            SC_PV2_LP_FILTERS,
            // Pipeline V2 Entity Alignment Preview («Сущности и маппинг», read-only)
            scPv2EaVisible, scPv2EaLoading, scPv2EaError, scPv2EaResp,
            scPv2EaPairId, scPv2EaFilter, scPv2EaSummary, scPv2EaPairs,
            scPv2EaUnpaired, scPv2EaNotFound, scPv2EaRespError,
            scPv2EaShowUnpaired, scPv2EaShowPairs, scPv2EaFilteredPairs,
            scPv2EaClassMeta, scPv2EaConfPct, scPv2EaLoad, scPv2EaToggle,
            scPv2EaOpenBlockLink, SC_PV2_EA_FILTERS,
            // Pipeline V2 Link Validation (read-only, mark-only)
            scPv2LvResp, scPv2LvLoading, scPv2LvError, scPv2LvSummary,
            scPv2LvItems, scPv2LvAvailable, scPv2LvNotFound, scPv2LvRespError,
            scPv2LvDecisionMeta, scPv2LvConfPct, scPv2LvLoad, scPv2LvOpenBlockLink,
            // Pipeline V2 Exclusion Preview v2 (read-only, mark-only)
            scPv2XpResp, scPv2XpLoading, scPv2XpError,
            scPv2XpSummary, scPv2XpItems, scPv2XpAvailable, scPv2XpNotFound, scPv2XpRespError,
            scPv2XpClassMeta, scPv2XpConfPct, scPv2XpLoad, scPv2XpReset,
            // Pipeline V2 Skip Readiness (read-only, mark-only, observe)
            scPv2SrResp, scPv2SrLoading, scPv2SrError,
            scPv2SrSummary, scPv2SrItems, scPv2SrAvailable, scPv2SrNotFound, scPv2SrRespError,
            scPv2SrReadinessMeta, scPv2SrConfPct, scPv2SrLoad, scPv2SrReset,
            SC_PV2_SR_READINESS_META,
            // Pipeline V2 Controlled Enforce Preflight (read-only / observe-only)
            scPv2CeResp, scPv2CeLoading, scPv2CeError,
            scPv2CeSummary, scPv2CeGuards, scPv2CeRuntimeRoot,
            scPv2CeFatalBlocks, scPv2CeBlockedItems, scPv2CeEligibleItems,
            scPv2CeReportStatus, scPv2CeAvailable, scPv2CeNotFound, scPv2CeRespError,
            scPv2CeStatusMeta, scPv2CeBlockMeta, scPv2CeLoad, scPv2CeReset,
            SC_PV2_CE_STATUS_META, SC_PV2_CE_BLOCK_META,
            // Pipeline V2 Controlled Enforce Dry-run (read-only / observe-only)
            scPv2CdrResp, scPv2CdrLoading, scPv2CdrError,
            scPv2CdrSummary, scPv2CdrTransitions, scPv2CdrItems,
            scPv2CdrReportStatus, scPv2CdrAvailable, scPv2CdrNotFound, scPv2CdrRespError,
            scPv2CdrLoad, scPv2CdrReset,
            // Pipeline V2 Controlled Enforce State + Selection Observe (read-only)
            scPv2CesSection, scPv2CesAvailable,
            scPv2CesoSection, scPv2CesoAvailable,
            // Pipeline V2 Enrichment Selection Observe (read-only observe-plan)
            scPv2EsoSection, scPv2EsoAvailable, scPv2EsoRedundant,
            // Pipeline V2 Controlled Enforce State deactivate (rollback, write)
            SC_PV2_CDS_PHRASE, scPv2CdsOpen, scPv2CdsConfirmText, scPv2CdsComment,
            scPv2CdsRunId, scPv2CdsBusy, scPv2CdsError, scPv2CdsDone, scPv2CdsConfirmOk,
            scPv2CdsBegin, scPv2CdsCancel, scPv2CdsSubmit,
            // Pipeline V2 Manual Entity Mapping (write-слой)
            SC_PV2_EA_DECISIONS, scPv2EaDrafts, scPv2EaSaving, scPv2EaSaveErr,
            scPv2EaSaveHint, scPv2EaPairKey, scPv2EaUnpairedKey, scPv2EaDraft,
            scPv2EaDecisionMeta, scPv2EaManualMeta, scPv2EaSavePair,
            scPv2EaUnpairedCounterparts, scPv2EaSaveUnpaired,
            // Saved canonical config (one-click apply/save)
            scSavedConfig, scSavedConfigSaving, scSavedConfigMsg,
            scLoadSavedConfig, scApplySavedConfig, scSaveCurrentAsCanonical,
            // Каноничная конфигурация v2 (одна на объект, session-aware)
            scCanonicalConfig, scCanonicalStale,
            scLoadCanonicalConfig, scSaveSessionAsCanonical, scTryOpenCanonical,
            // Drag-and-drop pair reorder
            scPairDragFromIdx, scPairDragOverIdx, scPairOrderSaving, scPairOrderError,
            scOnPairDragStart, scOnPairDragOver, scOnPairDragLeave, scOnPairDrop, scOnPairDragEnd,
            // Auto-objects (выбор объекта вместо ручного ввода путей)
            scObjects, scObjectsRoots, scObjectsLoading, scObjectsError,
            scSelectedObjectId, scSelectedStageA, scSelectedStageB,
            scSelectedObject, scLoadObjects, scApplySelectedObject,
            scScanning, scLinking, scError, scWarnings,
            scSession, scSessions, scSessionsListOpen,
            scAutoLoadInfo, scAutoLoading,
            scActivePair, scPairData, scCurrentPage,
            scCanvasRefs, scCanvasNat, scSelectedLeft, scSelectedRight,
            scSelectedSlotLeft, scSelectedSlotRight,
            scTextLLMDiff, scTextLLMConfig,
            scLoadTextLLMDiff, scLoadTextLLMConfig,
            // Session-level batch preflight + job
            scTextLLMBatchPreflight, scTextLLMBatchLoading, scTextLLMBatchOpen,
            scTextLLMBatchError, scTextLLMBatchForce, scTextLLMBatchJob, scTextLLMBatchPolling,
            scOpenBatchTextLLM, scRefreshBatchPreflight, scCloseBatchPreflight,
            scConfirmBatchRun, scPollTextLLMJob, scCancelTextLLMJob,
            // Session-level flat-таблица текстовых изменений
            scTextLLMFlat, scTextLLMFlatLoading, scTextLLMFlatError, scLoadTextLLMFlat,
            scTextFlatFilterPair, scTextFlatFilterType, scTextFlatFilterCategory,
            scTextFlatFilterSeverity, scTextFlatFilterHumanReview, scTextFlatSearch,
            scTextFlatPairOptions, scTextFlatTypeOptions, scTextFlatCategoryOptions,
            scTextFlatItemsFiltered,
            // MD enrichment (Qwen image descriptions for enriched MD)
            scMdEnrichmentSummary, scMdEnrichmentLoading, scMdEnrichmentRunning,
            scMdEnrichmentError, scMdEnrichmentConfirmOpen,
            scMdEnrichmentJob, scMdEnrichmentJobPolling, scMdEnrichmentJobTimedOut,
            scLoadMdEnrichmentSummary, scMdEnrichmentDryRun,
            scMdEnrichmentRequestConfirm, scMdEnrichmentRunModel,
            scPollMdEnrichmentJob, scCancelMdEnrichmentJob, scRefreshMdEnrichmentJob,
            // Stage 1: «Распознать графику» (session-level Qwen enrichment job)
            scRecogJob, scRecogPolling, scRecogStarting, scRecogError,
            scRecogRestoreActive, scRecogPairStatus, scRecogPairBadge,
            scRecogPairBlocks, scOpusPairBadge,
            scPairCompareStatus, scLoadPairCompareStatuses,
            scExpertPerPair, scPairExpertBadge, scLoadExpertPerPair,
            scRecogElapsedLabel, scFormatDuration, scRecogPairProgress,
            // Stage 1: «Проанализировать и сравнить» (session-level Opus batch)
            scOpusJob, scOpusPolling, scOpusStarting, scOpusError,
            scOpusPreflight, scOpusPreflightLoading,
            scOpusRestoreActive, scOpusLoadPreflight,
            scOpusElapsedLabel, scOpusCurrentPairLabel, scOpusStartTitle,
            scOpusFallbackLabel,
            // Per-pair Opus fallback (evidence_first_s2_fallback) для too_large
            scOpusFallbackByPair, scOpusFallbackStarting, scOpusRunFallbackForPair,
            // Per-pair analysis mode
            scAnalysisMode, scAnalysisModeSaving, scAnalysisModeError,
            scLoadAnalysisMode, scSetAnalysisMode, scToggleAnalysisMode,
            // Unified analysis (Qwen enrichment + Opus comparison) — primary UX
            scUnifiedConfig, scUnifiedPairStatus, scUnifiedPairLoading,
            scUnifiedFlat, scUnifiedFlatLoading, scUnifiedFlatError,
            scUnifiedShowAllPairs, scUnifiedFlatScopePairId, scUnifiedToggleShowAllPairs,
            scUnifiedPreflight, scUnifiedPreflightScope, scUnifiedPreflightOpen,
            scUnifiedPreflightLoading, scUnifiedPreflightError,
            scUnifiedForceEnrichment, scUnifiedForceCompare,
            scUnifiedRunning, scUnifiedJob, scUnifiedJobPolling, scUnifiedError,
            scUnifiedFilterPair, scUnifiedFilterSourceLayer, scUnifiedFilterType,
            scUnifiedFilterCategory, scUnifiedFilterSeverity,
            scUnifiedFilterHumanReview, scUnifiedSearch,
            scUnifiedSortField, scUnifiedSortDir, scUnifiedToggleSort, scUnifiedSortIndicator,
            scUnifiedExportXlsxUrl,
            scUnifiedPairOptions, scUnifiedSourceLayerOptions, scUnifiedTypeOptions,
            scUnifiedCategoryOptions, scUnifiedItemsFiltered, scUnifiedItemsSorted,
            // V2 режим вкладки «Расхождения» (pair-scoped ручная верификация)
            scV2View, scV2Data, scV2Loading, scV2Error, scV2SaveBusy,
            scV2Selected, scV2Filters, scV2StatusOptions,
            scV2ShowFormal, scV2ToggleShowFormal,
            scV2ProfileBadge, scV2DenseWarning, scV2DowngradeBlocked,
            scSetV2View, scLoadV2Changes, scV2FilteredItems, scV2SelectedIds,
            scV2AllSelected, scV2ToggleAll, scV2ToggleOne, scV2SummaryCards,
            scV2ReviewProgress,
            scV2SourceLabel, scV2ExportXlsxUrl, scV2SetStatus, scV2SaveComment,
            scV2ImpactLabel,
            scV2IsExcludedClass, scV2ImpactBadgeStyle, scV2ImpactClassOptions,
            scV2BulkStatus, scV2Goto,
            // Expert review для расхождений (per-session)
            scExpertReviewMode, scExpertDecisions, scExpertReviewSaving,
            scToggleExpertReview, scLoadExpertDecisions,
            scSetExpertDecision, scSetExpertReason,
            scGetExpertDecision, scResolvedDecision, scGetExpertReason, scExpertReviewSummary,
            scExpertItemFlags, scV2TransferBusy, scV2TransferReviews,
            scSubmitExpertReview,
            scLoadUnifiedConfig, scLoadUnifiedPairStatus, scLoadUnifiedFlat,
            scOpenUnifiedPairPreflight, scOpenUnifiedSessionPreflight, scCloseUnifiedPreflight,
            scRunUnifiedPair, scRunUnifiedSession, scPollUnifiedJob, scCancelUnifiedJob,
            scUnifiedSourceLabel, scUnifiedLines, scGotoUnifiedChange,
            scDirectionLabel, scCostDirectionLabel, scCostDirectionStyle,
            scSwitchDiffSubtab, scGotoTextChange,
            scHumanizeDuration,
            scTextLLMTypeLabel, scTextLLMCategoryLabel, scTextLLMSeverityLabel, scTextLLMStatusLabel,
            scFindingTypeLabel,
            scGraphicSummary, scGraphicPreview,
            scGraphicDiffRunning,
            scPaneRefs, scSlotRefs, scZoom, scSyncScroll,
            scVisibleSlot, scVisibleSlotLeft, scVisibleSlotRight,
            scRenderBufferBefore, scRenderBufferAfter,
            scAlignment, scAlignmentItems,
            scAlignmentActionRunning, scAlignmentActionError,
            scAlignmentGoToSlot,
            scCurrentSlotForSide, scCanInsertBlankSide, scCanMovePageSide,
            scInsertBlankSide, scMovePageSide,
            scUnmatched, scIsPdfUsedRight,
            scMatchPairDialogOpen, scMatchPairTargetPair, scMatchPairChoiceRight,
            // Inline-match (popover на клик по названию правого PDF)
            scInlineMatchPairId, scInlineMatchChoice, scInlineMatchFilter,
            scInlineMatchSaving, scInlineMatchError,
            scOpenInlineMatch, scCloseInlineMatch, scInlineMatchOptions, scInlineMatchConfirm,
            scMatchPairError, scMatchPairSaving,
            scConfirmAllRunning, scConfirmAllError, scConfirmAllMaybe,
            scCreatePairDialogOpen, scCreatePairLeft, scCreatePairRight,
            scCreatePairError, scCreatePairSaving,
            scLoadUnmatched, scOpenMatchPairDialog, scCloseMatchPairDialog,
            scSavePairMatch, scOpenCreatePairDialog, scSaveCreatePair,
            scDeletePair, scStaleLinksCount,
            scPairs, scPairsCounts, scAllLinksForGraphic,
            scStatusLabel, scDiffTypeLabel,
            scScanFolders, scOpenProject, scLoadSessionsList, scFetchSessionsList, scLoadSession,
            scOpenPair, scLoadPairData, scLoadAlignment,
            scQOSelected, scQOJob, scQOConfirm, scQORunning, scQOPreflighting, scQOSelectedCount, scQOAllSelected,
            scQOClearBeforeRun, scQOMode, scQOClearing, scQOClearAnalysis, scQOStartOpusOnly,
            scQOToggleAll, scQOPairLabel, scQOPairBadge, scQOOpenConfirm, scQOProcessPair,
            scQOStartConfirmed, scQOStart, scQOCancel,
            scPv2RunByPair, scPv2RunModal, scPv2RunState, scPv2RunBtnLabel,
            scPv2RunBtnTitle, scPv2RunErrorFor, scPv2RunOpenModal, scPv2RunSubmit,
            scQODetailsOpen, scQOElapsedMs, scQOEtaSec, scQOItemFor, scQOItemLaneLabel,
            scQOPairTimings, scQOLoadPairTimings,
            scQOLaneCell, scQOLaneColor, scQOItemTotalLabel, scQOCurrentBlock, scQOBlocksOverall,
            scFailedPopoverPairId, scFailedBlocks, scFailedBlocksLoading, scFailedBlocksError,
            scToggleFailedPopover, scLoadFailedBlocks, scGotoFailedBlock, scFocusBlock,
            scPageImageUrl, scOnImageLoad, scOnPageImageLoad,
            scShowMd, scMdView, scMdViewLoading, scMdViewError, scMdRenderMode, scMdPaneRefs,
            scToggleMdView, scLoadEnrichedMd, scMdHighlightHtml, scMdRenderHtml, scOnMdPaneScroll,
            scSlotBlocks, scBlankPageStyle,
            scBlockOverlayStyle, scBlockOverlayClass, scIsBlockLinked, scSelectBlock,
            scBlockLinkInfo, scLinkColor, scLinkVisualIndex, scSelectLinkedBlock,
            scActiveLinkKey, scActiveLink, scActiveLinkInfo,
            scDeleteActiveLink, scClearStaleLinks,
            scOnPaneScroll, scOnPaneWheel, scOnPanePanStart, scZoomBy, scZoomReset,
            scSetSlotRef, scIsSlotRendered, scSlotContainerStyle, scSlotPlaceholderStyle,
            scCreateLink, scDeleteLink, scRunAutoLink,
            // Сопоставление листов по штампам
            scStampProposals, scStampLoading, scStampApplying, scStampError,
            scStampSelected, scStampRowKey, scStampToggleRow, scStampMatchedRows,
            scStampAllRows, scStampSelectableRows, scStampIsSelectable,
            scStampSelectedCount, scSuggestByStamp, scApplyStampProposals,
            scAutoMatchApplyLoading, scAutoMatchApplyResult, scAutoMatchApplyError, scAutoMatchApplySheets,
            scCloseStampProposals, scStampUseLlm,
            scStampTypeLabel, scStampRiskLabel, scStampTypeColor, scStampRowTitle,
            scStampDisplayName, scStampNameIsDerived,
            // Пакетное авто-сопоставление листов (раздел «1. Загрузка документации»)
            scAutoMatchJob, scAutoMatchStarting, scAutoMatchError, scAutoMatchUseLlm,
            scAutoMatchOverwrite, scAutoMatchRunning, scAutoMatchStart,
            scAutoMatchCancel, scAutoMatchLoadLast,
            scAutoMatchAsk, scAutoMatchOpenDialog, scAutoMatchCloseDialog, scAutoMatchConfirm,
            // Pair config templates
            scTemplateSaving, scTemplateLastSaveMsg, scTemplateError,
            scSavePairTemplate,
            scLoadGraphicSummary,
            scFindGraphicDiff, scPrepareGraphicDiff, scRunGraphicDiff,
            // ─── Report tab (read-only сводка согласованных) ───
            scOpenReportTab, scReportLoad,
            scReportLoading, scReportError,
            scReportApprovedCountFor, scReportApprovedFor,
            scReportTotalApproved, scReportPairsWithApprovedCount,
            scReportExpandedPairs, scReportIsPairExpanded, scReportTogglePair,
            scReportPairLoading, scReportPairLoaded, scReportExportUrl,
            scReportPrefetching, scReportPrefetchDone, scReportPrefetchTotal,
        };
    }
});

app.mount('#app');
