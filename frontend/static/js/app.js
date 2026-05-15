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
        const currentProjectId = ref(null);
        const currentProject = ref(null);
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
        // Список файлов активной версии (для панели "Версии" / upload).
        const versionFiles = ref([]);
        // Прогресс / последняя ошибка загрузки файлов в версию.
        const versionUploading = ref(false);
        const versionUploadError = ref('');
        // Состояние modal-а "Создать версию".
        const showCreateVersionModal = ref(false);
        const newVersionComment = ref('');
        const versionsPanelOpen = ref(false);  // боковая панель/блок в info-вкладке

        // ─── Контроль ранее согласованных замечаний (migrated findings) ───
        // Отчёт пишется backend'ом в _versions/v{N}/_output/migrated_findings_report.json.
        // На фронте — только чтение/запуск, без редактирования содержимого.
        const migratedFindingsReport = ref(null);
        const migratedFindingsReportLoading = ref(false);
        const migratedFindingsCheckRunning = ref(false);
        const migratedFindingsError = ref('');
        const migratedFindingsPanelOpen = ref(false);

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
        const sidebarSectionsOpen = ref(true);
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
        const highlightedFindingId = ref(null);  // ID замечания для подсветки на блоке
        const allHighlightsVisible = ref(true);           // глобальный вкл/выкл подсветок
        const hiddenHighlightFindings = ref(new Set());   // finding_id с выключенной подсветкой

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
        const kbStats = ref({ rejected: 0, accepted: 0, customer_confirmed: 0, total: 0 });
        const kbLoading = ref(false);
        const kbSearch = ref('');
        const kbSectionFilter = ref('');
        const missingNorms = ref([]);
        const missingNormsStats = ref({ pending: 0, added: 0, dismissed: 0, total: 0 });
        const missingNormsFilter = ref('pending'); // 'pending' | 'added' | 'dismissed' | ''
        const kbPatterns = ref([]);
        const kbPatternsLoading = ref(false);
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

        // Текущая фаза «размышления модели»: merge | critic | corrector | done | ''
        const findingStage = ref({});     // {projectId: 'merge'|...}
        // Быстрый индекс finding_id → entry в projectLogs[pid] для обновления статуса
        const findingIndex = ref({});     // {projectId: {finding_id: entry}}

        // logEntries — computed, показывает логи текущего проекта
        const logEntries = computed(() => {
            const pid = logProjectId.value;
            return pid ? (projectLogs.value[pid] || []) : [];
        });

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

        async function fetchPaidCost() {
            try {
                const data = await api('/usage/paid-cost');
                paidCost.value = data;
            } catch (e) {
                console.error('Failed to fetch paid cost:', e);
            }
        }

        async function resetPaidCost() {
            if (!confirm('Обнулить счётчик расходов? Общая сумма за всё время сохранится.')) return;
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
        const GEMMA_STAGE_UI_LABEL = 'Gemma OCR enrichment / предварительное распознавание чертежей';

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

        // ─── Prepare-data queue (Gemma enrichment) ───────────────────────
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
                'gemma_enrichment': GEMMA_STAGE_UI_LABEL,
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
            const resp = await fetch(url);
            if (!resp.ok) {
                const err = await resp.json().catch(() => ({}));
                throw new Error(err.detail || `API error: ${resp.status}`);
            }
            return resp.json();
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
                loadKnowledgeBase();
                loadKBStats();
            } else if (hash === '/queue') {
                currentView.value = 'queue';
                connectGlobalWS();
                refreshBatchQueue();
                fetchPrepareQueue();   // подгрузить prepare-data queue
                refreshProjects();  // для списка добавления
            } else if (hash === '/lms') {
                currentView.value = 'lms';
                connectGlobalWS();
                lmsRefresh();
            } else if (hash === '/model-control') {
                currentView.value = 'model-control';
                connectGlobalWS();
            } else if (hash === '/critic-v2-ui') {
                // Experimental offline view. Does NOT touch production pipeline.
                currentView.value = 'critic-v2-ui';
                connectGlobalWS();
            } else if (hash === '/') {
                currentView.value = 'dashboard';
                sidebarFilterSection.value = null;
                connectGlobalWS();  // Вернуться на global WS
                refreshProjects();
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
            } else if (hash.match(/^\/project\/(.+)\/discussions\/([^/]+)$/)) {
                const m = hash.match(/^\/project\/(.+)\/discussions\/([^/]+)$/);
                const id = decodeURIComponent(m[1]);
                const itemId = decodeURIComponent(m[2]);
                currentView.value = 'discussions';
                currentProjectId.value = id;
                // Определить тип по префиксу ID
                discussionTab.value = itemId.startsWith('OPT') ? 'optimization' : 'finding';
                connectGlobalWS();
                loadProject(id);
                loadDiscussionModels();
                loadDiscussionItems(id, discussionTab.value).then(() => openDiscussion(id, itemId));
            } else if (hash.match(/^\/project\/(.+)\/discussions$/)) {
                const id = decodeURIComponent(hash.match(/^\/project\/(.+)\/discussions$/)[1]);
                currentView.value = 'discussions';
                currentProjectId.value = id;
                activeDiscussion.value = null;
                discussionMessages.value = [];
                connectGlobalWS();
                loadProject(id);
                loadDiscussionModels();
                loadDiscussionItems(id, discussionTab.value);
            } else if (hash.match(/^\/project\/(.+)\/document$/)) {
                const id = decodeURIComponent(hash.match(/^\/project\/(.+)\/document$/)[1]);
                currentView.value = 'document';
                currentProjectId.value = id;
                connectGlobalWS();
                loadProject(id);
                loadDocument(id);
            } else if (hash.match(/^\/project\/(.+)\/prompts$/)) {
                const id = decodeURIComponent(hash.match(/^\/project\/(.+)\/prompts$/)[1]);
                currentView.value = 'prompts';
                currentProjectId.value = id;
                promptsProjectId.value = id;
                activePromptTab.value = 0;
                connectGlobalWS();
                loadProject(id);
                loadPromptDisciplines().then(() => {
                    const proj = projects.value.find(p => p.name === id || p.project_id === id);
                    const section = proj?.section || 'EOM';
                    promptsDiscipline.value = section;
                    loadTemplates(section);
                });
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
                // Загружаем историю логов из файла (если ещё не загружена)
                if (!projectLogs.value[id] || projectLogs.value[id].length === 0) {
                    loadProjectLog(id);
                }
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
        const prepareQueue = ref(null);  // Gemma enrichment queue (см. prepare_service.py)
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

        // Пакетное применение merge: каждая (source → target) пара
        // выполняется отдельным запросом, ошибки одной не останавливают другие.
        async function applyMergeAllAsVersion() {
            const pairs = [];
            for (const src of editProjectsSelected.value) {
                const tgt = editProjectsMergeMap.value[src.project_id];
                if (tgt) pairs.push({ source: src, targetId: tgt });
            }
            if (pairs.length === 0) return;
            if (!confirm(
                `Привязать ${pairs.length} проект(ов) как версии существующих?\n` +
                `Исходные карточки будут удалены. V1 каждого target не изменится.`
            )) return;
            editProjectsLoading.value = true;
            const errors = [];
            const okList = [];
            try {
                for (const { source, targetId } of pairs) {
                    try {
                        const resp = await fetch(
                            '/api/projects/versions/from-project',
                            {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({
                                    target_project_id: targetId,
                                    source_project_id: source.project_id,
                                    comment: 'Привязано из окна Изменить выбранные проекты',
                                    source: 'edit_projects_modal',
                                    delete_source: true,
                                }),
                            },
                        );
                        if (!resp.ok) {
                            const err = await resp.json().catch(() => ({}));
                            throw new Error(err.detail || `HTTP ${resp.status}`);
                        }
                        const data = await resp.json();
                        const verLabel = (data.version && data.version.label) || 'V?';
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
            text_analysis: "01 Текст",
            block_batch: "02 Блоки",
            findings_merge: "03 Свод",
            findings_critic: "C Critic",
            findings_corrector: "F Fix",
            norm_verify: "04 Нормы",
            norm_fix: "04b Пересмотр",
            optimization: "05 Оптимизация",
            optimization_critic: "C OPT Critic",
            optimization_corrector: "F OPT Fix",
        };

        const stageModelRestrictions = ref({});
        const stageModelHints = ref({});
        const modelPresets = {
            findings_only: {
                label: "Production Gemma+GPT5.4",
                hint: "Production: Markdown → Gemma OCR enrichment → Stage 01 → Stage 02 findings-only single-block на GPT-5.4.",
                config: {
                    text_analysis:          "claude-opus-4-7",
                    block_batch:            "openai/gpt-5.4",
                    findings_merge:         "claude-opus-4-7",
                    findings_critic:        "claude-sonnet-4-6",
                    findings_corrector:     "claude-sonnet-4-6",
                    norm_verify:            "claude-sonnet-4-6",
                    norm_fix:               "claude-sonnet-4-6",
                    optimization:           "claude-opus-4-7",
                    optimization_critic:    "claude-sonnet-4-6",
                    optimization_corrector: "claude-sonnet-4-6",
                },
                batchModes: { block_batch: "findings_only_gemma_pair" },
            },
        };
        const activePreset = ref(null);
        const activePresetHint = computed(() => {
            const key = activePreset.value;
            return key ? (modelPresets[key]?.hint || '') : '';
        });
        const stageBatchModes = ref({});  // { block_batch: "findings_only_gemma_pair" }
        const stageBatchModeChoices = ref({});

        // Production Stage 02: Gemma enrichment is separate; block analysis uses GPT-5.4.
        const findingsOnlyCompatibleBlockModels = [
            'openai/gpt-5.4',
        ];

        function isFindingsOnlyMode() {
            return stageBatchModes.value?.block_batch === 'findings_only_gemma_pair';
        }

        function getMatchingPresetKey(config, batchModes) {
            return Object.entries(modelPresets).find(([, preset]) => {
                const cfgMatch = Object.entries(preset.config).every(([stageKey, modelId]) => config?.[stageKey] === modelId);
                if (!cfgMatch) return false;
                const presetModes = preset.batchModes || {};
                return Object.entries(presetModes).every(([stage, mode]) => (batchModes?.[stage] || 'findings_only_gemma_pair') === mode);
            })?.[0] || null;
        }

        function applyPreset(presetKey) {
            const preset = modelPresets[presetKey];
            if (!preset) return;
            stageModelConfig.value = { ...preset.config };
            stageBatchModes.value = { ...(preset.batchModes || { block_batch: 'findings_only_gemma_pair' }) };
            activePreset.value = presetKey;
        }

        function isModelAllowed(stageKey, modelId) {
            const r = stageModelRestrictions.value[stageKey];
            if (r && !r.includes(modelId)) return false;
            // findings_only_gemma_pair: production block_batch is GPT-5.4 only.
            if (stageKey === 'block_batch' && isFindingsOnlyMode()) {
                return findingsOnlyCompatibleBlockModels.includes(modelId);
            }
            return true;
        }

        function modelInputType(stageKey, modelId) {
            return 'radio';
        }

        function isStageModelChecked(stageKey, modelId) {
            return stageModelConfig.value[stageKey] === modelId;
        }

        function selectStageModel(stageKey, modelId, event) {
            stageModelConfig.value[stageKey] = modelId;
        }

        async function loadStageModels() {
            try {
                stageModelSaveError.value = '';
                const data = await api('/audit/model/stages');
                stageModelConfig.value = data.stages || {};
                availableModels.value = data.available_models || [];
                stageModelRestrictions.value = data.restrictions || {};
                stageModelHints.value = data.hints || {};
                if (data.config_errors && Object.keys(data.config_errors).length > 0) {
                    stageModelSaveError.value = `Текущая конфигурация моделей невалидна: ${formatRejected(data.config_errors)}`;
                }
                // Параллельно подгружаем batch-modes (production: findings_only_gemma_pair)
                try {
                    const bm = await api('/audit/model/batch-modes');
                    stageBatchModes.value = bm.modes || { block_batch: 'findings_only_gemma_pair' };
                    stageBatchModeChoices.value = bm.choices || {};
                } catch (_) {
                    stageBatchModes.value = { block_batch: 'findings_only_gemma_pair' };
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
                'norm_verify': 'Верификация норм',
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

        async function startSmartAudit(projectId) {
            try {
                auditRunning.value = true;
                await apiPost(`/audit/${encodeURIComponent(projectId)}/smart-audit`);
                _afterAuditStart(projectId);
            } catch (e) { _friendlyAuditError(e); auditRunning.value = false; }
        }

        async function startAudit(projectId) {
            // Показать модальник с выбором моделей перед запуском
            openModelConfig(projectId);
        }

        async function startAuditDirect(projectId) {
            try {
                auditRunning.value = true;
                await apiPost(`/audit/${encodeURIComponent(projectId)}/full-audit`);
                _afterAuditStart(projectId);
            } catch (e) { _friendlyAuditError(e); auditRunning.value = false; }
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
            try {
                auditRunning.value = true;
                await apiPost(`/audit/${encodeURIComponent(projectId)}/resume`);
                _afterAuditStart(projectId);
            } catch (e) { _friendlyAuditError(e); auditRunning.value = false; }
        }

        async function resumeToQueue(projectId) {
            try {
                const resp = await fetch('/api/audit/batch/add-resume', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ project_id: projectId }),
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
        };

        const stageLabelMap = {
            'prepare': 'Кроп блоков',
            'gemma_enrichment': GEMMA_STAGE_UI_LABEL,
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
            const baseAllowed = status === 'done' || status === 'error' || status === 'skipped' || status === 'pending' || status === 'partial' || status === 'interrupted';
            if (!baseAllowed) return false;

            const pipeline = currentProject.value.pipeline || {};
            const ready = (key) => pipeline[key] === 'done' || pipeline[key] === 'partial';
            // Для downstream-этапов Gemma считается ОК если: done/partial, migration_required,
            // или blocks_analysis уже done (старые проекты без Gemma-прогона)
            const gemmaOk = () => ready('gemma_enrichment') || pipeline['gemma_enrichment'] === 'migration_required' || ready('blocks_analysis');
            if (pipelineKey === 'gemma_enrichment') {
                return ready('crop_blocks');
            }
            if (pipelineKey === 'blocks_analysis') {
                return ready('gemma_enrichment') && ready('text_analysis');
            }
            if ([
                'findings', 'findings_critic', 'findings_corrector',
                'norms_verified', 'optimization', 'optimization_critic',
                'optimization_corrector', 'excel',
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
            const gemmaOk = () => ready('gemma_enrichment') || pipeline['gemma_enrichment'] === 'migration_required' || ready('blocks_analysis');
            if (stage === 'gemma_enrichment') {
                return ready('crop_blocks');
            }
            if (stage === 'block_analysis') {
                return ready('gemma_enrichment') && ready('text_analysis');
            }
            if ([
                'findings_merge', 'findings_critic', 'findings_review',
                'findings_corrector', 'norm_verify', 'optimization',
                'optimization_critic', 'optimization_corrector', 'excel',
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
                'crop_blocks': 'Кроп блоков', 'gemma_enrichment': GEMMA_STAGE_UI_LABEL,
                'text_analysis': 'Анализ текста',
                'block_analysis': 'Анализ блоков', 'findings_merge': 'Свод замечаний',
                'findings_critic': 'Critic замечаний', 'findings_review': 'Critic замечаний',
                'findings_corrector': 'Corrector замечаний',
                'norm_verify': 'Верификация норм', 'optimization': 'Оптимизация',
                'optimization_critic': 'Critic оптимизации', 'optimization_corrector': 'Corrector оптимизации',
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
            try {
                auditRunning.value = true;
                if (stage === 'optimization') {
                    await apiPost(`/optimization/${encodeURIComponent(projectId)}/run`);
                } else {
                    await apiPost(`/audit/${encodeURIComponent(projectId)}/retry/${stage}`);
                }
                _afterAuditStart(projectId);
            } catch (e) { _friendlyAuditError(e); auditRunning.value = false; }
        }

        async function retryStageToQueue() {
            const { projectId, stage, mode } = retryDialog.value;
            retryDialog.value.show = false;
            try {
                let resp;
                if (mode === 'resume') {
                    // Запустить с этапа + все последующие
                    resp = await fetch('/api/audit/batch/add-retry', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ project_id: projectId, stage: stage }),
                    });
                } else {
                    // Только один этап — прямой retry
                    resp = await fetch(`/api/audit/${encodeURIComponent(projectId)}/retry/${stage}`, {
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
        const objectsList = ref([]);
        const currentObjectId = ref(null);
        const showObjectPicker = ref(false);
        const showAddObjectModal = ref(false);
        const newObjectName = ref('');

        async function loadObjects() {
            try {
                const data = await api('/objects');
                objectsList.value = data.objects || [];
                currentObjectId.value = data.current_id;
            } catch (e) {
                console.error('Failed to load objects:', e);
            }
        }

        async function switchObject(id) {
            try {
                await apiPost('/objects/switch', { id });
                currentObjectId.value = id;
                const obj = objectsList.value.find(o => o.id === id);
                if (obj) objectName.value = obj.name;
                showObjectPicker.value = false;
                await Promise.all([refreshProjects(), loadProjectGroups()]);
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

        const filteredSectionProjects = computed(() => {
            if (!sidebarFilterSection.value) return [];
            return projects.value.filter(p => p.section === sidebarFilterSection.value);
        });

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

        // ─── Excel по одному проекту ───
        const projectExcelLoading = ref(false);

        async function exportProjectExcel(projectId) {
            if (!projectId) return;
            projectExcelLoading.value = true;
            try {
                const resp = await fetch('/api/export/excel/section', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        section: '',
                        project_ids: [projectId],
                    }),
                });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || resp.statusText);
                }
                const data = await resp.json();
                window.open('/api/export/download/' + encodeURIComponent(data.file), '_blank');
            } catch (e) {
                alert('Ошибка генерации Excel: ' + e.message);
            } finally {
                projectExcelLoading.value = false;
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

        // Нормализация имени для матчинга candidate ↔ существующий проект.
        // Убираем расширение, "(1)", "_document", "Изм.1", лишние пробелы,
        // приводим к нижнему регистру.
        function normalizeProjectName(name) {
            if (!name) return '';
            let s = String(name).toLowerCase();
            s = s.replace(/\.pdf$/, '');
            s = s.replace(/\.md$/, '');
            s = s.replace(/_document$/, '');
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

        async function loadProject(id, forceRefresh) {
            currentProjectId.value = id;
            // Кеш ключуется по (id, activeVersionId), чтобы V1/V2 одного проекта
            // не наступали друг на друга.
            const cacheKey = activeVersionId.value
                ? `${id}::${activeVersionId.value}`
                : id;
            if (!forceRefresh) {
                const cached = _cacheGet('project', cacheKey);
                if (cached) { currentProject.value = cached; return; }
            }
            try {
                // Загружаем список версий параллельно — он нужен и для UI,
                // и для определения latest, если activeVersionId ещё не задан.
                await loadProjectVersions(id);
                const project = await api(`/projects/${encodeURIComponent(id)}`);
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
                console.error('Failed to load project:', e);
                currentProject.value = null;
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

        async function createNewVersion() {
            if (!currentProjectId.value) return;
            const comment = (newVersionComment.value || '').trim();
            try {
                const data = await apiPost(
                    `/projects/${encodeURIComponent(currentProjectId.value)}/versions`,
                    { comment, source: 'manual', status: 'new' },
                    { withVersion: false },
                );
                const newId = data.version && data.version.version_id;
                newVersionComment.value = '';
                showCreateVersionModal.value = false;
                // Обновляем список и активируем новую версию
                await loadProjectVersions(currentProjectId.value);
                if (newId) selectVersion(newId);
                versionsPanelOpen.value = true;
                return data;
            } catch (e) {
                alert('Не удалось создать версию: ' + e.message);
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

        async function runMigratedFindingsCheck() {
            const pid = currentProjectId.value;
            const vid = activeVersionId.value;
            if (!pid || !vid) return null;
            const guard = VAPI ? VAPI.canRunMigratedCheck(vid) : { ok: vid !== 'v1', reason: '' };
            if (!guard.ok) {
                migratedFindingsError.value = guard.reason || 'Контроль недоступен.';
                return null;
            }
            migratedFindingsCheckRunning.value = true;
            migratedFindingsError.value = '';
            try {
                const url = VAPI
                    ? VAPI.migratedFindingsCheckUrl(pid, vid)
                    : `/api/projects/${encodeURIComponent(pid)}/versions/${encodeURIComponent(vid)}/migrated-findings/check`;
                const resp = await fetch(url, { method: 'POST' });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    migratedFindingsError.value = VAPI
                        ? VAPI.describeMigratedCheckError(resp.status, err.detail || '')
                        : (err.detail || `Ошибка ${resp.status}`);
                    return null;
                }
                const data = await resp.json();
                // Backend возвращает {status, source_version_id, reason, report}.
                // В UI нам нужен сам report (с items + counts).
                const report = (data && data.report) ? data.report : data;
                migratedFindingsReport.value = report || null;
                migratedFindingsPanelOpen.value = true;

                // still_relevant мог быть добавлен в 03_findings.json — обновляем
                // список findings и статус проекта, чтобы пользователь увидел
                // migrated-замечания и пересчитанные счётчики.
                _cacheInvalidate('findings');
                _cacheInvalidate('project');
                if (currentView.value === 'findings') {
                    loadFindings(pid);
                }
                loadProject(pid, true);

                const total = report && report.total_previous_accepted_findings != null
                    ? report.total_previous_accepted_findings
                    : 0;
                try {
                    alert(`Контроль завершён. Проверено ${total} ранее согласованных замечаний.`);
                } catch (_) {}
                return data;
            } catch (e) {
                migratedFindingsError.value = e.message || String(e);
                return null;
            } finally {
                migratedFindingsCheckRunning.value = false;
            }
        }

        // Компьютед-summary для UI (через VersionAPI helper).
        const migratedFindingsSummary = computed(() => {
            if (!VAPI) {
                return {
                    hasReport: !!migratedFindingsReport.value,
                    sourceVersionId: '',
                    total: 0, stillRelevant: 0, duplicate: 0,
                    resolved: 0, notVerifiable: 0, sourceMissing: 0,
                    checkedAt: '', itemsCount: 0,
                };
            }
            return VAPI.summarizeMigratedReport(migratedFindingsReport.value);
        });

        function migratedStatusLabel(status) {
            return VAPI ? VAPI.formatMigratedStatusLabel(status) : (status || '—');
        }
        function migratedStatusTone(status) {
            return VAPI ? VAPI.formatMigratedStatusTone(status) : 'muted';
        }
        function findingMigratedBadge(f) {
            return VAPI ? VAPI.findingMigratedBadge(f) : null;
        }

        // Доступен ли контроль для текущей активной версии.
        const canRunMigratedCheckNow = computed(() => {
            const vid = activeVersionId.value
                || (currentProject.value && currentProject.value.latest_version_id)
                || null;
            if (!VAPI) return { ok: vid && vid !== 'v1', reason: vid === 'v1' ? 'Только V2+' : '' };
            return VAPI.canRunMigratedCheck(vid);
        });

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

        async function loadFindings(id, forceRefresh) {
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
            findingsData.value = { ..._findingsAll.value, findings: items };
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

        const currentPageBlocks = computed(() => {
            if (!blockPages.value.length) return null;
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
            highlightedFindingId.value = null;
            allHighlightsVisible.value = true;
            hiddenHighlightFindings.value = new Set();
            resetBlockZoom();
        }

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
                            highlight_regions: (f.highlight_regions || []).filter(r => {
                                const rb = (r.block_id || '').replace(/^block_/, '');
                                return rb === bid || !r.block_id;
                            }),
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

        // ─── Highlight regions для текущего блока ───
        const currentBlockHighlights = computed(() => {
            if (!selectedBlock.value) return [];
            const bid = selectedBlock.value.block_id;
            const hidden = hiddenHighlightFindings.value;
            const findings = getBlockFindings(bid);
            const regions = [];
            for (const f of findings) {
                if (!f.highlight_regions || !f.highlight_regions.length) continue;
                if (hidden.has(f.id)) continue;
                for (const r of f.highlight_regions) {
                    regions.push({
                        ...r,
                        finding_id: f.id,
                        severity: f.severity,
                    });
                }
            }
            // Также из блочного анализа (G-замечания)
            const analysis = blockAnalysis.value[bid];
            if (analysis && analysis.findings) {
                for (const gf of analysis.findings) {
                    if (!gf.highlight_regions || !gf.highlight_regions.length) continue;
                    if (hidden.has(gf.id)) continue;
                    for (const r of gf.highlight_regions) {
                        regions.push({
                            ...r,
                            finding_id: gf.id,
                            severity: gf.severity,
                        });
                    }
                }
            }
            return regions;
        });

        function highlightFinding(findingId) {
            highlightedFindingId.value = highlightedFindingId.value === findingId ? null : findingId;
        }

        function toggleFindingHighlight(findingId) {
            const s = new Set(hiddenHighlightFindings.value);
            if (s.has(findingId)) s.delete(findingId); else s.add(findingId);
            hiddenHighlightFindings.value = s;
            // Обновить глобальный флаг
            allHighlightsVisible.value = s.size === 0;
        }

        function isFindingHighlightVisible(findingId) {
            return !hiddenHighlightFindings.value.has(findingId);
        }

        function toggleAllHighlights() {
            if (allHighlightsVisible.value) {
                // Выключить все — собрать все finding_id с регионами
                const allIds = new Set();
                if (selectedBlock.value) {
                    const bid = selectedBlock.value.block_id;
                    for (const f of getBlockFindings(bid)) {
                        if (f.highlight_regions && f.highlight_regions.length) allIds.add(f.id);
                    }
                    const analysis = blockAnalysis.value[bid];
                    if (analysis && analysis.findings) {
                        for (const gf of analysis.findings) {
                            if (gf.highlight_regions && gf.highlight_regions.length && gf.id) allIds.add(gf.id);
                        }
                    }
                }
                hiddenHighlightFindings.value = allIds;
                allHighlightsVisible.value = false;
            } else {
                // Включить все
                hiddenHighlightFindings.value = new Set();
                allHighlightsVisible.value = true;
            }
        }

        function severityColor(severity) {
            const s = (severity || '').toUpperCase();
            if (s.includes('КРИТИЧ')) return 'rgba(255, 60, 60, 0.25)';
            if (s.includes('ЭКОНОМ')) return 'rgba(255, 180, 30, 0.25)';
            if (s.includes('ЭКСПЛУАТ')) return 'rgba(100, 180, 255, 0.25)';
            if (s.includes('РЕКОМЕНД')) return 'rgba(100, 220, 140, 0.25)';
            return 'rgba(150, 150, 200, 0.25)';
        }

        function severityStroke(severity) {
            const s = (severity || '').toUpperCase();
            if (s.includes('КРИТИЧ')) return 'rgba(255, 60, 60, 0.8)';
            if (s.includes('ЭКОНОМ')) return 'rgba(255, 180, 30, 0.8)';
            if (s.includes('ЭКСПЛУАТ')) return 'rgba(100, 180, 255, 0.8)';
            if (s.includes('РЕКОМЕНД')) return 'rgba(100, 220, 140, 0.8)';
            return 'rgba(150, 150, 200, 0.8)';
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

        async function loadOptimization(id, forceRefresh) {
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
                currentProject.value = await api(`/projects/${id}`);
                _cacheSet('project', id, currentProject.value);
                const resp = await api(`/optimization/${id}`);
                if (resp.has_data) {
                    optimizationData.value = resp.data;
                    _cacheSet('optimization', id, resp.data);
                }
                loadOptBlockMap(id);
            } catch (e) {
                console.error('Failed to load optimization:', e);
            }
            optimizationLoading.value = false;
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
                const url = `/api/export/audit-package/${encodeURIComponent(currentProjectId.value)}`;
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
            // ↓ Кнопка «Подготовить данные»: crop PNG + Gemma enrichment в MD
            const ids = Array.from(selectedProjects.value);
            if (!ids.length) return;
            // Фильтр: только проекты без аудита (findings_count == 0)
            const byId = new Map(projects.value.map(p => [p.project_id, p]));
            const targets = ids.filter(pid => {
                const p = byId.get(pid);
                return p && !(p.findings_count > 0);
            });
            const skipped = ids.length - targets.length;
            if (!targets.length) {
                alert(`Все ${ids.length} выбранных проектов уже имеют аудит — подготовка пропущена.\nИспользуйте Force re-enrich на странице проекта если хотите переобогатить.`);
                return;
            }
            const confirmMsg = `Подготовить данные для ${targets.length} проектов?\n` +
                               `Будут выполнены: crop PNG + Gemma enrichment MD.\n` +
                               `Время: ~30-60 сек на блок (зависит от размера проекта).` +
                               (skipped > 0 ? `\n(пропущено ${skipped} с уже выполненным аудитом)` : '');
            if (!confirm(confirmMsg)) return;

            const force = confirm(
                `Force re-enrich?\n\n` +
                `OK = переобогатить даже уже подготовленные проекты (с backup _output/).\n` +
                `Cancel = пропустить уже подготовленные.`
            );

            batchCropLoading.value = true;
            let done = 0;
            const errors = [];
            for (const pid of targets) {
                batchCropProgress.value = `${done}/${targets.length}`;
                try {
                    const url = `/api/audit/${encodeURIComponent(pid)}/prepare-data?force=${force ? 'true' : 'false'}`;
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
                        (skipped > 0 ? `\nПропущено (есть аудит): ${skipped}` : '') +
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
                const resp = await fetch(`/api/discussions/${encodeURIComponent(pid)}/resolved/excel?type=${discussionTab.value}`);
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
                const url = `/api/discussions/${encodeURIComponent(currentProjectId.value)}/${encodeURIComponent(activeDiscussion.value)}/chat/stream?type=${discussionTab.value}`;
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
        const paginatedFindings = computed(() => {
            const all = sortedFindings.value;
            const start = (findingsPage.value - 1) * PAGE_SIZE;
            return all.slice(start, start + PAGE_SIZE);
        });
        const findingsTotalPages = computed(() => Math.max(1, Math.ceil(sortedFindings.value.length / PAGE_SIZE)));

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
            if (status === 'running') return 'step-running';
            if (status === 'skipped') return 'step-skipped';
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
            const entries = logEntries.value;
            if (!entries.length) return;
            const text = entries.map(serializeLogEntry).filter(Boolean).join('\n');
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
            try {
                const resp = await fetch(`/api/audit/${encodeURIComponent(projectId)}/log?limit=500`);
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
                        };
                    });
                    projectLogs.value[projectId] = entries;
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
            closeProjectWS();
            wsCurrentProjectId = projectId;
            wsProjectReconnects = 0;
            const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
            wsProject = new WebSocket(`${proto}//${location.host}/ws/audit/${encodeURIComponent(projectId)}`);
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
            wsGlobal.onopen = () => {
                wsConnected.value = true;
                // При подключении подгружаем актуальное состояние prepare-queue (badge в навигации)
                fetchPrepareQueue();
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
                });
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
            } else if (msg.type === 'error') {
                pushToProjectLog(pid, {
                    time: time,
                    level: 'error',
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
                // При начале новой фазы merge — сбрасываем индекс (новый запуск конвейера)
                if (msg.data.stage === 'merge') {
                    findingIndex.value[pid] = {};
                }
            } else if (msg.type === 'finding_added') {
                pushFindingCard(pid, {
                    kind: 'finding',
                    time: time,
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

        async function loadExpertDecisions() {
            if (!currentProjectId.value) return;
            const map = {};
            try {
                const resp = await fetch(`/api/knowledge-base/expert-review/${encodeURIComponent(currentProjectId.value)}`);
                const data = await resp.json();
                if (data.has_review && data.data && data.data.decisions) {
                    for (const d of data.data.decisions) {
                        map[d.item_id] = { decision: d.decision, rejection_reason: d.rejection_reason || '', item_type: d.item_type || 'finding' };
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
                if (existing.decision) {
                    const status = existing.decision === 'accepted' ? 'confirmed' : 'rejected';
                    const reason = existing.rejection_reason || '';
                    const summary = reason || (status === 'confirmed' ? 'Принято экспертом' : 'Отклонено экспертом');
                    fetch(`/api/discussions/${encodeURIComponent(currentProjectId.value)}/${encodeURIComponent(itemId)}/resolve?type=${discType}`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ status, summary }),
                    }).catch(() => {});
                } else {
                    fetch(`/api/discussions/${encodeURIComponent(currentProjectId.value)}/${encodeURIComponent(itemId)}/resolve?type=${discType}`, {
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
                const resp = await fetch(`/api/knowledge-base/expert-review/${encodeURIComponent(currentProjectId.value)}`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ decisions, removed_ids: removedIds, reviewer: '' }),
                });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.detail || `Ошибка сохранения: ${resp.statusText}`);
                }
                const result = await resp.json();
                // Синхронизировать принятые/отклонённые решения с системой обсуждений
                for (const d of decisions) {
                    const discType = d.item_id.startsWith('OPT') ? 'optimization' : 'finding';
                    const status = d.decision === 'accepted' ? 'confirmed' : 'rejected';
                    const summary = d.rejection_reason || (status === 'confirmed' ? 'Принято экспертом' : 'Отклонено экспертом');
                    fetch(`/api/discussions/${encodeURIComponent(currentProjectId.value)}/${encodeURIComponent(d.item_id)}/resolve?type=${discType}`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({ status, summary }),
                    }).catch(() => {});
                }
                // Сбросить статус для отменённых решений
                for (const itemId of removedIds) {
                    const discType = itemId.startsWith('OPT') ? 'optimization' : 'finding';
                    fetch(`/api/discussions/${encodeURIComponent(currentProjectId.value)}/${encodeURIComponent(itemId)}/resolve?type=${discType}`, {
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
        function expertReviewSummary() {
            const vals = Object.values(expertDecisions.value);
            return {
                total: vals.filter(d => d.decision).length,
                accepted: vals.filter(d => d.decision === 'accepted').length,
                rejected: vals.filter(d => d.decision === 'rejected').length,
            };
        }

        // ─── Knowledge Base (база знаний) ───
        async function loadKnowledgeBase() {
            kbLoading.value = true;
            try {
                const params = new URLSearchParams({ status: kbTab.value, limit: '200', offset: '0' });
                if (kbSearch.value) params.set('search', kbSearch.value);
                if (kbSectionFilter.value) params.set('section', kbSectionFilter.value);
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
                const resp = await fetch('/api/knowledge-base/stats');
                kbStats.value = await resp.json();
            } catch (e) { console.warn('KB stats error:', e); }
        }

        function switchKBTab(tab) {
            kbTab.value = tab;
            if (tab === 'missing_norms') {
                loadMissingNorms();
            } else {
                loadKnowledgeBase();
            }
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

        async function loadKBPatterns() {
            kbPatternsLoading.value = true;
            try {
                const resp = await fetch('/api/knowledge-base/patterns');
                const data = await resp.json();
                kbPatterns.value = data.patterns || [];
            } catch (e) { console.error('Load patterns error:', e); }
            finally { kbPatternsLoading.value = false; }
        }

        async function detectPatterns() {
            kbPatternsLoading.value = true;
            try {
                const resp = await fetch('/api/knowledge-base/patterns/detect', { method: 'POST' });
                const data = await resp.json();
                kbPatterns.value = data.patterns || [];
            } catch (e) { console.error('Detect patterns error:', e); }
            finally { kbPatternsLoading.value = false; }
        }

        async function approvePattern(patternId) {
            await fetch(`/api/knowledge-base/patterns/${patternId}/approve`, { method: 'POST' });
            loadKBPatterns();
        }

        async function dismissPattern(patternId) {
            await fetch(`/api/knowledge-base/patterns/${patternId}/dismiss`, { method: 'POST' });
            loadKBPatterns();
        }

        async function uploadDecisionsExcel(event) {
            const file = event.target.files[0];
            if (!file) return;
            kbUploadLoading.value = true;
            try {
                const formData = new FormData();
                formData.append('file', file);
                const resp = await fetch('/api/knowledge-base/upload-excel', { method: 'POST', body: formData });
                const data = await resp.json();
                if (data.status === 'ok') {
                    alert('Решения загружены: ' + Object.keys(data.projects).length + ' проектов');
                    loadKnowledgeBase();
                    loadKBStats();
                }
            } catch (e) {
                console.error('Upload error:', e);
                alert('Ошибка загрузки файла');
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
                const formData = new FormData();
                formData.append('file', file);
                const resp = await fetch('/api/knowledge-base/upload-excel', { method: 'POST', body: formData });
                const data = await resp.json();
                if (data.status === 'ok') {
                    const count = Object.keys(data.projects).length;
                    // Загрузить решения для текущего проекта и включить режим оценки
                    if (currentProjectId.value) {
                        const revResp = await fetch(`/api/knowledge-base/expert-review/${encodeURIComponent(currentProjectId.value)}`);
                        const revData = await revResp.json();
                        if (revData.has_review && revData.data && revData.data.decisions) {
                            const map = {};
                            for (const d of revData.data.decisions) {
                                map[d.item_id] = { decision: d.decision, rejection_reason: d.rejection_reason || '', item_type: d.item_type || 'finding' };
                            }
                            expertDecisions.value = map;
                            expertReviewMode.value = true;
                        }
                    }
                    alert(`Решения загружены (${count} проектов). Колонки заполнены автоматически.`);
                }
            } catch (e) {
                console.error('Upload & apply error:', e);
                alert('Ошибка загрузки файла');
            } finally {
                kbUploadLoading.value = false;
                event.target.value = '';
            }
        }

        // Watch severity filter
        // Client-side фильтрация — без перезапроса с сервера
        watch(filterSeverity, () => _applyFindingsFilter());
        watch(filterSearch, () => _applyFindingsFilter());
        // Inline Critic v2 toggles
        watch(cv2ShowHidden, () => { findingsPage.value = 1; _applyFindingsFilter(); });
        watch(cv2DisplayFilter, () => { findingsPage.value = 1; _applyFindingsFilter(); });

        // ─── Init ───
        onMounted(() => {
            window.addEventListener('hashchange', handleRoute);
            handleRoute();
            connectGlobalWS();
            startPolling();
            // Параллельная загрузка — сначала объект (нужен currentObjectId), потом группы
            Promise.all([
                loadDisciplines(),
                loadObjects().then(() => loadProjectGroups()),
                pollGlobalUsage(),
                fetchAccountInfo(),
                fetchPaidCost(),
            ]);
            usagePollTimer = setInterval(() => { pollGlobalUsage(); fetchPaidCost(); }, 60000);
            startLmsHealthPolling();
        });

        onUnmounted(() => {
            window.removeEventListener('hashchange', handleRoute);
            stopPolling();
            if (usagePollTimer) { clearInterval(usagePollTimer); usagePollTimer = null; }
            stopLmsHealthPolling();
        });

        return {
            // Theme
            theme, toggleTheme,
            // State
            currentView, currentProject, currentProjectId, projects, loading,
            findingsData, filterSeverity, filterSearch, severityOptions,
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
            blockAnalysis, selectedBlockAnalysis, currentPageBlocks,
            emptyBlocksList, noFindingsBlocksList, skippedBlocksList,
            blockStatus, blockParentId, blockMergedBadge, blockOriginalLabel,
            currentBlocksList, currentBlockIndex, navigateBlock,
            blockHasAnalysis, blockFindingsCount, blockMaxSeverity,
            openBlock, loadBlocks, blockToFindings, getBlockFindings,
            blockImageContainer, blockImageStyle, onBlockZoomWheel, onBlockPanStart, resetBlockZoom, onBlockImageLoad,
            blockNatW, blockNatH, highlightedFindingId, currentBlockHighlights, highlightFinding, severityColor, severityStroke,
            allHighlightsVisible, hiddenHighlightFindings, toggleFindingHighlight, isFindingHighlightVisible, toggleAllHighlights,
            logProjectId, logEntries, logAutoScroll, logContainer, logLoading,
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
            navigate, refreshProjects, stepClass, combinedCriticStatus, sevClass, sevIcon,
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
            startSmartAudit, startAudit, startStandardAudit, startProAudit,
            startNormVerify, startOptimization, cancelAudit, generateExcel,
            startAllProjects, resumePipeline, resumeToQueue, resumeInfo,
            startFromStage, canStartFrom, pipelineToStage,
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
            applyNewSectionToSelected, hideSelectedFromUI,
            // Edit projects — merge as version of existing (per-row)
            editProjectsMergeMap, editProjectsMergeReadyCount,
            mergeTargetsFor, mergeNextLabelFor, mergeTargetNameFor,
            applyMergeAllAsVersion,
            // Pause
            showPauseModal, isPaused, pauseMode, anyRunning,
            pausePipeline, resumePipelineGlobal,
            // Model config
            showModelConfig, stageModelConfig, availableModels, stageLabels,
            stageModelSaveError,
            stageModelRestrictions, stageModelHints, isModelAllowed,
            modelInputType, isStageModelChecked, selectStageModel,
            modelPresets, activePreset, activePresetHint, applyPreset,
            stageBatchModes, isFindingsOnlyMode,
            loadStageModels, saveStageModels, openModelConfig, saveAndStartAudit,
            startAuditDirect,
            modelConfigPendingProjectId,
            toggleProjectSelection, toggleSelectAll, isProjectSelected,
            isSectionSelected, toggleSectionSelection,
            sectionExcelLoading, exportSectionExcel,
            projectExcelLoading, exportProjectExcel,
            openBatchModal, confirmBatchAction, startBatchAction, cancelBatch, addToBatch,
            batchActionLabel,
            // Queue management
            queueAddMode, queueAddAction, queueAddSelected, queueDragIdx, queueDragOverIdx,
            refreshBatchQueue, removeFromQueue, updateQueueItemAction, reorderQueue,
            clearQueueHistory, resumeBatchQueue,
            onQueueDragStart, onQueueDragOver, onQueueDragEnd,
            toggleQueueAddProject, confirmQueueAdd, startQueueFromView,
            queueAvailableProjects,
            // Add project
            showAddProject, addProjectStep, unregisteredFolders, addProjectLoading,
            openAddModal, goToAddSection, goToAddProject, addSection,
            newSectionName, newSectionCode, newSectionColor,
            scanFolders, scanExternalFolder, registerProject, registerAllProjects, closeAddProject,
            externalPath, projectSource,
            // Add project — version-of-existing mode
            onCandidatePrimaryAction, registerProjectAsVersion,
            candidateTargetOptions, candidateTargetName, candidateNextVersionLabel,
            normalizeProjectName,
            // Objects
            objectsList, currentObjectId, showObjectPicker, showAddObjectModal, newObjectName,
            loadObjects, switchObject, addNewObject,
            // Dashboard stats
            auditedProjectsCount, totalFindings, totalBySeverity, sevPercent,
            sectionFindingsCount, filteredSectionProjects,
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
            currentSectionProjectsList, prevProject, nextProject,
            showCreateGroup, newGroupName, editingGroupId, editingGroupName,
            createGroup, renameGroup, startRenameGroup, deleteProjectGroup,
            dragProjectId, dragGroupId, dragOverGroupId,
            onProjectDragStart, onGroupDragOver, onGroupDragLeave, onProjectDropOnGroup,
            onGroupHeaderDragStart, onGroupHeaderDragEnd,
            // Model switcher
            // Paid cost
            paidCost, showPaidCost, fetchPaidCost, resetPaidCost, formatCostShort,
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
            optTypeLabel, optTypeColor, optTypeClass, loadOptimization,
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
            getExpertDecision, getExpertReason, expertReviewSummary,
            // Knowledge Base
            kbTab, kbEntries, kbStats, kbLoading, kbSearch, kbSectionFilter,
            kbPatterns, kbPatternsLoading, kbUploadLoading,
            loadKnowledgeBase, loadKBStats, switchKBTab,
            missingNorms, missingNormsStats, missingNormsFilter,
            loadMissingNorms, markNormAdded, dismissNorm, restoreNorm,
            confirmCustomer, unconfirmCustomer, revokeKBDecision,
            loadKBPatterns, detectPatterns, approvePattern, dismissPattern,
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
            showCreateVersionModal, newVersionComment, versionsPanelOpen,
            loadProjectVersions, loadVersionFiles, selectVersion,
            createNewVersion, uploadFilesToVersion,
            handleUploadInput, handleUploadInputReplace,
            activeVersionEntry, canStartAuditNow, versionBadgeFor,
            // ─── Migrated findings (контроль ранее согласованных замечаний) ───
            migratedFindingsReport, migratedFindingsReportLoading,
            migratedFindingsCheckRunning, migratedFindingsError,
            migratedFindingsPanelOpen, migratedFindingsSummary,
            canRunMigratedCheckNow,
            loadMigratedFindingsReport, runMigratedFindingsCheck,
            migratedStatusLabel, migratedStatusTone, findingMigratedBadge,
        };
    }
});

app.mount('#app');
