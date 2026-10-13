(function (root, factory) {
    const api = factory();
    if (typeof module === 'object' && module.exports) module.exports = api;
    root.ProjectChangeView = api;
}(typeof globalThis !== 'undefined' ? globalThis : this, function () {
    'use strict';
    const OBJECT = '4f3e5916'; // Canonical production /api/objects registry ID.
    const SOURCES = ['TEXT', 'TABLE', 'GRAPHIC'];
    const STATUS = {
        REVIEW: 'Нужно проверить', CONFIRMED: 'Подтверждено', REJECTED: 'Не изменение',
        UNDETERMINED: 'Не могу определить', PROBLEM: 'Проблема', CONFLICT: 'Конфликт источников',
    };
    const TYPES = {
        EQUIPMENT: 'Оборудование', SYSTEM: 'Система', QUANTITY: 'Количество',
        PARAMETERS: 'Параметр', REQUIREMENT: 'Требование',
        LAYOUT: 'Трассировка', OTHER: 'Другое',
    };
    const TYPE_MAP = {
        EQUIPMENT_REPLACED: 'EQUIPMENT', EQUIPMENT_ADDED: 'EQUIPMENT', EQUIPMENT_REMOVED: 'EQUIPMENT',
        EQUIPMENT_COUNT_CHANGED: 'QUANTITY', SYSTEM_TYPE_CHANGED: 'SYSTEM',
        SYSTEM_CONFIGURATION_CHANGED: 'SYSTEM', SYSTEM_MODE_CHANGED: 'PARAMETERS',
        CAPACITY_CHANGED: 'PARAMETERS', LAYOUT_CHANGED: 'LAYOUT', ROUTING_CHANGED: 'LAYOUT',
        REQUIREMENT_CHANGED: 'REQUIREMENT', ENGINEERING_SOLUTION_CHANGED: 'SYSTEM',
    };
    const REASONS = {
        OLD_SCOPE_NOT_ESTABLISHED: 'Область поиска в OLD не установлена. Отсутствие оборудования пока не доказано.',
        AMBIGUOUS_ENTITY: 'Найдены похожие объекты, но не установлено, что это одна установка.',
        NOT_FOUND_UNPROVEN: 'На другой стороне не найдено надёжного соответствия. Это ещё не доказывает изменение.',
        SAME_SUBJECT: 'Проверьте, относятся ли фрагменты к одному инженерному объекту.',
    };
    const arr = value => Array.isArray(value) ? value : [];
    const str = value => typeof value === 'string' ? value : '';
    const pageNumber = value => Number.isInteger(Number(value)) && Number(value) > 0 ? Number(value) : null;
    // Bind the whole event, never just its first evidence. The registry and
    // version-pinned PDF paths are supplied by the presentation API itself.
    function pairBinding(evidence, pairs) {
        const registry = new Map(arr(pairs).map(p => [p.id, p]));
        const ids = new Set();
        if (!arr(evidence).length) return {pair_ids: [], pair_binding_error: true};
        for (const e of evidence) {
            const pair = registry.get(e?.pair_id);
            const doc = pair?.[e?.side === 'OLD' ? 'left' : e?.side === 'NEW' ? 'right' : ''];
            if (!doc || !e.document?.pdf_path || !e.document?.version
                    || e.document.pdf_path !== doc.pdf_path || e.document.version !== doc.version_id) {
                return {pair_ids: [], pair_binding_error: true};
            }
            ids.add(pair.id);
        }
        return {pair_ids: [...ids].sort(), pair_binding_error: false};
    }
    function inPair(changes, pairId) {
        return pairId ? changes.filter(c => !c.pair_binding_error && arr(c.pair_ids).includes(pairId)) : [];
    }
    function displaySystem(value) { return /^(UNKNOWN|UNSPECIFIED|N\/A)$/i.test(str(value).trim()) ? '' : str(value); }
    function compactText(value, limit = 140) {
        const text = str(value).replace(/\s+/g, ' ').trim();
        return text.length > limit ? text.slice(0, limit - 1).trimEnd() + '…' : text;
    }
    function safeImage(value) {
        const url = str(value);
        return /^\/(?!\/)/.test(url) && !/[\s\\]/.test(url) ? url : '';
    }
    function region(value) {
        if (!value || value.units !== 'normalized') return null;
        const {x, y, width, height} = value;
        return [x, y, width, height].every(Number.isFinite) && x >= 0 && y >= 0
            && width > 0 && height > 0 && x + width <= 1.001 && y + height <= 1.001
            ? {x, y, width, height, units: 'normalized'} : null;
    }
    function evidenceView(e, i) {
        e = e && typeof e === 'object' ? e : {};
        return {
            id: str(e.id) || `evidence-${i}`, source_type: SOURCES.includes(e.source_type) ? e.source_type : null,
            side: ['OLD', 'NEW'].includes(e.side) ? e.side : null,
            document: {id: str(e.document?.id), label: str(e.document?.label),
                version: str(e.document?.version), pdf_path: str(e.document?.pdf_path)},
            pair_id: str(e.pair_id), page: pageNumber(e.page), region: region(e.region),
            image_url: safeImage(e.image_url), page_view_url: safeImage(e.page_view_url),
            crop_precision: e.crop_precision !== 'PAGE_LEVEL' && region(e.region) ? 'EXACT_REGION' : 'PAGE_LEVEL',
            short_explanation_ru: str(e.short_explanation_ru) || 'Фрагмент источника для проверки инженерного изменения.',
            quote: str(e.quote),
        };
    }
    // Accept only an explicit presentation envelope. Atomic differences and
    // legacy findings are deliberately not promoted to engineering events.
    function fromEnvelope(envelope, objectId) {
        if (!str(objectId) || envelope?.object_id !== objectId
                || envelope?.schema_version !== 'project-change-view/1') return [];
        const research = envelope.origin === 'RESEARCH';
        const seen = new Set();
        return arr(envelope.items).filter(c => c && str(c.id) && !seen.has(c.id) && seen.add(c.id)).map((c, index) => {
            const conflicts = arr(c.conflicts).filter(x => x && typeof x === 'object').map(x => ({
                explanation_ru: str(x.explanation_ru) || 'Источники противоречат друг другу.',
                resolved: x.resolved === true,
                values: arr(x.values).map(v => ({source_type: SOURCES.includes(v.source_type) ? v.source_type : null,
                    value: str(v.value)})),
            }));
            const openConflict = conflicts.some(x => !x.resolved);
            const actionStatus = {CONFIRM:'CONFIRMED', NOT_A_CHANGE:'REJECTED', UNSURE:'UNDETERMINED', BROKEN_CASE:'PROBLEM'};
            const human = research && envelope.mode === 'BACKEND_PREVIEW' && envelope.decision_mode !== 'READ_ONLY' && c.decision_state === 'ACTIVE'
                && c.effective_decision?.decision_key === c.decision_key
                && c.effective_decision?.binding_signature === c.binding_signature
                && c.effective_decision?.candidate_version === c.candidate_version
                ? actionStatus[c.effective_decision.decision] : null;
            const expert = c.expert_review_available === true && c.candidate_version === 'projectchange_v3' ? c.expert_review : null;
            const expertStatus = {accepted:'CONFIRMED', rejected:'REJECTED'}[expert?.decision];
            const status = expertStatus || (openConflict ? 'CONFLICT' : human || (envelope.origin !== 'PRODUCTION' ? 'REVIEW'
                : Object.hasOwn(STATUS, c.status) ? c.status : 'REVIEW'));
            return {
                id: c.id, display_id: `PC-${String(index + 1).padStart(3, '0')}`,
                projectchange_id: str(c.projectchange_id), pair_id: str(c.pair_id),
                expert_review_available: c.expert_review_available === true && c.candidate_version === 'projectchange_v3',
                expert_review: expert || null,
                expert_review_run_id: str(c.expert_review_run_id) || str(c.source_run_id),
                ...pairBinding(c.evidence, envelope.viewer_session?.pairs),
                summary_ru: str(c.summary_ru) || 'Изменение требует проверки',
                change_type: Object.hasOwn(TYPES, c.change_type) ? c.change_type : 'OTHER', status,
                importance: c.importance === 'HIGH' ? 'HIGH' : 'NORMAL',
                cipher: str(c.cipher), discipline: str(c.discipline), engineering_system: displaySystem(c.engineering_system),
                engineering_subject: str(c.engineering_subject), old_state: str(c.old_state), new_state: str(c.new_state),
                evidence: arr(c.evidence).map(evidenceView),
                details: arr(c.details).filter(d => d && typeof d === 'object')
                    .map(d => ({label: str(d.label) || 'Характеристика', old: str(d.old), new: str(d.new)})),
                conflicts, review_question: str(c.review_question) || 'Подтверждается ли изменение по этим источникам?',
                review_explanation_ru: str(c.review_explanation_ru) || 'Сопоставьте OLD и NEW и проверьте, относится ли вывод к одному объекту.',
                technical_provenance: arr(c.technical_provenance).map(str),
                session_id: str(c.session_id), source_run_id: str(c.source_run_id), candidate_version: str(c.candidate_version),
                research_status: str(c.research_status), decision_key: str(c.decision_key),
                binding_signature: str(c.binding_signature), decision_state: str(c.decision_state),
                effective_decision: human ? c.effective_decision : null,
                research, revision: str(envelope.revision),
            };
        });
    }
    // Offline adapter boundary. The UI never reads research field names.
    // Bindings are generated from admitted, version-pinned source PDFs.
    function fromResearch(changes, context) {
        if (context.object_id !== OBJECT || context.partition !== 'DEV') return null;
        return {schema_version: 'project-change-view/1', object_id: OBJECT, origin: 'RESEARCH',
            revision: context.revision, items: arr(changes).map(c => {
                const meta = context.metadata?.[c.project_change_id] || {};
                return {
                    id: c.project_change_id, summary_ru: meta.summary_ru || c.short_summary_ru,
                    change_type: TYPE_MAP[c.change_type] || 'OTHER', status: 'REVIEW',
                    importance: c.importance === 'MATERIAL' ? 'HIGH' : 'NORMAL',
                    cipher: meta.cipher || '', discipline: meta.discipline || '',
                    engineering_system: meta.engineering_system || c.engineering_subject?.system || '',
                    engineering_subject: meta.engineering_subject || c.engineering_subject?.mark
                        || c.engineering_subject?.equipment_class || 'Инженерный объект требует уточнения',
                    old_state: meta.old_state || c.old_state, new_state: meta.new_state || c.new_state,
                    evidence: ['old', 'new'].flatMap(side => arr(c['evidence_' + side]).flatMap(e =>
                        arr(context.bindings?.[e.evidence_id]).map(binding => ({
                            ...binding, id: `${e.evidence_id}-${binding.page}`, source_type: e.route,
                            side: side.toUpperCase(), quote: e.quote,
                            short_explanation_ru: binding.short_explanation_ru ||
                                (side === 'old' ? 'Исходное состояние объекта.' : 'Состояние объекта в новой редакции.'),
                        })))),
                    details: arr(c.supporting_fact_changes).map(f => ({
                        label: context.property_labels?.[f.property] || 'Изменившаяся характеристика',
                        old: f.old?.quote || f.old?.value, new: f.new?.quote || f.new?.value,
                    })),
                    conflicts: arr(c.conflicts).map(f => ({resolved: f.status === 'RESOLVED',
                        explanation_ru: 'Источники противоречат друг другу.', values: []})),
                    review_question: meta.review_question || 'Подтверждается ли изменение по этим источникам?',
                    review_explanation_ru: meta.review_explanation_ru ||
                        arr(c.review_reasons).map(r => REASONS[r]).filter(Boolean).join(' ') ||
                        'Исследовательский вывод ещё не проверен инженером. Сверьте состояние одного объекта в OLD и NEW.',
                    technical_provenance: [c.project_change_id, `Research status: ${c.status}`,
                        ...arr(c.review_reasons), ...arr(c.decision_reasons)],
                };
            })};
    }
    function applyDecision(change, decision) {
        if (!['CONFIRMED', 'REJECTED', 'UNDETERMINED', 'PROBLEM'].includes(decision)) return change;
        if (decision === 'CONFIRMED' && change.conflicts.some(c => !c.resolved)) return change;
        return {...change, status: decision, local_decision: true};
    }
    function filter(changes, filters) {
        return changes.filter(c => (!filters.cipher || `${c.cipher} · ${c.discipline}` === filters.cipher)
            && (!filters.system || c.engineering_system === filters.system)
            && (!filters.type || c.change_type === filters.type)
            && (!filters.status || c.status === filters.status)
            && (!filters.source || c.evidence.some(e => e.source_type === filters.source)));
    }
    function report(changes) { return changes.filter(c => c.status === 'CONFIRMED' && !c.conflicts.some(x => !x.resolved)); }
    function summary(changes) {
        return {confirmed: report(changes).length,
            review: changes.filter(c => ['REVIEW', 'UNDETERMINED', 'PROBLEM'].includes(c.status)).length,
            conflicts: changes.filter(c => c.conflicts.some(x => !x.resolved)).length,
            high: changes.filter(c => c.importance === 'HIGH' && c.status !== 'REJECTED').length};
    }
    // A V3 run publishes ProjectChanges only when it finishes: while it works,
    // and when it fails, the change list is empty.  The run itself is the
    // only thing to show then — silence must never look like "no changes".
    function runBanner(state, nowMs) {
        if (!state || state.engine !== 'projectchange_v3') return null;
        const status = str(state.status).toUpperCase();
        if (status === 'RUNNING' && state.orphaned_run === true) {
            return {tone: 'failed', cancellable: false, title: 'Анализ прерван',
                text: 'Процесс анализа больше не выполняется, результат не опубликован. Запустите анализ пары заново.'};
        }
        if (status === 'RUNNING') {
            const started = Date.parse(str(state.started_at));
            const parts = [str(state.message) || 'Анализ выполняется'];
            if (Number.isFinite(started) && Number.isFinite(nowMs) && nowMs >= started) {
                parts.push(`идёт ${Math.floor((nowMs - started) / 60000)} мин`);
            }
            if (Number(state.model_calls) > 0) parts.push(`обращений к модели: ${Number(state.model_calls)}`);
            return {tone: 'running', cancellable: true, title: 'Идёт анализ изменений', text: parts.join(' · ')};
        }
        if (status === 'FAILED') {
            const reason = str(state.reason_code);
            // Miner этого прогона сохранён: его можно досчитать с Dedupe без повторного анализа областей.
            return {tone: 'failed', cancellable: false, title: 'Анализ не выполнен',
                text: (str(state.message) || 'Причина не указана.') + (reason ? ` (${reason})` : ''),
                resumable: state.resume_available === true, runId: str(state.run_id)};
        }
        if (['COMPLETED', 'REVIEW'].includes(status) && state.human_mapping_published === false) {
            return {tone: 'warning', cancellable: false, title: 'Human Mapping не опубликован',
                text: str(state.human_mapping_error) || str(state.message)};
        }
        return ['COMPLETED', 'REVIEW'].includes(status) ? consolidationBanner(state.consolidation, nowMs) : null;
    }
    // Последний этап прогона — «Сведение дублей» (Консолидатор): ход и итог, если он не COMPLETED.
    function consolidationBanner(stage, nowMs) {
        if (!stage || typeof stage !== 'object') return null;
        const status = str(stage.status).toUpperCase();
        if (status === 'RUNNING') {
            const parts = [str(stage.message) || 'Модель сводит дубли изменений'];
            if (Number(stage.calls_total) > 0) parts.push(`вызов ${Math.min(Number(stage.calls_done || 0) + 1, Number(stage.calls_total))} из ${Number(stage.calls_total)}`);
            const started = Date.parse(str(stage.started_at));
            if (Number.isFinite(started) && Number.isFinite(nowMs) && nowMs >= started) {
                parts.push(`идёт ${Math.floor((nowMs - started) / 60000)} мин`);
            }
            return {tone: 'running', cancellable: true, title: 'Последний этап: сведение дублей', text: parts.join(' · ')};
        }
        if (['FAILED', 'INTERRUPTED', 'CANCELLED', 'SKIPPED'].includes(status)) {
            const title = {FAILED: 'Сведение дублей не удалось', INTERRUPTED: 'Сведение дублей прервано',
                CANCELLED: 'Сведение дублей остановлено', SKIPPED: 'Сведение дублей не выполнялось'}[status];
            const reason = str(stage.reason_code);
            return {tone: 'warning', cancellable: false, title, consolidationRetry: true,
                text: (str(stage.message) || 'Причина не указана.') + (reason && status !== 'CANCELLED' ? ` (${reason})` : '')};
        }
        return null;
    }
    const COMPARISON_STAGES = [
        ['SOURCE_PREPARATION', 'Подготовка документов', 'Подготавливаем страницы, текст, таблицы и изображения OLD и NEW.'],
        ['MAPPING', 'Смысловое сопоставление', 'Находим связанные системы и фрагменты в обеих редакциях.'],
        ['MINING', 'Поиск изменений', 'Сравниваем проектные решения и собираем подтверждения из OLD и NEW.'],
        ['UNMATCHED_REVIEW', 'Разбор несопоставленного', 'Проверяем страницы без пары и отмечаем недостающие доказательства.'],
        ['DEDUPE', 'Свод изменений', 'Объединяем повторные находки одного изменения, сохраняя подтверждения.'],
        ['SOURCE_VERIFICATION', 'Проверка по источникам', 'Перепроверяем параметры, единицы и принадлежность значений нужному объекту.'],
        ['RESULT', 'Подготовка результата', 'Сохраняем изменения, подготавливаем сопоставления и выполняем включённое итоговое сведение дублей.'],
    ];
    const PIPELINE_LABELS = {WAITING: 'Ожидает', RUNNING: 'Выполняется', COMPLETED: 'Выполнено',
        PARTIAL: 'Требует внимания', FAILED: 'Ошибка', CANCELLED: 'Остановлено',
        INTERRUPTED: 'Прервано', SKIPPED: 'Пропущено', REUSED: 'Из сохранённого', UNKNOWN: 'Нет данных об этапе'};
    function sourceVerificationDisplay(row, status) {
        if (!['PARTIAL', 'COMPLETED'].includes(status)) return {};
        const counts = row.verification || {};
        const count = key => Number.isInteger(counts[key]) && counts[key] >= 0 ? counts[key] : null;
        // Older recorded stages may have only the parameter progress counter.
        const pending = count('pending') ?? (row.unit === 'parameter' && Number.isInteger(row.total)
            && Number.isInteger(row.processed) && row.total >= row.processed && row.processed >= 0
            ? row.total - row.processed : null);
        const reviewCounts = ['corrected', 'conflicts', 'unreadable'].map(count);
        const review = reviewCounts.every(value => value != null) ? reviewCounts.reduce((a, b) => a + b, 0) : null;
        const details = [];
        const confirmed = count('confirmed'), failed = count('failed');
        if (confirmed != null) details.push(`Подтверждено по источникам: ${confirmed}.`);
        if (count('corrected')) details.push(`Предложены поправки: ${count('corrected')}.`);
        if (count('conflicts')) details.push(`Противоречия с источником: ${count('conflicts')}.`);
        if (count('unreadable')) details.push(`Не удалось прочитать источник: ${count('unreadable')}.`);
        if (pending > 0) details.push(`Проверка не завершена для ${pending} параметров${failed ? `, из них с ошибкой проверки: ${failed}` : ''}.`);
        if (review > 0) details.push('Параметры с поправками, противоречиями и нечитаемыми источниками требуют проверки инженером.');
        let label;
        if (pending > 0) label = `Не проверено: ${pending}`;
        else if (pending === 0 && review > 0) label = `Проверено всё · требуют разбора: ${review}`;
        else if (pending === 0 && status === 'PARTIAL') label = 'Проверено всё · есть замечания';
        return {label, detail: details.join(' '),
            attention: pending > 0 && review > 0 ? `Требуют разбора: ${review}` : ''};
    }
    function comparisonPipeline(state) {
        if (!state || (state.engine !== 'projectchange_v3' && !state.pipeline_pending)) return null;
        const saved = state.pipeline_stages || {};
        const hasHistory = Object.keys(saved).length > 0;
        const restored = state.pipeline_history_source === 'saved_result';
        const terminal = ['COMPLETED', 'REVIEW'].includes(state.status);
        const failed = state.status === 'FAILED';
        const interrupted = state.orphaned_run === true;
        const current = state.pipeline_current || ({PERSISTING: 'RESULT', HUMAN_MAPPING: 'RESULT'}[state.current_stage])
            || state.current_stage || (state.pipeline_pending ? 'SOURCE_PREPARATION' : '');
        const consolidation = state.consolidation;
        const stages = COMPARISON_STAGES.map(([id, title, description], index) => {
            const row = {...(saved[id] || {})};
            let status = row.status || ((hasHistory && !restored) || state.pipeline_pending ? 'WAITING' : 'UNKNOWN');
            if (!hasHistory && id === current) {
                status = failed ? (state.reason_code === 'v3_cancelled' ? 'CANCELLED' : 'FAILED') : 'RUNNING';
                Object.assign(row, {message: state.message, processed: state.processed, total: state.total, unit: state.unit});
            }
            if (id === current && interrupted) status = 'INTERRUPTED';
            if (id === current && failed) {
                status = state.reason_code === 'v3_cancelled' ? 'CANCELLED' : 'FAILED';
                row.message = state.message;
            }
            if (id === 'RESULT' && terminal) {
                if (state.human_mapping_published === false) {
                    status = 'PARTIAL';
                    row.message = 'Изменения сохранены, но сопоставления для просмотра не подготовлены.';
                } else if (consolidation) {
                    status = consolidation.status === 'RUNNING' && consolidation.live === false
                        ? 'INTERRUPTED' : consolidation.status === 'SKIPPED' ? 'PARTIAL' : consolidation.status;
                    row.message = consolidation.status === 'COMPLETED' ? 'Результат сохранён. Итоговое сведение дублей завершено.'
                        : consolidation.message || 'Итоговое сведение дублей';
                    row.processed = consolidation.calls_done;
                    row.total = consolidation.calls_total;
                    row.unit = 'call';
                    row.finished_at = consolidation.finished_at;
                } else if (state.consolidation_expected) {
                    status = 'WAITING';
                    row.message = 'Результат сохранён. Ожидает итогового сведения дублей.';
                } else if (hasHistory) {
                    row.message = 'Результат сохранён и доступен для просмотра.';
                }
            }
            const processed = Number(row.processed), total = Number(row.total);
            const units = {region: 'областей', batch: 'пакетов', portion: 'частей', parameter: 'параметров', call: 'обращений'};
            const counter = row.processed != null && row.total != null && Number.isFinite(processed)
                && Number.isFinite(total) && total > 0 ? `${processed} из ${total} ${units[row.unit] || ''}`.trim() : '';
            const start = Date.parse(row.started_at), end = Date.parse(row.finished_at);
            const duration = Number.isFinite(start) && Number.isFinite(end) && end >= start
                ? `${Math.floor((end - start) / 60000)} мин ${Math.floor((end - start) / 1000) % 60} с` : '';
            const message = str(row.message).replace(/^V3:\s*/, '').replace(/\s*\((?:Mapper|Miner|Dedupe)\)/g, '');
            const verification = id === 'SOURCE_VERIFICATION' ? sourceVerificationDisplay(row, status) : {};
            return {id, title, description, number: String(index + 1).padStart(2, '0'), status,
                label: verification.label || PIPELINE_LABELS[status] || PIPELINE_LABELS.UNKNOWN,
                counter, duration, message, attention: verification.attention || '', detail: verification.detail || '',
                css: ({COMPLETED: 'step-done', REUSED: 'step-done', RUNNING: 'step-running', FAILED: 'step-error',
                    PARTIAL: 'step-partial', CANCELLED: 'step-partial', INTERRUPTED: 'step-partial'}[status]) || 'step-pending'};
        });
        let title = 'Анализ выполняется';
        if (failed) title = state.reason_code === 'v3_cancelled' ? 'Анализ остановлен' : 'Анализ не завершён';
        else if (interrupted) title = 'Анализ прерван';
        else if (terminal) {
            const result = stages[6];
            title = result.status === 'RUNNING' ? 'Итоговое сведение дублей'
                : result.status === 'WAITING' ? 'Ожидает итогового сведения'
                : ['FAILED', 'CANCELLED', 'INTERRUPTED'].includes(result.status) ? 'Подготовка результата не завершена'
                : state.status === 'REVIEW' || stages.some(s => s.status === 'PARTIAL')
                    ? 'Анализ завершён · требуется проверка' : 'Анализ завершён';
        }
        return {stages, title, runId: str(state.run_id), historical: !hasHistory && !state.pipeline_pending, restored,
            current: stages.find(s => s.status === 'RUNNING')?.id || current};
    }
    function qualityBanner(run) {
        if (!run || !run.coverage || typeof run.coverage !== 'object') return null;
        const pending = Number(run.coverage.pages_pending || 0);
        const unmatched = Number(run.coverage.content_unmatched_pending ?? run.coverage.content_unmatched ?? 0);
        if (pending > 0 || unmatched > 0) {
            const details = [];
            if (pending > 0) details.push(`не завершена обработка страниц: ${pending}`);
            if (unmatched > 0) details.push(`содержательных листов вне парных регионов: ${unmatched}`);
            return {tone: 'warning', title: 'Анализ охватил не все содержание', text: details.join(' · ')};
        }
        const corrected = Number(run.quality?.source_verification_corrected || 0);
        const conflicts = Number(run.quality?.source_verification_conflicts || 0);
        const unreadable = Number(run.quality?.source_verification_unreadable || 0);
        const sourcePending = Number(run.quality?.source_verification_pending || 0);
        // Source-verification counters are not shown as a banner (engineer's request);
        // the run already went through verification, so no fallback banner either.
        if (corrected || conflicts || unreadable || sourcePending) return null;
        const verified = Number(run.quality?.independently_verified || 0);
        if (Number(run.projectchange_count || 0) > 0 && verified === 0) {
            return {tone: 'warning', title: 'Параметры требуют инженерной проверки',
                text: 'Страницы обработаны, но адреса доказательств и confidence не являются независимым подтверждением значений.'};
        }
        return null;
    }
    function destination(change, selected) {
        const evidence = selected || change.evidence.find(e => e.page && e.pair_id);
        if (!evidence?.pair_id || !evidence.page || !evidence.document.id || !evidence.document.version
                || !['OLD', 'NEW'].includes(evidence.side)) return null;
        const side = evidence.side;
        const other = change.evidence.find(e => e.side && e.side !== side && e.pair_id === evidence.pair_id && e.page);
        return {change_id: change.id, pair_id: evidence.pair_id,
            OLD: side === 'OLD' ? evidence : other?.side === 'OLD' ? other : null,
            NEW: side === 'NEW' ? evidence : other?.side === 'NEW' ? other : null};
    }
    return {OBJECT, SOURCES, STATUS, TYPES, fromEnvelope, fromResearch, applyDecision, filter, report, summary, destination,
        pairBinding, inPair, displaySystem, compactText, runBanner, qualityBanner, comparisonPipeline};
}));
