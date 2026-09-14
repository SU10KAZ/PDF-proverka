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
        PARAMETERS: 'Производительность / параметры', REQUIREMENT: 'Требование',
        LAYOUT: 'Планировка / трассировка', OTHER: 'Другое',
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
            image_url: safeImage(e.image_url),
            crop_precision: e.crop_precision !== 'PAGE_LEVEL' && region(e.region) ? 'EXACT_REGION' : 'PAGE_LEVEL',
            short_explanation_ru: str(e.short_explanation_ru) || 'Фрагмент источника для проверки инженерного изменения.',
            quote: str(e.quote),
        };
    }
    // Accept only an explicit presentation envelope. Atomic differences and
    // legacy findings are deliberately not promoted to engineering events.
    function fromEnvelope(envelope, objectId) {
        if (objectId !== OBJECT || envelope?.object_id !== OBJECT
                || envelope?.schema_version !== 'project-change-view/1') return [];
        const research = envelope.origin === 'RESEARCH';
        const seen = new Set();
        return arr(envelope.items).filter(c => c && str(c.id) && !seen.has(c.id) && seen.add(c.id)).map(c => {
            const conflicts = arr(c.conflicts).filter(x => x && typeof x === 'object').map(x => ({
                explanation_ru: str(x.explanation_ru) || 'Источники противоречат друг другу.',
                resolved: x.resolved === true,
                values: arr(x.values).map(v => ({source_type: SOURCES.includes(v.source_type) ? v.source_type : null,
                    value: str(v.value)})),
            }));
            const openConflict = conflicts.some(x => !x.resolved);
            const actionStatus = {CONFIRM:'CONFIRMED', NOT_A_CHANGE:'REJECTED', UNSURE:'UNDETERMINED', BROKEN_CASE:'PROBLEM'};
            const human = research && envelope.mode === 'BACKEND_PREVIEW' && c.decision_state === 'ACTIVE'
                && c.effective_decision?.decision_key === c.decision_key
                && c.effective_decision?.binding_signature === c.binding_signature
                && c.effective_decision?.candidate_version === c.candidate_version
                ? actionStatus[c.effective_decision.decision] : null;
            const status = openConflict ? 'CONFLICT' : human || (envelope.origin !== 'PRODUCTION' ? 'REVIEW'
                : Object.hasOwn(STATUS, c.status) ? c.status : 'REVIEW');
            return {
                id: c.id, summary_ru: str(c.summary_ru) || 'Изменение требует проверки',
                change_type: Object.hasOwn(TYPES, c.change_type) ? c.change_type : 'OTHER', status,
                importance: c.importance === 'HIGH' ? 'HIGH' : 'NORMAL',
                cipher: str(c.cipher), discipline: str(c.discipline), engineering_system: str(c.engineering_system),
                engineering_subject: str(c.engineering_subject), old_state: str(c.old_state), new_state: str(c.new_state),
                evidence: arr(c.evidence).map(evidenceView),
                details: arr(c.details).filter(d => d && typeof d === 'object')
                    .map(d => ({label: str(d.label) || 'Характеристика', old: str(d.old), new: str(d.new)})),
                conflicts, review_question: str(c.review_question) || 'Подтверждается ли изменение по этим источникам?',
                review_explanation_ru: str(c.review_explanation_ru) || 'Сопоставьте OLD и NEW и проверьте, относится ли вывод к одному объекту.',
                technical_provenance: arr(c.technical_provenance).map(str),
                source_run_id: str(c.source_run_id), candidate_version: str(c.candidate_version),
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
    return {OBJECT, SOURCES, STATUS, TYPES, fromEnvelope, fromResearch, applyDecision, filter, report, summary, destination};
}));
