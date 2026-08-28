(function stageComparisonReviewModule(root, factory) {
    const api = factory();
    if (typeof module === 'object' && module.exports) module.exports = api;
    root.StageComparisonReview = api;
}(typeof globalThis !== 'undefined' ? globalThis : this, function buildModule() {
    'use strict';

    const REVIEW_DECISIONS = ['PENDING_REVIEW', 'APPROVED', 'REJECTED'];
    const QUESTION_CATEGORIES = ['SHEET', 'ENTITY', 'CHANGE'];
    const PIPELINE_STATUSES = [
        'NOT_STARTED', 'RUNNING', 'NEEDS_REVIEW', 'COMPLETED',
        'FAILED', 'PARTIAL', 'NOT_APPLICABLE',
    ];

    const REASON_LABELS = {
        COMPLEX_SHEET_RELATION_REQUIRES_EXPLICIT_GRAPHIC_SCOPE:
            'Для части листов графическое сравнение не выполнено: один лист соответствует нескольким. Групповое графическое сравнение пока не поддерживается.',
        GROUPED_GRAPHIC_COMPARISON_NOT_SUPPORTED:
            'Связь 1→N или N→1 сохранена для ручной проверки: групповое графическое сравнение пока не поддерживается.',
        GROUPED_PAGE_CARDINALITY_REQUIRES_NEW_COMPARATOR:
            'Выбранная группа листов не поддерживается текущим постраничным графическим компаратором.',
        AMBIGUOUS_PREPARED_GRAPHIC_BLOCKS:
            'На сопоставленном листе найдено несколько графических блоков. Нужно уточнить пару; первый блок автоматически не выбирается.',
        MULTIPLE_PREPARED_GRAPHIC_BLOCKS_ON_MATCHED_SHEET:
            'На сопоставленном листе найдено несколько графических блоков. Нужно уточнить пару; первый блок автоматически не выбирается.',
        MULTI_BLOCK_CORRESPONDENCE_NOT_IN_G1:
            'Однозначная пара графических блоков не доказана. Нужно выбрать соответствующие блоки вручную.',
        NO_PREPARED_GRAPHIC_BLOCK_ON_MATCHED_SHEET:
            'На сопоставленных листах нет подготовленной графики для сравнения.',
        NO_CLIENT_GRAPHIC_BLOCK_IN_EFFECTIVE_SHEET_SCOPE:
            'Выбранные графические блоки не входят в текущую область сопоставленных листов.',
        PAGE_ACTION_INVALIDATES_EXPLICIT_BLOCK_SCOPE:
            'Изменение области листов исключило ранее выбранные графические блоки; нужен новый выбор.',
        VISION_REQUIRED:
            'Детерминированный маршрутизатор не смог завершить графическое сравнение; требуется visual fallback.',
        EXTRACTION_COMPLETENESS_INSUFFICIENT:
            'Извлечённая геометрия не покрывает видимую графику достаточно полно. Нужен visual fallback или ручная проверка.',
        RASTER_BACKED_SOURCE:
            'Источник содержит растровую графику, которую нельзя надёжно проверить только по векторной геометрии.',
        TEXT_AS_CURVES_ASYMMETRY:
            'На сторонах различается представление текста как текста и кривых; автоматическое сравнение остановлено.',
        REGISTRATION_FAILED:
            'Графические области не удалось надёжно совместить; результат оставлен для visual fallback или ручной проверки.',
        LOW_MATCHED_GRAPHIC_COVERAGE:
            'После совмещения подтверждена слишком малая доля графики; автоматический вывод запрещён.',
        GRAPHIC_ROUTE_UNAVAILABLE:
            'Графический маршрут не определён; результат оставлен для проверки.',
        NO_GRAPHIC_COMPARISON:
            'Маршрутизатор подтвердил, что графическое сравнение к этой паре неприменимо.',
        SHEET_RELATION_REQUIRES_REVIEW:
            'Связь листов недостаточно уверенна для автоматического графического сравнения; нужно подтверждение инженера.',
        SHEET_RELATION_UNCONFIRMED:
            'Структурированные свойства найдены, но соответствие листов ещё не подтверждено.',
        OPPOSITE_SIDE_STRUCTURED_COVERAGE_INCOMPLETE:
            'На одной стороне нет полного структурированного покрытия. Свойства показаны для проверки, но не объявлены изменениями.',
        UNRESOLVED_TEXT_STRUCTURE:
            'Часть текста не имеет достаточно строгой структуры для автоматической классификации.',
        TEXT_SOURCE_MISSING:
            'Не найден один из исходных файлов текстовой подготовки.',
        TEXT_SOURCE_DECODING_FAILED:
            'Исходный текст не удалось прочитать в ожидаемой кодировке.',
        TEXT_SOURCE_READ_FAILED:
            'Не удалось прочитать исходный PDF или Markdown.',
        TEXT_PIPELINE_VALIDATION_FAILED:
            'Текстовый артефакт не прошёл проверку production-контракта.',
        TEXT_EXTRACTION_UNAVAILABLE:
            'Текстовую подготовку не удалось завершить; подробности доступны в диагностике.',
        FileNotFoundError:
            'Один из нужных файлов подготовки не найден.',
        UnicodeDecodeError:
            'Исходный текст не удалось прочитать в ожидаемой кодировке.',
        ValueError:
            'Один из артефактов не прошёл проверку формата.',
        RuntimeError:
            'Детерминированный этап не смог завершиться.',
    };

    function array(value) {
        return Array.isArray(value) ? value : [];
    }

    function text(value, fallback) {
        if (value === null || value === undefined || value === '') return fallback || '—';
        if (typeof value === 'string') return value;
        if (typeof value === 'number' || typeof value === 'boolean') return String(value);
        try { return JSON.stringify(value); } catch (_) { return String(value); }
    }

    function decision(value) {
        const normalized = String(value || '').toUpperCase();
        return REVIEW_DECISIONS.includes(normalized) ? normalized : 'PENDING_REVIEW';
    }

    function confidence(value) {
        if (value && typeof value === 'object') {
            return String(value.level || value.confidence || 'UNKNOWN').toUpperCase();
        }
        return String(value || 'UNKNOWN').toUpperCase();
    }

    function uniqueNumbers(values) {
        return [...new Set(array(values)
            .map(Number)
            .filter(value => Number.isInteger(value) && value > 0))]
            .sort((left, right) => left - right);
    }

    function pagesForSide(change, side) {
        const upper = String(side || '').toUpperCase();
        const lower = upper.toLowerCase();
        const found = [];
        const visited = new Set();

        function add(value) {
            const page = Number(value);
            if (Number.isInteger(page) && page > 0) found.push(page);
        }

        function visit(value) {
            if (!value || typeof value !== 'object' || visited.has(value)) return;
            visited.add(value);
            if (Array.isArray(value)) {
                value.forEach(visit);
                return;
            }
            array(value[`${lower}_pages`]).forEach(add);
            if (String(value.side || '').toUpperCase() === upper) add(value.page);
            const locations = value.locations;
            if (locations && typeof locations === 'object' && !Array.isArray(locations)) {
                array(locations[upper] || locations[lower]).forEach(location => {
                    if (location && typeof location === 'object') add(location.page);
                });
            }
            Object.values(value).forEach(visit);
        }

        visit(change);
        return uniqueNumbers(found);
    }

    function changeLabel(change) {
        const parts = [change.facet_ref, change.dimension, change.direction]
            .filter(value => value !== null && value !== undefined && value !== '')
            .map(String);
        return [...new Set(parts)].join(' · ') || text(change.outcome || change.type);
    }

    function normalizeRows(payload) {
        const rows = Array.isArray(payload) ? payload : array(payload && payload.rows);
        return rows.map((source, index) => {
            const change = source && typeof source.change === 'object'
                ? source.change
                : (source || {});
            const engineer = source && typeof source.engineer_decision === 'object'
                ? source.engineer_decision
                : (change.engineer_decision || {});
            const targetId = String(
                source.target_id || change.change_id || change.review_evidence_id || `row-${index + 1}`
            );
            const leftPages = pagesForSide(change, 'LEFT');
            const rightPages = pagesForSide(change, 'RIGHT');
            const groupId = source.presentation_group_id || null;
            return {
                target_id: targetId,
                target_kind: String(source.target_kind || (change.review_evidence_id ? 'REVIEW_EVIDENCE' : 'CHANGE')),
                object_ref: text(change.project_entity_ref || change.subject_ref || change.scope_ref),
                left_pages: leftPages,
                right_pages: rightPages,
                sheets_label: `LEFT ${leftPages.join(', ') || '—'} → RIGHT ${rightPages.join(', ') || '—'}`,
                change_label: changeLabel(change),
                before: text(change.before_value),
                after: text(change.after_value),
                source: String(change.source_mode || change.source || 'UNKNOWN').toUpperCase(),
                status: String(change.review_status || change.outcome || 'CONFIRMED').toUpperCase(),
                confidence: confidence(change.confidence),
                decision: decision(engineer.decision),
                author: engineer.author || '',
                comment: engineer.comment || '',
                reason_code: engineer.reason_code || '',
                target_input_signature: engineer.input_signature || '',
                decision_revision: Number.isInteger(Number(engineer.revision))
                    ? Number(engineer.revision)
                    : 0,
                stale: Boolean(engineer.stale),
                presentation_group_id: groupId,
                presentation_group_label: groupId ? `Группа ${groupId}` : '',
                reason_codes: array(change.reason_codes).map(String),
                raw_change: change,
                raw: source,
            };
        });
    }

    function reviewCounts(value) {
        const rows = Array.isArray(value) && value.every(item => item && item.target_id)
            ? value
            : normalizeRows(value);
        const counts = {total: rows.length, APPROVED: 0, REJECTED: 0, PENDING_REVIEW: 0};
        rows.forEach(row => { counts[decision(row.decision)] += 1; });
        return counts;
    }

    function object(value) {
        return value && typeof value === 'object' && !Array.isArray(value) ? value : {};
    }

    function finiteNumber(value) {
        const normalized = Number(value);
        return Number.isFinite(normalized) && normalized >= 0 ? normalized : null;
    }

    function firstNumber(sources, keys) {
        for (const source of array(sources)) {
            const candidate = object(source);
            for (const key of array(keys)) {
                const direct = finiteNumber(candidate[key]);
                if (direct !== null) return direct;
                const counted = finiteNumber(object(candidate.counts)[key]);
                if (counted !== null) return counted;
                const diagnostics = finiteNumber(object(candidate.diagnostics)[key]);
                if (diagnostics !== null) return diagnostics;
            }
        }
        return null;
    }

    function countersFrom(specs) {
        return array(specs).map(spec => {
            const value = firstNumber(spec.sources, spec.keys);
            return value === null ? null : {label: spec.label, value};
        }).filter(Boolean);
    }

    function normalizePipelineStatus(value) {
        const status = String(value || '').toUpperCase();
        if (['COMPLETED', 'READY', 'VALID', 'ABSENT'].includes(status)) return 'COMPLETED';
        if (['RUNNING', 'IN_PROGRESS', 'PROCESSING'].includes(status)) return 'RUNNING';
        if (['NEEDS_REVIEW', 'REVIEW_REQUIRED', 'PENDING_REVIEW'].includes(status)) {
            return 'NEEDS_REVIEW';
        }
        if (['FAILED', 'ERROR'].includes(status)) return 'FAILED';
        if (['PARTIAL', 'CHECK_BLOCKED', 'NOT_CHECKED', 'BLOCKED'].includes(status)) {
            return 'PARTIAL';
        }
        if (['NOT_APPLICABLE', 'SKIPPED'].includes(status)) return 'NOT_APPLICABLE';
        return 'NOT_STARTED';
    }

    function statusOf(source) {
        const value = object(source);
        return normalizePipelineStatus(value.status || value.source_state);
    }

    function aggregatePipelineStatus(values) {
        const statuses = array(values).map(value => (
            PIPELINE_STATUSES.includes(value) ? value : normalizePipelineStatus(value)
        )).filter(Boolean);
        if (!statuses.length || statuses.every(value => value === 'NOT_STARTED')) return 'NOT_STARTED';
        if (statuses.includes('FAILED')) return 'FAILED';
        if (statuses.includes('RUNNING')) return 'RUNNING';
        if (statuses.includes('NEEDS_REVIEW')) return 'NEEDS_REVIEW';
        if (statuses.includes('PARTIAL')) return 'PARTIAL';
        if (statuses.every(value => value === 'NOT_APPLICABLE')) return 'NOT_APPLICABLE';
        if (statuses.includes('COMPLETED') && statuses.includes('NOT_APPLICABLE')) return 'PARTIAL';
        if (statuses.includes('COMPLETED')) return 'COMPLETED';
        return statuses[0] || 'NOT_STARTED';
    }

    function collectReasonCodes(values) {
        const found = [];
        const visited = new Set();
        function add(value) {
            const code = String(value || '').trim();
            if (code && !found.includes(code)) found.push(code);
        }
        function visit(value, depth) {
            if (!value || typeof value !== 'object' || visited.has(value) || depth > 4) return;
            visited.add(value);
            if (Array.isArray(value)) {
                value.forEach(item => visit(item, depth + 1));
                return;
            }
            add(value.reason_code);
            array(value.reason_codes).forEach(add);
            Object.entries(value).forEach(([key, item]) => {
                if (['reason_code', 'reason_codes'].includes(key)) return;
                if (['diagnostics', 'group_results', 'groups', 'substage_results', 'router'].includes(key)) {
                    visit(item, depth + 1);
                }
            });
        }
        array(values).forEach(value => visit(value, 0));
        return found;
    }

    function humanizeReasonCode(value) {
        const code = String(value || '').trim();
        if (!code) return '';
        const normalized = code.toUpperCase();
        return REASON_LABELS[code] || REASON_LABELS[normalized]
            || 'Этап завершён с диагностикой. Внутренний код доступен в деталях.';
    }

    function reasonSummary(sources, stale) {
        if (stale) {
            return 'Результат относится к прежней версии входных данных; перед решениями нужен повторный запуск.';
        }
        const codes = collectReasonCodes(sources);
        const mapped = codes.find(code => (
            REASON_LABELS[code] || REASON_LABELS[String(code).toUpperCase()]
        ));
        return codes.length ? humanizeReasonCode(mapped || codes[0]) : '';
    }

    function questionsFrom(payload) {
        if (Array.isArray(payload)) return payload;
        return array(payload && payload.questions);
    }

    function normalizeQuestionCounts(payload) {
        const source = payload && typeof payload.counts === 'object' ? payload.counts : {};
        const counts = {SHEET: 0, ENTITY: 0, CHANGE: 0, total: 0};
        const hasServerCounts = QUESTION_CATEGORIES.some(category =>
            Number.isFinite(Number(source[category] !== undefined ? source[category] : source[category.toLowerCase()]))
        );
        if (hasServerCounts) {
            QUESTION_CATEGORIES.forEach(category => {
                const value = source[category] !== undefined ? source[category] : source[category.toLowerCase()];
                counts[category] = Math.max(0, Number(value) || 0);
            });
        } else {
            questionsFrom(payload).forEach(question => {
                const category = String(question && question.category || '').toUpperCase();
                if (QUESTION_CATEGORIES.includes(category)) counts[category] += 1;
            });
        }
        counts.total = QUESTION_CATEGORIES.reduce((sum, category) => sum + counts[category], 0);
        return counts;
    }

    function normalizeQuestions(payload) {
        return questionsFrom(payload).map((question, index) => {
            const answerRecord = question && typeof question.answer === 'object'
                ? question.answer
                : (question && typeof question.human_answer === 'object' ? question.human_answer : {});
            const answerValue = question && typeof question.answer === 'string'
                ? question.answer
                : (answerRecord.answer || question.selected_answer || '');
            return {
                question_id: String(question.question_id || `question-${index + 1}`),
                category: String(question.category || 'CHANGE').toUpperCase(),
                question_type: String(question.question_type || ''),
                prompt: text(question.prompt || question.question),
                options: array(question.options || question.answer_options).map(option => {
                    if (option && typeof option === 'object') {
                        return {
                            value: String(option.value || option.id || option.code || option.label || ''),
                            label: text(option.label || option.title || option.value || option.id),
                        };
                    }
                    return {value: String(option), label: text(option)};
                }).filter(option => option.value),
                status: String(question.status || 'PENDING').toUpperCase(),
                answer: answerValue,
                author: answerRecord.author || '',
                comment: answerRecord.comment || '',
                selected_refs: array(answerRecord.selected_refs).map(String),
                explicit_candidate: answerRecord.explicit_candidate
                    && typeof answerRecord.explicit_candidate === 'object'
                    ? {...answerRecord.explicit_candidate}
                    : null,
                typed_resolution: answerRecord.typed_resolution
                    && typeof answerRecord.typed_resolution === 'object'
                    ? {...answerRecord.typed_resolution}
                    : null,
                context: question.context || {},
                input_signature: question.input_signature || '',
                raw: question,
            };
        });
    }

    function firstStage(values) {
        return array(values).map(object).find(value => Object.keys(value).length) || {};
    }

    function substageRecord(id, label, source, fallback, specs, missingNote) {
        const reported = Object.keys(object(source)).length > 0;
        const effective = reported ? object(source) : object(fallback);
        const sources = reported ? [source, fallback] : [fallback];
        return {
            id,
            label,
            status: Object.keys(effective).length ? statusOf(effective) : 'NOT_STARTED',
            counters: countersFrom(array(specs).map(spec => ({...spec, sources}))),
            reason: reasonSummary(sources, Boolean(effective.stale)),
            reason_codes: collectReasonCodes(sources),
            note: !reported && Object.keys(effective).length ? missingNote : '',
            raw: reported ? object(source) : {},
        };
    }

    function textPipelineSubstages(stages) {
        const textStage = object(stages.text);
        const nested = object(textStage.substages);
        const components = object(textStage.components);
        const preparation = firstStage([
            stages.text_preparation, nested.preparation, components.preparation,
            textStage.preparation,
        ]);
        const differences = firstStage([
            stages.text_differences, nested.deterministic_diff, nested.differences,
            components.deterministic_diff, textStage.deterministic_diff,
        ]);
        const semantic = firstStage([
            stages.text_semantic_validation, nested.semantic_validation,
            components.semantic_validation, textStage.semantic_validation,
        ]);
        const atoms = firstStage([
            stages.text_atoms, nested.text_atoms, nested.atoms,
            components.text_atoms, textStage.text_atoms,
        ]);
        const inheritedNote = 'Этап выполнен внутри TEXT; backend не опубликовал отдельную метрику.';
        return [
            substageRecord('text-preparation', 'Preparation', preparation, textStage, [
                {label: 'Фрагменты', keys: ['fragments', 'fragment_count', 'fragments_total']},
                {label: 'Группы', keys: ['groups', 'groups_total']},
            ], inheritedNote),
            substageRecord('text-diff', 'Deterministic Diff', differences, textStage, [
                {label: 'Дельты', keys: ['deltas', 'differences', 'difference_count', 'deltas_total']},
                {label: 'Изменено', keys: ['changed']},
                {label: 'Добавлено', keys: ['added']},
                {label: 'Удалено', keys: ['removed']},
            ], inheritedNote),
            substageRecord('text-semantic', 'Semantic Validation', semantic, textStage, [
                {label: 'Факты', keys: ['facts', 'facts_total', 'validated_facts']},
                {label: 'Автоматически', keys: ['automatic', 'automatic_facts', 'validated']},
                {label: 'На проверку', keys: ['review_required', 'unresolved']},
                {label: 'Неприменимо', keys: ['not_applicable']},
            ], inheritedNote),
            substageRecord('text-atoms', 'Text Atoms', atoms, textStage, [
                {label: 'Атомы', keys: ['atoms', 'atom_count', 'atoms_total']},
                {label: 'Автоматически', keys: ['automatic_atoms', 'automatic']},
                {label: 'На проверку', keys: ['review_required', 'review_atoms']},
                {label: 'Неприменимо', keys: ['not_applicable']},
            ], inheritedNote),
        ];
    }

    function graphicPipelineSubstages(stages) {
        const graphic = object(stages.graphic);
        const nested = object(graphic.substages);
        const components = object(graphic.components);
        const results = array(graphic.group_results);
        const route = String(graphic.route || '').toUpperCase();
        const mode = String(graphic.mode || '').toUpperCase();
        const explicitRouter = firstStage([
            stages.graphic_router, nested.router, components.router, graphic.router,
        ]);
        const router = substageRecord('graphic-router', 'Router', explicitRouter, graphic, [
            {label: 'Группы', keys: ['groups_total']},
            {label: 'Готово', keys: ['groups_completed']},
            {label: 'Заблокировано', keys: ['groups_blocked']},
        ], 'Маршрутизация опубликована в общем GRAPHIC stage.');

        function derivedBranch(id, label, names, selected) {
            const explicit = firstStage(names);
            if (Object.keys(explicit).length) {
                return substageRecord(id, label, explicit, graphic, [
                    {label: 'Группы', keys: ['groups', 'groups_total', 'groups_completed']},
                    {label: 'Изменения', keys: ['changes']},
                ], '');
            }
            const selectedResults = results.filter(selected);
            if (selectedResults.length) {
                return {
                    id,
                    label,
                    status: aggregatePipelineStatus(selectedResults.map(statusOf)),
                    counters: [
                        {label: 'Группы', value: selectedResults.length},
                        {label: 'Изменения', value: selectedResults.reduce(
                            (sum, item) => sum + (finiteNumber(item.changes) || 0), 0,
                        )},
                    ],
                    reason: reasonSummary(selectedResults, Boolean(graphic.stale)),
                    reason_codes: collectReasonCodes(selectedResults),
                    note: '',
                    raw: {group_results: selectedResults},
                };
            }
            const branchSelected = selected({route, mode});
            return {
                id,
                label,
                status: !Object.keys(graphic).length
                    ? 'NOT_STARTED'
                    : branchSelected ? statusOf(graphic) : 'NOT_APPLICABLE',
                counters: branchSelected ? countersFrom([
                    {label: 'Изменения', keys: ['changes'], sources: [graphic]},
                ]) : [],
                reason: branchSelected ? reasonSummary([graphic], Boolean(graphic.stale)) : '',
                reason_codes: branchSelected ? collectReasonCodes([graphic]) : [],
                note: branchSelected ? 'Отдельная метрика ветки backend не опубликована.' : '',
                raw: {},
            };
        }

        const mode1 = derivedBranch(
            'graphic-mode-1', 'MODE 1',
            [stages.graphic_mode_1, nested.mode_1, components.mode_1, graphic.mode_1],
            item => String(item.route || route).toUpperCase() === 'MODE_1_APPLICABLE'
                || String(item.mode || mode).toUpperCase() === 'MODE_1',
        );
        const mode2 = derivedBranch(
            'graphic-mode-2', 'MODE 2',
            [stages.graphic_mode_2, nested.mode_2, components.mode_2, graphic.mode_2],
            item => String(item.route || route).toUpperCase() === 'MODE_2_REQUIRED'
                || String(item.mode || mode).toUpperCase() === 'MODE_2',
        );
        const vision = derivedBranch(
            'graphic-vision', 'Vision fallback',
            [stages.graphic_vision, nested.vision, nested.vision_fallback,
                components.vision, graphic.vision_fallback],
            item => String(item.route || route).toUpperCase() === 'VISION_REQUIRED'
                || String(item.mode || mode).toUpperCase() === 'VISION',
        );
        if (vision.status === 'PARTIAL' && (route === 'VISION_REQUIRED' || mode === 'VISION')) {
            vision.status = 'NEEDS_REVIEW';
        }
        return [router, mode1, mode2, vision];
    }

    function pipelineStatusLabel(value) {
        return ({
            NOT_STARTED: 'Не начато',
            RUNNING: 'Выполняется',
            NEEDS_REVIEW: 'Нужна проверка',
            COMPLETED: 'Готово',
            FAILED: 'Ошибка',
            PARTIAL: 'Частично',
            NOT_APPLICABLE: 'Неприменимо',
        })[normalizePipelineStatus(value)] || 'Не начато';
    }

    function normalizeProductionPipeline(payload) {
        const wrapper = object(payload);
        const state = object(wrapper.state);
        const stages = object(state.stages);
        const questionsArtifact = wrapper.questions;
        const changesArtifact = wrapper.changes;
        const finalReport = object(wrapper.final_report || wrapper.finalReport);
        const stale = Boolean(state.stale);
        const selection = object(state.selection);
        const questionRows = normalizeQuestions(questionsArtifact);
        const questionCounts = normalizeQuestionCounts(questionsArtifact);
        const questionsStage = object(stages.review_questions);
        const explicitQuestionTotal = firstNumber([questionsStage], ['questions', 'total']);
        const questionTotal = questionRows.length || questionCounts.total
            || explicitQuestionTotal || 0;
        const explicitAnswered = firstNumber([questionsStage], ['answered', 'answers']);
        const answeredKnown = questionRows.length > 0
            || Boolean(object(questionsArtifact).questions)
            || explicitAnswered !== null;
        const answered = questionRows.length
            ? questionRows.filter(question => Boolean(String(question.answer || '').trim())
                || ['ANSWERED', 'RESOLVED', 'CLOSED'].includes(question.status)).length
            : (explicitAnswered === null ? 0 : explicitAnswered);
        const reviewRows = normalizeRows(changesArtifact);
        const persistedCounts = reviewCounts(reviewRows);
        const decisionsStage = object(stages.engineer_decisions);
        const decisionsCounts = object(decisionsStage.counts);
        const hasReviewRows = Array.isArray(changesArtifact)
            || Array.isArray(object(changesArtifact).rows);
        const stageReviewTotal = firstNumber([decisionsCounts], ['total']);
        const rowsAreAuthoritative = hasReviewRows
            && (reviewRows.length > 0 || stageReviewTotal === null || stageReviewTotal === 0);
        const reviewTotal = rowsAreAuthoritative
            ? persistedCounts.total
            : (stageReviewTotal || 0);
        const reviewApproved = rowsAreAuthoritative
            ? persistedCounts.APPROVED
            : (firstNumber([decisionsCounts], ['APPROVED', 'approved']) || 0);
        const reviewRejected = rowsAreAuthoritative
            ? persistedCounts.REJECTED
            : (firstNumber([decisionsCounts], ['REJECTED', 'rejected']) || 0);
        const reviewPending = rowsAreAuthoritative
            ? persistedCounts.PENDING_REVIEW
            : (firstNumber([decisionsCounts], ['PENDING_REVIEW', 'pending']) || 0);
        const finalStage = object(stages.final_report);
        const finalApproved = firstNumber([
            object(finalReport.summary), finalStage,
        ], ['approved']);
        const approvedInReport = finalApproved !== null
            ? finalApproved
            : array(finalReport.approved_atomic_changes).length;

        function stageRecord(id, number, label, status, counters, sources, destination, extra) {
            const sourceList = array(sources);
            return {
                id, number, label,
                status,
                status_label: pipelineStatusLabel(status),
                counters: array(counters),
                reason: reasonSummary(sourceList, stale),
                reason_codes: collectReasonCodes(sourceList),
                destination,
                raw: sourceList.reduce((result, source, index) => {
                    if (source && typeof source === 'object' && Object.keys(source).length) {
                        result[`source_${index + 1}`] = source;
                    }
                    return result;
                }, {}),
                ...(extra || {}),
            };
        }

        const selectionCounters = [];
        if (Array.isArray(selection.left_pages) && selection.left_pages.length) {
            selectionCounters.push({label: 'LEFT листы', value: selection.left_pages.length});
        }
        if (Array.isArray(selection.right_pages) && selection.right_pages.length) {
            selectionCounters.push({label: 'RIGHT листы', value: selection.right_pages.length});
        }
        const selectionStatus = Object.keys(selection).length ? 'COMPLETED'
            : normalizePipelineStatus(state.status);
        const sheetMatching = object(stages.sheet_matching);
        const sheetScope = object(stages.sheet_scope);
        const sheetCounters = countersFrom([
            {label: 'Связи', keys: ['relations'], sources: [sheetMatching]},
            {label: 'Группы', keys: ['groups'], sources: [sheetScope]},
        ]);
        const relationCounts = firstStage([
            sheetMatching.relation_counts, sheetMatching.counts,
            object(sheetMatching.diagnostics).relation_counts,
        ]);
        [
            ['HIGH', 'HIGH'], ['POSSIBLE', 'POSSIBLE'], ['SPLIT', 'SPLIT'],
            ['MERGED', 'MERGED'], ['NO_MATCH', 'Без пары'],
        ].forEach(([key, label]) => {
            const value = finiteNumber(relationCounts[key]);
            if (value !== null) sheetCounters.push({label, value});
        });

        const text = object(stages.text);
        const graphic = object(stages.graphic);
        const textSubstages = textPipelineSubstages(stages);
        const graphicSubstages = graphicPipelineSubstages(stages);
        const contentCounters = countersFrom([
            {label: 'TEXT дельты', keys: ['deltas', 'differences', 'deltas_total'], sources: [text]},
            {label: 'TEXT атомы', keys: ['atoms', 'atoms_total'], sources: [text]},
            {label: 'TEXT авто', keys: ['automatic_atoms', 'automatic'], sources: [text]},
            {label: 'TEXT на проверку', keys: ['review_required', 'review_atoms'], sources: [text]},
            {label: 'TEXT неприменимо', keys: ['not_applicable'], sources: [text]},
            {label: 'GRAPHIC группы', keys: ['groups_total'], sources: [graphic]},
            {label: 'GRAPHIC готово', keys: ['groups_completed'], sources: [graphic]},
            {label: 'GRAPHIC неприменимо', keys: ['groups_not_applicable', 'not_applicable'], sources: [graphic]},
            {label: 'GRAPHIC на проверку', keys: ['groups_review_required', 'review_required'], sources: [graphic]},
            {label: 'GRAPHIC изменения', keys: ['changes'], sources: [graphic]},
        ]);

        const entityMatching = object(stages.entity_matching);
        const entityBinding = object(stages.effective_entity_binding);
        const rawBinding = object(stages.entity_binding);
        const reviewApplication = object(stages.review_application);
        const automaticSynthesis = object(stages.automatic_unified_synthesis);
        const synthesis = object(stages.unified_synthesis);

        const questionCategoryCounters = questionCounts.total || questionRows.length
            ? [
                {label: 'Листы', value: questionCounts.SHEET},
                {label: 'Объекты', value: questionCounts.ENTITY},
                {label: 'Изменения', value: questionCounts.CHANGE},
            ]
            : [];
        if (questionTotal || answeredKnown) {
            questionCategoryCounters.push({
                label: 'Ответы', value: answeredKnown ? `${answered} / ${questionTotal}` : `— / ${questionTotal}`,
            });
        }
        const categoryDetails = QUESTION_CATEGORIES.map(category => ({
            category,
            label: ({SHEET: 'Листы', ENTITY: 'Объекты', CHANGE: 'Изменения'})[category],
            total: questionCounts[category],
            answered: questionRows.filter(question => question.category === category
                && (Boolean(String(question.answer || '').trim())
                    || ['ANSWERED', 'RESOLVED', 'CLOSED'].includes(question.status))).length,
        }));
        const questionStatus = statusOf(questionsStage) === 'FAILED'
            ? 'FAILED'
            : questionTotal > answered ? 'NEEDS_REVIEW'
                : Object.keys(questionsStage).length ? 'COMPLETED' : 'NOT_STARTED';
        const reviewStatus = statusOf(decisionsStage) === 'FAILED'
            ? 'FAILED'
            : reviewPending > 0 ? 'NEEDS_REVIEW'
                : Object.keys(decisionsStage).length || hasReviewRows ? 'COMPLETED' : 'NOT_STARTED';
        const reviewCounters = Object.keys(decisionsStage).length || hasReviewRows ? [
            {label: 'Найдено', value: reviewTotal},
            {label: 'APPROVED', value: reviewApproved},
            {label: 'REJECTED', value: reviewRejected},
            {label: 'PENDING', value: reviewPending},
        ] : [];
        const reportCounters = Object.keys(finalStage).length || Object.keys(finalReport).length ? [
            {label: 'Найдено', value: reviewTotal},
            {label: 'APPROVED', value: reviewApproved},
            {label: 'REJECTED', value: reviewRejected},
            {label: 'PENDING', value: reviewPending},
            {label: 'Войдёт в отчёт', value: approvedInReport},
        ] : [];

        return [
            stageRecord('selection', 1, 'Выбор сравнения', selectionStatus,
                selectionCounters, [selection], {tab: 'upload'}, {
                    details: [selection.input_mode || state.input_mode
                        ? `Режим: ${selection.input_mode || state.input_mode}` : ''],
                }),
            stageRecord('sheets', 2, 'Сопоставление листов',
                aggregatePipelineStatus([statusOf(sheetMatching), statusOf(sheetScope)]),
                sheetCounters, [sheetMatching, sheetScope], {tab: 'links'}),
            stageRecord('content', 3, 'Анализ содержимого',
                aggregatePipelineStatus([statusOf(text), statusOf(graphic)]),
                contentCounters, [text, graphic], {tab: 'diffs', anchor: 'sc-production-review-stage'}, {
                    sections: [
                        {id: 'text', label: 'TEXT', substages: textSubstages},
                        {id: 'graphic', label: 'GRAPHIC', substages: graphicSubstages},
                    ],
                }),
            stageRecord('objects', 4, 'Сопоставление объектов',
                aggregatePipelineStatus([
                    statusOf(entityMatching), statusOf(entityBinding), statusOf(rawBinding),
                ]), countersFrom([
                    {label: 'Связи', keys: ['relations'], sources: [entityMatching]},
                    {label: 'Связано атомов', keys: ['bound_atoms'], sources: [entityBinding, rawBinding]},
                ]), [entityMatching, entityBinding, rawBinding],
                {tab: 'diffs', anchor: 'sc-production-review-stage'}),
            stageRecord('questions', 5, 'Вопросы инженеру', questionStatus,
                questionCategoryCounters, [questionsStage],
                {tab: 'diffs', anchor: 'sc-production-questions-stage'}, {
                    progress: {answered: answeredKnown ? answered : null, total: questionTotal},
                    categories: categoryDetails,
                    reason: stale ? reasonSummary([questionsStage], true) : questionTotal > answered
                        ? `Осталось ответить: ${questionTotal - answered}.`
                        : '',
                }),
            stageRecord('synthesis', 6, 'Синтез изменений',
                aggregatePipelineStatus([
                    statusOf(automaticSynthesis), statusOf(reviewApplication), statusOf(synthesis),
                ]), countersFrom([
                    {label: 'Авто изменения', keys: ['changes'], sources: [automaticSynthesis]},
                    {label: 'Авто на проверку', keys: ['review_items'], sources: [automaticSynthesis]},
                    {label: 'Применено ответов', keys: ['applied_decisions'], sources: [reviewApplication]},
                    {label: 'Итого изменений', keys: ['changes'], sources: [synthesis]},
                    {label: 'Итого на проверку', keys: ['review_items'], sources: [synthesis]},
                ]), [automaticSynthesis, reviewApplication, synthesis],
                {tab: 'diffs', anchor: 'sc-production-review-stage'}),
            stageRecord('review', 7, 'Проверка инженером', reviewStatus,
                reviewCounters, [decisionsStage], {tab: 'diffs', anchor: 'sc-production-review-table'}, {
                    reason: stale ? reasonSummary([decisionsStage], true) : reviewPending > 0
                        ? `Без решения инженера: ${reviewPending}.`
                        : '',
                }),
            stageRecord('report', 8, 'Итоговый отчёт',
                Object.keys(finalStage).length || Object.keys(finalReport).length
                    ? statusOf(finalStage.status ? finalStage : {status: 'COMPLETED'})
                    : 'NOT_STARTED', reportCounters, [finalStage], {tab: 'report'}, {
                    approved_only: object(finalReport.constraints).approved_only === true,
                }),
        ];
    }

    function point(value) {
        if (Array.isArray(value) && value.length >= 2) {
            return {x: Number(value[0]), y: Number(value[1])};
        }
        if (value && typeof value === 'object') {
            return {x: Number(value.x), y: Number(value.y)};
        }
        return null;
    }

    function finitePoint(value) {
        return value && Number.isFinite(value.x) && Number.isFinite(value.y);
    }

    function unitsFor(values) {
        return values.length && values.every(value => Number.isFinite(value) && value >= 0 && value <= 1)
            ? 'normalized'
            : 'absolute';
    }

    function regionFromBox(value) {
        let x;
        let y;
        let width;
        let height;
        if (Array.isArray(value) && value.length >= 4) {
            const numbers = value.slice(0, 4).map(Number);
            if (!numbers.every(Number.isFinite)) return null;
            x = numbers[0];
            y = numbers[1];
            width = numbers[2] - numbers[0];
            height = numbers[3] - numbers[1];
        } else if (value && typeof value === 'object') {
            x = Number(value.x !== undefined ? value.x : value.x0);
            y = Number(value.y !== undefined ? value.y : value.y0);
            width = Number(value.width !== undefined ? value.width : Number(value.x1) - x);
            height = Number(value.height !== undefined ? value.height : Number(value.y1) - y);
        }
        if (![x, y, width, height].every(Number.isFinite) || width <= 0 || height <= 0) return null;
        return {kind: 'BBOX', x, y, width, height, units: unitsFor([x, y, x + width, y + height])};
    }

    function regionFromPolygon(value) {
        const points = array(value).map(point).filter(finitePoint);
        if (points.length < 3) return null;
        const xs = points.map(item => item.x);
        const ys = points.map(item => item.y);
        const x = Math.min(...xs);
        const y = Math.min(...ys);
        const width = Math.max(...xs) - x;
        const height = Math.max(...ys) - y;
        if (!(width > 0 && height > 0)) return null;
        return {
            kind: 'POLYGON', x, y, width, height,
            units: unitsFor(points.flatMap(item => [item.x, item.y])),
            polygon: points.map(item => ({x: (item.x - x) / width, y: (item.y - y) / height})),
        };
    }

    function pageSize(location) {
        const value = location && location.page_size;
        if (Array.isArray(value) && value.length >= 2) {
            const width = Number(value[0]);
            const height = Number(value[1]);
            return width > 0 && height > 0 ? {width, height} : null;
        }
        if (value && typeof value === 'object') {
            const width = Number(value.width);
            const height = Number(value.height);
            return width > 0 && height > 0 ? {width, height} : null;
        }
        return null;
    }

    function normalizeRegionCoordinates(region, location) {
        if (!region) return null;
        const coordinateSpace = String(location && location.coordinate_space || '').toUpperCase();
        if (coordinateSpace === 'NORMALIZED_PAGE_TOP_LEFT') {
            if (region.units !== 'normalized') return null;
            return region;
        }
        if (coordinateSpace === 'PDF_VISUAL_PT' || region.units === 'absolute') {
            const size = pageSize(location);
            // Absolute PDF points without their exact page size cannot be
            // placed honestly in the normalized browser viewer.
            if (!size) return null;
            return {
                ...region,
                x: region.x / size.width,
                y: region.y / size.height,
                width: region.width / size.width,
                height: region.height / size.height,
                units: 'normalized',
            };
        }
        return region.units === 'normalized' ? region : null;
    }

    function regionsFromHighlight(highlight, location) {
        if (!highlight || typeof highlight !== 'object') return [];
        const kind = String(highlight.kind || '').toUpperCase();
        if (kind === 'POLYGON') {
            const region = normalizeRegionCoordinates(regionFromPolygon(highlight.polygon), location);
            return region ? [region] : [];
        }
        if (kind === 'BBOX_SET') {
            return array(highlight.bboxes)
                .map(regionFromBox)
                .map(region => normalizeRegionCoordinates(region, location))
                .filter(Boolean);
        }
        const region = normalizeRegionCoordinates(regionFromBox(highlight.bbox || highlight), location);
        return region ? [region] : [];
    }

    function normalizeEvidence(payload) {
        const sourceSides = payload && payload.sides && typeof payload.sides === 'object'
            ? payload.sides
            : {};
        const result = {
            target_id: String(payload && payload.target_id || ''),
            source_mode: String(payload && payload.source_mode || 'UNKNOWN').toUpperCase(),
            layout: String(payload && payload.layout || 'SIDE_BY_SIDE').toUpperCase(),
            sides: {},
            trace: array(payload && payload.trace),
            raw: payload || {},
        };
        for (const upper of ['LEFT', 'RIGHT']) {
            const locations = array(sourceSides[upper] || sourceSides[upper.toLowerCase()]);
            const hasPage = location => location && location.page !== null
                && location.page !== undefined && location.page !== ''
                && Number.isInteger(Number(location.page)) && Number(location.page) > 0;
            const firstPageLocation = locations.find(hasPage);
            const page = firstPageLocation ? Number(firstPageLocation.page) : null;
            const pages = uniqueNumbers(locations.filter(hasPage).map(location => location.page));
            const overlays = [];
            locations.forEach((location, locationIndex) => {
                const locationPage = hasPage(location)
                    ? Number(location.page)
                    : null;
                regionsFromHighlight(location && location.highlight, location).forEach((region, regionIndex) => {
                    overlays.push({
                        ...region,
                        id: `${upper}-${locationIndex}-${regionIndex}`,
                        page: locationPage,
                        source: String(location.source || 'UNKNOWN').toUpperCase(),
                        block_id: location.block_id || null,
                        node_id: location.node_id || null,
                        fragment_id: location.fragment_id || null,
                    });
                });
            });
            result.sides[upper.toLowerCase()] = {
                side: upper,
                page,
                pages,
                locations,
                overlays,
                has_evidence: locations.length > 0,
                coordinates_available: overlays.length > 0,
                coordinates_missing: locations.length > 0 && overlays.length === 0,
            };
        }
        result.has_any_coordinates = result.sides.left.coordinates_available
            || result.sides.right.coordinates_available;
        result.has_both_sides = result.sides.left.has_evidence && result.sides.right.has_evidence;
        return result;
    }

    function evidenceFocus(side) {
        const page = side && side.page;
        const overlays = array(side && side.overlays).filter(region => (
            page === null || page === undefined || Number(region.page) === Number(page)
        ));
        if (!overlays.length) return null;
        const units = overlays[0].units;
        const compatible = overlays.filter(region => region.units === units);
        const x0 = Math.min(...compatible.map(region => region.x));
        const y0 = Math.min(...compatible.map(region => region.y));
        const x1 = Math.max(...compatible.map(region => region.x + region.width));
        const y1 = Math.max(...compatible.map(region => region.y + region.height));
        return {x: (x0 + x1) / 2, y: (y0 + y1) / 2, width: x1 - x0, height: y1 - y0, units};
    }

    function normalizeFinalRows(report) {
        const approved = array(report && report.approved_atomic_changes);
        return normalizeRows(approved.map(change => ({
            target_id: change.change_id,
            target_kind: 'CHANGE',
            change,
            engineer_decision: {
                ...(change.engineer_decision || {}),
                decision: 'APPROVED',
            },
        })));
    }

    function normalizeSheetSuggestions(state) {
        const raw = state && state.sheet_suggestions;
        const suggestions = Array.isArray(raw) ? raw : array(raw && raw.suggestions);
        return suggestions.map((item, index) => {
            const selectedLeftPages = uniqueNumbers(item.selected_left_pages || []);
            const selectedRightPages = uniqueNumbers(item.selected_right_pages || []);
            const leftPages = uniqueNumbers(
                item.suggested_left_pages || item.left_pages
                || item.selected_left_pages || [item.left_page]
            );
            const rightPages = uniqueNumbers(
                item.suggested_right_pages || item.right_pages || item.candidate_right_pages
                || item.selected_right_pages || [item.right_page || item.candidate_right_page]
            );
            return {
                id: String(item.suggestion_id || item.question_id || `sheet-suggestion-${index + 1}`),
                question_id: item.question_id || null,
                selected_left_pages: selectedLeftPages,
                selected_right_pages: selectedRightPages,
                left_pages: leftPages,
                right_pages: rightPages,
                confidence: confidence(item.confidence || item.status),
                message: text(item.message || item.recommendation,
                    `Для LEFT ${leftPages.join(', ') || '—'} найден кандидат RIGHT ${rightPages.join(', ') || '—'}`),
                raw: item,
            };
        });
    }

    function normalizePageGroup(value) {
        if (!value || typeof value !== 'object' || Array.isArray(value)) return null;
        const leftPages = uniqueNumbers(value.left_pages || value.selected_left_pages);
        const rightPages = uniqueNumbers(value.right_pages || value.selected_right_pages);
        if (!leftPages.length || !rightPages.length) return null;
        return {left_pages: leftPages, right_pages: rightPages};
    }

    function firstBoolean(values) {
        const found = values.find(value => typeof value === 'boolean');
        return typeof found === 'boolean' ? found : null;
    }

    function normalizeSuggestionApplication(payload, suggestionId) {
        const wrapper = payload && typeof payload === 'object' ? payload : {};
        const state = wrapper.state && typeof wrapper.state === 'object'
            ? wrapper.state
            : wrapper;
        const questions = wrapper.review_questions && typeof wrapper.review_questions === 'object'
            ? wrapper.review_questions
            : (wrapper.production_review_questions
                && typeof wrapper.production_review_questions === 'object'
                ? wrapper.production_review_questions
                : {});
        const application = wrapper.application && typeof wrapper.application === 'object'
            ? wrapper.application
            : (questions.application && typeof questions.application === 'object'
                ? questions.application
                : {});
        const diagnostics = application.diagnostics && typeof application.diagnostics === 'object'
            ? application.diagnostics
            : (wrapper.diagnostics && typeof wrapper.diagnostics === 'object'
                ? wrapper.diagnostics
                : {});
        const stages = state.stages && typeof state.stages === 'object' ? state.stages : {};
        const sheetScope = stages.sheet_scope && typeof stages.sheet_scope === 'object'
            ? stages.sheet_scope
            : {};
        const generationScope = state.generation_scope
            && typeof state.generation_scope === 'object'
            ? state.generation_scope
            : {};
        const semantics = wrapper.suggestion_action_semantics
            && typeof wrapper.suggestion_action_semantics === 'object'
            ? wrapper.suggestion_action_semantics
            : (questions.suggestion_action_semantics
                && typeof questions.suggestion_action_semantics === 'object'
                ? questions.suggestion_action_semantics
                : (state.suggestion_action_semantics
                    && typeof state.suggestion_action_semantics === 'object'
                    ? state.suggestion_action_semantics
                    : {}));
        const actions = wrapper.suggestion_actions && typeof wrapper.suggestion_actions === 'object'
            ? wrapper.suggestion_actions
            : (questions.suggestion_actions && typeof questions.suggestion_actions === 'object'
                ? questions.suggestion_actions
                : (state.suggestion_actions && typeof state.suggestion_actions === 'object'
                    ? state.suggestion_actions
                    : {}));
        const outcome = array(semantics.outcomes).find(item => (
            item && String(item.suggestion_id || '') === String(suggestionId || '')
        )) || {};
        const rawGroupSources = [
            wrapper.effective_page_groups,
            diagnostics.effective_page_groups,
            diagnostics.page_groups,
            semantics.effective_page_groups,
            state.effective_page_groups,
            state.page_groups,
            sheetScope.effective_page_groups,
            generationScope.page_groups,
            Array.isArray(sheetScope.groups) ? sheetScope.groups : null,
        ];
        let groups = [];
        for (const source of rawGroupSources) {
            const normalized = array(source).map(normalizePageGroup).filter(Boolean);
            if (normalized.length) {
                groups = normalized;
                break;
            }
        }
        if (!groups.length) {
            const selection = diagnostics.effective_selection
                || wrapper.effective_selection
                || state.effective_selection
                || state.selection;
            const group = normalizePageGroup(selection);
            if (group) groups = [group];
        }
        return {
            action: String(actions[String(suggestionId || '')] || outcome.action || ''),
            state: String(outcome.state || semantics.state || diagnostics.state || ''),
            scope_applied: outcome.state === 'IGNORED' ? false : firstBoolean([
                outcome.scope_applied,
                wrapper.scope_applied,
                diagnostics.scope_applied,
                semantics.scope_applied,
                sheetScope.scope_applied,
            ]),
            pipeline_rerun: firstBoolean([
                outcome.pipeline_rerun,
                wrapper.pipeline_rerun,
                diagnostics.pipeline_rerun,
                semantics.pipeline_rerun,
                sheetScope.pipeline_rerun,
            ]),
            this_update_reran: firstBoolean([
                outcome.this_update_reran,
                wrapper.this_update_reran,
                diagnostics.this_update_reran,
                semantics.this_update_reran,
                sheetScope.this_update_reran,
            ]),
            generation_was_materialized: firstBoolean([
                wrapper.generation_was_materialized,
                diagnostics.generation_was_materialized,
                semantics.generation_was_materialized,
                sheetScope.generation_was_materialized,
            ]),
            generation_run_id: String(
                diagnostics.generation_run_id || wrapper.generation_run_id
                || semantics.generation_run_id || state.generation_run_id
                || state.run_id || '',
            ),
            groups,
        };
    }

    return {
        PIPELINE_STATUSES,
        QUESTION_CATEGORIES,
        REVIEW_DECISIONS,
        decision,
        evidenceFocus,
        humanizeReasonCode,
        normalizeEvidence,
        normalizeFinalRows,
        normalizePipelineStatus,
        normalizeProductionPipeline,
        normalizeQuestionCounts,
        normalizeQuestions,
        normalizeRows,
        normalizeSheetSuggestions,
        normalizeSuggestionApplication,
        pagesForSide,
        pipelineStatusLabel,
        reviewCounts,
        text,
    };
}));
