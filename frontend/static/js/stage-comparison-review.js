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
            'Детерминированный маршрутизатор не смог завершить графическое сравнение; нужна визуальная резервная проверка.',
        EXTRACTION_COMPLETENESS_INSUFFICIENT:
            'Извлечённая геометрия не покрывает видимую графику достаточно полно. Нужна визуальная резервная или ручная проверка.',
        RASTER_BACKED_SOURCE:
            'Источник содержит растровую графику, которую нельзя надёжно проверить только по векторной геометрии.',
        TEXT_AS_CURVES_ASYMMETRY:
            'На сторонах различается представление текста как текста и кривых; автоматическое сравнение остановлено.',
        REGISTRATION_FAILED:
            'Графические области не удалось надёжно совместить; результат оставлен для визуальной резервной или ручной проверки.',
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
        DOCUMENT_GRAPHIC_GROUPS_REQUIRE_ATTENTION:
            'Некоторые графические группы документа требуют внимания инженера; автоматически подтверждены не все группы.',
        ProductionStateConflictError:
            'Состояние анализа изменилось параллельно. Обновите данные и повторите действие.',
        OSError:
            'Операционная система не смогла прочитать или записать нужный файл. Проверьте доступность исходных документов и место на диске.',
        FileNotFoundError:
            'Один из нужных файлов подготовки не найден.',
        UnicodeDecodeError:
            'Исходный текст не удалось прочитать в ожидаемой кодировке.',
        ValueError:
            'Один из артефактов не прошёл проверку формата.',
        RuntimeError:
            'Детерминированный этап не смог завершиться.',
    };

    // ── Русский словарь инженера ──────────────────────────────────────────
    //
    // Инженер видит проект, а не внутренности алгоритма. Всё, что попадает в
    // вопрос, кнопку, статус, предупреждение, ошибку, причину или
    // рекомендацию, обязано быть по-русски и на языке проекта. Внутренний код
    // остаётся — но в «Диагностике», куда за ним идут осознанно.
    //
    // Словарь один на весь раздел: два разошедшихся перевода одного кода
    // хуже, чем сырой код, потому что сырой код хотя бы честен.

    const DIMENSION_LABELS = {
        PRINCIPLE: 'проектное решение',
        METHOD: 'способ выполнения',
        OPERATION: 'порядок работы',
        STRUCTURE: 'состав',
        CONNECTION: 'связь',
        TYPE: 'тип или марка',
        PARAMETER: 'числовое значение',
        QUANTITY: 'количество',
        SPACE: 'расположение',
        UNKNOWN_DIMENSION: 'тип изменения не определён',
    };
    const DIRECTION_LABELS = {
        ADDED: 'добавлено',
        REMOVED: 'удалено',
        REPLACED: 'заменено',
        INCREASED: 'увеличено',
        DECREASED: 'уменьшено',
        ALTERED: 'изменено',
    };
    const OUTCOME_LABELS = {
        MATERIAL_CHANGE: 'существенное изменение',
        DETAIL_ONLY: 'уточнение без последствий',
        REVIEW_REQUIRED: 'требуется проверка инженера',
    };
    const REVIEW_STATUS_LABELS = {
        REVIEW_REQUIRED: 'требуется проверка инженера',
        CONFIRMED: 'подтверждено',
        CHECK_BLOCKED: 'автоматическая проверка не выполнена',
        NOT_CHECKED: 'проверка не проводилась',
        NOT_APPLICABLE: 'неприменимо',
    };
    const CONFIDENCE_LABELS = {
        HIGH: 'высокая',
        MEDIUM: 'средняя',
        LOW: 'низкая',
        UNKNOWN: 'не определена',
        HUMAN: 'решение инженера',
    };
    const SOURCE_LABELS = {
        TEXT: 'текст',
        GRAPHIC: 'чертёж',
        BOTH: 'текст и чертёж',
        UNKNOWN: 'источник не определён',
    };
    const DECISION_LABELS = {
        PENDING_REVIEW: 'не рассмотрено',
        APPROVED: 'подтверждено',
        REJECTED: 'отклонено',
    };
    const RELATION_TYPE_LABELS = {
        MATCHED: 'один лист слева соответствует одному листу справа',
        SPLIT: 'один лист слева разделён на несколько листов справа',
        MERGED: 'несколько листов слева объединены в один лист справа',
        UNCERTAIN: 'соответствие листов не установлено',
        NO_MATCH: 'соответствия нет',
    };
    const SHEET_STATUS_LABELS = {
        HIGH: 'подтверждено',
        USER_CONFIRMED: 'подтверждено инженером',
        CONFIRMED: 'подтверждено',
        POSSIBLE: 'предложено, ждёт подтверждения',
        UNKNOWN: 'не установлено',
        NO_MATCH: 'соответствия нет',
        CANDIDATE_SUPERSEDED: 'заменено другим вариантом',
    };
    const QUESTION_CATEGORY_LABELS = {
        SHEET: 'Лист',
        ENTITY: 'Объект',
        CHANGE: 'Изменение',
    };
    // Почему элемент вернулся человеку после ИИ-анализа.
    const AI_REASON_LABELS = {
        VERIFIER_REJECTED: 'Автоматическая проверка не подтвердила вывод — нужен инженер.',
        CRITIC_REJECTED: 'Контрольная проверка отклонила вывод — нужен инженер.',
        MODEL_FAILED: 'ИИ-анализ не выполнен, изменение оставлено инженеру.',
        MODEL_TIMEOUT: 'ИИ-анализ не уложился во время, изменение оставлено инженеру.',
        MODEL_DECLINED: 'ИИ не смог решить по имеющимся данным.',
        BUDGET_EXHAUSTED: 'Достигнут предел ИИ-анализа, остаток оставлен инженеру.',
        CANCELLED: 'Анализ был остановлен.',
        VISION_CONTRADICTS_TEXT: 'Чертёж расходится с текстом — нужен инженер.',
        VISION_INSUFFICIENT: 'На чертеже не видно достаточно, чтобы решить.',
    };
    const AI_MODE_LABELS = {
        OFF: 'без ИИ',
        STANDARD: 'стандартный',
        DEEP: 'глубокий',
    };

    function labelFrom(dictionary, value, fallback) {
        const key = String(value === null || value === undefined ? '' : value)
            .toUpperCase();
        if (!key) return fallback === undefined ? '' : fallback;
        if (Object.prototype.hasOwnProperty.call(dictionary, key)) {
            return dictionary[key];
        }
        return fallback === undefined ? '' : fallback;
    }

    function dimensionLabel(value) {
        return labelFrom(DIMENSION_LABELS, value, 'тип изменения не определён');
    }

    function directionLabel(value) {
        return labelFrom(DIRECTION_LABELS, value, 'изменено');
    }

    function outcomeLabel(value) {
        return labelFrom(OUTCOME_LABELS, value, '');
    }

    function reviewStatusLabel(value) {
        return labelFrom(REVIEW_STATUS_LABELS, value, 'требуется проверка инженера');
    }

    function confidenceLabel(value) {
        return labelFrom(CONFIDENCE_LABELS, confidence(value), 'не определена');
    }

    function sourceLabel(value) {
        return labelFrom(SOURCE_LABELS, value, 'источник не определён');
    }

    function decisionLabel(value) {
        return labelFrom(DECISION_LABELS, decision(value), 'не рассмотрено');
    }

    function relationTypeLabel(value) {
        return labelFrom(RELATION_TYPE_LABELS, value, 'соответствие листов не установлено');
    }

    function sheetStatusLabel(value) {
        return labelFrom(SHEET_STATUS_LABELS, value, 'не установлено');
    }

    function questionCategoryLabel(value) {
        return labelFrom(QUESTION_CATEGORY_LABELS, value, 'Вопрос');
    }

    function aiReasonLabel(value) {
        return labelFrom(AI_REASON_LABELS, value, 'Оставлено инженеру.');
    }

    function aiModeLabel(value) {
        return labelFrom(AI_MODE_LABELS, value, 'без ИИ');
    }

    // «Лист 7» — номер из штампа проекта. «стр. PDF 29» — физическая страница
    // файла. Это разные числа, и на одной паре они расходятся почти всегда.
    function sheetReference(sheet) {
        if (!sheet || typeof sheet !== 'object') return '';
        const page = Number(sheet.page);
        const number = sheet.sheet_no === null || sheet.sheet_no === undefined
            ? '' : String(sheet.sheet_no).trim();
        const title = sheet.title || sheet.label || '';
        const head = [];
        if (title) head.push(`«${title}»`);
        if (number) head.push(`лист ${number}`);
        const tail = Number.isInteger(page) && page > 0 ? `стр. PDF ${page}` : '';
        if (!head.length) return tail || '—';
        return tail ? `${head.join(', ')} (${tail})` : head.join(', ');
    }

    function pagesReference(pages) {
        const list = uniqueNumbers(pages);
        if (!list.length) return '—';
        return `стр. PDF ${list.join(', ')}`;
    }

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
        // «PARAMETER · INCREASED» ничего не говорит инженеру. «числовое
        // значение · увеличено» говорит. Внутренний код остаётся доступен
        // в диагностике строки.
        const parts = [];
        if (change.dimension) parts.push(dimensionLabel(change.dimension));
        if (change.direction) parts.push(directionLabel(change.direction));
        const unique = [...new Set(parts.filter(Boolean))];
        if (unique.length) return unique.join(' · ');
        return outcomeLabel(change.outcome) || text(change.outcome || change.type);
    }

    function objectLabel(change) {
        // Хеш project_entity_ref — не имя объекта. Имя приходит из названия,
        // которое дал инженер или производитель фактов; хеш идёт в диагностику.
        const explicit = change.object_label
            || (change.provenance
                && change.provenance.entity
                && change.provenance.entity.original);
        if (explicit) return String(explicit);
        const subject = String(change.subject_ref || '');
        if (subject.startsWith('text_entity:')) {
            const name = subject.slice('text_entity:'.length).split(':')[0];
            if (name) return name.replace(/_/g, ' ');
        }
        return 'Объект не назван';
    }

    function aiExplanation(change) {
        // «Почему система так решила» — краткое обоснование по доказательствам.
        // Внутренних рассуждений модели здесь нет и быть не должно.
        const provenance = change && typeof change.provenance === 'object'
            ? change.provenance : {};
        const atoms = array(provenance.source_atoms);
        const sources = [provenance, ...atoms.map(item => (
            item && typeof item.provenance === 'object' ? item.provenance : {}
        ))];
        for (const source of sources) {
            const record = source && source.ai_change_resolution;
            if (record && record.engineering_summary) {
                return {
                    summary: String(record.engineering_summary),
                    quotes: array(record.evidence_quotes)
                        .filter(item => item && item.quote)
                        .map(item => ({
                            side: String(item.side || '').toUpperCase() === 'RIGHT'
                                ? 'справа' : 'слева',
                            quote: String(item.quote),
                        })),
                    confidence: confidenceLabel(record.confidence),
                    by_ai: true,
                };
            }
        }
        return null;
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
            const presentation = source && typeof source.presentation === 'object'
                ? source.presentation
                : null;
            const targetKind = String(
                source.target_kind || (change.review_evidence_id ? 'REVIEW_EVIDENCE' : 'CHANGE')
            );
            return {
                target_id: targetId,
                target_kind: targetKind,
                // Whether the engineer can decide on this row at all: a CHANGE
                // always, a review finding once it has a value and a page.
                decidable: targetKind !== 'REVIEW_EVIDENCE'
                    || Boolean(presentation && presentation.presentable),
                object_ref: objectLabel(change),
                object_diagnostic: text(
                    change.project_entity_ref || change.subject_ref || change.scope_ref,
                    '',
                ),
                left_pages: leftPages,
                right_pages: rightPages,
                sheets_label: `Было — ${pagesReference(leftPages)}; стало — ${pagesReference(rightPages)}`,
                left_sheets: array(source.left_sheets),
                right_sheets: array(source.right_sheets),
                change_label: changeLabel(change),
                before: text(change.before_value),
                after: text(change.after_value),
                source: String(change.source_mode || change.source || 'UNKNOWN').toUpperCase(),
                source_label: sourceLabel(change.source_mode || change.source),
                status: String(change.review_status || change.outcome || 'CONFIRMED').toUpperCase(),
                status_label: reviewStatusLabel(change.review_status || change.outcome),
                confidence: confidence(change.confidence),
                confidence_label: confidenceLabel(change.confidence),
                explanation: aiExplanation(change),
                decision: decision(engineer.decision),
                decision_label: decisionLabel(engineer.decision),
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
        if (value === null || value === undefined || value === '' || typeof value === 'boolean') {
            return null;
        }
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
        if (['RUNNING', 'UPDATING', 'IN_PROGRESS', 'PROCESSING'].includes(status)) return 'RUNNING';
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

    function productionRunActivity(value) {
        const source = object(value && value.state ? value.state : value);
        const status = String(source.status || '').toUpperCase();
        const hasRunningStatus = ['RUNNING', 'UPDATING'].includes(status);
        const orphaned = hasRunningStatus && (
            source.orphaned_run === true
            || (source.runner_active === false && source.run_recoverable === true)
        );
        return {
            status,
            has_running_status: hasRunningStatus,
            active: hasRunningStatus && !orphaned,
            runner_active: source.runner_active === true,
            is_orphaned: orphaned,
            run_recoverable: orphaned && source.run_recoverable === true,
        };
    }

    function productionPollingDirective(value) {
        const activity = productionRunActivity(value);
        if (activity.is_orphaned) return 'STOP_ORPHANED';
        if (activity.active) return 'POLL_ACTIVE';
        return 'STOP_TERMINAL';
    }

    function productionStateResponseAccepted(value, pendingRun) {
        const pending = object(pendingRun);
        if (!Object.keys(pending).length) return true;
        const source = object(value && value.state ? value.state : value);
        const observedRunId = String(source.run_id || source.generation_run_id || '');
        const previousRunId = String(pending.previousRunId || '');
        const acceptedRunId = String(pending.acceptedRunId || '');
        if (observedRunId && previousRunId && observedRunId === previousRunId) return false;
        if (observedRunId && acceptedRunId && observedRunId !== acceptedRunId) return false;
        return true;
    }

    function productionPollingTransition(runtime, event) {
        const current = object(runtime);
        const action = object(event);
        const type = String(action.type || '').toUpperCase();
        const token = Number.isFinite(Number(current.token)) ? Number(current.token) : 0;
        if (type === 'PAIR_CHANGED' || type === 'STOP') {
            return {polling: false, token: token + 1, pair_id: String(action.pair_id || '')};
        }
        if (type === 'STATE_RECEIVED') {
            const directive = productionPollingDirective(action.state);
            return {
                polling: directive === 'POLL_ACTIVE',
                token: directive === 'POLL_ACTIVE' ? token : token + 1,
                pair_id: String(current.pair_id || ''),
                directive,
            };
        }
        return {
            polling: Boolean(current.polling), token,
            pair_id: String(current.pair_id || ''),
        };
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

    function substageRecord(
        id, label, technicalLabel, source, fallback, specs, missingNote, progressOptions,
    ) {
        const reported = Object.keys(object(source)).length > 0;
        const effective = reported ? object(source) : object(fallback);
        const sources = reported ? [source, fallback] : [fallback];
        const status = Object.keys(effective).length ? statusOf(effective) : 'NOT_STARTED';
        return {
            id,
            label,
            technical_label: technicalLabel,
            reported,
            status,
            counters: countersFrom(array(specs).map(spec => ({...spec, sources}))),
            progress: normalizePipelineProgress(effective, {
                ...(progressOptions || {}), status,
            }),
            reason: reasonSummary(sources, Boolean(effective.stale)),
            reason_codes: collectReasonCodes(sources),
            note: !reported && Object.keys(effective).length ? missingNote : '',
            raw: reported ? object(source) : {},
        };
    }

    function textPipelineSubstages(stages, progressOptions) {
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
        const records = [
            substageRecord('text-preparation', 'Подготовка текста', 'Preparation', preparation, textStage, [
                {label: 'Фрагменты', keys: ['fragments', 'fragment_count', 'fragments_total']},
                {label: 'Группы', keys: ['groups', 'groups_total']},
            ], inheritedNote, progressOptions),
            substageRecord('text-diff', 'Поиск различий', 'Deterministic Diff', differences, textStage, [
                {label: 'Дельты', keys: ['deltas', 'differences', 'difference_count', 'deltas_total']},
                {label: 'Изменено', keys: ['changed']},
                {label: 'Добавлено', keys: ['added']},
                {label: 'Удалено', keys: ['removed']},
            ], inheritedNote, progressOptions),
            substageRecord('text-semantic', 'Проверка различий', 'Semantic Validation', semantic, textStage, [
                {label: 'Факты', keys: ['facts', 'facts_total', 'validated_facts']},
                {label: 'Автоматически', keys: ['automatic', 'automatic_facts', 'validated']},
                {label: 'На проверку', keys: ['review_required', 'unresolved']},
                {label: 'Неприменимо', keys: ['not_applicable']},
            ], inheritedNote, progressOptions),
            substageRecord('text-atoms', 'Формирование изменений', 'Text Atoms', atoms, textStage, [
                {label: 'Атомы', keys: ['atoms', 'atom_count', 'atoms_total']},
                {label: 'Автоматически', keys: ['automatic_atoms', 'automatic']},
                {label: 'На проверку', keys: ['review_required', 'review_atoms']},
                {label: 'Неприменимо', keys: ['not_applicable']},
            ], inheritedNote, progressOptions),
        ];
        const liveSubstage = String(
            progressSource(textStage).current_substage
                || object(progressOptions).current_substage
                || '',
        ).toLowerCase();
        const liveOrder = {
            text_preparation: 0,
            text_difference_search: 1,
            deterministic_diff: 1,
            text_difference_validation: 2,
            semantic_validation: 2,
            text_change_formation: 3,
            text_atoms: 3,
        };
        const liveIndex = liveOrder[liveSubstage];
        if (statusOf(textStage) === 'RUNNING' && Number.isInteger(liveIndex)) {
            records.forEach((record, index) => {
                if (record.reported) return;
                record.status = index < liveIndex
                    ? 'COMPLETED'
                    : index === liveIndex ? 'RUNNING' : 'NOT_STARTED';
                record.progress = normalizePipelineProgress(
                    index === liveIndex ? textStage : {},
                    {...(progressOptions || {}), status: record.status},
                );
            });
        }
        return records;
    }

    function graphicPipelineSubstages(stages, progressOptions) {
        const graphic = object(stages.graphic);
        const nested = object(graphic.substages);
        const components = object(graphic.components);
        const results = array(graphic.group_results);
        const route = String(graphic.route || '').toUpperCase();
        const mode = String(graphic.mode || '').toUpperCase();
        const graphicStatus = statusOf(graphic);
        const liveSubstage = String(
            progressSource(graphic).current_substage || '',
        ).toLowerCase();
        const explicitRouter = firstStage([
            stages.graphic_router, nested.router, components.router, graphic.router,
        ]);
        const router = substageRecord('graphic-router', 'Выбор метода', 'Router', explicitRouter, graphic, [
            {label: 'Группы', keys: ['groups_total']},
            {label: 'Маршрутов', keys: ['router_runs']},
            {label: 'Ошибок маршрута', keys: ['router_failed_groups']},
        ], 'Маршрутизация опубликована в общем GRAPHIC stage.', progressOptions);
        if (!router.reported && Object.keys(graphic).length) {
            const routerFailures = firstNumber([graphic], ['router_failed_groups']) || 0;
            if (graphicStatus === 'RUNNING') {
                // PAGE goes straight into structural comparison after method
                // selection. DOCUMENT keeps routing each independent group.
                router.status = liveSubstage === 'graphic_structural_comparison'
                    ? 'COMPLETED'
                    : 'RUNNING';
            } else if (graphicStatus === 'FAILED') {
                router.status = 'FAILED';
            } else {
                router.status = routerFailures > 0 ? 'PARTIAL' : 'COMPLETED';
            }
            router.progress = normalizePipelineProgress(
                router.status === 'RUNNING' ? graphic : {},
                {...(progressOptions || {}), status: router.status},
            );
        }

        function derivedBranch(id, label, technicalLabel, names, selected) {
            const explicit = firstStage(names);
            if (Object.keys(explicit).length) {
                return substageRecord(id, label, technicalLabel, explicit, graphic, [
                    {label: 'Группы', keys: ['groups', 'groups_total', 'groups_completed']},
                    {label: 'Изменения', keys: ['changes']},
                ], '', progressOptions);
            }
            const selectedResults = results.filter(selected);
            if (selectedResults.length) {
                const aggregateStatus = aggregatePipelineStatus(selectedResults.map(statusOf));
                const status = !['RUNNING', 'FAILED'].includes(aggregateStatus)
                    && selectedResults.some(item => item.review_required === true)
                    ? 'NEEDS_REVIEW'
                    : aggregateStatus;
                return {
                    id,
                    label,
                    technical_label: technicalLabel,
                    status,
                    counters: [
                        {label: 'Группы', value: selectedResults.length},
                        {label: 'Изменения', value: selectedResults.reduce(
                            (sum, item) => sum + (finiteNumber(item.changes) || 0), 0,
                        )},
                    ],
                    progress: normalizePipelineProgress({
                        ...graphic,
                        status,
                        processed: selectedResults.filter(item => statusOf(item) !== 'RUNNING').length,
                        total: selectedResults.length,
                    }, {...(progressOptions || {}), status}),
                    reason: reasonSummary(selectedResults, Boolean(graphic.stale)),
                    reason_codes: collectReasonCodes(selectedResults),
                    note: '',
                    raw: {group_results: selectedResults},
                };
            }
            const branchSelected = selected({route, mode});
            const liveBranch = (
                id === 'graphic-mode-1'
                    && ['graphic_mode_1', 'mode_1'].includes(liveSubstage)
            ) || (
                id === 'graphic-mode-2'
                    && ['graphic_structural_comparison', 'graphic_mode_2', 'mode_2']
                        .includes(liveSubstage)
            ) || (
                id === 'graphic-vision'
                    && ['graphic_vision', 'vision', 'vision_fallback'].includes(liveSubstage)
            );
            let status;
            if (!Object.keys(graphic).length) status = 'NOT_STARTED';
            else if (liveBranch && graphicStatus === 'RUNNING') status = 'RUNNING';
            else if (branchSelected) {
                status = graphicStatus !== 'RUNNING' && graphic.review_required === true
                    ? 'NEEDS_REVIEW'
                    : graphicStatus;
            } else if (graphicStatus === 'RUNNING' && !route && !mode) {
                // The common live progress record does not disclose which
                // DOCUMENT route will be selected. Unknown is not N/A.
                status = 'NOT_STARTED';
            } else status = 'NOT_APPLICABLE';
            return {
                id,
                label,
                technical_label: technicalLabel,
                status,
                counters: branchSelected || liveBranch ? countersFrom([
                    {label: 'Изменения', keys: ['changes'], sources: [graphic]},
                ]) : [],
                reason: branchSelected || liveBranch
                    ? reasonSummary([graphic], Boolean(graphic.stale)) : '',
                reason_codes: branchSelected || liveBranch ? collectReasonCodes([graphic]) : [],
                note: branchSelected || liveBranch
                    ? 'Отдельная метрика ветки backend не опубликована.' : '',
                progress: normalizePipelineProgress(
                    status === 'RUNNING' ? graphic : {},
                    {...(progressOptions || {}), status},
                ),
                raw: {},
            };
        }

        const mode1 = derivedBranch(
            'graphic-mode-1', 'Точное графическое сравнение', 'MODE 1',
            [stages.graphic_mode_1, nested.mode_1, components.mode_1, graphic.mode_1],
            item => String(item.route || route).toUpperCase() === 'MODE_1_APPLICABLE'
                || String(item.mode || mode).toUpperCase() === 'MODE_1',
        );
        const mode2 = derivedBranch(
            'graphic-mode-2', 'Структурное сравнение', 'MODE 2',
            [stages.graphic_mode_2, nested.mode_2, components.mode_2, graphic.mode_2],
            item => String(item.route || route).toUpperCase() === 'MODE_2_REQUIRED'
                || String(item.mode || mode).toUpperCase() === 'MODE_2',
        );
        const vision = derivedBranch(
            'graphic-vision', 'Визуальная проверка', 'Vision fallback',
            [stages.graphic_vision, nested.vision, nested.vision_fallback,
                components.vision, graphic.vision_fallback],
            item => String(item.route || route).toUpperCase() === 'VISION_REQUIRED'
                || String(item.mode || mode).toUpperCase() === 'VISION',
        );
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

    const HEARTBEAT_WARNING_THRESHOLD_MS = 120 * 1000;
    const HEARTBEAT_WARNING_TEXT = 'Давно нет обновлений. Операция может выполняться долго или ожидать завершения внутреннего процесса.';
    const PIPELINE_STAGE_LABELS = {
        selection: '1. Выбор сравнения',
        sheets: '2. Сопоставление листов',
        sheet_matching: '2. Сопоставление листов',
        sheet_scope: '2. Сопоставление листов',
        content: '3. Анализ содержимого',
        content_analysis: '3. Анализ содержимого',
        text: '3. Анализ содержимого',
        graphic: '3. Анализ содержимого',
        // ИИ-анализ не заводит девятый этап: он живёт внутри синтеза, где и
        // происходит — между разбором содержимого и вопросами инженеру.
        ai_resolution: '6. Синтез изменений',
        objects: '4. Сопоставление объектов',
        entity_matching: '4. Сопоставление объектов',
        entity_binding: '4. Сопоставление объектов',
        effective_entity_binding: '4. Сопоставление объектов',
        questions: '5. Вопросы инженеру',
        review_questions: '5. Вопросы инженеру',
        synthesis: '6. Синтез изменений',
        unified_synthesis: '6. Синтез изменений',
        automatic_unified_synthesis: '6. Синтез изменений',
        review: '7. Проверка инженером',
        engineer_decisions: '7. Проверка инженером',
        report: '8. Итоговый отчёт',
        final_report: '8. Итоговый отчёт',
    };
    const SUBSTAGE_LABELS = {
        preparation: 'Подготовка текста',
        text_preparation: 'Подготовка текста',
        deterministic_diff: 'Поиск различий',
        differences: 'Поиск различий',
        semantic_validation: 'Проверка различий',
        text_atoms: 'Формирование изменений',
        atoms: 'Формирование изменений',
        router: 'Выбор метода',
        mode_1: 'Точное графическое сравнение',
        mode_2: 'Структурное сравнение',
        vision: 'Визуальная проверка',
        vision_fallback: 'Визуальная проверка',
        sheet_candidate_matching: 'Поиск кандидатов для листов',
        sheet_scope: 'Формирование групп листов',
        page_sheet_advisory: 'Проверка рекомендаций по листам',
        text_difference_search: 'Поиск различий',
        text_difference_validation: 'Проверка различий',
        text_change_formation: 'Формирование текстовых изменений',
        graphic_method_selection: 'Выбор метода графического сравнения',
        graphic_structural_comparison: 'Структурное графическое сравнение',
        graphic_group_comparison: 'Сравнение групп графики',
        entity_matching: 'Сопоставление объектов',
        entity_binding: 'Привязка изменений к объектам',
        automatic_synthesis: 'Синтез автоматических изменений',
        review_application: 'Применение ответов инженера',
        ai_resolution: 'ИИ-анализ текста',
        ai_vision: 'ИИ-анализ графики',
        effective_synthesis: 'Синтез с учётом ответов',
        question_generation: 'Формирование вопросов инженеру',
        approved_report_projection: 'Обновление итогового отчёта',
    };

    function metricNumber(value) {
        if (value === null || value === undefined || value === '') return null;
        const normalized = Number(value);
        return Number.isFinite(normalized) && normalized >= 0 ? normalized : null;
    }

    function timestampMilliseconds(value) {
        if (value === null || value === undefined || value === '') return null;
        if (value instanceof Date) {
            const result = value.getTime();
            return Number.isFinite(result) ? result : null;
        }
        const numeric = Number(value);
        if (Number.isFinite(numeric) && String(value).trim() !== '') {
            return numeric > 0 && numeric < 100000000000 ? numeric * 1000 : numeric;
        }
        const parsed = Date.parse(String(value));
        return Number.isFinite(parsed) ? parsed : null;
    }

    function formatPipelineDuration(value) {
        const milliseconds = metricNumber(value);
        if (milliseconds === null) return '';
        const seconds = Math.max(0, Math.floor(milliseconds / 1000));
        const hours = Math.floor(seconds / 3600);
        const minutes = Math.floor((seconds % 3600) / 60);
        const rest = seconds % 60;
        if (hours) return `${hours} ч ${minutes} мин`;
        if (minutes) return `${minutes} мин ${rest} сек`;
        return `${rest} сек`;
    }

    function formatActivityAge(value) {
        const milliseconds = metricNumber(value);
        if (milliseconds === null) return '';
        if (milliseconds < 2000) return 'сейчас';
        const seconds = Math.floor(milliseconds / 1000);
        if (seconds < 60) return `${seconds} сек назад`;
        const minutes = Math.floor(seconds / 60);
        if (minutes < 60) return `${minutes} мин назад`;
        return `${Math.floor(minutes / 60)} ч назад`;
    }

    function progressSource(value) {
        const source = object(value);
        return Object.assign(
            {},
            source,
            object(source.progress),
            object(source.runtime_progress),
            object(source.live_progress),
            object(source.execution_progress),
        );
    }

    function progressMetric(source, keys) {
        const value = object(source);
        for (const key of array(keys)) {
            const direct = metricNumber(value[key]);
            if (direct !== null) return direct;
            const counted = metricNumber(object(value.counts)[key]);
            if (counted !== null) return counted;
        }
        return null;
    }

    function normalizedUnit(value) {
        const unit = String(value || '').trim();
        const known = {
            sheet: 'листов', sheets: 'листов', left_sheets: 'листов',
            page: 'страниц', pages: 'страниц',
            group: 'групп', groups: 'групп', item: 'единиц', items: 'единиц',
            fragment: 'фрагментов', fragments: 'фрагментов',
            delta: 'различий', deltas: 'различий', differences: 'различий',
            atom: 'изменений', atoms: 'изменений',
            relation: 'связей', relations: 'связей',
            atomic_targets: 'изменений', questions: 'вопросов',
            approved_changes: 'изменений',
        };
        return known[unit.toLowerCase()] || unit;
    }

    function unitDurations(source) {
        const candidates = [
            source.unit_durations_ms, source.completed_unit_durations_ms,
            source.recent_unit_durations_ms, source.durations_ms,
        ];
        for (const candidate of candidates) {
            const values = array(candidate).map(metricNumber).filter(value => value !== null && value > 0);
            if (values.length) return values;
        }
        return [];
    }

    function estimatePipelineEtaMs(source, options) {
        const settings = object(options);
        const effective = progressSource(source);
        const processed = settings.processed !== undefined
            ? metricNumber(settings.processed)
            : progressMetric(effective, ['processed', 'current', 'completed_units', 'groups_completed']);
        const total = settings.total !== undefined
            ? metricNumber(settings.total)
            : progressMetric(effective, ['total', 'total_units', 'groups_total']);
        if (processed === null || total === null || total <= processed || total <= 0) return null;
        const durations = unitDurations(effective);
        const explicitAverage = progressMetric(effective, [
            'average_unit_duration_ms', 'avg_unit_duration_ms', 'moving_average_ms',
        ]);
        let average = null;
        if (durations.length >= 2) {
            const sample = durations.slice(-5);
            average = sample.reduce((sum, value) => sum + value, 0) / sample.length;
        } else if (explicitAverage !== null && explicitAverage > 0) {
            average = explicitAverage;
        } else {
            const elapsed = settings.elapsed_ms !== undefined
                ? metricNumber(settings.elapsed_ms)
                : progressMetric(effective, ['elapsed_ms']);
            if (processed >= 2 && elapsed !== null && elapsed > 0) average = elapsed / processed;
        }
        if (!(average > 0)) return null;
        return Math.max(0, Math.round(average * (total - processed)));
    }

    function formatPipelineEta(value) {
        const milliseconds = metricNumber(value);
        if (milliseconds === null || milliseconds <= 0) return '';
        const seconds = milliseconds / 1000;
        if (seconds < 90) {
            const rounded = Math.max(5, Math.round(seconds / 5) * 5);
            return `~${rounded} сек`;
        }
        const minutes = Math.max(1, Math.round(seconds / 60));
        if (minutes < 60) return `~${minutes} мин`;
        const hours = Math.max(1, Math.round(minutes / 60));
        return `~${hours} ч`;
    }

    function formatProgressCurrentItem(value) {
        if (value === null || value === undefined || value === '') return '';
        if (typeof value !== 'object' || Array.isArray(value)) return text(value, '');
        const leftPages = uniqueNumbers(value.left_pages);
        const rightPages = uniqueNumbers(value.right_pages);
        if (leftPages.length || rightPages.length) {
            return `LEFT ${leftPages.join(', ') || '—'} ↔ RIGHT ${rightPages.join(', ') || '—'}`;
        }
        return text(value);
    }

    function normalizePipelineProgress(value, options) {
        const settings = object(options);
        const source = progressSource(value);
        const status = normalizePipelineStatus(settings.status || source.status);
        const now = timestampMilliseconds(settings.now_ms) ?? Date.now();
        const processed = progressMetric(source, [
            'processed', 'current', 'completed_units', 'items_processed',
            'groups_completed', 'deltas_processed', 'fragments_processed',
        ]);
        const total = progressMetric(source, [
            'total', 'total_units', 'items_total', 'groups_total',
            'deltas_total', 'fragments_total',
        ]);
        const determinate = processed !== null && total !== null && total > 0;
        const running = status === 'RUNNING';
        const kind = determinate ? 'determinate' : running ? 'indeterminate' : 'none';
        const startedAtMs = timestampMilliseconds(source.started_at);
        const completedAtMs = timestampMilliseconds(source.completed_at);
        const reportedDuration = progressMetric(source, ['duration_ms']);
        let elapsedMs = reportedDuration;
        if (startedAtMs !== null) {
            const end = running ? now : completedAtMs;
            if (end !== null) {
                const timestampDuration = Math.max(0, end - startedAtMs);
                elapsedMs = running && elapsedMs !== null
                    ? Math.max(elapsedMs, timestampDuration)
                    : (elapsedMs === null ? timestampDuration : elapsedMs);
            }
        }
        const activityAtMs = timestampMilliseconds(
            source.last_activity_at || source.updated_at || source.started_at,
        );
        const activityAgeMs = activityAtMs === null ? null : Math.max(0, now - activityAtMs);
        const configuredThreshold = metricNumber(
            settings.warning_threshold_ms !== undefined
                ? settings.warning_threshold_ms
                : source.heartbeat_warning_threshold_ms,
        );
        const constraintThresholdSeconds = metricNumber(
            object(source.constraints).activity_warning_threshold_sec,
        );
        const warningThresholdMs = configuredThreshold && configuredThreshold > 0
            ? configuredThreshold
            : constraintThresholdSeconds && constraintThresholdSeconds > 0
                ? constraintThresholdSeconds * 1000
                : HEARTBEAT_WARNING_THRESHOLD_MS;
        const human = Boolean(settings.human || source.human);
        const etaMs = running && !human && determinate
            ? estimatePipelineEtaMs(source, {processed, total, elapsed_ms: elapsedMs})
            : null;
        const unit = normalizedUnit(source.unit || settings.unit);
        const counterPrefix = String(settings.counter_prefix || '').trim();
        const counterLabel = determinate
            ? `${counterPrefix ? `${counterPrefix}: ` : ''}${processed} / ${total}${unit ? ` ${unit}` : ''}`
            : '';
        const completedAgeMs = !running && completedAtMs !== null
            ? Math.max(0, now - completedAtMs)
            : null;
        return {
            status,
            kind,
            mode: kind,
            determinate,
            indeterminate: kind === 'indeterminate',
            processed,
            total,
            unit,
            counter_label: counterLabel,
            percent: determinate ? Math.max(0, Math.min(100, (processed / total) * 100)) : null,
            message: source.message || source.operation
                ? text(source.message || source.operation)
                : '',
            current_stage: source.current_stage || '',
            current_substage: source.current_substage || '',
            current_item: formatProgressCurrentItem(source.current_item),
            current_item_raw: source.current_item || null,
            started_at: source.started_at || null,
            completed_at: source.completed_at || null,
            last_activity_at: source.last_activity_at || null,
            elapsed_ms: elapsedMs,
            elapsed_label: elapsedMs === null ? '' : formatPipelineDuration(elapsedMs),
            heartbeat_age_ms: activityAgeMs,
            last_activity_label: running && activityAgeMs !== null
                ? formatActivityAge(activityAgeMs)
                : '',
            completed_age_ms: completedAgeMs,
            completed_age_label: completedAgeMs === null ? '' : formatActivityAge(completedAgeMs),
            warning_threshold_ms: warningThresholdMs,
            heartbeat_warning: running && activityAgeMs !== null && activityAgeMs > warningThresholdMs,
            heartbeat_warning_text: running && activityAgeMs !== null && activityAgeMs > warningThresholdMs
                ? HEARTBEAT_WARNING_TEXT
                : '',
            eta_ms: etaMs,
            eta_label: etaMs === null ? '' : formatPipelineEta(etaMs),
            human,
            is_running: running,
            is_terminal: !['NOT_STARTED', 'RUNNING'].includes(status),
        };
    }

    function aggregateConcurrentPipelineStatus(values, runningHint) {
        const statuses = array(values).map(value => (
            PIPELINE_STATUSES.includes(value) ? value : normalizePipelineStatus(value)
        ));
        if (!statuses.length || statuses.every(status => status === 'NOT_STARTED')) return 'NOT_STARTED';
        if (statuses.includes('RUNNING')) return 'RUNNING';
        if (statuses.includes('FAILED') && !runningHint) return 'FAILED';
        if (statuses.includes('NOT_STARTED')) return runningHint ? 'RUNNING' : 'PARTIAL';
        if (statuses.includes('FAILED')) return 'FAILED';
        if (statuses.includes('NEEDS_REVIEW')) return 'NEEDS_REVIEW';
        if (statuses.includes('PARTIAL')) return 'PARTIAL';
        if (statuses.every(status => status === 'NOT_APPLICABLE')) return 'NOT_APPLICABLE';
        if (statuses.every(status => ['COMPLETED', 'NOT_APPLICABLE'].includes(status))) {
            return 'COMPLETED';
        }
        return statuses[0] || 'NOT_STARTED';
    }

    function contextualStageAction(id, destination) {
        const labels = {
            selection: 'Изменить выбор',
            sheets: 'Открыть сопоставление листов',
            content: 'Открыть анализ',
            objects: 'Открыть сопоставление объектов',
            questions: 'Ответить на вопросы',
            synthesis: 'Открыть результат синтеза',
            review: 'Проверить изменения',
            report: 'Открыть итоговый отчёт',
        };
        return {kind: 'OPEN', label: labels[id] || 'Открыть', destination};
    }

    function fullRerunFallback(number) {
        if (![2, 3, 4, 5, 6].includes(number)) return null;
        const dependencyNames = {
            2: ['3. Анализ содержимого', '4. Сопоставление объектов', '5. Вопросы инженеру', '6. Синтез изменений', '7. Проверка инженером', '8. Итоговый отчёт'],
            3: ['4. Сопоставление объектов', '5. Вопросы инженеру', '6. Синтез изменений', '7. Проверка инженером', '8. Итоговый отчёт'],
            4: ['5. Вопросы инженеру', '6. Синтез изменений', '7. Проверка инженером', '8. Итоговый отчёт'],
            5: ['6. Синтез изменений', '7. Проверка инженером', '8. Итоговый отчёт'],
            6: ['7. Проверка инженером', '8. Итоговый отчёт'],
        };
        return {
            kind: 'FULL_RERUN',
            supported: true,
            partial_rerun_supported: false,
            label: '↻ Запустить полный анализ заново',
            note: number === 3
                ? 'Повторный расчёт текста сейчас выполняется в составе полного автоматического анализа.'
                : 'Для пересчёта этого этапа необходимо повторить автоматический анализ.',
            dependencies: dependencyNames[number],
            requires_confirmation: true,
        };
    }

    function normalizeProductionPipeline(payload) {
        const wrapper = object(payload);
        const state = object(wrapper.state);
        const stages = object(state.stages);
        const stateStatus = normalizePipelineStatus(state.status);
        // Artifacts from the previous generation remain readable while the
        // synchronous producer is running.  During that window only the
        // current generation's state.stages are authoritative.
        const generationRunning = stateStatus === 'RUNNING';
        const questionsArtifact = generationRunning ? null : wrapper.questions;
        const changesArtifact = generationRunning ? null : wrapper.changes;
        const finalReport = generationRunning
            ? {}
            : object(wrapper.final_report || wrapper.finalReport);
        const stale = [state, wrapper.questions, wrapper.changes,
            wrapper.final_report || wrapper.finalReport]
            .some(value => Boolean(object(value).stale));
        const hasProductionRun = stateStatus !== 'NOT_STARTED'
            || Boolean(state.run_id || state.generation_run_id || state.started_at)
            || Object.keys(stages).length > 0;
        const selection = object(state.selection);
        const activePair = object(wrapper.active_pair);
        const selectedPages = object(wrapper.selected_pages);
        const selectedMode = String(
            wrapper.selected_mode || selection.input_mode || state.input_mode || 'DOCUMENT',
        ).toUpperCase();
        const backendWarningSeconds = metricNumber(
            object(state.constraints).activity_warning_threshold_sec,
        );
        const progressOptions = {
            now_ms: wrapper.now_ms,
            current_substage: state.current_substage
                || progressSource(state).current_substage
                || '',
            warning_threshold_ms: wrapper.heartbeat_warning_threshold_ms
                || state.heartbeat_warning_threshold_ms
                || (backendWarningSeconds ? backendWarningSeconds * 1000 : null),
        };
        const questionRows = normalizeQuestions(questionsArtifact);
        const questionCounts = normalizeQuestionCounts(questionsArtifact);
        const rawQuestionCounts = object(object(questionsArtifact).counts);
        const hasRawQuestionCounts = Object.keys(rawQuestionCounts).length > 0;
        const questionsStage = object(stages.review_questions);
        const explicitAnswered = firstNumber([questionsStage], ['answered', 'answers']);
        const stageQuestionTotal = firstNumber([questionsStage], ['total']);
        const legacyStageQuestions = firstNumber([questionsStage], ['questions']);
        const explicitQuestionTotal = stageQuestionTotal !== null
            ? stageQuestionTotal
            : (explicitAnswered !== null ? legacyStageQuestions : null);
        const resolvedUnchanged = firstNumber(
            [rawQuestionCounts], ['resolved_unchanged'],
        );
        const answeredRows = questionRows.filter(question => (
            Boolean(String(question.answer || '').trim())
            || ['ANSWERED', 'RESOLVED', 'CLOSED'].includes(question.status)
        )).length;
        const answeredKnown = questionRows.length > 0
            || Boolean(object(questionsArtifact).questions)
            || explicitAnswered !== null
            || resolvedUnchanged !== null;
        const answered = explicitAnswered !== null
            ? explicitAnswered
            : Math.max(answeredRows, resolvedUnchanged || 0);
        const pendingRows = Math.max(0, questionRows.length - answeredRows);
        const stagePending = firstNumber([questionsStage], ['pending']);
        const artifactPending = firstNumber([rawQuestionCounts], ['pending']);
        const explicitPending = stagePending !== null
            ? stagePending
            : artifactPending !== null
                ? artifactPending
                : questionRows.length
                    ? pendingRows
                : legacyStageQuestions !== null
                    ? Math.max(0, legacyStageQuestions - answered)
                    : null;
        const questionPending = explicitPending !== null ? explicitPending : pendingRows;
        const rawPendingTotal = firstNumber([rawQuestionCounts], ['total']);
        const questionTotal = explicitQuestionTotal !== null
            ? Math.max(explicitQuestionTotal, questionPending + answered)
            : Math.max(
                questionPending + answered,
                (rawPendingTotal || 0) + (resolvedUnchanged || 0),
                hasRawQuestionCounts
                    ? questionCounts.total + answered
                    : questionRows.length,
            );
        const reviewRows = normalizeRows(changesArtifact);
        const persistedCounts = reviewCounts(reviewRows);
        const decisionsStage = object(stages.engineer_decisions);
        const decisionsCounts = object(decisionsStage.counts);
        const synthesizedFindings = firstNumber([
            object(stages.unified_synthesis), object(stages.automatic_unified_synthesis),
        ], ['review_items']);
        const hasReviewRows = Array.isArray(changesArtifact)
            || Array.isArray(object(changesArtifact).rows);
        const stageReviewTotal = firstNumber([decisionsCounts], ['total']);
        const rowsAreAuthoritative = hasReviewRows
            && (reviewRows.length > 0 || stageReviewTotal === null || stageReviewTotal === 0);
        const reviewApproved = rowsAreAuthoritative
            ? persistedCounts.APPROVED
            : (firstNumber([decisionsCounts], ['APPROVED', 'approved']) || 0);
        const reviewRejected = rowsAreAuthoritative
            ? persistedCounts.REJECTED
            : (firstNumber([decisionsCounts], ['REJECTED', 'rejected']) || 0);
        const explicitReviewPending = firstNumber(
            [decisionsCounts], ['PENDING_REVIEW', 'pending'],
        );
        const baseReviewTotal = stageReviewTotal !== null
            ? stageReviewTotal
            : (synthesizedFindings || 0);
        const reviewPending = rowsAreAuthoritative
            ? persistedCounts.PENDING_REVIEW
            : (explicitReviewPending !== null
                ? explicitReviewPending
                : Math.max(0, baseReviewTotal - reviewApproved - reviewRejected));
        const reviewTotal = rowsAreAuthoritative
            ? persistedCounts.total
            : Math.max(
                baseReviewTotal,
                reviewApproved + reviewRejected + reviewPending,
            );
        const finalStage = object(stages.final_report);
        const finalApproved = firstNumber([
            object(finalReport.summary), finalStage,
        ], ['approved']);
        const approvedInReport = finalApproved !== null
            ? finalApproved
            : array(finalReport.approved_atomic_changes).length;

        function stageRecord(id, number, label, status, counters, sources, destination, extra) {
            const sourceList = array(sources);
            const progressSourceRecord = sourceList.find(source => statusOf(source) === 'RUNNING')
                || sourceList.find(source => Object.keys(progressSource(source)).some(key => (
                    ['processed', 'total', 'started_at', 'last_activity_at', 'duration_ms'].includes(key)
                )))
                || sourceList[0]
                || {};
            const action = contextualStageAction(id, destination);
            return {
                id, number, label,
                status,
                status_label: pipelineStatusLabel(status),
                counters: array(counters),
                progress: normalizePipelineProgress(progressSourceRecord, {
                    ...progressOptions, status,
                }),
                reason: reasonSummary(sourceList, stale),
                reason_codes: collectReasonCodes(sourceList),
                destination,
                action,
                action_label: action.label,
                rerun: hasProductionRun
                    && !['NOT_STARTED', 'RUNNING', 'NOT_APPLICABLE'].includes(status)
                    ? fullRerunFallback(number)
                    : null,
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
        function pairSideSelected(value) {
            if (typeof value === 'string') return Boolean(value.trim());
            const side = object(value);
            return Boolean(
                side.id || side.pdf_path || side.path || side.filename
                || side.name || side.document_id || side.document_code,
            );
        }
        const pairHasBothSides = pairSideSelected(activePair.left)
            && pairSideSelected(activePair.right);
        const useUiPageSelection = Boolean(wrapper.selected_mode && pairHasBothSides);
        const leftPages = uniqueNumbers(
            useUiPageSelection ? selectedPages.left
                : array(selection.left_pages).length ? selection.left_pages : selectedPages.left,
        );
        const rightPages = uniqueNumbers(
            useUiPageSelection ? selectedPages.right
                : array(selection.right_pages).length ? selection.right_pages : selectedPages.right,
        );
        if (selectedMode === 'PAGE' && leftPages.length) {
            selectionCounters.push({label: 'LEFT листы', value: leftPages.length});
        }
        if (selectedMode === 'PAGE' && rightPages.length) {
            selectionCounters.push({label: 'RIGHT листы', value: rightPages.length});
        }
        const hasSelectionSource = Object.keys(selection).length > 0 || pairHasBothSides;
        const hasSelection = hasSelectionSource && (
            selectedMode !== 'PAGE'
            || (leftPages.length === 1 && rightPages.length === 1)
        );
        const selectionStatus = hasSelection ? 'COMPLETED' : 'NOT_STARTED';
        function selectedSideDetail(side, pages) {
            const upper = side.toUpperCase();
            const pairSide = object(activePair[side]);
            const documentName = pairSide.filename || pairSide.name || pairSide.document_name
                || activePair[`${side}_filename`] || selection[`${side}_document`]
                || selection[`${side}_document_id`] || 'документ выбран';
            const pageText = selectedMode === 'PAGE' && pages.length
                ? ` · стр. ${pages.join(', ')}`
                : '';
            return `${upper}: ${documentName}${pageText}`;
        }
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
        // Счётчики хода конвейера инженер видит наравне с находками, поэтому
        // здесь тоже не место кодам HIGH/POSSIBLE/SPLIT/MERGED. Подписи
        // короткие: это плитки счётчиков, а не карточка вопроса.
        [
            ['HIGH', 'Подтверждено'],
            ['POSSIBLE', 'Ждут подтверждения'],
            ['SPLIT', 'Разделены'],
            ['MERGED', 'Объединены'],
            ['NO_MATCH', 'Без пары'],
        ].forEach(([key, label]) => {
            const value = finiteNumber(relationCounts[key]);
            if (value !== null) sheetCounters.push({label, value});
        });

        const text = object(stages.text);
        const graphic = object(stages.graphic);
        const textStatus = statusOf(text);
        const graphicStatus = statusOf(graphic);
        const contentStatus = aggregateConcurrentPipelineStatus(
            [textStatus, graphicStatus], normalizePipelineStatus(state.status) === 'RUNNING',
        );
        const textProgress = normalizePipelineProgress(text, {
            ...progressOptions, status: textStatus,
        });
        const graphicProgress = normalizePipelineProgress(graphic, {
            ...progressOptions, status: graphicStatus, unit: 'groups',
            counter_prefix: 'Обработано',
        });
        const textSubstages = textPipelineSubstages(stages, progressOptions);
        const graphicSubstages = graphicPipelineSubstages(stages, progressOptions);
        const graphicResultCounters = countersFrom([
            {
                label: 'Готово', keys: ['groups_completed'], sources: [graphic],
            },
            {
                label: 'Требует проверки',
                keys: ['groups_review_required', 'review_required'], sources: [graphic],
            },
            {
                label: 'Заблокировано', keys: ['groups_blocked', 'blocked'], sources: [graphic],
            },
            {
                label: 'Изменения', keys: ['changes'], sources: [graphic],
            },
        ]);
        const graphicChanges = firstNumber([graphic], ['changes']);
        const graphicHasRuntime = Object.keys(graphic).length > 0;
        let graphicSummary = '';
        if (graphicHasRuntime && graphicProgress.indeterminate) {
            graphicSummary = graphicProgress.message
                || 'Выполняется одна графическая операция';
        } else if (graphicHasRuntime && graphicProgress.is_terminal && !graphicProgress.determinate) {
            graphicSummary = 'Графическое сравнение завершено'
                + (graphicChanges === null ? '' : ` · изменений: ${graphicChanges}`);
        }
        const graphicMiniCounter = graphicProgress.counter_label
            || (graphicProgress.is_running ? 'Выполняется'
                : graphicProgress.is_terminal
                    ? (graphicChanges === null ? 'Завершено' : `${graphicChanges} изм.`)
                    : '');
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
            {label: 'GRAPHIC заблокировано', keys: ['groups_blocked', 'blocked'], sources: [graphic]},
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
        const reportedQuestionStatus = statusOf(questionsStage);
        const questionStatus = reportedQuestionStatus === 'RUNNING'
            ? 'RUNNING'
            : reportedQuestionStatus === 'FAILED' ? 'FAILED'
            : questionPending > 0 ? 'NEEDS_REVIEW'
                : Object.keys(questionsStage).length ? 'COMPLETED' : 'NOT_STARTED';
        const reviewStatus = statusOf(decisionsStage) === 'FAILED'
            ? 'FAILED'
            : reviewPending > 0 ? 'NEEDS_REVIEW'
                : reviewTotal > 0 ? 'COMPLETED' : 'NOT_STARTED';
        const hasSynthesis = [automaticSynthesis, reviewApplication, synthesis]
            .some(source => Object.keys(source).length > 0);
        const explicitReportStatus = statusOf(finalStage);
        const reportStatus = explicitReportStatus === 'RUNNING'
            ? 'RUNNING'
            : explicitReportStatus === 'FAILED' ? 'FAILED'
            : !hasSynthesis && !Object.keys(finalStage).length && !Object.keys(finalReport).length
                ? 'NOT_STARTED'
                : questionPending > 0 || reviewPending > 0
                    ? 'NEEDS_REVIEW'
                    : 'COMPLETED';
        const reviewCounters = reviewTotal > 0 ? [
            {label: 'Найдено', value: reviewTotal},
            {label: 'Подтверждено', value: reviewApproved},
            {label: 'Отклонено', value: reviewRejected},
            {label: 'Ожидает решения', value: reviewPending},
        ] : [];
        const reportCounters = hasSynthesis || Object.keys(finalStage).length
            || Object.keys(finalReport).length ? [
            {label: 'Найдено', value: reviewTotal},
            {label: 'Подтверждено', value: reviewApproved},
            {label: 'Отклонено', value: reviewRejected},
            {label: 'Ещё не проверено', value: reviewPending},
            {label: 'Войдёт в отчёт', value: approvedInReport},
        ] : [];

        return [
            stageRecord('selection', 1, 'Выбор сравнения', selectionStatus,
                selectionCounters, [selection, activePair], {tab: 'upload'}, {
                    details: hasSelection ? [
                        selectedSideDetail('left', leftPages),
                        selectedSideDetail('right', rightPages),
                        `Режим: ${selectedMode} ↔ ${selectedMode}`,
                    ] : [],
                    selection: {
                        mode: selectedMode,
                        mode_label: `${selectedMode} ↔ ${selectedMode}`,
                        left: {document: selectedSideDetail('left', leftPages), pages: leftPages},
                        right: {document: selectedSideDetail('right', rightPages), pages: rightPages},
                    },
                }),
            stageRecord('sheets', 2, 'Сопоставление листов',
                aggregatePipelineStatus([statusOf(sheetMatching), statusOf(sheetScope)]),
                sheetCounters, [sheetMatching, sheetScope], {tab: 'links'}),
            stageRecord('content', 3, 'Анализ содержимого',
                contentStatus,
                contentCounters, [text, graphic], {tab: 'diffs', anchor: 'sc-production-review-stage'}, {
                    progress: {
                        status: contentStatus,
                        kind: 'parallel', mode: 'parallel', aggregate: false,
                        branches: {text: textProgress, graphic: graphicProgress},
                    },
                    mini_counters: [
                        textProgress.counter_label
                            ? {label: 'TEXT', value: textProgress.counter_label} : null,
                        graphicMiniCounter
                            ? {label: 'GRAPHIC', value: graphicMiniCounter} : null,
                    ].filter(Boolean),
                    sections: [
                        {
                            id: 'text', label: 'TEXT (текст)', technical_label: 'TEXT',
                            progress: textProgress, mini_counter: textProgress.counter_label,
                            substages: textSubstages,
                        },
                        {
                            id: 'graphic', label: 'GRAPHIC (графика)', technical_label: 'GRAPHIC',
                            progress: graphicProgress, mini_counter: graphicMiniCounter,
                            summary: graphicSummary,
                            result_counters: graphicResultCounters,
                            substages: graphicSubstages,
                        },
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
                    progress: {
                        ...normalizePipelineProgress({
                            ...questionsStage,
                            processed: answeredKnown ? answered : null,
                            total: questionTotal,
                            unit: 'вопросов',
                        }, {...progressOptions, status: questionStatus, human: true}),
                        answered: answeredKnown ? answered : null,
                    },
                    categories: categoryDetails,
                    pending: questionPending,
                    reason: stale ? reasonSummary([questionsStage], true) : questionPending > 0
                        ? `Осталось ответить: ${questionPending}.`
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
                    progress: normalizePipelineProgress({
                        ...decisionsStage,
                        processed: reviewApproved + reviewRejected,
                        total: reviewTotal,
                        unit: 'изменений',
                    }, {...progressOptions, status: reviewStatus, human: true}),
                    pending: reviewPending,
                    reason: stale ? reasonSummary([decisionsStage], true) : reviewPending > 0
                        ? `Без решения инженера: ${reviewPending}.`
                        : '',
                }),
            stageRecord('report', 8, 'Итоговый отчёт',
                reportStatus, reportCounters, [finalStage], {tab: 'report'}, {
                    approved_only: object(finalReport.constraints).approved_only === true,
                    pending: reviewPending,
                }),
        ];
    }

    function stageLabelFromValue(value, stages) {
        if (value !== null && value !== undefined && value !== '') {
            const raw = String(value).trim();
            const numericMatch = raw.match(/(?:^|STAGE[_\s-]*)([1-8])$/i);
            if (numericMatch) {
                const stage = array(stages).find(item => item.number === Number(numericMatch[1]));
                if (stage) return `${stage.number}. ${stage.label}`;
            }
            const normalized = raw.toLowerCase().replace(/[\s.-]+/g, '_');
            if (PIPELINE_STAGE_LABELS[normalized]) return PIPELINE_STAGE_LABELS[normalized];
            const byId = array(stages).find(item => item.id === normalized);
            if (byId) return `${byId.number}. ${byId.label}`;
            return raw;
        }
        const running = array(stages).find(stage => stage.status === 'RUNNING');
        return running ? `${running.number}. ${running.label}` : '';
    }

    function substageLabelFromValue(value) {
        if (value === null || value === undefined || value === '') return '';
        const raw = String(value).trim();
        const normalized = raw.toLowerCase().replace(/[\s.-]+/g, '_');
        const direct = SUBSTAGE_LABELS[normalized];
        if (direct) return direct;
        const suffix = Object.keys(SUBSTAGE_LABELS).find(key => normalized.endsWith(`_${key}`));
        return suffix ? SUBSTAGE_LABELS[suffix] : raw;
    }

    // Прогресс ИИ-анализа на карточке конвейера. Инженеру нужны три числа:
    // сколько разобрано, сколько закрыто автоматически и сколько осталось ему.
    function normalizeAiProgress(payload) {
        const wrapper = object(payload);
        const state = object(wrapper.state || wrapper);
        const stage = object(object(state.stages).ai_resolution);
        const mode = String(stage.mode || 'OFF').toUpperCase();
        if (!Object.keys(stage).length || mode === 'OFF') {
            return {available: false, mode: 'OFF', mode_label: aiModeLabel('OFF')};
        }
        const total = metricNumber(stage.total) || 0;
        const processed = metricNumber(stage.processed) || 0;
        const resolved = metricNumber(stage.ai_resolved) || 0;
        const human = metricNumber(stage.human_required) || 0;
        const reasons = object(stage.human_reasons);
        return {
            available: true,
            mode,
            mode_label: aiModeLabel(mode),
            status: normalizePipelineStatus(stage.status),
            status_label: pipelineStatusLabel(stage.status),
            title: 'ИИ-анализ текста',
            total,
            processed,
            resolved,
            human,
            progress_label: `${processed} / ${total}`,
            resolved_label: `Автоматически разрешено: ${resolved}`,
            human_label: `Осталось человеку: ${human}`,
            vision_calls: metricNumber(stage.vision_calls) || 0,
            vision_label: metricNumber(stage.vision_items)
                ? `ИИ-анализ графики: ${metricNumber(stage.vision_items)}`
                : '',
            model_calls: metricNumber(stage.model_calls) || 0,
            cache_hits: metricNumber(stage.cache_hits) || 0,
            duration_ms: metricNumber(stage.duration_ms) || 0,
            // Причины показываем по-русски; коды остаются в диагностике.
            reasons: Object.keys(reasons)
                .map(code => ({
                    code,
                    label: aiReasonLabel(code),
                    count: metricNumber(reasons[code]) || 0,
                }))
                .filter(item => item.count > 0)
                .sort((left, right) => right.count - left.count),
            budgets_hit: array(stage.budgets_hit).map(String),
        };
    }

    function normalizeProductionOverview(payload, normalizedStages) {
        const wrapper = object(payload);
        const state = object(wrapper.state);
        const stages = Array.isArray(normalizedStages)
            ? normalizedStages
            : normalizeProductionPipeline(wrapper);
        const stateStatus = normalizePipelineStatus(state.status);
        const runActivity = productionRunActivity(state);
        const running = runActivity.active || (
            !runActivity.is_orphaned && stages.some(stage => stage.status === 'RUNNING')
        );
        const orphaned = runActivity.is_orphaned;
        const failed = stateStatus === 'FAILED';
        const stale = [state, wrapper.questions, wrapper.changes,
            wrapper.final_report || wrapper.finalReport]
            .some(value => Boolean(object(value).stale));
        const runStarted = stateStatus !== 'NOT_STARTED'
            || Boolean(state.run_id || state.generation_run_id || state.started_at)
            || Object.keys(object(state.stages)).length > 0;
        const questionsStage = stages.find(stage => stage.id === 'questions') || {};
        const reviewStage = stages.find(stage => stage.id === 'review') || {};
        const selectionReady = stages.some(stage => (
            stage.id === 'selection' && stage.status === 'COMPLETED'
        ));
        const questionsPending = metricNumber(questionsStage.pending) || 0;
        const findingsPending = metricNumber(reviewStage.pending) || 0;
        const humanPending = questionsPending > 0 || findingsPending > 0;
        const automaticPartial = stateStatus === 'PARTIAL' || stages.some(stage => (
            stage.number >= 2 && stage.number <= 6 && stage.status === 'PARTIAL'
        ));
        let overviewState;
        if (orphaned) overviewState = 'INTERRUPTED';
        else if (running) overviewState = 'RUNNING';
        else if (failed) overviewState = 'FAILED';
        else if (!runStarted) overviewState = 'NOT_STARTED';
        else if (humanPending) overviewState = 'NEEDS_REVIEW';
        else if (automaticPartial) overviewState = 'PARTIAL';
        else overviewState = 'COMPLETED';

        const stateProgress = progressSource(state);
        const progress = normalizePipelineProgress(state, {
            now_ms: wrapper.now_ms,
            warning_threshold_ms: wrapper.heartbeat_warning_threshold_ms
                || state.heartbeat_warning_threshold_ms
                || (metricNumber(object(state.constraints).activity_warning_threshold_sec)
                    ? metricNumber(object(state.constraints).activity_warning_threshold_sec) * 1000
                    : null),
            status: overviewState === 'RUNNING'
                ? 'RUNNING'
                : overviewState === 'INTERRUPTED' ? 'FAILED' : stateStatus,
        });
        const currentStage = state.current_stage || stateProgress.current_stage;
        const currentSubstage = state.current_substage || stateProgress.current_substage;
        const currentStageLabel = stageLabelFromValue(currentStage, stages);
        const currentSubstageLabel = substageLabelFromValue(currentSubstage);
        const failedStageLabel = state.failed_stage
            ? stageLabelFromValue(state.failed_stage, stages) : '';
        const failedSubstageLabel = state.failed_substage
            ? substageLabelFromValue(state.failed_substage) : '';
        const failedReason = state.reason_code
            ? humanizeReasonCode(state.reason_code) : '';
        const overviewReasonCodes = collectReasonCodes([state]);
        const detailLines = [];
        let headline;
        let cta;
        if (overviewState === 'NOT_STARTED') {
            headline = 'Анализ ещё не запускался.';
            cta = {
                kind: 'RUN', label: '▶ Запустить полный анализ',
                disabled: !selectionReady,
            };
        } else if (overviewState === 'RUNNING') {
            headline = 'Анализ выполняется';
            if (progress.elapsed_label) detailLines.push(`Прошло: ${progress.elapsed_label}.`);
            if (currentStageLabel) detailLines.push(`Текущий этап: ${currentStageLabel}.`);
            if (currentSubstageLabel) detailLines.push(`Текущая операция: ${currentSubstageLabel}.`);
            if (progress.eta_label) {
                detailLines.push(`Для текущей операции осталось примерно: ${progress.eta_label}.`);
            }
            if (progress.last_activity_label) {
                detailLines.push(`Последняя активность: ${progress.last_activity_label}.`);
            }
            if (progress.heartbeat_warning_text) detailLines.push(progress.heartbeat_warning_text);
            cta = {kind: 'RUNNING', label: 'Анализ выполняется…', disabled: true};
        } else if (overviewState === 'INTERRUPTED') {
            headline = '⚠ Предыдущий анализ был прерван';
            detailLines.push(
                'Сохранённое состояние показывает выполняющийся анализ, но активного процесса больше нет. Это могло произойти после перезапуска сервера.',
            );
            cta = {kind: 'RECOVER', label: '↻ Повторить анализ', disabled: !selectionReady};
        } else if (overviewState === 'FAILED') {
            headline = 'Во время анализа произошла ошибка.';
            if (failedStageLabel) detailLines.push(`Ошибка на этапе: ${failedStageLabel}.`);
            if (failedSubstageLabel) detailLines.push(`Операция: ${failedSubstageLabel}.`);
            if (failedReason) detailLines.push(`Причина: ${failedReason}`);
            cta = {kind: 'RETRY', label: 'Повторить анализ', disabled: !selectionReady};
        } else if (stale) {
            headline = 'Результат анализа устарел.';
            detailLines.push('Исходные документы или область сравнения изменились. Требуется повторный автоматический анализ.');
            cta = {kind: 'RERUN', label: '↻ Запустить анализ заново', disabled: !selectionReady};
        } else if (overviewState === 'NEEDS_REVIEW') {
            headline = automaticPartial
                ? 'Автоматический анализ завершён частично.'
                : 'Автоматический анализ завершён.';
            if (questionsPending) detailLines.push(`Требуется ответить на вопросы: ${questionsPending}.`);
            if (findingsPending) detailLines.push(`Требуется проверить изменения: ${findingsPending}.`);
            cta = {
                kind: 'CONTINUE_REVIEW', label: 'Продолжить проверку', disabled: false,
                destination: questionsPending ? questionsStage.destination : reviewStage.destination,
            };
        } else {
            headline = overviewState === 'PARTIAL'
                ? 'Автоматический анализ завершён частично.'
                : 'Анализ полностью завершён.';
            cta = {kind: 'RERUN', label: '↻ Запустить анализ заново', disabled: !selectionReady};
        }
        if (!progress.is_running && progress.kind === 'none' && progress.completed_age_label) {
            detailLines.push(`Завершено: ${progress.completed_age_label}.`);
        }
        return {
            state: overviewState,
            headline,
            detail_lines: detailLines,
            current_stage_label: currentStageLabel,
            current_substage_label: currentSubstageLabel,
            failed_stage_label: failedStageLabel,
            failed_substage_label: failedSubstageLabel,
            reason_codes: overviewReasonCodes,
            progress,
            stale,
            run_activity: runActivity,
            human: {
                questions_pending: questionsPending,
                findings_pending: findingsPending,
            },
            cta,
        };
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

    function normalizeProductionTextEvidenceItem(value, kind, index) {
        const source = object(value);
        const evidenceId = String(
            source.evidence_id || source.id || `${kind.toLowerCase()}-${index + 1}`,
        );
        const normalized = normalizeEvidence({
            ...source,
            target_id: evidenceId,
            source_mode: 'TEXT',
            layout: 'SIDE_BY_SIDE',
        });
        return {
            ...normalized,
            evidence_id: evidenceId,
            target_id: source.target_id ? String(source.target_id) : '',
            kind,
            target_kind: String(source.target_kind || ''),
            title: text(source.title, kind === 'MATCH' ? 'Совпавший текст' : 'Изменение текста'),
            before: source.before === null || source.before === undefined
                ? '' : text(source.before, ''),
            after: source.after === null || source.after === undefined
                ? '' : text(source.after, ''),
            review_required: Boolean(source.review_required),
            review_status: String(source.review_status || ''),
            raw_item: source,
        };
    }

    function normalizeProductionTextEvidence(payload) {
        const source = object(payload);
        const summary = object(source.summary);
        const numberOrNull = value => {
            if (value === null || value === undefined || value === '' || typeof value === 'boolean') {
                return null;
            }
            const normalized = Number(value);
            return Number.isFinite(normalized) && normalized >= 0 ? normalized : null;
        };
        return {
            kind: String(source.kind || ''),
            schema_version: String(source.schema_version || ''),
            available: Boolean(source.available),
            stale: Boolean(source.stale),
            run_status: String(source.run_status || 'NOT_STARTED').toUpperCase(),
            generation_run_id: String(source.generation_run_id || ''),
            generation_revision: source.generation_revision !== null
                && source.generation_revision !== undefined
                && source.generation_revision !== ''
                && Number.isInteger(Number(source.generation_revision))
                ? Number(source.generation_revision) : null,
            input_signature: String(source.input_signature || ''),
            synthesis_input_signature: String(source.synthesis_input_signature || ''),
            text_result_state: String(source.text_result_state || 'PUBLISHED').toUpperCase(),
            text_blocked_reason: String(source.text_blocked_reason || ''),
            text_blocked_error: String(source.text_blocked_error || ''),
            match_evidence_state: String(source.match_evidence_state || 'UNKNOWN').toUpperCase(),
            change_items: numberOrNull(source.change_items),
            available_change_items: numberOrNull(source.available_change_items),
            summary: {
                available_match_pairs: numberOrNull(
                    source.available_match_pairs !== undefined
                        ? source.available_match_pairs : summary.available_match_pairs,
                ),
                matched_fragments: numberOrNull(summary.matched_fragments),
                changed_fragments: numberOrNull(summary.changed_fragments),
                changed: numberOrNull(summary.changed),
                removed: numberOrNull(summary.removed),
                added: numberOrNull(summary.added),
                review_required: numberOrNull(summary.review_required),
                prepared_fragments: numberOrNull(summary.prepared_fragments),
                text_atoms: numberOrNull(summary.text_atoms),
            },
            matches: array(source.matches).map((item, index) => (
                normalizeProductionTextEvidenceItem(item, 'MATCH', index)
            )),
            changes: array(source.changes).map((item, index) => (
                normalizeProductionTextEvidenceItem(item, 'CHANGE', index)
            )),
            constraints: object(source.constraints),
            raw: source,
        };
    }

    function productionTextEvidenceMatchesGeneration(stateValue, changesValue, evidenceValue) {
        const stateWrapper = object(stateValue);
        const state = object(stateWrapper.state || stateWrapper);
        const changesWrapper = object(changesValue);
        const changes = object(changesWrapper.production_changes || changesWrapper);
        const evidence = object(evidenceValue);
        if (!evidence.available) return true;
        const stateRunId = String(state.run_id || state.generation_run_id || '');
        const evidenceRunId = String(evidence.generation_run_id || '');
        const stateInputSignature = String(state.input_signature || '');
        const evidenceInputSignature = String(evidence.input_signature || '');
        const changesSignature = String(changes.input_signature || '');
        const evidenceSynthesisSignature = String(evidence.synthesis_input_signature || '');
        const stateRevisionPresent = state.revision !== null
            && state.revision !== undefined && state.revision !== '';
        const evidenceRevisionPresent = evidence.generation_revision !== null
            && evidence.generation_revision !== undefined
            && evidence.generation_revision !== '';
        const stateRevision = Number(state.revision);
        const evidenceRevision = Number(evidence.generation_revision);
        return Boolean(
            stateRunId && evidenceRunId && stateRunId === evidenceRunId
            && stateInputSignature && evidenceInputSignature
            && stateInputSignature === evidenceInputSignature
            && stateRevisionPresent && evidenceRevisionPresent
            && Number.isInteger(stateRevision) && Number.isInteger(evidenceRevision)
            && stateRevision === evidenceRevision
            && changesSignature && evidenceSynthesisSignature
            && changesSignature === evidenceSynthesisSignature
        );
    }

    function productionTextEvidenceOverlays(payload, mode, side, page, activeId) {
        const source = object(payload);
        const normalizedMode = String(mode || 'all').toLowerCase();
        const normalizedSide = String(side || '').toLowerCase();
        const normalizedPage = Number(page);
        if (!source.available || source.stale || normalizedMode === 'all'
                || !['left', 'right'].includes(normalizedSide)
                || !Number.isInteger(normalizedPage) || normalizedPage < 1) return [];
        const items = normalizedMode === 'matches'
            ? array(source.matches)
            : normalizedMode === 'changes' ? array(source.changes) : [];
        return items.flatMap(item => {
            const evidenceSide = object(object(item.sides)[normalizedSide]);
            return array(evidenceSide.overlays)
                .filter(overlay => Number(overlay.page) === normalizedPage)
                .map(overlay => ({
                    ...overlay,
                    evidence_id: item.evidence_id,
                    target_id: item.target_id || '',
                    evidence_kind: item.kind,
                    title: item.title,
                    review_required: Boolean(item.review_required),
                    paired: Boolean(item.has_both_sides),
                    active: Boolean(activeId) && String(activeId) === String(item.evidence_id),
                }));
        });
    }

    function productionTextEvidenceItem(payload, evidenceId) {
        const target = String(evidenceId || '');
        if (!target) return null;
        return [...array(object(payload).matches), ...array(object(payload).changes)]
            .find(item => String(item.evidence_id || '') === target) || null;
    }

    function reviewFragmentPhrase(count) {
        const value = Math.max(0, Number(count) || 0);
        const lastTwo = value % 100;
        const last = value % 10;
        const noun = last === 1 && lastTwo !== 11
            ? 'фрагмент'
            : [2, 3, 4].includes(last) && ![12, 13, 14].includes(lastTwo)
                ? 'фрагмента' : 'фрагментов';
        const verb = last === 1 && lastTwo !== 11 ? 'требует' : 'требуют';
        return `${value} ${noun} ${verb} дополнительной проверки.`;
    }

    function textResultState(state, evidence) {
        const textStage = object(object(state.stages).text);
        const blocked = ['status', 'source_state'].some(key => (
            String(textStage[key] || '').toUpperCase() === 'CHECK_BLOCKED'
        )) || String(evidence.text_result_state || '') === 'BLOCKED';
        if (blocked) return 'BLOCKED';
        return String(evidence.text_result_state || '').toUpperCase() || '';
    }

    function pairPhrase(count) {
        const value = Math.max(0, Number(count) || 0);
        const lastTwo = value % 100;
        const last = value % 10;
        if (last === 1 && lastTwo !== 11) return 'пара';
        if ([2, 3, 4].includes(last) && ![12, 13, 14].includes(lastTwo)) return 'пары';
        return 'пар';
    }

    function normalizeProductionTextPresentation(stateValue, evidenceValue) {
        const state = object(stateValue && stateValue.state ? stateValue.state : stateValue);
        const evidence = object(evidenceValue);
        const textStage = object(object(state.stages).text);
        const hasTextStage = Object.keys(textStage).length > 0;
        const stageStatus = normalizePipelineStatus(textStage.status || textStage.source_state);
        const runStatus = normalizePipelineStatus(state.status);
        const stale = Boolean(state.stale || evidence.stale);
        const summary = object(evidence.summary);
        const resultState = textResultState(state, evidence);
        const blocked = resultState === 'BLOCKED';
        const blockedReason = String(evidence.text_blocked_reason || textStage.reason_code || '');
        const blockedError = String(evidence.text_blocked_error || textStage.error_type || '');
        // A blocked branch published zeros for an aborted run.  Reading them
        // as counted results would claim the document was checked.
        const fallbackReview = blocked ? null : firstNumber([textStage], [
            'review_required', 'review_required_atoms', 'unresolved',
        ]);
        const reviewRequired = finiteNumber(summary.review_required) !== null
            ? finiteNumber(summary.review_required)
            : fallbackReview;
        let tone = 'idle';
        let label = 'Не запущен';
        let message = 'TEXT запускается автоматически в составе полного анализа.';
        if (stale) {
            tone = 'warning';
            label = 'Результат устарел';
            message = 'Текстовый результат относится к предыдущему generation. Запустите полный анализ заново.';
        } else if (runStatus === 'RUNNING' || stageStatus === 'RUNNING') {
            tone = 'running';
            label = 'Выполняется';
            message = 'Текстовый анализ выполняется в составе полного автоматического анализа.';
        } else if (blocked) {
            tone = 'warning';
            label = 'Не завершён';
            message = 'Текстовый анализ не завершён: результат не построен.'
                + (blockedReason || blockedError
                    ? ` ${humanizeReasonCode(blockedReason || blockedError)}`
                    : ' Причина не опубликована этапом.');
        } else if (stageStatus === 'FAILED' || (!hasTextStage && runStatus === 'FAILED')) {
            tone = 'warning';
            label = 'Не завершён';
            message = 'Текстовый анализ не удалось завершить. Запустите полный анализ заново.';
        } else if (stageStatus === 'PARTIAL'
                || (!hasTextStage && runStatus === 'PARTIAL')) {
            tone = 'warning';
            label = 'Завершён частично';
            message = 'Текстовый анализ завершён частично. Часть фрагментов требует проверки.';
        } else if (stageStatus === 'NEEDS_REVIEW') {
            tone = 'review';
            label = 'Требует проверки';
            message = reviewRequired > 0
                ? `Детерминированная проверка завершена. ${reviewFragmentPhrase(reviewRequired)}`
                : 'Детерминированная проверка завершена. Часть фрагментов требует дополнительной проверки.';
        } else if (stageStatus === 'COMPLETED') {
            tone = reviewRequired > 0 ? 'review' : 'completed';
            label = reviewRequired > 0 ? 'Требует проверки' : 'Завершён';
            message = reviewRequired > 0
                ? `Детерминированная проверка завершена. ${reviewFragmentPhrase(reviewRequired)}`
                : 'Детерминированная проверка завершена. Дополнительная семантическая проверка не применялась.';
        }
        // Unknown is never rendered as a counted zero: an absent value means
        // the projection could not prove the number, not that it found none.
        const availableMatchPairs = finiteNumber(summary.available_match_pairs);
        const totalChangeItems = finiteNumber(evidence.change_items);
        const availableChangeItems = finiteNumber(evidence.available_change_items);
        const resultCounters = [
            availableMatchPairs === null ? null : {
                label: 'Можно показать на листах',
                value: availableMatchPairs,
                suffix: `${pairPhrase(availableMatchPairs)} совпадений`,
            },
            availableChangeItems === null ? null : {
                label: 'Изменения на листах',
                value: availableChangeItems,
                suffix: totalChangeItems === null ? '' : `из ${totalChangeItems}`,
            },
            ...[
                ['Изменено', summary.changed],
                ['Удалено', summary.removed],
                ['Добавлено', summary.added],
                ['Требуют проверки', reviewRequired],
            ].map(([counterLabel, value]) => (finiteNumber(value) === null ? null : {
                label: counterLabel, value: Number(value), suffix: '',
            })),
        ].filter(Boolean);
        const counters = hasTextStage && !blocked
            && !['NOT_STARTED', 'RUNNING'].includes(stageStatus)
            ? resultCounters : [];
        const itemOverlays = (item, side) => array(
            object(object(object(item).sides)[side]).overlays,
        );
        const matchesHaveCoordinates = array(evidence.matches).some(item => (
            itemOverlays(item, 'left').length > 0 && itemOverlays(item, 'right').length > 0
        ));
        const changesHaveCoordinates = array(evidence.changes).some(item => (
            ['left', 'right'].some(side => itemOverlays(item, side).length > 0)
        ));
        const matchEvidenceState = String(evidence.match_evidence_state || '').toUpperCase();
        const coverageNotes = [];
        if (matchesHaveCoordinates) {
            coverageNotes.push(
                'Показаны только совпадения, для которых есть точные координаты на обеих сторонах.',
            );
        }
        if (changesHaveCoordinates) {
            coverageNotes.push(
                'На листах отображаются только изменения с доступными точными координатами.',
            );
        }
        return {
            tone,
            label,
            message,
            counters,
            coverage_notes: coverageNotes,
            read_only_note: 'Режим просмотра — результаты анализа не изменяются.',
            review_required: reviewRequired,
            text_result_state: resultState,
            blocked,
            blocked_reason: blockedReason,
            match_evidence_state: matchEvidenceState,
            matches_unavailable_reason: matchEvidenceState === 'UNVERIFIED_LEGACY_GENERATION'
                ? 'Совпадения этого расчёта сохранены без подписи, покрывающей точные пары. '
                    + 'Чтобы показать их на листах, запустите полный анализ заново.'
                : '',
            available: Boolean(evidence.available) && !stale,
            can_visualize_matches: Boolean(evidence.available) && !stale
                && matchesHaveCoordinates,
            can_visualize_changes: Boolean(evidence.available) && !stale
                && changesHaveCoordinates,
            show_rerun: stale || blocked || ['FAILED', 'PARTIAL'].includes(stageStatus)
                || (!hasTextStage && ['FAILED', 'PARTIAL'].includes(runStatus)),
            generation_run_id: String(evidence.generation_run_id || state.run_id || ''),
            stage_status: stageStatus,
            run_status: runStatus,
            stale,
        };
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
        aggregateConcurrentPipelineStatus,
        aiExplanation,
        aiModeLabel,
        aiReasonLabel,
        changeLabel,
        confidenceLabel,
        decision,
        decisionLabel,
        dimensionLabel,
        directionLabel,
        objectLabel,
        outcomeLabel,
        pagesReference,
        questionCategoryLabel,
        relationTypeLabel,
        reviewStatusLabel,
        sheetReference,
        sheetStatusLabel,
        sourceLabel,
        estimatePipelineEtaMs,
        evidenceFocus,
        formatActivityAge,
        formatPipelineDuration,
        formatPipelineEta,
        humanizeReasonCode,
        normalizeEvidence,
        normalizeFinalRows,
        normalizePipelineStatus,
        normalizePipelineProgress,
        normalizeProductionTextEvidence,
        normalizeProductionTextPresentation,
        normalizeAiProgress,
        normalizeProductionOverview,
        normalizeProductionPipeline,
        normalizeQuestionCounts,
        normalizeQuestions,
        normalizeRows,
        normalizeSheetSuggestions,
        normalizeSuggestionApplication,
        pagesForSide,
        pipelineStatusLabel,
        productionPollingDirective,
        productionPollingTransition,
        productionRunActivity,
        productionStateResponseAccepted,
        productionTextEvidenceMatchesGeneration,
        productionTextEvidenceItem,
        productionTextEvidenceOverlays,
        reviewCounts,
        text,
    };
}));
