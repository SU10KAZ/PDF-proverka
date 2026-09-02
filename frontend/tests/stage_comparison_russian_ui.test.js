/**
 * Инженер видит проект, а не внутренности алгоритма.
 *
 * Тесты закрывают три обещания раздела: всё видимое — по-русски; сырые
 * технические коды живут только в «Диагностике»; номер листа из штампа и
 * страница PDF не смешиваются.
 */
import {describe, expect, it} from 'vitest';
import {readFileSync} from 'node:fs';
import {resolve} from 'node:path';

import review from '../static/js/stage-comparison-review.js';

const html = readFileSync(resolve(__dirname, '../index.html'), 'utf8');
const app = readFileSync(resolve(__dirname, '../static/js/app.js'), 'utf8');

// Всё, что попадало в интерфейс сырым кодом.
const INTERNAL_CODES = [
    'UNKNOWN_DIMENSION', 'REVIEW_REQUIRED', 'CHECK_BLOCKED', 'MATCHED',
    'POSSIBLE', 'SPLIT', 'MERGED', 'ENTITY_IDENTITY', 'SHEET_RELATION',
    'STAMP_EXACT', 'MATERIAL_CHANGE', 'PENDING_REVIEW',
];

describe('русский интерфейс раздела сравнения', () => {
    it('переводит каждое закрытое перечисление, а не одно из них', () => {
        expect(review.dimensionLabel('PARAMETER')).toBe('числовое значение');
        expect(review.dimensionLabel('UNKNOWN_DIMENSION'))
            .toBe('тип изменения не определён');
        expect(review.directionLabel('INCREASED')).toBe('увеличено');
        expect(review.outcomeLabel('MATERIAL_CHANGE')).toBe('существенное изменение');
        expect(review.reviewStatusLabel('REVIEW_REQUIRED'))
            .toBe('требуется проверка инженера');
        expect(review.reviewStatusLabel('CHECK_BLOCKED'))
            .toBe('автоматическая проверка не выполнена');
        expect(review.confidenceLabel({level: 'HIGH'})).toBe('высокая');
        expect(review.sourceLabel('GRAPHIC')).toBe('Чертёж');
        expect(review.decisionLabel('APPROVED')).toBe('подтверждено');
        expect(review.questionCategoryLabel('SHEET')).toBe('Лист');
    });

    it('объясняет кардинальность словами, а не стрелкой N→1', () => {
        expect(review.relationTypeLabel('MERGED'))
            .toBe('несколько листов слева объединены в один лист справа');
        expect(review.relationTypeLabel('SPLIT'))
            .toBe('один лист слева разделён на несколько листов справа');
        expect(review.sheetStatusLabel('POSSIBLE'))
            .toBe('предложено, ждёт подтверждения');
    });

    it('ни одна подпись не содержит внутреннего кода', () => {
        const labels = [
            ...INTERNAL_CODES.map(review.dimensionLabel),
            ...INTERNAL_CODES.map(review.directionLabel),
            ...INTERNAL_CODES.map(review.reviewStatusLabel),
            ...INTERNAL_CODES.map(review.relationTypeLabel),
            ...INTERNAL_CODES.map(review.sheetStatusLabel),
            ...INTERNAL_CODES.map(review.decisionLabel),
        ];
        labels.forEach(label => {
            expect(/[A-Z]{3,}/.test(label)).toBe(false);
        });
    });

    it('различает номер листа из штампа и страницу PDF', () => {
        expect(review.sheetReference({
            page: 29, sheet_no: '7', title: 'Корпуса 1, 2. План 3 этажа',
        })).toBe('«Корпуса 1, 2. План 3 этажа», лист 7 (стр. PDF 29)');
        expect(review.sheetReference({page: 29})).toBe('стр. PDF 29');
        expect(review.pagesReference([3, 4])).toBe('стр. PDF 3, 4');
    });

    it('называет объект именем, а хеш прячет в диагностику', () => {
        const rows = review.normalizeRows({rows: [{
            target_id: 'uchg_1',
            target_kind: 'CHANGE',
            change: {
                change_id: 'uchg_1',
                subject_ref: 'text_entity:помещение 24.5',
                project_entity_ref: 'project_text_entity_abcdef',
                dimension: 'PARAMETER',
                direction: 'INCREASED',
            },
        }]});
        expect(rows[0].object_ref).toBe('помещение 24.5');
        expect(rows[0].object_diagnostic).toBe('project_text_entity_abcdef');
        expect(rows[0].change_label).toBe('Свойство не удалось однозначно определить');
    });

    it('показывает обоснование ИИ и не показывает его рассуждений', () => {
        const rows = review.normalizeRows({rows: [{
            target_id: 'uchg_1',
            target_kind: 'CHANGE',
            change: {
                change_id: 'uchg_1',
                dimension: 'PARAMETER',
                provenance: {
                    source_atoms: [{
                        provenance: {
                            ai_change_resolution: {
                                engineering_summary: 'Площадь увеличена с 6,02 до 6,40 м².',
                                confidence: 'HIGH',
                                evidence_quotes: [{side: 'LEFT', quote: '24.5 Кладовая 6,02'}],
                            },
                        },
                    }],
                },
            },
        }]});
        const explanation = rows[0].explanation;
        expect(explanation.summary).toBe('Площадь увеличена с 6,02 до 6,40 м².');
        expect(explanation.confidence).toBe('высокая');
        expect(explanation.quotes[0].side).toBe('слева');
        expect(explanation).not.toHaveProperty('reasoning');
        expect(explanation).not.toHaveProperty('thinking');
    });

    it('прогресс ИИ говорит три числа, а причины — по-русски', () => {
        const progress = review.normalizeAiProgress({state: {stages: {ai_resolution: {
            status: 'COMPLETED', mode: 'STANDARD', total: 900, processed: 820,
            ai_resolved: 760, human_required: 60,
            human_reasons: {VERIFIER_REJECTED: 20, MODEL_DECLINED: 40},
        }}}});
        expect(progress.available).toBe(true);
        expect(progress.progress_label).toBe('820 / 900');
        expect(progress.resolved_label).toBe('Автоматически разрешено: 760');
        expect(progress.human_label).toBe('Осталось человеку: 60');
        expect(progress.reasons[0].label).toBe('ИИ не смог решить по имеющимся данным.');
        expect(progress.mode_label).toBe('стандартный');
    });

    it('выключенный ИИ не показывает блок вовсе', () => {
        expect(review.normalizeAiProgress({state: {stages: {}}}).available).toBe(false);
        expect(review.normalizeAiProgress({state: {stages: {ai_resolution: {mode: 'OFF'}}}})
            .available).toBe(false);
    });
});

describe('шаблон раздела не печатает внутренние коды', () => {
    it('таблица решений показывает русские подписи, а коды — под «Диагностика»', () => {
        expect(html).toContain('{{ row.status_label }}');
        expect(html).toContain('{{ row.source_label }}');
        expect(html).toContain('Уверенность: {{ row.confidence_label }}');
        expect(html).toContain("{{ scProductionSheetRef(row, 'LEFT') }}");
        // reason_codes больше не висят на виду — только внутри <details>.
        const reasonBlock = html.slice(
            html.indexOf('row.reason_codes.length'),
        ).slice(0, 400);
        expect(reasonBlock).toContain('Диагностика');
        expect(html).not.toContain("<small v-if=\"row.reason_codes.length\">");
    });

    it('карточка вопроса зовёт открыть доказательство, а не «лист (стр. N)»', () => {
        expect(html).toContain('Открыть доказательство');
        expect(html).not.toContain('Открыть лист (стр. {{ sheet.page }})');
    });

    it('карточка вопроса называет объект и суть изменения', () => {
        expect(html).toContain('Объект: <b>{{ scProductionQuestionChange(question).object }}</b>');
        expect(html).toContain('Что изменилось: <b>{{ scProductionQuestionChange(question).change }}</b>');
        expect(app).toContain('StageComparisonReview.objectLabel(context)');
    });

    it('причина решения спрашивается словами, а не кодом', () => {
        expect(html).toContain('Причина решения (необязательно)');
        expect(html).not.toContain('Код причины (необязательно)');
    });

    it('не выводит удалённую сводку прогресса в шапке конвейера', () => {
        expect(html).not.toContain('scProductionAiProgress');
        expect(html).not.toContain('scProductionOverview.headline');
        expect(html).not.toContain('Конвейер сравнения: слева → справа');
        expect(app).not.toContain('const scProductionAiProgress = computed');
    });

    it('подписи берутся из одного словаря, а не дублируются в app.js', () => {
        expect(app).toContain('StageComparisonReview.questionCategoryLabel(category)');
        expect(app).toContain('StageComparisonReview.reviewStatusLabel(status)');
        expect(app).toContain('StageComparisonReview.decisionLabel(value)');
        // Старый локальный словарь, возвращавший APPROVED как APPROVED.
        expect(app).not.toContain("APPROVED: 'APPROVED'");
    });
});

describe('прогресс ИИ в панели хода', () => {
    const stage = {
        status: 'COMPLETED', mode: 'DEEP',
        total: 423, processed: 423, ai_resolved: 5, human_required: 418,
        model_calls: 18, cache_hits: 92, duration_ms: 84209,
        vision_items: 15, vision_calls: 15,
        human_reasons: {MODEL_DECLINED: 395, VISION_CONTRADICTS_TEXT: 12,
                        CRITIC_REJECTED: 11},
        budgets_hit: ['max_vision_items'],
    };

    it('показывает разбор по чертежу отдельной строкой «сделано / взято»', () => {
        const progress = review.normalizeAiProgress({state: {stages: {ai_resolution: stage}}});
        expect(progress.vision_title).toBe('ИИ-анализ графики');
        expect(progress.vision_label).toBe('15 / 15');
        expect(progress.progress_label).toBe('423 / 423');
    });

    it('объясняет исчерпанный предел по-русски, а не кодом', () => {
        const progress = review.normalizeAiProgress({state: {stages: {ai_resolution: stage}}});
        expect(progress.budget_labels).toEqual(['достигнут предел разборов по чертежу']);
        expect(progress.budget_labels.join(' ')).not.toContain('max_');
    });

    it('называет причины возврата человеку по-русски', () => {
        const progress = review.normalizeAiProgress({state: {stages: {ai_resolution: stage}}});
        const labels = progress.reasons.map(item => item.label).join(' ');
        expect(labels).not.toContain('MODEL_DECLINED');
        expect(labels).toContain('Чертёж расходится с текстом');
        expect(progress.reasons[0].count).toBe(395);
    });

    it('без разбора по чертежу строки графики нет вовсе', () => {
        const progress = review.normalizeAiProgress({
            state: {stages: {ai_resolution: {...stage, vision_items: 0, vision_calls: 0}}},
        });
        expect(progress.vision_label).toBe('');
    });
});

describe('стороны, режимы и глубина анализа по-русски', () => {
    it('называет стороны словами, а не LEFT и RIGHT', () => {
        expect(review.sideLabel('LEFT')).toBe('Слева');
        expect(review.sideLabel('RIGHT')).toBe('Справа');
        expect(review.sideEditionLabel('LEFT')).toBe('исходная редакция');
        expect(review.inputModeLabel('PAGE')).toBe('Страница ↔ страница');
        expect(review.inputModeLabel('DOCUMENT')).toBe('Документ ↔ документ');
    });

    it('показывает глубину анализа, а не состояние подсистемы', () => {
        expect(review.aiRunModeLabel('FAST')).toBe('Быстро');
        expect(review.aiRunModeLabel('STANDARD')).toBe('Стандартно');
        expect(review.aiRunModeLabel('DEEP')).toBe('Глубокая проверка');
        // «Выключено» инженеру не показывается ни под каким именем.
        expect(review.aiRunModeLabel('OFF')).toBe('Быстро');
    });

    it('объясняет несостоявшуюся контрольную проверку, а не показывает код', () => {
        const label = review.aiReasonLabel('CRITIC_UNAVAILABLE');
        expect(label).toContain('не состоялась');
        expect(label).not.toContain('CRITIC');
        const runtime = review.aiReasonLabel('RUNTIME_UNAVAILABLE');
        expect(runtime).toContain('среда не готова');
        expect(runtime).not.toContain('RUNTIME');
    });

    it('различает «критик не ответил» и «критик ответил не по форме»', () => {
        // Для инженера это две разные истории: одна про доступность, вторая
        // про то, что ответ пришёл и разобрать его нечем.
        const invalid = review.aiReasonLabel('CRITIC_INVALID');
        expect(invalid).toContain('не по форме');
        expect(invalid).not.toContain('CRITIC');
        expect(/[A-Z]{3,}/.test(invalid)).toBe(false);
        expect(invalid).not.toBe(review.aiReasonLabel('CRITIC_UNAVAILABLE'));
    });

    it('называет обе несостоявшиеся проверки в сводке хода ИИ', () => {
        const progress = review.normalizeAiProgress({
            state: {
                stages: {
                    ai_resolution: {
                        mode: 'DEEP',
                        human_reasons: {CRITIC_UNAVAILABLE: 2, CRITIC_INVALID: 3},
                    },
                },
            },
        });
        const byCode = Object.fromEntries(
            progress.reasons.map(reason => [reason.code, reason.label]),
        );
        expect(Object.keys(byCode).sort())
            .toEqual(['CRITIC_INVALID', 'CRITIC_UNAVAILABLE']);
        Object.entries(byCode).forEach(([code, label]) => {
            expect(/[A-Z]{3,}/.test(label), `${code} → ${label}`).toBe(false);
        });
    });

    it('объясняет непроверенную полноту распознавания словами инженера', () => {
        const label = review.humanizeReasonCode('recognition_coverage_not_proven');
        expect(label).toContain('Полнота распознавания');
        expect(label).not.toMatch(/[A-Z]{3,}/);
        const opposite = review.humanizeReasonCode(
            'opposite_side_native_text_contains_value',
        );
        expect(opposite).toContain('разошлось распознавание');
    });

    it('предлагает инженеру выбрать глубину анализа при запуске', () => {
        expect(html).toContain('Глубина анализа');
        expect(html).toContain('v-model="scProductionAiMode"');
        expect(html).toContain('scProductionAiModeOptions');
        expect(app).toContain("{code: 'FAST', label: 'Быстро'}");
        expect(app).toContain("{code: 'DEEP', label: 'Глубокая проверка'}");
        expect(app).not.toContain("{code: 'OFF'");
    });

    it('даёт кнопку остановки идущего анализа', () => {
        expect(html).toContain('Остановить анализ');
        expect(html).toContain('scCancelProductionRun()');
        expect(app).toContain("scProductionRequest(\n                    '/cancel'");
    });
});

describe('в основном интерфейсе не осталось системных кодов', () => {
    // Проверка идёт по видимому тексту шаблона: подписи внутри <details> с
    // диагностикой и значения :value отбрасываются — там код уместен.
    const SECTION = html.slice(
        html.indexOf('class="sc-production-pipeline"'),
        html.indexOf('</body>'),
    );
    const VISIBLE = SECTION
        .replace(/<!--[\s\S]*?-->/g, '')
        .replace(/<details[\s\S]*?<\/details>/g, '')
        .replace(/:value="[^"]*"/g, '')
        .replace(/v-for="[^"]*"/g, '')
        .replace(/:class="[^"]*"/g, '')
        .replace(/:key="[^"]*"/g, '')
        .replace(/\btitle="[^"]*"/g, '')
        .replace(/v-if="[^"]*"/g, '')
        .replace(/v-else-if="[^"]*"/g, '')
        .replace(/@[a-z]+="[^"]*"/g, '')
        .replace(/:disabled="[^"]*"/g, '')
        // Привязки Vue — не подпись: значение перечисления в них обязано
        // остаться кодом, иначе сломается контракт с сервером.
        .replace(/:[a-z-]+="[^"]*"/g, '')
        .replace(/\{\{[^}]*\}\}/g, '');

    const FORBIDDEN = [
        'MATERIAL_CHANGE', 'REVIEW_REQUIRED', 'UNKNOWN_DIMENSION',
        'CHECK_BLOCKED', 'STAMP_EXACT', 'PENDING_REVIEW', 'APPROVED',
        'REJECTED', 'DOCUMENT ↔', 'LEFT → RIGHT', 'Production comparison',
        // Аудит нашёл их в живом интерфейсе: список кодов был неполон, а не
        // проверка слаба. Полный разбор видимого текста — в
        // stage_comparison_visible_language.test.js.
        'LEFT', 'RIGHT', 'GRAPHIC', 'Generation',
        'TEXT дельты', 'GRAPHIC группы',
    ];

    FORBIDDEN.forEach(code => {
        it(`не показывает «${code}» как подпись`, () => {
            expect(VISIBLE).not.toContain(code);
        });
    });

    it('не оставляет «production generation» и «evidence» в видимом тексте', () => {
        expect(VISIBLE.toLowerCase()).not.toContain('production generation');
        expect(VISIBLE).not.toContain('Evidence:');
        expect(VISIBLE).not.toContain('Sheet Matcher');
    });
});
