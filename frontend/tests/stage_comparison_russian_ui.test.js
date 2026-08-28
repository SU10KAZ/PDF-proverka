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
        expect(review.sourceLabel('GRAPHIC')).toBe('чертёж');
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
        expect(rows[0].change_label).toBe('числовое значение · увеличено');
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

    it('прогресс ИИ выведен на карточку конвейера', () => {
        expect(html).toContain('scProductionAiProgress.available');
        expect(html).toContain('{{ scProductionAiProgress.resolved_label }}');
        expect(html).toContain('Почему часть осталась инженеру');
        expect(app).toContain('const scProductionAiProgress = computed');
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
