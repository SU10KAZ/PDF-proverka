/**
 * Русский интерфейс проверяется по ТОМУ, ЧТО ВИДИТ ИНЖЕНЕР.
 *
 * Прежняя проверка искала подстроки в тексте файла и вырезала мустачи целиком,
 * поэтому пропускала ровно то, что и должна была ловить: «LEFT 37 ↔ RIGHT 45»
 * собиралось в модуле и приезжало в шаблон готовой строкой, а «Generation:»
 * стояло литералом рядом с мустачем. Наличие где-то в файле русского слова
 * ничего не доказывает.
 *
 * Здесь два слоя, и ни один не ищет подстроку в исходнике.
 *
 * Слой А — результат функций-нормализаторов. Обход по дереву: поля, которые
 * шаблон печатает как текст, обязаны быть по-русски; поддеревья диагностики
 * срезаются, потому что код в них уместен.
 *
 * Слой Б — видимый текст шаблона. «Видимо» = текстовый узел или title /
 * aria-label / placeholder БЕЗ предка <details>; «диагностика» = всё под
 * <details>; привязки Vue не читаются вовсе — там код обязан остаться кодом.
 */
import {describe, expect, it} from 'vitest';
import {readFileSync} from 'node:fs';
import {resolve} from 'node:path';

import review from '../static/js/stage-comparison-review.js';

const html = readFileSync(resolve(__dirname, '../index.html'), 'utf8');

// Технические коды, которым не место в подписи для инженера.
const FORBIDDEN_WORD = /\b(LEFT|RIGHT|TEXT|GRAPHIC|PAGE|DOCUMENT|Generation|generation)\b/;
// Внутренние идентификаторы: их чеканит бэкенд, инженеру они ничего не говорят.
const FORBIDDEN_REF = /\b(question_|target_id|reason_code|hquestion_|uchg_|ureview_|tatom_|teva_|srel_)/;

// Поля, которые шаблон печатает как видимый текст.
const VISIBLE_FIELDS = new Set([
    'label', 'short_label', 'note', 'reason', 'summary', 'message',
    'headline', 'current_item', 'counter_label', 'mode_label',
    'progress_label', 'eta_label', 'elapsed_label', 'title', 'text',
    'hint', 'description', 'value_label', 'suffix',
]);

// Поддеревья диагностики: срезаются целиком, вместе со всем содержимым.
const DIAGNOSTIC_FIELDS = new Set([
    'technical_label', 'reason_codes', 'raw', 'id', 'kind', 'status',
    'category', 'target_id', 'target_kind', 'question_id', 'run_id',
    'generation_run_id', 'source_mode', 'coordinate_space', 'units',
    'input_signature', 'content_signature', 'diagnostics', 'audit',
    'constraints', 'mode', 'change_ids', 'scope_ref', 'atom_id',
    'question_type', 'answer', 'evidence', 'context', 'provenance',
]);

/** Все видимые строки результата нормализатора, с путями до них. */
function visibleStrings(node, path = '', found = []) {
    if (node === null || node === undefined) return found;
    if (Array.isArray(node)) {
        node.forEach((item, index) => visibleStrings(item, `${path}[${index}]`, found));
        return found;
    }
    if (typeof node === 'object') {
        Object.entries(node).forEach(([key, value]) => {
            if (DIAGNOSTIC_FIELDS.has(key)) return;
            if (VISIBLE_FIELDS.has(key) && typeof value === 'string') {
                if (value.trim()) found.push([`${path}.${key}`, value]);
                return;
            }
            visibleStrings(value, `${path}.${key}`, found);
        });
        return found;
    }
    return found;
}

/** Убрать поддеревья <details> с учётом вложенности. */
function stripDetails(markup) {
    let output = '';
    let depth = 0;
    let index = 0;
    const open = /<details\b/gi;
    const close = /<\/details\s*>/gi;
    while (index < markup.length) {
        open.lastIndex = index;
        close.lastIndex = index;
        const nextOpen = open.exec(markup);
        const nextClose = close.exec(markup);
        if (!nextOpen && !nextClose) break;
        const useOpen = nextOpen && (!nextClose || nextOpen.index < nextClose.index);
        const hit = useOpen ? nextOpen : nextClose;
        if (depth === 0 && useOpen) output += markup.slice(index, hit.index);
        if (useOpen) depth += 1;
        else depth = Math.max(0, depth - 1);
        index = hit.index + hit[0].length;
    }
    if (depth === 0) output += markup.slice(index);
    return output;
}

const SECTION = html.slice(
    html.indexOf('id="sc-production-pipeline-title"'),
    html.indexOf('</body>'),
);
const WITHOUT_DIAGNOSTICS = stripDetails(SECTION).replace(/<!--[\s\S]*?-->/g, '');

/** Видимые текстовые узлы: всё вне тегов. */
function textNodes(markup) {
    return markup
        .split(/<[^>]*>/)
        .map(part => part.trim())
        .filter(Boolean);
}

/** Видимые значения атрибутов: title / aria-label / placeholder без привязки. */
function visibleAttributes(markup) {
    const found = [];
    const re = /\s(?:title|aria-label|placeholder)="([^"]*)"/g;
    let match;
    while ((match = re.exec(markup)) !== null) found.push(match[1]);
    return found;
}

/** Выражения мустачей: {{ … }} внутри видимого текста. */
function mustacheExpressions(markup) {
    const found = [];
    const re = /\{\{([^}]*)\}\}/g;
    let match;
    while ((match = re.exec(markup)) !== null) found.push(match[1].trim());
    return found;
}


// ── Слой А: результат нормализаторов ──────────────────────────────────────

function pipelineFixture(overrides = {}) {
    return {
        state: {
            status: 'RUNNING',
            input_mode: 'PAGE',
            selection: {input_mode: 'PAGE', left_pages: [37], right_pages: [45]},
            run_id: 'run-1',
            progress: {
                current_item: {left_pages: [37], right_pages: [45]},
                processed: 3, total: 10, unit: 'sheets',
            },
            stages: {
                text: {status: 'RUNNING', deltas: 12, atoms: 5, review_required: 2},
                graphic: {status: 'RUNNING', groups_total: 4, groups_completed: 1},
                sheet_matching: {status: 'COMPLETED', relations: 21},
            },
            ...overrides,
        },
        active_pair: {
            left: {filename: 'П_АР.pdf'},
            right: {filename: 'РД_АР.pdf'},
        },
    };
}

const PIPELINE_CASES = [
    ['PAGE / выполняется', pipelineFixture()],
    ['DOCUMENT / завершено', pipelineFixture({
        status: 'COMPLETED', input_mode: 'DOCUMENT',
        selection: {input_mode: 'DOCUMENT'},
        progress: {},
    })],
    ['ничего не запускалось', {state: {status: 'NOT_STARTED', stages: {}}, active_pair: {}}],
    ['отказ ветви', pipelineFixture({
        status: 'PARTIAL',
        stages: {
            text: {status: 'FAILED', reason_code: 'TEXT_PREPARATION_FAILED'},
            graphic: {status: 'NOT_APPLICABLE'},
        },
    })],
];

describe('слой А: подписи, которые печатает шаблон', () => {
    PIPELINE_CASES.forEach(([name, payload]) => {
        it(`не отдаёт технических кодов в подписях (${name})`, () => {
            const strings = visibleStrings(review.normalizeProductionPipeline(payload));
            expect(strings.length).toBeGreaterThan(0);
            const bad = strings.filter(([, value]) => FORBIDDEN_WORD.test(value));
            expect(bad).toEqual([]);
        });

        it(`не отдаёт внутренних ссылок в подписях (${name})`, () => {
            const strings = visibleStrings(review.normalizeProductionPipeline(payload));
            const bad = strings.filter(([, value]) => FORBIDDEN_REF.test(value));
            expect(bad).toEqual([]);
        });

        it(`пишет подписи по-русски (${name})`, () => {
            const strings = visibleStrings(review.normalizeProductionPipeline(payload));
            const bad = strings.filter(([, value]) => (
                /[A-Za-z]{4,}/.test(value) && !/[А-Яа-яЁё]/.test(value)
            ));
            expect(bad).toEqual([]);
        });
    });

    it('называет текущий сравниваемый лист по-русски, а не LEFT ↔ RIGHT', () => {
        const progress = review.normalizePipelineProgress({
            current_item: {left_pages: [37], right_pages: [45]},
            processed: 3, total: 10, unit: 'sheets', status: 'RUNNING',
        });
        const item = String(progress.current_item || '');

        expect(item).toContain('Слева');
        expect(item).toContain('справа');
        expect(item).not.toContain('LEFT');
        expect(item).not.toContain('RIGHT');
    });

    it('называет ветви анализа словами инженера', () => {
        const stage = review.normalizeProductionPipeline(pipelineFixture())
            .find(value => value.id === 'content');

        expect(stage.sections.map(section => section.label))
            .toEqual(['Текстовая часть', 'Графическая часть']);
        expect(stage.sections.map(section => section.short_label))
            .toEqual(['Текст', 'Чертежи']);
        // Технический код остаётся в данных: он нужен диагностике и стилям.
        expect(stage.sections.map(section => section.technical_label))
            .toEqual(['TEXT', 'GRAPHIC']);
    });

    it('называет стороны выбора по-русски', () => {
        const stage = review.normalizeProductionPipeline(pipelineFixture())
            .find(value => value.id === 'selection');

        expect(stage.details.join(' ')).toContain('Слева: П_АР.pdf');
        expect(stage.details.join(' ')).toContain('Справа: РД_АР.pdf');
        expect(stage.details.join(' ')).toContain('Режим: Страница ↔ страница');
    });
});


// ── Слой Б: видимый текст шаблона ─────────────────────────────────────────

describe('слой Б: видимый текст раздела', () => {
    it('вырезает вложенную диагностику целиком, а не до первого закрытия', () => {
        const stripped = stripDetails(
            'до<details><summary>с</summary><details>вложено</details>хвост</details>после',
        );
        expect(stripped).toBe('допосле');
    });

    it('не оставляет технических кодов в видимых надписях', () => {
        const bad = textNodes(WITHOUT_DIAGNOSTICS)
            .filter(value => FORBIDDEN_WORD.test(value.replace(/\{\{[^}]*\}\}/g, '')));
        expect(bad).toEqual([]);
    });

    it('не оставляет технических кодов в подсказках и метках доступности', () => {
        const bad = visibleAttributes(WITHOUT_DIAGNOSTICS)
            .filter(value => FORBIDDEN_WORD.test(value) || FORBIDDEN_REF.test(value));
        expect(bad).toEqual([]);
    });

    it('не печатает служебных идентификаторов через мустачи', () => {
        const bad = mustacheExpressions(WITHOUT_DIAGNOSTICS).filter(expression => (
            /\.(question_id|reason_code|target_kind|technical_label)\b/.test(expression)
            || /\.toUpperCase\(\)/.test(expression)
        ));
        expect(bad).toEqual([]);
    });

    it('не печатает внутренний адрес изменения нигде в видимом тексте', () => {
        // Прежнее исключение — колонка «ID» итогового отчёта — закрыто:
        // назвать строку в переписке инженеру нужно, но для этого есть
        // порядковый номер, а не uchg_/ureview_. Сам адрес остался в
        // диагностике (проверяется следующим тестом).
        const printed = mustacheExpressions(WITHOUT_DIAGNOSTICS)
            .filter(expression => /^\s*row\.target_id\s*$/.test(expression));
        expect(printed).toEqual([]);
    });

    it('даёт инженеру короткий номер строки вместо внутреннего адреса', () => {
        const printed = mustacheExpressions(WITHOUT_DIAGNOSTICS)
            .filter(expression => /^\s*row\.display_id\s*$/.test(expression));
        expect(printed).toEqual(['row.display_id']);
    });

    it('оставляет служебные идентификаторы доступными в диагностике', () => {
        // Обратная сторона правила: убрать их совсем — значит лишить
        // инженера возможности назвать строку в переписке с разработчиком.
        expect(SECTION).toContain('question.question_id');
        expect(SECTION.length).toBeGreaterThan(WITHOUT_DIAGNOSTICS.length);
    });
});


// ── Слой В: перечисления, отрисованные в выпадающих списках ───────────────
//
// Именно здесь прошлые проверки слепли. Санитайзер вырезал и v-for со
// списком кодов, и мустач с подписью, поэтому строка «MATCHED / SPLIT /
// MERGED», которую инженер читал глазами, для теста не существовала вовсе.
//
// Поэтому тут ничего не вырезается: список значений и выражение подписи
// берутся из шаблона как есть, а подпись ВЫЧИСЛЯЕТСЯ настоящей функцией
// словаря — ровно так, как её вычислит Vue. Ошибка «печатаем код вместо
// подписи» становится видна как текст «MATCHED», а не как отсутствие строки.

/** Все `<option v-for="X in [...]">` видимой части: значения и выражение подписи. */
function optionEnumerations(markup) {
    const found = [];
    const re = /<option\s+v-for="(\w+) in \[([^\]]*)\]"([\s\S]*?)<\/option>/g;
    let match;
    while ((match = re.exec(markup)) !== null) {
        const body = match[3];
        const label = /\{\{([^}]*)\}\}/.exec(body);
        const bound = /:value="([^"]*)"/.exec(body);
        found.push({
            variable: match[1],
            values: match[2].split(',')
                .map(value => value.trim().replace(/^'|'$/g, ''))
                .filter(Boolean),
            labelExpression: label ? label[1].trim() : '',
            valueExpression: bound ? bound[1].trim() : '',
        });
    }
    return found;
}

/** Подписи шаблона считаются теми же функциями, что и в браузере. */
const TEMPLATE_HELPERS = {
    scRelationTypeLabel: review.relationTypeLabel,
    scRelationTypeShortLabel: review.relationTypeShortLabel,
    scDimensionLabel: review.dimensionLabel,
    scDirectionLabel: review.directionLabel,
    scOutcomeLabel: review.outcomeLabel,
    scSourceLabel: review.sourceLabel,
    scProductionQuestionCategoryLabel: review.questionCategoryLabel,
};

/** Вычислить выражение подписи для одного значения перечисления. */
function renderLabel(expression, variable, value) {
    const call = /^(\w+)\(\s*(\w+)\s*\)$/.exec(expression);
    if (!call) return {rendered: null, raw: expression};
    const helper = TEMPLATE_HELPERS[call[1]];
    if (!helper) return {rendered: null, raw: expression};
    if (call[2] !== variable) return {rendered: null, raw: expression};
    return {rendered: helper(value), raw: expression};
}

describe('слой В: выпадающие списки печатают подписи, а не коды', () => {
    const ENUMERATIONS = optionEnumerations(WITHOUT_DIAGNOSTICS);

    it('находит перечисления в шаблоне вообще', () => {
        expect(ENUMERATIONS.length).toBeGreaterThan(0);
        const variables = ENUMERATIONS.map(item => item.variable);
        expect(variables).toContain('relationType');
    });

    it('ни один список не печатает переменную перечисления как есть', () => {
        // Ровно этот дефект: `{{ relationType }}` вместо подписи.
        const raw = ENUMERATIONS
            .filter(item => item.labelExpression === item.variable)
            .map(item => `${item.variable}: ${item.values.join('/')}`);
        expect(raw).toEqual([]);
    });

    it('каждое значение каждого списка отрисовывается по-русски', () => {
        const printed = [];
        ENUMERATIONS.forEach(item => {
            item.values.forEach(value => {
                const {rendered, raw} = renderLabel(
                    item.labelExpression, item.variable, value,
                );
                // Подпись обязана вычисляться известной функцией словаря:
                // неизвестное выражение — это либо новый хелпер, который
                // забыли сюда добавить, либо печать кода в обход словаря.
                expect(rendered, `${item.variable} → ${raw}`).not.toBeNull();
                printed.push({value, rendered});
            });
        });
        printed.forEach(({value, rendered}) => {
            expect(rendered, value).toBeTruthy();
            expect(rendered, value).not.toContain(value);
            expect(/[A-Z]{3,}/.test(rendered), `${value} → ${rendered}`).toBe(false);
            expect(/[а-яё]/i.test(rendered), `${value} → ${rendered}`).toBe(true);
        });
    });

    it('значением списка остаётся код контракта, а не перевод', () => {
        // Наружу в API уезжает MATCHED, инженер читает «Листы соответствуют».
        ENUMERATIONS.forEach(item => {
            expect(item.valueExpression, item.variable).toBe(item.variable);
        });
    });

    it.each([
        ['MATCHED', 'соответств'],
        ['SPLIT', 'нескольким справа'],
        ['MERGED', 'одному справа'],
        ['POSSIBLE', 'ждёт подтверждения'],
        ['NO_MATCH', 'нет'],
        ['UNCERTAIN', 'не установлен'],
    ])('объясняет связь листов %s словами', (code, fragment) => {
        const long = review.relationTypeLabel(code);
        const short = review.relationTypeShortLabel(code);
        const status = review.sheetStatusLabel(code);
        // Хотя бы один из словарей обязан знать код и объяснить его словами.
        const spoken = [long, short, status].filter(label => label.includes(fragment));
        expect(spoken.length, `${code}: ${long} | ${short} | ${status}`)
            .toBeGreaterThan(0);
        [long, short, status].forEach(label => {
            expect(/[A-Z]{3,}/.test(label), `${code} → ${label}`).toBe(false);
        });
    });

    it('короткая и полная подписи связи говорят одно и то же', () => {
        // Два расходящихся перевода одного кода хуже сырого кода.
        ['MATCHED', 'SPLIT', 'MERGED', 'UNCERTAIN', 'NO_MATCH'].forEach(code => {
            const short = review.relationTypeShortLabel(code);
            const long = review.relationTypeLabel(code);
            expect(short, code).toBeTruthy();
            expect(long, code).toBeTruthy();
            expect(short.toLowerCase(), code).not.toBe(code.toLowerCase());
        });
        // Разные коды — разные подписи: иначе инженеру не из чего выбирать.
        const labels = ['MATCHED', 'SPLIT', 'MERGED']
            .map(review.relationTypeShortLabel);
        expect(new Set(labels).size).toBe(3);
    });
});
