// Одинаковые листы в сборке группы (22.11.2026): чистые функции окна и проводка в портале.
import {describe, expect, it} from 'vitest';
import {readFileSync} from 'node:fs';
import D from '../static/js/stage-comparison-duplicates.js';

const read = path => readFileSync(new URL(path, import.meta.url), 'utf8');
const html = read('../index.html');
const app = read('../static/js/app.js');

function functionSource(name) {
    const start = app.indexOf(`function ${name}(`);
    if (start < 0) throw new Error(`function ${name} not found`);
    let depth = 0;
    for (let i = app.indexOf('{', start); i < app.length; i += 1) {
        if (app[i] === '{') depth += 1;
        if (app[i] === '}') depth -= 1;
        if (depth === 0) return app.slice(start, i + 1);
    }
    throw new Error(`function ${name} not closed`);
}

const CONSTANTS = ['SC_SIDES', 'SC_GROUP_BUILDING'].map(name => app.match(new RegExp(`const ${name} = [^;]+;`))[0]).join('\n');

const pair = (id, extra = {}) => ({
    pair_id: id,
    original: {document_code: 'ЭМ-01', source_index: 0, source_page: 44, sheet: 'Sheet 2 — Альбом'},
    copy: {document_code: 'ЭМ-02', source_index: 1, source_page: 55, sheet: 'Page 55'},
    ignored_zones: {original: [[0.7, 0.86, 0.99, 0.99]], copy: []},
    ...extra,
});

describe('модуль окна дублей', () => {
    it('подписывает лист проектом, листом и страницей без повтора «Page N»', () => {
        expect(D.sideLabel(pair('a').original)).toBe('ЭМ-01 · Sheet 2 — Альбом · стр. 44');
        expect(D.sideLabel(pair('a').copy)).toBe('ЭМ-02 · стр. 55');
    });

    it('плашка строки: поиск, ожидание проверки, итог сборки', () => {
        expect(D.sideBadge({status: 'SCANNING', duplicates: {done_pages: 30, total_pages: 900}}))
            .toEqual({tone: 'running', label: '30 из 900 стр.'});
        expect(D.sideBadge({status: 'REVIEW', duplicates: {pairs: 342, undecided: 342}}))
            .toMatchObject({tone: 'review', label: '342 пары', review: true});
        expect(D.sideBadge({status: 'READY', duplicates: {pairs: 5, removed_pages: 4, kept_pairs: 1}}).label)
            .toBe('Удалено дублей: 4, возвращено: 1');
        // Искали и не нашли — это видно (раньше плашки не было и «не искали» не отличалось).
        expect(D.sideBadge({status: 'READY', page_count: 131, duplicates: {pairs: 0, removed_pages: 0, kept_pairs: 0}}))
            .toEqual({tone: 'done', label: 'Дублей не найдено · проверено 131 стр.'});
        expect(D.sideBadge({status: 'READY', duplicates: {pairs: 0, removed_pages: 0, kept_pairs: 0}}).label)
            .toBe('Дублей не найдено');
        expect(D.sideBadge({status: 'READY'})).toBeNull();
    });

    it('плашка группы: сторона, ждущая проверки, важнее «Дублей не найдено»', () => {
        const group = {
            left: {status: 'READY', page_count: 115, duplicates: {pairs: 0, removed_pages: 0, kept_pairs: 0}},
            right: {status: 'REVIEW', duplicates: {pairs: 67, undecided: 67}},
        };
        expect(D.groupBadge(group)).toMatchObject({side: 'right', review: true, label: '67 пар справа'});
        expect(D.groupBadge({left: group.left, right: {status: 'SINGLE'}}))
            .toEqual({tone: 'done', label: 'Дублей не найдено · проверено 115 стр.', side: 'left'});
        expect(D.groupBadge({left: {status: 'SINGLE'}, right: {status: 'SINGLE'}})).toBeNull();
    });

    it('считает удаляемые и возвращённые копии, помнит прежние возвраты', () => {
        const pairs = [pair('a', {decision: 'keep'}), pair('b', {decision: null}), pair('c', {decision: 'remove'})];
        const keep = D.initialKeep(pairs);
        expect(keep).toEqual(['a']);
        expect(D.counts(pairs, keep)).toEqual({total: 3, keep: 1, remove: 2});
    });

    it('зона исключения — проценты поверх картинки листа', () => {
        expect(D.zoneStyle([0.7, 0.86, 0.99, 0.99]))
            .toEqual({left: '70.000%', top: '86.000%', width: '29.000%', height: '13.000%'});
    });

    it('адрес картинки листа и клавиши окна', () => {
        expect(D.pageUrl('/x/duplicates', 'right', pair('a').copy, 1400))
            .toBe('/x/duplicates/page?side=right&source_index=1&page=55&width=1400');
        expect([D.clampIndex(-1, 3), D.clampIndex(5, 3), D.clampIndex(1, 0)]).toEqual([0, 2, 0]);
        expect(['ArrowRight', 'ArrowLeft', ' ', 'Escape', 'a'].map(D.keyAction))
            .toEqual(['next', 'prev', 'toggle', 'close', null]);
    });
});

describe('проводка в портале', () => {
    it('модуль подключён до app.js, окно и кнопка есть в разметке', () => {
        expect(html.indexOf('stage-comparison-duplicates.js')).toBeGreaterThan(0);
        expect(html.indexOf('stage-comparison-duplicates.js')).toBeLessThan(html.indexOf('/static/js/app.js'));
        expect(html).toContain('scOpenDuplicates(row)">Проверить дубли</button>');
        expect(html).toContain('@click="scSubmitDuplicates()"');
        expect(html).toContain('class="sc-dup-zone"');
    });

    it('поиск дублей — фаза сборки, ожидание проверки — не ошибка и не «Пересобрать»', () => {
        const status = new Function(`${CONSTANTS}\n${functionSource('scGroupSideStatus')}; return scGroupSideStatus;`)();
        expect(status({assembled: true, status: 'SCANNING'})).toMatchObject({tone: 'running', label: 'Ищем одинаковые листы'});
        expect(status({assembled: true, status: 'REVIEW'})).toMatchObject({tone: 'incomplete', label: 'Проверьте одинаковые листы'});
        const needsRebuild = new Function(`${CONSTANTS}\n${functionSource('scGroupNeedsRebuild')}; return scGroupNeedsRebuild;`)();
        expect(needsRebuild({right: {assembled: true, status: 'REVIEW', stale: false}})).toBe(false);
        expect(needsRebuild({right: {assembled: true, status: 'SCANNING', stale: false}})).toBe(false);
        expect(needsRebuild({right: {assembled: true, status: 'FAILED', stale: false}})).toBe(true);
    });

    it('согласование уходит POST-запросом с возвращёнными парами', () => {
        const submit = functionSource('scSubmitDuplicates');
        expect(submit).toContain("method: 'POST'");
        expect(submit).toContain('keep_pair_ids: scDuplicates.keep');
        expect(app).toMatch(/const SC_GROUP_BUILDING = \[[^\]]*'SCANNING'/);
    });
});
