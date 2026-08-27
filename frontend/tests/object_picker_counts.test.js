/**
 * Переключатель объектов в шапке: порядок по номеру + два счётчика.
 *
 * Постановка Андрея Ивановича 2026-08-27:
 *   * объекты сортируются по ПЕРВОМУ числу названия по возрастанию
 *     (в objects.json они лежат в порядке создания — 256 оказывался последним);
 *   * после названия — два числа: «Не запускались на проверку» (оранжевое)
 *     и «Нет решений эксперта» (фирменное бирюзовое), оба в рамке,
 *     как бейдж сайдбара (.nav-badge).
 */
import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const frontendRoot = path.resolve(here, '..');
const html = fs.readFileSync(path.join(frontendRoot, 'index.html'), 'utf8');
const css = fs.readFileSync(path.join(frontendRoot, 'static/css/styles.css'), 'utf8');
const appJs = fs.readFileSync(path.join(frontendRoot, 'static/js/app.js'), 'utf8');

function extractFunction(name) {
    const match = appJs.match(new RegExp(`function ${name}\\([^)]*\\) \\{[\\s\\S]*?\\n        \\}`));
    if (!match) throw new Error(`Function ${name} not found`);
    return match[0];
}

const collator = appJs.match(/const _objectNameCollator = [^\n]+/)[0];
const sortBody = appJs.match(/return objectsList\.value\.slice\(\)\.sort\(\(a, b\) => \{[\s\S]*?\n {12}\}\);/)[0]
    .replace('objectsList.value', 'list');

const api = Function(`
    ${collator}
    ${extractFunction('objectLeadNumber')}
    function sortObjects(list) {
        ${sortBody}
    }
    return { objectLeadNumber, sortObjects };
`)();

const names = (list) => api.sortObjects(list).map(o => o.name);

describe('Порядок объектов', () => {
    it('сортирует по первому числу названия по возрастанию', () => {
        expect(names([
            { name: '256. Примавера К14 (Спартак)' },
            { name: '213. Мосфильмовская 31А "King&Sons"' },
            { name: '314. Событие 6.1 (Донстрой)' },
            { name: '214. Alia (ASTERUS)' },
            { name: '272. Садовническая 76 (Балчуг Эстейт)' },
        ])).toEqual([
            '213. Мосфильмовская 31А "King&Sons"',
            '214. Alia (ASTERUS)',
            '256. Примавера К14 (Спартак)',
            '272. Садовническая 76 (Балчуг Эстейт)',
            '314. Событие 6.1 (Донстрой)',
        ]);
    });

    it('число берётся как число, а не как строка (9 перед 10)', () => {
        expect(names([{ name: '10. Десятый' }, { name: '9. Девятый' }]))
            .toEqual(['9. Девятый', '10. Десятый']);
    });

    it('объекты без ведущего числа уходят вниз и идут по алфавиту', () => {
        expect(names([
            { name: 'Тестовый объект' },
            { name: '214. Alia (ASTERUS)' },
            { name: 'Альфа' },
        ])).toEqual(['214. Alia (ASTERUS)', 'Альфа', 'Тестовый объект']);
    });

    it('не мутирует исходный список', () => {
        const list = [{ name: '272. Б' }, { name: '213. А' }];
        api.sortObjects(list);
        expect(list[0].name).toBe('272. Б');
    });
});

describe('Разметка выпадашки', () => {
    it('перебирает отсортированный список, а не исходный', () => {
        expect(html).toMatch(/v-for="obj in sortedObjectsList"/);
        expect(html).toMatch(/v-for="o in sortedObjectsList"/);   // и в форме загрузки
    });

    it('показывает два счётчика после названия объекта', () => {
        const item = html.match(/<div v-for="obj in sortedObjectsList"[\s\S]*?<\/div>\s*<div class="dash-object-picker__add"/)[0];
        expect(item.indexOf('dash-object-picker__name'))
            .toBeLessThan(item.indexOf('obj-badge--todo'));
        expect(item.indexOf('obj-badge--todo')).toBeLessThan(item.indexOf('obj-badge--proc'));
        expect(item).toMatch(/objectStatOf\(obj\.id, 'not_started'\)/);
        expect(item).toMatch(/objectStatOf\(obj\.id, 'no_decisions'\)/);
    });

    it('галочка рендерится всегда — иначе столбцы счётчиков съезжают', () => {
        const item = html.match(/<div v-for="obj in sortedObjectsList"[\s\S]*?<\/div>\s*<div class="dash-object-picker__add"/)[0];
        expect(item).not.toMatch(/dash-object-picker__check" v-if=/);
    });
});

describe('Оформление счётчиков', () => {
    const rule = (sel) => {
        const m = css.match(new RegExp(`\\${sel} \\{[^}]*\\}`));
        if (!m) throw new Error(`CSS rule ${sel} not found`);
        return m[0];
    };

    it('первый — оранжевый, второй — фирменный бирюзовый', () => {
        expect(rule('.obj-badge--todo')).toMatch(/color: var\(--amber\)/);
        expect(rule('.obj-badge--proc')).toMatch(/color: var\(--teal\)/);
    });

    it('оба в рамке с подложкой — как бейдж сайдбара', () => {
        expect(rule('.obj-badge--todo')).toMatch(/background: var\(--amber-light\)/);
        expect(rule('.obj-badge--todo')).toMatch(/border-color: var\(--amber-border\)/);
        expect(rule('.obj-badge--proc')).toMatch(/background: var\(--teal-light\)/);
        expect(rule('.obj-badge--proc')).toMatch(/border-color: var\(--teal-border\)/);
        expect(rule('.obj-badge')).toMatch(/border: 1px solid/);
    });

    it('--amber-border определён в обеих темах', () => {
        expect(css.match(/--amber-border/g).length).toBeGreaterThanOrEqual(3);
    });
});

describe('Загрузка счётчиков', () => {
    it('идёт лениво — при открытии списка объектов', () => {
        expect(appJs).toMatch(/if \(willOpen && which === 'object'\) loadObjectStats\(\);/);
    });

    it('берёт числа у бэкенда, а не считает их во фронте', () => {
        expect(appJs).toMatch(/api\('\/objects\/stats'\)/);
    });

    it('счётчиков нет — бейджи не показываются (объект недоступен)', () => {
        expect(html).toMatch(/v-if="objectStatOf\(obj\.id, 'total'\) !== null"/);
        expect(extractFunction('objectStatOf')).toMatch(/s && !s\.error/);
    });
});
