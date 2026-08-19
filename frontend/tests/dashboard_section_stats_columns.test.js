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

const suffixRe = appJs.match(/const _VERSION_SUFFIX_RE = [^\n]+/)[0];

const api = Function(`
    ${suffixRe}
    ${extractFunction('projectBaseKey')}
    ${extractFunction('isProjectUnanalyzed')}
    ${extractFunction('cardVersionRank')}
    ${extractFunction('latestProjectCards')}
    ${extractFunction('isProjectExpertResolved')}
    return { isProjectUnanalyzed, latestProjectCards, isProjectExpertResolved, cardVersionRank };
`)();

// Столбцы главной считаются ровно так же, как в sectionStatsMap.
function columns(items) {
    const latest = api.latestProjectCards(items);
    let expertChecked = 0, notStarted = 0;
    for (const p of latest) {
        if (api.isProjectExpertResolved(p)) expertChecked++;
        if (api.isProjectUnanalyzed(p)) notStarted++;
    }
    return {
        total: latest.length,
        notStarted,
        noDecisions: latest.length - expertChecked,
        expertChecked,
    };
}

const audited = (extra = {}) => ({
    findings_count: 5, optimization_count: 0,
    findings_review_status: '', optimization_review_status: '', ...extra,
});

describe('«Не запускались на проверку»', () => {
    it('считает проекты без единого результата аудита', () => {
        expect(api.isProjectUnanalyzed({ findings_count: 0, optimization_count: 0 })).toBe(true);
        expect(api.isProjectUnanalyzed({ findings_count: 3, optimization_count: 0 })).toBe(false);
        // аудит нашёл только оптимизации — он всё равно запускался
        expect(api.isProjectUnanalyzed({ findings_count: 0, optimization_count: 4 })).toBe(false);
    });
});

describe('«Проверено Экспертом» — строго по последней версии', () => {
    it('непроверенная последняя версия обнуляет проверку проекта', () => {
        const cards = [
            { project_id: 'X V1', base_project_key: 'x', ...audited({ findings_review_status: 'complete' }) },
            { project_id: 'X V2', base_project_key: 'x', ...audited({ findings_review_status: 'complete' }) },
            { project_id: 'X V3', base_project_key: 'x', ...audited({ findings_review_status: 'partial' }) },
        ];
        expect(columns(cards)).toEqual({ total: 1, notStarted: 0, noDecisions: 1, expertChecked: 0 });
    });

    it('проверенная последняя версия даёт один проверенный проект, а не три', () => {
        const cards = [
            { project_id: 'X V1', base_project_key: 'x', ...audited({ findings_review_status: 'complete' }) },
            { project_id: 'X V2', base_project_key: 'x', ...audited({ findings_review_status: 'complete' }) },
            { project_id: 'X V3', base_project_key: 'x', ...audited({ findings_review_status: 'complete' }) },
        ];
        expect(columns(cards)).toEqual({ total: 1, notStarted: 0, noDecisions: 0, expertChecked: 1 });
    });

    it('последняя версия без аудита — проект «не запускался», а не «проверен»', () => {
        const cards = [
            { project_id: 'X V1', base_project_key: 'x', ...audited({ findings_review_status: 'complete' }) },
            { project_id: 'X V2', base_project_key: 'x', findings_count: 0, optimization_count: 0 },
        ];
        expect(columns(cards)).toEqual({ total: 1, notStarted: 1, noDecisions: 1, expertChecked: 0 });
    });

    it('пустая категория галочку не блокирует, сводный статус — fallback', () => {
        expect(api.isProjectExpertResolved(audited({ findings_review_status: 'complete' }))).toBe(true);
        expect(api.isProjectExpertResolved(audited({
            optimization_count: 2, findings_review_status: 'complete',
            optimization_review_status: '',
        }))).toBe(false);
        expect(api.isProjectExpertResolved(audited({ expert_review_status: 'complete' }))).toBe(true);
        // проверять нечего — в «Проверено» такой проект не попадает
        expect(api.isProjectExpertResolved({
            findings_count: 0, optimization_count: 0, expert_review_status: 'complete',
        })).toBe(false);
    });
});

describe('«Нет решений эксперта» и «Всего»', () => {
    it('всё, кроме полностью проверенных, включая проекты без аудита', () => {
        const cards = [
            { project_id: 'A', base_project_key: 'a', ...audited({ findings_review_status: 'complete' }) },
            { project_id: 'B', base_project_key: 'b', ...audited() },
            { project_id: 'C', base_project_key: 'c', findings_count: 0, optimization_count: 0 },
        ];
        expect(columns(cards)).toEqual({ total: 3, notStarted: 1, noDecisions: 2, expertChecked: 1 });
    });

    it('«Всего» считает уникальные проекты, а не карточки-версии', () => {
        const cards = [
            { project_id: 'X V1', base_project_key: 'x', ...audited() },
            { project_id: 'X_V2', base_project_key: 'x', ...audited() },
            { project_id: 'Y', base_project_key: 'y', ...audited() },
        ];
        expect(columns(cards).total).toBe(2);
    });
});

describe('выбор последней карточки проекта', () => {
    it('старшинство решает версионный суффикс имени', () => {
        expect(api.cardVersionRank({ project_id: 'X V2' })).toBe(2);
        expect(api.cardVersionRank({ project_id: 'X_V10' })).toBe(10);
        expect(api.cardVersionRank({ project_id: 'X' })).toBe(0);
        const picked = api.latestProjectCards([
            { project_id: 'X V2', base_project_key: 'x' },
            { project_id: 'X', base_project_key: 'x' },
        ]);
        expect(picked.map(p => p.project_id)).toEqual(['X V2']);
    });

    it('при равных суффиксах решает version_no карточки', () => {
        const picked = api.latestProjectCards([
            { project_id: 'X', base_project_key: 'x', version_no: 1 },
            { project_id: 'X', base_project_key: 'x', version_no: 3 },
        ]);
        expect(picked[0].version_no).toBe(3);
    });
});

describe('шапка таблицы «Разделы проекта»', () => {
    it('новые заголовки столбцов', () => {
        expect(html).toContain('>Не запускались на проверку<');
        expect(html).toContain('>Нет решений эксперта<');
        expect(html).toContain('>Проверено Экспертом<');
        expect(html).not.toContain('>Необработаны<');
        expect(html).not.toContain('>Обработаны<');
    });

    it('строки и «Итого» читают новые ключи', () => {
        expect(html).toContain('{{ sectionStatsMap[code].notStarted }}');
        expect(html).toContain('{{ sectionStatsMap[code].noDecisions }}');
        expect(html).toContain('{{ sectionStatsMap[code].expertChecked }}');
        expect(html).toContain('{{ sectionStatsTotals.notStarted }}');
        expect(html).toContain('{{ sectionStatsTotals.noDecisions }}');
        expect(html).toContain('{{ sectionStatsTotals.expertChecked }}');
        expect(html).not.toContain('sectionStatsMap[code].unanalyzed');
        expect(html).not.toContain('sectionStatsTotals.analyzed');
    });

    it('длинные заголовки переносятся и стоят над своими цифрами', () => {
        expect(css).toContain('white-space: normal; line-height: 1.25; overflow-wrap: break-word;');
        expect(css).toMatch(/\.dash-stat \{\s*\n\s*width: 118px;/);
        expect(css).toMatch(/\.dash-stat__head \{\s*\n\s*width: 118px;/);
    });
});
