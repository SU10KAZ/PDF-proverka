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

const counters = Function(`
    ${extractFunction('hasBothExpertChecks')}
    ${extractFunction('isProjectReviewPending')}
    ${extractFunction('projectBaseKey')}
    ${extractFunction('uniqueProjectCount')}
    ${extractFunction('expertUncheckedCount')}
    return { expertUncheckedCount, uniqueProjectCount };
`)();
const countExpertUnchecked = counters.expertUncheckedCount;
const countUniqueProjects = counters.uniqueProjectCount;

describe('счётчик непроверенных экспертами проектов', () => {
    it('решает review_pending от бэкенда, fallback — две галочки', () => {
        const projects = [
            { project_id: 'a', review_pending: true },
            { project_id: 'b', review_pending: false },
            // без поля — старая логика двух галочек
            { project_id: 'c', findings_review_status: '', optimization_review_status: '' },
            { project_id: 'd', findings_review_status: 'partial', optimization_review_status: 'complete' },
            { project_id: 'e', findings_review_status: 'complete', optimization_review_status: 'complete' },
        ];

        expect(countExpertUnchecked(projects)).toBe(3);
        expect(countExpertUnchecked([])).toBe(0);
        expect(countExpertUnchecked(null)).toBe(0);
    });

    it('считает проекты, а не карточки-версии', () => {
        const projects = [
            { project_id: 'X V1', base_project_key: 'x', review_pending: true },
            { project_id: 'X_V1', base_project_key: 'x', review_pending: true },
            { project_id: 'Y', base_project_key: 'y', review_pending: true },
        ];

        expect(countUniqueProjects(projects)).toBe(2);
        expect(countExpertUnchecked(projects)).toBe(2);
        // без base_project_key ключом остаётся project_id — счёт как раньше
        expect(countUniqueProjects([{ project_id: 'A' }, { project_id: 'B' }])).toBe(2);
    });

    it('показывает счётчики у кнопки «Разделы» и у конкретных разделов', () => {
        expect((html.match(/nav-sub-badge--expert-unchecked/g) || []).length).toBe(2);
        expect(html).toContain('v-if="expertUncheckedCount(projects) > 0"');
        expect(html).toContain('{{ expertUncheckedCount(projects) }}');
        expect(html).toContain('v-if="expertUncheckedCount(items) > 0"');
        expect(html).toContain('{{ expertUncheckedCount(items) }}');
        expect(html).toContain('{{ uniqueProjectCount(projects) }}');
        expect(html).toContain('{{ uniqueProjectCount(items) }}');
        expect(css).toContain('.nav-sub-badge--expert-unchecked');
        expect(css).toContain('border-bottom: 1px solid currentColor;');
        expect(css).toContain('font-size: 9px;');
        expect(css).toContain('box-shadow: none;');
        expect(css).toContain('align-items: baseline;');
    });

    it('обновляет список проектов сразу после сохранения экспертных решений', () => {
        const submitBlock = appJs.match(/async function submitExpertReview\(\) \{[\s\S]*?\n        \}/)?.[0] || '';
        expect(submitBlock).toContain('await refreshProjects();');
    });
});

describe('кнопка «Разделы» объединена с «Все разделы»', () => {
    it('отдельного пункта «Все разделы» в подменю больше нет', () => {
        const submenu = html.match(/<div class="nav-submenu"[\s\S]*?<\/div>\s*<div class="nav-item"/)?.[0] || '';
        expect(submenu).not.toContain('Все разделы');
        expect(html).not.toContain(">\n                        Все разделы");
    });

    it('клик по «Разделы» открывает страницу и переключает список', () => {
        expect(html).toContain('@click="toggleSectionsNav()"');
        const fn = extractFunction('toggleSectionsNav');
        expect(fn).toContain('sidebarSectionsOpen.value = !sidebarSectionsOpen.value');
        expect(fn).toContain("navigate('/section/__all__')");
    });

    it('кнопка подсвечивается на странице «Разделы проекта»', () => {
        expect(html).toContain("active: currentView === 'dashboard' && sidebarFilterSection === '__all__'");
        expect(css).toContain('.nav-item-parent.active');
        expect(css).toMatch(/\.nav-parent-badges \{[^}]*margin-left: auto/);
    });

    it('подпись и шеврон помещаются рядом с бейджами в 214px сайдбара', () => {
        // Подпись обёрнута в span и сжимается многоточием, бейджи и шеврон
        // не выдавливаются наружу — иначе стрелка уезжала за плашку.
        expect(html).toContain('<span class="nav-parent-label">Разделы</span>');
        expect(css).toMatch(/\.nav-parent-label \{[^}]*text-overflow: ellipsis/);
        expect(css).toMatch(/\.nav-parent-badges \{[^}]*flex-shrink: 0/);
        expect(css).toContain('.nav-item-parent .nav-arrow { width: 12px; height: 12px; }');
    });

    it('маршрут __all__ не раскрывает подменю принудительно', () => {
        expect(appJs).toContain("if (code !== '__all__') sidebarSectionsOpen.value = true;");
    });
});
