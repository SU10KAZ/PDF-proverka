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

const countExpertUnchecked = Function(`
    ${extractFunction('hasBothExpertChecks')}
    ${extractFunction('expertUncheckedCount')}
    return expertUncheckedCount;
`)();

describe('счётчик непроверенных экспертами проектов', () => {
    it('уменьшается только при двух complete-галочках', () => {
        const projects = [
            { findings_review_status: '', optimization_review_status: '' },
            { findings_review_status: 'complete', optimization_review_status: '' },
            { findings_review_status: '', optimization_review_status: 'complete' },
            { findings_review_status: 'partial', optimization_review_status: 'complete' },
            { findings_review_status: 'complete', optimization_review_status: 'complete' },
            // Общий legacy-статус не заменяет две галочки.
            { expert_review_status: 'complete' },
        ];

        expect(countExpertUnchecked(projects)).toBe(5);
        expect(countExpertUnchecked([])).toBe(0);
        expect(countExpertUnchecked(null)).toBe(0);
    });

    it('показывает красный счётчик перед общим и для всех разделов, и для каждого раздела', () => {
        expect((html.match(/nav-sub-badge--expert-unchecked/g) || []).length).toBe(2);
        expect(html).toContain('{{ expertUncheckedCount(projects) }}');
        expect(html).toContain('{{ expertUncheckedCount(items) }}');
        expect(css).toContain('.nav-sub-badge--expert-unchecked');
        expect(css).toContain('border-color: var(--red);');
    });

    it('обновляет список проектов сразу после сохранения экспертных решений', () => {
        const submitBlock = appJs.match(/async function submitExpertReview\(\) \{[\s\S]*?\n        \}/)?.[0] || '';
        expect(submitBlock).toContain('await refreshProjects();');
    });
});
