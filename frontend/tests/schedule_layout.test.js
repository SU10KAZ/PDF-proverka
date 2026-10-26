import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const styles = readFileSync(new URL('../static/css/styles.css', import.meta.url), 'utf8');
const reserveStyles = readFileSync(new URL('../static/css/reserve-calendar.css', import.meta.url), 'utf8');

describe('компоновка графика работ', () => {
    it('держит управление периодом в одной верхней строке с вкладками', () => {
        const topbar = html.slice(html.indexOf('<div class="sched-topbar">'), html.indexOf('<div id="sched-reserve-panel"'));
        expect(topbar).toContain('sched-reserve-tabs');
        expect(topbar).toContain('sched-actions--top');
        expect(topbar).toContain('Изменить план');
        expect(reserveStyles).toContain('.sched-topbar{display:flex;align-items:center;justify-content:space-between');
    });

    it('использует компактные KPI и отдельную обёртку для длинных счётчиков', () => {
        expect(styles).toContain('.sched-kpi { display: flex; align-items: center; gap: 11px; min-height: 64px;');
        expect(styles).toContain('.sched-stats-table th:nth-child(5) { width: 14%; }');
        expect(html.match(/class="sched-remarks-values"/g)).toHaveLength(2);
        expect(styles).toContain('font-variant-numeric: tabular-nums');
    });
});
