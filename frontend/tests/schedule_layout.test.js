import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const styles = readFileSync(new URL('../static/css/styles.css', import.meta.url), 'utf8');
const reserveStyles = readFileSync(new URL('../static/css/reserve-calendar.css', import.meta.url), 'utf8');
const reserveScript = readFileSync(new URL('../static/js/reserve-calendar.js', import.meta.url), 'utf8');

describe('компоновка графика работ', () => {
    it('держит управление периодом в одной верхней строке с вкладками', () => {
        const topbar = html.slice(html.indexOf('<div class="sched-topbar">'), html.indexOf('<div id="sched-reserve-panel"'));
        expect(topbar).toContain('sched-reserve-tabs');
        expect(topbar).toContain('sched-actions--top');
        expect(topbar).toContain('Изменить план');
        expect(topbar).not.toContain('>Сегодня</button>');
        expect(topbar).not.toContain('Фильтры');
        expect(html).not.toContain('План и статистика по инженерам — месяц');
        expect(reserveStyles).toContain('.sched-page{padding-top:0}');
        expect(reserveStyles).toContain('margin-bottom:6px');
        expect(reserveStyles).toContain('.sched-topbar{display:flex;align-items:center;justify-content:space-between');
    });

    it('не дублирует название и пояснение под верхней панелью', () => {
        const panel = html.slice(html.indexOf('<div id="sched-completed-panel"'), html.indexOf('<!-- Модалка: расход подписки'));
        expect(panel).not.toContain('График производства работ');
        expect(panel).not.toContain('Контроль фактически выполненных проверок по инженерам');
        expect(reserveScript).not.toContain('<header class="rc-heading">');
        expect(reserveScript).not.toContain('<div class="rc-cards">');
        expect(reserveScript).not.toContain('<div class="rc-period">');
        expect(reserveScript).not.toContain('<p class="rc-help">');
        expect(reserveScript).not.toContain('<div class="rc-summary">');
        expect(reserveScript).not.toContain('<p class="rc-footnote">');
    });

    it('использует компактные KPI и отдельную обёртку для длинных счётчиков', () => {
        expect(styles).toContain('.sched-kpi { display: flex; align-items: center; gap: 9px; min-height: 46px;');
        expect(styles).toContain('.sched-kpi__body { display: flex; align-items: baseline; gap: 8px;');
        expect(styles).toContain('gap: 10px; margin: 0 0 6px;');
        expect(styles).toContain('.sched-stats-table th:nth-child(5) { width: 14%; }');
        expect(html.match(/class="sched-remarks-values"/g)).toHaveLength(2);
        expect(styles).toContain('font-variant-numeric: tabular-nums');
    });

    it('не показывает служебный остаток и дневную норму в строках инженеров', () => {
        expect(reserveScript).not.toContain('Остаток:');
        expect(reserveScript).not.toContain('/ раб. день');
    });

    it('рисует выбор объекта единым полем без вложенной рамки кнопки', () => {
        expect(reserveStyles).toContain('.rc-object-picker{position:relative;display:flex;height:34px;margin-top:6px;border:1px');
        expect(reserveStyles).toContain('.rc-object-picker__toggle{position:static;flex:0 0 38px;');
        expect(reserveStyles).toContain('border:0;border-left:1px solid #cbd5e1');
    });

    it('показывает проекты вертикальными короткими плашками и различает подготовку', () => {
        expect(html).toContain('schedCompactLabel(it.full || it.short)');
        expect(styles).toContain('.sched-daystack { display: flex; flex-direction: column;');
        expect(styles).toContain('.sched-plashka__txt { overflow: visible; text-overflow: clip;');
        expect(styles).toContain('width: 165px; min-width: 165px; max-width: 165px;');
        expect(styles).toContain('.sched-gridwrap--month .sched-day--weekend,.sched-gridwrap--month .sched-cell--weekend{width:20px;');
        expect(styles).toContain('.sched-gridwrap--month .sched-plashka { padding: 2px 2px; font-size: 11px;');
        expect(reserveStyles).toContain('.rc-dots{min-height:68px;padding:7px 2px 17px;display:flex;flex-direction:column;');
        expect(reserveStyles).toContain('button.rc-point.rc-unrecognized');
        expect(reserveStyles).toContain('button.rc-point.rc-manual');
        expect(reserveStyles).toContain('.rc-legend-chip.rc-unrecognized');
        expect(reserveStyles).toContain('.rc-legend-chip.rc-manual');
        expect(reserveScript).toContain('<i class="rc-legend-chip"></i>Готов к проверке');
        expect(reserveScript).not.toContain('>АБВГ..</i>');
        expect(reserveStyles).toContain('width:44px;min-height:18px;padding:2px 3px;background:var(--teal-light');
        expect(reserveScript).toContain("chars.slice(0, 4).join('') + '..'");
    });
});
