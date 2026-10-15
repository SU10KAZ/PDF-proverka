import {describe, it, expect} from 'vitest';
import fs from 'fs';
import path from 'path';
import vm from 'vm';

const SRC = fs.readFileSync(path.resolve(__dirname, '../static/js/version-comparison-ai.js'), 'utf-8');

function load() {
    const components = {};
    const ctx = {window: {}, console};
    ctx.globalThis = ctx.window;
    vm.runInNewContext(SRC, ctx);
    ctx.window.VersionComparisonAI.register({component: (name, def) => { components[name] = def; }});
    return components;
}

describe('ИИ-описание изменений — панель', () => {
    it('регистрирует компонент с раздельными счётчиками и статусами', () => {
        const c = load()['vc-ai-report'];
        expect(c).toBeTruthy();
        expect(c.emits).toContain('open-evidence');
        const t = c.template;
        expect(t).toContain('coverage_labels');
        expect(t).toContain('Движок:');
        expect(t).toContain('ИИ:');
        expect(t).toContain('Не просмотрено');
        expect(t).toContain('Классифицировано движком');
        expect(t).toContain('Выгрузить MD');
    });
    it('сводка сверху, исходные пункты по раскрытию; пункт — с проверкой и переходами к СО', () => {
        const all = load();
        const t = all['vc-ai-report'].template, item = all['vc-ai-item'].template;
        expect(t).toContain('Кратко');
        expect(t).toContain('Исходные пункты');
        expect(t).toContain('Подробно: все пункты');
        expect(t).toContain('layer_errors');
        expect(item).toContain('Связанное оформление');
        expect(item).toContain('Проверка предупреждения');
        expect(item).toContain('добавлено при проверке, модели не отправлялось');
        expect(item).toContain('к строке СО');
        expect(item).toContain('к схеме');
        expect(all['vc-ai-item'].emits).toEqual(['goto', 'goto-ref']);
        expect(SRC).toContain('/ref-focus?ref=');
    });
    it('сохранённое описание открывается только чтением, запуск — лишь с подтверждением', () => {
        expect(SRC).toContain('/latest');
        expect(SRC).toMatch(/consent: consent\.value/);
        expect(SRC).toMatch(/:disabled="!consent \|\| starting \|\| !picked.length"/);
        expect(SRC).not.toMatch(/openrouter|anthropic|\/api\/stage-comparison/i);
    });
    it('четыре корзины учёта в фиксированном порядке', () => {
        const ctx = {window: {}, console};
        vm.runInNewContext(SRC, ctx);
        expect(ctx.window.VersionComparisonAI.BUCKETS).toEqual(['ai_reviewed', 'engine_classified', 'linked_unverified', 'not_reviewed']);
    });
});

describe('ИИ-описание — переход в просмотрщике', () => {
    const V = fs.readFileSync(path.resolve(__dirname, '../static/js/version-comparison-viewer.js'), 'utf-8');
    it('по умолчанию один фрагмент крупно, пред./след., обзор всех — отдельной командой', () => {
        expect(V).toContain('‹ пред.');
        expect(V).toContain('след. ›');
        expect(V).toContain('Обзор всех');
        expect(V).toMatch(/focusAll\.value\s*=\s*!!props\.focus\.all/);
        expect(V).toMatch(/pad = 2\.5; floor = 0\.35/);
    });
    it('баннер называет версию, лист и страницу обеих сторон', () => {
        expect(V).toContain('лист ${x.sheet_no');
        expect(V).toContain('стр. PDF ${x.page');
    });
});
