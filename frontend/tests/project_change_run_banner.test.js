import {describe, expect, it} from 'vitest';
import {createRequire} from 'node:module';
import {readFileSync} from 'node:fs';
const require = createRequire(import.meta.url);
const V = require('../static/js/project-change-view.js');

// Live acceptance 2026-09-20: a V3 run publishes ProjectChanges only at the end, the legacy run panel is
// hidden in the ProjectChange shell — a running or FAILED V3 run looked exactly like "no changes found".
const T0 = Date.parse('2026-09-20T20:46:45Z');
const state = extra => ({engine: 'projectchange_v3', status: 'RUNNING', run_id: 'r1', started_at: '2026-09-20T20:46:45Z',
    message: 'V3: поиск изменений, регион 3 из 12 (Miner)', model_calls: 3, orphaned_run: false, ...extra});

describe('ProjectChange run banner (V3 run state above the change list)', () => {
    it('shows a running V3 analysis with stage, elapsed time, model calls and a stop control', () => {
        const b = V.runBanner(state(), T0 + 17 * 60000 + 5000);
        expect(b).toMatchObject({tone: 'running', cancellable: true, title: 'Идёт анализ изменений'});
        expect(b.text).toBe('V3: поиск изменений, регион 3 из 12 (Miner) · идёт 17 мин · обращений к модели: 3');
    });
    it('never invents elapsed time or calls it does not know', () => {
        expect(V.runBanner(state({started_at: null, model_calls: 0, message: ''}), T0).text).toBe('Анализ выполняется');
        expect(V.runBanner(state(), T0 - 5000).text).not.toContain('мин');
    });
    it('makes a failed run visible with its reason instead of an empty list', () => {
        const b = V.runBanner(state({status: 'FAILED', reason_code: 'source_preparation_failed',
            message: 'V3: подготовка источников не удалась: OLD markdown missing'}), T0);
        expect(b).toMatchObject({tone: 'failed', cancellable: false, title: 'Анализ не выполнен'});
        expect(b.text).toBe('V3: подготовка источников не удалась: OLD markdown missing (source_preparation_failed)');
        expect(V.runBanner(state({status: 'FAILED', message: '', reason_code: ''}), T0).text).toBe('Причина не указана.');
    });
    it('reports a run whose process is gone as interrupted, not as running', () => {
        const b = V.runBanner(state({orphaned_run: true}), T0);
        expect(b).toMatchObject({tone: 'failed', cancellable: false, title: 'Анализ прерван'});
    });
    it('warns when ProjectChanges are published but Human Mapping is not', () => {
        const b = V.runBanner(state({status: 'REVIEW', human_mapping_published: false,
            human_mapping_error: 'ScopeUnresolved: no owner'}), T0);
        expect(b).toMatchObject({tone: 'warning', cancellable: false, text: 'ScopeUnresolved: no owner'});
    });
    it('stays silent for a published run, for legacy runs and without a run', () => {
        expect(V.runBanner(state({status: 'COMPLETED', human_mapping_published: true}), T0)).toBeNull();
        expect(V.runBanner(state({status: 'REVIEW', human_mapping_published: true}), T0)).toBeNull();
        expect(V.runBanner({status: 'RUNNING'}, T0)).toBeNull();          // legacy engine: its own panel
        expect(V.runBanner({engine: 'legacy', status: 'FAILED'}, T0)).toBeNull();
        expect(V.runBanner(null, T0)).toBeNull();
    });
    it('is wired into the ProjectChange tab with the stop control', () => {
        const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
        const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
        const banner = html.slice(html.lastIndexOf('<div', html.indexOf('id="pc-run-banner"')), html.indexOf('<project-change-list'));
        expect(banner).toContain("scTab === 'diffs' && pcRunBanner");
        expect(banner).toContain('@click="scCancelProductionRun()"');
        expect(html.indexOf('id="pc-run-banner"')).toBeLessThan(html.indexOf('<project-change-list'));
        expect(app).toContain('PC.runBanner(scProductionState.value, scProductionClock.value)');
        expect(app).toMatch(/pcOpenEvidence, pcRunBanner,/);
    });
});
