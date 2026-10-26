import {readFileSync} from 'node:fs';
import vm from 'node:vm';
import {describe, expect, it} from 'vitest';
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const sandbox = {};
vm.runInNewContext(readFileSync(new URL('../static/js/project-comparison-catalog.js', import.meta.url), 'utf8'), sandbox);
const catalog = sandbox.ProjectComparisonCatalog;

describe('experimental comparison method selection', () => {
    it.each(['standard', 'subjects'])('sends %s independently of model and depth', method => {
        const refs = {scComparisonMode: {value: method}, scProductionInputMode: {value: 'DOCUMENT'},
            scViewerEmpty: {}, scCurrentPage: {}, scProductionAiMode: {value: 'STANDARD'},
            scV3ModelOptions: {value: [{code: 'astra'}, {code: 'opus55'}]}, scV3Model: {value: 'astra'},
            scV3Parallelism: {value: 10}};
        const fn = app.slice(app.indexOf('function scProductionRunBody()'), app.indexOf('function scSelectV3Model('));
        const body = new Function(...Object.keys(refs), fn + ';return scProductionRunBody();')(...Object.values(refs));
        expect(body).toMatchObject({comparison_mode: method, model_profile: 'astra', ai_mode: 'STANDARD', parallelism: 10});
    });
    it('resume inherits the donor method instead of the current launch selection', () => {
        const start = app.indexOf('if (settings.resumeFromRunId) {');
        const code = app.slice(start, app.indexOf('const requestedAt =', start));
        const runBody = {comparison_mode: 'standard', model_profile: 'astra'};
        new Function('runBody', 'settings', code)(runBody, {resumeFromRunId: 'donor'});
        expect(runBody).toEqual({resume_from_run_id: 'donor'});
    });
    it('labels method, actual usage and incomplete accounting in saved results', () => {
        expect(catalog.model({engine: {comparison_mode_label: 'По предметам', model: 'M'}})).toContain('По предметам');
        const text = catalog.effort({comparison_metrics: {model_calls: 6, elapsed_seconds: 120, resumed: true,
            usage_total: {input_tokens: 200, output_tokens: 30, calls_with_usage: 5, calls_without_usage: 1}}});
        expect(text).toContain('200 входных');
        expect(text).toContain('30 выходных');
        expect(text).toContain('не для всех');
        expect(text).toContain('предыдущие запуски');
        expect(catalog.effort({})).toBe('—');
    });
});
