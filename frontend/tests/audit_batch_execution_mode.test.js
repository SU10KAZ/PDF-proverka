import {describe, it, expect} from 'vitest';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const body = source.slice(source.indexOf('        async function confirmBatchAction()'),
    source.indexOf('        function batchActionLabel('));

describe('audit queue execution mode', () => {
    it.each(['sequential', 'parallel'])('sends %s after model selection without losing scope or projects', async mode => {
        let submit, sent;
        const c = {
            showBatchModal: {value: true}, batchScope: {value: 'both'},
            batchExecutionMode: {value: mode}, batchAllMode: {value: false},
            selectedProjects: {value: new Set(['A', 'B', 'C', 'D'])},
            batchRunning: {value: false}, batchQueue: {value: null}, selectAllChecked: {value: true},
            openModelConfig: (a, b, callback) => {submit = callback;},
            _isSubmitLocked: () => false, _withSubmitLock: (key, callback) => callback(),
            fetch: async (url, options) => {
                sent = {url, ...JSON.parse(options.body)};
                return {ok: true, json: async () => ({queue: {execution_mode: mode}})};
            }, alert: message => {throw new Error(message);},
        };
        vm.runInNewContext(body, c);
        await c.confirmBatchAction();
        c.batchExecutionMode.value = 'changed';
        await submit();
        expect(sent).toEqual({url: '/api/audit/batch', project_ids: ['A', 'B', 'C', 'D'],
            action: 'audit+optimization', execution_mode: mode});
        expect(c.batchQueue.value.execution_mode).toBe(mode);
    });
});
