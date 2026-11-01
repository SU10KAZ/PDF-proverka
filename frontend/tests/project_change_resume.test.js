import {readFileSync} from 'node:fs';
import {describe, expect, it, vi} from 'vitest';

const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const functions = app.slice(app.indexOf('function scResumeProductionRun('), app.indexOf('function scProductionRunBody('));

function recovery(mode = 'before_mining') {
    const refs = {
        scActivePair: {value: {id: 'pair'}},
        scProductionState: {value: {run_id: 'failed', resume_available: true, resume_options: {
            model_profile: 'opus55', mode, can_switch_model: mode !== 'partial_mining',
            saved_regions: 0, planned_new_calls: 102, max_new_miner_calls: 64,
        }}},
        scResumeDialog: {value: null}, scProductionError: {value: ''}, scRunProductionComparison: vi.fn(),
        scLoadProductionAiModes: vi.fn(),
    };
    const api = new Function(...Object.keys(refs), `${functions}; return {open: scResumeProductionRun, submit: scConfirmResumeProductionRun};`)(...Object.values(refs));
    return {...refs, ...api};
}

describe('resume a saved comparison', () => {
    it('waits for model confirmation, then resumes the whole remaining plan', () => {
        const r = recovery(); r.open('failed');
        expect(r.scRunProductionComparison).not.toHaveBeenCalled();
        expect(r.scResumeDialog.value).toMatchObject({modelProfile: 'opus55'});
        r.scResumeDialog.value.modelProfile = 'astra';
        r.submit();
        expect(r.scRunProductionComparison).toHaveBeenCalledWith({resumeFromRunId: 'failed', resumeModelProfile: 'astra',
            resumeModelPolicy: 'remaining_stages'});
        expect(r.scResumeDialog.value).toBeNull();
    });
    it('keeps the source model by default and omits mining limits for a completed Miner', () => {
        const r = recovery('tail'); r.open('failed'); r.submit();
        expect(r.scRunProductionComparison).toHaveBeenCalledWith({resumeFromRunId: 'failed', resumeModelProfile: 'opus55'});
    });
    it('cannot switch the model of a partially mined comparison', () => {
        const r = recovery('partial_mining'); r.open('failed');
        r.scResumeDialog.value.modelProfile = 'astra'; r.submit();
        expect(r.scRunProductionComparison).not.toHaveBeenCalled();
    });
    it.each(['pair', 'run'])('does not resume another %s after navigation or polling', changed => {
        const r = recovery(); r.open('failed');
        if (changed === 'pair') r.scActivePair.value = {id: 'other'};
        else r.scProductionState.value.run_id = 'new';
        r.submit(); expect(r.scRunProductionComparison).not.toHaveBeenCalled();
    });
    it('serializes recovery settings without inheriting an unrelated launch model', () => {
        const block = app.slice(app.indexOf('if (settings.resumeFromRunId) {'), app.indexOf('const requestedAt =', app.indexOf('if (settings.resumeFromRunId) {')));
        const body = {model_profile: 'unrelated'};
        new Function('settings', 'runBody', block)({resumeFromRunId: 'failed', resumeModelProfile: 'astra',
            resumeModelPolicy: 'remaining_stages', maxNewMinerCalls: 128}, body);
        expect(body).toEqual({model_profile: 'astra', resume_from_run_id: 'failed',
            resume_model_policy: 'remaining_stages'});
    });
    it('offers model switching only when the server permits it and explains preserved work', () => {
        expect(html).toContain('!scResumeDialog.can_switch_model && option.code !== scResumeDialog.model_profile');
        expect(html).toContain('Сопоставление готово и будет сохранено.');
        expect(html).not.toContain('v-model.number="scResumeDialog.maxNewMinerCalls"');
        expect(html).toContain('Будут обработаны все оставшиеся участки.');
        expect(html).not.toContain('Mapper и Miner не повторяются: их сохранённые ответы');
    });
});
