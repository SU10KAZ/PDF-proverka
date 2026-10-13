import {describe, expect, it} from 'vitest';
import {createRequire} from 'node:module';
const require = createRequire(import.meta.url);
const {comparisonPipeline} = require('../static/js/project-change-view.js');

const running = extra => ({engine: 'projectchange_v3', run_id: 'run-1', status: 'RUNNING',
    current_stage: 'MINING', pipeline_current: 'MINING', pipeline_stages: {
        SOURCE_PREPARATION: {status: 'COMPLETED'}, MAPPING: {status: 'COMPLETED'},
        MINING: {status: 'RUNNING', processed: 2, total: 12, unit: 'region'},
    }, ...extra});
const stage = (state, id) => comparisonPipeline(state).stages.find(s => s.id === id);

describe('shared seven-stage comparison pipeline', () => {
    it('appears before the server accepts a launch and excludes legacy/empty state', () => {
        expect(comparisonPipeline(null)).toBeNull();
        expect(comparisonPipeline({engine: 'legacy', status: 'RUNNING'})).toBeNull();
        const pending = comparisonPipeline({pipeline_pending: true, status: 'RUNNING'});
        expect(pending.stages).toHaveLength(7);
        expect(pending.stages.map(s => s.status)).toEqual(['RUNNING', ...Array(6).fill('WAITING')]);
    });
    it('restores progress from a serialized server snapshot without inventing percentages', () => {
        const state = JSON.parse(JSON.stringify(running()));
        const panel = comparisonPipeline(state);
        expect(panel.stages[2]).toMatchObject({status: 'RUNNING', counter: '2 из 12 областей'});
        expect(panel.stages.slice(3).every(s => s.status === 'WAITING')).toBe(true);
        expect(panel).toEqual(comparisonPipeline(JSON.parse(JSON.stringify(state))));
        expect(stage(running({pipeline_stages: {MINING: {status: 'RUNNING'}}}), 'MINING').counter).toBe('');
    });
    it.each([
        [{status: 'FAILED', reason_code: 'miner_failed'}, 'FAILED'],
        [{status: 'FAILED', reason_code: 'v3_cancelled'}, 'CANCELLED'],
        [{orphaned_run: true}, 'INTERRUPTED'],
    ])('keeps finished work and marks only the interrupted stage: %j', (extra, expected) => {
        const result = comparisonPipeline(running(extra));
        expect(result.stages.map(s => s.status)).toEqual(['COMPLETED', 'COMPLETED', expected,
            'WAITING', 'WAITING', 'WAITING', 'WAITING']);
    });
    it('distinguishes skipped work, partial verification and reused answers', () => {
        const result = comparisonPipeline(running({status: 'REVIEW', pipeline_stages: {
            MAPPING: {status: 'REUSED'}, MINING: {status: 'REUSED'},
            UNMATCHED_REVIEW: {status: 'SKIPPED'}, SOURCE_VERIFICATION: {status: 'PARTIAL'},
            RESULT: {status: 'COMPLETED'},
        }}));
        expect(result.stages[1].label).toBe('Из сохранённого');
        expect(result.stages[3].label).toBe('Пропущено');
        expect(result.stages[5].label).toBe('Требует внимания');
        expect(result.title).toContain('требуется проверка');
    });
    it('separates unfinished verification from parameters needing engineering review', () => {
        const result = stage(running({status: 'REVIEW', pipeline_stages: {SOURCE_VERIFICATION: {
            status: 'PARTIAL', processed: 32, total: 69, unit: 'parameter',
            verification: {completed: 32, pending: 37, confirmed: 26, corrected: 1,
                conflicts: 0, unreadable: 5, failed: 0},
        }}}), 'SOURCE_VERIFICATION');
        expect(result).toMatchObject({label: 'Не проверено: 37', counter: '32 из 69 параметров',
            attention: 'Требуют разбора: 6', css: 'step-partial'});
        expect(result.detail).toContain('Подтверждено по источникам: 26.');
        expect(result.detail).toContain('Предложены поправки: 1.');
        expect(result.detail).toContain('Не удалось прочитать источник: 5.');
    });
    it('does not label finished checks with proposed corrections as unfinished or successful', () => {
        const result = stage(running({pipeline_stages: {SOURCE_VERIFICATION: {
            status: 'PARTIAL', verification: {pending: 0, confirmed: 7, corrected: 1, conflicts: 1, unreadable: 1},
        }}}), 'SOURCE_VERIFICATION');
        expect(result).toMatchObject({label: 'Проверено всё · требуют разбора: 3', attention: '', css: 'step-partial'});
        expect(result.detail).toContain('Противоречия с источником: 1.');
    });
    it('counts failed checks within the pending total, not as engineering disputes', () => {
        const result = stage(running({pipeline_stages: {SOURCE_VERIFICATION: {
            status: 'PARTIAL', verification: {pending: 8, failed: 8, corrected: 0, conflicts: 0, unreadable: 0},
        }}}), 'SOURCE_VERIFICATION');
        expect(result).toMatchObject({label: 'Не проверено: 8', attention: ''});
        expect(result.detail).toContain('из них с ошибкой проверки: 8');
    });
    it('uses old parameter counters without inventing review or confirmed totals', () => {
        const result = stage(running({pipeline_stages: {SOURCE_VERIFICATION: {
            status: 'PARTIAL', processed: 32, total: 69, unit: 'parameter',
        }}}), 'SOURCE_VERIFICATION');
        expect(result).toMatchObject({label: 'Не проверено: 37', attention: ''});
        expect(result.detail).not.toContain('Подтверждено');
        expect(result.detail).not.toContain('проверки инженером');
    });
    it.each(['RUNNING', 'FAILED', 'CANCELLED', 'INTERRUPTED', 'SKIPPED'])
    ('preserves the operational status %s instead of reporting completion', status => {
        const result = stage(running({pipeline_stages: {SOURCE_VERIFICATION: {
            status, verification: {pending: 0, corrected: 1, conflicts: 0, unreadable: 0},
        }}}), 'SOURCE_VERIFICATION');
        expect(result.status).toBe(status);
        expect(result.label).not.toContain('Проверено всё');
        expect(result.detail).toBe('');
    });
    it('does not mark the result ready in the gap before consolidation starts', () => {
        const state = running({status: 'COMPLETED', consolidation_expected: true});
        expect(stage(state, 'RESULT').status).toBe('WAITING');
        expect(comparisonPipeline(state).title).toBe('Ожидает итогового сведения');
    });
    it.each(['RUNNING', 'FAILED', 'CANCELLED', 'INTERRUPTED', 'COMPLETED', 'SKIPPED'])
    ('includes the final consolidation status %s', status => {
        const state = running({status: 'REVIEW', consolidation_expected: true,
            consolidation: {status, live: status === 'RUNNING', calls_done: 3, calls_total: 9}});
        expect(stage(state, 'RESULT')).toMatchObject({status: status === 'SKIPPED' ? 'PARTIAL' : status,
            counter: '3 из 9 обращений'});
    });
    it('keeps publication failure visible and never fabricates history for an old result', () => {
        const historical = comparisonPipeline({engine: 'projectchange_v3', status: 'COMPLETED'});
        expect(historical.historical).toBe(true);
        expect(historical.stages.every(s => s.status === 'UNKNOWN')).toBe(true);
        expect(stage(running({status: 'REVIEW', human_mapping_published: false}), 'RESULT').status).toBe('PARTIAL');
    });
    it('shows reconstructed completion, preserves partial work and leaves missing evidence unknown', () => {
        const state = {engine: 'projectchange_v3', status: 'REVIEW', pipeline_history_source: 'saved_result',
            pipeline_stages: {MAPPING: {status: 'REUSED'}, MINING: {status: 'COMPLETED'},
                SOURCE_VERIFICATION: {status: 'PARTIAL', message: 'Осталось: 100.'}, RESULT: {status: 'COMPLETED'}}};
        const panel = comparisonPipeline(state);
        expect(panel.restored).toBe(true);
        expect(panel.historical).toBe(false);
        expect(panel.stages[0].status).toBe('UNKNOWN');
        expect(panel.stages[1].css).toBe('step-done');
        expect(panel.stages[5]).toMatchObject({status: 'PARTIAL', message: 'Осталось: 100.', duration: ''});
        expect(panel.stages[6].status).toBe('COMPLETED');
        expect(stage({...state, consolidation: {status: 'FAILED'}}, 'RESULT').status).toBe('FAILED');
    });
});
