import {readFileSync} from 'node:fs';
import {describe, expect, it, vi} from 'vitest';

const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const source = app.slice(
    app.indexOf('async function scCancelProductionRun()'),
    app.indexOf('function scConfirmProductionFullRerun()'),
);

function deferred() {
    let resolve;
    const promise = new Promise(done => { resolve = done; });
    return {promise, resolve};
}

function cancellation(result = {cancelled: true}) {
    const context = {sessionId: 'session', pairId: 'pair', token: 1};
    let current = true;
    const refs = {
        scProductionCancelling: {value: false},
        scProductionError: {value: 'Идущего анализа этой пары не найдено.'},
        scActivePairRequestContext: vi.fn(() => context),
        scActivePairRequestContextCurrent: vi.fn(() => current),
        scProductionRequest: vi.fn(async () => result),
        scLoadProductionReview: vi.fn(async () => {}),
    };
    const cancel = new Function(
        ...Object.keys(refs), `${source}; return scCancelProductionRun;`,
    )(...Object.values(refs));
    return {...refs, cancel, context, navigate: () => { current = false; }};
}

describe('stopping a comparison run', () => {
    it.each(['FAILED', 'CANCELLED', 'REVIEW', 'COMPLETED', 'PARTIAL'])(
        'refreshes an already finished %s run without a false stop error', async status => {
            const r = cancellation({cancelled: false, reason_code: 'no_active_run', status});
            await r.cancel();
            expect(r.scProductionError.value).toBe('');
            expect(r.scLoadProductionReview).toHaveBeenCalledWith({silent: true});
            expect(r.scProductionCancelling.value).toBe(false);
        },
    );

    it('keeps duplicate stop clicks blocked through the state refresh', async () => {
        const r = cancellation();
        const refresh = deferred();
        const refreshing = deferred();
        r.scLoadProductionReview.mockImplementation(() => {
            refreshing.resolve();
            return refresh.promise;
        });
        const pending = r.cancel();
        await refreshing.promise;
        expect(r.scProductionCancelling.value).toBe(true);
        await r.cancel();
        expect(r.scProductionRequest).toHaveBeenCalledTimes(1);
        expect(r.scProductionRequest).toHaveBeenCalledWith('/cancel', {
            context: r.context, fetch: {method: 'POST'},
        });
        refresh.resolve();
        await pending;
        expect(r.scProductionCancelling.value).toBe(false);
        expect(r.scProductionError.value).toBe('');
    });

    it('explains when the live runner did not receive the stop request', async () => {
        const r = cancellation({cancelled: false, reason_code: 'run_not_owned_by_this_process', status: 'RUNNING'});
        await r.cancel();
        expect(r.scProductionError.value).toContain('Не удалось передать команду остановки');
        expect(r.scLoadProductionReview).toHaveBeenCalledOnce();
    });

    it('does not silently accept an unknown stop rejection', async () => {
        const r = cancellation({cancelled: false, reason_code: 'unknown'});
        await r.cancel();
        expect(r.scProductionError.value).toContain('Сервер не подтвердил остановку');
    });

    it('reports transport failures and still refreshes the state', async () => {
        const r = cancellation();
        r.scProductionRequest.mockRejectedValue(new Error('offline'));
        await r.cancel();
        expect(r.scProductionError.value).toBe('Не удалось остановить анализ.');
        expect(r.scLoadProductionReview).toHaveBeenCalledOnce();
        expect(r.scProductionCancelling.value).toBe(false);
    });

    it.each(['rejected', 'failed'])(
        'does not apply a %s stop response to another pair after navigation', async outcome => {
            const r = cancellation();
            const response = deferred();
            r.scProductionRequest.mockImplementation(async () => {
                await response.promise;
                if (outcome === 'failed') throw new Error('offline');
                return {cancelled: false, reason_code: 'run_not_owned_by_this_process'};
            });
            const pending = r.cancel();
            r.navigate();
            r.scProductionError.value = 'Ошибка другой пары';
            response.resolve();
            await pending;
            expect(r.scProductionError.value).toBe('Ошибка другой пары');
            expect(r.scLoadProductionReview).not.toHaveBeenCalled();
            expect(r.scProductionCancelling.value).toBe(false);
        },
    );

    it('requires an active document pair', async () => {
        const r = cancellation();
        r.scActivePairRequestContext.mockReturnValue(null);
        await r.cancel();
        expect(r.scProductionRequest).not.toHaveBeenCalled();
    });
});
