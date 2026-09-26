(function (root, factory) {
    const api = factory();
    if (typeof module === 'object' && module.exports) module.exports = api;
    root.PrelinkDraftsClient = api;
}(typeof globalThis !== 'undefined' ? globalThis : this, function () {
    'use strict';
    // The only client of the pre-analysis prelink drafts API (human-prelink-drafts/1) and of the run's
    // prelink reconciliation (read-only). Kept apart from stage-block-mapping.js, whose wiring test pins
    // that the workspace itself writes only the two Human Mapping POSTs. Nothing here is ever sent to a model.
    const enc = encodeURIComponent;
    const draftsBase = (sid, pid) => `/api/stage-comparison/sessions/${enc(sid)}/pairs/${enc(pid)}/prelinks`;
    const reconciliationUrl = (sid, pid, run) =>
        `/api/stage-comparison/sessions/${enc(sid)}/pairs/${enc(pid)}/block-mapping/runs/${enc(run)}/prelink-reconciliation`;

    function errorCode(body) {
        if (!body || typeof body !== 'object') return '';
        const detail = body.detail;
        if (detail && typeof detail === 'object' && detail.error) return String(detail.error);
        return typeof detail === 'string' ? detail : '';
    }

    function create(fetchImpl) {
        async function send(url, init) {
            let response;
            try {
                response = await fetchImpl(url, init);
            } catch (_) {
                return {ok: false, network: true};
            }
            let body = null;
            try { body = await response.json(); } catch (_) { body = null; }
            if (response.ok) return {ok: true, status: response.status, body};
            return {ok: false, status: response.status, code: errorCode(body),
                detail: body && typeof body.detail === 'object' ? body.detail : null};
        }
        const json = (method, payload) => ({method, headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)});
        return {
            view: (sid, pid) => send(draftsBase(sid, pid)),
            create: (sid, pid, {expectedRevision, oldIds, newIds, note = ''}) => send(draftsBase(sid, pid),
                json('POST', {expected_revision: expectedRevision, old_block_ids: oldIds, new_block_ids: newIds, note})),
            replace: (sid, pid, prelinkId, {expectedRevision, oldIds, newIds, note = ''}) =>
                send(`${draftsBase(sid, pid)}/${enc(prelinkId)}`,
                    json('PUT', {expected_revision: expectedRevision, old_block_ids: oldIds, new_block_ids: newIds, note})),
            remove: (sid, pid, prelinkId, expectedRevision) =>
                send(`${draftsBase(sid, pid)}/${enc(prelinkId)}?expected_revision=${Number(expectedRevision)}`, {method: 'DELETE'}),
            reconciliation: (sid, pid, run) => send(reconciliationUrl(sid, pid, run)),
        };
    }

    return Object.freeze({create, draftsBase, reconciliationUrl, errorCode});
}));
