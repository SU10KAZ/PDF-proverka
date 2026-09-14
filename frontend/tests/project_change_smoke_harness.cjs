/* Browser-only smoke infrastructure. Never imported by the application. */
const MUTATIONS = ['POST', 'PUT', 'PATCH', 'DELETE'];
function previewActive(state, origin) {
    const url = new URL(state.url);
    return url.origin === origin && state.objectId === '4f3e5916';
}
function newAudit() {
    return {requests: [], writes: [], legacyRequests: [], legacyWrites: [],
        errors: [], consoleErrors: [], harnessErrors: [], failed: []};
}
async function createContext(browser, {base, objectId, cookies = [], audit}) {
    const origin = new URL(base).origin;
    const context = await browser.newContext({viewport: {width: 1600, height: 1100}, serviceWorkers: 'block'});
    context.setDefaultTimeout(20000);
    // Observe every page before creating it, including initialization failures.
    context.on('page', page => {
        page.on('pageerror', error => audit.errors.push({url: page.url(), message: error.message, stack: error.stack}));
        page.on('console', message => {
            if (message.type()==='error' && !message.text().startsWith('Failed to load resource:'))
                audit.consoleErrors.push({url:page.url(),message:message.text()});
        });
    });
    if (cookies.length) await context.addCookies(cookies);
    await context.addInitScript(({origin, objectId}) => {
        // about:blank and foreign/opaque frames have no usable same-origin storage.
        // Storage failures on the intended origin still surface as pageerror.
        if (location.origin !== origin || !/^https?:$/.test(location.protocol)) return;
        sessionStorage.setItem('currentObjectId', objectId);
    }, {origin, objectId});
    const entries = new WeakMap();
    context.on('response', response => {
        const entry = entries.get(response.request());
        if (!entry) return;
        entry.status = response.status();
        if (entry.preview && new URL(entry.url).pathname.startsWith('/api/project-change-preview/')
                && response.status() >= 400) audit.failed.push({...entry});
    });
    await context.route('**/*', async route => {
        const request = route.request(), url = new URL(request.url()), method = request.method();
        if (url.origin !== origin) return route.abort();
        // All API requests and all potentially mutating requests are classified
        // by the initiating page's ACTUAL URL and current object, never a test flag.
        if (url.pathname.startsWith('/api/') || MUTATIONS.includes(method)) {
            let state;
            try {
                state = await request.frame().evaluate(() => ({
                    url: location.href, objectId: sessionStorage.getItem('currentObjectId'),
                }));
            } catch (error) {
                audit.harnessErrors.push({url: request.url(), message: String(error)});
                return route.abort(); // An unclassified request cannot pass silently.
            }
            const entry = {method, url: request.url(), page_url: state.url,
                object_id: state.objectId, preview: previewActive(state, origin)};
            entries.set(request, entry);
            (entry.preview ? audit.requests : audit.legacyRequests).push(entry);
            if (MUTATIONS.includes(method)) {
                (entry.preview ? audit.writes : audit.legacyWrites).push(entry);
                if (url.pathname.includes('/api/project-change-preview/') && url.pathname.endsWith('/decisions')) return route.abort();
            }
        }
        // Document mutations are allowed for every object; only snapshot decision writes are blocked.
        return route.continue();
    });
    return context;
}
module.exports = {MUTATIONS, previewActive, newAudit, createContext};
