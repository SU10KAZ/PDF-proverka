/* Real Vue/CSS, synthetic matrix and intercepted loopback requests only. */
const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {chromium} = require(process.env.PLAYWRIGHT_MODULE || 'playwright');
const root = path.resolve(__dirname, '..');

test('candidate clicks preserve the scrolled matrix and compare submits all choices once', async () => {
    const versions = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l'];
    const matched = page => ({state: 'matched', page});
    const absent = () => ({state: 'absent'});
    const rows = Array.from({length: 40}, (_, i) => ({row_id: 'row-' + i,
        anchor: [i < 2 ? 'b' : 'a', i + 3], display_title: 'Лист ' + (i + 1),
        cells: Object.fromEntries(versions.map(vid => [vid,
            i < 2 ? (vid === 'b' ? matched(i + 3) : absent()) : matched(i + 3)]))}));
    for (const i of [20, 21]) rows[i].cells.b = {state: 'unmatched', candidates: [{page: i - 17, score: 0.7}]};
    const matrix = {state: 'ready', versions: versions.map(version_id => ({version_id, sha256: version_id,
        user_label: 'Версия ' + version_id, page_count: 40})), rows, edits: []};
    const posts = [], errors = [];
    let submitted = false;
    const html = `<!doctype html><html><meta charset="utf-8"><link rel="stylesheet" href="/styles.css">
        <link rel="stylesheet" href="/vc.css"><div id="app"><version-comparison-page /></div>
        <script src="/vue.js"></script><script src="/vc.js"></script><script>
        localStorage.setItem('version-comparison:object','obj');
        const app=Vue.createApp({}); VersionComparison.register(app); app.mount('#app');</script></html>`;
    const browser = await chromium.launch({headless: true, executablePath: process.env.CHROME_BIN || '/opt/google/chrome/chrome', args: ['--no-sandbox']});
    try {
        const page = await browser.newPage({viewport: {width: 1660, height: 900}});
        page.on('pageerror', e => errors.push(e.message));
        await page.route('**/*', async route => {
            const req = route.request(), url = new URL(req.url());
            assert.equal(url.origin, 'http://127.0.0.1:33119');
            const files = {'/vue.js': 'static/js/vue.global.prod.js', '/vc.js': 'static/js/version-comparison.js',
                '/styles.css': 'static/css/styles.css', '/vc.css': 'static/css/version-comparison.css'};
            if (files[url.pathname]) return route.fulfill({contentType: url.pathname.endsWith('.css') ? 'text/css' : 'text/javascript',
                body: fs.readFileSync(path.join(root, files[url.pathname]), 'utf8')});
            if (url.pathname === '/') return route.fulfill({contentType: 'text/html', body: html});
            if (url.pathname.endsWith('/thumb')) return route.fulfill({contentType: 'image/svg+xml',
                body: '<svg xmlns="http://www.w3.org/2000/svg" width="120" height="160"><rect width="120" height="160" fill="#eee"/></svg>'});
            let data;
            if (req.method() === 'POST') {
                posts.push({url: url.pathname, body: req.postDataJSON()});
                if (url.pathname.endsWith('/links/batch')) {submitted = true; data = {state: 'building'};}
                else if (url.pathname.endsWith('/series')) data = {series_id: 's', state: 'queued', pairs: [], progress: {total: 11, done: 0}};
                else throw new Error('Unexpected mutation: ' + url.pathname);
            } else if (url.pathname.endsWith('/objects')) data = {items: [{id: 'obj', name: 'Тест'}]};
            else if (url.pathname.endsWith('/catalog')) data = {projects: [{project_id: 'p', title: '133/23-ГК-АР3', versions}]};
            else if (url.pathname.endsWith('/matrix')) data = matrix;
            else if (url.pathname.endsWith('/runs')) data = {runs: []};
            else if (url.pathname.endsWith('/series')) data = {series: []};
            else throw new Error('Unexpected request: ' + url.pathname);
            return route.fulfill({contentType: 'application/json', body: JSON.stringify(data)});
        });
        await page.goto('http://127.0.0.1:33119');
        const wrap = page.locator('.vc-matrix-wrap');
        await page.locator('[data-row-id="row-20"] .vc-cands button').waitFor();
        await page.locator('[data-row-id="row-20"]').evaluate(el => el.scrollIntoView({block: 'center'}));
        await wrap.evaluate(el => {el.scrollLeft = 120; el.dataset.preserved = 'yes';});
        const before = await wrap.evaluate(el => ({top: el.scrollTop, left: el.scrollLeft}));
        await page.locator('[data-row-id="row-20"] .vc-cands button').click();
        await page.locator('[data-row-id="row-21"] .vc-cands button').click();
        const after = await wrap.evaluate(el => ({top: el.scrollTop, left: el.scrollLeft, preserved: el.dataset.preserved}));
        assert.equal(after.preserved, 'yes', 'matrix DOM was recreated');
        assert.equal(after.left, before.left);
        assert(Math.abs(after.top - before.top) < 2, `scroll jumped: ${before.top} → ${after.top}`);
        assert.equal(posts.length, 0, 'a choice started rebuilding');
        await page.getByRole('button', {name: 'Применить сопоставления (2)', exact: true}).waitFor();
        await page.getByRole('button', {name: 'Сравнить все соседние', exact: true}).click();
        await page.waitForFunction(() => document.querySelector('.vc-series'));
        assert(submitted);
        assert.equal(posts.length, 2);
        assert(posts[0].url.endsWith('/links/batch'));
        assert.equal(posts[0].body.edits.length, 2);
        assert(posts[1].url.endsWith('/series'));
        assert.deepEqual(errors, []);
    } finally {await browser.close();}
});
