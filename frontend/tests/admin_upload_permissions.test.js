import {describe, expect, it} from 'vitest';
import {readFileSync} from 'node:fs';
import vm from 'node:vm';

const source = readFileSync(new URL('../static/js/admin-users.js', import.meta.url), 'utf8');
function fn(name) {
    const start = source.indexOf('    function ' + name + '(');
    const end = source.indexOf('\n    function ', start + 1);
    return source.slice(start, end);
}

class Node {
    constructor(tag) { this.tag = tag; this.children = []; this.attrs = {}; this.dataset = {}; }
    appendChild(node) { this.children.push(node); return node; }
    setAttribute(name, value) { this.attrs[name] = value; }
    querySelectorAll() {
        return this.children.flatMap(c => [c, ...c.querySelectorAll()])
            .filter(c => c.tag === 'input' && c.attrs.type === 'checkbox');
    }
}

function setup() {
    const requests = [];
    const document = {createElement: tag => new Node(tag), createTextNode: text => Object.assign(new Node('#text'), {textContent: text})};
    const meta = {sections: [
        {code: 'audit_results', title: 'Результаты аудита', downloads: true, uploads: true},
        {code: 'version_comparison', title: 'Сравнение версий', downloads: true, uploads: true},
        {code: 'stage_comparison', title: 'Сравнение стадий', downloads: true, uploads: true},
        {code: 'schedule', title: 'График работ', downloads: false, uploads: false},
    ]};
    const account = {id: 7, login: 'kamaev.l.a', role: 'viewer', status: 'active', uploads: {}, sections: {
        audit_results: {view: false}, version_comparison: {view: true}, stage_comparison: {view: true}, schedule: {view: false},
    }};
    const ctx = vm.createContext({document, meta,
        act: (method, path, body) => requests.push({method, path, body}),
        api: async (method, path, body) => {requests.push({method, path, body}); return {};},
        reload: async () => {}, banner: () => {},
    });
    vm.runInContext(fn('el') + fn('sectionsMatrix') + fn('uploadPermission'), ctx);
    return {ctx, account, requests};
}

describe('загрузка проектов в настройках пользователей', () => {
    it('общая галочка активна при доступе только к сравнениям', async () => {
        const {ctx, account, requests} = setup();
        const cb = ctx.uploadPermission(account).children[0];
        expect(cb.disabled).toBe(false);
        cb.checked = true;
        cb.onchange();
        await Promise.resolve();
        expect(requests[0]).toEqual({method: 'PUT', path: '/accounts/7/upload-projects', body: {upload_projects: true}});
    });

    it('сохраняет отдельные галочки загрузки вместе с просмотром', () => {
        const {ctx, account, requests} = setup();
        const matrix = ctx.sectionsMatrix(account);
        const inputs = matrix.querySelectorAll();
        const upload = inputs.find(c => c.attrs['aria-label'] === 'Сравнение версий: загрузка проектов');
        upload.checked = true;
        upload.onchange();
        const view = inputs.find(c => c.attrs['aria-label'] === 'Сравнение версий: просмотр');
        expect(view.checked).toBe(true);
        matrix.children[1].onclick();
        expect(requests[0].body.sections.version_comparison).toEqual({view: true, download: false, upload: true});
        expect(requests[0].body.sections.stage_comparison.upload).toBe(false);
        expect(inputs.some(c => c.attrs['aria-label'] === 'График работ: загрузка проектов')).toBe(false);
        view.checked = false;
        view.onchange();
        expect(upload.checked).toBe(false);
    });

    it('показывает частичное разрешение и блокирует загрузку для неактивного аккаунта', () => {
        const {ctx, account} = setup();
        account.uploads.version_comparison = true;
        const partial = ctx.uploadPermission(account).children[0];
        expect(partial.indeterminate).toBe(true);
        expect(partial.disabled).toBe(false);
        account.status = 'blocked';
        expect(ctx.uploadPermission(account).children[0].disabled).toBe(true);
        expect(ctx.sectionsMatrix(account).querySelectorAll().filter(c => c.dataset.kind === 'upload').every(c => c.disabled)).toBe(true);
    });
});
