// Версии сравнения стадий (18.11.2026): чистые функции модуля и проводка в портале.
import {describe, expect, it} from 'vitest';
import {readFileSync} from 'node:fs';
import V from '../static/js/stage-comparison-versions.js';

const read = path => readFileSync(new URL(path, import.meta.url), 'utf8');

const version = (id, label, left, right, extra = {}) => ({
    comparison_version: id, label, created_at: '2026-11-18T09:00:00+00:00',
    stages: {
        stage_1: {documents: left[0], set_key: left[1], reusable: left[0] > 0},
        stage_2: {documents: right[0], set_key: right[1], reusable: right[0] > 0},
    },
    ...extra,
});
const V1 = version('cv001', 'Версия 1', [67, 'P1'], [54, 'RD1']);
const V2 = version('cv002', 'Версия 2', [67, 'P1'], [50, 'RD2']);
const V3 = version('cv003', 'Пустая', [0, null], [0, null]);

describe('модуль версий сравнения', () => {
    it('добавляет версию к адресу объектного маршрута, а без версии оставляет его прежним', () => {
        expect(V.withVersion('/api/x/document-groups', 'cv002')).toBe('/api/x/document-groups?comparison_version=cv002');
        expect(V.withVersion('/api/x/members/A?side=left', 'cv002')).toBe('/api/x/members/A?side=left&comparison_version=cv002');
        expect(V.withVersion('/api/x', '')).toBe('/api/x');
        expect(V.withVersion('/api/x', 'v002')).toBe('/api/x');
    });

    it('склоняет «документ» по-русски', () => {
        expect([1, 2, 5, 11, 21, 22, 67, 104].map(V.documentsText)).toEqual([
            '1 документ', '2 документа', '5 документов', '11 документов',
            '21 документ', '22 документа', '67 документов', '104 документа',
        ]);
    });

    it('склеивает одинаковые комплекты разных версий и пропускает пустые стороны', () => {
        const options = V.setOptions([V1, V2, V3]);
        expect(options.map(o => o.value)).toEqual(['cv001:stage_1', 'cv001:stage_2', 'cv002:stage_2']);
        expect(options[0].label).toBe('Версия 1 · слева — 67 документов (также в: Версия 2)');
        expect(options[2].label).toBe('Версия 2 · справа — 50 документов');
        expect(V.parseOption('cv002:stage_2')).toEqual({comparison_version: 'cv002', stage: 'stage_2'});
        expect(V.parseOption('cv002:stage_3')).toBeNull();
    });

    it('выбирает запомненную версию, иначе самую новую, и предлагает следующее название', () => {
        expect(V.pickVersion([V1, V2], 'cv001')).toBe('cv001');
        expect(V.pickVersion([V1, V2], 'cv009')).toBe('cv002');
        expect(V.pickVersion([], 'cv001')).toBe('');
        expect(V.nextLabel([V1, V2])).toBe('Версия 3');
        expect(V.optionLabel(V2)).toBe('Версия 2 · 18.11.2026 · слева 67, справа 50');
    });

    it('по умолчанию берёт левый комплект выбранной версии и новую загрузку справа', () => {
        expect(V.initialForm([V1, V2], 'cv002')).toEqual({
            label: '', left: {mode: 'existing', set: 'cv001:stage_1'}, right: {mode: 'new', set: ''},
        });
        expect(V.initialForm([], '')).toEqual({label: '', left: {mode: 'new', set: ''}, right: {mode: 'new', set: ''}});
    });

    it('собирает тело создания и отказывает в одном комплекте на обеих сторонах', () => {
        const options = V.setOptions([V1, V2]);
        const ok = V.createPayload({label: ' Выпуск РД 2 ', left: {mode: 'existing', set: 'cv001:stage_1'},
            right: {mode: 'new', set: ''}}, options);
        expect(ok.errors).toEqual([]);
        expect(ok.body).toEqual({label: 'Выпуск РД 2',
            stage_1: {source: 'existing', comparison_version: 'cv001', stage: 'stage_1'}, stage_2: {source: 'new'}});
        expect(V.newSides(ok.body)).toEqual(['stage_2']);
        const same = V.createPayload({left: {mode: 'existing', set: 'cv001:stage_1'},
            right: {mode: 'existing', set: 'cv001:stage_1'}}, options);
        expect(same.errors).toContain('Слева и справа выбран один и тот же комплект');
        const missing = V.createPayload({left: {mode: 'existing', set: ''}, right: {mode: 'new'}}, options);
        expect(missing.errors).toEqual(['Выберите комплект слева']);
        expect(V.createPayload({label: 'x'.repeat(101), left: {mode: 'new'}, right: {mode: 'new'}}, options).errors)
            .toEqual(['Название версии длиннее 100 символов']);
    });
});

describe('проводка версий в портале', () => {
    const html = read('../index.html');
    const app = read('../static/js/app.js');
    const catalog = read('../static/js/project-comparison-catalog.js');
    const main = read('../../backend/app/main.py');

    it('подключает модуль до app.js и учитывает его в версии статики', () => {
        const module = html.indexOf('<script src="/static/js/stage-comparison-versions.js?v={{js_version}}"></script>');
        const appJs = html.indexOf('<script src="/static/js/app.js?v={{js_version}}"></script>');
        expect(module).toBeGreaterThan(0);
        expect(module).toBeLessThan(appJs);
        const tuple = main.slice(main.indexOf('pc_js = ['), main.indexOf('js_mtimes = ['));
        expect(tuple).toContain("'stage-comparison-versions.js'");
    });

    it('передаёт выбранную версию во все объектные запросы раздела', () => {
        expect(app).toContain("const SC_VERSIONS = window.StageComparisonVersions;");
        expect(app).toContain("return scVersionUrl(`/api/stage-comparison/objects/${encodeURIComponent(currentObjectId.value)}/document-groups${suffix}`);");
        expect(app).toContain("return scVersionUrl(`/api/stage-comparison/objects/${encodeURIComponent(objectId)}/session`);");
        expect(app).toContain("scVersionUrl('/api/project-comparison/catalog?object_id=' + encodeURIComponent(objectId), version)");
        expect(app).toContain("scVersionUrl('/api/stage-comparison/objects?object_id=' + encodeURIComponent(objectId), version)");
        expect(app).toContain("params.set('comparison_version', scComparisonVersion.value);");
        // Пачка загрузки помнит версию со старта.
        expect(app).toContain('comparisonVersion: scComparisonVersion.value,');
        expect(app).toContain('batch.objectId, batch.comparisonVersion, batch.stageName, candidate, retainBackup,');
        expect(app).toContain('&& batch.comparisonVersion === scComparisonVersion.value) {');
        expect(html).toContain(':comparison-version="scComparisonVersion || \'\'"');
        expect(catalog).toContain("comparisonVersion: {type: String, default: ''},");
        expect(catalog).toContain("versioned(API + '/progress?object_id=' + encodeURIComponent(props.objectId))");
    });

    it('открывает результат другой версии, сначала переключив версию', () => {
        const ensure = app.slice(app.indexOf('async function pcEnsureResultSession'), app.indexOf('async function pcOpenCatalogEntry'));
        expect(ensure).toContain('const owner = data.comparison_version;');
        expect(ensure).toContain('await scSelectComparisonVersion(owner)');
        expect(ensure.indexOf('scSelectComparisonVersion(owner)')).toBeLessThan(ensure.indexOf("const stages = scSelectedObject.value?.stages || [];"));
    });

    it('сбрасывает контекст сравнения и при смене объекта, и при смене версии', () => {
        expect(app).toContain('function scResetComparisonContext() {');
        const select = app.slice(app.indexOf('async function scSelectComparisonVersion'), app.indexOf('async function scOnComparisonVersionChange'));
        expect(select).toContain('scFlushDocumentPairing()');
        expect(select).toContain('scResetComparisonContext();');
        expect(select).toContain('await scLoadObjects();');
    });

    it('показывает переключатель, переименование и окно новой версии только с правами администратора на изменение', () => {
        const bar = html.slice(html.indexOf('class="sc-steps-bar"'), html.indexOf('Загрузить проекты', html.indexOf('class="sc-steps-bar"')));
        expect(bar).toContain('class="sc-version-switch"');
        expect(bar).toContain('<label class="sc-version-switch__label" for="sc-comparison-version">Версия сравнения</label>');
        expect(bar).toContain('v-if="canAdminPanels && scSelectedComparisonVersion"');
        expect(bar).toContain('v-if="canAdminPanels" type="button" class="btn btn-outline btn-sm sc-version-switch__new"');
        expect(bar).toContain('+ Новая версия');
        const dialog = html.slice(html.indexOf('id="sc-version-dialog-title"'), html.indexOf('sc-upload-tray'));
        expect(dialog).toContain('Новая версия сравнения');
        expect(dialog).toContain('Уже загруженный комплект');
        expect(dialog).toContain('Загрузить новый комплект');
        expect(dialog).toContain('{{ option.label }}');
    });
});
