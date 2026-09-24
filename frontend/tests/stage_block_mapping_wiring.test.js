// Stage 2 «Смысловые блоки» (phase B): static wiring of index.html / app.js / main.py and the
// literals that existing tests and the plan pin (BACKWARD_COMPATIBILITY §4).
import {describe, expect, it} from 'vitest';
import {readFileSync} from 'node:fs';

const read = path => readFileSync(new URL(path, import.meta.url), 'utf8');
const html = read('../index.html');
const app = read('../static/js/app.js');
const module = read('../static/js/stage-block-mapping.js');
const css = read('../static/css/stage-block-mapping.css');
const main = read('../../backend/app/main.py');
const sheetMapHtml = html.slice(html.indexOf('class="sc-sheet-map"'), html.indexOf('v-if="scProcessingError"'));
const blockSection = app.slice(app.indexOf('// ─── Stage 2 «Смысловые блоки»'), app.indexOf('        return {', app.indexOf('// ─── Stage 2 «Смысловые блоки»')));

describe('assets and registration', () => {
    it('links the stylesheet next to project-change-ui.css and both scripts before app.js', () => {
        const pcCss = html.indexOf('/static/css/project-change-ui.css?v={{js_version}}');
        const sbmCss = html.indexOf('<link rel="stylesheet" href="/static/css/stage-block-mapping.css?v={{js_version}}">');
        expect(sbmCss).toBeGreaterThan(pcCss);
        expect(sbmCss - pcCss).toBeLessThan(200);
        const catalog = html.indexOf('/static/js/project-comparison-catalog.js?v={{js_version}}');
        const core = html.indexOf('<script src="/static/js/human-mapping-core.js?v={{js_version}}"></script>');
        const sbm = html.indexOf('<script src="/static/js/stage-block-mapping.js?v={{js_version}}"></script>');
        const appJs = html.indexOf('<script src="/static/js/app.js?v={{js_version}}"></script>');
        expect(catalog).toBeGreaterThan(0);
        expect(catalog < core && core < sbm && sbm < appJs).toBe(true);
    });

    it('feeds the new files into js_version (the CSS is linked with js_version too)', () => {
        const tuple = main.slice(main.indexOf('pc_js = ['), main.indexOf('js_mtimes = ['));
        for (const name of ["'human-mapping-core.js'", "'stage-block-mapping.js'", "'../css/stage-block-mapping.css'"]) {
            expect(tuple).toContain(name);
        }
    });

    it('registers the components only when the module is present', () => {
        expect(app).toContain('if (window.StageBlockMapping) window.StageBlockMapping.register(app);');
        expect(app.indexOf('window.StageBlockMapping.register(app)')).toBeLessThan(app.indexOf("app.mount('#app')"));
        expect(app).toContain('const scBlockStore = ref(window.StageBlockMapping ? window.StageBlockMapping.createStore() : null);');
        expect(module).toContain('if (!root.HumanMappingCore) return null;');
    });
});

describe('app.js state of stage 2 (additive)', () => {
    it('starts on «Листы» and never persists the mode', () => {
        expect(app).toContain("const scStage2Mode = ref('pages');");
        expect(blockSection).not.toMatch(/localStorage|sessionStorage/);
        expect(module).not.toContain('localStorage');
        expect(blockSection).toContain("scStage2Mode.value = 'pages';");
        expect(blockSection).toContain("watch(scProductionEvidence, v => v && (scStage2Mode.value = 'pages'));");
    });

    it('loads the store on pair/session change and follows the saved sheet links for the §12 notice', () => {
        expect(blockSection).toContain('scBlockStore.value.load(bridge ? {} : {sessionId, pairId, catalogFocus: pcCatalogFocus.value})');
        expect(blockSection).toContain('watch(() => scMatchState.value?.links?.links');
        expect(blockSection).toContain('noteSheetLinks(');
        // Page rasters are built by the viewer's own function and signature (shared browser cache).
        // Rasters share the viewer's cache: same scPagePreviewUrl, same per-PDF signature, kept across page-info reloads.
        expect(blockSection).toContain('scBlockStore.value.setPreviewUrlBuilder((side, page, width) => {');
        expect(blockSection).toContain('const signature = scPageSignatures[side] || scBlockPdfSignatures[side];');
        expect(blockSection).toContain('if (signature) return scPagePreviewUrl(side, page, width, signature);');
        expect(blockSection).toContain('return scPageLoading[side] && scPageInfoRequest[side] ? null : \'\';');
        expect(blockSection).toContain('scBlockPdfSignatures.left = scBlockPdfSignatures.right = \'\';');
        for (const name of ['function scSetStage2Mode(mode)', 'function scOpenBlockRow(row)', 'function scBlockChip(row)']) {
            expect(blockSection).toContain(name);
        }
    });

    it('keeps the pinned slices and adjacency of other tests untouched', () => {
        const start = app.indexOf('let pcNavigationToken =');
        const slice = app.slice(start, app.indexOf('const scTextDifferenceFilterOptions', start));
        expect(slice).not.toMatch(/scStage2Mode|scBlockStore|StageBlockMapping/);
        expect(app).toMatch(/pcOpenEvidence, pcRunBanner,/);
        const catalog = app.slice(app.indexOf('async function pcOpenCatalogEntry'));
        expect(catalog.slice(0, catalog.indexOf('\n        }\n'))).not.toMatch(/scStage2Mode|scBlockStore/);
        // C13: the catalog focus carries result_id only for a sealed snapshot.
        expect(catalog).toContain('result_source: entry.result_source,');
        expect(catalog).toContain("result_id: entry.result_source === 'SEALED_SNAPSHOT' ? entry.provenance?.result_id || '' : '',");
        expect(app).toContain('scStage2Mode, scBlockStore, scSetStage2Mode, scOpenBlockRow, scBlockChip, scCurrentSheetMapRow,');
    });

    it('keeps API calls out of app.js and adds no forbidden token', () => {
        expect(app).not.toMatch(/\/block-mapping\/|region-index|page-blocks|bridge-check|\/api\/human-mapping/);
        for (const token of ['prepared-comparison', 'page-image', 'block-image', 'graphic-diff', 'semantic-diff',
            'change-regions', 'change-groups', 'pipeline-v2', 'unified-analysis', 'text-llm', 'comparison-statuses']) {
            expect(app).not.toContain(token);
            expect(module).not.toContain(token);
        }
        expect(html).not.toContain('page-image');
    });
});

describe('index.html markup', () => {
    it('puts the segment at the left of the viewer toolbar', () => {
        const toolbar = html.slice(html.indexOf('<div class="sc-viewer-toolbar">'), html.indexOf('<div class="sc-viewer-layout"'));
        expect(toolbar.indexOf('class="sbm-segment"')).toBeLessThan(toolbar.indexOf('sc-viewer-toolbar__spacer'));
        expect(toolbar).toContain("@click=\"scSetStage2Mode('pages')\">Листы</button>");
        expect(toolbar).toContain("@click=\"scSetStage2Mode('blocks')\">{{ scBlockStore.segmentLabel(scCurrentSheetMapRow()) }}</button>");
        expect(toolbar).toContain('v-if="scBlockStore && !pcBridgeActive" class="sbm-segment"');
    });

    it('hides the sheet viewer with v-show and mounts the workspace as a sibling with v-if', () => {
        expect(html).toContain('<div class="sc-viewer-layout" v-show="scStage2Mode !== \'blocks\'">');
        const workspace = html.indexOf('<semantic-block-workspace v-if="scBlockStore && scStage2Mode === \'blocks\'"');
        expect(workspace).toBeGreaterThan(html.indexOf('class="sc-view-mode-switch"'));
        expect(workspace).toBeGreaterThan(html.indexOf('<div class="sc-viewer-shell">'));
        // After the closing tags of the viewer and the layout, before the one of the shell.
        const tail = html.slice(html.indexOf('class="sc-view-mode-switch"'), workspace);
        expect(tail.match(/<\/div>/g).length).toBe(3);
        const element = html.slice(workspace, html.indexOf('</semantic-block-workspace>', workspace));
        expect(element).toContain(':current-row="scCurrentSheetMapRow()"');
        expect(element).toContain('@open-row="scOpenBlockRow"');
        expect(html.slice(workspace).match(/^[\s\S]*?<\/semantic-block-workspace>\s*<\/div>\s*<\/template>/)).not.toBeNull();
    });

    it('adds the row chip inside the badge cell without edit words or the actions class', () => {
        const row = sheetMapHtml.slice(sheetMapHtml.indexOf('class="sc-sheet-map__row"'), sheetMapHtml.indexOf('sc-sheet-map__empty'));
        const chip = row.slice(row.indexOf('<button v-if="scBlockChip(row)"'), row.indexOf('</button>', row.indexOf('<button v-if="scBlockChip(row)"')) + 9);
        expect(chip).toContain('class="sc-sheet-map__semantic"');
        expect(chip).toContain('@click.stop="scOpenBlockRow(row)"');
        expect(chip).not.toMatch(/Изменить|\+ Добавить|Удалить|sc-sheet-map__actions/);
        expect(row.indexOf('class="sbm-badge-cell"')).toBeLessThan(row.indexOf('<button v-if="scBlockChip(row)"'));
        expect(row.indexOf('<button v-if="scBlockChip(row)"')).toBeLessThan(row.indexOf('class="sc-sheet-map__badge"'));
    });

    it('adds the summary lines to the head and the pinned regions row under the list (not a map row)', () => {
        const head = sheetMapHtml.slice(sheetMapHtml.indexOf('class="sc-sheet-map__head"'), sheetMapHtml.indexOf('v-if="scActiveSheetLinkRepair"'));
        expect(head).toContain('<sbm-sheet-summary v-if="scBlockStore"');
        const footer = sheetMapHtml.indexOf('<sbm-map-footer v-if="scBlockStore"');
        expect(footer).toBeGreaterThan(sheetMapHtml.indexOf('v-if="!scSheetMapCollapsed" class="sc-sheet-map__list"'));
        expect(footer).toBeLessThan(sheetMapHtml.indexOf('</section>', footer));
        expect(module.slice(module.indexOf("app.component('sbm-map-footer'"))).not.toContain('sc-sheet-map__row');
    });

    it('keeps the literals pinned by existing tests', () => {
        expect(html).toContain('2. Сопоставление листов');
        expect(html).toContain('class="sc-shell sc-shell--viewer"');
        expect(html).toContain('<div class="sc-viewer-shell">');
        const panel = html.slice(html.indexOf('class="sc-thumbs"'), html.indexOf('class="sc-vector-viewer"'));
        expect(panel.match(/<button/g).length).toBe(1);
        for (const bad of ['>Изменить</button>', '>+ Добавить</button>', '>Удалить</button>', 'class="sc-sheet-map__actions"']) {
            expect(sheetMapHtml).not.toContain(bad);
        }
    });
});

describe('module and stylesheet', () => {
    it('only reads: no write verbs, no patched global fetch, explicit scope for HM', () => {
        expect(module).not.toMatch(/method:\s*'(POST|PUT|DELETE|PATCH)'/);
        expect(module).not.toMatch(/window\.fetch\s*=|root\.fetch\s*=/);
        expect(module).toContain("`session_id=${enc(scope.sessionId)}&run_id=${enc(scope.runId)}`");
        expect(module).toContain('`result_id=${enc(scope.resultId)}`');
    });

    it('styles only .sbm-* classes and the row chip', () => {
        const selectors = css.replace(/\/\*[\s\S]*?\*\//g, '').match(/[^{}]+(?=\{)/g)
            .map(s => s.trim()).filter(s => s && !s.startsWith('@'));
        for (const selector of selectors) {
            for (const part of selector.split(',')) expect(part, part).toMatch(/\.sbm-|\.sc-sheet-map__semantic/);
        }
    });

    it('carries the matrix texts verbatim', () => {
        for (const text of [
            'Смысловые регионы строит анализ изменений (шаг 3). Сейчас можно посмотреть блоки распознавания и проверить, правильно ли сопоставлены листы. Связи блоков сохраняются в конкретный результат анализа и станут доступны после первого анализа.',
            'Редактирование смысловых связей в этом разделе ещё не включено. Для правки откройте Human Mapping ↗',
            'Решения этого снимка хранятся отдельно от результатов анализа и не могут стать якорями нового анализа.',
            'Пара листов не подтверждена — блоки показаны по предложению.',
            'Файлы распознавания перезаписаны без изменений — работа с блоками доступна.',
            'Идёт анализ изменений. Смысловые регионы появятся после его завершения.',
            'Пары листов не построены — смысловые блоки открываются по регионам. Постройте карту листов, чтобы видеть регионы по парам листов.',
            'Пары листов не построены. Нажмите «Обработать», чтобы построить карту листов.',
            'Показаны видимые страницы; прокрутите для остальных.',
            'Не удалось загрузить смысловые блоки.',
            'Автор в данных не хранится.',
            'записано вне этого результата', 'записано вне этого снимка', '⟳ контекст изменился',
            'ИИ-анализ отключён администратором', 'модель недоступна или исчерпан лимит подписки',
            'блок не найден в данных результата', 'регион не найден', 'Нет пары листов, где видны обе стороны региона',
            'Все листы решены',
        ]) expect(module).toContain(text);
    });
});
