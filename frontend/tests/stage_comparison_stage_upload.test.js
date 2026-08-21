import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const css = readFileSync(new URL('../static/css/styles.css', import.meta.url), 'utf8');

describe('documentation comparison shell', () => {
  it('keeps the four shell tabs and the later-stage empty state', () => {
    expect(html).toContain('1. Загрузка документации');
    expect(html).toContain('2. Связь блоков');
    expect(html).toContain('3. Расхождения');
    expect(html).toContain('4. Отчёт');
    expect(html).toContain('Сравнение ещё не выполнено');
  });

  it('does not expose the source-card heading or internal session id', () => {
    expect(html).not.toContain('Исходные документы');
    expect(html).not.toContain('Выберите PDF с каждой стадии');
    expect(html).not.toContain('Пары документов');
    expect(html).not.toContain('Выставьте соответствующие проекты друг напротив друга');
    expect(html).not.toContain('Нажмите проект в одной колонке');
    expect(html).not.toContain('Сбросить порядок');
    expect(html).not.toContain('{{ scSession.id }}');
  });

  it('renders only the vector PDF endpoint', () => {
    expect(app).toContain('/page-svg?side=${side}&page=${scCurrentPage[side]}');
    expect(html).not.toContain('page-image');
    expect(app).not.toContain('page-image');
    expect(app).not.toContain('block-image');
  });

  it('inlines the page SVG instead of rasterising it through <img>', () => {
    expect(html).toContain('class="sc-vector-stage"');
    expect(html).toContain('v-html="scPageSvg[side]"');
    expect(html).not.toContain('<img :src="scPageSrcUrl(side)"');
    expect(css).toContain('.sc-vector-stage > svg');
    // will-change закрепил бы растровый масштаб слоя — лист мылился бы при зуме
    expect(css).not.toMatch(/\.sc-vector-stage\s*\{[^}]*will-change/);
  });

  it('drives both panes from one normalised viewport', () => {
    expect(app).toContain('function scViewFor(side)');
    expect(app).toContain('return scSyncView.value ? scViews.left : scViews[side]');
    expect(app).toContain('function scFitScale(side)');
    expect(app).toContain('function scApplyView()');
    expect(app).toContain('scScheduleView');
    expect(app).toContain('requestAnimationFrame');
  });

  it('keeps the sheet map to five compact rows', () => {
    // высота списка считается из высоты строки — иначе «пять» разъедется
    expect(css).toContain('--sc-sheet-map-row: 24px;');
    expect(css).toContain('--sc-sheet-map-visible: 5;');
    expect(css).toContain('max-height: calc((var(--sc-sheet-map-row) + 1px) * var(--sc-sheet-map-visible));');
    expect(css).toContain('min-height: var(--sc-sheet-map-row);');
    expect(css).not.toContain('max-height: min(42vh, 520px);');
    // на узком экране кнопки уезжают на свою строку — строка выше
    expect(css).toContain('.sc-sheet-map__list { --sc-sheet-map-row: 46px; }');
  });

  it('keeps the active sheet-map row visible while paging', () => {
    // scrollIntoView утащил бы всю страницу — крутим сам список
    expect(app).toContain('function scRevealActiveSheetMapRow()');
    expect(app).toContain(".sc-sheet-map__row.is-active");
    expect(app).toContain('list.scrollTop -= listRect.top - rowRect.top;');
    expect(app).toContain('list.scrollTop += rowRect.bottom - listRect.bottom;');
    expect(app).not.toContain('.sc-sheet-map__row.is-active\').scrollIntoView');
  });

  it('promotes the layer for panning only, never for zooming', () => {
    // will-change при зуме заморозил бы растровый масштаб слоя → мыло
    expect(app).toContain('function scBoostPan()');
    expect(app).toContain('function scDropPanBoost()');
    expect(app).toContain("stage.style.willChange = active ? 'transform' : ''");
    expect(app).toMatch(/function scZoomAt\([^)]*\)\s*\{\s*\n\s*scDropPanBoost\(\);/);
    expect(app).toMatch(/scBoostPan\(\);\s*\n\s*scScheduleView\(\);/);
  });

  it('zooms on Ctrl+wheel and scrolls the sheet on a bare wheel', () => {
    expect(app).toContain('function scOnViewerWheel(side, event)');
    expect(app).toContain('if (event.ctrlKey || event.metaKey)');
    expect(app).toContain("addEventListener('wheel', onWheel, {passive: false})");
    expect(app).toContain('const SC_ZOOM_MAX = 100;');
  });

  it('runs only the new Markdown sheet matcher', () => {
    expect(html).toContain('@click="scProcessPairRow(row)"');
    expect(html).toContain('Запустить сравнение');
    expect(app).toContain("'/sheet-match-suggestions'");
    expect(app).toContain("'/sheet-links'");
    expect(app).toContain("method: 'PUT'");
  });

  it('manages every PDF pair in its own draggable row', () => {
    expect(html).not.toContain('id="sc-left-pdf"');
    expect(html).not.toContain('id="sc-right-pdf"');
    expect(html).toContain('v-for="row in scPairRows"');
    expect(html).toContain("scStartDocumentDrag($event, 'left', row.index)");
    expect(html).toContain("scStartDocumentDrag($event, 'right', row.index)");
    expect(html).toContain("scDropDocument('left', row.index)");
    expect(html).toContain("scDropDocument('right', row.index)");
    expect(html).toContain('@click="scOpenPairRow(row)">Открыть</button>');
    expect(app).toContain('stage-comparison:pair-order:');
    expect(app).toContain('scPersistDocumentOrder');
    expect(app).toContain('scPairRowStates');
  });

  it('confirms pairs by clicking both documents and keeps them blue', () => {
    expect(html).toContain("@click=\"scSelectPairDocument('left', row)\"");
    expect(html).toContain("@click=\"scSelectPairDocument('right', row)\"");
    expect(html).toContain("'is-confirmed': scIsPairRowConfirmed(row)");
    expect(app).toContain('confirmedPairs: Object.values(scConfirmedDocumentPairs)');
    expect(app).toContain('scConfirmedDocumentPairs[scPairPathsKey(leftPdf, rightPdf)]');
    expect(css).toContain('.sc-pair-document.is-confirmed');
    expect(css).toContain('border-color: #2563eb');
  });

  it('saves project pairing on the server and restores it before a local draft', () => {
    expect(html).toContain('Сохранить сопоставленные проекты');
    expect(html).toContain('@click="scSaveDocumentPairing()"');
    expect(app).toContain('/document-pairing`');
    expect(app).toContain("method: 'PUT'");
    expect(app).toContain('serverPairing.left_order');
    expect(app).toContain('serverPairing.right_order');
    expect(app).toContain('serverPairing.confirmed_pairs');
    expect(app).toContain('if (useSaved && !loadedFromServer && storageKey)');
    expect(app).toContain("scPairingSaveMessage.value = loadedFromServer ? 'Загружено сохранённое сопоставление'");
    expect(css).toContain('.sc-pair-savebar');
  });

  it('moves a whole pair vertically from the row handle', () => {
    expect(html).toContain('@dragstart="scStartPairRowDrag($event, row)"');
    expect(html).toContain('@dragover.prevent="scDragPairRowOver($event, row.index)"');
    expect(html).toContain('@drop.prevent="scDropPairRow(row.index)"');
    expect(app).toContain("for (const side of ['left', 'right'])");
    expect(app).toContain('values.splice(index, 0, document)');
    expect(css).toContain('.sc-pair-row-handle');
  });

  it('renders one ordered sheet map instead of summary cards and unmatched chips', () => {
    expect(html).toContain('Карта листов');
    expect(html).toContain('v-for="row in scSheetMapRows"');
    expect(html).toContain("scSheetMapSideLabel(row.leftPages, 'left')");
    expect(html).toContain("scSheetMapSideLabel(row.rightPages, 'right')");
    expect(html).toContain('@click="scOpenSheetMapRow(row)"');
    expect(html).not.toContain('class="sc-match-summary"');
    expect(html).not.toContain('Текущая связь');
    expect(html).not.toContain('Не сопоставлены П:');
    expect(html).not.toContain('Не сопоставлены РД:');
    expect(css).not.toContain('.sc-match-summary');
    expect(css).not.toContain('.sc-link-passports');
  });

  it('keeps compact actions and manual many-to-many rows in the sheet map', () => {
    expect(html).toContain('>Оставить</button>');
    expect(html).toContain("scOpenSheetMapEditor(row, 'replace')");
    expect(html).toContain("scOpenSheetMapEditor(row, 'add')");
    expect(html).toContain('@click="scDeleteSheetMapRow(row)"');
    expect(html).toContain('>Изменить</button>');
    expect(html).toContain('>+ Добавить</button>');
    expect(html).toContain('>Удалить</button>');
    expect(app).toContain('leftPages: link.left_pages || []');
    expect(app).toContain('rightPages: link.right_pages || []');
    expect(app).toContain('return `Листы ${uniquePages.map(sheetNumber).join(\', \')}`');
    expect(app).toContain('payload.left_sheet_index || []');
    expect(app).toContain('payload.right_sheet_index || []');
    expect(app).toContain('suggestionsPayload.right_sheet_index');
    expect(app).toContain('String(sheet.title)');
    expect(app).not.toContain('scPassportFor');
    expect(app).not.toContain('sheet_title_reliable');
    expect(app).not.toContain('passport.buildings');
    expect(app).toContain('left_pages: leftPages, right_pages: rightPages');
    expect(app).toContain('user_corrected');
  });

  it('does not call removed analytical APIs', () => {
    const forbidden = [
      'prepared-comparison', 'page-image', 'block-image', 'graphic-diff',
      'semantic-diff', 'change-regions', 'change-groups', 'pipeline-v2',
      'unified-analysis', 'text-llm', 'comparison-statuses',
    ];
    for (const token of forbidden) expect(app).not.toContain(token);
    expect(app.match(/\/api\/stage-comparison/g)?.length).toBeGreaterThanOrEqual(6);
  });
});
