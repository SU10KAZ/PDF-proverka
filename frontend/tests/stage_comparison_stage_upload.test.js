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
    expect(html).toContain('<img :src="scPageSrcUrl(side)"');
    expect(html).not.toContain('page-image');
    expect(app).not.toContain('page-image');
    expect(app).not.toContain('block-image');
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
