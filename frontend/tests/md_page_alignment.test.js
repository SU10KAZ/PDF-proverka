import { describe, it, expect } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const APP_JS = fs.readFileSync(path.join(__dirname, '../static/js/app.js'), 'utf8');
const INDEX_HTML = fs.readFileSync(path.join(__dirname, '../index.html'), 'utf8');
const CSS = fs.readFileSync(path.join(__dirname, '../static/css/styles.css'), 'utf8');

describe('MD enrichment — постраничное выравнивание', () => {
  it('разбивает обе стороны на одинаковые page-секции', () => {
    expect(APP_JS).toContain('function _scMdSplitPages(text)');
    expect(APP_JS).toContain('data-sc-md-page');
    expect(APP_JS).toContain('blank: !own.pages.has(page)');
    expect(INDEX_HTML).toContain('scMdRenderHtml(scMdView[side].content, side)');
    expect(INDEX_HTML).toContain('scMdHighlightHtml(scMdView[side].content, side)');
  });

  it('задаёт обеим версиям страницы высоту более длинной стороны', () => {
    expect(APP_JS).toContain('function scSyncMdPageHeights()');
    expect(APP_JS).toContain('left.style.minHeight = `${height}px`');
    expect(APP_JS).toContain('right.style.minHeight = `${height}px`');
    expect(CSS).toContain('.sc-md-page-section');
    expect(CSS).toContain('.sc-md-page-section--blank');
  });

  it('прокручивает выровненные панели на одинаковое расстояние', () => {
    expect(APP_JS).toContain('otherPane.scrollTop = pane.scrollTop');
    expect(INDEX_HTML).toContain('@load.capture="scScheduleMdPageAlignment()"');
  });
});
