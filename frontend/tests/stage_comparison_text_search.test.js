import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const css = readFileSync(new URL('../static/css/styles.css', import.meta.url), 'utf8');

describe('stage comparison PDF text search', () => {
  it('renders a compact independent search form in every viewer pane', () => {
    expect(html).toContain('v-model="scTextSearchQuery[side]"');
    expect(html).toContain('@submit.prevent="scSearchText(side)"');
    expect(html).toContain('Поиск по тексту в');
    expect(html).toContain('title="Найти" aria-label="Найти"');
    expect(html).toContain('<circle cx="10.5" cy="10.5" r="6.5"></circle>');
    expect(html).not.toContain('>\n                                            Найти\n');
    expect(css).toContain('.sc-text-search input {');
    expect(css).toContain('width: 112px;');
  });

  it('keeps left and right results separate and advances through occurrences', () => {
    expect(app).toContain("const scTextSearchQuery = reactive({left: '', right: ''})");
    expect(app).toContain('const scTextSearchPages = reactive({left: [], right: []})');
    expect(app).toContain('const scTextSearchResults = reactive({left: [], right: []})');
    expect(app).toContain('function scResetTextSearch(side, clearQuery = false)');
    expect(app).toContain('/text-search?${params}');
    expect(app).toContain('function scNavigateTextSearch(side, direction)');
    expect(app).toContain('(current + step + results.length) % results.length');
    expect(html).toContain('@click="scNavigateTextSearch(side, -1)"');
    expect(html).toContain('@click="scNavigateTextSearch(side, 1)"');
    expect(html).toContain('Предыдущее совпадение');
    expect(html).toContain('Следующее совпадение');
  });

  it('draws independent coordinate highlights over paged and continuous sheets', () => {
    expect(app).toContain('const scTextSearchHighlights = reactive({left: {}, right: {}})');
    expect(app).toContain('function scTextSearchHighlightsFor(side, page)');
    expect(app).toContain('function scTextSearchHighlightStyle(highlight)');
    expect(html.match(/class="sc-text-highlight"/g)).toHaveLength(2);
    expect(html).toContain('scTextSearchHighlightsFor(side, scCurrentPage[side])');
    expect(html).toContain('scTextSearchHighlightsFor(side, entry.page)');
    expect(css).toContain('.sc-text-highlight {');
    expect(css).toContain('pointer-events: none;');
    expect(html).toContain("'is-active': scTextSearchHighlightActive");
    expect(css).toContain('.sc-text-highlight.is-active {');
  });

  it('centers the active occurrence in paged and continuous modes', () => {
    expect(app).toContain('const SC_SEARCH_FOCUS_ZOOM = 2.5;');
    expect(app).toContain('function scCenterTextSearchResult(side, result)');
    expect(app).toContain('view.cx = x;');
    expect(app).toContain('view.cy = y;');
    expect(app).toContain('scSetContinuousAnchor(side, result.page, anchor)');
    expect(app).toContain('x, y, viewportX: 0.5, viewportY: 0.5');
  });
});
