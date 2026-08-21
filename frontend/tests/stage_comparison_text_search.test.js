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
    expect(html).toContain('Найти');
    expect(css).toContain('.sc-text-search input {');
    expect(css).toContain('width: 112px;');
  });

  it('keeps left and right results separate and advances on repeated search', () => {
    expect(app).toContain("const scTextSearchQuery = reactive({left: '', right: ''})");
    expect(app).toContain('const scTextSearchPages = reactive({left: [], right: []})');
    expect(app).toContain('function scResetTextSearch(side, clearQuery = false)');
    expect(app).toContain('/text-search?${params}');
    expect(app).toContain('(scTextSearchIndex[side] + 1) % pages.length');
    expect(app).toContain('scOpenTextSearchPage(side, pages[scTextSearchIndex[side]])');
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
  });
});
