import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const frontendRoot = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(frontendRoot, 'index.html'), 'utf8');
const css = fs.readFileSync(path.join(frontendRoot, 'static/css/styles.css'), 'utf8');

describe('block graph window layout', () => {
  it('uses a wider modal with dedicated navigation rails', () => {
    expect(html).toContain("'tile-split-container--navigable': currentBlocksList.length > 1");
    expect(css).toContain('max-width: 1440px; height: 94vh; max-height: 94vh;');
    expect(css).toContain('grid-template-columns: 36px minmax(0, 1fr) 380px 36px;');
    expect(css).toContain('.tile-split-container--navigable .block-nav-btn--prev { grid-column: 1;');
    expect(css).toContain('.tile-split-container--navigable .block-nav-btn--next { grid-column: 4;');
  });

  it('removes repetitive node state column and shows only review exceptions', () => {
    expect(html).toContain('<th>Обозначение</th><th>Тип</th></tr>');
    expect(html).not.toContain('<th>Обозначение</th><th>Тип</th><th>Состояние</th>');
    expect(html).toContain('blockLlmText.profiled_graph_display.nodes_review_total');
    expect(html).toContain('reference.match_score * 100');
    expect(html).toContain('reference.explanation');
    expect(html).toContain('reference_catalog.catalog_version');
    expect(html).toContain(':title="n.state_title">⚠ проверить</span>');
    expect(css).toContain('.pvg-node-row--review td:first-child');
  });

  it('always enters the blocks section through the all-blocks collection', () => {
    const appJs = fs.readFileSync(path.join(frontendRoot, 'static/js/app.js'), 'utf8');
    expect(appJs).toContain("selectedBlockPage.value = 'all';");
    expect(appJs).not.toContain('selectedBlockPage.value = blockPages.value[0].page_num;');
  });
});
