import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const frontendRoot = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(frontendRoot, 'index.html'), 'utf8');
const appJs = fs.readFileSync(path.join(frontendRoot, 'static/js/app.js'), 'utf8');
const css = fs.readFileSync(path.join(frontendRoot, 'static/css/styles.css'), 'utf8');

describe('blocks without PDF vector text', () => {
  it('marks block cards and the opened block in Russian', () => {
    expect((html.match(/Векторный граф блока отсутствует/g) || []).length).toBeGreaterThanOrEqual(5);
    expect(html).toContain('class="tile-no-vector-badge"');
    expect(html).toContain('class="tile-status-pill tile-status-pill--no-vector"');
    expect(css).toContain('.tile-no-vector-badge {');
  });

  it('does not render or offer TXT for a confirmed image-only block', () => {
    expect(html).toContain('showBlockLlmText && !blockHasNoVectorGraph(selectedBlock)');
    expect(html).toContain('<button v-else class="tile-status-pill tile-status-pill--txt"');
    expect(appJs).toContain('block.vector_text_available === false');
    expect(appJs).toContain('showBlockLlmText.value = false;');
  });

  it('accepts the preview endpoint as the final fallback source of truth', () => {
    expect(appJs).toContain('if (payload.vector_text_available === false)');
    expect(appJs).toContain("block.vector_graph_message = payload.vector_graph_message");
    expect(appJs).toContain("|| 'Векторный граф блока отсутствует';");
  });
});
