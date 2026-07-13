import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const frontendRoot = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(frontendRoot, 'index.html'), 'utf8');
const appJs = fs.readFileSync(path.join(frontendRoot, 'static/js/app.js'), 'utf8');
const css = fs.readFileSync(path.join(frontendRoot, 'static/css/styles.css'), 'utf8');

describe('text-layer shadow highlights', () => {
  it('loads the optional version-aware shadow artifact without breaking findings', () => {
    expect(appJs).toContain("api(`/findings/${id}/textlayer-highlights-shadow`).catch(() => null)");
    expect(appJs).toContain('Array.isArray(shadowResp.records)');
    expect(appJs).toContain('currentBlockTextlayerHighlights = computed');
  });

  it('keeps the diagnostic overlay separate from final LLM highlights', () => {
    expect(html).toContain('showTextlayerHighlightsShadow && currentBlockTextlayerHighlights.length > 0');
    expect(html).toContain('block-textlayer-shadow-svg');
    expect(html).toContain("{{ r.finding_id }} · {{ r.label || 'text-layer' }}");
    expect(html).not.toContain('currentBlockHighlights');
    expect(html).not.toContain('finding-visibility-toggle');
    expect(html).not.toContain('btn-toggle-highlights');
  });

  it('offers a visible toggle and renders dashed magenta boxes', () => {
    expect(html).toContain('@click.stop="toggleTextlayerHighlightsShadow()"');
    expect(html).toContain("showTextlayerHighlightsShadow ? '✓ замечания' : 'замечания'");
    expect(css).toContain('.tile-status-pill--textlayer-shadow');
    expect(css).toContain('.block-textlayer-shadow-rect');
    expect(css).toContain('stroke-dasharray: 6 3');
  });
});
