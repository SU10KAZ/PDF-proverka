import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const css = readFileSync(new URL('../static/css/styles.css', import.meta.url), 'utf8');

describe('removed production text evidence panel', () => {
  it('does not render the text summary or its mode controls', () => {
    expect(html).not.toContain('id="sc-text-evidence-title"');
    expect(html).not.toContain('class="sc-text-evidence-summary"');
    expect(html).not.toContain('scProductionTextPresentation');
    expect(html).not.toContain('scSetTextEvidenceMode');
    expect(html).not.toContain('Показать совпадения');
    expect(html).not.toContain('Показать изменения');
  });

  it('does not load or retain state for the removed viewer feature', () => {
    expect(app).not.toContain("'/text-evidence'");
    expect(app).not.toContain('scProductionTextEvidence');
    expect(app).not.toContain('scTextEvidenceMode');
    expect(app).not.toContain('scTextEvidenceOverlaysFor');
    expect(app).not.toContain('scActivateTextEvidence');
  });

  it('does not draw text evidence overlays in either viewer mode', () => {
    expect(html).not.toContain('sc-text-evidence-overlay');
    expect(css).not.toContain('.sc-text-evidence-summary');
    expect(css).not.toContain('.sc-text-evidence-overlay');
  });
});
