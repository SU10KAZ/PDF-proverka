import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const css = readFileSync(new URL('../static/css/styles.css', import.meta.url), 'utf8');

describe('legacy deterministic stage comparison text compatibility', () => {
  it('keeps the persisted legacy flow callable without exposing a second production control', () => {
    expect(app).toContain('const scTextComparison = ref(null)');
    expect(app).toContain("scTextComparison.value = data.text_comparison || null");
    expect(app).toContain("scPairUrl(scActivePair.value.id, '/text-comparison')");
    expect(html).not.toContain('scRunTextComparison');
    expect(html).not.toContain('scRunTextDifferences');
    expect(html).not.toContain('scRunTextAiReview');
    expect(html).not.toContain('scRunProjectChangeSummary');
    expect(html).not.toContain('Пересчитать текст');
    expect(html).not.toContain('Legacy-диагностика связей листов');
    expect(html).toContain('Legacy-результаты TEXT — только для совместимости');
  });

  it('removes legacy overlays from both production viewer modes', () => {
    expect(html).not.toContain('class="sc-text-comparison-mask"');
    expect(html).not.toContain('scTextComparisonOverlaysFor(side, scCurrentPage[side])');
    expect(html).not.toContain('scTextComparisonOverlaysFor(side, entry.page)');
    expect(html).not.toContain('sc-text-evidence-overlay');
    expect(css).not.toContain('.sc-text-evidence-overlay');
  });

  it('retains legacy stale bookkeeping without exposing link mutations', () => {
    expect(app).toContain("scTextComparison.value = {...scTextComparison.value, stale: true}");
    expect(app).toContain('async function scApplyTextHint(hint, replace = false)');
    expect(html).not.toContain('@click="scApplyTextHint(hint, false)"');
    expect(html).not.toContain('@click="scApplyTextHint(hint, true)"');
  });
});
