import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');

describe('Stage 4 semantic AI reviewer', () => {
  it('loads separate reviewer and final artifacts and invokes the reviewer after Stage 3', () => {
    expect(app).toContain('const scTextAiReview = ref(null)');
    expect(app).toContain('const scTextFinalComparison = ref(null)');
    expect(app).toContain('data.text_ai_review || null');
    expect(app).toContain('data.text_final_comparison || null');
    expect(app).toContain("scPairUrl(scActivePair.value.id, '/text-ai-review')");
    expect(app).toContain('await scRunTextAiReview();');
  });

  it('keeps the legacy overlay adapter for compatibility but does not render it', () => {
    expect(app).toContain('if (!scTextFinalComparison.value || scTextFinalComparison.value.stale) return []');
    expect(app).toContain('const overlays = scTextFinalComparison.value.overlays');
    expect(html).not.toContain('scTextComparisonOverlaysFor');
    expect(html).not.toContain('sc-text-evidence-overlay');
    expect(html).not.toContain("overlay.status === 'found_on_other_sheet'");
  });

  it('keeps one row per sheet group and exposes uncertainty separately', () => {
    expect(html).toContain('v-for="group in scTextDifferenceGroups"');
    expect(html).toContain('<th>Требует проверки</th>');
    expect(html).toContain("group.uncertain || []");
    expect(app).toContain('window.StageComparisonDifferences.buildRows');
  });

  it('does not make legacy AI review a production success prerequisite', () => {
    expect(html).toContain('Проверено ИИ');
    expect(html).toContain('Требует проверки:');
    expect(html).toContain('Только детерминированная проверка');
    expect(html).toContain('Частично проверено ИИ');
    expect(html).toContain("scTextFinalComparison.review_status === 'partial'");
    expect(html).not.toContain('Запустить ИИ-проверку');
    expect(html).not.toContain('Повторить ИИ-проверку');
    expect(html).not.toContain('scProductionTextPresentation');
    expect(html).not.toContain('ИИ-проверка не выполнена полностью');
    expect(app).toContain("scTextFinalComparison.value.review_status === 'completed'");
  });
});
