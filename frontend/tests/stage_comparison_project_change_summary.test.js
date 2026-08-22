import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');

describe('Stage 5 project change summary', () => {
  it('loads a separate artifact and runs aggregation after the Stage 4 reviewer', () => {
    expect(app).toContain('const scProjectChangeSummary = ref(null)');
    expect(app).toContain('data.project_change_summary || null');
    expect(app).toContain("scPairUrl(scActivePair.value.id, '/text-change-summary')");
    expect(app).toContain('await scRunProjectChangeSummary();');
  });

  it('makes project changes primary and preserves provenance navigation', () => {
    expect(html).toContain('<strong>Основные изменения проекта</strong>');
    expect(html).toContain('Показать детали ({{ change.count }})');
    expect(html).toContain('v-for="item in change.details"');
    expect(html).toContain("scOpenDifferenceSource(group, item, 'left')");
    expect(html).toContain("scOpenDifferenceSource(group, item, 'right')");
  });

  it('keeps service and raw atomic evidence collapsed by default', () => {
    expect(html).toContain(
      '<details v-if="group.service_structure.length" class="sc-project-group__service">',
    );
    expect(html).toContain(
      '<details v-if="scTextResultAvailable" class="sc-raw-differences">',
    );
    expect(html).toContain('Показать исходные текстовые различия');
    expect(html).toContain('v-for="group in scTextDifferenceGroups"');
    expect(html).not.toContain(
      '<details v-if="group.service_structure.length" class="sc-project-group__service" open>',
    );
    expect(html).not.toContain(
      '<details v-if="scTextResultAvailable" class="sc-raw-differences" open>',
    );
  });

  it('shows wrong-purpose pairs prominently and suppresses their project section', () => {
    expect(html).toContain("group.pair_precheck.status === 'PAIR_REVIEW_REQUIRED'");
    expect(html).toContain('⚠ Возможно неверно сопоставлены листы');
    expect(html).toContain('Технические выводы для этой пары не сформированы');
    expect(html).toContain('<section v-else class="sc-project-group__changes">');
  });
});
