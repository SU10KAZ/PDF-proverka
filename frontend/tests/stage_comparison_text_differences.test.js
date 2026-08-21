import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const css = readFileSync(new URL('../static/css/styles.css', import.meta.url), 'utf8');

describe('grouped Stage 3 text differences', () => {
  it('loads and rebuilds the durable text_differences contract', () => {
    expect(app).toContain('const scTextDifferences = ref(null)');
    expect(app).toContain('scTextDifferences.value = data.text_differences || null');
    expect(app).toContain("scPairUrl(scActivePair.value.id, '/text-differences')");
    expect(app).toContain('await scRunTextDifferences();');
  });

  it('renders exactly one table row per sheet group with three grouped lists', () => {
    expect(html).toContain('v-for="group in scTextDifferenceGroups"');
    expect(html).toContain("v-for=\"bucket in ['changed', 'removed', 'added']\"");
    expect(html).toContain('<th>Изменилось</th>');
    expect(html).toContain('<th>Удалено</th>');
    expect(html).toContain('<th>Добавлено</th>');
    expect(html).not.toContain('v-for="item in scTextDifferenceGroups"');
  });

  it('keeps full before/after text collapsed and opens its source area', () => {
    expect(html).toContain('<summary>{{ item.summary }}</summary>');
    expect(html).toContain('<pre>{{ item.before }}</pre>');
    expect(html).toContain('<pre>{{ item.after }}</pre>');
    expect(html).toContain("scOpenDifferenceSource(group, item, 'left')");
    expect(html).toContain("scOpenDifferenceSource(group, item, 'right')");
    expect(app).toContain('view.cx = Number(box.x || 0)');
    expect(css).toContain('.sc-differences__items details');
  });

  it('does not show unchanged groups and marks stale results', () => {
    expect(app).toContain('window.StageComparisonDifferences.buildRows');
    expect(html).toContain('scTextAllDifferenceGroups.length');
    expect(app).toContain('scTextDifferences.value = {...scTextDifferences.value, stale: true}');
    expect(html).toContain('Текстовых расхождений нет');
    expect(html).toContain('Связи или исходные документы изменились');
  });
});
