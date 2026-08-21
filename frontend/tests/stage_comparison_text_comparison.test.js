import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const css = readFileSync(new URL('../static/css/styles.css', import.meta.url), 'utf8');

describe('deterministic stage comparison text overlay', () => {
  it('starts and restores a persisted text comparison independently of sheet matching', () => {
    expect(app).toContain('const scTextComparison = ref(null)');
    expect(app).toContain("scTextComparison.value = data.text_comparison || null");
    expect(app).toContain("scPairUrl(scActivePair.value.id, '/text-comparison')");
    expect(html).toContain('Сравнить текст');
    expect(html).toContain('Осталось для проверки');
  });

  it('draws grey overlays in both paged and continuous viewer modes', () => {
    expect(html.match(/class="sc-text-comparison-mask"/g)).toHaveLength(2);
    expect(html).toContain('scTextComparisonOverlaysFor(side, scCurrentPage[side])');
    expect(html).toContain('scTextComparisonOverlaysFor(side, entry.page)');
    expect(css).toContain('background: rgba(100, 116, 139, .54);');
    expect(css).toContain('.sc-text-comparison-mask.is-elsewhere::after');
  });

  it('marks stale overlays and exposes only explicit human link actions', () => {
    expect(app).toContain("scTextComparison.value = {...scTextComparison.value, stale: true}");
    expect(html).toContain('карта листов изменилась — нужен пересчёт');
    expect(html).toContain('Добавить связь');
    expect(html).toContain('Заменить связь');
    expect(app).toContain('async function scApplyTextHint(hint, replace = false)');
  });
});
