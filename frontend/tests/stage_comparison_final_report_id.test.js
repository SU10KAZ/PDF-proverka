import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const require = createRequire(import.meta.url);
const review = require('../static/js/stage-comparison-review.js');
const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');

describe('Итоговый отчёт называет изменения по-человечески', () => {
  const report = {
    approved_atomic_changes: [
      {change_id: 'uchg_9f3a1c', object_ref: 'ВРУ-1', before_value: 'A', after_value: 'B'},
      {change_id: 'uchg_2b7e40', object_ref: 'ВРУ-2', before_value: 'C', after_value: 'D'},
    ],
    approved_review_findings: [
      {review_evidence_id: 'ureview_5d0c', left_pages: [1], right_pages: [1]},
    ],
  };

  it('строки получают порядковый номер, а внутренний ID остаётся отдельно', () => {
    const rows = review.normalizeFinalRows(report);

    expect(rows.map(row => row.display_id)).toEqual(['1', '2', '3']);
    expect(rows.map(row => row.ordinal)).toEqual([1, 2, 3]);
    expect(rows.map(row => row.target_id))
      .toEqual(['uchg_9f3a1c', 'uchg_2b7e40', 'ureview_5d0c']);
  });

  it('видимая колонка ID печатает номер, а не uchg_/ureview_', () => {
    const table = html.match(
      /<table class="findings-table sc-production-final-table">[\s\S]*?<\/table>/,
    );

    expect(table).not.toBeNull();
    const cell = table[0].match(/<td class="id-cell">[\s\S]*?<\/td>/);
    expect(cell).not.toBeNull();
    expect(cell[0]).toContain('row.display_id');
    // Внутренний идентификатор остаётся доступен, но не как главный текст.
    expect(cell[0]).not.toMatch(/finding-id-link">\{\{ row\.target_id/);
    expect(cell[0]).toContain('Диагностика');
    expect(cell[0]).toContain('row.target_id');
  });
});
