import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');

describe('documentation comparison shell', () => {
  it('keeps the four shell tabs and the empty comparison state', () => {
    expect(html).toContain('1. Загрузка документации');
    expect(html).toContain('2. Связь блоков');
    expect(html).toContain('3. Расхождения');
    expect(html).toContain('4. Отчёт');
    expect(html).toContain('Сравнение ещё не выполнено');
  });

  it('renders only the vector PDF endpoint', () => {
    expect(app).toContain('/page-svg?side=${side}&page=${scCurrentPage[side]}');
    expect(html).toContain('<img :src="scPageSrcUrl(side)"');
    expect(html).not.toContain('page-image');
    expect(app).not.toContain('page-image');
    expect(app).not.toContain('block-image');
  });

  it('keeps processing controls as disabled no-op placeholders', () => {
    expect(html).toMatch(/<button[^>]*disabled[^>]*>Обработать<\/button>/);
    expect(html).toMatch(/<button[^>]*disabled[^>]*>Обработать выбранные<\/button>/);
    expect(html).not.toMatch(/@click="[^"]*Обработ/);
  });

  it('does not call removed analytical APIs', () => {
    const forbidden = [
      'prepared-comparison', 'page-image', 'block-image', 'graphic-diff',
      'semantic-diff', 'change-regions', 'change-groups', 'pipeline-v2',
      'unified-analysis', 'text-llm', 'comparison-statuses',
    ];
    for (const token of forbidden) expect(app).not.toContain(token);
    expect(app.match(/\/api\/stage-comparison/g)?.length).toBe(6);
  });
});
