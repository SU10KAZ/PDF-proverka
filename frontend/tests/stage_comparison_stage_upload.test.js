import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');

describe('documentation comparison shell', () => {
  it('keeps the four shell tabs and the later-stage empty state', () => {
    expect(html).toContain('1. Загрузка документации');
    expect(html).toContain('2. Связь блоков');
    expect(html).toContain('3. Расхождения');
    expect(html).toContain('4. Отчёт');
    expect(html).toContain('Сравнение ещё не выполнено');
  });

  it('does not expose the source-card heading or internal session id', () => {
    expect(html).not.toContain('Исходные документы');
    expect(html).not.toContain('Выберите PDF с каждой стадии');
    expect(html).not.toContain('{{ scSession.id }}');
  });

  it('renders only the vector PDF endpoint', () => {
    expect(app).toContain('/page-svg?side=${side}&page=${scCurrentPage[side]}');
    expect(html).toContain('<img :src="scPageSrcUrl(side)"');
    expect(html).not.toContain('page-image');
    expect(app).not.toContain('page-image');
    expect(app).not.toContain('block-image');
  });

  it('runs only the new Markdown sheet matcher', () => {
    expect(html).toContain('@click="scProcessCurrentSelection()"');
    expect(html).toContain('@click="scProcessCheckedPairs()"');
    expect(app).toContain("'/sheet-match-suggestions'");
    expect(app).toContain("'/sheet-links'");
    expect(app).toContain("method: 'PUT'");
  });

  it('supports manual many-to-many sheet correction in the viewer', () => {
    expect(html).toContain('+ Добавить лист РД');
    expect(html).toContain('Удалить связь');
    expect(html).toContain('Не сопоставлены П');
    expect(app).toContain('right_pages: rightPages');
    expect(app).toContain('left_pages: [leftPage]');
    expect(app).toContain('user_corrected');
  });

  it('does not call removed analytical APIs', () => {
    const forbidden = [
      'prepared-comparison', 'page-image', 'block-image', 'graphic-diff',
      'semantic-diff', 'change-regions', 'change-groups', 'pipeline-v2',
      'unified-analysis', 'text-llm', 'comparison-statuses',
    ];
    for (const token of forbidden) expect(app).not.toContain(token);
    expect(app.match(/\/api\/stage-comparison/g)?.length).toBeGreaterThanOrEqual(6);
  });
});
