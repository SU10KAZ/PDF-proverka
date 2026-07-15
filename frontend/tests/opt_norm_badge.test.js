import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const frontendRoot = path.resolve(__dirname, '..');
const appJs = fs.readFileSync(path.join(frontendRoot, 'static/js/app.js'), 'utf8');
const html = fs.readFileSync(path.join(frontendRoot, 'index.html'), 'utf8');
const css = fs.readFileSync(path.join(frontendRoot, 'static/css/styles.css'), 'utf8');

// app.js — не модуль (один большой Vue setup), импортировать нельзя.
// Вырезаем функцию с её словарём и исполняем: так тест проверяет ПОВЕДЕНИЕ,
// а не совпадение текста.
function loadOptNormBadge() {
  const dict = appJs.match(/const OPT_NORM_OUTCOME_LABEL = \{[\s\S]*?\n {8}\};/);
  const fn = appJs.match(/function optNormBadge\(item\) \{[\s\S]*?\n {8}\}/);
  if (!dict || !fn) throw new Error('optNormBadge не найдена в app.js — тест устарел');
  // eslint-disable-next-line no-new-func
  return new Function(`${dict[0]}\n${fn[0]}\nreturn optNormBadge;`)();
}

describe('статус нормы на карточке предложения', () => {
  const optNormBadge = loadOptNormBadge();

  it('молчит, пока этап 04 не проверил предложение', () => {
    // Отсутствие проверки и провал проверки — разные вещи. Пока полей нет,
    // рисовать «не проверена» нельзя: это оболгало бы старые прогоны.
    expect(optNormBadge({ id: 'OPT-001', norm: 'СП 30.13330.2020' })).toBeNull();
    expect(optNormBadge(null)).toBeNull();
    expect(optNormBadge({ norm_status: 'ok' })).toBeNull();  // без norm_verified
  });

  it('показывает спокойный знак, когда норма подтвердилась', () => {
    const b = optNormBadge({ norm_verified: true, norm_status: 'ok' });
    expect(b.tone).toBe('ok');
    expect(b.text).toContain('проверена');
  });

  it('still_valid: ссылку обновили, предложение в силе', () => {
    const b = optNormBadge({
      norm_verified: true, norm_status: 'revised', norm_outcome: 'still_valid',
      norm_revision: { original_norm: 'СП 30.13330.2016', revision_reason: 'редакция обновлена' },
    });
    expect(b.tone).toBe('revised');
    expect(b.text).toContain('в силе');
    expect(b.title).toContain('СП 30.13330.2016');   // видно, что было
    expect(b.title).toContain('редакция обновлена');
  });

  it('obsolete подсвечивается как предупреждение, а не как штатный пересмотр', () => {
    const b = optNormBadge({
      norm_verified: true, norm_status: 'revised', norm_outcome: 'obsolete',
      norm_revision: { revision_reason: 'новая норма делает замену обязательной' },
    });
    expect(b.tone).toBe('warn');
    expect(b.text).toContain('обесценила');
  });

  it('warning: замена не найдена — причина видна', () => {
    const b = optNormBadge({
      norm_verified: true, norm_status: 'warning',
      norm_revision: { revision_reason: 'норма отменена без замены' },
    });
    expect(b.tone).toBe('warn');
    expect(b.title).toBe('норма отменена без замены');
  });

  it('неизвестный исход не роняет карточку', () => {
    const b = optNormBadge({ norm_verified: true, norm_status: 'revised', norm_outcome: 'что-то новое' });
    expect(b.tone).toBe('revised');
    expect(b.text).toContain('норма пересмотрена');
    expect(optNormBadge({ norm_verified: true, norm_status: 'абракадабра' })).toBeNull();
  });

  it('бейдж подключён к ячейке нормы и стилизован', () => {
    expect(html).toContain('optNormBadge(item)');
    expect(html).toContain("'opt-norm-badge--' + optNormBadge(item).tone");
    expect(appJs).toContain('optNormBadge, loadOptimization');   // экспорт в setup
    expect(css).toContain('.opt-norm-badge--warn');
  });
});
