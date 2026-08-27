import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
const js = fs.readFileSync(path.join(root, 'static/js/app.js'), 'utf8');
const css = fs.readFileSync(path.join(root, 'static/css/styles.css'), 'utf8');

describe('пилюли карточки при нулевой категории', () => {
  it('обе пилюли рендерятся всегда, пока у проекта есть результаты аудита', () => {
    // блок статистики по-прежнему скрыт у проектов без аудита
    expect(html).toContain('v-if="p.findings_count > 0 || p.optimization_count > 0" class="project-card__stats"');
    // а сами пилюли больше не прячутся поодиночке
    expect(html).not.toContain('<button v-if="p.findings_count > 0"');
    expect(html).not.toContain('<button v-if="p.optimization_count > 0"');
    expect(html).toContain('{{ p.optimization_count || 0 }}');
    expect(html).toContain('{{ p.findings_count || 0 }}');
  });

  it('нулевая пилюля приглушена и помечена прочерком вместо пустой клетки', () => {
    expect(html).toContain("'project-card__stat--empty': !(p.optimization_count > 0)");
    expect(html).toContain("'project-card__stat--empty': !(p.findings_count > 0)");
    expect(html).toContain("'project-card__stat-mark--none': !(p.optimization_count > 0)");
    expect(html).toContain('<template v-if="!(p.optimization_count > 0)">&mdash;</template>');
    expect(css).toContain('.project-card__stat--empty { opacity: .5; }');
    expect(css).toContain('.project-card__stat-mark--none { border-color: transparent;');
  });

  it('подсказка нулевой пилюли объясняет, что отрабатывать нечего', () => {
    expect(html).toContain("'Предложений по оптимизации не найдено — отрабатывать нечего'");
    expect(html).toContain("'Замечаний не найдено — отрабатывать нечего'");
    expect(js).toContain('Предложений по оптимизации не найдено');
    expect(js).toContain('Замечаний не найдено');
  });
});
