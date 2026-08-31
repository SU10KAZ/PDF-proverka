import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, '..');
const html = fs.readFileSync(path.join(root, 'index.html'), 'utf8');
const js = fs.readFileSync(path.join(root, 'static/js/app.js'), 'utf8');
const css = fs.readFileSync(path.join(root, 'static/css/styles.css'), 'utf8');

describe('поштучная нормативная верификация замечаний', () => {
  it('рендерит каждую norm_references запись отдельно с пунктом и цитатой', () => {
    expect(html).toContain('v-for="(ref, ri) in findingNormReferences(f)"');
    expect(html).toContain('normReferenceDesignation(ref)');
    expect(html).toContain('normReferenceQuote(ref)');
    expect(js).toContain("VERIFIED: { text: '✓ пункт подтверждён'");
    expect(css).toContain('.norm-reference-item__quote');
  });

  it('показывает редакцию, special policy и критическое предупреждение', () => {
    expect(html).toContain('normReferenceEdition(ref)');
    expect(html).toContain('ref.special_policy.message');
    expect(html).toContain('f.critical_norm_notice');
    expect(js).toContain("edition_applicability === 'historical_applicable'");
    expect(css).toContain('.critical-norm-notice');
  });

  it('не объединяет статусы нескольких ссылок в фиктивную общую цитату', () => {
    expect(js).toContain('Статус каждой ссылки показан отдельно');
    expect(js).toContain('ref.resolution_status === \'VERIFIED\'');
    expect(html).toContain('class="norm-reference-list"');
  });
});
