import { describe, it, expect } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const APP_JS = fs.readFileSync(path.join(__dirname, '../static/js/app.js'), 'utf8');
const INDEX_HTML = fs.readFileSync(path.join(__dirname, '../index.html'), 'utf8');

describe('Pipeline V2 — основной экран', () => {
  it('оставляет основной запуск и просмотр результата', () => {
    expect(INDEX_HTML).toContain('Pipeline V2');
    expect(APP_JS).toContain('scPv2Run');
    expect(APP_JS).toContain('/api/stage-comparison/pipeline-v2/${encodeURIComponent(scSession.value.id)}/pairs/${encodeURIComponent(pid)}/run');
  });

  it('не содержит удалённых вспомогательных панелей', () => {
    expect(INDEX_HTML).not.toContain('Pipeline V2 связи');
    expect(INDEX_HTML).not.toContain('Pipeline V2 сущности');
    expect(APP_JS).not.toContain('scPv2LpToggle');
    expect(APP_JS).not.toContain('scPv2EaToggle');
    expect(APP_JS).not.toContain('/block-link-preview?pair_id=');
    expect(APP_JS).not.toContain('/entity-alignment-preview?pair_id=');
  });
});
