import {readFileSync} from 'node:fs';
import {describe, expect, it} from 'vitest';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');

describe('копирование результата отдельного этапа', () => {
  it('показывает кнопку в раскрытой карточке каждого этапа', () => {
    expect(html).toContain('v-for="stage in scProductionPipeline"');
    expect(html).toContain('@click="scCopyProductionStageResult(stage)"');
    expect(html).toContain("'Скопировать результат'");
    expect(html).toContain("['NOT_STARTED', 'RUNNING'].includes(stage.status)");
  });

  it('запрашивает диагностический JSON выбранного этапа и копирует весь payload', () => {
    const start = app.indexOf('async function scCopyProductionStageResult(stage)');
    const end = app.indexOf('async function scScrollToProductionElement', start);
    const copy = app.slice(start, end);

    expect(copy).toContain('`/stages/${encodeURIComponent(stageId)}/result`');
    expect(copy).toContain('JSON.stringify(payload, null, 2)');
    expect(copy).toContain('navigator.clipboard.writeText(text)');
    expect(copy).toContain('fallbackCopy(text)');
    expect(copy).not.toContain('scRunProductionComparison');
    expect(copy).not.toContain("method: 'POST'");
    expect(copy).not.toContain("method: 'PUT'");
  });
});
