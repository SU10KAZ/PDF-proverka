import {readFileSync} from 'node:fs';
import {describe, expect, it} from 'vitest';

const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const css = readFileSync(new URL('../static/css/styles.css', import.meta.url), 'utf8');

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

    expect(copy).toContain('scProductionState.value');
    expect(copy).toContain('scProductionState.value.run_id');
    expect(copy).toContain('const pairContext = scActivePairRequestContext()');
    expect(copy).toContain('`/stages/${encodeURIComponent(stageId)}/result?run_id=${encodeURIComponent(runId)}`');
    expect(copy).toContain('{context: pairContext}');
    expect(copy).toContain("String(payload.run_id || '') !== runId");
    expect(copy).toContain("String(payload.pair_id || '') !== pairContext.pairId");
    expect(copy).toContain("String(payload.stage && payload.stage.id || '') !== stageId");
    expect(copy).toContain('JSON.stringify(payload, null, 2)');
    expect(copy).toContain('navigator.clipboard.writeText(text)');
    expect(copy).toContain('fallbackCopy(text)');
    expect(copy).not.toContain('scRunProductionComparison');
    expect(copy).not.toContain("method: 'POST'");
    expect(copy).not.toContain("method: 'PUT'");
  });

  it('не показывает selection и перенумеровывает семь видимых этапов', () => {
    expect(html).toContain('aria-label="7 этапов сравнения"');
    expect(app).toContain(".filter(stage => stage.id !== 'selection')");
    expect(app).toContain('.map((stage, index) => ({...stage, number: index + 1}))');
    expect(app).toContain('scProductionPipelineAll.value.some(stage => (');
    expect(css).toContain('grid-template-columns: repeat(7, minmax(142px, 1fr))');
  });
});
