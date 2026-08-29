import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const require = createRequire(import.meta.url);
const review = require('../static/js/stage-comparison-review.js');
const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');

const NOW = Date.parse('2026-08-29T09:10:00.000Z');

function pair() {
  return {
    id: 'pair-1',
    left: {filename: 'LEFT.pdf'},
    right: {filename: 'RIGHT.pdf'},
  };
}

// Прогон, у которого всё остальное действительно завершено: иначе «частично»
// на верхней сводке ничего не доказывает — его дал бы любой другой этап.
function completedStages(aiStage) {
  return {
    sheet_matching: {status: 'COMPLETED', relations: 1},
    sheet_scope: {status: 'COMPLETED', groups: 1},
    text: {status: 'COMPLETED', deltas: 1},
    graphic: {status: 'COMPLETED', changes: 1},
    entity_matching: {status: 'COMPLETED', relations: 1},
    entity_binding: {status: 'COMPLETED', bound_atoms: 1},
    automatic_unified_synthesis: {status: 'COMPLETED', changes: 1, review_items: 0},
    review_application: {status: 'COMPLETED', applied_decisions: 0},
    unified_synthesis: {status: 'COMPLETED', changes: 1, review_items: 0},
    review_questions: {status: 'COMPLETED', pending: 0, answered: 0, total: 0, counts: {}},
    final_report: {status: 'COMPLETED', approved: 0},
    ...(aiStage ? {ai_resolution: aiStage} : {}),
  };
}

function payload(aiStage, {status = 'COMPLETED'} = {}) {
  return {
    active_pair: pair(),
    selected_mode: 'DOCUMENT',
    now_ms: NOW,
    state: {
      run_id: 'run-1',
      status,
      started_at: '2026-08-29T09:00:00.000Z',
      stages: completedStages(aiStage),
    },
  };
}

function stageById(stages, id) {
  return stages.find(stage => stage.id === id) || {};
}

describe('Статус ИИ-слоя доезжает до шага и до верхней сводки', () => {
  it('неполный ИИ-анализ опускает «Синтез изменений» до «Частично»', () => {
    const stages = review.normalizeProductionPipeline(payload({
      status: 'PARTIAL', mode: 'DEEP', run_mode: 'DEEP',
      runtime_ready: false, total: 4, processed: 0, human_required: 4,
    }, {status: 'PARTIAL'}));

    expect(stageById(stages, 'synthesis').status).toBe('PARTIAL');
    expect(review.pipelineStatusLabel(stageById(stages, 'synthesis').status))
      .toBe('Частично');
  });

  it('верхняя сводка не говорит «полностью завершён» при неполном ИИ-анализе', () => {
    const data = payload({
      status: 'PARTIAL', mode: 'STANDARD', run_mode: 'STANDARD',
      runtime_ready: false, runtime_problems: ['CLI не найден'],
      total: 4, processed: 0, human_required: 4,
    }, {status: 'PARTIAL'});
    const overview = review.normalizeProductionOverview(data);

    expect(overview.state).toBe('PARTIAL');
    expect(overview.headline).toBe('Автоматический анализ завершён частично.');
  });

  it('режим «Быстро» ничего не обещал и ничего не опускает', () => {
    const data = payload({status: 'NOT_APPLICABLE', mode: 'OFF', run_mode: 'FAST'});
    const stages = review.normalizeProductionPipeline(data);

    expect(stageById(stages, 'synthesis').status).toBe('COMPLETED');
    expect(review.normalizeProductionOverview(data).headline)
      .toBe('Анализ полностью завершён.');
  });

  it('прогон без ИИ-слоя вообще (старое состояние) читается как раньше', () => {
    const data = payload(null);

    expect(stageById(review.normalizeProductionPipeline(data), 'synthesis').status)
      .toBe('COMPLETED');
    expect(review.normalizeProductionOverview(data).headline)
      .toBe('Анализ полностью завершён.');
  });

  it('упавший ИИ-слой не показывается как «Готово»', () => {
    const aiStage = {
      status: 'PARTIAL', mode: 'DEEP', run_mode: 'DEEP', layer_error: 'RuntimeError',
      total: 4, processed: 4, human_required: 4, runtime_ready: true,
    };
    const progress = review.normalizeAiProgress(payload(aiStage, {status: 'PARTIAL'}));

    expect(progress.status).toBe('PARTIAL');
    expect(progress.status_label).toBe('Частично');
    expect(progress.status_label).not.toBe('Готово');
  });

  it('несостоявшийся критик виден инженером как «Частично»', () => {
    const progress = review.normalizeAiProgress(payload({
      status: 'PARTIAL', mode: 'DEEP', run_mode: 'DEEP', runtime_ready: true,
      critic_required: 4, critic_unavailable: 4, mode_completeness: 'PARTIAL',
      total: 4, processed: 4, human_required: 4,
      human_reasons: {CRITIC_UNAVAILABLE: 4},
    }, {status: 'PARTIAL'}));

    expect(progress.status).toBe('PARTIAL');
    expect(progress.reasons.map(reason => reason.code)).toContain('CRITIC_UNAVAILABLE');
  });
});

describe('Остановленный прогон', () => {
  it('не читается как «не начато» и не выдаётся за завершённый', () => {
    expect(review.normalizePipelineStatus('CANCELLED')).toBe('CANCELLED');
    expect(review.pipelineStatusLabel('CANCELLED')).toBe('Остановлено');
  });

  it('верхняя сводка честно сообщает об остановке', () => {
    const overview = review.normalizeProductionOverview(
      payload({status: 'CANCELLED', mode: 'DEEP', run_mode: 'DEEP'},
        {status: 'CANCELLED'}),
    );

    expect(overview.state).toBe('CANCELLED');
    expect(overview.headline).toBe('Анализ был остановлен.');
    expect(overview.cta.kind).toBe('RERUN');
  });
});
