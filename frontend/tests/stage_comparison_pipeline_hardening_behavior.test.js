import { createRequire } from 'node:module';
import { describe, expect, it } from 'vitest';

const require = createRequire(import.meta.url);
const review = require('../static/js/stage-comparison-review.js');
const NOW = Date.parse('2026-08-28T09:10:00.000Z');

function pair() {
  return {
    id: 'pair-1',
    left: {filename: 'LEFT.pdf'},
    right: {filename: 'RIGHT.pdf'},
  };
}

function presentation(state) {
  return {
    active_pair: pair(),
    selected_mode: 'DOCUMENT',
    selected_pages: {left: [1], right: [1]},
    now_ms: NOW,
    state,
  };
}

describe('Stage Comparison pipeline hardening behavior', () => {
  it('A: keeps the primary CTA disabled while a real runner owns the pair lock', () => {
    const overview = review.normalizeProductionOverview(presentation({
      status: 'RUNNING', runner_active: true, orphaned_run: false,
      run_recoverable: false, run_id: 'run-active', stages: {},
    }));

    expect(review.productionRunActivity({
      status: 'RUNNING', runner_active: true, orphaned_run: false,
    })).toMatchObject({active: true, is_orphaned: false});
    expect(overview).toMatchObject({
      state: 'RUNNING',
      cta: {kind: 'RUNNING', disabled: true},
    });
  });

  it('B: makes an orphaned persisted RUNNING recoverable', () => {
    const overview = review.normalizeProductionOverview(presentation({
      status: 'RUNNING', runner_active: false, orphaned_run: true,
      run_recoverable: true, run_id: 'run-old', stages: {},
    }));

    expect(overview).toMatchObject({
      state: 'INTERRUPTED',
      headline: '⚠ Предыдущий анализ был прерван',
      cta: {kind: 'RECOVER', label: '↻ Повторить анализ', disabled: false},
      run_activity: {active: false, is_orphaned: true, run_recoverable: true},
    });
    expect(overview.detail_lines.join(' ')).toContain('активного процесса больше нет');
  });

  it('C: rejects a late response from the interrupted generation', () => {
    const pending = {
      pairId: 'pair-1', previousRunId: 'run-old', acceptedRunId: 'run-new',
    };

    expect(review.productionStateResponseAccepted(
      {status: 'RUNNING', run_id: 'run-new'}, pending,
    )).toBe(true);
    expect(review.productionStateResponseAccepted(
      {status: 'RUNNING', run_id: 'run-old'}, pending,
    )).toBe(false);
    expect(review.productionStateResponseAccepted(
      {status: 'COMPLETED', run_id: 'run-other'}, pending,
    )).toBe(false);
  });

  it('D: stops active polling as soon as an orphan is detected', () => {
    expect(review.productionPollingDirective({
      status: 'RUNNING', runner_active: false,
      orphaned_run: true, run_recoverable: true,
    })).toBe('STOP_ORPHANED');

    expect(review.productionPollingTransition(
      {polling: true, token: 4, pair_id: 'pair-1'},
      {type: 'STATE_RECEIVED', state: {
        status: 'RUNNING', runner_active: false,
        orphaned_run: true, run_recoverable: true,
      }},
    )).toEqual({
      polling: false, token: 5, pair_id: 'pair-1', directive: 'STOP_ORPHANED',
    });
  });

  it('E: invalidates polling when the selected pair changes', () => {
    expect(review.productionPollingTransition(
      {polling: true, token: 7, pair_id: 'pair-1'},
      {type: 'PAIR_CHANGED', pair_id: 'pair-2'},
    )).toEqual({polling: false, token: 8, pair_id: 'pair-2'});
  });

  it('F: replaces terminal activity age with completion age and duration', () => {
    const progress = review.normalizePipelineProgress({
      status: 'COMPLETED',
      started_at: '2026-08-28T09:05:00.000Z',
      completed_at: '2026-08-28T09:07:00.000Z',
      last_activity_at: '2026-08-28T09:07:00.000Z',
    }, {now_ms: NOW});

    expect(progress).toMatchObject({
      is_terminal: true,
      last_activity_label: '',
      completed_age_label: '3 мин назад',
      elapsed_label: '2 мин 0 сек',
    });
  });

  it('G: preserves PARTIAL in the headline while human work is pending', () => {
    const overview = review.normalizeProductionOverview(presentation({
      status: 'COMPLETED', run_id: 'run-partial',
      stages: {
        text: {status: 'COMPLETED'},
        graphic: {status: 'CHECK_BLOCKED'},
        review_questions: {status: 'NEEDS_REVIEW', pending: 15, total: 15},
        unified_synthesis: {status: 'COMPLETED', review_items: 332},
        engineer_decisions: {
          status: 'READY',
          counts: {total: 332, PENDING_REVIEW: 332},
        },
      },
    }));

    expect(overview).toMatchObject({
      state: 'NEEDS_REVIEW',
      headline: 'Автоматический анализ завершён частично.',
      human: {questions_pending: 15, findings_pending: 332},
      cta: {kind: 'CONTINUE_REVIEW'},
    });
  });

  it('H: separates processed GRAPHIC groups from their result outcomes', () => {
    const content = review.normalizeProductionPipeline(presentation({
      status: 'PARTIAL', run_id: 'run-graphic',
      stages: {
        text: {status: 'COMPLETED'},
        graphic: {
          status: 'CHECK_BLOCKED',
          groups_total: 4,
          groups_completed: 0,
          groups_review_required: 3,
          groups_blocked: 1,
          changes: 0,
          progress: {processed: 4, total: 4, unit: 'groups'},
        },
      },
    }))[2];
    const graphic = content.sections.find(section => section.id === 'graphic');

    expect(content.status).toBe('PARTIAL');
    expect(graphic.progress).toMatchObject({
      processed: 4, total: 4, percent: 100,
      counter_label: 'Обработано: 4 / 4 групп',
    });
    expect(graphic.result_counters).toEqual([
      {label: 'Готово', value: 0},
      {label: 'Требует проверки', value: 3},
      {label: 'Заблокировано', value: 1},
      {label: 'Изменения', value: 0},
    ]);

    const opaqueGraphic = review.normalizeProductionPipeline({
      ...presentation({
        status: 'PARTIAL', run_id: 'run-page', input_mode: 'PAGE',
        stages: {graphic: {status: 'CHECK_BLOCKED', mode: 'MODE_2', changes: 4}},
      }),
      selected_mode: 'PAGE',
    })[2].sections.find(section => section.id === 'graphic');
    expect(opaqueGraphic).toMatchObject({
      mini_counter: '4 изм.',
      summary: 'Графическое сравнение завершено · изменений: 4',
      progress: {kind: 'none', determinate: false},
    });
  });

  it('explains audited reason codes in Russian while preserving diagnostics separately', () => {
    expect(review.humanizeReasonCode('document_graphic_groups_require_attention'))
      .toContain('требуют внимания инженера');
    expect(review.humanizeReasonCode('ProductionStateConflictError'))
      .toContain('изменилось параллельно');
    expect(review.humanizeReasonCode('OSError'))
      .toContain('не смогла прочитать или записать');
    const extraction = review.humanizeReasonCode('EXTRACTION_COMPLETENESS_INSUFFICIENT');
    expect(extraction).toContain('визуальная резервная');
    expect(extraction).not.toContain('visual fallback');
  });
});
