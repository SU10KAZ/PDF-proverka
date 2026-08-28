import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const require = createRequire(import.meta.url);
const review = require('../static/js/stage-comparison-review.js');
const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');

const NOW = Date.parse('2026-08-28T09:10:00.000Z');

function pair() {
  return {
    id: 'pair-1',
    left: {filename: 'LEFT.pdf'},
    right: {filename: 'RIGHT.pdf'},
  };
}

function pipeline(payload = {}) {
  return review.normalizeProductionPipeline({
    active_pair: pair(),
    selected_mode: 'DOCUMENT',
    selected_pages: {left: [2], right: [4]},
    now_ms: NOW,
    ...payload,
  });
}

describe('Stage Comparison pipeline state semantics', () => {
  it('A: completes Stage 1 from the active UI pair before the first production run', () => {
    const stages = pipeline({state: {status: 'NOT_STARTED', stages: {}}});

    expect(stages[0]).toMatchObject({
      id: 'selection',
      status: 'COMPLETED',
      counters: [],
      details: ['LEFT: LEFT.pdf', 'RIGHT: RIGHT.pdf', 'Режим: DOCUMENT ↔ DOCUMENT'],
      selection: {mode: 'DOCUMENT', mode_label: 'DOCUMENT ↔ DOCUMENT'},
    });

    const page = pipeline({selected_mode: 'PAGE'});
    expect(page[0].details).toEqual([
      'LEFT: LEFT.pdf · стр. 2',
      'RIGHT: RIGHT.pdf · стр. 4',
      'Режим: PAGE ↔ PAGE',
    ]);
    expect(page[0].counters).toEqual([
      {label: 'LEFT листы', value: 1},
      {label: 'RIGHT листы', value: 1},
    ]);
  });

  it('uses the persisted selection after a run even without transient UI pair metadata', () => {
    const stages = review.normalizeProductionPipeline({
      state: {
        status: 'COMPLETED',
        selection: {input_mode: 'PAGE', left_pages: [8], right_pages: [13]},
        stages: {},
      },
    });

    expect(stages[0]).toMatchObject({status: 'COMPLETED'});
    expect(stages[0].details).toContain('Режим: PAGE ↔ PAGE');
    expect(stages[0].details[0]).toContain('стр. 8');
    expect(stages[0].details[1]).toContain('стр. 13');
  });

  it('requires a real pair and one page per side for a future PAGE run', () => {
    const emptyPair = review.normalizeProductionPipeline({
      active_pair: {left: {}, right: {}}, selected_mode: 'DOCUMENT',
      state: {status: 'NOT_STARTED', stages: {}},
    });
    expect(emptyPair[0].status).toBe('NOT_STARTED');

    const missingRight = pipeline({selected_mode: 'PAGE', selected_pages: {left: [2], right: []}});
    expect(missingRight[0].status).toBe('NOT_STARTED');
    expect(review.normalizeProductionOverview({
      active_pair: pair(), selected_mode: 'PAGE', selected_pages: {left: [2], right: []},
      state: {status: 'NOT_STARTED', stages: {}},
    }).cta.disabled).toBe(true);

    const switched = pipeline({
      selected_mode: 'PAGE', selected_pages: {left: [9], right: [10]},
      state: {
        status: 'COMPLETED', input_mode: 'DOCUMENT',
        selection: {input_mode: 'DOCUMENT', left_pages: [], right_pages: []}, stages: {},
      },
    });
    expect(switched[0].selection).toMatchObject({
      mode: 'PAGE', mode_label: 'PAGE ↔ PAGE',
      left: {pages: [9]}, right: {pages: [10]},
    });
  });

  it('G: never completes Stage 7 while findings are pending or before findings exist', () => {
    const pending = pipeline({state: {status: 'COMPLETED', stages: {
      engineer_decisions: {
        status: 'READY',
        counts: {total: 3, APPROVED: 1, REJECTED: 1, PENDING_REVIEW: 1},
      },
    }}})[6];
    expect(pending.status).toBe('NEEDS_REVIEW');
    expect(pending.counters).toEqual([
      {label: 'Найдено', value: 3},
      {label: 'Подтверждено', value: 1},
      {label: 'Отклонено', value: 1},
      {label: 'Ожидает решения', value: 1},
    ]);

    const noFindings = pipeline({state: {status: 'COMPLETED', stages: {
      engineer_decisions: {status: 'READY', counts: {total: 0}},
    }}})[6];
    expect(noFindings).toMatchObject({status: 'NOT_STARTED', counters: []});

    const decided = pipeline({state: {status: 'COMPLETED', stages: {
      engineer_decisions: {
        status: 'READY', counts: {total: 2, APPROVED: 1, REJECTED: 1},
      },
    }}})[6];
    expect(decided.status).toBe('COMPLETED');

    const persistedProjection = pipeline({state: {status: 'COMPLETED', stages: {
      unified_synthesis: {status: 'COMPLETED', review_items: 12},
      engineer_decisions: {
        status: 'READY', counts: {APPROVED: 48, REJECTED: 12, PENDING_REVIEW: 272},
      },
    }}})[6];
    expect(persistedProjection.counters).toEqual([
      {label: 'Найдено', value: 332},
      {label: 'Подтверждено', value: 48},
      {label: 'Отклонено', value: 12},
      {label: 'Ожидает решения', value: 272},
    ]);
    expect(persistedProjection.progress).toMatchObject({processed: 60, total: 332});
  });

  it('keeps resolved Stage 5 questions in the answered/total progress', () => {
    const questions = pipeline({
      state: {status: 'COMPLETED', stages: {
        review_questions: {
          status: 'NEEDS_REVIEW', questions: 11, pending: 11, answered: 4, total: 15,
        },
      }},
      questions: {
        questions: Array.from({length: 11}, (_, index) => ({
          question_id: `pending-${index}`, category: 'CHANGE', status: 'PENDING',
        })),
        counts: {total: 11, pending: 11, resolved_unchanged: 4, CHANGE: 11},
      },
    })[4];

    expect(questions.status).toBe('NEEDS_REVIEW');
    expect(questions.pending).toBe(11);
    expect(questions.progress).toMatchObject({answered: 4, processed: 4, total: 15});
    expect(questions.counters).toContainEqual({label: 'Ответы', value: '4 / 15'});
  });

  it('H: gates Stage 8 on synthesis and unfinished human review', () => {
    const beforeSynthesis = pipeline({state: {status: 'NOT_STARTED', stages: {}}})[7];
    expect(beforeSynthesis.status).toBe('NOT_STARTED');

    const waiting = pipeline({state: {status: 'COMPLETED', stages: {
      unified_synthesis: {status: 'COMPLETED', changes: 3, review_items: 2},
      engineer_decisions: {
        status: 'READY', counts: {total: 2, APPROVED: 1, PENDING_REVIEW: 1},
      },
      final_report: {status: 'READY', approved: 1},
    }}, final_report: {summary: {approved: 1}}})[7];
    expect(waiting.status).toBe('NEEDS_REVIEW');
    expect(waiting.counters).toContainEqual({label: 'Ещё не проверено', value: 1});

    const available = pipeline({state: {status: 'COMPLETED', stages: {
      unified_synthesis: {status: 'COMPLETED', changes: 2, review_items: 2},
      engineer_decisions: {
        status: 'READY', counts: {total: 2, APPROVED: 1, REJECTED: 1, PENDING_REVIEW: 0},
      },
      final_report: {status: 'READY', approved: 1},
    }}})[7];
    expect(available.status).toBe('COMPLETED');
  });

  it('uses live Stage 5/8 statuses and ignores previous-generation artifacts while running', () => {
    const stages = pipeline({
      state: {status: 'RUNNING', stages: {
        review_questions: {status: 'RUNNING', progress: {status: 'RUNNING'}},
        final_report: {status: 'RUNNING', progress: {status: 'RUNNING'}},
      }},
      questions: {questions: [{question_id: 'old', category: 'CHANGE'}]},
      changes: {rows: [
        {target_id: 'old', decision: 'PENDING_REVIEW'},
      ]},
      final_report: {summary: {approved: 4}},
    });
    expect(stages[4].status).toBe('RUNNING');
    expect(stages[6]).toMatchObject({status: 'NOT_STARTED', counters: []});
    expect(stages[7].status).toBe('RUNNING');
  });

  it('J/K/L: exposes contextual actions and only an honest full-rerun fallback', () => {
    const stages = pipeline();
    expect(stages[4].action.label).toBe('Ответить на вопросы');
    expect(stages[6].action.label).toBe('Проверить изменения');
    expect(stages[7].action.label).toBe('Открыть итоговый отчёт');
    expect(stages[1].action.label).toBe('Открыть сопоставление листов');
    expect(stages[2].action.label).toBe('Открыть анализ');
    expect(stages.map(stage => stage.action_label)).not.toContain('Перейти к этапу');
    expect(stages[1].rerun).toBeNull();

    const completedStages = pipeline({state: {status: 'COMPLETED', stages: {
      sheet_matching: {status: 'COMPLETED'},
      sheet_scope: {status: 'COMPLETED'},
    }}});
    expect(completedStages[1].rerun).toMatchObject({
      kind: 'FULL_RERUN', partial_rerun_supported: false,
      label: '↻ Запустить полный анализ заново',
    });
    expect(completedStages[1].rerun.note).toContain('повторить автоматический анализ');
    expect(completedStages[1].rerun.dependencies).toContain('3. Анализ содержимого');
    expect(completedStages[1].rerun.dependencies).toContain('7. Проверка инженером');
    expect(completedStages[1].rerun.dependencies).toContain('8. Итоговый отчёт');
    expect(html).toContain("stage.rerun.dependencies.join(', ')");
  });

  it('M: keeps TEXT and GRAPHIC progress independent and applies concurrent terminal rules', () => {
    const content = pipeline({state: {status: 'RUNNING', stages: {
      text: {
        status: 'COMPLETED',
        progress: {processed: 47, total: 47, unit: 'deltas', duration_ms: 102000},
      },
      graphic: {
        status: 'RUNNING', processed: 8, total: 12, unit: 'groups',
        started_at: '2026-08-28T09:06:42.000Z',
      },
    }}})[2];

    expect(content.status).toBe('RUNNING');
    expect(content.progress).toMatchObject({status: 'RUNNING', kind: 'parallel', aggregate: false});
    expect(content.progress).not.toHaveProperty('percent');
    expect(content.sections.map(section => section.label)).toEqual([
      'TEXT (текст)', 'GRAPHIC (графика)',
    ]);
    expect(content.sections[0].progress).toMatchObject({
      kind: 'determinate', processed: 47, total: 47, counter_label: '47 / 47 различий',
    });
    expect(content.sections[1].progress).toMatchObject({
      kind: 'determinate', processed: 8, total: 12, counter_label: '8 / 12 групп',
    });
    expect(content.mini_counters).toEqual([
      {label: 'TEXT', value: '47 / 47 различий'},
      {label: 'GRAPHIC', value: '8 / 12 групп'},
    ]);

    expect(review.aggregateConcurrentPipelineStatus(['COMPLETED', 'NOT_APPLICABLE']))
      .toBe('COMPLETED');
    expect(review.aggregateConcurrentPipelineStatus(['FAILED', 'RUNNING']))
      .toBe('RUNNING');
    expect(review.aggregateConcurrentPipelineStatus(['COMPLETED', 'FAILED']))
      .toBe('FAILED');
    expect(review.aggregateConcurrentPipelineStatus(['COMPLETED', 'NOT_STARTED'], false))
      .toBe('PARTIAL');
    expect(review.aggregateConcurrentPipelineStatus(['FAILED', 'NOT_STARTED'], false))
      .toBe('FAILED');
    expect(review.aggregateConcurrentPipelineStatus(['FAILED', 'NOT_STARTED'], true))
      .toBe('RUNNING');
  });

  it('marks only the real backend TEXT substage as running', () => {
    const content = pipeline({state: {status: 'RUNNING', stages: {
      text: {status: 'RUNNING', progress: {
        status: 'RUNNING', current_substage: 'text_difference_validation',
        message: 'Проверка найденных различий…',
      }},
      graphic: {status: 'NOT_STARTED'},
    }}})[2];

    expect(content.sections[0].substages.map(stage => stage.status)).toEqual([
      'COMPLETED', 'COMPLETED', 'RUNNING', 'NOT_STARTED',
    ]);
    expect(content.sections[0].substages[2].progress.message).toBe('Проверка найденных различий…');
    const overview = review.normalizeProductionOverview({
      active_pair: pair(), state: {
        status: 'RUNNING', current_stage: 'content_analysis',
        current_substage: 'text_difference_search', stages: {},
      },
    });
    expect(overview.current_substage_label).toBe('Поиск различий');
  });

  it('uses the top-level live TEXT operation when stage progress omits it', () => {
    const content = pipeline({state: {
      status: 'RUNNING', current_stage: 'content_analysis',
      current_substage: 'text_difference_validation',
      stages: {
        text: {
          status: 'RUNNING',
          preparation: {status: 'COMPLETED'},
          deterministic_diff: {status: 'COMPLETED'},
          semantic_validation: {status: 'RUNNING'},
        },
        graphic: {status: 'NOT_STARTED'},
      },
    }})[2];

    expect(content.sections[0].substages.map(stage => stage.status)).toEqual([
      'COMPLETED', 'COMPLETED', 'RUNNING', 'NOT_STARTED',
    ]);
  });

  it('keeps unknown live GRAPHIC routes pending and activates only a known branch', () => {
    const documentGraphic = pipeline({state: {status: 'RUNNING', stages: {
      text: {status: 'COMPLETED'},
      graphic: {status: 'RUNNING', progress: {
        status: 'RUNNING', current_substage: 'graphic_group_comparison',
        message: 'Сравнение групп графики…', processed: 1, total: 4,
      }},
    }}})[2].sections[1].substages;

    expect(documentGraphic.map(stage => stage.status)).toEqual([
      'RUNNING', 'NOT_STARTED', 'NOT_STARTED', 'NOT_STARTED',
    ]);

    const pageGraphic = pipeline({state: {status: 'RUNNING', stages: {
      text: {status: 'COMPLETED'},
      graphic: {status: 'RUNNING', progress: {
        status: 'RUNNING', current_substage: 'graphic_structural_comparison',
        message: 'Структурное сравнение…',
      }},
    }}})[2].sections[1].substages;

    expect(pageGraphic.map(stage => stage.status)).toEqual([
      'COMPLETED', 'NOT_STARTED', 'RUNNING', 'NOT_STARTED',
    ]);
  });

  it('maps review-required GRAPHIC group results to human review, not N/A', () => {
    const graphic = pipeline({state: {status: 'PARTIAL', stages: {
      text: {status: 'COMPLETED'},
      graphic: {
        status: 'CHECK_BLOCKED', mode: 'DOCUMENT_GRAPHIC_BUNDLE', route: 'MULTI_ROUTE',
        groups_total: 3, groups_completed: 1, groups_review_required: 2,
        router_runs: 3, router_failed_groups: 0,
        group_results: [
          {status: 'COMPLETED', route: 'MODE_1_APPLICABLE', mode: 'MODE_1', changes: 2},
          {status: 'NOT_APPLICABLE', route: 'MODE_2_REQUIRED', review_required: true},
          {status: 'CHECK_BLOCKED', route: 'VISION_REQUIRED', review_required: true},
        ],
      },
    }}})[2].sections[1].substages;

    expect(graphic.map(stage => stage.status)).toEqual([
      'COMPLETED', 'COMPLETED', 'NEEDS_REVIEW', 'NEEDS_REVIEW',
    ]);
  });

  it('keeps Russian primary substage names and technical diagnostic names side by side', () => {
    const content = pipeline({state: {status: 'COMPLETED', stages: {
      text: {status: 'COMPLETED'},
      graphic: {status: 'NOT_APPLICABLE'},
    }}})[2];

    expect(content.sections[0].substages.map(stage => stage.label)).toEqual([
      'Подготовка текста', 'Поиск различий', 'Проверка различий', 'Формирование изменений',
    ]);
    expect(content.sections[0].substages.map(stage => stage.technical_label)).toEqual([
      'Preparation', 'Deterministic Diff', 'Semantic Validation', 'Text Atoms',
    ]);
    expect(content.sections[1].substages.map(stage => stage.technical_label)).toEqual([
      'Router', 'MODE 1', 'MODE 2', 'Vision fallback',
    ]);
  });
});

describe('Stage Comparison pure progress helpers', () => {
  it('treats backend UPDATING as active RUNNING state', () => {
    expect(review.normalizePipelineStatus('UPDATING')).toBe('RUNNING');
    expect(review.normalizePipelineProgress({status: 'UPDATING'}, {now_ms: NOW}))
      .toMatchObject({status: 'RUNNING', kind: 'indeterminate', is_running: true});
  });

  it('N: creates a determinate progress model only from known processed/total', () => {
    const progress = review.normalizePipelineProgress({
      status: 'RUNNING', processed: 31, total: 48, unit: 'sheets',
    }, {now_ms: NOW});

    expect(progress).toMatchObject({
      kind: 'determinate', determinate: true, indeterminate: false,
      processed: 31, total: 48, unit: 'листов', counter_label: '31 / 48 листов',
    });
    expect(progress.percent).toBeCloseTo(64.5833, 3);
  });

  it('O/R: unknown or insufficient totals stay indeterminate without fake percent or ETA', () => {
    const unknown = review.normalizePipelineProgress({
      status: 'RUNNING', processed: 31, started_at: '2026-08-28T09:08:00.000Z',
    }, {now_ms: NOW});
    expect(unknown).toMatchObject({
      kind: 'indeterminate', determinate: false, percent: null, eta_ms: null, eta_label: '',
    });

    const insufficient = review.normalizePipelineProgress({
      status: 'RUNNING', processed: 1, total: 12,
      started_at: '2026-08-28T09:09:00.000Z',
    }, {now_ms: NOW});
    expect(insufficient).toMatchObject({kind: 'determinate', eta_ms: null, eta_label: ''});
  });

  it('P: derives elapsed time from persisted started_at after reload', () => {
    const progress = review.normalizePipelineProgress({
      status: 'RUNNING', started_at: '2026-08-28T09:08:18.000Z', duration_ms: 0,
    }, {now_ms: NOW});

    expect(progress.elapsed_ms).toBe(102000);
    expect(progress.elapsed_label).toBe('1 мин 42 сек');
    expect(review.formatPipelineDuration(3_661_000)).toBe('1 ч 1 мин');
  });

  it('Q: reports heartbeat age and a configurable soft warning without declaring a hang', () => {
    const fresh = review.normalizePipelineProgress({
      status: 'RUNNING', last_activity_at: '2026-08-28T09:09:55.000Z',
    }, {now_ms: NOW, warning_threshold_ms: 10_000});
    expect(fresh).toMatchObject({
      heartbeat_age_ms: 5000, last_activity_label: '5 сек назад', heartbeat_warning: false,
    });

    const old = review.normalizePipelineProgress({
      status: 'RUNNING', last_activity_at: '2026-08-28T09:09:55.000Z',
    }, {now_ms: NOW, warning_threshold_ms: 3000});
    expect(old.heartbeat_warning).toBe(true);
    expect(old.heartbeat_warning_text).toContain('Давно нет обновлений');
    expect(old.heartbeat_warning_text.toLowerCase()).not.toContain('завис');
    expect(review.formatActivityAge(500)).toBe('сейчас');

    const backendConfigured = review.normalizePipelineProgress({
      status: 'RUNNING', last_activity_at: '2026-08-28T09:09:55.000Z',
      constraints: {activity_warning_threshold_sec: 4},
    }, {now_ms: NOW});
    expect(backendConfigured).toMatchObject({
      warning_threshold_ms: 4000, heartbeat_warning: true,
    });
    expect(review.normalizePipelineProgress({status: 'RUNNING'}, {now_ms: NOW}))
      .toMatchObject({warning_threshold_ms: 120000});
  });

  it('S: estimates approximate ETA when enough completed work is known', () => {
    const progress = review.normalizePipelineProgress({
      status: 'RUNNING', processed: 4, total: 10, unit: 'groups',
      started_at: '2026-08-28T09:06:00.000Z',
    }, {now_ms: NOW});

    expect(progress.eta_ms).toBe(360000);
    expect(progress.eta_label).toBe('~6 мин');
    expect(review.estimatePipelineEtaMs({
      processed: 8, total: 12, unit_durations_ms: [20000, 30000, 25000],
    })).toBe(100000);
  });

  it('T: never gives automatic ETA to human stages', () => {
    const progress = review.normalizePipelineProgress({
      status: 'RUNNING', processed: 4, total: 15,
      started_at: '2026-08-28T09:06:00.000Z',
      unit_durations_ms: [1000, 1000],
    }, {now_ms: NOW, human: true});

    expect(progress).toMatchObject({human: true, eta_ms: null, eta_label: ''});
  });

  it('V: keeps reason codes diagnostic while returning a human-readable explanation', () => {
    const content = pipeline({state: {status: 'PARTIAL', stages: {
      text: {status: 'COMPLETED'},
      graphic: {status: 'PARTIAL', reason_code: 'GROUPED_GRAPHIC_COMPARISON_NOT_SUPPORTED'},
    }}})[2];

    expect(content.reason).toContain('групповое графическое сравнение пока не поддерживается');
    expect(content.reason).not.toContain('GROUPED_GRAPHIC_COMPARISON_NOT_SUPPORTED');
    expect(content.reason_codes).toContain('GROUPED_GRAPHIC_COMPARISON_NOT_SUPPORTED');
  });
});

describe('Stage Comparison overview and CTA metadata', () => {
  it('returns distinct CTA states before, during, after, and on failure', () => {
    const before = review.normalizeProductionOverview({
      active_pair: pair(), state: {status: 'NOT_STARTED', stages: {}}, now_ms: NOW,
    });
    expect(before).toMatchObject({
      state: 'NOT_STARTED', headline: 'Анализ ещё не запускался.',
      cta: {kind: 'RUN', label: '▶ Запустить полный анализ', disabled: false},
    });

    const running = review.normalizeProductionOverview({
      active_pair: pair(), now_ms: NOW,
      state: {
        status: 'RUNNING', current_stage: 'content', current_substage: 'mode_2',
        started_at: '2026-08-28T09:06:00.000Z',
        last_activity_at: '2026-08-28T09:09:58.000Z',
        processed: 4, total: 10, unit: 'groups',
        stages: {text: {status: 'COMPLETED'}, graphic: {status: 'RUNNING'}},
      },
    });
    expect(running).toMatchObject({
      state: 'RUNNING', headline: 'Анализ выполняется',
      current_stage_label: '3. Анализ содержимого',
      current_substage_label: 'Структурное сравнение',
      cta: {kind: 'RUNNING', label: 'Анализ выполняется…', disabled: true},
    });
    expect(running.progress).toMatchObject({kind: 'determinate', eta_label: '~6 мин'});
    expect(running.detail_lines).toContain('Последняя активность: 2 сек назад.');

    const backendNames = review.normalizeProductionOverview({
      active_pair: pair(), now_ms: NOW,
      state: {
        status: 'UPDATING', current_stage: 'content_analysis',
        current_substage: 'graphic_structural_comparison',
        current_item: {left_pages: [37], right_pages: [45]},
        constraints: {activity_warning_threshold_sec: 180},
        stages: {text: {status: 'COMPLETED'}, graphic: {status: 'RUNNING'}},
      },
    });
    expect(backendNames).toMatchObject({
      state: 'RUNNING',
      current_stage_label: '3. Анализ содержимого',
      current_substage_label: 'Структурное графическое сравнение',
      progress: {current_item: 'LEFT 37 ↔ RIGHT 45', warning_threshold_ms: 180000},
    });

    const failed = review.normalizeProductionOverview({
      active_pair: pair(), state: {
        status: 'FAILED', failed_stage: 'content_analysis',
        failed_substage: 'text_difference_search', reason_code: 'TEXT_SOURCE_MISSING',
        stages: {},
      }, now_ms: NOW,
    });
    expect(failed.cta).toEqual({kind: 'RETRY', label: 'Повторить анализ', disabled: false});
    expect(failed).toMatchObject({
      failed_stage_label: '3. Анализ содержимого',
      failed_substage_label: 'Поиск различий',
      reason_codes: ['TEXT_SOURCE_MISSING'],
    });
    expect(failed.detail_lines).toEqual([
      'Ошибка на этапе: 3. Анализ содержимого.',
      'Операция: Поиск различий.',
      'Причина: Не найден один из исходных файлов текстовой подготовки.',
    ]);
    expect(html).toContain('scProductionOverview.reason_codes.join');
    expect(html).toContain('Внутренняя диагностика ошибки');
  });

  it('offers a full rerun instead of blocked review for stale results', () => {
    const overview = review.normalizeProductionOverview({
      active_pair: pair(), now_ms: NOW,
      state: {status: 'COMPLETED', stale: true, stages: {
        review_questions: {status: 'NEEDS_REVIEW', pending: 2, total: 2},
        engineer_decisions: {
          status: 'READY', counts: {PENDING_REVIEW: 4},
        },
      }},
    });
    expect(overview).toMatchObject({
      stale: true, headline: 'Результат анализа устарел.',
      cta: {kind: 'RERUN', label: '↻ Запустить анализ заново', disabled: false},
    });
  });

  it('summarizes human work and routes Continue review to questions first', () => {
    const overview = review.normalizeProductionOverview({
      active_pair: pair(), now_ms: NOW,
      state: {status: 'COMPLETED', selection: {input_mode: 'DOCUMENT'}, stages: {
        review_questions: {status: 'COMPLETED', questions: 3, answered: 1},
        unified_synthesis: {status: 'COMPLETED', changes: 4, review_items: 4},
        engineer_decisions: {
          status: 'READY', counts: {total: 4, APPROVED: 1, REJECTED: 1, PENDING_REVIEW: 2},
        },
        final_report: {status: 'READY', approved: 1},
      }},
    });

    expect(overview).toMatchObject({
      state: 'NEEDS_REVIEW',
      headline: 'Автоматический анализ завершён.',
      human: {questions_pending: 2, findings_pending: 2},
      cta: {kind: 'CONTINUE_REVIEW', label: 'Продолжить проверку', disabled: false},
    });
    expect(overview.cta.destination).toMatchObject({anchor: 'sc-production-questions-stage'});
    expect(Object.keys(overview.human)).toEqual(['questions_pending', 'findings_pending']);
    expect(overview.detail_lines).toEqual([
      'Требуется ответить на вопросы: 2.',
      'Требуется проверить изменения: 2.',
    ]);
  });
});
