import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const require = createRequire(import.meta.url);
const review = require('../static/js/stage-comparison-review.js');
const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const css = readFileSync(new URL('../static/css/styles.css', import.meta.url), 'utf8');

function change(overrides = {}) {
  return {
    change_id: 'uchg_voltage',
    scope_ref: 'scope:ios',
    subject_ref: 'equipment:SHR-1',
    project_entity_ref: 'project:SHR-1',
    facet_ref: 'voltage',
    dimension: 'PARAMETER',
    direction: 'ALTERED',
    before_value: '220 В',
    after_value: '380 В',
    source_mode: 'BOTH',
    review_status: 'CONFIRMED',
    confidence: {level: 'HIGH', basis: 'CORROBORATED'},
    provenance: {
      source_atoms: [{
        provenance: {
          locations: {
            LEFT: [{page: 10}],
            RIGHT: [{page: 24}],
          },
        },
      }],
    },
    ...overrides,
  };
}

function row(id, decision = 'PENDING_REVIEW', overrides = {}) {
  return {
    target_id: id,
    target_kind: 'CHANGE',
    change: change({change_id: id, ...overrides}),
    presentation_group_id: 'pgroup_parameters',
    engineer_decision: {
      target_id: id,
      decision,
      author: decision === 'PENDING_REVIEW' ? null : 'Инженер',
      comment: decision === 'APPROVED' ? 'Проверено' : null,
      reason_code: null,
      input_signature: `target-signature-${id}`,
      revision: decision === 'PENDING_REVIEW' ? 1 : 2,
    },
  };
}

describe('Stage Comparison production review helpers', () => {
  it('keeps every atomic target as its own row and only marks presentation groups', () => {
    const payload = {rows: [
      row('uchg_voltage'),
      row('uchg_temperature', 'APPROVED', {
        facet_ref: 'temperature_range',
        before_value: '-10…+40 °C',
        after_value: '-25…+50 °C',
      }),
    ]};

    const rows = review.normalizeRows(payload);

    expect(rows).toHaveLength(2);
    expect(rows.map(item => item.target_id)).toEqual(['uchg_voltage', 'uchg_temperature']);
    expect(rows[0].presentation_group_id).toBe('pgroup_parameters');
    expect(rows[0].left_pages).toEqual([10]);
    expect(rows[0].right_pages).toEqual([24]);
    // Инженер читает «стр. PDF», а не LEFT/RIGHT: сторон в проекте нет,
    // есть «было» и «стало».
    expect(rows[0].sheets_label).toBe('Было — стр. PDF 10; стало — стр. PDF 24');
    expect(rows[1].decision).toBe('APPROVED');
    expect(rows[1].target_input_signature).toBe('target-signature-uchg_temperature');
    expect(rows[1].decision_revision).toBe(2);
  });

  it('counts explicit decisions and treats an empty decision as PENDING_REVIEW', () => {
    const rows = review.normalizeRows({rows: [
      row('a', 'APPROVED'),
      row('b', 'REJECTED'),
      {...row('c'), engineer_decision: {}},
    ]});

    expect(review.reviewCounts(rows)).toEqual({
      total: 3,
      APPROVED: 1,
      REJECTED: 1,
      PENDING_REVIEW: 1,
    });
  });

  it('keeps a REVIEW_EVIDENCE target as an independent atomic row', () => {
    const rows = review.normalizeRows({rows: [{
      target_id: 'urev_graphic_only',
      target_kind: 'REVIEW_EVIDENCE',
      presentation_group_id: null,
      change: {
        review_evidence_id: 'urev_graphic_only',
        scope_ref: 'scope:ios',
        outcome: 'REVIEW_REQUIRED',
        source_mode: 'GRAPHIC',
        confidence: 'LOW',
      },
      engineer_decision: {decision: 'PENDING_REVIEW'},
    }]});

    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      target_id: 'urev_graphic_only',
      target_kind: 'REVIEW_EVIDENCE',
      source: 'GRAPHIC',
      status: 'REVIEW_REQUIRED',
      decision: 'PENDING_REVIEW',
      presentation_group_id: null,
    });
  });

  it('normalizes SHEET / ENTITY / CHANGE question counters and answers', () => {
    const artifact = {
      questions: [
        {
          question_id: 'q-sheet', category: 'SHEET', prompt: 'Какой лист?',
          answer_options: [{code: '24', label: 'Лист 24'}, {code: '25', label: 'Лист 25'}],
        },
        {question_id: 'q-entity', category: 'ENTITY', prompt: 'Один объект?'},
        {question_id: 'q-change', category: 'CHANGE', prompt: 'Это изменение?'},
      ],
    };

    expect(review.normalizeQuestionCounts(artifact)).toEqual({
      SHEET: 1, ENTITY: 1, CHANGE: 1, total: 3,
    });
    expect(review.normalizeQuestions(artifact)[0].options).toEqual([
      {value: '24', label: 'Лист 24'},
      {value: '25', label: 'Лист 25'},
    ]);
    expect(review.normalizeQuestionCounts({counts: {SHEET: 4, ENTITY: 2, CHANGE: 1, total: 99}}))
      .toEqual({SHEET: 4, ENTITY: 2, CHANGE: 1, total: 7});
  });

  it('retains typed and explicit human-answer fields for reloadable editors', () => {
    const questions = review.normalizeQuestions({questions: [
      {
        question_id: 'q-change', category: 'CHANGE', question_type: 'CHANGE_REVIEW_EVIDENCE',
        prompt: 'Уточнить', input_signature: 'qsig-1',
        human_answer: {
          answer: 'OTHER', selected_refs: ['uchg-1'],
          typed_resolution: {dimension: 'PARAMETER', project_entity_ref: 'project:SHR-1'},
        },
      },
      {
        question_id: 'q-entity', category: 'ENTITY', prompt: 'Выбрать объект',
        answer: {
          answer: 'OTHER',
          explicit_candidate: {right_entity_ref: 'right:SHR-1'},
        },
      },
    ]});

    expect(questions[0]).toMatchObject({
      question_type: 'CHANGE_REVIEW_EVIDENCE',
      selected_refs: ['uchg-1'],
      typed_resolution: {dimension: 'PARAMETER', project_entity_ref: 'project:SHR-1'},
    });
    expect(questions[1].explicit_candidate).toEqual({right_entity_ref: 'right:SHR-1'});
  });

  it('normalizes side-by-side BBOX_SET and POLYGON evidence without merging sides', () => {
    const evidence = review.normalizeEvidence({
      target_id: 'uchg_voltage',
      source_mode: 'BOTH',
      layout: 'SIDE_BY_SIDE',
      sides: {
        LEFT: [{
          source: 'TEXT', page: 10,
          coordinate_space: 'NORMALIZED_PAGE_TOP_LEFT',
          highlight: {kind: 'BBOX_SET', bboxes: [[0.1, 0.2, 0.4, 0.3]]},
        }],
        RIGHT: [{
          source: 'GRAPHIC', page: 24,
          coordinate_space: 'NORMALIZED_PAGE_TOP_LEFT',
          highlight: {kind: 'POLYGON', polygon: [[0.5, 0.5], [0.7, 0.5], [0.7, 0.8]]},
        }],
      },
    });

    expect(evidence.has_both_sides).toBe(true);
    expect(evidence.sides.left.page).toBe(10);
    expect(evidence.sides.right.page).toBe(24);
    expect(evidence.sides.left.overlays[0]).toMatchObject({
      x: 0.1, y: 0.2, units: 'normalized', source: 'TEXT',
    });
    expect(evidence.sides.left.overlays[0].width).toBeCloseTo(0.3);
    expect(evidence.sides.left.overlays[0].height).toBeCloseTo(0.1);
    expect(evidence.sides.right.overlays[0].kind).toBe('POLYGON');
    expect(review.evidenceFocus(evidence.sides.left)).toMatchObject({x: 0.25, y: 0.25});
  });

  it('converts PDF visual points only when exact page dimensions are present', () => {
    const withSize = review.normalizeEvidence({
      sides: {LEFT: [{
        source: 'GRAPHIC', page: 2,
        coordinate_space: 'PDF_VISUAL_PT',
        page_size: {width: 1000, height: 500},
        highlight: {kind: 'BBOX', bbox: [100, 50, 300, 150]},
      }]},
    });
    expect(withSize.sides.left.overlays[0]).toMatchObject({
      x: 0.1, y: 0.1, width: 0.2, height: 0.2, units: 'normalized',
    });

    const withoutSize = review.normalizeEvidence({
      sides: {LEFT: [{
        source: 'GRAPHIC', page: 2,
        coordinate_space: 'PDF_VISUAL_PT',
        highlight: {kind: 'BBOX', bbox: [100, 50, 300, 150]},
        coordinates_available: true,
      }]},
    });
    expect(withoutSize.sides.left.overlays).toEqual([]);
    expect(withoutSize.sides.left.coordinates_missing).toBe(true);
  });

  it('keeps exact TEXT pages while honestly reporting absent coordinates', () => {
    const evidence = review.normalizeEvidence({
      source_mode: 'TEXT',
      sides: {
        LEFT: [{source: 'TEXT', page: 3, highlight: null, coordinates_available: false}],
        RIGHT: [{source: 'TEXT', page: 7, highlight: null, coordinates_available: false}],
      },
    });

    expect(evidence.source_mode).toBe('TEXT');
    expect(evidence.sides.left).toMatchObject({page: 3, coordinates_missing: true});
    expect(evidence.sides.right).toMatchObject({page: 7, coordinates_missing: true});
    expect(evidence.sides.left.overlays).toEqual([]);
    expect(evidence.sides.right.overlays).toEqual([]);
    expect(evidence.has_any_coordinates).toBe(false);
  });

  it('retains evidence overlays from every page on a side', () => {
    const evidence = review.normalizeEvidence({
      sides: {LEFT: [
        {
          source: 'TEXT', page: 2, coordinate_space: 'NORMALIZED_PAGE_TOP_LEFT',
          highlight: {kind: 'BBOX', bbox: [0.1, 0.1, 0.2, 0.2]},
        },
        {
          source: 'GRAPHIC', page: 3, coordinate_space: 'NORMALIZED_PAGE_TOP_LEFT',
          highlight: {kind: 'BBOX', bbox: [0.4, 0.4, 0.6, 0.6]},
        },
      ]},
    });

    expect(evidence.sides.left.pages).toEqual([2, 3]);
    expect(evidence.sides.left.overlays.map(item => item.page)).toEqual([2, 3]);
    const focus = review.evidenceFocus(evidence.sides.left);
    expect(focus.x).toBeCloseTo(0.15);
    expect(focus.y).toBeCloseTo(0.15);
  });

  it('does not turn an explicitly missing evidence page into page zero', () => {
    const evidence = review.normalizeEvidence({
      sides: {
        LEFT: [{source: 'GRAPHIC', page: null, highlight: null, coordinates_available: false}],
      },
    });

    expect(evidence.sides.left.page).toBeNull();
    expect(evidence.sides.left.has_evidence).toBe(true);
    expect(evidence.sides.left.coordinates_missing).toBe(true);
  });

  it('builds final rows exclusively from what the engineer approved', () => {
    const report = {
      approved_atomic_changes: [{
        ...change(),
        engineer_decision: {author: 'Инженер', comment: 'Подтверждаю'},
      }],
      rejected_changes: [change({change_id: 'must-not-leak'})],
      pending_changes: [change({change_id: 'also-must-not-leak'})],
    };

    const rows = review.normalizeFinalRows(report);
    expect(rows).toHaveLength(1);
    expect(rows[0].target_id).toBe('uchg_voltage');
    expect(rows[0].decision).toBe('APPROVED');
    expect(rows[0].comment).toBe('Подтверждаю');
  });

  it('keeps an approved review finding in the report instead of dropping it', () => {
    // Инженер подтверждает два вида строк, и бэкенд отдаёт оба списка.
    // Отчёт читал только первый — подтверждённая находка исчезала молча.
    const report = {
      approved_atomic_changes: [change({change_id: 'uchg_1'})],
      approved_review_findings: [{
        review_evidence_id: 'ureview_1',
        scope_ref: 'scope:ar',
        dimension: 'UNKNOWN_DIMENSION',
        direction: 'ALTERED',
        before_value: 'EI 60',
        after_value: 'EI 90',
        left_pages: [29],
        right_pages: [8],
        reason_codes: ['dimension_unknown'],
        engineer_decision: {author: 'Инженер', comment: 'Подтверждаю'},
      }],
    };

    const rows = review.normalizeFinalRows(report);

    expect(rows.map(item => item.target_id)).toEqual(['uchg_1', 'ureview_1']);
    const finding = rows[1];
    expect(finding.target_kind).toBe('REVIEW_EVIDENCE');
    expect(finding.decision).toBe('APPROVED');
    expect(finding.before).toBe('EI 60');
    expect(finding.after).toBe('EI 90');
    expect(finding.left_pages).toEqual([29]);
    expect(finding.right_pages).toEqual([8]);
  });

  it('counts approved review findings in the report badge as well', () => {
    // Иначе в бейдже «Войдёт в отчёт» одно число, а в таблице другое.
    const stages = review.normalizeProductionPipeline({
      state: {status: 'COMPLETED', input_mode: 'DOCUMENT', stages: {}},
      final_report: {
        summary: {approved: 1, approved_review_findings: 2},
        approved_atomic_changes: [change({change_id: 'uchg_1'})],
        approved_review_findings: [
          {review_evidence_id: 'ureview_1'},
          {review_evidence_id: 'ureview_2'},
        ],
        constraints: {approved_only: true},
      },
    });

    const report = stages[stages.length - 1];
    const badge = report.counters.find(item => item.label === 'Войдёт в отчёт');
    expect(badge.value).toBe(3);
  });

  it('normalizes production PAGE suggestions and keeps selected versus suggested pages', () => {
    const suggestions = review.normalizeSheetSuggestions({
      sheet_suggestions: {
        suggestions: [{
          suggestion_id: 'sheet-suggestion-1',
          selected_left_pages: [10],
          selected_right_pages: [20],
          suggested_left_pages: [10, 11],
          suggested_right_pages: [24, 25],
          status: 'PENDING_REVIEW',
        }],
      },
    });

    expect(suggestions).toHaveLength(1);
    expect(suggestions[0]).toMatchObject({
      id: 'sheet-suggestion-1',
      selected_left_pages: [10],
      selected_right_pages: [20],
      left_pages: [10, 11],
      right_pages: [24, 25],
      confidence: 'PENDING_REVIEW',
    });
  });

  it('reads PAGE action results only from authoritative application diagnostics', () => {
    const application = review.normalizeSuggestionApplication({
      state: {
        run_id: 'production-generation-2',
        selection: {left_pages: [10], right_pages: [24]},
        suggestion_actions: {'sheet-suggestion-1': 'REPLACE'},
        suggestion_action_semantics: {
          scope_applied: true,
          pipeline_rerun: true,
          generation_run_id: 'production-generation-2',
          effective_page_groups: [{left_pages: [10, 11], right_pages: [24]}],
          outcomes: [{
            suggestion_id: 'sheet-suggestion-1', action: 'REPLACE', state: 'MATERIALIZED',
            scope_applied: true, pipeline_rerun: true, this_update_reran: true,
          }],
        },
      },
    }, 'sheet-suggestion-1');

    expect(application).toEqual({
      action: 'REPLACE',
      state: 'MATERIALIZED',
      scope_applied: true,
      pipeline_rerun: true,
      this_update_reran: true,
      generation_was_materialized: null,
      generation_run_id: 'production-generation-2',
      groups: [{left_pages: [10, 11], right_pages: [24]}],
    });

    expect(review.normalizeSuggestionApplication({}, 'missing')).toMatchObject({
      action: '', scope_applied: null, pipeline_rerun: null, groups: [],
    });

    const ignored = review.normalizeSuggestionApplication({
      suggestion_actions: {'sheet-suggestion-2': 'IGNORE'},
      suggestion_action_semantics: {
        scope_applied: true,
        pipeline_rerun: false,
        outcomes: [{suggestion_id: 'sheet-suggestion-2', action: 'IGNORE', state: 'IGNORED'}],
      },
    }, 'sheet-suggestion-2');
    expect(ignored).toMatchObject({
      action: 'IGNORE', state: 'IGNORED', scope_applied: false, pipeline_rerun: false,
    });

    const independentlyIgnored = review.normalizeSuggestionApplication({
      suggestion_actions: {'sheet-suggestion-3': 'IGNORE'},
      suggestion_action_semantics: {
        pipeline_rerun: true,
        outcomes: [{
          suggestion_id: 'sheet-suggestion-3', action: 'IGNORE', state: 'IGNORED',
          scope_applied: false, pipeline_rerun: false, this_update_reran: false,
        }],
      },
    }, 'sheet-suggestion-3');
    expect(independentlyIgnored).toMatchObject({
      action: 'IGNORE', state: 'IGNORED', scope_applied: false,
      pipeline_rerun: false, this_update_reran: false,
    });
  });

  it('normalizes the eight production stages from persisted state without invented percentages', () => {
    const stages = review.normalizeProductionPipeline({
      state: {
        status: 'PARTIAL', input_mode: 'DOCUMENT', stale: false,
        selection: {input_mode: 'DOCUMENT', left_pages: [], right_pages: []},
        stages: {
          sheet_matching: {status: 'COMPLETED', relations: 12, relation_counts: {HIGH: 4, SPLIT: 1}},
          sheet_scope: {status: 'COMPLETED', groups: 5},
          text: {
            status: 'COMPLETED', atoms: 31, deltas: 47,
            automatic_atoms: 31, review_required: 16,
            substages: {
              preparation: {status: 'COMPLETED', fragments: 80},
              deterministic_diff: {status: 'COMPLETED', deltas: 47},
              semantic_validation: {status: 'COMPLETED', facts: 31, review_required: 16},
              text_atoms: {status: 'COMPLETED', atoms: 47, automatic_atoms: 31, review_required: 16},
            },
          },
          graphic: {
            status: 'CHECK_BLOCKED', changes: 4, groups_total: 6,
            groups_completed: 4, groups_not_applicable: 2,
            reason_code: 'document_graphic_groups_require_attention',
            reason_codes: ['grouped_graphic_comparison_not_supported'],
            group_results: [{
              status: 'COMPLETED', route: 'MODE_1_APPLICABLE', mode: 'MODE_1', changes: 4,
            }, {
              status: 'NOT_APPLICABLE', reason_code: 'grouped_graphic_comparison_not_supported',
            }],
          },
          entity_matching: {status: 'COMPLETED', relations: 9},
          entity_binding: {status: 'COMPLETED', bound_atoms: 25},
          effective_entity_binding: {status: 'COMPLETED', bound_atoms: 27},
          review_questions: {status: 'COMPLETED', questions: 3},
          review_application: {status: 'COMPLETED', applied_decisions: 1},
          automatic_unified_synthesis: {status: 'COMPLETED', changes: 20, review_items: 7},
          unified_synthesis: {status: 'COMPLETED', changes: 21, review_items: 6},
          engineer_decisions: {status: 'READY', counts: {total: 3, APPROVED: 1, REJECTED: 1, PENDING_REVIEW: 1}},
          final_report: {status: 'READY', approved: 1},
        },
      },
      questions: {questions: [
        {question_id: 'qs', category: 'SHEET', answer: 'YES'},
        {question_id: 'qe', category: 'ENTITY'},
        {question_id: 'qc', category: 'CHANGE', human_answer: {answer: 'NO'}},
      ]},
      changes: {rows: [row('approved', 'APPROVED'), row('rejected', 'REJECTED'), row('pending')]},
      final_report: {summary: {approved: 1}, approved_atomic_changes: [change({change_id: 'approved'})], constraints: {approved_only: true}},
    });

    expect(stages).toHaveLength(8);
    expect(stages.map(stage => stage.label)).toEqual([
      'Выбор сравнения', 'Сопоставление листов', 'Анализ содержимого',
      'Сопоставление объектов', 'Вопросы инженеру', 'Синтез изменений',
      'Проверка инженером', 'Итоговый отчёт',
    ]);
    expect(stages[2].status).toBe('PARTIAL');
    expect(stages[2].sections.map(section => section.label)).toEqual([
      'Текстовая часть', 'Графическая часть',
    ]);
    expect(stages[2].sections[0].substages.map(stage => stage.label)).toEqual([
      'Подготовка текста', 'Поиск различий', 'Проверка различий', 'Формирование изменений',
    ]);
    expect(stages[2].sections[1].substages.map(stage => stage.label)).toEqual([
      'Выбор метода', 'Точное графическое сравнение', 'Структурное сравнение', 'Визуальная проверка',
    ]);
    expect(stages[2].sections[1].substages.map(stage => stage.technical_label)).toEqual([
      'Router', 'MODE 1', 'MODE 2', 'Vision fallback',
    ]);
    expect(stages[2].reason).toContain('требуют внимания инженера');
    expect(stages[2].reason).not.toContain('grouped_graphic_comparison_not_supported');
    expect(stages[2].reason_codes).toContain('grouped_graphic_comparison_not_supported');
    expect(stages[4]).toMatchObject({
      status: 'NEEDS_REVIEW', progress: {answered: 2, total: 3},
    });
    expect(stages[4].categories.map(category => category.total)).toEqual([1, 1, 1]);
    expect(stages[6].counters.map(counter => counter.value)).toEqual([3, 1, 1, 1]);
    expect(stages[7].counters.map(counter => counter.value)).toEqual([3, 1, 1, 1, 1]);
    expect(stages[7].approved_only).toBe(true);
    expect(JSON.stringify(stages)).not.toContain('percentage');
  });

  it('keeps honest fallback substages when older TEXT state has only aggregate counters', () => {
    const pipeline = review.normalizeProductionPipeline({
      state: {selection: {input_mode: 'PAGE'}, stages: {
        text: {status: 'COMPLETED', atoms: 7},
        graphic: {status: 'NOT_APPLICABLE', reason_code: 'NO_PREPARED_GRAPHIC_BLOCK_ON_MATCHED_SHEET'},
      }},
    });

    const content = pipeline[2];
    expect(content.sections[0].substages).toHaveLength(4);
    expect(content.sections[0].substages[0]).toMatchObject({
      status: 'COMPLETED', counters: [],
    });
    expect(content.sections[0].substages[0].note)
      .toContain('отдельная метрика не публиковалась');
    expect(content.sections[0].substages[3].counters).toEqual([{label: 'Атомы', value: 7}]);
    expect(content.reason).toContain('нет подготовленной графики');
  });

  it('humanizes real document graphic and text coverage reason codes', () => {
    expect(review.humanizeReasonCode('EXTRACTION_COMPLETENESS_INSUFFICIENT'))
      .toContain('геометрия');
    expect(review.humanizeReasonCode('opposite_side_structured_coverage_incomplete'))
      .toContain('структурированного покрытия');
    expect(review.humanizeReasonCode('MULTI_BLOCK_CORRESPONDENCE_NOT_IN_G1'))
      .toContain('пара графических блоков');
    expect(review.humanizeReasonCode('unknown_internal_code'))
      .not.toContain('unknown_internal_code');
  });
});

describe('Stage Comparison production review integration', () => {
  it('loads the UMD helpers before app setup and exports production refs/functions', () => {
    expect(html.indexOf('stage-comparison-review.js')).toBeLessThan(html.indexOf('/static/js/app.js'));
    expect(app).toContain('const SC_PRODUCTION_REVIEW = window.StageComparisonReview');
    expect(app).toContain('scProductionState, scProductionChanges, scProductionQuestions');
    expect(app).toContain('scLoadProductionReview, scDiscardProductionDrafts');
    expect(app).toContain('scRunProductionComparison, scProductionRunBody');
    expect(app).toContain('scOpenProductionEvidence, scCloseProductionEvidence');
  });

  it('uses the complete pair-scoped production API contract', () => {
    expect(app).toContain("`/production${suffix}`");
    expect(app).toContain("'/state', '/changes', '/questions', '/final-report', '/text-evidence'");
    expect(app).toContain("scProductionRequest('/run'");
    expect(app).toContain("scProductionRequest('/decisions'");
    expect(app).toContain("scProductionRequest('/answers'");
    expect(app).toContain('`/changes/${encodeURIComponent(row.target_id)}/evidence`');
    expect(app).toContain("input_mode: pageMode ? 'PAGE' : 'DOCUMENT'");
    expect(app).toContain('expected_revision: baseVersion.revision');
    expect(app).toContain('expected_revision: Number.isInteger(expectedRevision)');
  });

  it('renders one production finding row with all required review columns', () => {
    expect(html).toContain('v-for="row in scProductionRows"');
    for (const heading of [
      'ID', 'Объект', 'Листы', 'Изменение', 'Было', 'Стало', 'Источник',
      'Статус / уверенность', 'Решение эксперта', 'Причина / комментарий',
    ]) {
      expect(html).toContain(`<th${heading === 'ID' ? ' class="sc-production-col-id"' : ''}>${heading}</th>`);
    }
    expect(html).toContain("scSetProductionDecision(row, 'APPROVED')");
    expect(html).toContain("scSetProductionDecision(row, 'REJECTED')");
    expect(html).toContain('row.presentation_group_id');
    expect(html).not.toContain('scSetProductionGroupDecision');
    expect(css).toContain('.sc-production-review__table { min-width: 1760px; }');
  });

  it('shows non-blocking clarification counters and all PAGE suggestion actions', () => {
    expect(html).toContain('Требуется уточнение');
    expect(html).toContain('scProductionQuestionCounts.SHEET');
    expect(html).toContain('scProductionQuestionCounts.ENTITY');
    expect(html).toContain('scProductionQuestionCounts.CHANGE');
    expect(html).toContain("['COMPARE_ADDITIONALLY', 'REPLACE', 'ADD_TO_GROUP', 'IGNORE']");
    expect(html).toContain('применение области и пересчёт подтверждает сервер');
    expect(html).toContain('scProductionSuggestionStatus(suggestion)');
    expect(app).toContain('expectedInputSignature');
    expect(app).toContain('expectedRevision');
    expect(app).toContain('scApplyProductionReplacementView(application)');
    expect(app).toContain("application.action !== 'REPLACE'");
    expect(app).toContain('application.scope_applied !== true');
    expect(app).toContain('production pipeline пересчитан');
    expect(app).toContain('область не менялась, pipeline не перезапускался');
    expect(app).toContain('const application = {...authoritative};');
    expect(app).not.toContain('savedMatchesGeneration ? saved : authoritative');
    expect(app).toContain('scHydrateProductionSuggestionActions');
    expect(app).not.toContain('scProductionSuggestionActions[suggestion.id] = action');
  });

  it('renders strict typed/explicit answers and stale-safe review controls', () => {
    expect(html).toContain('scProductionQuestionNeedsTypedResolution(question)');
    expect(html).toContain('scSetProductionQuestionTypedField(question, field');
    expect(html).toContain('scProductionQuestionNeedsExplicitCandidate(question)');
    expect(html).toContain("scSetProductionQuestionExplicitField(question, 'right_entity_ref'");
    expect(app).toContain('payload.explicit_candidate = {right_entity_ref: rightEntityRef}');
    expect(app).toContain('payload.typed_resolution = typed');
    expect(app).toContain('row_signature: rowSignature');
    expect(app).toContain('question_signature: questionSignature');
    expect(app).toContain('base_artifact_input_signature');
    expect(app).toContain('base_artifact_revision');
    expect(app).toContain('scProductionLoading.value');
    expect(app).toContain('preserveDrafts: true');
    expect(app).toContain('scProductionValidateSheetRelationType');
    expect(app).toContain('initial_typed_resolution: {...typedResolution}');
    expect(app).toContain("draft.answer === 'OTHER' && !typedChanged && !hasSavedTyped");
    expect(app).toContain('allowedChangeIds.has(changeId)');
    expect(app).toContain('!allowedChangeIds.size');
    expect(app).toContain('typed.selected_change_ids.length >= allowedChangeIds.size');
    expect(app).toContain("selected === 'UNCERTAIN'");
    expect(html).toContain("['MATCHED', 'SPLIT', 'MERGED']");
    expect(html).not.toContain("['MATCHED', 'SPLIT', 'MERGED', 'UNCERTAIN']");
    expect(html).toContain("['MATERIAL_CHANGE', 'DETAIL_ONLY']");
    expect(html).not.toContain("['MATERIAL_CHANGE', 'DETAIL_ONLY', 'REVIEW_REQUIRED']");
    expect(html).toContain('scProductionStale');
    // A review finding is approvable once it has a value and a page; only a
    // row with nothing to show stays blocked.
    expect(html).toContain('!row.decidable');
    expect(html).toContain('Нечего показать: нет значения или расположения');
    expect(html).not.toContain('сначала разрешите CHANGE-вопрос');
  });

  it('opens exact LEFT and RIGHT evidence pages and paints transient overlays', () => {
    expect(app).toContain("for (const side of ['left', 'right'])");
    expect(app).toContain('scCurrentPage[side] = page');
    expect(app).toContain('scProductionEvidenceVisible.value = false');
    expect(html).toContain('scProductionEvidenceOverlaysFor(side, scCurrentPage[side])');
    expect(html).toContain("scProductionEvidenceSideNotice('left')");
    expect(html).toContain("scProductionEvidenceSideNotice('right')");
    expect(html).toContain('scShowProductionEvidencePage(side, page)');
    expect(app).toContain('scSetViewerEmpty(side, true)');
    expect(css).toContain('.sc-production-evidence-overlay');
  });

  it('renders the collapsible eight-stage pipeline and direct question/review/report destinations', () => {
    expect(app).toContain('SC_PRODUCTION_REVIEW.normalizeProductionPipeline');
    expect(app).toContain('scOpenProductionPipelineDestination');
    expect(app).toContain('scOpenProductionQuestions');
    expect(html).toContain('v-for="stage in scProductionPipeline"');
    expect(html).toContain('scToggleProductionPipeline(stage)');
    expect(html).toContain('Конвейер сравнения: слева → справа');
    expect(html).toContain('v-for="section in stage.sections"');
    expect(html).toContain('v-for="substage in section.substages"');
    expect(html).toContain('scOpenProductionQuestions(category.category)');
    expect(html).toContain('id="sc-production-review-table"');
    expect(css).toContain('.sc-production-pipeline__rail');
    expect(css).toContain('.sc-production-pipeline__content-sections');
  });

  it('offers a return from evidence to the exact atomic review row', () => {
    expect(html).toContain('Вернуться к той же строке');
    expect(html).toContain(':data-production-target-id="row.target_id"');
    expect(app).toContain('scReturnToProductionReviewRow');
    expect(app).toContain("document.querySelectorAll('[data-production-target-id]')");
    expect(app).toContain("row.querySelector('.sc-production-evidence-link')");
    expect(css).toContain('.sc-production-review__table tr.is-return-target');
  });

  it('keeps the legacy Stage Comparison tabs and discrepancy ledger intact', () => {
    expect(html).toContain("scTab==='upload'");
    expect(html).toContain("scTab==='links'");
    expect(html).toContain("scTab==='diffs'");
    expect(html).toContain("scTab==='report'");
    expect(html).toContain('v-for="group in scTextDifferenceGroups"');
    expect(app).toContain('scRunTextComparison');
    expect(app).toContain('scRunTextAiReview');
  });
});

describe('Stage Comparison: exception questions vs engineer review', () => {
  it('marks a review finding decidable once it has a value and a page', () => {
    const [decidable, blind] = review.normalizeRows({rows: [
      {
        target_id: 'ureview_1',
        target_kind: 'REVIEW_EVIDENCE',
        presentation: {presentable: true, left_pages: [29], right_pages: [8]},
        change: {
          review_evidence_id: 'ureview_1',
          dimension: 'UNKNOWN_DIMENSION',
          before_value: 'EI 60',
          after_value: 'EI 90',
        },
      },
      {
        target_id: 'ureview_2',
        target_kind: 'REVIEW_EVIDENCE',
        presentation: {presentable: false, left_pages: [], right_pages: []},
        change: {review_evidence_id: 'ureview_2', dimension: 'UNKNOWN_DIMENSION'},
      },
    ]});

    expect(decidable.decidable).toBe(true);
    expect(blind.decidable).toBe(false);
    // A CHANGE row is always decidable, presentation or not.
    expect(review.normalizeRows({rows: [{target_id: 'uchg_1', target_kind: 'CHANGE', change: {}}]})[0].decidable)
      .toBe(true);
  });

  it('never prints an internal enum where the engineer reads the change', () => {
    const [row] = review.normalizeRows({rows: [{
      target_id: 'ureview_1',
      target_kind: 'REVIEW_EVIDENCE',
      presentation: {presentable: true, left_pages: [29], right_pages: [8]},
      change: {
        review_evidence_id: 'ureview_1',
        dimension: 'UNKNOWN_DIMENSION',
        direction: 'ALTERED',
        before_value: 'EI 60',
        after_value: 'EI 90',
      },
    }]});

    expect(row.change_label).not.toContain('UNKNOWN_DIMENSION');
    expect(row.change_label).toContain('тип изменения не определён');
    expect(row.object_ref).toBe('Объект не назван');
  });

  it('asks for an object name and never for an internal ref', () => {
    expect(app).toContain("const SC_INTERNAL_REF_FIELDS = ['subject_ref', 'project_entity_ref']");
    expect(app).toContain("'dimension', 'object_label', 'facet_ref', 'direction'");
    expect(app).toContain('object_label: \'Объект (как он называется в проекте)\'');
    // The free-text stable-id boxes are gone from the question card.
    expect(html).not.toContain('Project entity ref');
    expect(html).not.toContain("scSetProductionQuestionExplicitField(question, 'project_entity_ref'");
  });

  it('shows the sheets a sheet question is about, by name', () => {
    expect(app).toContain('function scProductionQuestionSheets(row, side)');
    expect(app).toContain('function scOpenProductionSheet(side, page)');
    expect(html).toContain("scProductionQuestionSheets(question, 'LEFT')");
    expect(html).toContain("scProductionQuestionSheets(question, 'RIGHT')");
    expect(html).toContain('{{ scSheetReference(sheet) }}');
    expect(html).toContain('Открыть доказательство');
    expect(html).toContain('Было: <b>{{ scProductionQuestionChange(question).before }}</b>');
    expect(app).toContain('function scProductionQuestionWhy(row)');
    expect(html).toContain('Почему предложено:');
  });

  it('keeps raw identifiers in diagnostics rather than in the prompt line', () => {
    expect(html).toContain('<summary>Диагностика</summary>');
    expect(html).toContain('sc-production-question__diagnostics');
    expect(html).toContain('sc-production-row__diagnostics');
  });
});
