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
    expect(rows[0].sheets_label).toBe('LEFT 10 → RIGHT 24');
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

  it('builds final rows exclusively from approved_atomic_changes', () => {
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
    expect(app).toContain("const suffixes = ['/state', '/changes', '/questions', '/final-report']");
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
    expect(html).toContain("row.target_kind === 'REVIEW_EVIDENCE'");
    expect(html).toContain('сначала разрешите CHANGE-вопрос');
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
