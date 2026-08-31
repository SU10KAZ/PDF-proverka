import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const require = createRequire(import.meta.url);
const review = require('../static/js/stage-comparison-review.js');
const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');

function report(overrides = {}) {
  return {
    kind: 'stage_comparison_preliminary_report',
    schema_version: 'preliminary-comparison-report.v1',
    available: true,
    stale: false,
    run_status: 'COMPLETED',
    summary: {
      title: 'Предварительный отчёт анализа',
      sentences: ['Система нашла 1 изменение.'],
      counts: {
        automatic: 1, ai_verified: 0, review: 1,
        inconsistency: 1, unproven: 1,
      },
    },
    sections: [
      {
        section_id: 'scheme', title: 'Основные изменения схемы',
        items: [{
          item_id: 'automatic-1', status: 'Найдено автоматически',
          text: 'Число отходящих линий изменено с 27 на 30.',
          evidence: {},
          has_evidence: true,
          navigation: {kind: 'CHANGE', target_id: 'change-automatic'},
        }],
      },
      {
        section_id: 'equipment', title: 'Изменения по оборудованию и фидерам',
        groups: [{
          group_id: 'group-xm1', title: 'ХМ1 — холодильная машина',
          items: [{
            item_id: 'automatic-2', status: 'Найдено автоматически',
            text: 'ХМ1: расчётная активная мощность увеличена с 157,5 до 335 кВт.',
            evidence: {
              LEFT: {page_index: 0, bbox: [1, 2, 3, 4]},
              RIGHT: {page_index: 0, bbox: [5, 6, 7, 8]},
            },
            navigation: {kind: 'CHANGE', target_id: 'change-xm1'},
          }],
        }],
      },
      {section_id: 'ai_verified', title: 'Уточнено ИИ и проверено правилами', items: []},
      {
        section_id: 'inconsistencies', title: 'Внутренние противоречия документа',
        items: [{
          item_id: 'inconsistency-1', status: 'Внутреннее противоречие документа',
          text: 'На листе указан невозможный коэффициент мощности.',
          evidence: {RIGHT: {bbox: [1, 2, 3, 4]}},
          navigation: {kind: 'DOCUMENT_INCONSISTENCY', target_id: 'inconsistency-1'},
        }],
      },
      {
        section_id: 'review', title: 'Что требует проверки инженера',
        items: [{
          item_id: 'review-1', status: 'Требуется проверка инженера',
          text: 'Значения относятся к разным расчётным режимам. '
            + 'Прямое изменение автоматически не подтверждено.',
          evidence: {
            LEFT: {page_index: 0, bbox: [1, 2, 3, 4]},
            RIGHT: {page_index: 0, bbox: [5, 6, 7, 8]},
          },
          navigation: {kind: 'CHANGE', target_id: 'change-review'},
        }],
      },
      {
        section_id: 'unproven', title: 'Что система не смогла доказать',
        items: [{
          item_id: 'unproven-1', status: 'Недостаточно доказательств',
          text: 'Для строки не найдена доказанная пара на другом листе.',
          evidence: {}, navigation: {kind: 'NOT_COMPARABLE', target_id: ''},
        }],
      },
    ],
    ...overrides,
  };
}

function productionPayload(preliminary = report()) {
  const state = {
    status: 'COMPLETED', run_id: 'run-current', input_mode: 'DOCUMENT',
    selection: {input_mode: 'DOCUMENT'},
    stages: {
      review_questions: {status: 'NEEDS_REVIEW', total: 1, pending: 1, answered: 0},
      unified_synthesis: {status: 'COMPLETED', changes: 1, review_items: 1},
      engineer_decisions: {
        status: 'NEEDS_REVIEW',
        counts: {total: 1, PENDING_REVIEW: 1, APPROVED: 0, REJECTED: 0},
      },
      final_report: {status: 'NEEDS_REVIEW', approved: 0},
    },
  };
  return {
    state,
    active_pair: {left: {filename: 'П.pdf'}, right: {filename: 'РД.pdf'}},
    selected_mode: 'DOCUMENT',
    questions: {questions: [{question_id: 'q1', category: 'CHANGE', prompt: 'Уточнить?'}]},
    changes: {rows: [{
      target_id: 'change-review', target_kind: 'CHANGE',
      change: {
        change_id: 'change-review', before_value: '1', after_value: '2',
        review_status: 'REVIEW_REQUIRED', source_mode: 'TEXT',
      },
      engineer_decision: {decision: 'PENDING_REVIEW'},
    }]},
    preliminary_report: preliminary,
    preliminary_opened: false,
    final_report: {
      approved_atomic_changes: [], summary: {approved: 0},
      constraints: {approved_only: true},
    },
  };
}

describe('Stage Comparison preliminary report presentation', () => {
  it('normalizes the backend report into exactly four Russian product sections', () => {
    const normalized = review.normalizePreliminaryReport(report(), {status: 'COMPLETED'});

    expect(normalized.state).toBe('READY');
    expect(normalized.sections.map(section => section.title)).toEqual([
      'Автоматически найденные изменения',
      'Требуется проверка инженера',
      'Внутренние противоречия документа',
      'Недостаточно доказательств',
    ]);
    expect(normalized.sections[0].subsections[1].groups[0].title)
      .toBe('ХМ1 — холодильная машина');
    expect(normalized.sections[0].subsections[0].items[0]).toMatchObject({
      has_evidence: true,
      evidence: {},
      navigation: {kind: 'CHANGE', target_id: 'change-automatic'},
    });
    expect(normalized.sections[0].subsections[1].groups[0].items[0].has_evidence).toBe(true);
    expect(normalized.sections[1].subsections[0].items[0]).toMatchObject({
      can_review: true,
      navigation: {kind: 'CHANGE', target_id: 'change-review'},
    });
  });

  it('uses backend evidence availability and never exposes a false button', () => {
    const payload = report();
    payload.sections[0].items[0] = {
      ...payload.sections[0].items[0],
      has_evidence: false,
      evidence: {LEFT: {page_index: 0, bbox: [1, 2, 3, 4]}},
    };

    const item = review.normalizePreliminaryReport(
      payload, {status: 'COMPLETED'},
    ).sections[0].subsections[0].items[0];

    expect(item.has_evidence).toBe(false);
  });

  it('handles stale, not-ready, partial, running and empty reports honestly', () => {
    expect(review.normalizePreliminaryReport(
      report({stale: true}), {status: 'COMPLETED'},
    )).toMatchObject({state: 'STALE', available: false});
    expect(review.normalizePreliminaryReport(
      {available: false, run_status: 'COMPLETED', sections: []}, {status: 'COMPLETED'},
    )).toMatchObject({state: 'NOT_READY', available: false});
    expect(review.normalizePreliminaryReport(
      report({run_status: 'PARTIAL'}), {status: 'PARTIAL'},
    )).toMatchObject({state: 'PARTIAL', available: true, partial: true});
    expect(review.normalizePreliminaryReport(
      report({run_status: 'RUNNING'}), {status: 'RUNNING'},
    )).toMatchObject({state: 'RUNNING', available: false});

    const empty = report({
      summary: {title: 'Предварительный отчёт анализа', sentences: [], counts: {}},
      sections: [],
    });
    expect(review.normalizePreliminaryReport(empty, {status: 'COMPLETED'}))
      .toMatchObject({state: 'EMPTY', available: true, empty: true});
  });

  it('routes a completed analysis to the preliminary report before questions', () => {
    const payload = productionPayload();
    const pipeline = review.normalizeProductionPipeline(payload);
    const overview = review.normalizeProductionOverview(payload, pipeline);

    expect(overview.state).toBe('NEEDS_REVIEW');
    expect(overview.cta).toEqual({
      kind: 'OPEN_PRELIMINARY_REPORT',
      label: 'Открыть предварительный отчёт',
      disabled: false,
    });

    const afterOpen = review.normalizeProductionOverview(
      {...payload, preliminary_opened: true}, pipeline,
    );
    expect(afterOpen.cta.kind).toBe('CONTINUE_REVIEW');
  });

  it('keeps the final report approved-only', () => {
    const rows = review.normalizeFinalRows({
      approved_atomic_changes: [{
        change_id: 'approved-1', before_value: '1', after_value: '2',
        engineer_decision: {decision: 'APPROVED'},
      }],
      rejected_atomic_changes: [{
        target_id: 'rejected-1',
        change: {change_id: 'rejected-1', before_value: '3', after_value: '4'},
        engineer_decision: {decision: 'REJECTED'},
      }],
      constraints: {approved_only: true},
    });

    expect(rows.map(row => row.target_id)).toEqual(['approved-1']);
  });
});

describe('Stage Comparison preliminary report UI integration', () => {
  it('loads the pair-scoped endpoint together with state and renders the report first', () => {
    expect(app).toContain("'/state', '/changes', '/questions', '/preliminary-report'");
    expect(app).toContain("result.suffix === '/preliminary-report'");
    expect(app).toContain('scApplyProductionPreliminaryReport(result.data)');

    const reportIndex = html.indexOf('id="sc-production-preliminary-report"');
    const questionsIndex = html.indexOf('id="sc-production-questions-stage"');
    const reviewIndex = html.indexOf('id="sc-production-review-table"');
    expect(reportIndex).toBeGreaterThan(-1);
    expect(reportIndex).toBeLessThan(questionsIndex);
    expect(reportIndex).toBeLessThan(reviewIndex);
  });

  it('renders evidence and review navigation without requiring an inline answer', () => {
    const reportBlock = html.slice(
      html.indexOf('id="sc-production-preliminary-report"'),
      html.indexOf('class="sc-production-suggestions"'),
    );
    expect(reportBlock).toContain('Открыть доказательство');
    expect(reportBlock).toContain('scOpenPreliminaryEvidence(item)');
    expect(reportBlock).toContain('Проверить этот пункт');
    expect(reportBlock).toContain('scOpenPreliminaryReviewItem(item)');
    expect(reportBlock).toContain('Перейти к проверке инженером');
    expect(reportBlock).not.toContain('v-model');
    expect(reportBlock).not.toContain('<textarea');
    expect(app).toContain("return_anchor: 'sc-production-preliminary-report'");
    const returnFunction = app.slice(
      app.indexOf('async function scReturnToProductionReviewRow()'),
      app.indexOf('function scCloseProductionEvidence'),
    );
    expect(returnFunction.indexOf("destination.anchor === 'sc-production-preliminary-report'"))
      .toBeLessThan(returnFunction.indexOf("querySelectorAll('[data-production-target-id]')"));
  });

  it('shows only Russian product labels and keeps internal codes out of the report', () => {
    const reportBlock = html.slice(
      html.indexOf('id="sc-production-preliminary-report"'),
      html.indexOf('class="sc-production-suggestions"'),
    );
    for (const label of [
      'Автоматически найдено', 'Требует проверки инженера',
      'Внутренние противоречия документа', 'Недостаточно доказательств',
    ]) expect(reportBlock).toContain(label);
    for (const code of [
      'MATERIAL_CHANGE', 'REVIEW_REQUIRED', 'UNKNOWN_DIMENSION',
      'MODE_MISMATCH', 'PENDING_REVIEW', 'question_id', 'project_entity_ref',
    ]) expect(reportBlock).not.toContain(code);
    expect(reportBlock).not.toContain('>target_id<');
  });

  it('is an integration-style state + report flow with live backend counts', () => {
    const payload = productionPayload(report({
      summary: {
        title: 'Предварительный отчёт анализа', sentences: [],
        counts: {automatic: 41, review: 44, inconsistency: 11, unproven: 20},
      },
    }));
    const presentation = review.normalizePreliminaryReport(
      payload.preliminary_report, payload.state,
    );
    const overview = review.normalizeProductionOverview(
      payload, review.normalizeProductionPipeline(payload),
    );

    expect(presentation.counts).toEqual({
      automatic: 41, review: 44, inconsistencies: 11, unproven: 20,
    });
    expect(presentation.sections).toHaveLength(4);
    expect(overview.cta.label).toBe('Открыть предварительный отчёт');
  });
});
