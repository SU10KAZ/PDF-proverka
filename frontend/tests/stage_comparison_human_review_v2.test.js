import {createRequire} from 'node:module';
import {readFileSync} from 'node:fs';
import {describe, expect, it} from 'vitest';

const require = createRequire(import.meta.url);
const review = require('../static/js/stage-comparison-review.js');
const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');

function humanReviewPayload() {
  return {
    available: true,
    stale: false,
    input_signature: 'sha256:current',
    revision: 2,
    summary: {
      interactions_total: 6,
      interactions_answered: 1,
      interactions_pending: 5,
    },
    review_groups: [{
      group_id: 'hrg_mode_33434a66cf174adbf52396e7',
      title: 'Сопоставимость расчётных режимов',
      question: 'Можно ли считать режимы сопоставимыми?',
      affected_subjects: ['ВРУ-А', 'ВРУ3'],
      mode_sets: {LEFT: ['Рабочий', 'пожарн'], RIGHT: ['Авар. режим', 'ПП режим']},
      allowed_answers: [
        {answer_id: 'NOT_COMPARABLE', label: 'Не сопоставимы'},
        {
          answer_id: 'DECLARE_MODE_MAPPING', label: 'Задать соответствие режимов',
          requires_mapping: true,
          mapping_fields: [{
            left_mode: 'Рабочий', right_mode_choices: ['Авар. режим', 'ПП режим'],
          }],
        },
        {answer_id: 'ADDITIONAL_DOCUMENT_REQUIRED', label: 'Требуется дополнительный документ'},
      ],
      affected_atomic_changes: Array.from({length: 14}, (_, index) => ({
        target_id: `hmode_${index}`,
        subject: index ? 'ВРУ3' : 'ВРУ-А',
        effective_resolution: {decision_source: 'DETERMINISTIC_CANDIDATE'},
      })),
    }],
    standalone_questions: Array.from({length: 5}, (_, index) => ({
      question_id: `question_${index}`,
      title: index === 4 ? 'Выберите строку ВРУ3' : 'Уточните найденное различие',
      question: index === 4 ? 'Какая строка ВРУ3 соответствует строке справа?' : 'Что означает различие?',
      affected_target_ids: [`target_${index}`],
      allowed_answers: index === 4 ? [{
        answer_id: 'SELECT_ROW_PAIR',
        left_row_ids: ['left-a', 'left-b'],
        right_row_ids: ['right-a'],
      }] : [{answer_id: 'CONFIRMED', label: 'Подтверждено'}],
    })),
  };
}

function preliminaryPayload() {
  const section = (section_id, count) => ({
    section_id,
    items: Array.from({length: count}, (_, index) => ({
      item_id: `${section_id}-${index}`,
      status: 'Найдено автоматически',
      text: `${section_id} ${index}`,
      has_evidence: section_id !== 'unproven',
      navigation: {kind: 'TEXT_REQUIREMENT_CHANGE', target_id: `${section_id}-${index}`},
    })),
  });
  return {
    available: true,
    stale: false,
    run_status: 'COMPLETED',
    summary: {counts: {
      automatic: 41, ai_verified: 4, review: 6,
      inconsistency: 12, text_requirements: 9, unproven: 19, metadata: 11,
    }},
    sections: [
      section('scheme', 45), section('inconsistencies', 12),
      section('text_requirements', 9), section('review', 6),
      section('unproven', 19), section('metadata_changes', 11),
    ],
  };
}

describe('AI Analyst v2 human review production UX', () => {
  it('normalizes exactly one group plus five standalone interactions while preserving 14 atoms', () => {
    const normalized = review.normalizeHumanReview(humanReviewPayload());

    expect(normalized.available).toBe(true);
    expect(normalized.summary).toEqual({total: 6, answered: 1, pending: 5});
    expect(normalized.review_groups).toHaveLength(1);
    expect(normalized.review_groups[0].interaction_id)
      .toBe('hrg_mode_33434a66cf174adbf52396e7');
    expect(normalized.review_groups[0].atoms).toHaveLength(14);
    expect(normalized.standalone_questions).toHaveLength(5);
    expect(normalized.standalone_questions[4].options[0]).toMatchObject({
      answer_id: 'SELECT_ROW_PAIR',
      label: 'Выбрать соответствующие строки',
      left_row_ids: ['left-a', 'left-b'],
      right_row_ids: ['right-a'],
    });
  });

  it('moves the six clarifications out of the preliminary report into a separate UI stage', () => {
    const human = review.normalizeHumanReview(humanReviewPayload());
    const normalized = review.normalizePreliminaryReport(
      preliminaryPayload(), {status: 'COMPLETED'}, human,
    );

    expect(normalized.sections.map(section => [section.title, section.count])).toEqual([
      ['Автоматически найденные изменения', 45],
      ['Внутренние противоречия документа', 12],
      ['Новые технические требования', 9],
      ['Не удалось сравнить', 19],
      ['Изменения оформления и штампа', 11],
    ]);
    expect(normalized.sections.at(-1).collapsed).toBe(true);
    expect(normalized.sections.some(section => section.id === 'review')).toBe(false);
  });

  it('renders explicit clarification/final-review separation and version-bound saves', () => {
    expect(html).toContain('id="sc-human-review-orchestrator"');
    expect(html).toContain('Это уточняющие вопросы анализа');
    expect(html).toContain('Финальная инженерная проверка результатов');
    expect(html).toContain('scHumanReview.available');
    expect(app).toContain("'/human-review'");
    expect(app).toContain("'/human-review/answers'");
    expect(app).toContain('expected_input_signature: scHumanReview.value.input_signature');
    expect(app).toContain('expected_revision: scHumanReview.value.revision');
    expect(app).toContain('human_review: scProductionHumanReview.value');
  });

  it('shows an auditable AI-closed question with evidence and human reopen', () => {
    const payload = humanReviewPayload();
    payload.summary = {
      interactions_total: 5, interactions_answered: 0, interactions_pending: 5,
    };
    payload.standalone_questions = payload.standalone_questions.slice(0, 4);
    payload.closed_questions = [{
      question_id: 'question-npe',
      title: 'Эквивалентность требования по шинам N и PE',
      question: 'То же ли это требование?',
      affected_target_ids: ['target-npe'],
      history_message: 'Вопрос снят автоматически: система сравнила ограниченный набор вариантов.',
      can_reopen: true,
      closure: {
        selected_candidate_type: 'DIFFERENT_REQUIREMENT',
        verifier_status: 'VERIFIED_SELECTION',
        two_pass_unanimous: true,
        evidence_refs: ['LEFT:TEXT:pe', 'RIGHT:TEXT:npe'],
      },
    }];
    const normalized = review.normalizeHumanReview(payload);

    expect(normalized.summary.pending).toBe(5);
    expect(normalized.closed_questions).toHaveLength(1);
    expect(normalized.closed_questions[0]).toMatchObject({
      interaction_id: 'question-npe',
      selected_candidate_type: 'DIFFERENT_REQUIREMENT',
      verifier_status: 'VERIFIED_SELECTION',
      two_pass_unanimous: true,
      can_reopen: true,
    });
    expect(html).toContain('История автоматического снятия');
    expect(html).toContain('Посмотреть основание');
    expect(html).toContain('Вернуть инженеру');
    expect(app).toContain("answer: {answer_id: 'REOPEN_FOR_HUMAN'}");
  });

  it('uses HRO interactions instead of the hidden raw queue in pipeline counters', () => {
    const payload = {
      state: {status: 'COMPLETED', stages: {
        review_questions: {status: 'NEEDS_REVIEW', total: 23, pending: 23},
        human_review: {status: 'NEEDS_REVIEW', total: 6, answered: 1, pending: 5},
        unified_synthesis: {status: 'COMPLETED', changes: 54, review_items: 23},
        engineer_decisions: {
          status: 'READY',
          counts: {total: 77, APPROVED: 0, REJECTED: 0, PENDING_REVIEW: 77},
        },
      }},
      questions: {
        questions: Array.from({length: 23}, (_, index) => ({
          question_id: `raw-${index}`, category: 'ENTITY', status: 'PENDING',
        })),
        counts: {total: 23, pending: 23, ENTITY: 23},
      },
      human_review: humanReviewPayload(),
      changes: {rows: []},
      preliminary_opened: true,
    };
    const questions = review.normalizeProductionPipeline(payload)[4];
    const overview = review.normalizeProductionOverview(payload);

    expect(questions).toMatchObject({
      status: 'NEEDS_REVIEW', pending: 5,
      action: {destination: {anchor: 'sc-human-review-orchestrator'}},
    });
    expect(questions.counters).toEqual([
      {label: 'Группы', value: 1},
      {label: 'Отдельные', value: 5},
      {label: 'Ответы', value: '1 / 6'},
    ]);
    expect(overview.detail_lines).toContain('Требуется ответить на вопросы: 5.');
    expect(overview.detail_lines).not.toContain('Требуется ответить на вопросы: 23.');
  });
});
