import {createRequire} from 'node:module';
import {readFileSync} from 'node:fs';
import {describe, expect, it} from 'vitest';

const require = createRequire(import.meta.url);
const review = require('../static/js/stage-comparison-review.js');
const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');

function row(id, overrides = {}) {
  return {
    target_id: id,
    target_kind: 'CHANGE',
    presentation_group_id: overrides.group_id || null,
    presentation_group: overrides.group_id ? {
      group_id: overrides.group_id,
      title: 'Изменены параметры объекта',
      creates_engineering_fact: false,
    } : null,
    presentation: {
      object_label: overrides.object_label,
      object_known: overrides.object_known !== false,
      property_label: overrides.property_label,
      before_display: overrides.before_display,
      after_display: overrides.after_display,
      unit: overrides.unit,
      detail: overrides.detail || null,
    },
    change: {
      change_id: id,
      subject_ref: overrides.subject_ref || `graphic_subject_${id}`,
      project_entity_ref: null,
      facet_ref: overrides.facet_ref || 'rated_current_a',
      dimension: 'PARAMETER',
      direction: 'INCREASED',
      before_value: overrides.before ?? 2500,
      after_value: overrides.after ?? 3200,
      source_mode: overrides.source_mode || 'GRAPHIC',
      review_status: 'CONFIRMED',
      provenance: {source_atoms: [{provenance: {locations: {
        LEFT: [{page: 1}], RIGHT: [{page: 1}],
      }}}]},
    },
    engineer_decision: {decision: 'PENDING_REVIEW'},
  };
}

describe('финальная UX-доводка Engineer Review', () => {
  it('показывает human object/facet labels и единицы, а raw ref оставляет диагностике', () => {
    const [normalized] = review.normalizeRows({rows: [row('qf1', {
      subject_ref: 'graphic_subject_564d5d28101da84f0a1b',
      object_label: 'Вводной выключатель секции 1',
      property_label: 'Номинальный ток увеличен',
      before_display: '2500 А',
      after_display: '3200 А',
      unit: 'А',
    })]});

    expect(normalized.object_ref).toBe('Вводной выключатель секции 1');
    expect(normalized.change_label).toBe('Номинальный ток увеличен');
    expect(normalized.before).toBe('2500 А');
    expect(normalized.after).toBe('3200 А');
    expect(JSON.stringify({
      object: normalized.object_ref,
      change: normalized.change_label,
      before: normalized.before,
      after: normalized.after,
    })).not.toContain('graphic_subject_');
    expect(normalized.object_diagnostic).toBe('graphic_subject_564d5d28101da84f0a1b');
  });

  it('использует честный русский fallback неизвестного объекта', () => {
    const [normalized] = review.normalizeRows({rows: [{
      ...row('unknown', {object_known: false}),
      presentation: {
        object_label: null,
        object_known: false,
        property_label: 'Свойство не удалось однозначно определить',
      },
    }]});

    expect(normalized.object_ref).toBe('Не удалось однозначно определить объект');
    expect(normalized.object_ref).not.toContain('graphic');
    expect(html).not.toContain('Объект не назван');

    const [legacy] = review.normalizeRows({rows: [{
      ...row('legacy-raw'),
      change: {
        ...row('legacy-raw').change,
        object_label: 'graphic.subject.3da7deadbeef',
      },
      presentation: null,
    }]});
    expect(legacy.object_ref).toBe('Не удалось однозначно определить объект');
  });

  it('очеловечивает entity question и варианты, сохраняя refs в диагностике', () => {
    const rows = review.normalizeRows({rows: [
      row('left', {
        subject_ref: 'graphic_subject_left', object_label: 'ВРУ-ХЦ, ввод 1',
        property_label: 'Расчётный ток увеличен', before_display: '100 А', after_display: '120 А',
      }),
      row('right', {
        subject_ref: 'graphic_subject_right', object_label: 'Шкаф управления холодильным центром',
        property_label: 'Расчётный ток увеличен', before_display: '100 А', after_display: '120 А',
      }),
    ]});
    const [question] = review.normalizeQuestions({questions: [{
      question_id: 'hquestion_raw',
      category: 'ENTITY',
      question_type: 'ENTITY_IDENTITY',
      prompt: '«graphic.subject.left» слева и «graphic.subject.right» справа — один объект?',
      answer_options: [
        {code: 'YES', label: 'Да'}, {code: 'NO', label: 'Нет'},
        {code: 'UNSURE', label: 'Не уверен'},
      ],
      context: {
        left_entity_ref: 'graphic_subject_left',
        candidate_relations: [{right_entity_ref: 'graphic_subject_right'}],
      },
    }]}, rows);

    expect(question.prompt).toBe('Система не смогла однозначно сопоставить эти объекты.');
    expect(question.left_object_label).toBe('ВРУ-ХЦ, ввод 1');
    expect(question.right_object_label).toBe('Шкаф управления холодильным центром');
    expect(question.entity_question).toBe('Это один и тот же функциональный объект?');
    expect(question.options.map(option => option.label)).toEqual([
      'Да, это один объект', 'Нет, разные объекты', 'Не уверен',
    ]);
    expect(JSON.stringify({
      prompt: question.prompt,
      left: question.left_object_label,
      right: question.right_object_label,
      options: question.options,
    })).not.toContain('graphic.subject');
    expect(question.diagnostic_refs).toEqual([
      'hquestion_raw', 'graphic_subject_left', 'graphic_subject_right',
    ]);
  });

  it('ведёт из Preliminary Report в точный target с тем же human label', () => {
    const rows = review.normalizeRows({rows: [row('uchg_qf1', {
      object_label: 'Вводной выключатель секции 1',
      property_label: 'Номинальный ток увеличен',
      before_display: '2500 А', after_display: '3200 А',
    })]});
    const item = {
      text: 'Вводной выключатель секции 1: номинальный ток увеличен с 2500 до 3200 А.',
      navigation: {kind: 'CHANGE', target_id: 'uchg_qf1'},
    };

    const target = review.reviewTargetForPreliminary(item, rows);
    expect(target.target_id).toBe('uchg_qf1');
    expect(item.text).toContain(target.object_ref);
    expect(html).toContain("'is-return-target': scProductionReturnTargetId === row.target_id");
  });

  it('группирует режимные параметры только визуально и сохраняет 77 атомарных решений', () => {
    const rows = review.normalizeRows({rows: [
      row('mode-power', {
        group_id: 'pgroup_vru1', object_label: 'ВРУ1',
        property_label: 'Расчётная мощность увеличена',
        before_display: '181,8 кВт', after_display: '223,2 кВт', detail: 'ввод 1',
      }),
      row('mode-current', {
        group_id: 'pgroup_vru1', object_label: 'ВРУ1',
        property_label: 'Расчётный ток увеличен',
        before_display: '314,4 А', after_display: '365,8 А', detail: 'ввод 1',
      }),
      ...Array.from({length: 75}, (_, index) => row(`single-${index}`, {
        object_label: `Объект ${index + 1}`,
        property_label: 'Номинальный ток увеличен',
        before_display: '10 А', after_display: '16 А',
      })),
    ]});
    const groups = review.reviewGroups(rows);
    const modeGroup = groups.find(group => group.key === 'group:pgroup_vru1');

    expect(rows).toHaveLength(77);
    expect(review.reviewCounts(rows).total).toBe(77);
    expect(groups.flatMap(group => group.rows)).toHaveLength(77);
    expect(modeGroup.label).toBe('ВРУ1 · ввод 1');
    expect(modeGroup.rows.map(item => item.change_label)).toEqual([
      'Расчётная мощность увеличена', 'Расчётный ток увеличен',
    ]);
    expect(html).toContain('Параметры сгруппированы только визуально');
  });
});
