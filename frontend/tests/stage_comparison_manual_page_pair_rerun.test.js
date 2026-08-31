import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { describe, expect, it } from 'vitest';

// Инженер вручную собрал пару «страница ↔ страница», а раздел продолжил
// показывать прогон двухдневной давности как текущий: «Завершено: 48 ч назад»
// и главная кнопка «Продолжить проверку», ведущая в чужие 26 вопросов и
// 27 изменений. Здесь закреплено, что изменившаяся пара — отдельное
// состояние главной кнопки, а не разновидность «есть что проверить».

const require = createRequire(import.meta.url);
const review = require('../static/js/stage-comparison-review.js');
const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');

const NOW = Date.parse('2026-08-30T09:32:36.000Z');

function pair() {
  return {id: 'pair-1', left: {filename: 'LEFT.pdf'}, right: {filename: 'RIGHT.pdf'}};
}

// Завершённый прогон с непроверенной человеком работой: ровно то состояние,
// в котором боевая пара выдавала «Продолжить проверку».
function completedRun(overrides = {}) {
  return {
    status: 'COMPLETED',
    run_id: 'prun_ba92aa1658c760a8b97ee2dc',
    started_at: '2026-08-28T09:32:33.000Z',
    completed_at: '2026-08-28T09:32:36.000Z',
    selection: {input_mode: 'PAGE', left_pages: [52], right_pages: [21]},
    stages: {
      review_questions: {status: 'COMPLETED', questions: 26, answered: 0},
      unified_synthesis: {status: 'COMPLETED', changes: 4, review_items: 23},
      engineer_decisions: {
        status: 'READY', counts: {total: 27, APPROVED: 0, REJECTED: 0, PENDING_REVIEW: 27},
      },
      final_report: {status: 'READY', approved: 0},
    },
    ...overrides,
  };
}

function overview(payload = {}) {
  return review.normalizeProductionOverview({
    active_pair: pair(),
    now_ms: NOW,
    selected_mode: 'PAGE',
    selected_pages: {left: [52], right: [21]},
    ...payload,
  });
}

describe('Повторный анализ после ручной пересборки пары страниц', () => {
  it('B: прогон соответствует открытой паре — «Продолжить проверку»', () => {
    const result = overview({state: completedRun()});

    expect(result.state).toBe('NEEDS_REVIEW');
    expect(result.selection_changed).toBe(false);
    expect(result.cta).toEqual({
      kind: 'CONTINUE_REVIEW',
      label: 'Продолжить проверку',
      disabled: false,
      destination: expect.anything(),
    });
  });

  it('C: открыта другая пара страниц — «Повторить анализ», а не «Продолжить»', () => {
    const result = overview({
      selected_pages: {left: [52], right: [30]},
      state: completedRun(),
    });

    expect(result.state).toBe('SELECTION_CHANGED');
    expect(result.selection_changed).toBe(true);
    expect(result.headline).toBe('Пара страниц изменена. Анализ нужно выполнить заново.');
    expect(result.cta).toEqual({
      kind: 'RERUN', label: '↻ Повторить анализ', disabled: false,
    });
    expect(result.detail_lines).toContain(
      'Сейчас открыта пара: слева стр. 52, справа стр. 30.',
    );
    expect(result.detail_lines).toContain(
      'Прошлый анализ выполнен для пары: слева стр. 52, справа стр. 21.',
    );
  });

  it('C: бэкенд сообщил о ручной пересборке — то же состояние без сдвига страниц', () => {
    const result = overview({
      state: completedRun({stale: true, stale_reason: 'MANUAL_PAGE_PAIRING_CHANGED'}),
    });

    expect(result.state).toBe('SELECTION_CHANGED');
    expect(result.cta.label).toBe('↻ Повторить анализ');
    expect(result.detail_lines).toContain(
      'Ручное сопоставление страниц изменилось после прошлого анализа.',
    );
  });

  it('F/G: прошлые вопросы и изменения не выдаются за результат новой пары', () => {
    const result = overview({
      selected_pages: {left: [52], right: [30]},
      state: completedRun(),
    });

    expect(result.detail_lines).toContain(
      'Показанные вопросы и изменения относятся к прошлой паре и результатом текущей не являются.',
    );
    // «Требуется ответить на вопросы: 26» больше не подаётся как работа по
    // текущей паре: сначала пересчёт.
    expect(result.detail_lines.some(line => line.startsWith('Требуется'))).toBe(false);
  });

  it('§8: возраст прошлого прогона подписан как прошлый, а не как «Завершено»', () => {
    const changed = overview({
      selected_pages: {left: [52], right: [30]},
      state: completedRun(),
    });
    const unchanged = overview({state: completedRun()});

    // Именно та строка, которую видел инженер: «Завершено: 48 ч назад».
    expect(changed.detail_lines).toContain('Прошлый анализ завершён: 48 ч назад.');
    // Для своей же пары подпись прежняя: это и есть текущий результат.
    expect(unchanged.detail_lines).toContain('Завершено: 48 ч назад.');
  });

  it('устаревание по документам остаётся отдельной причиной', () => {
    const result = overview({state: completedRun({stale: true})});

    expect(result.state).not.toBe('SELECTION_CHANGED');
    expect(result.headline).toBe('Результат анализа устарел.');
    expect(result.cta.label).toBe('↻ Запустить анализ заново');
  });

  it('молчание интерфейса о выбранных страницах не считается другой парой', () => {
    const result = overview({selected_pages: {}, state: completedRun()});

    expect(result.selection_changed).toBe(false);
    expect(result.cta.kind).toBe('CONTINUE_REVIEW');
  });

  it('до первого прогона расхождения не бывает', () => {
    const result = overview({
      selected_pages: {left: [52], right: [30]},
      state: {status: 'NOT_STARTED', stages: {}},
    });

    expect(result.state).toBe('NOT_STARTED');
    expect(result.cta.label).toBe('▶ Запустить полный анализ');
  });

  it('режим документов расхождением страниц не управляется', () => {
    const result = overview({
      selected_mode: 'DOCUMENT',
      selected_pages: {left: [52], right: [30]},
      state: completedRun({selection: {input_mode: 'DOCUMENT', left_pages: [], right_pages: []}}),
    });

    expect(result.selection_changed).toBe(false);
  });

  it('D: главная кнопка ведёт в новый прогон, а не в старую очередь проверки', () => {
    expect(html).toContain('{{ scProductionOverview.cta.label }}');
    expect(html).toContain('@click="scHandleProductionPrimaryAction()"');
    const handler = app.slice(
      app.indexOf('async function scHandleProductionPrimaryAction()'),
      app.indexOf('async function scOpenProductionQuestions'),
    );
    expect(handler).toContain("cta.kind === 'RERUN'");
    expect(handler).toContain('scRunProductionComparison');
  });

  it('изменившаяся пара закрывает записи инженера и объясняет причину', () => {
    expect(app).toContain('scProductionNeedsNewAnalysis.value || [');
    expect(html).toContain('v-else-if="scProductionSelectionChanged"');
    expect(html).toContain('Пара страниц изменена после анализа.');
  });
});

describe('Повторный анализ после смены глубины', () => {
  function fastRun(overrides = {}) {
    return completedRun({
      analysis_config: {ai_mode: 'FAST', recorded: true},
      selection: {
        input_mode: 'PAGE', left_pages: [52], right_pages: [21], ai_mode: 'FAST',
      },
      ...overrides,
    });
  }

  it.each([
    ['STANDARD', 'Стандартно'],
    ['DEEP', 'Глубокая проверка'],
  ])('FAST → %s требует новый анализ', (selectedMode, selectedLabel) => {
    const result = overview({
      selected_ai_mode: selectedMode,
      selected_ai_mode_changed: true,
      state: fastRun(),
    });

    expect(result.state).toBe('ANALYSIS_MODE_CHANGED');
    expect(result.analysis_mode_changed).toBe(true);
    expect(result.needs_new_analysis).toBe(true);
    expect(result.headline).toBe('Выбрана другая глубина. Нужен новый анализ.');
    expect(result.detail_lines).toContain('Текущий результат рассчитан в режиме «Быстро».');
    expect(result.detail_lines).toContain(`Выбран режим «${selectedLabel}».`);
    expect(result.cta).toEqual({
      kind: 'RERUN', label: 'Повторить анализ', disabled: false,
    });
  });

  it('не меняет состояние, пока выбран режим готового результата', () => {
    const result = overview({
      selected_ai_mode: 'FAST', selected_ai_mode_changed: true, state: fastRun(),
    });

    expect(result.analysis_mode_changed).toBe(false);
    expect(result.needs_new_analysis).toBe(false);
    expect(result.cta.kind).toBe('CONTINUE_REVIEW');
  });

  it('программная инициализация селектора не считается сменой инженером', () => {
    const result = overview({
      selected_ai_mode: 'STANDARD',
      selected_ai_mode_changed: false,
      state: fastRun(),
    });

    expect(result.analysis_mode_changed).toBe(false);
    expect(result.cta.kind).toBe('CONTINUE_REVIEW');
  });

  it('не приписывает глубину старому прогону, где она не записана', () => {
    const result = overview({
      selected_ai_mode: 'DEEP',
      selected_ai_mode_changed: true,
      state: fastRun({
        analysis_config: {ai_mode: null, recorded: false},
        selection: {input_mode: 'PAGE', left_pages: [52], right_pages: [21]},
      }),
    });

    expect(result.analysis_mode_changed).toBe(false);
    expect(result.cta.kind).toBe('CONTINUE_REVIEW');
  });

  it('передаёт выбранную глубину в сводку и показывает новый статус', () => {
    expect(app).toContain('selected_ai_mode: scProductionAiMode.value');
    expect(app).toContain('selected_ai_mode_changed: scProductionAiModeChangedByUser.value');
    expect(html).toContain('@change="scOnProductionAiModeChange()"');
    expect(html).toContain("scProductionNeedsNewAnalysis ? 'Нужен новый анализ' : 'Текущий анализ'");
    expect(html).toContain('class="sc-production-pipeline__stale">нужен новый анализ</span>');
  });
});
