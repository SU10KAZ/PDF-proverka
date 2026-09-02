import {createRequire} from 'node:module';
import {readFileSync} from 'node:fs';
import {describe, expect, it} from 'vitest';

const require = createRequire(import.meta.url);
const review = require('../static/js/stage-comparison-review.js');
const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');

function matchState({confidence = 'high', links = [], unmatched = false} = {}) {
  return {
    suggestions: {
      left_sheet_index: [{pdf_page: 1}, {pdf_page: 2}],
      right_sheet_index: [{pdf_page: 11}, {pdf_page: 12}],
      suggestions: [
        {
          left_page: 1,
          primary_right_page: 11,
          primary_right_pages: [11],
          confidence,
          reason: ['same_sheet_number'],
        },
        {
          left_page: 2,
          primary_right_page: unmatched ? null : 12,
          primary_right_pages: unmatched ? [] : [12],
          confidence: unmatched ? 'unmatched' : confidence,
          reason: [],
        },
      ],
    },
    links: {links, unlinked_left_pages: []},
  };
}

describe('единая кнопка запуска сравнения', () => {
  it('открывает выбор из трёх режимов', () => {
    expect(html).toContain('@click="scOpenComparisonLaunchDialog(row)">Запустить сравнение</button>');
    expect(html).not.toContain('Сопоставить листы');
    expect(html).toContain('v-for="mode in scComparisonLaunchModes"');
    expect(app).toContain("{code: 'FAST', label: 'Быстро'}");
    expect(app).toContain("{code: 'STANDARD', label: 'Стандартно'}");
    expect(app).toContain("{code: 'DEEP', label: 'Глубоко', disabled: true, note: 'В разработке'}");
    expect(html).toContain('<span v-if="mode.note">{{ mode.note }}</span>');
    expect(app).toContain('&& !launchMode.disabled');
    expect(html).not.toContain('Production-расхождения П ↔ РД');
    expect(html).not.toContain('Сравнение редакций: слева → справа');
    expect(html).not.toContain('Атомарные изменения: одна строка');
  });

  it('передаёт выбранный ai_mode и запускает production pipeline', () => {
    const start = app.slice(
      app.indexOf('async function scStartComparisonLaunch(aiMode)'),
      app.indexOf('async function scProcessCurrentSelection()'),
    );
    const resume = app.slice(
      app.indexOf('async function scResumeComparisonLaunch(options)'),
      app.indexOf('async function scConfirmComparisonSheetMap()'),
    );
    expect(start).toContain('scProductionAiMode.value = mode');
    expect(start).toContain('await scResumeComparisonLaunch');
    expect(resume).toContain('await scRunProductionComparison');
  });

  it('при однозначном auto-match сохраняет карту и продолжает автоматически', () => {
    const plan = review.comparisonLaunchPlan(matchState());
    expect(plan.action).toBe('AUTO_ACCEPT');
    expect(plan.links).toHaveLength(2);
    expect(plan.links.every(link => link.source === 'auto')).toBe(true);
    expect(app).toContain("if (plan.action === 'AUTO_ACCEPT')");
    expect(app).toContain('await scResumeComparisonLaunch({force: true, matchState: saved})');
  });

  it('при неоднозначном сопоставлении останавливается для выбора пользователя', () => {
    expect(review.comparisonLaunchPlan(matchState({confidence: 'medium'})).action)
      .toBe('REVIEW_REQUIRED');
    expect(review.comparisonLaunchPlan(matchState({unmatched: true})).action)
      .toBe('REVIEW_REQUIRED');
    expect(html).toContain('Нужно уточнить соответствие листов');
  });

  it('после ручного выбора продолжает тот же pipeline', () => {
    const state = matchState({confidence: 'medium', links: [
      {left_pages: [1], right_pages: [11], source: 'manual'},
      {left_pages: [2], right_pages: [12], source: 'manual'},
    ]});
    expect(review.comparisonLaunchDecisionsComplete(state)).toBe(true);
    expect(app).toContain('SC_PRODUCTION_REVIEW.comparisonLaunchDecisionsComplete(data)');
    expect(html).toContain('@click="scConfirmComparisonSheetMap()"');
  });

  it('переиспользует сохранённое сопоставление без повторного вопроса', () => {
    const state = matchState({links: [
      {left_pages: [1], right_pages: [11], source: 'manual'},
    ]});
    expect(review.comparisonLaunchPlan(state).action).toBe('USE_SAVED');
    expect(app).toContain("if (plan.action !== 'USE_SAVED')");
    expect(app).toContain("if (plan.action === 'USE_SAVED')");
  });
});
