import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { describe, expect, it } from 'vitest';

const require = createRequire(import.meta.url);
const review = require('../static/js/stage-comparison-review.js');
const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const css = readFileSync(new URL('../static/css/styles.css', import.meta.url), 'utf8');

function sourceBetween(source, start, end) {
  const from = source.indexOf(start);
  const to = source.indexOf(end, from + start.length);
  expect(from, `missing source marker: ${start}`).toBeGreaterThanOrEqual(0);
  expect(to, `missing source marker: ${end}`).toBeGreaterThan(from);
  return source.slice(from, to);
}

describe('Stage Comparison pipeline controls', () => {
  it('B/C: presents one primary full-analysis CTA wired to POST production/run', () => {
    const overview = review.normalizeProductionOverview({
      active_pair: {left: {}, right: {}},
      state: {status: 'NOT_STARTED', stages: {}},
    });
    expect(overview.cta.label).toBe('▶ Запустить полный анализ');
    expect(html).toContain('{{ scProductionOverview.cta.label }}');
    expect(html).toContain('@click="scHandleProductionPrimaryAction()"');

    const handler = sourceBetween(
      app,
      'async function scHandleProductionPrimaryAction()',
      'async function scOpenProductionQuestions',
    );
    expect(handler).toContain('scRunProductionComparison');
    const runner = sourceBetween(
      app,
      'async function scRunProductionComparison(options)',
      'async function scSaveProductionDecisions',
    );
    expect(runner).toContain("scProductionRequest('/run'");
    expect(runner).toContain("method: 'POST'");
  });

  it('D: disables the full-analysis CTA throughout an active backend run', () => {
    const running = review.normalizeProductionOverview({
      active_pair: {left: {}, right: {}},
      state: {status: 'RUNNING', stages: {}},
    });
    expect(running.cta).toMatchObject({
      kind: 'RUNNING', label: 'Анализ выполняется…', disabled: true,
    });
    expect(html).toContain(
      ':disabled="scProductionOverview.cta.disabled || scProductionMutating || scProductionRunActive"',
    );
  });

  it('E/I: stage-card clicks only toggle details and the ambiguous action is absent', () => {
    expect(html).toContain('@click="scToggleProductionPipeline(stage)"');
    expect(html).not.toContain('@click="scRunProductionComparison(stage');
    expect(html).not.toContain('Перейти к этапу');
    const toggle = sourceBetween(
      app,
      'function scToggleProductionPipeline(stage)',
      'function scProductionPipelineStatusLabel',
    );
    expect(toggle).not.toContain('fetch(');
    expect(toggle).not.toContain('scProductionRequest');
    expect(toggle).not.toContain('scRunProductionComparison');
  });

  it('F: restores the active pair and reloads persisted production state after reload', () => {
    expect(app).toContain('stage-comparison:active-pair:${sessionId}');
    expect(app).toContain('localStorage.setItem(key, String(pair.id))');
    const refresh = sourceBetween(app, 'async function scRefreshSession()', 'async function scLoadObjects');
    expect(refresh).toContain('scStoredActivePairId(data.id)');
    expect(refresh).toContain('await scOpenPair(restoredPair)');
    const activate = sourceBetween(app, 'function scActivatePairData(data)', 'async function scCreatePairForDocuments');
    expect(activate).toContain('void scLoadProductionReview({silent: true})');
    expect(app).toContain('scProductionClock.value = Date.now()');
  });

  it('N/O: renders distinct determinate and indeterminate progress tracks inside stages', () => {
    expect(html).toContain('stage.progress.determinate ? stage.progress.percent : null');
    expect(html).toContain("'is-' + stage.progress.kind");
    expect(css).toContain('.sc-production-progress.is-indeterminate');
    expect(css).toContain('@keyframes sc-production-progress-indeterminate');
  });

  it('U: polls at a two-second active cadence and stops on a terminal state', () => {
    const poll = sourceBetween(
      app,
      'async function scPollProductionState(token, pairId)',
      'function scProductionAuthor',
    );
    expect(poll).toContain('scScheduleProductionPoll(token, pairId, 2000)');
    expect(poll).toContain('scStopProductionPolling()');
    expect(poll.indexOf('scStopProductionPolling()')).toBeLessThan(
      poll.indexOf('await scLoadProductionReview'),
    );
    const stop = sourceBetween(
      app,
      'function scStopProductionPolling()',
      'function scScheduleProductionPoll',
    );
    expect(stop).toContain('clearTimeout(scProductionPollTimer)');
    expect(stop).toContain('clearInterval(scProductionClockTimer)');
  });

  it('keeps an in-flight run scoped to its pair and rejects a preceding generation', () => {
    const runner = sourceBetween(
      app,
      'async function scRunProductionComparison(options)',
      'async function scSaveProductionDecisions',
    );
    expect(runner).toContain('const pairId = scActivePair.value.id');
    expect(runner).toContain('const runToken = ++scProductionRunToken');
    expect(runner.match(/scProductionRunContextCurrent\(runToken, pairId\)/g)).toHaveLength(4);

    const reset = sourceBetween(
      app,
      'function scResetProductionReview()',
      'function scProductionUrl',
    );
    expect(reset).toContain('scProductionRunToken += 1');
    expect(reset).toContain('scProductionPendingRun = null');

    const poll = sourceBetween(
      app,
      'async function scPollProductionState(token, pairId)',
      'function scProductionAuthor',
    );
    expect(poll).toContain('scProductionStatePredatesPendingRun(data, pairId)');
    expect(poll).toContain('scScheduleProductionPoll(token, pairId, 500)');
  });

  it('orders pair/session activation and ignores superseded review loads', () => {
    const refresh = sourceBetween(app, 'async function scRefreshSession()', 'async function scLoadObjects');
    expect(refresh).toContain('const requestToken = ++scSessionRequestToken');
    expect(refresh).toContain('if (requestToken !== scSessionRequestToken) return');
    expect(refresh).toContain('scResetProductionReview()');
    expect(refresh).not.toContain('if (sessionChanged) scForgetActivePair(data.id)');

    const openPair = sourceBetween(app, 'async function scOpenPair(pair)', 'function scPairUrl');
    expect(openPair).toContain('const context = scBeginPairOpen()');
    expect(openPair).toContain('if (!scPairOpenContextCurrent(context)) return');
    expect(openPair).toContain('context.sessionId');

    const load = sourceBetween(
      app,
      'async function scLoadProductionReview(options)',
      'async function scDiscardProductionDrafts',
    );
    expect(load).toContain('const loadToken = ++scProductionLoadToken');
    expect(load).toContain('loadToken === scProductionLoadToken');
    expect(load).toContain("['COMPLETED', 'PARTIAL']");
    expect(load).toContain('terminalRetryAttempt: retryAttempt + 1');
    expect(load).toContain('terminalRetryErrorBaseline: retryErrorBaseline');
    expect(load).toContain('scProductionError.value === retryErrorBaseline');
    expect(load).toContain('retryAttempt < 3');
    expect(load).toContain('preserveDrafts: true');

    const projectSummary = sourceBetween(
      app,
      'async function scRunProjectChangeSummary()',
      'function scOpenDifferenceSource',
    );
    expect(projectSummary).toContain('const pairContext = scActivePairRequestContext()');
    expect(projectSummary.match(/scActivePairRequestContextCurrent\(pairContext\)/g).length)
      .toBeGreaterThanOrEqual(3);

    const undoRepair = sourceBetween(
      app,
      'async function scUndoSheetLinkRepair()',
      'function scToggleSheetMap',
    );
    expect(undoRepair).toContain('scPairRequestUrl(');
    expect(undoRepair).toContain('if (!scActivePairRequestContextCurrent(pairContext)) return');
  });
});
