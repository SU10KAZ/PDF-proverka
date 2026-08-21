import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const require = createRequire(import.meta.url);
const ledger = require('../static/js/stage-comparison-differences.js');
const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const css = readFileSync(new URL('../static/css/styles.css', import.meta.url), 'utf8');

function item(status, index = 1) {
  return {
    final_status: status,
    deterministic_status: status,
    summary: `${status} ${index}`,
    before: `П ${index}`,
    after: `РД ${index}`,
    reason: `Причина ${index}`,
    left_pages: [index],
    right_pages: [index + 10],
    left_anchors: [{page: index, bboxes: [{x: 0.1, y: 0.2, width: 0.3, height: 0.04}]}],
    right_anchors: [{page: index + 10, bboxes: []}],
  };
}

function group(id, leftPages, rightPages, buckets = {}) {
  return {
    id,
    left_pages: leftPages,
    right_pages: rightPages,
    left_labels: leftPages.map(page => `Лист ${page} — П`),
    right_labels: rightPages.map(page => `Лист ${page} — РД`),
    changed: [], removed: [], added: [], uncertain: [],
    review_status: 'ai_reviewed',
    ...buckets,
  };
}

function finalResult(...groups) {
  return {stale: false, sheet_groups: groups};
}

describe('Stage 4 discrepancy ledger', () => {
  it('keeps one sheet-link group as one table row', () => {
    const source = group('g1', [5], [10], {
      changed: [item('CHANGED', 1), item('CHANGED', 2)],
      removed: [item('REMOVED', 3)],
    });
    expect(ledger.buildRows(finalResult(source), null, {})).toEqual([source]);
    expect(html).toContain('v-for="group in scTextDifferenceGroups"');
    expect(css).not.toContain('.sc-differences__pages { display: grid;');
    expect(css).toContain('.sc-differences__pages button {\n    display: block;');
  });

  it('does not create discrepancy rows from SAME or MOVED coverage', () => {
    const coverageOnly = group('coverage', [1], [2]);
    expect(ledger.buildRows(finalResult(coverageOnly), null, {})).toEqual([]);
  });

  it.each([
    ['changed', 'CHANGED'],
    ['removed', 'REMOVED'],
    ['added', 'ADDED'],
    ['uncertain', 'UNCERTAIN'],
  ])('shows the %s bucket without mixing statuses', (bucket, status) => {
    const source = group(bucket, [4], [14], {[bucket]: [item(status)]});
    expect(ledger.buildRows(finalResult(source), null, {filter: bucket})).toHaveLength(1);
    expect(ledger.buildRows(finalResult(source), null, {
      filter: bucket === 'changed' ? 'removed' : 'changed',
    })).toHaveLength(0);
  });

  it('collapses buckets longer than five items', () => {
    const source = group('long', [1], [2], {
      changed: Array.from({length: 7}, (_, index) => item('CHANGED', index + 1)),
    });
    expect(ledger.visibleItems(source, 'changed', false)).toHaveLength(5);
    expect(ledger.remainingCount(source, 'changed', false)).toBe(2);
    expect(html).toContain('+ ещё {{ scTextBucketRemaining(group, bucket) }}');
  });

  it('returns the complete bucket when expanded', () => {
    const source = group('long', [1], [2], {
      added: Array.from({length: 8}, (_, index) => item('ADDED', index + 1)),
    });
    expect(ledger.visibleItems(source, 'added', true)).toHaveLength(8);
    expect(ledger.remainingCount(source, 'added', true)).toBe(0);
  });

  it('keeps a many-to-many sheet link in one sorted row', () => {
    const many = group('many', [5, 6], [10, 11], {changed: [item('CHANGED')]});
    const earlier = group('earlier', [4], [9], {added: [item('ADDED')]});
    const rows = ledger.buildRows(finalResult(many, earlier), null, {});
    expect(rows.map(row => row.id)).toEqual(['earlier', 'many']);
    expect(rows[1].left_pages).toEqual([5, 6]);
    expect(rows[1].right_pages).toEqual([10, 11]);
  });

  it('keeps existing page and bbox source navigation', () => {
    expect(html).toContain("scOpenDifferenceSource(group, item, 'left')");
    expect(html).toContain("scOpenDifferenceSource(group, item, 'right')");
    expect(app).toContain('const anchors = item[`${side}_anchors`] || []');
    expect(app).toContain('view.cx = Number(box.x || 0)');
  });

  it('shows deterministic fallback without claiming AI review', () => {
    expect(html).toContain('Только детерминированная проверка');
    const review = {sheet_groups: [{id: 'g1', status: 'failed', decisions: []}]};
    expect(ledger.groupAiDiagnostics(review, 'g1')).toBeNull();
  });

  it('opens the discrepancy tab without calling a model endpoint', () => {
    const tabButton = html.match(/<button class="project-tab"[^>]+scTab==='diffs'[\s\S]+?<\/button>/)[0];
    expect(tabButton).toContain("@click=\"scTab='diffs'\"");
    expect(tabButton).not.toContain('scRunTextAiReview');
    expect(tabButton).not.toContain('/text-ai-review');
  });

  it('uses factual summaries and keeps uncertainty diagnostics collapsed', () => {
    expect(html).toContain('<summary>{{ item.summary }}</summary>');
    expect(html).toContain('<p v-if="item.reason"><b>Причина:</b> {{ item.reason }}</p>');
    expect(html).toContain('scTextUncertainReasonLabel(item)');
    expect(html).toContain('Показать корректировки ИИ');
  });

  it('filters by source text and labels without changing the artifact', () => {
    const first = group('g1', [1], [2], {changed: [item('CHANGED', 1)]});
    const second = group('g2', [3], [4], {removed: [item('REMOVED', 2)]});
    expect(ledger.buildRows(finalResult(first, second), null, {query: 'П 2'}))
      .toEqual([second]);
  });
});
