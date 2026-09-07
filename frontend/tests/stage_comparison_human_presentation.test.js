import {createRequire} from 'node:module';
import {describe, expect, it} from 'vitest';
const require = createRequire(import.meta.url);
const review = require('../static/js/stage-comparison-review.js');

const row = (key, state = 'ACTIONABLE') => ({domain_key: key, question_id: `runtime-${key}`, question_state: state});
const payload = {presentation: {enabled: true, items: [
  {group_id: 'g', item_type: 'QuestionGroup', children: [{domain_key: 'x'}, {domain_key: 'y'}]},
  {item_type: 'AtomicQuestion', domain_key: 'z'},
]}};

describe('Human Presentation consumes domain-bound atomic rows', () => {
  it('renders fresh and unavailable Human Contour questions with null human answers', () => {
    const rows = review.normalizeQuestions({questions: [
      {...row('x'), answer: null, human_answer: null},
      {...row('y', 'UNAVAILABLE'), human_answer: null, answerable: false},
    ]});
    expect(rows.map(row => row.answer)).toEqual(['', '']);
    expect(rows[1].answerable).toBe(false);
  });
  it('expands by DomainKey despite legacy drift, preserves siblings, recomputes filters', () => {
    const rows = [row('x'), row('y'), row('z', 'UNAVAILABLE')];
    let items = review.presentationItems(payload, rows, 'pending');
    expect(items).toHaveLength(1);
    expect(items[0].rows).toEqual(rows.slice(0, 2));
    rows[0] = {...rows[0], question_id: 'new-runtime', question_state: 'RESOLVED'};
    items = review.presentationItems(payload, rows, 'partial');
    expect(items[0].key).toBe('g');
    expect(items[0].rows[1].question_state).toBe('ACTIONABLE');
    expect(review.presentationItems(payload, rows, 'historical')[0].rows).toEqual([rows[2]]);
    expect(review.presentationItems(payload, rows, 'resolved')).toHaveLength(0);
  });
  it('never uses a legacy alias to bind a mismatched DomainKey or hides an orphan', () => {
    const rows = [row('w')];
    const stale = {presentation: {enabled: true, items: [{group_id: 'g',
      children: [{domain_key: 'old', legacy_id: 'runtime-w'}]}]}};
    const items = review.presentationItems(stale, rows, 'pending');
    expect(items).toHaveLength(1);
    expect(items[0].grouped).toBe(false);
    expect(items[0].rows).toEqual(rows);
  });
  it('keeps flag-off rows including historical questions and review rows separate', () => {
    const rows = [row('x'), row('y', 'UNAVAILABLE')];
    expect(review.presentationItems({}, rows, 'pending').flatMap(item => item.rows)).toEqual(rows);
    expect(review.presentationItems(payload, rows, 'all', true).flatMap(item => item.rows)).toEqual(rows);
  });
  it.each(['STALE', 'REQUIRES_REVALIDATION'])('keeps %s in revalidation', state => {
    const rows = [row('z', state)];
    expect(review.presentationItems(payload, rows, 'pending')).toHaveLength(0);
    expect(review.presentationItems(payload, rows, 'revalidation')[0].rows).toEqual(rows);
  });
});
