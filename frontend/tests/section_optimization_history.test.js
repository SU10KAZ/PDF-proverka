import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import { runInNewContext } from 'node:vm';

const source = fs.readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const start = source.indexOf('const sectionOptimizationFilteredHistory = computed(');
const end = source.indexOf('const sectionOptimizationFilteredSignals = computed(', start);
const expression = source.slice(start, end) + '\nsectionOptimizationFilteredHistory.value;';

function filter(data, project = '', query = '') {
  return runInNewContext(expression, {
    computed: fn => ({ value: fn() }),
    sectionOptimizationData: { value: data },
    sectionOptimizationProjectFilter: { value: project },
    sectionOptimizationSearch: { value: query },
  });
}

const entries = [
  { history_id: 'H1', project_id: 'P1', proposed: 'Датчик', origins: [{ version_id: 'v001', item_id: 'OPT-7' }] },
  { history_id: 'H2', project_id: 'P2', proposed: 'Кабель', origins: [{ version_id: 'v002', item_id: 'OPT-7' }] },
];

describe('section optimization historical ideas', () => {
  it('reads old snapshots without history as an empty list', () => {
    expect(filter({})).toEqual([]);
    expect(filter(null)).toEqual([]);
  });

  it('combines project scope with search by historical version and item', () => {
    const data = { historical_optimizations: entries };
    expect(filter(data, 'P1', 'OPT-7').map(x => x.history_id)).toEqual(['H1']);
    expect(filter(data, 'P1', 'v002')).toEqual([]);
    expect(filter(data, '', 'v002').map(x => x.history_id)).toEqual(['H2']);
    expect(filter(data, '', 'ДАТЧИК').map(x => x.history_id)).toEqual(['H1']);
    expect(data.historical_optimizations).toEqual(entries);
  });
});
