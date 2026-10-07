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

const navigateStart = source.indexOf('async function openSectionOptimizationVersion(');
const navigateEnd = source.indexOf('const sectionOptimizationFilteredHistory', navigateStart);
const navigationExpression = source.slice(navigateStart, navigateEnd) + '\nopenSectionOptimizationVersion;';

async function openVersion(requested, ids, fail = false) {
  const routes = [];
  const errors = [];
  const calls = [];
  const open = runInNewContext(navigationExpression, {
    api: async (path, options) => {
      calls.push({ path, options });
      if (fail) throw new Error('API unavailable');
      return { versions: ids.map(version_id => ({ version_id })), latest_version_id: 'v2' };
    },
    navigate: route => routes.push(route),
    alert: message => errors.push(message),
  });
  await open('Project / K4', requested);
  return { routes, errors, calls };
}

describe('historical source navigation', () => {
  it('opens the declared v1 for a v001 source instead of latest v2', async () => {
    const result = await openVersion('v001', ['v1', 'v2']);
    expect(result.routes).toEqual(['/project/Project%20%2F%20K4/optimization?version_id=v1']);
    expect(result.calls[0]).toEqual({ path: '/projects/Project%20%2F%20K4/versions', options: { withVersion: false } });
    expect(result.errors).toEqual([]);
  });

  it('preserves an exact ID and supports canonical IDs returned by the API', async () => {
    expect((await openVersion('v001', ['v1', 'v001', 'v002'])).routes[0]).toContain('version_id=v001');
    expect((await openVersion('v2', ['v001', 'v002'])).routes[0]).toContain('version_id=v002');
  });

  it('never substitutes latest for a missing, ambiguous or unreadable source', async () => {
    for (const result of [
      await openVersion('v001', ['v2']),
      await openVersion('v0001', ['v1', 'v01']),
      await openVersion('v001', ['v1', 'v2'], true),
    ]) {
      expect(result.routes).toEqual([]);
      expect(result.errors).toHaveLength(1);
    }
  });
});
