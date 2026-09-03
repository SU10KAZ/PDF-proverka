/**
 * Тесты helper'ов авто-связывания загружаемых папок с версиями проекта.
 *
 * Вырезаем декларации из frontend/static/js/app.js (они живут внутри Vue
 * setup()) и исполняем в node:vm — как в add_project_version_target.test.js.
 *
 * Запуск:
 *   cd frontend && npm test
 */
import { describe, it, expect, beforeAll } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import vm from 'node:vm';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

function _extractFunction(src, name) {
  const i = src.indexOf('function ' + name + '(');
  if (i === -1) throw new Error('Helper not found: ' + name);
  const braceStart = src.indexOf('{', i);
  let depth = 0;
  for (let j = braceStart; j < src.length; j++) {
    if (src[j] === '{') depth += 1;
    else if (src[j] === '}') {
      depth -= 1;
      if (depth === 0) return src.slice(i, j + 1);
    }
  }
  throw new Error('Unbalanced braces for ' + name);
}

let fns;
let candidates;
beforeAll(() => {
  const src = fs.readFileSync(
    path.join(__dirname, '..', 'static', 'js', 'app.js'), 'utf-8');
  const helpers = [
    'versionSuffixRe',
    'normalizeProjectName',
    'projectVersionNo',
    '_orderCandidatesByVersion',
    '_linkPendingVersionsTo',
  ].map(n => _extractFunction(src, n)).join('\n\n');
  const shim = `
    const uploadCandidates = { value: globalThis.__CANDS };
    ${helpers}
    globalThis._fns = { normalizeProjectName, projectVersionNo,
                        _orderCandidatesByVersion, _linkPendingVersionsTo };
  `;
  const ctx = vm.createContext({ globalThis: {} });
  ctx.globalThis = ctx;
  candidates = [];
  ctx.__CANDS = candidates;
  vm.runInContext(shim, ctx);
  fns = ctx.globalThis._fns;
});

function _cand(name, over = {}) {
  return Object.assign({
    name, folder: name, addMode: 'new_project', targetProjectId: '',
    suggestedTarget: '', suggestedTargetName: '', suggestedReason: '',
    suggestedLabel: '', modeTouched: false, status: 'ready',
    message: '', checked: true,
  }, over);
}

describe('projectVersionNo', () => {
  it('читает номер из «_V2» / «-V2» / « V2»', () => {
    expect(fns.projectVersionNo('X_V2')).toBe(2);
    expect(fns.projectVersionNo('X-V3')).toBe(3);
    expect(fns.projectVersionNo('X V10')).toBe(10);
  });
  it('без суффикса — 0, литера секции «-В2» версией не считается', () => {
    expect(fns.projectVersionNo('X')).toBe(0);
    expect(fns.projectVersionNo('СТ26-АР1-В2')).toBe(0);
  });
});

describe('normalizeProjectName', () => {
  it('снимает «_V2» — «..._V1» и «..._V2» это один проект', () => {
    expect(fns.normalizeProjectName('СТ26_01-14-АР3-3-РД_V2'))
      .toBe(fns.normalizeProjectName('СТ26_01-14-АР3-3-РД_V1'));
  });
  it('не склеивает разные секции «-В1»/«-В2»', () => {
    expect(fns.normalizeProjectName('СТ26-АР1-В1'))
      .not.toBe(fns.normalizeProjectName('СТ26-АР1-В2'));
  });
});

describe('_orderCandidatesByVersion', () => {
  it('внутри одного проекта младшая версия идёт первой', () => {
    const list = [_cand('AAA_V2'), _cand('BBB_V1'), _cand('AAA_V1')];
    const out = fns._orderCandidatesByVersion(list).map(c => c.name);
    expect(out).toEqual(['AAA_V1', 'BBB_V1', 'AAA_V2']);
  });
  it('порядок остальных строк не трогает', () => {
    const list = [_cand('CCC'), _cand('AAA'), _cand('BBB')];
    expect(fns._orderCandidatesByVersion(list).map(c => c.name))
      .toEqual(['CCC', 'AAA', 'BBB']);
  });
});

describe('_linkPendingVersionsTo', () => {
  it('старшая версия из той же пачки становится версией загруженной', () => {
    const v1 = _cand('AAA_V1');
    const v2 = _cand('AAA_V2');
    candidates.length = 0; candidates.push(v1, v2);
    fns._linkPendingVersionsTo(v1, 'AR/AAA_V1', 'AAA_V1');
    expect(v2.addMode).toBe('new_version');
    expect(v2.targetProjectId).toBe('AR/AAA_V1');
    expect(v2.checked).toBe(true);
  });
  it('не трогает строку с ручным выбором режима', () => {
    const v1 = _cand('AAA_V1');
    const v2 = _cand('AAA_V2', { modeTouched: true });
    candidates.length = 0; candidates.push(v1, v2);
    fns._linkPendingVersionsTo(v1, 'AR/AAA_V1', 'AAA_V1');
    expect(v2.addMode).toBe('new_project');
    expect(v2.targetProjectId).toBe('');
  });
  it('не трогает чужой проект и уже загруженные строки', () => {
    const v1 = _cand('AAA_V1');
    const other = _cand('BBB_V2');
    const done = _cand('AAA_V3', { status: 'done' });
    candidates.length = 0; candidates.push(v1, other, done);
    fns._linkPendingVersionsTo(v1, 'AR/AAA_V1', 'AAA_V1');
    expect(other.addMode).toBe('new_project');
    expect(done.addMode).toBe('new_project');
  });
  it('младшую версию к старшей не привязывает', () => {
    const v2 = _cand('AAA_V2');
    const v1 = _cand('AAA_V1');
    candidates.length = 0; candidates.push(v2, v1);
    fns._linkPendingVersionsTo(v2, 'AR/AAA_V2', 'AAA_V2');
    expect(v1.addMode).toBe('new_project');
  });
});
