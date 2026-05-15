/**
 * Тесты helper-функций для UI «Добавить как версию существующего проекта».
 *
 * Helpers лежат внутри Vue setup() в frontend/static/js/app.js. Чтобы протестировать
 * их без Vue/bundler, вырезаем декларации `function normalizeProjectName`,
 * `function candidateBasename`, `function candidateTargetOptions` и
 * `function candidateNextVersionLabel` через regex и исполняем в node:vm-контексте,
 * прокидывая нужные refs.
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
  // Находим `function <name>(` и берём всё до сбалансированной закрывающей `}`.
  const i = src.indexOf('function ' + name + '(');
  if (i === -1) throw new Error('Helper not found: ' + name);
  // Найти '{' открывающую функцию
  let braceStart = src.indexOf('{', i);
  let depth = 0;
  for (let j = braceStart; j < src.length; j++) {
    const ch = src[j];
    if (ch === '{') depth += 1;
    else if (ch === '}') {
      depth -= 1;
      if (depth === 0) {
        return src.slice(i, j + 1);
      }
    }
  }
  throw new Error('Unbalanced braces for ' + name);
}

let ctx;
beforeAll(() => {
  const src = fs.readFileSync(
    path.join(__dirname, '..', 'static', 'js', 'app.js'),
    'utf-8',
  );
  const helpers = [
    'normalizeProjectName',
    'candidateBasename',
    'candidateTargetOptions',
    'candidateTargetName',
    'candidateNextVersionLabel',
  ].map(n => _extractFunction(src, n)).join('\n\n');

  // ref-shim: возвращает контейнер с .value (как Vue ref)
  const shim = `
    const projects = { value: __PROJECTS };
    ${helpers}
    globalThis._fns = {
      normalizeProjectName,
      candidateBasename,
      candidateTargetOptions,
      candidateTargetName,
      candidateNextVersionLabel,
    };
  `;
  // Подменим placeholder на список проектов из самого теста — но проще:
  // оставим как глобальный массив, заменяемый перед каждым тестом
  const code = shim.replace('__PROJECTS', 'globalThis.__projects || []');
  ctx = vm.createContext({ globalThis: {}, console });
  vm.runInContext(code, ctx);
});

function _set(projects) {
  ctx.globalThis.__projects = projects;
  // re-evaluate `projects.value`: проще пересоздать контекст? нет — используем
  // в helpers ссылку на projects.value, которая выводится из переменной.
  // Перезапускаем bootstrap, чтобы projects.value переинициализировался:
  // (см. shim — projects = { value: globalThis.__projects || [] })
  // Простой путь — пересобираем код:
}

describe('normalizeProjectName', () => {
  it('убирает .pdf и .md', () => {
    expect(ctx.globalThis._fns.normalizeProjectName('foo.pdf')).toBe('foo');
    expect(ctx.globalThis._fns.normalizeProjectName('foo.md')).toBe('foo');
  });
  it('убирает _document', () => {
    // Дополнительно нормализатор схлопывает _-/пробелы в одиночный пробел,
    // чтобы matching был устойчив к смешиванию разделителей.
    expect(ctx.globalThis._fns.normalizeProjectName('13АВ-РД-КЖ5_document.md'))
      .toBe('13ав рд кж5');
  });
  it('убирает (1)', () => {
    expect(ctx.globalThis._fns.normalizeProjectName('foo (1).pdf')).toBe('foo');
  });
  it('убирает Изм.1', () => {
    expect(ctx.globalThis._fns.normalizeProjectName('foo_Изм.1.pdf')).toBe('foo');
  });
  it('приводит регистр и пробелы', () => {
    expect(ctx.globalThis._fns.normalizeProjectName('  FOO   BAR.PDF'))
      .toBe('foo bar');
  });
});

describe('candidateTargetOptions (фильтр по разделу)', () => {
  beforeAll(() => {
    // Заново подготавливаем контекст с заданным projects.value
    const src = fs.readFileSync(
      path.join(__dirname, '..', 'static', 'js', 'app.js'),
      'utf-8',
    );
    const helpers = [
      'normalizeProjectName',
      'candidateBasename',
      'candidateTargetOptions',
      'candidateTargetName',
      'candidateNextVersionLabel',
    ].map(n => _extractFunction(src, n)).join('\n\n');
    const bootstrap = `
      const projects = { value: [
        { project_id: 'KJ_A', name: 'KJ_A', section: 'KJ' },
        { project_id: 'KJ_B', name: '13АВ-РД-КЖ5.22', section: 'KJ', version_count: 1 },
        { project_id: 'AR_A', name: 'AR_A', section: 'AR' },
      ]};
      ${helpers}
      globalThis._fns = {
        normalizeProjectName,
        candidateBasename,
        candidateTargetOptions,
        candidateTargetName,
        candidateNextVersionLabel,
      };
    `;
    ctx = vm.createContext({ globalThis: {}, console });
    vm.runInContext(bootstrap, ctx);
  });

  it('возвращает только проекты текущего раздела', () => {
    const f = { folder: 'X', _selectedDiscipline: 'KJ', pdf_files: ['unknown.pdf'] };
    const opts = ctx.globalThis._fns.candidateTargetOptions(f);
    const ids = opts.map(o => o.project_id);
    expect(ids).toContain('KJ_A');
    expect(ids).toContain('KJ_B');
    expect(ids).not.toContain('AR_A');
  });

  it('подсказывает target по совпадению basename', () => {
    const f = {
      folder: '13АВ-РД-КЖ5.22',
      _selectedDiscipline: 'KJ',
      pdf_files: ['13АВ-РД-КЖ5.22.pdf'],
    };
    const opts = ctx.globalThis._fns.candidateTargetOptions(f);
    const suggested = opts.find(o => o._suggested);
    expect(suggested).toBeDefined();
    expect(suggested.project_id).toBe('KJ_B');
  });

  it('пусто если раздел не выбран', () => {
    expect(ctx.globalThis._fns.candidateTargetOptions({ folder: 'X', pdf_files: ['x.pdf'] }))
      .toEqual([]);
  });
});

describe('candidateNextVersionLabel', () => {
  beforeAll(() => {
    const src = fs.readFileSync(
      path.join(__dirname, '..', 'static', 'js', 'app.js'),
      'utf-8',
    );
    const helpers = ['candidateNextVersionLabel']
      .map(n => _extractFunction(src, n)).join('\n\n');
    const bootstrap = `
      const projects = { value: [
        { project_id: 'KJ_B', name: 'KJ_B', section: 'KJ', version_count: 1 },
        { project_id: 'KJ_C', name: 'KJ_C', section: 'KJ', version_count: 3 },
      ]};
      ${helpers}
      globalThis._fns = { candidateNextVersionLabel };
    `;
    ctx = vm.createContext({ globalThis: {}, console });
    vm.runInContext(bootstrap, ctx);
  });

  it('V2 для проекта с 1 версией', () => {
    expect(ctx.globalThis._fns.candidateNextVersionLabel({ _targetProjectId: 'KJ_B' }))
      .toBe('V2');
  });
  it('V4 для проекта с 3 версиями', () => {
    expect(ctx.globalThis._fns.candidateNextVersionLabel({ _targetProjectId: 'KJ_C' }))
      .toBe('V4');
  });
  it('V? без target', () => {
    expect(ctx.globalThis._fns.candidateNextVersionLabel({}))
      .toBe('V?');
  });
});
