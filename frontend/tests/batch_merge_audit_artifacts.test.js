/**
 * Тест helper-функции `_projectHasAuditArtifacts` из batch-merge модалки.
 * Функция определяет, есть ли у source-проекта готовые findings/нормы/
 * оптимизации, чтобы пред-предупредить пользователя перед склейкой
 * (backend всё равно вернёт 409 без discard_source_output, но фронт
 * показывает явный confirm заранее).
 *
 * Запуск: cd frontend && npm test
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
  let braceStart = src.indexOf('{', i);
  let depth = 0;
  for (let j = braceStart; j < src.length; j++) {
    const ch = src[j];
    if (ch === '{') depth += 1;
    else if (ch === '}') {
      depth -= 1;
      if (depth === 0) return src.slice(i, j + 1);
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
  const helpers = _extractFunction(src, '_projectHasAuditArtifacts');
  const bootstrap = `
    ${helpers}
    globalThis._fn = _projectHasAuditArtifacts;
  `;
  ctx = vm.createContext({ globalThis: {}, console });
  vm.runInContext(bootstrap, ctx);
});

describe('_projectHasAuditArtifacts', () => {
  it('null/undefined → false', () => {
    expect(ctx.globalThis._fn(null)).toBe(false);
    expect(ctx.globalThis._fn(undefined)).toBe(false);
  });

  it('пустой проект → false', () => {
    expect(ctx.globalThis._fn({})).toBe(false);
    expect(ctx.globalThis._fn({ findings_count: 0, optimization_count: 0 })).toBe(false);
  });

  it('findings_count > 0 → true', () => {
    expect(ctx.globalThis._fn({ findings_count: 3 })).toBe(true);
  });

  it('optimization_count > 0 → true', () => {
    expect(ctx.globalThis._fn({ optimization_count: 5 })).toBe(true);
  });

  it('pipeline.text_analysis = done → true', () => {
    expect(ctx.globalThis._fn({ pipeline: { text_analysis: 'done' } })).toBe(true);
  });

  it('pipeline.findings = done → true', () => {
    expect(ctx.globalThis._fn({ pipeline: { findings: 'done' } })).toBe(true);
  });

  it('pipeline.optimization = done → true', () => {
    expect(ctx.globalThis._fn({ pipeline: { optimization: 'done' } })).toBe(true);
  });

  it('все pipeline stages = pending → false', () => {
    expect(ctx.globalThis._fn({
      pipeline: {
        text_analysis: 'pending',
        blocks_analysis: 'pending',
        findings: 'pending',
        optimization: 'pending',
        norms_verified: 'pending',
      },
    })).toBe(false);
  });
});
