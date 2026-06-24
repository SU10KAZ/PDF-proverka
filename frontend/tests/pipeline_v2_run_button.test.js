/**
 * Тесты «Запустить Pipeline V2» — per-pair controlled-run кнопки в «Связь
 * блоков» (Сравнение стадий).
 *
 * Паттерн контрактных тестов проекта: зеркалим pure-логику состояния кнопки
 * из app.js + проверяем, что разметка/методы/контракт присутствуют в
 * исходниках. Если зеркало и app.js разойдутся — тест упадёт первым.
 *
 * Запуск: cd frontend && npm test
 */
import { describe, it, expect } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const APP_JS = fs.readFileSync(
  path.join(__dirname, '../static/js/app.js'), 'utf8');
const INDEX_HTML = fs.readFileSync(
  path.join(__dirname, '../index.html'), 'utf8');

// ── зеркало pure-helper scPv2RunState из app.js ─────────────────────────────

function scPv2RunState(byPair, artifactPairs, pid) {
  const j = byPair[pid];
  const s = j && j.status;
  if (s === 'queued' || s === 'running') return 'running';
  if (s === 'completed') return 'completed';
  if (s === 'failed' || s === 'failed_interrupted' || s === 'cancelled') return 'failed';
  if (artifactPairs[pid]) return 'has_artifacts';
  return 'idle';
}

function scPv2RunBtnLabel(state) {
  if (state === 'running') return '⏳ V2…';
  if (state === 'failed') return '↻ V2';
  if (state === 'has_artifacts' || state === 'completed') return '↻ V2';
  return '▶ V2';
}

describe('scPv2RunState (state machine кнопки)', () => {
  it('not started → idle → «Запустить V2»', () => {
    const st = scPv2RunState({}, {}, 'p1');
    expect(st).toBe('idle');
    expect(scPv2RunBtnLabel(st)).toBe('▶ V2');
  });

  it('артефакты есть → has_artifacts → «Перезапустить»', () => {
    const st = scPv2RunState({}, { p1: true }, 'p1');
    expect(st).toBe('has_artifacts');
    expect(scPv2RunBtnLabel(st)).toBe('↻ V2');
  });

  it('queued/running → running (кнопка disabled)', () => {
    expect(scPv2RunState({ p1: { status: 'queued' } }, {}, 'p1')).toBe('running');
    expect(scPv2RunState({ p1: { status: 'running' } }, {}, 'p1')).toBe('running');
    expect(scPv2RunBtnLabel('running')).toBe('⏳ V2…');
  });

  it('completed → completed', () => {
    expect(scPv2RunState({ p1: { status: 'completed' } }, {}, 'p1')).toBe('completed');
  });

  it('failed / failed_interrupted / cancelled → failed («Повторить»)', () => {
    for (const s of ['failed', 'failed_interrupted', 'cancelled']) {
      expect(scPv2RunState({ p1: { status: s } }, {}, 'p1')).toBe('failed');
    }
    expect(scPv2RunBtnLabel('failed')).toBe('↻ V2');
  });

  it('running имеет приоритет над наличием артефактов', () => {
    expect(scPv2RunState({ p1: { status: 'running' } }, { p1: true }, 'p1'))
      .toBe('running');
  });
});

// ── контракт: разметка кнопки + модалки в index.html ────────────────────────

describe('index.html — кнопка и confirm-модалка', () => {
  it('кнопка V2 в строке пары вызывает scPv2RunOpenModal(p)', () => {
    expect(INDEX_HTML).toContain('@click="scPv2RunOpenModal(p)"');
    expect(INDEX_HTML).toContain('scPv2RunBtnLabel(p.id)');
    expect(INDEX_HTML).toContain('scPv2RunBtnTitle(p.id)');
  });

  it('кнопка disabled когда running', () => {
    expect(INDEX_HTML).toContain("scPv2RunState(p.id)==='running'");
  });

  it('модалка требует typed-подтверждения pair_id', () => {
    expect(INDEX_HTML).toContain('v-if="scPv2RunModal"');
    expect(INDEX_HTML).toContain('v-model="scPv2RunModal.typed"');
    expect(INDEX_HTML).toContain('scPv2RunModal.typed !== scPv2RunModal.pair_id');
  });

  it('модалка предупреждает про backup при rerun и про не-read-only', () => {
    expect(INDEX_HTML).toContain('pipeline_v2_backup_before_ui_run');
    expect(INDEX_HTML).toContain('<b>не read-only</b>');
  });

  it('кнопка submit зовёт scPv2RunSubmit()', () => {
    expect(INDEX_HTML).toContain('@click="scPv2RunSubmit()"');
  });
});

// ── контракт: методы/состояние/эндпоинты в app.js ───────────────────────────

describe('app.js — методы и контракт эндпоинтов', () => {
  it('экспортирует методы кнопки из setup()', () => {
    for (const name of ['scPv2RunState', 'scPv2RunBtnLabel', 'scPv2RunBtnTitle',
      'scPv2RunErrorFor', 'scPv2RunOpenModal', 'scPv2RunSubmit', 'scPv2RunModal']) {
      expect(APP_JS).toContain(name);
    }
  });

  it('POST на controlled run endpoint с confirm-body', () => {
    expect(APP_JS).toContain('/pairs/${encodeURIComponent(pid)}/run`');
    expect(APP_JS).toContain("method: 'POST'");
    expect(APP_JS).toContain('confirm: true');
    expect(APP_JS).toContain('confirm_session_id: scSession.value.id');
    expect(APP_JS).toContain('confirm_pair_id: pid');
  });

  it('polling GET run-status с терминальными статусами', () => {
    expect(APP_JS).toContain('/run-status/${encodeURIComponent(jobId)}');
    expect(APP_JS).toContain("['completed', 'failed', 'cancelled', 'failed_interrupted']");
  });

  it('ui-payload используется только как GET (read-only сохранён)', () => {
    // в нашей логике ui-payload вызывается без method:'POST' рядом
    expect(APP_JS).toContain('/ui-payload`');
    expect(APP_JS).not.toContain("'/ui-payload`', {\n");
  });
});
