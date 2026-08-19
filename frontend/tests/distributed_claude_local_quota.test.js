// 12J — как интерфейс показывает остаток Claude из локального кеша Claude Code.
//
// Проверяется ровно то, что оператор видит глазами:
//   * два окна лимита, а не одно (недельное не прячется за пятичасовым);
//   * возраст ДАННЫХ, а не момент нашего чтения;
//   * причина, когда остатка нет, — вместо голого «Остаток недоступен»;
//   * недокументированность источника названа прямо.
import { describe, expect, it } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import vm from 'node:vm';
import { fileURLToPath } from 'node:url';

const testDir = path.dirname(fileURLToPath(import.meta.url));
const frontendRoot = path.resolve(testDir, '..');
const serviceSource = fs.readFileSync(path.join(frontendRoot, 'static/js/distributed-service.js'), 'utf8');
const featureSource = fs.readFileSync(path.join(frontendRoot, 'static/js/distributed-feature.js'), 'utf8');
const pageSource = fs.readFileSync(path.join(frontendRoot, 'static/js/distributed-page.js'), 'utf8');
const cssSource = fs.readFileSync(path.join(frontendRoot, 'static/css/distributed.css'), 'utf8');

function loadRuntime() {
  const context = vm.createContext({ console, setTimeout, clearTimeout, structuredClone, location: {} });
  vm.runInContext(serviceSource, context, { filename: 'distributed-service.js' });
  vm.runInContext(featureSource, context, { filename: 'distributed-feature.js' });
  return context;
}

/** Компонент, зарегистрированный настоящим registerComponents. */
function quotaComponent() {
  const registry = {};
  const app = { component: (name, definition) => { registry[name] = definition; } };
  loadRuntime().DistributedFeature.registerComponents(app);
  return registry['distributed-quota-bar'];
}

/** Значения computed-свойств компонента при данной квоте.
 *
 * Свойства ссылаются друг на друга (`otherWindows` смотрит в `windows`),
 * поэтому контекст собирается один и ленивыми геттерами — так же, как это
 * делает Vue. */
function computedFor(quota, extra = {}) {
  const component = quotaComponent();
  const scope = { quota, stale: false, ...extra };
  for (const [name, fn] of Object.entries(component.computed)) {
    Object.defineProperty(scope, name, { get: () => fn.call(scope), enumerable: true, configurable: true });
  }
  return scope;
}

const LOCAL_CACHE_QUOTA = Object.freeze({
  availability: 'available',
  percentageRemaining: 84,
  status: 'ok',
  quotaState: 'ready',
  source: 'local_usage_statistics',
  sourceStability: 'undocumented',
  confidence: 'medium',
  isEstimated: false,
  stale: false,
  ageSec: 2013,
  reason: 'local_cache_available',
  resetIn: '2 ч 14 мин',
  windows: [
    { windowId: 'five_hour', label: '5 часов', remainingPercent: 84, usedPercent: 16, resetIn: '2 ч 14 мин', resetAt: '2026-08-19T09:10:00Z' },
    { windowId: 'seven_day', label: '7 дней', remainingPercent: 88, usedPercent: 12, resetIn: '4 дн', resetAt: '2026-08-24T04:00:00Z' },
  ],
});

describe('Claude local usage cache in the distributed UI', () => {
  it('exposes both windows with the most constrained first', () => {
    const values = computedFor(LOCAL_CACHE_QUOTA);
    expect(values.windows).toHaveLength(2);
    expect(values.primaryWindow.windowId).toBe('five_hour');
    expect(values.otherWindows.map((w) => w.windowId)).toEqual(['seven_day']);
  });

  it('renders the weekly window in the template, not only the primary one', () => {
    const component = quotaComponent();
    expect(component.template).toContain('otherWindows');
    expect(component.template).toContain('primaryWindow.label');
    // Экран «Лимиты» обязан показывать оба окна каждого воркера.
    expect(pageSource).toContain('distributed.quotaWindows(item[provider.key])');
  });

  it('reports the age of the observation, never the moment of reading', () => {
    const { DistributedFeature } = loadRuntime();
    expect(DistributedFeature.quotaAgeText({ ageSec: 2013 })).toBe('данные 34 мин назад');
    expect(DistributedFeature.quotaAgeText({ ageSec: 20 })).toBe('данные только что');
    expect(DistributedFeature.quotaAgeText({ ageSec: 7200 })).toBe('данные 2 ч назад');
    expect(DistributedFeature.quotaAgeText({})).toBe('');
  });

  it('explains WHY the number is missing instead of a bare "недоступен"', () => {
    const { DistributedFeature } = loadRuntime();
    const text = DistributedFeature.quotaReasonText({ reason: 'no_safe_supported_source' });
    expect(text).toContain('Claude Code не сообщает остаток');
    expect(DistributedFeature.quotaReasonText({ reason: 'local_cache_missing' })).toContain('пока не появились');
    expect(DistributedFeature.quotaReasonText({ reason: 'local_cache_schema_unsupported' })).toContain('не распознан');
    // Неизвестный код не превращается в пустое место с текстом-заглушкой.
    expect(DistributedFeature.quotaReasonText({ reason: 'что-то новое' })).toBe('');
  });

  it('never presents the undocumented cache as an official API', () => {
    const { DistributedFeature } = loadRuntime();
    const available = DistributedFeature.quotaReasonText({ reason: 'local_cache_available' });
    expect(available).toContain('Локальные данные Claude Code');
    expect(available).toContain('Официального машиночитаемого остатка у Claude Code нет');
    const values = computedFor(LOCAL_CACHE_QUOTA);
    expect(values.undocumented).toBe(true);
  });

  it('marks stale data as last known rather than current', () => {
    const stale = { ...LOCAL_CACHE_QUOTA, stale: true, status: 'stale', reason: 'local_cache_stale', ageSec: 7300 };
    const { DistributedFeature } = loadRuntime();
    expect(DistributedFeature.quotaReasonText(stale)).toContain('последнее известное значение');
    const component = quotaComponent();
    expect(component.template).toContain('distributed-quota__age--stale');
    expect(component.template).toContain('Последние известные данные');
  });

  it('ships styles for every new element it renders', () => {
    for (const cls of [
      'distributed-quota__window',
      'distributed-quota__windows',
      'distributed-quota__age',
      'distributed-quota__note',
      'distributed-provider-row__windows',
      'distributed-provider-row__reason',
    ]) {
      expect(cssSource).toContain(`.${cls}`);
    }
  });

  it('keeps Codex rendering untouched', () => {
    const { DistributedFeature } = loadRuntime();
    const codex = { percentageRemaining: 3, status: 'critical', source: 'official_app_server_rpc', confidence: 'high', isEstimated: false, reason: null, windows: [] };
    expect(DistributedFeature.quotaReasonText(codex)).toBe('');
    const values = computedFor(codex);
    expect(values.windows).toEqual([]);
    expect(values.primaryWindow).toBeNull();
    expect(values.undocumented).toBe(false);
  });
});
