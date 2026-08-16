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
const appSource = fs.readFileSync(path.join(frontendRoot, 'static/js/app.js'), 'utf8');
const htmlSource = fs.readFileSync(path.join(frontendRoot, 'index.html'), 'utf8');

function loadRuntime(options = {}) {
  const context = vm.createContext({
    console,
    setTimeout,
    clearTimeout,
    structuredClone,
    location: options.location || {},
    fetch: options.fetch,
    __DISTRIBUTED_UI_CONFIG__: options.config,
    Vue: options.Vue,
  });
  vm.runInContext(serviceSource, context, { filename: 'distributed-service.js' });
  vm.runInContext(featureSource, context, { filename: 'distributed-feature.js' });
  return context;
}

describe('distributed UI integration', () => {
  it('registers the dispatcher route, navigation item and all six direct tabs', () => {
    const runtime = loadRuntime();
    expect(htmlSource).toContain('Распределённые вычисления');
    expect(htmlSource).toContain("navigate('/distributed')");
    expect(appSource).toContain("currentView.value = 'distributed'");
    expect(runtime.DistributedFeature.routeToTab('/distributed')).toBe('overview');
    expect(runtime.DistributedFeature.routeToTab('/distributed/queue')).toBe('queue');
    expect(runtime.DistributedFeature.routeToTab('/distributed/tasks')).toBe('tasks');
    expect(runtime.DistributedFeature.routeToTab('/distributed/workers')).toBe('workers');
    expect(runtime.DistributedFeature.routeToTab('/distributed/limits')).toBe('limits');
    expect(runtime.DistributedFeature.routeToTab('/distributed/diagnostics')).toBe('diagnostics');
  });

  it('renders the overview contract with exactly six KPI cards', () => {
    expect(pageSource.match(/distributed-kpi(?: |")/g)).toHaveLength(6);
    for (const label of ['Серверы онлайн', 'Выполняется', 'В очереди', 'Ошибки', 'Свободные слоты', 'Завершено сегодня']) {
      expect(pageSource).toContain(label);
    }
  });

  it('provides five workers with online/offline states and provider quotas', async () => {
    const { DistributedData } = loadRuntime();
    const service = DistributedData.createMockService({ latency: 0 });
    const workers = await service.getWorkers();
    expect(workers).toHaveLength(5);
    expect(workers.filter((worker) => worker.status !== 'offline')).toHaveLength(4);
    expect(workers.filter((worker) => worker.status === 'offline')).toHaveLength(1);
    expect(workers.every((worker) => worker.quotas.claude && worker.quotas.codex)).toBe(true);
    expect(workers.find((worker) => worker.id === 'worker-mow-04').quotas.claude.status).toBe('critical');
  });

  it('returns the required overview KPI values and next recommendation', async () => {
    const { DistributedData } = loadRuntime();
    const overview = await DistributedData.createMockService({ latency: 0 }).getOverview();
    expect(overview.kpis).toEqual({ online: 4, totalWorkers: 5, active: 7, queued: 18, errors: 2, freeSlots: 3, totalSlots: 8, completedToday: 2 });
    expect(overview.recommendation.workerId).toBe('worker-mow-03');
    expect(overview.recommendation.reasons.length).toBeGreaterThanOrEqual(6);
  });

  it('changes manual assignment and send state through the mock service', async () => {
    const { DistributedData } = loadRuntime();
    const service = DistributedData.createMockService({ latency: 0 });
    const assigned = await service.assignTask('project-primavera-eom', 'worker-mow-04');
    expect(assigned.assignment).toBe('worker-mow-04');
    const sent = await service.sendTask('project-primavera-eom', 'worker-mow-03');
    expect(sent.status).toBe('Добавлен в очередь');
    expect((await service.getOverview()).projects.find((item) => item.id === sent.id).status).toBe('Добавлен в очередь');
  });

  it('reorders all 18 queue items and persists priority changes', async () => {
    const { DistributedData } = loadRuntime();
    const service = DistributedData.createMockService({ latency: 0 });
    const before = await service.getQueue();
    expect(before).toHaveLength(18);
    const reordered = await service.moveQueueItem(before[1].id, 'first');
    expect(reordered[0].id).toBe(before[1].id);
    expect(reordered.map((item) => item.position)).toEqual(Array.from({ length: 18 }, (_, index) => index + 1));
    await service.changePriority(reordered[0].id, 'critical');
    expect((await service.getQueue())[0].priority).toBe('critical');
  });

  it('supports active/completed/error task views and detail drawer content', async () => {
    const { DistributedData, DistributedFeature } = loadRuntime();
    const tasks = await DistributedData.createMockService({ latency: 0 }).getTasks();
    expect(tasks.active).toHaveLength(7);
    expect(tasks.completed.length).toBeGreaterThanOrEqual(2);
    expect(tasks.errors).toHaveLength(2);
    const anchor = new Date('2026-08-16T12:00:00+03:00');
    expect(DistributedFeature.filterCompletedTasks(tasks.completed, 'today', undefined, undefined, anchor)).toHaveLength(2);
    expect(DistributedFeature.filterCompletedTasks(tasks.completed, '7d', undefined, undefined, anchor)).toHaveLength(4);
    expect(DistributedFeature.filterCompletedTasks(tasks.completed, 'custom', '2026-08-15', '2026-08-15')).toHaveLength(1);
    expect(DistributedFeature.filterCompletedTasks(
      [{ completedAtIso: '2026-08-16T21:05:00+00:00' }],
      'today', undefined, undefined, new Date('2026-08-17T12:00:00+03:00'),
    )).toHaveLength(1);
    for (const label of ['Активные', 'Завершённые', 'Ошибки', 'Подробности задачи', 'Техническая информация']) expect(pageSource).toContain(label);
  });

  it('moves an error task into active state on mock retry', async () => {
    const { DistributedData } = loadRuntime();
    const service = DistributedData.createMockService({ latency: 0 });
    const retried = await service.retryTask('task-error-01');
    expect(retried.stage).toBe('transfer');
    const tasks = await service.getTasks();
    expect(tasks.errors.some((task) => task.id === retried.id)).toBe(false);
    expect(tasks.active.some((task) => task.id === retried.id)).toBe(true);
  });

  it('toggles worker intake in demo state', async () => {
    const { DistributedData } = loadRuntime();
    const service = DistributedData.createMockService({ latency: 0 });
    const updated = await service.setWorkerIntake('worker-mow-03', false);
    expect(updated.acceptsNewTasks).toBe(false);
    expect((await service.getWorkers()).find((worker) => worker.id === updated.id).acceptsNewTasks).toBe(false);
  });

  it('renders limits aggregation and provider reset information', () => {
    for (const label of ['Claude · средний остаток', 'Codex · средний остаток', 'Ближайший сброс', 'Больше всего Claude', 'Больше всего Codex']) {
      expect(pageSource).toContain(label);
    }
    expect(pageSource).toContain('Использовано сегодня');
  });

  it('renders safe diagnostics and never places credential fields in the diagnostic model', async () => {
    const { DistributedData } = loadRuntime();
    const service = DistributedData.createMockService({ latency: 0 });
    const diagnostics = await service.getDiagnostics();
    expect(diagnostics).toHaveLength(5);
    expect(diagnostics[0].diagnostic.eventOutbox).toMatchObject({ pending: 0 });
    const safeText = service.getSafeDiagnosticsText();
    expect(safeText).not.toMatch(/private.?key|access.?token|auth.?token|authorization.?header|cookie|session.?secret|openrouter.?secret/i);
    expect(safeText).toContain('eventOutbox');
  });

  it('provides explicit empty and error states through the adapter', async () => {
    const { DistributedData } = loadRuntime();
    const empty = DistributedData.createMockService({ latency: 0, scenario: 'empty' });
    expect((await empty.getWorkers())).toEqual([]);
    expect((await empty.getOverview()).kpis.totalWorkers).toBe(0);
    const failing = DistributedData.createMockService({ latency: 0, scenario: 'error' });
    await expect(failing.getOverview()).rejects.toThrow('Не удалось загрузить');
    expect(pageSource).toContain('Не удалось получить данные');
    expect(pageSource).toContain('Вычислительные узлы не добавлены');
    expect(pageSource).toContain('Очередь пуста');
    expect(pageSource).toContain('Нет данных о лимитах');
  });

  it('contains the required modal, drawer, toast and resource monitoring primitives', () => {
    for (const marker of ['distributed-drawer-overlay', 'distributed-modal', 'distributed-toast', 'distributed-worker-card', 'distributed-progress', 'Почему выбран?', 'Другой VPS']) {
      expect(pageSource + featureSource).toContain(marker);
    }
  });

  it('uses RealDistributedService by default and calls only AuditManager read endpoints', async () => {
    const calls = [];
    const payloads = {
      overview: { kpis: {}, workers: [], projects: [], queuePreview: [], attention: [], recommendation: { available: false } },
      snapshot: { overview: {}, workers: [], queue: [], tasks: {}, limits: [], diagnostics: [] },
      workers: { workers: [] },
      queue: { queue: [] },
      tasks: { tasks: { active: [], completed: [], errors: [] } },
      limits: { limits: [] },
      diagnostics: { diagnostics: [] },
      recommendation: { recommendation: { available: false, source: 'unavailable' } },
    };
    const runtime = loadRuntime({
      fetch: async (url, options) => {
        calls.push({ url, options });
        const key = String(url).split('/').at(-1);
        return { ok: true, status: 200, json: async () => payloads[key] };
      },
    });
    const service = runtime.DistributedData.createDefaultService();
    expect(service.mode).toBe('real');
    expect(service.readOnly).toBe(true);
    await Promise.all([
      service.getOverview(), service.getWorkers(), service.getQueue(), service.getTasks(),
      service.getProviderLimits(), service.getDiagnostics(), service.getNextRecommendation(), service.getSnapshot(),
    ]);
    expect(calls.map((call) => call.url).sort()).toEqual([
      '/api/workers/distributed/diagnostics',
      '/api/workers/distributed/limits',
      '/api/workers/distributed/overview',
      '/api/workers/distributed/queue',
      '/api/workers/distributed/recommendation',
      '/api/workers/distributed/snapshot',
      '/api/workers/distributed/tasks',
      '/api/workers/distributed/workers',
    ]);
    expect(calls.every((call) => call.options.method === 'GET' && call.options.credentials === 'same-origin')).toBe(true);
  });

  it('loads one consistent backend snapshot for the production manager', async () => {
    const calls = [];
    const Vue = {
      ref: (value) => ({ value }),
      computed: (getter) => Object.defineProperty({}, 'value', { get: getter }),
      reactive: (value) => value,
    };
    const runtime = loadRuntime({
      Vue,
      fetch: async (url) => {
        calls.push(url);
        return {
          ok: true,
          status: 200,
          json: async () => ({
            overview: { projects: [], workers: [], recommendation: { available: false } },
            workers: [], queue: [], tasks: { active: [], completed: [], errors: [] },
            limits: [], diagnostics: [], metadata: { readOnly: true },
          }),
        };
      },
    });
    const manager = runtime.DistributedFeature.createManager();
    await manager.load();
    expect(calls).toEqual(['/api/workers/distributed/snapshot']);
  });

  it('enables mock data only through an explicit option, query, or test config', () => {
    expect(loadRuntime().DistributedData.createDefaultService().mode).toBe('real');
    expect(loadRuntime({ location: { search: '?distributed_mode=mock' } }).DistributedData.createDefaultService().mode).toBe('mock');
    expect(loadRuntime({ location: { hash: '#/distributed?distributed_demo=1' } }).DistributedData.createDefaultService().mode).toBe('mock');
    expect(loadRuntime({ config: { mode: 'mock' } }).DistributedData.createDefaultService().mode).toBe('mock');
    expect(loadRuntime().DistributedData.createDefaultService({ mode: 'mock', latency: 0 }).mode).toBe('mock');
  });

  it('never falls back to mock data when the real API fails', async () => {
    const runtime = loadRuntime({
      fetch: async () => { throw new Error('network down'); },
    });
    const service = runtime.DistributedData.createDefaultService();
    await expect(service.getOverview()).rejects.toThrow('AuditManager API недоступен: network down');

    const httpFailure = runtime.DistributedData.createRealService({
      fetch: async () => ({ ok: false, status: 503, json: async () => ({ detail: 'backend unavailable' }) }),
    });
    await expect(httpFailure.getWorkers()).rejects.toThrow('AuditManager API: backend unavailable');
    expect(service.mode).toBe('real');
  });

  it('rejects every production mutation before network or local state changes', async () => {
    const calls = [];
    const { DistributedData } = loadRuntime({ fetch: async (...args) => { calls.push(args); } });
    const service = DistributedData.createRealService();
    const mutations = [
      service.assignTask('p', 'w'), service.sendTask('p', 'w'),
      service.changePriority('q', 'high'), service.moveQueueItem('q', 'up'),
      service.setWorkerIntake('w', true), service.retryTask('t'), service.transferTask('t', 'w'),
    ];
    for (const mutation of mutations) {
      await expect(mutation).rejects.toThrow('следующем этапе');
    }
    expect(calls).toEqual([]);
    expect(pageSource).toContain('distributed.modal && distributed.isDemo');
    expect(pageSource).toContain('Реальные данные · только чтение');
  });

  it('uses explicit progress provenance and unavailable telemetry copy', async () => {
    const { DistributedData } = loadRuntime();
    const tasks = await DistributedData.createMockService({ latency: 0 }).getTasks();
    for (const task of [...tasks.active, ...tasks.completed, ...tasks.errors]) {
      expect(['exact', 'estimated', 'unavailable']).toContain(task.progressKind);
      expect(task).toHaveProperty('progressPercent');
    }
    expect(pageSource + featureSource).toContain('Прогресс недоступен');
    expect(pageSource + featureSource).toContain('Нет телеметрии');
    expect(pageSource + featureSource).toContain('Остаток недоступен');
    expect(pageSource).toContain("'importing'");
  });
});
