/**
 * Тесты pure-helper'ов версионности (frontend/static/js/version_api.js).
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

// version_api.js намеренно не ESM — он подключается обычным <script> тегом
// в браузере. Чтобы протестировать его без bundler-а, исполняем код в
// node:vm-контексте и достаём `globalThis.VersionAPI`.
let VersionAPI;
beforeAll(() => {
  const code = fs.readFileSync(
    path.join(__dirname, '..', 'static', 'js', 'version_api.js'),
    'utf-8',
  );
  // Node:vm context не имеет глобальных WebAPI по умолчанию — подкладываем
  // URLSearchParams, чтобы parseVersionFromHash работал так же, как в браузере.
  const context = {
    URLSearchParams: globalThis.URLSearchParams,
    encodeURIComponent: globalThis.encodeURIComponent,
  };
  vm.createContext(context);
  vm.runInContext(code, context);
  VersionAPI = context.VersionAPI;
  expect(VersionAPI).toBeDefined();
});

describe('apiUrl', () => {
  it('адаптирует /api префикс', () => {
    expect(VersionAPI.apiUrl('/projects')).toBe('/api/projects');
    expect(VersionAPI.apiUrl('projects')).toBe('/api/projects');
  });

  it('не добавляет version_id если versionId=null', () => {
    expect(VersionAPI.apiUrl('/projects/M31A')).toBe('/api/projects/M31A');
    expect(VersionAPI.apiUrl('/projects/M31A', { versionId: null })).toBe('/api/projects/M31A');
  });

  it('добавляет ?version_id= когда versionId установлен', () => {
    expect(VersionAPI.apiUrl('/projects/M31A', { versionId: 'v2' })).toBe(
      '/api/projects/M31A?version_id=v2',
    );
  });

  it('склеивает через & если уже есть ?', () => {
    expect(VersionAPI.apiUrl('/findings/M31A?severity=KR', { versionId: 'v2' })).toBe(
      '/api/findings/M31A?severity=KR&version_id=v2',
    );
  });

  it('withVersion=false отключает добавление даже если versionId задан', () => {
    expect(
      VersionAPI.apiUrl('/projects/M31A/versions/v2/files', {
        versionId: 'v2',
        withVersion: false,
      }),
    ).toBe('/api/projects/M31A/versions/v2/files');
  });

  it('URL-encode значения version_id', () => {
    expect(VersionAPI.apiUrl('/projects/M31A', { versionId: 'v 2/test' })).toBe(
      '/api/projects/M31A?version_id=v%202%2Ftest',
    );
  });
});

describe('parseVersionFromHash', () => {
  it('возвращает null если хеш без query', () => {
    expect(VersionAPI.parseVersionFromHash('')).toBeNull();
    expect(VersionAPI.parseVersionFromHash('#/project/M31A')).toBeNull();
    expect(VersionAPI.parseVersionFromHash('#/project/M31A/findings')).toBeNull();
  });

  it('достаёт version_id из query', () => {
    expect(VersionAPI.parseVersionFromHash('#/project/M31A?version_id=v2')).toBe('v2');
    expect(VersionAPI.parseVersionFromHash('#/project/M31A/findings?version_id=v3')).toBe('v3');
  });

  it('ignore лишние пробелы', () => {
    expect(VersionAPI.parseVersionFromHash('#/p/x?version_id=  v2  ')).toBe('v2');
  });
});

describe('buildHashRoute', () => {
  it('без versionId возвращает чистый route', () => {
    expect(VersionAPI.buildHashRoute('/project/M31A', null)).toBe('/project/M31A');
  });

  it('добавляет version_id', () => {
    expect(VersionAPI.buildHashRoute('/project/M31A/findings', 'v2')).toBe(
      '/project/M31A/findings?version_id=v2',
    );
  });

  it('срезает существующий query', () => {
    expect(VersionAPI.buildHashRoute('/project/M31A?old=1', 'v2')).toBe(
      '/project/M31A?version_id=v2',
    );
  });
});

describe('formatVersionBadge', () => {
  it('null project → null', () => {
    expect(VersionAPI.formatVersionBadge(null)).toBeNull();
  });

  it('одна версия с PDF и findings → null (не показываем)', () => {
    const p = {
      version_id: 'v1',
      version_count: 1,
      findings_count: 17,
      versions_summary: [
        { version_id: 'v1', label: 'V1', is_latest: true, has_source_files: true },
      ],
    };
    expect(VersionAPI.formatVersionBadge(p)).toBeNull();
  });

  it('V2 без файлов → warn "Нет файлов"', () => {
    const p = {
      version_id: 'v2',
      version_count: 1,
      findings_count: 0,
      versions_summary: [
        { version_id: 'v2', label: 'V2', is_latest: true, has_source_files: false },
      ],
    };
    const b = VersionAPI.formatVersionBadge(p);
    expect(b).toEqual({ text: 'Нет файлов', tone: 'warn' });
  });

  it('две версии → "V1–V2"', () => {
    const p = {
      version_id: 'v2',
      version_count: 2,
      findings_count: 0,
      versions_summary: [
        { version_id: 'v1', label: 'V1', is_latest: false, has_source_files: true },
        { version_id: 'v2', label: 'V2', is_latest: true, has_source_files: true },
      ],
    };
    expect(VersionAPI.formatVersionBadge(p)).toEqual({ text: 'V1–V2', tone: 'info' });
  });

  it('две версии, V2 пустая → warn с подсказкой', () => {
    const p = {
      version_id: 'v2',
      version_count: 2,
      findings_count: 0,
      versions_summary: [
        { version_id: 'v1', label: 'V1', is_latest: false, has_source_files: true },
        { version_id: 'v2', label: 'V2', is_latest: true, has_source_files: false },
      ],
    };
    const b = VersionAPI.formatVersionBadge(p);
    expect(b.tone).toBe('warn');
    expect(b.text).toContain('V1–V2');
    expect(b.text).toContain('нужна загрузка');
  });

  it('V2 с файлами но без findings → "не проверена"', () => {
    const p = {
      version_id: 'v2',
      version_count: 1,
      findings_count: 0,
      versions_summary: [
        { version_id: 'v2', label: 'V2', is_latest: true, has_source_files: true },
      ],
    };
    expect(VersionAPI.formatVersionBadge(p)).toEqual({ text: 'не проверена', tone: 'muted' });
  });
});

describe('canStartAudit', () => {
  it('null version → не запускаем', () => {
    const r = VersionAPI.canStartAudit(null);
    expect(r.ok).toBe(false);
    expect(r.code).toBe('no_version');
  });

  it('V1 с can_run_audit=true → ok', () => {
    const r = VersionAPI.canStartAudit({ version_id: 'v1', can_run_audit: true });
    expect(r.ok).toBe(true);
    expect(r.code).toBe('ok');
  });

  it('V2 без source files → not ok, code=no_files', () => {
    const r = VersionAPI.canStartAudit({
      version_id: 'v2', can_run_audit: false, has_source_files: false,
    });
    expect(r.ok).toBe(false);
    expect(r.code).toBe('no_files');
    expect(r.reason).toMatch(/PDF/);
  });

  it('V2 с файлами + serverCaps.v2AuditSupported=false → not ok, code=runner_no_v2', () => {
    const r = VersionAPI.canStartAudit(
      { version_id: 'v2', label: 'V2', can_run_audit: true, has_source_files: true },
      { serverCaps: { v2AuditSupported: false } },
    );
    expect(r.ok).toBe(false);
    expect(r.code).toBe('runner_no_v2');
    expect(r.reason).toMatch(/legacy runner/);
  });

  it('V2 с файлами + v2AuditSupported=true → ok', () => {
    const r = VersionAPI.canStartAudit(
      { version_id: 'v2', can_run_audit: true, has_source_files: true },
      { serverCaps: { v2AuditSupported: true } },
    );
    expect(r.ok).toBe(true);
    expect(r.code).toBe('ok');
  });

  it('V1 не блокируется legacy serverCaps', () => {
    const r = VersionAPI.canStartAudit(
      { version_id: 'v1', can_run_audit: true },
      { serverCaps: { v2AuditSupported: false } },
    );
    expect(r.ok).toBe(true);
  });

  it('serverCaps по умолчанию v2AuditSupported=true (на случай отсутствия opts)', () => {
    const r = VersionAPI.canStartAudit({
      version_id: 'v2', can_run_audit: true, has_source_files: true,
    });
    expect(r.ok).toBe(true);
  });

  it('V2 без version_id (синтетика) трактуется как V1', () => {
    // Если entry пришёл без явного version_id — это legacy-проект без manifest,
    // и мы не должны его блокировать runner-гейтом.
    const r = VersionAPI.canStartAudit(
      { can_run_audit: true },
      { serverCaps: { v2AuditSupported: false } },
    );
    expect(r.ok).toBe(true);
  });
});

describe('v2EmptyStubFor', () => {
  const LEGACY = { v2AuditSupported: false };
  const NEW = { v2AuditSupported: true };

  it('null version → null (запрос проходит)', () => {
    expect(VersionAPI.v2EmptyStubFor('/findings/X', null, LEGACY)).toBeNull();
  });

  it('v1 → null', () => {
    expect(VersionAPI.v2EmptyStubFor('/findings/X', 'v1', LEGACY)).toBeNull();
  });

  it('v2 + новый runner → null (V2 reads уже work)', () => {
    expect(VersionAPI.v2EmptyStubFor('/findings/X', 'v2', NEW)).toBeNull();
  });

  it('v2 + legacy + /findings/{id} → пустой findings stub', () => {
    const s = VersionAPI.v2EmptyStubFor('/findings/MyProj', 'v2', LEGACY);
    expect(s).toEqual({ findings: [], total: 0, by_severity: {} });
  });

  it('v2 + legacy + /findings/{id}/block-map → пустой block map', () => {
    const s = VersionAPI.v2EmptyStubFor('/findings/X/block-map', 'v2', LEGACY);
    expect(s).toEqual({ block_map: {}, block_info: {}, text_evidence: {} });
  });

  it('v2 + legacy + /optimization/{id} → has_data=false', () => {
    expect(VersionAPI.v2EmptyStubFor('/optimization/X', 'v2', LEGACY))
      .toEqual({ has_data: false, data: null });
  });

  it('v2 + legacy + /document/{id}/pages → empty pages', () => {
    expect(VersionAPI.v2EmptyStubFor('/document/X/pages', 'v2', LEGACY))
      .toEqual({ pages: [], total: 0 });
  });

  it('v2 + legacy + /document/{id}/page/3 → empty page', () => {
    const s = VersionAPI.v2EmptyStubFor('/document/X/page/3', 'v2', LEGACY);
    expect(s.page).toBeNull();
    expect(s.blocks).toEqual([]);
  });

  it('v2 + legacy + /tiles/{id}/blocks → empty blocks', () => {
    expect(VersionAPI.v2EmptyStubFor('/tiles/X/blocks', 'v2', LEGACY))
      .toEqual({ blocks: [], total: 0 });
  });

  it('v2 + legacy + другие пути → null (запрос проходит)', () => {
    // versions, files, migrated-findings — это специальные роуты для V2,
    // они уже корректно работают на legacy. НЕ подменяем.
    expect(VersionAPI.v2EmptyStubFor('/projects/X/versions', 'v2', LEGACY)).toBeNull();
    expect(VersionAPI.v2EmptyStubFor('/projects/X/versions/v2/files', 'v2', LEGACY)).toBeNull();
    expect(VersionAPI.v2EmptyStubFor('/projects/X/versions/v2/migrated-findings/report', 'v2', LEGACY)).toBeNull();
    expect(VersionAPI.v2EmptyStubFor('/audit/X/live-status', 'v2', LEGACY)).toBeNull();
  });

  it('игнорирует query string', () => {
    expect(VersionAPI.v2EmptyStubFor('/findings/X?severity=KR', 'v2', LEGACY))
      .toEqual({ findings: [], total: 0, by_severity: {} });
  });

  it('v3+ тоже срабатывает', () => {
    expect(VersionAPI.v2EmptyStubFor('/findings/X', 'v3', LEGACY))
      .toEqual({ findings: [], total: 0, by_severity: {} });
  });
});

describe('describeAuditError', () => {
  it('409 legacy runner → короткий title + полный detail', () => {
    const e = VersionAPI.describeAuditError(
      409,
      "Запуск аудита версии 'v2' временно недоступен в legacy runner. ...",
    );
    expect(e.code).toBe('runner_no_v2');
    expect(e.title).toMatch(/временно недоступен/);
    expect(e.detail).toMatch(/legacy runner/);
  });

  it('409 без legacy runner → conflict', () => {
    const e = VersionAPI.describeAuditError(409, 'Аудит уже запущен');
    expect(e.code).toBe('conflict');
    expect(e.title).toBe('Аудит уже запущен');
  });

  it('404 → not_found', () => {
    const e = VersionAPI.describeAuditError(404, '');
    expect(e.code).toBe('not_found');
    expect(e.title).toMatch(/не найден/);
  });

  it('400 → bad_request, показывает detail', () => {
    const e = VersionAPI.describeAuditError(400, 'PDF не найден');
    expect(e.code).toBe('bad_request');
    expect(e.title).toBe('PDF не найден');
  });

  it('500 → error fallback', () => {
    const e = VersionAPI.describeAuditError(500, '');
    expect(e.code).toBe('error');
    expect(e.title).toMatch(/500/);
  });
});

describe('describeUploadError', () => {
  it('409 → подсказка про "Заменить"', () => {
    expect(VersionAPI.describeUploadError(409, '')).toMatch(/уже существует/);
  });
  it('403 → запрет', () => {
    expect(VersionAPI.describeUploadError(403, '')).toMatch(/запрещена/);
  });
  it('404 → версия не найдена', () => {
    expect(VersionAPI.describeUploadError(404, '')).toMatch(/не найдена/);
  });
  it('400 показывает detail backend', () => {
    expect(VersionAPI.describeUploadError(400, "Расширение '.exe' не разрешено")).toContain(
      ".exe",
    );
  });
});

// ─── Migrated Findings helpers ───

describe('migratedFindingsReportUrl', () => {
  it('собирает GET-URL БЕЗ ?version_id= (version в path)', () => {
    const url = VersionAPI.migratedFindingsReportUrl('M31A', 'v2');
    expect(url).toBe('/api/projects/M31A/versions/v2/migrated-findings/report');
    expect(url).not.toContain('?');
  });

  it('URL-encode непростые id', () => {
    const url = VersionAPI.migratedFindingsReportUrl('АР/133', 'v2');
    expect(url).toContain('%D0%90%D0%A0%2F133');
  });
});

describe('migratedFindingsCheckUrl', () => {
  it('собирает POST-URL БЕЗ ?version_id=', () => {
    const url = VersionAPI.migratedFindingsCheckUrl('M31A', 'v3');
    expect(url).toBe('/api/projects/M31A/versions/v3/migrated-findings/check');
    expect(url).not.toContain('?');
  });
});

describe('canRunMigratedCheck', () => {
  it('null → не запускаем', () => {
    expect(VersionAPI.canRunMigratedCheck(null).ok).toBe(false);
  });
  it('v1 → не запускаем (нет более ранней версии)', () => {
    const r = VersionAPI.canRunMigratedCheck('v1');
    expect(r.ok).toBe(false);
    expect(r.reason).toMatch(/V2/);
  });
  it('v2 → ok', () => {
    expect(VersionAPI.canRunMigratedCheck('v2')).toEqual({ ok: true, reason: '' });
  });
  it('v3 → ok', () => {
    expect(VersionAPI.canRunMigratedCheck('v3').ok).toBe(true);
  });
});

describe('formatMigratedStatusLabel', () => {
  it('still_relevant → "Осталось актуальным"', () => {
    expect(VersionAPI.formatMigratedStatusLabel('still_relevant')).toBe('Осталось актуальным');
  });
  it('duplicate_of_new_finding → "Уже найдено в V2"', () => {
    expect(VersionAPI.formatMigratedStatusLabel('duplicate_of_new_finding')).toBe('Уже найдено в V2');
  });
  it('resolved_in_new_version → "Устранено"', () => {
    expect(VersionAPI.formatMigratedStatusLabel('resolved_in_new_version')).toBe('Устранено');
  });
  it('not_verifiable → "Нужна ручная проверка"', () => {
    expect(VersionAPI.formatMigratedStatusLabel('not_verifiable')).toBe('Нужна ручная проверка');
  });
  it('source_missing → "Источник отсутствует"', () => {
    expect(VersionAPI.formatMigratedStatusLabel('source_missing')).toBe('Источник отсутствует');
  });
  it('unknown → возвращает as-is', () => {
    expect(VersionAPI.formatMigratedStatusLabel('something_else')).toBe('something_else');
    expect(VersionAPI.formatMigratedStatusLabel('')).toBe('—');
  });
});

describe('formatMigratedStatusTone', () => {
  it('still_relevant → warn (важно, надо смотреть)', () => {
    expect(VersionAPI.formatMigratedStatusTone('still_relevant')).toBe('warn');
  });
  it('resolved_in_new_version → success', () => {
    expect(VersionAPI.formatMigratedStatusTone('resolved_in_new_version')).toBe('success');
  });
  it('unknown → muted', () => {
    expect(VersionAPI.formatMigratedStatusTone('xxx')).toBe('muted');
  });
});

describe('summarizeMigratedReport', () => {
  it('null report → hasReport=false и нули', () => {
    const s = VersionAPI.summarizeMigratedReport(null);
    expect(s.hasReport).toBe(false);
    expect(s.total).toBe(0);
    expect(s.stillRelevant).toBe(0);
    expect(s.duplicate).toBe(0);
    expect(s.resolved).toBe(0);
    expect(s.itemsCount).toBe(0);
  });

  it('backend-формат: counts разложены на верхнем уровне report', () => {
    // Так пишет backend.app.services.findings.migrated_findings_service:
    // write_migrated_findings_report спредит summary в корень report.
    const report = {
      source_version_id: 'v1',
      total_previous_accepted_findings: 7,
      checked_at: '2026-05-13T10:00:00Z',
      still_relevant: 3,
      duplicate_of_new_finding: 2,
      resolved_in_new_version: 1,
      not_verifiable: 1,
      source_missing: 0,
      items: [{}, {}, {}, {}, {}, {}, {}],
    };
    const s = VersionAPI.summarizeMigratedReport(report);
    expect(s.hasReport).toBe(true);
    expect(s.sourceVersionId).toBe('v1');
    expect(s.total).toBe(7);
    expect(s.stillRelevant).toBe(3);
    expect(s.duplicate).toBe(2);
    expect(s.resolved).toBe(1);
    expect(s.notVerifiable).toBe(1);
    expect(s.sourceMissing).toBe(0);
    expect(s.itemsCount).toBe(7);
    expect(s.checkedAt).toBe('2026-05-13T10:00:00Z');
  });

  it('legacy-формат с {counts: {...}} тоже поддерживается', () => {
    const report = {
      source_version_id: 'v1',
      total_previous_accepted_findings: 2,
      counts: {
        still_relevant: 1,
        duplicate_of_new_finding: 1,
        resolved_in_new_version: 0,
        not_verifiable: 0,
        source_missing: 0,
      },
      items: [{}, {}],
    };
    const s = VersionAPI.summarizeMigratedReport(report);
    expect(s.stillRelevant).toBe(1);
    expect(s.duplicate).toBe(1);
  });

  it('report без counts → нули', () => {
    const s = VersionAPI.summarizeMigratedReport({
      source_version_id: 'v1',
      total_previous_accepted_findings: 0,
    });
    expect(s.hasReport).toBe(true);
    expect(s.stillRelevant).toBe(0);
  });
});

describe('describeMigratedCheckError', () => {
  it('400 c "V1" в detail → подсказка про V2+', () => {
    const msg = VersionAPI.describeMigratedCheckError(400, 'Контроль не доступен для V1');
    expect(msg).toMatch(/V2/);
  });
  it('400 без V1-detail → возвращает detail', () => {
    expect(VersionAPI.describeMigratedCheckError(400, 'Нет previous version')).toBe(
      'Нет previous version',
    );
  });
  it('404 → "не найдены"', () => {
    expect(VersionAPI.describeMigratedCheckError(404, '')).toMatch(/не найдены/);
  });
  it('500 → fallback', () => {
    expect(VersionAPI.describeMigratedCheckError(500, '')).toMatch(/500/);
  });
});

describe('findingMigratedBadge', () => {
  it('null → null', () => {
    expect(VersionAPI.findingMigratedBadge(null)).toBeNull();
  });
  it('обычный finding → null', () => {
    expect(VersionAPI.findingMigratedBadge({ id: 'F-001', severity: 'KR' })).toBeNull();
  });
  it('is_migrated=true → бейдж "Из V1"', () => {
    const b = VersionAPI.findingMigratedBadge({
      id: 'MIG-V1-F-003', is_migrated: true, origin_version_id: 'v1',
    });
    expect(b).toEqual({ text: 'Из V1', tone: 'warn' });
  });
  it('source_type="migrated_from_previous_version" → тоже бейдж', () => {
    const b = VersionAPI.findingMigratedBadge({
      source_type: 'migrated_from_previous_version', origin_version_id: 'v2',
    });
    expect(b.text).toBe('Из V2');
    expect(b.tone).toBe('warn');
  });
  it('has_origin_from_previous_version → "Связано с V1"', () => {
    const b = VersionAPI.findingMigratedBadge({
      has_origin_from_previous_version: true, origin_version_id: 'v1',
    });
    expect(b).toEqual({ text: 'Связано с V1', tone: 'info' });
  });
  it('migrated без origin_version_id → fallback V1', () => {
    const b = VersionAPI.findingMigratedBadge({ is_migrated: true });
    expect(b.text).toBe('Из V1');
  });
});
