/**
 * version_api.js — pure-helpers версионности проектов для SPA.
 *
 * Этот файл намеренно free of Vue/DOM: только чистые функции. Используется
 * как из `app.js` (через global `window.VersionAPI` для CDN-сборки), так и из
 * vitest-тестов (через ESM import).
 *
 * Обязанности:
 * 1. Сборка URL для backend-API с подмешиванием `?version_id=`.
 * 2. Разбор/сборка URL-хеша SPA, который хранит активную версию проекта.
 * 3. Формирование badge-строки для карточки проекта.
 *
 * Никаких fetch здесь нет — приватность тестов важнее DRY.
 */

const API_PREFIX = '/api';

/**
 * Сформировать абсолютный URL к backend-API.
 *
 * @param {string} path        — путь без префикса `/api`, может содержать `?...` уже.
 * @param {object} [options]
 * @param {string|null} [options.versionId] — активная версия (v1/v2/...);
 *     если null/undefined — параметр не подмешивается.
 * @param {boolean} [options.withVersion=true] — добавлять ли `?version_id=`;
 *     false — для endpoint'ов которые сами управляют version_id
 *     (например `/projects/M31A/versions/v2/files`).
 * @returns {string} полный URL для fetch.
 */
function apiUrl(path, options = {}) {
  const { versionId = null, withVersion = true } = options;
  const base = API_PREFIX + (path.startsWith('/') ? path : '/' + path);
  if (!withVersion || !versionId) return base;
  const sep = base.includes('?') ? '&' : '?';
  return `${base}${sep}version_id=${encodeURIComponent(versionId)}`;
}

/**
 * Достать `version_id` из строки SPA-hash или из window.location.hash.
 *
 * @param {string} hash — например `#/project/M31A/findings?version_id=v2`.
 * @returns {string|null}
 */
function parseVersionFromHash(hash) {
  if (!hash) return null;
  const qIdx = hash.indexOf('?');
  if (qIdx < 0) return null;
  const qs = hash.slice(qIdx + 1);
  const params = new URLSearchParams(qs);
  const v = params.get('version_id');
  return v ? v.trim() : null;
}

/**
 * Собрать SPA-хеш для (route, versionId).
 *
 * @param {string} route — `/project/M31A/findings` (без `#` и без query).
 * @param {string|null} versionId
 * @returns {string} `/project/M31A/findings` либо `/project/M31A/findings?version_id=v2`.
 */
function buildHashRoute(route, versionId) {
  if (!route) route = '/';
  const clean = route.split('?')[0];
  if (!versionId) return clean;
  return `${clean}?version_id=${encodeURIComponent(versionId)}`;
}

/**
 * Дисплейный badge для карточки проекта.
 *
 * Возвращает короткую человекочитаемую метку, либо null если бейдж не нужен
 * показывать (одна версия без проблем).
 *
 * @param {object} project — объект из `GET /api/projects`.
 *   Поля: version_label, version_count, latest_version_id, has_versions,
 *   versions_summary (массив записей).
 * @returns {{text: string, tone: 'info'|'warn'|'success'|'muted'}|null}
 */
function formatVersionBadge(project) {
  if (!project) return null;
  const count = project.version_count || 1;
  const summary = Array.isArray(project.versions_summary) ? project.versions_summary : [];
  const latest = summary.find(v => v && v.is_latest) || null;

  // Проверка готовности latest-версии (поля приходят из обогащённого
  // versions_summary, см. backend get_versions_summary).
  const latestEmpty = latest && latest.has_source_files === false;
  const latestNeedsAudit = (
    latest &&
    latest.has_source_files &&
    project.findings_count === 0 &&
    project.version_id !== 'v1' // V1 без findings — это legacy, не «новая»
  );

  if (count > 1) {
    const last = (latest && latest.label) || summary[summary.length - 1]?.label || `V${count}`;
    if (latestEmpty) {
      return { text: `${last} · нужна загрузка`, tone: 'warn' };
    }
    return { text: last, tone: 'info' };
  }

  // count == 1
  if (latestEmpty) {
    return { text: 'Нет файлов', tone: 'warn' };
  }
  if (latestNeedsAudit) {
    return { text: 'не проверена', tone: 'muted' };
  }
  // По умолчанию: одну версию мы не показываем, чтобы не захламлять
  // интерфейс legacy-проектов.
  return null;
}

/**
 * Можно ли запускать аудит конкретной версии?
 *
 * Возвращает `{ok, reason}` плюс `code` — стабильный тег причины, чтобы UI
 * мог дифференцировать стили («нужны файлы» vs «runner не поддерживает»).
 *
 * @param {object|null} versionEntry — запись из versions_summary.
 * @param {object} [opts]
 * @param {{v2AuditSupported?: boolean}} [opts.serverCaps] — capabilities
 *     текущего сервера. Для legacy webapp v2AuditSupported=false →
 *     V2-аудит блокируется, даже если файлы загружены.
 * @returns {{ok: boolean, reason: string, code: string}}
 */
function canStartAudit(versionEntry, opts = {}) {
  const serverCaps = opts.serverCaps || {};
  const v2Supported = serverCaps.v2AuditSupported !== false; // default true
  if (!versionEntry) return { ok: false, reason: 'Версия не выбрана', code: 'no_version' };

  const vid = versionEntry.version_id || '';
  const isV1 = !vid || vid === 'v1';

  // V2+ на сервере без поддержки V2-аудита — блок, даже если файлы есть.
  if (!isV1 && !v2Supported) {
    return {
      ok: false,
      code: 'runner_no_v2',
      reason:
        `Аудит ${versionEntry.label || vid} временно недоступен на текущем ` +
        `legacy runner. Версия и файлы сохранены, контроль ранее ` +
        `согласованных замечаний доступен. Полноценный аудит V2 появится ` +
        `после переключения на version-aware backend.`,
    };
  }

  if (versionEntry.can_run_audit) return { ok: true, reason: '', code: 'ok' };
  if (!versionEntry.has_source_files) {
    return {
      ok: false,
      code: 'no_files',
      reason: 'В этой версии нет исходных PDF/MD файлов. Сначала загрузите файлы.',
    };
  }
  return { ok: false, reason: 'Запуск аудита недоступен', code: 'unknown' };
}

/**
 * Перевод ошибки start-audit (или start-optimization) в дружелюбный текст.
 *
 * Backend safety-gate возвращает 409 с длинным текстом
 * «Запуск аудита версии 'v2' временно недоступен в legacy runner...».
 * Показывать это сырое сообщение пользователю некрасиво — заменяем коротким,
 * полный backend detail можно положить в tooltip/details.
 *
 * @param {number} status
 * @param {string} backendDetail
 * @returns {{title: string, detail: string, code: string}}
 */
function describeAuditError(status, backendDetail) {
  const raw = backendDetail || '';
  if (status === 409 && /legacy runner/i.test(raw)) {
    return {
      title: 'Запуск аудита этой версии временно недоступен.',
      detail: raw,
      code: 'runner_no_v2',
    };
  }
  if (status === 409) {
    return {
      title: raw || 'Запуск аудита недоступен.',
      detail: raw,
      code: 'conflict',
    };
  }
  if (status === 404) {
    return {
      title: 'Проект или версия не найдены.',
      detail: raw,
      code: 'not_found',
    };
  }
  if (status === 400) {
    return {
      title: raw || 'Запрос отклонён.',
      detail: raw,
      code: 'bad_request',
    };
  }
  return {
    title: raw || `Ошибка ${status}`,
    detail: raw,
    code: 'error',
  };
}


/**
 * Перевод HTTP-ошибки в дружелюбное сообщение.
 *
 * @param {number} status
 * @param {string} backendDetail — `detail` поле из FastAPI HTTPException.
 * @returns {string}
 */
function describeUploadError(status, backendDetail) {
  const fallback = backendDetail || `Ошибка ${status}`;
  if (status === 403) return 'Загрузка в эту версию запрещена. ' + (backendDetail || '');
  if (status === 404) return 'Версия проекта не найдена.';
  if (status === 409) return backendDetail || 'Такой файл уже существует. Включите «Заменить», чтобы перезаписать.';
  if (status === 400) return backendDetail || 'Файл отклонён валидацией.';
  return fallback;
}

// ─── Migrated Findings (контроль ранее согласованных замечаний) ───
//
// API:
//   GET  /api/projects/{pid}/versions/{vid}/migrated-findings/report
//   POST /api/projects/{pid}/versions/{vid}/migrated-findings/check
//
// Оба эндпоинта несут version_id в path, поэтому apiUrl вызываем
// c withVersion:false — чтобы не подмешать ?version_id= из activeVersionId.

/**
 * URL для GET migrated-findings/report.
 * @param {string} projectId
 * @param {string} versionId
 * @returns {string}
 */
function migratedFindingsReportUrl(projectId, versionId) {
  const pid = encodeURIComponent(projectId);
  const vid = encodeURIComponent(versionId);
  return apiUrl(`/projects/${pid}/versions/${vid}/migrated-findings/report`, {
    withVersion: false,
  });
}

/**
 * URL для POST migrated-findings/check.
 * @param {string} projectId
 * @param {string} versionId
 * @returns {string}
 */
function migratedFindingsCheckUrl(projectId, versionId) {
  const pid = encodeURIComponent(projectId);
  const vid = encodeURIComponent(versionId);
  return apiUrl(`/projects/${pid}/versions/${vid}/migrated-findings/check`, {
    withVersion: false,
  });
}

/**
 * Можно ли запускать контроль ранее согласованных замечаний?
 * Доступно только для V2 и выше — у V1 нет более ранней версии.
 *
 * @param {string|null} versionId
 * @returns {{ok: boolean, reason: string}}
 */
function canRunMigratedCheck(versionId) {
  if (!versionId) return { ok: false, reason: 'Версия не выбрана' };
  if (versionId === 'v1') {
    return { ok: false, reason: 'Контроль доступен только для V2 и выше.' };
  }
  return { ok: true, reason: '' };
}

const MIGRATED_STATUS_LABELS = {
  still_relevant: 'Осталось актуальным',
  duplicate_of_new_finding: 'Уже найдено в V2',
  resolved_in_new_version: 'Устранено',
  not_verifiable: 'Нужна ручная проверка',
  source_missing: 'Источник отсутствует',
};

const MIGRATED_STATUS_TONES = {
  still_relevant: 'warn',         // важно: замечание всё ещё актуально
  duplicate_of_new_finding: 'info',
  resolved_in_new_version: 'success',
  not_verifiable: 'warn',
  source_missing: 'muted',
};

/**
 * Человекочитаемая метка migration_status.
 * @param {string} status
 * @returns {string}
 */
function formatMigratedStatusLabel(status) {
  return MIGRATED_STATUS_LABELS[status] || status || '—';
}

/**
 * Цветовой тон чипа для migration_status.
 * @param {string} status
 * @returns {'info'|'warn'|'success'|'muted'}
 */
function formatMigratedStatusTone(status) {
  return MIGRATED_STATUS_TONES[status] || 'muted';
}

/**
 * Безопасный formatter для отчёта migrated_findings_report.json.
 * Принимает report-объект (или null), возвращает summary для UI.
 *
 * Поля report (см. migrated_findings_service):
 *   source_version_id, total_previous_accepted_findings,
 *   counts: {still_relevant, duplicate_of_new_finding,
 *            resolved_in_new_version, not_verifiable, source_missing},
 *   checked_at, items[].
 *
 * @param {object|null} report
 * @returns {{
 *   hasReport: boolean,
 *   sourceVersionId: string,
 *   total: number,
 *   stillRelevant: number,
 *   duplicate: number,
 *   resolved: number,
 *   notVerifiable: number,
 *   sourceMissing: number,
 *   checkedAt: string,
 *   itemsCount: number,
 * }}
 */
function summarizeMigratedReport(report) {
  if (!report || typeof report !== 'object') {
    return {
      hasReport: false,
      sourceVersionId: '',
      total: 0,
      stillRelevant: 0,
      duplicate: 0,
      resolved: 0,
      notVerifiable: 0,
      sourceMissing: 0,
      checkedAt: '',
      itemsCount: 0,
    };
  }
  // Backend пишет counts на верхнем уровне report (still_relevant, ...),
  // плюс поддерживаем устаревший формат {counts: {...}} для совместимости.
  const counts = report.counts || {};
  const pick = (key) => {
    if (typeof report[key] === 'number') return report[key];
    if (typeof counts[key] === 'number') return counts[key];
    return 0;
  };
  return {
    hasReport: true,
    sourceVersionId: report.source_version_id || '',
    total: report.total_previous_accepted_findings || 0,
    stillRelevant: pick('still_relevant'),
    duplicate: pick('duplicate_of_new_finding'),
    resolved: pick('resolved_in_new_version'),
    notVerifiable: pick('not_verifiable'),
    sourceMissing: pick('source_missing'),
    checkedAt: report.checked_at || '',
    itemsCount: Array.isArray(report.items) ? report.items.length : 0,
  };
}

/**
 * Описание ошибки при POST .../migrated-findings/check.
 * @param {number} status
 * @param {string} backendDetail
 * @returns {string}
 */
function describeMigratedCheckError(status, backendDetail) {
  if (status === 400) {
    if ((backendDetail || '').toLowerCase().includes('v1')) {
      return 'Контроль доступен только для V2 и выше.';
    }
    return backendDetail || 'Запрос отклонён.';
  }
  if (status === 404) return 'Версия или проект не найдены.';
  return backendDetail || `Ошибка контроля замечаний (${status})`;
}

/**
 * Метка для migrated-finding в общем списке замечаний.
 * Возвращает null, если у finding нет migrated-меток.
 *
 * @param {object|null} finding
 * @returns {{text: string, tone: 'info'|'warn'|'success'|'muted'}|null}
 */
function findingMigratedBadge(finding) {
  if (!finding) return null;
  const isMig = finding.is_migrated === true
    || finding.source_type === 'migrated_from_previous_version';
  if (isMig) {
    const origin = finding.origin_version_id
      ? String(finding.origin_version_id).toUpperCase()
      : 'V1';
    return { text: `Из ${origin}`, tone: 'warn' };
  }
  if (finding.has_origin_from_previous_version === true) {
    const origin = finding.origin_version_id
      ? String(finding.origin_version_id).toUpperCase()
      : 'V1';
    return { text: `Связано с ${origin}`, tone: 'info' };
  }
  return null;
}

/**
 * Pure-function: для V2+ на legacy runner (без поддержки V2-аудита) ряд
 * read-endpoints (findings/document/blocks/optimization) возвращают V1-данные,
 * потому что legacy webapp игнорирует ?version_id=. Чтобы UI не показывал
 * V1 содержимое внутри V2, фронт перехватывает такие запросы и возвращает
 * пустой stub.
 *
 * Возвращает stub-объект (тип зависит от read-endpoint'а) либо null, если
 * запрос должен пройти как обычно.
 *
 * @param {string} path — путь без /api (то, что передаётся в api()).
 * @param {string|null} versionId
 * @param {{v2AuditSupported?: boolean}} [serverCaps]
 * @returns {object|null}
 */
function v2EmptyStubFor(path, versionId, serverCaps) {
  if (!versionId || versionId === 'v1') return null;
  if (serverCaps && serverCaps.v2AuditSupported) return null;
  const clean = String(path || '').split('?')[0];

  if (/^\/?findings\/[^/]+$/.test(clean)) {
    return { findings: [], total: 0, by_severity: {} };
  }
  if (/^\/?findings\/[^/]+\/block-map$/.test(clean)) {
    return { block_map: {}, block_info: {}, text_evidence: {} };
  }
  if (/^\/?optimization\/[^/]+$/.test(clean)) {
    return { has_data: false, data: null };
  }
  if (/^\/?optimization\/[^/]+\/block-map$/.test(clean)) {
    return { block_map: {}, block_info: {} };
  }
  if (/^\/?document\/[^/]+\/pages$/.test(clean)) {
    return { pages: [], total: 0 };
  }
  if (/^\/?document\/[^/]+\/page\//.test(clean)) {
    return { page: null, blocks: [], text: '' };
  }
  if (/^\/?tiles\/[^/]+\/blocks$/.test(clean)) {
    return { blocks: [], total: 0 };
  }
  if (/^\/?tiles\/[^/]+\/blocks\/analysis$/.test(clean)) {
    return { analysis: {}, total: 0 };
  }
  return null;
}


// Browser-side: глобал для не-модульного <script> подключения.
// Тестируется через vitest, который импортирует файл и читает window.VersionAPI.
const VersionAPI = {
  apiUrl,
  parseVersionFromHash,
  buildHashRoute,
  formatVersionBadge,
  canStartAudit,
  describeAuditError,
  describeUploadError,
  v2EmptyStubFor,
  // Migrated findings
  migratedFindingsReportUrl,
  migratedFindingsCheckUrl,
  canRunMigratedCheck,
  formatMigratedStatusLabel,
  formatMigratedStatusTone,
  summarizeMigratedReport,
  describeMigratedCheckError,
  findingMigratedBadge,
};

if (typeof window !== 'undefined') {
  window.VersionAPI = VersionAPI;
}
if (typeof globalThis !== 'undefined') {
  globalThis.VersionAPI = VersionAPI;
}
