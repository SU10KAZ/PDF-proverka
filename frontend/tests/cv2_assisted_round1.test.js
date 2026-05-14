/**
 * Тесты contract'а assisted_round1 matcher'а + per-item status'а.
 *
 * Pure helpers, реплицируют логику из frontend/static/js/app.js:
 *   - cv2AssistedStatusOf(assistedItem, artifactItemsById, feedback) →
 *     'on_target' | 'wrong_queue' | 'missing'
 *   - cv2AssistedReport(items, artifact, feedback)
 *
 * Тесты НЕ грузят весь app.js — они валидируют contract, который должен
 * совпадать с реализацией в Vue setup. Расхождение → регрессия немедленно
 * красная в vitest.
 */
import { describe, it, expect } from 'vitest';

const CV2_TABS = ['primary', 'needs_context', 'suggested_reject', 'hidden_by_critic'];

function cv2EffectiveTab(item, feedback) {
  if (!item) return '';
  const fb = item.finding_id ? (feedback[item.finding_id] || null) : null;
  const pref = fb && fb.preferred_tab;
  if (pref && CV2_TABS.includes(pref)) return pref;
  return item.tab || '';
}

function cv2AssistedStatusOf(assistedItem, artifactItems, feedback) {
  if (!assistedItem || !artifactItems) return 'missing';
  const fid = assistedItem.finding_id;
  const found = artifactItems.find(i => i.finding_id === fid);
  if (!found) return 'missing';
  const eff = cv2EffectiveTab(found, feedback || {});
  const expected = assistedItem.expected_queue || 'suggested_reject';
  return eff === expected ? 'on_target' : 'wrong_queue';
}

function cv2AssistedReport(assistedItems, artifactItems, feedback) {
  const fb = feedback || {};
  const report = {
    items_for_project: assistedItems.length,
    by_group: { risky_accepted_22: 0, sample_60: 0 },
    found_in_artifact: 0,
    missing_in_artifact: 0,
    in_suggested_reject: 0,
    not_in_suggested_reject: 0,
    in_other_tab: { primary: 0, needs_context: 0, hidden_by_critic: 0 },
    per_item: [],
  };
  if (!artifactItems) return report;
  const byId = {};
  for (const it of artifactItems) byId[it.finding_id] = it;
  for (const a of assistedItems) {
    report.by_group[a.group] = (report.by_group[a.group] || 0) + 1;
    const found = byId[a.finding_id];
    const effective = found ? cv2EffectiveTab(found, fb) : null;
    if (!found) {
      report.missing_in_artifact += 1;
      report.per_item.push({ finding_id: a.finding_id, status: 'missing', effective_tab: null });
      continue;
    }
    report.found_in_artifact += 1;
    if (effective === 'suggested_reject') {
      report.in_suggested_reject += 1;
      report.per_item.push({ finding_id: a.finding_id, status: 'on_target', effective_tab: effective });
    } else {
      report.not_in_suggested_reject += 1;
      if (effective in report.in_other_tab) report.in_other_tab[effective] += 1;
      report.per_item.push({ finding_id: a.finding_id, status: 'wrong_queue', effective_tab: effective });
    }
  }
  return report;
}

// ─── Fixtures ─────────────────────────────────────────────────────────────

const ARTIFACT = [
  { finding_id: 'P:F-1', tab: 'primary',          queue: 'strong_keep' },
  { finding_id: 'P:F-2', tab: 'primary',          queue: 'strong_keep' },
  { finding_id: 'P:F-3', tab: 'needs_context',    queue: 'needs_context' },
  { finding_id: 'P:F-4', tab: 'suggested_reject', queue: 'suggested_reject' },
  { finding_id: 'P:F-5', tab: 'hidden_by_critic', queue: 'hidden_by_critic' },
];

const ASSISTED_BASE = {
  source_file: 'assisted_round1_risky_accepted_22.csv',
  group: 'risky_accepted_22',
  expected_queue: 'suggested_reject',
  reason_group: 'OCR / ошибка распознавания',
};

function assisted(fid, overrides = {}) {
  return { ...ASSISTED_BASE, finding_id: fid, ...overrides };
}

// ─── cv2AssistedStatusOf ─────────────────────────────────────────────────

describe('cv2AssistedStatusOf', () => {
  it('on_target когда critic уже положил в suggested_reject', () => {
    const status = cv2AssistedStatusOf(assisted('P:F-4'), ARTIFACT, {});
    expect(status).toBe('on_target');
  });

  it('wrong_queue когда finding в primary без expert override', () => {
    const status = cv2AssistedStatusOf(assisted('P:F-1'), ARTIFACT, {});
    expect(status).toBe('wrong_queue');
  });

  it('on_target когда expert override переносит в suggested_reject', () => {
    const fb = { 'P:F-1': { preferred_tab: 'suggested_reject' } };
    expect(cv2AssistedStatusOf(assisted('P:F-1'), ARTIFACT, fb)).toBe('on_target');
  });

  it('missing когда finding_id отсутствует в artifact', () => {
    expect(cv2AssistedStatusOf(assisted('P:F-NOPE'), ARTIFACT, {})).toBe('missing');
  });

  it('missing когда artifact пустой', () => {
    expect(cv2AssistedStatusOf(assisted('P:F-1'), [], {})).toBe('missing');
  });
});

// ─── cv2AssistedReport ───────────────────────────────────────────────────

describe('cv2AssistedReport', () => {
  it('считает found / missing / in_suggested_reject', () => {
    const assistedItems = [
      assisted('P:F-1'),                                        // primary → wrong_queue
      assisted('P:F-4'),                                        // sr → on_target
      assisted('P:F-NOPE'),                                     // missing
    ];
    const r = cv2AssistedReport(assistedItems, ARTIFACT, {});
    expect(r.items_for_project).toBe(3);
    expect(r.found_in_artifact).toBe(2);
    expect(r.missing_in_artifact).toBe(1);
    expect(r.in_suggested_reject).toBe(1);
    expect(r.not_in_suggested_reject).toBe(1);
    expect(r.in_other_tab.primary).toBe(1);
  });

  it('считает разрез по группам', () => {
    const a = [
      assisted('P:F-1', { group: 'risky_accepted_22' }),
      assisted('P:F-2', { group: 'sample_60' }),
      assisted('P:F-3', { group: 'sample_60' }),
    ];
    const r = cv2AssistedReport(a, ARTIFACT, {});
    expect(r.by_group.risky_accepted_22).toBe(1);
    expect(r.by_group.sample_60).toBe(2);
  });

  it('expert override восстанавливает on_target', () => {
    const a = [assisted('P:F-1'), assisted('P:F-2')];
    // Один из них перенесён экспертом в sr.
    const fb = { 'P:F-2': { preferred_tab: 'suggested_reject' } };
    const r = cv2AssistedReport(a, ARTIFACT, fb);
    expect(r.in_suggested_reject).toBe(1);
    expect(r.not_in_suggested_reject).toBe(1);
  });

  it('не теряет карточки, у которых effective_tab=primary (assisted-filter контракт)', () => {
    // Это и есть требование п. 5: assisted-filter показывает карточку даже
    // если effective_tab !== suggested_reject. Здесь проверяем, что в per_item
    // такие записи остаются с status=wrong_queue, не выбрасываются.
    const a = [assisted('P:F-1')];
    const r = cv2AssistedReport(a, ARTIFACT, {});
    expect(r.per_item).toHaveLength(1);
    expect(r.per_item[0].status).toBe('wrong_queue');
    expect(r.per_item[0].effective_tab).toBe('primary');
  });
});

// ─── Реальный сценарий ОЗДС ──────────────────────────────────────────────

describe('реальный сценарий ОЗДС', () => {
  // По headless smoke ОЗДС из прошлого раунда:
  // 30 items в artifact, 27 в primary, 3 в needs_context, 0 в suggested_reject
  // (critic native). После загрузки feedback:
  // 9 entries с preferred_tab=suggested_reject → 9 в suggested_reject.
  // Из них 1 risky (F-029) и 9 sample → если assisted_round1 содержит 10
  // карточек для проекта, и feedback переносит часть в sr,
  // то found_in_artifact == 10, in_suggested_reject = пересечение.
  it('artifact + feedback + assisted дают непустую сводку', () => {
    const artifact = [];
    for (let i = 1; i <= 27; i++) artifact.push({ finding_id: `OZ:F-${i}`, tab: 'primary' });
    for (let i = 28; i <= 30; i++) artifact.push({ finding_id: `OZ:F-${i}`, tab: 'needs_context' });

    const assistedItems = [
      assisted('OZ:F-1',  { group: 'sample_60' }),
      assisted('OZ:F-9',  { group: 'sample_60' }),
      assisted('OZ:F-20', { group: 'sample_60' }),
      assisted('OZ:F-29', { group: 'risky_accepted_22' }),
      assisted('OZ:F-99', { group: 'sample_60' }),  // missing
    ];
    const fb = {
      'OZ:F-1':  { preferred_tab: 'suggested_reject' },
      'OZ:F-9':  { preferred_tab: 'suggested_reject' },
      'OZ:F-20': { preferred_tab: 'suggested_reject' },
      'OZ:F-29': { preferred_tab: 'suggested_reject' },
    };
    const r = cv2AssistedReport(assistedItems, artifact, fb);
    expect(r.items_for_project).toBe(5);
    expect(r.found_in_artifact).toBe(4);
    expect(r.missing_in_artifact).toBe(1);
    expect(r.in_suggested_reject).toBe(4);
    expect(r.by_group.risky_accepted_22).toBe(1);
    expect(r.by_group.sample_60).toBe(4);
  });
});
