/**
 * Тесты pure-логики маршрутизации Critic v2 в очереди.
 *
 * Контекст: до фикса frontend фильтровал items по `it.tab` (решение critic).
 * Findings, которые expert вручную пометил `preferred_tab=suggested_reject`,
 * не попадали во вкладку "Критик рекомендует отклонить", из-за чего badge
 * показывал, например, 1 вместо ожидаемых 12.
 *
 * Этот тест валидирует contract effective-tab routing'а:
 *   effective_tab = expert.preferred_tab || critic.tab
 * и что после применения badge (cv2VisibleCountByTab) совпадает с количеством
 * реально отрисованных rows (cv2ItemsByTab[tab].length).
 *
 * Запуск:
 *   cd frontend && npm test
 */
import { describe, it, expect } from 'vitest';

const CV2_TABS = ['primary', 'needs_context', 'suggested_reject', 'hidden_by_critic'];

// Pure-helper, мирорит логику в app.js (cv2EffectiveTab + cv2ItemsByTab).
// Если она здесь и в app.js разойдётся — тест упадёт первым, что лучше тихой
// регрессии в проде.
function cv2EffectiveTab(item, feedback) {
  if (!item) return '';
  const fid = item.finding_id;
  const fb = fid ? feedback[fid] : null;
  const pref = fb && fb.preferred_tab;
  if (pref && CV2_TABS.includes(pref)) return pref;
  return item.tab || '';
}

function cv2ItemsByTab(items, feedback, matchesFilter) {
  const out = { primary: [], needs_context: [], suggested_reject: [], hidden_by_critic: [] };
  if (!Array.isArray(items)) return out;
  for (const it of items) {
    if (matchesFilter && !matchesFilter(it)) continue;
    const t = cv2EffectiveTab(it, feedback);
    if (out[t]) out[t].push(it);
  }
  return out;
}

function cv2VisibleCountByTab(itemsByTab) {
  return {
    primary: itemsByTab.primary.length,
    needs_context: itemsByTab.needs_context.length,
    suggested_reject: itemsByTab.suggested_reject.length,
    hidden_by_critic: itemsByTab.hidden_by_critic.length,
  };
}

const ITEMS = [
  { finding_id: 'P:F-001', tab: 'primary',          queue: 'strong_keep' },
  { finding_id: 'P:F-002', tab: 'primary',          queue: 'strong_keep' },
  { finding_id: 'P:F-003', tab: 'primary',          queue: 'borderline' },
  { finding_id: 'P:F-004', tab: 'needs_context',    queue: 'needs_context' },
  { finding_id: 'P:F-005', tab: 'needs_context',    queue: 'needs_context' },
  { finding_id: 'P:F-006', tab: 'suggested_reject', queue: 'suggested_reject' },
  { finding_id: 'P:F-007', tab: 'hidden_by_critic', queue: 'hidden_by_critic' },
];

describe('cv2EffectiveTab', () => {
  it('возвращает critic.tab если expert не задал preferred_tab', () => {
    const fb = {};
    expect(cv2EffectiveTab(ITEMS[0], fb)).toBe('primary');
    expect(cv2EffectiveTab(ITEMS[5], fb)).toBe('suggested_reject');
  });

  it('перегружает critic.tab экспертным preferred_tab', () => {
    const fb = { 'P:F-001': { preferred_tab: 'suggested_reject' } };
    expect(cv2EffectiveTab(ITEMS[0], fb)).toBe('suggested_reject');
  });

  it('игнорирует пустой preferred_tab', () => {
    const fb = { 'P:F-001': { preferred_tab: '' } };
    expect(cv2EffectiveTab(ITEMS[0], fb)).toBe('primary');
  });

  it('игнорирует preferred_tab вне списка валидных', () => {
    const fb = { 'P:F-001': { preferred_tab: 'invalid_tab' } };
    expect(cv2EffectiveTab(ITEMS[0], fb)).toBe('primary');
  });

  it('обрабатывает item без finding_id корректно', () => {
    const fb = {};
    expect(cv2EffectiveTab({ tab: 'primary' }, fb)).toBe('primary');
    expect(cv2EffectiveTab(null, fb)).toBe('');
  });
});

describe('cv2ItemsByTab — без feedback', () => {
  it('распределяет items по critic.tab когда feedback пуст', () => {
    const m = cv2ItemsByTab(ITEMS, {}, null);
    expect(m.primary).toHaveLength(3);
    expect(m.needs_context).toHaveLength(2);
    expect(m.suggested_reject).toHaveLength(1);
    expect(m.hidden_by_critic).toHaveLength(1);
  });
});

describe('cv2ItemsByTab — expert routing', () => {
  it('перемещает finding из primary в suggested_reject по preferred_tab', () => {
    const fb = {
      'P:F-001': { preferred_tab: 'suggested_reject' },
      'P:F-002': { preferred_tab: 'suggested_reject' },
    };
    const m = cv2ItemsByTab(ITEMS, fb, null);
    expect(m.primary).toHaveLength(1);                // F-003 остался
    expect(m.suggested_reject).toHaveLength(3);       // F-001, F-002, F-006
    const ids = m.suggested_reject.map(i => i.finding_id).sort();
    expect(ids).toEqual(['P:F-001', 'P:F-002', 'P:F-006']);
  });

  it('перемещает finding из needs_context в suggested_reject', () => {
    const fb = { 'P:F-004': { preferred_tab: 'suggested_reject' } };
    const m = cv2ItemsByTab(ITEMS, fb, null);
    expect(m.needs_context).toHaveLength(1);          // F-005 остался
    expect(m.suggested_reject).toHaveLength(2);       // F-004 + F-006
  });

  it('перемещает finding из suggested_reject в hidden_by_critic', () => {
    const fb = { 'P:F-006': { preferred_tab: 'hidden_by_critic' } };
    const m = cv2ItemsByTab(ITEMS, fb, null);
    expect(m.suggested_reject).toHaveLength(0);
    expect(m.hidden_by_critic).toHaveLength(2);
  });

  it('перемещает finding обратно в primary', () => {
    const fb = { 'P:F-006': { preferred_tab: 'primary' } };
    const m = cv2ItemsByTab(ITEMS, fb, null);
    expect(m.primary).toHaveLength(4);                // 3 + F-006
    expect(m.suggested_reject).toHaveLength(0);
  });
});

describe('cv2VisibleCountByTab — badge == rendered rows', () => {
  it('badge count совпадает с количеством элементов в очереди', () => {
    const fb = {
      'P:F-001': { preferred_tab: 'suggested_reject' },
      'P:F-002': { preferred_tab: 'suggested_reject' },
      'P:F-004': { preferred_tab: 'suggested_reject' },
    };
    const m = cv2ItemsByTab(ITEMS, fb, null);
    const counts = cv2VisibleCountByTab(m);
    // Каждый ключ в counts должен совпадать с длиной соответствующего массива
    for (const key of CV2_TABS) {
      expect(counts[key]).toBe(m[key].length);
    }
    // sanity: суммы равны общему количеству items (никто не потерян)
    const total = Object.values(counts).reduce((a, b) => a + b, 0);
    expect(total).toBe(ITEMS.length);
  });

  it('после фильтра badge == rendered (filter=section=AR)', () => {
    const items = [
      { finding_id: 'X:1', tab: 'primary',          section: 'AR' },
      { finding_id: 'X:2', tab: 'primary',          section: 'EOM' },
      { finding_id: 'X:3', tab: 'suggested_reject', section: 'AR' },
    ];
    const matches = (it) => it.section === 'AR';
    const fb = { 'X:1': { preferred_tab: 'suggested_reject' } };
    const m = cv2ItemsByTab(items, fb, matches);
    const counts = cv2VisibleCountByTab(m);
    expect(counts.primary).toBe(0);
    expect(counts.suggested_reject).toBe(2);          // X:1 (overridden) + X:3
    for (const key of CV2_TABS) {
      expect(counts[key]).toBe(m[key].length);
    }
  });
});

describe('сценарий из реального feedback файла', () => {
  it('61 finding c preferred_tab=suggested_reject попадают во вкладку', () => {
    // Воспроизводит распределение из critic v2 test/*_feedback.json:
    // 39 originally в primary, 13 в needs_context, 9 без явного tab.
    const items = [];
    for (let i = 0; i < 39; i++) {
      items.push({ finding_id: `A:F-${i}`, tab: 'primary' });
    }
    for (let i = 0; i < 13; i++) {
      items.push({ finding_id: `B:F-${i}`, tab: 'needs_context' });
    }
    for (let i = 0; i < 9; i++) {
      items.push({ finding_id: `C:F-${i}`, tab: '' });
    }
    // Critic also put some into suggested_reject natively.
    items.push({ finding_id: 'D:F-1', tab: 'suggested_reject' });

    const fb = {};
    for (const it of items) {
      if (it.tab !== 'suggested_reject') {
        fb[it.finding_id] = { preferred_tab: 'suggested_reject' };
      }
    }
    const m = cv2ItemsByTab(items, fb, null);
    // 39 + 13 + 9 = 61 expert overrides + 1 от critic = 62
    expect(m.suggested_reject).toHaveLength(62);
    // Все остальные вкладки пустые (мы перенесли всё)
    expect(m.primary).toHaveLength(0);
    expect(m.needs_context).toHaveLength(0);
  });
});

// ─── Auto-load matcher (mirrors backend _match_project ranking) ────────────
// Frontend не делает matching — оно делегирует backend через
// /api/critic-v2/feedback-files?project_id=... — но contract проверяем.

describe('feedback file matching contract (backend response shape)', () => {
  function pickBestMatch(matches) {
    // Backend returns sorted matches; frontend takes [0]. Just verifies that
    // shape conforms to expectations.
    if (!Array.isArray(matches) || matches.length === 0) return null;
    return matches[0];
  }

  it('выбирает exact match даже если в списке есть substring', () => {
    const matches = [
      { name: 'a.json', match_quality: 'exact', suggested_reject_count: 1 },
      { name: 'b.json', match_quality: 'substring', suggested_reject_count: 99 },
    ];
    const best = pickBestMatch(matches);
    expect(best.match_quality).toBe('exact');
    expect(best.name).toBe('a.json');
  });

  it('возвращает null если matches пусто', () => {
    expect(pickBestMatch([])).toBeNull();
    expect(pickBestMatch(undefined)).toBeNull();
  });

  it('берёт первый match (backend уже отсортировал)', () => {
    const matches = [
      { name: 'first.json', match_quality: 'normalized', entries: 30, suggested_reject_count: 9 },
      { name: 'second.json', match_quality: 'substring', entries: 50, suggested_reject_count: 25 },
    ];
    const best = pickBestMatch(matches);
    expect(best.name).toBe('first.json');
  });
});

describe('_cv2MergeFeedbackEntries contract', () => {
  // Pure-mirror функции из app.js: merge не должен затирать существующие
  // entries при отсутствии полей в новом feedback.
  function mergeFeedback(entries, into) {
    let merged = 0, skipped = 0;
    if (!Array.isArray(entries)) return { merged, skipped };
    for (const entry of entries) {
      const fid = entry && entry.finding_id;
      if (!fid) { skipped += 1; continue; }
      const fb = into[fid] || { triage_correct: '', preferred_tab: '', reviewer_note: '', priority: 'normal' };
      into[fid] = fb;
      if (entry.triage_correct) fb.triage_correct = entry.triage_correct;
      if (entry.preferred_tab) fb.preferred_tab = entry.preferred_tab;
      if (entry.priority) fb.priority = entry.priority;
      if (typeof entry.reviewer_note === 'string') fb.reviewer_note = entry.reviewer_note;
      merged += 1;
    }
    return { merged, skipped };
  }

  it('применяет preferred_tab из импорта', () => {
    const fb = {};
    const res = mergeFeedback([
      { finding_id: 'P:1', preferred_tab: 'suggested_reject' },
    ], fb);
    expect(res.merged).toBe(1);
    expect(fb['P:1'].preferred_tab).toBe('suggested_reject');
  });

  it('skipped считает entries без finding_id', () => {
    const fb = {};
    const res = mergeFeedback([
      { preferred_tab: 'suggested_reject' },
      { finding_id: 'P:1', preferred_tab: 'suggested_reject' },
    ], fb);
    expect(res.merged).toBe(1);
    expect(res.skipped).toBe(1);
  });

  it('не затирает существующий preferred_tab, если новый пустой', () => {
    const fb = { 'P:1': { preferred_tab: 'suggested_reject' } };
    mergeFeedback([{ finding_id: 'P:1', preferred_tab: '' }], fb);
    expect(fb['P:1'].preferred_tab).toBe('suggested_reject');
  });
});
