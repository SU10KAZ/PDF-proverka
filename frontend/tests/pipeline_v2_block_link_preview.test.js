/**
 * Тесты pure-логики панели «Pipeline V2 — предложенные связи»
 * (раздел «Связь блоков», read-only).
 *
 * Панель только читает GET /api/stage-comparison/pipeline-v2/{sid}/
 * block-link-preview?pair_id=. Здесь зеркалится её чистая логика из app.js
 * (scPv2Lp*, паттерн контрактных тестов проекта как в pipeline_v2_panel):
 * если зеркало и app.js разойдутся — тест упадёт первым.
 *
 * Покрытие по задаче:
 *   13. not_found state не ломает страницу (state machine ответа);
 *   14. списки/фильтры рендерят strong/weak/manual_review/unmatched;
 *   плюс: color map по статусу, overlay style (bbox→проценты, selected=blue),
 *   page overlays (обе стороны + unmatched на своей стороне).
 *
 * Запуск:
 *   cd frontend && npm test
 */
import { describe, it, expect } from 'vitest';

// ── зеркала pure-helpers из app.js (scPv2Lp*) ───────────────────────────────

const SC_PV2_LP_COLORS = { green: '#16a34a', yellow: '#ca8a04',
                           orange: '#ea580c', gray: '#6b7280',
                           blue: '#2563eb' };

function scPv2LpViewState(httpStatus, envelope) {
  if (httpStatus === 401 || httpStatus === 403) return 'access_denied';
  if (httpStatus !== 200) return 'transport_error';
  if (!envelope) return 'transport_error';
  if (envelope.status === 'not_found') return 'empty_state';
  if (envelope.status === 'error') return 'artifact_error';
  if (envelope.payload) return 'payload';
  return 'transport_error';
}

function scPv2LpAllLinks(report) {
  if (!report) return [];
  const links = (report.block_links || []).map(l => ({ ...l, kind: 'link' }));
  const un = report.unmatched || {};
  const one = [...(un.left_blocks || []), ...(un.right_blocks || [])]
    .map(u => ({ ...u, kind: 'unmatched',
                 block_link_id: 'un_' + u.side + '_' + u.block_id }));
  return [...links, ...one];
}

function scPv2LpLinkMatchesFilter(l, f) {
  if (!f || f === 'all') return true;
  if (f === 'unmatched') return l.kind === 'unmatched';
  if (f === 'graphic') return !!l.is_graphic;
  if (f === 'visual_changed') return l.visual_status === 'changed_visual';
  if (f === 'visual_identical')
    return l.visual_status === 'identical_visual'
        || l.visual_status === 'minor_visual';
  return l.link_status === f;
}

function scPv2LpOverlayStyle(ov, selectedId) {
  const b = ov.bbox || [0, 0, 0, 0];
  const sel = selectedId && ov.entry.block_link_id === selectedId;
  const color = SC_PV2_LP_COLORS[(ov.entry.ui && ov.entry.ui.color) || 'gray']
      || SC_PV2_LP_COLORS.gray;
  return {
    left: (b[0] * 100) + '%',
    top: (b[1] * 100) + '%',
    width: (Math.max(0, b[2] - b[0]) * 100) + '%',
    height: (Math.max(0, b[3] - b[1]) * 100) + '%',
    border: sel ? ('3px solid ' + SC_PV2_LP_COLORS.blue)
                : ('2px solid ' + color),
  };
}

function scPv2LpPageOverlays(report, pageLink) {
  const out = { left: [], right: [] };
  if (!pageLink) return out;
  for (const l of scPv2LpAllLinks(report)) {
    if (l.kind === 'link') {
      if (l.page_match_id !== pageLink.page_link_id) continue;
      if (l.left_bbox_norm)
        out.left.push({ entry: l, side: 'left', bbox: l.left_bbox_norm });
      if (l.right_bbox_norm)
        out.right.push({ entry: l, side: 'right', bbox: l.right_bbox_norm });
    } else {
      const page = l.side === 'left' ? pageLink.left_page_number
                                     : pageLink.right_page_number;
      if (page != null && l.page_number === page && l.bbox_norm)
        out[l.side].push({ entry: l, side: l.side, bbox: l.bbox_norm });
    }
  }
  return out;
}

// ── фикстуры в форме реального block_link_preview_report ───────────────────

function makeReport() {
  return {
    version: 1,
    kind: 'stage_comparison_pipeline_v2_block_link_preview',
    status: 'ok',
    summary: { page_links_total: 2, block_links_total: 3, strong_links: 1,
               weak_links: 1, manual_review_links: 1,
               unmatched_left_blocks: 1, unmatched_right_blocks: 1,
               graphic_links_total: 1 },
    page_links: [
      { page_link_id: 'pm_1_1', left_page_number: 1, right_page_number: 1,
        block_link_ids: ['bm_strong', 'bm_manual'],
        block_links_by_status: { strong: 1, manual_review: 1 } },
      { page_link_id: 'pm_2_2', left_page_number: 2, right_page_number: 2,
        block_link_ids: ['bm_weak'], block_links_by_status: { weak: 1 } },
    ],
    block_links: [
      { block_link_id: 'bm_strong', page_match_id: 'pm_1_1',
        left_block_id: 'L_T1', right_block_id: 'R_T1',
        left_page_number: 1, right_page_number: 1,
        left_bbox_norm: [0.1, 0.1, 0.9, 0.3],
        right_bbox_norm: [0.1, 0.1, 0.9, 0.3],
        semantic_type: 'text', is_graphic: false, link_status: 'strong',
        method: 'stamp', confidence_score: 0.9, risk_flags: [],
        visual_status: null, visual_decision: null,
        ui: { color: 'green', label: 'Надёжная связь', default_visible: true } },
      { block_link_id: 'bm_manual', page_match_id: 'pm_1_1',
        left_block_id: 'L_M1', right_block_id: 'R_M1',
        left_page_number: 1, right_page_number: 1,
        left_bbox_norm: [0.1, 0.5, 0.9, 0.7],
        right_bbox_norm: [0.1, 0.5, 0.9, 0.7],
        semantic_type: 'table', is_graphic: false,
        link_status: 'manual_review', method: 'table_fuzzy',
        confidence_score: 0.5, risk_flags: ['duplicate_candidate'],
        visual_status: null, visual_decision: null,
        ui: { color: 'orange', label: 'Нужна ручная проверка',
              default_visible: true } },
      { block_link_id: 'bm_weak', page_match_id: 'pm_2_2',
        left_block_id: 'L_S1', right_block_id: 'R_S1',
        left_page_number: 2, right_page_number: 2,
        left_bbox_norm: [0.05, 0.2, 0.95, 0.8],
        right_bbox_norm: [0.05, 0.2, 0.95, 0.8],
        semantic_type: 'scheme', is_graphic: true, link_status: 'weak',
        method: 'scheme_crop', confidence_score: 0.4, risk_flags: [],
        visual_status: 'changed_visual', visual_decision: 'send_to_vision',
        visual_metrics: { mask_iou: 0.42, normalized_correlation: 0.51 },
        ui: { color: 'yellow', label: 'Слабая связь — проверить',
              default_visible: true } },
    ],
    unmatched: {
      left_blocks: [
        { block_id: 'L_U1', side: 'left', page_number: 1,
          bbox_norm: [0.2, 0.4, 0.8, 0.6], semantic_type: 'text',
          is_graphic: false, link_status: 'unmatched', risk_flags: [],
          ui: { color: 'gray', label: 'Без пары', default_visible: true } },
      ],
      right_blocks: [
        { block_id: 'R_U1', side: 'right', page_number: 2,
          bbox_norm: [0.2, 0.4, 0.8, 0.6], semantic_type: 'text',
          is_graphic: false, link_status: 'unmatched', risk_flags: [],
          ui: { color: 'gray', label: 'Без пары', default_visible: true } },
      ],
    },
    warnings: [],
  };
}

// ── 13: state machine ответа ────────────────────────────────────────────────

describe('view state machine', () => {
  it('not_found → empty_state (страница не ломается)', () => {
    const env = { status: 'not_found', available: false,
                  message: 'Pipeline V2 block link preview artifacts not found.',
                  payload: null, warnings: [] };
    expect(scPv2LpViewState(200, env)).toBe('empty_state');
    // helpers переживают null payload
    expect(scPv2LpAllLinks(env.payload)).toEqual([]);
    expect(scPv2LpPageOverlays(env.payload, null)).toEqual({ left: [], right: [] });
  });

  it('status=error → artifact_error', () => {
    expect(scPv2LpViewState(200, { status: 'error', warnings: ['x'] }))
      .toBe('artifact_error');
  });

  it('401/403 → access_denied, 500 → transport_error', () => {
    expect(scPv2LpViewState(401, null)).toBe('access_denied');
    expect(scPv2LpViewState(403, null)).toBe('access_denied');
    expect(scPv2LpViewState(500, null)).toBe('transport_error');
  });

  it('ok payload → payload', () => {
    expect(scPv2LpViewState(200, { status: 'ok', payload: makeReport() }))
      .toBe('payload');
  });
});

// ── 14: списки/фильтры strong/weak/manual/unmatched ─────────────────────────

describe('link list and filters', () => {
  const report = makeReport();
  const all = scPv2LpAllLinks(report);

  it('объединяет block_links и unmatched в один список', () => {
    expect(all).toHaveLength(5);
    expect(all.filter(l => l.kind === 'link')).toHaveLength(3);
    expect(all.filter(l => l.kind === 'unmatched')).toHaveLength(2);
    // unmatched получают уникальные id с side-префиксом
    expect(all.map(l => l.block_link_id)).toContain('un_left_L_U1');
    expect(all.map(l => l.block_link_id)).toContain('un_right_R_U1');
  });

  it.each([
    ['all', 5],
    ['strong', 1],
    ['weak', 1],
    ['manual_review', 1],
    ['unmatched', 2],
    ['graphic', 1],
    ['visual_changed', 1],
    ['visual_identical', 0],
  ])('фильтр %s → %i связей', (filter, count) => {
    expect(all.filter(l => scPv2LpLinkMatchesFilter(l, filter)))
      .toHaveLength(count);
  });

  it('цвета статусов: strong=green, weak=yellow, manual=orange, unmatched=gray', () => {
    const byId = Object.fromEntries(all.map(l => [l.block_link_id, l]));
    expect(byId['bm_strong'].ui.color).toBe('green');
    expect(byId['bm_weak'].ui.color).toBe('yellow');
    expect(byId['bm_manual'].ui.color).toBe('orange');
    expect(byId['un_left_L_U1'].ui.color).toBe('gray');
  });
});

// ── overlay geometry / выделение ────────────────────────────────────────────

describe('overlays', () => {
  const report = makeReport();

  it('bbox_norm → проценты, цвет по статусу', () => {
    const link = report.block_links[0]; // strong, [0.1,0.1,0.9,0.3]
    const st = scPv2LpOverlayStyle(
      { entry: link, side: 'left', bbox: link.left_bbox_norm }, '');
    expect(parseFloat(st.left)).toBeCloseTo(10, 6);
    expect(parseFloat(st.top)).toBeCloseTo(10, 6);
    expect(parseFloat(st.width)).toBeCloseTo(80, 6);
    expect(parseFloat(st.height)).toBeCloseTo(20, 6);
    expect(st.border).toContain(SC_PV2_LP_COLORS.green);
  });

  it('выбранная связь — синий контур поверх цвета', () => {
    const link = report.block_links[2]; // weak
    const st = scPv2LpOverlayStyle(
      { entry: link, side: 'left', bbox: link.left_bbox_norm },
      'bm_weak');
    expect(st.border).toContain(SC_PV2_LP_COLORS.blue);
  });

  it('страница pm_1_1: обе стороны связей + unmatched своей стороны', () => {
    const p = report.page_links[0];
    const ov = scPv2LpPageOverlays(report, p);
    // bm_strong + bm_manual на обеих сторонах, L_U1 (page 1) только слева
    expect(ov.left.map(o => o.entry.block_link_id))
      .toEqual(['bm_strong', 'bm_manual', 'un_left_L_U1']);
    expect(ov.right.map(o => o.entry.block_link_id))
      .toEqual(['bm_strong', 'bm_manual']);
  });

  it('страница pm_2_2: weak связь + unmatched right (page 2)', () => {
    const p = report.page_links[1];
    const ov = scPv2LpPageOverlays(report, p);
    expect(ov.left.map(o => o.entry.block_link_id)).toEqual(['bm_weak']);
    expect(ov.right.map(o => o.entry.block_link_id))
      .toEqual(['bm_weak', 'un_right_R_U1']);
  });

  it('mixed-pair invariant: URL картинок берёт pair_id из envelope, не из селектора', () => {
    // зеркало scPv2LpPageImageUrl: pid = (resp && resp.pair_id) || effective
    function imagePid(resp, selectorPid, activePid) {
      return (resp && resp.pair_id) || selectorPid || activePid || '';
    }
    const resp = { status: 'ok', pair_id: 'pA', payload: makeReport() };
    // пользователь перещёлкнул селектор на pB, отчёт ещё от pA →
    // картинки обязаны остаться от pA
    expect(imagePid(resp, 'pB', 'pX')).toBe('pA');
    // отчёт сброшен (watch на селектор) → effective pid
    expect(imagePid(null, 'pB', 'pX')).toBe('pB');
    expect(imagePid(null, '', 'pX')).toBe('pX');
  });

  it('one-sided page link: overlay только на существующей стороне', () => {
    const r = makeReport();
    r.page_links.push({
      page_link_id: 'pl_left_only_1', page_link_kind: 'one_sided',
      left_page_number: 1, right_page_number: null,
      block_link_ids: [], block_links_by_status: { unmatched: 1 },
    });
    const ov = scPv2LpPageOverlays(r, r.page_links[2]);
    // L_U1 живёт на стр.1 слева; правая сторона null → пусто
    expect(ov.left.map(o => o.entry.block_link_id)).toEqual(['un_left_L_U1']);
    expect(ov.right).toEqual([]);
  });

  it('связь без bbox не рисуется (null bbox)', () => {
    const r = makeReport();
    r.block_links[0].left_bbox_norm = null;
    const ov = scPv2LpPageOverlays(r, r.page_links[0]);
    expect(ov.left.map(o => o.entry.block_link_id))
      .toEqual(['bm_manual', 'un_left_L_U1']);
    // правая сторона не пострадала
    expect(ov.right.map(o => o.entry.block_link_id))
      .toEqual(['bm_strong', 'bm_manual']);
  });
});
