/**
 * Тесты pure-логики панели «Pipeline V2 (β)» (Сравнение стадий).
 *
 * Панель read-only: только GET /api/stage-comparison/pipeline-v2/{sid}/
 * ui-payload (+?pair_id=). Здесь зеркалится её чистая логика из app.js
 * (паттерн контрактных тестов проекта, как в cv2_effective_tab.test.js):
 * если зеркало и app.js разойдутся — тест упадёт первым.
 *
 * Покрытие по задаче:
 *   1. not_found → empty-state (+ available_pairs подсказка);
 *   2. payload с 5 секциями → все секции рендерятся в порядке payload;
 *   3. noise-секция скрыта по умолчанию (default_visible=false);
 *   4. карточка с отсутствующими полями не падает;
 *   5. фильтры карточек + 401/403 и error состояния.
 *
 * Запуск:
 *   cd frontend && npm test
 */
import { describe, it, expect } from 'vitest';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ── зеркала pure-helpers из app.js (scPv2*) ─────────────────────────────────

const SC_PV2_SECTION_EMOJI = {
  confirmed_changes: '✅', needs_review: '🟡',
  weak_graphic_review: '🟠', likely_noise_hidden_by_default: '⚪',
  llm_failed_or_skipped: '🔴',
};
function scPv2SectionEmoji(key) {
  return SC_PV2_SECTION_EMOJI[key] || '▫';
}

function scPv2CardMatches(card, f) {
  if (!card) return false;
  return (!f.entity_type || card.entity_type === f.entity_type)
      && (!f.risk_level || card.risk_level === f.risk_level)
      && (!f.critic_verdict || card.critic_verdict === f.critic_verdict)
      && (!f.delta_type || card.delta_type === f.delta_type);
}

function scPv2CardsFor(sec, filters) {
  const cards = (sec && sec.cards) || [];
  const active = Object.values(filters || {}).some(v => v);
  if (!active) return cards;
  return cards.filter(c => scPv2CardMatches(c, filters));
}

function scPv2ApplyDefaultOpen(payload) {
  const open = {};
  for (const sec of (payload && payload.sections) || []) {
    if (sec && sec.key) open[sec.key] = !!sec.default_visible;
  }
  return open;
}

// Зеркало state-machine ответа панели: что показывать по envelope/HTTP.
function scPv2ViewState(httpStatus, envelope) {
  if (httpStatus === 401 || httpStatus === 403) return 'access_denied';
  if (httpStatus !== 200) return 'transport_error';
  if (!envelope) return 'transport_error';
  if (envelope.status === 'not_found') return 'empty_state';
  if (envelope.status === 'error') return 'artifact_error';
  if (envelope.payload) return 'payload';
  return 'transport_error';
}

// Зеркало нормализации карточки в шаблоне (fallback-выражения).
// pageLeft/pageRight существуют только при hasPages (v-if="card.page_numbers"
// в шаблоне) — без page_numbers сегмент страниц вообще не рендерится.
function scPv2CardView(card) {
  const c = card || {};
  const hasPages = !!c.page_numbers;
  return {
    title: c.title || c.delta_id || '—',
    subtitle: c.subtitle || '',
    hasValues: !!(c.old_value || c.new_value),
    oldValue: c.old_value || '∅',
    newValue: c.new_value || '∅',
    verdictChip: c.critic_verdict || 'нет объяснения',
    showConfidence: c.confidence != null,
    hiddenForEngineer: c.should_show_to_engineer === false,
    hasPages,
    pageLeft: hasPages ? (c.page_numbers.left != null ? c.page_numbers.left : '—') : null,
    pageRight: hasPages ? (c.page_numbers.right != null ? c.page_numbers.right : '—') : null,
  };
}

// Зеркало merged warnings (envelope + payload).
function scPv2AllWarnings(envelope) {
  const env = (envelope && envelope.warnings) || [];
  const pl = (envelope && envelope.payload && envelope.payload.warnings) || [];
  return [...env, ...pl];
}

// Зеркало sequence-guard в scPv2Load: авторитетен последний запрос.
function makePv2Loader() {
  let seq = 0;
  const state = { resp: null, loadedFor: '' };
  return {
    state,
    async load(key, responsePromise) {
      const myReq = ++seq;
      const j = await responsePromise;
      if (myReq !== seq) return 'stale_dropped';
      state.resp = j;
      state.loadedFor = key;
      return 'applied';
    },
    invalidate() { seq++; },
  };
}

// ── фикстуры ────────────────────────────────────────────────────────────────

const FIVE_KEYS = ['confirmed_changes', 'needs_review', 'weak_graphic_review',
                   'likely_noise_hidden_by_default', 'llm_failed_or_skipped'];

function makePayload() {
  return {
    version: 1,
    kind: 'stage_comparison_pipeline_v2_ui_payload',
    status: 'ok',
    headline: {
      deltas_total: 5, selected_for_explanation_total: 5,
      confirmed_total: 1, needs_review_total: 1, weak_graphic_total: 1,
      hidden_noise_total: 1, failed_or_skipped_total: 1, coverage_notes_total: 0,
    },
    sections: [
      { key: 'confirmed_changes', title: 'Подтверждённые изменения', badge: 'confirmed',
        default_visible: true, display_hint: 'normal', show_in_diagnostics: false,
        count: 1, description: 'd', delta_ids: ['d1'],
        cards: [{ delta_id: 'd1', title: 'Смена организации',
                  subtitle: 'stamp_field · changed · high risk',
                  entity_type: 'stamp_field', delta_type: 'changed',
                  old_value: 'ARTEL', new_value: 'ИНПАД', confidence: 0.85,
                  risk_level: 'high', critic_verdict: 'accept',
                  groundedness: 'grounded', should_show_to_engineer: true,
                  summary: 's', contractor_impact: 'ci',
                  page_numbers: { left: 5, right: 3 },
                  block_ids: { left: 'L1', right: 'R1' } }] },
      { key: 'needs_review', title: 'На ручную проверку', badge: 'review',
        default_visible: true, display_hint: 'normal', show_in_diagnostics: false,
        count: 1, description: 'd', delta_ids: ['d2'],
        cards: [{ delta_id: 'd2', entity_type: 'stamp_field', delta_type: 'changed',
                  risk_level: 'low', critic_verdict: 'possible_ocr_noise' }] },
      { key: 'weak_graphic_review', title: 'Слабая графика', badge: 'weak_graphic',
        default_visible: true, display_hint: 'warning', show_in_diagnostics: false,
        count: 1, description: 'd', delta_ids: ['d3'], cards: [{ delta_id: 'd3' }] },
      { key: 'likely_noise_hidden_by_default', title: 'Вероятный шум', badge: 'noise',
        default_visible: false, display_hint: 'hidden', show_in_diagnostics: false,
        count: 1, description: 'd', delta_ids: ['d4'], cards: [{ delta_id: 'd4' }] },
      { key: 'llm_failed_or_skipped', title: 'Необъяснённые', badge: 'failed',
        default_visible: false, display_hint: 'diagnostics', show_in_diagnostics: true,
        count: 1, description: 'd', delta_ids: ['d5'], cards: [{ delta_id: 'd5' }] },
    ],
    filters: { entity_types: ['stamp_field'], risk_levels: ['high', 'low'],
               critic_verdicts: ['accept', 'possible_ocr_noise'],
               delta_types: ['changed'] },
    graphic_readiness: { status: 'ok', graphic_blocks_total: 4,
                         usable_for_diff_total: 4 },
    warnings: [],
    artifact_refs: {},
  };
}

const NOT_FOUND = {
  status: 'not_found', available: false, session_id: 's1', pair_id: null,
  source: null, message: 'Pipeline V2 artifacts not found for this session.',
  payload: null, warnings: [], available_pairs: ['p1'],
};

// ── 1: not_found → empty-state ──────────────────────────────────────────────

describe('panel view-state machine', () => {
  it('not_found рендерит empty-state с подсказкой пар', () => {
    expect(scPv2ViewState(200, NOT_FOUND)).toBe('empty_state');
    expect(NOT_FOUND.available_pairs).toEqual(['p1']);
  });

  it('error-артефакты дают диагностический warning, не payload', () => {
    expect(scPv2ViewState(200, { status: 'error', payload: null,
                                 warnings: ['broken'] })).toBe('artifact_error');
  });

  it('401/403 → отказ в доступе; 500/сбой → transport_error', () => {
    expect(scPv2ViewState(401, null)).toBe('access_denied');
    expect(scPv2ViewState(403, null)).toBe('access_denied');
    expect(scPv2ViewState(500, null)).toBe('transport_error');
    expect(scPv2ViewState(200, null)).toBe('transport_error');
  });

  it('ok и partial рендерят payload', () => {
    expect(scPv2ViewState(200, { status: 'ok', payload: makePayload() }))
      .toBe('payload');
    expect(scPv2ViewState(200, { status: 'partial', payload: makePayload() }))
      .toBe('payload');
  });
});

// ── 2-3: секции и default-видимость ─────────────────────────────────────────

describe('sections rendering contract', () => {
  it('payload с 5 секциями рендерит все секции в порядке payload', () => {
    const payload = makePayload();
    expect(payload.sections.map(s => s.key)).toEqual(FIVE_KEYS);
    for (const s of payload.sections) {
      expect(scPv2SectionEmoji(s.key)).not.toBe('▫');
      expect(s.title).toBeTruthy();
    }
  });

  it('noise скрыта по умолчанию, llm_failed свёрнута как диагностика', () => {
    const open = scPv2ApplyDefaultOpen(makePayload());
    expect(open).toEqual({
      confirmed_changes: true,
      needs_review: true,
      weak_graphic_review: true,
      likely_noise_hidden_by_default: false,
      llm_failed_or_skipped: false,
    });
    const failed = makePayload().sections.find(
      s => s.key === 'llm_failed_or_skipped');
    expect(failed.show_in_diagnostics).toBe(true);
  });

  it('неизвестная будущая секция получает нейтральную иконку и не падает', () => {
    const payload = makePayload();
    payload.sections.push({ key: 'future_x', title: 'x', default_visible: true,
                            count: 0, cards: [] });
    const open = scPv2ApplyDefaultOpen(payload);
    expect(open.future_x).toBe(true);
    expect(scPv2SectionEmoji('future_x')).toBe('▫');
  });
});

// ── 4: карточка с отсутствующими полями ─────────────────────────────────────

describe('card fallbacks', () => {
  it('полная карточка отображает все поля', () => {
    const v = scPv2CardView(makePayload().sections[0].cards[0]);
    expect(v.title).toBe('Смена организации');
    expect(v.oldValue).toBe('ARTEL');
    expect(v.newValue).toBe('ИНПАД');
    expect(v.verdictChip).toBe('accept');
    expect(v.showConfidence).toBe(true);
    expect(v.hiddenForEngineer).toBe(false);
    expect(v.pageLeft).toBe(5);
  });

  it('карточка без explanation/полей не падает и показывает фолбэки', () => {
    const v = scPv2CardView({ delta_id: 'd9' });
    expect(v.title).toBe('d9');
    expect(v.hasValues).toBe(false);
    expect(v.verdictChip).toBe('нет объяснения');
    expect(v.showConfidence).toBe(false);
    // без page_numbers сегмент страниц не рендерится вовсе (v-if)
    expect(v.hasPages).toBe(false);
    // совсем пустой вход тоже безопасен
    expect(scPv2CardView(null).title).toBe('—');
    // confidence=0 — легитимное значение, не прячется
    expect(scPv2CardView({ confidence: 0 }).showConfidence).toBe(true);
    // page 0 — легитимное значение; null-сторона → прочерк
    const pv = scPv2CardView({ page_numbers: { left: 0 } });
    expect(pv.hasPages).toBe(true);
    expect(pv.pageLeft).toBe(0);
    expect(pv.pageRight).toBe('—');
  });

  it('should_show_to_engineer=false подсвечивается, null — нет', () => {
    expect(scPv2CardView({ should_show_to_engineer: false }).hiddenForEngineer)
      .toBe(true);
    expect(scPv2CardView({}).hiddenForEngineer).toBe(false);
  });
});

// ── 5: фильтры ──────────────────────────────────────────────────────────────

describe('card filters', () => {
  const sec = makePayload().sections[0];
  const empty = { entity_type: '', risk_level: '', critic_verdict: '', delta_type: '' };

  it('без активных фильтров возвращаются все карточки', () => {
    expect(scPv2CardsFor(sec, empty)).toHaveLength(1);
  });

  it('фильтр по risk_level отбирает совпадения', () => {
    expect(scPv2CardsFor(sec, { ...empty, risk_level: 'high' })).toHaveLength(1);
    expect(scPv2CardsFor(sec, { ...empty, risk_level: 'low' })).toHaveLength(0);
  });

  it('карточка без поля не проходит конкретный фильтр, но не падает', () => {
    const bare = { key: 'x', cards: [{ delta_id: 'd' }] };
    expect(scPv2CardsFor(bare, { ...empty, critic_verdict: 'accept' }))
      .toHaveLength(0);
    expect(scPv2CardsFor(bare, empty)).toHaveLength(1);
  });

  it('секция без cards не падает', () => {
    expect(scPv2CardsFor({ key: 'x' }, empty)).toEqual([]);
    expect(scPv2CardsFor(null, empty)).toEqual([]);
  });
});

// ── фиксы адверсариального ревью ────────────────────────────────────────────

describe('merged warnings + failed payload', () => {
  it('warnings объединяют envelope и payload (adapter/summary warnings видны)', () => {
    const env = {
      status: 'partial', warnings: ['entity_diff_report.json: broken'],
      payload: { warnings: ['section_confirmed_changes: 1 delta(s) without card data'] },
    };
    expect(scPv2AllWarnings(env)).toEqual([
      'entity_diff_report.json: broken',
      'section_confirmed_changes: 1 delta(s) without card data',
    ]);
    expect(scPv2AllWarnings({ status: 'ok' })).toEqual([]);
    expect(scPv2AllWarnings(null)).toEqual([]);
  });

  it('payload.status=failed — отдельное состояние (красный баннер)', () => {
    // контракт ui_payload: status ∈ ok|completed_with_warnings|failed;
    // шаблон обязан различать failed, а не показывать как здоровый ok
    const payload = makePayload();
    payload.status = 'failed';
    const isFailedBanner = payload.status === 'failed';
    const isPartialBanner = !isFailedBanner
      && payload.status === 'completed_with_warnings';
    expect(isFailedBanner).toBe(true);
    expect(isPartialBanner).toBe(false);
  });
});

describe('request sequence guard (race)', () => {
  it('поздний ответ старой пары не перетирает актуальный', async () => {
    const loader = makePv2Loader();
    let resolveA;
    const slowA = new Promise(res => { resolveA = res; });
    const pA = loader.load('sid|A', slowA);              // запрос A (медленный)
    const rB = await loader.load('sid|B', Promise.resolve({ pair: 'B' }));
    expect(rB).toBe('applied');
    resolveA({ pair: 'A' });                             // A пришёл ПОСЛЕ B
    expect(await pA).toBe('stale_dropped');
    expect(loader.state.resp).toEqual({ pair: 'B' });
    expect(loader.state.loadedFor).toBe('sid|B');
  });

  it('смена сессии инвалидирует in-flight ответ старой сессии', async () => {
    const loader = makePv2Loader();
    let resolveOld;
    const slowOld = new Promise(res => { resolveOld = res; });
    const pOld = loader.load('oldsid|', slowOld);
    loader.invalidate();                                  // watch смены сессии
    resolveOld({ session: 'old' });
    expect(await pOld).toBe('stale_dropped');
    expect(loader.state.resp).toBeNull();
  });
});

// ── Графика / Vision grounding (зеркало scPv2GraphicVision /
//    scPv2GraphicGrounding / scPv2GroundingRejectedTotal из app.js) ──────────

function scPv2GraphicVision(payload) {
  return (payload && payload.graphic_vision) || null;
}
function scPv2GraphicGrounding(payload) {
  return (payload && payload.graphic_vision_grounding) || null;
}
function scPv2GroundingRejectedTotal(payload) {
  const g = scPv2GraphicGrounding(payload);
  if (!g) return 0;
  const n = (x) => (typeof x === 'number' && x > 0 ? x : 0);
  return n(g.artificial_series_rejected) + n(g.designator_range_rejected)
    + n(g.noop_changes_rejected);
}

function makeGroundingPayload() {
  return {
    status: 'completed_with_warnings',
    sections: [],
    graphic_vision: {
      enabled: true, status: 'ok', selected_total: 3,
      vision_calls_succeeded: 3, vision_calls_failed: 0, skipped_no_runner: 0,
    },
    graphic_vision_grounding: {
      enabled: true, status: 'ok', entities_total: 262,
      entities_grounded: 87, entities_weakly_grounded: 39,
      entities_ungrounded: 126, changes_grounded: 8,
      changes_weakly_grounded: 21, changes_rejected: 1,
      artificial_series_rejected: 0, designator_range_rejected: 10,
      noop_changes_rejected: 1,
    },
  };
}

describe('Pipeline V2 — Графика / Vision grounding', () => {
  it('1. grounding присутствует → блок рендерится (computed не null)', () => {
    const p = makeGroundingPayload();
    expect(scPv2GraphicGrounding(p)).not.toBeNull();
    expect(scPv2GraphicVision(p)).not.toBeNull();
  });

  it('2. counts отображаются (runtime ИОС1.1 значения)', () => {
    const g = scPv2GraphicGrounding(makeGroundingPayload());
    expect(g.entities_total).toBe(262);
    expect(g.entities_grounded).toBe(87);
    expect(g.entities_weakly_grounded).toBe(39);
    expect(g.entities_ungrounded).toBe(126);
    expect(g.changes_grounded).toBe(8);
  });

  it('3. missing graphic_vision не ломает панель → empty-state, grounding жив', () => {
    const p = makeGroundingPayload();
    delete p.graphic_vision;
    expect(scPv2GraphicVision(p)).toBeNull();        // → empty msg в шаблоне
    expect(scPv2GraphicGrounding(p)).not.toBeNull(); // grounding всё ещё есть
  });

  it('4. missing graphic_vision_grounding не ломает панель', () => {
    const p = makeGroundingPayload();
    delete p.graphic_vision_grounding;
    expect(scPv2GraphicGrounding(p)).toBeNull();
    expect(scPv2GroundingRejectedTotal(p)).toBe(0);  // не падает на отсутствии
    expect(scPv2GraphicVision(p)).not.toBeNull();
  });

  it('5. rejected counts суммируются отдельно (artificial+designator+noop)', () => {
    const p = makeGroundingPayload();
    expect(scPv2GroundingRejectedTotal(p)).toBe(11); // 0 + 10 + 1
    p.graphic_vision_grounding.artificial_series_rejected = 31;
    expect(scPv2GroundingRejectedTotal(p)).toBe(42); // 31 + 10 + 1
  });

  it('6. нулевые counts показываются как 0, не падают', () => {
    const p = {
      status: 'ok', sections: [],
      graphic_vision: { enabled: true, status: 'ok', selected_total: 0,
        vision_calls_succeeded: 0, vision_calls_failed: 0 },
      graphic_vision_grounding: { enabled: true, status: 'ok',
        entities_total: 0, entities_grounded: 0, entities_weakly_grounded: 0,
        entities_ungrounded: 0, changes_grounded: 0, changes_weakly_grounded: 0,
        changes_rejected: 0, artificial_series_rejected: 0,
        designator_range_rejected: 0, noop_changes_rejected: 0 },
    };
    expect(scPv2GraphicGrounding(p).entities_grounded).toBe(0);
    expect(scPv2GroundingRejectedTotal(p)).toBe(0);
  });

  it('7. старые секции/headline не ломаются добавлением grounding-блока', () => {
    const p = makePayload();                 // существующий payload без vision
    expect(scPv2GraphicVision(p)).toBeNull();        // empty-state
    expect(scPv2GraphicGrounding(p)).toBeNull();     // empty-state
    expect((p.sections || []).length).toBeGreaterThan(0); // секции целы
    expect(p.headline).toBeTruthy();                 // headline цел
  });
});

// ── Grounding detail drawer (зеркало scPv2GdMatch / scPv2GdCards /
//    scPv2GdStatusColor / фильтрации из app.js) ────────────────────────────

function scPv2GdCards(resp) {
  const f = resp && resp.flat;
  if (!f) return [];
  return [].concat(f.entities || [], f.changes || [], f.rejected || []);
}
function scPv2GdMatch(card, tab) {
  if (tab === 'all') return true;
  if (tab === 'changes') return card.card_type === 'change';
  if (tab === 'grounded') return card.status === 'grounded';
  if (tab === 'weak') return card.status === 'weakly_grounded';
  if (tab === 'ungrounded')
    return card.status === 'ungrounded' || card.status === 'no_anchor_available';
  if (tab === 'rejected')
    return typeof card.status === 'string' && card.status.indexOf('rejected_') === 0;
  return true;
}
function scPv2GdFiltered(resp, tab) {
  return scPv2GdCards(resp).filter(c => scPv2GdMatch(c, tab));
}
function scPv2GdStatusColor(status) {
  if (status === 'grounded') return 'green';
  if (status === 'weakly_grounded') return 'amber';
  if (typeof status === 'string' && status.indexOf('rejected_') === 0) return 'red';
  return 'gray';
}

function makeDetailResp() {
  return {
    status: 'ok', available: true,
    summary: { entities_grounded: 87, entities_weakly_grounded: 39,
      entities_ungrounded: 126, designator_range_rejected: 10,
      artificial_series_rejected: 0, noop_changes_rejected: 1 },
    flat: {
      entities: [
        { id: 'a', card_type: 'entity', value: 'QF5 400А', status: 'grounded',
          reason: 'grounded', use_as_fact: true, fact_level: 'confirmed',
          anchor: '400a', anchor_source: 'full_text', page_number: 52 },
        { id: 'b', card_type: 'entity', value: 'QF9 999А', status: 'ungrounded',
          reason: 'not_found_in_anchors', use_as_fact: false, fact_level: 'not_fact' },
        { id: 'c', card_type: 'entity', value: 'QF2 125А', status: 'weakly_grounded',
          reason: 'partial_match', use_as_fact: true, fact_level: 'weak' },
      ],
      changes: [
        { id: 'd', card_type: 'change', value: 'QF5: 400А → 200А', status: 'grounded',
          reason: 'grounded', use_as_fact: true, left_page_number: 52, right_page_number: 21 },
      ],
      rejected: [
        { id: 'e', card_type: 'entity', value: 'QF1...QF100',
          status: 'rejected_designator_range', reason: 'artificial_designator_range',
          use_as_fact: false },
        { id: 'f', card_type: 'change', value: '… (без изменений)',
          status: 'rejected_noop', reason: 'noop_change', use_as_fact: false },
      ],
    },
    pagination: { limit: 500, offset: 0, returned: 6, total: 6 },
  };
}

describe('Pipeline V2 — grounding detail drawer', () => {
  it('1. кнопка деталей доступна при наличии grounding (summary present)', () => {
    const p = makeGroundingPayload();
    // условие рендера кнопки = scPv2GraphicGrounding не null
    expect(scPv2GraphicGrounding(p)).not.toBeNull();
  });

  it('2. missing grounding → кнопки нет, панель цела', () => {
    const p = makeGroundingPayload();
    delete p.graphic_vision_grounding;
    expect(scPv2GraphicGrounding(p)).toBeNull();
  });

  it('3. drawer cards собираются из flat (entities+changes+rejected)', () => {
    const cards = scPv2GdCards(makeDetailResp());
    expect(cards.length).toBe(6);
  });

  it('4. grounded карточки рендерятся (фильтр grounded)', () => {
    const g = scPv2GdFiltered(makeDetailResp(), 'grounded');
    expect(g.length).toBe(2);                 // QF5 entity + QF5 change
    expect(g.every(c => c.status === 'grounded')).toBe(true);
    expect(scPv2GdStatusColor('grounded')).toBe('green');
  });

  it('5. rejected карточки рендерятся (фильтр rejected) + красный цвет', () => {
    const r = scPv2GdFiltered(makeDetailResp(), 'rejected');
    expect(r.length).toBe(2);                 // designator_range + noop
    expect(r.every(c => c.status.indexOf('rejected_') === 0)).toBe(true);
    expect(scPv2GdStatusColor('rejected_designator_range')).toBe('red');
    expect(scPv2GdStatusColor('rejected_noop')).toBe('red');
  });

  it('6. empty state — нет элементов в выбранной категории', () => {
    const resp = makeDetailResp();
    resp.flat.rejected = [];
    expect(scPv2GdFiltered(resp, 'rejected').length).toBe(0);
  });

  it('7. counts совпадают с summary', () => {
    const resp = makeDetailResp();
    expect(resp.summary.entities_grounded).toBe(87);
    const rejTotal = (resp.summary.designator_range_rejected || 0)
      + (resp.summary.artificial_series_rejected || 0)
      + (resp.summary.noop_changes_rejected || 0);
    expect(rejTotal).toBe(11);
  });

  it('8. фильтр changes → только change-карточки', () => {
    const ch = scPv2GdFiltered(makeDetailResp(), 'changes');
    expect(ch.length).toBe(2);                // grounded change + rejected noop change
    expect(ch.every(c => c.card_type === 'change')).toBe(true);
  });

  it('9. fact-level: grounded=факт, weak=факт(проверить), rejected=не факт', () => {
    const cards = scPv2GdCards(makeDetailResp());
    const byId = Object.fromEntries(cards.map(c => [c.id, c]));
    expect(byId.a.use_as_fact).toBe(true);    // grounded
    expect(byId.c.use_as_fact).toBe(true);    // weak (с пометкой)
    expect(byId.b.use_as_fact).toBe(false);   // ungrounded
    expect(byId.e.use_as_fact).toBe(false);   // rejected
  });

  it('10. ungrounded фильтр включает no_anchor_available', () => {
    const resp = makeDetailResp();
    resp.flat.entities.push({ id: 'g', card_type: 'entity', value: 'X',
      status: 'no_anchor_available', use_as_fact: false });
    const u = scPv2GdFiltered(resp, 'ungrounded');
    expect(u.map(c => c.status).sort()).toEqual(['no_anchor_available', 'ungrounded']);
  });
});

// ── Grounding → block link preview jump (зеркало scPv2GdBlockIdsFromCard /
//    scPv2GdCardHasTarget / scPv2GdMatchLink + state-machine из app.js) ──────

function scPv2GdBlockIdsFromCard(card) {
  let left = (card && card.left_block_id) || null;
  let right = (card && card.right_block_id) || null;
  if ((!left || !right) && card && card.item_id) {
    const parts = String(card.item_id).replace(/^gv_/, '').split('__');
    if (parts.length === 2) { left = left || parts[0]; right = right || parts[1]; }
  }
  if (!left && card && card.side === 'old' && card.block_id) left = card.block_id;
  if (!right && card && card.side === 'new' && card.block_id) right = card.block_id;
  if (!left && card && card.block_id) left = card.block_id;
  return { left, right };
}
function scPv2GdCardHasTarget(card) {
  if (!card) return false;
  return !!(card.left_block_id || card.right_block_id || card.item_id
    || card.block_id || card.left_page_number != null
    || card.right_page_number != null || card.page_number != null);
}
function scPv2GdMatchLink(links, target) {
  let m = links.find(l => l.kind === 'link'
    && l.left_block_id === target.left_block_id
    && l.right_block_id === target.right_block_id);
  if (m) return m;
  m = links.find(l => l.kind === 'link'
    && (l.left_block_id === target.left_block_id || l.right_block_id === target.right_block_id));
  if (m) return m;
  m = links.find(l => l.kind === 'unmatched'
    && (l.block_id === target.left_block_id || l.block_id === target.right_block_id));
  if (m) return m;
  if (target.left_page_number != null && target.right_page_number != null) {
    m = links.find(l => l.kind === 'link'
      && l.left_page_number === target.left_page_number
      && l.right_page_number === target.right_page_number);
    if (m) return m;
  }
  return null;
}

const GD_LINKS = [
  { kind: 'link', block_link_id: 'bm_7EMD__763U', left_block_id: '7EMD-DT4R-6TN',
    right_block_id: '763U-YFTA-DVQ', left_page_number: 52, right_page_number: 21 },
  { kind: 'link', block_link_id: 'bm_A__B', left_block_id: 'A', right_block_id: 'B',
    left_page_number: 3, right_page_number: 3 },
  { kind: 'unmatched', block_link_id: 'un_left_X', side: 'left', block_id: 'X', page_number: 9 },
];

// state-machine моста (зеркало scPv2OpenBlockLinkFromGrounding)
function makeGdJump() {
  const state = {
    drawerOpen: true, tab: 'pv2', activePairId: null, lpVisible: false,
    lpPairId: '', lpResp: null, selectedLink: '', banner: '', warning: '',
    fetched: 0,
  };
  async function open(card, opts = {}) {
    const ids = scPv2GdBlockIdsFromCard(card);
    const pid = opts.groundingPair || 'pf06effb7';
    const target = { left_block_id: ids.left, right_block_id: ids.right,
      left_page_number: card.left_page_number != null ? card.left_page_number
        : (card.side === 'old' ? card.page_number : null),
      right_page_number: card.right_page_number != null ? card.right_page_number
        : (card.side === 'new' ? card.page_number : null), label: card.value };
    state.drawerOpen = false;                  // 1
    if (pid && state.activePairId !== pid) { state.activePairId = pid; state.tab = 'links'; }
    else state.tab = 'links';
    state.lpPairId = pid;                       // 4
    state.lpVisible = true;                     // 3
    const loadedPid = state.lpResp && state.lpResp.pair_id;
    if (!state.lpResp || loadedPid !== pid) {   // 5
      state.fetched++;
      state.lpResp = { pair_id: pid, payload: { block_links: opts.links || GD_LINKS } };
    }
    const links = (state.lpResp.payload.block_links || []).map(l => ({ ...l }));
    const m = scPv2GdMatchLink(links, target);
    state.banner = target.label;
    if (m) { state.selectedLink = m.block_link_id; state.warning = ''; }
    else { state.warning = 'Связь блоков для этой grounding-карточки не найдена. Откройте пару вручную.'; }
    return state;
  }
  return { state, open };
}

describe('Pipeline V2 — grounding → block link jump', () => {
  it('1. card с left/right block ids → кнопка показывается', () => {
    expect(scPv2GdCardHasTarget({ left_block_id: 'A', right_block_id: 'B' })).toBe(true);
  });

  it('2. card без идентификаторов → кнопки нет', () => {
    expect(scPv2GdCardHasTarget({ value: 'x' })).toBe(false);
    expect(scPv2GdCardHasTarget(null)).toBe(false);
  });

  it('3. block ids выводятся из item_id (gv_<L>__<R>)', () => {
    const ids = scPv2GdBlockIdsFromCard({ item_id: 'gv_7EMD-DT4R-6TN__763U-YFTA-DVQ' });
    expect(ids.left).toBe('7EMD-DT4R-6TN');
    expect(ids.right).toBe('763U-YFTA-DVQ');
  });

  it('4. exact match по обоим block_id', () => {
    const m = scPv2GdMatchLink(GD_LINKS,
      { left_block_id: '7EMD-DT4R-6TN', right_block_id: '763U-YFTA-DVQ' });
    expect(m && m.block_link_id).toBe('bm_7EMD__763U');
  });

  it('5. fallback по одному block_id', () => {
    const m = scPv2GdMatchLink(GD_LINKS,
      { left_block_id: '7EMD-DT4R-6TN', right_block_id: 'NOPE' });
    expect(m && m.block_link_id).toBe('bm_7EMD__763U');
  });

  it('6. fallback по unmatched single block', () => {
    const m = scPv2GdMatchLink(GD_LINKS, { left_block_id: 'X', right_block_id: 'Y' });
    expect(m && m.block_link_id).toBe('un_left_X');
  });

  it('7. fallback по номерам страниц', () => {
    const m = scPv2GdMatchLink(GD_LINKS,
      { left_block_id: 'zzz', right_block_id: 'qqq', left_page_number: 52, right_page_number: 21 });
    expect(m && m.block_link_id).toBe('bm_7EMD__763U');
  });

  it('8. ничего не найдено → null (UI покажет warning)', () => {
    const m = scPv2GdMatchLink(GD_LINKS,
      { left_block_id: 'no', right_block_id: 'no' });
    expect(m).toBeNull();
  });

  it('9. jump закрывает drawer и выбирает связь', async () => {
    const j = makeGdJump();
    await j.open({ value: 'QF5 400А → 200А', item_id: 'gv_7EMD-DT4R-6TN__763U-YFTA-DVQ',
      left_block_id: '7EMD-DT4R-6TN', right_block_id: '763U-YFTA-DVQ' });
    expect(j.state.drawerOpen).toBe(false);
    expect(j.state.lpVisible).toBe(true);
    expect(j.state.selectedLink).toBe('bm_7EMD__763U');
    expect(j.state.banner).toBe('QF5 400А → 200А');
    expect(j.state.warning).toBe('');
  });

  it('10. если payload не загружен — сначала fetch, потом selection', async () => {
    const j = makeGdJump();
    expect(j.state.lpResp).toBeNull();
    await j.open({ value: 'X', item_id: 'gv_A__B' });
    expect(j.state.fetched).toBe(1);
    expect(j.state.selectedLink).toBe('bm_A__B');
  });

  it('11. payload уже загружен для пары → без повторного fetch', async () => {
    const j = makeGdJump();
    j.state.lpResp = { pair_id: 'pf06effb7', payload: { block_links: GD_LINKS } };
    await j.open({ value: 'X', item_id: 'gv_A__B' });
    expect(j.state.fetched).toBe(0);            // не перезагружали
    expect(j.state.selectedLink).toBe('bm_A__B');
  });

  it('12. не найдено → warning, drawer закрыт, UI не падает', async () => {
    const j = makeGdJump();
    await j.open({ value: 'ZZ', item_id: 'gv_no__pe' });
    expect(j.state.warning).toContain('не найдена');
    expect(j.state.drawerOpen).toBe(false);
    expect(j.state.selectedLink).toBe('');
  });

  it('13. pair_id передаётся в block link preview', async () => {
    const j = makeGdJump();
    await j.open({ value: 'X', item_id: 'gv_A__B' }, { groundingPair: 'pXYZ' });
    expect(j.state.lpPairId).toBe('pXYZ');
    expect(j.state.activePairId).toBe('pXYZ');
  });
});

// ── Grounded evidence badges (зеркало scPv2GeBadgeStyle / scPv2GeAnchorText /
//    scPv2GeInterestingCards из app.js) ────────────────────────────────────

function scPv2GeBadgeStyle(level) {
  const L = (level || '').toLowerCase();
  if (L === 'grounded') return {emoji: '✅', text: 'Grounded vision',
    bg: '#dcfce7', fg: '#166534', border: '#86efac'};
  if (L === 'weak') return {emoji: '🟡', text: 'Weak vision',
    bg: '#fef9c3', fg: '#854d0e', border: '#fde047'};
  if (L === 'conflict' || L === 'rejected_only')
    return {emoji: '⚠', text: 'Rejected/conflict', bg: '#ffedd5',
      fg: '#9a3412', border: '#fdba74'};
  return null;
}
function scPv2GeAnchorText(a) {
  if (!a) return '';
  const parts = [];
  if (a.designator) parts.push(String(a.designator).toUpperCase());
  const ov = a.old_anchor || '';
  const nv = a.new_anchor || '';
  if (ov || nv) parts.push((ov || '—') + ' → ' + (nv || '—'));
  return parts.join(': ');
}
function scPv2GeInterestingCards(ge) {
  const cards = (ge && Array.isArray(ge.cards)) ? ge.cards : [];
  return cards.filter(c => c && c.evidence_level && c.evidence_level !== 'none');
}

describe('Pipeline V2 — grounded evidence badges', () => {
  it('3. grounded badge renders (green ✅)', () => {
    const b = scPv2GeBadgeStyle('grounded');
    expect(b).not.toBeNull();
    expect(b.emoji).toBe('✅');
    expect(b.text).toBe('Grounded vision');
    expect(b.bg).toBe('#dcfce7');
  });

  it('4. weak badge renders (yellow 🟡)', () => {
    const b = scPv2GeBadgeStyle('weak');
    expect(b.emoji).toBe('🟡');
    expect(b.text).toBe('Weak vision');
  });

  it('5. conflict/rejected warning renders (orange ⚠)', () => {
    const c = scPv2GeBadgeStyle('conflict');
    const r = scPv2GeBadgeStyle('rejected_only');
    expect(c.emoji).toBe('⚠');
    expect(c.text).toBe('Rejected/conflict');
    expect(r.text).toBe('Rejected/conflict');   // оба → один warning-стиль
  });

  it('6. none / missing level → no badge (neutral)', () => {
    expect(scPv2GeBadgeStyle('none')).toBeNull();
    expect(scPv2GeBadgeStyle('')).toBeNull();
    expect(scPv2GeBadgeStyle(undefined)).toBeNull();
  });

  it('7. interesting cards filter drops "none" (rejected/grounded kept)', () => {
    const ge = {cards: [
      {delta_id: 'a', evidence_level: 'grounded'},
      {delta_id: 'b', evidence_level: 'none'},
      {delta_id: 'c', evidence_level: 'rejected_only'},
      {delta_id: 'd', evidence_level: 'weak'},
    ]};
    const out = scPv2GeInterestingCards(ge).map(c => c.delta_id);
    expect(out).toEqual(['a', 'c', 'd']);
  });

  it('8. anchor text renders designator + old→new', () => {
    const t = scPv2GeAnchorText({designator: 'qf5', old_anchor: 'QF5 (400А)',
      new_anchor: 'QF5 (200А)'});
    expect(t).toContain('QF5');
    expect(t).toContain('QF5 (400А) → QF5 (200А)');
  });

  it('9. missing grounded_evidence → empty interesting list, no crash', () => {
    expect(scPv2GeInterestingCards(null)).toEqual([]);
    expect(scPv2GeInterestingCards({})).toEqual([]);
    expect(scPv2GeInterestingCards({cards: null})).toEqual([]);
  });

  it('10. anchor text tolerates empty anchor', () => {
    expect(scPv2GeAnchorText(null)).toBe('');
    expect(scPv2GeAnchorText({})).toBe('');
  });
});

// ── Evidence + critic verdict chips (зеркало scPv2CriticVerdictStyle /
//    scPv2ShowText / scPv2GeVerdictBreakdown / rejected-not-fact из app.js) ──

function scPv2CriticVerdictStyle(v) {
  const V = (v || '').toLowerCase();
  if (V === 'accept') return {text:'accept', bg:'#dcfce7', fg:'#166534', border:'#86efac'};
  if (V === 'needs_human_review') return {text:'на проверку', bg:'#fef9c3', fg:'#854d0e', border:'#fde047'};
  if (V === 'possible_weak_graphic') return {text:'слабая графика', bg:'#ffedd5', fg:'#9a3412', border:'#fdba74'};
  if (V === 'possible_ocr_noise') return {text:'возможно OCR-шум', bg:'#f1f5f9', fg:'#475569', border:'#cbd5e1'};
  if (V === 'reject') return {text:'отклонено', bg:'#fee2e2', fg:'#991b1b', border:'#fecaca'};
  if (V === 'failed' || V === 'skipped') return {text:'сбой/пропуск', bg:'#fee2e2', fg:'#991b1b', border:'#fecaca'};
  return null;
}
function scPv2ShowText(v) {
  if (v === true) return 'да';
  if (v === false) return 'нет';
  return '—';
}
function scPv2GeVerdictBreakdown(sections) {
  const out = {accept: 0, needs_review: 0, weak_other: 0, total: 0};
  for (const sec of (sections || [])) {
    for (const c of ((sec && sec.cards) || [])) {
      const ge = c && c.grounded_evidence;
      if (!ge || !ge.evidence_level || ge.evidence_level === 'none') continue;
      out.total++;
      const lvl = ge.evidence_level;
      const v = (c.critic_verdict || '').toLowerCase();
      if (lvl === 'conflict' || lvl === 'rejected_only' || lvl === 'weak') out.weak_other++;
      else if (v === 'accept') out.accept++;
      else out.needs_review++;
    }
  }
  return out;
}
const isRejectedNotFact = (lvl) => lvl === 'rejected_only' || lvl === 'conflict';

describe('Pipeline V2 — evidence + critic verdict chips', () => {
  it('1. grounded badge style green (card-level)', () => {
    const card = {grounded_evidence: {evidence_level: 'grounded'}};
    const b = scPv2GeBadgeStyle(card.grounded_evidence.evidence_level);
    expect(b.bg).toBe('#dcfce7');
  });

  it('2. weak badge style yellow', () => {
    expect(scPv2GeBadgeStyle('weak').bg).toBe('#fef9c3');
  });

  it('3. conflict/rejected → warning badge + "not a fact" line', () => {
    expect(scPv2GeBadgeStyle('rejected_only').emoji).toBe('⚠');
    expect(isRejectedNotFact('rejected_only')).toBe(true);
    expect(isRejectedNotFact('conflict')).toBe(true);
    expect(isRejectedNotFact('grounded')).toBe(false);
    expect(isRejectedNotFact('weak')).toBe(false);
  });

  it('4. critic_verdict=accept → green accept chip', () => {
    const s = scPv2CriticVerdictStyle('accept');
    expect(s).not.toBeNull();
    expect(s.text).toBe('accept');
    expect(s.bg).toBe('#dcfce7');
  });

  it('5. critic_verdict=needs_human_review → yellow review chip', () => {
    const s = scPv2CriticVerdictStyle('needs_human_review');
    expect(s.text).toBe('на проверку');
    expect(s.bg).toBe('#fef9c3');
  });

  it('5b. other critic verdicts mapped', () => {
    expect(scPv2CriticVerdictStyle('possible_weak_graphic').bg).toBe('#ffedd5');
    expect(scPv2CriticVerdictStyle('possible_ocr_noise').bg).toBe('#f1f5f9');
    expect(scPv2CriticVerdictStyle('reject').bg).toBe('#fee2e2');
  });

  it('6. grounded + needs_review render as TWO separate chips (evidence ≠ critic)', () => {
    const card = {grounded_evidence: {evidence_level: 'grounded'}, critic_verdict: 'needs_human_review'};
    const ev = scPv2GeBadgeStyle(card.grounded_evidence.evidence_level);
    const cr = scPv2CriticVerdictStyle(card.critic_verdict);
    expect(ev.text).toBe('Grounded vision');   // evidence stays grounded
    expect(cr.text).toBe('на проверку');        // critic separate verdict
    expect(ev.bg).not.toBe(cr.bg);              // not conflated
  });

  it('7. missing grounded_evidence → no badge, no crash', () => {
    const card = {critic_verdict: 'accept'};
    expect(card.grounded_evidence == null).toBe(true);
    expect(scPv2CriticVerdictStyle(card.critic_verdict)).not.toBeNull();
  });

  it('8. missing critic verdict → null (UI shows "нет объяснения")', () => {
    expect(scPv2CriticVerdictStyle(undefined)).toBeNull();
    expect(scPv2CriticVerdictStyle('')).toBeNull();
    expect(scPv2CriticVerdictStyle(null)).toBeNull();
  });

  it('9. should_show text mapping', () => {
    expect(scPv2ShowText(true)).toBe('да');
    expect(scPv2ShowText(false)).toBe('нет');
    expect(scPv2ShowText(undefined)).toBe('—');
    expect(scPv2ShowText(null)).toBe('—');
  });

  it('10. verdict breakdown joins evidence × critic over section cards', () => {
    const sections = [{cards: [
      {grounded_evidence: {evidence_level: 'grounded'}, critic_verdict: 'accept'},
      {grounded_evidence: {evidence_level: 'grounded'}, critic_verdict: 'needs_human_review'},
      {grounded_evidence: {evidence_level: 'weak'}, critic_verdict: 'needs_human_review'},
      {grounded_evidence: {evidence_level: 'rejected_only'}, critic_verdict: 'needs_human_review'},
      {grounded_evidence: {evidence_level: 'none'}, critic_verdict: 'accept'},  // skipped
      {critic_verdict: 'accept'},  // no evidence → skipped
    ]}];
    const b = scPv2GeVerdictBreakdown(sections);
    expect(b.total).toBe(4);          // none + no-evidence excluded
    expect(b.accept).toBe(1);          // grounded+accept
    expect(b.needs_review).toBe(1);    // grounded+review
    expect(b.weak_other).toBe(2);      // weak + rejected
  });

  it('11. breakdown empty when no grounded evidence cards (no crash)', () => {
    expect(scPv2GeVerdictBreakdown([]).total).toBe(0);
    expect(scPv2GeVerdictBreakdown(null).total).toBe(0);
    expect(scPv2GeVerdictBreakdown([{cards: null}]).total).toBe(0);
  });
});

// ── Pipeline V2 — Entity Alignment Preview («Сущности и маппинг», read-only) ──
//
// Зеркало pure-логики панели scPv2Ea* из app.js + контрактные проверки
// read-only / отсутствия apply-кнопок прямо по файлам index.html / app.js.

const SC_PV2_EA_CLASS_META = {
  same_entity_likely:        { label: 'Same entity', icon: '🟢', color: '#16a34a', bg: '#dcfce7', fg: '#166534' },
  possible_rename:           { label: 'Возможно переименование', icon: '🔵', color: '#2563eb', bg: '#dbeafe', fg: '#1e40af' },
  scope_reorganized:         { label: 'Реорганизация', icon: '🟠', color: '#ea580c', bg: '#ffedd5', fg: '#9a3412' },
  mismatch_likely:           { label: 'Mismatch', icon: '🔴', color: '#dc2626', bg: '#fee2e2', fg: '#991b1b' },
  link_validation_candidate: { label: 'Проверка связи', icon: '🟣', color: '#7c3aed', bg: '#ede9fe', fg: '#5b21b6' },
};
function scPv2EaClassMeta(c) {
  return SC_PV2_EA_CLASS_META[c]
      || { label: c || '—', icon: '⚪', color: '#6b7280', bg: '#f3f4f6', fg: '#6b7280' };
}
function scPv2EaConfPct(p) {
  const c = p && p.confidence;
  return (typeof c === 'number') ? Math.round(c * 100) + '%' : '';
}
function scPv2EaFilteredPairs(pairs, f) {
  pairs = pairs || [];
  if (f === 'unpaired') return [];
  if (!f || f === 'all') return pairs;
  return pairs.filter((p) => p.classification === f);
}
function scPv2EaShowUnpaired(f) { return f === 'all' || f === 'unpaired'; }
function scPv2EaShowPairs(f) { return f !== 'unpaired'; }
// state-machine ответа панели (detail-формат: status/available)
function scPv2EaViewState(httpStatus, resp) {
  if (httpStatus === 401 || httpStatus === 403) return 'access_denied';
  if (httpStatus !== 200) return 'transport_error';
  if (!resp) return 'transport_error';
  if (resp.status === 'not_found') return 'empty_state';
  if (resp.status === 'error') return 'resp_error';
  if (resp.status === 'ok' && resp.summary) return 'ok';
  return 'idle';
}

// runtime ИОС1.1 значения (как в build/runtime smoke)
const EA_REAL = {
  status: 'ok', available: true,
  kind: 'stage_comparison_pipeline_v2_entity_alignment_preview',
  summary: {
    graphic_pairs_total: 54, same_entity_likely: 7, possible_rename: 0,
    scope_reorganized: 5, mismatch_likely: 17, link_validation_candidate: 25,
    needs_manual_mapping: 5, unpaired_left: 22, unpaired_right: 21,
  },
  pairs: [
    { pair_key: 'a__b', left_block_id: '9T7M', right_block_id: 'DW7M',
      left_page_number: 28, right_page_number: 27, left_entity_label: 'ВРУ-3',
      right_entity_label: 'ВРУ-3', entity_family: 'ВРУ',
      classification: 'same_entity_likely', confidence: 0.9,
      recommended_action: 'use_for_enrichment',
      reasons: ['entity id совпадает'], risk_flags: [], evidence: {} },
    { pair_key: 'c__d', left_block_id: 'EYMU', right_block_id: 'PNNH',
      left_page_number: 27, right_page_number: 26, left_entity_label: 'ВРУ-3',
      right_entity_label: 'ВРУ-2', entity_family: 'ВРУ',
      classification: 'scope_reorganized', confidence: 0.6,
      recommended_action: 'manual_mapping',
      reasons: ['numbered_entity_conflict', 'equipment overlap insufficient'],
      risk_flags: ['numbered_conflict'], evidence: {} },
    { pair_key: 'e__f', left_block_id: 'EQRC', right_block_id: '64E3',
      left_page_number: 34, right_page_number: 33, left_entity_label: 'ЯК-3',
      right_entity_label: 'ЩО-1', entity_family: 'ЯК',
      classification: 'mismatch_likely', confidence: 0.85,
      recommended_action: 'exclude_from_enrichment',
      reasons: ['family_conflict'], risk_flags: [], evidence: {} },
    { pair_key: 'g__h', left_block_id: 'XX', right_block_id: 'YY',
      left_page_number: 40, right_page_number: 41, left_entity_label: null,
      right_entity_label: null, entity_family: null,
      classification: 'link_validation_candidate', confidence: 0.4,
      recommended_action: 'link_validation_only', reasons: [], risk_flags: [],
      evidence: {} },
  ],
  unpaired_entities: {
    left: [{ entity_label: 'ЩО-7', family: 'ЩО', graphic_type: 'scheme',
             sheet_name: 'Схема ЩО-7', block_ids: ['z1'] }],
    right: [{ entity_label: 'ВРУ-А', family: 'ВРУ', graphic_type: 'scheme',
              sheet_name: 'Схема ВРУ-А', block_ids: ['z2'] }],
  },
  warnings: [],
};

describe('Pipeline V2 — Entity Alignment summary', () => {
  it('1. summary рендерится со всеми классами (runtime ИОС1.1)', () => {
    const s = EA_REAL.summary;
    expect(s.graphic_pairs_total).toBe(54);
    expect(s.same_entity_likely).toBe(7);
    expect(s.possible_rename).toBe(0);
    expect(s.scope_reorganized).toBe(5);
    expect(s.mismatch_likely).toBe(17);
    expect(s.link_validation_candidate).toBe(25);
    expect(s.needs_manual_mapping).toBe(5);
    expect(s.unpaired_left).toBe(22);
    expect(s.unpaired_right).toBe(21);
  });
  it('2. classMeta даёт цвет/иконку для каждого класса + fallback', () => {
    expect(scPv2EaClassMeta('same_entity_likely').color).toBe('#16a34a');
    expect(scPv2EaClassMeta('scope_reorganized').icon).toBe('🟠');
    expect(scPv2EaClassMeta('mismatch_likely').fg).toBe('#991b1b');
    expect(scPv2EaClassMeta('totally_unknown_future').label).toBe('totally_unknown_future');
  });
  it('3. confidence форматируется в проценты, нет числа → пусто', () => {
    expect(scPv2EaConfPct({ confidence: 0.9 })).toBe('90%');
    expect(scPv2EaConfPct({ confidence: 0 })).toBe('0%');
    expect(scPv2EaConfPct({})).toBe('');
    expect(scPv2EaConfPct(null)).toBe('');
  });
});

describe('Pipeline V2 — Entity Alignment cards', () => {
  it('1. same_entity_likely карточка рендерится (ВРУ-3↔ВРУ-3)', () => {
    const c = EA_REAL.pairs.find((p) => p.classification === 'same_entity_likely');
    expect(c.left_entity_label).toBe('ВРУ-3');
    expect(c.right_entity_label).toBe('ВРУ-3');
    expect(scPv2EaClassMeta(c.classification).label).toBe('Same entity');
  });
  it('2. scope_reorganized карточка рендерится (ВРУ-3↔ВРУ-2)', () => {
    const c = EA_REAL.pairs.find((p) => p.classification === 'scope_reorganized');
    expect(c.left_entity_label).toBe('ВРУ-3');
    expect(c.right_entity_label).toBe('ВРУ-2');
    expect(c.recommended_action).toBe('manual_mapping');
    expect(c.reasons.length).toBeGreaterThan(0);
  });
  it('3. mismatch_likely карточка рендерится (ЯК↔ЩО)', () => {
    const c = EA_REAL.pairs.find((p) => p.classification === 'mismatch_likely');
    expect(c.left_entity_label).toBe('ЯК-3');
    expect(c.right_entity_label).toBe('ЩО-1');
    expect(scPv2EaClassMeta(c.classification).icon).toBe('🔴');
  });
  it('4. карточка без меток/семьи не падает (фолбэк —)', () => {
    const c = EA_REAL.pairs.find((p) => p.classification === 'link_validation_candidate');
    const lbl = c.left_entity_label || '—';
    expect(lbl).toBe('—');
    expect(scPv2EaConfPct(c)).toBe('40%');
  });
  it('5. unpaired state рендерится (left/right)', () => {
    expect(EA_REAL.unpaired_entities.left[0].entity_label).toBe('ЩО-7');
    expect(EA_REAL.unpaired_entities.right[0].entity_label).toBe('ВРУ-А');
    expect(scPv2EaShowUnpaired('all')).toBe(true);
    expect(scPv2EaShowUnpaired('unpaired')).toBe(true);
    expect(scPv2EaShowUnpaired('mismatch_likely')).toBe(false);
  });
});

describe('Pipeline V2 — Entity Alignment filters / states', () => {
  it('1. фильтр classification отбирает только нужный класс', () => {
    expect(scPv2EaFilteredPairs(EA_REAL.pairs, 'all').length).toBe(4);
    expect(scPv2EaFilteredPairs(EA_REAL.pairs, 'same_entity_likely').length).toBe(1);
    expect(scPv2EaFilteredPairs(EA_REAL.pairs, 'mismatch_likely')
      .every((p) => p.classification === 'mismatch_likely')).toBe(true);
  });
  it('2. фильтр unpaired скрывает карточки пар, показывает unpaired', () => {
    expect(scPv2EaFilteredPairs(EA_REAL.pairs, 'unpaired').length).toBe(0);
    expect(scPv2EaShowPairs('unpaired')).toBe(false);
    expect(scPv2EaShowUnpaired('unpaired')).toBe(true);
  });
  it('3. missing report (not_found) → empty-state, панель не падает', () => {
    expect(scPv2EaViewState(200, { status: 'not_found', available: false })).toBe('empty_state');
  });
  it('4. битый report (error) → resp_error, не 500', () => {
    expect(scPv2EaViewState(200, { status: 'error', available: false, warnings: ['x'] })).toBe('resp_error');
  });
  it('5. 401/403 → отказ в доступе; ok+summary → ok', () => {
    expect(scPv2EaViewState(401, null)).toBe('access_denied');
    expect(scPv2EaViewState(403, null)).toBe('access_denied');
    expect(scPv2EaViewState(200, EA_REAL)).toBe('ok');
  });
});

describe('Pipeline V2 — Entity Alignment read-only contract (files)', () => {
  const indexHtml = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
  const appJs = fs.readFileSync(path.join(__dirname, '..', 'static', 'js', 'app.js'), 'utf8');

  function eaPanelBlock() {
    const start = indexHtml.indexOf('Pipeline V2 — выравнивание сущностей');
    const end = indexHtml.indexOf('One-click авто-сопоставление листов: ошибка', start);
    expect(start).toBeGreaterThan(0);
    expect(end).toBeGreaterThan(start);
    return indexHtml.slice(start, end);
  }
  function eaJsBlock() {
    const start = appJs.indexOf('Pipeline V2 Entity Alignment Preview');
    const end = appJs.indexOf('return {', start);
    expect(start).toBeGreaterThan(0);
    expect(end).toBeGreaterThan(start);
    return appJs.slice(start, end);
  }

  it('1. панель сущностей присутствует с summary-карточками и фильтрами', () => {
    const blk = eaPanelBlock();
    expect(blk).toContain('🧩 Pipeline V2 — сущности и маппинг');
    expect(blk).toContain('SC_PV2_EA_FILTERS');
    expect(blk).toContain('same:');        // summary card
    expect(blk).toContain('mismatch:');
    expect(blk).toContain('без пары:');
  });
  it('2. есть decision UI, но НЕТ авто-применения к block links / vision', () => {
    const blk = eaPanelBlock();
    // ручные решения теперь есть (write-слой)
    expect(blk).toContain('Решение:');
    expect(blk).toContain('SC_PV2_EA_DECISIONS');
    expect(blk).toContain('scPv2EaSavePair');
    // но НЕТ кнопок авто-применения связей / автозапуска
    for (const forbidden of ['Применить', 'Принять связь', 'Запустить vision',
                             'Запустить распознавание']) {
      expect(blk).not.toContain(forbidden);
    }
    // явная подсказка, что vision/links не трогаются
    expect(blk).toContain('НЕ трогаются');
  });
  it('3. есть read-only jump «Открыть связь блоков»', () => {
    expect(eaPanelBlock()).toContain('Открыть связь блоков');
  });
  it('4. чтение = GET preview; запись = PUT ТОЛЬКО в entity-mapping-overrides', () => {
    const blk = eaJsBlock();
    expect(blk).toContain("/entity-alignment-preview?pair_id=");
    // единственный write — PUT в overrides-эндпоинт
    expect(blk).toContain("/entity-mapping-overrides?pair_id=");
    expect(blk).toContain("method: 'PUT'");
    // никаких вызовов, запускающих vision/Qwen/Opus/jobs/сравнение
    expect(blk).not.toContain('md-enrichment-jobs');
    expect(blk).not.toContain('unified-analysis');
    expect(blk).not.toContain('graphic-diff-jobs');
    expect(blk).not.toMatch(/method:\s*['"]POST['"]/);   // overrides — это PUT, не POST-job
  });
  it('5. старые панели не тронуты (block link preview + ui-payload живы)', () => {
    expect(appJs).toContain('scPv2LpToggle');         // block link preview
    expect(appJs).toContain('/block-link-preview?pair_id=');
    expect(indexHtml).toContain('🔗 Pipeline V2 связи');
    expect(appJs).toContain('/ui-payload');           // основная панель Pipeline V2 жива
  });
});

// ── Pipeline V2 — Manual Entity Mapping (write-слой) ─────────────────────────
//
// Зеркало pure-логики decision-сохранения (scPv2Ea* write) + контрактные
// проверки по файлам: PUT только в overrides, без авто-применения к links/vision.

const SC_PV2_EA_DECISIONS = [
  { key: 'confirmed_same_entity', label: '✅ Та же сущность' },
  { key: 'confirmed_rename', label: '🔁 Переименование' },
  { key: 'confirmed_reorganized', label: '🟠 Реорганизация' },
  { key: 'rejected_mapping', label: '❌ Отклонить связь' },
  { key: 'no_match', label: '⚪ Нет пары' },
];
function eaManualStatusForDecision(decision) {
  if (['confirmed_same_entity', 'confirmed_rename', 'confirmed_reorganized'].includes(decision))
    return 'mapped';
  if (decision === 'rejected_mapping') return 'rejected';
  if (decision === 'no_match') return 'no_match';
  return 'none';
}
function eaPairKey(p) {
  return (p && (p.pair_key || ((p.left_block_id || '') + '__' + (p.right_block_id || '')))) || '';
}
function eaUnpairedKey(e, side) {
  const b = (e && e.block_ids && e.block_ids[0]) || '';
  return side + ':' + ((e && e.entity_label) || '') + ':' + b;
}
// зеркало применения ответа PUT к карточке
function eaApplyManual(targetObj, override) {
  targetObj.manual_mapping = {
    status: eaManualStatusForDecision(override.manual_decision),
    decision: override.manual_decision,
    mapping_id: override.mapping_id,
    comment: override.comment || null,
    updated_at: override.updated_at,
  };
  return targetObj;
}

describe('Pipeline V2 — Manual Entity Mapping logic', () => {
  it('1. decision → manual status маппинг', () => {
    expect(eaManualStatusForDecision('confirmed_same_entity')).toBe('mapped');
    expect(eaManualStatusForDecision('confirmed_reorganized')).toBe('mapped');
    expect(eaManualStatusForDecision('rejected_mapping')).toBe('rejected');
    expect(eaManualStatusForDecision('no_match')).toBe('no_match');
    expect(eaManualStatusForDecision(undefined)).toBe('none');
  });
  it('2. 5 решений доступны', () => {
    expect(SC_PV2_EA_DECISIONS.map((d) => d.key)).toEqual([
      'confirmed_same_entity', 'confirmed_rename', 'confirmed_reorganized',
      'rejected_mapping', 'no_match']);
  });
  it('3. ключи карточек стабильны (pair / unpaired)', () => {
    expect(eaPairKey({ pair_key: 'A__B' })).toBe('A__B');
    expect(eaPairKey({ left_block_id: 'L', right_block_id: 'R' })).toBe('L__R');
    expect(eaUnpairedKey({ entity_label: 'ЯК-5', block_ids: ['6XLX'] }, 'left'))
      .toBe('left:ЯК-5:6XLX');
  });
  it('4. ответ PUT обновляет manual_mapping карточки', () => {
    const card = { pair_key: 'EYMU__PNNH', manual_mapping: { status: 'none' } };
    eaApplyManual(card, { mapping_id: 'm_x', manual_decision: 'confirmed_reorganized',
                          comment: 'c', updated_at: 't' });
    expect(card.manual_mapping.status).toBe('mapped');
    expect(card.manual_mapping.decision).toBe('confirmed_reorganized');
    expect(card.manual_mapping.mapping_id).toBe('m_x');
    expect(card.manual_mapping.comment).toBe('c');
  });
  it('5. rejected/no_match отражаются на карточке', () => {
    const c1 = { manual_mapping: { status: 'none' } };
    eaApplyManual(c1, { mapping_id: 'm1', manual_decision: 'rejected_mapping' });
    expect(c1.manual_mapping.status).toBe('rejected');
    const c2 = { manual_mapping: { status: 'none' } };
    eaApplyManual(c2, { mapping_id: 'm2', manual_decision: 'no_match' });
    expect(c2.manual_mapping.status).toBe('no_match');
  });
});

describe('Pipeline V2 — Manual Entity Mapping contract (files)', () => {
  const indexHtml = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
  const appJs = fs.readFileSync(path.join(__dirname, '..', 'static', 'js', 'app.js'), 'utf8');
  function eaPanel() {
    const s = indexHtml.indexOf('Pipeline V2 — выравнивание сущностей');
    const e = indexHtml.indexOf('One-click авто-сопоставление листов: ошибка', s);
    return indexHtml.slice(s, e);
  }

  it('1. карточка пары показывает decision UI + кнопку сохранить', () => {
    const blk = eaPanel();
    expect(blk).toContain('Решение:');
    expect(blk).toContain('scPv2EaSavePair');
    expect(blk).toContain('Сохранить решение');
  });
  it('2. save шлёт PUT в правильный endpoint', () => {
    expect(appJs).toContain("/entity-mapping-overrides?pair_id=");
    expect(appJs).toContain("method: 'PUT'");
    expect(appJs).toContain('_scPv2EaPutMapping');
  });
  it('3. успешный save обновляет card.manual_mapping (есть applier)', () => {
    expect(appJs).toContain('_scPv2EaApplyManual');
    expect(appJs).toContain('targetObj.manual_mapping');
  });
  it('4. ошибка save показывается (scPv2EaSaveErr)', () => {
    expect(eaPanel()).toContain('scPv2EaSaveErr');
    expect(appJs).toContain('scPv2EaSaveErr[key]');
  });
  it('5. unpaired section всё ещё рендерится + имеет decision UI', () => {
    const blk = eaPanel();
    expect(blk).toContain('Сущности без пары');
    expect(blk).toContain('scPv2EaSaveUnpaired');
    expect(blk).toContain('scPv2EaUnpairedCounterparts');
  });
  it('6. НЕТ кнопки авто-применения/автозапуска vision', () => {
    const blk = eaPanel();
    for (const forbidden of ['Применить к связям', 'Применить', 'Запустить vision',
                             'Принять связь']) {
      expect(blk).not.toContain(forbidden);
    }
    // подсказка о no-auto-run присутствует в JS
    expect(appJs).toContain('сейчас ничего не');
  });
  it('7. старые панели Pipeline V2 не сломаны', () => {
    expect(appJs).toContain('scPv2LpToggle');
    expect(appJs).toContain('scPv2EaToggle');
    expect(indexHtml).toContain('🔗 Pipeline V2 связи');
    expect(indexHtml).toContain('🧩 Pipeline V2 сущности');
  });
  it('8. block link preview jump не сломан (мост переиспользован)', () => {
    expect(appJs).toContain('scPv2OpenBlockLinkFromGrounding');
    expect(eaPanel()).toContain('Открыть связь блоков');
  });
});

// ── Pipeline V2 — Link Validation (read-only, mark-only) ─────────────────────
//
// Зеркало pure-логики scPv2Lv* из app.js + контрактные проверки по файлам:
// панель НИЧЕГО не применяет, не запускает vision/Qwen/Opus, не создаёт
// замечаний; link-validation никогда не grounded-факт.

const SC_PV2_LV_DECISION_META = {
  valid_mapping: { label: 'valid_mapping', icon: '🟢', bg: '#dcfce7', fg: '#166534' },
  manual_review: { label: 'manual_review', icon: '🟡', bg: '#fef9c3', fg: '#854d0e' },
  reject_mapping: { label: 'reject_mapping', icon: '🔴', bg: '#fee2e2', fg: '#991b1b' },
};
function scPv2LvDecisionMeta(d) {
  return SC_PV2_LV_DECISION_META[d]
    || { label: d || '—', icon: '⚪', bg: '#f3f4f6', fg: '#374151' };
}
function scPv2LvConfPct(it) {
  const c = it && it.validation && it.validation.confidence;
  return (typeof c === 'number') ? Math.round(c * 100) + '%' : '';
}
// зеркало state-машины подпанели по detail-envelope
function scPv2LvViewState(resp) {
  if (!resp) return 'idle';
  if (resp.status === 'ok' && resp.available) return 'available';
  if (resp.status === 'not_found') return 'not_found';
  if (resp.status === 'error') return 'error';
  return 'idle';
}

function makeLvReport() {
  return {
    version: 1, kind: 'stage_comparison_pipeline_v2_link_validation', status: 'ok',
    available: true, source: 'ready_report',
    summary: {
      candidates_total: 3, attempted: 3, succeeded: 3, failed: 0,
      valid_mapping: 1, manual_review: 1, reject_mapping: 1,
      agrees_with_manual_mapping: 1, conflicts_with_manual_mapping: 1,
      orientation_failed: 0,
    },
    items: [
      { item_id: 'a2', left_block_id: '6XDP-JLWQ-KNX', right_block_id: '3T6X-4PHG-D96',
        left_page_number: 27, right_page_number: 26,
        left_entity_label: 'ВРУ-3', right_entity_label: 'ВРУ-2',
        manual_decision: 'confirmed_reorganized', recommended_action: 'manual_review_mapping',
        validation: { old_new_orientation_ok: true, entity_relation: 'different_entity',
                      decision: 'reject_mapping', confidence: 0.95, do_not_use_as_fact: true },
        agreement: { agrees_with_manual_mapping: false, conflicts_with_manual_mapping: true,
                     reason: 'vision противоречит manual mapping' },
        use_as_grounded_fact: false, use_for_delta_explanation: false },
      { item_id: 'a1', left_block_id: '9T7M', right_block_id: 'DW7M',
        left_page_number: 28, right_page_number: 27,
        left_entity_label: 'ВРУ-3', right_entity_label: 'ВРУ-3',
        manual_decision: 'confirmed_same', recommended_action: 'keep_mapping',
        validation: { old_new_orientation_ok: true, entity_relation: 'same_entity',
                      decision: 'valid_mapping', confidence: 0.92, do_not_use_as_fact: true },
        agreement: { agrees_with_manual_mapping: true, conflicts_with_manual_mapping: false,
                     reason: 'vision согласуется' },
        use_as_grounded_fact: false, use_for_delta_explanation: false },
    ],
    warnings: [],
  };
}

describe('Pipeline V2 — Link Validation logic', () => {
  it('1. decision meta: цвета valid/review/reject + fallback', () => {
    expect(scPv2LvDecisionMeta('valid_mapping').fg).toBe('#166534');
    expect(scPv2LvDecisionMeta('manual_review').fg).toBe('#854d0e');
    expect(scPv2LvDecisionMeta('reject_mapping').fg).toBe('#991b1b');
    expect(scPv2LvDecisionMeta('reject_mapping').icon).toBe('🔴');
    expect(scPv2LvDecisionMeta('weird').icon).toBe('⚪');   // fallback
  });
  it('2. confidence → проценты, без числа = пусто', () => {
    expect(scPv2LvConfPct({ validation: { confidence: 0.95 } })).toBe('95%');
    expect(scPv2LvConfPct({ validation: {} })).toBe('');
    expect(scPv2LvConfPct({})).toBe('');
  });
  it('3. view-state по detail-envelope', () => {
    expect(scPv2LvViewState(null)).toBe('idle');
    expect(scPv2LvViewState(makeLvReport())).toBe('available');
    expect(scPv2LvViewState({ status: 'not_found' })).toBe('not_found');
    expect(scPv2LvViewState({ status: 'error', warnings: ['x'] })).toBe('error');
  });
  it('4. summary рендерит счётчики (включая conflicts)', () => {
    const s = makeLvReport().summary;
    expect(s.attempted).toBe(3);
    expect(s.reject_mapping).toBe(1);
    expect(s.conflicts_with_manual_mapping).toBe(1);
  });
  it('5. конфликтный item: manual confirmed_reorganized vs vision reject', () => {
    const it = makeLvReport().items[0];
    expect(it.manual_decision).toBe('confirmed_reorganized');
    expect(it.validation.decision).toBe('reject_mapping');
    expect(it.agreement.conflicts_with_manual_mapping).toBe(true);
    expect(scPv2LvDecisionMeta(it.validation.decision).fg).toBe('#991b1b'); // красный
  });
  it('6. mark-only инвариант: ни один item не grounded-факт', () => {
    for (const it of makeLvReport().items) {
      expect(it.use_as_grounded_fact).toBe(false);
      expect(it.use_for_delta_explanation).toBe(false);
      expect(it.validation.do_not_use_as_fact).toBe(true);
    }
  });
  it('7. missing report не ломает подпанель', () => {
    expect(scPv2LvViewState({ status: 'not_found' })).toBe('not_found');
    // items-геттер на пустом ответе → []
    const items = (({ items }) => items || [])({});
    expect(items).toEqual([]);
  });
});

describe('Pipeline V2 — Link Validation read-only contract (files)', () => {
  const indexHtml = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
  const appJs = fs.readFileSync(path.join(__dirname, '..', 'static', 'js', 'app.js'), 'utf8');
  function lvBlock() {
    const s = indexHtml.indexOf('🔎 Link validation (read-only, mark-only)');
    const e = indexHtml.indexOf('<!-- transport / HTTP errors -->', s);
    expect(s).toBeGreaterThan(0);
    expect(e).toBeGreaterThan(s);
    return indexHtml.slice(s, e);
  }

  it('1. подпанель Link validation присутствует с заголовком и summary', () => {
    const blk = lvBlock();
    expect(blk).toContain('🔎 Link validation');
    expect(blk).toContain('scPv2LvSummary');
    expect(blk).toContain('scPv2LvItems');
    expect(blk).toContain('attempted:');
    expect(blk).toContain('reject:');
    expect(blk).toContain('conflicts:');
  });
  it('2. mark-only предупреждение присутствует дословно', () => {
    expect(lvBlock()).toContain(
      'Link validation is mark-only. It is not used as grounded fact and is not used for delta explanation.');
  });
  it('3. НЕТ кнопок применить/запустить vision/изменить mapping/создать замечание', () => {
    const blk = lvBlock();
    for (const forbidden of ['Применить', 'Запустить vision', 'Изменить mapping',
                             'Создать замечание', 'Принять связь']) {
      expect(blk).not.toContain(forbidden);
    }
  });
  it('4. конфликт визуализируется (бейдж + цветовой код)', () => {
    const blk = lvBlock();
    expect(blk).toContain('conflicts_with_manual_mapping');
    expect(blk).toContain('конфликт с manual mapping');
    expect(blk).toContain('scPv2LvDecisionMeta');
  });
  it('5. опциональный read-only jump «Открыть связь блоков»', () => {
    expect(lvBlock()).toContain('Открыть связь блоков');
    expect(appJs).toContain('scPv2LvOpenBlockLink');
    expect(appJs).toContain('scPv2OpenBlockLinkFromGrounding');   // мост переиспользован
  });
  it('6. чтение = GET /link-validation; никаких write/job вызовов в LV-логике', () => {
    expect(appJs).toContain('/link-validation?pair_id=');
    const s = appJs.indexOf('async function scPv2LvLoad');
    const e = appJs.indexOf('function scPv2LvReset', s);
    const lvJs = appJs.slice(s, e);
    expect(lvJs).not.toMatch(/method:\s*['"](POST|PUT|DELETE)['"]/);
    expect(lvJs).not.toContain('md-enrichment-jobs');
    expect(lvJs).not.toContain('unified-analysis');
    expect(lvJs).not.toContain('entity-mapping-overrides');
  });
  it('7. старая панель entity-alignment не сломана', () => {
    expect(appJs).toContain('scPv2EaToggle');
    expect(appJs).toContain('/entity-alignment-preview?pair_id=');
    expect(indexHtml).toContain('🧩 Pipeline V2 сущности');
  });
  it('8. старые grounding / block-link / ui-payload панели не сломаны', () => {
    expect(appJs).toContain('scPv2LpToggle');                 // block link preview
    expect(appJs).toContain('/block-link-preview?pair_id=');
    expect(appJs).toContain('scPv2OpenBlockLinkFromGrounding'); // grounding jump
    expect(appJs).toContain('/ui-payload');                   // основная панель жива
  });
});

// ─── Exclusion Preview v2 — JS логика ─────────────────────────────────────

describe('Pipeline V2 Exclusion Preview v2 — JS logic', () => {
  const appJs = fs.readFileSync(path.join(__dirname, '..', 'static', 'js', 'app.js'), 'utf8');

  it('1. реактивное состояние scPv2XpResp/Loading/Error/ReqSeq декларировано', () => {
    expect(appJs).toContain('scPv2XpResp = ref(null)');
    expect(appJs).toContain('scPv2XpLoading = ref(false)');
    expect(appJs).toContain("scPv2XpError = ref('')");
    expect(appJs).toContain('scPv2XpReqSeq = 0');
  });

  it('2. SC_PV2_XP_CLASS_META содержит все 4 классификации', () => {
    expect(appJs).toContain('candidate_exclude');
    expect(appJs).toContain('review_only');
    expect(appJs).toContain('link_validation_required');
    // keep в meta
    const metaStart = appJs.indexOf('SC_PV2_XP_CLASS_META');
    const metaEnd = appJs.indexOf('};', metaStart);
    const meta = appJs.slice(metaStart, metaEnd);
    expect(meta).toContain('keep');
  });

  it('3. computeds scPv2XpSummary/Items/Available/NotFound/RespError декларированы', () => {
    expect(appJs).toContain('scPv2XpSummary');
    expect(appJs).toContain('scPv2XpItems');
    expect(appJs).toContain('scPv2XpAvailable');
    expect(appJs).toContain('scPv2XpNotFound');
    expect(appJs).toContain('scPv2XpRespError');
  });

  it('4. scPv2XpLoad читает GET /exclusion-preview-v2 — никаких write/job вызовов', () => {
    expect(appJs).toContain('/exclusion-preview-v2?pair_id=');
    const s = appJs.indexOf('async function scPv2XpLoad');
    const e = appJs.indexOf('function scPv2XpReset', s);
    const fnBody = appJs.slice(s, e);
    expect(fnBody).not.toMatch(/method:\s*['"](POST|PUT|DELETE)['"]/);
    expect(fnBody).not.toContain('md-enrichment-jobs');
    expect(fnBody).not.toContain('unified-analysis');
    expect(fnBody).not.toContain('entity-mapping-overrides');
    expect(fnBody).not.toContain('link-validation');
  });

  it('5. scPv2XpLoad вызывается вместе с scPv2LvLoad (после EA успешного load)', () => {
    // должны стоять рядом в одном try-блоке EA
    const lvIdx = appJs.indexOf('scPv2LvLoad();');
    const xpIdx = appJs.indexOf('scPv2XpLoad();');
    expect(xpIdx).toBeGreaterThan(0);
    // XP load вызывается в той же области что LV load (разница ≤ 200 chars)
    expect(Math.abs(xpIdx - lvIdx)).toBeLessThan(200);
  });

  it('6. scPv2XpReset вызывается из scPv2EaReset и watch(scPv2EaPairId)', () => {
    expect(appJs.split('scPv2XpReset()').length - 1).toBeGreaterThanOrEqual(2);
  });

  it('7. функции экспортированы в return-объект Vue', () => {
    const retStart = appJs.lastIndexOf('return {');
    const retSection = appJs.slice(retStart);
    expect(retSection).toContain('scPv2XpLoad');
    expect(retSection).toContain('scPv2XpReset');
    expect(retSection).toContain('scPv2XpClassMeta');
    expect(retSection).toContain('scPv2XpSummary');
  });
});

// ─── Exclusion Preview v2 — HTML панель ────────────────────────────────────

describe('Pipeline V2 Exclusion Preview v2 — HTML panel', () => {
  const indexHtml = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
  function xpBlock() {
    const s = indexHtml.indexOf('scPv2XpAvailable || scPv2XpNotFound || scPv2XpRespError');
    expect(s).toBeGreaterThan(0);
    const e = indexHtml.indexOf('</div>', indexHtml.lastIndexOf('scPv2XpItems', s + 5000)) + 6;
    expect(e).toBeGreaterThan(s);
    return indexHtml.slice(s, e);
  }

  it('1. панель присутствует с заголовком и summary chips', () => {
    const blk = xpBlock();
    expect(blk).toContain('🚫 Exclusion Preview v2');
    expect(blk).toContain('scPv2XpSummary');
    expect(blk).toContain('scPv2XpItems');
    expect(blk).toContain('candidate_exclude');
    expect(blk).toContain('keep');
  });

  it('2. mark-only предупреждение присутствует', () => {
    const blk = xpBlock();
    expect(blk).toContain(
      'Exclusion Preview v2 is mark-only. Nothing is excluded, applied, or enforced.');
  });

  it('3. НЕТ кнопок применить/исключить/принять/запустить', () => {
    const blk = xpBlock();
    for (const forbidden of ['Применить', 'Исключить', 'Принять', 'Запустить',
                             'Создать замечание']) {
      expect(blk).not.toContain(forbidden);
    }
  });

  it('4. risk_flags показываются как ⚑-бейджи', () => {
    const blk = xpBlock();
    expect(blk).toContain('risk_flags');
    expect(blk).toContain('⚑');
  });

  it('5. панель расположена ПОСЛЕ link-validation и ДО transport/HTTP errors', () => {
    const lvEnd = indexHtml.lastIndexOf('🔎 Link validation');
    const xpStart = indexHtml.indexOf('🚫 Exclusion Preview v2');
    // Find the transport comment that comes AFTER the XP panel (there is an earlier one before LV)
    const transportStart = indexHtml.indexOf('<!-- transport / HTTP errors -->', xpStart);
    expect(xpStart).toBeGreaterThan(lvEnd);
    expect(xpStart).toBeLessThan(transportStart);
  });

  it('6. OLD/NEW grid использует left_entity_label/right_entity_label', () => {
    // xpBlock() ends before the entity label grid — check the full XP panel region directly
    const xpPanelStart = indexHtml.indexOf('🚫 Exclusion Preview v2');
    const xpRegion = indexHtml.slice(xpPanelStart, xpPanelStart + 10000);
    expect(xpRegion).toContain('left_entity_label');
    expect(xpRegion).toContain('right_entity_label');
  });

  it('7. старые LV и EA панели не сломаны', () => {
    expect(indexHtml).toContain('🔎 Link validation');
    expect(indexHtml).toContain('🧩 Pipeline V2 сущности');
    expect(indexHtml).toContain('scPv2LvItems');
  });
});

// ─── Controlled Enforce Preflight — JS логика (read-only / observe-only) ─────
//
// Зеркало pure-логики scPv2Ce* из app.js + контрактные проверки по файлам:
// панель observe-only, НИЧЕГО не применяет/не enforce'ит, нет apply/skip-кнопок.

const SC_PV2_CE_STATUS_META = {
  blocked:           { icon: '🔴', label: 'enforce заблокирован' },
  preflight_ok:      { icon: '🟢', label: 'preflight ok (не применяется)' },
  no_eligible_items: { icon: '🟡', label: 'нет eligible' },
};
function scPv2CeStatusMeta(status) {
  return SC_PV2_CE_STATUS_META[status]
    || { icon: '⚪', label: status || '—' };
}
function scPv2CeViewState(resp) {
  if (!resp) return 'idle';
  if (resp.status === 'ok' && resp.available) return 'available';
  if (resp.status === 'not_found') return 'not_found';
  if (resp.status === 'error') return 'error';
  return 'idle';
}
function makeCeReport(overrides) {
  return Object.assign({
    version: 1,
    kind: 'stage_comparison_pipeline_v2_controlled_enforce_preflight',
    status: 'ok', available: true, report_status: 'blocked',
    summary: {
      ready_to_skip_items: 0, eligible_items: 0, blocked_items: 2,
      fatal_blocks: 1, would_apply: false, enforce_enabled: false,
    },
    global_guards: { active_runtime_root_confirmed: true, ready_to_skip_present: false },
    runtime_root: { active: '/x/comparison', confirmed: true, source: '/api/info' },
    fatal_blocks: ['ready_to_skip_zero'],
    eligible_items: [],
    blocked_items: [
      { item_id: 'a', reason: 'missing_operator_approval', source_readiness: 'blocked' },
      { item_id: 'b', reason: 'needs_review', source_readiness: 'needs_review' },
    ],
    would_apply: false, enforce_enabled: false,
    auto_apply: false, enforce_allowed: false,
  }, overrides || {});
}

describe('Pipeline V2 Controlled Enforce Preflight — JS logic', () => {
  it('1. available report → view-state available, summary читается', () => {
    const r = makeCeReport();
    expect(scPv2CeViewState(r)).toBe('available');
    expect(r.summary.ready_to_skip_items).toBe(0);
  });
  it('2. blocked status имеет meta-иконку', () => {
    expect(scPv2CeStatusMeta('blocked').icon).toBe('🔴');
    expect(scPv2CeStatusMeta('preflight_ok').icon).toBe('🟢');
  });
  it('3. fatal block ready_to_skip_zero присутствует', () => {
    const r = makeCeReport();
    expect(r.fatal_blocks).toContain('ready_to_skip_zero');
  });
  it('4. would_apply=false и enforce_enabled=false', () => {
    const r = makeCeReport();
    expect(r.summary.would_apply).toBe(false);
    expect(r.summary.enforce_enabled).toBe(false);
    expect(r.would_apply).toBe(false);
    expect(r.enforce_enabled).toBe(false);
  });
  it('5. active runtime root confirmed читается', () => {
    const r = makeCeReport();
    expect(r.runtime_root.confirmed).toBe(true);
    expect(r.global_guards.active_runtime_root_confirmed).toBe(true);
  });
  it('6. not_found не ломает view-state', () => {
    expect(scPv2CeViewState({ status: 'not_found', available: false })).toBe('not_found');
    expect(scPv2CeViewState(null)).toBe('idle');
  });
  it('7. unknown status → нейтральная иконка, не падает', () => {
    expect(scPv2CeStatusMeta('weird_future').icon).toBe('⚪');
  });
});

describe('Pipeline V2 Controlled Enforce Preflight — HTML panel (files)', () => {
  const indexHtml = fs.readFileSync(path.join(__dirname, '..', 'index.html'), 'utf8');
  const appJs = fs.readFileSync(path.join(__dirname, '..', 'static', 'js', 'app.js'), 'utf8');
  function ceBlock() {
    const s = indexHtml.indexOf('scPv2CeAvailable || scPv2CeNotFound || scPv2CeRespError');
    expect(s).toBeGreaterThan(0);
    const e = indexHtml.indexOf('transport / HTTP errors', s);
    expect(e).toBeGreaterThan(s);
    return indexHtml.slice(s, e);
  }

  it('1. summary панель с заголовком 🧯 и chips рендерится', () => {
    const blk = ceBlock();
    expect(blk).toContain('🧯 Controlled Enforce Preflight');
    expect(blk).toContain('scPv2CeSummary');
    expect(blk).toContain('ready_to_skip');
    expect(blk).toContain('eligible');
    expect(blk).toContain('blocked');
  });
  it('2. blocked status рендерится через scPv2CeReportStatus', () => {
    expect(ceBlock()).toContain('scPv2CeReportStatus');
  });
  it('3. fatal block ready_to_skip_zero рендерится (scPv2CeFatalBlocks)', () => {
    const blk = ceBlock();
    expect(blk).toContain('scPv2CeFatalBlocks');
    expect(blk).toContain('Fatal blocks:');
  });
  it('4. would_apply=false рендерится', () => {
    expect(ceBlock()).toContain('would_apply: false');
  });
  it('5. enforce_enabled=false рендерится', () => {
    expect(ceBlock()).toContain('enforce_enabled: false');
  });
  it('6. active runtime root confirmed рендерится', () => {
    const blk = ceBlock();
    expect(blk).toContain('scPv2CeRuntimeRoot');
    expect(blk).toContain('active root confirmed');
  });
  it('7. observe-only warning присутствует', () => {
    expect(ceBlock()).toContain(
      'Controlled Enforce Preflight is observe-only. It does not skip, exclude, enforce, change links, or create findings.');
  });
  it('8. missing report не ломает UI (not_found ветка есть)', () => {
    expect(ceBlock()).toContain('scPv2CeNotFound');
    expect(ceBlock()).toContain('ещё не построен');
  });
  it('9. НЕТ кнопок apply/enforce/skip/исключить/создать замечание', () => {
    const blk = ceBlock();
    for (const forbidden of ['Применить', 'Запустить enforce', 'Запустить skip',
                             'Исключить сейчас', 'Создать замечание', 'Изменить block links']) {
      expect(blk).not.toContain(forbidden);
    }
  });
  it('10. старые панели (exclusion/skip/link/entity) не сломаны', () => {
    expect(indexHtml).toContain('🚫 Exclusion Preview v2');
    expect(indexHtml).toContain('🛡 Skip Readiness');
    expect(indexHtml).toContain('🔎 Link validation');
    expect(indexHtml).toContain('🧩 Pipeline V2 сущности');
    // endpoint wiring jolts
    expect(appJs).toContain('/controlled-enforce-preflight?pair_id=');
    expect(appJs).toContain('scPv2CeLoad');
    expect(appJs).toContain('scPv2SrLoad');
  });
});
