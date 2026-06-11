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
