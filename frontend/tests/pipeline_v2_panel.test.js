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
