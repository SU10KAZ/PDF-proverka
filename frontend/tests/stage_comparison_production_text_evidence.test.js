import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const require = createRequire(import.meta.url);
const review = require('../static/js/stage-comparison-review.js');
const html = readFileSync(new URL('../index.html', import.meta.url), 'utf8');
const app = readFileSync(new URL('../static/js/app.js', import.meta.url), 'utf8');
const css = readFileSync(new URL('../static/css/styles.css', import.meta.url), 'utf8');

function location(page, fragmentId, bbox) {
  return {
    source: 'TEXT', page, fragment_id: fragmentId,
    coordinate_space: 'NORMALIZED_PAGE_TOP_LEFT',
    coordinates_available: true,
    highlight: {kind: 'BBOX_SET', bboxes: [bbox]},
  };
}

function payload(overrides = {}) {
  return {
    kind: 'stage_comparison_production_text_evidence',
    schema_version: 'production-text-evidence.v2',
    available: true,
    stale: false,
    run_status: 'COMPLETED',
    generation_run_id: 'run-current',
    generation_revision: 11,
    input_signature: 'generation-current',
    synthesis_input_signature: 'synthesis-current',
    text_result_state: 'PUBLISHED',
    match_evidence_state: 'VERIFIED',
    available_match_pairs: 1,
    change_items: 1,
    available_change_items: 1,
    summary: {
      matched_fragments: null, changed_fragments: 1,
      changed: 1, removed: 0, added: 0, review_required: 0,
    },
    matches: [{
      evidence_id: 'match-1', title: 'Одинаковый заголовок',
      sides: {
        LEFT: [location(7, 'left-same', [0.1, 0.2, 0.4, 0.26])],
        RIGHT: [location(16, 'right-same', [0.12, 0.22, 0.42, 0.28])],
      },
    }],
    changes: [{
      evidence_id: 'change-1', target_id: 'change-1', target_kind: 'CHANGE',
      title: '220 В → 380 В', review_status: 'CONFIRMED',
      before: '220 В', after: '380 В', review_required: false,
      sides: {
        LEFT: [location(7, 'left-change', [0.2, 0.4, 0.5, 0.46])],
        RIGHT: [location(16, 'right-change', [0.22, 0.42, 0.52, 0.48])],
      },
    }],
    ...overrides,
  };
}

describe('production TEXT cleanup and visual evidence', () => {
  it('A/B: full analysis owns TEXT and no partial legacy computation control is rendered', () => {
    expect(review.normalizeProductionOverview({
      active_pair: {left: {filename: 'LEFT.pdf'}, right: {filename: 'RIGHT.pdf'}},
      state: {status: 'NOT_STARTED', stages: {}},
    }).cta.label).toBe('▶ Запустить полный анализ');
    expect(html).not.toContain('scRunTextComparison');
    expect(html).not.toContain('scRunTextDifferences');
    expect(html).not.toContain('scRunTextAiReview');
    expect(html).not.toContain('scRunProjectChangeSummary');
    expect(html).not.toContain('Пересчитать текст');
    expect(html).toContain('↻ Запустить полный анализ заново');

    const contentStage = review.normalizeProductionPipeline({
      active_pair: {left: {filename: 'LEFT.pdf'}, right: {filename: 'RIGHT.pdf'}},
      state: {status: 'COMPLETED', stages: {
        text: {status: 'COMPLETED'}, graphic: {status: 'COMPLETED'},
      }},
    })[2];
    expect(contentStage.rerun).toMatchObject({
      kind: 'FULL_RERUN', partial_rerun_supported: false,
    });
    expect(contentStage.rerun.note).toBe(
      'Повторный расчёт текста сейчас выполняется в составе полного автоматического анализа.',
    );
  });

  it('C/F: mode changes are local-only and reload defaults to an unfiltered document', () => {
    expect(app).toContain("const scTextEvidenceMode = ref('all')");
    for (const suffix of [
      '/state', '/changes', '/questions', '/preliminary-report',
      '/final-report', '/text-evidence',
    ]) expect(app).toContain(`'${suffix}'`);
    const start = app.indexOf('function scSetTextEvidenceMode(mode)');
    const end = app.indexOf('function scTextEvidenceOverlaysFor', start);
    const implementation = app.slice(start, end);
    expect(implementation).not.toContain('fetch(');
    expect(implementation).not.toContain('scProductionRequest');
    expect(implementation).not.toContain('scRunProductionComparison');

    const normalized = review.normalizeProductionTextEvidence(payload());
    expect(review.productionTextEvidenceOverlays(normalized, 'all', 'left', 7, '')).toEqual([]);
    expect(html).toContain("@click=\"scSetTextEvidenceMode('matches')\"");
    expect(html).toContain("@click=\"scSetTextEvidenceMode('changes')\"");
  });

  it('D: one persisted match activates exact LEFT and RIGHT anchors together', () => {
    const normalized = review.normalizeProductionTextEvidence(payload());
    const left = review.productionTextEvidenceOverlays(normalized, 'matches', 'left', 7, 'match-1');
    const right = review.productionTextEvidenceOverlays(normalized, 'matches', 'right', 16, 'match-1');

    expect(left).toHaveLength(1);
    expect(right).toHaveLength(1);
    expect(left[0]).toMatchObject({
      evidence_id: 'match-1', evidence_kind: 'MATCH', active: true, paired: true,
    });
    expect(right[0]).toMatchObject({
      evidence_id: 'match-1', evidence_kind: 'MATCH', active: true, paired: true,
    });
    expect(left[0].fragment_id).toBe('left-same');
    expect(right[0].fragment_id).toBe('right-same');
    expect(css).toContain('.sc-text-evidence-overlay.is-match.is-active::after');
    expect(css).toContain('content: "↔"');
  });

  it('E: change overlays remain generation-bound and carry only an existing atomic target mapping', () => {
    const normalized = review.normalizeProductionTextEvidence(payload());
    const state = {
      run_id: 'run-current', input_signature: 'generation-current', revision: 11,
    };
    const changes = {input_signature: 'synthesis-current'};
    const overlays = review.productionTextEvidenceOverlays(
      normalized, 'changes', 'left', 7, 'change-1',
    );

    expect(review.productionTextEvidenceMatchesGeneration(state, changes, normalized)).toBe(true);
    expect(normalized.generation_run_id).toBe('run-current');
    expect(normalized.generation_revision).toBe(11);
    expect(normalized.input_signature).toBe('generation-current');
    expect(normalized.synthesis_input_signature).toBe('synthesis-current');
    expect(overlays[0]).toMatchObject({
      evidence_id: 'change-1', target_id: 'change-1',
      evidence_kind: 'CHANGE', active: true,
    });
    expect(app).toContain('await scOpenProductionReviewTarget(item.target_id)');

    const stale = review.normalizeProductionTextEvidence(payload({stale: true}));
    expect(review.productionTextEvidenceOverlays(stale, 'changes', 'left', 7, '')).toEqual([]);

    for (const [statePatch, changesPatch, evidencePatch] of [
      [{run_id: 'run-other'}, {}, {}],
      [{input_signature: 'generation-other'}, {}, {}],
      [{revision: 12}, {}, {}],
      [{}, {input_signature: 'synthesis-other'}, {}],
      [{revision: 0}, {}, {generation_revision: null}],
    ]) {
      expect(review.productionTextEvidenceMatchesGeneration(
        {...state, ...statePatch},
        {...changes, ...changesPatch},
        review.normalizeProductionTextEvidence(payload(evidencePatch)),
      )).toBe(false);
    }
  });

  it('omits unknown counters and disables coordinate-free visualization modes', () => {
    const empty = review.normalizeProductionTextEvidence(payload({
      available_match_pairs: 0,
      summary: {
        matched_fragments: null, changed_fragments: null,
        changed: null, removed: null, added: null, review_required: null,
      },
      matches: [{
        evidence_id: 'page-only',
        sides: {
          LEFT: [{source: 'TEXT', page: 7, fragment_id: 'l', highlight: null}],
          RIGHT: [{source: 'TEXT', page: 16, fragment_id: 'r', highlight: null}],
        },
      }],
      changes: [],
    }));
    const presentation = review.normalizeProductionTextPresentation(
      {status: 'NOT_STARTED', stages: {}}, empty,
    );

    expect(presentation.counters).toEqual([]);
    expect(presentation.can_visualize_matches).toBe(false);
    expect(presentation.can_visualize_changes).toBe(false);
  });

  it('separates a true TEXT failure from prescribed partial completion wording', () => {
    const failed = review.normalizeProductionTextPresentation({
      status: 'FAILED', stages: {text: {status: 'FAILED'}},
    }, null);
    const partial = review.normalizeProductionTextPresentation({
      status: 'PARTIAL', stages: {text: {status: 'PARTIAL'}},
    }, null);

    expect(failed).toMatchObject({tone: 'warning', label: 'Не завершён', show_rerun: true});
    expect(failed.message).toContain('не удалось завершить');
    expect(partial).toMatchObject({
      tone: 'warning', label: 'Завершён частично', show_rerun: true,
    });
    expect(partial.message).toBe(
      'Текстовый анализ завершён частично. Часть фрагментов требует проверки.',
    );
  });

  it('G/H: deterministic completion is successful without making AI a green-state prerequisite', () => {
    const evidence = review.normalizeProductionTextEvidence(payload());
    const completed = review.normalizeProductionTextPresentation({
      status: 'COMPLETED', stages: {text: {status: 'COMPLETED', review_required: 0}},
    }, evidence);
    expect(completed).toMatchObject({tone: 'completed', label: 'Завершён'});
    expect(completed.message).toBe(
      'Детерминированная проверка завершена. Дополнительная семантическая проверка не применялась.',
    );
    expect(html).not.toContain('ИИ-проверка не выполнена полностью');
    expect(html).not.toContain('Текст проверен детерминированно, ИИ');
  });

  it('renders REVIEW_REQUIRED with a label, icon and patterned overlay rather than color alone', () => {
    const evidence = review.normalizeProductionTextEvidence(payload({
      summary: {matched_fragments: 1, changed_fragments: 2, review_required: 2},
    }));
    const presentation = review.normalizeProductionTextPresentation({
      status: 'COMPLETED', stages: {text: {status: 'COMPLETED', review_required: 2}},
    }, evidence);

    expect(presentation).toMatchObject({tone: 'review', label: 'Требует проверки'});
    expect(presentation.message).toContain('2 фрагмента требуют дополнительной проверки');
    expect(html).toContain("'⚠'");
    expect(css).toContain('.sc-text-evidence-overlay.is-review-required');
    expect(css).toContain('repeating-linear-gradient');
    expect(css).toContain('content: "!"');
  });

  it('labels overlays accessibly and focuses exact anchors in continuous mode', () => {
    expect(html.match(/scTextEvidenceOverlayLabel\(overlay, side\)/g)).toHaveLength(4);
    expect(app).toContain("if (scViewMode.value === 'continuous') await nextTick()");
    expect(app).toContain('scFocusTextEvidenceSide');
    expect(app).toContain('scSetContinuousAnchor(side, page, {');
  });

  it('CHECK_BLOCKED reads as an unfinished analysis, not as a checked zero', () => {
    const blocked = review.normalizeProductionTextPresentation({
      status: 'PARTIAL',
      stages: {text: {
        status: 'CHECK_BLOCKED', source_state: 'CHECK_BLOCKED',
        atoms: 0, deltas: 0, review_required: 0,
        reason_code: 'TEXT_SOURCE_MISSING', error_type: 'FileNotFoundError',
      }},
    }, review.normalizeProductionTextEvidence({
      available: false, text_result_state: 'BLOCKED',
      text_blocked_reason: 'TEXT_SOURCE_MISSING',
      available_match_pairs: null, change_items: null,
      available_change_items: null,
      summary: {
        matched_fragments: null, changed_fragments: null, changed: null,
        removed: null, added: null, review_required: null,
      },
    }));

    expect(blocked.blocked).toBe(true);
    expect(blocked.text_result_state).toBe('BLOCKED');
    expect(blocked.label).toBe('Не завершён');
    expect(blocked.message).toContain('не завершён');
    expect(blocked.message).not.toContain('частично');
    expect(blocked.message).toContain(
      'Не найден один из исходных файлов текстовой подготовки.',
    );
    // Unknown is hidden entirely: no "Доступно пар 0", no zero counters.
    expect(blocked.counters).toEqual([]);
    expect(blocked.review_required).toBe(null);
    expect(blocked.show_rerun).toBe(true);
  });

  it('labels visualization coverage instead of claiming a total match count', () => {
    const presentation = review.normalizeProductionTextPresentation({
      status: 'COMPLETED', stages: {text: {status: 'COMPLETED', review_required: 0}},
    }, review.normalizeProductionTextEvidence(payload({
      available_match_pairs: 123, change_items: 390, available_change_items: 250,
    })));

    expect(presentation.counters).toContainEqual({
      label: 'Можно показать на листах', value: 123, suffix: 'пары совпадений',
    });
    expect(presentation.counters).toContainEqual({
      label: 'Изменения на листах', value: 250, suffix: 'из 390',
    });
    expect(presentation.counters.some(item => item.label === 'Доступно пар')).toBe(false);
    expect(presentation.coverage_notes).toContain(
      'Показаны только совпадения, для которых есть точные координаты на обеих сторонах.',
    );
    expect(presentation.coverage_notes).toContain(
      'На листах отображаются только изменения с доступными точными координатами.',
    );
    expect(html).toContain('scProductionTextPresentation.coverage_notes');
  });

  it('shows the read-only mode caption without a warning banner', () => {
    const presentation = review.normalizeProductionTextPresentation({
      status: 'COMPLETED', stages: {text: {status: 'COMPLETED'}},
    }, review.normalizeProductionTextEvidence(payload()));

    expect(presentation.read_only_note).toBe(
      'Режим просмотра — результаты анализа не изменяются.',
    );
    expect(html).toContain('scProductionTextPresentation.read_only_note');
    expect(css).toContain('.sc-text-evidence-modes__note');
  });

  it('explains why a legacy generation cannot draw its exact pairs', () => {
    const legacy = review.normalizeProductionTextPresentation({
      status: 'COMPLETED', stages: {text: {status: 'COMPLETED', review_required: 0}},
    }, review.normalizeProductionTextEvidence(payload({
      match_evidence_state: 'UNVERIFIED_LEGACY_GENERATION',
      available_match_pairs: null, matches: [],
    })));

    expect(legacy.match_evidence_state).toBe('UNVERIFIED_LEGACY_GENERATION');
    expect(legacy.can_visualize_matches).toBe(false);
    expect(legacy.matches_unavailable_reason).toContain('запустите полный анализ заново');
    expect(legacy.counters.some(item => (
      item.label === 'Можно показать на листах'
    ))).toBe(false);
    expect(html).toContain('scProductionTextPresentation.matches_unavailable_reason');
  });

  it('routes a REVIEW_REQUIRED item to its current review row, never to a confirmed style', () => {
    const normalized = review.normalizeProductionTextEvidence(payload({
      changes: [{
        evidence_id: 'ureview-1', target_id: 'ureview-1',
        target_kind: 'REVIEW_EVIDENCE', review_status: 'REVIEW_REQUIRED',
        title: 'Добавлено: 40 575x400(h)', before: null, after: '40 575x400(h)',
        review_required: true,
        sides: {
          LEFT: [], RIGHT: [location(16, 'right-added', [0.2, 0.4, 0.5, 0.46])],
        },
      }],
    }));
    const overlays = review.productionTextEvidenceOverlays(
      normalized, 'changes', 'right', 16, '',
    );

    expect(overlays[0]).toMatchObject({
      evidence_id: 'ureview-1', target_id: 'ureview-1', review_required: true,
    });
    expect(review.productionTextEvidenceItem(normalized, 'ureview-1').target_kind)
      .toBe('REVIEW_EVIDENCE');
    expect(css).toContain('.sc-text-evidence-overlay.is-review-required');
  });

  it('never renders a clickable change without a current target', () => {
    const normalized = review.normalizeProductionTextEvidence(payload());
    expect(normalized.changes.every(item => item.target_id)).toBe(true);
    // The backend keys viewer identity on the target itself, so a rejected or
    // unselected atom simply has no row here.
    expect(normalized.changes[0].evidence_id).toBe(normalized.changes[0].target_id);
    expect(app).not.toContain('scTextEvidenceFuzzy');
  });
});
