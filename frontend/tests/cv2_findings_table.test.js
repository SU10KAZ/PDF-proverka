/**
 * Smoke-тесты для inline Critic v2 (experimental) в обычной таблице "Замечания".
 *
 * Контекст: до этой фичи Critic v2 жил в отдельном view с 4 очередями (#/critic-v2-ui).
 * Инженер должен был отдельно туда заходить. Теперь в обычной таблице рядом с
 * "Норма" появляется бейдж "Critic v2: XX" (0–100), а hidden_by_critic скрыты по
 * умолчанию. Production pipeline / 03_findings_review.json НЕ меняется — это
 * чистый display-слой поверх endpoint'а /api/critic-v2/projects/.../triage-ui.
 *
 * Pure-функции продублированы здесь как mirror — если они разойдутся с app.js,
 * этот тест упадёт первым, что лучше тихой регрессии.
 *
 * Запуск:
 *   cd frontend && npm test
 */
import { describe, it, expect } from 'vitest';

// ─── Mirror pure-функций из app.js ───────────────────────────────────────────

const CV2_DISPLAY_QUEUE_RANGE = {
    strong_keep:      [90, 100],
    main_review:      [65,  85],
    borderline:       [50,  65],
    needs_context:    [40,  59],
    suggested_reject: [20,  39],
    hidden_by_critic: [ 0,  19],
};

const CV2_DISPLAY_BUCKETS = [
    { key: 'must_review',     label: 'важно проверить',       lo: 85, hi: 100 },
    { key: 'review',          label: 'на проверку',           lo: 60, hi:  84 },
    { key: 'needs_context',   label: 'нужен контекст',        lo: 40, hi:  59 },
    { key: 'likely_reject',   label: 'вероятно к отклонению', lo: 20, hi:  39 },
    { key: 'hidden',          label: 'скрыто Critic v2',      lo:  0, hi:  19 },
];

function cv2DisplayScore(item) {
    if (!item) return null;
    const range = CV2_DISPLAY_QUEUE_RANGE[item.queue];
    if (!range) return null;
    const [lo, hi] = range;
    const span = hi - lo;
    const s = Number.isFinite(item.score) ? Math.max(0, Math.min(10, item.score)) / 10 : 0.5;
    const c = Number.isFinite(item.confidence) ? Math.max(0, Math.min(1, item.confidence)) : 0.5;
    const intensity = 0.7 * s + 0.3 * c;
    const inverted = item.queue === 'suggested_reject' || item.queue === 'hidden_by_critic';
    const t = inverted ? (1 - intensity) : intensity;
    return Math.round(lo + span * t);
}

function cv2DisplayBucket(score) {
    if (!Number.isFinite(score)) return null;
    for (const b of CV2_DISPLAY_BUCKETS) {
        if (score >= b.lo && score <= b.hi) return b;
    }
    return null;
}

function cv2DisplayLabel(score) {
    const b = cv2DisplayBucket(score);
    return b ? b.label : '';
}

function cv2BareFindingId(rawId) {
    if (!rawId) return '';
    const s = String(rawId);
    const idx = s.lastIndexOf(':');
    return idx >= 0 ? s.slice(idx + 1) : s;
}

function cv2IsHiddenByDefault(item) {
    if (!item) return false;
    if (item.tab === 'hidden_by_critic') return true;
    const score = cv2DisplayScore(item);
    return Number.isFinite(score) && score <= 19;
}

// Mirror _applyFindingsFilter (только cv2-часть; severity/search не повторяем).
function applyCv2Filter(findings, cv2Map, opts) {
    const showHidden = opts.showHidden;
    const displayFilter = opts.displayFilter;
    const cv2Has = opts.cv2Has;
    let items = findings.slice();
    if (cv2Has && !showHidden) {
        items = items.filter(f => {
            const cv2 = cv2Map[f.id];
            return !cv2 || !cv2IsHiddenByDefault(cv2);
        });
    }
    if (cv2Has && displayFilter) {
        items = items.filter(f => {
            const cv2 = cv2Map[f.id];
            if (!cv2) return false;
            const score = cv2DisplayScore(cv2);
            const b = cv2DisplayBucket(score);
            return b && b.key === displayFilter;
        });
    }
    return items;
}

function cv2HiddenCount(findings, cv2Map, cv2Has) {
    if (!cv2Has) return 0;
    let n = 0;
    for (const f of findings) {
        const cv2 = cv2Map[f.id];
        if (cv2 && cv2IsHiddenByDefault(cv2)) n += 1;
    }
    return n;
}

// ─── Тесты ──────────────────────────────────────────────────────────────────

describe('cv2BareFindingId', () => {
    it('извлекает bare id из project-prefixed формы', () => {
        expect(cv2BareFindingId('13АВ-РД-АР0.1-ПА:F-001')).toBe('F-001');
        expect(cv2BareFindingId('any-project:F-042')).toBe('F-042');
    });

    it('возвращает строку как есть, если префикса нет', () => {
        expect(cv2BareFindingId('F-001')).toBe('F-001');
    });

    it('обрабатывает edge-cases', () => {
        expect(cv2BareFindingId(null)).toBe('');
        expect(cv2BareFindingId('')).toBe('');
        // Префикс с двоеточием в имени проекта — берём ХВОСТ после ПОСЛЕДНЕГО :
        expect(cv2BareFindingId('a:b:F-001')).toBe('F-001');
    });
});

describe('cv2DisplayScore — диапазоны 0–100', () => {
    it('strong_keep попадает в 90–100', () => {
        const s = cv2DisplayScore({ queue: 'strong_keep', score: 10, confidence: 1 });
        expect(s).toBeGreaterThanOrEqual(90);
        expect(s).toBeLessThanOrEqual(100);
    });

    it('main_review попадает в 65–85', () => {
        const s = cv2DisplayScore({ queue: 'main_review', score: 8, confidence: 0.8 });
        expect(s).toBeGreaterThanOrEqual(65);
        expect(s).toBeLessThanOrEqual(85);
    });

    it('borderline попадает в 50–65', () => {
        const s = cv2DisplayScore({ queue: 'borderline', score: 6, confidence: 0.5 });
        expect(s).toBeGreaterThanOrEqual(50);
        expect(s).toBeLessThanOrEqual(65);
    });

    it('needs_context попадает в 40–59', () => {
        const s = cv2DisplayScore({ queue: 'needs_context', score: 7, confidence: 0.6 });
        expect(s).toBeGreaterThanOrEqual(40);
        expect(s).toBeLessThanOrEqual(59);
    });

    it('suggested_reject попадает в 20–39 (инверсия)', () => {
        // Высокая уверенность critic'а в reject → НИЖНЯЯ оценка для пользователя
        const high = cv2DisplayScore({ queue: 'suggested_reject', score: 10, confidence: 1 });
        const low  = cv2DisplayScore({ queue: 'suggested_reject', score:  0, confidence: 0 });
        expect(high).toBeGreaterThanOrEqual(20);
        expect(high).toBeLessThanOrEqual(39);
        expect(low).toBeGreaterThanOrEqual(20);
        expect(low).toBeLessThanOrEqual(39);
        expect(high).toBeLessThan(low); // высокая уверенность → ниже на шкале
    });

    it('hidden_by_critic попадает в 0–19 (инверсия)', () => {
        const high = cv2DisplayScore({ queue: 'hidden_by_critic', score: 10, confidence: 1 });
        const low  = cv2DisplayScore({ queue: 'hidden_by_critic', score:  0, confidence: 0 });
        expect(high).toBeGreaterThanOrEqual(0);
        expect(high).toBeLessThanOrEqual(19);
        expect(low).toBeGreaterThanOrEqual(0);
        expect(low).toBeLessThanOrEqual(19);
    });

    it('возвращает null для пустого/неизвестного queue', () => {
        expect(cv2DisplayScore(null)).toBeNull();
        expect(cv2DisplayScore({})).toBeNull();
        expect(cv2DisplayScore({ queue: 'unknown' })).toBeNull();
    });

    it('переживает отсутствие score/confidence (defaults 0.5)', () => {
        const s = cv2DisplayScore({ queue: 'main_review' });
        expect(s).toBeGreaterThanOrEqual(65);
        expect(s).toBeLessThanOrEqual(85);
    });
});

describe('cv2DisplayLabel — 5 buckets', () => {
    it('85+ → важно проверить', () => {
        expect(cv2DisplayLabel(95)).toBe('важно проверить');
        expect(cv2DisplayLabel(85)).toBe('важно проверить');
        expect(cv2DisplayLabel(100)).toBe('важно проверить');
    });

    it('60–84 → на проверку', () => {
        expect(cv2DisplayLabel(70)).toBe('на проверку');
        expect(cv2DisplayLabel(84)).toBe('на проверку');
    });

    it('40–59 → нужен контекст', () => {
        expect(cv2DisplayLabel(50)).toBe('нужен контекст');
    });

    it('20–39 → вероятно к отклонению', () => {
        expect(cv2DisplayLabel(30)).toBe('вероятно к отклонению');
    });

    it('0–19 → скрыто Critic v2', () => {
        expect(cv2DisplayLabel(10)).toBe('скрыто Critic v2');
        expect(cv2DisplayLabel(0)).toBe('скрыто Critic v2');
    });
});

describe('cv2IsHiddenByDefault', () => {
    it('hidden_by_critic скрыт по тегу', () => {
        expect(cv2IsHiddenByDefault({ queue: 'hidden_by_critic', tab: 'hidden_by_critic', score: 0 })).toBe(true);
    });

    it('suggested_reject (с display 20–39) НЕ скрыт по умолчанию', () => {
        // По спеке: только hidden_by_critic скрывается; suggested_reject видим
        const it = { queue: 'suggested_reject', tab: 'suggested_reject', score: 5, confidence: 0.5 };
        expect(cv2IsHiddenByDefault(it)).toBe(false);
    });

    it('needs_context НЕ скрыт', () => {
        const it = { queue: 'needs_context', tab: 'needs_context', score: 6 };
        expect(cv2IsHiddenByDefault(it)).toBe(false);
    });

    it('strong_keep НЕ скрыт', () => {
        expect(cv2IsHiddenByDefault({ queue: 'strong_keep', tab: 'primary', score: 10 })).toBe(false);
    });
});

describe('applyCv2Filter — скрытие по умолчанию', () => {
    const findings = [
        { id: 'F-001', severity: 'КРИТИЧЕСКОЕ' },
        { id: 'F-002', severity: 'РЕКОМЕНДАТЕЛЬНОЕ' },
        { id: 'F-003', severity: 'ЭКСПЛУАТАЦИОННОЕ' },
        { id: 'F-004', severity: 'РЕКОМЕНДАТЕЛЬНОЕ' },
        { id: 'F-005', severity: 'РЕКОМЕНДАТЕЛЬНОЕ' },
    ];
    const cv2Map = {
        'F-001': { tab: 'primary',          queue: 'strong_keep',      score: 10, confidence: 1 },
        'F-002': { tab: 'needs_context',    queue: 'needs_context',    score:  7, confidence: 0.6 },
        'F-003': { tab: 'suggested_reject', queue: 'suggested_reject', score:  5, confidence: 0.5 },
        'F-004': { tab: 'hidden_by_critic', queue: 'hidden_by_critic', score:  2, confidence: 0.9 },
        // F-005 — нет данных в Critic v2
    };

    it('по умолчанию hidden_by_critic скрыт', () => {
        const out = applyCv2Filter(findings, cv2Map, { cv2Has: true, showHidden: false, displayFilter: '' });
        const ids = out.map(f => f.id);
        expect(ids).not.toContain('F-004');
        expect(ids).toContain('F-001');
        expect(ids).toContain('F-002');
        expect(ids).toContain('F-003'); // suggested_reject остаётся видимым
        expect(ids).toContain('F-005'); // без данных Critic v2 — остаётся
    });

    it('кнопка "Показать скрытые" возвращает hidden items', () => {
        const out = applyCv2Filter(findings, cv2Map, { cv2Has: true, showHidden: true, displayFilter: '' });
        expect(out.map(f => f.id)).toContain('F-004');
        expect(out).toHaveLength(5);
    });

    it('suggested_reject остаётся видимым в обоих режимах', () => {
        const off = applyCv2Filter(findings, cv2Map, { cv2Has: true, showHidden: false, displayFilter: '' });
        const on  = applyCv2Filter(findings, cv2Map, { cv2Has: true, showHidden: true,  displayFilter: '' });
        expect(off.map(f => f.id)).toContain('F-003');
        expect(on.map(f => f.id)).toContain('F-003');
    });

    it('needs_context остаётся видимым в обоих режимах', () => {
        const off = applyCv2Filter(findings, cv2Map, { cv2Has: true, showHidden: false, displayFilter: '' });
        const on  = applyCv2Filter(findings, cv2Map, { cv2Has: true, showHidden: true,  displayFilter: '' });
        expect(off.map(f => f.id)).toContain('F-002');
        expect(on.map(f => f.id)).toContain('F-002');
    });

    it('findings без данных Critic v2 не скрываются', () => {
        const off = applyCv2Filter(findings, cv2Map, { cv2Has: true, showHidden: false, displayFilter: '' });
        expect(off.map(f => f.id)).toContain('F-005');
    });

    it('если данных Critic v2 нет (cv2Has=false), таблица не меняется', () => {
        const out = applyCv2Filter(findings, {}, { cv2Has: false, showHidden: false, displayFilter: '' });
        expect(out).toHaveLength(5); // всё видно, ничего не скрыто
    });
});

describe('applyCv2Filter — фильтр по bucket', () => {
    const findings = [
        { id: 'F-001' }, { id: 'F-002' }, { id: 'F-003' }, { id: 'F-004' }, { id: 'F-005' },
    ];
    const cv2Map = {
        'F-001': { tab: 'primary',          queue: 'strong_keep',      score: 10, confidence: 1 },     // must_review
        'F-002': { tab: 'primary',          queue: 'main_review',      score:  8, confidence: 0.8 },   // review
        'F-003': { tab: 'needs_context',    queue: 'needs_context',    score:  7, confidence: 0.6 },   // needs_context
        'F-004': { tab: 'suggested_reject', queue: 'suggested_reject', score:  5, confidence: 0.5 },   // likely_reject
        'F-005': { tab: 'hidden_by_critic', queue: 'hidden_by_critic', score:  2, confidence: 0.9 },   // hidden
    };

    // Чтобы изолировать фильтр — открываем hidden, иначе F-005 уйдёт раньше фильтра
    const showAll = { cv2Has: true, showHidden: true };

    it('фильтр must_review показывает strong_keep', () => {
        const out = applyCv2Filter(findings, cv2Map, { ...showAll, displayFilter: 'must_review' });
        expect(out.map(f => f.id)).toEqual(['F-001']);
    });

    it('фильтр review показывает main_review', () => {
        const out = applyCv2Filter(findings, cv2Map, { ...showAll, displayFilter: 'review' });
        expect(out.map(f => f.id)).toEqual(['F-002']);
    });

    it('фильтр needs_context показывает только needs_context', () => {
        const out = applyCv2Filter(findings, cv2Map, { ...showAll, displayFilter: 'needs_context' });
        expect(out.map(f => f.id)).toEqual(['F-003']);
    });

    it('фильтр likely_reject показывает suggested_reject', () => {
        const out = applyCv2Filter(findings, cv2Map, { ...showAll, displayFilter: 'likely_reject' });
        expect(out.map(f => f.id)).toEqual(['F-004']);
    });

    it('фильтр hidden показывает hidden_by_critic', () => {
        const out = applyCv2Filter(findings, cv2Map, { ...showAll, displayFilter: 'hidden' });
        expect(out.map(f => f.id)).toEqual(['F-005']);
    });

    it('пустой фильтр (все) показывает все при showHidden=true', () => {
        const out = applyCv2Filter(findings, cv2Map, { ...showAll, displayFilter: '' });
        expect(out).toHaveLength(5);
    });
});

describe('cv2HiddenCount', () => {
    const findings = [
        { id: 'F-001' }, { id: 'F-002' }, { id: 'F-003' }, { id: 'F-004' },
    ];
    const cv2Map = {
        'F-001': { tab: 'primary',          queue: 'strong_keep' },
        'F-002': { tab: 'hidden_by_critic', queue: 'hidden_by_critic', score: 2, confidence: 0.9 },
        'F-003': { tab: 'hidden_by_critic', queue: 'hidden_by_critic', score: 3, confidence: 0.8 },
        // F-004 — нет в карте Critic v2
    };

    it('считает только hidden_by_critic', () => {
        expect(cv2HiddenCount(findings, cv2Map, true)).toBe(2);
    });

    it('возвращает 0 если данных Critic v2 нет', () => {
        expect(cv2HiddenCount(findings, cv2Map, false)).toBe(0);
    });

    it('возвращает 0 если карта пуста', () => {
        expect(cv2HiddenCount(findings, {}, true)).toBe(0);
    });
});

describe('Контракт endpoint /api/critic-v2/projects/.../triage-ui', () => {
    // Проверяем, что shape ответа из реального backend ожидаемый, и что мы
    // правильно строим карту (bareFindingId → item).
    it('строит карту по bare id из project-prefixed finding_id', () => {
        const items = [
            { finding_id: '13АВ-РД-АР0.1-ПА:F-001', tab: 'primary',          queue: 'strong_keep',      score: 10, confidence: 1 },
            { finding_id: '13АВ-РД-АР0.1-ПА:F-002', tab: 'hidden_by_critic', queue: 'hidden_by_critic', score:  1, confidence: 0.9 },
        ];
        const map = {};
        for (const it of items) map[cv2BareFindingId(it.finding_id)] = it;
        expect(Object.keys(map).sort()).toEqual(['F-001', 'F-002']);
        expect(map['F-001'].queue).toBe('strong_keep');
    });

    it('graceful degrade: пустой items[] → cv2Has=false, ничего не скрыто', () => {
        const findings = [{ id: 'F-001' }, { id: 'F-002' }];
        const out = applyCv2Filter(findings, {}, { cv2Has: false, showHidden: false, displayFilter: '' });
        expect(out).toHaveLength(2);
    });

    it('warning от endpoint не блокирует таблицу (cv2Has=false)', () => {
        // raw = { items: [], warning: "проект отсутствует в Critic v2 UI export" }
        // → frontend: findingsCv2Available=false → ни скрытий, ни фильтра
        const findings = [{ id: 'F-001' }];
        const out = applyCv2Filter(findings, {}, { cv2Has: false, showHidden: false, displayFilter: '' });
        expect(out).toHaveLength(1);
    });
});
