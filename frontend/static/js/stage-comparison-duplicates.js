(function (root, factory) {
    const api = factory();
    if (typeof module === 'object' && module.exports) module.exports = api;
    root.StageComparisonDuplicates = api;
}(typeof globalThis !== 'undefined' ? globalThis : this, function () {
    'use strict';
    // Одинаковые листы в сборке группы (22.11.2026). Перед склейкой сервер
    // находит копии листов, инженер пролистывает пары в окне, отмечает
    // «Вернуть» те, что не устраивают, остальные копии удаляются из сборки.
    // Здесь — чистые функции окна; состояние и запросы живут в app.js.

    function plural(count, one, few, many) {
        const n = Math.abs(Number(count) || 0) % 100;
        const last = n % 10;
        if (n > 10 && n < 20) return many;
        if (last === 1) return one;
        if (last >= 2 && last <= 4) return few;
        return many;
    }

    function pairsText(count) {
        const n = Number(count) || 0;
        return n + ' ' + plural(n, 'пара', 'пары', 'пар');
    }

    function pagesText(count) {
        const n = Number(count) || 0;
        return n + ' ' + plural(n, 'лист', 'листа', 'листов');
    }

    // Подпись листа пары: «ЭМ-02 · Sheet 4 — Узлы · стр. 55». Подпись «Page N»
    // распознавание ставит листам без штампа — она повторяет номер страницы.
    function sideLabel(ref) {
        if (!ref) return '';
        const sheet = String(ref.sheet || '').trim();
        return [
            ref.document_code,
            sheet && !/^page\s+\d+$/i.test(sheet) ? sheet : '',
            'стр. ' + ref.source_page,
        ].filter(Boolean).join(' · ');
    }

    // Плашка дублей стороны группы рядом со статусом строки (null — не показывать).
    // Статус строки уже говорит «Ищем одинаковые листы» / «Проверьте одинаковые
    // листы», поэтому плашка добавляет только число: прогресс или пары.
    function sideBadge(view) {
        const dup = view && view.duplicates;
        if (!view || !dup) return null;
        if (view.status === 'SCANNING') {
            const total = Number(dup.total_pages) || 0;
            const done = Math.min(Number(dup.done_pages) || 0, total);
            return total ? {tone: 'running', label: `${done} из ${total} стр.`} : null;
        }
        if (view.status === 'REVIEW') {
            return {tone: 'review', label: pairsText(dup.pairs), review: true};
        }
        if (view.status === 'READY' && Number(dup.pairs) > 0) {
            const kept = Number(dup.kept_pairs) || 0;
            return {
                tone: 'done',
                label: 'Удалено дублей: ' + (Number(dup.removed_pages) || 0) + (kept ? `, возвращено: ${kept}` : ''),
                review: true,
            };
        }
        return null;
    }

    // Пары, которые инженер уже вернул раньше (решение запомнено) — отмечены сразу.
    function initialKeep(pairs) {
        return (pairs || []).filter(pair => pair.decision === 'keep').map(pair => pair.pair_id);
    }

    function counts(pairs, keepIds) {
        const keep = new Set(keepIds || []);
        const total = (pairs || []).length;
        const kept = (pairs || []).filter(pair => keep.has(pair.pair_id)).length;
        return {total, keep: kept, remove: total - kept};
    }

    function clampIndex(index, total) {
        if (!total) return 0;
        return Math.max(0, Math.min(total - 1, Number(index) || 0));
    }

    // Зона, которая при сравнении не учитывалась (доли страницы) → CSS поверх картинки.
    function zoneStyle(zone) {
        const [x0, y0, x1, y1] = (zone || []).map(Number);
        const pct = value => (Math.max(0, Math.min(1, value)) * 100).toFixed(3) + '%';
        return {left: pct(x0), top: pct(y0), width: pct(x1 - x0), height: pct(y1 - y0)};
    }

    // Картинка листа: base — адрес «…/document-groups/<id>/duplicates».
    function pageUrl(base, side, ref, width) {
        const params = new URLSearchParams({
            side: side || 'right',
            source_index: String(ref.source_index),
            page: String(ref.source_page),
            width: String(width || 1400),
        });
        return base + '/page?' + params.toString();
    }

    // Клавиши окна: ← → (или ↑ ↓) — пары, пробел — «Вернуть», Esc — закрыть.
    function keyAction(key) {
        if (key === 'ArrowRight' || key === 'ArrowDown' || key === 'PageDown') return 'next';
        if (key === 'ArrowLeft' || key === 'ArrowUp' || key === 'PageUp') return 'prev';
        if (key === ' ' || key === 'Spacebar') return 'toggle';
        if (key === 'Escape') return 'close';
        return null;
    }

    return {pairsText, pagesText, sideLabel, sideBadge, initialKeep, counts, clampIndex, zoneStyle, pageUrl, keyAction};
}));
