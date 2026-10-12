(function (root, factory) {
    const api = factory();
    if (typeof module === 'object' && module.exports) module.exports = api;
    root.StageComparisonBatch = api;
}(typeof globalThis !== 'undefined' ? globalThis : this, function () {
    'use strict';
    // Очередь анализа выбранных пар (шаг «Загрузка документации»): галочки у строк,
    // один выбор режима и модели, сервер анализирует пары по одной.
    const ITEM = {
        QUEUED: {label: 'В очереди', tone: 'muted'},
        RUNNING: {label: 'Идёт анализ', tone: 'running'},
        COMPLETED: {label: 'Анализ готов', tone: 'ok'},
        REVIEW: {label: 'Анализ готов', tone: 'ok'},
        FAILED: {label: 'Ошибка анализа', tone: 'error'},
        CANCELLED: {label: 'Анализ остановлен', tone: 'muted'},
        STOPPED: {label: 'Снято с очереди', tone: 'muted'},
        INTERRUPTED: {label: 'Прервано перезапуском', tone: 'error'},
        SKIPPED: {label: 'Пропущено', tone: 'muted'},
    };
    const DONE = new Set(['COMPLETED', 'REVIEW', 'FAILED', 'CANCELLED', 'STOPPED', 'INTERRUPTED', 'SKIPPED']);

    function key(row) {
        return row && row.left && row.right && row.left.pdf_path && row.right.pdf_path
            ? row.left.pdf_path + '\n' + row.right.pdf_path : '';
    }

    function label(row) {
        if (!key(row)) return '';
        const members = Array.isArray(row.members) ? row.members.length : 0;
        return row.left.filename + ' → ' + (row.group && members ? 'сборка из ' + members : row.right.filename);
    }

    function payload(rows) {
        const seen = new Set();
        return (rows || []).filter(row => {
            const k = key(row);
            if (!k || seen.has(k)) return false;
            seen.add(k);
            return true;
        }).map(row => ({left_pdf: row.left.pdf_path, right_pdf: row.right.pdf_path, label: label(row)}));
    }

    function running(batch) {
        return Boolean(batch && batch.status === 'RUNNING');
    }

    // The queue item of this row, with its 1-based place in the queue.
    function itemFor(batch, row) {
        const k = key(row);
        if (!batch || !k || !Array.isArray(batch.items)) return null;
        const index = batch.items.findIndex(item => item.left_pdf + '\n' + item.right_pdf === k);
        return index < 0 ? null : {...batch.items[index], position: index + 1};
    }

    function badge(item) {
        if (!item) return null;
        const known = ITEM[item.status] || {label: String(item.status || ''), tone: 'muted'};
        const text = item.status === 'QUEUED' ? known.label + ' · №' + item.position : known.label;
        return {label: text, tone: known.tone, title: item.message || ''};
    }

    function summary(batch) {
        if (!batch || !Array.isArray(batch.items) || !batch.items.length) return null;
        const total = batch.items.length;
        const done = batch.items.filter(item => DONE.has(item.status)).length;
        const failed = batch.items.filter(item => ['FAILED', 'INTERRUPTED'].includes(item.status)).length;
        const current = batch.items.find(item => item.status === 'RUNNING');
        let text;
        if (running(batch)) {
            text = 'Очередь анализа: ' + done + ' из ' + total + ' готово'
                + (current ? ' · сейчас: ' + (current.label || 'пара') : '')
                + (batch.stop_requested ? ' · остановится после текущей пары' : '');
        } else {
            const how = {DONE: 'выполнена', STOPPED: 'остановлена', INTERRUPTED: 'прервана перезапуском сервера'}[batch.status]
                || String(batch.status || '').toLowerCase();
            text = 'Очередь анализа ' + how + ': ' + done + ' из ' + total;
        }
        if (failed) text += ' · с ошибкой: ' + failed;
        return {text, running: running(batch), done, total, failed};
    }

    return {ITEM, key, label, payload, running, itemFor, badge, summary};
}));
