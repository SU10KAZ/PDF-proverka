(function (root, factory) {
    const api = factory();
    if (typeof module === 'object' && module.exports) module.exports = api;
    root.StageComparisonBatch = api;
}(typeof globalThis !== 'undefined' ? globalThis : this, function () {
    'use strict';
    // Очереди анализа выбранных пар (шаг «Загрузка документации»): галочки у строк,
    // один выбор режима и модели на очередь. У сессии две независимые очереди —
    // «дорожки» Codex и Claude: они идут параллельно, внутри дорожки пары по одной.
    // Функции принимают и одну очередь, и список очередей (ответ API ``batches``).
    const LANE_LABELS = {codex: 'Codex', claude: 'Claude'};
    const ITEM = {
        QUEUED: {label: 'В очереди', tone: 'muted'},
        RUNNING: {label: 'Идёт анализ', tone: 'running'},
        COMPLETED: {label: 'Анализ готов', tone: 'ok'},
        REVIEW: {label: 'Анализ готов', tone: 'ok'},
        PAUSED: {label: 'На паузе · прогресс сохранён', tone: 'muted'},
        FAILED: {label: 'Ошибка анализа', tone: 'error'},
        CANCELLED: {label: 'Анализ остановлен', tone: 'muted'},
        STOPPED: {label: 'Снято с очереди', tone: 'muted'},
        INTERRUPTED: {label: 'Прервано перезапуском', tone: 'error'},
        SKIPPED: {label: 'Пропущено', tone: 'muted'},
    };
    const DONE = new Set(['PAUSED', 'COMPLETED', 'REVIEW', 'FAILED', 'CANCELLED', 'STOPPED', 'INTERRUPTED', 'SKIPPED']);

    function key(row) {
        return row && row.left && row.right && row.left.pdf_path && row.right.pdf_path
            ? row.left.pdf_path + '\n' + row.right.pdf_path : '';
    }

    // Сторона строки: склеенная группа — «сборка из N», иначе имя файла.
    function sideLabel(row, document, members) {
        const count = Array.isArray(members) ? members.length : 0;
        return row.group && count ? 'сборка из ' + count : document.filename;
    }

    function label(row) {
        if (!key(row)) return '';
        return sideLabel(row, row.left, row.leftMembers) + ' → ' + sideLabel(row, row.right, row.members);
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

    function list(batches) {
        if (Array.isArray(batches)) return batches.filter(Boolean);
        return batches ? [batches] : [];
    }

    function isRunning(batch) {
        return Boolean(batch && batch.status === 'RUNNING');
    }

    function running(batches) {
        return list(batches).some(isRunning);
    }

    function laneLabel(lane) {
        return LANE_LABELS[lane] || String(lane || '');
    }

    // Дорожки, в которых сейчас идёт очередь: в них новую очередь не поставить.
    function busyLanes(batches) {
        return new Set(list(batches).filter(isRunning).map(batch => batch.lane).filter(Boolean));
    }

    // Пары, которые стоят или анализируются в идущих очередях (ключи строк).
    function busyKeys(batches) {
        const keys = new Set();
        list(batches).filter(isRunning).forEach(batch => (batch.items || []).forEach(item => {
            if (item.status === 'QUEUED' || item.status === 'RUNNING') keys.add(item.left_pdf + '\n' + item.right_pdf);
        }));
        return keys;
    }

    // The queue item of this row, with its 1-based place in the queue; идущие
    // очереди важнее завершённых, среди равных — более поздняя.
    function itemFor(batches, row) {
        const k = key(row);
        if (!k) return null;
        const ordered = list(batches).slice().sort((a, b) => (isRunning(b) - isRunning(a))
            || String(b.created_at || '').localeCompare(String(a.created_at || '')));
        for (const batch of ordered) {
            if (!Array.isArray(batch.items)) continue;
            const index = batch.items.findIndex(item => item.left_pdf + '\n' + item.right_pdf === k);
            if (index >= 0) return {...batch.items[index], position: index + 1, lane: batch.lane};
        }
        return null;
    }

    function badge(item) {
        if (!item) return null;
        const known = ITEM[item.status] || {label: String(item.status || ''), tone: 'muted'};
        const lane = item.lane ? ' ' + laneLabel(item.lane) : '';
        let text = known.label;
        if (item.status === 'QUEUED') text = known.label + lane + ' · №' + item.position;
        else if (item.status === 'RUNNING') text = known.label + (lane ? ' ·' + lane : '');
        if (['COMPLETED', 'REVIEW'].includes(item.status)) {
            const finished = item.completed_at || item.finished_at;
            const date = finished ? new Date(finished) : null;
            text = 'Анализ выполнен' + (date && Number.isFinite(date.getTime())
                ? ' · ' + date.toLocaleDateString('ru-RU') : '');
        }
        return {label: text, tone: known.tone, title: item.message || ''};
    }

    function summary(batch) {
        if (!batch || !Array.isArray(batch.items) || !batch.items.length) return null;
        const total = batch.items.length;
        const done = batch.items.filter(item => DONE.has(item.status)).length;
        const failed = batch.items.filter(item => ['FAILED', 'INTERRUPTED'].includes(item.status)).length;
        const current = batch.items.find(item => item.status === 'RUNNING');
        const name = 'Очередь анализа' + (batch.lane ? ' ' + laneLabel(batch.lane) : '');
        let text;
        if (isRunning(batch)) {
            text = name + ': ' + done + ' из ' + total + ' готово'
                + (current ? ' · сейчас: ' + (current.label || 'пара') : '')
                + (batch.stop_requested ? ' · остановится после текущей пары' : '');
        } else {
            const how = {DONE: 'выполнена', STOPPED: 'остановлена', INTERRUPTED: 'прервана перезапуском сервера'}[batch.status]
                || String(batch.status || '').toLowerCase();
            text = name + ' ' + how + ': ' + done + ' из ' + total;
        }
        if (failed) text += ' · с ошибкой: ' + failed;
        return {text, running: isRunning(batch), done, total, failed, lane: batch.lane || ''};
    }

    // Сводки всех очередей: идущие первыми.
    function summaries(batches) {
        return list(batches).map(summary).filter(Boolean)
            .sort((a, b) => Number(b.running) - Number(a.running));
    }

    return {ITEM, LANE_LABELS, key, label, payload, list, running, laneLabel, busyLanes, busyKeys,
            itemFor, badge, summary, summaries};
}));
