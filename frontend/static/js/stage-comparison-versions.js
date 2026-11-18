(function (root, factory) {
    const api = factory();
    if (typeof module === 'object' && module.exports) module.exports = api;
    root.StageComparisonVersions = api;
}(typeof globalThis !== 'undefined' ? globalThis : this, function () {
    'use strict';
    // Версии сравнения стадий (с 18.11.2026): у проекта несколько независимых
    // сравнений, у каждой своя пара комплектов и свой результат. Здесь — чистые
    // функции раздела; состояние и запросы живут в app.js.
    const LEGACY = 'cv001';
    const VERSION_RE = /^cv\d{3}$/;
    const STAGES = ['stage_1', 'stage_2'];
    const SIDE = {stage_1: 'слева', stage_2: 'справа'};
    const LABEL_MAX = 100;

    function isVersion(value) {
        return typeof value === 'string' && VERSION_RE.test(value) && value !== 'cv000';
    }

    // Объектный маршрут раздела с выбранной версией; без версии сервер берёт Версию 1.
    function withVersion(url, version) {
        if (!isVersion(version)) return url;
        return url + (String(url).includes('?') ? '&' : '?') + 'comparison_version=' + encodeURIComponent(version);
    }

    function number(version) {
        return isVersion(version) ? Number(version.slice(2)) : 0;
    }

    function documentsWord(count) {
        const n = Math.abs(Number(count) || 0) % 100;
        const last = n % 10;
        if (n > 10 && n < 20) return 'документов';
        if (last === 1) return 'документ';
        if (last >= 2 && last <= 4) return 'документа';
        return 'документов';
    }

    function documentsText(count) {
        const n = Number(count) || 0;
        return n + ' ' + documentsWord(n);
    }

    function date(value) {
        const parsed = value ? new Date(value) : null;
        if (!parsed || Number.isNaN(parsed.getTime())) return '';
        const pad = part => String(part).padStart(2, '0');
        return pad(parsed.getDate()) + '.' + pad(parsed.getMonth() + 1) + '.' + parsed.getFullYear();
    }

    // Подпись версии в списке: «Версия 2 · 18.11.2026 · слева 67, справа 54».
    function optionLabel(version) {
        if (!version) return '';
        const stages = version.stages || {};
        const sides = STAGES.map(stage => SIDE[stage] + ' ' + (Number(stages[stage]?.documents) || 0)).join(', ');
        return [version.label, date(version.created_at), sides].filter(Boolean).join(' · ');
    }

    // Комплекты для новой версии: стороны всех версий с документами. Одинаковые
    // комплекты (тот же set_key — копия в другой версии) — одной строкой с
    // источником в самой ранней версии.
    function setOptions(versions) {
        const options = [];
        const byKey = new Map();
        for (const version of versions || []) {
            for (const stage of STAGES) {
                const info = version && version.stages && version.stages[stage];
                if (!info || !info.reusable || !(Number(info.documents) > 0)) continue;
                const key = info.set_key || (version.comparison_version + ':' + stage);
                const known = byKey.get(key);
                if (known) {
                    known.alsoIn.push(version.label);
                    continue;
                }
                const option = {
                    value: version.comparison_version + ':' + stage,
                    comparison_version: version.comparison_version,
                    stage,
                    setKey: key,
                    documents: Number(info.documents),
                    versionLabel: version.label,
                    alsoIn: [],
                };
                byKey.set(key, option);
                options.push(option);
            }
        }
        for (const option of options) {
            option.label = option.versionLabel + ' · ' + SIDE[option.stage] + ' — ' + documentsText(option.documents)
                + (option.alsoIn.length ? ' (также в: ' + option.alsoIn.join(', ') + ')' : '');
        }
        return options;
    }

    function parseOption(value) {
        const [version, stage] = String(value || '').split(':');
        return isVersion(version) && STAGES.includes(stage) ? {comparison_version: version, stage} : null;
    }

    function nextLabel(versions) {
        const top = Math.max(0, ...(versions || []).map(version => number(version && version.comparison_version)));
        return 'Версия ' + (top + 1);
    }

    // Выбранная версия: запомненная, если она есть у объекта, иначе самая новая.
    function pickVersion(versions, remembered) {
        const list = versions || [];
        if (remembered && list.some(version => version.comparison_version === remembered)) return remembered;
        let latest = '';
        for (const version of list) {
            if (number(version.comparison_version) > number(latest)) latest = version.comparison_version;
        }
        return latest;
    }

    // Начальная форма новой версии: слева — комплект слева выбранной версии
    // (типичный случай: та же стадия П, новый комплект РД), справа — загрузка.
    function initialForm(versions, selected) {
        const options = setOptions(versions);
        const current = (versions || []).find(version => version.comparison_version === selected);
        const key = current && current.stages && current.stages.stage_1 && current.stages.stage_1.set_key;
        const left = options.find(option => key && option.setKey === key) || options[0];
        return {
            label: '',
            left: {mode: left ? 'existing' : 'new', set: left ? left.value : ''},
            right: {mode: 'new', set: ''},
        };
    }

    // Тело POST …/comparison-versions и ошибки формы (по-русски, для показа как есть).
    function createPayload(form, options) {
        const errors = [];
        const body = {};
        const label = String((form && form.label) || '').trim();
        if (label.length > LABEL_MAX) errors.push('Название версии длиннее ' + LABEL_MAX + ' символов');
        if (label) body.label = label;
        const keys = {};
        for (const [stage, side] of [['stage_1', form && form.left], ['stage_2', form && form.right]]) {
            if (side && side.mode === 'existing') {
                const parsed = parseOption(side.set);
                if (!parsed) {
                    errors.push('Выберите комплект ' + SIDE[stage]);
                    continue;
                }
                body[stage] = {source: 'existing', comparison_version: parsed.comparison_version, stage: parsed.stage};
                const option = (options || []).find(item => item.value === side.set);
                keys[stage] = option ? option.setKey : side.set;
            } else {
                body[stage] = {source: 'new'};
            }
        }
        if (keys.stage_1 && keys.stage_1 === keys.stage_2) errors.push('Слева и справа выбран один и тот же комплект');
        return {body, errors};
    }

    // Стороны, которые создаются пустыми и ждут загрузки (в порядке слева → справа).
    function newSides(body) {
        return STAGES.filter(stage => body && body[stage] && body[stage].source === 'new');
    }

    return {
        LEGACY, LABEL_MAX, isVersion, withVersion, number, documentsWord, documentsText, date, optionLabel,
        setOptions, parseOption, nextLabel, pickVersion, initialForm, createPayload, newSides,
    };
}));
