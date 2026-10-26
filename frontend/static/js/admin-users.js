/**
 * Страница «Пользователи» (только admin): заявки, роли, связь с сотрудником,
 * права по разделам, блокировка, сброс пароля, завершение сеансов, журнал.
 * Всё решает сервер (/api/admin/*); страница — только форма к API.
 */
(function () {
    'use strict';
    var H = { 'Content-Type': 'application/json', 'X-Requested-With': 'AuditManager' };
    var meta = { sections: [], staff: [], me: null };
    var accounts = [];
    var selectedAccountId = null;

    function el(tag, attrs, children) {
        var node = document.createElement(tag);
        Object.keys(attrs || {}).forEach(function (k) {
            if (k === 'text') node.textContent = attrs[k];
            else if (k === 'onclick' || k === 'onchange') node[k] = attrs[k];
            else node.setAttribute(k, attrs[k]);
        });
        (children || []).forEach(function (c) { if (c) node.appendChild(c); });
        return node;
    }

    function banner(text, kind) {
        var b = document.getElementById('banner');
        b.className = 'banner ' + (kind || 'ok');
        b.textContent = '';
        if (text instanceof Node) b.appendChild(text); else b.textContent = text;
        window.scrollTo(0, 0);
    }

    function api(method, path, body) {
        return fetch('/api/admin' + path, {
            method: method, headers: H, body: body === undefined ? undefined : JSON.stringify(body),
        }).then(function (resp) {
            return resp.json().catch(function () { return {}; }).then(function (data) {
                if (!resp.ok) {
                    var d = data && data.detail;
                    throw new Error((d && d.message) || (typeof d === 'string' ? d : 'Ошибка ' + resp.status));
                }
                return data;
            });
        });
    }

    function act(method, path, body, okText) {
        return api(method, path, body).then(function (data) {
            if (okText) banner(okText, 'ok');
            return reload().then(function () { return data; });
        }).catch(function (e) { banner(e.message, 'err'); });
    }

    var STATUS = { pending: 'ожидает', active: 'действует', rejected: 'отклонена', blocked: 'заблокирован' };

    function statusBadge(s) { return el('span', { 'class': 'status s-' + s, text: STATUS[s] || s }); }

    function sectionsMatrix(acc) {
        var table = el('table', { 'class': 'matrix' });
        meta.sections.forEach(function (sec) {
            var cur = (acc.sections || {})[sec.code] || { view: false, download: false };
            var view = el('input', { type: 'checkbox' });
            var dl = el('input', { type: 'checkbox' });
            view.checked = !!cur.view;
            dl.checked = !!cur.download;
            if (!sec.downloads) {
                dl.disabled = true;
                dl.title = 'В разделе нет выгрузок — право скачивания не назначается';
            }
            view.onchange = function () { if (!view.checked) dl.checked = false; };
            dl.onchange = function () { if (dl.checked) view.checked = true; };
            view.dataset.section = sec.code; dl.dataset.section = sec.code;
            view.dataset.kind = 'view'; dl.dataset.kind = 'download';
            table.appendChild(el('tr', {}, [
                el('td', { text: sec.title }),
                el('td', {}, [el('label', {}, [view, document.createTextNode(' просмотр')])]),
                el('td', {}, [el('label', { title: dl.title || '' }, [dl, document.createTextNode(
                    sec.downloads ? ' скачивание' : ' скачивание (нет выгрузок)')])]),
            ]));
        });
        var save = el('button', { 'class': 'btn primary', text: 'Сохранить разделы', onclick: function () {
            var out = {};
            table.querySelectorAll('input[type=checkbox]').forEach(function (cb) {
                out[cb.dataset.section] = out[cb.dataset.section] || { view: false, download: false };
                out[cb.dataset.section][cb.dataset.kind] = cb.checked;
            });
            act('PUT', '/accounts/' + acc.id + '/sections', { sections: out }, 'Разделы сохранены');
        } });
        return el('div', {}, [table, save]);
    }

    function staffSelect(acc) {
        var sel = el('select', {});
        sel.appendChild(el('option', { value: '', text: '— не связан —' }));
        meta.staff.forEach(function (s) {
            var o = el('option', { value: s.id, text: s.name + ' (' + s.id + ')' });
            if (s.id === acc.staff_id) o.selected = true;
            sel.appendChild(o);
        });
        sel.onchange = function () {
            act('PUT', '/accounts/' + acc.id + '/staff', { staff_id: sel.value || null }, 'Связь с сотрудником сохранена');
        };
        return sel;
    }

    function roleSelect(acc) {
        var sel = el('select', {});
        [['viewer', 'сотрудник'], ['admin', 'администратор']].forEach(function (r) {
            var o = el('option', { value: r[0], text: r[1] });
            if (acc.role === r[0]) o.selected = true;
            sel.appendChild(o);
        });
        sel.onchange = function () {
            if (sel.value === 'admin' && !confirm('Назначить «' + acc.login + '» администратором? Администратор может всё, включая управление пользователями.')) {
                sel.value = acc.role; return;
            }
            act('PUT', '/accounts/' + acc.id + '/role', { role: sel.value }, 'Роль изменена, сеансы пользователя завершены');
        };
        return sel;
    }

    function actions(acc) {
        var out = [];
        var self = acc.id === meta.me;
        if (acc.status === 'pending' || acc.status === 'rejected') {
            out.push(el('button', { 'class': 'btn primary', text: 'Одобрить', onclick: function () {
                act('POST', '/accounts/' + acc.id + '/approve', undefined,
                    'Заявка одобрена. Назначьте разделы на вкладке «Пользователи».');
            } }));
        }
        if (acc.status === 'pending') {
            out.push(el('button', { 'class': 'btn danger', text: 'Отклонить', onclick: function () {
                var reason = prompt('Причина отклонения (необязательно):', '') ;
                if (reason === null) return;
                act('POST', '/accounts/' + acc.id + '/reject', { reason: reason }, 'Заявка отклонена');
            } }));
        }
        if (acc.status === 'active' && !self) {
            out.push(el('button', { 'class': 'btn danger', text: 'Заблокировать', onclick: function () {
                var reason = prompt('Причина блокировки (необязательно):', '');
                if (reason === null) return;
                act('POST', '/accounts/' + acc.id + '/block', { reason: reason }, 'Заблокирован, сеансы завершены');
            } }));
        }
        if (acc.status === 'blocked') {
            out.push(el('button', { 'class': 'btn', text: 'Разблокировать', onclick: function () {
                act('POST', '/accounts/' + acc.id + '/unblock', undefined, 'Разблокирован');
            } }));
        }
        if (acc.status === 'active') {
            out.push(el('button', { 'class': 'btn', text: 'Сбросить пароль', onclick: function () {
                if (!confirm('Выдать «' + acc.login + '» временный пароль? Все его сеансы будут завершены.')) return;
                api('POST', '/accounts/' + acc.id + '/reset-password').then(function (data) {
                    banner(el('div', {}, [
                        document.createTextNode('Временный пароль для ' + acc.login + ' (показывается один раз): '),
                        el('span', { 'class': 'secret', text: data.temporary_password }),
                        document.createTextNode(' — при входе пользователь сразу задаст свой.'),
                    ]), 'ok');
                    return reload();
                }).catch(function (e) { banner(e.message, 'err'); });
            } }));
            out.push(el('button', { 'class': 'btn', text: 'Завершить сеансы', onclick: function () {
                act('POST', '/accounts/' + acc.id + '/revoke-sessions', undefined, 'Сеансы завершены');
            } }));
        }
        if ((acc.status === 'pending' || acc.status === 'rejected') && !self) {
            out.push(el('button', { 'class': 'btn danger', text: 'Удалить заявку', onclick: function () {
                if (!confirm('Удалить заявку «' + acc.login + '»? Логин освободится.')) return;
                act('DELETE', '/accounts/' + acc.id, undefined, 'Заявка удалена');
            } }));
        }
        return el('div', {}, out);
    }

    function renderPending() {
        var root = document.getElementById('tab-pending');
        root.textContent = '';
        var rows = accounts.filter(function (a) { return a.status === 'pending'; });
        document.getElementById('pending-count').textContent = rows.length ? '(' + rows.length + ')' : '';
        if (!rows.length) { root.appendChild(el('p', { 'class': 'muted', text: 'Новых заявок нет.' })); return; }
        var table = el('table', {}, [el('tr', {}, ['Логин', 'ФИО', 'Контакт', 'Комментарий', 'Подана', 'IP', ''].map(function (h) {
            return el('th', { text: h });
        }))]);
        rows.forEach(function (a) {
            table.appendChild(el('tr', {}, [
                el('td', { text: a.login }), el('td', { text: a.display_name }), el('td', { text: a.contact || '—' }),
                el('td', { text: a.note || '—' }), el('td', { text: (a.created_at || '').replace('T', ' ').replace('Z', '') }),
                el('td', { text: a.registration_ip || '—' }), el('td', {}, [actions(a)]),
            ]));
        });
        root.appendChild(table);
    }

    function uploadPermission(acc) {
        var cb = el('input', { type: 'checkbox', 'aria-label': 'Загрузка проектов: ' + acc.login });
        cb.checked = acc.role === 'admin' || !!acc.upload_projects;
        var hasAudit = !!(acc.sections && acc.sections.audit_results && acc.sections.audit_results.view);
        cb.disabled = acc.role === 'admin' || acc.status !== 'active' || !hasAudit;
        var title = acc.role === 'admin' ? 'Администратору загрузка доступна всегда'
            : !hasAudit ? 'Сначала включите просмотр результатов аудита в настройках пользователя'
            : 'Разрешить загрузку проектов и новых версий с компьютера';
        cb.onchange = function () {
            var allowed = cb.checked;
            cb.disabled = true;
            api('PUT', '/accounts/' + acc.id + '/upload-projects', { upload_projects: allowed })
                .then(function () {
                    banner(allowed ? 'Загрузка проектов разрешена: ' + acc.login : 'Загрузка проектов запрещена: ' + acc.login, 'ok');
                    return reload();
                }).catch(function (e) {
                    cb.checked = !allowed;
                    cb.disabled = false;
                    banner(e.message, 'err');
                });
        };
        return el('label', { 'class': 'upload-permission', title: title, onclick: function (event) {
            event.stopPropagation();
        } }, [cb, document.createTextNode(acc.role === 'admin' ? ' всегда' : ' разрешить')]);
    }

    function accountDetails(acc) {
        var detail = el('aside', { 'class': 'account-details', id: 'account-details', 'aria-label': 'Настройки пользователя' });
        if (!acc) {
            detail.appendChild(el('p', { 'class': 'muted', text: 'Нажмите на пользователя, чтобы выбрать разделы и изменить его настройки.' }));
            return detail;
        }
        detail.appendChild(el('h2', { text: acc.login + (acc.id === meta.me ? ' (вы)' : '') }));
        if (acc.display_name !== acc.login) detail.appendChild(el('p', { text: acc.display_name }));
        detail.appendChild(el('p', { 'class': 'muted', text: 'Последний вход: ' + ((acc.last_login_at || '—').replace('T', ' ').replace('Z', ' UTC')) }));
        if (acc.must_change_password) detail.appendChild(el('p', { 'class': 'muted', text: 'Ожидает смены временного пароля' }));
        if (acc.status_reason) detail.appendChild(el('p', { 'class': 'muted', text: acc.status_reason }));
        detail.appendChild(el('label', { 'class': 'detail-field' }, [el('span', { text: 'Роль' }), roleSelect(acc)]));
        detail.appendChild(el('label', { 'class': 'detail-field' }, [el('span', { text: 'Сотрудник (авторство решений)' }), staffSelect(acc)]));
        detail.appendChild(el('h3', { text: 'Доступные разделы' }));
        detail.appendChild(acc.role === 'admin'
            ? el('p', { 'class': 'muted', text: 'Администратору доступны все разделы и загрузка проектов.' })
            : sectionsMatrix(acc));
        detail.appendChild(el('h3', { text: 'Действия' }));
        detail.appendChild(actions(acc));
        return detail;
    }

    function renderAccounts() {
        var root = document.getElementById('tab-accounts');
        root.textContent = '';
        var table = el('table', { 'class': 'accounts-list' }, [el('thead', {}, [el('tr', {}, ['Пользователь', 'Статус', 'Роль', 'Разделы', 'Загрузка проектов'].map(function (h) {
            return el('th', { text: h });
        }))])]);
        var body = el('tbody', {});
        accounts.filter(function (a) { return a.status !== 'pending'; }).forEach(function (a) {
            function select() {
                selectedAccountId = a.id;
                renderAccounts();
            }
            var who = el('div', {}, [
                el('button', { 'class': 'account-select', text: a.login + (a.id === meta.me ? ' (вы)' : ''),
                    'aria-controls': 'account-details', 'aria-expanded': String(selectedAccountId === a.id),
                    onclick: function (event) { event.stopPropagation(); select(); } }),
                a.display_name !== a.login ? el('div', { 'class': 'muted', text: a.display_name }) : null,
            ]);
            var count = meta.sections.filter(function (sec) { return a.sections && a.sections[sec.code] && a.sections[sec.code].view; }).length;
            body.appendChild(el('tr', { 'class': selectedAccountId === a.id ? 'selected' : '', onclick: select }, [
                el('td', {}, [who]), el('td', {}, [statusBadge(a.status)]),
                el('td', { text: a.role === 'admin' ? 'администратор' : 'сотрудник' }),
                el('td', { text: a.role === 'admin' ? 'все' : count + ' из ' + meta.sections.length }),
                el('td', {}, [uploadPermission(a)]),
            ]));
        });
        table.appendChild(body);
        var selected = accounts.find(function (a) { return a.id === selectedAccountId && a.status !== 'pending'; });
        root.appendChild(el('div', { 'class': 'accounts-layout' }, [
            el('div', { 'class': 'accounts-table-wrap' }, [table]), accountDetails(selected),
        ]));
    }

    function renderEvents() {
        return api('GET', '/auth-events?limit=300').then(function (data) {
            var root = document.getElementById('tab-events');
            root.textContent = '';
            var byId = {};
            accounts.forEach(function (a) { byId[a.id] = a.login; });
            var table = el('table', {}, [el('tr', {}, ['Время (UTC)', 'Событие', 'Кто', 'Над кем', 'Подробности'].map(function (h) {
                return el('th', { text: h });
            }))]);
            (data.events || []).forEach(function (e) {
                table.appendChild(el('tr', {}, [
                    el('td', { text: (e.at || '').replace('T', ' ').replace('Z', '') }), el('td', { text: e.event }),
                    el('td', { text: byId[e.actor_account_id] || (e.actor_account_id == null ? 'CLI' : '#' + e.actor_account_id) }),
                    el('td', { text: byId[e.target_account_id] || ('#' + e.target_account_id) }),
                    el('td', { 'class': 'muted', text: JSON.stringify(e.details || {}) }),
                ]));
            });
            root.appendChild(table);
        });
    }

    function reload() {
        return api('GET', '/accounts').then(function (data) {
            accounts = data.accounts || [];
            renderPending();
            renderAccounts();
            if (document.getElementById('tab-events').style.display !== 'none') return renderEvents();
        });
    }

    document.querySelectorAll('.tabs button').forEach(function (btn) {
        btn.onclick = function () {
            document.querySelectorAll('.tabs button').forEach(function (b) { b.classList.remove('active'); });
            btn.classList.add('active');
            ['pending', 'accounts', 'events'].forEach(function (t) {
                document.getElementById('tab-' + t).style.display = t === btn.dataset.tab ? '' : 'none';
            });
            if (btn.dataset.tab === 'events') renderEvents().catch(function (e) { banner(e.message, 'err'); });
        };
    });

    api('GET', '/accounts/meta').then(function (data) {
        meta = data;
        return fetch('/api/auth/me').then(function (r) { return r.json(); }).then(function (me) {
            document.getElementById('me').textContent = me && me.username ? 'вы вошли как ' + me.username : '';
        });
    }).then(reload).catch(function (e) { banner(e.message, 'err'); });
})();
