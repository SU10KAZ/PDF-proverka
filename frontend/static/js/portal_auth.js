/**
 * Portal auth glue для SPA.
 *
 * 1. Возможности вошедшего — window.__portalCaps (подставляет сервер в
 *    index.html) и помощники window.portalCan.{operate,admin,view,download}.
 *    Это подсказка интерфейсу: защиту обеспечивает сервер (access_policy).
 * 2. Перехват fetch и XMLHttpRequest: каждый запрос к /api/ получает заголовок
 *    X-Requested-With: AuditManager (сервер требует его у изменяющих запросов).
 * 3. Отказы: 401 → /login (с причиной: сеанс истёк/завершён); 403
 *    password_change_required → /change-password; 403 forbidden_section /
 *    admin_only → событие 'portal-forbidden' (интерфейс показывает сообщение).
 * 4. window.portalLogout() — выход.
 *
 * Грузится ДО app.js.
 */
(function () {
    var CSRF_HEADER = 'X-Requested-With';
    var CSRF_VALUE = 'AuditManager';
    var redirecting = false;

    var caps = window.__portalCaps && typeof window.__portalCaps === 'object' ? window.__portalCaps : {};
    window.__portalCaps = caps;
    var sections = caps.sections || {};

    window.portalCan = {
        // Запуски и правки — admin; возврат решений из Excel разрешается отдельно.
        operate: function () { return caps.can_operate !== false; },
        importDecisions: function () {
            return caps.can_operate !== false || caps.can_import_decisions === true;
        },
        // Служебные панели: расход, пользователи, учётная запись модели.
        admin: function () { return caps.is_admin !== false; },
        view: function (section) {
            if (caps.can_operate !== false) return true;
            return !!(sections[section] && sections[section].view);
        },
        download: function (section) {
            var s = sections[section];
            if (caps.can_operate !== false) return !s || s.download !== false;
            return !!(s && s.view && s.download);
        },
        accountsMode: function () { return caps.mode === 'accounts'; },
    };

    function goTo(path) {
        if (redirecting) return;
        if (window.location.pathname === path) return;
        redirecting = true;
        window.location.replace(path);
    }

    function urlPath(input) {
        try {
            var raw = typeof input === 'string' ? input : (input && input.url) || '';
            var u = new URL(raw, window.location.origin);
            return u.origin === window.location.origin ? u.pathname : '';
        } catch (e) {
            return '';
        }
    }

    function isApi(path) { return path.indexOf('/api/') === 0; }

    function withCsrf(input, init) {
        var path = urlPath(input);
        if (!isApi(path)) return init;
        init = init || {};
        var headers = new Headers(init.headers || (typeof input !== 'string' && input && input.headers) || {});
        if (!headers.has(CSRF_HEADER)) headers.set(CSRF_HEADER, CSRF_VALUE);
        var out = {};
        for (var k in init) { if (Object.prototype.hasOwnProperty.call(init, k)) out[k] = init[k]; }
        out.headers = headers;
        return out;
    }

    function handleDenied(resp, path) {
        if (!resp || !isApi(path) || path.indexOf('/api/auth/') === 0) return;
        if (resp.status !== 401 && resp.status !== 403) return;
        resp.clone().json().then(function (body) {
            var detail = (body && body.detail) || {};
            var code = typeof detail === 'object' ? detail.code : '';
            if (resp.status === 401) {
                goTo(code && code !== 'not_authenticated' ? '/login?reason=' + encodeURIComponent(code) : '/login');
            } else if (code === 'password_change_required') {
                goTo('/change-password');
            } else if (code === 'forbidden_section' || code === 'forbidden_download' || code === 'admin_only' || code === 'forbidden_decision_import') {
                window.dispatchEvent(new CustomEvent('portal-forbidden', { detail: detail }));
            }
        }).catch(function () {
            if (resp.status === 401) goTo('/login');
        });
    }

    var nativeFetch = window.fetch.bind(window);
    window.fetch = function (input, init) {
        var path = urlPath(input);
        return nativeFetch(input, withCsrf(input, init)).then(function (resp) {
            handleDenied(resp, path);
            return resp;
        });
    };

    // XHR (загрузки с прогрессом в сравнении версий и app.js) — тот же заголовок.
    var xhrOpen = XMLHttpRequest.prototype.open;
    var xhrSend = XMLHttpRequest.prototype.send;
    XMLHttpRequest.prototype.open = function (method, url) {
        this.__portalPath = urlPath(url);
        return xhrOpen.apply(this, arguments);
    };
    XMLHttpRequest.prototype.send = function () {
        if (this.__portalPath && isApi(this.__portalPath)) {
            try { this.setRequestHeader(CSRF_HEADER, CSRF_VALUE); } catch (e) { /* уже отправлен */ }
        }
        return xhrSend.apply(this, arguments);
    };

    window.portalLogout = function () {
        nativeFetch('/api/auth/logout', { method: 'POST', headers: { 'X-Requested-With': CSRF_VALUE } })
            .catch(function () {})
            .then(function () { window.location.replace('/login'); });
    };

    document.addEventListener('DOMContentLoaded', function () {
        var enabled = caps.auth_enabled === true;
        window.__portalAuthEnabled = enabled;
        if (!enabled) return;
        var logout = document.getElementById('portal-logout-item');
        if (logout) {
            logout.style.display = '';
            if (!caps.is_admin || caps.mode !== 'accounts') logout.style.marginTop = 'auto';
        }
        var accounts = caps.mode === 'accounts';
        var users = document.getElementById('portal-users-item');
        if (accounts && caps.is_admin && users) users.style.display = '';
    });
})();
