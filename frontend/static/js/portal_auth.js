/**
 * Portal auth glue для SPA.
 *
 * 1. Перехватывает fetch: при 401 от /api/... (кроме /api/auth/*) — редирект
 *    на /login (сессия истекла или пользователь не вошёл).
 * 2. window.portalLogout() — выход (POST /api/auth/logout → /login).
 * 3. На старте опрашивает /api/auth/me: если auth включён — показывает кнопку
 *    «Выйти» (#portal-logout-item). Если выключен — ничего не трогает.
 *
 * Грузится ДО app.js. Загружается только аутентифицированным пользователям,
 * т.к. сам index.html и /static защищены middleware.
 */
(function () {
    var redirecting = false;

    function goToLogin() {
        if (redirecting) return;
        if (window.location.pathname === '/login') return;
        redirecting = true;
        window.location.replace('/login');
    }

    function urlPath(input) {
        try {
            var raw = typeof input === 'string' ? input : (input && input.url) || '';
            return new URL(raw, window.location.origin).pathname;
        } catch (e) {
            return '';
        }
    }

    var nativeFetch = window.fetch.bind(window);
    window.fetch = function (input, init) {
        return nativeFetch(input, init).then(function (resp) {
            if (resp && resp.status === 401) {
                var path = urlPath(input);
                if (path.indexOf('/api/') === 0 && path.indexOf('/api/auth/') !== 0) {
                    goToLogin();
                }
            }
            return resp;
        });
    };

    window.portalLogout = function () {
        nativeFetch('/api/auth/logout', { method: 'POST' })
            .catch(function () {})
            .then(function () { window.location.replace('/login'); });
    };

    document.addEventListener('DOMContentLoaded', function () {
        nativeFetch('/api/auth/me')
            .then(function (r) { return r.json(); })
            .then(function (data) {
                window.__portalAuthEnabled = !!(data && data.auth_enabled);
                if (window.__portalAuthEnabled) {
                    var item = document.getElementById('portal-logout-item');
                    if (item) item.style.display = '';
                }
            })
            .catch(function () {});
    });
})();
