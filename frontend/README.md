# Audit Manager — Frontend

Vue 3 SPA без сборки (CDN). Использует Vite только для dev-сервера с proxy и опционального build.

## Команды

```bash
npm install
npm run dev      # dev-сервер на :5173 с proxy → backend :8081
npm run build    # сборка в dist/
npm run preview  # preview сборки
```

## Proxy

Vite проксирует запросы к backend:
- `/api/*` → `http://localhost:8081`
- `/ws/*`  → `ws://localhost:8081`

## Файлы

- `index.html` — главный дашборд
- `model-control.html` — управление LLM-моделями
- `css/styles.css` — основные стили
- `css/model-control.css` — стили model-control
- `js/app.js` — основная Vue-логика
- `js/model-control.js` — логика model-control
- `js/vue.global.prod.js` — Vue 3 CDN bundle
- `js/marked.min.js` — Markdown renderer

## Без Vite

Открыть `index.html` напрямую через backend:
```
http://localhost:8081/
```
Backend отдаёт frontend через `StaticFiles` fallback.
