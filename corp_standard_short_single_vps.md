# Краткая выжимка корпоративного стандарта v3.1: single-VPS baseline

## 1. Базовая архитектура и инфраструктура

Корпоративный портал разворачивается как web-приложение с frontend, backend API, managed PostgreSQL, S3-compatible object storage, централизованной аутентификацией, управляемым deploy-процессом и базовой наблюдаемостью.

Базовый вариант первичного внедрения — **single-VPS deployment**: одна production VPS/VM в Yandex Compute Cloud, на которой размещаются nginx reverse proxy, backend API, workers и Keycloak. Вторая VPS/VM и Yandex Application Load Balancer на этом этапе не создаются.

Обязательный стек приложения:

- backend: **Node.js + TypeScript + Fastify**;
- frontend: **React + TypeScript + Ant Design 5**;
- БД: **Yandex Managed PostgreSQL**;
- ORM/runtime query layer: **Drizzle ORM**;
- миграции: **Drizzle Kit + SQL-first migrations**;
- файловое хранилище: **S3-compatible object storage**;
- логи: **pino JSON logs**;
- runtime validation: **zod**.

Целевая production-инфраструктура первичного внедрения:

```text
DNS A/AAAA records
   ├─ auth.su10.ru        → backend-vps-1 public IP
   ├─ auth-admin.su10.ru  → backend-vps-1 public IP
   ├─ api.portal-a.ru     → backend-vps-1 public IP
   ├─ api.portal-b.ru     → backend-vps-1 public IP
   └─ api.portal-c.ru     → backend-vps-1 public IP

Yandex Compute Cloud / VPS
   └─ backend-vps-1
        ├─ nginx reverse proxy
        ├─ portal containers
        ├─ worker containers
        └─ keycloak container

Yandex Managed PostgreSQL
   ├─ portal databases
   └─ keycloak_db

S3-compatible storage
   └─ cloud.ru S3 / Cloudflare R2
```

Nginx используется как входной L7-слой для TLS termination, HTTP → HTTPS redirect и маршрутизации HTTP/HTTPS-запросов к Keycloak и backend API порталов.

Docker Compose используется как способ запуска сервисов на production VPS/VM. Каждый портал, Keycloak и nginx запускаются как отдельные compose projects или отдельные infrastructure services.

Single-VPS схема является упрощённым стартовым вариантом и содержит single point of failure на уровне `backend-vps-1`. Переход к HA выполняется отдельным этапом: добавляется вторая VPS/VM, управляемый ingress/load balancer и rolling deployment.

---

## 2. Аутентификация и авторизация

Для корпоративных пользователей используется централизованная аутентификация через **Keycloak** на домене:

```text
auth.su10.ru
```

Keycloak интегрируется с **Active Directory** через LDAP/LDAPS User Federation.

Источники пользователей:

| Тип пользователя | Источник |
|---|---|
| Внутренние сотрудники | Active Directory |
| Подрядчики | Локальная база пользователей Keycloak |

Сотрудники входят через AD. Подрядчики в AD не заводятся и управляются в Keycloak.

Keycloak отвечает за:

- login;
- SSO;
- MFA, если включено;
- активность пользователя;
- связь с AD;
- крупные роли доступа;
- client roles для порталов.

Порталы отвечают за:

- бизнес-авторизацию;
- доступ к конкретным объектам;
- права на документы, заявки, отделы, подрядчиков;
- audit бизнес-действий.

Модель ролей:

```text
Keycloak:
  passdesk.access
  passdesk.admin
  passdesk.manager
  passdesk.contractor_viewer
  passdesk.contractor_editor

Backend портала:
  проверка доступа к конкретному объекту,
  отделу,
  подрядчику,
  документу,
  статусу бизнес-процесса.
```

Авторизация выполняется на backend. Клиентские проверки ролей используются только для UX.

---

## 3. Active Directory

Контроллер домена находится во внутренней сети компании и не публикуется в интернет.

Связь Keycloak с AD выполняется через защищённый канал:

```text
Keycloak на backend-vps-1
   ↓ site-to-site VPN
внутренняя сеть компании
   ↓ LDAPS 636
Domain Controller
```

Требования:

- использовать LDAPS;
- отдельный read-only service account для LDAP bind;
- доступ к AD только от `backend-vps-1` / Keycloak subnet;
- мониторинг VPN и LDAP bind;
- алерт при недоступности AD;
- AD groups мапятся в Keycloak groups / client roles.

---

## 4. Keycloak deployment

Keycloak размещается в Docker на той же production VPS/VM, что и порталы, но как отдельный инфраструктурный сервис:

```text
/opt/infra/keycloak/docker-compose.yml
```

Keycloak не входит в compose-проекты порталов.

Требования:

- отдельная БД `keycloak_db`;
- отдельный DB user;
- отдельные secrets;
- отдельный deploy/update process;
- отдельные health checks;
- resource limits для контейнера;
- `auth-admin.su10.ru` доступен только через VPN или IP allowlist;
- management port Keycloak `9000` наружу не публикуется;
- регулярный realm export;
- документированная процедура восстановления Keycloak и `keycloak_db`.

В single-VPS baseline Keycloak не кластеризуется. Недоступность `backend-vps-1` означает недоступность Keycloak, поэтому backup/restore и uptime checks обязательны.

---

## 5. База данных и миграции

Основная БД: **Yandex Managed PostgreSQL**.

Требования:

- подключение только от доверенных backend-сервисов;
- TLS;
- private/network-restricted access;
- доступ в single-VPS baseline только от `backend-vps-1` и доверенных migration/deploy runners;
- отдельный runtime user;
- отдельный migration user;
- минимальные права;
- backups;
- PITR;
- явный `conn_limit` для каждого DB user;
- расчёт connection pool для backend, worker и Keycloak.

Для первичного deployment:

```text
runtime_instance_count = 1
conn_limit >= runtime_instance_count × process_count × pool.max + reserve
```

Миграции:

- SQL-first;
- versioned SQL files;
- `drizzle-kit push` не используется в production;
- миграции применяются отдельным deployment step;
- миграции не запускаются автоматически из backend/worker containers;
- миграции должны быть безопасны для controlled single-VPS deployment и будущего перехода к rolling deployment.

---

## 6. Файлы

Файлы хранятся в S3-compatible storage:

```text
cloud.ru S3
или
Cloudflare R2
```

Production upload выполняется только через upload session и presigned URL:

```text
1. backend создаёт upload session;
2. backend генерирует object key;
3. backend выдаёт presigned URL;
4. frontend загружает файл напрямую в S3-compatible storage;
5. frontend подтверждает загрузку;
6. backend проверяет объект;
7. backend создаёт file record;
8. backend создаёт фоновые задачи обработки файла.
```

Backend хранит в PostgreSQL только metadata, upload status и связанные бизнес-сущности. Сами файлы backend локально не хранит.

---

## 7. Фоновые задачи

Для простых и средних порталов используются:

- PostgreSQL-based jobs;
- transactional outbox;
- retry с exponential backoff;
- idempotency;
- `attempts`;
- `max_attempts`;
- `next_run_at`;
- `locked_until`;
- dead-state.

В single-VPS baseline workers запускаются на `backend-vps-1`. Допускается несколько worker-процессов на одной VPS/VM, если хватает CPU/RAM и DB connection budget.

Захват задач должен выполняться через PostgreSQL locking, чтобы схема работала при нескольких workers и могла быть расширена до HA без перепроектирования.

Redis/BullMQ могут использоваться для отдельных сценариев:

- высокая частота коротких задач;
- большое количество параллельных workers;
- delayed/repeatable jobs;
- pub/sub;
- websocket-сценарии;
- high-frequency distributed rate-limit;
- заметная нагрузка PostgreSQL jobs на основную БД.

Для базового стандарта PostgreSQL jobs/outbox остаются основным вариантом.

---

## 8. Email

Основной transactional email provider: **Amazon SES**.

Amazon SES используется для:

- password reset;
- login notifications;
- MFA/security notifications;
- приглашений;
- системных уведомлений;
- workflow-уведомлений;
- сообщений подрядчикам и сотрудникам.

Обязательные требования:

- SPF;
- DKIM;
- DMARC;
- bounce handling;
- complaint handling;
- suppression list;
- email audit;
- rate-limit;
- шаблоны;
- секреты в protected secret storage.

Допустимый альтернативный provider: **Yandex Cloud Postbox**.

Достоинства Yandex Postbox:

- находится в том же Yandex Cloud-контуре, что и backend-инфраструктура;
- поддерживает SMTP;
- поддерживает AWS SES-compatible API;
- проще интегрировать с Yandex IAM и облачной инфраструктурой;
- использует защищённое соединение TLS 1.2+;
- может снизить количество внешних облачных зависимостей.

---

## 9. Секреты

Production-секреты не хранятся:

- в git;
- в Docker image;
- во frontend-коде;
- в логах;
- в БД.

Предпочтительный secret storage:

```text
Yandex Lockbox
```

Допустимые варианты для простых развёртываний:

- protected environment variables;
- Docker secrets;
- secret files с ограниченными правами;
- Vault;
- иной корпоративный secret manager.

В secret storage хранятся:

- DB credentials;
- Keycloak secrets;
- OIDC client secrets;
- SES/Postbox credentials;
- S3/R2 credentials;
- Sentry tokens;
- log HMAC key;
- encryption keys;
- service account secrets.

---

## 10. Deployment

Production VPS/VM не выполняет:

```text
git pull
npm install
npm run build
```

Production deployment выполняется через deploy runner или CI/CD.

Deployment flow для single-VPS baseline:

```text
1. deployment lock;
2. preflight checks;
3. build Docker image from exact commit;
4. push immutable image tag в Yandex Container Registry;
5. migration plan;
6. apply SQL migrations one time;
7. update portal API compose project on backend-vps-1;
8. health check API;
9. update/restart portal workers on backend-vps-1;
10. worker health/status check;
11. post-deploy checks;
12. deployment report.
```

Controlled restart конкретного портала на одной VPS/VM допустим для первичного внедрения. Нулевой downtime и rolling update становятся обязательными только при отдельном HA-этапе.

Deploy должен быть portal-scoped и не должен изменять соседние порталы, Keycloak, nginx или другие infrastructure services.

Каждый deploy формирует отчёт:

- portal;
- environment;
- actor;
- commit SHA;
- image tag;
- pending migrations;
- applied migrations;
- status сервисов на `backend-vps-1`;
- health check results;
- итоговый результат;
- причина ошибки.

---

## 11. Observability

Используется managed/SaaS-подход.

Рекомендуемый набор:

- **Yandex Monitoring**;
- **Yandex Managed Service for Prometheus** для Prometheus-style custom metrics;
- **Yandex Cloud Logging** или **Monium Logs**;
- **Sentry SaaS**;
- **Node Exporter**;
- **cAdvisor**, если нужны container metrics;
- **nginx access/error logs**;
- **Uptime Kuma** или uptime-SaaS.

Для single-VPS baseline обязательны алерты на недоступность `backend-vps-1`, nginx/reverse proxy, Keycloak, backend API, PostgreSQL errors, high 5xx rate, dead jobs, disk space, CPU/memory pressure и истечение TLS-сертификатов.

Sentry используется как SaaS для frontend/backend ошибок, stack traces, release tracking и source maps. Для frontend source maps должны загружаться в Sentry при сборке.

В Sentry запрещено отправлять:

- request body с ПДн;
- cookies;
- Authorization headers;
- access/refresh tokens;
- presigned URLs;
- пароли;
- секреты;
- сканы и документы.

---

## 12. Shared skills / общие компоненты

Общие технические решения не копируются вручную между проектами.

Используются три слоя:

```text
1. portal-template
2. versioned internal npm packages @su10/*
3. infra-standards repository
```

Примеры пакетов:

- `@su10/config`;
- `@su10/logger`;
- `@su10/fastify-security`;
- `@su10/oidc`;
- `@su10/mail`;
- `@su10/s3`;
- `@su10/jobs`;
- `@su10/observability`.
