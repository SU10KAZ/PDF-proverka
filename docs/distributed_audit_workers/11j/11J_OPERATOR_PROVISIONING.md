# 11J.1 — безопасный provisioning OpenRouter на `.31`

Ключ вводится только человеком в SSH-сессии на самом VPS. Его нельзя
присылать в чат, класть в AuditManager job, `.env` центра, repository, job root
или package/result root.

## 1. Создать worker-local secret

На `.31`, под пользователем production worker:

```bash
SECRET_DIR=/home/coder/.config/audit-worker-secrets/openrouter
SECRET_FILE="$SECRET_DIR/credentials.json"

install -d -m 700 "$SECRET_DIR"
read -rs -p 'OpenRouter API key: ' OR_KEY && echo
umask 077
printf '%s\n' "$OR_KEY" > "$SECRET_FILE"
chmod 600 "$SECRET_FILE"
unset OR_KEY
```

Формат одной строки поддерживается штатным `openrouter_secret`; JSON руками
собирать не нужно. Команда `read -s` не показывает значение.

## 2. Указать только путь

В локальном `worker.env` production worker добавить только:

```bash
AUDIT_WORKER_PROVIDER_OPENROUTER_CREDENTIAL=/home/coder/.config/audit-worker-secrets/openrouter/credentials.json
```

`OPENROUTER_API_KEY=...` в env запрещён. В provider policy должна быть
локальная пара `openrouter/block_detector`; модель выбирает администратор VPS.

## 3. Zero-inference проверка

После обычного rollout 11J-сборки:

```bash
cd /home/coder/audit-worker/current
AUDIT_WORKER_ROOT=/home/coder/audit-worker \
  /home/coder/audit-worker/venv/bin/python -m audit_worker providers
```

Ожидается `openrouter.auth_state=logged_in`, `credential_mode=0600`. Это
означает «файл безопасно настроен», а не «ключ проверен провайдером»; сетевой
probe выполнять не нужно.

## 4. Перерегистрировать способности

```bash
systemctl --user restart audit-worker-agent audit-worker-executor
```

Новый heartbeat/registration должен показать
`provider_capabilities.openrouter=["block_detector"]`,
`http_providers_v1=true`, `provider_endpoints_stubbed=false`.

Ни одна команда этой процедуры не вызывает OpenRouter inference. Значение
ключа не выводится и не передаётся через центр.
