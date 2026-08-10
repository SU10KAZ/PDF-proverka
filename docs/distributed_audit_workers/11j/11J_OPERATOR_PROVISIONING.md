# 11J — как оператор добавляет воркер с ключом OpenRouter

Процедура рассчитана на то, что ключ **не показывается никому и никуда не
копируется через посредника**: ни в переписку, ни в задачу, ни в конфигурацию
центра. Ниже — ровно те команды, которые выполняет человек с ssh на машину.

## 1. Полный порядок добавления воркера

```
1. развернуть код воркера
2. авторизовать Claude   (claude login — на самой машине)
3. авторизовать Codex    (codex login  — на самой машине)
4. выдать ключ OpenRouter локально          ← новое на 11J
5. описать локальную политику моделей
6. heartbeat показывает способности
7. центр видит воркера готовым
```

Шаги 2 и 3 не изменились. Ниже — только 4 и 5.

## 2. Выдача ключа (шаг 4)

**Ключ вводится ТОЛЬКО в ssh-сессии на самой машине.** Не в чат, не в задачу,
не в файл конфигурации центра.

```bash
# на воркере, под пользователем воркера
WORKER_ROOT=/home/coder/audit-worker/data          # корень ДАННЫХ воркера
DIR="$WORKER_ROOT/providers/openrouter/home/.openrouter"

install -d -m 700 "$DIR"
# read -s не отображает ввод и не оставляет строки в истории оболочки
read -rs -p 'OpenRouter API key: ' OR_KEY && echo
umask 077
printf '{"api_key": "%s"}\n' "$OR_KEY" > "$DIR/credentials.json"
chmod 600 "$DIR/credentials.json"
unset OR_KEY
history -d "$(history 1 | awk '{print $1}')" 2>/dev/null || true
```

Проверка — **без показа значения**:

```bash
stat -c '%a %U %s %n' "$DIR/credentials.json"
# ожидается: 600 <пользователь воркера> <размер> …/credentials.json
```

Ещё одна проверка, уже кодом воркера, тоже без значения:

```bash
cd /path/to/worker/code && python3 - <<'PY'
from pathlib import Path
from audit_worker.providers import openrouter_secret, paths
home = paths.provider_home(Path("/home/coder/audit-worker/data"), "openrouter")
print(openrouter_secret.probe(home.credential_path).as_dict())
PY
# ожидается configured=true, mode=0600, owner_is_current_user=true
```

### Если путь нужен другой

```bash
# в worker.env — ПУТЬ, не ключ
AUDIT_WORKER_PROVIDER_OPENROUTER_CREDENTIAL=/etc/audit-worker/openrouter.key
```

Файл по этому пути тоже обязан быть 0600 и принадлежать пользователю воркера.
Путь обязан быть абсолютным.

### Чего делать НЕЛЬЗЯ

* класть ключ в `worker.env` как `OPENROUTER_API_KEY=…` — имя входит в чёрный
  список окружения подпроцессов, и такой ключ не будет прочитан; зато он
  попадёт в любой дамп окружения;
* копировать ключ центра автоматически. Даже если счёт один, provisioning
  выполняется отдельно на каждой машине — иначе ключ проходит через центр, а
  значит через его БД, журналы и резервные копии;
* передавать ключ через задание, пакет или API центра. Такое задание не
  разбирается: набор полей закрыт.

## 3. Локальная политика моделей (шаг 5)

`<WORKER_ROOT>/provider_policy.json`:

```json
{
  "policy_version": 1,
  "claude": {
    "auth_mode": "ambient_user",
    "capabilities": {
      "strong_audit": {"model": "claude-opus-5"},
      "cheap_review": {"model": "claude-sonnet-5"}
    }
  },
  "codex": {
    "capabilities": {
      "strong_audit":          {"model": "gpt-5.4-codex",      "model_report": "unsupported"},
      "cheap_review":          {"model": "gpt-5.4-codex",      "model_report": "unsupported"},
      "block_detector":        {"model": "gpt-5.4-codex",      "model_report": "unsupported"},
      "block_detector_strong": {"model": "gpt-5.4-codex-high", "model_report": "unsupported"},
      "block_judge":           {"model": "gpt-5.4-codex",      "model_report": "unsupported"},
      "visual_reasoning":      {"model": "gpt-5.4-codex",      "model_report": "unsupported"}
    }
  },
  "openrouter": {
    "capabilities": {
      "block_detector": {"model": "openai/gpt-5.4"}
    }
  }
}
```

Три замечания.

`model_report: "unsupported"` у Codex — не послабление «на всякий случай», а
измеренное свойство CLI: поток `exec --json` идентификатора модели не содержит
ни в одном событии. У OpenRouter он есть, поэтому там остаётся `required`.

Способности OpenRouter объявляются центру **только при настроенном ключе**.
Запись в политике без файла ключа ничего не даёт: воркер её не объявит, и центр
не назначит задание. Это сделано намеренно — способность, которую нечем
исполнить, хуже отсутствия записи.

Точные строки моделей — собственность машины. Центру они не уезжают: наружу
уходит только «openrouter умеет block_detector».

## 4. Проверка готовности (шаги 6–7)

```bash
systemctl --user restart audit-worker-agent
journalctl --user -u audit-worker-agent -n 40 --no-pager
```

В карточке VPS на центре должно появиться:

* `openrouter: installed / logged_in`, `credential_mode 0600`;
* `provider_capabilities.openrouter = ["block_detector"]`;
* `http_providers_v1 = true`;
* `provider_endpoints_stubbed = false` (на боевом воркере — обязательно false).

Если центр по-прежнему отказывает в назначении: способности воркера
фиксируются при РЕГИСТРАЦИИ. После выдачи ключа нужен повторный
`PUT /registration` (перезапуск агента его выполняет), иначе центр продолжит
решать по устаревшему объявлению.

## 5. Стенд с заглушками

На стенде внешние точки объявляются заглушками явно:

```bash
AUDIT_WORKER_PROVIDER_ENDPOINTS_STUBBED=true
AUDIT_WORKER_PROVIDER_OPENROUTER_BASE_URL=http://127.0.0.1:8099
```

Без первой переменной вторая отвергается: увести ключ с официального хоста
молча нельзя. Объявление уезжает в heartbeat, поэтому прогон на стенде и боевой
прогон в отчёте различимы.

**На боевом воркере обе переменные должны отсутствовать.**

## 6. Отзыв ключа

```bash
shred -u "$DIR/credentials.json" 2>/dev/null || rm -f "$DIR/credentials.json"
systemctl --user restart audit-worker-agent
```

После этого воркер перестаёт объявлять способность шлюза, и центр перестаёт
назначать ему задания с точным пресетом — до выдачи нового ключа. Идущее
задание при этом не «деградирует»: конкретное действие падает с
`auth_required`, соседние ноги продолжают.

## 7. Ничего не hardcode

Ни адрес `176.12.77.31`, ни пользователь `coder`, ни путь к ключу в бизнес-логике
не зашиты. Всё три — конфигурация машины: корень данных задаётся при
развёртывании, путь к ключу вычисляется от него либо переопределяется
переменной администратора.
