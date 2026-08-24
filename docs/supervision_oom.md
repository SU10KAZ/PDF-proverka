# Супервизия прода и защита от OOM-killer

Разбор инцидента 23.08.2026 (портал 502 в течение 12 ч 40 мин) и две правки,
закрывающие его причины. Живые файлы лежат вне репозитория, версионированные
копии — в `docs/distributed_audit_workers/12f1_phaseb/staged/`.

| Роль | Живой путь | Копия в репозитории |
|------|-----------|---------------------|
| Вотчдог (крон, раз в минуту) | `~/bin/webapp-watchdog.sh` | `staged/webapp-watchdog.sh` |
| Аварийный подъём, обёртка | `~/auditmanager/bin/emergency-backend.sh` | `staged/emergency-backend.sh` |
| Аварийный подъём, лаунчер | `~/auditmanager/bin/emergency-backend.py` | `staged/emergency-backend.py` |
| Юнит бэкенда | `~/.config/systemd/user/auditmanager-backend.service` | `staged/auditmanager-backend.service` |
| Защита от OOM (root) | `/etc/systemd/system/user@1001.service.d/10-oom-protection.conf` | `staged/10-oom-protection.conf` |
| Установщик root-части | — | `staged/root-prerequisites.sh` |

Копии обязаны совпадать байт-в-байт: `diff staged/webapp-watchdog.sh ~/bin/webapp-watchdog.sh`.

## Что сломалось 23.08

Супервизия была одноуровневой: бэкенд держит `systemd --user`, а его самого не
держит никто. OOM-killer весь день бил по `user-1001.slice`; бэкенд исправно
поднимался через `Restart=on-failure`, но в 11:18:15 тот же OOM убил SIGKILL'ом
**сам менеджер** (`user@1001.service`, `Result=signal`). Вместе с менеджером
исчез и `Restart=`. Портал лежал, пока Андрей Иванович не сообщил.

Аудит был ни при чём: за сутки 299 api-вызовов и ни одного pipeline-события,
пик самого юнита — 191 МБ. Память ели соседи по слайсу.

Вотчдог не помог: `systemctl --user is-enabled` при мёртвой шине возвращает
ошибку, старый код трактовал это как «юнит не включён» и 768 раз подряд написал
об этом в лог, не сделав ничего.

## Правка 1 — вотчдог различает состояния

Порядок проверок в `webapp-watchdog.sh` (проверено на изолированной копии,
все пять веток):

1. **Менеджер мёртв** (`systemctl is-active user@$(id -u).service` у *системного*
   systemd — шина пользователя для этого не нужна):
   - `:8081` отвечает → аварийный бэкенд уже держит портал, ничего не делаем;
   - `:8081` закрыт → запускаем `emergency-backend.sh`.
   В обоих случаях в лог идёт `КРИТИЧНО:` и готовая root-команда для возврата.
2. **Менеджер жив, но аварийный процесс держит порт** → предупреждение с точной
   инструкцией по передаче порта. Автоматически не убиваем: под ним может идти аудит.
3. **Юнит `failed`** → `reset-failed` + `start`. Закрывает выгорание
   `StartLimitBurst=3` за 300 с при серии OOM подряд.
4. **Юнит `inactive`** → не трогаем. `inactive` — это намеренная остановка
   (выкатка, обслуживание), и рестарт в окне деплоя опаснее простоя. Различие
   `failed` vs `inactive` здесь и есть предохранитель.
5. Всё в порядке → молчит.

Надзор за cloudflared не менялся.

## Правка 2 — прод перестаёт быть жертвой №1

Замер 24.08.2026 на живой машине (чем больше `oom_score`, тем раньше убьют):

```
auditmanager-backend (90 МБ)   adj=200  oom_score=801   <- жертва №1
systemd --user (менеджер)      adj=100  oom_score=733   <- жертва №2
node/vscode-server (1290 МБ)   adj=0    oom_score=678
claude, chrome (≈500 МБ)       adj=0    oom_score=671
```

Это не случайность, а конфигурация: апстримный systemd в
`/usr/lib/systemd/system/user@.service:33` ставит `OOMScoreAdjust=100`
(«пользовательские сессии убивать раньше системных служб»), и из этого
выводится `DefaultOOMScoreAdjust=200` для всех служб менеджера. Для рабочей
станции разумно, для этого хоста — нет: боевой портал живёт именно в `user@1001`.

Drop-in ставит менеджеру `-300`, отсюда `-200` его службам. Порядок жертв
становится осмысленным: первым умирает реальный пожиратель памяти.

**Рабочий стол под защиту не попадает — это проверено, а не предположено.**
`DefaultOOMScoreAdjust` действует только на юниты, которые менеджер форкает сам,
то есть на `*.service`. Chrome и vscode-server живут в scope'ах
(`session-*.scope`, `app-code-*.scope`, `app-com.google.Chrome-*.scope`), куда
процессы лишь усыновляются, и systemd им `oom_score_adj` не выставляет. Замер:
у `code` adj=0 при adj=200 у службы-бэкенда. Под защиту попадают ровно 10 служб:
`auditmanager-backend`, `dbus`, `pulseaudio`, `gpg-agent`, `dconf`,
`gnome-keyring`, `gvfs-*`, `at-spi*` — все мелкие, и все они 23.08 умирали
вместе с продом.

Прод не становится неубиваемым: при росте до нескольких ГБ во время аудита он
снова поднимется в списке. Буфер в 200 пунктов лишь не даёт убить 90-мегабайтную
службу раньше соседа на порядок жирнее.

### Установка (нужен root)

```bash
sudo bash docs/distributed_audit_workers/12f1_phaseb/staged/root-prerequisites.sh
```

Скрипт идемпотентен, ставит drop-in, делает `daemon-reload` и **применяет
значения к уже работающим процессам** через `/proc/<pid>/oom_score_adj`, поэтому
перезапуск менеджера не нужен и идущие аудиты не рвутся. Понижать `oom_score_adj`
вправе только `CAP_SYS_RESOURCE` — из-под `coder` это невозможно.

Живой цикл трогает только `*.service` под менеджером, а не всё дерево cgroup:
иначе защита накрыла бы scope'ы Chrome и vscode-server, то есть ровно тех, кого
и надо убивать первыми.

## Аварийный подъём

`emergency-backend.py` повторяет `ExecStart`/`WorkingDirectory`/`EnvironmentFile`
юнита. Отличия — цена режима: нет `Restart=on-failure` и нет sandbox-хардненинга.

Две неочевидные детали:

- **PID пишется в `~/.cloudflared/backend-fallback.pid` до `execv`.** `execv` PID
  не меняет, а cmdline после него неотличима от systemd-запуска — опознать
  аварийный процесс иначе нельзя.
- **Окружение вычищается** от `CLAUDE*`, `VSCODE_*`, `ANTHROPIC_*`. Аудит
  порождает раннеры `claude -p`, наследующие окружение целиком; при запуске из
  сессии Claude Code они сочли бы себя вложенной сессией, а `CLAUDE_EFFORT=xhigh`
  перебил бы боевой режим. Напоролись 24.08 при ручном подъёме.

Возврат под systemd — строго в этом порядке, иначе юнит не сядет на занятый
`:8081` и выжжет `StartLimitBurst`:

```bash
kill $(cat ~/.cloudflared/backend-fallback.pid)
sudo systemctl reset-failed user@$(id -u).service
sudo systemctl start user@$(id -u).service
```

## Диагностика 502 за один шаг

```bash
systemctl is-active user@1001.service     # failed → дело не в бэкенде, а в супервизоре
tail ~/.cloudflared/logs/watchdog.log      # строки КРИТИЧНО: → менеджер мёртв
tail ~/.cloudflared/logs/backend-manual-recovery.log
```

## Осталось незакрытым

Первопричина — прод живёт в `user-1001.slice` бок о бок с рабочим столом.
Защита меняет порядок жертв, но не убирает соседство. Радикальное решение —
вынести бэкенд в системный юнит (`/etc/systemd/system/`), но это меняет всю
модель эксплуатации: `@reboot boot-backend.sh` в кроне, вотчдог и скрипты
выкатки завязаны на `systemctl --user`.
