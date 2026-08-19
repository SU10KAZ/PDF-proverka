# 11K — как worker появился на `.31` до one-click

Источник — scripts/docs 10–11J и read-only SSH inventory 2026-08-11. Ни один файл или service на `.31` не менялся.

| Ручной шаг | Зачем | Фактический код | Автоматизируем | root | человек | секрет |
|---|---|---|---|---|---|---|
| Проверить ОС/CPU/диск/TLS | не начать на неподдерживаемой машине | `smoke_distributed_audit_real_vps.py::phase_preflight_worker` | да | нет | нет | нет |
| Заранее доверить SSH host key | не попасть на другой VPS | внешний `known_hosts` | да, при out-of-band fingerprint | нет | один fingerprint | нет |
| Собрать bundle/manifest | закрепить revision/hash | `deploy_audit_worker.py::build_artifact` | да | нет | нет | нет |
| Создать `app/data/config/logs/incoming/venv` | runtime layout | `remote_bootstrap_layout`, `remote_sync_venv` | да | нет | нет | нет |
| SCP bundle и manifest | admin deploy | `Remote.copy` | да | нет | нет | нет |
| Проверить SHA, распаковать, self-test, switch | атомарный release | `remote_install_release/selftest/switch_current` | да | нет | нет | нет |
| Составить `worker.env` | URL, revision, slots, policy mode | `worker_env_file` в real-VPS smoke | да | нет | нет | нет |
| Положить provider policy | capability → local model | `_PROVIDER_SETUP_PROBE` | да | нет | нет | нет |
| Создать два user units | Agent отдельно от Executor | `systemd_unit` | да | иногда linger через sudo | нет | нет |
| Передать global bootstrap secret в register | создать заявку | `python -m audit_worker register --bootstrap-secret ...` | заменено scoped stdin-token | нет | нет | да |
| Одобрить worker в UI/API | выпустить runtime token | `approve_worker` + `/claim` | да внутри trusted bootstrap manager | нет | да раньше | claim/runtime token |
| Установить Claude/Codex | provider runtime | ручные official installers в 11B | да, pinned hashed artifact | нет | нет | нет |
| Войти Claude/Codex | subscription identity | official auth/device commands | нет объективно | нет | да | credential остаётся VPS |
| Ввести OpenRouter key | HTTP provider | ручной файл `0600` | только интерактивный remote helper | нет | да | да, только VPS |
| Проверить heartbeat/fake job | доказать runtime | smoke/network tests | да | нет | нет | нет |

Read-only факт `.31`: Ubuntu 24.04, `coder` uid 1001, x86_64, 8 CPU, release `20260809T140821-c1070168cda4`, token mode `0600`, Claude 2.1.220 logged in, Codex 0.147.0 logged in, OpenRouter absent. Legacy units обнаружены `disabled/inactive`; bootstrap их не запускал и не правил.
