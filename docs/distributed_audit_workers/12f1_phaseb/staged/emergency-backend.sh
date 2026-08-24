#!/usr/bin/env bash
# Аварийный подъём иммутабельного релиза БЕЗ systemd --user.
#
# Вызывается вотчдогом, когда user@<uid>.service мёртв (убит OOM) и :8081 закрыт.
# Поднять сам менеджер из-под coder нельзя — polkit требует пароль, поэтому
# единственный доступный вариант супервизии в этой ситуации: держать бэкенд руками.
#
# Возврат под systemd (нужен root), СТРОГО в этом порядке — иначе юнит
# не сядет на занятый 8081 и выжжет StartLimitBurst:
#   kill $(cat ~/.cloudflared/backend-fallback.pid)
#   sudo systemctl reset-failed user@$(id -u).service
#   sudo systemctl start user@$(id -u).service
set -u

LAUNCHER="/home/coder/auditmanager/bin/emergency-backend.py"
PY="/home/coder/auditmanager/current/venv/bin/python"
LOG="$HOME/.cloudflared/logs/backend-manual-recovery.log"
PIDFILE="$HOME/.cloudflared/backend-fallback.pid"
STAMP="$HOME/.cloudflared/backend-fallback.last-attempt"
RETRY_COOLDOWN=180
PREFIX='[emergency]'

mkdir -p "$HOME/.cloudflared/logs"

log() { echo "$PREFIX $*"; }

# Аварийный процесс уже работает?
fallback_alive() {
    [ -f "$PIDFILE" ] || return 1
    local pid
    pid=$(cat "$PIDFILE" 2>/dev/null) || return 1
    [ -n "$pid" ] || return 1
    kill -0 "$pid" 2>/dev/null
}

if fallback_alive; then
    log "аварийный бэкенд уже работает (pid=$(cat "$PIDFILE")), повторный запуск не нужен"
    exit 0
fi

if [ ! -x "$PY" ] || [ ! -f "$LAUNCHER" ]; then
    log "ОТКАЗ: нет $PY или $LAUNCHER — релиз недоступен"
    exit 1
fi

# Защита от шторма перезапусков, если бэкенд падает сразу после старта.
NOW=$(date +%s)
LAST=0
[ -f "$STAMP" ] && LAST=$(cat "$STAMP" 2>/dev/null || echo 0)
if [ -n "$LAST" ] && [ $((NOW - LAST)) -lt "$RETRY_COOLDOWN" ] 2>/dev/null; then
    log "предыдущая попытка была $((NOW - LAST))с назад, жду ${RETRY_COOLDOWN}с между попытками"
    exit 0
fi
echo "$NOW" > "$STAMP"

rm -f "$PIDFILE"
log "ЗАПУСК: user-менеджер мёртв, поднимаю релиз напрямую (без Restart= и без sandbox)"
setsid nohup "$PY" "$LAUNCHER" >> "$LOG" 2>&1 < /dev/null &
disown 2>/dev/null || true
exit 0
