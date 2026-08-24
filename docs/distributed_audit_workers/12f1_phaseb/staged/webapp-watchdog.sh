#!/usr/bin/env bash
set -u

ROOT="$HOME/projects/PDF-proverka"
SERVER_DIR="$ROOT/scripts/server"
PORT=8081
API_URL="http://127.0.0.1:${PORT}/api/info"
TUNNEL_PATTERN='cloudflared tunnel --url http://127.0.0.1:8081'
COOLDOWN_FILE="$HOME/.cloudflared/cooldown_until"
COOLDOWN_NOTICE="$HOME/.cloudflared/cooldown_last_notice"
QUEUE_FILE="$ROOT/backend/app/data/batch_queue.json"
LOG_PREFIX='[watchdog]'
SYSTEMD_UNIT='auditmanager-backend.service'
MANAGER_UNIT="user@$(id -u).service"
EMERGENCY_SCRIPT="$HOME/auditmanager/bin/emergency-backend.sh"
FALLBACK_PID="$HOME/.cloudflared/backend-fallback.pid"

mkdir -p "$HOME/.cloudflared/logs"
export XDG_RUNTIME_DIR="/run/user/$(id -u)"

port_open() {
    ss -ltn 2>/dev/null | grep -q "127.0.0.1:${PORT}"
}

# Аварийный бэкенд (запущен мимо systemd) — опознаётся по pid-файлу, который
# emergency-backend.py пишет до execv. По cmdline его не отличить от штатного.
fallback_alive() {
    [ -f "$FALLBACK_PID" ] || return 1
    local pid
    pid=$(cat "$FALLBACK_PID" 2>/dev/null) || return 1
    [ -n "$pid" ] || return 1
    kill -0 "$pid" 2>/dev/null
}

# Владелец :8081 — immutable user-служба. Restart=on-failure отрабатывает systemd.
# Этот вотчдог никогда не поднимает mutable-чекаут и не плодит второй бэкенд.
#
# Инцидент 23.08.2026: OOM-killer убил не бэкенд, а САМ systemd --user. Супервизор
# исчез, `systemctl --user` начал возвращать ошибку шины, старая версия скрипта
# трактовала это как «юнит не включён» и 768 раз подряд написала об этом в лог,
# ничего не сделав. Портал пролежал 12 ч 40 мин. Поэтому первым делом проверяем
# живость самого менеджера — у СИСТЕМНОГО systemd, которому шина пользователя не нужна.
if ! systemctl is-active --quiet "$MANAGER_UNIT"; then
    echo "$LOG_PREFIX КРИТИЧНО: $MANAGER_UNIT мёртв (OOM?) — супервизии не существует"
    echo "$LOG_PREFIX вернуть штатно (нужен root): sudo systemctl reset-failed $MANAGER_UNIT && sudo systemctl start $MANAGER_UNIT"
    if port_open; then
        echo "$LOG_PREFIX :${PORT} отвечает — аварийный бэкенд держит портал, вмешательство не требуется"
    else
        echo "$LOG_PREFIX :${PORT} закрыт — портал лежит, запускаю аварийный бэкенд"
        "$EMERGENCY_SCRIPT" || echo "$LOG_PREFIX аварийный запуск не удался (код $?)"
    fi
elif fallback_alive; then
    FB=$(cat "$FALLBACK_PID" 2>/dev/null)
    echo "$LOG_PREFIX ВНИМАНИЕ: менеджер жив, но :${PORT} держит аварийный процесс (pid=$FB)"
    echo "$LOG_PREFIX юнит не сядет на занятый порт — передать вручную: kill $FB && systemctl --user reset-failed $SYSTEMD_UNIT && systemctl --user start $SYSTEMD_UNIT"
elif ! systemctl --user is-enabled --quiet "$SYSTEMD_UNIT" 2>/dev/null; then
    echo "$LOG_PREFIX immutable $SYSTEMD_UNIT is not enabled"
else
    UNIT_STATE=$(systemctl --user is-active "$SYSTEMD_UNIT" 2>/dev/null || true)
    case "$UNIT_STATE" in
        active|activating|reloading)
            : # работает, либо systemd сам поднимает после падения — не мешаем
            ;;
        failed)
            # systemd сдался сам: обычно выжжен StartLimitBurst=3 за 300с
            # (серия OOM подряд). Без этой ветки юнит остаётся мёртвым навсегда.
            echo "$LOG_PREFIX $SYSTEMD_UNIT в состоянии failed — systemd сдался, поднимаю"
            systemctl --user reset-failed "$SYSTEMD_UNIT" 2>&1 || true
            systemctl --user start "$SYSTEMD_UNIT" 2>&1 || echo "$LOG_PREFIX подъём не удался"
            ;;
        *)
            # inactive = остановлен намеренно (деплой, обслуживание). НЕ трогаем:
            # рестарт в окне выкатки опаснее простоя.
            echo "$LOG_PREFIX $SYSTEMD_UNIT в состоянии '${UNIT_STATE:-unknown}' — похоже на намеренную остановку, не вмешиваюсь"
            ;;
    esac
fi

# Production cloudflared supervision is intentionally unchanged.
if ! pgrep -af "$TUNNEL_PATTERN" >/dev/null 2>&1; then
    NOW=$(date +%s)
    UNTIL=0
    [ -f "$COOLDOWN_FILE" ] && UNTIL=$(cat "$COOLDOWN_FILE" 2>/dev/null || echo 0)
    if [ -n "$UNTIL" ] && [ "$UNTIL" -gt "$NOW" ] 2>/dev/null; then
        LAST=0
        [ -f "$COOLDOWN_NOTICE" ] && LAST=$(cat "$COOLDOWN_NOTICE" 2>/dev/null || echo 0)
        if [ $((NOW - LAST)) -ge 300 ]; then
            echo "$LOG_PREFIX tunnel cooldown active, $((UNTIL - NOW))s left, skipping"
            echo "$NOW" > "$COOLDOWN_NOTICE"
        fi
    else
        [ -f "$COOLDOWN_FILE" ] && rm -f "$COOLDOWN_FILE" "$COOLDOWN_NOTICE"
        echo "$LOG_PREFIX web tunnel is down, starting"
        "$HOME/bin/cf-tunnel.sh" >/dev/null 2>&1 || true
    fi
fi
