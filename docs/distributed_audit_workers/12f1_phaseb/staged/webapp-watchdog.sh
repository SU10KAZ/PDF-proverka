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

mkdir -p "$HOME/.cloudflared/logs"
export XDG_RUNTIME_DIR="/run/user/$(id -u)"

# The immutable user service is the sole owner of :8081. Restart=on-failure is
# handled by systemd. This watchdog reports supervision failures but never
# starts the mutable checkout or a second backend.
if ! systemctl --user is-enabled --quiet "$SYSTEMD_UNIT" 2>/dev/null; then
    echo "$LOG_PREFIX immutable $SYSTEMD_UNIT is not enabled"
elif ! systemctl --user is-active --quiet "$SYSTEMD_UNIT" 2>/dev/null; then
    echo "$LOG_PREFIX immutable $SYSTEMD_UNIT is enabled but not active"
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
