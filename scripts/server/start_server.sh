#!/bin/bash
# Фоновый запуск production-backend + логи + ожидание старта.
# Логи и pid — в <корень репо>/logs/ (gitignored).
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="$ROOT_DIR/logs"
mkdir -p "$LOG_DIR"
PORT=8081

echo "========================================"
echo "  Audit Manager background start"
echo "========================================"

if ss -ltn 2>/dev/null | grep -q ":$PORT "; then
    echo "Server is already running on http://localhost:$PORT"
    exit 0
fi

# Ротация вместо удаления. История: 15.07.2026 бэкенд упал под батчем, watchdog
# (ежеминутный cron) дёрнул этот скрипт — и первым же действием стёр server.err.log
# с трейсбеком краха. Механизм реагирования на падение уничтожал улики падения,
# и причину установить не удалось. Держим 10 прошлых запусков.
for _log in server.log server.err.log; do
    if [ -s "$LOG_DIR/$_log" ]; then
        mv -f "$LOG_DIR/$_log" "$LOG_DIR/${_log%.log}.$(date +%Y%m%d-%H%M%S).log" 2>/dev/null || true
    fi
    rm -f "$LOG_DIR/$_log"
done
# Чистим только СТАРЬЁ, отдельно по каждому семейству (server.* и server.err.*),
# иначе всплеск рестартов за минуту вымоет весь архив другого семейства.
for _fam in "server.2*.log" "server.err.2*.log"; do
    # shellcheck disable=SC2012
    ls -1t "$LOG_DIR"/$_fam 2>/dev/null | tail -n +11 | while read -r _old; do rm -f "$_old"; done
done

cd "$ROOT_DIR"
# Production cutover (2026-05-14): порт 8081 обслуживает backend.app.main:app.
# Cron-watchdog запускает этот скрипт при падении.
# В фоне запускаем БЕЗ --reload: reloader порождает дочерний процесс и
# нестабилен для nohup/daemon-style запуска.
# setsid + stdin from /dev/null уменьшают шанс, что процесс погибнет вместе
# с родительской shell/PTY-сессией.
if command -v setsid >/dev/null 2>&1; then
    nohup setsid python3 -m uvicorn backend.app.main:app --host 127.0.0.1 --port $PORT \
        >"$LOG_DIR/server.log" \
        2>"$LOG_DIR/server.err.log" \
        </dev/null &
else
    nohup python3 -m uvicorn backend.app.main:app --host 127.0.0.1 --port $PORT \
        >"$LOG_DIR/server.log" \
        2>"$LOG_DIR/server.err.log" \
        </dev/null &
fi
SERVER_PID=$!
echo $SERVER_PID > "$LOG_DIR/server.pid"

echo "Waiting for server (PID $SERVER_PID)..."
for i in $(seq 1 15); do
    sleep 1
    if ss -ltn 2>/dev/null | grep -q ":$PORT "; then
        echo "Server is up: http://localhost:$PORT"
        echo "Stdout log: $LOG_DIR/server.log"
        echo "Stderr log: $LOG_DIR/server.err.log"
        exit 0
    fi
    echo "  wait $i/15"
done

echo "ERROR: server did not start in time"
[ -f "$LOG_DIR/server.err.log" ] && tail -n 40 "$LOG_DIR/server.err.log"
exit 1
