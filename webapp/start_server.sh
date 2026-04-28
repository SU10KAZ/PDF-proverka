#!/bin/bash
# Linux-эквивалент start_server.bat: фоновый запуск + логи + ожидание старта.
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PORT=8081

echo "========================================"
echo "  Audit Manager background start"
echo "========================================"

if ss -ltn 2>/dev/null | grep -q ":$PORT "; then
    echo "Server is already running on http://localhost:$PORT"
    exit 0
fi

rm -f "$SCRIPT_DIR/server.log" "$SCRIPT_DIR/server.err.log"

cd "$ROOT_DIR"
# В фоне запускаем БЕЗ --reload: reloader порождает дочерний процесс и
# нестабилен для nohup/daemon-style запуска.
# setsid + stdin from /dev/null уменьшают шанс, что процесс погибнет вместе
# с родительской shell/PTY-сессией.
if command -v setsid >/dev/null 2>&1; then
    nohup setsid python3 -m uvicorn webapp.main:app --host 127.0.0.1 --port $PORT \
        >"$SCRIPT_DIR/server.log" \
        2>"$SCRIPT_DIR/server.err.log" \
        </dev/null &
else
    nohup python3 -m uvicorn webapp.main:app --host 127.0.0.1 --port $PORT \
        >"$SCRIPT_DIR/server.log" \
        2>"$SCRIPT_DIR/server.err.log" \
        </dev/null &
fi
SERVER_PID=$!
echo $SERVER_PID > "$SCRIPT_DIR/server.pid"

echo "Waiting for server (PID $SERVER_PID)..."
for i in $(seq 1 15); do
    sleep 1
    if ss -ltn 2>/dev/null | grep -q ":$PORT "; then
        echo "Server is up: http://localhost:$PORT"
        echo "Stdout log: $SCRIPT_DIR/server.log"
        echo "Stderr log: $SCRIPT_DIR/server.err.log"
        exit 0
    fi
    echo "  wait $i/15"
done

echo "ERROR: server did not start in time"
[ -f "$SCRIPT_DIR/server.err.log" ] && tail -n 40 "$SCRIPT_DIR/server.err.log"
exit 1
