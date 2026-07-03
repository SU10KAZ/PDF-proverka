#!/bin/bash
# Останавливает production-backend на порту 8081.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="$ROOT_DIR/logs"
PORT=8081

echo "Stopping Audit Manager server on port $PORT..."

PIDS=$(ss -ltnp 2>/dev/null | awk -v p=":$PORT" '$4 ~ p {print $NF}' \
    | grep -oP 'pid=\K[0-9]+' | sort -u)

if [ -z "$PIDS" ]; then
    # Fallback на сохранённый PID (новое место, затем legacy webapp/)
    for pidfile in "$LOG_DIR/server.pid" "$ROOT_DIR/webapp/server.pid"; do
        if [ -f "$pidfile" ]; then
            SAVED=$(cat "$pidfile")
            if kill -0 "$SAVED" 2>/dev/null; then
                PIDS="$SAVED"
                break
            fi
        fi
    done
fi

if [ -z "$PIDS" ]; then
    # Последний fallback — поиск по командной строке
    PIDS=$(pgrep -f "uvicorn.*backend.app.main" || true)
fi

if [ -z "$PIDS" ]; then
    echo "Server is not running."
    exit 0
fi

for pid in $PIDS; do
    echo "  kill PID $pid"
    kill "$pid" 2>/dev/null || true
done

sleep 2

for pid in $PIDS; do
    if kill -0 "$pid" 2>/dev/null; then
        echo "  force kill PID $pid"
        kill -9 "$pid" 2>/dev/null || true
    fi
done

rm -f "$LOG_DIR/server.pid" "$ROOT_DIR/webapp/server.pid" 2>/dev/null
echo "Stopped."
