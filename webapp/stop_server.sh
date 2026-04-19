#!/bin/bash
# Linux-эквивалент stop_server.bat: останавливает сервер на порту 8080.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PORT=8081

echo "Stopping Audit Manager server on port $PORT..."

PIDS=$(ss -ltnp 2>/dev/null | awk -v p=":$PORT" '$4 ~ p {print $NF}' \
    | grep -oP 'pid=\K[0-9]+' | sort -u)

if [ -z "$PIDS" ]; then
    # Fallback на сохранённый PID
    if [ -f "$SCRIPT_DIR/server.pid" ]; then
        SAVED=$(cat "$SCRIPT_DIR/server.pid")
        if kill -0 "$SAVED" 2>/dev/null; then
            PIDS="$SAVED"
        fi
    fi
fi

if [ -z "$PIDS" ]; then
    # Последний fallback — поиск по командной строке
    PIDS=$(pgrep -f "webapp.main|uvicorn.*webapp" || true)
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

rm -f "$SCRIPT_DIR/server.pid"
echo "Stopped."
