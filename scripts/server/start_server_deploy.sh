#!/bin/bash
# Прод-лаунчер: запускает backend из этого репозитория (единая папка,
# ветка main) и прибивает все runtime data-корни к MAIN — единственному
# источнику истины (объекты, аудит-проекты, findings/knowledge_base,
# stage-comparison данные).
#
# История: до 2026-06-24 код жил в отдельном deploy-worktree, а данные — в MAIN;
# после сведения в одну папку редиректы стали тождественными, но оставлены
# явно — они защищают от случайного запуска из чужого worktree.
# Перенесено из webapp/ в scripts/server/ при ликвидации legacy-пакета (2026-07-04).
set -e
MAIN_DIR="/home/coder/projects/PDF-proverka"
export AUDIT_DATA_DIR="$MAIN_DIR"
export AUDIT_APP_DATA_DIR="$MAIN_DIR/backend/app/data"
export COMPARISON_ROOT="$MAIN_DIR/comparison"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "$SCRIPT_DIR/start_server.sh" "$@"
