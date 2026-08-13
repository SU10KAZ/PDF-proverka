#!/bin/bash
# 11D.2: снимок дерева процессов ВО ВРЕМЯ вызова модели.
#
# Отличие от сборщика 11D: тот искал CLI по пути установленной версии
# (`/home/coder/.local/share/claude/versions`), а в cmdline стоит путь
# ЛАУНЧЕРА — из-за чего exe/cwd самого процесса CLI в снимок 11D не попали
# (зафиксировано как gap в 11D_STAGE01_RUN.json). Здесь CLI ловится по ИМЕНИ
# процесса (`pgrep -x claude`), поэтому /proc/<pid>/exe и cwd читаются.
OUT="$1"; DEADLINE=$((SECONDS + ${2:-2400}))
: > "$OUT"
CAUGHT=0
while [ $SECONDS -lt $DEADLINE ]; do
  PIDS=$( { pgrep -f "run_11d_text_analysis_provider"; pgrep -x claude; } 2>/dev/null | sort -u )
  if [ -n "$PIDS" ]; then
    {
      echo "=== СНИМОК $(date -Is) ==="
      ps -eo pid,ppid,user,lstart,etime,args --forest | grep -E "run_11d_text_analysis|claude|python3" | grep -v grep
      for p in $PIDS; do
        echo "--- pid=$p ---"
        echo "comm: $(cat /proc/$p/comm 2>/dev/null)"
        echo "exe: $(readlink -f /proc/$p/exe 2>/dev/null)"
        echo "cwd: $(readlink -f /proc/$p/cwd 2>/dev/null)"
        echo "cmdline: $(tr '\0' ' ' < /proc/$p/cmdline 2>/dev/null)"
        echo "ppid_chain: $(ps -o pid=,ppid=,comm= -p $p 2>/dev/null)"
        echo "sockets: $(ls -l /proc/$p/fd 2>/dev/null | grep -c socket)"
      done
    } >> "$OUT"
    CAUGHT=1
  elif [ $CAUGHT -eq 1 ]; then
    echo "=== процессы завершились $(date -Is) ===" >> "$OUT"
    break
  fi
  sleep 2
done
