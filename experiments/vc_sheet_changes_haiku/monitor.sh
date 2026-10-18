#!/bin/bash
# Наблюдение за сервером во время нагрузочного прогона: нагрузка, память, сессии codex, ответ портала.
OUT=${1:?файл}; PAT=${2:?шаблон процесса прогона}
echo "время load1 cpu_idle% mem_avail_gb codex_sessions pool_workers portal_ms" > "$OUT"
while pgrep -f "$PAT" >/dev/null; do
  l=$(cut -d' ' -f1 /proc/loadavg)
  idle=$(top -bn1 | awk '/%Cpu/{for(i=1;i<=NF;i++) if($i ~ /id,?/) print $(i-1)}' | head -1)
  mem=$(free -g | awk '/Mem:/{print $7}')
  cs=$(pgrep -fc "codex-0.162.0-alpha.17.2/codex exec")
  pw=$(pgrep -fc "multiprocessing.spawn")
  ms=$(curl -s -o /dev/null -m 10 -w "%{time_total}" http://127.0.0.1:8081/api/info | awk '{printf "%d", $1*1000}')
  echo "$(date +%T) $l $idle $mem $cs $pw $ms" >> "$OUT"
  sleep 5
done
