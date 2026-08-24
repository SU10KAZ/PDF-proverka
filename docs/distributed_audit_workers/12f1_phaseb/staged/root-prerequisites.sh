#!/usr/bin/env bash
set -euo pipefail

# ── каталоги состояния и linger (было с 12F1 phase B) ───────────────────────
install -d -o coder -g coder -m 0700 /var/lib/auditmanager
install -d -o coder -g coder -m 0700 /var/lib/auditmanager/distributed_workers
loginctl enable-linger coder

stat -c '%n %U:%G %a' \
  /var/lib/auditmanager \
  /var/lib/auditmanager/distributed_workers
loginctl show-user coder -p Linger -p State -p RuntimePath

# ── защита прода от OOM-killer (добавлено 24.08.2026) ───────────────────────
# Разбор причины и замеры — в шапке 10-oom-protection.conf рядом с этим файлом.
# Коротко: апстримный systemd вешает на user@.service OOMScoreAdjust=100, из
# чего выводится adj=200 для всех его служб — боевой бэкенд оказывался жертвой
# №1 на машине (oom_score=801 против 678 у 1,2-гигабайтного vscode-server).
UID_NUM=1001
DROPIN_DIR="/etc/systemd/system/user@${UID_NUM}.service.d"
SRC="$(dirname "$(readlink -f "$0")")/10-oom-protection.conf"

install -d -m 0755 "$DROPIN_DIR"
install -m 0644 "$SRC" "$DROPIN_DIR/10-oom-protection.conf"
systemctl daemon-reload
echo "drop-in установлен: $DROPIN_DIR/10-oom-protection.conf"

# Drop-in вступит в силу при следующем старте user@1001.service. Чтобы не
# перезапускать менеджер (это оборвало бы аудиты), применяем к уже живым
# процессам напрямую — понижать oom_score_adj вправе только CAP_SYS_RESOURCE,
# то есть этот скрипт под root.
MGR_PID="$(systemctl show "user@${UID_NUM}.service" -p MainPID --value)"
CG="/sys/fs/cgroup/user.slice/user-${UID_NUM}.slice/user@${UID_NUM}.service"

if [ -n "$MGR_PID" ] && [ "$MGR_PID" != "0" ] && [ -d "$CG" ]; then
    # ТОЛЬКО *.service — ровно то множество, которое drop-in накроет после
    # перезагрузки. Обходить всё дерево cgroup нельзя: под менеджером в
    # app.slice висят ещё и scope'ы рабочего стола (app-code-*.scope,
    # app-com.google.Chrome-*.scope). Их systemd не форкает, DefaultOOMScoreAdjust
    # на них не действует (замер: у `code` adj=0 при adj=200 у службы-бэкенда),
    # и защищать пожирателей памяти наравне с продом — ровно наоборот смыслу.
    N=0
    while read -r svc_procs; do
        while read -r pid; do
            [ -n "$pid" ] || continue
            echo -200 > "/proc/$pid/oom_score_adj" 2>/dev/null && N=$((N + 1)) || true
        done < "$svc_procs"
    done < <(find "$CG" -type d -name "*.service" -exec echo {}/cgroup.procs \; 2>/dev/null)
    # сам менеджер — его потеря дороже всего: без него Restart=on-failure мёртв
    echo -300 > "/proc/$MGR_PID/oom_score_adj"
    echo "применено к живым: менеджер pid=$MGR_PID adj=-300, процессов служб adj=-200: $N"
else
    echo "ВНИМАНИЕ: user@${UID_NUM}.service не запущен — применится при следующем старте"
fi

# ── проверка результата ─────────────────────────────────────────────────────
echo "--- контроль: службы под защитой (чем МЕНЬШЕ oom_score, тем позже убьют) ---"
printf '%-34s %-6s %s\n' ПРОЦЕСС adj oom_score
while read -r svc_procs; do
    while read -r pid; do
        [ -r "/proc/$pid/comm" ] || continue
        printf '%-34s %-6s %s\n' \
            "$(tr -d '\n' < "/proc/$pid/comm")" \
            "$(cat "/proc/$pid/oom_score_adj" 2>/dev/null)" \
            "$(cat "/proc/$pid/oom_score" 2>/dev/null)"
    done < "$svc_procs"
done < <(find "$CG" -type d -name "*.service" -exec echo {}/cgroup.procs \; 2>/dev/null)
echo "--- для сравнения, топ пожирателей памяти вне защиты ---"
ps -eo pid,rss,comm --sort=-rss --no-headers | head -4 | while read -r pid rss comm; do
    printf '%-34s %-6s %s (rss %sM)\n' "$comm" \
        "$(cat "/proc/$pid/oom_score_adj" 2>/dev/null)" \
        "$(cat "/proc/$pid/oom_score" 2>/dev/null)" "$((rss/1024))"
done
