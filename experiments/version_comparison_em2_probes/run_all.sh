#!/usr/bin/env bash
# Повторная проверка фактов плана «Сравнение версий» (§16) по корпусу ЭМ2.
# Использование: run_all.sh <папка с 12 PDF «133_23-ГК-ЭМ2 (N).pdf»> [<путь к EM2_реестр_сравнения.xlsx>]
# Скрипты только читают PDF. Вывод — в stdout; profile_pdfs пишет JSON во временную папку.
set -euo pipefail
PDF_DIR="${1:?укажите папку с PDF корпуса ЭМ2}"
XLSX="${2:-}"
HERE="$(cd "$(dirname "$0")" && pwd)"
OUT="$(mktemp -d)"
run() { echo; echo "######## $1"; python3 -I "$HERE/$1" "${@:2}"; }
run profile_pdfs.py "$PDF_DIR" "$OUT/pdf_profile.json"
run check_warn.py "$PDF_DIR"
run same_count_check.py "$PDF_DIR"
run annot_text_leak.py "$PDF_DIR"
run stamp_probe.py "$PDF_DIR"
run claims_check.py "$PDF_DIR"
run sheetno_probe.py "$PDF_DIR"
run anchor_reg.py "$PDF_DIR"
run cloud_check.py "$PDF_DIR"
run cloud_words.py "$PDF_DIR"
run so_probe.py "$PDF_DIR"
run permit_probe.py "$PDF_DIR"
run perf_probe.py "$PDF_DIR"
if [[ -n "$XLSX" ]]; then run xlsx_probe.py "$XLSX"; fi
echo; echo "profile JSON: $OUT/pdf_profile.json"
