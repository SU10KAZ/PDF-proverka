"""Shared report category vocabulary; no inference from free-form prose."""

# Нормализация старых/английских имён → каноничные
SEV_NORMALIZE = {
    # старые русские
    "КРИТИЧНО":       "КРИТИЧЕСКОЕ",
    "СУЩЕСТВЕННО":    "СУЩЕСТВЕННОЕ",
    "РЕКОМЕНДАЦИЯ":   "РЕКОМЕНДАТЕЛЬНОЕ",
    "ПРОВЕРИТЬ":      "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
    # английские (из пакетного анализа тайлов)
    "CRITICAL":       "КРИТИЧЕСКОЕ",
    "SUBSTANTIAL":    "СУЩЕСТВЕННОЕ",
    "ECONOMIC":       "ЭКОНОМИЧЕСКОЕ",
    "OPERATIONAL":    "ЭКСПЛУАТАЦИОННОЕ",
    "RECOMMENDATION": "РЕКОМЕНДАТЕЛЬНОЕ",
    "INFORMATIONAL":  "РЕКОМЕНДАТЕЛЬНОЕ",
    "CHECK_RELATED":  "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
}

# Порядок сортировки категорий в сводке (по убыванию важности)
SEV_ORDER = [
    "КРИТИЧЕСКОЕ",
    "СУЩЕСТВЕННОЕ",
    "ЭКОНОМИЧЕСКОЕ",
    "ЭКСПЛУАТАЦИОННОЕ",
    "РЕКОМЕНДАТЕЛЬНОЕ",
    "ПРОВЕРИТЬ ПО СМЕЖНЫМ",
    "СНЯТО",
]


def is_report_severity(value):
    return isinstance(value, str) and SEV_NORMALIZE.get(value, value) in SEV_ORDER
