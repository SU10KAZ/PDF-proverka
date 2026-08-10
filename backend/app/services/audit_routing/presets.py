"""Пресеты и разбор шаблона `stage_models.json` на стороне ЦЕНТРА.

Почему это появилось на бэкенде, а не осталось во фронтенде.

Инвентаризация зафиксировала неприятный факт: пресеты `Claude+GPT+Codex` и
`Full Codex` существуют ТОЛЬКО как объект `modelPresets` в
`frontend/static/js/app.js`. Сервер знает лишь плоскую таблицу
`stage_models.json` и не знает, что она означает. Из этого следуют две вещи:

  * компилировать план из пресета во фронтенде нельзя — план обязан быть
    серверной сущностью, иначе «что запустили» определяется браузером;
  * определить пресет сервер может только ОБРАТНЫМ сопоставлением сохранённой
    таблицы с эталонными раскладками. Значит эталоны обязаны жить и здесь, и
    расхождение с фронтендом обязано ловиться тестом, а не глазами.

Второй факт, ради которого модуль строгий: `stage_models.json` грузится без
всякой валидации. Файл не закоммичен и правится на проде руками; опечатка в
значении молча превращает ансамбль из трёх ног в одну модель, и аудит
продолжает идти — просто хуже. Ошибка конфигурации, не приводящая к отказу,
дороже отказа.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional

from backend.app.services.audit_routing import registry
from backend.app.services.audit_routing.plan import RoutingPlanError

#: Идентификаторы пресетов. Совпадают с ключами `modelPresets` во фронтенде —
#: это часть контракта, и её проверяет отдельный тест.
PRESET_CLAUDE_GPT_CODEX = "claude_gpt_codex"
PRESET_FULL_CODEX = "codex_exec"
#: Раскладка, не совпавшая ни с одним эталоном. Не ошибка: оператор вправе
#: задать модели вручную. Но и молчать об этом нельзя — план обязан честно
#: назвать себя «своей раскладкой», иначе отчёт будет утверждать, что прогон
#: шёл на пресете, которого никто не выбирал.
PRESET_CUSTOM = "custom"

KNOWN_PRESETS: tuple[str, ...] = (
    PRESET_CLAUDE_GPT_CODEX,
    PRESET_FULL_CODEX,
    PRESET_CUSTOM,
)

#: Ключи таблицы моделей. Порядок — порядок конвейера, а не алфавит.
STAGE_MODEL_KEYS: tuple[str, ...] = (
    "text_analysis",
    "block_batch",
    "findings_merge",
    "findings_critic",
    "findings_corrector",
    "norm_verify",
    "norm_fix",
    "norm_requote",
    "optimization",
    "optimization_critic",
    "optimization_corrector",
)

#: Селекторы-ансамбли. Не модели: каждый раскрывается компилятором в НЕСКОЛЬКО
#: действий с разными провайдерами.
ENSEMBLE_BLOCK = "ensemble/gpt-codex"
ENSEMBLE_OPTIMIZATION = "ensemble/claude-codex-opt"
ENSEMBLE_SELECTORS: tuple[str, ...] = (ENSEMBLE_BLOCK, ENSEMBLE_OPTIMIZATION)

#: Где ансамбль вообще допустим. Ансамбль оптимизации на этапе текста означал
#: бы, что компилятор не знает, что делать, — и «сделает что-нибудь».
ENSEMBLE_ALLOWED_ON: dict[str, str] = {
    ENSEMBLE_BLOCK: "block_batch",
    ENSEMBLE_OPTIMIZATION: "optimization",
}


@dataclass(frozen=True)
class ModelSelector:
    """Разобранное значение ячейки таблицы моделей.

    Хранит провайдера и КЛАСС модели, но не саму строку: строка нужна только для
    сообщений оператору и в план не попадает.
    """

    raw: str
    provider: Optional[str]
    model_class: Optional[str]
    ensemble: Optional[str] = None

    @property
    def is_ensemble(self) -> bool:
        return self.ensemble is not None

    @property
    def capability(self) -> Optional[str]:
        if self.model_class is None:
            return None
        return registry.CLASS_TO_CAPABILITY.get(self.model_class)


#: Точные строки, которые сегодня умеет предлагать интерфейс, и их класс.
#:
#: Классификация по ПРЕФИКСУ, а не по полному списку моделей: список меняется
#: при каждом поколении, а правило «opus — сильная, sonnet/haiku — дешёвая»
#: пережило уже три. Неизвестный префикс — отказ, а не «наверное, сильная».
_CLAUDE_STRONG_MARKERS: tuple[str, ...] = ("opus",)
_CLAUDE_CHEAP_MARKERS: tuple[str, ...] = ("sonnet", "haiku")


def parse_selector(raw: Any, *, stage_key: str = "") -> ModelSelector:
    """Разобрать значение ячейки. Неизвестное значение — отказ."""
    value = str(raw or "").strip()
    where = f"stage_models[{stage_key}]" if stage_key else "stage_models"
    if not value:
        raise RoutingPlanError(f"{where}: пустое значение модели")

    if value in ENSEMBLE_SELECTORS:
        expected = ENSEMBLE_ALLOWED_ON[value]
        if stage_key and stage_key != expected:
            raise RoutingPlanError(
                f"{where}: ансамбль {value!r} допустим только на этапе "
                f"{expected!r}"
            )
        return ModelSelector(raw=value, provider=None, model_class=None, ensemble=value)

    if value.startswith("claude-"):
        low = value.lower()
        if any(m in low for m in _CLAUDE_STRONG_MARKERS):
            return ModelSelector(value, registry.PROVIDER_CLAUDE, registry.MODEL_CLASS_STRONG)
        if any(m in low for m in _CLAUDE_CHEAP_MARKERS):
            return ModelSelector(value, registry.PROVIDER_CLAUDE, registry.MODEL_CLASS_CHEAP)
        raise RoutingPlanError(
            f"{where}: модель {value!r} — Claude неизвестного класса. Центр обязан "
            "уметь назвать КЛАСС модели (сильная/дешёвая), потому что в задание "
            "уезжает способность, а не строка"
        )

    if value.startswith("codex/"):
        return ModelSelector(value, registry.PROVIDER_CODEX, registry.MODEL_CLASS_STRONG)

    if value.startswith("openai/") or value.startswith("anthropic/") or "/" in value:
        # Всё, что идёт через внешний шлюз, — OpenRouter. Отдельный провайдер, а
        # не «разновидность Codex»: у него свой канал, свой ключ и своя цена.
        return ModelSelector(value, registry.PROVIDER_OPENROUTER, registry.MODEL_CLASS_STRONG)

    raise RoutingPlanError(
        f"{where}: неизвестный селектор модели {value!r}. Молчаливое умолчание "
        "здесь означало бы, что правка stage_models.json тихо меняет маршрут"
    )


def validate_stage_models(config: Mapping[str, Any]) -> dict[str, ModelSelector]:
    """Проверить таблицу моделей целиком. Возвращает разобранные селекторы.

    Проверяется ровно то, что молча ломается сегодня: неизвестный ключ этапа,
    неизвестный селектор, ансамбль не на своём этапе и нарушение ограничений
    `STAGE_MODEL_RESTRICTIONS`.
    """
    if not isinstance(config, Mapping):
        raise RoutingPlanError("stage_models: ожидается объект")

    problems: list[str] = []
    unknown = sorted(set(config) - set(STAGE_MODEL_KEYS))
    if unknown:
        problems.append(
            f"неизвестные ключи этапов {unknown} — конвейер их не читает, "
            "а оператор считает, что настроил"
        )
    missing = [k for k in STAGE_MODEL_KEYS if k not in config]
    if missing:
        problems.append(f"нет ключей {missing}: маршрут этих этапов не определён")

    parsed: dict[str, ModelSelector] = {}
    for key in STAGE_MODEL_KEYS:
        if key not in config:
            continue
        try:
            parsed[key] = parse_selector(config[key], stage_key=key)
        except RoutingPlanError as exc:
            problems.append(str(exc))

    # Ограничения интерфейса — часть контракта, а не подсказка.
    try:
        from backend.app.core import config as _cfg

        restrictions = dict(getattr(_cfg, "STAGE_MODEL_RESTRICTIONS", {}) or {})
    except Exception:                                   # noqa: BLE001 — офлайн-разбор
        restrictions = {}
    for key, allowed in restrictions.items():
        if key in config and str(config[key]) not in set(allowed):
            problems.append(
                f"stage_models[{key}]={config[key]!r} не входит в разрешённые "
                f"{sorted(allowed)}"
            )

    if problems:
        raise RoutingPlanError(
            "Таблица моделей этапов не принята:\n  • " + "\n  • ".join(problems)
        )
    return parsed


def reference_config(preset_id: str, *, codex_model_id: str) -> dict[str, str]:
    """Эталонная раскладка пресета. Зеркало `modelPresets` фронтенда.

    `codex_model_id` — то, во что фронтенд раскрывает плейсхолдер
    `__codex_exec__` (`resolvePresetModelId` → `codexModelId()`). Значение
    приходит извне, потому что оно зависит от `AUDIT_CODEX_STAGE_MODEL` центра.
    """
    base = {
        "text_analysis": "claude-opus-5",
        "block_batch": ENSEMBLE_BLOCK,
        "findings_merge": "claude-opus-5",
        "findings_critic": "claude-sonnet-5",
        "findings_corrector": "claude-sonnet-5",
        "norm_verify": "claude-opus-5",
        "norm_fix": "claude-opus-5",
        "norm_requote": "claude-opus-5",
        "optimization": ENSEMBLE_OPTIMIZATION,
        "optimization_critic": "claude-sonnet-5",
        "optimization_corrector": "claude-sonnet-5",
    }
    if preset_id == PRESET_CLAUDE_GPT_CODEX:
        return base
    if preset_id == PRESET_FULL_CODEX:
        codex = str(codex_model_id)
        return {
            **{k: codex for k in STAGE_MODEL_KEYS},
            "block_batch": ENSEMBLE_BLOCK,
            "optimization": ENSEMBLE_OPTIMIZATION,
        }
    raise RoutingPlanError(f"эталона для пресета {preset_id!r} нет")


def detect_preset(config: Mapping[str, Any], *, codex_model_id: str) -> str:
    """Какому пресету соответствует сохранённая таблица. Иначе — `custom`."""
    for preset_id in (PRESET_CLAUDE_GPT_CODEX, PRESET_FULL_CODEX):
        expected = reference_config(preset_id, codex_model_id=codex_model_id)
        if all(str(config.get(k, "")) == v for k, v in expected.items()):
            return preset_id
    return PRESET_CUSTOM
