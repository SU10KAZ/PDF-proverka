"""I-P9 — inference exactly-once per attempt.

Инвариант формулируется одной строкой: **одна попытка + один вызов = не более
одного оплачиваемого обращения к модели, при любом числе перезапусков,
повторных доставок и replay'ев.**

Почему этого не даёт ни одна уже существующая гарантия подсистемы.

  * `probe_grant`/`inference_grant` списывают ЗАРАНЕЕ и защищают от «второго
    прогона» — но не от повторного вызова ВНУТРИ одного прогона (crash-loop
    исполнителя, повторный `_dispatch_action`, retry этапа);
  * `EventOutbox` и `last_seen_seq` (I-04) защищают от двойного ПРИМЕНЕНИЯ
    события центром — но событие идёт уже ПОСЛЕ вызова модели;
  * `completed.marker` / `process_exit.json` (I-17) отличают «процесс отработал»
    от «процесс исчез» — но их пишет процесс конвейера в конце, а деньги
    тратятся в середине.

Отсюда протокол журнала. Три файла на ключ, и порядок их появления и есть
доказательство:

    <ledger>/<key>.claim.json     — «вызов начат»; создаётся O_CREAT|O_EXCL
    <ledger>/<key>.result.json    — «вызов завершён, вот результат»
    <ledger>/<key>.indeterminate  — «начат, но исход неизвестен»

Разбор состояний при повторном входе:

  * есть `result` → возвращаем СОХРАНЁННЫЙ результат. Модель не зовём. Это и
    есть требуемое поведение «упали после inference, но до отправки»;
  * есть `claim`, нет `result` → исход НЕИЗВЕСТЕН. Второй вызов запрещён: мы не
    знаем, была ли попытка оплачена, а «на всякий случай повторить» — это
    ровно то, чего инвариант не допускает. Состояние терминальное и требует
    решения человека, как и `executor_interrupted` (§8.6);
  * нет ничего → вызов разрешён, `claim` создаётся ДО обращения к модели.

`O_CREAT|O_EXCL` выбран не как «атомарный на всякий случай», а потому что это
единственная примитивная операция, у которой ровно один победитель и на
локальной ФС, и между процессами: два исполнителя, одновременно вошедшие в
один каталог попытки, получат разные ответы без всякой договорённости.
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from audit_worker.providers.inference import ProviderInferenceResult

#: Состояния входа в журнал.
STATE_ALLOWED = "allowed"
STATE_REPLAY = "replay"
STATE_INDETERMINATE = "indeterminate"

LEDGER_DIRNAME = "inference"


class InferenceLedgerError(RuntimeError):
    """Журнал повреждён или недоступен."""


def ledger_dir(job_dir: Path) -> Path:
    """Журнал живёт ВНУТРИ каталога попытки.

    Не в каталоге воркера: журнал обязан удаляться вместе с попыткой и обязан
    уезжать в пакет результата как evidence. Общий журнал на все попытки дал бы
    ровно одну новую возможность — перепутать попытки.
    """
    return Path(job_dir) / LEDGER_DIRNAME


def call_key(
    *,
    attempt_id: str,
    provider: str,
    purpose: str,
    prompt: str,
    attachments_sha256: str = "",
    action_id: str = "",
) -> str:
    """Ключ вызова. Один вызов = одна пара (что спрашиваем, у кого).

    Промпт входит в ключ ХЭШЕМ: два разных этапа одной попытки — это два разных
    оплачиваемых вызова, и объединять их одним ключом значило бы, что второй
    этап молча получит ответ первого. При этом сам промпт в имени файла не
    появляется: имена файлов попадают в журналы и в пакет.

    `attachments_sha256` — отпечаток ВЛОЖЕНИЙ (этап 11F). Без него анализ двух
    разных чертежей с одинаковым текстом задания дал бы один и тот же ключ, и
    второй блок молча получил бы ответ по первому: не ошибка транспорта, а
    подмена данных, которую не видно ни по одному артефакту. Пустая строка
    сохраняет ключи текстовых вызовов побайтово теми же, что до 11F.

    `action_id` — НОГА АНСАМБЛЯ (этап 11I), и это не украшение журнала.

    Ансамбль этапа 01 даёт трём детекторам ОДИН И ТОТ ЖЕ промпт, ту же картинку
    и тот же purpose; две codex-ноги вдобавок идут через одного провайдера.
    Без этого поля их ключи совпадали БЫ побайтово, и вторая нога получила бы
    `replay` ответа первой: центр видел бы три ноги, оплачена была бы одна, а
    «независимые детекторы» превратились бы в одну модель, скопированную
    трижды. Заметить это по артефактам было бы невозможно — ответы совпадают
    ровно потому, что это один ответ.

    Пустая строка сохраняет ключи прежних вызовов побайтово теми же, что до 11I.
    """
    parts = [str(attempt_id), str(provider), str(purpose), str(prompt)]
    if attachments_sha256:
        parts.append(str(attachments_sha256))
    if action_id:
        parts.append("action:" + str(action_id))
    digest = hashlib.sha256(
        "\x00".join(parts).encode("utf-8", "replace")
    ).hexdigest()
    return f"{str(purpose)[:32]}-{digest[:32]}"


@dataclass(frozen=True)
class LedgerEntry:
    """Состояние ключа на момент входа."""

    state: str
    key: str
    result: Optional[ProviderInferenceResult] = None
    claimed_at: Optional[float] = None
    detail: str = ""
    #: pid процесса, который ФАКТИЧЕСКИ выполнил вызов и сохранил результат.
    #: Нужен, чтобы отличить «модель звали сейчас» от «результат прочитан из
    #: журнала». Разница существенна: во втором случае подписка не тратилась,
    #: и отчёт обязан говорить об этом прямо, а не показывать одинаковую
    #: картинку для оплаченного вызова и для повтора.
    performed_by_pid: Optional[int] = None
    completed_at: Optional[float] = None

    @property
    def may_call_model(self) -> bool:
        return self.state == STATE_ALLOWED

    def as_dict(self) -> dict[str, Any]:
        return {
            "state": self.state,
            "key": self.key,
            "claimed_at": self.claimed_at,
            "detail": self.detail,
            "has_result": self.result is not None,
            "performed_by_pid": self.performed_by_pid,
            "completed_at": self.completed_at,
        }


class InferenceLedger:
    """Журнал одной попытки. Ключей в нём может быть несколько."""

    def __init__(self, job_dir: Path, *, attempt_id: str, job_id: str = "") -> None:
        self.root = ledger_dir(job_dir)
        self.attempt_id = str(attempt_id)
        self.job_id = str(job_id)

    # ── пути ─────────────────────────────────────────────────────────────────
    def _claim_path(self, key: str) -> Path:
        return self.root / f"{key}.claim.json"

    def _result_path(self, key: str) -> Path:
        return self.root / f"{key}.result.json"

    def _indeterminate_path(self, key: str) -> Path:
        return self.root / f"{key}.indeterminate.json"

    # ── чтение ───────────────────────────────────────────────────────────────
    def inspect(self, key: str) -> LedgerEntry:
        """Что известно про ключ. НИЧЕГО не создаёт — для отчётов и проверок."""
        result = self._load_result(key)
        if result is not None:
            saved = self._load_json(self._result_path(key)) or {}
            return LedgerEntry(
                state=STATE_REPLAY, key=key, result=result,
                detail="результат уже сохранён",
                performed_by_pid=saved.get("performed_by_pid"),
                completed_at=saved.get("completed_at"),
            )
        claim = self._load_json(self._claim_path(key))
        if claim is not None:
            return LedgerEntry(
                state=STATE_INDETERMINATE, key=key,
                claimed_at=claim.get("claimed_at"),
                detail=(
                    "вызов начат, результат не сохранён: исход неизвестен, "
                    "повтор запрещён"
                ),
            )
        return LedgerEntry(state=STATE_ALLOWED, key=key, detail="вызовов не было")

    def _load_json(self, path: Path) -> Optional[dict[str, Any]]:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as exc:
            raise InferenceLedgerError(f"журнал {path.name} не читается: {exc}") from None
        return data if isinstance(data, dict) else None

    def _load_result(self, key: str) -> Optional[ProviderInferenceResult]:
        data = self._load_json(self._result_path(key))
        if data is None:
            return None
        payload = data.get("provider_result")
        if not isinstance(payload, dict):
            raise InferenceLedgerError(
                f"журнал {key}: сохранённый результат повреждён"
            )
        return ProviderInferenceResult.from_dict(payload)

    # ── протокол ─────────────────────────────────────────────────────────────
    def begin(self, key: str, *, provider: str, purpose: str,
              prompt_sha256: str) -> LedgerEntry:
        """Заявить вызов. Единственный победитель — тот, кто создал `claim`.

        Возвращает `allowed` только когда `claim` создан ЭТИМ вызовом. Любой
        другой исход означает, что модель звать нельзя.
        """
        self.root.mkdir(parents=True, exist_ok=True)
        existing = self.inspect(key)
        if existing.state != STATE_ALLOWED:
            return existing
        payload = {
            "key": key,
            "job_id": self.job_id,
            "attempt_id": self.attempt_id,
            "provider": provider,
            "purpose": purpose,
            "prompt_sha256": prompt_sha256,
            "claimed_at": time.time(),
            "pid": os.getpid(),
        }
        try:
            handle = os.open(
                str(self._claim_path(key)),
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o600,
            )
        except FileExistsError:
            # Гонка проиграна: победитель — другой процесс, и его вызов может
            # быть уже оплачен.
            return self.inspect(key)
        except OSError as exc:
            raise InferenceLedgerError(f"журнал {key}: не создать заявку: {exc}") from None
        with os.fdopen(handle, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        return LedgerEntry(state=STATE_ALLOWED, key=key,
                           claimed_at=payload["claimed_at"],
                           detail="заявка создана этим вызовом")

    def complete(self, key: str, result: ProviderInferenceResult) -> Path:
        """Сохранить результат. Пишется атомарно, читается после рестарта.

        Сохраняется ЛЮБОЙ исход, включая ошибочный: «модель ответила ошибкой» —
        это тоже израсходованная попытка, и повторять её автоматически нельзя.
        """
        self.root.mkdir(parents=True, exist_ok=True)
        target = self._result_path(key)
        payload = {
            "key": key,
            "job_id": self.job_id,
            "attempt_id": self.attempt_id,
            "completed_at": time.time(),
            # Кто выполнил. Читается обратно `inspect()`, и по нему этап
            # отличает собственный вызов от чужого сохранённого результата.
            "performed_by_pid": os.getpid(),
            "provider_result": result.as_dict(),
        }
        tmp = target.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, target)
        return target

    def mark_indeterminate(self, key: str, *, reason: str) -> Path:
        """Пометить ключ как «исход неизвестен» — явно и с причиной.

        Отдельный файл рядом с `claim` нужен для человека: `claim` без
        `result` уже означает неизвестность, но не объясняет её.
        """
        self.root.mkdir(parents=True, exist_ok=True)
        target = self._indeterminate_path(key)
        target.write_text(
            json.dumps(
                {"key": key, "attempt_id": self.attempt_id, "reason": reason,
                 "observed_at": time.time()},
                ensure_ascii=False, indent=2,
            ),
            encoding="utf-8",
        )
        return target

    def summary(self) -> dict[str, Any]:
        """Свод по всем ключам попытки — уезжает в evidence пакета."""
        if not self.root.is_dir():
            return {"keys": [], "calls_started": 0, "calls_completed": 0}
        keys = sorted({
            path.name.split(".", 1)[0] for path in self.root.glob("*.json")
        })
        rows = []
        started = completed = 0
        for key in keys:
            try:
                entry = self.inspect(key)
            except InferenceLedgerError as exc:
                rows.append({"key": key, "state": "corrupt", "detail": str(exc)})
                continue
            if entry.state in (STATE_REPLAY, STATE_INDETERMINATE):
                started += 1
            if entry.state == STATE_REPLAY:
                completed += 1
            rows.append(entry.as_dict())
        return {"keys": rows, "calls_started": started, "calls_completed": completed}
