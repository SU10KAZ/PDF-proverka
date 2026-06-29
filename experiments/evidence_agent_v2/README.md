# Evidence Agent v2 (EV2) — независимый агент-верификатор замечаний

Изолированная альтернатива пакету
`backend/app/pipeline/stages/findings_review/evidence_verifier/` (его параллельно
пишет Cursor). Цель — построить **другой алгоритм** и потом честно сравнить два
подхода на одном golden-set. Файлы Cursor не редактируются; во время тестов на
локальных моделях через ngrok нельзя пересекаться (один 35B на LM Studio — потолок).

## Зачем

Пайплайн генерит замечания (`03_findings.json`); часть — ложные срабатывания
(ИИ неверно прочитал чертёж). KB-агент сверяет замечание с базой экспертных
решений. EV2 идёт дальше: **заново лезет в чертёж** и проверяет, подтверждается ли
замечание визуально, прогоняя графический блок через локальную vision-модель.

## Чем EV2 отличается от подхода Cursor

| | Cursor (`evidence_verifier`) | EV2 |
|--|--|--|
| Решение | один vision-вызов → модель сразу выдаёт `accept/reject` | **восприятие отдельно от суждения** |
| Роль модели | целостное суждение «верно ли замечание» | узкое: «что показано на чертеже и противоречит ли это утверждению» |
| Вердикт | зашит в промпт | **явная Python-политика** (тюнится, аудируется) |
| Робастность | один shot | **голосование самосогласованности** (K прогонов) |
| Смещение | — | **консервативное**: уверенный `reject` требует ≥2 согласных «yes»; один «yes» → `borderline` (не удаляем реальное замечание) |
| Golden-set | сырой join F-ID (доверяет на 100%) | **фильтр консистентности** (отбрасывает осыпавшиеся F-ID) |
| Метрика выбора модели | accuracy (перекошена классом) | сбалансированно, **по `false_reject` в первую очередь** |

## Карта файлов

| Файл | Роль |
|--|--|
| `context.py` | разрешение контекста замечания (блоки, PNG, OCR, MD). Верный вызов `compute_text_evidence(graph, ocr_index, findings)` |
| `golden.py` | сбалансированный сэмплер golden-set + фильтр консистентности + приоритет visual-misread |
| `extract.py` | шаг ВОСПРИЯТИЯ: vision-модель читает чертёж, возвращает `contradicts_finding=yes/no/cannot_tell` |
| `verify.py` | политика: агрегирует K голосов в `accept/reject/borderline/needs_human` |
| `ngrok_guard.py` | координация доступа к LM Studio (lock + снимок загруженных моделей) |
| `run_benchmark.py` | бенчмарк vision-моделей: `perception` (выбор модели, K=1) и `verdict` (полный EV2) |
| `test_policy_offline.py` | офлайн-тесты ядра без ngrok (10/10) |

## Запуск

```bash
# офлайн-тесты ядра (без ngrok)
python -m pytest experiments/evidence_agent_v2/test_policy_offline.py -q

# read-only: что сейчас загружено в LM Studio (не висит ли модель Cursor)
python -c "import sys;sys.path.insert(0,'.');from dotenv import load_dotenv;load_dotenv('.env');from experiments.evidence_agent_v2 import ngrok_guard;ngrok_guard.preflight()"

# бенчмарк выбора модели (perception, K=1, дёшево). СНАЧАЛА убедиться, что Cursor НЕ на ngrok!
python -m experiments.evidence_agent_v2.run_benchmark \
  --mode perception --per-class 12 \
  --models qwen/qwen3.6-35b-a3b qwen/qwen3.6-27b google/gemma-4-26b-a4b
```

## Найденные баги (касаются и кода Cursor)

1. **`compute_text_evidence` — неверная сигнатура.**
   `evidence_verifier/context_loader.py:195` зовёт `compute_text_evidence(items, block_info)`,
   но функция требует `(graph, ocr_index, findings)` → **TypeError на каждом замечании**;
   графический путь Cursor не загружает контекст ни для одного finding. EV2 делает верно.

2. **`DescribeResult.raw_text` не существует.**
   `evidence_verifier/graphic_verifier.py` читает `result.raw_text` / `result.parsed`,
   но у `DescribeResult` поля `full_raw_response` / `raw_response_excerpt` (и `status`
   для не-diff JSON = `invalid_json`). → **AttributeError** при первом vision-ответе.
   EV2 берёт `full_raw_response` и принимает `invalid_json`, если сырой текст есть.

3. **Golden-set контаминирован на ~8%.**
   Сверка `summary`@решения (из `decisions_log`) против текущего `finding.problem`:
   из 3199 графических кейсов **254 (8%)** — осыпавшийся F-ID (метка эксперта про
   ДРУГОЕ замечание; см. [[project_kb_orphans_root_cause]]). Коварно — первые по
   порядку кейсы (проект АР1.1-К5-К6) почти все битые. Бенчмарк по сырому golden-set
   меряет шум. EV2 фильтрует консистентность (consistency_gate).

## Baseline (qwen/qwen3.6-35b-a3b, verdict, 10+10, чистые метки)

`false_reject=0.4  true_reject=0.2  abstain=0.4  lat≈5.3s/кейс (K=2)`

Наблюдения:
- При temp=0 модель детерминирована → голосование идентичными ре-ранами не помогает;
  голосовать надо по **возмущениям** (разный масштаб картинки / разные модели).
- Высокий `false_reject` — во многом из-за подачи **первого** блока, который может не
  содержать предмета замечания → модель говорит «противоречит» вместо «не вижу».
  Следующий шаг: усилить промпт (сначала подтвердить видимость предмета) + подавать
  правильный блок/несколько блоков.

## Что дальше

1. Мультимодельный `perception`-бенчмарк (qwen3.6-35b / 27b / gemma-4-26b) — **по
   согласованию окна с оператором**, чтобы не пересечься с Cursor на ngrok.
2. Промпт восприятия: явный шаг «виден ли предмет замечания» → меньше ложных «yes».
3. Голосование по возмущениям (масштаб/модель), а не по идентичным temp-0 ре-ранам.
4. Финальное сравнение EV2 vs Cursor на одном консистентном срезе golden-set.
