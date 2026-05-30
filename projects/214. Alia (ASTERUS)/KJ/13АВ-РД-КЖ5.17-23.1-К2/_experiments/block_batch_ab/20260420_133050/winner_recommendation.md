# Winner recommendation — Claude stage 02 block_batch

**Production profile:** `conservative` с parallelism=**3**.

## Почему

- НЕТ runs с coverage=100% — victor выбран из всего пула по elapsed.

## Альтернативы

- **Fastest** (короткий elapsed): `conservative_p3` — если бюджет времени критичен.
- **Best quality** (coverage+findings): `aggressive_p1` — если важна полнота разбора.

## Компромисс

- Consercative профиль даёт больше батчей, но каждый короче — меньше context-dilution, выше шанс coverage=100% и меньше unreadable. Компенсируется более длинным elapsed.
- Aggressive профиль ужимает число батчей, но рискует пробить attention на heavy-блоках.
- Parallelism=1 — самый стабильный, но в N раз медленнее. Parallelism=3 — жмёт rate limit окно.
