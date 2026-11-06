"""Execute a reproducible source-only slice of the inventory using host evidence tools."""
import argparse
import asyncio
import json
from pathlib import Path
import sys
from source_contract import read, write, sha
import investigate


def prepare(work, discipline):
    inventory = work / 'inventory_v2' / discipline / 'registry.json'
    data = read(inventory)
    # Selection is independent of engineer answers and previously found defects.
    # Use the first three pump observations with operations in source packet order.
    selected = [o for o in data['observations']
                if 'насос' in o.get('kind', '').casefold() and o.get('checks')][:3]
    if len(selected) != 3:
        raise ValueError('Need three source pump observations')
    target = work / 'inventory_operations' / discipline
    target.mkdir(parents=True, exist_ok=False)
    (target / 'sources').mkdir()
    (target / 'sources' / discipline).symlink_to(work / 'sources' / discipline)
    (target / 'STOP_REQUESTED.json').symlink_to(work / 'STOP_REQUESTED.json')
    project = next(p for p in read(work / 'manifest.json') if p['discipline'] == discipline)
    write(target / 'manifest.json', [project])
    write(target / 'provider_policy.json', read(work / 'provider_policy.json'))
    write(target / 'selected_operations.json', selected)
    write(target / 'protocol.json', {
        'discipline': discipline, 'wrapper_sha256': sha(__file__),
        'runner_sha256': sha(investigate.__file__), 'inventory_snapshot_sha256': sha(inventory),
        'selected_sha256': sha(target / 'selected_operations.json'),
        'manifest_sha256': sha(target / 'manifest.json'),
        'selection': 'First three pump observations with checks in source packet order',
        'model': investigate.MODELS['codex'], 'effort': 'medium', 'steps_max': 24,
        'source_only': True, 'history_or_engineer_answers_supplied': False,
        'scope': 'Inventory continuation, not an independent repeat audit. '
                 'Inventory observations are unverified. Outcomes remain provisional.',
        'production_changed': False,
    })
    print('Selected', [o['observation_id'] for o in selected], flush=True)


async def run(work, discipline):
    target = work / 'inventory_operations' / discipline
    protocol = read(target / 'protocol.json')
    for path, expected in [(Path(__file__), protocol['wrapper_sha256']),
                           (Path(investigate.__file__), protocol['runner_sha256']),
                           (target / 'selected_operations.json', protocol['selected_sha256']),
                           (target / 'manifest.json', protocol['manifest_sha256'])]:
        if sha(path) != expected:
            raise ValueError(f'Protocol input changed: {path}')
    selected = read(target / 'selected_operations.json')
    investigate.PROMPT += '''
В этом запуске выполняй только операции по перечисленным ниже наблюдениям реестра.
Это предварительно извлечённые наблюдения, а не проверенные факты или готовые замечания.
Начни с чтения их исходных блоков; найди связанные спецификации, расчёты, графику
во всём комплекте через search/read/image. Не ограничивайся стартовыми ссылками.
Установи тождество объекта, зону, исполнение и режим до сравнения параметров.
На каждый исходный check дай отдельный результат в operations; в поле object укажи
observation_id. Не заменяй невыполненную операцию утверждением «проверено».
Для несовпадений сохраняй исходные значения, адреса и ограничения. Нет необходимых
данных — needs_data. done=true означает завершение этой ограниченной задачи,
а не всего проекта. Не делай никаких выводов об отсутствующих других системах.
Назначенные наблюдения (данные):
''' + json.dumps(selected, ensure_ascii=False)
    result = await investigate.run(target, discipline, 'codex')
    if result['status'] == 'failed':
        raise RuntimeError(result.get('error', 'Operation research failed'))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['prepare', 'run'])
    parser.add_argument('--work', type=Path, required=True)
    parser.add_argument('--discipline', required=True)
    args = parser.parse_args()
    if args.command == 'prepare':
        prepare(args.work, args.discipline)
    else:
        asyncio.run(run(args.work, args.discipline))
