"""Restore a downstream stage's model from its source run, never server defaults."""
from .contracts import MODEL_PROFILES, REASONING


def from_state(state):
    provenance = (state or {}).get('provenance') or {}
    matches = [p for p in MODEL_PROFILES.values()
               if p.model == provenance.get('model') and p.provider == provenance.get('provider')]
    if len(matches) != 1:
        raise ValueError('Модель исходного прогона не определена; автоматическая подмена запрещена.')
    profile = matches[0]
    if provenance.get('model_profile') not in (None, '', profile.key):
        raise ValueError('Профиль и модель исходного прогона не совпадают.')
    if provenance.get('reasoning') not in (None, REASONING):
        raise ValueError('Режим reasoning исходного прогона не поддерживается.')
    return profile


def for_run(session_id, pair_id, run_id):
    from . import run_storage
    state = run_storage.read(run_storage.run_dir(session_id, pair_id, run_id) / 'state.json')
    if not state or state.get('run_id') != run_id:
        raise ValueError('Настройки модели исходного прогона недоступны.')
    return from_state(state)
