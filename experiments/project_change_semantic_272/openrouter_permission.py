"""Stop legacy bulk inference: it has no per-request user approval channel.

This is a deliberate stop, not an environment-controlled opt-in. The agent must
ask the user before each OpenRouter request, including free account reads.
Existing bulk runners cannot fulfill that contract and must not auto-resume.
Offline preparation and source analysis remain available. A future network
executor must enforce per-request consent before these runners can be enabled.
"""


class OpenRouterPermissionRequired(PermissionError):
    pass


def require_openrouter_permission():
    raise OpenRouterPermissionRequired(
        'OPENROUTER_USER_PERMISSION_REQUIRED: ask the user before each request '
        'and wait for explicit consent. This bulk runner has no per-request '
        'approval channel and is disabled; credentials, quota and paid API '
        'settings cannot authorize it.'
    )
