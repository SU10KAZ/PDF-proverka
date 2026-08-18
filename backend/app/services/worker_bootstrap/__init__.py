"""One-click bootstrap удалённых audit-worker (этап 11K).

Пакет — единственный доменный слой для CLI и HTTP API. Он не принимает
закрытые ключи или provider credentials: только ссылки на central secret-store.
"""

BOOTSTRAP_VERSION = "1.0.0"

from .manager import BootstrapManager
from .models import BootstrapOperation, BootstrapRequest, BootstrapState

__all__ = [
    "BootstrapManager",
    "BootstrapOperation",
    "BootstrapRequest",
    "BootstrapState",
    "BOOTSTRAP_VERSION",
]
