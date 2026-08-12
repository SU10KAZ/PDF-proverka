"""Security primitives: redaction, fingerprints and secret-free subprocesses."""
from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import stat
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Mapping, Sequence


SECRET_KEY_RE = re.compile(
    r"(secret|token|password|passwd|private[_-]?key|api[_-]?key|credential|authorization)",
    re.IGNORECASE,
)
SECRET_TEXT_RE = re.compile(
    r"(?i)(bearer\s+|sk-[A-Za-z0-9_-]{8,}|(?:secret|token|password|api[_-]?key)\s*[=:]\s*)\S+"
)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def redact(value: Any) -> Any:
    """Рекурсивно убрать секретоподобные поля перед логом/API/state."""
    if isinstance(value, Mapping):
        return {
            str(key): ("[REDACTED]" if SECRET_KEY_RE.search(str(key)) else redact(item))
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact(item) for item in value)
    if isinstance(value, str):
        return SECRET_TEXT_RE.sub(lambda match: match.group(1) + "[REDACTED]", value)
    return value


def safe_json(value: Any) -> str:
    return json.dumps(redact(value), ensure_ascii=False, sort_keys=True)


def constant_time_hash_matches(plain: str, expected_hash: str) -> bool:
    return hmac.compare_digest(sha256_bytes(plain.encode()), expected_hash)


class HostKeyMismatch(RuntimeError):
    pass


def enroll_known_host(
    *, host: str, port: int, expected_fingerprint: str, known_hosts: Path
) -> str:
    """Получить ключ, сверить out-of-band fingerprint и записать атомарно.

    До этой функции SSH не запускается. TOFU намеренно нет: fingerprint должен
    прийти отдельным доверенным каналом.
    """
    scan = subprocess.run(
        ["ssh-keyscan", "-p", str(port), "--", host],
        check=True,
        capture_output=True,
        timeout=20,
    )
    if not scan.stdout.strip():
        raise HostKeyMismatch("ssh-keyscan не вернул ключ хоста")
    with tempfile.NamedTemporaryFile(prefix="worker-hostkey-", delete=False) as tmp:
        tmp.write(scan.stdout)
        tmp_path = Path(tmp.name)
    try:
        # Validate and persist the *same* key lines. Merely finding one
        # expected fingerprint and then trusting every line returned by
        # ssh-keyscan would also enroll unverified RSA/ECDSA keys.
        matching_lines: list[bytes] = []
        fingerprints: set[str] = set()
        for line in scan.stdout.splitlines():
            if not line.strip() or line.lstrip().startswith(b"#"):
                continue
            tmp_path.write_bytes(line + b"\n")
            shown = subprocess.run(
                ["ssh-keygen", "-lf", str(tmp_path), "-E", "sha256"],
                check=True,
                capture_output=True,
                text=True,
                timeout=10,
            )
            found = {
                part
                for output_line in shown.stdout.splitlines()
                for part in output_line.split()
                if part.startswith("SHA256:")
            }
            fingerprints.update(found)
            if expected_fingerprint in found:
                matching_lines.append(line)
        if not matching_lines:
            raise HostKeyMismatch(
                f"host key mismatch: ожидался {expected_fingerprint}, получены {sorted(fingerprints)}"
            )
        known_hosts.parent.mkdir(parents=True, exist_ok=True)
        fd, raw_tmp = tempfile.mkstemp(prefix=known_hosts.name + ".", dir=known_hosts.parent)
        os.close(fd)
        staged = Path(raw_tmp)
        staged.write_bytes(b"\n".join(matching_lines) + b"\n")
        staged.chmod(stat.S_IRUSR | stat.S_IWUSR)
        os.replace(staged, known_hosts)
        return expected_fingerprint
    finally:
        tmp_path.unlink(missing_ok=True)


def secret_free_argv(argv: Sequence[str]) -> None:
    """Fail closed, если вызывающий пытается положить секрет в argv."""
    safe_channel_flags = {"--bootstrap-secret-stdin"}
    secret_prefixes = ("wbt_", "wtk_", "clm_", "sk-or-", "sk-")
    for arg in argv:
        text = str(arg)
        if text in safe_channel_flags:
            continue
        lowered = text.lower()
        # Название stdin-флага не секрет. Опасны значение известного token
        # prefix, Authorization header и `--password=<value>` style forms.
        if text.startswith(secret_prefixes) or SECRET_TEXT_RE.search(text):
            raise ValueError("секреты запрещено передавать через argv")
        if "=" in text:
            name, value = text.split("=", 1)
            if SECRET_KEY_RE.search(name) and value:
                raise ValueError("секреты запрещено передавать через argv")
