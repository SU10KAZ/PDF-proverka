"""Reproducibly compile Agent Stream Protocol v1 without enabling gRPC runtime."""
from __future__ import annotations

import hashlib
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PROTO_FILES = (
    "contracts/agent_stream/v1/common.proto",
    "contracts/agent_stream/v1/agent_stream.proto",
)
DESCRIPTOR = ROOT / "contracts/agent_stream/v1/agent_stream_v1.desc"


def main() -> int:
    command = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{ROOT}",
        f"--python_out={ROOT}",
        f"--grpc_python_out={ROOT}",
        f"--descriptor_set_out={DESCRIPTOR}",
        "--include_imports",
        *PROTO_FILES,
    ]
    subprocess.run(command, cwd=ROOT, check=True)
    digest = hashlib.sha256(DESCRIPTOR.read_bytes()).hexdigest()
    print(f"generated {DESCRIPTOR.relative_to(ROOT)} sha256={digest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
