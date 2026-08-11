"""Run with: python -m backend.app.agent_gateway"""
from __future__ import annotations

import asyncio
import logging
import signal

from backend.app.agent_gateway.config import GatewayConfig
from backend.app.agent_gateway.server import GatewayServer
from backend.app.security.issuer_rpc import UnixSocketRenewalAuthority


async def _main() -> None:
    config = GatewayConfig.from_env()
    logging.basicConfig(
        level=getattr(logging, config.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    renewal = (
        UnixSocketRenewalAuthority(config.issuer_socket_path)
        if config.issuer_socket_path is not None else None
    )
    server = GatewayServer(config, renewal_authority=renewal)
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stopped.set)
    await server.start()
    await stopped.wait()
    await server.stop()


if __name__ == "__main__":
    asyncio.run(_main())
