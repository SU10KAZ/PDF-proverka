"""Run with: python -m backend.app.agent_gateway"""
from __future__ import annotations

import asyncio
import logging
import signal

from backend.app.agent_gateway.config import GatewayConfig
from backend.app.agent_gateway.server import GatewayServer


async def _main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    server = GatewayServer(GatewayConfig.from_env())
    stopped = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stopped.set)
    await server.start()
    await stopped.wait()
    await server.stop()


if __name__ == "__main__":
    asyncio.run(_main())
