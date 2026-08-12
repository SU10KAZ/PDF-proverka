"""Real grpcio process-scoped chaos coverage for the critical 12E C02 path."""
from __future__ import annotations

import json
import threading

from tests.chaos_harness_12e import GatewayProcess
from tests.test_agent_grpc_client_12c import (
    _registered_grpc_agent,
    _wait_until,
    grpc_e2e_env,
)


def test_12e_gateway_sigkill_recovery_is_persistence_only(grpc_e2e_env):
    """C02: hard Gateway death preserves work, outbox and ResultAck recovery."""
    from tests.test_distributed_workers_e2e import running_executor

    gateway = GatewayProcess.start(worker_settings=grpc_e2e_env[1])
    replacement = None
    config, agent, admin, identity = _registered_grpc_agent(grpc_e2e_env, gateway.port)
    created = admin.post(
        "/api/workers/jobs",
        json={
            "worker_id": identity["worker_id"],
            "project_id": "12e-gateway-sigkill",
            "params": {"label": "gateway-sigkill", "steps": 45, "step_seconds": 0.03},
        },
    ).json()["job"]
    try:
        with running_executor(config, max_jobs=1):
            agent.heartbeat.beat_once()
            assert _wait_until(lambda: agent.client._ready.is_set(), 10)
            assignment = _wait_until(
                lambda: agent.poller.poll(free_slots=1, compressions=["gzip"]), 15
            )
            assert assignment
            runner_outcome: dict = {}
            runner = threading.Thread(
                target=lambda: runner_outcome.update(agent.execute_job(assignment)), daemon=True,
            )
            runner.start()
            assert _wait_until(
                lambda: (agent.db.queue_item(assignment["attempt_id"]) or {}).get("state")
                == "running",
                10,
            )
            executor_before = agent.db.process_row(assignment["attempt_id"])
            epoch_before = json.loads(config.state_path.read_text())["connection_epoch"]

            gateway.sigkill()
            assert _wait_until(
                lambda: (agent.db.queue_item(assignment["attempt_id"]) or {}).get("state")
                in {"running", "finished"},
                4,
            )
            replacement = GatewayProcess.start(
                worker_settings=grpc_e2e_env[1], port=gateway.port
            )
            assert _wait_until(
                lambda: json.loads(config.state_path.read_text())["connection_epoch"] > epoch_before
                and agent.client._ready.is_set(),
                15,
            )
            # This single observer is deliberately the sender for C02: the
            # process fault is isolated from the separate C35 sender-race
            # test.  Its bounded predicate records durable convergence rather
            # than assuming a result has been delivered after an arbitrary
            # sleep.
            assert _wait_until(lambda: not runner.is_alive(), 30, 0.2)
            assert runner_outcome.get("ok"), runner_outcome
            assert _wait_until(
                lambda: (
                    agent._deliver_pending_results()
                    or (agent.jobs.load(created["job_id"], assignment["attempt_id"]) or {})
                    .get("retention_until")
                ),
                15,
                0.2,
            )
            executor_after = agent.db.process_row(assignment["attempt_id"])
            assert executor_after["pid"] == executor_before["pid"]
        final = admin.get(f"/api/workers/jobs/{created['job_id']}").json()["job"]
        assert final["state"] == "completed"
        events = admin.get(f"/api/workers/jobs/{created['job_id']}/events").json()["events"]
        sequences = [event["sequence"] for event in events]
        assert sequences == sorted(set(sequences))
    finally:
        agent.shutdown()
        if replacement is not None:
            replacement.stop()
        gateway.stop()
