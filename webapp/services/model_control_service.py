"""
Utilities for controlling and monitoring local/remote LM Studio models.
"""

from __future__ import annotations

import csv
import os
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

import psutil
import requests
from dotenv import load_dotenv


ROOT_DIR = Path(__file__).resolve().parents[2]


def _load_env() -> None:
    load_dotenv(ROOT_DIR / ".env")


def _gib(value_bytes: int | float | None) -> float | None:
    if value_bytes is None:
        return None
    return round(float(value_bytes) / (1024 ** 3), 2)


def _mib(value_bytes: int | float | None) -> float | None:
    if value_bytes is None:
        return None
    return round(float(value_bytes) / (1024 ** 2), 1)


def _chandra_config() -> tuple[str, tuple[str, str] | None, dict[str, str], list[str]]:
    _load_env()
    base = os.environ.get("CHANDRA_BASE_URL", "").rstrip("/")
    user = os.environ.get("NGROK_AUTH_USER", "")
    password = os.environ.get("NGROK_AUTH_PASS", "")
    missing: list[str] = []
    if not base:
        missing.append("CHANDRA_BASE_URL")
    if not user:
        missing.append("NGROK_AUTH_USER")
    if not password:
        missing.append("NGROK_AUTH_PASS")
    auth = (user, password) if user and password else None
    headers = {"ngrok-skip-browser-warning": "true"}
    return base, auth, headers, missing


def _request_json(
    method: str,
    path: str,
    *,
    json_body: dict[str, Any] | None = None,
    timeout: float = 30,
) -> dict[str, Any]:
    base, auth, headers, missing = _chandra_config()
    if missing:
        return {
            "ok": False,
            "status_code": None,
            "elapsed_s": 0,
            "error": f"missing env vars: {', '.join(missing)}",
            "response": None,
        }
    if json_body is not None:
        headers = dict(headers)
        headers["content-type"] = "application/json"
    started = time.perf_counter()
    try:
        response = requests.request(
            method,
            f"{base}{path}",
            auth=auth,
            headers=headers,
            json=json_body,
            timeout=timeout,
        )
        elapsed = round(time.perf_counter() - started, 3)
        try:
            payload = response.json()
        except Exception:
            payload = {"raw_text": response.text[:4000]}
        ok = 200 <= response.status_code < 300 and not (isinstance(payload, dict) and payload.get("error"))
        return {
            "ok": ok,
            "status_code": response.status_code,
            "elapsed_s": elapsed,
            "error": payload.get("error") if isinstance(payload, dict) else None,
            "response": payload,
        }
    except Exception as exc:
        return {
            "ok": False,
            "status_code": None,
            "elapsed_s": round(time.perf_counter() - started, 3),
            "error": f"{type(exc).__name__}: {exc}",
            "response": None,
        }


def _models_payload() -> dict[str, Any]:
    return _request_json("GET", "/api/v1/models", timeout=30)


def _list_models() -> list[dict[str, Any]]:
    data = _models_payload()
    if not data["ok"]:
        return []
    return list((data.get("response") or {}).get("models", []))


def _loaded_instances(models: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for model in models:
        for instance in model.get("loaded_instances") or []:
            rows.append(
                {
                    "model_key": model.get("key"),
                    "display_name": model.get("display_name"),
                    "instance_id": instance.get("id"),
                    "config": instance.get("config") or {},
                    "capabilities": model.get("capabilities") or {},
                    "max_context_length": model.get("max_context_length"),
                    "size_bytes": model.get("size_bytes"),
                    "selected_variant": model.get("selected_variant"),
                }
            )
    return rows


def _system_memory() -> dict[str, Any]:
    vm = psutil.virtual_memory()
    swap = psutil.swap_memory()
    return {
        "ram": {
            "total_bytes": vm.total,
            "used_bytes": vm.used,
            "available_bytes": vm.available,
            "percent": round(vm.percent, 1),
            "total_gib": _gib(vm.total),
            "used_gib": _gib(vm.used),
            "available_gib": _gib(vm.available),
        },
        "swap": {
            "total_bytes": swap.total,
            "used_bytes": swap.used,
            "free_bytes": swap.free,
            "percent": round(swap.percent, 1),
            "total_gib": _gib(swap.total),
            "used_gib": _gib(swap.used),
            "free_gib": _gib(swap.free),
        },
        "cpu": {
            "percent": round(psutil.cpu_percent(interval=None), 1),
            "logical_cores": psutil.cpu_count(logical=True),
            "physical_cores": psutil.cpu_count(logical=False),
        },
    }


def _match_lmstudio_process(proc: psutil.Process) -> bool:
    try:
        name = (proc.info.get("name") or "").lower()
        cmdline = " ".join(proc.info.get("cmdline") or []).lower()
        return any(
            token in name or token in cmdline
            for token in ("lm studio", "lmstudio", "lms server", "lms", "lm-studio")
        )
    except Exception:
        return False


def _lmstudio_processes() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for proc in psutil.process_iter(["pid", "name", "cmdline", "memory_info", "cpu_percent"]):
        if not _match_lmstudio_process(proc):
            continue
        rss = getattr(proc.info.get("memory_info"), "rss", None)
        rows.append(
            {
                "pid": proc.info.get("pid"),
                "name": proc.info.get("name"),
                "cmdline": " ".join(proc.info.get("cmdline") or [])[:500],
                "rss_bytes": rss,
                "rss_mib": _mib(rss),
                "cpu_percent": round(float(proc.info.get("cpu_percent") or 0), 1),
            }
        )
    rows.sort(key=lambda item: item.get("rss_bytes") or 0, reverse=True)
    return rows


def _nvidia_gpu_stats() -> dict[str, Any]:
    exe = shutil.which("nvidia-smi")
    if not exe:
        return {
            "backend": "nvidia-smi",
            "available": False,
            "error": "nvidia-smi not found on this host",
            "gpus": [],
            "processes": [],
        }

    gpu_cmd = [
        exe,
        "--query-gpu=index,uuid,name,memory.total,memory.used,memory.free,utilization.gpu,temperature.gpu",
        "--format=csv,noheader,nounits",
    ]
    proc_cmd = [
        exe,
        "--query-compute-apps=gpu_uuid,pid,process_name,used_gpu_memory",
        "--format=csv,noheader,nounits",
    ]
    try:
        gpu_output = subprocess.run(gpu_cmd, capture_output=True, text=True, check=True, timeout=10)
        gpu_rows: list[dict[str, Any]] = []
        for row in csv.reader(gpu_output.stdout.splitlines()):
            if not row:
                continue
            row = [item.strip() for item in row]
            gpu_rows.append(
                {
                    "index": int(row[0]),
                    "uuid": row[1],
                    "name": row[2],
                    "memory_total_mib": int(row[3]),
                    "memory_used_mib": int(row[4]),
                    "memory_free_mib": int(row[5]),
                    "memory_total_gib": round(int(row[3]) / 1024, 2),
                    "memory_used_gib": round(int(row[4]) / 1024, 2),
                    "memory_free_gib": round(int(row[5]) / 1024, 2),
                    "utilization_gpu_pct": int(row[6]),
                    "temperature_c": int(row[7]),
                }
            )
    except Exception as exc:
        return {
            "backend": "nvidia-smi",
            "available": False,
            "error": f"{type(exc).__name__}: {exc}",
            "gpus": [],
            "processes": [],
        }

    process_rows: list[dict[str, Any]] = []
    try:
        proc_output = subprocess.run(proc_cmd, capture_output=True, text=True, check=True, timeout=10)
        text = proc_output.stdout.strip()
        if text and "no running processes found" not in text.lower():
            for row in csv.reader(text.splitlines()):
                if not row:
                    continue
                row = [item.strip() for item in row]
                gpu_uuid, pid, process_name, used_gpu_memory = row
                process_rows.append(
                    {
                        "gpu_uuid": gpu_uuid,
                        "pid": int(pid),
                        "process_name": process_name,
                        "used_gpu_memory_mib": int(used_gpu_memory),
                        "used_gpu_memory_gib": round(int(used_gpu_memory) / 1024, 2),
                    }
                )
    except Exception:
        pass

    return {
        "backend": "nvidia-smi",
        "available": True,
        "error": None,
        "gpus": gpu_rows,
        "processes": process_rows,
    }


def _cli_info() -> dict[str, Any]:
    lms_path = shutil.which("lms")
    return {
        "available": bool(lms_path),
        "path": lms_path,
    }


def get_status() -> dict[str, Any]:
    models_result = _models_payload()
    models = list((models_result.get("response") or {}).get("models", [])) if models_result["ok"] else []
    loaded = _loaded_instances(models)
    base, _, _, missing = _chandra_config()
    return {
        "generated_at": int(time.time()),
        "chandra": {
            "configured": not missing,
            "base_url": base,
            "missing_env": missing,
            "reachable": models_result["ok"],
            "error": models_result["error"],
            "elapsed_s": models_result["elapsed_s"],
        },
        "notes": {
            "host_metrics_scope": "current webapp host",
            "estimate_scope": "current host lms CLI",
            "remote_control_scope": "target Chandra LM Studio endpoint",
        },
        "system": _system_memory(),
        "gpu": _nvidia_gpu_stats(),
        "processes": {
            "lmstudio": _lmstudio_processes(),
        },
        "lms_cli": _cli_info(),
        "models": models,
        "loaded_instances": loaded,
        "summary": {
            "available_models": len(models),
            "loaded_instances": len(loaded),
            "vision_models": sum(1 for model in models if (model.get("capabilities") or {}).get("vision")),
        },
    }


def load_model(
    *,
    model: str,
    context_length: int,
    flash_attention: bool = True,
    offload_kv_cache_to_gpu: bool = True,
    eval_batch_size: int | None = None,
    num_experts: int | None = None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "model": model,
        "context_length": context_length,
        "flash_attention": flash_attention,
        "offload_kv_cache_to_gpu": offload_kv_cache_to_gpu,
        "echo_load_config": True,
    }
    if eval_batch_size is not None:
        body["eval_batch_size"] = eval_batch_size
    if num_experts is not None:
        body["num_experts"] = num_experts
    return _request_json("POST", "/api/v1/models/load", json_body=body, timeout=900)


def unload_instance(*, instance_id: str) -> dict[str, Any]:
    return _request_json(
        "POST",
        "/api/v1/models/unload",
        json_body={"instance_id": instance_id},
        timeout=180,
    )


def unload_all() -> dict[str, Any]:
    models_result = _models_payload()
    if not models_result["ok"]:
        return {
            "ok": False,
            "error": models_result["error"],
            "count": 0,
            "unloaded": [],
        }
    models = list((models_result.get("response") or {}).get("models", []))
    rows: list[dict[str, Any]] = []
    for item in _loaded_instances(models):
        result = unload_instance(instance_id=item["instance_id"])
        rows.append({"instance_id": item["instance_id"], "model_key": item["model_key"], **result})
    return {
        "ok": all(row.get("ok") for row in rows) if rows else True,
        "count": len(rows),
        "unloaded": rows,
    }


def estimate_load(
    *,
    model: str,
    context_length: int,
    gpu: str | None = None,
) -> dict[str, Any]:
    lms_path = shutil.which("lms")
    if not lms_path:
        return {"ok": False, "error": "lms CLI not found on this host", "raw_output": ""}

    cmd = [lms_path, "load", "--estimate-only", model, "--context-length", str(context_length)]
    if gpu:
        cmd.extend(["--gpu", gpu])

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "raw_output": ""}

    stdout = (proc.stdout or "").strip()
    stderr = (proc.stderr or "").strip()
    text = "\n".join(part for part in [stdout, stderr] if part).strip()

    gpu_match = re.search(r"Estimated GPU Memory:\s*([0-9.]+)\s*GB", text, re.I)
    total_match = re.search(r"Estimated Total Memory:\s*([0-9.]+)\s*GB", text, re.I)
    verdict_match = re.search(r"Estimate:\s*(.+)", text, re.I)

    return {
        "ok": proc.returncode == 0,
        "returncode": proc.returncode,
        "estimated_gpu_memory_gb": float(gpu_match.group(1)) if gpu_match else None,
        "estimated_total_memory_gb": float(total_match.group(1)) if total_match else None,
        "estimate_verdict": verdict_match.group(1).strip() if verdict_match else None,
        "raw_output": text,
        "command": cmd,
    }
