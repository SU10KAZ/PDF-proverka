"""Управление аккаунтами портала (страница «Пользователи», только admin).

Работает только в режиме ``accounts``. В аварийном режиме (``emergency_owner``)
и прежнем ``env_users`` — 503: менять права некуда. Ответы — проекция без
хешей; временный пароль при сбросе возвращается один раз и нигде не пишется.
Операции, меняющие статус, роль или версию сессии, сразу закрывают открытые
WebSocket этого аккаунта.
"""
from __future__ import annotations

import asyncio
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, ConfigDict

from backend.app.core.access import (
    MODE_ACCOUNTS,
    SECTION_TITLES,
    SECTIONS,
    SECTIONS_WITHOUT_DOWNLOADS,
    AccountContext,
    portal_mode,
    require_admin,
)

router = APIRouter(prefix="/api/admin", tags=["accounts-admin"])


def _admin(request: Request) -> AccountContext:
    ctx = require_admin(request)
    if portal_mode() != MODE_ACCOUNTS:
        raise HTTPException(503, {"code": "accounts_mode_required",
                                  "message": "Управление пользователями доступно в режиме аккаунтов."})
    return ctx


class ReasonBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    reason: str = ""


class SectionsBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    sections: dict[str, dict[str, bool]]


class RoleBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    role: str


class UploadProjectsBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    upload_projects: bool


class StaffBody(BaseModel):
    model_config = ConfigDict(extra="forbid")
    staff_id: Optional[str] = None


async def _call(fn, *args: Any) -> Any:
    from backend.app.services.accounts import store

    try:
        return await asyncio.to_thread(fn, *args)
    except store.AccountError as exc:
        raise HTTPException(exc.status, {"code": exc.code, "message": exc.message}) from exc


async def _revoke_ws(account_id: int) -> None:
    from backend.app.ws.manager import ws_manager

    await ws_manager.revoke_account(account_id)


@router.get("/accounts/meta")
async def accounts_meta(ctx: AccountContext = Depends(_admin)):
    """Справочник для страницы: разделы и сотрудники users.json для связи."""
    from backend.app.services.common import user_service

    staff = await asyncio.to_thread(user_service.list_users)
    return {
        "sections": [{"code": s, "title": SECTION_TITLES[s],
                      "downloads": s not in SECTIONS_WITHOUT_DOWNLOADS} for s in SECTIONS],
        "staff": [{"id": u.get("id"), "name": u.get("name")} for u in staff],
        "me": ctx.account_id,
    }


@router.get("/accounts")
async def list_accounts(status: Optional[str] = Query(None), _ctx: AccountContext = Depends(_admin)):
    from backend.app.services.accounts import store

    return {"accounts": await _call(store.list_accounts, status)}


@router.get("/accounts/{account_id}")
async def get_account(account_id: int, _ctx: AccountContext = Depends(_admin)):
    from backend.app.services.accounts import store

    acc = await _call(store.get_account, account_id)
    if acc is None:
        raise HTTPException(404, {"code": "not_found", "message": "Аккаунт не найден."})
    return {"account": acc, "events": await _call(store.list_events, 100, account_id)}


@router.post("/accounts/{account_id}/approve")
async def approve(account_id: int, ctx: AccountContext = Depends(_admin)):
    from backend.app.services.accounts import store

    return {"account": await _call(store.approve, ctx.account_id, account_id)}


@router.post("/accounts/{account_id}/reject")
async def reject(account_id: int, body: ReasonBody, ctx: AccountContext = Depends(_admin)):
    from backend.app.services.accounts import store

    acc = await _call(store.reject, ctx.account_id, account_id, body.reason)
    await _revoke_ws(account_id)
    return {"account": acc}


@router.post("/accounts/{account_id}/block")
async def block(account_id: int, body: ReasonBody, ctx: AccountContext = Depends(_admin)):
    from backend.app.services.accounts import store

    acc = await _call(store.block, ctx.account_id, account_id, body.reason)
    await _revoke_ws(account_id)
    return {"account": acc}


@router.post("/accounts/{account_id}/unblock")
async def unblock(account_id: int, ctx: AccountContext = Depends(_admin)):
    from backend.app.services.accounts import store

    return {"account": await _call(store.unblock, ctx.account_id, account_id)}


@router.post("/accounts/{account_id}/reset-password")
async def reset_password(account_id: int, ctx: AccountContext = Depends(_admin)):
    from backend.app.services.accounts import store

    acc, temp = await _call(store.reset_password, ctx.account_id, account_id)
    await _revoke_ws(account_id)
    return {"account": acc, "temporary_password": temp,
            "message": "Временный пароль показывается один раз. Передайте его пользователю."}


@router.post("/accounts/{account_id}/revoke-sessions")
async def revoke_sessions(account_id: int, ctx: AccountContext = Depends(_admin)):
    from backend.app.services.accounts import store

    acc = await _call(store.revoke_sessions, ctx.account_id, account_id)
    await _revoke_ws(account_id)
    return {"account": acc}


@router.put("/accounts/{account_id}/sections")
async def set_sections(account_id: int, body: SectionsBody, ctx: AccountContext = Depends(_admin)):
    from backend.app.services.accounts import store

    return {"account": await _call(store.set_sections, ctx.account_id, account_id, body.sections)}


@router.put("/accounts/{account_id}/role")
async def set_role(account_id: int, body: RoleBody, ctx: AccountContext = Depends(_admin)):
    from backend.app.services.accounts import store

    acc = await _call(store.set_role, ctx.account_id, account_id, body.role)
    await _revoke_ws(account_id)
    return {"account": acc}


@router.put("/accounts/{account_id}/upload-projects")
async def set_upload_projects(account_id: int, body: UploadProjectsBody, ctx: AccountContext = Depends(_admin)):
    from backend.app.services.accounts import store

    return {"account": await _call(store.set_upload_projects, ctx.account_id, account_id, body.upload_projects)}


@router.put("/accounts/{account_id}/staff")
async def set_staff(account_id: int, body: StaffBody, ctx: AccountContext = Depends(_admin)):
    from backend.app.services.accounts import store

    return {"account": await _call(store.set_staff, ctx.account_id, account_id, body.staff_id)}


@router.delete("/accounts/{account_id}")
async def delete_account(account_id: int, ctx: AccountContext = Depends(_admin)):
    from backend.app.services.accounts import store

    await _call(store.delete, ctx.account_id, account_id)
    return {"ok": True}


@router.get("/auth-events")
async def auth_events(limit: int = Query(200), _ctx: AccountContext = Depends(_admin)):
    from backend.app.services.accounts import store

    return {"events": await _call(store.list_events, limit)}
