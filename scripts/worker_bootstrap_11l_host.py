#!/usr/bin/env python3
"""Scoped host evidence and clean-user preparation for physical bootstrap 11L.

This is deliberately not an installer.  Installation, resume, repair and
update continue to use ``scripts/audit_worker_bootstrap.py``.  The commands in
this file cover the two administrator-only actions which precede that public
interface: read-only host inventory and creation of an empty Unix identity.
"""
from __future__ import annotations

import argparse
import base64
import json
import re
import subprocess
from pathlib import Path
from typing import Any


_SAFE_SSH_TARGET = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.@:-]{0,254}$")
_SAFE_USER = re.compile(r"^[a-z_][a-z0-9_-]{0,30}$")


class HostCommandError(RuntimeError):
    pass


def _ssh(target: str, command: str, *, stdin: str = "", timeout: int = 120) -> str:
    if not _SAFE_SSH_TARGET.fullmatch(target):
        raise ValueError("unsafe SSH target")
    result = subprocess.run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=15",
            target,
            command,
        ],
        input=stdin,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode:
        detail = (result.stderr or result.stdout)[-2000:].replace("\x00", "")
        raise HostCommandError(f"remote command failed ({result.returncode}): {detail}")
    return result.stdout


_INVENTORY_PROGRAM = r'''
import hashlib,json,os,platform,sqlite3,stat,subprocess,time
from pathlib import Path

def run(argv):
    p=subprocess.run(argv,text=True,capture_output=True,timeout=30,check=False)
    return {"rc":p.returncode,"stdout":p.stdout.splitlines(),"stderr_tail":p.stderr.splitlines()[-5:]}

def unit_rows(scope, pattern):
    argv=["systemctl"] + (["--user"] if scope=="user" else []) + ["list-unit-files","--type=service","--no-legend","--no-pager"]
    rows=[]
    for line in run(argv)["stdout"]:
        fields=line.split()
        if not fields or pattern not in fields[0].lower():
            continue
        name=fields[0]
        prefix=["systemctl"] + (["--user"] if scope=="user" else [])
        active=run(prefix+["is-active",name])
        rows.append({"unit":name,"enabled":fields[1] if len(fields)>1 else "unknown","active":active["stdout"][0] if active["stdout"] else "unknown"})
    return rows

def db_states(path):
    result={"exists":path.is_file(),"tables":[],"state_counts":{},"active_count":0}
    if not path.is_file(): return result
    try:
        c=sqlite3.connect("file:"+str(path)+"?mode=ro",uri=True)
        tables={r[0] for r in c.execute("select name from sqlite_master where type=?",("table",))}
        result["tables"]=sorted(tables)
        if "execution_queue" in tables:
            rows=c.execute("select local_state,count(*) from execution_queue group by local_state").fetchall()
            result["state_counts"]={str(k):int(v) for k,v in rows}
            terminal={"finished","failed","cancelled","executor_interrupted"}
            result["active_count"]=sum(int(v) for k,v in rows if str(k) not in terminal)
        c.close()
    except Exception as exc:
        result["error"]=type(exc).__name__
    return result

def credential_metadata(path):
    p=Path(path)
    if not p.exists(): return {"path":path,"exists":False}
    s=p.lstat()
    value={"path":path,"exists":True,"uid":s.st_uid,"mode":format(stat.S_IMODE(s.st_mode),"04o"),"size":s.st_size,"kind":"symlink" if p.is_symlink() else "file" if p.is_file() else "other"}
    if p.is_file() and s.st_size<=1024*1024:
        value["sha256"]=hashlib.sha256(p.read_bytes()).hexdigest()
    return value

os_release={}
for line in Path("/etc/os-release").read_text(errors="replace").splitlines():
    if "=" in line:
        k,v=line.split("=",1); os_release[k]=v.strip().strip('"')

protected_names=("apache","nginx","dovecot","exim","postfix","mysql","mariadb","named","ihttpd","plesk")
system_files=run(["systemctl","list-unit-files","--type=service","--no-legend","--no-pager"])["stdout"]
protected=[]
for line in system_files:
    fields=line.split()
    if fields and any(x in fields[0].lower() for x in protected_names):
        name=fields[0]
        state=run(["systemctl","is-active",name])
        protected.append({"unit":name,"enabled":fields[1] if len(fields)>1 else "unknown","active":state["stdout"][0] if state["stdout"] else "unknown"})

roots=[]
for root in sorted(Path("/home/coder").glob("audit-worker*")):
    if not root.is_dir(): continue
    release=None
    current=root/"current"
    if current.is_symlink():
        try: release=current.resolve().name
        except OSError: release="broken"
    roots.append({"path":str(root),"uid":root.stat().st_uid,"release":release,"db":db_states(root/"data"/"worker.db")})

listening=run(["ss","-lntupH"])
network={"addresses":run(["ip","-j","address","show"]),"routes":run(["ip","-j","route","show"]),"resolver":run(["resolvectl","status"])}
payload={
 "captured_at_epoch":time.time(),
 "host":{"hostname":platform.node(),"os":os_release,"kernel":platform.release(),"architecture":platform.machine(),"uptime_seconds":float(Path("/proc/uptime").read_text().split()[0]),"identity":run(["id"]),"users":run(["who"])},
 "resources":{"cpu_count":os.cpu_count(),"memory":run(["free","-b"]),"disk":run(["df","-PT"]),"mounts":run(["findmnt","-rn","-o","TARGET,SOURCE,FSTYPE,OPTIONS"])},
 "services":{"running":run(["systemctl","list-units","--type=service","--state=running","--no-legend","--no-pager"]),"protected":protected,"audit_system":unit_rows("system","audit-worker"),"audit_user":unit_rows("user","audit-worker")},
 "network":{"listening":listening,**network},
 "worker":{"roots":roots,"active_processes":[line for line in run(["ps","-eo","pid,user,stat,lstart,args","--sort=pid"])["stdout"] if "audit_worker" in line or "audit-worker" in line]},
 "coder_provider_credentials":[credential_metadata("/home/coder/.claude/.credentials.json"),credential_metadata("/home/coder/.codex/auth.json")],
 "safety":{"secret_values_collected":False,"provider_cli_invoked":False,"real_inference":{"claude":0,"codex":0,"openrouter":0}}
}
print(json.dumps(payload,ensure_ascii=False,sort_keys=True))
'''


def capture_inventory(target: str) -> dict[str, Any]:
    raw = _ssh(target, "python3 -", stdin=_INVENTORY_PROGRAM, timeout=180)
    return json.loads(raw)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def command_inventory(args: argparse.Namespace) -> int:
    payload = capture_inventory(args.ssh_target)
    _write_json(Path(args.output), payload)
    print(json.dumps({"output": args.output, "hostname": payload["host"]["hostname"]}, ensure_ascii=False))
    return 0


def command_prepare_user(args: argparse.Namespace) -> int:
    user = args.unix_user
    if not _SAFE_USER.fullmatch(user):
        raise ValueError("unsafe Unix user")
    public_key = Path(args.public_key).read_text(encoding="utf-8").strip()
    if not public_key.startswith(("ssh-ed25519 ", "ssh-rsa ", "ecdsa-sha2-")):
        raise ValueError("unsupported public key")
    payload = json.dumps({"user": user, "public_key": public_key})
    encoded = base64.b64encode(payload.encode()).decode()
    program = r'''
import base64,json,os,pwd,re,shutil,subprocess,time
from pathlib import Path
d=json.loads(base64.b64decode("__PAYLOAD__")); user=d["user"]
if not re.fullmatch(r"[a-z_][a-z0-9_-]{0,30}",user): raise SystemExit("unsafe user")
try: pwd.getpwnam(user); raise SystemExit("user already exists; refusing to reuse it")
except KeyError: pass
subprocess.run(["/usr/sbin/useradd","--create-home","--shell","/bin/bash","--user-group",user],check=True)
pw=pwd.getpwnam(user); home=Path(pw.pw_dir)
if home != Path("/home")/user: raise SystemExit("unexpected HOME")
for child in list(home.iterdir()):
    if child.is_dir() and not child.is_symlink(): shutil.rmtree(child)
    else: child.unlink()
empty_before_ssh=list(home.iterdir())==[]
ssh=home/".ssh"; ssh.mkdir(mode=0o700)
auth=ssh/"authorized_keys"; auth.write_text(d["public_key"]+"\n",encoding="utf-8"); auth.chmod(0o600)
os.chown(ssh,pw.pw_uid,pw.pw_gid); os.chown(auth,pw.pw_uid,pw.pw_gid); os.chown(home,pw.pw_uid,pw.pw_gid)
sudoers=Path("/etc/sudoers.d")/("audit-worker-bootstrap-"+user)
sudoers.write_text(f"{user} ALL=(root) NOPASSWD: /usr/bin/loginctl enable-linger {user}\n",encoding="utf-8")
sudoers.chmod(0o440)
subprocess.run(["/usr/sbin/visudo","-cf",str(sudoers)],check=True,stdout=subprocess.DEVNULL)
entries=sorted(p.name for p in home.iterdir())
print(json.dumps({"captured_at_epoch":time.time(),"unix_user":user,"uid":pw.pw_uid,"gid":pw.pw_gid,"home":str(home),"home_empty_before_admin_ssh":empty_before_ssh,"home_entries_after_admin_ssh":entries,"audit_worker_present":False,"repository_present":False,"claude_auth_present":False,"codex_auth_present":False,"openrouter_present":False,"provider_policy_present":False,"job_data_present":False,"worker_user_units_present":False,"sudo_scope":f"/usr/bin/loginctl enable-linger {user}","provider_cli_invoked":False,"real_inference":{"claude":0,"codex":0,"openrouter":0}},ensure_ascii=False,sort_keys=True))
'''
    raw = _ssh(
        args.ssh_target,
        "sudo -n python3 -",
        stdin=program.replace("__PAYLOAD__", encoded),
        timeout=120,
    )
    result = json.loads(raw)
    _write_json(Path(args.output), result)
    print(json.dumps({"output": args.output, "unix_user": user}, ensure_ascii=False))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    inventory = sub.add_parser("inventory")
    inventory.add_argument("--ssh-target", required=True)
    inventory.add_argument("--output", required=True)
    inventory.set_defaults(func=command_inventory)
    prepare = sub.add_parser("prepare-user")
    prepare.add_argument("--ssh-target", required=True)
    prepare.add_argument("--unix-user", required=True)
    prepare.add_argument("--public-key", required=True)
    prepare.add_argument("--output", required=True)
    prepare.set_defaults(func=command_prepare_user)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
