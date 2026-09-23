"""Test-run safety: forbid internet sockets and research corpus reads."""
import os
import socket
import sys

_original_connect = socket.socket.connect

def connect(sock, address):
    if sock.family in (socket.AF_INET, socket.AF_INET6) and address[0] not in ('127.0.0.1', '::1', 'localhost'):
        raise AssertionError('External network forbidden during kill-switch tests')
    return _original_connect(sock, address)
socket.socket.connect = connect

def audit(event, args):
    if event == 'open' and args and isinstance(args[0], (str, bytes)):
        name = os.fsdecode(args[0])
        if '/corpus-audits/' in name or '/experiments/project_change_272/' in name:
            raise AssertionError('Research corpus access forbidden during kill-switch tests')
sys.addaudithook(audit)
