#!/bin/sh
# Install beside stop_vc_projects_stand.py as the stand's stop.sh.
set -eu
exec /usr/bin/python3 "$(dirname "$0")/stop_vc_projects_stand.py" \
    /home/coder/auditmanager/stands/vc-projects-page "$@"
