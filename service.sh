#!/bin/sh
set -eu
ROOT=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
LABEL="gui/$(id -u)/com.cherry.qq-collector"
PLIST="$HOME/Library/LaunchAgents/com.cherry.qq-collector.plist"
case "${1:-status}" in
  status) exec "$ROOT/.venv/bin/python" "$ROOT/collector.py" status ;;
  start) launchctl bootstrap "gui/$(id -u)" "$PLIST" ;;
  stop) launchctl bootout "$LABEL" ;;
  restart) launchctl kickstart -k "$LABEL" ;;
  logs) exec tail -n 50 -f "$ROOT/logs/collector.log" ;;
  *) echo "Usage: $0 {status|start|stop|restart|logs}" >&2; exit 2 ;;
esac
